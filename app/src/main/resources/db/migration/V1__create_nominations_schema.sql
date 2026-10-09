-- =====================================================================
-- Estado actual: una fila por nominación. Fuente de verdad del estado.
-- =====================================================================
CREATE TABLE nominations (
    id               UUID         PRIMARY KEY,
    entity_id        VARCHAR(20)  NOT NULL,
    request_id       UUID         NOT NULL,
    customer_id      VARCHAR(36)  NOT NULL,
    account_id       VARCHAR(34)  NOT NULL,   -- en producción: cifrado de columna (KMS); se expone enmascarado
    card_token       VARCHAR(64)  NOT NULL,   -- token o referencia enmascarada, nunca el PAN
    alias            VARCHAR(50),
    status           VARCHAR(24)  NOT NULL,
    rejection_reason VARCHAR(40),             -- motivo normalizado
    abm_reason_code  VARCHAR(40),             -- código original de ABM, solo auditoría
    correlation_id   VARCHAR(64)  NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL,
    version          BIGINT       NOT NULL DEFAULT 0,

    -- Idempotencia de ingreso: una entidad no puede repetir request_id, aun con requests concurrentes.
    CONSTRAINT uq_nominations_entity_request UNIQUE (entity_id, request_id),
    CONSTRAINT ck_nominations_status
        CHECK (status IN ('RECEIVED', 'PENDING_ABM', 'APPROVED', 'REJECTED', 'ABM_TIMEOUT')),
    -- Solo un rechazo tiene motivo, y todo rechazo lo tiene.
    CONSTRAINT ck_nominations_rejection_reason
        CHECK ((status = 'REJECTED') = (rejection_reason IS NOT NULL))
);

-- Sweeper: nominaciones abiertas hace más tiempo que el SLA de ABM.
CREATE INDEX ix_nominations_open_by_updated_at
    ON nominations (updated_at)
    WHERE status IN ('RECEIVED', 'PENDING_ABM');

-- =====================================================================
-- Historial: una fila por transición. Solo inserción (auditoría).
-- =====================================================================
CREATE TABLE nomination_history (
    id             BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nomination_id  UUID         NOT NULL REFERENCES nominations (id),
    from_status    VARCHAR(24),                -- NULL en la creación
    to_status      VARCHAR(24)  NOT NULL,
    source         VARCHAR(20)  NOT NULL,      -- API, ABM_ADAPTER, ABM_RESPONSE, SWEEPER, OPERATOR
    detail         VARCHAR(255),
    correlation_id VARCHAR(64)  NOT NULL,
    occurred_at    TIMESTAMPTZ  NOT NULL
);

CREATE INDEX ix_nomination_history_nomination ON nomination_history (nomination_id, occurred_at);

-- La auditoría no se reescribe: se bloquea UPDATE y DELETE a nivel base, no solo en la aplicación.
CREATE FUNCTION forbid_history_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'nomination_history es solo de inserción';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_nomination_history_append_only
    BEFORE UPDATE OR DELETE ON nomination_history
    FOR EACH ROW EXECUTE FUNCTION forbid_history_mutation();

-- =====================================================================
-- Transactional Outbox: eventos escritos en la misma TX que el cambio de estado.
-- =====================================================================
CREATE TABLE outbox_events (
    id             UUID          PRIMARY KEY,   -- también es el event_id para deduplicar en consumidores
    aggregate_id   UUID          NOT NULL,
    aggregate_type VARCHAR(40)   NOT NULL,
    event_type     VARCHAR(60)   NOT NULL,
    topic          VARCHAR(100)  NOT NULL,
    payload        JSONB         NOT NULL,
    headers        JSONB,
    created_at     TIMESTAMPTZ   NOT NULL,
    published_at   TIMESTAMPTZ,
    attempts       INT           NOT NULL DEFAULT 0,
    last_error     VARCHAR(500)
);

-- Relay: pendientes en orden de creación.
CREATE INDEX ix_outbox_events_pending ON outbox_events (created_at) WHERE published_at IS NULL;

-- Un único evento de resultado por nominación, garantizado por la base (E3, E7).
CREATE UNIQUE INDEX uq_outbox_events_single_result
    ON outbox_events (aggregate_id)
    WHERE event_type = 'nomination.result';
