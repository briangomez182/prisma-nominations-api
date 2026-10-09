-- =====================================================================
-- Deduplicación del lado del consumidor (at-least-once → efecto único).
-- Una fila por (consumidor, event_id) procesado. La usa el consumidor de ejemplo de
-- nomination.result.v1; cada consumidor real tendría la suya en su propia base.
-- =====================================================================
CREATE TABLE consumer_processed_events (
    consumer     VARCHAR(100) NOT NULL,   -- consumer group
    event_id     UUID         NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL,
    PRIMARY KEY (consumer, event_id)
);

-- Purga de filas viejas (más allá de la retención del tópico ya no puede llegar un duplicado).
CREATE INDEX ix_consumer_processed_events_processed_at ON consumer_processed_events (processed_at);
