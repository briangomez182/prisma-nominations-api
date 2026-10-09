-- El relay publica solo el evento pendiente más viejo de cada nominación (orden por agregado
-- aun con varias instancias en paralelo). Este índice soporta esa búsqueda.
CREATE INDEX ix_outbox_events_pending_by_aggregate
    ON outbox_events (aggregate_id, created_at)
    WHERE published_at IS NULL;
