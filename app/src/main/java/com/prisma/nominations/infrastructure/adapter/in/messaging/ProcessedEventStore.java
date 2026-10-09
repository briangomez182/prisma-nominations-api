package com.prisma.nominations.infrastructure.adapter.in.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.UUID;

/**
 * Deduplicación del lado del consumidor: una fila por (consumidor, event_id) ya procesado.
 * Debe llamarse dentro de la misma transacción que el efecto del procesamiento.
 */
@Component
class ProcessedEventStore {

    private final JdbcTemplate jdbc;
    private final Clock clock;

    ProcessedEventStore(JdbcTemplate jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    /**
     * Registra el evento como procesado. Atómico frente a entregas concurrentes (rebalanceo): la PK decide.
     *
     * @return false si ese consumidor ya lo había procesado (duplicado)
     */
    boolean markProcessed(String consumer, UUID eventId) {
        int inserted = jdbc.update("""
                INSERT INTO consumer_processed_events (consumer, event_id, processed_at)
                VALUES (?, ?, ?)
                ON CONFLICT (consumer, event_id) DO NOTHING
                """, consumer, eventId, Timestamp.from(clock.instant()));
        return inserted == 1;
    }
}
