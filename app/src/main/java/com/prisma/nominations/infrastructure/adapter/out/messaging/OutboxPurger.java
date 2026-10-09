package com.prisma.nominations.infrastructure.adapter.out.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;

/**
 * Purga de eventos ya publicados con más antigüedad que {@code nominations.outbox.retention}: la tabla
 * queda chica y el índice parcial de pendientes no se degrada (E10). Los pendientes nunca se borran.
 *
 * <p>Con Debezium en producción el relay se reemplaza, pero la purga se mantiene (o se pasa a borrar
 * apenas se inserta, porque CDC lee del WAL y no necesita la fila).
 */
@Component
@ConditionalOnProperty(name = "nominations.outbox.purge.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPurger {

    private static final Logger log = LoggerFactory.getLogger(OutboxPurger.class);

    /** Borrado por tandas: transacciones cortas, sin bloquear la tabla ni inflar el WAL de golpe. */
    static final int DELETE_CHUNK = 1_000;

    private static final String DELETE_PUBLISHED_BEFORE = """
            DELETE FROM outbox_events
             WHERE id IN (SELECT id FROM outbox_events
                           WHERE published_at IS NOT NULL AND published_at < ?
                           LIMIT ?)
            """;

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final Duration retention;

    OutboxPurger(JdbcTemplate jdbc, Clock clock, OutboxProperties properties) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.retention = properties.retention();
    }

    @Scheduled(initialDelayString = "${nominations.outbox.purge.initial-delay:1m}",
            fixedDelayString = "${nominations.outbox.purge.fixed-delay:1h}")
    void scheduledPurge() {
        try {
            purge();
        } catch (RuntimeException e) {
            log.error("Outbox: falló la purga: {}", OutboxRelay.describe(e));
        }
    }

    /** Borra los publicados antes de {@code now - retention}. Devuelve cuántas filas borró. */
    public int purge() {
        Timestamp cutoff = Timestamp.from(clock.instant().minus(retention));
        int total = 0;
        int deleted;
        do {
            deleted = jdbc.update(DELETE_PUBLISHED_BEFORE, cutoff, DELETE_CHUNK);
            total += deleted;
        } while (deleted == DELETE_CHUNK);
        if (total > 0) {
            log.info("Outbox: {} eventos publicados purgados (retención {})", total, retention);
        }
        return total;
    }
}
