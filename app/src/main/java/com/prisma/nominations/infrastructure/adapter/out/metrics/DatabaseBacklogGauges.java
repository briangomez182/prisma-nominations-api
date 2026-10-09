package com.prisma.nominations.infrastructure.adapter.out.metrics;

import com.prisma.nominations.domain.NominationStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Gauges de backlog leídos de la base: nominaciones abiertas por estado y estado del outbox. Son los indicadores
 * adelantados de degradación (crecen antes de que el cliente note demoras).
 * <ul>
 *   <li>{@code nominations.open{status}}: RECEIVED, PENDING_ABM y ABM_TIMEOUT.</li>
 *   <li>{@code outbox.pending}, {@code outbox.failing} (pendientes con algún intento fallido) y
 *       {@code outbox.oldest.pending.age} (segundos; 0 si no hay pendientes).</li>
 * </ul>
 * <b>Cache:</b> las consultas no se hacen en cada scrape: el primer gauge leído después de vencido el TTL refresca
 * la foto completa (dos consultas, sobre índices parciales). La antigüedad se calcula al leer, contra la fecha de
 * creación cacheada, así que sigue creciendo entre refrescos. Si la base falla se conserva la última foto.
 */
@Component
public class DatabaseBacklogGauges {

    private static final Logger log = LoggerFactory.getLogger(DatabaseBacklogGauges.class);

    static final List<NominationStatus> OPEN_STATUSES =
            List.of(NominationStatus.RECEIVED, NominationStatus.PENDING_ABM, NominationStatus.ABM_TIMEOUT);

    private static final String COUNT_OPEN = """
            SELECT status, count(*) FROM nominations
             WHERE status IN ('RECEIVED', 'PENDING_ABM', 'ABM_TIMEOUT')
             GROUP BY status
            """;
    private static final String OUTBOX_BACKLOG = """
            SELECT count(*), count(*) FILTER (WHERE attempts > 0), min(created_at)
              FROM outbox_events
             WHERE published_at IS NULL
            """;

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final Duration ttl;

    private volatile Snapshot snapshot = Snapshot.EMPTY;
    private volatile Instant refreshedAt;
    private boolean failing;

    public DatabaseBacklogGauges(JdbcTemplate jdbc, Clock clock, MeterRegistry registry,
                                 @Value("${nominations.metrics.db-gauges.cache-ttl:15s}") Duration ttl) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.ttl = ttl;
        for (var status : OPEN_STATUSES) {
            Gauge.builder("nominations.open", this, g -> g.current().open().getOrDefault(status, 0L))
                    .description("Nominaciones abiertas por estado (sin resultado final)")
                    .tag("status", status.name())
                    .register(registry);
        }
        Gauge.builder("outbox.pending", this, g -> g.current().outboxPending())
                .description("Eventos del outbox sin publicar en Kafka")
                .register(registry);
        Gauge.builder("outbox.failing", this, g -> g.current().outboxFailing())
                .description("Eventos del outbox sin publicar con al menos un intento fallido")
                .register(registry);
        Gauge.builder("outbox.oldest.pending.age", this, DatabaseBacklogGauges::oldestPendingAgeSeconds)
                .description("Antigüedad del evento sin publicar más viejo (0 si no hay)")
                .baseUnit("seconds")
                .register(registry);
    }

    /** Fuerza una lectura de la base (tests, diagnóstico). */
    public void refresh() {
        synchronized (this) {
            load();
        }
    }

    Snapshot current() {
        var now = clock.instant();
        var last = refreshedAt;
        if (last == null || !now.isBefore(last.plus(ttl))) {
            synchronized (this) {
                if (refreshedAt == last) { // otro hilo no refrescó mientras se esperaba el lock
                    load();
                }
            }
        }
        return snapshot;
    }

    private double oldestPendingAgeSeconds() {
        var oldest = current().oldestPending();
        if (oldest == null) {
            return 0;
        }
        return Math.max(0, Duration.between(oldest, clock.instant()).toMillis() / 1000.0);
    }

    /** Debe llamarse con el lock tomado. */
    private void load() {
        try {
            Map<NominationStatus, Long> open = new EnumMap<>(NominationStatus.class);
            jdbc.query(COUNT_OPEN, (RowCallbackHandler) rs ->
                    open.put(NominationStatus.valueOf(rs.getString(1)), rs.getLong(2)));
            var outbox = jdbc.queryForObject(OUTBOX_BACKLOG, (rs, i) -> {
                Timestamp oldest = rs.getTimestamp(3);
                return new Snapshot(open, rs.getLong(1), rs.getLong(2), oldest == null ? null : oldest.toInstant());
            });
            snapshot = outbox;
            if (failing) {
                log.info("Métricas de backlog: la base volvió a responder");
                failing = false;
            }
        } catch (RuntimeException e) {
            // Se conserva la última foto; se avisa una vez por racha de fallas (no en cada scrape).
            if (!failing) {
                log.warn("Métricas de backlog: no se pudo leer la base, se mantienen los últimos valores: {}",
                        e.toString());
                failing = true;
            } else {
                log.debug("Métricas de backlog: la base sigue sin responder: {}", e.toString());
            }
        } finally {
            // Aun ante una falla: con la base caída no se reintenta en cada scrape.
            refreshedAt = clock.instant();
        }
    }

    record Snapshot(Map<NominationStatus, Long> open, long outboxPending, long outboxFailing, Instant oldestPending) {
        static final Snapshot EMPTY = new Snapshot(Map.of(), 0, 0, null);
    }
}
