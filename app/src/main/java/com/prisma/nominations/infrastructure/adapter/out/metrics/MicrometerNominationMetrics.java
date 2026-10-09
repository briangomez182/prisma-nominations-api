package com.prisma.nominations.infrastructure.adapter.out.metrics;

import com.prisma.nominations.application.port.out.NominationMetrics;
import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.RejectionReason;
import com.prisma.nominations.domain.ResolutionOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Métricas de negocio con Micrometer (exportadas a Prometheus en /actuator/prometheus).
 * <p>
 * Los nombres son contrato con tableros y alertas (ops/): no cambiarlos sin actualizarlos. Las series con tags de
 * valores enumerables se registran en 0 al arrancar, para que {@code rate()} y las alertas existan antes del
 * primer evento.
 * <p>
 * <b>Cardinalidad:</b> {@code entity_id} es la única dimensión abierta y está acotada (decenas de entidades
 * adheridas; el dominio además limita su formato). Nunca se etiqueta con nomination_id, request_id ni
 * correlation_id: identifican una operación y van a logs y trazas.
 */
@Component
public class MicrometerNominationMetrics implements NominationMetrics {

    /**
     * No es "nominations.created": el cliente Prometheus 1.x reserva el sufijo {@code _created} (timestamp de
     * creación de un counter) y lo recorta, con lo que la serie saldría como {@code nominations_total}.
     */
    static final String CREATED = "nominations.received";
    static final String REPLAYED = "nominations.replayed";
    static final String IDEMPOTENCY_CONFLICTS = "nominations.idempotency.conflicts";
    static final String ABM_SUBMISSIONS = "abm.submissions";
    static final String ABM_RESPONSES = "abm.responses";
    static final String RESOLVED = "nominations.resolved";
    static final String RESOLUTION_TIME = "nominations.resolution.time";
    static final String ABM_TIMEOUTS = "nominations.abm.timeouts";
    static final String REPROCESSED = "nominations.reprocessed";

    static final String TAG_ENTITY = "entity_id";
    static final String TAG_OUTCOME = "outcome";
    static final String TAG_STATUS = "status";
    static final String TAG_REASON = "reason";
    static final String TAG_SOURCE = "source";
    static final String NO_REASON = "none";

    private final MeterRegistry registry;

    public MicrometerNominationMetrics(MeterRegistry registry) {
        this.registry = registry;
        preRegister();
    }

    @Override
    public void created(String entityId) {
        perEntity(CREATED, "Nominaciones nuevas aceptadas", entityId).increment();
    }

    @Override
    public void replayed(String entityId) {
        perEntity(REPLAYED, "Reintentos idempotentes del canal (mismo request_id y contenido)", entityId).increment();
    }

    @Override
    public void idempotencyConflict(String entityId) {
        perEntity(IDEMPOTENCY_CONFLICTS, "Mismo request_id con contenido distinto (409)", entityId).increment();
    }

    @Override
    public void submission(SubmissionOutcome outcome) {
        submissionCounter(outcome).increment();
    }

    @Override
    public void abmResponse(ResolutionOutcome outcome) {
        responseCounter(outcome).increment();
    }

    @Override
    public void resolved(Nomination nomination) {
        var status = nomination.status();
        if (!status.isFinal()) {
            return; // defensivo: solo APPROVED/REJECTED cuentan como resolución
        }
        resolvedCounter(status, nomination.rejectionReason()).increment();
        var elapsed = Duration.between(nomination.createdAt(), nomination.updatedAt());
        resolutionTimer(status).record(elapsed.isNegative() ? Duration.ZERO : elapsed);
    }

    @Override
    public void abmTimeout(ChangeSource source) {
        timeoutCounter(source).increment();
    }

    @Override
    public void reprocessed() {
        reprocessedCounter().increment();
    }

    private void preRegister() {
        for (var outcome : SubmissionOutcome.values()) {
            submissionCounter(outcome);
        }
        for (var outcome : ResolutionOutcome.values()) {
            responseCounter(outcome);
        }
        resolvedCounter(NominationStatus.APPROVED, null);
        for (var reason : RejectionReason.values()) {
            resolvedCounter(NominationStatus.REJECTED, reason);
        }
        resolutionTimer(NominationStatus.APPROVED);
        resolutionTimer(NominationStatus.REJECTED);
        timeoutCounter(ChangeSource.ABM_ADAPTER);
        timeoutCounter(ChangeSource.SWEEPER);
        reprocessedCounter();
    }

    private Counter perEntity(String name, String description, String entityId) {
        return Counter.builder(name).description(description).tag(TAG_ENTITY, entityId).register(registry);
    }

    private Counter submissionCounter(SubmissionOutcome outcome) {
        return Counter.builder(ABM_SUBMISSIONS)
                .description("Intentos de envío a ABM por resultado")
                .tag(TAG_OUTCOME, outcome.name())
                .register(registry);
    }

    private Counter responseCounter(ResolutionOutcome outcome) {
        return Counter.builder(ABM_RESPONSES)
                .description("Respuestas de ABM procesadas por resultado (CONFLICT: ABM contradice un resultado)")
                .tag(TAG_OUTCOME, outcome.name())
                .register(registry);
    }

    private Counter resolvedCounter(NominationStatus status, RejectionReason reason) {
        return Counter.builder(RESOLVED)
                .description("Nominaciones que llegaron a estado final")
                .tag(TAG_STATUS, status.name())
                .tag(TAG_REASON, reason == null ? NO_REASON : reason.name())
                .register(registry);
    }

    /**
     * Histograma con buckets hasta 1 h: en producción ABM responde en minutos y los buckets por defecto de un
     * timer llegan a 30 s. El histograma en Prometheus lo habilita application.yml (percentiles-histogram).
     */
    private Timer resolutionTimer(NominationStatus status) {
        return Timer.builder(RESOLUTION_TIME)
                .description("Tiempo desde el alta (created_at) hasta el estado final")
                .tag(TAG_STATUS, status.name())
                .minimumExpectedValue(Duration.ofMillis(100))
                .maximumExpectedValue(Duration.ofHours(1))
                .register(registry);
    }

    private Counter timeoutCounter(ChangeSource source) {
        return Counter.builder(ABM_TIMEOUTS)
                .description("Pasajes a ABM_TIMEOUT (ABM_ADAPTER: reintentos agotados; SWEEPER: sin respuesta en SLA)")
                .tag(TAG_SOURCE, source.name())
                .register(registry);
    }

    private Counter reprocessedCounter() {
        return Counter.builder(REPROCESSED)
                .description("Reprocesos manuales desde ABM_TIMEOUT")
                .register(registry);
    }
}
