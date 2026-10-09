package com.prisma.nominations.abmmock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.abmmock.AbmMockMessages.Response;
import com.prisma.nominations.abmmock.AbmMockMessages.SubmitRequest;
import io.micrometer.context.ContextSnapshotFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Simulador de ABM, el sistema externo que da de alta la nominación. Procesa en forma asincrónica: acepta el
 * pedido por HTTP (202 con {@code abm_operation_id}) y, pasado {@code response-delay}, publica el resultado en
 * Kafka {@code abm.responses.v1} (key = nomination_id, header {@code correlation_id}).
 * <p>
 * <b>Idempotente</b>: el mismo {@code nomination_id} devuelve el mismo {@code abm_operation_id} y no programa
 * una segunda respuesta (el adapter puede reintentar un envío que ABM sí había recibido).
 * <p>
 * <b>Escenarios</b>: se eligen por el {@code card_id} (sin distinguir mayúsculas, "contiene"):
 * <table>
 *   <caption>Escenarios del simulador</caption>
 *   <tr><th>card_id contiene</th><th>HTTP</th><th>Respuesta por Kafka</th><th>Caso</th></tr>
 *   <tr><td>FAIL</td><td>503 siempre</td><td>ninguna</td><td>E6: reintentos / circuit breaker</td></tr>
 *   <tr><td>SLOW</td><td>202 luego de {@code slow-http-delay}</td><td>APPROVED</td><td>E6: timeout de lectura</td></tr>
 *   <tr><td>SILENT</td><td>202</td><td>ninguna</td><td>E6: sin respuesta (sweeper)</td></tr>
 *   <tr><td>DUP</td><td>202</td><td>APPROVED dos veces, separadas {@code duplicate-gap}</td><td>E7</td></tr>
 *   <tr><td>REJECT_010 / _020 / _030 / _060</td><td>202</td><td>REJECTED ABM-010 / 020 / 030 / 060</td><td>E5</td></tr>
 *   <tr><td>REJECT (otro sufijo o ninguno)</td><td>202</td><td>REJECTED ABM-051 (Cuenta bloqueada)</td><td>E5</td></tr>
 *   <tr><td>cualquier otro</td><td>202</td><td>APPROVED</td><td>E4</td></tr>
 * </table>
 * Faltan nomination_id, request_id, account_id o card_id → 400 (contrato inválido, no se reintenta).
 * Precedencia si hay varias marcas: FAIL, SLOW, SILENT, DUP, REJECT.
 * <p>
 * El registro es en memoria (demo): se pierde al reiniciar, igual que las respuestas programadas pendientes.
 */
public class AbmMockEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AbmMockEngine.class);
    private static final String MDC_CORRELATION_ID = "correlationId";
    private static final ContextSnapshotFactory CONTEXT_SNAPSHOTS = ContextSnapshotFactory.builder().build();

    private final AbmMockProperties properties;
    private final AbmResponsePublisher publisher;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final ScheduledThreadPoolExecutor scheduler;
    /** nomination_id → operación aceptada (idempotencia). */
    private final Map<String, Operation> operations = new ConcurrentHashMap<>();

    AbmMockEngine(AbmMockProperties properties, AbmResponsePublisher publisher, ObjectMapper objectMapper, Clock clock) {
        this.properties = properties;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.scheduler = new ScheduledThreadPoolExecutor(2, namedThreads());
        // Al apagar se descartan las respuestas pendientes: como un ABM que se reinicia (lo cubre el sweeper).
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setRemoveOnCancelPolicy(true);
    }

    /** Resultado del alta, que el controller traduce a HTTP. */
    sealed interface Outcome {
        record Accepted(String abmOperationId) implements Outcome {
        }

        record Invalid(List<String> missingFields) implements Outcome {
        }

        record Unavailable() implements Outcome {
        }
    }

    private record Operation(String abmOperationId, AbmMockScenario scenario) {
    }

    /** Recibe un pedido de alta. En el escenario SLOW bloquea el hilo del request (virtual) a propósito. */
    Outcome submit(SubmitRequest request) {
        List<String> missing = missingFields(request);
        if (!missing.isEmpty()) {
            log.warn("ABM mock: pedido inválido, faltan {}", missing);
            return new Outcome.Invalid(missing);
        }
        AbmMockScenario scenario = AbmMockScenario.of(request.cardId());
        log.info("ABM mock: recibido {} cardId={} → escenario {}", request, request.cardId(), scenario);

        if (scenario == AbmMockScenario.FAIL) {
            log.info("ABM mock: nominación {} → 503 (ABM no disponible)", request.nominationId());
            return new Outcome.Unavailable();
        }
        if (scenario == AbmMockScenario.SLOW) {
            sleep(properties.slowHttpDelay());
        }

        boolean[] created = {false};
        Operation operation = operations.computeIfAbsent(request.nominationId(), id -> {
            created[0] = true;
            return new Operation("ABM-OP-" + UUID.randomUUID(), scenario);
        });
        if (created[0]) {
            log.info("ABM mock: nominación {} aceptada con operación {}", request.nominationId(), operation.abmOperationId());
            scheduleResponse(request, operation);
        } else {
            log.info("ABM mock: nominación {} ya recibida (operación {}): sin nueva respuesta",
                    request.nominationId(), operation.abmOperationId());
        }
        return new Outcome.Accepted(operation.abmOperationId());
    }

    private void scheduleResponse(SubmitRequest request, Operation operation) {
        switch (operation.scenario()) {
            case SILENT -> log.info("ABM mock: operación {} nunca va a responder (SILENT)", operation.abmOperationId());
            default -> scheduler.schedule(withCurrentContext(() -> respond(request, operation)),
                    properties.responseDelay().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    /** Arma la respuesta una sola vez y la publica; en DUP programa el reenvío idéntico (mismo responded_at). */
    private void respond(SubmitRequest request, Operation operation) {
        Response response = buildResponse(request, operation);
        String json;
        try {
            json = objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            log.warn("ABM mock: no se pudo serializar la respuesta de la operación {}", operation.abmOperationId(), e);
            return;
        }
        publish(request, operation, response, json);
        if (operation.scenario() == AbmMockScenario.DUPLICATE) {
            scheduler.schedule(withCurrentContext(() -> publish(request, operation, response, json)),
                    properties.duplicateGap().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void publish(SubmitRequest request, Operation operation, Response response, String json) {
        if (request.correlationId() != null) {
            MDC.put(MDC_CORRELATION_ID, request.correlationId());
        }
        try {
            publisher.publish(request.nominationId(), request.correlationId(), json);
            log.info("ABM mock: respuesta {}{} enviada para nominación {} (operación {})", response.result(),
                    response.reasonCode() == null ? "" : " " + response.reasonCode(),
                    request.nominationId(), operation.abmOperationId());
        } catch (RuntimeException e) {
            // ABM no reintenta: una respuesta perdida la resuelve el sweeper del lado de nominaciones.
            log.warn("ABM mock: no se pudo enviar la respuesta de la operación {}: {}",
                    operation.abmOperationId(), e.toString());
        } finally {
            MDC.remove(MDC_CORRELATION_ID);
        }
    }

    private Response buildResponse(SubmitRequest request, Operation operation) {
        String respondedAt = clock.instant().toString();
        if (operation.scenario() == AbmMockScenario.REJECT) {
            AbmMockScenario.Rejection rejection = AbmMockScenario.Rejection.of(request.cardId());
            return new Response(operation.abmOperationId(), request.nominationId(), request.requestId(),
                    request.correlationId(), "REJECTED", rejection.code(), rejection.description(), respondedAt);
        }
        return new Response(operation.abmOperationId(), request.nominationId(), request.requestId(),
                request.correlationId(), "APPROVED", null, "Nominación aprobada", respondedAt);
    }

    private static List<String> missingFields(SubmitRequest request) {
        List<String> missing = new ArrayList<>();
        if (request == null) {
            return List.of("nomination_id", "request_id", "account_id", "card_id");
        }
        addIfBlank(missing, "nomination_id", request.nominationId());
        addIfBlank(missing, "request_id", request.requestId());
        addIfBlank(missing, "account_id", request.accountId());
        addIfBlank(missing, "card_id", request.cardId());
        return missing;
    }

    private static void addIfBlank(List<String> missing, String field, String value) {
        if (value == null || value.isBlank()) {
            missing.add(field);
        }
    }

    private static void sleep(Duration delay) {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * La respuesta sale de otro hilo, segundos después: se lleva el contexto actual (observation/traza del pedido
     * HTTP) para que la publicación en Kafka continúe la misma traza. Sin tracing es no-op.
     */
    private static Runnable withCurrentContext(Runnable task) {
        return CONTEXT_SNAPSHOTS.captureAll().wrap(task);
    }

    private static ThreadFactory namedThreads() {
        return Thread.ofPlatform().name("abm-mock-", 1).daemon(true).factory();
    }

    /** Apagado ordenado: no ejecuta respuestas pendientes y espera las que se están publicando. */
    @Override
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
