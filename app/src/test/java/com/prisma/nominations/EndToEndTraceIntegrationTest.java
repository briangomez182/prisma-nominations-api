package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.adapter.out.messaging.OutboxRelay;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Una sola traza de punta a punta, también a través de ABM (simulador real por HTTP + respuesta por Kafka):
 * canal → outbox(nomination.requested) → relay → ABM Adapter → POST ABM → abm.responses.v1 → consumer de respuestas
 * → outbox(nomination.result). Se verifica que el traceparent guardado con el nomination.result tiene el mismo trace
 * id que el span del canal. La nominación se crea con el caso de uso (sin HTTP a la API); el relay se invoca a mano.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nominations.abm.sweeper.enabled=false",
        "nominations.outbox.relay.enabled=false",
        "nominations.abm-mock.response-delay=200ms",
        "management.otlp.tracing.export.enabled=false"})
@AutoConfigureObservability
@Import(TestcontainersConfiguration.class)
class EndToEndTraceIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private CreateNominationUseCase createNomination;

    @Autowired
    private OutboxRelay relay;

    @Autowired
    private Tracer tracer;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void elNominationResultContinuaLaTrazaDelCanal() {
        String correlationId = "it-e2e-trace-" + UUID.randomUUID();
        Span canal = tracer.nextSpan().name("canal").start();
        UUID nominationId;
        try (Tracer.SpanInScope ignored = tracer.withSpan(canal)) {
            MDC.put(ApiHeaders.CORRELATION_ID_MDC_KEY, correlationId);
            nominationId = createNomination.create(new CreateNominationCommand("ENT-E2E", UUID.randomUUID(), "123456",
                    NominationEventFlowIntegrationTest.ACCOUNT_ID, "tok_e2e_trace", "CUENTA SUELDO", correlationId))
                    .nomination().id();
        } finally {
            MDC.remove(ApiHeaders.CORRELATION_ID_MDC_KEY);
            canal.end();
        }
        String traceId = canal.context().traceId();

        // Publica nomination.requested; el resto (adapter, mock de ABM, consumer de respuestas) es asincrónico.
        await().atMost(TIMEOUT).until(() -> relay.relayOnce() >= 0 && resultHeaders(nominationId).size() == 1);

        var headers = readJson(resultHeaders(nominationId).getFirst());
        assertThat(headers.path(KafkaTopics.HEADER_CORRELATION_ID).asText()).isEqualTo(correlationId);
        assertThat(headers.path(KafkaTopics.HEADER_TRACEPARENT).asText())
                .matches("00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}")
                .startsWith("00-" + traceId + "-");
    }

    private List<String> resultHeaders(UUID nominationId) {
        return jdbc.queryForList("SELECT headers::text FROM outbox_events WHERE aggregate_id = ? "
                + "AND event_type = 'nomination.result'", String.class, nominationId);
    }

    private JsonNode readJson(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
