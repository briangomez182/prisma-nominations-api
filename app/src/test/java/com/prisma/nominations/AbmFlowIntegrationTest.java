package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestClient;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.prisma.nominations.KafkaTopicProbe.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integración con ABM de punta a punta, todo encendido como en producción: POST /v1/nominations → outbox → relay →
 * nomination.requested.v1 → ABM Adapter → HTTP al simulador de ABM → abm.responses.v1 → consumer de respuestas →
 * estado + historial + outbox → relay → nomination.result.v1 → consumidor de ejemplo.
 * <p>
 * Servidor real (RANDOM_PORT): el {@code AbmHttpClient} llama por HTTP al simulador, que vive en la misma app
 * ({@code nominations.abm.base-url} usa {@code ${local.server.port}}). El escenario de ABM se elige por el card_id
 * (ver docs/abm-mock.md). {@link AbmRequestRecorder} cuenta, del lado del simulador, cuántas veces llegó cada
 * nomination_id (sin tocar código de producción).
 * <p>
 * Determinismo: se espera a estados o a offsets confirmados, nunca un sleep a ciegas. "No hay un segundo mensaje"
 * se verifica leyendo el tópico hasta el final ({@link KafkaTopicProbe}) después de que el consumer ya procesó el
 * duplicado, y sosteniendo la condición durante varios ciclos del relay.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nominations.abm-mock.response-delay=300ms",
        "nominations.abm-mock.duplicate-gap=100ms"})
@Import({TestcontainersConfiguration.class, AbmFlowIntegrationTest.AbmRequestRecorder.class})
class AbmFlowIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    /** Varios ciclos del relay (500 ms) y más que response-delay: si algo se duplicara, aparecería aquí. */
    private static final Duration QUIET_PERIOD = Duration.ofMillis(1_500);
    private static final String ACCOUNT_ID = NominationEventFlowIntegrationTest.ACCOUNT_ID;
    private static final String DEMO_CONSUMER = "notifications-demo";
    private static final String ABM_RESPONSE_GROUP = "abm-response-processor";

    @LocalServerPort
    private int port;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private AbmRequestRecorder abmRequests;

    @Nested
    @DisplayName("E4 - ABM aprueba: APPROVED, historial completo y un único nomination.result")
    class E4Approved {

        @Test
        void approvedNominationPublishesOneMaskedResultConsumedByTheDemoConsumer() throws Exception {
            Created created = post("tok_demo_ok_01");

            JsonNode nomination = awaitStatus(created, "APPROVED");
            assertThat(nomination.has("rejection_reason")).isFalse();

            // Lo normal es pasar por PENDING_ABM; si la respuesta de ABM gana la carrera contra la confirmación del
            // envío, RECEIVED → APPROVED directo también es válido (ver SubmitToAbmService).
            assertThat(historyStatuses(created.nominationId())).isIn(
                    List.of("RECEIVED", "PENDING_ABM", "APPROVED"),
                    List.of("RECEIVED", "APPROVED"));

            UUID eventId = resultEventId(created.nominationId());
            awaitProcessedByDemoConsumer(eventId);

            ConsumerRecord<String, String> message = singleResultMessage(created.nominationId());
            assertThat(header(message, KafkaTopics.HEADER_EVENT_ID)).isEqualTo(eventId.toString());
            assertThat(header(message, KafkaTopics.HEADER_EVENT_TYPE)).isEqualTo("nomination.result");
            JsonNode payload = objectMapper.readTree(message.value());
            assertThat(payload.get("status").asText()).isEqualTo("APPROVED");
            assertThat(payload.has("rejection_reason")).isFalse();
            assertThat(payload.get("account_id").asText()).isEqualTo("****7654");
            assertThat(payload.get("card_id").asText()).isEqualTo("****k_01");
            assertThat(message.value()).doesNotContain(ACCOUNT_ID).doesNotContain("tok_demo_ok_01");

            assertThat(abmRequests.count(created.nominationId())).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("E5 - ABM rechaza: REJECTED con motivo normalizado, sin exponer el código de ABM")
    class E5Rejected {

        @Test
        void rejectionExposesNormalizedReasonAndKeepsAbmCodeOnlyInTheDatabase() throws Exception {
            Created created = post("tok_demo_REJECT_030");

            JsonNode nomination = awaitStatus(created, "REJECTED");
            assertThat(nomination.get("rejection_reason").asText()).isEqualTo("CARD_NOT_ELIGIBLE");
            assertThat(nomination.toString()).doesNotContain("ABM-030");
            assertThat(get("/v1/nominations/" + created.nominationId() + "/history", created.entity()).getBody())
                    .doesNotContain("ABM-030");

            // El código original queda en la base, solo para auditoría.
            assertThat(jdbc.queryForMap("SELECT rejection_reason, abm_reason_code FROM nominations WHERE id = ?",
                    created.nominationId()))
                    .containsEntry("rejection_reason", "CARD_NOT_ELIGIBLE")
                    .containsEntry("abm_reason_code", "ABM-030");

            awaitProcessedByDemoConsumer(resultEventId(created.nominationId()));
            ConsumerRecord<String, String> message = singleResultMessage(created.nominationId());
            JsonNode payload = objectMapper.readTree(message.value());
            assertThat(payload.get("status").asText()).isEqualTo("REJECTED");
            assertThat(payload.get("rejection_reason").asText()).isEqualTo("CARD_NOT_ELIGIBLE");
            assertThat(message.value()).doesNotContain("ABM-030");

            assertThat(abmRequests.count(created.nominationId())).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("E7 - ABM responde dos veces: un solo cambio de estado y un solo nomination.result")
    class E7DuplicateResponse {

        @Test
        void secondIdenticalResponseHasNoEffect() throws Exception {
            Created created = post("tok_demo_DUP_01");

            awaitStatus(created, "APPROVED");

            // Las dos respuestas están en abm.responses.v1 y el consumer ya confirmó el offset de la segunda:
            // a partir de acá, cualquier efecto del duplicado ya estaría en la base.
            List<ConsumerRecord<String, String>> responses;
            try (var probe = probe(KafkaTopics.ABM_RESPONSES)) {
                await().pollInSameThread().atMost(TIMEOUT)
                        .until(() -> probe.recordsWithKey(created.nominationId()).size(), equalTo(2));
                responses = probe.recordsWithKey(created.nominationId());
            }
            assertThat(responses.get(0).value()).isEqualTo(responses.get(1).value());
            awaitCommitted(ABM_RESPONSE_GROUP, responses.get(1));

            assertThat(jdbc.queryForObject("""
                    SELECT count(*) FROM nomination_history WHERE nomination_id = ? AND to_status = 'APPROVED'""",
                    Integer.class, created.nominationId())).isEqualTo(1);
            assertThat(jdbc.queryForObject("""
                    SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.result'""",
                    Integer.class, created.nominationId())).isEqualTo(1);

            UUID eventId = resultEventId(created.nominationId());
            awaitProcessedByDemoConsumer(eventId);
            assertThat(header(singleResultMessage(created.nominationId()), KafkaTopics.HEADER_EVENT_ID))
                    .isEqualTo(eventId.toString());

            assertThat(abmRequests.count(created.nominationId())).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Sin doble envío: ABM recibe cada nominación una sola vez, aunque el canal reintente")
    class NoDoubleSubmission {

        @Test
        void channelRetryDoesNotResubmitToAbm() throws Exception {
            String entity = NominationEventFlowIntegrationTest.newEntity();
            UUID requestId = UUID.randomUUID();
            Created created = post(entity, requestId, "tok_demo_ok_02", "it-abm-nodup-" + UUID.randomUUID());

            awaitStatus(created, "APPROVED");
            // Reintento del canal con la misma clave, ya resuelta: replay, sin evento nuevo ni envío a ABM.
            ResponseEntity<String> replay = postRaw(entity, requestId, "tok_demo_ok_02", created.correlationId());
            assertThat(replay.getStatusCode().value()).isEqualTo(202);
            assertThat(replay.getHeaders().getFirst(ApiHeaders.IDEMPOTENT_REPLAYED)).isEqualTo("true");

            awaitProcessedByDemoConsumer(resultEventId(created.nominationId()));
            assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?",
                    Integer.class, created.nominationId())).isEqualTo(2); // requested + result
            assertThat(abmRequests.count(created.nominationId())).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("E6 - ABM no responde (SILENT): queda en PENDING_ABM mientras no venza el SLA")
    class E6SilentAbm {

        /**
         * Dentro del SLA de ABM (15 min por defecto) la nominación espera en PENDING_ABM y no publica resultado. El
         * paso a ABM_TIMEOUT por el sweeper y el reproceso los cubre {@code AbmResilienceIntegrationTest}.
         */
        @Test
        void staysPendingWithoutResult() throws Exception {
            Created created = post("tok_demo_SILENT_01");

            awaitStatus(created, "PENDING_ABM");
            // Más que response-delay: si ABM fuera a responder, ya lo habría hecho.
            await().during(QUIET_PERIOD).atMost(TIMEOUT)
                    .until(() -> status(created), equalTo("PENDING_ABM"));

            try (var probe = probe(KafkaTopics.ABM_RESPONSES)) {
                assertThat(probe.recordsWithKey(created.nominationId())).isEmpty();
            }
            assertThat(jdbc.queryForObject("""
                    SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.result'""",
                    Integer.class, created.nominationId())).isZero();
            assertThat(historyStatuses(created.nominationId())).containsExactly("RECEIVED", "PENDING_ABM");
            assertThat(abmRequests.count(created.nominationId())).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Trazabilidad: el correlation_id del POST llega al historial y a nomination.result.v1")
    class Traceability {

        @Test
        void correlationIdTravelsThroughAbmAndBack() throws Exception {
            Created created = post("tok_demo_REJECT_010");

            awaitStatus(created, "REJECTED");
            List<Map<String, Object>> history = jdbc.queryForList("""
                    SELECT to_status, source, correlation_id FROM nomination_history
                     WHERE nomination_id = ? ORDER BY occurred_at, id""", created.nominationId());
            assertThat(history).hasSizeGreaterThanOrEqualTo(2)
                    .allSatisfy(row -> assertThat(row.get("correlation_id")).isEqualTo(created.correlationId()));
            assertThat(history.getLast()).containsEntry("to_status", "REJECTED").containsEntry("source", "ABM_RESPONSE");

            awaitProcessedByDemoConsumer(resultEventId(created.nominationId()));
            ConsumerRecord<String, String> message = singleResultMessage(created.nominationId());
            assertThat(header(message, KafkaTopics.HEADER_CORRELATION_ID)).isEqualTo(created.correlationId());
            JsonNode payload = objectMapper.readTree(message.value());
            assertThat(payload.get("correlation_id").asText()).isEqualTo(created.correlationId());
            assertThat(payload.get("rejection_reason").asText()).isEqualTo("INVALID_ACCOUNT");
        }
    }

    // ---------------------------------------------------------------- soporte

    record Created(UUID nominationId, String entity, String correlationId) {
    }

    private Created post(String cardId) throws Exception {
        return post(NominationEventFlowIntegrationTest.newEntity(), UUID.randomUUID(), cardId,
                "it-abm-flow-" + UUID.randomUUID());
    }

    private Created post(String entity, UUID requestId, String cardId, String correlationId) throws Exception {
        ResponseEntity<String> response = postRaw(entity, requestId, cardId, correlationId);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getHeaders().getFirst(ApiHeaders.CORRELATION_ID)).isEqualTo(correlationId);
        UUID nominationId = UUID.fromString(objectMapper.readTree(response.getBody()).get("nomination_id").asText());
        return new Created(nominationId, entity, correlationId);
    }

    private ResponseEntity<String> postRaw(String entity, UUID requestId, String cardId, String correlationId) {
        String body = """
                {
                  "request_id": "%s",
                  "customer_id": "CUST-000123",
                  "account_id": "%s",
                  "card_id": "%s",
                  "alias": "CUENTA SUELDO"
                }
                """.formatted(requestId, ACCOUNT_ID, cardId);
        return http().post().uri("/v1/nominations")
                .header(ApiHeaders.ENTITY_ID, entity)
                .header(ApiHeaders.CORRELATION_ID, correlationId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .toEntity(String.class);
    }

    private ResponseEntity<String> get(String path, String entity) {
        return http().get().uri(path).header(ApiHeaders.ENTITY_ID, entity).retrieve().toEntity(String.class);
    }

    private RestClient http() {
        return RestClient.create("http://localhost:" + port);
    }

    private String status(Created created) throws Exception {
        return nomination(created).get("status").asText();
    }

    private JsonNode nomination(Created created) throws Exception {
        return objectMapper.readTree(get("/v1/nominations/" + created.nominationId(), created.entity()).getBody());
    }

    /** Espera el estado por la API pública (GET) y devuelve la última representación leída. */
    private JsonNode awaitStatus(Created created, String expected) throws Exception {
        await().atMost(TIMEOUT).until(() -> status(created), equalTo(expected));
        return nomination(created);
    }

    private List<String> historyStatuses(UUID nominationId) {
        return jdbc.queryForList("""
                SELECT to_status FROM nomination_history WHERE nomination_id = ? ORDER BY occurred_at, id""",
                String.class, nominationId);
    }

    /** event_id del nomination.result, una vez publicado por el relay (hay a lo sumo uno por nominación). */
    private UUID resultEventId(UUID nominationId) {
        String sql = """
                SELECT id FROM outbox_events
                 WHERE aggregate_id = ? AND event_type = 'nomination.result' AND published_at IS NOT NULL""";
        await().atMost(TIMEOUT).until(() -> !jdbc.queryForList(sql, UUID.class, nominationId).isEmpty());
        return jdbc.queryForObject(sql, UUID.class, nominationId);
    }

    private void awaitProcessedByDemoConsumer(UUID eventId) {
        await().atMost(TIMEOUT).until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM consumer_processed_events WHERE consumer = ? AND event_id = ?",
                Integer.class, DEMO_CONSUMER, eventId), equalTo(1));
    }

    /** Exactamente un mensaje en nomination.result.v1, sostenido durante varios ciclos del relay. */
    private ConsumerRecord<String, String> singleResultMessage(UUID nominationId) {
        try (var probe = probe(KafkaTopics.NOMINATION_RESULT)) {
            await().pollInSameThread().during(QUIET_PERIOD).atMost(TIMEOUT)
                    .until(() -> probe.recordsWithKey(nominationId).size(), equalTo(1));
            return probe.recordsWithKey(nominationId).getFirst();
        }
    }

    /** Espera a que el consumer group haya confirmado el offset del mensaje (ya lo procesó). */
    private void awaitCommitted(String groupId, ConsumerRecord<String, String> record) {
        var partition = new TopicPartition(record.topic(), record.partition());
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            await().atMost(TIMEOUT).until(() -> {
                Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                OffsetAndMetadata committed = offsets.get(partition);
                return committed != null && committed.offset() > record.offset();
            });
        }
    }

    private KafkaTopicProbe probe(String topic) {
        return new KafkaTopicProbe(kafka.getBootstrapServers(), topic);
    }

    /**
     * Registro de lo que recibió el simulador de ABM: un filtro de servlet sobre {@code /abm-mock/*} que cuenta los
     * POST por nomination_id. Mide en el borde HTTP del "sistema ABM", sin exponer nada en producción.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class AbmRequestRecorder {

        private final Map<UUID, AtomicInteger> received = new ConcurrentHashMap<>();
        private final ObjectMapper reader = new ObjectMapper();

        int count(UUID nominationId) {
            AtomicInteger count = received.get(nominationId);
            return count == null ? 0 : count.get();
        }

        @Bean
        FilterRegistrationBean<OncePerRequestFilter> abmRequestRecorderFilter() {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>(new OncePerRequestFilter() {
                @Override
                protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                                FilterChain chain) throws ServletException, IOException {
                    var cached = new ContentCachingRequestWrapper(request);
                    try {
                        chain.doFilter(cached, response);
                    } finally {
                        record(cached.getContentAsByteArray());
                    }
                }
            });
            registration.addUrlPatterns("/abm-mock/*");
            return registration;
        }

        private void record(byte[] body) {
            try {
                JsonNode id = reader.readTree(body).get("nomination_id");
                if (id != null) {
                    received.computeIfAbsent(UUID.fromString(id.asText()), k -> new AtomicInteger()).incrementAndGet();
                }
            } catch (IOException | IllegalArgumentException e) {
                // Cuerpo inválido: no es un alta que cuente.
            }
        }
    }
}
