package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.event.NominationResult;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.application.port.out.OutboxPort;
import com.prisma.nominations.domain.AbmDecision;
import com.prisma.nominations.domain.ResolutionOutcome;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.prisma.nominations.KafkaTopicProbe.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Flujo de eventos de punta a punta: POST → nominación + outbox en una TX → relay → Kafka → consumidor.
 * Relay programado ENCENDIDO, como en producción. Misma configuración que {@link NominationApiIntegrationTest}:
 * comparten el contexto (y los contenedores) del cache de Spring. ABM Adapter apagado (sin servidor HTTP no hay
 * a quién enviar); el circuito con ABM está en {@link AbmFlowIntegrationTest}.
 * <p>
 * Cada test usa entidad, request_id y correlation_id propios y lee los tópicos filtrando por key
 * (nomination_id), así no lo afectan los mensajes de otros tests. E8 (relay apagado) necesita otro contexto:
 * está en {@link NominationEventFlowRelayDownIntegrationTest}.
 */
@SpringBootTest(properties = "nominations.abm.adapter.enabled=false")
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@ExtendWith(OutputCaptureExtension.class)
@Tag("integration")
class NominationEventFlowIntegrationTest {

    static final String BASE = "/v1/nominations";
    static final String ACCOUNT_ID = "0001234567890987654";
    static final String CARD_TOKEN = "tok_4f9a2c7b8d1e";
    static final Duration TIMEOUT = Duration.ofSeconds(30);
    /** Varios ciclos del relay (500 ms): si hubiera una republicación, aparecería en esta ventana. */
    private static final Duration QUIET_PERIOD = Duration.ofMillis(1_500);
    private static final String DEMO_CONSUMER = "notifications-demo";

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private NominationRepository repository;
    @Autowired
    private OutboxPort outbox;
    @Autowired
    private TransactionTemplate tx;
    @Autowired
    private Clock clock;

    @Nested
    @DisplayName("E1 - flujo completo de ingreso: POST → outbox → nomination.requested.v1")
    @Tag("E1")
    class E1RequestedEventPublished {

        @Test
        void postPublishesOneRequestedEventWithKeyHeadersAndFullPayload() throws Exception {
            String entity = newEntity();
            UUID requestId = UUID.randomUUID();
            String correlationId = "it-flow-e1-" + UUID.randomUUID();

            UUID nominationId = nominationId(mockMvc.perform(create(entity, body(requestId))
                            .header(ApiHeaders.CORRELATION_ID, correlationId))
                    .andExpect(status().isAccepted())
                    .andReturn());

            Map<String, Object> row = awaitRequestedPublished(nominationId);
            assertThat(row.get("published_at")).isNotNull();
            assertThat(row.get("attempts")).isEqualTo(1);
            assertThat(row.get("topic")).isEqualTo(KafkaTopics.NOMINATION_REQUESTED);

            try (var probe = probe(KafkaTopics.NOMINATION_REQUESTED)) {
                List<ConsumerRecord<String, String>> records = probe.recordsWithKey(nominationId);
                assertThat(records).hasSize(1);
                ConsumerRecord<String, String> message = records.getFirst();

                assertThat(message.key()).isEqualTo(nominationId.toString());
                assertThat(header(message, KafkaTopics.HEADER_EVENT_ID)).isEqualTo(row.get("id").toString());
                assertThat(header(message, KafkaTopics.HEADER_EVENT_TYPE)).isEqualTo("nomination.requested");
                assertThat(header(message, KafkaTopics.HEADER_SCHEMA_VERSION)).isEqualTo("1");
                assertThat(header(message, KafkaTopics.HEADER_CORRELATION_ID)).isEqualTo(correlationId);

                JsonNode payload = objectMapper.readTree(message.value());
                assertThat(payload.get("event_id").asText()).isEqualTo(row.get("id").toString());
                assertThat(payload.get("event_type").asText()).isEqualTo("nomination.requested");
                assertThat(payload.get("schema_version").asInt()).isEqualTo(1);
                assertThat(payload.get("nomination_id").asText()).isEqualTo(nominationId.toString());
                assertThat(payload.get("request_id").asText()).isEqualTo(requestId.toString());
                assertThat(payload.get("entity_id").asText()).isEqualTo(entity);
                assertThat(payload.get("customer_id").asText()).isEqualTo("CUST-000123");
                // Tópico interno hacia ABM: cuenta completa; la tarjeta siempre como token.
                assertThat(payload.get("account_id").asText()).isEqualTo(ACCOUNT_ID);
                assertThat(payload.get("card_id").asText()).isEqualTo(CARD_TOKEN);
                assertThat(payload.get("alias").asText()).isEqualTo("CUENTA SUELDO");
                assertThat(payload.get("correlation_id").asText()).isEqualTo(correlationId);
                assertThat(payload.get("occurred_at").asText()).isNotBlank();
            }
        }
    }

    @Nested
    @DisplayName("E3 - reintentos del canal: exactamente un mensaje por nominación")
    @Tag("E3")
    class E3OneMessagePerNomination {

        @Test
        void sequentialAndConcurrentRetriesPublishASingleEventEach() throws Exception {
            String entity = newEntity();
            String sequentialBody = body(UUID.randomUUID());

            UUID sequentialId = nominationId(mockMvc.perform(create(entity, sequentialBody))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "false"))
                    .andReturn());
            mockMvc.perform(create(entity, sequentialBody))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "true"));

            List<MvcResult> concurrent = postConcurrently(entity, body(UUID.randomUUID()), 5);
            assertThat(concurrent).allSatisfy(r -> assertThat(r.getResponse().getStatus()).isEqualTo(202));
            List<UUID> concurrentIds = new ArrayList<>();
            for (MvcResult result : concurrent) {
                concurrentIds.add(nominationId(result));
            }
            assertThat(concurrentIds).containsOnly(concurrentIds.getFirst());
            assertThat(concurrent).filteredOn(r -> "false".equals(r.getResponse().getHeader(ApiHeaders.IDEMPOTENT_REPLAYED)))
                    .hasSize(1);
            UUID concurrentId = concurrentIds.getFirst();

            // Las TX perdedoras hicieron rollback: una sola fila de outbox por nominación.
            assertThat(outboxRows(sequentialId)).isEqualTo(1);
            assertThat(outboxRows(concurrentId)).isEqualTo(1);
            awaitRequestedPublished(sequentialId);
            awaitRequestedPublished(concurrentId);

            // Ya publicadas, el tópico debe seguir con un solo mensaje por nominación durante varios ciclos.
            try (var probe = probe(KafkaTopics.NOMINATION_REQUESTED)) {
                await().pollInSameThread().during(QUIET_PERIOD).atMost(TIMEOUT)
                        .until(() -> probe.recordsWithKey(sequentialId).size(), equalTo(1));
                assertThat(probe.recordsWithKey(concurrentId)).hasSize(1);
            }
        }
    }

    @Nested
    @DisplayName("E9 + resultado único: nomination.result.v1 → consumidor con dedup")
    @Tag("E9")
    class E9ResultConsumedOnce {

        @Test
        void resultIsPublishedOnceAndConsumedOnceEvenIfRedelivered(CapturedOutput output) throws Exception {
            UUID nominationId = nominationId(mockMvc.perform(create(newEntity(), body(UUID.randomUUID())))
                    .andExpect(status().isAccepted())
                    .andReturn());
            awaitRequestedPublished(nominationId);

            // Simula la fase 5 (respuesta de ABM): estado final + resultado en el outbox, en una sola TX.
            NominationResult result = tx.execute(status -> {
                var nomination = repository.findById(nominationId).orElseThrow();
                assertThat(nomination.resolve(AbmDecision.approved(), clock.instant()))
                        .isEqualTo(ResolutionOutcome.APPLIED);
                var saved = repository.save(nomination);
                var event = NominationResult.of(saved, clock.instant());
                outbox.append(event);
                return event;
            });
            assertThat(result).isNotNull();
            UUID eventId = result.eventId();

            await().atMost(TIMEOUT).until(() -> publishedAt(eventId) != null);
            awaitProcessed(eventId);

            ConsumerRecord<String, String> published;
            try (var probe = probe(KafkaTopics.NOMINATION_RESULT)) {
                List<ConsumerRecord<String, String>> records = probe.recordsWithKey(nominationId);
                assertThat(records).hasSize(1);
                published = records.getFirst();
            }
            assertThat(header(published, KafkaTopics.HEADER_EVENT_ID)).isEqualTo(eventId.toString());
            assertThat(header(published, KafkaTopics.HEADER_EVENT_TYPE)).isEqualTo("nomination.result");
            JsonNode payload = objectMapper.readTree(published.value());
            assertThat(payload.get("status").asText()).isEqualTo("APPROVED");
            assertThat(payload.has("rejection_reason")).isFalse();
            // Tópico público: sin datos sensibles.
            assertThat(payload.get("account_id").asText()).isEqualTo("****7654");
            assertThat(payload.get("card_id").asText()).isEqualTo("****8d1e");
            assertThat(published.value()).doesNotContain(ACCOUNT_ID);

            // Un segundo resultado para la misma nominación lo rechaza la base (índice único parcial).
            assertThatThrownBy(() -> tx.executeWithoutResult(status -> outbox.append(
                    NominationResult.of(repository.findById(nominationId).orElseThrow(), clock.instant()))))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(nominationId.toString());
            assertThat(jdbc.queryForObject("""
                    SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.result'""",
                    Integer.class, nominationId)).isEqualTo(1);

            // Reentrega (at-least-once): el mismo mensaje crudo otra vez, con sus headers. Detrás, un marcador en
            // la misma partición: cuando el marcador está procesado, el duplicado ya pasó por el consumidor.
            UUID marker = UUID.randomUUID();
            var producerFactory = new DefaultKafkaProducerFactory<String, String>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
            try {
                var template = new KafkaTemplate<>(producerFactory);
                template.send(new ProducerRecord<>(published.topic(), published.partition(), published.key(),
                        published.value(), published.headers())).get(10, TimeUnit.SECONDS);
                template.send(markerRecord(published.partition(), marker)).get(10, TimeUnit.SECONDS);
            } finally {
                producerFactory.destroy();
            }
            awaitProcessed(marker);

            assertThat(processedRows(eventId)).isEqualTo(1);
            assertThat(output.getOut().lines()
                    .filter(l -> l.contains("Notificación enviada") && l.contains("event_id=" + eventId))
                    .count()).isEqualTo(1);
        }

        private ProducerRecord<String, String> markerRecord(int partition, UUID eventId) {
            UUID nominationId = UUID.randomUUID();
            String json = """
                    {"event_id": "%s", "event_type": "nomination.result", "schema_version": 1,
                     "nomination_id": "%s", "status": "APPROVED"}""".formatted(eventId, nominationId);
            var record = new ProducerRecord<>(KafkaTopics.NOMINATION_RESULT, partition, nominationId.toString(), json);
            record.headers().add(KafkaTopics.HEADER_EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8));
            return record;
        }
    }

    @Nested
    @DisplayName("Trazabilidad: el mismo correlation_id de punta a punta")
    @Tag("E1")
    class Traceability {

        @Test
        void correlationIdTravelsFromHttpToHistoryToKafka() throws Exception {
            String correlationId = "it-flow-trace-" + UUID.randomUUID();

            MvcResult created = mockMvc.perform(create(newEntity(), body(UUID.randomUUID()))
                            .header(ApiHeaders.CORRELATION_ID, correlationId))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string(ApiHeaders.CORRELATION_ID, correlationId))
                    .andReturn();
            UUID nominationId = nominationId(created);
            assertThat(json(created).get("correlation_id").asText()).isEqualTo(correlationId);

            assertThat(jdbc.queryForList("SELECT correlation_id FROM nomination_history WHERE nomination_id = ?",
                    String.class, nominationId)).containsExactly(correlationId);

            awaitRequestedPublished(nominationId);
            try (var probe = probe(KafkaTopics.NOMINATION_REQUESTED)) {
                ConsumerRecord<String, String> message = probe.recordsWithKey(nominationId).getFirst();
                assertThat(header(message, KafkaTopics.HEADER_CORRELATION_ID)).isEqualTo(correlationId);
                assertThat(objectMapper.readTree(message.value()).get("correlation_id").asText())
                        .isEqualTo(correlationId);
            }
        }
    }

    // ---------------------------------------------------------------- soporte

    private List<MvcResult> postConcurrently(String entity, String body, int requests) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(requests)) {
            List<Future<MvcResult>> futures = new ArrayList<>();
            for (int i = 0; i < requests; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return mockMvc.perform(create(entity, body)).andReturn();
                }));
            }
            start.countDown();
            List<MvcResult> results = new ArrayList<>();
            for (Future<MvcResult> future : futures) {
                results.add(future.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
            }
            return results;
        }
    }

    private KafkaTopicProbe probe(String topic) {
        return new KafkaTopicProbe(kafka.getBootstrapServers(), topic);
    }

    private Map<String, Object> awaitRequestedPublished(UUID nominationId) {
        await().atMost(TIMEOUT).until(() -> requestedRow(jdbc, nominationId).get("published_at") != null);
        return requestedRow(jdbc, nominationId);
    }

    private Object publishedAt(UUID eventId) {
        return jdbc.queryForObject("SELECT published_at FROM outbox_events WHERE id = ?", Object.class, eventId);
    }

    private int outboxRows(UUID nominationId) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?",
                Integer.class, nominationId);
    }

    private void awaitProcessed(UUID eventId) {
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(processedRows(eventId)).isEqualTo(1));
    }

    private int processedRows(UUID eventId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM consumer_processed_events WHERE consumer = ? AND event_id = ?",
                Integer.class, DEMO_CONSUMER, eventId);
    }

    private JsonNode json(MvcResult result) throws Exception {
        return objectMapper.readTree(result.getResponse().getContentAsString(StandardCharsets.UTF_8));
    }

    private UUID nominationId(MvcResult result) throws Exception {
        return UUID.fromString(json(result).get("nomination_id").asText());
    }

    /** Fila del outbox del nomination.requested de una nominación (hay exactamente una). */
    static Map<String, Object> requestedRow(JdbcTemplate jdbc, UUID nominationId) {
        return jdbc.queryForMap("""
                SELECT id, topic, published_at, attempts, last_error FROM outbox_events
                 WHERE aggregate_id = ? AND event_type = 'nomination.requested'""", nominationId);
    }

    static String newEntity() {
        return "EF" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
    }

    static String body(UUID requestId) {
        return """
                {
                  "request_id": "%s",
                  "customer_id": "CUST-000123",
                  "account_id": "%s",
                  "card_id": "%s",
                  "alias": "CUENTA SUELDO"
                }
                """.formatted(requestId, ACCOUNT_ID, CARD_TOKEN);
    }

    static MockHttpServletRequestBuilder create(String entity, String body) {
        return post(BASE).header(HttpHeaders.AUTHORIZATION, TestTokens.bearer(entity))
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }
}
