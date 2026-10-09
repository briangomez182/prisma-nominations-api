package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.TestcontainersConfiguration;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Consumidor de notificaciones contra Kafka y PostgreSQL reales. Los eventos se publican directo con un
 * KafkaTemplate propio (relay del outbox apagado), con los mismos headers que pone el relay.
 */
@SpringBootTest(properties = {
        "nominations.outbox.relay.enabled=false",
        "nominations.demo-consumer.enabled=true",
        "nominations.demo-consumer.backoff=100ms"})
@Import(TestcontainersConfiguration.class)
@ExtendWith(OutputCaptureExtension.class)
class NominationResultNotifierIntegrationTest {

    private static final String CONSUMER = "notifications-demo";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Value("${nominations.kafka.partitions}")
    private int partitions;

    private DefaultKafkaProducerFactory<String, String> producerFactory;
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        producerFactory = new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @AfterEach
    void tearDown() {
        producerFactory.destroy();
    }

    @Test
    @DisplayName("procesa un evento con headers: registra el event_id y deja la notificación en el log")
    void processesEvent(CapturedOutput output) throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID nominationId = UUID.randomUUID();

        publish(eventId, nominationId, payload(eventId, nominationId, "REJECTED", "INVALID_CARD", Map.of()));

        awaitProcessed(eventId);
        assertThat(output.getOut()).contains("Notificación enviada: nomination_id=" + nominationId
                + ", status=REJECTED, motivo=INVALID_CARD");
    }

    @Test
    @DisplayName("E7/E9: el mismo evento entregado dos veces produce una sola fila y un solo efecto")
    void duplicateEvent_processedOnce(CapturedOutput output) throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID nominationId = UUID.randomUUID();
        String json = payload(eventId, nominationId, "APPROVED", null, Map.of());

        publish(eventId, nominationId, json);
        publish(eventId, nominationId, json);
        // Misma key → misma partición: cuando se procesa el marcador, los dos duplicados ya pasaron.
        UUID marker = UUID.randomUUID();
        publish(marker, nominationId, payload(marker, nominationId, "APPROVED", null, Map.of()));

        awaitProcessed(marker);
        assertThat(processedRows(eventId)).isEqualTo(1);
        assertThat(occurrences(output.getOut(), "event_id=" + eventId)).isEqualTo(1);
    }

    @Test
    @DisplayName("E9: con el consumidor detenido los eventos esperan en el tópico y al volver los procesa todos")
    void consumerDown_resumesFromCommittedOffset() throws Exception {
        MessageListenerContainer container = registry.getListenerContainer(NominationResultNotifier.LISTENER_ID);
        assertThat(container).isNotNull();
        List<UUID> eventIds = new ArrayList<>();

        container.stop();
        try {
            for (int i = 0; i < 5; i++) {
                UUID eventId = UUID.randomUUID();
                UUID nominationId = UUID.randomUUID();
                eventIds.add(eventId);
                publish(eventId, nominationId, payload(eventId, nominationId, "APPROVED", null, Map.of()));
            }
            // Mientras está caído, nada se procesa (la API no se ve afectada: los eventos quedan en Kafka).
            TimeUnit.SECONDS.sleep(2);
            assertThat(eventIds).allSatisfy(id -> assertThat(processedRows(id)).isZero());
        } finally {
            container.start();
        }

        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(eventIds).allSatisfy(id -> assertThat(processedRows(id)).isEqualTo(1)));
    }

    @Test
    @DisplayName("poison pill: payload no parseable va directo a nomination.result.v1-dlt")
    void unparseablePayload_goesToDlt() throws Exception {
        String key = UUID.randomUUID().toString();
        String garbage = "{esto no es json";
        kafkaTemplate.send(new ProducerRecord<>(KafkaTopics.NOMINATION_RESULT, key, garbage)).get(10, TimeUnit.SECONDS);

        try (KafkaConsumer<String, String> dltConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            dltConsumer.subscribe(List.of(KafkaTopics.NOMINATION_RESULT + "-dlt"));
            List<ConsumerRecord<String, String>> found = new ArrayList<>();
            await().atMost(TIMEOUT).until(() -> {
                dltConsumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (key.equals(r.key())) {
                        found.add(r);
                    }
                });
                return !found.isEmpty();
            });
            assertThat(found.getFirst().value()).isEqualTo(garbage);
            assertThat(new String(found.getFirst().headers().lastHeader("kafka_dlt-exception-fqcn").value(),
                    StandardCharsets.UTF_8)).endsWith("ListenerExecutionFailedException");
        }
    }

    @Test
    @DisplayName("evolución compatible: campo desconocido y schema_version mayor se procesan igual")
    void unknownFieldAndNewerSchemaVersion_processed() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID nominationId = UUID.randomUUID();
        String json = payload(eventId, nominationId, "APPROVED", null,
                Map.of("schema_version", 2, "channel", "HOME_BANKING", "extra", Map.of("anidado", true)));

        kafkaTemplate.send(record(eventId, nominationId, json, "2")).get(10, TimeUnit.SECONDS);

        awaitProcessed(eventId);
    }

    @Test
    @DisplayName("sin header event_id se usa el del payload")
    void eventIdFallsBackToPayload() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID nominationId = UUID.randomUUID();
        String json = payload(eventId, nominationId, "APPROVED", null, Map.of());

        kafkaTemplate.send(new ProducerRecord<>(KafkaTopics.NOMINATION_RESULT, nominationId.toString(), json))
                .get(10, TimeUnit.SECONDS);

        awaitProcessed(eventId);
    }

    @Test
    @DisplayName("los tópicos y sus DLT existen con las particiones configuradas")
    void topicsCreatedWithConfiguredPartitions() throws Exception {
        List<String> topics = List.of(
                KafkaTopics.NOMINATION_REQUESTED, KafkaTopics.NOMINATION_REQUESTED + "-dlt",
                KafkaTopics.ABM_RESPONSES, KafkaTopics.ABM_RESPONSES + "-dlt",
                KafkaTopics.NOMINATION_RESULT, KafkaTopics.NOMINATION_RESULT + "-dlt");

        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames()
                    .get(10, TimeUnit.SECONDS);
            assertThat(descriptions).containsOnlyKeys(topics);
            assertThat(descriptions.values()).allSatisfy(d -> assertThat(d.partitions()).hasSize(partitions));
        }
    }

    // ---------------------------------------------------------------- helpers

    private void publish(UUID eventId, UUID nominationId, String json) throws Exception {
        kafkaTemplate.send(record(eventId, nominationId, json, "1")).get(10, TimeUnit.SECONDS);
    }

    /** Mismos headers y key que el relay del outbox. */
    private static ProducerRecord<String, String> record(UUID eventId, UUID nominationId, String json,
                                                         String schemaVersion) {
        var record = new ProducerRecord<>(KafkaTopics.NOMINATION_RESULT, nominationId.toString(), json);
        record.headers()
                .add(KafkaTopics.HEADER_EVENT_ID, eventId.toString().getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_EVENT_TYPE, "nomination.result".getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_SCHEMA_VERSION, schemaVersion.getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_CORRELATION_ID, ("it-" + eventId).getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private String payload(UUID eventId, UUID nominationId, String status, String reason,
                           Map<String, Object> overrides) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("event_id", eventId.toString());
        body.put("event_type", "nomination.result");
        body.put("schema_version", 1);
        body.put("nomination_id", nominationId.toString());
        body.put("entity_id", "0072");
        body.put("request_id", UUID.randomUUID().toString());
        body.put("status", status);
        if (reason != null) {
            body.put("rejection_reason", reason);
        }
        body.put("account_id", "****7654");
        body.put("card_id", "****8d1e");
        body.put("correlation_id", "it-" + eventId);
        body.put("occurred_at", "2026-10-09T12:00:00Z");
        body.putAll(overrides);
        return objectMapper.writeValueAsString(body);
    }

    private void awaitProcessed(UUID eventId) {
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(processedRows(eventId)).isEqualTo(1));
    }

    private int processedRows(UUID eventId) {
        Integer rows = jdbc.queryForObject(
                "SELECT count(*) FROM consumer_processed_events WHERE consumer = ? AND event_id = ?",
                Integer.class, CONSUMER, eventId);
        return rows == null ? 0 : rows;
    }

    private static long occurrences(String text, String token) {
        return text.lines().filter(l -> l.contains("Notificación enviada") && l.contains(token)).count();
    }
}
