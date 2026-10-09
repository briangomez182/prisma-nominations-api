package com.prisma.nominations.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.TestcontainersConfiguration;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Relay y purga del outbox contra PostgreSQL y Kafka reales. El relay programado está apagado: cada test
 * lo invoca a mano. Las filas se insertan directo (aggregate_id no tiene FK) y cada test publica en un
 * tópico propio, así lo que consume es solo suyo.
 */
@SpringBootTest(properties = {
        "nominations.outbox.relay.enabled=false",
        "nominations.outbox.purge.enabled=false",
        "nominations.demo-consumer.enabled=false",
        "nominations.outbox.relay.batch-size=5"
})
@Import(TestcontainersConfiguration.class)
class OutboxRelayIntegrationTest {

    private static final int PARTITIONS = 3;

    @Autowired
    private OutboxRelay relay;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private PlatformTransactionManager txManager;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaContainer kafka;

    private String topic;
    private Instant base;

    @BeforeEach
    void setUp() throws Exception {
        drain();
        topic = "outbox-it-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 1))).all().get();
        }
        base = Instant.now().minusSeconds(60);
    }

    @Test
    @DisplayName("publica key, value y headers, y respeta el orden por nominación")
    void publishesKeyValueHeadersInOrder() throws Exception {
        UUID nominationA = UUID.randomUUID();
        UUID nominationB = UUID.randomUUID();
        UUID a1 = insert(nominationA, "nomination.requested", 0, "corr-a");
        UUID b1 = insert(nominationB, "nomination.requested", 1, "corr-b");
        UUID a2 = insert(nominationA, "nomination.result", 2, "corr-a");

        assertThat(drain()).isEqualTo(3);

        List<ConsumerRecord<String, String>> records = consume(3);
        ConsumerRecord<String, String> first = byEventId(records, a1);
        assertThat(first.key()).isEqualTo(nominationA.toString());
        assertThat(objectMapper.readTree(first.value())).isEqualTo(objectMapper.readTree(payload(a1, nominationA)));
        assertThat(header(first, KafkaTopics.HEADER_EVENT_ID)).isEqualTo(a1.toString());
        assertThat(header(first, KafkaTopics.HEADER_EVENT_TYPE)).isEqualTo("nomination.requested");
        assertThat(header(first, KafkaTopics.HEADER_SCHEMA_VERSION)).isEqualTo("1");
        assertThat(header(first, KafkaTopics.HEADER_CORRELATION_ID)).isEqualTo("corr-a");
        assertThat(byEventId(records, b1).key()).isEqualTo(nominationB.toString());

        assertThat(eventIdsOf(records, nominationA)).containsExactly(a1.toString(), a2.toString());
        for (UUID id : List.of(a1, b1, a2)) {
            Map<String, Object> row = row(id);
            assertThat(row.get("published_at")).isNotNull();
            assertThat(row.get("attempts")).isEqualTo(1);
            assertThat(row.get("last_error")).isNull();
        }
    }

    @Test
    @DisplayName("E10 - dos relays en paralelo: cada evento se publica una sola vez y en orden (SKIP LOCKED)")
    void concurrentRelaysPublishEachEventOnce() throws Exception {
        int nominations = 10;
        int eventsPerNomination = 4;
        List<UUID> nominationIds = Stream.generate(UUID::randomUUID).limit(nominations).toList();
        Map<UUID, List<String>> expected = new HashMap<>();
        int order = 0;
        for (int e = 0; e < eventsPerNomination; e++) {
            for (UUID nomination : nominationIds) {
                expected.computeIfAbsent(nomination, k -> new ArrayList<>())
                        .add(insert(nomination, "nomination.requested", order++, "corr-" + nomination).toString());
            }
        }
        int total = nominations * eventsPerNomination;

        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            List<Future<Integer>> results = new ArrayList<>();
            for (int t = 0; t < 2; t++) {
                results.add(pool.submit(() -> {
                    start.await();
                    int published = 0;
                    for (int i = 0; i < 50; i++) {
                        published += relay.relayOnce();
                    }
                    return published;
                }));
            }
            start.countDown();
            int published = 0;
            for (Future<Integer> result : results) {
                published += result.get();
            }
            assertThat(published + drain()).isEqualTo(total);
        }

        List<ConsumerRecord<String, String>> records = consume(total);
        Set<String> distinct = records.stream().map(r -> header(r, KafkaTopics.HEADER_EVENT_ID)).collect(Collectors.toSet());
        assertThat(records).hasSize(total);
        assertThat(distinct).hasSize(total);
        expected.forEach((nomination, ids) -> assertThat(eventIdsOf(records, nomination)).containsExactlyElementsOf(ids));
    }

    @Test
    @DisplayName("E8 - Kafka falla: el evento queda pendiente con el intento registrado y el lote se corta")
    void sendFailureKeepsEventPendingAndCutsBatch() throws Exception {
        UUID first = insert(UUID.randomUUID(), "nomination.requested", 0, "corr-e8-1");
        UUID second = insert(UUID.randomUUID(), "nomination.requested", 1, "corr-e8-2");

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> broken = mock(KafkaTemplate.class);
        when(broken.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new KafkaException("Broker no disponible")));

        assertThat(relayWith(broken, Duration.ofSeconds(1)).relayOnce()).isZero();

        Map<String, Object> failed = row(first);
        assertThat(failed.get("published_at")).isNull();
        assertThat(failed.get("attempts")).isEqualTo(1);
        assertThat((String) failed.get("last_error")).isEqualTo("KafkaException: Broker no disponible");
        Map<String, Object> notTried = row(second);
        assertThat(notTried.get("published_at")).isNull();
        assertThat(notTried.get("attempts")).isEqualTo(0);

        // Kafka vuelve: el ciclo siguiente publica ambos, en orden.
        assertThat(relay.relayOnce()).isEqualTo(2);
        List<ConsumerRecord<String, String>> records = consume(2);
        assertThat(records).extracting(r -> header(r, KafkaTopics.HEADER_EVENT_ID))
                .containsExactlyInAnyOrder(first.toString(), second.toString());
        Map<String, Object> recovered = row(first);
        assertThat(recovered.get("published_at")).isNotNull();
        assertThat(recovered.get("attempts")).isEqualTo(2);
        assertThat(recovered.get("last_error")).isNull();
    }

    @Test
    @DisplayName("E8 - el ack no llega dentro del send-timeout: cuenta como falla y se reintenta")
    void ackTimeoutCountsAsFailure() {
        UUID id = insert(UUID.randomUUID(), "nomination.requested", 0, "corr-e8-timeout");

        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> hanging = mock(KafkaTemplate.class);
        when(hanging.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<SendResult<String, String>>());

        assertThat(relayWith(hanging, Duration.ofMillis(200)).relayOnce()).isZero();

        Map<String, Object> row = row(id);
        assertThat(row.get("published_at")).isNull();
        assertThat(row.get("attempts")).isEqualTo(1);
        assertThat((String) row.get("last_error")).startsWith("TimeoutException");
        assertThat(drain()).isEqualTo(1);
    }

    @Test
    @DisplayName("last_error se trunca al tamaño de la columna")
    void describeTruncatesTo500() {
        String error = OutboxRelay.describe(new KafkaException("x".repeat(2_000)));
        assertThat(error).hasSize(OutboxRelay.MAX_ERROR_LENGTH).startsWith("KafkaException: ");
    }

    @Test
    @DisplayName("la purga borra solo eventos publicados más viejos que la retención")
    void purgeDeletesOnlyOldPublished() {
        Instant now = Instant.now();
        UUID oldPublished = insert(UUID.randomUUID(), "nomination.requested", 0, "c1");
        UUID recentPublished = insert(UUID.randomUUID(), "nomination.requested", 1, "c2");
        UUID oldPending = insert(UUID.randomUUID(), "nomination.requested", 2, "c3");
        jdbc.update("UPDATE outbox_events SET created_at = ?, published_at = ? WHERE id = ?",
                Timestamp.from(now.minus(Duration.ofDays(9))), Timestamp.from(now.minus(Duration.ofDays(8))), oldPublished);
        jdbc.update("UPDATE outbox_events SET published_at = ? WHERE id = ?",
                Timestamp.from(now.minus(Duration.ofDays(1))), recentPublished);
        jdbc.update("UPDATE outbox_events SET created_at = ? WHERE id = ?",
                Timestamp.from(now.minus(Duration.ofDays(30))), oldPending);

        var properties = new OutboxProperties(new OutboxProperties.Relay(false, Duration.ofMillis(500), 5,
                Duration.ofSeconds(1)), Duration.ofDays(7));
        assertThat(new OutboxPurger(jdbc, Clock.systemUTC(), properties).purge()).isEqualTo(1);

        assertThat(exists(oldPublished)).isFalse();
        assertThat(exists(recentPublished)).isTrue();
        assertThat(exists(oldPending)).isTrue();
        jdbc.update("DELETE FROM outbox_events WHERE id = ?", oldPending);
    }

    // --- helpers ---

    /** Corre ciclos hasta que no quede nada publicable. Devuelve el total publicado. */
    private int drain() {
        int total = 0;
        int published;
        while ((published = relay.relayOnce()) > 0) {
            total += published;
        }
        return total;
    }

    private OutboxRelay relayWith(KafkaTemplate<String, String> template, Duration sendTimeout) {
        var properties = new OutboxProperties(new OutboxProperties.Relay(false, Duration.ofMillis(500), 5, sendTimeout),
                Duration.ofDays(7));
        return new OutboxRelay(jdbc, txManager, template, objectMapper, properties);
    }

    private UUID insert(UUID nominationId, String eventType, int order, String correlationId) {
        UUID id = UUID.randomUUID();
        String headers = """
                {"event_id": "%s", "event_type": "%s", "schema_version": 1, "correlation_id": "%s"}"""
                .formatted(id, eventType, correlationId);
        jdbc.update("""
                        INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, topic, payload, headers, created_at)
                        VALUES (?, ?, 'nomination', ?, ?, ?::jsonb, ?::jsonb, ?)""",
                id, nominationId, eventType, topic, payload(id, nominationId), headers,
                Timestamp.from(base.plusMillis(order)));
        return id;
    }

    private static String payload(UUID eventId, UUID nominationId) {
        return """
                {"event_id": "%s", "nomination_id": "%s", "schema_version": 1}""".formatted(eventId, nominationId);
    }

    private Map<String, Object> row(UUID id) {
        return jdbc.queryForMap("SELECT published_at, attempts, last_error FROM outbox_events WHERE id = ?", id);
    }

    private boolean exists(UUID id) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE id = ?", Integer.class, id) == 1;
    }

    private List<ConsumerRecord<String, String>> consume(int expected) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "outbox-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 20_000;
            while (records.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(records::add);
            }
            // Un poll extra para detectar duplicados que llegaran tarde.
            consumer.poll(Duration.ofMillis(500)).forEach(records::add);
        }
        return records;
    }

    private static ConsumerRecord<String, String> byEventId(List<ConsumerRecord<String, String>> records, UUID eventId) {
        return records.stream().filter(r -> eventId.toString().equals(header(r, KafkaTopics.HEADER_EVENT_ID)))
                .findFirst().orElseThrow();
    }

    /** event_id de una nominación en orden de offset (misma key = misma partición). */
    private static List<String> eventIdsOf(List<ConsumerRecord<String, String>> records, UUID nominationId) {
        return records.stream().filter(r -> nominationId.toString().equals(r.key()))
                .sorted((x, y) -> Long.compare(x.offset(), y.offset()))
                .map(r -> header(r, KafkaTopics.HEADER_EVENT_ID)).toList();
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
