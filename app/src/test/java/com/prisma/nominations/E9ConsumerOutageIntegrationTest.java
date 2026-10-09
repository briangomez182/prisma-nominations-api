package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.KafkaConsumerConfig;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.web.client.RestClient;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.prisma.nominations.KafkaTopicProbe.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * E9 - consumidor temporalmente fuera de servicio, de punta a punta y con todo encendido (ABM simulado incluido).
 * <p>
 * Se detiene el contenedor del consumidor de ejemplo de {@code nomination.result.v1} (listener
 * {@code notifications-demo}) y, mientras está caído, varias nominaciones llegan a estado final. Se verifica:
 * <ol>
 *   <li>Productor desacoplado: la API sigue respondiendo (202 y GET con el estado final), el outbox queda sin
 *       pendientes y cada resultado está en el tópico una sola vez.</li>
 *   <li>Los eventos esperan en Kafka: el consumer group no avanza (offsets committeados intactos) y hay lag.</li>
 *   <li>Recuperación: al volver, el consumidor retoma exactamente desde el offset committeado de cada partición
 *       (nada anterior se reentrega), procesa todos una sola vez, en orden de offset por partición (y por lo tanto
 *       por key = nomination_id), y el lag vuelve a cero.</li>
 * </ol>
 * Las entregas se observan con un {@link RecordInterceptor} de test sobre la factory del consumidor ({@link
 * DeliveryLog}): ve cada registro antes del listener, también los que la deduplicación descartaría.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nominations.abm-mock.response-delay=300ms",
        "nominations.abm-mock.duplicate-gap=100ms"})
@Import({TestcontainersConfiguration.class, E9ConsumerOutageIntegrationTest.DeliveryLogConfig.class})
@ExtendWith(OutputCaptureExtension.class)
@Tag("integration")
@Tag("E9")
class E9ConsumerOutageIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final String LISTENER_ID = "notifications-demo";
    private static final String DEMO_CONSUMER_GROUP = "notifications-demo";
    private static final String ACCOUNT_ID = NominationEventFlowIntegrationTest.ACCOUNT_ID;
    private static final int APPROVED = 4;
    private static final int REJECTED = 2;

    @LocalServerPort
    private int port;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private DeliveryLog deliveries;
    @Value("${nominations.kafka.partitions}")
    private int partitions;

    private Admin admin;

    @BeforeEach
    void setUp() {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    @AfterEach
    void tearDown() {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        if (container != null && !container.isRunning()) {
            container.start(); // contexto cacheado: nunca dejarlo detenido para otros tests
        }
        admin.close();
    }

    @Test
    @DisplayName("E9 - con el consumidor caído la API sigue y los eventos esperan; al volver retoma del offset "
            + "committeado y procesa todos una vez, en orden")
    void consumerOutage_eventsWaitInTopicAndAreProcessedOnceInOrderAfterRestart(CapturedOutput output)
            throws Exception {
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        assertThat(container).isNotNull();
        assertThat(container.isRunning()).isTrue();

        // Línea de base: una nominación procesada y el grupo al día (committed == end en todas las particiones).
        Created warmUp = post("tok_demo_e9_warmup");
        awaitStatus(warmUp, "APPROVED");
        awaitProcessed(awaitResultEventId(warmUp.nominationId()));
        await().atMost(TIMEOUT).until(() -> lag(committedOffsets()) == 0);

        // ---------------------------------------------------------------- consumidor fuera de servicio
        container.stop();
        assertThat(container.isRunning()).isFalse();
        Map<TopicPartition, Long> committedAtStop = committedOffsets();
        deliveries.clear();

        List<Created> created = new ArrayList<>();
        Map<UUID, UUID> eventIdByNomination = new LinkedHashMap<>();
        Map<TopicPartition, Long> endWhileDown;
        try {
            for (int i = 0; i < APPROVED + REJECTED; i++) {
                created.add(post(i < APPROVED ? "tok_demo_e9_ok_" + i : "tok_demo_REJECT_030_e9_" + i));
            }

            // La API sigue respondiendo y las nominaciones llegan a estado final sin depender del consumidor.
            for (int i = 0; i < created.size(); i++) {
                Created c = created.get(i);
                JsonNode nomination = awaitStatus(c, i < APPROVED ? "APPROVED" : "REJECTED");
                if (i >= APPROVED) {
                    assertThat(nomination.get("rejection_reason").asText()).isEqualTo("CARD_NOT_ELIGIBLE");
                }
                eventIdByNomination.put(c.nominationId(), awaitResultEventId(c.nominationId()));
            }

            // Productor intacto: nada pendiente en el outbox de estas nominaciones (requested + result publicados).
            List<UUID> ids = created.stream().map(Created::nominationId).toList();
            assertThat(ids).allSatisfy(id -> {
                assertThat(jdbc.queryForObject(
                        "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND published_at IS NULL",
                        Integer.class, id)).isZero();
                assertThat(jdbc.queryForObject(
                        "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.result'",
                        Integer.class, id)).isEqualTo(1);
            });

            // Los eventos están en el tópico (uno por nominación, después del offset committeado) y nadie los leyó.
            try (var probe = new KafkaTopicProbe(kafka.getBootstrapServers(), KafkaTopics.NOMINATION_RESULT)) {
                for (Created c : created) {
                    List<ConsumerRecord<String, String>> records = probe.recordsWithKey(c.nominationId());
                    assertThat(records).hasSize(1);
                    ConsumerRecord<String, String> record = records.getFirst();
                    assertThat(header(record, KafkaTopics.HEADER_EVENT_ID))
                            .isEqualTo(eventIdByNomination.get(c.nominationId()).toString());
                    assertThat(record.offset())
                            .isGreaterThanOrEqualTo(committedAtStop.get(partition(record)));
                }
            }
            assertThat(committedOffsets()).as("el grupo no avanza con el consumidor caído")
                    .isEqualTo(committedAtStop);
            endWhileDown = endOffsets();
            assertThat(lag(committedAtStop, endWhileDown)).isGreaterThanOrEqualTo(created.size());
            assertThat(eventIdByNomination.values()).allSatisfy(id -> assertThat(processedRows(id)).isZero());
            assertThat(deliveries.all()).isEmpty();
        } finally {
            container.start();
        }

        // ---------------------------------------------------------------- recuperación
        assertThat(container.isRunning()).isTrue();
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(eventIdByNomination.values())
                .allSatisfy(id -> assertThat(processedRows(id)).isEqualTo(1)));
        await().atMost(TIMEOUT).until(() -> lag(committedOffsets()) == 0);

        List<Delivery> delivered = deliveries.all();
        Map<TopicPartition, List<Long>> offsetsByPartition = delivered.stream().collect(Collectors.groupingBy(
                Delivery::partition, LinkedHashMap::new, Collectors.mapping(Delivery::offset, Collectors.toList())));

        // Retoma exactamente desde el offset committeado y entrega cada registro pendiente una sola vez, en orden.
        offsetsByPartition.forEach((tp, offsets) -> {
            long from = committedAtStop.get(tp);
            long to = endWhileDown.get(tp);
            assertThat(offsets).as("entregas de %s", tp)
                    .containsExactlyElementsOf(LongStream.range(from, to).boxed().toList());
        });
        assertThat(delivered).hasSize((int) lag(committedAtStop, endWhileDown));

        // Cada evento, una sola entrega y un solo efecto.
        Map<String, Long> deliveriesByEvent = delivered.stream()
                .filter(d -> d.eventId() != null)
                .collect(Collectors.groupingBy(Delivery::eventId, Collectors.counting()));
        assertThat(eventIdByNomination.values()).allSatisfy(id -> {
            assertThat(deliveriesByEvent.get(id.toString())).isEqualTo(1L);
            assertThat(output.getOut().lines()
                    .filter(l -> l.contains("Notificación enviada") && l.contains("event_id=" + id))
                    .count()).isEqualTo(1);
        });
        // Por key (= nomination_id): cada nominación se entregó una vez; el orden por key lo da el orden de offset
        // por partición verificado arriba (misma key → misma partición).
        assertThat(delivered).extracting(Delivery::key)
                .containsAll(created.stream().map(c -> c.nominationId().toString()).toList());

        // El productor no se vio afectado: los estados finales siguen igual.
        for (int i = 0; i < created.size(); i++) {
            assertThat(status(created.get(i))).isEqualTo(i < APPROVED ? "APPROVED" : "REJECTED");
        }
    }

    // ---------------------------------------------------------------- soporte

    record Created(UUID nominationId, String entity) {
    }

    private Created post(String cardId) throws Exception {
        String entity = NominationEventFlowIntegrationTest.newEntity();
        String body = """
                {
                  "request_id": "%s",
                  "customer_id": "CUST-000123",
                  "account_id": "%s",
                  "card_id": "%s",
                  "alias": "CUENTA SUELDO"
                }
                """.formatted(UUID.randomUUID(), ACCOUNT_ID, cardId);
        ResponseEntity<String> response = http().post().uri("/v1/nominations")
                .header(HttpHeaders.AUTHORIZATION, TestTokens.bearer(entity))
                .header(ApiHeaders.CORRELATION_ID, "it-e9-" + UUID.randomUUID())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .toEntity(String.class);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        UUID nominationId = UUID.fromString(objectMapper.readTree(response.getBody()).get("nomination_id").asText());
        return new Created(nominationId, entity);
    }

    private RestClient http() {
        return RestClient.create("http://localhost:" + port);
    }

    private JsonNode nomination(Created created) throws Exception {
        return objectMapper.readTree(http().get().uri("/v1/nominations/" + created.nominationId())
                .header(HttpHeaders.AUTHORIZATION, TestTokens.bearer(created.entity()))
                .retrieve().body(String.class));
    }

    private String status(Created created) throws Exception {
        return nomination(created).get("status").asText();
    }

    private JsonNode awaitStatus(Created created, String expected) throws Exception {
        await().atMost(TIMEOUT).until(() -> status(created), equalTo(expected));
        return nomination(created);
    }

    /** event_id del nomination.result, una vez publicado por el relay. */
    private UUID awaitResultEventId(UUID nominationId) {
        String sql = """
                SELECT id FROM outbox_events
                 WHERE aggregate_id = ? AND event_type = 'nomination.result' AND published_at IS NOT NULL""";
        await().atMost(TIMEOUT).until(() -> !jdbc.queryForList(sql, UUID.class, nominationId).isEmpty());
        return jdbc.queryForObject(sql, UUID.class, nominationId);
    }

    private void awaitProcessed(UUID eventId) {
        await().atMost(TIMEOUT).until(() -> processedRows(eventId), equalTo(1));
    }

    private int processedRows(UUID eventId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM consumer_processed_events WHERE consumer = ? AND event_id = ?",
                Integer.class, DEMO_CONSUMER_GROUP, eventId);
    }

    private List<TopicPartition> resultPartitions() {
        return IntStream.range(0, partitions)
                .mapToObj(p -> new TopicPartition(KafkaTopics.NOMINATION_RESULT, p))
                .toList();
    }

    private static TopicPartition partition(ConsumerRecord<?, ?> record) {
        return new TopicPartition(record.topic(), record.partition());
    }

    /** Offset committeado del grupo por partición (0 si nunca committeó: auto-offset-reset=earliest). */
    private Map<TopicPartition, Long> committedOffsets() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(DEMO_CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        Map<TopicPartition, Long> result = new HashMap<>();
        for (TopicPartition tp : resultPartitions()) {
            OffsetAndMetadata offset = committed.get(tp);
            result.put(tp, offset == null ? 0L : offset.offset());
        }
        return result;
    }

    private Map<TopicPartition, Long> endOffsets() throws Exception {
        Map<TopicPartition, OffsetSpec> request = resultPartitions().stream()
                .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
        Map<TopicPartition, Long> result = new HashMap<>();
        admin.listOffsets(request).all().get(10, TimeUnit.SECONDS)
                .forEach((tp, info) -> result.put(tp, info.offset()));
        return result;
    }

    private long lag(Map<TopicPartition, Long> committed) throws Exception {
        return lag(committed, endOffsets());
    }

    private static long lag(Map<TopicPartition, Long> committed, Map<TopicPartition, Long> end) {
        return end.entrySet().stream().mapToLong(e -> e.getValue() - committed.getOrDefault(e.getKey(), 0L)).sum();
    }

    record Delivery(TopicPartition partition, long offset, String key, String eventId) {
    }

    /** Registra cada entrega al listener del consumidor de ejemplo, antes de la deduplicación. */
    static final class DeliveryLog implements BeanPostProcessor, RecordInterceptor<String, String> {

        private final List<Delivery> deliveries = new CopyOnWriteArrayList<>();

        @Override
        @SuppressWarnings("unchecked")
        public Object postProcessAfterInitialization(Object bean, String beanName) {
            if (KafkaConsumerConfig.NOTIFICATIONS_CONTAINER_FACTORY.equals(beanName)
                    && bean instanceof ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
                ((ConcurrentKafkaListenerContainerFactory<String, String>) factory).setRecordInterceptor(this);
            }
            return bean;
        }

        @Override
        public ConsumerRecord<String, String> intercept(ConsumerRecord<String, String> record,
                                                        Consumer<String, String> consumer) {
            var eventId = record.headers().lastHeader(KafkaTopics.HEADER_EVENT_ID);
            deliveries.add(new Delivery(partition(record), record.offset(), record.key(),
                    eventId == null ? null : new String(eventId.value(), StandardCharsets.UTF_8)));
            return record;
        }

        List<Delivery> all() {
            return List.copyOf(deliveries);
        }

        void clear() {
            deliveries.clear();
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class DeliveryLogConfig {

        @Bean
        static DeliveryLog e9DeliveryLog() {
            return new DeliveryLog();
        }
    }
}
