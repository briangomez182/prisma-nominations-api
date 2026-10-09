package com.prisma.nominations;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.out.AbmClient;
import com.prisma.nominations.application.port.out.AbmRequest;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * E6 / E10 de punta a punta con base y Kafka reales: ABM (el {@link AbmClient}, ya con Resilience4j adentro) se
 * reemplaza por un mock que falla a pedido. Se verifica la recuperación no bloqueante: tópicos de retry, DLT,
 * paso a ABM_TIMEOUT sin evento de resultado, y que un mensaje fallando no frena a los demás de su partición.
 * <p>
 * El nomination.requested se publica a mano como lo haría el relay (payload del outbox, key, headers); el relay
 * está apagado para controlar partición y momento.
 */
@SpringBootTest(properties = {
        "nominations.outbox.relay.enabled=false",
        "nominations.abm-mock.enabled=false",
        "nominations.abm.response-consumer.enabled=false",
        "nominations.abm.sweeper.enabled=false",
        "nominations.demo-consumer.enabled=false",
        "nominations.abm.adapter.enabled=true",
        // Esperas cortas pero medibles: el primer mensaje tarda >= 2 s en agotar sus reintentos.
        "nominations.abm.adapter.retry-delays=1s,1s"})
@Import(TestcontainersConfiguration.class)
class AbmAdapterRetryIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String DLT = KafkaTopics.NOMINATION_REQUESTED + "-dlt";
    private static final String RETRY_0 = KafkaTopics.NOMINATION_REQUESTED + "-retry-0";
    private static final String RETRY_1 = KafkaTopics.NOMINATION_REQUESTED + "-retry-1";

    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private CreateNominationUseCase createNomination;
    @MockitoBean
    private AbmClient abmClient;

    private DefaultKafkaProducerFactory<String, String> producerFactory;
    private KafkaTemplate<String, String> kafkaTemplate;
    /** Comportamiento de ABM por nomination_id: recibe el nº de intento (1..n) y devuelve el abm_operation_id o lanza. */
    private final Map<UUID, Function<Integer, String>> behaviour = new ConcurrentHashMap<>();
    /** Cada llamada a ABM, en orden de llegada. */
    private final List<Call> calls = new CopyOnWriteArrayList<>();

    record Call(UUID nominationId, Instant at) {
    }

    @BeforeEach
    void setUp() {
        producerFactory = new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
        doAnswer(inv -> {
            AbmRequest request = inv.getArgument(0);
            calls.add(new Call(request.nominationId(), Instant.now()));
            int attempt = attempts(request.nominationId());
            return behaviour.getOrDefault(request.nominationId(), n -> "ABM-OP-" + n).apply(attempt);
        }).when(abmClient).submit(any());
    }

    @AfterEach
    void tearDown() {
        producerFactory.destroy();
    }

    @Test
    @DisplayName("E6: ABM no disponible → -retry-0 → -retry-1 → DLT → ABM_TIMEOUT, sin nomination.result")
    void unavailable_exhaustsRetriesAndMarksAbmTimeout() throws Exception {
        UUID id = create("it-unavailable");
        behaviour.put(id, attempt -> {
            throw new AbmUnavailableException("ABM no responde (circuit breaker abierto)", null);
        });

        publish(id, null);

        await().atMost(TIMEOUT).until(() -> "ABM_TIMEOUT".equals(status(id)));
        assertThat(attempts(id)).isEqualTo(3);
        assertThat(lastHistory(id)).containsExactly("RECEIVED", "ABM_TIMEOUT", "ABM_ADAPTER",
                "Reintentos agotados: ABM no disponible", "it-unavailable");
        assertThat(resultEvents(id)).isZero();
        try (var retry0 = new KafkaTopicProbe(kafka.getBootstrapServers(), RETRY_0);
             var retry1 = new KafkaTopicProbe(kafka.getBootstrapServers(), RETRY_1);
             var dlt = new KafkaTopicProbe(kafka.getBootstrapServers(), DLT)) {
            assertThat(retry0.recordsWithKey(id)).hasSize(1);
            assertThat(retry1.recordsWithKey(id)).hasSize(1);
            assertThat(dlt.recordsWithKey(id)).singleElement().satisfies(dead -> {
                assertThat(KafkaTopicProbe.header(dead, "kafka_exception-cause-fqcn"))
                        .isEqualTo(AbmUnavailableException.class.getName());
                assertThat(KafkaTopicProbe.header(dead, KafkaTopics.HEADER_CORRELATION_ID))
                        .isEqualTo("it-unavailable");
            });
        }
    }

    @Test
    @DisplayName("contrato: sin reintentos, DLT y ABM_TIMEOUT por contrato")
    void contract_goesStraightToDltAndMarksAbmTimeout() throws Exception {
        UUID id = create("it-contract");
        behaviour.put(id, attempt -> {
            throw new AbmContractException("ABM rechazó el pedido por contrato: HTTP 422");
        });

        publish(id, null);

        await().atMost(TIMEOUT).until(() -> "ABM_TIMEOUT".equals(status(id)));
        assertThat(attempts(id)).isEqualTo(1);
        assertThat(lastHistory(id)).containsExactly("RECEIVED", "ABM_TIMEOUT", "ABM_ADAPTER",
                "ABM rechazó el pedido por contrato", "it-contract");
        assertThat(resultEvents(id)).isZero();
        try (var retry0 = new KafkaTopicProbe(kafka.getBootstrapServers(), RETRY_0);
             var dlt = new KafkaTopicProbe(kafka.getBootstrapServers(), DLT)) {
            assertThat(retry0.recordsWithKey(id)).isEmpty();
            assertThat(dlt.recordsWithKey(id)).hasSize(1);
        }
    }

    @Test
    @DisplayName("transitorio que se recupera en el 2º intento → PENDING_ABM, sin DLT")
    void transient_recoversOnSecondAttempt() throws Exception {
        UUID id = create("it-transient");
        behaviour.put(id, attempt -> {
            if (attempt == 1) {
                throw new AbmUnavailableException("ABM respondió HTTP 503", null);
            }
            return "ABM-OP-" + id;
        });

        publish(id, null);

        await().atMost(TIMEOUT).until(() -> "PENDING_ABM".equals(status(id)));
        assertThat(attempts(id)).isEqualTo(2);
        try (var dlt = new KafkaTopicProbe(kafka.getBootstrapServers(), DLT)) {
            assertThat(dlt.recordsWithKey(id)).isEmpty();
        }
    }

    @Test
    @DisplayName("E10: un mensaje fallando no bloquea su partición: el siguiente se procesa antes de que agote reintentos")
    void failingMessage_doesNotBlockItsPartition() throws Exception {
        UUID failing = create("it-blocking-1");
        UUID healthy = create("it-blocking-2");
        behaviour.put(failing, attempt -> {
            throw new AbmUnavailableException("ABM no responde", null);
        });

        // Misma partición, en orden: primero el que falla.
        publish(failing, 0);
        publish(healthy, 0);

        await().atMost(TIMEOUT).until(() -> "PENDING_ABM".equals(status(healthy)));
        // El sano se envió con el primero todavía reintentando (le faltan >= 2 s de esperas).
        assertThat(status(failing)).isEqualTo("RECEIVED");
        assertThat(attempts(failing)).isLessThan(3);

        await().atMost(TIMEOUT).until(() -> "ABM_TIMEOUT".equals(status(failing)));
        Instant healthySent = firstCall(healthy);
        Instant failingLastAttempt = calls.stream().filter(c -> c.nominationId().equals(failing))
                .map(Call::at).max(Instant::compareTo).orElseThrow();
        assertThat(healthySent).isBefore(failingLastAttempt);
    }

    @Test
    @DisplayName("poison pill: DLT sin cambios de estado ni llamadas a ABM")
    void poisonPill_goesToDltWithoutStateChanges() throws Exception {
        UUID id = create("it-poison");
        var record = new ProducerRecord<>(KafkaTopics.NOMINATION_REQUESTED, "no-es-un-uuid",
                "{\"event_type\":\"nomination.requested\",\"schema_version\":1}");
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        try (var dlt = new KafkaTopicProbe(kafka.getBootstrapServers(), DLT)) {
            await().atMost(TIMEOUT).pollInSameThread().until(() -> !dlt.recordsWithKey("no-es-un-uuid").isEmpty());
        }
        assertThat(calls).isEmpty();
        assertThat(status(id)).isEqualTo("RECEIVED");
    }

    // ---------------------------------------------------------------- helpers

    private UUID create(String correlationId) {
        var result = createNomination.create(new CreateNominationCommand("ENT01", UUID.randomUUID(), "123456",
                NominationEventFlowIntegrationTest.ACCOUNT_ID, "tok_4f9a2c7b8d1e", "CUENTA SUELDO", correlationId));
        return result.nomination().id();
    }

    /** Publica el nomination.requested del outbox como el relay: key, payload y headers estándar. */
    private void publish(UUID nominationId, Integer partition) throws Exception {
        Map<String, Object> row = jdbc.queryForMap("SELECT id, payload::text AS payload FROM outbox_events "
                + "WHERE aggregate_id = ? AND event_type = 'nomination.requested'", nominationId);
        String correlationId = jdbc.queryForObject("SELECT correlation_id FROM nominations WHERE id = ?",
                String.class, nominationId);
        var record = new ProducerRecord<>(KafkaTopics.NOMINATION_REQUESTED, partition, nominationId.toString(),
                (String) row.get("payload"));
        record.headers()
                .add(KafkaTopics.HEADER_EVENT_ID, row.get("id").toString().getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_EVENT_TYPE, "nomination.requested".getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_SCHEMA_VERSION, "1".getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
    }

    private String status(UUID id) {
        return jdbc.queryForObject("SELECT status FROM nominations WHERE id = ?", String.class, id);
    }

    private int attempts(UUID id) {
        return (int) calls.stream().filter(c -> c.nominationId().equals(id)).count();
    }

    private Instant firstCall(UUID id) {
        return calls.stream().filter(c -> c.nominationId().equals(id)).map(Call::at).findFirst().orElseThrow();
    }

    private List<String> lastHistory(UUID id) {
        return jdbc.queryForObject("SELECT from_status, to_status, source, detail, correlation_id "
                        + "FROM nomination_history WHERE nomination_id = ? ORDER BY id DESC LIMIT 1",
                (rs, n) -> List.of(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5)), id);
    }

    private int resultEvents(UUID id) {
        Integer count = jdbc.queryForObject("SELECT count(*) FROM outbox_events "
                + "WHERE aggregate_id = ? AND event_type = 'nomination.result'", Integer.class, id);
        return count == null ? 0 : count;
    }
}
