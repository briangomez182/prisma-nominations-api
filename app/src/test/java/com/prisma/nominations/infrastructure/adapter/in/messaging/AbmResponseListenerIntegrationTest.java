package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.TestcontainersConfiguration;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.RejectionReason;
import com.prisma.nominations.domain.StatusChange;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
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
 * Consumer de abm.responses.v1 contra Kafka y PostgreSQL reales. ABM no existe en este contexto (adapter y mock
 * apagados): las respuestas se publican a mano, con el contrato que produce ABM. Relay apagado: el evento de
 * resultado se verifica en la tabla del outbox.
 */
@SpringBootTest(properties = {
        "nominations.abm.adapter.enabled=false",
        "nominations.abm-mock.enabled=false",
        "nominations.outbox.relay.enabled=false",
        "nominations.abm.response-consumer.enabled=true",
        "nominations.abm.response-consumer.backoff=100ms"})
@Import(TestcontainersConfiguration.class)
@ExtendWith(OutputCaptureExtension.class)
@Tag("integration")
class AbmResponseListenerIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String DLT = KafkaTopics.ABM_RESPONSES + "-dlt";

    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private CreateNominationUseCase createNomination;
    @Autowired
    private NominationRepository repository;

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
    @Tag("E4")
    @DisplayName("E4: ABM aprueba → APPROVED, historial y un nomination.result en el outbox")
    void approved_resolvesAndAppendsResult() throws Exception {
        Nomination nomination = createNomination();

        publish(nomination, response(nomination, "APPROVED", null));

        awaitStatus(nomination, NominationStatus.APPROVED);
        assertThat(repository.findHistory(nomination.id())).extracting(StatusChange::to)
                .containsExactly(NominationStatus.RECEIVED, NominationStatus.APPROVED);
        assertThat(resultEvents(nomination)).isEqualTo(1);
    }

    @Test
    @Tag("E7")
    @DisplayName("E7: la misma respuesta entregada dos veces se procesa una sola vez")
    void duplicateResponse_processedOnce(CapturedOutput output) throws Exception {
        Nomination nomination = createNomination();
        String json = response(nomination, "APPROVED", null);

        publish(nomination, json);
        awaitStatus(nomination, NominationStatus.APPROVED);
        publish(nomination, json);

        await().atMost(TIMEOUT).until(() -> output.getOut().lines().anyMatch(l ->
                l.contains("Respuesta de ABM procesada: nomination_id=" + nomination.id())
                        && l.contains("outcome=DUPLICATE")));
        assertThat(resultEvents(nomination)).isEqualTo(1);
        assertThat(repository.findHistory(nomination.id())).hasSize(2);
    }

    @Test
    @Tag("E5")
    @DisplayName("E5: ABM rechaza con ABM-051 → REJECTED con motivo ACCOUNT_BLOCKED y evento")
    void rejected_normalizesReason() throws Exception {
        Nomination nomination = createNomination();

        publish(nomination, response(nomination, "REJECTED", "ABM-051"));

        awaitStatus(nomination, NominationStatus.REJECTED);
        Nomination stored = repository.findById(nomination.id()).orElseThrow();
        assertThat(stored.rejectionReason()).isEqualTo(RejectionReason.ACCOUNT_BLOCKED);
        assertThat(stored.abmReasonCode()).isEqualTo("ABM-051");
        assertThat(resultEvents(nomination)).isEqualTo(1);
        String payload = jdbc.queryForObject("SELECT payload::text FROM outbox_events "
                + "WHERE aggregate_id = ? AND event_type = 'nomination.result'", String.class, nomination.id());
        assertThat(objectMapper.readTree(payload).path("rejection_reason").asText()).isEqualTo("ACCOUNT_BLOCKED");
        assertThat(payload).doesNotContain("ABM-051");
    }

    @Test
    @DisplayName("poison pill: JSON inválido va directo a abm.responses.v1-dlt")
    void invalidJson_goesToDlt() throws Exception {
        String key = UUID.randomUUID().toString();
        String garbage = "{esto no es json";
        kafkaTemplate.send(new ProducerRecord<>(KafkaTopics.ABM_RESPONSES, key, garbage)).get(10, TimeUnit.SECONDS);

        assertThat(awaitDlt(key).value()).isEqualTo(garbage);
    }

    @Test
    @DisplayName("respuesta con request_id ajeno va al DLT y no modifica la nominación")
    void foreignRequestId_goesToDlt() throws Exception {
        Nomination nomination = createNomination();
        Map<String, Object> body = body(nomination, "APPROVED", null);
        body.put("request_id", UUID.randomUUID().toString());

        publish(nomination, objectMapper.writeValueAsString(body));

        ConsumerRecord<String, String> dead = awaitDlt(nomination.id().toString());
        assertThat(new String(dead.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value(),
                StandardCharsets.UTF_8)).endsWith("NominationNotFoundException");
        assertThat(repository.findById(nomination.id()).orElseThrow().status()).isEqualTo(NominationStatus.RECEIVED);
        assertThat(resultEvents(nomination)).isZero();
    }

    // ---------------------------------------------------------------- helpers

    private Nomination createNomination() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        return createNomination.create(new CreateNominationCommand("E" + suffix, UUID.randomUUID(), "123456",
                "0001234567890987654", "tok_4f9a2c7b8d1e", "CUENTA_PRINCIPAL", "it-" + suffix)).nomination();
    }

    private Map<String, Object> body(Nomination n, String result, String reasonCode) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("abm_operation_id", "ABM-OP-" + n.id());
        body.put("nomination_id", n.id().toString());
        body.put("request_id", n.requestId().toString());
        body.put("correlation_id", n.correlationId());
        body.put("result", result);
        if (reasonCode != null) {
            body.put("reason_code", reasonCode);
            body.put("reason_description", "Cuenta bloqueada");
        }
        body.put("responded_at", "2026-10-09T12:00:00Z");
        body.put("campo_nuevo", "se ignora");
        return body;
    }

    private String response(Nomination n, String result, String reasonCode) throws Exception {
        return objectMapper.writeValueAsString(body(n, result, reasonCode));
    }

    private void publish(Nomination n, String json) throws Exception {
        var record = new ProducerRecord<>(KafkaTopics.ABM_RESPONSES, n.id().toString(), json);
        record.headers().add(KafkaTopics.HEADER_CORRELATION_ID, n.correlationId().getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
    }

    private void awaitStatus(Nomination n, NominationStatus expected) {
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(repository.findById(n.id()).orElseThrow().status()).isEqualTo(expected));
    }

    private int resultEvents(Nomination n) {
        Integer rows = jdbc.queryForObject("SELECT count(*) FROM outbox_events "
                + "WHERE aggregate_id = ? AND event_type = 'nomination.result'", Integer.class, n.id());
        return rows == null ? 0 : rows;
    }

    private ConsumerRecord<String, String> awaitDlt(String key) {
        try (KafkaConsumer<String, String> dltConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            dltConsumer.subscribe(List.of(DLT));
            List<ConsumerRecord<String, String>> found = new ArrayList<>();
            await().atMost(TIMEOUT).until(() -> {
                dltConsumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (key.equals(r.key())) {
                        found.add(r);
                    }
                });
                return !found.isEmpty();
            });
            return found.getFirst();
        }
    }
}
