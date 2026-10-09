package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.TestcontainersConfiguration;
import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.port.in.MarkAbmFailureUseCase;
import com.prisma.nominations.application.port.in.SubmitToAbmUseCase;
import com.prisma.nominations.application.port.in.SubmitToAbmUseCase.SubmitOutcome;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
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
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * ABM Adapter contra Kafka real con los casos de uso mockeados: se verifica el cableado del listener (lectura del
 * evento, MDC, clasificación de errores, tópicos de retry no bloqueantes, DLT y su handler). Los eventos se publican
 * a mano con los headers del relay. El flujo con base real está en AbmAdapterRetryIntegrationTest.
 */
@SpringBootTest(properties = {
        "nominations.outbox.relay.enabled=false",
        "nominations.abm-mock.enabled=false",
        "nominations.abm.response-consumer.enabled=false",
        "nominations.abm.sweeper.enabled=false",
        "nominations.abm.adapter.enabled=true",
        "nominations.abm.adapter.retry-delays=200ms,200ms"})
@Import(TestcontainersConfiguration.class)
class NominationRequestedListenerIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String DLT = KafkaTopics.NOMINATION_REQUESTED + "-dlt";
    private static final String RETRY_0 = KafkaTopics.NOMINATION_REQUESTED + "-retry-0";
    private static final String RETRY_1 = KafkaTopics.NOMINATION_REQUESTED + "-retry-1";

    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private ObjectMapper objectMapper;
    @MockitoBean
    private SubmitToAbmUseCase submitToAbm;
    @MockitoBean
    private MarkAbmFailureUseCase markAbmFailure;
    @Value("${nominations.kafka.partitions}")
    private int partitions;

    private DefaultKafkaProducerFactory<String, String> producerFactory;
    private KafkaTemplate<String, String> kafkaTemplate;
    /** Correlation id visto en el MDC durante cada llamada al caso de uso, por nomination_id. */
    private final Map<UUID, String> mdcDuringSubmit = new ConcurrentHashMap<>();
    /** Correlation id visto en el MDC durante cada markFailed (DLT handler), por nomination_id. */
    private final Map<UUID, String> mdcDuringMarkFailed = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        producerFactory = new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
        doAnswer(inv -> {
            mdcDuringSubmit.put(inv.getArgument(0), String.valueOf(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)));
            return SubmitOutcome.SUBMITTED;
        }).when(submitToAbm).submit(any());
        doAnswer(inv -> {
            mdcDuringMarkFailed.put(inv.getArgument(0), String.valueOf(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)));
            return true;
        }).when(markAbmFailure).markFailed(any(), any());
    }

    @AfterEach
    void tearDown() {
        producerFactory.destroy();
    }

    @Test
    @DisplayName("invoca SubmitToAbmUseCase con el nomination_id y el correlation id en el MDC")
    void submitsWithCorrelationIdInMdc() throws Exception {
        UUID nominationId = UUID.randomUUID();

        publish(nominationId, payload(nominationId, Map.of()), "1");

        await().atMost(TIMEOUT).untilAsserted(() -> verify(submitToAbm).submit(nominationId));
        assertThat(mdcDuringSubmit).containsEntry(nominationId, "it-" + nominationId);
    }

    @Test
    @DisplayName("evolución compatible: campo desconocido y schema_version mayor se procesan igual")
    void unknownFieldAndNewerSchemaVersion_processed() throws Exception {
        UUID nominationId = UUID.randomUUID();

        publish(nominationId, payload(nominationId,
                Map.of("schema_version", 2, "channel", "HOME_BANKING", "extra", Map.of("anidado", true))), "2");

        await().atMost(TIMEOUT).untilAsserted(() -> verify(submitToAbm).submit(nominationId));
    }

    @Test
    @DisplayName("AbmContractException → nomination.requested.v1-dlt sin reintentos y ABM_TIMEOUT por contrato")
    void contractError_goesToDltWithoutRetries() throws Exception {
        UUID nominationId = UUID.randomUUID();
        doAnswer(inv -> {
            throw new AbmContractException("ABM rechazó el pedido por contrato: HTTP 422");
        }).when(submitToAbm).submit(eq(nominationId));
        String json = payload(nominationId, Map.of());

        publish(nominationId, json, "1");

        ConsumerRecord<String, String> dead = awaitDlt(nominationId.toString());
        assertThat(dead.value()).isEqualTo(json);
        assertThat(header(dead, "kafka_exception-cause-fqcn")).isEqualTo(AbmContractException.class.getName());
        await().atMost(TIMEOUT).untilAsserted(() -> verify(markAbmFailure)
                .markFailed(nominationId, NominationRequestedDltHandler.CONTRACT_DETAIL));
        verify(submitToAbm, times(1)).submit(nominationId);
        assertThat(mdcDuringMarkFailed).containsEntry(nominationId, "it-" + nominationId);
    }

    @Test
    @DisplayName("AbmUnavailableException → -retry-0 → -retry-1 → DLT y ABM_TIMEOUT, con el correlation id en todos")
    void unavailable_retriedThroughRetryTopicsThenDlt() throws Exception {
        UUID nominationId = UUID.randomUUID();
        List<String> mdcPerAttempt = new CopyOnWriteArrayList<>();
        doAnswer(inv -> {
            mdcPerAttempt.add(String.valueOf(MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY)));
            throw new AbmUnavailableException("ABM respondió HTTP 503", null);
        }).when(submitToAbm).submit(eq(nominationId));
        String json = payload(nominationId, Map.of());

        publish(nominationId, json, "1");

        ConsumerRecord<String, String> dead = awaitDlt(nominationId.toString());
        assertThat(dead.value()).isEqualTo(json);
        assertThat(header(dead, "kafka_exception-cause-fqcn")).isEqualTo(AbmUnavailableException.class.getName());
        assertThat(header(dead, KafkaTopics.HEADER_EVENT_ID)).isNotBlank();
        assertThat(awaitRecord(RETRY_0, nominationId.toString()).value()).isEqualTo(json);
        assertThat(awaitRecord(RETRY_1, nominationId.toString()).value()).isEqualTo(json);
        await().atMost(TIMEOUT).untilAsserted(() -> verify(markAbmFailure)
                .markFailed(nominationId, NominationRequestedDltHandler.EXHAUSTED_DETAIL));
        // 1 intento + 2 reintentos (uno por valor de retry-delays).
        verify(submitToAbm, times(3)).submit(nominationId);
        assertThat(mdcPerAttempt).containsExactly("it-" + nominationId, "it-" + nominationId, "it-" + nominationId);
        assertThat(mdcDuringMarkFailed).containsEntry(nominationId, "it-" + nominationId);
    }

    @Test
    @DisplayName("transitorio que se recupera en el 2º intento: no llega al DLT ni marca falla")
    void unavailableOnce_recoversOnRetry() throws Exception {
        UUID nominationId = UUID.randomUUID();
        doAnswer(inv -> {
            throw new AbmUnavailableException("timeout", null);
        }).doReturn(SubmitOutcome.SUBMITTED).when(submitToAbm).submit(eq(nominationId));

        publish(nominationId, payload(nominationId, Map.of()), "1");

        await().atMost(TIMEOUT).untilAsserted(() -> verify(submitToAbm, times(2)).submit(nominationId));
        verify(markAbmFailure, never()).markFailed(eq(nominationId), any());
    }

    @Test
    @DisplayName("los tópicos de retry los crea Spring Kafka con las particiones configuradas")
    void retryTopicsCreatedWithConfiguredPartitions() throws Exception {
        List<String> topics = List.of(RETRY_0, RETRY_1, DLT);
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames()
                    .get(10, TimeUnit.SECONDS);
            assertThat(descriptions.values()).allSatisfy(d -> assertThat(d.partitions()).hasSize(partitions));
            assertThat(admin.listTopics().names().get(10, TimeUnit.SECONDS))
                    .doesNotContain(KafkaTopics.NOMINATION_REQUESTED + "-retry-2");
        }
    }

    @Test
    @DisplayName("poison pill: payload no parseable va directo al DLT sin llamar al caso de uso")
    void unparseablePayload_goesToDlt() throws Exception {
        String key = UUID.randomUUID().toString();
        String garbage = "{esto no es json";
        kafkaTemplate.send(new ProducerRecord<>(KafkaTopics.NOMINATION_REQUESTED, key, garbage))
                .get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> dead = awaitDlt(key);
        assertThat(dead.value()).isEqualTo(garbage);
        assertThat(header(dead, "kafka_exception-cause-fqcn")).isEqualTo(InvalidEventException.class.getName());
        verify(submitToAbm, times(0)).submit(UUID.fromString(key));
        verify(markAbmFailure, never()).markFailed(any(), any());
    }

    // ---------------------------------------------------------------- helpers

    /** Mismos headers y key que el relay del outbox. */
    private void publish(UUID nominationId, String json, String schemaVersion) throws Exception {
        var record = new ProducerRecord<>(KafkaTopics.NOMINATION_REQUESTED, nominationId.toString(), json);
        record.headers()
                .add(KafkaTopics.HEADER_EVENT_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_EVENT_TYPE, "nomination.requested".getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_SCHEMA_VERSION, schemaVersion.getBytes(StandardCharsets.UTF_8))
                .add(KafkaTopics.HEADER_CORRELATION_ID, ("it-" + nominationId).getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
    }

    private String payload(UUID nominationId, Map<String, Object> overrides) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("event_id", UUID.randomUUID().toString());
        body.put("event_type", "nomination.requested");
        body.put("schema_version", 1);
        body.put("nomination_id", nominationId.toString());
        body.put("entity_id", "0072");
        body.put("request_id", UUID.randomUUID().toString());
        body.put("customer_id", "CUST-000123");
        body.put("account_id", "0720000088000037654321");
        body.put("card_id", "tok_4f9a2c7b8d1e");
        body.put("alias", "CUENTA SUELDO");
        body.put("correlation_id", "payload-" + nominationId);
        body.put("occurred_at", "2026-10-09T12:00:00Z");
        body.putAll(overrides);
        return objectMapper.writeValueAsString(body);
    }

    private ConsumerRecord<String, String> awaitDlt(String key) {
        return awaitRecord(DLT, key);
    }

    private ConsumerRecord<String, String> awaitRecord(String topic, String key) {
        try (KafkaConsumer<String, String> dltConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            dltConsumer.subscribe(List.of(topic));
            List<ConsumerRecord<String, String>> found = new ArrayList<>();
            await().atMost(TIMEOUT).until(() -> {
                dltConsumer.poll(Duration.ofMillis(500)).forEach(r -> {
                    if (key.equals(r.key())) {
                        found.add(r);
                    }
                });
                return !found.isEmpty();
            });
            assertThat(found).hasSize(1);
            return found.getFirst();
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
