package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.prisma.nominations.KafkaTopicProbe.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Simulador de ABM de punta a punta: POST HTTP real → respuesta en Kafka {@code abm.responses.v1}.
 * Adapter, consumer de respuestas y relay apagados: solo habla el "sistema ABM".
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nominations.abm-mock.enabled=true",
        "nominations.abm-mock.response-delay=200ms",
        "nominations.abm-mock.duplicate-gap=100ms",
        "nominations.abm-mock.slow-http-delay=3s",
        "nominations.abm.adapter.enabled=false",
        "nominations.abm.response-consumer.enabled=false",
        "nominations.outbox.relay.enabled=false"})
@Import(TestcontainersConfiguration.class)
@Tag("integration")
class AbmMockIntegrationTest {

    private static final String TOPIC = "abm.responses.v1";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @LocalServerPort
    private int port;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private ObjectMapper objectMapper;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    @Tag("E4")
    @DisplayName("E4: 202 con abm_operation_id y APPROVED en abm.responses.v1 con key, header y JSON correctos")
    void acceptsAndPublishesApproval() throws Exception {
        String nominationId = UUID.randomUUID().toString();
        String requestId = UUID.randomUUID().toString();
        String correlationId = "it-abm-mock-" + UUID.randomUUID();

        HttpResponse<String> response = post(body(nominationId, requestId, correlationId, "tok_demo_ok_01"),
                correlationId, Duration.ofSeconds(5));

        assertThat(response.statusCode()).isEqualTo(202);
        JsonNode accepted = objectMapper.readTree(response.body());
        String opId = accepted.get("abm_operation_id").asText();
        assertThat(opId).startsWith("ABM-OP-");
        assertThat(accepted.get("status").asText()).isEqualTo("ACCEPTED");

        ConsumerRecord<String, String> message = awaitRecords(nominationId, 1).getFirst();
        assertThat(message.key()).isEqualTo(nominationId);
        assertThat(header(message, "correlation_id")).isEqualTo(correlationId);
        JsonNode payload = objectMapper.readTree(message.value());
        assertThat(payload.get("abm_operation_id").asText()).isEqualTo(opId);
        assertThat(payload.get("nomination_id").asText()).isEqualTo(nominationId);
        assertThat(payload.get("request_id").asText()).isEqualTo(requestId);
        assertThat(payload.get("correlation_id").asText()).isEqualTo(correlationId);
        assertThat(payload.get("result").asText()).isEqualTo("APPROVED");
        assertThat(payload.has("reason_code")).isFalse();
        assertThat(Instant.parse(payload.get("responded_at").asText())).isNotNull();

        // Reenvío del mismo nomination_id: mismo op id y ninguna respuesta nueva.
        HttpResponse<String> replay = post(body(nominationId, requestId, correlationId, "tok_demo_ok_01"),
                correlationId, Duration.ofSeconds(5));
        assertThat(objectMapper.readTree(replay.body()).get("abm_operation_id").asText()).isEqualTo(opId);
        // Durante más que response-delay no aparece una segunda respuesta.
        await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(records(nominationId)).hasSize(1));
    }

    @Test
    @Tag("E5")
    @DisplayName("E5: REJECT_030 → REJECTED con reason_code ABM-030")
    void rejects() throws Exception {
        String nominationId = UUID.randomUUID().toString();

        post(body(nominationId, UUID.randomUUID().toString(), "it-rej", "tok_demo_REJECT_030"), "it-rej",
                Duration.ofSeconds(5));

        JsonNode payload = objectMapper.readTree(awaitRecords(nominationId, 1).getFirst().value());
        assertThat(payload.get("result").asText()).isEqualTo("REJECTED");
        assertThat(payload.get("reason_code").asText()).isEqualTo("ABM-030");
    }

    @Test
    @Tag("E7")
    @DisplayName("E7: DUP → dos mensajes idénticos")
    void duplicates() throws Exception {
        String nominationId = UUID.randomUUID().toString();

        post(body(nominationId, UUID.randomUUID().toString(), "it-dup", "tok_demo_DUP_01"), "it-dup",
                Duration.ofSeconds(5));

        List<ConsumerRecord<String, String>> messages = awaitRecords(nominationId, 2);
        assertThat(messages).hasSize(2);
        assertThat(messages.get(0).value()).isEqualTo(messages.get(1).value());
    }

    @Test
    @Tag("E6")
    @DisplayName("E6: FAIL → 503; contrato inválido → 400 (respuestas simples de ABM, no ProblemDetail)")
    void failAndInvalid() throws Exception {
        HttpResponse<String> unavailable = post(body(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                "it-fail", "tok_demo_FAIL_01"), "it-fail", Duration.ofSeconds(5));
        assertThat(unavailable.statusCode()).isEqualTo(503);
        assertThat(objectMapper.readTree(unavailable.body()).get("error").asText()).isEqualTo("SERVICE_UNAVAILABLE");

        HttpResponse<String> invalid = post("{\"nomination_id\":\"x\"}", "it-bad", Duration.ofSeconds(5));
        assertThat(invalid.statusCode()).isEqualTo(400);
        assertThat(objectMapper.readTree(invalid.body()).get("error").asText()).isEqualTo("INVALID_REQUEST");

        HttpResponse<String> malformed = post("{no es json", "it-bad", Duration.ofSeconds(5));
        assertThat(malformed.statusCode()).isEqualTo(400);
        assertThat(objectMapper.readTree(malformed.body()).get("error").asText()).isEqualTo("MALFORMED_REQUEST");
    }

    @Test
    @Tag("E6")
    @DisplayName("E6: SLOW → el HTTP no responde dentro de un read-timeout corto")
    void slowTimesOut() {
        String json = body(UUID.randomUUID().toString(), UUID.randomUUID().toString(), "it-slow", "tok_demo_SLOW_01");

        assertThatThrownBy(() -> post(json, "it-slow", Duration.ofSeconds(1)))
                .isInstanceOf(HttpTimeoutException.class);
    }

    @Test
    @DisplayName("El simulador no aparece en la documentación OpenAPI")
    void hiddenFromOpenApi() throws Exception {
        HttpResponse<String> spec = http.send(HttpRequest.newBuilder(URI.create(base() + "/v3/api-docs")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(spec.statusCode()).isEqualTo(200);
        assertThat(spec.body()).doesNotContain("abm-mock");
    }

    private HttpResponse<String> post(String json, String correlationId, Duration timeout) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(base() + "/abm-mock/v1/nominations"))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("X-Correlation-Id", correlationId)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private String base() {
        return "http://localhost:" + port;
    }

    private List<ConsumerRecord<String, String>> awaitRecords(String key, int count) {
        await().atMost(TIMEOUT).pollInterval(Duration.ofMillis(200)).until(() -> records(key).size() >= count);
        return records(key);
    }

    private List<ConsumerRecord<String, String>> records(String key) {
        try (var probe = new KafkaTopicProbe(kafka.getBootstrapServers(), TOPIC)) {
            return probe.recordsWithKey(key);
        }
    }

    private static String body(String nominationId, String requestId, String correlationId, String cardId) {
        return """
                {"nomination_id":"%s","request_id":"%s","correlation_id":"%s","entity_id":"ENT01",
                 "customer_id":"CUST-1","account_id":"0001234567890987654","card_id":"%s","alias":"mi.alias"}
                """.formatted(nominationId, requestId, correlationId, cardId);
    }
}
