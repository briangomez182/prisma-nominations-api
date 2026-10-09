package com.prisma.nominations.abmmock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.abmmock.AbmMockEngine.Outcome;
import com.prisma.nominations.abmmock.AbmMockMessages.SubmitRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Motor del simulador sin Kafka: el publisher captura los mensajes. Demoras cortas para que el test sea rápido.
 * "No hay más publicaciones" se verifica esperando más que la última respuesta programada.
 */
class AbmMockEngineTest {

    private static final Duration RESPONSE_DELAY = Duration.ofMillis(50);
    private static final Duration DUPLICATE_GAP = Duration.ofMillis(50);
    private static final Duration SLOW_HTTP_DELAY = Duration.ofMillis(300);
    /** Más que la última respuesta posible (DUP: delay + gap). */
    private static final Duration QUIET = Duration.ofMillis(400);
    private static final Instant NOW = Instant.parse("2026-10-09T12:00:00Z");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<Published> published = new CopyOnWriteArrayList<>();
    private final AbmMockEngine engine = new AbmMockEngine(
            new AbmMockProperties(true, RESPONSE_DELAY, DUPLICATE_GAP, SLOW_HTTP_DELAY),
            (key, correlationId, json) -> published.add(new Published(key, correlationId, json)),
            objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

    record Published(String key, String correlationId, String json) {
    }

    @AfterEach
    void tearDown() {
        engine.close();
    }

    @Test
    @Tag("E4")
    @DisplayName("E4: card_id común → 202 y APPROVED con payload exacto en snake_case")
    void approves() throws Exception {
        SubmitRequest request = request("tok_demo_ok_01");

        Outcome outcome = engine.submit(request);

        assertThat(outcome).isInstanceOf(Outcome.Accepted.class);
        String opId = ((Outcome.Accepted) outcome).abmOperationId();
        assertThat(opId).startsWith("ABM-OP-");
        List<Published> messages = awaitMessages(1);
        Published message = messages.getFirst();
        assertThat(message.key()).isEqualTo(request.nominationId());
        assertThat(message.correlationId()).isEqualTo(request.correlationId());
        assertThat(objectMapper.readTree(message.json())).isEqualTo(objectMapper.readTree("""
                {"abm_operation_id":"%s","nomination_id":"%s","request_id":"%s","correlation_id":"%s",
                 "result":"APPROVED","reason_description":"Nominación aprobada","responded_at":"2026-10-09T12:00:00Z"}
                """.formatted(opId, request.nominationId(), request.requestId(), request.correlationId())));
    }

    @Test
    @Tag("E7")
    @DisplayName("Idempotencia: el mismo nomination_id → mismo abm_operation_id y una sola respuesta")
    void idempotentByNominationId() throws Exception {
        SubmitRequest request = request("tok_demo_ok_02");

        Outcome first = engine.submit(request);
        Outcome second = engine.submit(request);

        assertThat(second).isEqualTo(first);
        Thread.sleep(QUIET);
        assertThat(published).hasSize(1);
    }

    @ParameterizedTest(name = "{0} → {1}")
    @CsvSource({
            "tok_demo_REJECT_01,     ABM-051, Cuenta bloqueada",
            "tok_demo_reject_010,    ABM-010, Cuenta inexistente o inválida",
            "tok_demo_REJECT_020,    ABM-020, Tarjeta inexistente o inválida",
            "tok_demo_REJECT_030,    ABM-030, Tarjeta no habilitada para nominación",
            "tok_demo_REJECT_060,    ABM-060, La cuenta ya está nominada a la tarjeta"})
    @Tag("E5")
    @DisplayName("E5: REJECT → REJECTED con el código según el sufijo (ABM-051 por defecto)")
    void rejects(String cardId, String reasonCode, String reasonDescription) throws Exception {
        assertThat(engine.submit(request(cardId))).isInstanceOf(Outcome.Accepted.class);

        JsonNode json = objectMapper.readTree(awaitMessages(1).getFirst().json());
        assertThat(json.get("result").asText()).isEqualTo("REJECTED");
        assertThat(json.get("reason_code").asText()).isEqualTo(reasonCode);
        assertThat(json.get("reason_description").asText()).isEqualTo(reasonDescription);
    }

    @Test
    @Tag("E7")
    @DisplayName("E7: DUP → la misma respuesta APPROVED publicada dos veces")
    void duplicates() throws Exception {
        SubmitRequest request = request("tok_demo_DUP_01");

        engine.submit(request);

        List<Published> messages = awaitMessages(2);
        assertThat(messages.get(0)).isEqualTo(messages.get(1));
        assertThat(objectMapper.readTree(messages.get(0).json()).get("result").asText()).isEqualTo("APPROVED");
        Thread.sleep(QUIET);
        assertThat(published).hasSize(2);
    }

    @Test
    @Tag("E6")
    @DisplayName("E6: SILENT → 202 y nunca responde")
    void silent() throws Exception {
        assertThat(engine.submit(request("tok_demo_SILENT_01"))).isInstanceOf(Outcome.Accepted.class);

        Thread.sleep(QUIET);
        assertThat(published).isEmpty();
    }

    @Test
    @Tag("E6")
    @DisplayName("E6: FAIL → no disponible (503) en cada intento, sin registrar ni responder")
    void fails() throws Exception {
        SubmitRequest request = request("tok_demo_FAIL_01");

        assertThat(engine.submit(request)).isInstanceOf(Outcome.Unavailable.class);
        assertThat(engine.submit(request)).isInstanceOf(Outcome.Unavailable.class);

        Thread.sleep(QUIET);
        assertThat(published).isEmpty();
    }

    @Test
    @Tag("E6")
    @DisplayName("E6: SLOW → el alta tarda slow-http-delay y luego aprueba")
    void slow() {
        long start = System.nanoTime();

        Outcome outcome = engine.submit(request("tok_demo_SLOW_01"));

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(SLOW_HTTP_DELAY);
        assertThat(outcome).isInstanceOf(Outcome.Accepted.class);
        assertThat(awaitMessages(1)).hasSize(1);
    }

    @Test
    @DisplayName("Contrato inválido: faltan campos obligatorios → Invalid con la lista de campos")
    void invalid() {
        SubmitRequest request = new SubmitRequest(null, " ", "corr", "ENT01", "CUST", null, null, null);

        Outcome outcome = engine.submit(request);

        assertThat(outcome).isEqualTo(new Outcome.Invalid(List.of("nomination_id", "request_id", "account_id", "card_id")));
        assertThat(engine.submit(null)).isInstanceOf(Outcome.Invalid.class);
    }

    @Test
    @DisplayName("Precedencia: FAIL gana sobre REJECT; mayúsculas indistintas")
    void precedence() {
        assertThat(AbmMockScenario.of("tok_reject_fail")).isEqualTo(AbmMockScenario.FAIL);
        assertThat(AbmMockScenario.of("tok_dup_reject")).isEqualTo(AbmMockScenario.DUPLICATE);
        assertThat(AbmMockScenario.of("tok_Silent")).isEqualTo(AbmMockScenario.SILENT);
        assertThat(AbmMockScenario.of("tok_4f9a2c7b8d1e")).isEqualTo(AbmMockScenario.APPROVE);
    }

    @Test
    @DisplayName("toString del pedido no expone account_id completo")
    void masksAccount() {
        assertThat(request("tok").toString()).doesNotContain("0001234567890987654").contains("***************7654");
    }

    private List<Published> awaitMessages(int count) {
        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() >= count);
        return List.copyOf(published);
    }

    private static SubmitRequest request(String cardId) {
        return new SubmitRequest(UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                "corr-" + UUID.randomUUID(), "ENT01", "CUST-1", "0001234567890987654", cardId, "mi.alias");
    }
}
