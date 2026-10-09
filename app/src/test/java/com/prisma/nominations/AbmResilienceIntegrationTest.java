package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestClient;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.prisma.nominations.KafkaTopicProbe.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

/**
 * E6 - ABM no responde, de punta a punta y todo encendido: API → outbox → relay → ABM Adapter (capa 1: Retry +
 * CircuitBreaker + read-timeout; capa 2: tópicos de retry + DLT) → simulador HTTP real → abm.responses.v1 →
 * consumer de respuestas → nomination.result.v1. Más el sweeper del SLA y el reproceso por el endpoint interno.
 * <p>
 * <b>Tiempos comprimidos</b> (un solo contexto de Spring para todos los escenarios):
 * <ul>
 *   <li>capa 2: {@code retry-delays=500ms,500ms} → 3 pasadas (principal, -retry-0, -retry-1) y luego DLT;</li>
 *   <li>capa 1: 3 intentos por pasada, backoff de 100 ms (exponencial + jitter), {@code read-timeout=500ms};</li>
 *   <li>SLOW: el HTTP tarda {@value #SLOW_HTTP_DELAY_SECONDS} s, bastante más que el peor caso hasta ABM_TIMEOUT
 *       (3 × (3 × 0,5 s + backoff) + 2 × 0,5 s ≈ 7 s): la respuesta tardía llega siempre <i>después</i> del
 *       ABM_TIMEOUT, que es lo que se quiere demostrar;</li>
 *   <li>sweeper: SLA de 2 s, ciclo de 500 ms;</li>
 *   <li>circuit breaker: el del yml (ventana 20, mínimo 10 llamadas, 50 %) con espera en OPEN larga (2 min) y una
 *       sola llamada de prueba en HALF_OPEN. Se resetea antes y después de cada test: un FAIL (9 llamadas) no
 *       llega al mínimo y no lo abre, así cada escenario arranca con el CB cerrado y sin métricas previas.</li>
 * </ul>
 * {@link AbmRequestRecorder} registra, del lado del simulador, cada request que llegó (por nomination_id, con su
 * status HTTP y su abm_operation_id), sin tocar código de producción.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nominations.abm.adapter.retry-delays=500ms,500ms",
        "nominations.abm.read-timeout=500ms",
        "resilience4j.retry.instances.abm.wait-duration=100ms",
        "resilience4j.circuitbreaker.instances.abm.wait-duration-in-open-state=2m",
        "resilience4j.circuitbreaker.instances.abm.permitted-number-of-calls-in-half-open-state=1",
        "nominations.abm-mock.response-delay=200ms",
        "nominations.abm-mock.slow-http-delay=" + AbmResilienceIntegrationTest.SLOW_HTTP_DELAY_SECONDS + "s",
        "nominations.abm.sweeper.fixed-delay=500ms",
        "nominations.abm.sweeper.response-sla=2s"})
@Import({TestcontainersConfiguration.class, AbmResilienceIntegrationTest.AbmRequestRecorder.class})
@Tag("integration")
class AbmResilienceIntegrationTest {

    static final int SLOW_HTTP_DELAY_SECONDS = 12;

    private static final Duration TIMEOUT = Duration.ofSeconds(45);
    /** Varios ciclos del relay: si se publicara un segundo nomination.result, aparecería acá. */
    private static final Duration QUIET_PERIOD = Duration.ofMillis(1_500);
    private static final String ACCOUNT_ID = NominationEventFlowIntegrationTest.ACCOUNT_ID;
    private static final String RETRY_0 = KafkaTopics.NOMINATION_REQUESTED + "-retry-0";
    private static final String RETRY_1 = KafkaTopics.NOMINATION_REQUESTED + "-retry-1";
    private static final String DLT = KafkaTopics.NOMINATION_REQUESTED + "-dlt";
    private static final String EXHAUSTED_DETAIL = "Reintentos agotados: ABM no disponible";
    /** 3 pasadas de la capa 2 (principal + 2 tópicos de retry) × 3 intentos de la capa 1. */
    private static final int ATTEMPTS_UNTIL_DLT = 3 * 3;
    private static final AtomicBoolean WARMED_UP = new AtomicBoolean();

    @LocalServerPort
    private int port;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private AbmRequestRecorder abm;
    @Autowired
    private CircuitBreakerRegistry circuitBreakers;

    /**
     * Una vez por contexto, una vuelta completa por el simulador (SILENT: sin respuesta). La primera llamada HTTP de
     * un contexto nuevo (JIT, serializadores) puede acercarse al read-timeout corto del test y disparar un
     * reintento que ensuciaría los conteos exactos. Después se resetea el CB.
     */
    @BeforeEach
    void warmUpAndResetCircuitBreaker() throws Exception {
        if (WARMED_UP.compareAndSet(false, true)) {
            Created warmUp = post("tok_warmup_SILENT_00");
            awaitStatus(warmUp, "PENDING_ABM");
        }
        circuitBreaker().reset();
    }

    @AfterEach
    void resetCircuitBreaker() {
        circuitBreaker().reset();
    }

    @Nested
    @DisplayName("E6-FAIL - ABM caído (503): capa 1 × capa 2, DLT y ABM_TIMEOUT sin resultado")
    @Tag("E6")
    class E6Fail {

        @Test
        void exhaustsBothRetryLayersAndEndsInAbmTimeout() throws Exception {
            Created created = post("tok_demo_FAIL_01");

            JsonNode nomination = awaitStatus(created, "ABM_TIMEOUT");
            assertThat(nomination.has("rejection_reason")).isFalse();

            // ABM recibió 1 + 2 reintentos de la capa 1 en cada una de las 3 pasadas de la capa 2: 9 requests, todas
            // 503. 9 < minimum-number-of-calls (10): el CB no llegó a abrir y no cortó ningún intento.
            assertThat(abm.calls(created.nominationId())).hasSize(ATTEMPTS_UNTIL_DLT)
                    .allSatisfy(call -> assertThat(call.status()).isEqualTo(503));
            assertThat(circuitBreaker().getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            assertThat(history(created.nominationId())).containsExactly(
                    List.of("RECEIVED", "API"),
                    List.of("ABM_TIMEOUT", "ABM_ADAPTER"));
            assertThat(lastDetail(created.nominationId())).isEqualTo(EXHAUSTED_DETAIL);
            assertThat(resultEvents(created.nominationId())).isZero();

            try (var retry0 = probe(RETRY_0); var retry1 = probe(RETRY_1); var dlt = probe(DLT)) {
                assertThat(retry0.recordsWithKey(created.nominationId())).hasSize(1);
                assertThat(retry1.recordsWithKey(created.nominationId())).hasSize(1);
                // Headers de diagnóstico del DLT de este tópico: kafka_exception-* / kafka_original-* (ver docs/events.md).
                assertThat(dlt.recordsWithKey(created.nominationId())).singleElement().satisfies(dead -> {
                    assertThat(header(dead, "kafka_exception-cause-fqcn"))
                            .isEqualTo(AbmUnavailableException.class.getName());
                    // original-topic conserva el tópico de origen (no el último de retry): sirve para republicar.
                    assertThat(header(dead, "kafka_original-topic")).isEqualTo(KafkaTopics.NOMINATION_REQUESTED);
                    assertThat(header(dead, KafkaTopics.HEADER_CORRELATION_ID)).isEqualTo(created.correlationId());
                });
            }
        }
    }

    @Nested
    @DisplayName("E6-SLOW - read-timeout, reintentos agotados y respuesta tardía: ABM_TIMEOUT → APPROVED")
    @Tag("E6")
    class E6Slow {

        /**
         * El simulador tarda {@value #SLOW_HTTP_DELAY_SECONDS} s en contestar: el cliente corta cada intento a los
         * 500 ms, se agotan las dos capas y la nominación pasa a ABM_TIMEOUT. Pero ABM sí recibió el pedido: al
         * terminar su demora lo registra (una sola operación, es idempotente) y publica APPROVED.
         * <p>
         * Esto demuestra que ABM_TIMEOUT <b>no es final</b>: es "no sabemos", no "falló". La respuesta tardía se
         * aplica (ABM_TIMEOUT → APPROVED) y recién ahí se publica el resultado. Como ABM_TIMEOUT no había publicado
         * nada, el consumidor ve <b>un único</b> nomination.result, con el resultado real.
         */
        @Test
        void lateApprovalAfterTimeoutPublishesASingleResult() throws Exception {
            Created created = post("tok_demo_SLOW_01");

            awaitStatus(created, "ABM_TIMEOUT");
            assertThat(lastDetail(created.nominationId())).isEqualTo(EXHAUSTED_DETAIL);
            assertThat(resultEvents(created.nominationId())).isZero();

            JsonNode nomination = awaitStatus(created, "APPROVED");
            assertThat(nomination.has("rejection_reason")).isFalse();
            assertThat(history(created.nominationId())).containsExactly(
                    List.of("RECEIVED", "API"),
                    List.of("ABM_TIMEOUT", "ABM_ADAPTER"),
                    List.of("APPROVED", "ABM_RESPONSE"));

            ConsumerRecord<String, String> result = singleResultMessage(created.nominationId());
            assertThat(objectMapper.readTree(result.value()).get("status").asText()).isEqualTo("APPROVED");
            assertThat(resultEvents(created.nominationId())).isEqualTo(1);

            // Los 9 intentos llegaron a ABM (cada uno termina recién cuando vence la demora del simulador), pero todos
            // corresponden a una única operación: ABM es idempotente por nomination_id.
            await().atMost(TIMEOUT).until(() -> abm.calls(created.nominationId()).size(), equalTo(ATTEMPTS_UNTIL_DLT));
            List<AbmCall> calls = abm.calls(created.nominationId());
            assertThat(calls).allSatisfy(call -> assertThat(call.status()).isEqualTo(202));
            assertThat(calls).extracting(AbmCall::abmOperationId).doesNotContainNull().containsOnly(
                    calls.getFirst().abmOperationId());
            try (var responses = probe(KafkaTopics.ABM_RESPONSES)) {
                assertThat(responses.recordsWithKey(created.nominationId())).hasSize(1);
            }
        }
    }

    @Nested
    @DisplayName("E6-SILENT - sin respuesta: sweeper → ABM_TIMEOUT y reproceso por el endpoint interno")
    @Tag("E6")
    class E6Silent {

        /**
         * ABM acepta (202) y nunca responde. Pasado el SLA el sweeper la vence (source SWEEPER). El operador la
         * reprocesa: RECEIVED + nuevo nomination.requested → el adapter la reenvía → ABM, idempotente, devuelve el
         * <b>mismo</b> abm_operation_id → PENDING_ABM.
         * <p>
         * En la vida real el reproceso sirve para reenviar cuando el pedido no llegó, o para forzar que ABM vuelva
         * a mirar una operación trabada; el simulador SILENT, en cambio, seguirá sin responder (y el sweeper la
         * volverá a vencer: el caso se escala a ABM).
         */
        @Test
        void sweeperTimesOutAndOperatorReprocesses() throws Exception {
            Created created = post("tok_demo_SILENT_01");

            awaitStatus(created, "PENDING_ABM");
            awaitStatus(created, "ABM_TIMEOUT");
            assertThat(history(created.nominationId()).getLast()).isEqualTo(List.of("ABM_TIMEOUT", "SWEEPER"));
            assertThat(lastDetail(created.nominationId())).startsWith("Sin respuesta de ABM dentro del SLA");
            assertThat(resultEvents(created.nominationId())).isZero();

            String operatorCorrelationId = "it-reprocess-" + UUID.randomUUID();
            ResponseEntity<String> reprocess = http().post()
                    .uri("/internal/v1/nominations/{id}/reprocess", created.nominationId())
                    .header(HttpHeaders.AUTHORIZATION, TestTokens.operatorBearer())
                    .header(ApiHeaders.CORRELATION_ID, operatorCorrelationId)
                    .retrieve()
                    .toEntity(String.class);
            assertThat(reprocess.getStatusCode().value()).isEqualTo(202);
            assertThat(reprocess.getHeaders().getFirst(ApiHeaders.CORRELATION_ID)).isEqualTo(operatorCorrelationId);
            assertThat(reprocess.getHeaders().getLocation())
                    .hasToString("/v1/nominations/" + created.nominationId());
            assertThat(objectMapper.readTree(reprocess.getBody()).get("status").asText()).isEqualTo("RECEIVED");

            // Vuelve a PENDING_ABM (el sweeper la vencería de nuevo 2 s después: se mira solo el tramo esperado).
            awaitStatus(created, "PENDING_ABM");
            assertThat(history(created.nominationId()).subList(0, 5)).containsExactly(
                    List.of("RECEIVED", "API"),
                    List.of("PENDING_ABM", "ABM_ADAPTER"),
                    List.of("ABM_TIMEOUT", "SWEEPER"),
                    List.of("RECEIVED", "OPERATOR"),
                    List.of("PENDING_ABM", "ABM_ADAPTER"));
            assertThat(jdbc.queryForObject("""
                    SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.requested'""",
                    Integer.class, created.nominationId())).isEqualTo(2);

            List<AbmCall> calls = abm.calls(created.nominationId());
            assertThat(calls).hasSize(2).allSatisfy(call -> assertThat(call.status()).isEqualTo(202));
            assertThat(calls.get(1).abmOperationId()).isNotNull().isEqualTo(calls.get(0).abmOperationId());
            assertThat(resultEvents(created.nominationId())).isZero();
        }
    }

    @Nested
    @DisplayName("Circuit breaker - abre con fallas reales, corta sin llamar a ABM y se recupera en HALF_OPEN")
    @Tag("E6")
    class CircuitBreakerOpensAndRecovers {

        /**
         * Abrirlo es E2E (fallas reales contra el simulador); la vuelta a HALF_OPEN es manual. Para que una nominación
         * sana pruebe el fail fast, todas sus pasadas tienen que caer con el CB abierto: con esperas de capa 2 de
         * 500 ms y la apertura dependiendo del relay y de los tópicos, dejar que el CB pase solo a HALF_OPEN
         * a mitad de camino sería una carrera. Por eso la espera en OPEN es larga (2 min) y la transición se hace con
         * {@code transitionToHalfOpenState()}: es exactamente lo que hace la transición automática al vencer la
         * espera.
         * <p>
         * En producción la secuencia es la misma sin operador: el CB queda abierto 30 s y la capa 2 reintenta a los
         * 10 s, 1 min y 5 min; el reintento de 1 min ya lo encuentra en HALF_OPEN/CLOSED y la nominación se aprueba.
         * Acá, con la capa 2 agotada en ~1 s, la recuperación es el reproceso.
         */
        @Test
        void opensOnRealFailuresFailsFastAndRecovers() throws Exception {
            // 1) Un FAIL completo: 9 fallas, por debajo del mínimo de 10 llamadas → sigue CLOSED.
            Created first = post("tok_demo_FAIL_cb_01");
            awaitStatus(first, "ABM_TIMEOUT");
            assertThat(abm.calls(first.nominationId())).hasSize(ATTEMPTS_UNTIL_DLT);
            assertThat(circuitBreakerHealth().path("state").asText()).isEqualTo("CLOSED");

            // 2) Segundo FAIL: su primer intento es la llamada nº 10 con 100 % de fallas → OPEN. El resto de sus
            //    intentos ya no llega a ABM.
            Created second = post("tok_demo_FAIL_cb_02");
            await().atMost(TIMEOUT).until(() -> circuitBreakerHealth().path("state").asText(), equalTo("OPEN"));
            JsonNode health = circuitBreakerHealth();
            assertThat(health.path("failureRate").asText()).isEqualTo("100.0%");
            awaitStatus(second, "ABM_TIMEOUT");
            assertThat(abm.calls(second.nominationId())).hasSize(1);

            // 3) Con el CB abierto, una nominación sana no llega a ABM (fail fast): recorre retry-0, retry-1 y DLT.
            Created healthy = post("tok_demo_ok_cb_01");
            awaitStatus(healthy, "ABM_TIMEOUT");
            assertThat(abm.calls(healthy.nominationId())).isEmpty();
            try (var retry0 = probe(RETRY_0); var retry1 = probe(RETRY_1); var dlt = probe(DLT)) {
                assertThat(retry0.recordsWithKey(healthy.nominationId())).hasSize(1);
                assertThat(retry1.recordsWithKey(healthy.nominationId())).hasSize(1);
                assertThat(dlt.recordsWithKey(healthy.nominationId())).singleElement()
                        .satisfies(dead -> assertThat(header(dead, "kafka_exception-cause-fqcn"))
                                .isEqualTo(AbmUnavailableException.class.getName()));
            }
            assertThat(circuitBreaker().getMetrics().getNumberOfNotPermittedCalls()).isGreaterThanOrEqualTo(3);

            // 4) Vence la espera en OPEN → HALF_OPEN. El operador reprocesa: la llamada de prueba pasa, el CB se
            //    cierra y la nominación se aprueba.
            circuitBreaker().transitionToHalfOpenState();
            assertThat(circuitBreakerHealth().path("state").asText()).isEqualTo("HALF_OPEN");
            ResponseEntity<String> reprocess = http().post()
                    .uri("/internal/v1/nominations/{id}/reprocess", healthy.nominationId())
                    .header(HttpHeaders.AUTHORIZATION, TestTokens.operatorBearer())
                    .retrieve()
                    .toEntity(String.class);
            assertThat(reprocess.getStatusCode().value()).isEqualTo(202);

            awaitStatus(healthy, "APPROVED");
            assertThat(circuitBreakerHealth().path("state").asText()).isEqualTo("CLOSED");
            assertThat(abm.calls(healthy.nominationId())).singleElement()
                    .satisfies(call -> assertThat(call.status()).isEqualTo(202));
            assertThat(statuses(healthy.nominationId()))
                    .startsWith("RECEIVED", "ABM_TIMEOUT", "RECEIVED")
                    .endsWith("APPROVED");
            singleResultMessage(healthy.nominationId());
        }
    }

    @Nested
    @DisplayName("Rechazo funcional vs falla técnica: REJECTED sin reintentos ni tópicos de retry")
    @Tag("E5")
    @Tag("E6")
    class FunctionalRejectionIsNotRetried {

        /**
         * Un REJECTED es una respuesta válida de ABM (llega por abm.responses.v1), no un error del envío: el HTTP fue
         * 202 al primer intento. No hay nada que reintentar y el CB lo cuenta como llamada exitosa.
         */
        @Test
        void rejectionIsAppliedWithASingleRequestToAbm() throws Exception {
            Created created = post("tok_demo_REJECT_020");

            JsonNode nomination = awaitStatus(created, "REJECTED");
            assertThat(nomination.get("rejection_reason").asText()).isEqualTo("INVALID_CARD");

            assertThat(abm.calls(created.nominationId())).singleElement()
                    .satisfies(call -> assertThat(call.status()).isEqualTo(202));
            try (var retry0 = probe(RETRY_0); var dlt = probe(DLT)) {
                assertThat(retry0.recordsWithKey(created.nominationId())).isEmpty();
                assertThat(dlt.recordsWithKey(created.nominationId())).isEmpty();
            }
            assertThat(circuitBreaker().getMetrics().getNumberOfFailedCalls()).isZero();
            assertThat(statuses(created.nominationId())).doesNotContain("ABM_TIMEOUT");
            singleResultMessage(created.nominationId());
        }
    }

    // ---------------------------------------------------------------- soporte

    record Created(UUID nominationId, String entity, String correlationId) {
    }

    private Created post(String cardId) throws Exception {
        String entity = NominationEventFlowIntegrationTest.newEntity();
        String correlationId = "it-resilience-" + UUID.randomUUID();
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
                .header(ApiHeaders.CORRELATION_ID, correlationId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .toEntity(String.class);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        UUID nominationId = UUID.fromString(objectMapper.readTree(response.getBody()).get("nomination_id").asText());
        return new Created(nominationId, entity, correlationId);
    }

    private RestClient http() {
        return RestClient.create("http://localhost:" + port);
    }

    private JsonNode nomination(Created created) throws Exception {
        return objectMapper.readTree(http().get().uri("/v1/nominations/" + created.nominationId())
                .header(HttpHeaders.AUTHORIZATION, TestTokens.bearer(created.entity())).retrieve().body(String.class));
    }

    /** Espera el estado por la API pública (GET) y devuelve la última representación leída. */
    private JsonNode awaitStatus(Created created, String expected) throws Exception {
        await().atMost(TIMEOUT).until(() -> nomination(created).get("status").asText(), equalTo(expected));
        return nomination(created);
    }

    /** [to_status, source] de cada transición, en orden. */
    private List<List<String>> history(UUID nominationId) {
        return jdbc.query("SELECT to_status, source FROM nomination_history WHERE nomination_id = ? ORDER BY id",
                (rs, n) -> List.of(rs.getString(1), rs.getString(2)), nominationId);
    }

    private List<String> statuses(UUID nominationId) {
        return history(nominationId).stream().map(List::getFirst).toList();
    }

    private String lastDetail(UUID nominationId) {
        return jdbc.queryForObject("""
                SELECT detail FROM nomination_history WHERE nomination_id = ? ORDER BY id DESC LIMIT 1""",
                String.class, nominationId);
    }

    private int resultEvents(UUID nominationId) {
        return jdbc.queryForObject("""
                SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.result'""",
                Integer.class, nominationId);
    }

    /** Exactamente un mensaje en nomination.result.v1, sostenido durante varios ciclos del relay. */
    private ConsumerRecord<String, String> singleResultMessage(UUID nominationId) {
        try (var probe = probe(KafkaTopics.NOMINATION_RESULT)) {
            await().pollInSameThread().during(QUIET_PERIOD).atMost(TIMEOUT)
                    .until(() -> probe.recordsWithKey(nominationId).size(), equalTo(1));
            return probe.recordsWithKey(nominationId).getFirst();
        }
    }

    private CircuitBreaker circuitBreaker() {
        return circuitBreakers.circuitBreaker("abm");
    }

    /** components.circuitBreakers.details.abm.details de /actuator/health (el detalle requiere token de operador). */
    private JsonNode circuitBreakerHealth() throws Exception {
        String body = http().get().uri("/actuator/health")
                .header(HttpHeaders.AUTHORIZATION, TestTokens.operatorBearer()).retrieve().body(String.class);
        return objectMapper.readTree(body).at("/components/circuitBreakers/details/abm/details");
    }

    private KafkaTopicProbe probe(String topic) {
        return new KafkaTopicProbe(kafka.getBootstrapServers(), topic);
    }

    /** Un request recibido por el simulador: status HTTP y abm_operation_id (null si no fue un 202). */
    record AbmCall(int status, String abmOperationId) {
    }

    /**
     * Registro de lo que recibió el simulador de ABM: un filtro de servlet sobre {@code /abm-mock/*} que guarda, por
     * nomination_id, el status y el abm_operation_id de cada POST. Mide en el borde HTTP del "sistema ABM".
     * <p>
     * Se registra al terminar el request: en SLOW eso es cuando vence la demora del simulador, aunque el cliente ya
     * haya cortado por read-timeout (lo registrado es lo que ABM contestó, le haya llegado o no al cliente).
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class AbmRequestRecorder {

        private final Map<UUID, List<AbmCall>> received = new ConcurrentHashMap<>();
        private final ObjectMapper reader = new ObjectMapper();

        List<AbmCall> calls(UUID nominationId) {
            return List.copyOf(received.getOrDefault(nominationId, List.of()));
        }

        @Bean
        FilterRegistrationBean<OncePerRequestFilter> abmResilienceRecorderFilter() {
            var registration = new FilterRegistrationBean<OncePerRequestFilter>(new OncePerRequestFilter() {
                @Override
                protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                                FilterChain chain) throws ServletException, IOException {
                    var cachedRequest = new ContentCachingRequestWrapper(request);
                    var cachedResponse = new ContentCachingResponseWrapper(response);
                    try {
                        chain.doFilter(cachedRequest, cachedResponse);
                    } finally {
                        record(cachedRequest.getContentAsByteArray(), cachedResponse.getStatus(),
                                operationId(cachedResponse.getContentAsByteArray()));
                        try {
                            cachedResponse.copyBodyToResponse();
                        } catch (IOException clientGone) {
                            // El cliente ya cortó por read-timeout (SLOW): la respuesta no le llega.
                        }
                    }
                }
            });
            registration.addUrlPatterns("/abm-mock/*");
            return registration;
        }

        private String operationId(byte[] body) {
            try {
                JsonNode id = reader.readTree(body).get("abm_operation_id");
                return id == null ? null : id.asText();
            } catch (IOException e) {
                return null;
            }
        }

        private void record(byte[] body, int status, String operationId) {
            try {
                JsonNode id = reader.readTree(body).get("nomination_id");
                if (id != null) {
                    received.computeIfAbsent(UUID.fromString(id.asText()), k -> new CopyOnWriteArrayList<>())
                            .add(new AbmCall(status, operationId));
                }
            } catch (IOException | IllegalArgumentException e) {
                // Cuerpo inválido: no es un alta que cuente.
            }
        }
    }
}
