package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.out.AbmClient;
import com.prisma.nominations.application.port.out.AbmRequest;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.adapter.out.messaging.OutboxRelay;
import com.prisma.nominations.infrastructure.config.KafkaConsumerConfig;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.logging.logback.StructuredLogEncoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Trazas distribuidas (W3C traceparent) y logs JSON, de punta a punta y sin HTTP a la API: la nominación se crea con
 * el caso de uso dentro de un span que hace de "request del canal".
 * <ul>
 *   <li>append del outbox → guarda el traceparent del span activo;</li>
 *   <li>relay (invocado a mano, en otro momento y sin span) → publica con traceparent de la MISMA traza;</li>
 *   <li>consumer (container factory propia con observation) → su span y su MDC tienen el mismo trace id;</li>
 *   <li>HTTP saliente a ABM → el RestClient propaga traceparent;</li>
 *   <li>perfil json-logs → cada línea es JSON (ECS) con correlationId, traceId y spanId del MDC;</li>
 *   <li>factories propias → métricas del cliente Kafka en el MeterRegistry.</li>
 * </ul>
 * {@link AutoConfigureObservability}: en tests Boot apaga el tracing salvo que se pida; el export OTLP queda apagado.
 */
@SpringBootTest(properties = {
        "nominations.abm.adapter.enabled=false",
        "nominations.abm-mock.enabled=false",
        "nominations.abm.sweeper.enabled=false",
        "nominations.outbox.relay.enabled=false",
        "management.otlp.tracing.export.enabled=false"})
@AutoConfigureObservability
@ActiveProfiles("json-logs")
@Import({TestcontainersConfiguration.class, DistributedTracingIntegrationTest.ProbeConfig.class})
@Tag("integration")
class DistributedTracingIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(DistributedTracingIntegrationTest.class);
    private static final Pattern TRACEPARENT = Pattern.compile("00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /** ABM falso: registra los headers del POST y responde 202 con abm_operation_id. */
    private static final HttpServer FAKE_ABM = startFakeAbm();
    private static final Map<String, String> ABM_HEADERS = new ConcurrentHashMap<>();

    @Autowired
    private CreateNominationUseCase createNomination;

    @Autowired
    private OutboxRelay relay;

    @Autowired
    private Tracer tracer;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private AbmClient abmClient;

    @Autowired
    private TraceProbe probe;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private Environment environment;

    @DynamicPropertySource
    static void abmBaseUrl(DynamicPropertyRegistry registry) {
        registry.add("nominations.abm.base-url", () -> "http://localhost:" + FAKE_ABM.getAddress().getPort());
    }

    @AfterAll
    static void stopFakeAbm() {
        FAKE_ABM.stop(0);
    }

    @Test
    @Tag("E1")
    void outboxGuardaElTraceparentYElConsumerContinuaLaMismaTraza() throws Exception {
        String correlationId = "it-trace-" + UUID.randomUUID();
        var canal = inSpan(correlationId, () -> createNomination.create(command(correlationId)));
        UUID nominationId = canal.value().nomination().id();
        String traceId = canal.traceId();

        // 1. Outbox: el JSON de headers tiene el traceparent del span activo al hacer append.
        JsonNode headers = objectMapper.readTree(jdbc.queryForObject(
                "SELECT headers::text FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.requested'",
                String.class, nominationId));
        assertThat(headers.path(KafkaTopics.HEADER_CORRELATION_ID).asText()).isEqualTo(correlationId);
        String stored = headers.path(KafkaTopics.HEADER_TRACEPARENT).asText();
        assertThat(stored).matches(TRACEPARENT);
        assertThat(traceIdOf(stored)).isEqualTo(traceId);

        // 2. Relay: otro momento, sin span activo en el hilo; publica dentro de la traza guardada.
        assertThat(tracer.currentSpan()).isNull();
        assertThat(relay.relayOnce()).isPositive();

        // 3. Consumer: header traceparent, span del listener y MDC con el mismo trace id.
        await().atMost(TIMEOUT).until(() -> probe.received.containsKey(nominationId.toString()));
        TraceProbe.Received received = probe.received.get(nominationId.toString());
        assertThat(received.traceparent()).matches(TRACEPARENT);
        assertThat(traceIdOf(received.traceparent())).isEqualTo(traceId);
        // El span del mensaje es el PRODUCER del relay, no el del canal (hijo, misma traza).
        assertThat(spanIdOf(received.traceparent())).isNotEqualTo(spanIdOf(stored));
        assertThat(received.consumerTraceId()).isEqualTo(traceId);
        assertThat(received.mdcTraceId()).isEqualTo(traceId);
        assertThat(received.correlationId()).isEqualTo(correlationId);
    }

    @Test
    @Tag("E1")
    void sinSpanActivoElOutboxNoGuardaTraceparent() throws Exception {
        String correlationId = "it-notrace-" + UUID.randomUUID();
        assertThat(tracer.currentSpan()).isNull();
        UUID nominationId = createNomination.create(command(correlationId)).nomination().id();

        JsonNode headers = objectMapper.readTree(jdbc.queryForObject(
                "SELECT headers::text FROM outbox_events WHERE aggregate_id = ?", String.class, nominationId));
        assertThat(headers.has(KafkaTopics.HEADER_TRACEPARENT)).isFalse();
        assertThat(headers.path(KafkaTopics.HEADER_CORRELATION_ID).asText()).isEqualTo(correlationId);
    }

    @Test
    @Tag("E1")
    void elClienteHttpDeAbmPropagaElTraceparent() {
        String correlationId = "it-abm-" + UUID.randomUUID();
        var request = new AbmRequest(UUID.randomUUID(), UUID.randomUUID(), correlationId, "ENT01", "123456",
                NominationEventFlowIntegrationTest.ACCOUNT_ID, "tok_trace_01", "CUENTA SUELDO");

        var call = inSpan(correlationId, () -> abmClient.submit(request));

        assertThat(call.value()).isEqualTo("ABM-OP-TRACE");
        assertThat(ABM_HEADERS.get("traceparent")).matches(TRACEPARENT);
        assertThat(traceIdOf(ABM_HEADERS.get("traceparent"))).isEqualTo(call.traceId());
        assertThat(ABM_HEADERS.get("x-correlation-id")).isEqualTo(correlationId);
    }

    /**
     * El perfil json-logs activa ECS en consola. Logback se inicializa una sola vez por JVM (el primer contexto de la
     * corrida de tests manda), así que la consola no sirve para verificarlo de forma determinista: se formatea con el
     * mismo encoder que usa Boot ({@link StructuredLogEncoder}) y el formato que resolvió el Environment del perfil.
     */
    @Test
    @Tag("E1")
    void conElPerfilJsonLogsCadaLineaEsJsonConCorrelationIdYTraceId() throws Exception {
        String format = environment.getProperty("logging.structured.format.console");
        assertThat(format).isEqualTo("ecs");

        String correlationId = "it-json-" + UUID.randomUUID();
        String marker = "probe-json-" + UUID.randomUUID();
        var out = new ByteArrayOutputStream();
        var context = (LoggerContext) LoggerFactory.getILoggerFactory();
        Object previousEnvironment = context.getObject(Environment.class.getName());
        context.putObject(Environment.class.getName(), environment);
        var encoder = new StructuredLogEncoder();
        encoder.setContext(context);
        encoder.setFormat(format);
        encoder.start();
        var appender = new OutputStreamAppender<ILoggingEvent>();
        appender.setName("json-logs-probe");
        appender.setContext(context);
        appender.setEncoder(encoder);
        appender.setOutputStream(out);
        appender.start();
        var logger = context.getLogger(DistributedTracingIntegrationTest.class);
        logger.addAppender(appender);
        Traced<String> logged;
        try {
            logged = inSpan(correlationId, () -> {
                log.info("Línea de prueba {}", marker);
                return tracer.currentSpan().context().spanId();
            });
        } finally {
            logger.detachAppender(appender);
            appender.stop();
            context.putObject(Environment.class.getName(), previousEnvironment);
        }

        String line = out.toString(StandardCharsets.UTF_8).lines().filter(l -> l.contains(marker))
                .findFirst().orElseThrow();
        JsonNode json = objectMapper.readTree(line);
        assertThat(json.path("message").asText()).contains(marker);
        assertThat(json.path("log").path("level").asText()).isEqualTo("INFO"); // ECS anida log.level
        assertThat(json.path("service").path("name").asText()).isEqualTo("prisma-nominations-api");
        assertThat(json.path(ApiHeaders.CORRELATION_ID_MDC_KEY).asText()).isEqualTo(correlationId);
        assertThat(json.path("traceId").asText()).isEqualTo(logged.traceId());
        assertThat(json.path("spanId").asText()).isEqualTo(logged.value());
    }

    @Test
    void lasFactoriesPropiasExponenMetricasDelClienteKafka() {
        // Consumers de las factories propias (notificaciones, respuestas de ABM): lag y fetch en /actuator/prometheus.
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(
                meterRegistry.find("kafka.consumer.fetch.manager.records.lag.max").meters())
                .anySatisfy(m -> assertThat(m.getId().getTag("client.id")).contains("notifications-demo"))
                .anySatisfy(m -> assertThat(m.getId().getTag("client.id")).contains("abm-response-processor")));
    }

    // --- soporte ---

    private record Traced<T>(T value, String traceId) {
    }

    /** Simula el request del canal: span raíz + correlation id en el MDC, como el filtro HTTP. */
    private <T> Traced<T> inSpan(String correlationId, Supplier<T> body) {
        Span span = tracer.nextSpan().name("canal").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            MDC.put(ApiHeaders.CORRELATION_ID_MDC_KEY, correlationId);
            return new Traced<>(body.get(), span.context().traceId());
        } finally {
            MDC.remove(ApiHeaders.CORRELATION_ID_MDC_KEY);
            span.end();
        }
    }

    private static CreateNominationCommand command(String correlationId) {
        return new CreateNominationCommand("ENT-TRACE", UUID.randomUUID(), "123456",
                NominationEventFlowIntegrationTest.ACCOUNT_ID, "tok_trace_01", "CUENTA SUELDO", correlationId);
    }

    private static String traceIdOf(String traceparent) {
        var m = TRACEPARENT.matcher(traceparent);
        assertThat(m.matches()).isTrue();
        return m.group(1);
    }

    private static String spanIdOf(String traceparent) {
        var m = TRACEPARENT.matcher(traceparent);
        assertThat(m.matches()).isTrue();
        return m.group(2);
    }

    private static HttpServer startFakeAbm() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.createContext("/v1/nominations", exchange -> {
                exchange.getRequestHeaders().forEach((name, values) ->
                        ABM_HEADERS.put(name.toLowerCase(), values.getFirst()));
                exchange.getRequestBody().readAllBytes();
                byte[] body = "{\"abm_operation_id\":\"ABM-OP-TRACE\",\"status\":\"RECEIVED\"}"
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(202, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ProbeConfig {
        @Bean
        TraceProbe traceProbe(Tracer tracer) {
            return new TraceProbe(tracer);
        }
    }

    /** Consumer de prueba sobre la container factory propia de notificaciones (observation habilitada). */
    static class TraceProbe {

        record Received(String traceparent, String consumerTraceId, String mdcTraceId, String correlationId) {
        }

        final Map<String, Received> received = new ConcurrentHashMap<>();
        private final Tracer tracer;

        TraceProbe(Tracer tracer) {
            this.tracer = tracer;
        }

        @KafkaListener(id = "tracing-probe", groupId = "tracing-probe", topics = KafkaTopics.NOMINATION_REQUESTED,
                containerFactory = KafkaConsumerConfig.NOTIFICATIONS_CONTAINER_FACTORY)
        void onMessage(ConsumerRecord<String, String> record) {
            var header = record.headers().lastHeader(KafkaTopics.HEADER_TRACEPARENT);
            var correlation = record.headers().lastHeader(KafkaTopics.HEADER_CORRELATION_ID);
            Span current = tracer.currentSpan();
            received.put(record.key(), new Received(
                    header == null ? null : new String(header.value(), StandardCharsets.UTF_8),
                    current == null ? null : current.context().traceId(),
                    MDC.get("traceId"),
                    correlation == null ? null : new String(correlation.value(), StandardCharsets.UTF_8)));
        }
    }
}
