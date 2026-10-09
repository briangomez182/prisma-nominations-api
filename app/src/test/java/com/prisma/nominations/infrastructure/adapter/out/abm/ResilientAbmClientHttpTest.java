package com.prisma.nominations.infrastructure.adapter.out.abm;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Decorador + {@link AbmHttpClient} real contra un servidor HTTP embebido (JDK): intentos y timeouts reales.
 */
class ResilientAbmClientHttpTest {

    private static final Duration READ_TIMEOUT = Duration.ofMillis(300);

    private final MockEnvironment environment = new MockEnvironment();
    private final AtomicInteger received = new AtomicInteger();
    private final CountDownLatch release = new CountDownLatch(1);
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private HttpServer server;
    private AbmHttpClient http;

    @AfterEach
    void tearDown() {
        release.countDown();
        if (http != null) {
            http.close();
        }
        if (server != null) {
            server.stop(0);
        }
        executor.shutdownNow();
    }

    @Test
    @DisplayName("ABM responde 503 siempre → el servidor recibe exactamente 3 intentos y sale AbmUnavailableException")
    void serviceUnavailable_isRetriedThreeTimes() throws Exception {
        start(exchange -> respond(exchange, 503, "{}"));

        assertThatThrownBy(() -> client().submit(ResilientAbmClientTest.request()))
                .isInstanceOf(AbmUnavailableException.class)
                .hasMessageContaining("HTTP 503");
        assertThat(received.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("ABM lento (supera el read-timeout) → reintenta y agota en AbmUnavailableException, acotado en tiempo")
    void slowAbm_readTimeoutIsRetriedThenFails() throws Exception {
        start(exchange -> {
            try {
                release.await(10, TimeUnit.SECONDS); // no responde hasta el fin del test
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });

        long start = System.nanoTime();
        assertThatThrownBy(() -> client().submit(ResilientAbmClientTest.request()))
                .isInstanceOf(AbmUnavailableException.class)
                .hasMessageContaining("ABM no disponible");
        assertThat(received.get()).isEqualTo(3);
        // 3 × read-timeout + backoff corto: el consumer no queda bloqueado más que eso.
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
    }

    @Test
    @DisplayName("primer intento con timeout, segundo responde 202 → éxito con 2 pedidos recibidos")
    void timeoutThenSuccess_recovers() throws Exception {
        start(exchange -> {
            if (received.get() == 1) {
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                exchange.close();
                return;
            }
            respond(exchange, 202, "{\"abm_operation_id\":\"ABM-OP-RETRY\"}");
        });

        assertThat(client().submit(ResilientAbmClientTest.request())).isEqualTo("ABM-OP-RETRY");
        assertThat(received.get()).isEqualTo(2);
    }

    // ---------------------------------------------------------------- helpers

    private interface Handler {
        void handle(HttpExchange exchange) throws IOException;
    }

    private void start(Handler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            received.incrementAndGet();
            handler.handle(exchange);
        });
        server.start();
        environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY, "http://127.0.0.1:" + server.getAddress().getPort());
    }

    private ResilientAbmClient client() {
        http = new AbmHttpClient(RestClient.builder(), environment, new ObjectMapper(),
                new AbmHttpClientProperties(Duration.ofSeconds(1), READ_TIMEOUT));
        CircuitBreakerRegistry cbRegistry = CircuitBreakerRegistry.of(CircuitBreakerConfig.custom()
                .recordExceptions(AbmUnavailableException.class)
                .build());
        return new ResilientAbmClient(http, ResilientAbmClientTest.retryRegistry(), cbRegistry);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] out = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, out.length);
        exchange.getResponseBody().write(out);
        exchange.close();
    }
}
