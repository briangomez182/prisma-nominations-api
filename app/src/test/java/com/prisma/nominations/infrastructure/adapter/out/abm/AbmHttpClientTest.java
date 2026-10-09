package com.prisma.nominations.infrastructure.adapter.out.abm;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.port.out.AbmRequest;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Cliente HTTP de ABM contra un servidor HTTP real embebido (JDK): timeouts y conexión rechazada son reales,
 * no simulados.
 */
class AbmHttpClientTest {

    private static final String ACCOUNT_ID = "0720000088000037654321";
    private static final Duration READ_TIMEOUT = Duration.ofMillis(300);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MockEnvironment environment = new MockEnvironment();
    private final List<HttpServer> servers = new ArrayList<>();
    private final List<ExecutorService> executors = new ArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private AbmHttpClient client;

    /** Lo que recibió el servidor embebido. */
    record Received(String method, String path, String contentType, String correlationId, byte[] body) {
    }

    @AfterEach
    void tearDown() {
        release.countDown();
        if (client != null) {
            client.close();
        }
        servers.forEach(s -> s.stop(0));
        executors.forEach(ExecutorService::shutdownNow);
    }

    @Test
    @DisplayName("202: devuelve abm_operation_id; body snake_case con los 8 campos y header X-Correlation-Id")
    void accepted_returnsOperationId() throws Exception {
        List<Received> received = new CopyOnWriteArrayList<>();
        String baseUrl = server(received, 202, "{\"abm_operation_id\":\"ABM-OP-123\",\"status\":\"ACCEPTED\",\"extra\":1}");
        environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY, baseUrl + "/abm-mock");
        client = client();

        AbmRequest request = request();
        String operationId = client.submit(request);

        assertThat(operationId).isEqualTo("ABM-OP-123");
        assertThat(received).hasSize(1);
        Received r = received.getFirst();
        assertThat(r.method()).isEqualTo("POST");
        assertThat(r.path()).isEqualTo("/abm-mock/v1/nominations");
        assertThat(r.contentType()).startsWith("application/json");
        assertThat(r.correlationId()).isEqualTo("corr-abc-1");
        Map<String, Object> body = objectMapper.readValue(r.body(), new TypeReference<>() {
        });
        assertThat(body).containsExactlyInAnyOrderEntriesOf(Map.of(
                "nomination_id", request.nominationId().toString(),
                "request_id", request.requestId().toString(),
                "correlation_id", "corr-abc-1",
                "entity_id", "0072",
                "customer_id", "CUST-000123",
                "account_id", ACCOUNT_ID,
                "card_id", "tok_4f9a2c7b8d1e",
                "alias", "CUENTA SUELDO"));
    }

    @Test
    @DisplayName("400 y 422 → AbmContractException (sin datos sensibles en el mensaje)")
    void clientError_isContractException() throws Exception {
        for (int status : new int[]{400, 422}) {
            String baseUrl = server(new CopyOnWriteArrayList<>(), status,
                    "{\"error\":\"cuenta inválida " + ACCOUNT_ID + "\"}");
            environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY, baseUrl);
            client = client();

            assertThatThrownBy(() -> client.submit(request()))
                    .isInstanceOf(AbmContractException.class)
                    .hasMessageContaining("HTTP " + status)
                    .message().doesNotContain(ACCOUNT_ID);
        }
    }

    @Test
    @Tag("E6")
    @DisplayName("503 y 500 → AbmUnavailableException")
    void serverError_isUnavailable() throws Exception {
        for (int status : new int[]{503, 500}) {
            String baseUrl = server(new CopyOnWriteArrayList<>(), status, "{}");
            environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY, baseUrl);
            client = client();

            assertThatThrownBy(() -> client.submit(request()))
                    .isInstanceOf(AbmUnavailableException.class)
                    .hasMessageContaining("HTTP " + status);
        }
    }

    @Test
    @Tag("E6")
    @DisplayName("429 → AbmUnavailableException (transitorio, no es error de contrato)")
    void tooManyRequests_isUnavailable() throws Exception {
        environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY, server(new CopyOnWriteArrayList<>(), 429, "{}"));
        client = client();

        assertThatThrownBy(() -> client.submit(request())).isInstanceOf(AbmUnavailableException.class);
    }

    @Test
    @DisplayName("2xx sin abm_operation_id → AbmContractException")
    void acceptedWithoutOperationId_isContractException() throws Exception {
        environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY,
                server(new CopyOnWriteArrayList<>(), 202, "{\"status\":\"ACCEPTED\"}"));
        client = client();

        assertThatThrownBy(() -> client.submit(request())).isInstanceOf(AbmContractException.class);
    }

    @Test
    @Tag("E6")
    @DisplayName("timeout de lectura → AbmUnavailableException, sin esperar más que el read-timeout")
    void readTimeout_isUnavailable() throws Exception {
        HttpServer server = newServer();
        server.createContext("/", exchange -> {
            try {
                release.await(10, TimeUnit.SECONDS); // no responde hasta el fin del test
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            exchange.close();
        });
        server.start();
        environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY, url(server));
        client = client();

        long start = System.nanoTime();
        assertThatThrownBy(() -> client.submit(request()))
                .isInstanceOf(AbmUnavailableException.class)
                .hasMessageContaining("ABM no disponible");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    @Tag("E6")
    @DisplayName("conexión rechazada → AbmUnavailableException")
    void connectionRefused_isUnavailable() throws Exception {
        int freePort;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            freePort = socket.getLocalPort();
        }
        environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY, "http://127.0.0.1:" + freePort);
        client = client();

        assertThatThrownBy(() -> client.submit(request()))
                .isInstanceOf(AbmUnavailableException.class)
                .hasMessageContaining("ABM no disponible");
    }

    @Test
    @DisplayName("la base-url se resuelve en cada llamada, incluido ${local.server.port} definido después")
    void baseUrlResolvedOnEachCall() throws Exception {
        List<Received> first = new CopyOnWriteArrayList<>();
        List<Received> second = new CopyOnWriteArrayList<>();
        HttpServer firstServer = serverInstance(first, 202, "{\"abm_operation_id\":\"ABM-OP-1\"}");
        HttpServer secondServer = serverInstance(second, 202, "{\"abm_operation_id\":\"ABM-OP-2\"}");
        // Como en application.yml: el puerto real se conoce recién cuando el servidor arrancó.
        environment.setProperty(AbmHttpClient.BASE_URL_PROPERTY,
                "http://127.0.0.1:${local.server.port:1}/abm-mock");
        client = client();

        environment.setProperty("local.server.port", String.valueOf(firstServer.getAddress().getPort()));
        assertThat(client.submit(request())).isEqualTo("ABM-OP-1");

        environment.setProperty("local.server.port", String.valueOf(secondServer.getAddress().getPort()));
        assertThat(client.submit(request())).isEqualTo("ABM-OP-2");

        assertThat(first).hasSize(1);
        assertThat(second).hasSize(1);
    }

    // ---------------------------------------------------------------- helpers

    private AbmHttpClient client() {
        if (client != null) {
            client.close();
        }
        return new AbmHttpClient(RestClient.builder(), environment, objectMapper,
                new AbmHttpClientProperties(Duration.ofSeconds(1), READ_TIMEOUT));
    }

    private static AbmRequest request() {
        return new AbmRequest(UUID.randomUUID(), UUID.randomUUID(), "corr-abc-1", "0072", "CUST-000123",
                ACCOUNT_ID, "tok_4f9a2c7b8d1e", "CUENTA SUELDO");
    }

    private String server(List<Received> received, int status, String responseBody) throws IOException {
        return url(serverInstance(received, status, responseBody));
    }

    private HttpServer serverInstance(List<Received> received, int status, String responseBody) throws IOException {
        HttpServer server = newServer();
        server.createContext("/", exchange -> respond(exchange, received, status, responseBody));
        server.start();
        return server;
    }

    private HttpServer newServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        ExecutorService executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        servers.add(server);
        executors.add(executor);
        return server;
    }

    private static void respond(HttpExchange exchange, List<Received> received, int status, String responseBody)
            throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        received.add(new Received(exchange.getRequestMethod(), exchange.getRequestURI().getPath(),
                exchange.getRequestHeaders().getFirst("Content-Type"),
                exchange.getRequestHeaders().getFirst("X-Correlation-Id"), body));
        byte[] out = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, out.length);
        exchange.getResponseBody().write(out);
        exchange.close();
    }

    private static String url(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }
}
