package com.prisma.nominations.infrastructure.adapter.out.abm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.TestTokens;
import com.prisma.nominations.TestcontainersConfiguration;
import com.prisma.nominations.application.port.out.AbmClient;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cableado real: el puerto AbmClient recibe el decorador resiliente y el estado del circuit breaker "abm" se ve en
 * /actuator/health. Adapter, consumers, simulador y sweeper apagados: solo interesa el contexto.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nominations.abm.adapter.enabled=false",
        "nominations.abm.response-consumer.enabled=false",
        "nominations.abm-mock.enabled=false",
        "nominations.abm.sweeper.enabled=false"})
@TestPropertySource(properties = {
        "management.health.circuitbreakers.enabled=true",
        "management.endpoint.health.show-details=always"})
@Import(TestcontainersConfiguration.class)
class AbmCircuitBreakerHealthIntegrationTest {

    @LocalServerPort
    private int port;
    @Autowired
    private AbmClient abmClient;
    @Autowired
    private CircuitBreakerRegistry registry;
    @Autowired
    private ObjectMapper objectMapper;

    private final HttpClient http = HttpClient.newHttpClient();

    @AfterEach
    void reset() {
        circuitBreaker().reset();
    }

    @Test
    @DisplayName("el puerto AbmClient inyectado es el decorador resiliente (@Primary sobre AbmHttpClient)")
    void abmClientIsResilientDecorator() {
        assertThat(abmClient).isInstanceOf(ResilientAbmClient.class);
    }

    @Test
    @DisplayName("/actuator/health expone circuitBreakers.abm con estado y config del yml; OPEN se refleja")
    void healthShowsCircuitBreaker() throws Exception {
        JsonNode abm = health().path("components").path("circuitBreakers").path("details").path("abm");
        assertThat(abm.path("status").asText()).isEqualTo("UP");
        assertThat(abm.path("details").path("state").asText()).isEqualTo("CLOSED");
        assertThat(abm.path("details").path("failureRateThreshold").asText()).isEqualTo("50.0%");

        circuitBreaker().transitionToOpenState();

        JsonNode root = health();
        // ABM caído no tumba la app: el CB abierto es informativo (allow-health-indicator-to-fail=false por defecto).
        assertThat(root.path("status").asText()).isEqualTo("UP");
        JsonNode open = root.path("components").path("circuitBreakers").path("details").path("abm");
        assertThat(open.path("status").asText()).isEqualTo("CIRCUIT_OPEN");
        assertThat(open.path("details").path("state").asText()).isEqualTo("OPEN");
    }

    private CircuitBreaker circuitBreaker() {
        return registry.circuitBreaker(ResilientAbmClient.INSTANCE);
    }

    private JsonNode health() throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/health"))
                        // El detalle de componentes solo se muestra con token de operador (show-details: when-authorized).
                        .header(HttpHeaders.AUTHORIZATION, TestTokens.operatorBearer()).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        return objectMapper.readTree(response.body());
    }
}
