package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.prisma.nominations.TestTokens.OPERATE;
import static com.prisma.nominations.TestTokens.READ;
import static com.prisma.nominations.TestTokens.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Seguridad de punta a punta con tokens reales (HS256 con la clave de demo) contra el servidor HTTP: decoder,
 * reglas por scope, entidad del token, 401/403 en formato RFC 9457 y endpoints públicos.
 * Con {@code @AutoConfigureObservability(tracing = false)}: sin eso los tests no exportan métricas y
 * /actuator/prometheus no existiría.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nominations.abm-mock.enabled=true",
        "nominations.abm-mock.response-delay=200ms",
        "nominations.abm-mock.duplicate-gap=100ms",
        "nominations.abm-mock.slow-http-delay=3s",
        "nominations.abm.adapter.enabled=false",
        "nominations.abm.response-consumer.enabled=false",
        "nominations.outbox.relay.enabled=false"})
@AutoConfigureObservability(tracing = false)
@Import(TestcontainersConfiguration.class)
@Tag("integration")
class SecurityIntegrationTest {

    private static final String BASE = "/v1/nominations";
    private static final String OTHER_SECRET = "c2VjcmV0by1kZS1vdHJvLWlkcC1xdWUtbm8tY29ub2NlbW9zLTEyMzQ1Njc4OTA=";

    @LocalServerPort
    private int port;
    @Autowired
    private ObjectMapper objectMapper;

    @Nested
    @DisplayName("401: sin token o token inválido")
    class Unauthenticated {

        @Test
        void withoutToken_401ProblemWithCorrelationIdAndChallenge() throws Exception {
            Response response = call(HttpMethod.POST, BASE, null, body(), "it-sec-anon-1");

            assertThat(response.status()).isEqualTo(401);
            assertThat(response.headers().getFirst(HttpHeaders.WWW_AUTHENTICATE)).isEqualTo("Bearer");
            assertThat(response.headers().getContentType()).isNotNull();
            assertThat(response.headers().getContentType().isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)).isTrue();
            assertThat(response.headers().getFirst(ApiHeaders.CORRELATION_ID)).isEqualTo("it-sec-anon-1");
            JsonNode problem = response.json();
            assertThat(problem.path("status").asInt()).isEqualTo(401);
            assertThat(problem.path("code").asText()).isEqualTo("UNAUTHORIZED");
            assertThat(problem.path("type").asText()).isEqualTo("https://api.prisma.example/problems/unauthorized");
            assertThat(problem.path("correlation_id").asText()).isEqualTo("it-sec-anon-1");
            assertThat(problem.path("instance").asText()).isEqualTo("urn:correlation-id:it-sec-anon-1");
            assertThat(problem.path("timestamp").asText()).isNotBlank();
        }

        @Test
        void expiredToken_401WithoutTokenDetails() throws Exception {
            String token = TestTokens.token().entity(newEntity()).scopes(READ)
                    .expiresAt(Instant.now().minus(Duration.ofHours(1))).bearer();

            Response response = call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(), token, null, null);

            assertInvalidToken(response);
        }

        @Test
        void tokenWithoutExp_401() throws Exception {
            assertInvalidToken(call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(),
                    TestTokens.token().entity(newEntity()).scopes(READ).expiresAt(null).bearer(), null, null));
        }

        @Test
        void tokenSignedWithAnotherKey_401() throws Exception {
            assertInvalidToken(call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(),
                    TestTokens.token().entity(newEntity()).scopes(READ).secret(OTHER_SECRET).bearer(), null, null));
        }

        @Test
        void tokenFromAnotherIssuer_401() throws Exception {
            assertInvalidToken(call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(),
                    TestTokens.token().entity(newEntity()).scopes(READ).issuer("otro-idp").bearer(), null, null));
        }

        @ParameterizedTest
        @ValueSource(strings = {"Bearer no-es-un-jwt",
                // alg=none sin firma: nunca se acepta
                "Bearer eyJhbGciOiJub25lIn0.eyJpc3MiOiJwcmlzbWEtbm9taW5hdGlvbnMtZGVtbyIsInNjb3BlIjoibm9taW5hdGlvbnM6cmVhZCJ9."})
        void malformedOrUnsignedToken_401(String authorization) throws Exception {
            assertInvalidToken(call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(), authorization, null, null));
        }

        private void assertInvalidToken(Response response) throws Exception {
            assertThat(response.status()).isEqualTo(401);
            assertThat(response.headers().getFirst(HttpHeaders.WWW_AUTHENTICATE)).isEqualTo("Bearer error=\"invalid_token\"");
            assertThat(response.json().path("code").asText()).isEqualTo("UNAUTHORIZED");
            assertThat(response.json().path("correlation_id").asText())
                    .isEqualTo(response.headers().getFirst(ApiHeaders.CORRELATION_ID));
            // Sin pistas del motivo (vencido, firma, issuer) ni eco del token
            assertThat(response.body()).doesNotContainIgnoringCase("expired").doesNotContainIgnoringCase("jwt")
                    .doesNotContain("signature").doesNotContain("otro-idp");
        }
    }

    @Nested
    @DisplayName("Scopes y entidad")
    class Authorization {

        @Test
        void writeOnlyCanCreate_readOnlyCanQuery_andNotTheOtherWayAround() throws Exception {
            String entity = newEntity();

            Response created = call(HttpMethod.POST, BASE, TestTokens.bearer(entity, WRITE), body(), null);
            assertThat(created.status()).isEqualTo(202);
            String location = created.headers().getLocation().toString();

            Response read = call(HttpMethod.GET, location, TestTokens.bearer(entity, READ), null, null);
            assertThat(read.status()).isEqualTo(200);
            assertThat(read.json().path("status").asText()).isEqualTo("RECEIVED");
            assertThat(call(HttpMethod.GET, location + "/history", TestTokens.bearer(entity, READ), null, null).status())
                    .isEqualTo(200);

            Response readWithWrite = call(HttpMethod.GET, location, TestTokens.bearer(entity, WRITE), null, null);
            assertForbidden(readWithWrite);

            Response createWithRead = call(HttpMethod.POST, BASE, TestTokens.bearer(entity, READ), body(), null);
            assertForbidden(createWithRead);
        }

        @Test
        void entityComesFromTheToken_otherEntityGets404() throws Exception {
            String owner = newEntity();
            String location = call(HttpMethod.POST, BASE, TestTokens.bearer(owner), body(), null)
                    .headers().getLocation().toString();

            Response other = call(HttpMethod.GET, location, TestTokens.bearer(newEntity()), null, null);
            assertThat(other.status()).isEqualTo(404);
            assertThat(other.json().path("code").asText()).isEqualTo("NOMINATION_NOT_FOUND");

            // Un header X-Entity-Id ya no tiene efecto: la entidad es siempre la del token.
            Response spoofed = call(HttpMethod.GET, location, TestTokens.bearer(newEntity()), null, null, owner);
            assertThat(spoofed.status()).isEqualTo(404);
        }

        @Test
        void tokenWithoutEntity_403OnV1() throws Exception {
            String noEntity = TestTokens.token().scopes(WRITE, READ).bearer();

            assertForbidden(call(HttpMethod.POST, BASE, noEntity, body(), null));
            assertForbidden(call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(), noEntity, null, null));
            // Un operador tampoco opera sobre /v1: no tiene entidad ni scopes de canal
            assertForbidden(call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(), TestTokens.operatorBearer(), null, null));
        }

        @Test
        void tokenWithInvalidEntityClaim_403() throws Exception {
            String invalid = TestTokens.token().entity("ENT 01'; --").scopes(WRITE, READ).bearer();

            assertForbidden(call(HttpMethod.GET, BASE + "/" + UUID.randomUUID(), invalid, null, null));
        }

        @Test
        void internalRequiresOperateScope() throws Exception {
            String reprocess = "/internal/v1/nominations/" + UUID.randomUUID() + "/reprocess";

            assertThat(call(HttpMethod.POST, reprocess, null, null, null).status()).isEqualTo(401);
            assertForbidden(call(HttpMethod.POST, reprocess, TestTokens.bearer(newEntity(), WRITE, READ), null, null));

            // Con operate pasa la seguridad y llega al caso de uso: la nominación no existe → 404
            Response operator = call(HttpMethod.POST, reprocess, TestTokens.operatorBearer(), null, null);
            assertThat(operator.status()).isEqualTo(404);
            assertThat(operator.json().path("code").asText()).isEqualTo("NOMINATION_NOT_FOUND");
        }

        @Test
        void unknownRoutesAreDeniedByDefault() throws Exception {
            assertThat(call(HttpMethod.GET, "/no-existe", null, null, null).status()).isEqualTo(401);
            assertForbidden(call(HttpMethod.GET, "/no-existe", TestTokens.bearer(newEntity()), null, null));
        }

        private void assertForbidden(Response response) throws Exception {
            assertThat(response.status()).isEqualTo(403);
            assertThat(response.headers().getFirst(HttpHeaders.WWW_AUTHENTICATE))
                    .isEqualTo("Bearer error=\"insufficient_scope\"");
            assertThat(response.headers().getContentType().isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)).isTrue();
            assertThat(response.json().path("code").asText()).isEqualTo("FORBIDDEN");
            assertThat(response.json().path("correlation_id").asText())
                    .isEqualTo(response.headers().getFirst(ApiHeaders.CORRELATION_ID));
        }
    }

    @Nested
    @DisplayName("Endpoints públicos y Actuator")
    class PublicEndpoints {

        @Test
        void healthIsPublicButDetailsRequireOperate() throws Exception {
            Response anonymous = call(HttpMethod.GET, "/actuator/health", null, null, null);
            assertThat(anonymous.status()).isEqualTo(200);
            assertThat(anonymous.json().path("status").asText()).isEqualTo("UP");
            assertThat(anonymous.json().has("components")).isFalse();

            Response channel = call(HttpMethod.GET, "/actuator/health", TestTokens.bearer(newEntity()), null, null);
            assertThat(channel.json().has("components")).isFalse();

            Response operator = call(HttpMethod.GET, "/actuator/health", TestTokens.operatorBearer(), null, null);
            assertThat(operator.json().path("components").has("db")).isTrue();

            assertThat(call(HttpMethod.GET, "/actuator/health/liveness", null, null, null).status()).isEqualTo(200);
            assertThat(call(HttpMethod.GET, "/actuator/health/readiness", null, null, null).status()).isEqualTo(200);
            assertThat(call(HttpMethod.GET, "/actuator/info", null, null, null).status()).isEqualTo(200);
        }

        @Test
        void prometheusIsPublicForScraping() throws Exception {
            Response response = call(HttpMethod.GET, "/actuator/prometheus", null, null, null);
            assertThat(response.status()).isEqualTo(200);
            assertThat(response.body()).contains("jvm_");
        }

        @Test
        void otherActuatorEndpointsRequireOperate() throws Exception {
            assertThat(call(HttpMethod.GET, "/actuator/metrics", null, null, null).status()).isEqualTo(401);
            assertThat(call(HttpMethod.GET, "/actuator/metrics", TestTokens.bearer(newEntity()), null, null).status())
                    .isEqualTo(403);
            assertThat(call(HttpMethod.GET, "/actuator/metrics", TestTokens.operatorBearer(), null, null).status())
                    .isEqualTo(200);
        }

        @Test
        void openApiAndSwaggerArePublicInTheDemo() throws Exception {
            assertThat(call(HttpMethod.GET, "/v3/api-docs", null, null, null).status()).isEqualTo(200);
            assertThat(call(HttpMethod.GET, "/v3/api-docs.yaml", null, null, null).status()).isEqualTo(200);
            assertThat(call(HttpMethod.GET, "/swagger-ui/index.html", null, null, null).status()).isEqualTo(200);
        }

        @Test
        void abmMockIsNotBehindTheApiSecurity() throws Exception {
            // El simulador es "otro sistema": responde con su propio contrato (400 por body vacío), no 401.
            Response response = call(HttpMethod.POST, "/abm-mock/v1/nominations", null, "{}", null);
            assertThat(response.status()).isEqualTo(400);
        }

        @Test
        void statelessWithDefaultSecurityHeaders() throws Exception {
            Response response = call(HttpMethod.POST, BASE, TestTokens.bearer(newEntity()), body(), null);

            assertThat(response.status()).isEqualTo(202);
            assertThat(response.headers().get(HttpHeaders.SET_COOKIE)).isNull();
            assertThat(response.headers().getFirst("X-Content-Type-Options")).isEqualTo("nosniff");
            assertThat(response.headers().getFirst("X-Frame-Options")).isEqualTo("DENY");
            assertThat(response.headers().getFirst(HttpHeaders.CACHE_CONTROL)).contains("no-store");
        }
    }

    @Nested
    @DisplayName("scripts/mint-token.sh")
    class MintTokenScript {

        private static final Path SCRIPT = Path.of("..", "scripts", "mint-token.sh");

        @Test
        void tokensFromTheDemoScriptAreAccepted() throws Exception {
            assumeTrue(Files.isExecutable(SCRIPT) && commandExists("openssl"), "requiere bash + openssl");
            String entity = newEntity();

            String channel = "Bearer " + run(SCRIPT.toString(), entity);
            Response created = call(HttpMethod.POST, BASE, channel, body(), null);
            assertThat(created.status()).isEqualTo(202);
            assertThat(call(HttpMethod.GET, created.headers().getLocation().toString(), channel, null, null).status())
                    .isEqualTo(200);

            String operator = "Bearer " + run(SCRIPT.toString(), "", OPERATE);
            assertThat(call(HttpMethod.POST, "/internal/v1/nominations/" + UUID.randomUUID() + "/reprocess",
                    operator, null, null).status()).isEqualTo(404);

            String readOnly = "Bearer " + run(SCRIPT.toString(), entity, READ);
            assertThat(call(HttpMethod.POST, BASE, readOnly, body(), null).status()).isEqualTo(403);
        }

        private static boolean commandExists(String command) {
            try {
                return new ProcessBuilder("sh", "-c", "command -v " + command).start().waitFor() == 0;
            } catch (IOException | InterruptedException e) {
                return false;
            }
        }

        private static String run(String... command) throws Exception {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(process.exitValue()).as(output).isZero();
            return output;
        }
    }

    // ---------------------------------------------------------------- soporte

    record Response(int status, HttpHeaders headers, String body, ObjectMapper mapper) {
        JsonNode json() throws IOException {
            return mapper.readTree(body);
        }
    }

    private Response call(HttpMethod method, String path, String authorization, String body, String correlationId) {
        return call(method, path, authorization, body, correlationId, null);
    }

    private Response call(HttpMethod method, String path, String authorization, String body, String correlationId,
                          String entityHeader) {
        RestClient.RequestBodySpec request = RestClient.create("http://localhost:" + port).method(method).uri(path);
        if (authorization != null) {
            request.header(HttpHeaders.AUTHORIZATION, authorization);
        }
        if (correlationId != null) {
            request.header(ApiHeaders.CORRELATION_ID, correlationId);
        }
        if (entityHeader != null) {
            request.header("X-Entity-Id", entityHeader);
        }
        if (body != null) {
            request.contentType(MediaType.APPLICATION_JSON).body(body);
        }
        return request.exchange((req, res) -> new Response(res.getStatusCode().value(), res.getHeaders(),
                new String(res.getBody().readAllBytes(), StandardCharsets.UTF_8), objectMapper));
    }

    private static String newEntity() {
        return "SEC" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
    }

    private static String body() {
        return """
                {
                  "request_id": "%s",
                  "customer_id": "CUST-000123",
                  "account_id": "0001234567890987654",
                  "card_id": "tok_4f9a2c7b8d1e",
                  "alias": "CUENTA SUELDO"
                }
                """.formatted(UUID.randomUUID());
    }
}
