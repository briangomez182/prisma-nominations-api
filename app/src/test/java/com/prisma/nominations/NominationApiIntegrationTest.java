package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * API de punta a punta con contexto completo, PostgreSQL y Kafka reales (Testcontainers).
 * Cada test usa una entidad y un request_id aleatorios: los conteos en la base no se pisan entre tests.
 * <p>
 * ABM Adapter apagado: con MockMvc no hay servidor HTTP y el adapter no llegaría al simulador de ABM (solo
 * reintentos y DLT en segundo plano). El circuito con ABM está en {@link AbmFlowIntegrationTest}. Mismas
 * properties que {@link NominationEventFlowIntegrationTest}: comparten contexto.
 */
@SpringBootTest(properties = "nominations.abm.adapter.enabled=false")
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class NominationApiIntegrationTest {

    private static final String BASE = "/v1/nominations";
    private static final String PAN = "4111111111111111";
    /** Spec exportada a la raíz del repo (el test corre con cwd = app/). */
    static final Path OPENAPI_EXPORT = Path.of("..", "docs", "openapi.yaml");

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;

    @Nested
    @DisplayName("E1 - alta válida")
    class E1ValidNomination {

        @Test
        void returns202_thenGetAndHistoryShowReceivedWithSameCorrelationId() throws Exception {
            String entity = newEntity();
            UUID requestId = UUID.randomUUID();
            String correlationId = "it-e1-" + UUID.randomUUID();

            MvcResult created = mockMvc.perform(create(entity, body(requestId, "tok_4f9a2c7b8d1e"))
                            .header(ApiHeaders.CORRELATION_ID, correlationId))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string("Location", startsWith(BASE + "/")))
                    .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "false"))
                    .andExpect(header().string(ApiHeaders.CORRELATION_ID, correlationId))
                    .andExpect(jsonPath("$.status").value("RECEIVED"))
                    .andExpect(jsonPath("$.request_id").value(requestId.toString()))
                    .andExpect(jsonPath("$.account_id").value("****7654"))
                    .andExpect(jsonPath("$.card_id").value("****8d1e"))
                    .andExpect(jsonPath("$.correlation_id").value(correlationId))
                    .andReturn();

            String location = created.getResponse().getHeader("Location");
            String nominationId = json(created).get("nomination_id").asText();
            assertThat(location).isEqualTo(BASE + "/" + nominationId);

            mockMvc.perform(get(location).header(ApiHeaders.ENTITY_ID, entity))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nomination_id").value(nominationId))
                    .andExpect(jsonPath("$.status").value("RECEIVED"))
                    .andExpect(jsonPath("$.account_id").value("****7654"))
                    .andExpect(jsonPath("$.card_id").value("****8d1e"));

            mockMvc.perform(get(location + "/history").header(ApiHeaders.ENTITY_ID, entity))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.nomination_id").value(nominationId))
                    .andExpect(jsonPath("$.items", hasSize(1)))
                    .andExpect(jsonPath("$.items[0].to_status").value("RECEIVED"))
                    .andExpect(jsonPath("$.items[0].from_status").doesNotExist())
                    .andExpect(jsonPath("$.items[0].source").value("API"))
                    .andExpect(jsonPath("$.items[0].correlation_id").value(correlationId));

            assertThat(nominationsOf(entity)).isEqualTo(1);
            assertThat(historyOf(entity)).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("E2 - solicitud inválida: 400 y nada persistido")
    class E2InvalidRequest {

        @Test
        void missingRequiredFields_returnsValidationErrorWithSnakeCaseFields() throws Exception {
            String entity = newEntity();

            mockMvc.perform(create(entity, "{\"alias\": \"CUENTA SUELDO\"}"))
                    .andExpect(status().isBadRequest())
                    .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                    .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                    .andExpect(jsonPath("$.type").value("https://api.prisma.example/problems/validation-error"))
                    .andExpect(jsonPath("$.correlation_id").isNotEmpty())
                    .andExpect(jsonPath("$.timestamp").isNotEmpty())
                    .andExpect(jsonPath("$.errors[*].field",
                            containsInAnyOrder("request_id", "customer_id", "account_id", "card_id")));

            assertThat(nominationsOf(entity)).isZero();
        }

        @Test
        void cardIdWithPan_returnsValidationErrorWithoutEchoingThePan() throws Exception {
            String entity = newEntity();

            MvcResult result = mockMvc.perform(create(entity, body(UUID.randomUUID(), PAN)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("VALIDATION_ERROR"))
                    .andExpect(jsonPath("$.errors[0].field").value("card_id"))
                    .andReturn();

            assertThat(result.getResponse().getContentAsString(StandardCharsets.UTF_8)).doesNotContain(PAN);
            assertThat(nominationsOf(entity)).isZero();
        }

        @Test
        void invalidJson_returnsMalformedRequest() throws Exception {
            String entity = newEntity();

            mockMvc.perform(create(entity, "{\"request_id\": \"" + UUID.randomUUID() + "\", "))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("MALFORMED_REQUEST"));

            assertThat(nominationsOf(entity)).isZero();
        }

        @Test
        void missingEntityHeader_returnsMissingHeader() throws Exception {
            UUID requestId = UUID.randomUUID();

            mockMvc.perform(post(BASE).contentType(MediaType.APPLICATION_JSON)
                            .content(body(requestId, "tok_4f9a2c7b8d1e")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("MISSING_HEADER"))
                    .andExpect(jsonPath("$.detail").value("Falta el header obligatorio " + ApiHeaders.ENTITY_ID));

            Integer rows = jdbc.queryForObject("SELECT count(*) FROM nominations WHERE request_id = ?",
                    Integer.class, requestId);
            assertThat(rows).isZero();
        }
    }

    @Nested
    @DisplayName("E3 - idempotencia por (entidad, request_id)")
    class E3Idempotency {

        @Test
        void samePostTwice_replaysTheSameNominationWithoutDuplicating() throws Exception {
            String entity = newEntity();
            String body = body(UUID.randomUUID(), "tok_4f9a2c7b8d1e");

            String firstId = json(mockMvc.perform(create(entity, body))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "false"))
                    .andReturn()).get("nomination_id").asText();

            mockMvc.perform(create(entity, body))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "true"))
                    .andExpect(header().string("Location", BASE + "/" + firstId))
                    .andExpect(jsonPath("$.nomination_id").value(firstId));

            assertThat(nominationsOf(entity)).isEqualTo(1);
            assertThat(historyOf(entity)).isEqualTo(1);
        }

        @Test
        void sameRequestIdWithDifferentContent_returnsIdempotencyConflict() throws Exception {
            String entity = newEntity();
            UUID requestId = UUID.randomUUID();

            mockMvc.perform(create(entity, body(requestId, "tok_4f9a2c7b8d1e")))
                    .andExpect(status().isAccepted());

            mockMvc.perform(create(entity, body(requestId, "tok_otra_tarjeta")))
                    .andExpect(status().isConflict())
                    .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                    .andExpect(jsonPath("$.code").value("IDEMPOTENCY_CONFLICT"));

            assertThat(nominationsOf(entity)).isEqualTo(1);
        }

        @Test
        void sameRequestIdInAnotherEntity_createsANewNomination() throws Exception {
            String entityA = newEntity();
            String entityB = newEntity();
            String body = body(UUID.randomUUID(), "tok_4f9a2c7b8d1e");

            String idA = json(mockMvc.perform(create(entityA, body)).andExpect(status().isAccepted()).andReturn())
                    .get("nomination_id").asText();

            String idB = json(mockMvc.perform(create(entityB, body))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "false"))
                    .andReturn()).get("nomination_id").asText();

            assertThat(idB).isNotEqualTo(idA);
            assertThat(nominationsOf(entityA)).isEqualTo(1);
            assertThat(nominationsOf(entityB)).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Aislamiento por entidad")
    class EntityIsolation {

        @Test
        void otherEntityCannotSeeTheNominationOrItsHistory() throws Exception {
            String owner = newEntity();
            String location = mockMvc.perform(create(owner, body(UUID.randomUUID(), "tok_4f9a2c7b8d1e")))
                    .andExpect(status().isAccepted())
                    .andReturn().getResponse().getHeader("Location");

            mockMvc.perform(get(location).header(ApiHeaders.ENTITY_ID, newEntity()))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("NOMINATION_NOT_FOUND"));
            mockMvc.perform(get(location + "/history").header(ApiHeaders.ENTITY_ID, newEntity()))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("NOMINATION_NOT_FOUND"));
        }
    }

    @Nested
    @DisplayName("OpenAPI")
    class OpenApi {

        @Test
        void apiDocsDescribeTheEndpointsInSnakeCase() throws Exception {
            JsonNode spec = json(mockMvc.perform(get("/v3/api-docs"))
                    .andExpect(status().isOk())
                    .andReturn());

            assertThat(spec.at("/paths").has(BASE)).isTrue();
            assertThat(spec.at("/paths").has(BASE + "/{nominationId}/history")).isTrue();
            JsonNode request = spec.at("/components/schemas/CreateNominationRequest/properties");
            assertThat(request.has("request_id")).isTrue();
            assertThat(request.has("card_id")).isTrue();
            assertThat(request.has("requestId")).isFalse();
            assertThat(spec.at("/components/schemas/NominationResponse/properties").has("nomination_id")).isTrue();
            assertThat(spec.at("/components/schemas/ProblemDetail/properties").has("correlation_id")).isTrue();
            assertThat(spec.at("/components/parameters").has(ApiHeaders.ENTITY_ID)).isTrue();
            assertThat(spec.at("/components/parameters").has(ApiHeaders.CORRELATION_ID)).isTrue();
        }

        /**
         * Exporta la spec a docs/openapi.yaml para que quede versionada junto al código. Regenerar con
         * {@code mvn test -Dtest='NominationApiIntegrationTest*'} (o con cualquier {@code mvn test}).
         */
        @Test
        void exportsSpecToDocs() throws Exception {
            String yaml = mockMvc.perform(get("/v3/api-docs.yaml"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);
            assertThat(yaml).contains("openapi:").contains(BASE);

            Files.createDirectories(OPENAPI_EXPORT.getParent());
            if (!Files.exists(OPENAPI_EXPORT) || !Files.readString(OPENAPI_EXPORT).equals(yaml)) {
                Files.writeString(OPENAPI_EXPORT, yaml);
            }
        }
    }

    // ---------------------------------------------------------------- soporte

    private static String newEntity() {
        return "IT" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
    }

    private static String body(UUID requestId, String cardId) {
        return """
                {
                  "request_id": "%s",
                  "customer_id": "CUST-000123",
                  "account_id": "0001234567890987654",
                  "card_id": "%s",
                  "alias": "CUENTA SUELDO"
                }
                """.formatted(requestId, cardId);
    }

    private static MockHttpServletRequestBuilder create(String entity, String body) {
        return post(BASE).header(ApiHeaders.ENTITY_ID, entity)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
    }

    private JsonNode json(MvcResult result) throws Exception {
        return objectMapper.readTree(result.getResponse().getContentAsString(StandardCharsets.UTF_8));
    }

    private int nominationsOf(String entity) {
        return jdbc.queryForObject("SELECT count(*) FROM nominations WHERE entity_id = ?", Integer.class, entity);
    }

    private int historyOf(String entity) {
        return jdbc.queryForObject("""
                SELECT count(*) FROM nomination_history h
                JOIN nominations n ON n.id = h.nomination_id
                WHERE n.entity_id = ?""", Integer.class, entity);
    }
}
