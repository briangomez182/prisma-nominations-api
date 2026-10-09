package com.prisma.nominations.infrastructure.adapter.in.web;

import com.prisma.nominations.application.exception.DuplicateNominationException;
import com.prisma.nominations.application.exception.IdempotencyConflictException;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.infrastructure.adapter.in.web.GlobalExceptionHandlerTest.ErrorTestController;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.security.oauth2.resource.servlet.OAuth2ResourceServerAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityFilterAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Sin la cadena de seguridad: acá se prueba el modelo de errores de MVC (401/403 de la cadena: SecurityIntegrationTest).
@WebMvcTest(controllers = GlobalExceptionHandlerTest.ErrorTestController.class, excludeAutoConfiguration = {
        SecurityAutoConfiguration.class, SecurityFilterAutoConfiguration.class, OAuth2ResourceServerAutoConfiguration.class})
// El controller de prueba es una clase anidada: el escaneo de tests la excluye, por eso se importa explícitamente.
@Import({ErrorTestController.class, GlobalExceptionHandler.class, CorrelationIdFilter.class})
class GlobalExceptionHandlerTest {

    private static final String PAN = "4111111111111111";
    private static final String REQUIRED_HEADER = "X-Required";
    private static final String VALID_BODY = """
            {"request_id": "%s", "card_id": "tok_123", "account_number": "987654"}
            """.formatted(UUID.randomUUID());

    @Autowired
    private MockMvc mvc;

    @Test
    void beanValidationErrorsUseSnakeCaseFieldsAndNeverEchoValues() throws Exception {
        String body = """
                {"card_id": "%s", "account_number": ""}
                """.formatted(PAN);

        MvcResult result = expectProblem(post("/test/validated").header(REQUIRED_HEADER, "v")
                .contentType(MediaType.APPLICATION_JSON).content(body), 400, "VALIDATION_ERROR")
                .andExpect(jsonPath("$.type").value("https://api.prisma.example/problems/validation-error"))
                .andExpect(jsonPath("$.errors[*].field").value(containsInAnyOrder("request_id", "card_id", "account_number")))
                .andReturn();

        assertThat(result.getResponse().getContentAsString()).doesNotContain(PAN);
    }

    @Test
    void domainValidationErrorReportsDomainField() throws Exception {
        // card_id con formato de token válido para Bean Validation, pero el dominio detecta el PAN.
        String body = """
                {"request_id": "%s", "card_id": "tok_%s", "account_number": "987654"}
                """.formatted(UUID.randomUUID(), PAN);

        MvcResult result = expectProblem(post("/test/domain").contentType(MediaType.APPLICATION_JSON).content(body),
                400, "VALIDATION_ERROR")
                .andExpect(jsonPath("$.errors[0].field").value("card_id"))
                .andExpect(jsonPath("$.errors[0].message", notNullValue()))
                .andReturn();

        assertThat(result.getResponse().getContentAsString()).doesNotContain(PAN);
    }

    @Test
    void wrongTypeInBodyIsMalformedRequestWithFieldButNotValue() throws Exception {
        String body = """
                {"request_id": "%s", "card_id": "tok_1", "account_number": "987654"}
                """.formatted(PAN);

        MvcResult result = expectProblem(post("/test/validated").header(REQUIRED_HEADER, "v")
                .contentType(MediaType.APPLICATION_JSON).content(body), 400, "MALFORMED_REQUEST")
                .andExpect(jsonPath("$.errors[0].field").value("request_id"))
                .andReturn();

        assertThat(result.getResponse().getContentAsString()).doesNotContain(PAN);
    }

    @Test
    void invalidJsonIsMalformedRequest() throws Exception {
        expectProblem(post("/test/validated").header(REQUIRED_HEADER, "v")
                .contentType(MediaType.APPLICATION_JSON).content("{\"card_id\": "), 400, "MALFORMED_REQUEST");
    }

    @Test
    void missingRequiredHeader() throws Exception {
        expectProblem(post("/test/validated").contentType(MediaType.APPLICATION_JSON).content(VALID_BODY),
                400, "MISSING_HEADER");
    }

    @Test
    void invalidPathParameterDoesNotEchoValue() throws Exception {
        MvcResult result = expectProblem(get("/test/items/{id}", PAN), 400, "INVALID_PARAMETER")
                .andExpect(jsonPath("$.errors[0].field").value("id"))
                .andReturn();

        assertThat(result.getResponse().getContentAsString()).doesNotContain(PAN);
    }

    @ParameterizedTest
    @CsvSource({
            "not-found,   404, NOMINATION_NOT_FOUND",
            "idempotency, 409, IDEMPOTENCY_CONFLICT",
            "duplicate,   409, IDEMPOTENCY_CONFLICT",
            "optimistic,  409, CONCURRENT_MODIFICATION",
            "forbidden,   403, FORBIDDEN",
            "unexpected,  500, INTERNAL_ERROR"
    })
    void applicationExceptionsAreMapped(String kind, int status, String code) throws Exception {
        expectProblem(get("/test/throw/{kind}", kind), status, code);
    }

    @Test
    void internalErrorHidesInternalDetails() throws Exception {
        MvcResult result = expectProblem(get("/test/throw/unexpected"), 500, "INTERNAL_ERROR").andReturn();

        assertThat(result.getResponse().getContentAsString()).doesNotContain("secreto interno", "IllegalStateException");
    }

    @Test
    void springHandledErrorsFollowTheSameModel() throws Exception {
        expectProblem(delete("/test/validated"), 405, "METHOD_NOT_ALLOWED");
        expectProblem(post("/test/validated").header(REQUIRED_HEADER, "v")
                .contentType(MediaType.TEXT_PLAIN).content(PAN), 415, "UNSUPPORTED_MEDIA_TYPE");
        expectProblem(get("/test/no-existe"), 404, "RESOURCE_NOT_FOUND");
    }

    @Test
    void propagatesIncomingCorrelationIdToProblem() throws Exception {
        mvc.perform(get("/test/throw/not-found").header(ApiHeaders.CORRELATION_ID, "corr-42"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.correlation_id").value("corr-42"))
                .andExpect(jsonPath("$.instance").value("urn:correlation-id:corr-42"));
    }

    /** Verifica el contrato común a todos los errores. */
    private ResultActions expectProblem(MockHttpServletRequestBuilder request, int status, String code) throws Exception {
        ResultActions actions = mvc.perform(request)
                .andExpect(status().is(status))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(status))
                .andExpect(jsonPath("$.code").value(code))
                .andExpect(jsonPath("$.type").value(
                        "https://api.prisma.example/problems/" + code.toLowerCase().replace('_', '-')))
                .andExpect(jsonPath("$.title", notNullValue()))
                .andExpect(jsonPath("$.detail", notNullValue()))
                .andExpect(jsonPath("$.timestamp").isString());
        MvcResult result = actions.andReturn();
        String headerCorrelationId = result.getResponse().getHeader(ApiHeaders.CORRELATION_ID);
        assertThat(headerCorrelationId).isNotBlank();
        actions.andExpect(jsonPath("$.correlation_id").value(headerCorrelationId));
        return actions;
    }

    record TestRequest(
            @NotNull UUID requestId,
            @NotBlank @Pattern(regexp = "tok_[A-Za-z0-9]+", message = "debe ser un token") String cardId,
            @NotBlank String accountNumber) {
    }

    @RestController
    static class ErrorTestController {

        @PostMapping(path = "/test/validated", consumes = MediaType.APPLICATION_JSON_VALUE)
        void validated(@RequestHeader(REQUIRED_HEADER) String required, @Valid @RequestBody TestRequest request) {
        }

        @PostMapping(path = "/test/domain", consumes = MediaType.APPLICATION_JSON_VALUE)
        void domain(@RequestBody TestRequest request) {
            new CardToken(request.cardId().substring("tok_".length()));
        }

        @GetMapping("/test/items/{id}")
        String item(@PathVariable UUID id) {
            return id.toString();
        }

        @GetMapping("/test/throw/{kind}")
        void raise(@PathVariable String kind) {
            throw switch (kind) {
                case "not-found" -> new NominationNotFoundException(UUID.randomUUID());
                case "idempotency" -> new IdempotencyConflictException(UUID.randomUUID());
                case "duplicate" -> new DuplicateNominationException(new RuntimeException("unique violation"));
                case "optimistic" -> new OptimisticLockingFailureException("version mismatch");
                case "forbidden" -> new AccessDeniedException("sin entidad");
                default -> new IllegalStateException("secreto interno: conexión a db fallida");
            };
        }
    }
}
