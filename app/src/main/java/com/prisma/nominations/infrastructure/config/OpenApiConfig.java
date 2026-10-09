package com.prisma.nominations.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.adapter.in.web.GlobalExceptionHandler.ProblemCode;
import io.swagger.v3.core.jackson.ModelResolver;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.HeaderParameter;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Documentación OpenAPI (springdoc): /v3/api-docs, /v3/api-docs.yaml y Swagger UI en /swagger-ui.html.
 * <p>
 * Los headers comunes (X-Entity-Id, X-Correlation-Id) y el schema de error (ProblemDetail, RFC 9457) se
 * definen una sola vez en components y se agregan a cada operación de /v1 desde acá, no en cada endpoint.
 */
@Configuration(proxyBeanMethods = false)
class OpenApiConfig {

    static final String TAG_NOMINATIONS = "Nominaciones";
    private static final String API_PATH_PREFIX = "/v1/";
    private static final String PROBLEM_SCHEMA = "ProblemDetail";

    /**
     * swagger-core usa su propio ObjectMapper y no ve la naming strategy global de Jackson: sin esto los
     * schemas saldrían en camelCase aunque el JSON real sea snake_case. Con el ObjectMapper de Spring
     * el contrato documentado coincide con el serializado.
     */
    @Bean
    ModelResolver modelResolver(ObjectMapper objectMapper) {
        return new ModelResolver(objectMapper);
    }

    @Bean
    OpenAPI nominationsOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("Prisma Nominations API")
                        .version("v1")
                        .description("""
                                Nominación de cuentas y tarjetas para entidades financieras.

                                **Flujo asincrónico**: `POST /v1/nominations` persiste la nominación en estado \
                                `RECEIVED` junto con el pedido a ABM (outbox) y responde **202 Accepted** con \
                                `Location`. ABM resuelve en minutos; el estado se consulta por `GET` (o llega por \
                                el evento `nomination.result.v1`).

                                **Idempotencia**: la clave es `(X-Entity-Id, request_id)`. Repetir el mismo POST \
                                devuelve la nominación original con `Idempotent-Replayed: true`; reutilizar el \
                                `request_id` con otro contenido devuelve 409.

                                **Errores**: RFC 9457 (`application/problem+json`) con `code` estable, \
                                `correlation_id`, `timestamp` y `errors[]` por campo.

                                **Datos sensibles**: `card_id` debe ser un token (se rechaza un PAN); \
                                `account_id` y `card_id` se devuelven enmascarados."""))
                // Server fijo: si no, springdoc pone el host/puerto del request y la spec exportada cambia en cada corrida.
                .servers(List.of(new Server().url("http://localhost:8080").description("Local")))
                .tags(List.of(new Tag().name(TAG_NOMINATIONS)
                        .description("Alta asincrónica y consulta de nominaciones de cuentas y tarjetas")))
                .components(new Components()
                        .addParameters(ApiHeaders.ENTITY_ID, entityIdParameter())
                        .addParameters(ApiHeaders.CORRELATION_ID, correlationIdParameter())
                        .addHeaders(ApiHeaders.CORRELATION_ID, new Header()
                                .description("Correlation id de la operación: el recibido o uno generado")
                                .schema(new StringSchema().example("c0ffee00-1234-4abc-9def-000000000001")))
                        .addSchemas(PROBLEM_SCHEMA, problemSchema()));
    }

    /** Agrega los headers comunes a todas las operaciones de la API (no a actuator ni a la propia doc). */
    @Bean
    OpenApiCustomizer commonHeadersCustomizer() {
        return openApi -> openApi.getPaths().forEach((path, item) -> {
            if (!path.startsWith(API_PATH_PREFIX)) {
                return;
            }
            item.readOperations().forEach(OpenApiConfig::addCommonHeaders);
        });
    }

    private static void addCommonHeaders(Operation operation) {
        // X-Entity-Id ya está como @RequestHeader: se reemplaza por la definición común (con descripción).
        List<Parameter> parameters = new ArrayList<>(operation.getParameters() != null
                ? operation.getParameters() : List.of());
        parameters.removeIf(p -> "header".equals(p.getIn())
                && (ApiHeaders.ENTITY_ID.equalsIgnoreCase(p.getName()) || ApiHeaders.CORRELATION_ID.equalsIgnoreCase(p.getName())));
        parameters.add(0, new Parameter().$ref("#/components/parameters/" + ApiHeaders.CORRELATION_ID));
        parameters.add(0, new Parameter().$ref("#/components/parameters/" + ApiHeaders.ENTITY_ID));
        operation.setParameters(parameters);

        if (operation.getResponses() != null) {
            operation.getResponses().values().forEach(response -> response.addHeaderObject(ApiHeaders.CORRELATION_ID,
                    new Header().$ref("#/components/headers/" + ApiHeaders.CORRELATION_ID)));
        }
    }

    private static Parameter entityIdParameter() {
        return new HeaderParameter()
                .name(ApiHeaders.ENTITY_ID)
                .required(true)
                .description("Entidad financiera que llama. Aísla las nominaciones por entidad y forma parte de "
                        + "la clave de idempotencia. **Transitorio**: en la fase de seguridad sale del JWT.")
                .schema(new StringSchema().maxLength(20).example("ENT01"));
    }

    private static Parameter correlationIdParameter() {
        return new HeaderParameter()
                .name(ApiHeaders.CORRELATION_ID)
                .required(false)
                .description("Opcional. Si falta o no cumple `[A-Za-z0-9._-]{1,64}` se genera uno. "
                        + "Vuelve siempre en la respuesta y viaja a historial, logs y eventos.")
                .schema(new StringSchema().pattern("[A-Za-z0-9._-]{1,64}")
                        .example("c0ffee00-1234-4abc-9def-000000000001"));
    }

    /** RFC 9457 + las extensiones que agrega GlobalExceptionHandler. */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Schema problemSchema() {
        Schema fieldError = new ObjectSchema()
                .addProperty("field", new StringSchema().description("Campo en snake_case").example("card_id"))
                .addProperty("message", new StringSchema().description("Nunca incluye el valor recibido")
                        .example("card_id es obligatorio"));
        return new ObjectSchema()
                .description("Error RFC 9457 (application/problem+json)")
                .addProperty("type", new StringSchema().format("uri")
                        .description("https://api.prisma.example/problems/<code en kebab-case>")
                        .example("https://api.prisma.example/problems/validation-error"))
                .addProperty("title", new StringSchema().example("Datos inválidos"))
                .addProperty("status", new IntegerSchema().example(400))
                .addProperty("detail", new StringSchema().example("La solicitud contiene datos inválidos"))
                .addProperty("instance", new StringSchema().format("uri")
                        .description("urn:correlation-id:<correlation_id>")
                        .example("urn:correlation-id:c0ffee00-1234-4abc-9def-000000000001"))
                .addProperty("code", new StringSchema().description("Código estable para máquinas")
                        ._enum(Arrays.stream(ProblemCode.values()).map(Enum::name).toList()))
                .addProperty("correlation_id", new StringSchema().example("c0ffee00-1234-4abc-9def-000000000001"))
                .addProperty("timestamp", new StringSchema().format("date-time").example("2026-10-08T12:00:00Z"))
                .addProperty("errors", new ArraySchema().items(fieldError)
                        .description("Solo cuando el error es atribuible a campos"));
    }
}
