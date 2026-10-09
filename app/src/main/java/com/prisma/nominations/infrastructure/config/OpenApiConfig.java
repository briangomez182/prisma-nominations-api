package com.prisma.nominations.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.adapter.in.web.AuthenticatedEntity;
import com.prisma.nominations.infrastructure.adapter.in.web.GlobalExceptionHandler.ProblemCode;
import io.swagger.v3.core.jackson.ModelResolver;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.examples.Example;
import io.swagger.v3.oas.models.headers.Header;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.ObjectSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.HeaderParameter;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springdoc.core.utils.SpringDocUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Documentación OpenAPI (springdoc): /v3/api-docs, /v3/api-docs.yaml y Swagger UI en /swagger-ui.html.
 * <p>
 * El header común (X-Correlation-Id), el schema de error (ProblemDetail, RFC 9457), el esquema de seguridad
 * (bearer JWT) y las respuestas 401/403 se definen una sola vez en components y se agregan desde acá a cada
 * operación de /v1 e /internal, con el scope que exige SecurityConfig.
 */
@Configuration(proxyBeanMethods = false)
class OpenApiConfig {

    static final String TAG_NOMINATIONS = "Nominaciones";
    static final String SECURITY_SCHEME = "bearer-jwt";
    private static final String API_PATH_PREFIX = "/v1/";
    private static final String INTERNAL_PATH_PREFIX = "/internal/";
    private static final String PROBLEM_SCHEMA = "ProblemDetail";
    private static final String UNAUTHORIZED_RESPONSE = "Unauthorized";
    private static final String FORBIDDEN_RESPONSE = "Forbidden";
    private static final String EXAMPLE_CORRELATION_ID = "c0ffee00-1234-4abc-9def-000000000001";

    static {
        // La entidad sale del token: el parámetro @AuthenticatedEntity del controller no es un input del cliente.
        SpringDocUtils.getConfig().addAnnotationsToIgnore(AuthenticatedEntity.class);
    }

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

                                **Seguridad**: OAuth2 bearer JWT. La entidad financiera sale del claim `entity_id` \
                                del token (nunca de un header o del body) y cada operación exige un scope: \
                                `nominations:write` (alta), `nominations:read` (consultas) y `nominations:operate` \
                                (endpoints internos). Sin token o con token inválido → **401**; sin el scope o sin \
                                `entity_id` válido → **403**. En la demo el token se genera con `scripts/mint-token.sh`.

                                **Idempotencia**: la clave es `(entity_id, request_id)`. Repetir el mismo POST \
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
                        .addParameters(ApiHeaders.CORRELATION_ID, correlationIdParameter())
                        .addHeaders(ApiHeaders.CORRELATION_ID, new Header()
                                .description("Correlation id de la operación: el recibido o uno generado")
                                .schema(new StringSchema().example(EXAMPLE_CORRELATION_ID)))
                        .addSchemas(PROBLEM_SCHEMA, problemSchema())
                        .addSecuritySchemes(SECURITY_SCHEME, securityScheme())
                        .addResponses(UNAUTHORIZED_RESPONSE, securityResponse(ProblemCode.UNAUTHORIZED,
                                "`UNAUTHORIZED`: falta el token o es inválido (firma, vencimiento, issuer)",
                                "Desafío RFC 6750: `Bearer` o `Bearer error=\"invalid_token\"` (sin el motivo)"))
                        .addResponses(FORBIDDEN_RESPONSE, securityResponse(ProblemCode.FORBIDDEN,
                                "`FORBIDDEN`: el token no tiene el scope requerido o no trae un `entity_id` válido",
                                "Desafío RFC 6750: `Bearer error=\"insufficient_scope\"`")));
    }

    /**
     * A cada operación de la API pública (/v1) y de operación (/internal): X-Correlation-Id en request y response,
     * el requisito de seguridad con su scope y las respuestas 401/403. No toca actuator ni la propia doc.
     */
    @Bean
    OpenApiCustomizer commonHeadersCustomizer() {
        return openApi -> openApi.getPaths().forEach((path, item) -> {
            if (path.startsWith(API_PATH_PREFIX) || path.startsWith(INTERNAL_PATH_PREFIX)) {
                item.readOperationsMap().forEach((method, operation) ->
                        customize(operation, requiredScope(path, method)));
            }
        });
    }

    /** Mismo criterio que SecurityConfig: /internal → operate; en /v1, GET → read y el resto → write. */
    static String requiredScope(String path, PathItem.HttpMethod method) {
        if (path.startsWith(INTERNAL_PATH_PREFIX)) {
            return "nominations:operate";
        }
        return method == PathItem.HttpMethod.GET ? "nominations:read" : "nominations:write";
    }

    private static void customize(Operation operation, String scope) {
        List<Parameter> parameters = new ArrayList<>(operation.getParameters() != null
                ? operation.getParameters() : List.of());
        parameters.removeIf(p -> "header".equals(p.getIn()) && ApiHeaders.CORRELATION_ID.equalsIgnoreCase(p.getName()));
        parameters.add(0, new Parameter().$ref("#/components/parameters/" + ApiHeaders.CORRELATION_ID));
        operation.setParameters(parameters);

        if (operation.getResponses() != null) {
            operation.getResponses().values().forEach(response -> response.addHeaderObject(ApiHeaders.CORRELATION_ID,
                    new Header().$ref("#/components/headers/" + ApiHeaders.CORRELATION_ID)));
            // Después de los headers: una respuesta $ref no admite propiedades propias (el component ya los trae).
            operation.getResponses()
                    .addApiResponse("401", new ApiResponse().$ref("#/components/responses/" + UNAUTHORIZED_RESPONSE))
                    .addApiResponse("403", new ApiResponse().$ref("#/components/responses/" + FORBIDDEN_RESPONSE));
        }
        // OpenAPI 3.1 admite listar los roles/scopes requeridos también en esquemas que no son oauth2.
        operation.setSecurity(List.of(new SecurityRequirement().addList(SECURITY_SCHEME, scope)));
        operation.addExtension("x-required-scope", scope);
    }

    private static SecurityScheme securityScheme() {
        return new SecurityScheme()
                .type(SecurityScheme.Type.HTTP)
                .scheme("bearer")
                .bearerFormat("JWT")
                .description("""
                        JWT emitido por el IdP (OAuth2 client credentials). Claims: `sub` (client id del canal), \
                        `entity_id` (entidad financiera, `[A-Za-z0-9_-]{1,20}`, obligatorio en /v1), `scope` \
                        (separados por espacio: `nominations:write`, `nominations:read`, `nominations:operate`), \
                        `iss` y `exp` (obligatorios). Demo: HS256 con la clave de demo, generado con \
                        `scripts/mint-token.sh`. Producción: firma asimétrica validada contra el JWKS del IdP \
                        corporativo.""");
    }

    /** 401/403 reutilizables: mismo ProblemDetail que el resto de la API + X-Correlation-Id y WWW-Authenticate. */
    private static ApiResponse securityResponse(ProblemCode code, String description, String challenge) {
        Map<String, Object> example = new LinkedHashMap<>();
        example.put("type", code.type().toString());
        example.put("title", code.title());
        example.put("status", code.status().value());
        example.put("detail", code.defaultDetail());
        example.put("instance", "urn:correlation-id:" + EXAMPLE_CORRELATION_ID);
        example.put("code", code.name());
        example.put("correlation_id", EXAMPLE_CORRELATION_ID);
        example.put("timestamp", "2026-10-08T12:00:00Z");

        return new ApiResponse()
                .description(description)
                .content(new Content().addMediaType("application/problem+json", new MediaType()
                        .schema(new Schema<>().$ref("#/components/schemas/" + PROBLEM_SCHEMA))
                        .addExamples(code.name(), new Example().value(example))))
                .addHeaderObject(ApiHeaders.CORRELATION_ID,
                        new Header().$ref("#/components/headers/" + ApiHeaders.CORRELATION_ID))
                .addHeaderObject(HttpHeaders.WWW_AUTHENTICATE,
                        new Header().description(challenge).schema(new StringSchema()));
    }

    private static Parameter correlationIdParameter() {
        return new HeaderParameter()
                .name(ApiHeaders.CORRELATION_ID)
                .required(false)
                .description("Opcional. Si falta o no cumple `[A-Za-z0-9._-]{1,64}` se genera uno. "
                        + "Vuelve siempre en la respuesta y viaja a historial, logs y eventos.")
                .schema(new StringSchema().pattern("[A-Za-z0-9._-]{1,64}")
                        .example(EXAMPLE_CORRELATION_ID));
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
                        .example("urn:correlation-id:" + EXAMPLE_CORRELATION_ID))
                .addProperty("code", new StringSchema().description("Código estable para máquinas")
                        ._enum(Arrays.stream(ProblemCode.values()).map(Enum::name).toList()))
                .addProperty("correlation_id", new StringSchema().example(EXAMPLE_CORRELATION_ID))
                .addProperty("timestamp", new StringSchema().format("date-time").example("2026-10-08T12:00:00Z"))
                .addProperty("errors", new ArraySchema().items(fieldError)
                        .description("Solo cuando el error es atribuible a campos"));
    }
}
