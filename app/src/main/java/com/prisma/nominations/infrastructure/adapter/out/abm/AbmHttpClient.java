package com.prisma.nominations.infrastructure.adapter.out.abm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.port.out.AbmClient;
import com.prisma.nominations.application.port.out.AbmRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.UUID;

/**
 * Cliente HTTP de ABM: {@code POST {base-url}/v1/nominations}. ABM solo confirma recepción (202 con
 * abm_operation_id); la decisión llega después por {@code abm.responses.v1}.
 * <p>
 * <b>base-url por llamada:</b> se lee de {@link Environment} en cada envío, no al arrancar. En la demo el mock de ABM
 * vive en la misma app y la URL usa {@code ${local.server.port}}, que recién existe cuando el servidor levantó (puerto
 * aleatorio en tests). Resolverla al construir el bean tomaría el fallback (8080).
 * <p>
 * <b>Clasificación de errores (D10):</b>
 * <ul>
 *   <li>2xx con abm_operation_id → éxito.</li>
 *   <li>4xx → {@link AbmContractException} (no reintentable). Excepción: 408 y 429 son transitorios.</li>
 *   <li>5xx, 408, 429, timeout de conexión o lectura, conexión rechazada → {@link AbmUnavailableException}.</li>
 *   <li>2xx con cuerpo inválido o sin abm_operation_id → {@link AbmContractException}: ABM violó el contrato.</li>
 * </ul>
 * Los mensajes de las excepciones terminan en logs y en headers del DLT: nunca llevan el cuerpo ni account_id.
 * <p>
 * Usa el {@link HttpClient} del JDK y no {@code HttpURLConnection}, que reintenta un POST en silencio ante ciertos
 * errores de red (riesgo de doble envío). Igual, ABM es idempotente por nomination_id.
 */
@Component
@EnableConfigurationProperties(AbmHttpClientProperties.class)
class AbmHttpClient implements AbmClient, AutoCloseable {

    static final String BASE_URL_PROPERTY = "nominations.abm.base-url";
    static final String SUBMIT_PATH = "/v1/nominations";
    static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    private static final Logger log = LoggerFactory.getLogger(AbmHttpClient.class);

    private final Environment environment;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final RestClient restClient;

    AbmHttpClient(RestClient.Builder builder, Environment environment, ObjectMapper objectMapper,
                  AbmHttpClientProperties props) {
        this.environment = environment;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(props.connectTimeout())
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        var requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(props.readTimeout());
        this.restClient = builder.requestFactory(requestFactory).build();
    }

    /** Cuerpo del POST (contrato de ABM, snake_case). */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record SubmitBody(UUID nominationId, UUID requestId, String correlationId, String entityId, String customerId,
                      String accountId, String cardId, String alias) {

        static SubmitBody from(AbmRequest r) {
            return new SubmitBody(r.nominationId(), r.requestId(), r.correlationId(), r.entityId(), r.customerId(),
                    r.accountId(), r.cardId(), r.alias());
        }

        @Override
        public String toString() {
            return "SubmitBody[nominationId=%s]".formatted(nominationId);
        }
    }

    /** Respuesta 202 de ABM; campos desconocidos se ignoran. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    record AcceptedResponse(String abmOperationId, String status) {
    }

    @Override
    public String submit(AbmRequest request) {
        String uri = baseUrl() + SUBMIT_PATH;
        byte[] body = serialize(request);
        try {
            var post = restClient.post()
                    .uri(uri)
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON);
            if (request.correlationId() != null) {
                post = post.header(CORRELATION_ID_HEADER, request.correlationId());
            }
            String operationId = post.body(body)
                    .exchange((req, res) -> handle(request, res.getStatusCode(), res.getBody().readAllBytes()));
            log.info("ABM aceptó el pedido: nomination_id={}, abm_operation_id={}", request.nominationId(), operationId);
            return operationId;
        } catch (RestClientException e) {
            // Timeout de conexión/lectura, conexión rechazada, corte a mitad de respuesta (ResourceAccessException).
            log.warn("ABM no disponible: nomination_id={}, causa={}", request.nominationId(), rootCause(e));
            throw new AbmUnavailableException("ABM no disponible: " + rootCause(e), e);
        }
    }

    private String handle(AbmRequest request, HttpStatusCode status, byte[] responseBody) {
        if (status.is2xxSuccessful()) {
            return operationId(request, responseBody);
        }
        if (status.is4xxClientError() && !isTransient(status)) {
            log.warn("ABM rechazó el pedido por contrato: nomination_id={}, http_status={}",
                    request.nominationId(), status.value());
            throw new AbmContractException("ABM rechazó el pedido por contrato: HTTP " + status.value());
        }
        log.warn("ABM respondió con falla técnica: nomination_id={}, http_status={}",
                request.nominationId(), status.value());
        throw new AbmUnavailableException("ABM respondió HTTP " + status.value(), null);
    }

    private String operationId(AbmRequest request, byte[] responseBody) {
        AcceptedResponse accepted;
        try {
            accepted = objectMapper.readValue(responseBody, AcceptedResponse.class);
        } catch (IOException | IllegalArgumentException e) {
            // Se atrapa acá (sobre un byte[] solo puede fallar el parseo): si escapa como IOException, RestClient lo
            // convertiría en ResourceAccessException y se clasificaría como "no disponible".
            throw new AbmContractException("Respuesta de ABM no parseable: " + e.getClass().getSimpleName());
        }
        if (accepted == null || accepted.abmOperationId() == null || accepted.abmOperationId().isBlank()) {
            throw new AbmContractException("Respuesta de ABM sin abm_operation_id: nomination_id="
                    + request.nominationId());
        }
        return accepted.abmOperationId();
    }

    /** 408 (timeout del lado de ABM) y 429 (rate limit) no son errores de contrato: reintentar tiene sentido. */
    private static boolean isTransient(HttpStatusCode status) {
        return status.value() == HttpStatus.REQUEST_TIMEOUT.value()
                || status.value() == HttpStatus.TOO_MANY_REQUESTS.value();
    }

    private String baseUrl() {
        String baseUrl = environment.getProperty(BASE_URL_PROPERTY);
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalStateException("Falta la property " + BASE_URL_PROPERTY);
        }
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    private byte[] serialize(AbmRequest request) {
        try {
            return objectMapper.writeValueAsBytes(SubmitBody.from(request));
        } catch (JacksonException e) {
            throw new IllegalStateException("No se pudo serializar el pedido a ABM: nomination_id="
                    + request.nominationId(), e);
        }
    }

    /** Solo tipo y mensaje de la causa raíz (errores de red: no incluyen datos del pedido). */
    private static String rootCause(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() == null
                ? root.getClass().getSimpleName()
                : root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    @Override
    public void close() {
        httpClient.close();
    }
}
