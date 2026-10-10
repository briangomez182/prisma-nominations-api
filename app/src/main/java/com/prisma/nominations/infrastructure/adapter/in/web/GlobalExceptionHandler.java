package com.prisma.nominations.infrastructure.adapter.in.web;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.prisma.nominations.application.exception.DuplicateNominationException;
import com.prisma.nominations.application.exception.IdempotencyConflictException;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.exception.InvalidNominationDataException;
import com.prisma.nominations.domain.exception.InvalidStatusTransitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.lang.Nullable;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Modelo de errores único de la API: RFC 9457 ({@code application/problem+json}) para TODOS los errores,
 * los propios y los que resuelve Spring MVC (405, 415, 404 de ruta, etc.).
 * <p>
 * Cada problema lleva, además de los campos estándar:
 * <ul>
 *   <li>{@code code}: identificador estable para máquinas (ver {@link ProblemCode}).</li>
 *   <li>{@code correlation_id}: el mismo que viaja en el header {@code X-Correlation-Id}.</li>
 *   <li>{@code timestamp}: instante UTC del error.</li>
 *   <li>{@code errors}: lista de {@code {field, message}} cuando el error es atribuible a campos.</li>
 * </ul>
 * {@code type} es {@value #PROBLEM_TYPE_BASE} + el code en kebab-case (p.ej. {@code .../validation-error}).
 * {@code instance} es {@code urn:correlation-id:<id>} y no la URI del request (que Spring pondría por
 * defecto): la URI puede traer datos del cliente, p.ej. un PAN en lugar del id de la nominación.
 * <p>
 * Regla de seguridad: ningún {@code detail} ni {@code errors} incluye valores recibidos (pueden ser
 * PAN, cuentas, etc.). Por eso los detalles de Spring se reemplazan por textos fijos y los mensajes de
 * Jackson (que citan el valor) nunca se exponen ni se loguean. 4xx → WARN sin stack; 5xx → ERROR con stack.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    public static final String PROBLEM_TYPE_BASE = "https://api.prisma.example/problems/";

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final Pattern CAMEL_BOUNDARY = Pattern.compile("([a-z0-9])([A-Z])");

    /** Códigos estables expuestos en la propiedad {@code code}. */
    public enum ProblemCode {
        VALIDATION_ERROR(HttpStatus.BAD_REQUEST, "Datos inválidos", "La solicitud contiene datos inválidos"),
        MALFORMED_REQUEST(HttpStatus.BAD_REQUEST, "Solicitud mal formada",
                "El cuerpo de la solicitud no es JSON válido o tiene campos con tipo incorrecto"),
        MISSING_HEADER(HttpStatus.BAD_REQUEST, "Header obligatorio ausente", "Falta un header obligatorio"),
        INVALID_PARAMETER(HttpStatus.BAD_REQUEST, "Parámetro inválido", "Un parámetro de la solicitud tiene formato inválido"),
        BAD_REQUEST(HttpStatus.BAD_REQUEST, "Solicitud inválida", "La solicitud no pudo procesarse"),
        UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "No autenticado", "Se requiere un token de acceso válido"),
        FORBIDDEN(HttpStatus.FORBIDDEN, "Acceso denegado", "El token no tiene permisos para esta operación"),
        NOMINATION_NOT_FOUND(HttpStatus.NOT_FOUND, "Nominación inexistente", "No existe la nominación solicitada"),
        RESOURCE_NOT_FOUND(HttpStatus.NOT_FOUND, "Recurso inexistente", "El recurso solicitado no existe"),
        METHOD_NOT_ALLOWED(HttpStatus.METHOD_NOT_ALLOWED, "Método no permitido", "El método HTTP no está soportado para este recurso"),
        NOT_ACCEPTABLE(HttpStatus.NOT_ACCEPTABLE, "Formato no aceptable", "No se puede producir una respuesta en el formato solicitado"),
        IDEMPOTENCY_CONFLICT(HttpStatus.CONFLICT, "Conflicto de idempotencia",
                "El request_id ya fue utilizado con datos distintos"),
        CONCURRENT_MODIFICATION(HttpStatus.CONFLICT, "Modificación concurrente",
                "El recurso fue modificado por otra operación; reintentar"),
        INVALID_STATE_TRANSITION(HttpStatus.CONFLICT, "Estado no válido para la operación",
                "La nominación no está en un estado que admita esta operación"),
        PAYLOAD_TOO_LARGE(HttpStatus.PAYLOAD_TOO_LARGE, "Solicitud demasiado grande", "La solicitud excede el tamaño permitido"),
        UNSUPPORTED_MEDIA_TYPE(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "Content-Type no soportado",
                "El Content-Type de la solicitud no está soportado"),
        REQUEST_ERROR(HttpStatus.BAD_REQUEST, "Error en la solicitud", "La solicitud no pudo procesarse"),
        SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "Servicio no disponible", "Servicio no disponible temporalmente; reintentar"),
        INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Error interno", "Ocurrió un error inesperado");

        private final HttpStatus status;
        private final String title;
        private final String defaultDetail;

        ProblemCode(HttpStatus status, String title, String defaultDetail) {
            this.status = status;
            this.title = title;
            this.defaultDetail = defaultDetail;
        }

        public HttpStatus status() {
            return status;
        }

        public String title() {
            return title;
        }

        public String defaultDetail() {
            return defaultDetail;
        }

        public URI type() {
            return URI.create(PROBLEM_TYPE_BASE + name().toLowerCase(Locale.ROOT).replace('_', '-'));
        }

        /** Código para errores que resuelve Spring MVC sin handler propio. */
        static ProblemCode forStatus(HttpStatusCode status, Exception ex) {
            if (ex instanceof NoResourceFoundException) {
                return RESOURCE_NOT_FOUND;
            }
            return switch (status.value()) {
                case 400 -> BAD_REQUEST;
                case 401 -> UNAUTHORIZED;
                case 403 -> FORBIDDEN;
                case 404 -> RESOURCE_NOT_FOUND;
                case 405 -> METHOD_NOT_ALLOWED;
                case 406 -> NOT_ACCEPTABLE;
                case 413 -> PAYLOAD_TOO_LARGE;
                case 415 -> UNSUPPORTED_MEDIA_TYPE;
                case 503 -> SERVICE_UNAVAILABLE;
                default -> status.is5xxServerError() ? INTERNAL_ERROR : REQUEST_ERROR;
            };
        }
    }

    /** Error atribuible a un campo; {@code field} en snake_case, tal como lo ve el cliente. */
    public record FieldError(String field, String message) {
    }

    // ---------------------------------------------------------------- errores propios

    @ExceptionHandler(InvalidNominationDataException.class)
    ResponseEntity<Object> handleInvalidNominationData(InvalidNominationDataException ex, WebRequest request) {
        ProblemDetail problem = problem(ProblemCode.VALIDATION_ERROR, null);
        problem.setProperty("errors", List.of(new FieldError(ex.field(), ex.getMessage())));
        return respond(ex, problem, request);
    }

    @ExceptionHandler(NominationNotFoundException.class)
    ResponseEntity<Object> handleNotFound(NominationNotFoundException ex, WebRequest request) {
        return respond(ex, problem(ProblemCode.NOMINATION_NOT_FOUND, null), request);
    }

    @ExceptionHandler({IdempotencyConflictException.class, DuplicateNominationException.class})
    ResponseEntity<Object> handleIdempotencyConflict(RuntimeException ex, WebRequest request) {
        return respond(ex, problem(ProblemCode.IDEMPOTENCY_CONFLICT, null), request);
    }

    /** P.ej. reprocesar una nominación que no está en ABM_TIMEOUT. El detail es fijo: no cita estados ni ids. */
    @ExceptionHandler(InvalidStatusTransitionException.class)
    ResponseEntity<Object> handleInvalidTransition(InvalidStatusTransitionException ex, WebRequest request) {
        return respond(ex, problem(ProblemCode.INVALID_STATE_TRANSITION, null), request);
    }

    @ExceptionHandler(OptimisticLockingFailureException.class)
    ResponseEntity<Object> handleOptimisticLock(OptimisticLockingFailureException ex, WebRequest request) {
        return respond(ex, problem(ProblemCode.CONCURRENT_MODIFICATION, null), request);
    }

    /** Denegación detectada dentro de MVC (p.ej. token sin entidad al resolver {@link AuthenticatedEntity}). */
    @ExceptionHandler(AccessDeniedException.class)
    ResponseEntity<Object> handleAccessDenied(AccessDeniedException ex, WebRequest request) {
        return respond(ex, problem(ProblemCode.FORBIDDEN, null), request);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Object> handleUnexpected(Exception ex, WebRequest request) {
        return respond(ex, problem(ProblemCode.INTERNAL_ERROR, null), request);
    }

    // ---------------------------------------------------------------- errores resueltos por Spring MVC

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        ProblemDetail problem = problem(ProblemCode.VALIDATION_ERROR, null);
        List<FieldError> errors = ex.getBindingResult().getAllErrors().stream()
                .map(error -> error instanceof org.springframework.validation.FieldError fe
                        ? new FieldError(toSnakeCase(fe.getField()), fe.getDefaultMessage())
                        : new FieldError(null, error.getDefaultMessage()))
                .toList();
        problem.setProperty("errors", errors);
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        ProblemDetail problem = problem(ProblemCode.MALFORMED_REQUEST, null);
        String field = invalidFieldPath(ex);
        if (field != null) {
            // Solo el nombre del campo: el mensaje de Jackson cita el valor recibido y no se expone.
            problem.setProperty("errors", List.of(new FieldError(field, "Tipo o formato inválido")));
        }
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleServletRequestBindingException(ServletRequestBindingException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        if (ex instanceof MissingRequestHeaderException missing) {
            ProblemDetail problem = problem(ProblemCode.MISSING_HEADER,
                    "Falta el header obligatorio " + missing.getHeaderName());
            return handleExceptionInternal(ex, problem, headers, status, request);
        }
        return super.handleServletRequestBindingException(ex, headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleTypeMismatch(org.springframework.beans.TypeMismatchException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        ProblemDetail problem = problem(ProblemCode.INVALID_PARAMETER, null);
        if (ex instanceof MethodArgumentTypeMismatchException mismatch) {
            problem.setProperty("errors", List.of(new FieldError(toSnakeCase(mismatch.getName()), "Formato inválido")));
        }
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    /**
     * Punto único por el que pasan todas las respuestas de error: completa las propiedades comunes,
     * reemplaza los detalles de Spring (que pueden citar valores recibidos) y loguea.
     */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, @Nullable Object body,
            HttpHeaders headers, HttpStatusCode statusCode, WebRequest request) {
        ProblemDetail problem = body instanceof ProblemDetail pd && pd.getProperties() != null
                && pd.getProperties().containsKey("code")
                ? pd
                : problem(ProblemCode.forStatus(statusCode, ex), null);
        problem.setStatus(statusCode.value());
        enrich(problem, request);
        logProblem(ex, problem);
        return super.handleExceptionInternal(ex, problem, headers, statusCode, request);
    }

    // ---------------------------------------------------------------- soporte

    /**
     * Problema completo (code, correlation_id, timestamp, instance) para errores que ocurren fuera de Spring MVC,
     * en la cadena de seguridad ({@link SecurityProblemHandler}). Mismo formato que el resto de la API.
     */
    static ProblemDetail securityProblem(ProblemCode code, Exception ex) {
        ProblemDetail problem = problem(code, null);
        String correlationId = MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY);
        problem.setProperty("correlation_id", correlationId);
        problem.setProperty("timestamp", Instant.now());
        problem.setInstance(instance(correlationId));
        logProblem(ex, problem);
        return problem;
    }

    private ResponseEntity<Object> respond(Exception ex, ProblemDetail problem, WebRequest request) {
        return handleExceptionInternal(ex, problem, new HttpHeaders(), HttpStatusCode.valueOf(problem.getStatus()), request);
    }

    private static ProblemDetail problem(ProblemCode code, @Nullable String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(code.status(), detail != null ? detail : code.defaultDetail);
        problem.setType(code.type());
        problem.setTitle(code.title);
        problem.setProperty("code", code.name());
        return problem;
    }

    private static void enrich(ProblemDetail problem, WebRequest request) {
        String correlationId = correlationId(request);
        problem.setProperty("correlation_id", correlationId);
        problem.setProperty("timestamp", Instant.now());
        problem.setInstance(instance(correlationId));
    }

    private static String correlationId(WebRequest request) {
        String fromMdc = MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY);
        if (fromMdc != null) {
            return fromMdc;
        }
        // Sin filtro (no debería pasar): se intenta el header ya puesto en la respuesta.
        if (request instanceof NativeWebRequest nativeRequest) {
            var response = nativeRequest.getNativeResponse(jakarta.servlet.http.HttpServletResponse.class);
            if (response != null && response.getHeader(ApiHeaders.CORRELATION_ID) != null) {
                return response.getHeader(ApiHeaders.CORRELATION_ID);
            }
        }
        return null;
    }

    private static URI instance(@Nullable String correlationId) {
        return correlationId != null ? URI.create("urn:correlation-id:" + correlationId) : URI.create("about:blank");
    }

    private static void logProblem(Exception ex, ProblemDetail problem) {
        Object code = problem.getProperties() != null ? problem.getProperties().get("code") : null;
        if (problem.getStatus() >= 500) {
            log.error("Error {} ({}) atendiendo la solicitud", code, problem.getStatus(), ex);
        } else {
            // Sin ex.getMessage(): puede citar valores recibidos (p.ej. mensajes de Jackson).
            log.warn("Solicitud rechazada: {} ({}) - {}", code, problem.getStatus(), ex.getClass().getSimpleName());
        }
    }

    /** Ruta del campo con problema de tipo/formato según Jackson (ya en snake_case por la naming strategy). */
    private static String invalidFieldPath(HttpMessageNotReadableException ex) {
        if (!(ex.getCause() instanceof MismatchedInputException mismatch) || mismatch.getPath().isEmpty()) {
            return null;
        }
        StringBuilder path = new StringBuilder();
        for (JsonMappingException.Reference ref : mismatch.getPath()) {
            if (ref.getFieldName() != null) {
                if (!path.isEmpty()) {
                    path.append('.');
                }
                path.append(toSnakeCase(ref.getFieldName()));
            } else if (ref.getIndex() >= 0) {
                path.append('[').append(ref.getIndex()).append(']');
            }
        }
        return path.isEmpty() ? null : path.toString();
    }

    /** {@code cardId} → {@code card_id}; respeta rutas anidadas ({@code holder.firstName}, {@code items[0].x}). */
    static String toSnakeCase(String name) {
        return name == null ? null : CAMEL_BOUNDARY.matcher(name).replaceAll("$1_$2").toLowerCase(Locale.ROOT);
    }
}
