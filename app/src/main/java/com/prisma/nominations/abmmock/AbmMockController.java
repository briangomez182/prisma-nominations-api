package com.prisma.nominations.abmmock;

import com.prisma.nominations.abmmock.AbmMockEngine.Outcome;
import com.prisma.nominations.abmmock.AbmMockMessages.Accepted;
import com.prisma.nominations.abmmock.AbmMockMessages.Error;
import com.prisma.nominations.abmmock.AbmMockMessages.SubmitRequest;
import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * API HTTP del simulador de ABM: {@code POST /abm-mock/v1/nominations}. Escenarios en {@link AbmMockEngine}.
 * <p>
 * Sus errores son respuestas simples "de ABM" (no el ProblemDetail de la API de nominaciones): los arma este
 * controller, y el {@code @ExceptionHandler} local tiene prioridad sobre el {@code @RestControllerAdvice} global.
 * Oculto en OpenAPI: no es parte de la API publicada.
 */
@Hidden
@RestController
@RequestMapping(path = "/abm-mock", produces = MediaType.APPLICATION_JSON_VALUE)
@ConditionalOnProperty(prefix = "nominations.abm-mock", name = "enabled", havingValue = "true")
class AbmMockController {

    private final AbmMockEngine engine;

    AbmMockController(AbmMockEngine engine) {
        this.engine = engine;
    }

    /** El X-Correlation-Id del header lo procesa el filtro de la app; ABM usa el que viene en el body. */
    @PostMapping(path = "/v1/nominations", consumes = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<Object> submit(@RequestBody(required = false) SubmitRequest request) {
        Outcome outcome = engine.submit(request);
        return switch (outcome) {
            case Outcome.Accepted accepted -> ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(Accepted.of(accepted.abmOperationId()));
            case Outcome.Invalid invalid -> ResponseEntity.badRequest()
                    .body(new Error("INVALID_REQUEST", "Campos obligatorios ausentes: "
                            + String.join(", ", invalid.missingFields())));
            case Outcome.Unavailable unavailable -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new Error("SERVICE_UNAVAILABLE", "ABM no disponible"));
        };
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<Error> malformed() {
        return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON)
                .body(new Error("MALFORMED_REQUEST", "El cuerpo no es JSON válido"));
    }
}
