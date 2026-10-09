package com.prisma.nominations.infrastructure.adapter.in.web;

import com.prisma.nominations.application.port.in.ReprocessNominationUseCase;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

import static com.prisma.nominations.infrastructure.adapter.in.web.OpenApiExamples.PROBLEM_JSON;
import static com.prisma.nominations.infrastructure.adapter.in.web.OpenApiExamples.PROBLEM_SCHEMA_REF;

/**
 * Endpoints de operación (back-office), separados de la API pública /v1 que usan las entidades.
 * <p>
 * Sin entidad: el operador ve las nominaciones de todas las entidades. Por eso <b>no</b> debe quedar expuesto
 * como la API pública: SecurityConfig exige el scope {@code nominations:operate} y, en producción, se publica
 * solo hacia la red interna.
 */
@Tag(name = NominationOperationsController.TAG,
        description = "Recuperación controlada por un operador (uso interno, no para entidades)")
@RestController
@RequestMapping(NominationOperationsController.BASE_PATH)
public class NominationOperationsController {

    static final String TAG = "Operación";
    static final String BASE_PATH = "/internal/v1/nominations";

    private final ReprocessNominationUseCase reprocessNomination;

    public NominationOperationsController(ReprocessNominationUseCase reprocessNomination) {
        this.reprocessNomination = reprocessNomination;
    }

    @Operation(summary = "Reprocesar una nominación en ABM_TIMEOUT (operador)",
            description = """
                    Recuperación controlada de una nominación que quedó en `ABM_TIMEOUT` (reintentos agotados o sin \
                    respuesta de ABM dentro del SLA): pasa a `RECEIVED` y encola un nuevo pedido a ABM (outbox), en \
                    una única transacción. Responde **202**: el reenvío a ABM es asincrónico.

                    Solo desde `ABM_TIMEOUT`; desde cualquier otro estado responde **409** sin cambios. ABM es \
                    idempotente por `nomination_id`: si el pedido original sí había llegado, no se crea otro alta.

                    **Uso interno**: requiere el scope `nominations:operate`; no está acotado a una entidad.""")
    // X-Correlation-Id (request y response) lo agrega OpenApiConfig a todas las operaciones de /internal.
    @ApiResponse(responseCode = "202", description = "Reproceso aceptado: la nominación vuelve a `RECEIVED`",
            headers = @Header(name = "Location", description = "URI pública de la nominación",
                    schema = @Schema(type = "string", example = "/v1/nominations/7d1e6c2a-4b8f-4a51-9c3e-2f6a8b0d1e23")),
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = NominationResponse.class),
                    examples = @ExampleObject(name = "reprocesada", value = OpenApiExamples.NOMINATION_REPROCESSED)))
    @ApiResponse(responseCode = "400", description = "`INVALID_PARAMETER` (nomination_id no es UUID)",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF),
                    examples = @ExampleObject(name = "INVALID_PARAMETER", value = OpenApiExamples.INVALID_PARAMETER)))
    @ApiResponse(responseCode = "404", description = "`NOMINATION_NOT_FOUND`: no existe",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF),
                    examples = @ExampleObject(name = "NOMINATION_NOT_FOUND", value = OpenApiExamples.NOT_FOUND)))
    @ApiResponse(responseCode = "409",
            description = "`INVALID_STATE_TRANSITION` (no está en `ABM_TIMEOUT`) o `CONCURRENT_MODIFICATION` "
                    + "(cambió durante el reproceso; reintentar)",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF), examples = {
                    @ExampleObject(name = "INVALID_STATE_TRANSITION", value = OpenApiExamples.INVALID_STATE_TRANSITION),
                    @ExampleObject(name = "CONCURRENT_MODIFICATION", value = OpenApiExamples.CONCURRENT_MODIFICATION)}))
    @PostMapping("/{nominationId}/reprocess")
    public ResponseEntity<NominationResponse> reprocess(
            @Parameter(description = "Id de la nominación (UUID)") @PathVariable UUID nominationId) {
        var nomination = reprocessNomination.reprocess(nominationId);
        return ResponseEntity.accepted()
                .location(URI.create(NominationController.BASE_PATH + "/" + nomination.id()))
                .body(NominationResponse.from(nomination));
    }
}
