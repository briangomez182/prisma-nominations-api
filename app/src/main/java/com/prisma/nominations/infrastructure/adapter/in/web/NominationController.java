package com.prisma.nominations.infrastructure.adapter.in.web;

import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.in.GetNominationQuery;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.slf4j.MDC;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.UUID;

import static com.prisma.nominations.infrastructure.adapter.in.web.OpenApiExamples.PROBLEM_JSON;
import static com.prisma.nominations.infrastructure.adapter.in.web.OpenApiExamples.PROBLEM_SCHEMA_REF;

/**
 * API de nominaciones. El alta es asincrónica: responde 202 apenas la nominación queda persistida
 * con su pedido a ABM en el outbox; el resultado se consulta por GET o llega por evento.
 * <p>
 * El X-Entity-Id por header es transitorio: en la fase de seguridad la entidad sale del JWT.
 */
@Tag(name = "Nominaciones")
@RestController
@RequestMapping(NominationController.BASE_PATH)
public class NominationController {

    static final String BASE_PATH = "/v1/nominations";

    private final CreateNominationUseCase createNomination;
    private final GetNominationQuery getNomination;

    public NominationController(CreateNominationUseCase createNomination, GetNominationQuery getNomination) {
        this.createNomination = createNomination;
        this.getNomination = getNomination;
    }

    /**
     * Siempre 202, también en un replay idempotente: la operación sigue siendo asincrónica y el
     * cliente distingue el caso por el header Idempotent-Replayed.
     */
    @Operation(summary = "Solicitar una nominación (asincrónico)",
            description = """
                    Persiste la nominación en `RECEIVED` y encola el pedido a ABM. Responde **202** sin esperar \
                    a ABM; el estado se consulta en `Location`.

                    Idempotente por `(X-Entity-Id, request_id)`: repetir el mismo POST devuelve la misma \
                    nominación (**202** con `Idempotent-Replayed: true`, sin crear ni reenviar nada). El mismo \
                    `request_id` con otro contenido devuelve **409**.""",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true,
                    content = @Content(schema = @Schema(implementation = CreateNominationRequest.class),
                            examples = @ExampleObject(name = "nominacion", summary = "Alta con card_id tokenizado",
                                    value = OpenApiExamples.CREATE_REQUEST))))
    @ApiResponse(responseCode = "202",
            description = "Aceptada: nominación nueva (`Idempotent-Replayed: false`) o replay de una existente "
                    + "(`Idempotent-Replayed: true`, mismo `nomination_id`)",
            headers = {
                    @Header(name = "Location", description = "URI de la nominación",
                            schema = @Schema(type = "string", example = "/v1/nominations/7d1e6c2a-4b8f-4a51-9c3e-2f6a8b0d1e23")),
                    @Header(name = ApiHeaders.IDEMPOTENT_REPLAYED,
                            description = "`true` si se devolvió una nominación ya existente para el mismo request_id",
                            schema = @Schema(type = "boolean"))},
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = NominationResponse.class),
                    examples = @ExampleObject(name = "recibida", value = OpenApiExamples.NOMINATION)))
    @ApiResponse(responseCode = "400",
            description = "`VALIDATION_ERROR` (campos faltantes o inválidos, card_id con forma de PAN), "
                    + "`MALFORMED_REQUEST` (JSON inválido o tipo incorrecto) o `MISSING_HEADER` (falta X-Entity-Id)",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF), examples = {
                    @ExampleObject(name = "VALIDATION_ERROR", value = OpenApiExamples.VALIDATION_ERROR),
                    @ExampleObject(name = "MALFORMED_REQUEST", value = OpenApiExamples.MALFORMED_REQUEST),
                    @ExampleObject(name = "MISSING_HEADER", value = OpenApiExamples.MISSING_HEADER)}))
    @ApiResponse(responseCode = "409",
            description = "`IDEMPOTENCY_CONFLICT`: el request_id ya fue usado por la entidad con otro contenido",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF),
                    examples = @ExampleObject(name = "IDEMPOTENCY_CONFLICT", value = OpenApiExamples.IDEMPOTENCY_CONFLICT)))
    @PostMapping
    public ResponseEntity<NominationResponse> create(@RequestHeader(ApiHeaders.ENTITY_ID) String entityId,
                                                     @Valid @RequestBody CreateNominationRequest request) {
        var command = new CreateNominationCommand(entityId, request.requestId(), request.customerId(),
                request.accountId(), request.cardId(), request.alias(), currentCorrelationId());
        var result = createNomination.create(command);
        var nomination = result.nomination();
        return ResponseEntity.accepted()
                .location(URI.create(BASE_PATH + "/" + nomination.id()))
                .header(ApiHeaders.IDEMPOTENT_REPLAYED, String.valueOf(result.replayed()))
                .body(NominationResponse.from(nomination));
    }

    @Operation(summary = "Consultar el estado de una nominación",
            description = "Solo devuelve nominaciones de la entidad que consulta; las de otra entidad responden 404.")
    @ApiResponse(responseCode = "200", description = "Estado actual de la nominación",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = NominationResponse.class),
                    examples = @ExampleObject(name = "recibida", value = OpenApiExamples.NOMINATION)))
    @ApiResponse(responseCode = "400", description = "`MISSING_HEADER` o `INVALID_PARAMETER` (nomination_id no es UUID)",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF), examples = {
                    @ExampleObject(name = "MISSING_HEADER", value = OpenApiExamples.MISSING_HEADER),
                    @ExampleObject(name = "INVALID_PARAMETER", value = OpenApiExamples.INVALID_PARAMETER)}))
    @ApiResponse(responseCode = "404", description = "`NOMINATION_NOT_FOUND`: no existe o pertenece a otra entidad",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF),
                    examples = @ExampleObject(name = "NOMINATION_NOT_FOUND", value = OpenApiExamples.NOT_FOUND)))
    @GetMapping("/{nominationId}")
    public NominationResponse get(@RequestHeader(ApiHeaders.ENTITY_ID) String entityId,
                                  @Parameter(description = "Id de la nominación (UUID)") @PathVariable UUID nominationId) {
        return NominationResponse.from(getNomination.get(entityId, nominationId));
    }

    @Operation(summary = "Consultar el historial de una nominación",
            description = "Transiciones de estado en orden cronológico, con origen y correlation_id (auditoría).")
    @ApiResponse(responseCode = "200", description = "Historial cronológico",
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = NominationHistoryResponse.class),
                    examples = @ExampleObject(name = "creacion", value = OpenApiExamples.HISTORY)))
    @ApiResponse(responseCode = "400", description = "`MISSING_HEADER` o `INVALID_PARAMETER` (nomination_id no es UUID)",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF), examples = {
                    @ExampleObject(name = "MISSING_HEADER", value = OpenApiExamples.MISSING_HEADER),
                    @ExampleObject(name = "INVALID_PARAMETER", value = OpenApiExamples.INVALID_PARAMETER)}))
    @ApiResponse(responseCode = "404", description = "`NOMINATION_NOT_FOUND`: no existe o pertenece a otra entidad",
            content = @Content(mediaType = PROBLEM_JSON, schema = @Schema(ref = PROBLEM_SCHEMA_REF),
                    examples = @ExampleObject(name = "NOMINATION_NOT_FOUND", value = OpenApiExamples.NOT_FOUND)))
    @GetMapping("/{nominationId}/history")
    public NominationHistoryResponse history(@RequestHeader(ApiHeaders.ENTITY_ID) String entityId,
                                             @Parameter(description = "Id de la nominación (UUID)") @PathVariable UUID nominationId) {
        return NominationHistoryResponse.from(nominationId, getNomination.history(entityId, nominationId));
    }

    /** Lo carga CorrelationIdFilter; el fallback cubre un llamado que no pasó por el filtro. */
    private static String currentCorrelationId() {
        var correlationId = MDC.get(ApiHeaders.CORRELATION_ID_MDC_KEY);
        return correlationId != null ? correlationId : UUID.randomUUID().toString();
    }
}
