package com.prisma.nominations.infrastructure.adapter.in.web;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.UUID;

/**
 * Cuerpo del POST. Acá solo se valida presencia y tamaño; las reglas de formato (alfanumérico,
 * rechazo de PAN) las aplica el dominio. Los mensajes nunca incluyen el valor recibido.
 */
@Schema(description = "Solicitud de nominación de una cuenta y una tarjeta")
public record CreateNominationRequest(
        @NotNull(message = "request_id es obligatorio")
        @Schema(description = "Clave de idempotencia generada por el canal; única por entidad",
                example = "0b4a9f2e-6c1d-4e7a-8b3f-5d2c1a0e9f87")
        UUID requestId,

        @NotBlank(message = "customer_id es obligatorio")
        @Size(max = 36, message = "customer_id admite hasta {max} caracteres")
        @Schema(description = "Identificador del cliente en la entidad", example = "CUST-000123")
        String customerId,

        @NotBlank(message = "account_id es obligatorio")
        @Size(max = 34, message = "account_id admite hasta {max} caracteres")
        @Schema(description = "Cuenta a nominar (4 a 34 alfanuméricos). Se devuelve enmascarada",
                example = "0001234567890987654")
        String accountId,

        @NotBlank(message = "card_id es obligatorio")
        @Size(max = 64, message = "card_id admite hasta {max} caracteres")
        @Schema(description = "Token o referencia enmascarada de la tarjeta (4 a 64: alfanuméricos, '_', '-', '*'). "
                + "Un valor con forma de PAN (13 a 19 dígitos) se rechaza", example = "tok_4f9a2c7b8d1e")
        String cardId,

        @Size(max = 50, message = "alias admite hasta {max} caracteres")
        @Pattern(regexp = "[A-Za-z0-9_ ]+", message = "alias solo admite letras, números, espacios y '_'")
        @Schema(description = "Alias opcional de la nominación", example = "CUENTA SUELDO")
        String alias) {
}
