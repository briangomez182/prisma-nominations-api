package com.prisma.nominations.infrastructure.adapter.in.web;

import com.prisma.nominations.domain.model.Nomination;
import com.prisma.nominations.domain.enums.NominationStatus;
import com.prisma.nominations.domain.enums.RejectionReason;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Instant;
import java.util.UUID;

/**
 * Vista pública de una nominación. account_id y card_id van siempre enmascarados y el código
 * original de ABM no se expone: el consumidor solo ve el motivo normalizado.
 *
 * @param rejectionReason solo presente en REJECTED (los null se omiten del JSON)
 */
@Schema(description = "Estado público de una nominación")
public record NominationResponse(
        UUID nominationId,
        UUID requestId,
        NominationStatus status,
        String customerId,
        @Schema(description = "Enmascarado: solo los últimos 4 caracteres", example = "****7654")
        String accountId,
        @Schema(description = "Enmascarado: solo los últimos 4 caracteres", example = "****8d1e")
        String cardId,
        String alias,
        @Schema(description = "Motivo normalizado; solo presente en REJECTED")
        RejectionReason rejectionReason,
        String correlationId,
        Instant createdAt,
        Instant updatedAt) {

    public static NominationResponse from(Nomination nomination) {
        return new NominationResponse(
                nomination.id(),
                nomination.requestId(),
                nomination.status(),
                nomination.customerId(),
                nomination.accountId().masked(),
                nomination.cardToken().masked(),
                nomination.alias(),
                nomination.status() == NominationStatus.REJECTED ? nomination.rejectionReason() : null,
                nomination.correlationId(),
                nomination.createdAt(),
                nomination.updatedAt());
    }
}
