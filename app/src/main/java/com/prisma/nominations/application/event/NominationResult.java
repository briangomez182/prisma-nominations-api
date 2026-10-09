package com.prisma.nominations.application.event;

import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.RejectionReason;

import java.time.Instant;
import java.util.UUID;

/**
 * Resultado final de la nominación, para los consumidores. Sin datos sensibles: cuenta y tarjeta
 * enmascaradas, y el motivo de rechazo normalizado (nunca el código propio de ABM).
 *
 * @param rejectionReason solo si status es REJECTED
 */
public record NominationResult(
        UUID eventId,
        UUID nominationId,
        String entityId,
        UUID requestId,
        NominationStatus status,
        RejectionReason rejectionReason,
        String accountId,
        String cardId,
        String correlationId,
        Instant occurredAt) implements IntegrationEvent {

    public NominationResult {
        if (!status.isFinal()) {
            throw new IllegalArgumentException("Solo se publica el resultado de una nominación en estado final: " + status);
        }
    }

    public static NominationResult of(Nomination n, Instant now) {
        return new NominationResult(UUID.randomUUID(), n.id(), n.entityId(), n.requestId(), n.status(),
                n.rejectionReason(), n.accountId().masked(), n.cardToken().masked(), n.correlationId(), now);
    }

    @Override
    public String eventType() {
        return EventTypes.NOMINATION_RESULT;
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
