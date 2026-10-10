package com.prisma.nominations.application.event;

import com.prisma.nominations.domain.model.Nomination;

import java.time.Instant;
import java.util.UUID;

/**
 * Pedido a ABM. Lleva account_id completo porque ABM lo necesita: viaja solo por un tópico interno
 * (en producción, con ACL y cifrado). card_id siempre es un token.
 */
public record NominationRequested(
        UUID eventId,
        UUID nominationId,
        String entityId,
        UUID requestId,
        String customerId,
        String accountId,
        String cardId,
        String alias,
        String correlationId,
        Instant occurredAt) implements IntegrationEvent {

    public static NominationRequested of(Nomination n, Instant now) {
        return new NominationRequested(UUID.randomUUID(), n.id(), n.entityId(), n.requestId(), n.customerId(),
                n.accountId().value(), n.cardToken().value(), n.alias(), n.correlationId(), now);
    }

    @Override
    public String eventType() {
        return EventTypes.NOMINATION_REQUESTED;
    }

    @Override
    public int schemaVersion() {
        return 1;
    }
}
