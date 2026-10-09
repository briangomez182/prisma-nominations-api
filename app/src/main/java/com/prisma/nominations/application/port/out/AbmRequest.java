package com.prisma.nominations.application.port.out;

import java.util.UUID;

/**
 * Pedido hacia ABM. Los tres identificadores (nomination_id, request_id, correlation_id) viajan de ida
 * y vuelven en la respuesta: así se reconoce una respuesta ya procesada.
 */
public record AbmRequest(
        UUID nominationId,
        UUID requestId,
        String correlationId,
        String entityId,
        String customerId,
        String accountId,
        String cardId,
        String alias) {

    @Override
    public String toString() {
        // Nunca loguear account_id completo.
        return "AbmRequest[nominationId=%s, requestId=%s, correlationId=%s]".formatted(nominationId, requestId, correlationId);
    }
}
