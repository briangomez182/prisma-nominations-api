package com.prisma.nominations.application.port.in;

import java.util.UUID;

/**
 * Datos crudos de una solicitud. Las reglas de formato y de datos sensibles las aplica el dominio
 * al construir los value objects.
 *
 * @param entityId identidad de la entidad financiera (hoy por header; en la fase de seguridad, del JWT)
 */
public record CreateNominationCommand(
        String entityId,
        UUID requestId,
        String customerId,
        String accountId,
        String cardId,
        String alias,
        String correlationId) {
}
