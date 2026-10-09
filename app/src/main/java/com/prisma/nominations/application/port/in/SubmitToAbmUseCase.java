package com.prisma.nominations.application.port.in;

import java.util.UUID;

/**
 * Envía a ABM una nominación recibida. Lo invoca el consumer de nomination.requested.v1.
 * <p>
 * El evento puede llegar más de una vez (at-least-once): si la nominación ya no está en RECEIVED,
 * no se vuelve a enviar.
 */
public interface SubmitToAbmUseCase {

    SubmitOutcome submit(UUID nominationId);

    enum SubmitOutcome {
        /** ABM aceptó el pedido y la nominación pasó a PENDING_ABM. */
        SUBMITTED,
        /** La nominación ya no estaba en RECEIVED (reentrega o ABM ya respondió): no se hizo nada. */
        SKIPPED
    }
}
