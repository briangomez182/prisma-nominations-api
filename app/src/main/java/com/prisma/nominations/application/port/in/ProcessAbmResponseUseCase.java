package com.prisma.nominations.application.port.in;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.ResolutionOutcome;

/**
 * Aplica la respuesta de ABM de forma idempotente. Si cambia el estado, en la misma transacción
 * guarda estado + historial + evento nomination.result en el outbox.
 */
public interface ProcessAbmResponseUseCase {

    /**
     * @throws NominationNotFoundException si la nominación no existe o el request_id no coincide
     */
    ResolutionOutcome process(AbmResponseCommand command);
}
