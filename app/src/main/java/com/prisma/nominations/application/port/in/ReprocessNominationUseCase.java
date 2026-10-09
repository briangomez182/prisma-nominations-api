package com.prisma.nominations.application.port.in;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.InvalidStatusTransitionException;
import com.prisma.nominations.domain.Nomination;

import java.util.UUID;

/**
 * Reproceso controlado (lo decide un operador): ABM_TIMEOUT → RECEIVED y nuevo pedido a ABM por el outbox,
 * en la misma transacción.
 */
public interface ReprocessNominationUseCase {

    /**
     * @throws NominationNotFoundException       si no existe
     * @throws InvalidStatusTransitionException si no está en ABM_TIMEOUT
     */
    Nomination reprocess(UUID nominationId);
}
