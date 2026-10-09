package com.prisma.nominations.application.port.in;

/**
 * Detecta nominaciones en PENDING_ABM sin respuesta de ABM dentro del SLA y las pasa a ABM_TIMEOUT.
 */
public interface SweepStaleNominationsUseCase {

    /** @return cantidad de nominaciones pasadas a ABM_TIMEOUT en esta corrida */
    int sweep();
}
