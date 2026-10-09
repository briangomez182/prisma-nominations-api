package com.prisma.nominations.application.port.in;

import java.util.UUID;

/**
 * Cierre de un envío a ABM que agotó la recuperación automática (reintentos agotados o error de contrato):
 * la nominación pasa a ABM_TIMEOUT y queda para reproceso controlado. No publica resultado.
 */
public interface MarkAbmFailureUseCase {

    /**
     * @param detail motivo para el historial (sin datos sensibles)
     * @return {@code true} si cambió el estado; {@code false} si ya no estaba abierta (p.ej. ABM respondió)
     */
    boolean markFailed(UUID nominationId, String detail);
}
