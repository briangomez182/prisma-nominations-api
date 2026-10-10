package com.prisma.nominations.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Una transición de la máquina de estados. Es la unidad del historial de auditoría.
 *
 * @param from {@code null} en la creación
 */
public record StatusChange(
        UUID nominationId,
        NominationStatus from,
        NominationStatus to,
        ChangeSource source,
        String detail,
        String correlationId,
        Instant occurredAt) {
}
