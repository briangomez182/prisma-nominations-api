package com.prisma.nominations.application.port.in;

import com.prisma.nominations.domain.AbmDecision;

import java.util.UUID;

/**
 * Respuesta de ABM ya traducida: el motivo de rechazo viene normalizado en la decisión.
 */
public record AbmResponseCommand(
        UUID nominationId,
        UUID requestId,
        String correlationId,
        String abmOperationId,
        AbmDecision decision) {
}
