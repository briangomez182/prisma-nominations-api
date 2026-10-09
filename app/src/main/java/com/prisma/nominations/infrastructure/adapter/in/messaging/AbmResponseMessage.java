package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.time.Instant;
import java.util.UUID;

/**
 * Mensaje de abm.responses.v1 tal como lo produce ABM. Tolerant reader: los campos desconocidos se ignoran.
 *
 * @param result     APPROVED | REJECTED
 * @param reasonCode código propio de ABM, solo si result es REJECTED
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
record AbmResponseMessage(
        String abmOperationId,
        UUID nominationId,
        UUID requestId,
        String correlationId,
        String result,
        String reasonCode,
        String reasonDescription,
        Instant respondedAt) {
}
