package com.prisma.nominations.application.port.in;

import com.prisma.nominations.domain.Nomination;

/**
 * @param replayed {@code true} si el (entity_id, request_id) ya existía con el mismo contenido:
 *                 se devuelve la nominación original sin crear nada ni reenviar a ABM.
 */
public record CreateNominationResult(Nomination nomination, boolean replayed) {
}
