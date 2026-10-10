package com.prisma.nominations.application.port.in;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.model.Nomination;
import com.prisma.nominations.domain.model.StatusChange;

import java.util.List;
import java.util.UUID;

/**
 * Consultas acotadas a la entidad: una nominación de otra entidad se reporta como inexistente
 * (no se revela que existe).
 */
public interface GetNominationQuery {

    /** @throws NominationNotFoundException si no existe o pertenece a otra entidad */
    Nomination get(String entityId, UUID nominationId);

    /** Historial cronológico. @throws NominationNotFoundException si no existe o pertenece a otra entidad */
    List<StatusChange> history(String entityId, UUID nominationId);
}
