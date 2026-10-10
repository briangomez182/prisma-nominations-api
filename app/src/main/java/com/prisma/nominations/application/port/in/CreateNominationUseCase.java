package com.prisma.nominations.application.port.in;

import com.prisma.nominations.application.exception.IdempotencyConflictException;
import com.prisma.nominations.domain.exception.InvalidNominationDataException;

public interface CreateNominationUseCase {

    /**
     * Crea la nominación o, si el request_id ya fue usado por la entidad, devuelve la existente.
     *
     * @throws InvalidNominationDataException si un dato viola una regla del dominio
     * @throws IdempotencyConflictException   si el request_id ya fue usado con otro contenido
     */
    CreateNominationResult create(CreateNominationCommand command);
}
