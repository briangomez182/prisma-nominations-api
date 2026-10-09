package com.prisma.nominations.application.exception;

/**
 * Lanzada por el repositorio cuando el INSERT viola la unicidad (entity_id, request_id):
 * otra transacción concurrente creó la misma nominación primero.
 */
public class DuplicateNominationException extends RuntimeException {

    public DuplicateNominationException(Throwable cause) {
        super("Ya existe una nominación con el mismo (entity_id, request_id)", cause);
    }
}
