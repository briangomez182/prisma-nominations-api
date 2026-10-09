package com.prisma.nominations.application.exception;

import java.util.UUID;

/**
 * El request_id ya fue usado por la entidad con un contenido distinto.
 */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(UUID requestId) {
        super("El request_id " + requestId + " ya fue utilizado con datos distintos");
    }
}
