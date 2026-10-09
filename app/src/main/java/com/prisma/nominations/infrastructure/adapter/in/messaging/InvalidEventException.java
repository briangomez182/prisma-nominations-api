package com.prisma.nominations.infrastructure.adapter.in.messaging;

/**
 * Mensaje que nunca se va a poder procesar (poison pill): payload no parseable o sin campos obligatorios.
 * El error handler no lo reintenta y lo manda directo al DLT. El mensaje no incluye el payload.
 */
public class InvalidEventException extends RuntimeException {

    InvalidEventException(String message) {
        super(message);
    }

    InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
