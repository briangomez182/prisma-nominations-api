package com.prisma.nominations.application.exception;

/**
 * Falla técnica al hablar con ABM (timeout, 5xx, error de conexión). Reintentable.
 */
public class AbmUnavailableException extends RuntimeException {

    public AbmUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
