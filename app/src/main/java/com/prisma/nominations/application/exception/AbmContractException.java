package com.prisma.nominations.application.exception;

/**
 * ABM rechazó el pedido por contrato (4xx). Reintentarlo no cambia el resultado.
 */
public class AbmContractException extends RuntimeException {

    public AbmContractException(String message) {
        super(message);
    }
}
