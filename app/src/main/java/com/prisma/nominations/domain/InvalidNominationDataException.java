package com.prisma.nominations.domain;

/**
 * Dato de entrada que viola una regla del dominio (formato, dato sensible sin tokenizar, etc.).
 */
public class InvalidNominationDataException extends RuntimeException {

    private final String field;

    public InvalidNominationDataException(String field, String message) {
        super(message);
        this.field = field;
    }

    public String field() {
        return field;
    }
}
