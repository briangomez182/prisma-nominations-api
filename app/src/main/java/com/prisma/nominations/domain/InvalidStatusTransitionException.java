package com.prisma.nominations.domain;

import java.util.UUID;

public class InvalidStatusTransitionException extends RuntimeException {

    public InvalidStatusTransitionException(UUID nominationId, NominationStatus from, NominationStatus to) {
        super("Transición inválida para la nominación %s: %s → %s".formatted(nominationId, from, to));
    }
}
