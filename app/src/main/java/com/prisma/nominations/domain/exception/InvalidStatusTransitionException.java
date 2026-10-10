package com.prisma.nominations.domain.exception;

import com.prisma.nominations.domain.enums.NominationStatus;
import java.util.UUID;

public class InvalidStatusTransitionException extends RuntimeException {

    public InvalidStatusTransitionException(UUID nominationId, NominationStatus from, NominationStatus to) {
        super("Transición inválida para la nominación %s: %s → %s".formatted(nominationId, from, to));
    }
}
