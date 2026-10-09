package com.prisma.nominations.application.exception;

import java.util.UUID;

public class NominationNotFoundException extends RuntimeException {

    public NominationNotFoundException(UUID nominationId) {
        super("No existe la nominación " + nominationId);
    }
}
