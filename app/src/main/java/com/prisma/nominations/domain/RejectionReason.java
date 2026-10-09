package com.prisma.nominations.domain;

/**
 * Motivo de rechazo funcional normalizado. Los códigos propios de ABM se traducen a estos valores
 * en el adapter, para que los consumidores no dependan del vocabulario de ABM.
 */
public enum RejectionReason {
    INVALID_ACCOUNT,
    INVALID_CARD,
    CARD_NOT_ELIGIBLE,
    ACCOUNT_BLOCKED,
    ALREADY_NOMINATED,
    OTHER
}
