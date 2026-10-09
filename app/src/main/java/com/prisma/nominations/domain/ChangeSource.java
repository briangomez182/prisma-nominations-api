package com.prisma.nominations.domain;

/**
 * Componente que originó un cambio de estado. Queda en la auditoría.
 */
public enum ChangeSource {
    API,
    ABM_ADAPTER,
    ABM_RESPONSE,
    SWEEPER,
    OPERATOR
}
