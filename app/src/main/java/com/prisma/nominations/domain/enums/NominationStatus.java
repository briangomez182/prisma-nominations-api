package com.prisma.nominations.domain.enums;

import java.util.EnumSet;
import java.util.Set;

/**
 * Estados de una nominación y sus transiciones válidas.
 *
 * <pre>
 *   RECEIVED ──▶ PENDING_ABM ──▶ APPROVED | REJECTED
 *      │              │
 *      └──────────────┴──▶ ABM_TIMEOUT ──▶ APPROVED | REJECTED  (respuesta tardía de ABM)
 *                               └──────▶ RECEIVED               (reproceso controlado)
 * </pre>
 *
 * RECEIVED también puede pasar directo a APPROVED/REJECTED: ABM puede responder antes de que
 * el adapter confirme el envío (carrera entre el adapter y el consumer de respuestas).
 */
public enum NominationStatus {

    /** Persistida y con el pedido a ABM en el outbox. */
    RECEIVED,
    /** ABM aceptó el pedido; se espera su respuesta asincrónica. */
    PENDING_ABM,
    /** Final: ABM aprobó. Publica evento de resultado. */
    APPROVED,
    /** Final: ABM rechazó por regla funcional. Publica evento de resultado con motivo normalizado. */
    REJECTED,
    /** Falla técnica: reintentos agotados o sin respuesta dentro del SLA. No es final: admite respuesta tardía o reproceso. */
    ABM_TIMEOUT;

    public Set<NominationStatus> allowedTransitions() {
        return switch (this) {
            case RECEIVED -> EnumSet.of(PENDING_ABM, APPROVED, REJECTED, ABM_TIMEOUT);
            case PENDING_ABM -> EnumSet.of(APPROVED, REJECTED, ABM_TIMEOUT);
            case ABM_TIMEOUT -> EnumSet.of(APPROVED, REJECTED, RECEIVED);
            case APPROVED, REJECTED -> EnumSet.noneOf(NominationStatus.class);
        };
    }

    public boolean canTransitionTo(NominationStatus target) {
        return allowedTransitions().contains(target);
    }

    public boolean isFinal() {
        return this == APPROVED || this == REJECTED;
    }
}
