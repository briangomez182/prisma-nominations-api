package com.prisma.nominations.domain.enums;

/**
 * Qué pasó al aplicar una respuesta de ABM sobre una nominación.
 */
public enum ResolutionOutcome {
    /** Primera respuesta: cambia el estado y corresponde publicar el evento de resultado. */
    APPLIED,
    /** La nominación ya tenía ese mismo resultado: ACK sin efectos (respuesta duplicada, E7). */
    DUPLICATE,
    /** La nominación ya tenía un resultado distinto: no se modifica y se alerta. */
    CONFLICT
}
