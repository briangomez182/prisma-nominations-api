package com.prisma.nominations.abmmock;

import java.util.Locale;

/**
 * Comportamiento del simulador, elegido de forma determinística por el {@code card_id} (ver {@link AbmMockEngine}).
 * Si el card_id contiene más de una marca, gana la primera en este orden: FAIL, SLOW, SILENT, DUP, REJECT.
 */
public enum AbmMockScenario {
    /** HTTP 503 siempre: ABM caído (E6, reintentos / circuit breaker). */
    FAIL,
    /** El HTTP tarda {@code slow-http-delay} antes del 202 (E6, timeout del cliente). Luego aprueba. */
    SLOW,
    /** 202 pero nunca publica respuesta (E6, la resuelve el sweeper). */
    SILENT,
    /** Aprueba y publica la MISMA respuesta dos veces (E7). */
    DUPLICATE,
    /** Rechazo funcional con código de ABM (E5). */
    REJECT,
    /** Aprueba (E4). */
    APPROVE;

    static AbmMockScenario of(String cardId) {
        String id = cardId == null ? "" : cardId.toUpperCase(Locale.ROOT);
        if (id.contains("FAIL")) {
            return FAIL;
        }
        if (id.contains("SLOW")) {
            return SLOW;
        }
        if (id.contains("SILENT")) {
            return SILENT;
        }
        if (id.contains("DUP")) {
            return DUPLICATE;
        }
        if (id.contains("REJECT")) {
            return REJECT;
        }
        return APPROVE;
    }

    /** Motivos de rechazo de ABM (vocabulario propio de ABM; el adapter los normaliza). */
    record Rejection(String code, String description) {

        private static final Rejection DEFAULT = new Rejection("ABM-051", "Cuenta bloqueada");

        /** Código según el sufijo del card_id: REJECT_010, _020, _030, _060; sin sufijo conocido → ABM-051. */
        static Rejection of(String cardId) {
            String id = cardId == null ? "" : cardId.toUpperCase(Locale.ROOT);
            if (id.contains("REJECT_010")) {
                return new Rejection("ABM-010", "Cuenta inexistente o inválida");
            }
            if (id.contains("REJECT_020")) {
                return new Rejection("ABM-020", "Tarjeta inexistente o inválida");
            }
            if (id.contains("REJECT_030")) {
                return new Rejection("ABM-030", "Tarjeta no habilitada para nominación");
            }
            if (id.contains("REJECT_060")) {
                return new Rejection("ABM-060", "La cuenta ya está nominada a la tarjeta");
            }
            return DEFAULT;
        }
    }
}
