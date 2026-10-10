package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.prisma.nominations.domain.enums.RejectionReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Traduce los códigos de rechazo de ABM al motivo normalizado que ven los consumidores: el vocabulario de ABM no
 * sale de este adapter. Un código desconocido no rompe el flujo: se informa como OTHER (el código original queda
 * en la nominación para auditoría) y se avisa para agregarlo a la tabla.
 */
final class AbmReasonCodeMapper {

    private static final Logger log = LoggerFactory.getLogger(AbmReasonCodeMapper.class);

    private static final Map<String, RejectionReason> CODES = Map.of(
            "ABM-010", RejectionReason.INVALID_ACCOUNT,
            "ABM-020", RejectionReason.INVALID_CARD,
            "ABM-030", RejectionReason.CARD_NOT_ELIGIBLE,
            "ABM-051", RejectionReason.ACCOUNT_BLOCKED,
            "ABM-060", RejectionReason.ALREADY_NOMINATED);

    private AbmReasonCodeMapper() {
    }

    static RejectionReason toRejectionReason(String abmReasonCode) {
        var reason = abmReasonCode == null ? null : CODES.get(abmReasonCode.trim());
        if (reason == null) {
            log.warn("Código de rechazo de ABM desconocido: '{}', se normaliza como OTHER", printable(abmReasonCode));
            return RejectionReason.OTHER;
        }
        return reason;
    }

    /** Viene de afuera: sin saltos de línea ni caracteres raros en el log (log injection). */
    private static String printable(String value) {
        if (value == null) {
            return null;
        }
        var safe = value.replaceAll("[^A-Za-z0-9._-]", "?");
        return safe.length() > 32 ? safe.substring(0, 32) + "…" : safe;
    }
}
