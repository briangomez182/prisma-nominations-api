package com.prisma.nominations.domain.model;

import com.prisma.nominations.domain.enums.NominationStatus;
import com.prisma.nominations.domain.enums.RejectionReason;
import java.util.Objects;

/**
 * Resultado funcional informado por ABM. Las fallas técnicas (timeout, 5xx) no son decisiones:
 * se manejan con reintentos y, agotados, con {@link NominationStatus#ABM_TIMEOUT}.
 */
public sealed interface AbmDecision {

    NominationStatus targetStatus();

    static AbmDecision approved() {
        return new Approved();
    }

    static AbmDecision rejected(RejectionReason reason, String abmReasonCode) {
        return new Rejected(reason, abmReasonCode);
    }

    record Approved() implements AbmDecision {
        @Override
        public NominationStatus targetStatus() {
            return NominationStatus.APPROVED;
        }
    }

    /**
     * @param reason        motivo normalizado, el que se expone a consumidores
     * @param abmReasonCode código original de ABM, solo para auditoría
     */
    record Rejected(RejectionReason reason, String abmReasonCode) implements AbmDecision {
        public Rejected {
            Objects.requireNonNull(reason, "reason");
        }

        @Override
        public NominationStatus targetStatus() {
            return NominationStatus.REJECTED;
        }
    }
}
