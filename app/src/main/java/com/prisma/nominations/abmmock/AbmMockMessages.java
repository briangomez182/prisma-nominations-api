package com.prisma.nominations.abmmock;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * Contrato de ABM (HTTP y Kafka). Es el "lado ABM": se declara snake_case explícito para no depender de la
 * configuración global de Jackson de la app que lo hospeda.
 */
final class AbmMockMessages {

    private AbmMockMessages() {
    }

    /** Body de {@code POST /abm-mock/v1/nominations}. Los ids viajan como texto: ABM no interpreta su formato. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    record SubmitRequest(String nominationId,
                         String requestId,
                         String correlationId,
                         String entityId,
                         String customerId,
                         String accountId,
                         String cardId,
                         String alias) {

        @Override
        public String toString() {
            // account_id nunca completo en logs.
            return "SubmitRequest[nominationId=%s, requestId=%s, correlationId=%s, accountId=%s]"
                    .formatted(nominationId, requestId, correlationId, maskAccount(accountId));
        }
    }

    /** 202 del alta. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    record Accepted(String abmOperationId, String status) {

        static Accepted of(String abmOperationId) {
            return new Accepted(abmOperationId, "ACCEPTED");
        }
    }

    /** Error simple del "sistema ABM" (no es el ProblemDetail de la API de nominaciones). */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    record Error(String error, String message) {
    }

    /** Respuesta asincrónica publicada en {@code abm.responses.v1}. reason_* solo en REJECTED. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Response(String abmOperationId,
                    String nominationId,
                    String requestId,
                    String correlationId,
                    String result,
                    String reasonCode,
                    String reasonDescription,
                    String respondedAt) {
    }

    /** Últimos 4 dígitos; el resto con asteriscos. */
    static String maskAccount(String accountId) {
        if (accountId == null) {
            return null;
        }
        int visible = Math.min(4, accountId.length());
        return "*".repeat(accountId.length() - visible) + accountId.substring(accountId.length() - visible);
    }
}
