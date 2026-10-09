package com.prisma.nominations.infrastructure.adapter.in.web;

public final class ApiHeaders {

    /** Opcional en el request; si no viene se genera. Siempre vuelve en la respuesta. */
    public static final String CORRELATION_ID = "X-Correlation-Id";
    /** Clave del correlation id en el MDC de logging. */
    public static final String CORRELATION_ID_MDC_KEY = "correlationId";
    /** {@code true} cuando el POST devolvió una nominación ya existente (mismo request_id). */
    public static final String IDEMPOTENT_REPLAYED = "Idempotent-Replayed";

    private ApiHeaders() {
    }
}
