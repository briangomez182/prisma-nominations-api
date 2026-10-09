package com.prisma.nominations.application.event;

public final class EventTypes {

    /** Pedido de nominación hacia ABM. Interno: lo consume el ABM Adapter. */
    public static final String NOMINATION_REQUESTED = "nomination.requested";
    /**
     * Resultado final (APPROVED / REJECTED). Público. La base garantiza uno solo por nominación
     * (índice único parcial en outbox_events).
     */
    public static final String NOMINATION_RESULT = "nomination.result";

    private EventTypes() {
    }
}
