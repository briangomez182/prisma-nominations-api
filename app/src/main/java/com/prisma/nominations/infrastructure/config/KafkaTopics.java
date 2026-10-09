package com.prisma.nominations.infrastructure.config;

import com.prisma.nominations.application.event.EventTypes;

/**
 * Nombres de tópicos. La versión va en el nombre: un cambio incompatible crea un tópico .v2 en paralelo.
 */
public final class KafkaTopics {

    public static final String NOMINATION_REQUESTED = "nomination.requested.v1";
    public static final String ABM_RESPONSES = "abm.responses.v1";
    public static final String NOMINATION_RESULT = "nomination.result.v1";

    /** Headers estándar de todo mensaje publicado. */
    public static final String HEADER_EVENT_ID = "event_id";
    public static final String HEADER_EVENT_TYPE = "event_type";
    public static final String HEADER_SCHEMA_VERSION = "schema_version";
    public static final String HEADER_CORRELATION_ID = "correlation_id";

    private KafkaTopics() {
    }

    /** Tópico de destino según el tipo de evento. */
    public static String topicFor(String eventType) {
        return switch (eventType) {
            case EventTypes.NOMINATION_REQUESTED -> NOMINATION_REQUESTED;
            case EventTypes.NOMINATION_RESULT -> NOMINATION_RESULT;
            default -> throw new IllegalArgumentException("Tipo de evento sin tópico: " + eventType);
        };
    }
}
