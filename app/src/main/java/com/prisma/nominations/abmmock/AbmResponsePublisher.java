package com.prisma.nominations.abmmock;

/**
 * Salida del simulador: publica una respuesta ya serializada. Abstraído para testear el motor sin Kafka.
 */
@FunctionalInterface
interface AbmResponsePublisher {

    /**
     * @param key           nomination_id (key del mensaje)
     * @param correlationId va en el header {@code correlation_id}
     * @param json          payload JSON
     */
    void publish(String key, String correlationId, String json);
}
