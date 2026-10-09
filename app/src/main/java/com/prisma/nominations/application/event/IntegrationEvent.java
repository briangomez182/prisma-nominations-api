package com.prisma.nominations.application.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Evento que sale del servicio a través del outbox. Contrato público: dentro de una versión
 * ({@link #schemaVersion()}) solo se admiten cambios aditivos.
 */
public sealed interface IntegrationEvent permits NominationRequested, NominationResult {

    /** Identificador único: es el id de la fila del outbox y la clave de deduplicación en consumidores. */
    UUID eventId();

    /** Tipo lógico, independiente del tópico. Ver {@link EventTypes}. */
    String eventType();

    int schemaVersion();

    /** Agregado al que pertenece: también es la key de Kafka (orden por nominación). */
    UUID nominationId();

    String correlationId();

    Instant occurredAt();
}
