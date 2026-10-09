package com.prisma.nominations.application.port.out;

import com.prisma.nominations.application.event.IntegrationEvent;

/**
 * Transactional Outbox: el evento se guarda en la transacción en curso, junto con el cambio de estado.
 * Publicarlo en Kafka es responsabilidad del relay, nunca del caso de uso.
 */
public interface OutboxPort {

    /**
     * Debe llamarse dentro de una transacción activa: si la transacción hace rollback, el evento
     * tampoco existe. Falla si ya hay un {@code nomination.result} para la misma nominación.
     */
    void append(IntegrationEvent event);
}
