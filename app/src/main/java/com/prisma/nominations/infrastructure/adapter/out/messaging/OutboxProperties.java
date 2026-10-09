package com.prisma.nominations.infrastructure.adapter.out.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Configuración del relay y de la purga del outbox ({@code nominations.outbox.*}).
 *
 * @param relay     publicación de pendientes en Kafka
 * @param retention cuánto se conserva un evento ya publicado antes de purgarlo
 */
@ConfigurationProperties("nominations.outbox")
public record OutboxProperties(@DefaultValue Relay relay, @DefaultValue("7d") Duration retention) {

    /**
     * @param enabled     activa el ciclo programado (los tests lo apagan y lo invocan a mano)
     * @param fixedDelay  pausa entre el fin de un ciclo y el inicio del siguiente
     * @param batchSize   filas que toma (y bloquea) cada ciclo
     * @param sendTimeout espera máxima del ack de Kafka por evento
     */
    public record Relay(@DefaultValue("true") boolean enabled,
                        @DefaultValue("500ms") Duration fixedDelay,
                        @DefaultValue("100") int batchSize,
                        @DefaultValue("5s") Duration sendTimeout) {
    }
}
