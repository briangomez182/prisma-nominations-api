package com.prisma.nominations.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Consumidor de ejemplo de {@code nomination.result.v1} (servicio de notificaciones simulado).
 *
 * @param enabled     habilita el listener y su container factory
 * @param groupId     consumer group propio: avanza sus offsets independientemente de otros consumidores
 * @param maxAttempts intentos totales ante un error transitorio antes de mandar el mensaje al DLT
 * @param backoff     espera fija entre intentos
 */
@ConfigurationProperties("nominations.demo-consumer")
public record DemoConsumerProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("notifications-demo") String groupId,
        @DefaultValue("3") int maxAttempts,
        @DefaultValue("1s") Duration backoff) {
}
