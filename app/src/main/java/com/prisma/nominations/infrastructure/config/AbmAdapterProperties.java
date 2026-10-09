package com.prisma.nominations.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * ABM Adapter: consumidor de {@code nomination.requested.v1} que envía cada pedido a ABM.
 *
 * @param enabled     habilita el listener y su container factory
 * @param groupId     consumer group propio
 * @param maxAttempts intentos totales ante ABM no disponible antes de mandar el mensaje al DLT
 * @param backoff     espera fija entre intentos
 */
@ConfigurationProperties("nominations.abm.adapter")
record AbmAdapterProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("abm-adapter") String groupId,
        @DefaultValue("3") int maxAttempts,
        @DefaultValue("1s") Duration backoff) {
}
