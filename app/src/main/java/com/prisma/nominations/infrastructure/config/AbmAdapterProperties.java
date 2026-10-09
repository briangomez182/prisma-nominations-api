package com.prisma.nominations.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/**
 * ABM Adapter: consumidor de {@code nomination.requested.v1} que envía cada pedido a ABM.
 *
 * @param enabled     habilita el listener, sus tópicos de retry y su container factory
 * @param groupId     consumer group propio (los tópicos de retry y el DLT usan {@code <groupId>-retry-N} y
 *                    {@code <groupId>-dlt})
 * @param retryDelays espera antes de cada reintento no bloqueante ante ABM no disponible: un tópico de retry por
 *                    valor ({@code -retry-0}, {@code -retry-1}, …). Intentos totales = 1 + cantidad de valores.
 */
@ConfigurationProperties("nominations.abm.adapter")
record AbmAdapterProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("abm-adapter") String groupId,
        @DefaultValue({"10s", "1m", "5m"}) List<Duration> retryDelays) {

    AbmAdapterProperties {
        if (retryDelays == null || retryDelays.isEmpty()) {
            throw new IllegalArgumentException("nominations.abm.adapter.retry-delays debe tener al menos un valor");
        }
        if (retryDelays.stream().anyMatch(d -> d.isNegative() || d.isZero())) {
            throw new IllegalArgumentException("nominations.abm.adapter.retry-delays debe tener valores positivos");
        }
        retryDelays = List.copyOf(retryDelays);
    }
}
