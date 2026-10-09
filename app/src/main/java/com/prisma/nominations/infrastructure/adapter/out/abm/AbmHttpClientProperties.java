package com.prisma.nominations.infrastructure.adapter.out.abm;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Timeouts del cliente HTTP de ABM. La base-url no se bindea acá a propósito: se resuelve en cada llamada (ver
 * {@link AbmHttpClient}).
 *
 * @param connectTimeout espera máxima para establecer la conexión
 * @param readTimeout    espera máxima de la respuesta (ABM solo confirma recepción: debería ser rápido)
 */
@ConfigurationProperties("nominations.abm")
record AbmHttpClientProperties(
        @DefaultValue("2s") Duration connectTimeout,
        @DefaultValue("5s") Duration readTimeout) {
}
