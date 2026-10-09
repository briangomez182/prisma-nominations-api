package com.prisma.nominations.abmmock;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Configuración del simulador de ABM ({@code nominations.abm-mock.*}).
 *
 * @param enabled       activa el simulador (HTTP + respuesta por Kafka). En producción no existe.
 * @param responseDelay demora de la respuesta asincrónica (en la vida real, minutos)
 * @param duplicateGap  separación entre las dos copias de la respuesta en el escenario DUP (E7)
 * @param slowHttpDelay cuánto tarda en contestar el HTTP en el escenario SLOW: mayor que el read-timeout del cliente
 */
@ConfigurationProperties("nominations.abm-mock")
public record AbmMockProperties(@DefaultValue("false") boolean enabled,
                                @DefaultValue("2s") Duration responseDelay,
                                @DefaultValue("300ms") Duration duplicateGap,
                                @DefaultValue("10s") Duration slowHttpDelay) {
}
