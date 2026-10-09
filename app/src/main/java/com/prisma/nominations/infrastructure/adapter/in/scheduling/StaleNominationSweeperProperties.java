package com.prisma.nominations.infrastructure.adapter.in.scheduling;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Sweeper del SLA de ABM ({@code nominations.abm.sweeper.*}).
 *
 * @param enabled     activa el ciclo programado (los tests lo apagan e invocan el caso de uso a mano)
 * @param fixedDelay  pausa entre el fin de un ciclo y el inicio del siguiente
 * @param responseSla tiempo máximo en PENDING_ABM sin respuesta de ABM antes de pasar a ABM_TIMEOUT
 * @param batchSize   nominaciones que evalúa cada lote
 */
@ConfigurationProperties("nominations.abm.sweeper")
record StaleNominationSweeperProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("30s") Duration fixedDelay,
        @DefaultValue("15m") Duration responseSla,
        @DefaultValue("100") int batchSize) {
}
