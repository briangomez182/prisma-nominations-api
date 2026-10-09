package com.prisma.nominations.infrastructure.adapter.in.scheduling;

import com.prisma.nominations.application.port.in.SweepStaleNominationsUseCase;
import com.prisma.nominations.application.port.out.NominationMetrics;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.application.service.SweepStaleNominationsService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;

/**
 * Arma el caso de uso del sweeper con el SLA y el lote de las properties. Siempre activo, aunque el ciclo
 * programado esté apagado: el caso de uso se puede invocar a mano (tests, operación).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(StaleNominationSweeperProperties.class)
class StaleNominationSweeperConfig {

    @Bean
    SweepStaleNominationsUseCase sweepStaleNominationsUseCase(NominationRepository repository,
                                                              TransactionOperations transactions, Clock clock,
                                                              StaleNominationSweeperProperties properties,
                                                              NominationMetrics metrics) {
        return new SweepStaleNominationsService(repository, transactions, clock, properties.responseSla(),
                properties.batchSize(), metrics);
    }
}
