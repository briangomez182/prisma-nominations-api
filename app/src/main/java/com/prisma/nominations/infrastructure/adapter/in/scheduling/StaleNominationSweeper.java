package com.prisma.nominations.infrastructure.adapter.in.scheduling;

import com.prisma.nominations.application.port.in.SweepStaleNominationsUseCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Ciclo programado del sweeper del SLA de ABM. {@code fixedDelay}: un ciclo no arranca hasta que terminó el
 * anterior. Si un lote vino lleno se encadena otro, con tope, para vaciar un backlog (p.ej. tras una caída de ABM).
 * <p>
 * Multi-instancia: puede correr en todas las réplicas a la vez; el lock optimista garantiza que cada nominación
 * cambie una sola vez (ver {@code SweepStaleNominationsService}). Se apaga con
 * {@code nominations.abm.sweeper.enabled=false}.
 */
@Component
@ConditionalOnProperty(name = "nominations.abm.sweeper.enabled", havingValue = "true", matchIfMissing = true)
class StaleNominationSweeper {

    private static final Logger log = LoggerFactory.getLogger(StaleNominationSweeper.class);

    /** Tope de lotes encadenados por ciclo: cede el hilo del scheduler a otras tareas. */
    static final int MAX_BATCHES_PER_RUN = 10;

    private final SweepStaleNominationsUseCase sweep;
    private final int batchSize;

    StaleNominationSweeper(SweepStaleNominationsUseCase sweep, StaleNominationSweeperProperties properties) {
        this.sweep = sweep;
        this.batchSize = properties.batchSize();
    }

    @Scheduled(fixedDelayString = "${nominations.abm.sweeper.fixed-delay:30s}")
    void run() {
        try {
            for (int i = 0; i < MAX_BATCHES_PER_RUN; i++) {
                if (sweep.sweep() < batchSize) {
                    return;
                }
            }
        } catch (RuntimeException e) {
            // Base caída u otra falla del ciclo: el próximo reintenta, el scheduler no debe morir.
            log.error("Sweeper: falló el ciclo ({})", e.getClass().getSimpleName(), e);
        }
    }
}
