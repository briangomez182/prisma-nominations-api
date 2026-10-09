package com.prisma.nominations.application.service;

import com.prisma.nominations.application.port.in.SweepStaleNominationsUseCase;
import com.prisma.nominations.application.port.out.NominationMetrics;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.NominationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Sweeper del SLA de ABM (E6): ABM aceptó el pedido (PENDING_ABM) pero no respondió dentro de {@code responseSla}
 * → ABM_TIMEOUT. No publica resultado (ABM_TIMEOUT no es final: admite respuesta tardía o reproceso).
 * <p>
 * <b>Una TX corta por nominación:</b> la consulta de candidatas no bloquea nada; cada una se relee y se cambia en su
 * propia TX. Así una carrera o una fila problemática no revierte el lote entero ni retiene locks mientras dura.
 * <p>
 * <b>Carrera con la respuesta de ABM y multi-instancia:</b> es seguro correr el sweeper en varias instancias a la
 * vez sin lock distribuido. Cada cambio se guarda con lock optimista ({@code @Version}): si ABM respondió (o
 * otra instancia ya la venció) entre la relectura y el commit, esta TX falla y se descarta sin efectos. A lo sumo
 * una escritura gana por fila y por versión, y la respuesta de ABM nunca se pisa. Si ya no está en PENDING_ABM
 * al releer, simplemente se saltea.
 * <p>
 * Sin {@code @Service}: el SLA y el tamaño de lote vienen de properties de infraestructura, que arma el bean.
 */
public class SweepStaleNominationsService implements SweepStaleNominationsUseCase {

    private static final Logger log = LoggerFactory.getLogger(SweepStaleNominationsService.class);

    private final NominationRepository repository;
    private final TransactionOperations transactions;
    private final Clock clock;
    private final Duration responseSla;
    private final int batchSize;
    private final NominationMetrics metrics;

    public SweepStaleNominationsService(NominationRepository repository, TransactionOperations transactions,
                                        Clock clock, Duration responseSla, int batchSize, NominationMetrics metrics) {
        if (responseSla == null || responseSla.isNegative() || responseSla.isZero()) {
            throw new IllegalArgumentException("responseSla debe ser positivo");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize debe ser positivo");
        }
        this.repository = Objects.requireNonNull(repository);
        this.transactions = Objects.requireNonNull(transactions);
        this.clock = Objects.requireNonNull(clock);
        this.responseSla = responseSla;
        this.batchSize = batchSize;
        this.metrics = Objects.requireNonNull(metrics);
    }

    @Override
    public int sweep() {
        var cutoff = clock.instant().minus(responseSla);
        var candidates = repository.findIdsByStatusUpdatedBefore(NominationStatus.PENDING_ABM, cutoff, batchSize);
        if (candidates.isEmpty()) {
            return 0;
        }
        int timedOut = 0;
        for (UUID id : candidates) {
            try {
                if (timeOut(id)) {
                    timedOut++;
                    metrics.abmTimeout(ChangeSource.SWEEPER);
                }
            } catch (OptimisticLockingFailureException race) {
                // ABM (u otra instancia del sweeper) cambió la nominación entre la relectura y el commit: gana ella.
                log.info("Sweeper: nomination_id={} cambió concurrentemente (respuesta de ABM u otra instancia), "
                        + "no se vence", id);
            }
        }
        log.info("Sweeper: {} de {} candidatas en PENDING_ABM sin respuesta desde antes de {} pasaron a ABM_TIMEOUT",
                timedOut, candidates.size(), cutoff);
        return timedOut;
    }

    /** @return {@code true} si la nominación pasó a ABM_TIMEOUT en esta TX */
    private boolean timeOut(UUID id) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            var nomination = repository.findById(id).orElse(null);
            if (nomination == null || nomination.status() != NominationStatus.PENDING_ABM) {
                return false; // ya resuelta o reprocesada desde la consulta de candidatas
            }
            var pendingSince = nomination.updatedAt();
            nomination.markTimedOut(ChangeSource.SWEEPER, "Sin respuesta de ABM dentro del SLA (" + responseSla + ")",
                    clock.instant());
            repository.save(nomination);
            // Alerta operativa: nominations_abm_timeouts_total{source="SWEEPER"} (se cuenta tras el commit).
            log.warn("Sweeper: nomination_id={} sin respuesta de ABM desde {} (SLA {}): PENDING_ABM -> ABM_TIMEOUT",
                    id, pendingSince, responseSla);
            return true;
        }));
    }
}
