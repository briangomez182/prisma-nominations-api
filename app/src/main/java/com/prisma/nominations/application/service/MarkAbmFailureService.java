package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.MarkAbmFailureUseCase;
import com.prisma.nominations.application.port.out.NominationMetrics;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.enums.ChangeSource;
import com.prisma.nominations.domain.enums.NominationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.util.UUID;

/**
 * Pasa a ABM_TIMEOUT una nominación cuyo envío a ABM agotó la recuperación automática (lo invoca el handler del
 * DLT del ABM Adapter). Solo actúa si sigue abierta (RECEIVED o PENDING_ABM): si ABM ya respondió o ya estaba en
 * ABM_TIMEOUT no hay nada que hacer, lo que vuelve idempotente una reentrega del DLT.
 * <p>
 * No publica evento de resultado: ABM_TIMEOUT no es final (admite respuesta tardía o reproceso).
 * <p>
 * <b>Carrera con la respuesta de ABM:</b> si el consumer de respuestas commitea entre la lectura y el save, el lock
 * optimista falla y se reevalúa una vez en una TX nueva (normalmente da {@code false}: ABM ya respondió).
 */
@Service
class MarkAbmFailureService implements MarkAbmFailureUseCase {

    private static final Logger log = LoggerFactory.getLogger(MarkAbmFailureService.class);

    private final NominationRepository repository;
    private final TransactionOperations transactions;
    private final Clock clock;
    private final NominationMetrics metrics;

    MarkAbmFailureService(NominationRepository repository, TransactionOperations transactions, Clock clock,
                          NominationMetrics metrics) {
        this.repository = repository;
        this.transactions = transactions;
        this.clock = clock;
        this.metrics = metrics;
    }

    @Override
    public boolean markFailed(UUID nominationId, String detail) {
        boolean timedOut;
        try {
            timedOut = apply(nominationId, detail);
        } catch (OptimisticLockingFailureException race) {
            log.debug("Lock optimista al marcar ABM_TIMEOUT nomination_id={}: se reevalúa", nominationId);
            timedOut = apply(nominationId, detail);
        }
        if (timedOut) {
            metrics.abmTimeout(ChangeSource.ABM_ADAPTER);
        }
        return timedOut;
    }

    private boolean apply(UUID nominationId, String detail) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            var nomination = repository.findById(nominationId)
                    .orElseThrow(() -> new NominationNotFoundException(nominationId));
            var previous = nomination.status();
            if (previous != NominationStatus.RECEIVED && previous != NominationStatus.PENDING_ABM) {
                log.info("Falla de ABM sin efecto: nomination_id={} ya está en {} ({})", nominationId, previous, detail);
                return false;
            }
            nomination.markTimedOut(ChangeSource.ABM_ADAPTER, detail, clock.instant());
            repository.save(nomination);
            // Alerta operativa: queda para reproceso controlado.
            log.warn("Nominación pasada a ABM_TIMEOUT: nomination_id={} {} -> ABM_TIMEOUT ({})",
                    nominationId, previous, detail);
            return true;
        }));
    }
}
