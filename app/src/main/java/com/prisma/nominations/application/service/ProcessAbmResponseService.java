package com.prisma.nominations.application.service;

import com.prisma.nominations.application.event.NominationResult;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.AbmResponseCommand;
import com.prisma.nominations.application.port.in.ProcessAbmResponseUseCase;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.application.port.out.OutboxPort;
import com.prisma.nominations.domain.ResolutionOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;

/**
 * Aplica la respuesta de ABM (D9). La idempotencia la da la máquina de estados: si la nominación ya tiene un
 * resultado final, la respuesta es DUPLICATE (mismo resultado, ACK sin efectos) o CONFLICT (resultado distinto,
 * no se modifica y se alerta). No hace falta una tabla de deduplicación.
 * <p>
 * Estado + historial + evento nomination.result se confirman en una única TX.
 * <p>
 * <b>Respuestas simultáneas:</b> la segunda pierde por lock optimista ({@code @Version}) o por el índice único de
 * nomination.result en el outbox ({@link IllegalStateException}). En ambos casos su TX hizo rollback: se relee en
 * una TX nueva y se reevalúa una vez, lo que da DUPLICATE (o CONFLICT).
 */
@Service
class ProcessAbmResponseService implements ProcessAbmResponseUseCase {

    private static final Logger log = LoggerFactory.getLogger(ProcessAbmResponseService.class);

    private final NominationRepository repository;
    private final OutboxPort outbox;
    private final TransactionOperations transactions;
    private final Clock clock;

    ProcessAbmResponseService(NominationRepository repository, OutboxPort outbox, TransactionOperations transactions,
                              Clock clock) {
        this.repository = repository;
        this.outbox = outbox;
        this.transactions = transactions;
        this.clock = clock;
    }

    @Override
    public ResolutionOutcome process(AbmResponseCommand command) {
        try {
            return apply(command);
        } catch (OptimisticLockingFailureException | IllegalStateException race) {
            log.debug("Respuesta de ABM concurrente para nomination_id={}: se reevalúa ({})",
                    command.nominationId(), race.getClass().getSimpleName());
            return apply(command);
        }
    }

    private ResolutionOutcome apply(AbmResponseCommand command) {
        return transactions.execute(status -> {
            var nomination = repository.findById(command.nominationId())
                    .filter(n -> n.requestId().equals(command.requestId()))
                    .orElseThrow(() -> new NominationNotFoundException(command.nominationId()));
            var previous = nomination.status();
            var now = clock.instant();
            var outcome = nomination.resolve(command.decision(), now);
            switch (outcome) {
                case APPLIED -> {
                    var saved = repository.save(nomination);
                    outbox.append(NominationResult.of(saved, now));
                    log.info("Respuesta de ABM aplicada: nomination_id={} {} -> {} abm_operation_id={}",
                            saved.id(), previous, saved.status(), command.abmOperationId());
                }
                case DUPLICATE -> log.debug("Respuesta de ABM duplicada, sin efectos: nomination_id={} estado={}",
                        nomination.id(), nomination.status());
                // Alerta: ABM contradice un resultado ya publicado. La métrica llega con la fase de observabilidad.
                case CONFLICT -> log.warn("Respuesta de ABM contradictoria, se ignora: nomination_id={} "
                                + "resultado_actual={} resultado_recibido={} abm_operation_id={}",
                        nomination.id(), nomination.status(), command.decision().targetStatus(),
                        command.abmOperationId());
            }
            return outcome;
        });
    }
}
