package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.SubmitToAbmUseCase;
import com.prisma.nominations.application.port.out.AbmClient;
import com.prisma.nominations.application.port.out.AbmRequest;
import com.prisma.nominations.application.port.out.NominationMetrics;
import com.prisma.nominations.application.port.out.NominationMetrics.SubmissionOutcome;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.model.Nomination;
import com.prisma.nominations.domain.enums.NominationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.util.UUID;

/**
 * Envía a ABM una nominación en RECEIVED y la pasa a PENDING_ABM.
 * <p>
 * <b>Sin transacción durante la llamada HTTP:</b> ABM se llama fuera de toda TX (no se retiene una conexión de
 * base mientras se espera a la red). La confirmación del envío es una TX corta posterior.
 * <p>
 * <b>Por qué no hay doble envío a ABM:</b>
 * <ul>
 *   <li>Antes de llamar se consulta el estado: una reentrega del evento (at-least-once) sobre una nominación que
 *       ya no está en RECEIVED no llama a ABM.</li>
 *   <li>Si la app se cae entre la respuesta de ABM y el COMMIT, la nominación sigue en RECEIVED y la reentrega
 *       vuelve a llamar. Esa ventana no se puede cerrar del lado de la API: la cubre ABM, que es idempotente por
 *       nomination_id (devuelve el mismo abm_operation_id y no crea un segundo alta).</li>
 * </ul>
 * <b>Carrera con la respuesta de ABM:</b> ABM puede responder (por Kafka) antes de que se confirme el envío. Si al
 * confirmar la nominación ya no está en RECEIVED no se guarda nada; si el consumer de respuestas commitea mientras
 * tanto, el lock optimista falla y se reevalúa una vez sobre el estado nuevo.
 */
@Service
class SubmitToAbmService implements SubmitToAbmUseCase {

    private static final Logger log = LoggerFactory.getLogger(SubmitToAbmService.class);

    private final NominationRepository repository;
    private final AbmClient abmClient;
    private final TransactionOperations transactions;
    private final Clock clock;
    private final NominationMetrics metrics;

    SubmitToAbmService(NominationRepository repository, AbmClient abmClient, TransactionOperations transactions,
                       Clock clock, NominationMetrics metrics) {
        this.repository = repository;
        this.abmClient = abmClient;
        this.transactions = transactions;
        this.clock = clock;
        this.metrics = metrics;
    }

    /**
     * @return SUBMITTED si ABM aceptó el pedido (aunque ABM ya hubiera respondido y no quede en PENDING_ABM);
     * SKIPPED si la nominación ya no estaba en RECEIVED y no se llamó a ABM.
     * @throws AbmUnavailableException falla técnica: la política de reintentos, DLT y ABM_TIMEOUT es del
     *                                 ABM Adapter que invoca este caso de uso (tópicos de retry)
     * @throws AbmContractException    ABM rechazó el pedido por contrato: no reintentable
     */
    @Override
    public SubmitOutcome submit(UUID nominationId) {
        var nomination = load(nominationId);
        if (nomination.status() != NominationStatus.RECEIVED) {
            log.info("Envío a ABM omitido: nomination_id={} ya está en {} (reentrega o ABM ya respondió)",
                    nominationId, nomination.status());
            metrics.submission(SubmissionOutcome.SKIPPED);
            return SubmitOutcome.SKIPPED;
        }

        // Fuera de transacción. Las excepciones de ABM se propagan sin tocar el estado (sigue RECEIVED).
        // Cada intento fallido cuenta: la tasa de UNAVAILABLE anticipa la apertura del circuito y los ABM_TIMEOUT.
        String abmOperationId;
        try {
            abmOperationId = abmClient.submit(toRequest(nomination));
        } catch (AbmUnavailableException e) {
            metrics.submission(SubmissionOutcome.UNAVAILABLE);
            throw e;
        } catch (AbmContractException e) {
            metrics.submission(SubmissionOutcome.CONTRACT_ERROR);
            throw e;
        }

        try {
            confirmSent(nominationId, abmOperationId);
        } catch (OptimisticLockingFailureException race) {
            // El consumer de respuestas commiteó entre la lectura y el save: se reevalúa una vez en una TX nueva.
            log.debug("Lock optimista al confirmar el envío de nomination_id={}: se reintenta", nominationId);
            confirmSent(nominationId, abmOperationId);
        }
        metrics.submission(SubmissionOutcome.SUBMITTED);
        return SubmitOutcome.SUBMITTED;
    }

    private void confirmSent(UUID nominationId, String abmOperationId) {
        transactions.executeWithoutResult(status -> {
            var current = load(nominationId);
            if (!current.markSentToAbm(clock.instant())) {
                log.info("ABM respondió antes de confirmar el envío: nomination_id={} ya está en {}, abm_operation_id={}",
                        nominationId, current.status(), abmOperationId);
                return;
            }
            repository.save(current);
            // No hay columna para el abm_operation_id: queda en el log para trazabilidad con ABM.
            log.info("Nominación enviada a ABM: nomination_id={} abm_operation_id={}", nominationId, abmOperationId);
        });
    }

    private Nomination load(UUID nominationId) {
        return repository.findById(nominationId).orElseThrow(() -> new NominationNotFoundException(nominationId));
    }

    private static AbmRequest toRequest(Nomination n) {
        return new AbmRequest(n.id(), n.requestId(), n.correlationId(), n.entityId(), n.customerId(),
                n.accountId().value(), n.cardToken().value(), n.alias());
    }
}
