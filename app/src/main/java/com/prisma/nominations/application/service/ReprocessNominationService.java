package com.prisma.nominations.application.service;

import com.prisma.nominations.application.event.NominationRequested;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.ReprocessNominationUseCase;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.application.port.out.OutboxPort;
import com.prisma.nominations.domain.InvalidStatusTransitionException;
import com.prisma.nominations.domain.Nomination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.util.UUID;

/**
 * Reproceso controlado por un operador (E6, recuperación): ABM_TIMEOUT → RECEIVED y un nuevo
 * {@code nomination.requested} en el outbox, en una única TX. Desde ahí el circuito normal (relay → ABM Adapter)
 * vuelve a enviarla; ABM es idempotente por nomination_id, así que si el pedido original sí había llegado no se
 * crea un segundo alta.
 * <p>
 * La regla "solo desde ABM_TIMEOUT" es del dominio: desde otro estado lanza
 * {@link InvalidStatusTransitionException} y no se escribe nada.
 * <p>
 * <b>Carrera:</b> si una respuesta tardía de ABM commitea entre la lectura y el save, el lock optimista falla; se
 * reintenta una vez sobre el estado nuevo (que ya no es ABM_TIMEOUT → transición inválida, sin efectos).
 */
@Service
class ReprocessNominationService implements ReprocessNominationUseCase {

    private static final Logger log = LoggerFactory.getLogger(ReprocessNominationService.class);

    private final NominationRepository repository;
    private final OutboxPort outbox;
    private final TransactionOperations transactions;
    private final Clock clock;

    ReprocessNominationService(NominationRepository repository, OutboxPort outbox, TransactionOperations transactions,
                               Clock clock) {
        this.repository = repository;
        this.outbox = outbox;
        this.transactions = transactions;
        this.clock = clock;
    }

    @Override
    public Nomination reprocess(UUID nominationId) {
        try {
            return apply(nominationId);
        } catch (OptimisticLockingFailureException race) {
            log.debug("Reproceso concurrente con otro cambio para nomination_id={}: se reevalúa", nominationId);
            return apply(nominationId);
        }
    }

    private Nomination apply(UUID nominationId) {
        return transactions.execute(status -> {
            var nomination = repository.findById(nominationId)
                    .orElseThrow(() -> new NominationNotFoundException(nominationId));
            var previous = nomination.status();
            var now = clock.instant();
            try {
                nomination.reprocess(now);
            } catch (InvalidStatusTransitionException invalid) {
                log.warn("Reproceso rechazado: nomination_id={} estado={} (solo se reprocesa desde ABM_TIMEOUT)",
                        nominationId, previous);
                throw invalid;
            }
            var saved = repository.save(nomination);
            outbox.append(NominationRequested.of(saved, now));
            // Auditoría: el historial guarda source=OPERATOR; el operador concreto llega con la fase de seguridad.
            log.warn("Reproceso manual: nomination_id={} {} -> {}, nuevo pedido a ABM encolado",
                    saved.id(), previous, saved.status());
            return saved;
        });
    }
}
