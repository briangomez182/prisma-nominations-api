package com.prisma.nominations.application.service;

import com.prisma.nominations.application.event.NominationRequested;
import com.prisma.nominations.application.exception.DuplicateNominationException;
import com.prisma.nominations.application.exception.IdempotencyConflictException;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationResult;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.out.NominationMetrics;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.application.port.out.OutboxPort;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.Nomination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.util.Objects;

/**
 * Alta idempotente por (entity_id, request_id).
 * <p>
 * La lectura previa resuelve los reintentos secuenciales sin abrir transacción de escritura. La carrera
 * entre requests simultáneas la decide la constraint UNIQUE de la base: la que pierde recibe
 * {@link DuplicateNominationException}, relee y responde como replay (o conflicto).
 */
@Service
class NominationCommandService implements CreateNominationUseCase {

    private static final Logger log = LoggerFactory.getLogger(NominationCommandService.class);

    private final NominationRepository repository;
    private final OutboxPort outbox;
    private final TransactionOperations transactions;
    private final Clock clock;
    private final NominationMetrics metrics;

    NominationCommandService(NominationRepository repository, OutboxPort outbox, TransactionOperations transactions,
                             Clock clock, NominationMetrics metrics) {
        this.repository = repository;
        this.outbox = outbox;
        this.transactions = transactions;
        this.clock = clock;
        this.metrics = metrics;
    }

    @Override
    public CreateNominationResult create(CreateNominationCommand command) {
        var existing = repository.findByEntityIdAndRequestId(command.entityId(), command.requestId());
        if (existing.isPresent()) {
            return replay(existing.get(), command);
        }

        // Value objects primero: un dato inválido falla antes de tocar la base.
        var nomination = Nomination.receive(command.entityId(), command.requestId(), command.customerId(),
                new AccountId(command.accountId()), new CardToken(command.cardId()), command.alias(),
                command.correlationId(), clock.instant());
        try {
            // Transacción explícita (no @Transactional) para no depender de self-invocation.
            // Nominación y pedido a ABM (outbox) se confirman juntos o ninguno: sin commit no hay evento.
            var saved = transactions.execute(status -> {
                var created = repository.save(nomination);
                outbox.append(NominationRequested.of(created, clock.instant()));
                return created;
            });
            log.info("Nominación creada nomination_id={} request_id={}", saved.id(), saved.requestId());
            metrics.created(saved.entityId());
            return new CreateNominationResult(saved, false);
        } catch (DuplicateNominationException race) {
            // Otra request concurrente insertó primero. La TX fallida ya hizo rollback: se relee en una nueva.
            var winner = repository.findByEntityIdAndRequestId(command.entityId(), command.requestId())
                    .orElseThrow(() -> race);
            return replay(winner, command);
        }
    }

    private CreateNominationResult replay(Nomination existing, CreateNominationCommand command) {
        if (!sameContent(existing, command)) {
            log.warn("Conflicto de idempotencia nomination_id={} request_id={}", existing.id(), command.requestId());
            metrics.idempotencyConflict(existing.entityId());
            throw new IdempotencyConflictException(command.requestId());
        }
        log.info("Replay de nominación existente nomination_id={} request_id={}", existing.id(), command.requestId());
        metrics.replayed(existing.entityId());
        return new CreateNominationResult(existing, true);
    }

    private static boolean sameContent(Nomination existing, CreateNominationCommand command) {
        return Objects.equals(existing.customerId(), command.customerId())
                && Objects.equals(existing.accountId().value(), command.accountId())
                && Objects.equals(existing.cardToken().value(), command.cardId())
                && Objects.equals(existing.alias(), command.alias());
    }
}
