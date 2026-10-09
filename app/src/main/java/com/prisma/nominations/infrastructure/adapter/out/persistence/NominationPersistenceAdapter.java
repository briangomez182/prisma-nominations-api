package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.prisma.nominations.application.exception.DuplicateNominationException;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.StatusChange;
import org.hibernate.exception.ConstraintViolationException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Limit;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
class NominationPersistenceAdapter implements NominationRepository {

    /** Clave de idempotencia en la base (V1__create_nominations_schema.sql). */
    static final String IDEMPOTENCY_CONSTRAINT = "uq_nominations_entity_request";

    private final NominationJpaRepository nominations;
    private final NominationHistoryJpaRepository history;

    NominationPersistenceAdapter(NominationJpaRepository nominations, NominationHistoryJpaRepository history) {
        this.nominations = nominations;
        this.history = history;
    }

    /**
     * Estado e historial en la misma transacción. El flush inmediato hace que el lock optimista y la
     * unicidad fallen acá, y que la versión devuelta sea la definitiva. La violación de la clave de
     * idempotencia se traduce a {@link DuplicateNominationException}; cualquier otra se propaga igual.
     */
    @Override
    @Transactional
    public Nomination save(Nomination nomination) {
        var changes = nomination.pullPendingChanges();
        NominationJpaEntity saved;
        try {
            saved = nominations.saveAndFlush(toEntity(nomination));
        } catch (DataIntegrityViolationException e) {
            if (violates(e, IDEMPOTENCY_CONSTRAINT)) {
                throw new DuplicateNominationException(e);
            }
            throw e;
        }
        history.saveAll(changes.stream().map(NominationPersistenceAdapter::toEntity).toList());
        return toDomain(saved);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Nomination> findById(UUID id) {
        return nominations.findById(id).map(NominationPersistenceAdapter::toDomain);
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<Nomination> findByEntityIdAndRequestId(String entityId, UUID requestId) {
        return nominations.findByEntityIdAndRequestId(entityId, requestId).map(NominationPersistenceAdapter::toDomain);
    }

    @Override
    @Transactional(readOnly = true)
    public List<UUID> findIdsByStatusUpdatedBefore(NominationStatus status, Instant before, int limit) {
        return nominations.findIdsByStatusUpdatedBefore(status, before, Limit.of(limit));
    }

    @Override
    @Transactional(readOnly = true)
    public List<StatusChange> findHistory(UUID nominationId) {
        return history.findByNominationIdOrderByOccurredAtAscIdAsc(nominationId).stream()
                .map(NominationPersistenceAdapter::toDomain)
                .toList();
    }

    private static boolean violates(DataIntegrityViolationException e, String constraint) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ConstraintViolationException cve && constraint.equalsIgnoreCase(cve.getConstraintName())) {
                return true;
            }
        }
        return false;
    }

    private static NominationJpaEntity toEntity(Nomination n) {
        return new NominationJpaEntity(n.id(), n.entityId(), n.requestId(), n.customerId(), n.accountId().value(),
                n.cardToken().value(), n.alias(), n.status(), n.rejectionReason(), n.abmReasonCode(),
                n.correlationId(), n.createdAt(), n.updatedAt(), n.version());
    }

    private static Nomination toDomain(NominationJpaEntity e) {
        return Nomination.rehydrate(e.getId(), e.getEntityId(), e.getRequestId(), e.getCustomerId(),
                new AccountId(e.getAccountId()), new CardToken(e.getCardToken()), e.getAlias(),
                e.getCorrelationId(), e.getCreatedAt(), e.getStatus(), e.getRejectionReason(),
                e.getAbmReasonCode(), e.getUpdatedAt(), e.getVersion());
    }

    private static NominationHistoryJpaEntity toEntity(StatusChange c) {
        return new NominationHistoryJpaEntity(c.nominationId(), c.from(), c.to(), c.source(), c.detail(),
                c.correlationId(), c.occurredAt());
    }

    private static StatusChange toDomain(NominationHistoryJpaEntity e) {
        return new StatusChange(e.getNominationId(), e.getFromStatus(), e.getToStatus(), e.getSource(),
                e.getDetail(), e.getCorrelationId(), e.getOccurredAt());
    }
}
