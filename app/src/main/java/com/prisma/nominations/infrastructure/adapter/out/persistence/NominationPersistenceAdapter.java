package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.StatusChange;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
class NominationPersistenceAdapter implements NominationRepository {

    private final NominationJpaRepository nominations;
    private final NominationHistoryJpaRepository history;

    NominationPersistenceAdapter(NominationJpaRepository nominations, NominationHistoryJpaRepository history) {
        this.nominations = nominations;
        this.history = history;
    }

    /**
     * Estado e historial en la misma transacción. El flush inmediato hace que el lock optimista y la
     * unicidad fallen acá, y que la versión devuelta sea la definitiva.
     */
    @Override
    @Transactional
    public Nomination save(Nomination nomination) {
        var changes = nomination.pullPendingChanges();
        var saved = nominations.saveAndFlush(toEntity(nomination));
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
    public List<StatusChange> findHistory(UUID nominationId) {
        return history.findByNominationIdOrderByOccurredAtAscIdAsc(nominationId).stream()
                .map(NominationPersistenceAdapter::toDomain)
                .toList();
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
