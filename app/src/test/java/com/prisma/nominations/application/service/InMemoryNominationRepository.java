package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.DuplicateNominationException;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.StatusChange;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Repositorio en memoria con la misma regla de unicidad (entity_id, request_id) que la base.
 */
class InMemoryNominationRepository implements NominationRepository {

    private final Map<UUID, Nomination> nominations = new LinkedHashMap<>();
    private final List<StatusChange> history = new ArrayList<>();
    private Nomination concurrentWinner;
    int saves;

    /** Simula una request concurrente que inserta {@code winner} justo antes del próximo save. */
    void insertConcurrentlyBeforeNextSave(Nomination winner) {
        this.concurrentWinner = winner;
    }

    @Override
    public Nomination save(Nomination nomination) {
        saves++;
        if (concurrentWinner != null) {
            store(concurrentWinner);
            concurrentWinner = null;
        }
        var duplicate = nominations.values().stream().anyMatch(n -> !n.id().equals(nomination.id())
                && n.entityId().equals(nomination.entityId()) && n.requestId().equals(nomination.requestId()));
        if (duplicate) {
            throw new DuplicateNominationException(null);
        }
        store(nomination);
        return nomination;
    }

    private void store(Nomination nomination) {
        nominations.put(nomination.id(), nomination);
        history.addAll(nomination.pullPendingChanges());
    }

    @Override
    public Optional<Nomination> findById(UUID id) {
        return Optional.ofNullable(nominations.get(id));
    }

    @Override
    public Optional<Nomination> findByEntityIdAndRequestId(String entityId, UUID requestId) {
        return nominations.values().stream()
                .filter(n -> n.entityId().equals(entityId) && n.requestId().equals(requestId))
                .findFirst();
    }

    @Override
    public List<StatusChange> findHistory(UUID nominationId) {
        return history.stream().filter(c -> c.nominationId().equals(nominationId)).toList();
    }

    int count() {
        return nominations.size();
    }
}
