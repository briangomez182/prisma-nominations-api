package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.DuplicateNominationException;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.StatusChange;
import org.springframework.dao.OptimisticLockingFailureException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Repositorio en memoria con la misma regla de unicidad (entity_id, request_id) que la base.
 * <p>
 * Con {@link #withOptimisticLocking()} se comporta como la base real: las lecturas devuelven copias (modificar una
 * nominación sin guardarla no cambia lo almacenado) y el save valida y avanza la versión (lock optimista).
 */
class InMemoryNominationRepository implements NominationRepository {

    private final Map<UUID, Nomination> nominations = new LinkedHashMap<>();
    private final List<StatusChange> history = new ArrayList<>();
    private final boolean optimisticLocking;
    private Nomination concurrentWinner;
    private Runnable beforeNextSave;
    int saves;

    InMemoryNominationRepository() {
        this(false);
    }

    private InMemoryNominationRepository(boolean optimisticLocking) {
        this.optimisticLocking = optimisticLocking;
    }

    static InMemoryNominationRepository withOptimisticLocking() {
        return new InMemoryNominationRepository(true);
    }

    /** Ejecuta {@code action} (p.ej. otra transacción que commitea) al comienzo del próximo save. */
    void beforeNextSave(Runnable action) {
        this.beforeNextSave = action;
    }

    /** Simula una request concurrente que inserta {@code winner} justo antes del próximo save. */
    void insertConcurrentlyBeforeNextSave(Nomination winner) {
        this.concurrentWinner = winner;
    }

    @Override
    public Nomination save(Nomination nomination) {
        saves++;
        if (beforeNextSave != null) {
            var action = beforeNextSave;
            beforeNextSave = null;
            action.run();
        }
        if (optimisticLocking) {
            return saveVersioned(nomination);
        }
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

    private Nomination saveVersioned(Nomination nomination) {
        var stored = nominations.get(nomination.id());
        long expected = stored == null || stored.version() == null ? -1 : stored.version();
        long actual = nomination.version() == null ? -1 : nomination.version();
        if (expected != actual) {
            throw new OptimisticLockingFailureException("Versión " + actual + " desactualizada, actual " + expected);
        }
        history.addAll(nomination.pullPendingChanges());
        var saved = copy(nomination, actual + 1);
        nominations.put(saved.id(), saved);
        return copy(saved, saved.version());
    }

    private static Nomination copy(Nomination n, Long version) {
        return Nomination.rehydrate(n.id(), n.entityId(), n.requestId(), n.customerId(), n.accountId(),
                n.cardToken(), n.alias(), n.correlationId(), n.createdAt(), n.status(), n.rejectionReason(),
                n.abmReasonCode(), n.updatedAt(), version);
    }

    private void store(Nomination nomination) {
        nominations.put(nomination.id(), nomination);
        history.addAll(nomination.pullPendingChanges());
    }

    @Override
    public Optional<Nomination> findById(UUID id) {
        var found = Optional.ofNullable(nominations.get(id));
        return optimisticLocking ? found.map(n -> copy(n, n.version())) : found;
    }

    @Override
    public Optional<Nomination> findByEntityIdAndRequestId(String entityId, UUID requestId) {
        return nominations.values().stream()
                .filter(n -> n.entityId().equals(entityId) && n.requestId().equals(requestId))
                .findFirst();
    }

    @Override
    public List<UUID> findIdsByStatusUpdatedBefore(NominationStatus status, Instant before, int limit) {
        return nominations.values().stream()
                .filter(n -> n.status() == status && n.updatedAt().isBefore(before))
                .sorted(Comparator.comparing(Nomination::updatedAt))
                .limit(limit)
                .map(Nomination::id)
                .toList();
    }

    @Override
    public List<StatusChange> findHistory(UUID nominationId) {
        return history.stream().filter(c -> c.nominationId().equals(nominationId)).toList();
    }

    int count() {
        return nominations.size();
    }
}
