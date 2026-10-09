package com.prisma.nominations.application.port.out;

import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.StatusChange;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface NominationRepository {

    /**
     * Persiste la nominación y sus cambios de estado pendientes en el historial.
     * Falla si otra transacción la modificó antes (lock optimista) o si ya existe
     * una nominación con el mismo (entity_id, request_id).
     */
    Nomination save(Nomination nomination);

    Optional<Nomination> findById(UUID id);

    Optional<Nomination> findByEntityIdAndRequestId(String entityId, UUID requestId);

    /** Ids de nominaciones en {@code status} sin cambios desde antes de {@code before}, las más viejas primero. */
    List<UUID> findIdsByStatusUpdatedBefore(NominationStatus status, Instant before, int limit);

    /** Historial completo en orden cronológico. */
    List<StatusChange> findHistory(UUID nominationId);
}
