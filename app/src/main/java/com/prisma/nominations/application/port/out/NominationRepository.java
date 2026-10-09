package com.prisma.nominations.application.port.out;

import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.StatusChange;

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

    /** Historial completo en orden cronológico. */
    List<StatusChange> findHistory(UUID nominationId);
}
