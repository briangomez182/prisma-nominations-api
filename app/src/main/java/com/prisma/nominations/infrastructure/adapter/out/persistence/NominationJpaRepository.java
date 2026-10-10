package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.prisma.nominations.domain.enums.NominationStatus;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

interface NominationJpaRepository extends JpaRepository<NominationJpaEntity, UUID> {

    Optional<NominationJpaEntity> findByEntityIdAndRequestId(String entityId, UUID requestId);

    /** Usa el índice parcial ix_nominations_open_by_updated_at (RECEIVED / PENDING_ABM). */
    @Query("select n.id from NominationJpaEntity n where n.status = :status and n.updatedAt < :before order by n.updatedAt")
    List<UUID> findIdsByStatusUpdatedBefore(NominationStatus status, Instant before, Limit limit);
}
