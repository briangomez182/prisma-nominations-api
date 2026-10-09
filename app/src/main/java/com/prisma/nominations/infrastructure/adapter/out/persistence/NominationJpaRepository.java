package com.prisma.nominations.infrastructure.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

interface NominationJpaRepository extends JpaRepository<NominationJpaEntity, UUID> {

    Optional<NominationJpaEntity> findByEntityIdAndRequestId(String entityId, UUID requestId);
}
