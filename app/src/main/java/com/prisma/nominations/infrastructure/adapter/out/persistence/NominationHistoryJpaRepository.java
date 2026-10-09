package com.prisma.nominations.infrastructure.adapter.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

interface NominationHistoryJpaRepository extends JpaRepository<NominationHistoryJpaEntity, Long> {

    List<NominationHistoryJpaEntity> findByNominationIdOrderByOccurredAtAscIdAsc(UUID nominationId);
}
