package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.NominationStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;

import java.time.Instant;
import java.util.UUID;

@Entity
@Immutable
@Table(name = "nomination_history")
class NominationHistoryJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "nomination_id", nullable = false)
    private UUID nominationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_status")
    private NominationStatus fromStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", nullable = false)
    private NominationStatus toStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "source", nullable = false)
    private ChangeSource source;

    @Column(name = "detail")
    private String detail;

    @Column(name = "correlation_id", nullable = false)
    private String correlationId;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected NominationHistoryJpaEntity() {
    }

    NominationHistoryJpaEntity(UUID nominationId, NominationStatus fromStatus, NominationStatus toStatus,
                               ChangeSource source, String detail, String correlationId, Instant occurredAt) {
        this.nominationId = nominationId;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.source = source;
        this.detail = detail;
        this.correlationId = correlationId;
        this.occurredAt = occurredAt;
    }

    UUID getNominationId() {
        return nominationId;
    }

    NominationStatus getFromStatus() {
        return fromStatus;
    }

    NominationStatus getToStatus() {
        return toStatus;
    }

    ChangeSource getSource() {
        return source;
    }

    String getDetail() {
        return detail;
    }

    String getCorrelationId() {
        return correlationId;
    }

    Instant getOccurredAt() {
        return occurredAt;
    }
}
