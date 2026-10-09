package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.RejectionReason;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "nominations")
class NominationJpaEntity {

    @Id
    private UUID id;

    @Column(name = "entity_id", nullable = false, updatable = false)
    private String entityId;

    @Column(name = "request_id", nullable = false, updatable = false)
    private UUID requestId;

    @Column(name = "customer_id", nullable = false, updatable = false)
    private String customerId;

    @Column(name = "account_id", nullable = false, updatable = false)
    private String accountId;

    @Column(name = "card_token", nullable = false, updatable = false)
    private String cardToken;

    @Column(name = "alias", updatable = false)
    private String alias;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private NominationStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "rejection_reason")
    private RejectionReason rejectionReason;

    @Column(name = "abm_reason_code")
    private String abmReasonCode;

    @Column(name = "correlation_id", nullable = false, updatable = false)
    private String correlationId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    protected NominationJpaEntity() {
    }

    NominationJpaEntity(UUID id, String entityId, UUID requestId, String customerId, String accountId,
                        String cardToken, String alias, NominationStatus status, RejectionReason rejectionReason,
                        String abmReasonCode, String correlationId, Instant createdAt, Instant updatedAt,
                        Long version) {
        this.id = id;
        this.entityId = entityId;
        this.requestId = requestId;
        this.customerId = customerId;
        this.accountId = accountId;
        this.cardToken = cardToken;
        this.alias = alias;
        this.status = status;
        this.rejectionReason = rejectionReason;
        this.abmReasonCode = abmReasonCode;
        this.correlationId = correlationId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.version = version;
    }

    UUID getId() {
        return id;
    }

    String getEntityId() {
        return entityId;
    }

    UUID getRequestId() {
        return requestId;
    }

    String getCustomerId() {
        return customerId;
    }

    String getAccountId() {
        return accountId;
    }

    String getCardToken() {
        return cardToken;
    }

    String getAlias() {
        return alias;
    }

    NominationStatus getStatus() {
        return status;
    }

    RejectionReason getRejectionReason() {
        return rejectionReason;
    }

    String getAbmReasonCode() {
        return abmReasonCode;
    }

    String getCorrelationId() {
        return correlationId;
    }

    Instant getCreatedAt() {
        return createdAt;
    }

    Instant getUpdatedAt() {
        return updatedAt;
    }

    Long getVersion() {
        return version;
    }
}
