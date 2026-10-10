package com.prisma.nominations.domain.model;

import com.prisma.nominations.domain.enums.ChangeSource;
import com.prisma.nominations.domain.enums.NominationStatus;
import com.prisma.nominations.domain.enums.RejectionReason;
import com.prisma.nominations.domain.enums.ResolutionOutcome;
import com.prisma.nominations.domain.exception.InvalidNominationDataException;
import com.prisma.nominations.domain.exception.InvalidStatusTransitionException;
import com.prisma.nominations.domain.vo.AccountId;
import com.prisma.nominations.domain.vo.CardToken;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Agregado raíz. Todo cambio de estado pasa por {@link #transitionTo} y queda registrado como
 * {@link StatusChange} pendiente, que el repositorio persiste en el historial en la misma transacción.
 */
public final class Nomination {

    private final UUID id;
    private final String entityId;
    private final UUID requestId;
    private final String customerId;
    private final AccountId accountId;
    private final CardToken cardToken;
    private final String alias;
    private final String correlationId;
    private final Instant createdAt;

    private NominationStatus status;
    private RejectionReason rejectionReason;
    private String abmReasonCode;
    private Instant updatedAt;
    /** Lock optimista. {@code null} mientras la nominación no fue persistida. */
    private final Long version;

    private final List<StatusChange> pendingChanges = new ArrayList<>();

    private Nomination(UUID id, String entityId, UUID requestId, String customerId, AccountId accountId,
                       CardToken cardToken, String alias, String correlationId, Instant createdAt,
                       NominationStatus status, RejectionReason rejectionReason, String abmReasonCode,
                       Instant updatedAt, Long version) {
        this.id = Objects.requireNonNull(id, "id");
        this.entityId = requireEntityId(entityId);
        this.requestId = requirePresent(requestId, "request_id");
        this.customerId = requireText(customerId, "customer_id");
        this.accountId = requirePresent(accountId, "account_id");
        this.cardToken = requirePresent(cardToken, "card_id");
        this.alias = alias;
        this.correlationId = requireText(correlationId, "correlation_id");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
        this.status = Objects.requireNonNull(status, "status");
        this.rejectionReason = rejectionReason;
        this.abmReasonCode = abmReasonCode;
        this.updatedAt = Objects.requireNonNull(updatedAt, "updatedAt");
        this.version = version;
    }

    /** Nueva solicitud aceptada por la API. */
    public static Nomination receive(String entityId, UUID requestId, String customerId, AccountId accountId,
                                     CardToken cardToken, String alias, String correlationId, Instant now) {
        var nomination = new Nomination(UUID.randomUUID(), entityId, requestId, customerId, accountId, cardToken,
                alias, correlationId, now, NominationStatus.RECEIVED, null, null, now, null);
        nomination.record(null, NominationStatus.RECEIVED, ChangeSource.API, "Solicitud recibida", now);
        return nomination;
    }

    /** Reconstrucción desde persistencia. No genera historial. */
    public static Nomination rehydrate(UUID id, String entityId, UUID requestId, String customerId,
                                       AccountId accountId, CardToken cardToken, String alias, String correlationId,
                                       Instant createdAt, NominationStatus status, RejectionReason rejectionReason,
                                       String abmReasonCode, Instant updatedAt, Long version) {
        return new Nomination(id, entityId, requestId, customerId, accountId, cardToken, alias, correlationId,
                createdAt, status, rejectionReason, abmReasonCode, updatedAt, version);
    }

    /**
     * ABM aceptó el pedido.
     *
     * @return {@code false} si la nominación ya no está en RECEIVED (ABM respondió antes de que se
     * confirmara el envío): no es un error, simplemente no hay nada que hacer.
     */
    public boolean markSentToAbm(Instant now) {
        if (status != NominationStatus.RECEIVED) {
            return false;
        }
        transitionTo(NominationStatus.PENDING_ABM, ChangeSource.ABM_ADAPTER, "Enviada a ABM", now);
        return true;
    }

    /**
     * Aplica la respuesta de ABM de forma idempotente: si la nominación ya tiene un resultado final,
     * no se modifica nada.
     */
    public ResolutionOutcome resolve(AbmDecision decision, Instant now) {
        if (status.isFinal()) {
            return status == decision.targetStatus() ? ResolutionOutcome.DUPLICATE : ResolutionOutcome.CONFLICT;
        }
        switch (decision) {
            case AbmDecision.Approved approved ->
                    transitionTo(NominationStatus.APPROVED, ChangeSource.ABM_RESPONSE, "Aprobada por ABM", now);
            case AbmDecision.Rejected rejected -> {
                this.rejectionReason = rejected.reason();
                this.abmReasonCode = rejected.abmReasonCode();
                transitionTo(NominationStatus.REJECTED, ChangeSource.ABM_RESPONSE,
                        "Rechazada por ABM: " + rejected.reason(), now);
            }
        }
        return ResolutionOutcome.APPLIED;
    }

    /** Reintentos agotados (adapter) o sin respuesta dentro del SLA (sweeper). */
    public void markTimedOut(ChangeSource source, String detail, Instant now) {
        transitionTo(NominationStatus.ABM_TIMEOUT, source, detail, now);
    }

    /** Reproceso controlado de una nominación en ABM_TIMEOUT: vuelve a enviarse a ABM. */
    public void reprocess(Instant now) {
        transitionTo(NominationStatus.RECEIVED, ChangeSource.OPERATOR, "Reproceso manual", now);
    }

    /** Devuelve y limpia los cambios de estado aún no persistidos. */
    public List<StatusChange> pullPendingChanges() {
        var changes = List.copyOf(pendingChanges);
        pendingChanges.clear();
        return changes;
    }

    private void transitionTo(NominationStatus target, ChangeSource source, String detail, Instant now) {
        if (!status.canTransitionTo(target)) {
            throw new InvalidStatusTransitionException(id, status, target);
        }
        var from = status;
        status = target;
        updatedAt = now;
        record(from, target, source, detail, now);
    }

    private void record(NominationStatus from, NominationStatus to, ChangeSource source, String detail, Instant now) {
        pendingChanges.add(new StatusChange(id, from, to, source, detail, correlationId, now));
    }

    private static final Pattern ENTITY_ID = Pattern.compile("[A-Za-z0-9_-]{1,20}");

    /** Viene del canal (hoy header, luego JWT): se valida antes de llegar a la base, que admite hasta 20. */
    private static String requireEntityId(String value) {
        requireText(value, "entity_id");
        if (!ENTITY_ID.matcher(value).matches()) {
            throw new InvalidNominationDataException("entity_id",
                    "entity_id debe tener entre 1 y 20 caracteres alfanuméricos, '_' o '-'");
        }
        return value;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new InvalidNominationDataException(field, field + " es obligatorio");
        }
        return value;
    }

    private static <T> T requirePresent(T value, String field) {
        if (value == null) {
            throw new InvalidNominationDataException(field, field + " es obligatorio");
        }
        return value;
    }

    public UUID id() {
        return id;
    }

    public String entityId() {
        return entityId;
    }

    public UUID requestId() {
        return requestId;
    }

    public String customerId() {
        return customerId;
    }

    public AccountId accountId() {
        return accountId;
    }

    public CardToken cardToken() {
        return cardToken;
    }

    public String alias() {
        return alias;
    }

    public String correlationId() {
        return correlationId;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public NominationStatus status() {
        return status;
    }

    public RejectionReason rejectionReason() {
        return rejectionReason;
    }

    public String abmReasonCode() {
        return abmReasonCode;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    public Long version() {
        return version;
    }
}
