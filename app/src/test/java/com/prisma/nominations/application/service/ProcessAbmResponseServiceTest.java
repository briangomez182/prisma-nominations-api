package com.prisma.nominations.application.service;

import com.prisma.nominations.application.event.IntegrationEvent;
import com.prisma.nominations.application.event.NominationResult;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.AbmResponseCommand;
import com.prisma.nominations.application.port.out.OutboxPort;
import com.prisma.nominations.domain.AbmDecision;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.RejectionReason;
import com.prisma.nominations.domain.ResolutionOutcome;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.prisma.nominations.domain.NominationStatus.ABM_TIMEOUT;
import static com.prisma.nominations.domain.NominationStatus.APPROVED;
import static com.prisma.nominations.domain.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.NominationStatus.REJECTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcessAbmResponseServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-09T12:00:00Z");

    private final InMemoryNominationRepository repository = InMemoryNominationRepository.withOptimisticLocking();
    private final FakeOutbox outbox = new FakeOutbox();
    private final RecordingNominationMetrics metrics = new RecordingNominationMetrics();
    private final ProcessAbmResponseService service = new ProcessAbmResponseService(repository, outbox,
            TransactionOperations.withoutTransaction(), Clock.fixed(NOW, ZoneOffset.UTC), metrics);

    /** Outbox en memoria con la regla del índice único: un solo nomination.result por nominación. */
    static final class FakeOutbox implements OutboxPort {
        final List<IntegrationEvent> events = new ArrayList<>();

        @Override
        public void append(IntegrationEvent event) {
            if (event instanceof NominationResult && results(event.nominationId()) > 0) {
                throw new IllegalStateException("Ya existe un nomination.result para " + event.nominationId());
            }
            events.add(event);
        }

        long results(UUID nominationId) {
            return events.stream()
                    .filter(e -> e instanceof NominationResult && e.nominationId().equals(nominationId))
                    .count();
        }
    }

    private Nomination pending() {
        var nomination = Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("0001234567890987654"),
                new CardToken("tok_4f9a2c"), null, "corr-1", NOW.minusSeconds(300));
        nomination.markSentToAbm(NOW.minusSeconds(290));
        return repository.save(nomination);
    }

    private static AbmResponseCommand approved(Nomination n) {
        return new AbmResponseCommand(n.id(), n.requestId(), "corr-1", "ABM-OP-1", AbmDecision.approved());
    }

    private static AbmResponseCommand rejected(Nomination n, RejectionReason reason, String code) {
        return new AbmResponseCommand(n.id(), n.requestId(), "corr-1", "ABM-OP-1", AbmDecision.rejected(reason, code));
    }

    private Nomination reload(UUID id) {
        return repository.findById(id).orElseThrow();
    }

    @Test
    void approvedIsAppliedAndPublishesOneResult() {
        var nomination = pending();

        var outcome = service.process(approved(nomination));

        assertThat(outcome).isEqualTo(ResolutionOutcome.APPLIED);
        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
        assertThat(outbox.events).singleElement().isInstanceOfSatisfying(NominationResult.class, event -> {
            assertThat(event.nominationId()).isEqualTo(nomination.id());
            assertThat(event.status()).isEqualTo(APPROVED);
            assertThat(event.rejectionReason()).isNull();
            assertThat(event.accountId()).isEqualTo("****7654");
            assertThat(event.occurredAt()).isEqualTo(NOW);
        });
        assertThat(repository.findHistory(nomination.id())).last()
                .satisfies(change -> assertThat(change.source()).isEqualTo(ChangeSource.ABM_RESPONSE));
        assertThat(metrics.calls).containsExactly("abmResponse:APPLIED", "resolved:APPROVED");
        // Tiempo de resolución: desde el alta (NOW - 300 s) hasta la respuesta (NOW).
        assertThat(metrics.resolved).singleElement().satisfies(n ->
                assertThat(Duration.between(n.createdAt(), n.updatedAt())).isEqualTo(Duration.ofSeconds(300)));
    }

    @Test
    void rejectedKeepsNormalizedReasonAndOriginalCode() {
        var nomination = pending();

        var outcome = service.process(rejected(nomination, RejectionReason.ACCOUNT_BLOCKED, "ABM-051"));

        assertThat(outcome).isEqualTo(ResolutionOutcome.APPLIED);
        var stored = reload(nomination.id());
        assertThat(stored.status()).isEqualTo(REJECTED);
        assertThat(stored.rejectionReason()).isEqualTo(RejectionReason.ACCOUNT_BLOCKED);
        assertThat(stored.abmReasonCode()).isEqualTo("ABM-051");
        assertThat(outbox.events).singleElement().isInstanceOfSatisfying(NominationResult.class, event -> {
            assertThat(event.status()).isEqualTo(REJECTED);
            assertThat(event.rejectionReason()).isEqualTo(RejectionReason.ACCOUNT_BLOCKED);
        });
        assertThat(metrics.resolved).singleElement()
                .satisfies(n -> assertThat(n.rejectionReason()).isEqualTo(RejectionReason.ACCOUNT_BLOCKED));
    }

    @Test
    void duplicateResponseHasNoEffects() {
        var nomination = pending();
        service.process(approved(nomination));
        int saves = repository.saves;

        var outcome = service.process(approved(nomination));

        assertThat(outcome).isEqualTo(ResolutionOutcome.DUPLICATE);
        assertThat(repository.saves).isEqualTo(saves);
        assertThat(outbox.events).hasSize(1);
        // Solo la primera respuesta resuelve; la duplicada se cuenta como respuesta pero no como resolución.
        assertThat(metrics.calls).containsExactly("abmResponse:APPLIED", "resolved:APPROVED", "abmResponse:DUPLICATE");
    }

    @Test
    void contradictoryResponseIsConflictAndChangesNothing() {
        var nomination = pending();
        service.process(approved(nomination));
        int saves = repository.saves;

        var outcome = service.process(rejected(nomination, RejectionReason.ACCOUNT_BLOCKED, "ABM-051"));

        assertThat(outcome).isEqualTo(ResolutionOutcome.CONFLICT);
        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
        assertThat(reload(nomination.id()).rejectionReason()).isNull();
        assertThat(repository.saves).isEqualTo(saves);
        assertThat(outbox.events).hasSize(1);
    }

    @Test
    void foreignRequestIdIsNotFound() {
        var nomination = pending();
        var foreign = new AbmResponseCommand(nomination.id(), UUID.randomUUID(), "corr-1", "ABM-OP-1",
                AbmDecision.approved());

        assertThatThrownBy(() -> service.process(foreign)).isInstanceOf(NominationNotFoundException.class);
        assertThat(reload(nomination.id()).status()).isEqualTo(PENDING_ABM);
        assertThat(outbox.events).isEmpty();
    }

    @Test
    void unknownNominationIsNotFound() {
        var unknown = new AbmResponseCommand(UUID.randomUUID(), UUID.randomUUID(), "corr-1", "ABM-OP-1",
                AbmDecision.approved());

        assertThatThrownBy(() -> service.process(unknown)).isInstanceOf(NominationNotFoundException.class);
    }

    @Test
    void lateResponseAfterTimeoutIsApplied() {
        var nomination = pending();
        var timedOut = reload(nomination.id());
        timedOut.markTimedOut(ChangeSource.SWEEPER, "Sin respuesta dentro del SLA", NOW.minusSeconds(10));
        repository.save(timedOut);
        assertThat(reload(nomination.id()).status()).isEqualTo(ABM_TIMEOUT);

        var outcome = service.process(approved(nomination));

        assertThat(outcome).isEqualTo(ResolutionOutcome.APPLIED);
        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
        assertThat(outbox.results(nomination.id())).isEqualTo(1);
    }

    @Test
    void concurrentSameResponseLosesOptimisticLockAndEndsAsDuplicate() {
        var nomination = pending();
        // La otra respuesta idéntica commitea (estado + evento) entre la lectura y el save de esta.
        repository.beforeNextSave(() -> {
            var other = reload(nomination.id());
            other.resolve(AbmDecision.approved(), NOW);
            outbox.append(NominationResult.of(repository.save(other), NOW));
        });

        var outcome = service.process(approved(nomination));

        assertThat(outcome).isEqualTo(ResolutionOutcome.DUPLICATE);
        assertThat(outbox.results(nomination.id())).isEqualTo(1);
        // El intento que perdió la carrera no deja métricas: solo cuenta el resultado reevaluado.
        assertThat(metrics.calls).containsExactly("abmResponse:DUPLICATE");
    }

    @Test
    void concurrentSameResponseLosesOnResultIndexAndEndsAsDuplicate() {
        var nomination = pending();
        // La otra TX ya dejó su nomination.result: el append de esta viola el índice único (IllegalStateException).
        outbox.append(NominationResult.of(approvedCopy(nomination), NOW));

        var outcome = service.process(approved(nomination));

        // Sin rollback real, la relectura ve el APPROVED guardado (lo mismo que dejaría la TX ganadora): DUPLICATE.
        assertThat(outcome).isEqualTo(ResolutionOutcome.DUPLICATE);
        assertThat(outbox.results(nomination.id())).isEqualTo(1);
    }

    private static Nomination approvedCopy(Nomination n) {
        var copy = Nomination.rehydrate(n.id(), n.entityId(), n.requestId(), n.customerId(), n.accountId(),
                n.cardToken(), n.alias(), n.correlationId(), n.createdAt(), n.status(), null, null, n.updatedAt(),
                n.version());
        copy.resolve(AbmDecision.approved(), NOW);
        return copy;
    }
}
