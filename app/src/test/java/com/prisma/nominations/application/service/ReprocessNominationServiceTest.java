package com.prisma.nominations.application.service;

import com.prisma.nominations.application.event.NominationRequested;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.service.ProcessAbmResponseServiceTest.FakeOutbox;
import com.prisma.nominations.domain.model.AbmDecision;
import com.prisma.nominations.domain.vo.AccountId;
import com.prisma.nominations.domain.vo.CardToken;
import com.prisma.nominations.domain.enums.ChangeSource;
import com.prisma.nominations.domain.exception.InvalidStatusTransitionException;
import com.prisma.nominations.domain.model.Nomination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static com.prisma.nominations.domain.enums.NominationStatus.ABM_TIMEOUT;
import static com.prisma.nominations.domain.enums.NominationStatus.APPROVED;
import static com.prisma.nominations.domain.enums.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.enums.NominationStatus.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("E6")
class ReprocessNominationServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-09T12:00:00Z");

    private final InMemoryNominationRepository repository = InMemoryNominationRepository.withOptimisticLocking();
    private final FakeOutbox outbox = new FakeOutbox();
    private final RecordingNominationMetrics metrics = new RecordingNominationMetrics();
    private final ReprocessNominationService service = new ReprocessNominationService(repository, outbox,
            TransactionOperations.withoutTransaction(), Clock.fixed(NOW, ZoneOffset.UTC), metrics);

    @Test
    void timedOutGoesBackToReceivedAndEnqueuesANewAbmRequest() {
        var nomination = timedOut();

        var reprocessed = service.reprocess(nomination.id());

        assertThat(reprocessed.status()).isEqualTo(RECEIVED);
        assertThat(reprocessed.updatedAt()).isEqualTo(NOW);
        assertThat(repository.findById(nomination.id()).orElseThrow().status()).isEqualTo(RECEIVED);
        assertThat(outbox.events).singleElement().isInstanceOfSatisfying(NominationRequested.class, event -> {
            assertThat(event.nominationId()).isEqualTo(nomination.id());
            assertThat(event.requestId()).isEqualTo(nomination.requestId());
            assertThat(event.correlationId()).isEqualTo("corr-1");
            assertThat(event.occurredAt()).isEqualTo(NOW);
        });
        assertThat(repository.findHistory(nomination.id())).last().satisfies(change -> {
            assertThat(change.from()).isEqualTo(ABM_TIMEOUT);
            assertThat(change.to()).isEqualTo(RECEIVED);
            assertThat(change.source()).isEqualTo(ChangeSource.OPERATOR);
        });
        assertThat(metrics.calls).containsExactly("reprocessed");
    }

    @Test
    void fromAnotherStatusIsInvalidAndChangesNothing() {
        var nomination = pending();
        int saves = repository.saves;

        assertThatThrownBy(() -> service.reprocess(nomination.id()))
                .isInstanceOf(InvalidStatusTransitionException.class);

        assertThat(repository.findById(nomination.id()).orElseThrow().status()).isEqualTo(PENDING_ABM);
        assertThat(repository.saves).isEqualTo(saves);
        assertThat(outbox.events).isEmpty();
        assertThat(metrics.calls).isEmpty();
    }

    @Test
    void unknownNominationIsNotFound() {
        assertThatThrownBy(() -> service.reprocess(UUID.randomUUID()))
                .isInstanceOf(NominationNotFoundException.class);
        assertThat(outbox.events).isEmpty();
    }

    @Test
    void lateAbmResponseCommittedConcurrentlyWinsOverTheReprocess() {
        var nomination = timedOut();
        // ABM responde tarde y commitea entre la lectura y el save del reproceso: lock optimista, se reevalúa.
        repository.beforeNextSave(() -> {
            var current = repository.findById(nomination.id()).orElseThrow();
            current.resolve(AbmDecision.approved(), NOW);
            repository.save(current);
        });

        assertThatThrownBy(() -> service.reprocess(nomination.id()))
                .isInstanceOf(InvalidStatusTransitionException.class);

        assertThat(repository.findById(nomination.id()).orElseThrow().status()).isEqualTo(APPROVED);
        assertThat(outbox.events).isEmpty();
    }

    // ---------------------------------------------------------------- soporte

    private Nomination pending() {
        var nomination = Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("0001234567890987654"),
                new CardToken("tok_4f9a2c"), null, "corr-1", NOW.minusSeconds(3600));
        nomination.markSentToAbm(NOW.minusSeconds(3590));
        return repository.save(nomination);
    }

    private Nomination timedOut() {
        var nomination = pending();
        nomination.markTimedOut(ChangeSource.SWEEPER, "Sin respuesta de ABM dentro del SLA", NOW.minusSeconds(60));
        return repository.save(nomination);
    }
}
