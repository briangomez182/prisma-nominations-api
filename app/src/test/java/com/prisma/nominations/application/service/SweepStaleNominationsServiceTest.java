package com.prisma.nominations.application.service;

import com.prisma.nominations.domain.AbmDecision;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static com.prisma.nominations.domain.NominationStatus.ABM_TIMEOUT;
import static com.prisma.nominations.domain.NominationStatus.APPROVED;
import static com.prisma.nominations.domain.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.NominationStatus.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("E6")
class SweepStaleNominationsServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-09T12:00:00Z");
    private static final Duration SLA = Duration.ofMinutes(15);
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private final InMemoryNominationRepository repository = InMemoryNominationRepository.withOptimisticLocking();
    private final RecordingNominationMetrics metrics = new RecordingNominationMetrics();

    private SweepStaleNominationsService service(int batchSize) {
        return new SweepStaleNominationsService(repository, TransactionOperations.withoutTransaction(), CLOCK, SLA,
                batchSize, metrics);
    }

    @Test
    void timesOutOnlyPendingAbmOlderThanTheSla() {
        var stale = pendingSince(NOW.minus(SLA).minusSeconds(1));
        var recent = pendingSince(NOW.minus(SLA).plusSeconds(60));
        var oldReceived = received(NOW.minus(Duration.ofHours(2)));
        var oldApproved = pendingSince(NOW.minus(Duration.ofHours(2)));
        resolve(oldApproved);

        int swept = service(100).sweep();

        assertThat(swept).isEqualTo(1);
        assertThat(status(stale)).isEqualTo(ABM_TIMEOUT);
        assertThat(status(recent)).isEqualTo(PENDING_ABM);
        assertThat(status(oldReceived)).isEqualTo(RECEIVED);
        assertThat(status(oldApproved)).isEqualTo(APPROVED);
        assertThat(repository.findHistory(stale.id())).last().satisfies(change -> {
            assertThat(change.from()).isEqualTo(PENDING_ABM);
            assertThat(change.to()).isEqualTo(ABM_TIMEOUT);
            assertThat(change.source()).isEqualTo(ChangeSource.SWEEPER);
            assertThat(change.detail()).contains("SLA").contains("PT15M");
            assertThat(change.occurredAt()).isEqualTo(NOW);
        });
        assertThat(metrics.calls).containsExactly("abmTimeout:SWEEPER");
    }

    @Test
    void respectsTheBatchSizeOldestFirst() {
        var oldest = pendingSince(NOW.minus(Duration.ofHours(3)));
        var middle = pendingSince(NOW.minus(Duration.ofHours(2)));
        var newest = pendingSince(NOW.minus(Duration.ofHours(1)));
        var service = service(2);

        assertThat(service.sweep()).isEqualTo(2);
        assertThat(status(oldest)).isEqualTo(ABM_TIMEOUT);
        assertThat(status(middle)).isEqualTo(ABM_TIMEOUT);
        assertThat(status(newest)).isEqualTo(PENDING_ABM);

        assertThat(service.sweep()).isEqualTo(1);
        assertThat(status(newest)).isEqualTo(ABM_TIMEOUT);
        assertThat(service.sweep()).isZero();
    }

    @Test
    void nothingToSweepReturnsZero() {
        pendingSince(NOW.minusSeconds(30));

        assertThat(service(100).sweep()).isZero();
        assertThat(repository.saves).isEqualTo(1);
    }

    @Test
    void abmResponseCommittedConcurrentlyWinsAndTheSweepGoesOn() {
        var raced = pendingSince(NOW.minus(Duration.ofHours(2)));
        var other = pendingSince(NOW.minus(Duration.ofHours(1)));
        // La respuesta de ABM commitea entre la relectura del sweeper y su save: lock optimista.
        repository.beforeNextSave(() -> resolve(raced));

        int swept = service(100).sweep();

        assertThat(swept).isEqualTo(1);
        assertThat(status(raced)).isEqualTo(APPROVED);
        assertThat(repository.findHistory(raced.id()))
                .noneMatch(change -> change.source() == ChangeSource.SWEEPER);
        assertThat(status(other)).isEqualTo(ABM_TIMEOUT);
    }

    @Test
    void rejectsInvalidConfiguration() {
        var tx = TransactionOperations.withoutTransaction();
        assertThatThrownBy(() -> new SweepStaleNominationsService(repository, tx, CLOCK, Duration.ZERO, 10, metrics))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SweepStaleNominationsService(repository, tx, CLOCK, SLA, 0, metrics))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------- soporte

    private Nomination received(Instant at) {
        return repository.save(Nomination.receive("ENT01", UUID.randomUUID(), "123456",
                new AccountId("0001234567890987654"), new CardToken("tok_4f9a2c"), null, "corr-1", at));
    }

    private Nomination pendingSince(Instant at) {
        var nomination = Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("0001234567890987654"),
                new CardToken("tok_4f9a2c"), null, "corr-1", at.minusSeconds(5));
        nomination.markSentToAbm(at);
        return repository.save(nomination);
    }

    private void resolve(Nomination nomination) {
        var current = repository.findById(nomination.id()).orElseThrow();
        current.resolve(AbmDecision.approved(), NOW);
        repository.save(current);
    }

    private NominationStatus status(Nomination nomination) {
        return repository.findById(nomination.id()).orElseThrow().status();
    }
}
