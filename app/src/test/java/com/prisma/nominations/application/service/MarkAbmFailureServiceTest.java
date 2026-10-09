package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.AbmDecision;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.Nomination;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static com.prisma.nominations.domain.NominationStatus.ABM_TIMEOUT;
import static com.prisma.nominations.domain.NominationStatus.APPROVED;
import static com.prisma.nominations.domain.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.NominationStatus.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MarkAbmFailureServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-09T12:00:00Z");
    private static final String DETAIL = "Reintentos agotados: ABM no disponible";

    private final InMemoryNominationRepository repository = InMemoryNominationRepository.withOptimisticLocking();
    private final MarkAbmFailureService service = new MarkAbmFailureService(repository,
            TransactionOperations.withoutTransaction(), Clock.fixed(NOW, ZoneOffset.UTC));

    private Nomination received() {
        return repository.save(Nomination.receive("ENT01", UUID.randomUUID(), "123456",
                new AccountId("0001234567890987654"), new CardToken("tok_4f9a2c"), null, "corr-1",
                NOW.minusSeconds(60)));
    }

    private Nomination pending() {
        var nomination = received();
        var loaded = reload(nomination.id());
        loaded.markSentToAbm(NOW.minusSeconds(30));
        return repository.save(loaded);
    }

    private void approve(UUID id) {
        var other = reload(id);
        other.resolve(AbmDecision.approved(), NOW);
        repository.save(other);
    }

    private Nomination reload(UUID id) {
        return repository.findById(id).orElseThrow();
    }

    @Test
    void receivedMovesToAbmTimeoutWithHistory() {
        var nomination = received();

        boolean changed = service.markFailed(nomination.id(), DETAIL);

        assertThat(changed).isTrue();
        var stored = reload(nomination.id());
        assertThat(stored.status()).isEqualTo(ABM_TIMEOUT);
        assertThat(stored.updatedAt()).isEqualTo(NOW);
        assertThat(repository.findHistory(nomination.id())).last().satisfies(change -> {
            assertThat(change.from()).isEqualTo(RECEIVED);
            assertThat(change.to()).isEqualTo(ABM_TIMEOUT);
            assertThat(change.source()).isEqualTo(ChangeSource.ABM_ADAPTER);
            assertThat(change.detail()).isEqualTo(DETAIL);
        });
    }

    @Test
    void pendingAbmMovesToAbmTimeout() {
        var nomination = pending();

        assertThat(service.markFailed(nomination.id(), DETAIL)).isTrue();

        assertThat(reload(nomination.id()).status()).isEqualTo(ABM_TIMEOUT);
        assertThat(repository.findHistory(nomination.id())).extracting(c -> c.to())
                .containsExactly(RECEIVED, PENDING_ABM, ABM_TIMEOUT);
    }

    @Test
    void approvedIsLeftUntouched() {
        var nomination = received();
        approve(nomination.id());

        assertThat(service.markFailed(nomination.id(), DETAIL)).isFalse();

        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
        assertThat(repository.findHistory(nomination.id())).extracting(c -> c.to()).containsExactly(RECEIVED, APPROVED);
    }

    @Test
    void alreadyTimedOutReturnsFalse() {
        var nomination = received();
        service.markFailed(nomination.id(), DETAIL);
        int savesBefore = repository.saves;

        assertThat(service.markFailed(nomination.id(), DETAIL)).isFalse();

        assertThat(repository.saves).isEqualTo(savesBefore);
        assertThat(repository.findHistory(nomination.id())).extracting(c -> c.to())
                .containsExactly(RECEIVED, ABM_TIMEOUT);
    }

    @Test
    void optimisticLockRereadsAndDoesNotOverwriteTheResult() {
        var nomination = received();
        // El consumer de respuestas commitea la aprobación entre la lectura y el save.
        repository.beforeNextSave(() -> approve(nomination.id()));

        assertThat(service.markFailed(nomination.id(), DETAIL)).isFalse();

        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
    }

    @Test
    void unknownNominationIsNotFound() {
        assertThatThrownBy(() -> service.markFailed(UUID.randomUUID(), DETAIL))
                .isInstanceOf(NominationNotFoundException.class);
    }
}
