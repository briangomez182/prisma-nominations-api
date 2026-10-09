package com.prisma.nominations.application.service;

import com.prisma.nominations.application.event.IntegrationEvent;
import com.prisma.nominations.application.event.NominationRequested;
import com.prisma.nominations.application.exception.IdempotencyConflictException;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.InvalidNominationDataException;
import com.prisma.nominations.domain.Nomination;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.prisma.nominations.domain.NominationStatus.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NominationCommandServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-08T12:00:00Z");

    private final InMemoryNominationRepository repository = new InMemoryNominationRepository();
    /** Outbox en memoria: registra los eventos agregados. */
    private final List<IntegrationEvent> outbox = new ArrayList<>();
    private final NominationCommandService service = new NominationCommandService(repository, outbox::add,
            TransactionOperations.withoutTransaction(), Clock.fixed(NOW, ZoneOffset.UTC));

    private static CreateNominationCommand command(UUID requestId) {
        return new CreateNominationCommand("ENT01", requestId, "123456", "987654", "tok_4f9a2c",
                "CUENTA_PRINCIPAL", "corr-1");
    }

    @Test
    void createsNewNominationInReceived() {
        var result = service.create(command(UUID.randomUUID()));

        assertThat(result.replayed()).isFalse();
        assertThat(result.nomination().status()).isEqualTo(RECEIVED);
        assertThat(result.nomination().createdAt()).isEqualTo(NOW);
        assertThat(repository.findHistory(result.nomination().id()))
                .singleElement()
                .satisfies(change -> assertThat(change.to()).isEqualTo(RECEIVED));
        assertThat(outbox).singleElement().isInstanceOfSatisfying(NominationRequested.class, event -> {
            assertThat(event.nominationId()).isEqualTo(result.nomination().id());
            assertThat(event.accountId()).isEqualTo("987654");
            assertThat(event.correlationId()).isEqualTo("corr-1");
            assertThat(event.occurredAt()).isEqualTo(NOW);
        });
    }

    @Test
    void sameRequestWithSameContentReturnsExistingWithoutCreating() {
        var requestId = UUID.randomUUID();
        var first = service.create(command(requestId));

        var second = service.create(command(requestId));

        assertThat(second.replayed()).isTrue();
        assertThat(second.nomination().id()).isEqualTo(first.nomination().id());
        assertThat(repository.count()).isEqualTo(1);
        assertThat(repository.saves).isEqualTo(1);
        assertThat(outbox).hasSize(1);
    }

    @Test
    void sameRequestWithDifferentContentIsConflict() {
        var requestId = UUID.randomUUID();
        service.create(command(requestId));

        var changed = new CreateNominationCommand("ENT01", requestId, "123456", "987654", "tok_4f9a2c",
                "OTRO_ALIAS", "corr-2");

        assertThatThrownBy(() -> service.create(changed)).isInstanceOf(IdempotencyConflictException.class);
        assertThat(repository.count()).isEqualTo(1);
        assertThat(outbox).hasSize(1);
    }

    @Test
    void sameRequestIdInAnotherEntityIsIndependent() {
        var requestId = UUID.randomUUID();
        service.create(command(requestId));

        var other = new CreateNominationCommand("ENT02", requestId, "999", "111122", "tok_x1", null, "corr-2");

        assertThat(service.create(other).replayed()).isFalse();
        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void lostRaceRereadsAndReturnsWinnerAsReplay() {
        var requestId = UUID.randomUUID();
        var winner = Nomination.receive("ENT01", requestId, "123456", new AccountId("987654"),
                new CardToken("tok_4f9a2c"), "CUENTA_PRINCIPAL", "corr-otra", NOW);
        repository.insertConcurrentlyBeforeNextSave(winner);

        var result = service.create(command(requestId));

        assertThat(result.replayed()).isTrue();
        assertThat(result.nomination().id()).isEqualTo(winner.id());
        assertThat(repository.count()).isEqualTo(1);
        assertThat(outbox).isEmpty();
    }

    @Test
    void lostRaceWithDifferentContentIsConflict() {
        var requestId = UUID.randomUUID();
        var winner = Nomination.receive("ENT01", requestId, "OTRO", new AccountId("987654"),
                new CardToken("tok_4f9a2c"), "CUENTA_PRINCIPAL", "corr-otra", NOW);
        repository.insertConcurrentlyBeforeNextSave(winner);

        assertThatThrownBy(() -> service.create(command(requestId)))
                .isInstanceOf(IdempotencyConflictException.class);
        assertThat(outbox).isEmpty();
    }

    @Test
    void panAsCardIdIsRejectedAndNothingIsSaved() {
        var withPan = new CreateNominationCommand("ENT01", UUID.randomUUID(), "123456", "987654",
                "4111111111111111", null, "corr-1");

        assertThatThrownBy(() -> service.create(withPan))
                .isInstanceOf(InvalidNominationDataException.class)
                .extracting(e -> ((InvalidNominationDataException) e).field())
                .isEqualTo("card_id");
        assertThat(repository.saves).isZero();
        assertThat(outbox).isEmpty();
    }
}
