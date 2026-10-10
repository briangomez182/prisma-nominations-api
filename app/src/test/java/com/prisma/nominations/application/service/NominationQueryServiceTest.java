package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.vo.AccountId;
import com.prisma.nominations.domain.vo.CardToken;
import com.prisma.nominations.domain.model.Nomination;
import com.prisma.nominations.domain.model.StatusChange;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static com.prisma.nominations.domain.enums.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.enums.NominationStatus.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NominationQueryServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-08T12:00:00Z");

    private final InMemoryNominationRepository repository = new InMemoryNominationRepository();
    private final NominationQueryService service = new NominationQueryService(repository);

    private Nomination saved() {
        var nomination = Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("987654"),
                new CardToken("tok_4f9a2c"), null, "corr-1", NOW);
        repository.save(nomination);
        nomination.markSentToAbm(NOW);
        return repository.save(nomination);
    }

    @Test
    void returnsNominationOfTheSameEntity() {
        var nomination = saved();

        assertThat(service.get("ENT01", nomination.id()).id()).isEqualTo(nomination.id());
    }

    @Test
    void nominationOfAnotherEntityIsReportedAsNotFound() {
        var nomination = saved();

        assertThatThrownBy(() -> service.get("ENT02", nomination.id()))
                .isInstanceOf(NominationNotFoundException.class);
        assertThatThrownBy(() -> service.history("ENT02", nomination.id()))
                .isInstanceOf(NominationNotFoundException.class);
    }

    @Test
    void unknownNominationIsNotFound() {
        assertThatThrownBy(() -> service.get("ENT01", UUID.randomUUID()))
                .isInstanceOf(NominationNotFoundException.class);
    }

    @Test
    @Tag("E1")
    void historyIsChronological() {
        var nomination = saved();

        assertThat(service.history("ENT01", nomination.id()))
                .extracting(StatusChange::to)
                .containsExactly(RECEIVED, PENDING_ABM);
    }
}
