package com.prisma.nominations.domain;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static com.prisma.nominations.domain.NominationStatus.ABM_TIMEOUT;
import static com.prisma.nominations.domain.NominationStatus.APPROVED;
import static com.prisma.nominations.domain.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.NominationStatus.RECEIVED;
import static com.prisma.nominations.domain.NominationStatus.REJECTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NominationTest {

    private static final Instant NOW = Instant.parse("2026-10-08T12:00:00Z");

    static Nomination newNomination() {
        return Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("987654"),
                new CardToken("tok_4f9a2c"), "CUENTA_PRINCIPAL", "corr-1", NOW);
    }

    @Test
    void receiveStartsInReceivedAndRecordsCreation() {
        var nomination = newNomination();

        assertThat(nomination.status()).isEqualTo(RECEIVED);
        assertThat(nomination.version()).isNull();
        assertThat(nomination.pullPendingChanges())
                .singleElement()
                .satisfies(change -> {
                    assertThat(change.from()).isNull();
                    assertThat(change.to()).isEqualTo(RECEIVED);
                    assertThat(change.source()).isEqualTo(ChangeSource.API);
                    assertThat(change.correlationId()).isEqualTo("corr-1");
                });
    }

    @Test
    void happyPathRecordsEveryTransition() {
        var nomination = newNomination();

        assertThat(nomination.markSentToAbm(NOW)).isTrue();
        assertThat(nomination.resolve(AbmDecision.approved(), NOW)).isEqualTo(ResolutionOutcome.APPLIED);

        assertThat(nomination.status()).isEqualTo(APPROVED);
        assertThat(nomination.pullPendingChanges())
                .extracting(StatusChange::to)
                .containsExactly(RECEIVED, PENDING_ABM, APPROVED);
        assertThat(nomination.pullPendingChanges()).isEmpty();
    }

    @Test
    void rejectionKeepsNormalizedReasonAndAbmCode() {
        var nomination = newNomination();
        nomination.markSentToAbm(NOW);

        nomination.resolve(AbmDecision.rejected(RejectionReason.ACCOUNT_BLOCKED, "ABM-051"), NOW);

        assertThat(nomination.status()).isEqualTo(REJECTED);
        assertThat(nomination.rejectionReason()).isEqualTo(RejectionReason.ACCOUNT_BLOCKED);
        assertThat(nomination.abmReasonCode()).isEqualTo("ABM-051");
    }

    @Test
    void duplicatedAbmResponseHasNoEffect() {
        var nomination = newNomination();
        nomination.markSentToAbm(NOW);
        nomination.resolve(AbmDecision.approved(), NOW);
        nomination.pullPendingChanges();

        assertThat(nomination.resolve(AbmDecision.approved(), NOW)).isEqualTo(ResolutionOutcome.DUPLICATE);
        assertThat(nomination.pullPendingChanges()).isEmpty();
    }

    @Test
    void contradictoryAbmResponseIsReportedAndIgnored() {
        var nomination = newNomination();
        nomination.markSentToAbm(NOW);
        nomination.resolve(AbmDecision.approved(), NOW);

        var outcome = nomination.resolve(AbmDecision.rejected(RejectionReason.OTHER, "X"), NOW);

        assertThat(outcome).isEqualTo(ResolutionOutcome.CONFLICT);
        assertThat(nomination.status()).isEqualTo(APPROVED);
    }

    @Test
    void abmCanAnswerBeforeTheAdapterConfirmsTheSend() {
        var nomination = newNomination();

        nomination.resolve(AbmDecision.approved(), NOW);

        assertThat(nomination.markSentToAbm(NOW)).isFalse();
        assertThat(nomination.status()).isEqualTo(APPROVED);
    }

    @Test
    void timedOutNominationAcceptsLateResponse() {
        var nomination = newNomination();
        nomination.markSentToAbm(NOW);
        nomination.markTimedOut(ChangeSource.SWEEPER, "Sin respuesta de ABM dentro del SLA", NOW);

        assertThat(nomination.resolve(AbmDecision.approved(), NOW)).isEqualTo(ResolutionOutcome.APPLIED);
        assertThat(nomination.status()).isEqualTo(APPROVED);
    }

    @Test
    void timedOutNominationCanBeReprocessed() {
        var nomination = newNomination();
        nomination.markTimedOut(ChangeSource.ABM_ADAPTER, "Reintentos agotados", NOW);

        nomination.reprocess(NOW);

        assertThat(nomination.status()).isEqualTo(RECEIVED);
    }

    @Test
    void finalStatesRejectFurtherTransitions() {
        var nomination = newNomination();
        nomination.resolve(AbmDecision.approved(), NOW);

        assertThatThrownBy(() -> nomination.markTimedOut(ChangeSource.SWEEPER, "x", NOW))
                .isInstanceOf(InvalidStatusTransitionException.class);
        assertThatThrownBy(() -> nomination.reprocess(NOW))
                .isInstanceOf(InvalidStatusTransitionException.class);
    }

    @Test
    void onlyApprovedAndRejectedAreFinal() {
        assertThat(NominationStatus.values())
                .filteredOn(NominationStatus::isFinal)
                .containsExactlyInAnyOrder(APPROVED, REJECTED);
        assertThat(ABM_TIMEOUT.isFinal()).isFalse();
    }

    @Test
    void requiredFieldsAreValidated() {
        assertThatThrownBy(() -> Nomination.receive("ENT01", UUID.randomUUID(), " ", new AccountId("987654"),
                new CardToken("tok_4f9a2c"), null, "corr-1", NOW))
                .isInstanceOf(InvalidNominationDataException.class)
                .extracting(e -> ((InvalidNominationDataException) e).field())
                .isEqualTo("customer_id");
    }

    @Test
    void entityIdMustFitTheDatabaseColumn() {
        assertThatThrownBy(() -> Nomination.receive("E".repeat(21), UUID.randomUUID(), "123456",
                new AccountId("987654"), new CardToken("tok_4f9a2c"), null, "corr-1", NOW))
                .isInstanceOf(InvalidNominationDataException.class)
                .extracting(e -> ((InvalidNominationDataException) e).field())
                .isEqualTo("entity_id");
    }
}
