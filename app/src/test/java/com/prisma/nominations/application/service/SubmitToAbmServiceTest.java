package com.prisma.nominations.application.service;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.AbmUnavailableException;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.SubmitToAbmUseCase.SubmitOutcome;
import com.prisma.nominations.application.port.out.AbmClient;
import com.prisma.nominations.application.port.out.AbmRequest;
import com.prisma.nominations.domain.model.AbmDecision;
import com.prisma.nominations.domain.vo.AccountId;
import com.prisma.nominations.domain.vo.CardToken;
import com.prisma.nominations.domain.model.Nomination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionOperations;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.prisma.nominations.domain.enums.NominationStatus.APPROVED;
import static com.prisma.nominations.domain.enums.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.enums.NominationStatus.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubmitToAbmServiceTest {

    private static final Instant NOW = Instant.parse("2026-10-09T12:00:00Z");

    private final InMemoryNominationRepository repository = InMemoryNominationRepository.withOptimisticLocking();
    private final FakeAbmClient abm = new FakeAbmClient();
    private final RecordingNominationMetrics metrics = new RecordingNominationMetrics();
    private final SubmitToAbmService service = new SubmitToAbmService(repository, abm,
            TransactionOperations.withoutTransaction(), Clock.fixed(NOW, ZoneOffset.UTC), metrics);

    /** ABM en memoria: registra los pedidos y permite simular fallas o efectos durante la llamada. */
    static final class FakeAbmClient implements AbmClient {
        final List<AbmRequest> requests = new ArrayList<>();
        RuntimeException failure;
        Runnable duringCall;

        @Override
        public String submit(AbmRequest request) {
            requests.add(request);
            if (failure != null) {
                throw failure;
            }
            if (duringCall != null) {
                duringCall.run();
            }
            return "ABM-OP-" + request.nominationId();
        }
    }

    private Nomination received() {
        var nomination = Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("0001234567890987654"),
                new CardToken("tok_4f9a2c"), "CUENTA_PRINCIPAL", "corr-1", NOW.minusSeconds(60));
        return repository.save(nomination);
    }

    /** Otra transacción (el consumer de respuestas) que aprueba la nominación y commitea. */
    private void approveConcurrently(UUID id) {
        var other = repository.findById(id).orElseThrow();
        other.resolve(AbmDecision.approved(), NOW);
        repository.save(other);
    }

    private Nomination reload(UUID id) {
        return repository.findById(id).orElseThrow();
    }

    @Test
    void submitsAndMovesToPendingAbm() {
        var nomination = received();

        var outcome = service.submit(nomination.id());

        assertThat(outcome).isEqualTo(SubmitOutcome.SUBMITTED);
        assertThat(reload(nomination.id()).status()).isEqualTo(PENDING_ABM);
        assertThat(abm.requests).singleElement().satisfies(request -> {
            assertThat(request.nominationId()).isEqualTo(nomination.id());
            assertThat(request.requestId()).isEqualTo(nomination.requestId());
            assertThat(request.correlationId()).isEqualTo("corr-1");
            assertThat(request.accountId()).isEqualTo("0001234567890987654");
            assertThat(request.cardId()).isEqualTo("tok_4f9a2c");
        });
        assertThat(repository.findHistory(nomination.id())).extracting(c -> c.to()).containsExactly(RECEIVED, PENDING_ABM);
        assertThat(metrics.calls).containsExactly("submission:SUBMITTED");
    }

    @Test
    @Tag("E3")
    void alreadyPendingIsSkippedWithoutCallingAbm() {
        var nomination = received();
        service.submit(nomination.id());

        var outcome = service.submit(nomination.id());

        assertThat(outcome).isEqualTo(SubmitOutcome.SKIPPED);
        assertThat(abm.requests).hasSize(1);
        assertThat(metrics.calls).containsExactly("submission:SUBMITTED", "submission:SKIPPED");
    }

    @Test
    @Tag("E3")
    void alreadyResolvedIsSkippedWithoutCallingAbm() {
        var nomination = received();
        approveConcurrently(nomination.id());

        var outcome = service.submit(nomination.id());

        assertThat(outcome).isEqualTo(SubmitOutcome.SKIPPED);
        assertThat(abm.requests).isEmpty();
        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
    }

    @Test
    @Tag("E6")
    void abmFailurePropagatesAndKeepsReceived() {
        var nomination = received();
        abm.failure = new AbmUnavailableException("timeout", null);

        assertThatThrownBy(() -> service.submit(nomination.id())).isSameAs(abm.failure);
        assertThat(reload(nomination.id()).status()).isEqualTo(RECEIVED);
        assertThat(metrics.calls).containsExactly("submission:UNAVAILABLE");
    }

    @Test
    @Tag("E6")
    void abmContractErrorIsCountedAndPropagated() {
        var nomination = received();
        abm.failure = new AbmContractException("400 Bad Request");

        assertThatThrownBy(() -> service.submit(nomination.id())).isSameAs(abm.failure);
        assertThat(metrics.calls).containsExactly("submission:CONTRACT_ERROR");
    }

    @Test
    void abmRespondedBeforeConfirmingTheSubmitDoesNotBreak() {
        var nomination = received();
        // ABM responde (y el consumer commitea) mientras todavía no volvió la llamada HTTP.
        abm.duringCall = () -> approveConcurrently(nomination.id());

        var outcome = service.submit(nomination.id());

        assertThat(outcome).isEqualTo(SubmitOutcome.SUBMITTED);
        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
        assertThat(repository.findHistory(nomination.id())).extracting(c -> c.to()).containsExactly(RECEIVED, APPROVED);
    }

    @Test
    void optimisticLockOnConfirmRereadsAndDoesNotOverwriteTheResult() {
        var nomination = received();
        // El consumer de respuestas commitea entre la relectura y el save de la confirmación.
        repository.beforeNextSave(() -> approveConcurrently(nomination.id()));

        var outcome = service.submit(nomination.id());

        assertThat(outcome).isEqualTo(SubmitOutcome.SUBMITTED);
        assertThat(reload(nomination.id()).status()).isEqualTo(APPROVED);
        assertThat(abm.requests).hasSize(1);
    }

    @Test
    void unknownNominationIsNotFound() {
        assertThatThrownBy(() -> service.submit(UUID.randomUUID())).isInstanceOf(NominationNotFoundException.class);
        assertThat(abm.requests).isEmpty();
    }
}
