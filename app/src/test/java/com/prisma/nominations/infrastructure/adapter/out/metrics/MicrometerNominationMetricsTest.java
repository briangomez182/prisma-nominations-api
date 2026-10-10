package com.prisma.nominations.infrastructure.adapter.out.metrics;

import com.prisma.nominations.application.port.out.NominationMetrics.SubmissionOutcome;
import com.prisma.nominations.domain.model.AbmDecision;
import com.prisma.nominations.domain.vo.AccountId;
import com.prisma.nominations.domain.vo.CardToken;
import com.prisma.nominations.domain.enums.ChangeSource;
import com.prisma.nominations.domain.model.Nomination;
import com.prisma.nominations.domain.enums.RejectionReason;
import com.prisma.nominations.domain.enums.ResolutionOutcome;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Nombres y tags exactos del contrato de métricas (los usan tableros y alertas). */
class MicrometerNominationMetricsTest {

    private static final Instant CREATED_AT = Instant.parse("2026-10-09T12:00:00Z");

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerNominationMetrics metrics = new MicrometerNominationMetrics(registry);

    @Test
    void intakeCountersAreTaggedByEntity() {
        metrics.created("ENT01");
        metrics.created("ENT01");
        metrics.replayed("ENT01");
        metrics.idempotencyConflict("ENT02");

        assertThat(registry.get("nominations.received").tag("entity_id", "ENT01").counter().count()).isEqualTo(2);
        assertThat(registry.get("nominations.replayed").tag("entity_id", "ENT01").counter().count()).isEqualTo(1);
        assertThat(registry.get("nominations.idempotency.conflicts").tag("entity_id", "ENT02").counter().count())
                .isEqualTo(1);
    }

    @Test
    void enumeratedSeriesExistInZeroFromStartup() {
        for (var outcome : SubmissionOutcome.values()) {
            assertThat(registry.get("abm.submissions").tag("outcome", outcome.name()).counter().count()).isZero();
        }
        for (var outcome : ResolutionOutcome.values()) {
            assertThat(registry.get("abm.responses").tag("outcome", outcome.name()).counter().count()).isZero();
        }
        assertThat(registry.get("nominations.resolved").tags("status", "APPROVED", "reason", "none").counter().count())
                .isZero();
        assertThat(registry.get("nominations.resolved").tags("status", "REJECTED", "reason", "INVALID_CARD")
                .counter().count()).isZero();
        assertThat(registry.get("nominations.abm.timeouts").tag("source", "ABM_ADAPTER").counter().count()).isZero();
        assertThat(registry.get("nominations.abm.timeouts").tag("source", "SWEEPER").counter().count()).isZero();
        assertThat(registry.get("nominations.reprocessed").counter().count()).isZero();
        assertThat(registry.get("nominations.resolution.time").tag("status", "APPROVED").timer().count()).isZero();
    }

    @Test
    void submissionsAndResponsesByOutcome() {
        metrics.submission(SubmissionOutcome.SUBMITTED);
        metrics.submission(SubmissionOutcome.UNAVAILABLE);
        metrics.submission(SubmissionOutcome.UNAVAILABLE);
        metrics.abmResponse(ResolutionOutcome.CONFLICT);

        assertThat(registry.get("abm.submissions").tag("outcome", "SUBMITTED").counter().count()).isEqualTo(1);
        assertThat(registry.get("abm.submissions").tag("outcome", "UNAVAILABLE").counter().count()).isEqualTo(2);
        assertThat(registry.get("abm.responses").tag("outcome", "CONFLICT").counter().count()).isEqualTo(1);
    }

    @Test
    void approvedResolutionCountsWithoutReasonAndRecordsTimeSinceCreation() {
        var nomination = nomination();
        nomination.resolve(AbmDecision.approved(), CREATED_AT.plusSeconds(90));

        metrics.resolved(nomination);

        assertThat(registry.get("nominations.resolved").tags("status", "APPROVED", "reason", "none").counter().count())
                .isEqualTo(1);
        var timer = registry.get("nominations.resolution.time").tag("status", "APPROVED").timer();
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(TimeUnit.SECONDS)).isEqualTo(90);
    }

    @Test
    void rejectedResolutionIsTaggedWithTheNormalizedReason() {
        var nomination = nomination();
        nomination.resolve(AbmDecision.rejected(RejectionReason.ACCOUNT_BLOCKED, "ABM-051"), CREATED_AT.plusSeconds(5));

        metrics.resolved(nomination);

        assertThat(registry.get("nominations.resolved").tags("status", "REJECTED", "reason", "ACCOUNT_BLOCKED")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("nominations.resolution.time").tag("status", "REJECTED").timer().count()).isEqualTo(1);
    }

    @Test
    void nonFinalNominationIsNotCountedAsResolved() {
        metrics.resolved(nomination());

        assertThat(registry.find("nominations.resolved").counters()).allMatch(c -> c.count() == 0);
    }

    @Test
    void timeoutsBySourceAndReprocess() {
        metrics.abmTimeout(ChangeSource.SWEEPER);
        metrics.abmTimeout(ChangeSource.ABM_ADAPTER);
        metrics.abmTimeout(ChangeSource.SWEEPER);
        metrics.reprocessed();

        assertThat(registry.get("nominations.abm.timeouts").tag("source", "SWEEPER").counter().count()).isEqualTo(2);
        assertThat(registry.get("nominations.abm.timeouts").tag("source", "ABM_ADAPTER").counter().count())
                .isEqualTo(1);
        assertThat(registry.get("nominations.reprocessed").counter().count()).isEqualTo(1);
    }

    @Test
    void noMeterUsesPerOperationIdentifiersAsTags() {
        metrics.created("ENT01");
        metrics.resolved(resolvedNomination());

        assertThat(registry.getMeters()).flatExtracting(m -> m.getId().getTags())
                .extracting(tag -> tag.getKey())
                .doesNotContain("nomination_id", "request_id", "correlation_id", "customer_id");
        assertThat(registry.getMeters()).extracting(Meter::getId).extracting(id -> id.getName())
                .allMatch(name -> name.startsWith("nominations.") || name.startsWith("abm."));
    }

    private static Nomination nomination() {
        return Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("0001234567890987654"),
                new CardToken("tok_4f9a2c"), null, "corr-1", CREATED_AT);
    }

    private static Nomination resolvedNomination() {
        var nomination = nomination();
        nomination.resolve(AbmDecision.approved(), CREATED_AT.plusSeconds(1));
        return nomination;
    }
}
