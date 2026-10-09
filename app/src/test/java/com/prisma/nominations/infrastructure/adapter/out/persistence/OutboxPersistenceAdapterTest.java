package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.PostgresTestcontainersConfiguration;
import com.prisma.nominations.application.event.NominationRequested;
import com.prisma.nominations.application.event.NominationResult;
import com.prisma.nominations.domain.AbmDecision;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.Nomination;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Outbox contra PostgreSQL real. Sin transacción de test (NOT_SUPPORTED): cada caso controla sus
 * propias transacciones para poder verificar commit, rollback y el uso fuera de transacción.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@ImportAutoConfiguration(JacksonAutoConfiguration.class)
@Import({PostgresTestcontainersConfiguration.class, OutboxPersistenceAdapter.class, NominationPersistenceAdapter.class})
@Tag("integration")
class OutboxPersistenceAdapterTest {

    private static final Instant NOW = Instant.parse("2026-10-08T12:00:00Z");

    @Autowired
    private OutboxPersistenceAdapter outbox;

    @Autowired
    private NominationPersistenceAdapter nominations;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private static Nomination newNomination() {
        return Nomination.receive("ENT01", UUID.randomUUID(), "123456", new AccountId("0011223344556677"),
                new CardToken("tok_4f9a2c"), "CUENTA_PRINCIPAL", "corr-outbox", NOW);
    }

    private static Nomination approved() {
        var n = newNomination();
        n.markSentToAbm(NOW);
        n.resolve(AbmDecision.approved(), NOW);
        return n;
    }

    private TransactionTemplate tx() {
        return new TransactionTemplate(transactionManager);
    }

    private int outboxRows(UUID aggregateId) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ?", Integer.class,
                aggregateId);
    }

    @Test
    void appendStoresRowWithPayloadAndHeaders() throws Exception {
        var nomination = newNomination();
        var event = NominationRequested.of(nomination, NOW);

        tx().executeWithoutResult(status -> outbox.append(event));

        var row = jdbc.queryForMap("SELECT * FROM outbox_events WHERE id = ?", event.eventId());
        assertThat(row.get("aggregate_id")).isEqualTo(nomination.id());
        assertThat(row.get("aggregate_type")).isEqualTo("nomination");
        assertThat(row.get("event_type")).isEqualTo("nomination.requested");
        assertThat(row.get("topic")).isEqualTo("nomination.requested.v1");
        assertThat(((Timestamp) row.get("created_at")).toInstant()).isEqualTo(NOW);
        assertThat(row.get("published_at")).isNull();
        assertThat(row.get("attempts")).isEqualTo(0);

        JsonNode payload = mapper.readTree(row.get("payload").toString());
        assertThat(payload.get("event_id").asText()).isEqualTo(event.eventId().toString());
        assertThat(payload.get("event_type").asText()).isEqualTo("nomination.requested");
        assertThat(payload.get("schema_version").asInt()).isEqualTo(1);
        assertThat(payload.get("nomination_id").asText()).isEqualTo(nomination.id().toString());
        assertThat(payload.get("request_id").asText()).isEqualTo(nomination.requestId().toString());
        assertThat(payload.get("account_id").asText()).isEqualTo("0011223344556677");
        assertThat(payload.get("card_id").asText()).isEqualTo("tok_4f9a2c");
        assertThat(payload.get("correlation_id").asText()).isEqualTo("corr-outbox");
        assertThat(payload.get("occurred_at").asText()).isEqualTo("2026-10-08T12:00:00Z");
        assertThat(payload.has("eventId")).isFalse();

        JsonNode headers = mapper.readTree(row.get("headers").toString());
        assertThat(headers.get("event_id").asText()).isEqualTo(event.eventId().toString());
        assertThat(headers.get("event_type").asText()).isEqualTo("nomination.requested");
        assertThat(headers.get("schema_version").asText()).isEqualTo("1");
        assertThat(headers.get("correlation_id").asText()).isEqualTo("corr-outbox");
    }

    @Test
    void resultPayloadIsMasked() throws Exception {
        var event = NominationResult.of(approved(), NOW);

        tx().executeWithoutResult(status -> outbox.append(event));

        var row = jdbc.queryForMap("SELECT topic, payload FROM outbox_events WHERE id = ?", event.eventId());
        assertThat(row.get("topic")).isEqualTo("nomination.result.v1");
        JsonNode payload = mapper.readTree(row.get("payload").toString());
        assertThat(payload.get("status").asText()).isEqualTo("APPROVED");
        assertThat(payload.get("account_id").asText()).doesNotContain("0011223344556677");
        assertThat(payload.has("rejection_reason")).isFalse();
    }

    @Test
    @Tag("E8")
    void rollbackDiscardsNominationAndEvent() {
        var nomination = newNomination();

        assertThatThrownBy(() -> tx().executeWithoutResult(status -> {
            var saved = nominations.save(nomination);
            outbox.append(NominationRequested.of(saved, NOW));
            throw new IllegalStateException("falla posterior en la misma transacción");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(jdbc.queryForObject("SELECT count(*) FROM nominations WHERE id = ?", Integer.class,
                nomination.id())).isZero();
        assertThat(outboxRows(nomination.id())).isZero();
    }

    @Test
    @Tag("E8")
    void appendOutsideTransactionFails() {
        var event = NominationRequested.of(newNomination(), NOW);

        assertThatThrownBy(() -> outbox.append(event)).isInstanceOf(IllegalTransactionStateException.class);
        assertThat(outboxRows(event.nominationId())).isZero();
    }

    @Test
    @Tag("E7")
    void onlyOneResultPerNomination() {
        var nomination = approved();
        tx().executeWithoutResult(status -> outbox.append(NominationResult.of(nomination, NOW)));

        assertThatThrownBy(() -> tx().executeWithoutResult(
                status -> outbox.append(NominationResult.of(nomination, NOW))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Ya existe un nomination.result")
                .hasMessageContaining(nomination.id().toString());
        assertThat(outboxRows(nomination.id())).isEqualTo(1);
    }

    @Test
    void severalRequestsPerNominationAreAllowed() {
        var nomination = newNomination();

        tx().executeWithoutResult(status -> outbox.append(NominationRequested.of(nomination, NOW)));
        tx().executeWithoutResult(status -> outbox.append(NominationRequested.of(nomination, NOW)));

        assertThat(outboxRows(nomination.id())).isEqualTo(2);
    }
}
