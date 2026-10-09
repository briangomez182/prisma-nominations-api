package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.prisma.nominations.PostgresTestcontainersConfiguration;
import com.prisma.nominations.domain.AbmDecision;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.RejectionReason;
import com.prisma.nominations.domain.StatusChange;
import jakarta.persistence.PersistenceException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;

import java.time.Instant;
import java.util.UUID;

import static com.prisma.nominations.domain.NominationStatus.APPROVED;
import static com.prisma.nominations.domain.NominationStatus.PENDING_ABM;
import static com.prisma.nominations.domain.NominationStatus.RECEIVED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({PostgresTestcontainersConfiguration.class, NominationPersistenceAdapter.class})
class NominationPersistenceAdapterTest {

    private static final Instant NOW = Instant.parse("2026-10-08T12:00:00Z");

    @Autowired
    private NominationPersistenceAdapter adapter;

    @Autowired
    private TestEntityManager em;

    private static Nomination newNomination(String entityId, UUID requestId) {
        return Nomination.receive(entityId, requestId, "123456", new AccountId("987654"),
                new CardToken("tok_4f9a2c"), "CUENTA_PRINCIPAL", "corr-1", NOW);
    }

    @Test
    void savesStateAndHistoryAndFindsByIdempotencyKey() {
        var requestId = UUID.randomUUID();
        var saved = adapter.save(newNomination("ENT01", requestId));
        saved.markSentToAbm(NOW);
        adapter.save(saved);

        var found = adapter.findByEntityIdAndRequestId("ENT01", requestId).orElseThrow();
        assertThat(found.id()).isEqualTo(saved.id());
        assertThat(found.status()).isEqualTo(PENDING_ABM);
        assertThat(found.accountId().value()).isEqualTo("987654");
        assertThat(adapter.findHistory(saved.id()))
                .extracting(StatusChange::to)
                .containsExactly(RECEIVED, PENDING_ABM);
    }

    @Test
    void sameRequestIdIsUniquePerEntity() {
        var requestId = UUID.randomUUID();
        adapter.save(newNomination("ENT01", requestId));

        adapter.save(newNomination("ENT02", requestId));
        assertThatThrownBy(() -> adapter.save(newNomination("ENT01", requestId)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void concurrentAbmResponsesOnlyOneWins() {
        var id = adapter.save(newNomination("ENT01", UUID.randomUUID())).id();
        var first = adapter.findById(id).orElseThrow();
        var second = adapter.findById(id).orElseThrow();

        first.resolve(AbmDecision.approved(), NOW);
        adapter.save(first);

        second.resolve(AbmDecision.rejected(RejectionReason.OTHER, "X"), NOW);
        assertThatThrownBy(() -> adapter.save(second))
                .isInstanceOf(OptimisticLockingFailureException.class);
        em.clear();
        assertThat(adapter.findById(id).orElseThrow().status()).isEqualTo(APPROVED);
    }

    @Test
    void historyIsAppendOnlyAtDatabaseLevel() {
        adapter.save(newNomination("ENT01", UUID.randomUUID()));

        assertThatThrownBy(() -> em.getEntityManager()
                .createNativeQuery("UPDATE nomination_history SET detail = 'adulterado'")
                .executeUpdate())
                .isInstanceOf(PersistenceException.class)
                .hasStackTraceContaining("solo de inserción");
    }
}
