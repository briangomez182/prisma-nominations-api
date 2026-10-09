package com.prisma.nominations.application.service;

import com.prisma.nominations.TestcontainersConfiguration;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationResult;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Requests simultáneas con la misma clave de idempotencia contra PostgreSQL real: la constraint UNIQUE
 * decide la ganadora y el resto responde como replay, sin generar eventos en el outbox.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class NominationCommandServiceConcurrencyTest {

    private static final int THREADS = 10;

    @Autowired
    private CreateNominationUseCase useCase;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void concurrentRequestsWithSameKeyCreateExactlyOneNomination() throws Exception {
        var requestId = UUID.randomUUID();
        var command = new CreateNominationCommand("ENT-CONC", requestId, "123456", "987654", "tok_4f9a2c",
                "CUENTA_PRINCIPAL", "corr-conc");
        var start = new CountDownLatch(1);
        var futures = new ArrayList<Future<CreateNominationResult>>();

        try (var executor = Executors.newFixedThreadPool(THREADS)) {
            for (int i = 0; i < THREADS; i++) {
                futures.add(executor.submit((Callable<CreateNominationResult>) () -> {
                    start.await();
                    return useCase.create(command);
                }));
            }
            start.countDown();

            var results = new ArrayList<CreateNominationResult>();
            for (var future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }

            var id = results.getFirst().nomination().id();
            assertThat(results).allSatisfy(r -> assertThat(r.nomination().id()).isEqualTo(id));
            assertThat(results).filteredOn(r -> !r.replayed()).hasSize(1);
            assertThat(jdbc.queryForObject(
                    "SELECT count(*) FROM nominations WHERE entity_id = ? AND request_id = ?",
                    Integer.class, "ENT-CONC", requestId)).isEqualTo(1);
            assertThat(jdbc.queryForObject(
                    "SELECT count(*) FROM nomination_history WHERE nomination_id = ? AND to_status = 'RECEIVED'",
                    Integer.class, id)).isEqualTo(1);
            assertThat(jdbc.queryForObject(
                    "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'nomination.requested'",
                    Integer.class, id)).isEqualTo(1);
        }
    }
}
