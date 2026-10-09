package com.prisma.nominations;

import com.prisma.nominations.application.port.in.AbmResponseCommand;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.in.ProcessAbmResponseUseCase;
import com.prisma.nominations.application.port.in.SweepStaleNominationsUseCase;
import com.prisma.nominations.application.port.out.NominationRepository;
import com.prisma.nominations.domain.AbmDecision;
import com.prisma.nominations.domain.ResolutionOutcome;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * E6 - ABM no responde: sweeper del SLA (PENDING_ABM → ABM_TIMEOUT), reproceso controlado por un operador
 * (ABM_TIMEOUT → RECEIVED + nuevo pedido en el outbox) y respuesta tardía de ABM (ABM_TIMEOUT → APPROVED + un único
 * nomination.result), contra PostgreSQL real.
 * <p>
 * Todo lo asincrónico apagado (adapter, consumer de respuestas, simulador, relay y el ciclo del sweeper): los casos
 * de uso se invocan a mano y el resultado se verifica en la base, sin esperas. "Vieja" = updated_at llevado un día
 * atrás con SQL (las candidatas se toman de la más vieja a la más nueva, así que siempre entra en el lote).
 */
@SpringBootTest(properties = {
        "nominations.abm.adapter.enabled=false",
        "nominations.abm.response-consumer.enabled=false",
        "nominations.abm-mock.enabled=false",
        "nominations.abm.sweeper.enabled=false",
        "nominations.outbox.relay.enabled=false"})
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class AbmTimeoutRecoveryIntegrationTest {

    private static final String REPROCESS = "/internal/v1/nominations/{id}/reprocess";

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private CreateNominationUseCase createNomination;
    @Autowired
    private NominationRepository repository;
    @Autowired
    private SweepStaleNominationsUseCase sweeper;
    @Autowired
    private ProcessAbmResponseUseCase processAbmResponse;

    @Test
    @DisplayName("Sweeper: solo vence PENDING_ABM fuera del SLA, con historial SWEEPER y sin resultado")
    void sweeperTimesOutOnlyStalePendingNominations() {
        UUID stale = pendingAbm(true);
        UUID recent = pendingAbm(false);

        assertThat(sweeper.sweep()).isGreaterThanOrEqualTo(1);

        assertThat(statusOf(stale)).isEqualTo("ABM_TIMEOUT");
        assertThat(statusOf(recent)).isEqualTo("PENDING_ABM");
        assertThat(lastHistory(stale)).isEqualTo(List.of("ABM_TIMEOUT", "SWEEPER"));
        assertThat(outboxCount(stale, "nomination.result")).isZero();
    }

    @Test
    @DisplayName("Reproceso: ABM_TIMEOUT → RECEIVED (202), historial OPERATOR y nuevo nomination.requested")
    void operatorReprocessesTimedOutNomination() throws Exception {
        UUID id = timedOut();

        mockMvc.perform(post(REPROCESS, id).header(ApiHeaders.CORRELATION_ID, "it-reprocess-" + id))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/v1/nominations/" + id))
                .andExpect(header().string(ApiHeaders.CORRELATION_ID, "it-reprocess-" + id))
                .andExpect(jsonPath("$.nomination_id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("RECEIVED"))
                .andExpect(jsonPath("$.account_id").value("****7654"));

        assertThat(statusOf(id)).isEqualTo("RECEIVED");
        assertThat(lastHistory(id)).isEqualTo(List.of("RECEIVED", "OPERATOR"));
        assertThat(jdbc.queryForList("""
                SELECT to_status FROM nomination_history WHERE nomination_id = ? ORDER BY occurred_at, id""",
                String.class, id)).containsExactly("RECEIVED", "PENDING_ABM", "ABM_TIMEOUT", "RECEIVED");
        assertThat(outboxCount(id, "nomination.requested")).isEqualTo(2);
        assertThat(outboxCount(id, "nomination.result")).isZero();
    }

    @Test
    @DisplayName("Respuesta tardía de ABM tras el timeout: APPROVED y un único nomination.result; luego no se reprocesa")
    void lateAbmResponseAfterTimeoutIsAppliedAndBlocksReprocess() throws Exception {
        UUID id = timedOut();
        UUID requestId = jdbc.queryForObject("SELECT request_id FROM nominations WHERE id = ?", UUID.class, id);

        var outcome = processAbmResponse.process(
                new AbmResponseCommand(id, requestId, "it-late", "ABM-OP-LATE", AbmDecision.approved()));

        assertThat(outcome).isEqualTo(ResolutionOutcome.APPLIED);
        assertThat(statusOf(id)).isEqualTo("APPROVED");
        assertThat(lastHistory(id)).isEqualTo(List.of("APPROVED", "ABM_RESPONSE"));
        assertThat(outboxCount(id, "nomination.result")).isEqualTo(1);

        mockMvc.perform(post(REPROCESS, id))
                .andExpect(status().isConflict())
                .andExpect(header().string("Content-Type", "application/problem+json"))
                .andExpect(jsonPath("$.code").value("INVALID_STATE_TRANSITION"))
                .andExpect(jsonPath("$.detail").value("La nominación no está en un estado que admita esta operación"));

        assertThat(statusOf(id)).isEqualTo("APPROVED");
        assertThat(outboxCount(id, "nomination.requested")).isEqualTo(1);
    }

    @Test
    @DisplayName("Reproceso de una nominación inexistente: 404")
    void reprocessUnknownIsNotFound() throws Exception {
        mockMvc.perform(post(REPROCESS, UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("NOMINATION_NOT_FOUND"));
    }

    @Test
    @DisplayName("Reproceso con id que no es UUID: 400")
    void reprocessWithInvalidIdIsBadRequest() throws Exception {
        mockMvc.perform(post(REPROCESS, "no-es-uuid"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_PARAMETER"));
    }

    // ---------------------------------------------------------------- soporte

    /** Alta real (nominación + outbox) y envío a ABM confirmado; {@code stale} la deja un día sin cambios. */
    private UUID pendingAbm(boolean stale) {
        String entity = "IT" + UUID.randomUUID().toString().replace("-", "").substring(0, 10).toUpperCase();
        var created = createNomination.create(new CreateNominationCommand(entity, UUID.randomUUID(), "CUST-000123",
                "0001234567890987654", "tok_timeout_01", "CUENTA SUELDO", "it-timeout-" + UUID.randomUUID()));
        var nomination = repository.findById(created.nomination().id()).orElseThrow();
        assertThat(nomination.markSentToAbm(Instant.now())).isTrue();
        repository.save(nomination);
        if (stale) {
            jdbc.update("UPDATE nominations SET updated_at = now() - interval '1 day' WHERE id = ?", nomination.id());
        }
        return nomination.id();
    }

    private UUID timedOut() {
        UUID id = pendingAbm(true);
        sweeper.sweep();
        assertThat(statusOf(id)).isEqualTo("ABM_TIMEOUT");
        return id;
    }

    private String statusOf(UUID id) {
        return jdbc.queryForObject("SELECT status FROM nominations WHERE id = ?", String.class, id);
    }

    /** [to_status, source] del último cambio de estado. */
    private List<String> lastHistory(UUID id) {
        return jdbc.queryForObject("""
                SELECT to_status, source FROM nomination_history WHERE nomination_id = ?
                 ORDER BY occurred_at DESC, id DESC LIMIT 1""",
                (rs, n) -> List.of(rs.getString(1), rs.getString(2)), id);
    }

    private int outboxCount(UUID id, String eventType) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = ?",
                Integer.class, id, eventType);
    }
}
