package com.prisma.nominations.infrastructure.adapter.in.web;

import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationResult;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.in.GetNominationQuery;
import com.prisma.nominations.domain.AccountId;
import com.prisma.nominations.domain.CardToken;
import com.prisma.nominations.domain.ChangeSource;
import com.prisma.nominations.domain.Nomination;
import com.prisma.nominations.domain.NominationStatus;
import com.prisma.nominations.domain.RejectionReason;
import com.prisma.nominations.domain.StatusChange;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(NominationController.class)
class NominationControllerTest {

    private static final String ENTITY = "ENT01";
    private static final UUID NOMINATION_ID = UUID.fromString("7d1e6c2a-4b8f-4a51-9c3e-2f6a8b0d1e23");
    private static final UUID REQUEST_ID = UUID.fromString("0b4a9f2e-6c1d-4e7a-8b3f-5d2c1a0e9f87");
    private static final Instant CREATED = Instant.parse("2026-10-08T12:00:00Z");
    private static final Instant UPDATED = Instant.parse("2026-10-08T12:05:00Z");

    private static final String VALID_BODY = """
            {
              "request_id": "%s",
              "customer_id": "123456",
              "account_id": "0001234567890987654",
              "card_id": "tok_4f9a2c7b",
              "alias": "CUENTA PRINCIPAL"
            }
            """.formatted(REQUEST_ID);

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CreateNominationUseCase createNomination;

    @MockitoBean
    private GetNominationQuery getNomination;

    @Test
    void postNewNominationReturns202WithLocationAndMaskedBody() throws Exception {
        when(createNomination.create(any())).thenReturn(new CreateNominationResult(received(), false));

        mockMvc.perform(post("/v1/nominations")
                        .header(ApiHeaders.ENTITY_ID, ENTITY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/v1/nominations/" + NOMINATION_ID))
                .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "false"))
                .andExpect(jsonPath("$.nomination_id").value(NOMINATION_ID.toString()))
                .andExpect(jsonPath("$.request_id").value(REQUEST_ID.toString()))
                .andExpect(jsonPath("$.status").value("RECEIVED"))
                .andExpect(jsonPath("$.customer_id").value("123456"))
                .andExpect(jsonPath("$.account_id").value("****7654"))
                .andExpect(jsonPath("$.card_id").value("****2c7b"))
                .andExpect(jsonPath("$.alias").value("CUENTA PRINCIPAL"))
                .andExpect(jsonPath("$.correlation_id").value("corr-1"))
                .andExpect(jsonPath("$.created_at").value("2026-10-08T12:00:00Z"))
                .andExpect(jsonPath("$.rejection_reason").doesNotExist())
                .andExpect(jsonPath("$.abm_reason_code").doesNotExist());
    }

    @Test
    void postReplayStillReturns202AndFlagsReplay() throws Exception {
        when(createNomination.create(any())).thenReturn(new CreateNominationResult(received(), true));

        mockMvc.perform(post("/v1/nominations")
                        .header(ApiHeaders.ENTITY_ID, ENTITY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/v1/nominations/" + NOMINATION_ID))
                .andExpect(header().string(ApiHeaders.IDEMPOTENT_REPLAYED, "true"))
                .andExpect(jsonPath("$.nomination_id").value(NOMINATION_ID.toString()));
    }

    @Test
    void postPassesEntityFromHeaderAndFullDataToUseCase() throws Exception {
        when(createNomination.create(any())).thenReturn(new CreateNominationResult(received(), false));

        mockMvc.perform(post("/v1/nominations")
                        .header(ApiHeaders.ENTITY_ID, ENTITY)
                        .header(ApiHeaders.CORRELATION_ID, "corr-from-channel")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isAccepted());

        var captor = ArgumentCaptor.forClass(CreateNominationCommand.class);
        verify(createNomination).create(captor.capture());
        var command = captor.getValue();
        assertThat(command.entityId()).isEqualTo(ENTITY);
        assertThat(command.requestId()).isEqualTo(REQUEST_ID);
        assertThat(command.customerId()).isEqualTo("123456");
        assertThat(command.accountId()).isEqualTo("0001234567890987654");
        assertThat(command.cardId()).isEqualTo("tok_4f9a2c7b");
        assertThat(command.alias()).isEqualTo("CUENTA PRINCIPAL");
        // Lo resuelve el filtro (si existe) o el fallback del controller: nunca vacío
        assertThat(command.correlationId()).isNotBlank();
    }

    @Test
    void postWithoutRequiredFieldsReturns400AndDoesNotCallUseCase() throws Exception {
        mockMvc.perform(post("/v1/nominations")
                        .header(ApiHeaders.ENTITY_ID, ENTITY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"alias\": \"CUENTA\"}"))
                .andExpect(status().isBadRequest());

        verify(createNomination, never()).create(any());
    }

    @Test
    void postWithOversizedFieldReturns400AndDoesNotCallUseCase() throws Exception {
        var body = VALID_BODY.replace("0001234567890987654", "A".repeat(35));

        mockMvc.perform(post("/v1/nominations")
                        .header(ApiHeaders.ENTITY_ID, ENTITY)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());

        verify(createNomination, never()).create(any());
    }

    @Test
    void postWithoutEntityHeaderReturns400() throws Exception {
        mockMvc.perform(post("/v1/nominations")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_BODY))
                .andExpect(status().isBadRequest());

        verify(createNomination, never()).create(any());
    }

    @Test
    void getReturnsMaskedNominationWithRejectionReasonButNoAbmCode() throws Exception {
        when(getNomination.get(ENTITY, NOMINATION_ID)).thenReturn(rejected());

        mockMvc.perform(get("/v1/nominations/{id}", NOMINATION_ID).header(ApiHeaders.ENTITY_ID, ENTITY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nomination_id").value(NOMINATION_ID.toString()))
                .andExpect(jsonPath("$.status").value("REJECTED"))
                .andExpect(jsonPath("$.rejection_reason").value("CARD_NOT_ELIGIBLE"))
                .andExpect(jsonPath("$.account_id").value("****7654"))
                .andExpect(jsonPath("$.card_id").value("****2c7b"))
                .andExpect(jsonPath("$.updated_at").value("2026-10-08T12:05:00Z"))
                .andExpect(jsonPath("$.abm_reason_code").doesNotExist())
                .andExpect(jsonPath("$.entity_id").doesNotExist());
    }

    @Test
    void getHistoryReturnsTransitionsInSnakeCase() throws Exception {
        when(getNomination.history(ENTITY, NOMINATION_ID)).thenReturn(List.of(
                new StatusChange(NOMINATION_ID, null, NominationStatus.RECEIVED, ChangeSource.API,
                        "Solicitud recibida", "corr-1", CREATED),
                new StatusChange(NOMINATION_ID, NominationStatus.RECEIVED, NominationStatus.PENDING_ABM,
                        ChangeSource.ABM_ADAPTER, "Enviada a ABM", "corr-1", UPDATED)));

        mockMvc.perform(get("/v1/nominations/{id}/history", NOMINATION_ID).header(ApiHeaders.ENTITY_ID, ENTITY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nomination_id").value(NOMINATION_ID.toString()))
                .andExpect(jsonPath("$.items", hasSize(2)))
                .andExpect(jsonPath("$.items[0].from_status").doesNotExist())
                .andExpect(jsonPath("$.items[0].to_status").value("RECEIVED"))
                .andExpect(jsonPath("$.items[0].source").value("API"))
                .andExpect(jsonPath("$.items[0].detail").value("Solicitud recibida"))
                .andExpect(jsonPath("$.items[0].correlation_id").value("corr-1"))
                .andExpect(jsonPath("$.items[0].occurred_at").value("2026-10-08T12:00:00Z"))
                .andExpect(jsonPath("$.items[1].from_status").value("RECEIVED"))
                .andExpect(jsonPath("$.items[1].to_status").value("PENDING_ABM"))
                .andExpect(jsonPath("$.items[1].source").value("ABM_ADAPTER"));
    }

    private static Nomination received() {
        return nomination(NominationStatus.RECEIVED, null, null, CREATED);
    }

    private static Nomination rejected() {
        return nomination(NominationStatus.REJECTED, RejectionReason.CARD_NOT_ELIGIBLE, "ABM-417", UPDATED);
    }

    private static Nomination nomination(NominationStatus status, RejectionReason reason, String abmCode,
                                         Instant updatedAt) {
        return Nomination.rehydrate(NOMINATION_ID, ENTITY, REQUEST_ID, "123456",
                new AccountId("0001234567890987654"), new CardToken("tok_4f9a2c7b"), "CUENTA PRINCIPAL",
                "corr-1", CREATED, status, reason, abmCode, updatedAt, 0L);
    }
}
