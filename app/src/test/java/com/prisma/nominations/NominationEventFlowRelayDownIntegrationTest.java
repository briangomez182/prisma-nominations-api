package com.prisma.nominations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.out.messaging.OutboxRelay;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static com.prisma.nominations.NominationEventFlowIntegrationTest.body;
import static com.prisma.nominations.NominationEventFlowIntegrationTest.create;
import static com.prisma.nominations.NominationEventFlowIntegrationTest.newEntity;
import static com.prisma.nominations.NominationEventFlowIntegrationTest.requestedRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * E8 de punta a punta con el relay programado apagado: "Kafka no recibe" mientras no se invoca el relay, y
 * {@link OutboxRelay#relayOnce()} simula que vuelve. Contexto propio (relay off) separado del de
 * {@link NominationEventFlowIntegrationTest}, donde el relay corre como en producción.
 * <p>
 * La falla real de envío a Kafka (error del broker, ack que no llega dentro del send-timeout, intento registrado
 * y lote cortado) está cubierta en {@code OutboxRelayIntegrationTest}.
 */
@SpringBootTest(properties = {"nominations.outbox.relay.enabled=false", "nominations.abm.adapter.enabled=false"})
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
class NominationEventFlowRelayDownIntegrationTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private OutboxRelay relay;

    @Nested
    @DisplayName("E8 - consistencia estado ↔ publicación con Kafka sin publicar")
    class E8StateAndPublicationConsistency {

        @Test
        void nominationIsPersistedAndEventWaitsInOutboxUntilKafkaIsBack() throws Exception {
            String entity = newEntity();
            UUID first = post(entity);

            // La nominación existe y su evento quedó pendiente en el outbox (misma TX), sin llegar a Kafka.
            mockMvc.perform(get(NominationEventFlowIntegrationTest.BASE + "/" + first)
                            .header(HttpHeaders.AUTHORIZATION, TestTokens.bearer(entity)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status").value("RECEIVED"));
            assertPending(first);

            // El sistema sigue respondiendo: otra alta se acepta igual y también queda pendiente.
            UUID second = post(entity);
            assertPending(second);

            try (var probe = new KafkaTopicProbe(kafka.getBootstrapServers(), KafkaTopics.NOMINATION_REQUESTED)) {
                assertThat(probe.recordsWithKey(first)).isEmpty();
                assertThat(probe.recordsWithKey(second)).isEmpty();

                // "Kafka vuelve": el relay publica los pendientes y los marca.
                int published = 0;
                int batch;
                while ((batch = relay.relayOnce()) > 0) {
                    published += batch;
                }
                assertThat(published).isGreaterThanOrEqualTo(2);

                for (UUID nominationId : new UUID[]{first, second}) {
                    Map<String, Object> row = requestedRow(jdbc, nominationId);
                    assertThat(row.get("published_at")).isNotNull();
                    assertThat(row.get("attempts")).isEqualTo(1);
                    var records = probe.recordsWithKey(nominationId);
                    assertThat(records).hasSize(1);
                    assertThat(KafkaTopicProbe.header(records.getFirst(), KafkaTopics.HEADER_EVENT_ID))
                            .isEqualTo(row.get("id").toString());
                }
            }
        }
    }

    private UUID post(String entity) throws Exception {
        String response = mockMvc.perform(create(entity, body(UUID.randomUUID())))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);
        return UUID.fromString(objectMapper.readTree(response).get("nomination_id").asText());
    }

    private void assertPending(UUID nominationId) {
        Map<String, Object> row = requestedRow(jdbc, nominationId);
        assertThat(row.get("published_at")).isNull();
        assertThat(row.get("attempts")).isEqualTo(0);
    }
}
