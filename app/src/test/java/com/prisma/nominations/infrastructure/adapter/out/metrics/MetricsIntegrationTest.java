package com.prisma.nominations.infrastructure.adapter.out.metrics;

import com.prisma.nominations.TestcontainersConfiguration;
import com.prisma.nominations.application.port.in.AbmResponseCommand;
import com.prisma.nominations.application.port.in.CreateNominationCommand;
import com.prisma.nominations.application.port.in.CreateNominationUseCase;
import com.prisma.nominations.application.port.in.ProcessAbmResponseUseCase;
import com.prisma.nominations.domain.AbmDecision;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.Arrays;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Métricas de punta a punta contra PostgreSQL y Kafka reales, leídas del registry de Prometheus (lo mismo que expone
 * /actuator/prometheus). Los casos de uso se invocan directo; lo asincrónico está apagado y el relay también, así el
 * evento queda pendiente en el outbox.
 */
@SpringBootTest(properties = {
        "nominations.abm.adapter.enabled=false",
        "nominations.abm.response-consumer.enabled=false",
        "nominations.abm-mock.enabled=false",
        "nominations.abm.sweeper.enabled=false",
        "nominations.outbox.relay.enabled=false",
        "nominations.metrics.kafka-dlt.initial-delay=1h"})
@AutoConfigureObservability(tracing = false)
@Import(TestcontainersConfiguration.class)
class MetricsIntegrationTest {

    private static final String ENTITY = "ENTMETRICS";

    @Autowired
    private PrometheusMeterRegistry prometheus;
    @Autowired
    private CreateNominationUseCase createNomination;
    @Autowired
    private ProcessAbmResponseUseCase processAbmResponse;
    @Autowired
    private DatabaseBacklogGauges backlogGauges;
    @Autowired
    private KafkaDltGauges dltGauges;

    @Test
    @DisplayName("Alta + resolución: contadores, timer, gauges de backlog y DLT, y métricas de Resilience4j")
    void exposesTheMetricsContract() {
        var created = createNomination.create(new CreateNominationCommand(ENTITY, UUID.randomUUID(), "123456",
                "0001234567890987654", "tok_4f9a2c", null, "it-metrics-1")).nomination();
        createNomination.create(new CreateNominationCommand(ENTITY, UUID.randomUUID(), "123456",
                "0001234567890987655", "tok_4f9a2d", null, "it-metrics-2"));

        backlogGauges.refresh();
        dltGauges.refresh();
        var beforeResolution = prometheus.scrape();

        assertThat(value(beforeResolution, "nominations_received_total", Map.of("entity_id", ENTITY))).hasValue(2);
        assertThat(value(beforeResolution, "nominations_open", Map.of("status", "RECEIVED")).orElseThrow())
                .isGreaterThanOrEqualTo(2);
        assertThat(value(beforeResolution, "nominations_open", Map.of("status", "PENDING_ABM"))).isPresent();
        assertThat(value(beforeResolution, "nominations_open", Map.of("status", "ABM_TIMEOUT"))).isPresent();
        assertThat(value(beforeResolution, "outbox_pending", Map.of()).orElseThrow()).isGreaterThanOrEqualTo(2);
        assertThat(value(beforeResolution, "outbox_failing", Map.of())).hasValue(0);
        assertThat(value(beforeResolution, "outbox_oldest_pending_age_seconds", Map.of()).orElseThrow())
                .isGreaterThanOrEqualTo(0);
        assertThat(value(beforeResolution, "kafka_dlt_messages", Map.of("topic", "nomination.requested.v1-dlt")))
                .hasValue(0);
        assertThat(value(beforeResolution, "kafka_dlt_messages", Map.of("topic", "abm.responses.v1-dlt")))
                .hasValue(0);
        // Resilience4j publica sus propias métricas (instancia "abm" del ResilientAbmClient).
        assertThat(value(beforeResolution, "resilience4j_circuitbreaker_state",
                Map.of("name", "abm", "state", "closed"))).hasValue(1);
        assertThat(beforeResolution).contains("resilience4j_retry_calls_total{");
        assertThat(value(beforeResolution, "abm_submissions_total", Map.of("outcome", "UNAVAILABLE"))).hasValue(0);

        processAbmResponse.process(new AbmResponseCommand(created.id(), created.requestId(), "it-metrics-1",
                "ABM-OP-IT", AbmDecision.approved()));
        backlogGauges.refresh();
        var afterResolution = prometheus.scrape();

        assertThat(value(afterResolution, "abm_responses_total", Map.of("outcome", "APPLIED"))).hasValue(1);
        assertThat(value(afterResolution, "nominations_resolved_total", Map.of("status", "APPROVED", "reason", "none")))
                .hasValue(1);
        assertThat(value(afterResolution, "nominations_resolution_time_seconds_count", Map.of("status", "APPROVED")))
                .hasValue(1);
        assertThat(afterResolution).contains("nominations_resolution_time_seconds_bucket{");
        assertThat(value(afterResolution, "nominations_open", Map.of("status", "RECEIVED")).orElseThrow())
                .isEqualTo(value(beforeResolution, "nominations_open", Map.of("status", "RECEIVED")).orElseThrow() - 1);
        assertThat(afterResolution).contains("application=\"prisma-nominations-api\"");
        // Ninguna serie pierde su nombre por los sufijos reservados del cliente Prometheus (_created, _info...).
        assertThat(afterResolution).doesNotContain("\nnominations_total{");
    }

    /** Valor de la primera muestra {@code name{...}} que tiene todas las etiquetas pedidas. */
    private static OptionalDouble value(String scrape, String name, Map<String, String> labels) {
        return Arrays.stream(scrape.split("\n"))
                .filter(line -> line.startsWith(name + "{") || line.startsWith(name + " "))
                .filter(line -> labels.entrySet().stream()
                        .allMatch(l -> line.contains(l.getKey() + "=\"" + l.getValue() + "\"")))
                .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
                .findFirst();
    }
}
