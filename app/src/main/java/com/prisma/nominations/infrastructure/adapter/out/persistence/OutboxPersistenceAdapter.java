package com.prisma.nominations.infrastructure.adapter.out.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.prisma.nominations.application.event.IntegrationEvent;
import com.prisma.nominations.application.port.out.OutboxPort;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;

/**
 * Escritura del outbox con JDBC: es un INSERT sin lectura ni ciclo de vida, y el relay lee la misma
 * tabla con JDBC. Con JpaTransactionManager, JdbcTemplate usa la misma conexión que la transacción JPA
 * en curso, así que la fila queda atada al commit (o rollback) del cambio de estado.
 */
@Component
class OutboxPersistenceAdapter implements OutboxPort {

    /** Un único nomination.result por nominación (V1__create_nominations_schema.sql). */
    static final String SINGLE_RESULT_INDEX = "uq_outbox_events_single_result";
    static final String AGGREGATE_TYPE = "nomination";

    private static final String INSERT = """
            INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, topic, payload, headers, created_at)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)
            """;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    OutboxPersistenceAdapter(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    /**
     * MANDATORY: fuera de una transacción de negocio falla, nunca abre una propia. Un segundo
     * nomination.result para la misma nominación se rechaza con {@link IllegalStateException}.
     */
    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(IntegrationEvent event) {
        try {
            jdbc.update(INSERT, event.eventId(), event.nominationId(), AGGREGATE_TYPE, event.eventType(),
                    KafkaTopics.topicFor(event.eventType()), payload(event), headers(event),
                    OffsetDateTime.ofInstant(event.occurredAt(), ZoneOffset.UTC));
        } catch (DuplicateKeyException e) {
            if (String.valueOf(e.getMessage()).contains(SINGLE_RESULT_INDEX)) {
                throw new IllegalStateException(
                        "Ya existe un nomination.result para la nominación " + event.nominationId(), e);
            }
            throw e;
        }
    }

    /** Campos del record en snake_case, más event_type y schema_version (métodos, no componentes). */
    private String payload(IntegrationEvent event) {
        ObjectNode node = mapper.valueToTree(event);
        node.put("event_type", event.eventType());
        node.put("schema_version", event.schemaVersion());
        return write(node);
    }

    /** Valores string: el relay los copia tal cual como headers de Kafka. */
    private String headers(IntegrationEvent event) {
        var headers = new LinkedHashMap<String, String>();
        headers.put(KafkaTopics.HEADER_EVENT_ID, event.eventId().toString());
        headers.put(KafkaTopics.HEADER_EVENT_TYPE, event.eventType());
        headers.put(KafkaTopics.HEADER_SCHEMA_VERSION, String.valueOf(event.schemaVersion()));
        headers.put(KafkaTopics.HEADER_CORRELATION_ID, event.correlationId());
        return write(headers);
    }

    private String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("No se pudo serializar el evento del outbox", e);
        }
    }
}
