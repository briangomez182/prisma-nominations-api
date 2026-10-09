package com.prisma.nominations.infrastructure.adapter.out.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Relay del Transactional Outbox por polling (D6): publica en Kafka los eventos pendientes.
 *
 * <p>Cada ciclo corre en una transacción que toma un lote con {@code FOR UPDATE SKIP LOCKED}: varias
 * instancias pueden correr en paralelo sin tomar la misma fila (E10). Solo entra al lote el evento
 * pendiente más antiguo de cada nominación (cabeza de la cola): el siguiente espera a que ese se publique,
 * así otra instancia nunca adelanta un evento posterior del mismo agregado.
 *
 * <p>Garantía: <b>at-least-once</b>. La fila se marca publicada recién después del ack de Kafka
 * ({@code acks=all}, productor idempotente). Si el proceso cae entre el ack y el COMMIT, o si el envío
 * excede el timeout pero el broker termina aceptándolo, el evento se publica de nuevo: los consumidores
 * deduplican por {@code event_id}. Nunca se pierde un evento confirmado en la base (E8).
 *
 * <p>Ante una falla de envío se registra el intento y se corta el lote: no se publica nada posterior
 * fuera de orden y no se acumulan timeouts contra un broker caído. El ciclo siguiente reintenta.
 *
 * <p><b>Trazas:</b> si la fila trae {@code traceparent} (contexto W3C guardado al hacer append), el envío corre
 * dentro de un span hijo de esa traza ({@code outbox.relay}) aunque ocurra en otro hilo y más tarde; el
 * KafkaTemplate con observation crea el span PRODUCER e inyecta el {@code traceparent} nuevo en el mensaje, así el
 * consumer continúa la misma traza. Sin traceparent (filas viejas, tracing apagado) se publica sin padre.
 *
 * <p>En producción este componente se reemplaza por Debezium (CDC) sobre la misma tabla.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    static final int MAX_ERROR_LENGTH = 500;
    static final String MDC_CORRELATION_ID = "correlationId";
    static final String RELAY_SPAN_NAME = "outbox.relay";

    /** Cabezas pendientes por agregado, en orden de creación (desempate por id, igual que el ORDER BY). */
    private static final String SELECT_BATCH = """
            SELECT o.id, o.aggregate_id, o.event_type, o.topic, o.payload::text AS payload, o.headers::text AS headers
              FROM outbox_events o
             WHERE o.published_at IS NULL
               AND NOT EXISTS (SELECT 1 FROM outbox_events prev
                                WHERE prev.aggregate_id = o.aggregate_id
                                  AND prev.published_at IS NULL
                                  AND (prev.created_at, prev.id) < (o.created_at, o.id))
             ORDER BY o.created_at, o.id
             LIMIT ?
               FOR UPDATE SKIP LOCKED
            """;
    private static final String MARK_PUBLISHED =
            "UPDATE outbox_events SET published_at = now(), attempts = attempts + 1, last_error = NULL WHERE id = ?";
    private static final String MARK_FAILED =
            "UPDATE outbox_events SET attempts = attempts + 1, last_error = ? WHERE id = ?";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper objectMapper;
    private final int batchSize;
    private final Duration sendTimeout;
    private final Tracer tracer;
    private final Propagator propagator;

    /**
     * Template propio con {@code max.block.ms = send-timeout}: con el broker caído, {@code send()} no
     * bloquea los 60 s por defecto esperando metadata mientras la transacción retiene las filas. Con observation
     * habilitada (span PRODUCER + header traceparent). Tracing opcional: sin él, todo es no-op.
     */
    @Autowired
    OutboxRelay(JdbcTemplate jdbc, PlatformTransactionManager txManager, ProducerFactory<String, String> producerFactory,
                ObjectMapper objectMapper, OutboxProperties properties, ObjectProvider<ObservationRegistry> observations,
                ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        this(jdbc, txManager, observedTemplate(producerFactory, properties, observations), objectMapper, properties,
                tracer.getIfUnique(() -> Tracer.NOOP), propagator.getIfUnique(() -> Propagator.NOOP));
    }

    OutboxRelay(JdbcTemplate jdbc, PlatformTransactionManager txManager, KafkaTemplate<String, String> kafka,
                ObjectMapper objectMapper, OutboxProperties properties) {
        this(jdbc, txManager, kafka, objectMapper, properties, Tracer.NOOP, Propagator.NOOP);
    }

    OutboxRelay(JdbcTemplate jdbc, PlatformTransactionManager txManager, KafkaTemplate<String, String> kafka,
                ObjectMapper objectMapper, OutboxProperties properties, Tracer tracer, Propagator propagator) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(txManager);
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.batchSize = properties.relay().batchSize();
        this.sendTimeout = properties.relay().sendTimeout();
        this.tracer = tracer;
        this.propagator = propagator;
    }

    private static KafkaTemplate<String, String> observedTemplate(ProducerFactory<String, String> producerFactory,
                                                                  OutboxProperties properties,
                                                                  ObjectProvider<ObservationRegistry> observations) {
        var template = new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.MAX_BLOCK_MS_CONFIG, properties.relay().sendTimeout().toMillis()));
        // Creado con new (no es bean): el registry se pasa a mano, no lo resuelve desde el contexto.
        template.setObservationEnabled(true);
        template.setObservationRegistry(observations.getIfUnique(() -> ObservationRegistry.NOOP));
        return template;
    }

    /**
     * Un ciclo: toma un lote, lo publica en orden y confirma. Devuelve cuántos eventos publicó.
     * Público para invocarlo a mano (tests, operación); el ciclo programado es {@link OutboxRelayScheduler}.
     */
    public int relayOnce() {
        Integer published = tx.execute(status -> publishBatch());
        return published == null ? 0 : published;
    }

    private int publishBatch() {
        List<PendingEvent> batch = jdbc.query(SELECT_BATCH, OutboxRelay::mapRow, batchSize);
        int published = 0;
        for (PendingEvent event : batch) {
            if (!publish(event)) {
                break;
            }
            published++;
        }
        if (published > 0) {
            log.debug("Outbox: {} de {} eventos publicados", published, batch.size());
        }
        return published;
    }

    /** Publica un evento y registra el resultado en su fila. {@code false} corta el lote. */
    private boolean publish(PendingEvent event) {
        JsonNode headers = parseHeaders(event);
        String correlationId = text(headers, KafkaTopics.HEADER_CORRELATION_ID, null);
        String previous = MDC.get(MDC_CORRELATION_ID);
        if (correlationId != null) {
            MDC.put(MDC_CORRELATION_ID, correlationId);
        }
        Span span = relaySpan(event, text(headers, KafkaTopics.HEADER_TRACEPARENT, null));
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            kafka.send(toRecord(event, headers)).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
            jdbc.update(MARK_PUBLISHED, event.id());
            log.debug("Outbox: evento {} ({}) publicado en {}", event.id(), event.eventType(), event.topic());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            span.error(e);
            registerFailure(event, e);
            return false;
        } catch (Exception e) {
            span.error(e);
            registerFailure(event, e);
            return false;
        } finally {
            span.end();
            if (previous != null) {
                MDC.put(MDC_CORRELATION_ID, previous);
            } else {
                MDC.remove(MDC_CORRELATION_ID);
            }
        }
    }

    /**
     * Span del envío, hijo del contexto guardado en la fila (si lo hay). Sin traceparent o con uno ilegible, el
     * span abre una traza nueva: la correlación de negocio sigue por correlation_id.
     */
    private Span relaySpan(PendingEvent event, String traceparent) {
        Span.Builder builder = traceparent == null
                ? tracer.spanBuilder()
                : propagator.extract(Map.of(KafkaTopics.HEADER_TRACEPARENT, traceparent), Map::get);
        return builder.name(RELAY_SPAN_NAME)
                .tag("event_type", event.eventType())
                .tag("event_id", event.id().toString())
                .tag("messaging.destination.name", event.topic())
                .start();
    }

    private void registerFailure(PendingEvent event, Exception e) {
        String error = describe(e);
        jdbc.update(MARK_FAILED, error, event.id());
        log.warn("Outbox: no se pudo publicar el evento {} ({}) en {}; se reintenta en el próximo ciclo: {}",
                event.id(), event.eventType(), event.topic(), error);
    }

    /** key = aggregate_id (orden por nominación, D7); value = payload tal como se guardó. */
    private ProducerRecord<String, String> toRecord(PendingEvent event, JsonNode headers) {
        var record = new ProducerRecord<String, String>(event.topic(), event.aggregateId().toString(), event.payload());
        addHeader(record, KafkaTopics.HEADER_EVENT_ID, text(headers, KafkaTopics.HEADER_EVENT_ID, event.id().toString()));
        addHeader(record, KafkaTopics.HEADER_EVENT_TYPE, text(headers, KafkaTopics.HEADER_EVENT_TYPE, event.eventType()));
        addHeader(record, KafkaTopics.HEADER_SCHEMA_VERSION, text(headers, KafkaTopics.HEADER_SCHEMA_VERSION, null));
        addHeader(record, KafkaTopics.HEADER_CORRELATION_ID, text(headers, KafkaTopics.HEADER_CORRELATION_ID, null));
        // Respaldo: con observation, el KafkaTemplate lo reemplaza por el del span PRODUCER (misma traza).
        addHeader(record, KafkaTopics.HEADER_TRACEPARENT, text(headers, KafkaTopics.HEADER_TRACEPARENT, null));
        return record;
    }

    private static void addHeader(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) {
            record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private JsonNode parseHeaders(PendingEvent event) {
        if (event.headers() == null) {
            return null;
        }
        try {
            return objectMapper.readTree(event.headers());
        } catch (JsonProcessingException e) {
            // Headers ilegibles no bloquean la publicación: event_id y event_type salen de las columnas.
            log.warn("Outbox: headers ilegibles en el evento {}", event.id());
            return null;
        }
    }

    private static String text(JsonNode headers, String name, String fallback) {
        JsonNode value = headers == null ? null : headers.get(name);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    /**
     * Causa raíz resumida (tipo + mensaje), truncada al tamaño de la columna. Nunca incluye el payload:
     * los mensajes del cliente Kafka hablan de tópico, partición y timeouts, no del contenido.
     */
    static String describe(Throwable e) {
        Throwable root = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage() == null ? root.getClass().getSimpleName()
                : root.getClass().getSimpleName() + ": " + root.getMessage();
        return message.length() <= MAX_ERROR_LENGTH ? message : message.substring(0, MAX_ERROR_LENGTH);
    }

    private static PendingEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new PendingEvent(rs.getObject("id", UUID.class), rs.getObject("aggregate_id", UUID.class),
                rs.getString("event_type"), rs.getString("topic"), rs.getString("payload"), rs.getString("headers"));
    }

    private record PendingEvent(UUID id, UUID aggregateId, String eventType, String topic, String payload,
                                String headers) {
    }
}
