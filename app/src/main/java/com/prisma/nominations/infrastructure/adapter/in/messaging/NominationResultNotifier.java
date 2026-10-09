package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.DemoConsumerProperties;
import com.prisma.nominations.infrastructure.config.KafkaConsumerConfig;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Consumidor de ejemplo de {@code nomination.result.v1}: simula al servicio de notificaciones, que es "otro
 * sistema". Por eso lee el payload con su propio record y no reutiliza el del productor.
 * <p>
 * <b>Idempotencia (at-least-once → efecto único):</b> el event_id se registra en {@code consumer_processed_events}
 * en la misma transacción que el efecto. Si ya estaba, es un duplicado (reentrega, reenvío del relay): solo ACK.
 * <p>
 * <b>Política de versionado (tolerant reader):</b>
 * <ul>
 *   <li>Campos desconocidos se ignoran: dentro de {@code .v1} el productor solo agrega campos.</li>
 *   <li>{@code schema_version} mayor a {@link #SUPPORTED_SCHEMA_VERSION}: se procesa igual (el tópico .v1 garantiza
 *       compatibilidad) y se avisa con WARN para actualizar el consumidor. Sin schema_version se asume 1.</li>
 *   <li>Un cambio incompatible viaja por otro tópico ({@code .v2}): este consumidor nunca lo ve.</li>
 *   <li>Otro {@code event_type} en el tópico: se ignora (ACK) en lugar de fallar.</li>
 *   <li>Falta un campo obligatorio (event_id, nomination_id, status) o el JSON es inválido: poison pill → DLT.</li>
 * </ul>
 */
@Component
@ConditionalOnProperty(name = "nominations.demo-consumer.enabled", havingValue = "true")
class NominationResultNotifier {

    static final String LISTENER_ID = "notifications-demo";
    static final int SUPPORTED_SCHEMA_VERSION = 1;
    private static final String RESULT_EVENT_TYPE = "nomination.result";
    /** Mismo formato que acepta la API: un valor arbitrario en el MDC terminaría en los logs (log injection). */
    private static final Pattern SAFE_CORRELATION_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private static final Logger log = LoggerFactory.getLogger(NominationResultNotifier.class);

    private final ObjectMapper objectMapper;
    private final ProcessedEventStore processedEvents;
    private final TransactionTemplate tx;
    private final String consumer;

    NominationResultNotifier(ObjectMapper objectMapper, ProcessedEventStore processedEvents, TransactionTemplate tx,
                             DemoConsumerProperties props) {
        this.objectMapper = objectMapper;
        this.processedEvents = processedEvents;
        this.tx = tx;
        this.consumer = props.groupId();
    }

    /** Lo que este consumidor necesita del evento; el resto del payload se ignora. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    record ResultNotification(UUID eventId, String eventType, Integer schemaVersion, UUID nominationId,
                              String status, String rejectionReason, String correlationId) {
    }

    @KafkaListener(id = LISTENER_ID, groupId = "${nominations.demo-consumer.group-id:notifications-demo}",
            topics = KafkaTopics.NOMINATION_RESULT,
            containerFactory = KafkaConsumerConfig.NOTIFICATIONS_CONTAINER_FACTORY)
    void onResult(ConsumerRecord<String, String> record) {
        ResultNotification event = parse(record.value());
        String correlationId = firstNonBlank(header(record, KafkaTopics.HEADER_CORRELATION_ID), event.correlationId());
        if (correlationId != null && SAFE_CORRELATION_ID.matcher(correlationId).matches()) {
            MDC.put(ApiHeaders.CORRELATION_ID_MDC_KEY, correlationId);
        }
        try {
            handle(record, event);
        } finally {
            MDC.remove(ApiHeaders.CORRELATION_ID_MDC_KEY);
        }
    }

    private void handle(ConsumerRecord<String, String> record, ResultNotification event) {
        String eventType = firstNonBlank(header(record, KafkaTopics.HEADER_EVENT_TYPE), event.eventType());
        if (eventType != null && !RESULT_EVENT_TYPE.equals(eventType)) {
            log.warn("Evento ignorado: event_type {} no soportado (partición {}, offset {})",
                    eventType, record.partition(), record.offset());
            return;
        }
        UUID eventId = eventId(record, event);
        if (event.nominationId() == null || event.status() == null) {
            throw new InvalidEventException("nomination.result sin nomination_id o status: event_id=" + eventId);
        }
        int version = schemaVersion(record, event);
        if (version > SUPPORTED_SCHEMA_VERSION) {
            log.warn("schema_version {} mayor a la soportada ({}): se procesa por compatibilidad aditiva, event_id={}",
                    version, SUPPORTED_SCHEMA_VERSION, eventId);
        }

        tx.executeWithoutResult(status -> {
            if (!processedEvents.markProcessed(consumer, eventId)) {
                log.debug("Evento duplicado, ya procesado: event_id={}", eventId);
                return;
            }
            // El "efecto": en un servicio real, enviar la notificación. Nunca datos de cuenta o tarjeta en el log.
            log.info("Notificación enviada: nomination_id={}, status={}, motivo={}, event_id={}",
                    event.nominationId(), event.status(),
                    event.rejectionReason() == null ? "-" : event.rejectionReason(), eventId);
        });
    }

    private ResultNotification parse(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new InvalidEventException("nomination.result con payload vacío");
        }
        try {
            ResultNotification event = objectMapper.readValue(payload, ResultNotification.class);
            if (event == null) {
                throw new InvalidEventException("nomination.result con payload nulo");
            }
            return event;
        } catch (JacksonException e) {
            // Sin el payload ni el mensaje de Jackson (que lo cita): termina en headers del DLT y en logs.
            throw new InvalidEventException("nomination.result no parseable: " + e.getClass().getSimpleName());
        }
    }

    /** El header manda (lo pone el relay del outbox); el payload es el respaldo. */
    private static UUID eventId(ConsumerRecord<String, String> record, ResultNotification event) {
        String fromHeader = header(record, KafkaTopics.HEADER_EVENT_ID);
        if (fromHeader == null) {
            if (event.eventId() == null) {
                throw new InvalidEventException("nomination.result sin event_id");
            }
            return event.eventId();
        }
        try {
            return UUID.fromString(fromHeader);
        } catch (IllegalArgumentException e) {
            throw new InvalidEventException("Header event_id no es un UUID");
        }
    }

    private static int schemaVersion(ConsumerRecord<String, String> record, ResultNotification event) {
        String fromHeader = header(record, KafkaTopics.HEADER_SCHEMA_VERSION);
        if (fromHeader != null) {
            try {
                return Integer.parseInt(fromHeader);
            } catch (NumberFormatException e) {
                log.debug("Header schema_version no numérico, se usa el del payload");
            }
        }
        return event.schemaVersion() == null ? 1 : event.schemaVersion();
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        String value = new String(header.value(), StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? null : value;
    }

    private static String firstNonBlank(String first, String second) {
        return first != null ? first : (second == null || second.isBlank() ? null : second);
    }
}
