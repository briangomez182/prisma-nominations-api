package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.prisma.nominations.application.port.in.SubmitToAbmUseCase;
import com.prisma.nominations.application.port.in.SubmitToAbmUseCase.SubmitOutcome;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.AbmAdapterConsumerConfig;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * ABM Adapter (D8): consume {@code nomination.requested.v1} y envía cada nominación a ABM vía
 * {@link SubmitToAbmUseCase}. Corre fuera del request HTTP: la API responde 202 sin esperar a ABM (E1).
 * <p>
 * <b>Sin doble envío:</b> el evento puede llegar más de una vez (at-least-once). El caso de uso solo envía si la
 * nominación sigue en RECEIVED, y ABM es idempotente por nomination_id. No hace falta dedup por event_id.
 * <p>
 * Solo lee del payload el nomination_id (el caso de uso carga la nominación de la base, que es la fuente de verdad);
 * nunca loguea el payload, que lleva account_id completo.
 * <p>
 * <b>Versionado (tolerant reader):</b> campos desconocidos se ignoran; {@code schema_version} mayor se procesa con
 * WARN; otro {@code event_type} se ignora (ACK); JSON inválido o sin nomination_id → poison pill → DLT.
 * Los errores de ABM los clasifica el error handler de {@link AbmAdapterConsumerConfig}.
 */
@Component
@ConditionalOnProperty(name = "nominations.abm.adapter.enabled", havingValue = "true")
class NominationRequestedListener {

    static final String LISTENER_ID = "abm-adapter";
    static final int SUPPORTED_SCHEMA_VERSION = 1;
    private static final String REQUESTED_EVENT_TYPE = "nomination.requested";
    /** Mismo formato que acepta la API: un valor arbitrario en el MDC terminaría en los logs (log injection). */
    private static final Pattern SAFE_CORRELATION_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private static final Logger log = LoggerFactory.getLogger(NominationRequestedListener.class);

    private final ObjectMapper objectMapper;
    private final SubmitToAbmUseCase submitToAbm;

    NominationRequestedListener(ObjectMapper objectMapper, SubmitToAbmUseCase submitToAbm) {
        this.objectMapper = objectMapper;
        this.submitToAbm = submitToAbm;
    }

    /** Lo que el adapter necesita del evento; el resto (incluido account_id) ni se lee. */
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    record RequestedMessage(UUID eventId, String eventType, Integer schemaVersion, UUID nominationId,
                            String correlationId) {
    }

    @KafkaListener(id = LISTENER_ID, groupId = "${nominations.abm.adapter.group-id:abm-adapter}",
            topics = KafkaTopics.NOMINATION_REQUESTED,
            containerFactory = AbmAdapterConsumerConfig.ABM_ADAPTER_CONTAINER_FACTORY)
    void onRequested(ConsumerRecord<String, String> record) {
        RequestedMessage event = parse(record.value());
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

    private void handle(ConsumerRecord<String, String> record, RequestedMessage event) {
        String eventType = firstNonBlank(header(record, KafkaTopics.HEADER_EVENT_TYPE), event.eventType());
        if (eventType != null && !REQUESTED_EVENT_TYPE.equals(eventType)) {
            log.warn("Evento ignorado: event_type {} no soportado (partición {}, offset {})",
                    eventType, record.partition(), record.offset());
            return;
        }
        UUID nominationId = nominationId(record, event);
        int version = schemaVersion(record, event);
        if (version > SUPPORTED_SCHEMA_VERSION) {
            log.warn("schema_version {} mayor a la soportada ({}): se procesa por compatibilidad aditiva, "
                    + "nomination_id={}", version, SUPPORTED_SCHEMA_VERSION, nominationId);
        }

        SubmitOutcome outcome = submitToAbm.submit(nominationId);
        switch (outcome) {
            case SUBMITTED -> log.info("Nominación enviada a ABM: nomination_id={}", nominationId);
            case SKIPPED -> log.info("Envío a ABM omitido (ya no está en RECEIVED, posible reentrega): "
                    + "nomination_id={}", nominationId);
        }
    }

    private RequestedMessage parse(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new InvalidEventException("nomination.requested con payload vacío");
        }
        try {
            RequestedMessage event = objectMapper.readValue(payload, RequestedMessage.class);
            if (event == null) {
                throw new InvalidEventException("nomination.requested con payload nulo");
            }
            return event;
        } catch (JacksonException e) {
            // Sin el payload ni el mensaje de Jackson (que lo cita, y el payload lleva account_id completo).
            throw new InvalidEventException("nomination.requested no parseable: " + e.getClass().getSimpleName());
        }
    }

    /** El payload manda; la key (que el relay pone = nomination_id) es el respaldo. */
    private static UUID nominationId(ConsumerRecord<String, String> record, RequestedMessage event) {
        if (event.nominationId() != null) {
            return event.nominationId();
        }
        if (record.key() != null) {
            try {
                return UUID.fromString(record.key());
            } catch (IllegalArgumentException e) {
                log.debug("Key no es un UUID, no sirve como nomination_id");
            }
        }
        throw new InvalidEventException("nomination.requested sin nomination_id (partición %d, offset %d)"
                .formatted(record.partition(), record.offset()));
    }

    private static int schemaVersion(ConsumerRecord<String, String> record, RequestedMessage event) {
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
