package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.port.in.AbmResponseCommand;
import com.prisma.nominations.application.port.in.ProcessAbmResponseUseCase;
import com.prisma.nominations.domain.model.AbmDecision;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.AbmResponseConsumerConfig;
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
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Consumer de {@code abm.responses.v1}: traduce la respuesta de ABM a {@link AbmResponseCommand} (con el motivo de
 * rechazo ya normalizado) y la aplica. La idempotencia (E7) es del caso de uso: una respuesta repetida da
 * DUPLICATE y solo se hace ACK.
 * <p>
 * Errores (ver {@link AbmResponseConsumerConfig}): JSON inválido o sin campos obligatorios ({@link
 * InvalidEventException}) y respuesta de una nominación inexistente o ajena van directo al DLT; un error transitorio
 * (base caída) se reintenta unas pocas veces antes del DLT.
 */
@Component
@ConditionalOnProperty(name = "nominations.abm.response-consumer.enabled", havingValue = "true")
class AbmResponseListener {

    static final String LISTENER_ID = "abm-response-processor";
    private static final String APPROVED = "APPROVED";
    private static final String REJECTED = "REJECTED";
    /** Mismo formato que acepta la API: un valor arbitrario en el MDC terminaría en los logs (log injection). */
    private static final Pattern SAFE_CORRELATION_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private static final Logger log = LoggerFactory.getLogger(AbmResponseListener.class);

    private final ObjectMapper objectMapper;
    private final ProcessAbmResponseUseCase processAbmResponse;

    AbmResponseListener(ObjectMapper objectMapper, ProcessAbmResponseUseCase processAbmResponse) {
        this.objectMapper = objectMapper;
        this.processAbmResponse = processAbmResponse;
    }

    @KafkaListener(id = LISTENER_ID,
            groupId = "${nominations.abm.response-consumer.group-id:abm-response-processor}",
            topics = KafkaTopics.ABM_RESPONSES,
            containerFactory = AbmResponseConsumerConfig.ABM_RESPONSES_CONTAINER_FACTORY)
    void onResponse(ConsumerRecord<String, String> record) {
        AbmResponseMessage message = parse(record.value());
        String correlationId = header(record, KafkaTopics.HEADER_CORRELATION_ID);
        if (correlationId == null) {
            correlationId = message.correlationId();
        }
        if (correlationId != null && SAFE_CORRELATION_ID.matcher(correlationId).matches()) {
            MDC.put(ApiHeaders.CORRELATION_ID_MDC_KEY, correlationId);
        }
        try {
            var command = toCommand(message, correlationId);
            var outcome = processAbmResponse.process(command);
            log.info("Respuesta de ABM procesada: nomination_id={} result={} outcome={}",
                    message.nominationId(), message.result(), outcome);
        } finally {
            MDC.remove(ApiHeaders.CORRELATION_ID_MDC_KEY);
        }
    }

    private static AbmResponseCommand toCommand(AbmResponseMessage message, String correlationId) {
        if (message.nominationId() == null || message.requestId() == null || message.result() == null) {
            throw new InvalidEventException("abm.response sin nomination_id, request_id o result");
        }
        AbmDecision decision = switch (message.result().trim().toUpperCase(Locale.ROOT)) {
            case APPROVED -> AbmDecision.approved();
            case REJECTED -> AbmDecision.rejected(AbmReasonCodeMapper.toRejectionReason(message.reasonCode()),
                    message.reasonCode());
            default -> throw new InvalidEventException("abm.response con result no soportado: nomination_id="
                    + message.nominationId());
        };
        return new AbmResponseCommand(message.nominationId(), message.requestId(), correlationId,
                message.abmOperationId(), decision);
    }

    private AbmResponseMessage parse(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new InvalidEventException("abm.response con payload vacío");
        }
        try {
            AbmResponseMessage message = objectMapper.readValue(payload, AbmResponseMessage.class);
            if (message == null) {
                throw new InvalidEventException("abm.response con payload nulo");
            }
            return message;
        } catch (JacksonException e) {
            // Sin el payload ni el mensaje de Jackson (que lo cita): termina en headers del DLT y en logs.
            throw new InvalidEventException("abm.response no parseable: " + e.getClass().getSimpleName());
        }
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        String value = new String(header.value(), StandardCharsets.UTF_8).trim();
        return value.isEmpty() ? null : value;
    }
}
