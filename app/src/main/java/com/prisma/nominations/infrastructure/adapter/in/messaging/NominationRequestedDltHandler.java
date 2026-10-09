package com.prisma.nominations.infrastructure.adapter.in.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.application.port.in.MarkAbmFailureUseCase;
import com.prisma.nominations.infrastructure.adapter.in.messaging.NominationRequestedListener.RequestedMessage;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.config.AbmAdapterConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Consumidor de {@code nomination.requested.v1-dlt} (lo registra {@link AbmAdapterConsumerConfig} como DLT handler
 * de los tópicos de retry). Cierra el ciclo de recuperación automática según la causa (header
 * {@code kafka_exception-cause-fqcn}):
 * <ul>
 *   <li>Contrato ({@link AbmContractException}): ABM_TIMEOUT con "ABM rechazó el pedido por contrato".</li>
 *   <li>Poison pill ({@link InvalidEventException}) o nominación inexistente: solo log ERROR, no hay estado que
 *       cambiar. El mensaje queda en el DLT para diagnóstico.</li>
 *   <li>Cualquier otra (ABM no disponible con los reintentos agotados, error inesperado): ABM_TIMEOUT con
 *       "Reintentos agotados: ABM no disponible".</li>
 * </ul>
 * ABM_TIMEOUT no publica resultado: queda para reproceso controlado. Si ABM ya respondió, no se toca nada.
 */
@Component(AbmAdapterConsumerConfig.DLT_HANDLER_BEAN)
@ConditionalOnProperty(name = "nominations.abm.adapter.enabled", havingValue = "true")
class NominationRequestedDltHandler {

    static final String EXHAUSTED_DETAIL = "Reintentos agotados: ABM no disponible";
    static final String CONTRACT_DETAIL = "ABM rechazó el pedido por contrato";

    private static final Logger log = LoggerFactory.getLogger(NominationRequestedDltHandler.class);

    private final ObjectMapper objectMapper;
    private final MarkAbmFailureUseCase markAbmFailure;

    NominationRequestedDltHandler(ObjectMapper objectMapper, MarkAbmFailureUseCase markAbmFailure) {
        this.objectMapper = objectMapper;
        this.markAbmFailure = markAbmFailure;
    }

    /** El nombre lo referencia {@link AbmAdapterConsumerConfig#DLT_HANDLER_METHOD}. */
    void onDeadLetter(ConsumerRecord<String, String> record) {
        RequestedMessage event = tryParse(record);
        NominationRequestedListener.putCorrelationId(record, event);
        try {
            handle(record, event);
        } finally {
            MDC.remove(ApiHeaders.CORRELATION_ID_MDC_KEY);
        }
    }

    private void handle(ConsumerRecord<String, String> record, RequestedMessage event) {
        String cause = cause(record);
        UUID nominationId = event == null ? null : tryNominationId(record, event);
        if (nominationId == null || InvalidEventException.class.getName().equals(cause)) {
            log.error("Poison pill en {}: sin nomination_id procesable (causa {}, partición {}, offset {})",
                    record.topic(), cause, record.partition(), record.offset());
            return;
        }
        if (NominationNotFoundException.class.getName().equals(cause)) {
            log.error("nomination.requested de una nominación inexistente en {}: nomination_id={}",
                    record.topic(), nominationId);
            return;
        }
        String detail = AbmContractException.class.getName().equals(cause) ? CONTRACT_DETAIL : EXHAUSTED_DETAIL;
        try {
            boolean changed = markAbmFailure.markFailed(nominationId, detail);
            log.info("DLT de nomination.requested procesado: nomination_id={} causa={} abm_timeout={}",
                    nominationId, cause, changed);
        } catch (NominationNotFoundException e) {
            log.error("DLT de nomination.requested sin nominación: nomination_id={}", nominationId);
        }
    }

    private RequestedMessage tryParse(ConsumerRecord<String, String> record) {
        try {
            return NominationRequestedListener.parse(objectMapper, record.value());
        } catch (InvalidEventException e) {
            return null;
        }
    }

    private static UUID tryNominationId(ConsumerRecord<String, String> record, RequestedMessage event) {
        try {
            return NominationRequestedListener.nominationId(record, event);
        } catch (InvalidEventException e) {
            return null;
        }
    }

    /**
     * Causa raíz que dejó Spring Kafka en el header (con tópicos de retry los headers de diagnóstico son
     * {@code kafka_exception-*}, sin el prefijo {@code dlt-}). Basta para distinguir contrato, poison pill e
     * inexistente, que se lanzan sin causa; cualquier otra cosa se trata como falla técnica.
     */
    private static String cause(ConsumerRecord<String, String> record) {
        String cause = NominationRequestedListener.header(record, KafkaHeaders.EXCEPTION_CAUSE_FQCN);
        return cause != null ? cause : NominationRequestedListener.header(record, KafkaHeaders.EXCEPTION_FQCN);
    }
}
