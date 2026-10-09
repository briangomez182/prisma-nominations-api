package com.prisma.nominations.abmmock;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publica las respuestas de ABM en Kafka. Los nombres de tópico y header son el CONTRATO con la API de
 * nominaciones; se duplican a propósito (el simulador es otro sistema y no importa sus clases).
 */
class KafkaAbmResponsePublisher implements AbmResponsePublisher {

    /** Contrato: tópico de respuestas de ABM (= KafkaTopics.ABM_RESPONSES). */
    static final String TOPIC = "abm.responses.v1";
    /** Contrato: header de correlación (= KafkaTopics.HEADER_CORRELATION_ID). */
    static final String HEADER_CORRELATION_ID = "correlation_id";

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafka;

    KafkaAbmResponsePublisher(KafkaTemplate<String, String> kafka) {
        this.kafka = kafka;
    }

    @Override
    public void publish(String key, String correlationId, String json) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, json);
        if (correlationId != null) {
            record.headers().add(HEADER_CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));
        }
        try {
            kafka.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrumpido publicando respuesta de ABM", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("No se pudo publicar la respuesta de ABM", e);
        }
    }
}
