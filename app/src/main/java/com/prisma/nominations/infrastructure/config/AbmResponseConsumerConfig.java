package com.prisma.nominations.infrastructure.config;

import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.domain.exception.InvalidStatusTransitionException;
import com.prisma.nominations.infrastructure.adapter.in.messaging.InvalidEventException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.Map;

/**
 * Container factory propio del consumer de {@code abm.responses.v1} (mismo patrón que {@link KafkaConsumerConfig}:
 * bean con nombre propio, error handler no expuesto como bean).
 * <p>
 * Manejo de errores:
 * <ul>
 *   <li>Error transitorio (base caída, timeout): pocos reintentos con backoff fijo; agotados, el mensaje va a
 *       {@code abm.responses.v1-dlt} y se avanza el offset. La nominación queda sin resolver: la detecta el sweeper
 *       de SLA (ABM_TIMEOUT) y el DLT permite reprocesar la respuesta.</li>
 *   <li>Directo al DLT, sin reintentos (reintentar no lo arregla): poison pill ({@link InvalidEventException}),
 *       respuesta de una nominación inexistente o con request_id ajeno ({@link NominationNotFoundException}) y
 *       transición inválida.</li>
 * </ul>
 * Commit por registro después de procesarlo (at-least-once): una reentrega es inocua porque el caso de uso es
 * idempotente (D9).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "nominations.abm.response-consumer.enabled", havingValue = "true")
public class AbmResponseConsumerConfig implements DisposableBean {

    public static final String ABM_RESPONSES_CONTAINER_FACTORY = "abmResponsesContainerFactory";

    private DefaultKafkaProducerFactory<String, String> dltProducerFactory;

    @Bean(ABM_RESPONSES_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> abmResponsesContainerFactory(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails,
            @Value("${nominations.abm.response-consumer.group-id:abm-response-processor}") String groupId,
            @Value("${nominations.abm.response-consumer.max-attempts:3}") int maxAttempts,
            @Value("${nominations.abm.response-consumer.backoff:1s}") Duration backoff,
            ObjectProvider<ObservationRegistry> observations, ObjectProvider<MeterRegistry> meters) {
        ObservationRegistry observationRegistry = KafkaClientObservability.observations(observations);

        Map<String, Object> consumerProps = kafkaProperties.buildConsumerProperties(null);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getConsumer().getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        var consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(), new StringDeserializer());
        KafkaClientObservability.meter(consumerFactory, meters);

        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getProducer().getBootstrapServers());
        dltProducerFactory = new DefaultKafkaProducerFactory<>(producerProps,
                new StringSerializer(), new StringSerializer());
        KafkaClientObservability.meter(dltProducerFactory, meters);

        // abm.responses.v1-dlt, misma partición que el original.
        var recoverer = new DeadLetterPublishingRecoverer(
                KafkaClientObservability.observedTemplate(dltProducerFactory, observationRegistry),
                (rec, ex) -> new TopicPartition(rec.topic() + KafkaTopicsConfig.DLT_SUFFIX, rec.partition()));
        var errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(backoff.toMillis(), Math.max(maxAttempts - 1, 0)));
        errorHandler.addNotRetryableExceptions(InvalidEventException.class, NominationNotFoundException.class,
                InvalidStatusTransitionException.class);

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        KafkaClientObservability.observe(factory, observationRegistry);
        return factory;
    }

    @Override
    public void destroy() {
        if (dltProducerFactory != null) {
            dltProducerFactory.destroy();
        }
    }
}
