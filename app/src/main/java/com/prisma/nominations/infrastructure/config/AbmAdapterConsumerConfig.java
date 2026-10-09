package com.prisma.nominations.infrastructure.config;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.infrastructure.adapter.in.messaging.InvalidEventException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

/**
 * Container factory propio del ABM Adapter (mismo patrón que {@link KafkaConsumerConfig}: bean con nombre, error
 * handler no expuesto, consumer y producer del DLT con String explícito).
 * <p>
 * Manejo de errores (D10):
 * <ul>
 *   <li>ABM no disponible ({@code AbmUnavailableException}: timeout, 5xx, conexión) u otro error transitorio:
 *       {@code maxAttempts} intentos con backoff fijo; agotados → {@code nomination.requested.v1-dlt}.
 *       <b>Fase 6:</b> se reemplaza por retry + circuit breaker de Resilience4j alrededor del cliente y, agotados,
 *       paso de la nominación a {@code ABM_TIMEOUT} (hoy queda en RECEIVED con el mensaje en el DLT).</li>
 *   <li>Rechazo de contrato ({@link AbmContractException}, 4xx): directo al DLT, reintentar no lo arregla.</li>
 *   <li>Poison pill ({@link InvalidEventException}) o nominación inexistente: directo al DLT.</li>
 * </ul>
 * Los reintentos bloquean la partición (orden por nominación): con los defaults, a lo sumo
 * {@code maxAttempts × (read-timeout + backoff)} por mensaje.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AbmAdapterProperties.class)
@ConditionalOnProperty(name = "nominations.abm.adapter.enabled", havingValue = "true")
public class AbmAdapterConsumerConfig implements DisposableBean {

    public static final String ABM_ADAPTER_CONTAINER_FACTORY = "abmAdapterContainerFactory";

    private DefaultKafkaProducerFactory<String, String> dltProducerFactory;

    @Bean(ABM_ADAPTER_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> abmAdapterContainerFactory(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails, AbmAdapterProperties props) {

        Map<String, Object> consumerProps = kafkaProperties.buildConsumerProperties(null);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getConsumer().getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        var consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(), new StringDeserializer());

        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getProducer().getBootstrapServers());
        dltProducerFactory = new DefaultKafkaProducerFactory<>(producerProps,
                new StringSerializer(), new StringSerializer());

        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(dltProducerFactory),
                (rec, ex) -> new TopicPartition(rec.topic() + KafkaTopicsConfig.DLT_SUFFIX, rec.partition()));
        var errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(props.backoff().toMillis(), Math.max(props.maxAttempts() - 1, 0)));
        errorHandler.addNotRetryableExceptions(InvalidEventException.class, AbmContractException.class,
                NominationNotFoundException.class);

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // Commit por registro, después de procesarlo (at-least-once). Un reenvío lo absorbe SubmitToAbmUseCase
        // (solo envía si sigue en RECEIVED) y ABM es idempotente por nomination_id: no hay doble alta.
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        return factory;
    }

    @Override
    public void destroy() {
        if (dltProducerFactory != null) {
            dltProducerFactory.destroy();
        }
    }
}
