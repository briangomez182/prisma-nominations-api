package com.prisma.nominations.infrastructure.config;

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
 * Container factory propio del consumidor de notificaciones. Es un bean con nombre propio y el error handler
 * no se expone como bean: así no reemplaza la factory por defecto de Boot ni se aplica a otros listeners.
 * <p>
 * Manejo de errores:
 * <ul>
 *   <li>Error transitorio (p.ej. base caída): {@code maxAttempts} intentos con backoff fijo; agotados, el mensaje
 *       va a {@code <topic>-dlt} y se avanza el offset (no bloquea la partición).</li>
 *   <li>Poison pill ({@link InvalidEventException}: payload no parseable o sin campos obligatorios): directo al
 *       DLT, sin reintentos, porque reintentar no lo arregla.</li>
 * </ul>
 * Consumer y producer del DLT usan String explícito (no dependen de los serializers globales): el DLT recibe el
 * payload original byte a byte, listo para reprocesar.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(DemoConsumerProperties.class)
@ConditionalOnProperty(name = "nominations.demo-consumer.enabled", havingValue = "true")
public class KafkaConsumerConfig implements DisposableBean {

    public static final String NOTIFICATIONS_CONTAINER_FACTORY = "notificationsContainerFactory";

    private DefaultKafkaProducerFactory<String, String> dltProducerFactory;

    @Bean(NOTIFICATIONS_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> notificationsContainerFactory(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails, DemoConsumerProperties props) {

        Map<String, Object> consumerProps = kafkaProperties.buildConsumerProperties(null);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getConsumer().getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        var consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(), new StringDeserializer());

        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getProducer().getBootstrapServers());
        dltProducerFactory = new DefaultKafkaProducerFactory<>(producerProps,
                new StringSerializer(), new StringSerializer());

        // Mismo nombre que el original + "-dlt" y misma partición (el DLT tiene las mismas particiones).
        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(dltProducerFactory),
                (rec, ex) -> new TopicPartition(rec.topic() + KafkaTopicsConfig.DLT_SUFFIX, rec.partition()));
        var errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(props.backoff().toMillis(), Math.max(props.maxAttempts() - 1, 0)));
        errorHandler.addNotRetryableExceptions(InvalidEventException.class);

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // Commit por registro, después de procesarlo (at-least-once): una caída reentrega a lo sumo el último.
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
