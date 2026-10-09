package com.prisma.nominations.infrastructure.config;

import com.prisma.nominations.application.exception.AbmContractException;
import com.prisma.nominations.application.exception.NominationNotFoundException;
import com.prisma.nominations.infrastructure.adapter.in.messaging.InvalidEventException;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.backoff.Sleeper;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.ThreadWaitSleeper;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Consumo de {@code nomination.requested.v1} por el ABM Adapter: container factory propio y reintentos
 * <b>no bloqueantes</b> con tópicos de retry de Spring Kafka (D10, E6, E10).
 * <p>
 * Flujo ante error:
 * <pre>
 * nomination.requested.v1 ──falla técnica──▶ -retry-0 (10s) ──▶ -retry-1 (1m) ──▶ -retry-2 (5m) ──▶ -dlt
 *        └──── contrato / poison pill / nominación inexistente ─────────────────────────────────────▲
 * </pre>
 * <ul>
 *   <li>ABM no disponible ({@code AbmUnavailableException}: timeout, 5xx, conexión, circuit breaker abierto) u
 *       otro error inesperado: el mensaje se reenvía al tópico de retry siguiente, que lo consume recién cuando
 *       vence su espera ({@code nominations.abm.adapter.retry-delays}). La partición principal sigue avanzando:
 *       un ABM caído no frena las demás nominaciones. Agotados → DLT.</li>
 *   <li>Rechazo de contrato ({@link AbmContractException}), poison pill ({@link InvalidEventException}) o
 *       nominación inexistente ({@link NominationNotFoundException}): reintentar no lo arregla → DLT directo.</li>
 *   <li>DLT: lo consume el handler {@value #DLT_HANDLER_BEAN}, que pasa la nominación a ABM_TIMEOUT (reintentos
 *       agotados o contrato) o solo alerta (poison pill, inexistente). Si el handler falla se loguea y el mensaje
 *       queda en el DLT ({@link DltStrategy#FAIL_ON_ERROR}: sin bucle de republicación).</li>
 * </ul>
 * <b>Tópicos:</b> {@code nomination.requested.v1-retry-N} (sufijo por índice: el nombre no cambia si se ajustan las
 * esperas) los crea Spring Kafka al arrancar con las particiones y la replicación de {@code nominations.kafka.*};
 * el principal y el DLT los declara {@link KafkaTopicsConfig} (si un nombre está en ambos, gana esa declaración).
 * El reenvío conserva key (nomination_id), valor y headers (event_id, correlation_id) y va a la misma partición:
 * el orden por nominación se mantiene dentro de cada tópico.
 * <p>
 * Consumer groups: {@code <groupId>} (principal), {@code <groupId>-retry-N} y {@code <groupId>-dlt}.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AbmAdapterProperties.class)
@ConditionalOnProperty(name = "nominations.abm.adapter.enabled", havingValue = "true")
public class AbmAdapterConsumerConfig implements DisposableBean {

    public static final String ABM_ADAPTER_CONTAINER_FACTORY = "abmAdapterContainerFactory";
    /** Bean y método que consumen {@code nomination.requested.v1-dlt}. */
    public static final String DLT_HANDLER_BEAN = "abmAdapterDltHandler";
    public static final String DLT_HANDLER_METHOD = "onDeadLetter";
    static final String RETRY_TOPIC_SUFFIX = "-retry";

    private DefaultKafkaProducerFactory<String, String> retryProducerFactory;

    @Bean(ABM_ADAPTER_CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> abmAdapterContainerFactory(
            KafkaProperties kafkaProperties, KafkaConnectionDetails connectionDetails, AbmAdapterProperties props,
            ObjectProvider<ObservationRegistry> observations, ObjectProvider<MeterRegistry> meters) {

        Map<String, Object> consumerProps = kafkaProperties.buildConsumerProperties(null);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getConsumer().getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        var consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps,
                new StringDeserializer(), new StringDeserializer());
        KafkaClientObservability.meter(consumerFactory, meters);

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        // Sin error handler propio: lo pone Spring Kafka en cada container (principal, retry y DLT) según
        // abmAdapterRetryTopics. Commit por registro, después de procesarlo o reenviarlo (at-least-once). Una
        // reentrega la absorbe SubmitToAbmUseCase (solo envía si sigue en RECEIVED) y ABM es idempotente.
        factory.getContainerProperties().setAckMode(AckMode.RECORD);
        // Aplica también a los containers de retry y DLT (usan esta misma factory).
        KafkaClientObservability.observe(factory, KafkaClientObservability.observations(observations));
        return factory;
    }

    @Bean
    RetryTopicConfiguration abmAdapterRetryTopics(AbmAdapterProperties props,
                                                  KafkaTopicsConfig.TopicsProperties topics,
                                                  KafkaProperties kafkaProperties,
                                                  KafkaConnectionDetails connectionDetails,
                                                  ObjectProvider<ObservationRegistry> observations,
                                                  ObjectProvider<MeterRegistry> meters) {
        // Producer propio con String explícito: el retry y el DLT reciben el payload original byte a byte.
        Map<String, Object> producerProps = kafkaProperties.buildProducerProperties(null);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionDetails.getProducer().getBootstrapServers());
        retryProducerFactory = new DefaultKafkaProducerFactory<>(producerProps,
                new StringSerializer(), new StringSerializer());
        KafkaClientObservability.meter(retryProducerFactory, meters);

        return RetryTopicConfigurationBuilder.newInstance()
                .includeTopic(KafkaTopics.NOMINATION_REQUESTED)
                .listenerFactory(ABM_ADAPTER_CONTAINER_FACTORY)
                .maxAttempts(props.retryDelays().size() + 1)
                .customBackoff(new ListBackOffPolicy(props.retryDelays()))
                .retryTopicSuffix(RETRY_TOPIC_SUFFIX)
                .suffixTopicsWithIndexValues()
                .dltSuffix(KafkaTopicsConfig.DLT_SUFFIX)
                // Lista de exclusión: todo lo demás (ABM no disponible, base caída, …) se reintenta.
                .notRetryOn(List.of(InvalidEventException.class, AbmContractException.class,
                        NominationNotFoundException.class))
                .dltHandlerMethod(DLT_HANDLER_BEAN, DLT_HANDLER_METHOD)
                .dltProcessingFailureStrategy(DltStrategy.FAIL_ON_ERROR)
                .autoCreateTopics(true, topics.partitions(), topics.replicationFactor())
                // Con observation: el reenvío lleva un traceparent nuevo de la misma traza.
                .create(KafkaClientObservability.observedTemplate(retryProducerFactory,
                        KafkaClientObservability.observations(observations)));
    }

    @Override
    public void destroy() {
        if (retryProducerFactory != null) {
            retryProducerFactory.destroy();
        }
    }

    /**
     * Esperas explícitas, una por tópico de retry (Spring Retry solo trae fija, exponencial o aleatoria). Spring
     * Kafka la usa una sola vez al arrancar para calcular los tópicos: "duerme" con un {@link Sleeper} que solo
     * registra los valores.
     */
    static final class ListBackOffPolicy implements SleepingBackOffPolicy<ListBackOffPolicy> {

        private final List<Duration> delays;
        private final Sleeper sleeper;

        ListBackOffPolicy(List<Duration> delays) {
            this(delays, new ThreadWaitSleeper());
        }

        private ListBackOffPolicy(List<Duration> delays, Sleeper sleeper) {
            this.delays = List.copyOf(delays);
            this.sleeper = sleeper;
        }

        @Override
        public ListBackOffPolicy withSleeper(Sleeper sleeper) {
            return new ListBackOffPolicy(delays, sleeper);
        }

        @Override
        public BackOffContext start(RetryContext context) {
            return new Attempt();
        }

        @Override
        public void backOff(BackOffContext context) {
            var attempt = (Attempt) context;
            var delay = delays.get(Math.min(attempt.index++, delays.size() - 1));
            try {
                sleeper.sleep(delay.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BackOffInterruptedException("Backoff interrumpido", e);
            }
        }

        private static final class Attempt implements BackOffContext {
            private int index;
        }
    }
}
