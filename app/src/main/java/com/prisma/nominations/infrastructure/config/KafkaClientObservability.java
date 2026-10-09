package com.prisma.nominations.infrastructure.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;

/**
 * Observabilidad de los clientes Kafka creados a mano (las factories de Boot ya la traen):
 * <ul>
 *   <li><b>Observation</b> en containers y templates: span CONSUMER/PRODUCER y propagación automática del header
 *       W3C {@code traceparent} (el consumer continúa la traza del productor; el reenvío a retry/DLT también).</li>
 *   <li><b>Métricas del cliente</b> ({@code kafka_consumer_*}, {@code kafka_producer_*}, p.ej.
 *       {@code kafka_consumer_fetch_manager_records_lag_max}) en {@code /actuator/prometheus}.</li>
 * </ul>
 * El registry se pasa explícito: los templates y factories creados con {@code new} no lo resuelven del contexto.
 * Sin registries (tests sin actuator) todo queda no-op.
 */
final class KafkaClientObservability {

    private KafkaClientObservability() {
    }

    static ObservationRegistry observations(ObjectProvider<ObservationRegistry> provider) {
        return provider.getIfUnique(() -> ObservationRegistry.NOOP);
    }

    static <K, V> void observe(ConcurrentKafkaListenerContainerFactory<K, V> factory, ObservationRegistry registry) {
        factory.getContainerProperties().setObservationEnabled(true);
        factory.getContainerProperties().setObservationRegistry(registry);
    }

    static <K, V> KafkaTemplate<K, V> observedTemplate(DefaultKafkaProducerFactory<K, V> producerFactory,
                                                       ObservationRegistry registry) {
        var template = new KafkaTemplate<>(producerFactory);
        template.setObservationEnabled(true);
        template.setObservationRegistry(registry);
        return template;
    }

    static <K, V> void meter(DefaultKafkaConsumerFactory<K, V> factory, ObjectProvider<MeterRegistry> meters) {
        meters.ifAvailable(registry -> factory.addListener(new MicrometerConsumerListener<>(registry)));
    }

    static <K, V> void meter(DefaultKafkaProducerFactory<K, V> factory, ObjectProvider<MeterRegistry> meters) {
        meters.ifAvailable(registry -> factory.addListener(new MicrometerProducerListener<>(registry)));
    }
}
