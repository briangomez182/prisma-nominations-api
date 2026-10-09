package com.prisma.nominations.abmmock;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.time.Clock;

/**
 * Simulador de ABM para demo y tests. Solo existe con {@code nominations.abm-mock.enabled=true}; en
 * producción ABM es un sistema externo y nada de este paquete se carga.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "nominations.abm-mock", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(AbmMockProperties.class)
class AbmMockConfig {

    /**
     * Template propio sobre el ProducerFactory de Boot (serializers String), con observation: la respuesta lleva el
     * {@code traceparent} de la traza del pedido HTTP (como haría un ABM real instrumentado con W3C).
     */
    @Bean
    AbmResponsePublisher abmMockResponsePublisher(ProducerFactory<String, String> producerFactory,
                                                  ObjectProvider<ObservationRegistry> observations) {
        var template = new KafkaTemplate<>(producerFactory);
        template.setObservationEnabled(true);
        template.setObservationRegistry(observations.getIfUnique(() -> ObservationRegistry.NOOP));
        return new KafkaAbmResponsePublisher(template);
    }

    @Bean(destroyMethod = "close")
    AbmMockEngine abmMockEngine(AbmMockProperties properties, AbmResponsePublisher abmMockResponsePublisher,
                                ObjectMapper objectMapper) {
        return new AbmMockEngine(properties, abmMockResponsePublisher, objectMapper, Clock.systemUTC());
    }
}
