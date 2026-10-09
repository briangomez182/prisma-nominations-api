package com.prisma.nominations.abmmock;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    /** Template propio sobre el ProducerFactory de Boot (serializers String). */
    @Bean
    AbmResponsePublisher abmMockResponsePublisher(ProducerFactory<String, String> producerFactory) {
        return new KafkaAbmResponsePublisher(new KafkaTemplate<>(producerFactory));
    }

    @Bean(destroyMethod = "close")
    AbmMockEngine abmMockEngine(AbmMockProperties properties, AbmResponsePublisher abmMockResponsePublisher,
                                ObjectMapper objectMapper) {
        return new AbmMockEngine(properties, abmMockResponsePublisher, objectMapper, Clock.systemUTC());
    }
}
