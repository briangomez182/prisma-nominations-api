package com.prisma.nominations.infrastructure.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Tópicos del servicio (D7). Los crea {@code KafkaAdmin} al arrancar si no existen; si ya existen no los modifica
 * (salvo que se agreguen particiones). En producción los tópicos se gestionan como infraestructura (IaC) y este
 * bean sirve de documentación ejecutable para el entorno local y los tests.
 * <p>
 * Cada tópico tiene su Dead Letter Topic {@code <topic>-dlt}, la convención de Spring Kafka
 * ({@code DeadLetterPublishingRecoverer} publica en {@code <topic>-dlt} y en la <b>misma partición</b> que el
 * original), por eso el DLT se crea con la misma cantidad de particiones. Todos los tópicos tienen consumidores
 * con reintentos acotados (ABM Adapter, consumer de respuestas de ABM, notificaciones), así que todos tienen DLT.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkaTopicsConfig.TopicsProperties.class)
class KafkaTopicsConfig {

    /** Sufijo de Dead Letter Topic de Spring Kafka. */
    static final String DLT_SUFFIX = "-dlt";

    /**
     * @param partitions        unidad de paralelismo de los consumers (E10)
     * @param replicationFactor local 1; en producción >= 3 con min.insync.replicas=2
     */
    @ConfigurationProperties("nominations.kafka")
    record TopicsProperties(@DefaultValue("6") int partitions, @DefaultValue("1") short replicationFactor) {
    }

    @Bean
    KafkaAdmin.NewTopics nominationTopics(TopicsProperties props) {
        return new KafkaAdmin.NewTopics(
                topic(KafkaTopics.NOMINATION_REQUESTED, props),
                topic(KafkaTopics.NOMINATION_REQUESTED + DLT_SUFFIX, props),
                topic(KafkaTopics.ABM_RESPONSES, props),
                topic(KafkaTopics.ABM_RESPONSES + DLT_SUFFIX, props),
                topic(KafkaTopics.NOMINATION_RESULT, props),
                topic(KafkaTopics.NOMINATION_RESULT + DLT_SUFFIX, props));
    }

    private static NewTopic topic(String name, TopicsProperties props) {
        return TopicBuilder.name(name)
                .partitions(props.partitions())
                .replicas(props.replicationFactor())
                .build();
    }
}
