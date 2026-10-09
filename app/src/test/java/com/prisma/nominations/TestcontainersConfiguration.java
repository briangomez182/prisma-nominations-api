package com.prisma.nominations;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Infraestructura real (mismas imágenes que docker-compose) para tests de integración.
 */
@TestConfiguration(proxyBeanMethods = false)
@Import(PostgresTestcontainersConfiguration.class)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
    }
}
