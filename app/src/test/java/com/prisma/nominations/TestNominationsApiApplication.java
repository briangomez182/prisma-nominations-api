package com.prisma.nominations;

import org.springframework.boot.SpringApplication;

/**
 * Levanta la app con Postgres y Kafka en contenedores, sin docker-compose:
 * {@code mvn spring-boot:test-run}
 */
public class TestNominationsApiApplication {

    public static void main(String[] args) {
        SpringApplication.from(NominationsApiApplication::main)
                .with(TestcontainersConfiguration.class)
                .run(args);
    }
}
