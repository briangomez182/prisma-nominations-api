package com.prisma.nominations.infrastructure.config;

import com.prisma.nominations.infrastructure.adapter.out.messaging.OutboxProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Tareas programadas (relay y purga del outbox). Cada una se apaga con su propia property.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(OutboxProperties.class)
class SchedulingConfig {
}
