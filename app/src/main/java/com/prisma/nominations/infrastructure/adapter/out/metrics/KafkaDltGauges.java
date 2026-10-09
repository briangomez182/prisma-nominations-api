package com.prisma.nominations.infrastructure.adapter.out.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * {@code kafka.dlt.messages{topic}}: mensajes acumulados en cada Dead Letter Topic ({@code *-dlt}), como suma de
 * los end offsets de sus particiones. Todo mensaje en un DLT es un evento que no se pudo procesar: la alerta es
 * sobre su aumento ({@code increase()}), no sobre el valor absoluto (el offset no baja al reprocesar).
 * <p>
 * Se refresca en segundo plano (no en el scrape) con un {@link Admin} propio con timeouts cortos. Si Kafka no
 * responde se conservan los últimos valores y se avisa una vez por racha de fallas. Los DLT se descubren por el
 * sufijo, así un tópico nuevo con DLT queda cubierto sin tocar este código.
 */
@Component
@ConditionalOnProperty(name = "nominations.metrics.kafka-dlt.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaDltGauges implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(KafkaDltGauges.class);

    static final String DLT_SUFFIX = "-dlt";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final KafkaAdmin kafkaAdmin;
    private final MeterRegistry registry;
    private final Map<String, AtomicLong> messagesByTopic = new ConcurrentHashMap<>();
    private Admin admin;
    private boolean failing;

    public KafkaDltGauges(KafkaAdmin kafkaAdmin, MeterRegistry registry) {
        this.kafkaAdmin = kafkaAdmin;
        this.registry = registry;
    }

    @Scheduled(initialDelayString = "${nominations.metrics.kafka-dlt.initial-delay:5s}",
            fixedDelayString = "${nominations.metrics.kafka-dlt.refresh-interval:30s}")
    public synchronized void refresh() {
        try {
            var client = admin();
            var dlts = client.listTopics().names().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).stream()
                    .filter(name -> name.endsWith(DLT_SUFFIX))
                    .toList();
            if (dlts.isEmpty()) {
                return;
            }
            var request = new HashMap<TopicPartition, OffsetSpec>();
            client.describeTopics(dlts).allTopicNames().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .forEach((topic, description) -> description.partitions()
                            .forEach(p -> request.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest())));
            var totals = client.listOffsets(request).all().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .entrySet().stream()
                    .collect(Collectors.groupingBy(e -> e.getKey().topic(),
                            Collectors.summingLong(e -> e.getValue().offset())));
            totals.forEach((topic, total) -> gaugeFor(topic).set(total));
            if (failing) {
                log.info("Métrica de DLT: Kafka volvió a responder");
                failing = false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (!failing) {
                log.warn("Métrica de DLT: no se pudo consultar Kafka, se mantienen los últimos valores: {}",
                        e.toString());
                failing = true;
            } else {
                log.debug("Métrica de DLT: Kafka sigue sin responder: {}", e.toString());
            }
        }
    }

    /** Valor actual por tópico (tests, diagnóstico). */
    Map<String, Long> snapshot() {
        return messagesByTopic.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
    }

    private AtomicLong gaugeFor(String topic) {
        return messagesByTopic.computeIfAbsent(topic, t -> {
            var value = new AtomicLong();
            Gauge.builder("kafka.dlt.messages", value, AtomicLong::get)
                    .description("Mensajes acumulados en el Dead Letter Topic (suma de end offsets)")
                    .tag("topic", t)
                    .strongReference(true)
                    .register(registry);
            return value;
        });
    }

    /** Admin propio (no el de KafkaAdmin, que se crea y cierra en cada operación) con timeouts acotados. */
    private Admin admin() {
        if (admin == null) {
            var config = new HashMap<>(kafkaAdmin.getConfigurationProperties());
            config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis());
            config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) TIMEOUT.toMillis());
            config.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "nominations-dlt-metrics");
            admin = Admin.create(config);
        }
        return admin;
    }

    @Override
    public synchronized void destroy() {
        if (admin != null) {
            admin.close(Duration.ofSeconds(2));
            admin = null;
        }
    }
}
