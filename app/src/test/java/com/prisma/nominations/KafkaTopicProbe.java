package com.prisma.nominations;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Lector de un tópico para tests: asigna todas las particiones desde el principio (group id aleatorio, sin
 * commits) y lee hasta el final actual. Determinista: "no hay un segundo mensaje" se verifica leyendo hasta el
 * high watermark, no esperando a que algo no llegue. Los tests filtran por key para no ver mensajes de otros.
 * No es thread-safe (como KafkaConsumer): usarlo siempre desde el hilo del test.
 */
final class KafkaTopicProbe implements AutoCloseable {

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(15);

    private final KafkaConsumer<String, String> consumer;
    private final List<TopicPartition> partitions;
    private final List<ConsumerRecord<String, String>> records = new ArrayList<>();

    KafkaTopicProbe(String bootstrapServers, String topic) {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "it-probe-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
        partitions = consumer.partitionsFor(topic, READ_TIMEOUT).stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }

    /** Lee hasta el final actual del tópico y devuelve los mensajes con esa key, en orden de offset. */
    List<ConsumerRecord<String, String>> recordsWithKey(Object key) {
        readToEnd();
        String expected = key.toString();
        return records.stream()
                .filter(r -> expected.equals(r.key()))
                .sorted(Comparator.comparingLong(ConsumerRecord::offset))
                .toList();
    }

    private void readToEnd() {
        Map<TopicPartition, Long> end = consumer.endOffsets(partitions, READ_TIMEOUT);
        long deadline = System.nanoTime() + READ_TIMEOUT.toNanos();
        while (!caughtUp(end)) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("No se pudo leer el tópico hasta el final: " + end);
            }
            consumer.poll(Duration.ofMillis(200)).forEach(records::add);
        }
    }

    private boolean caughtUp(Map<TopicPartition, Long> end) {
        return end.entrySet().stream().allMatch(e -> consumer.position(e.getKey(), READ_TIMEOUT) >= e.getValue());
    }

    static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        consumer.close();
    }
}
