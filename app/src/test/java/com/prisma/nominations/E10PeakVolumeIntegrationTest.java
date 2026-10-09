package com.prisma.nominations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prisma.nominations.infrastructure.adapter.in.web.ApiHeaders;
import com.prisma.nominations.infrastructure.adapter.out.messaging.OutboxRelay;
import com.prisma.nominations.infrastructure.config.KafkaTopics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.prisma.nominations.KafkaTopicProbe.header;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * E10 - Pico de volumen: una ráfaga concurrente de altas pasa por el flujo completo (API → outbox → relay → Kafka →
 * ABM Adapter → simulador de ABM → abm.responses.v1 → estado final → nomination.result.v1 → consumidor de ejemplo)
 * sin perder garantías funcionales ni trazabilidad.
 * <p>
 * Qué se demuestra:
 * <ul>
 *   <li><b>API bajo carga</b>: cientos de POST simultáneos de varias entidades, con reintentos concurrentes del mismo
 *       request_id; todos 202, sin 5xx, y cada request_id resuelve a una sola nominación (idempotencia por la UNIQUE
 *       de la base, también en la carrera).</li>
 *   <li><b>Escalamiento horizontal del relay</b>: el ciclo programado se apaga y {@value #RELAY_REPLICAS} "réplicas"
 *       invocan {@link OutboxRelay#relayOnce()} en paralelo, como harían N instancias de la app. {@code FOR UPDATE SKIP
 *       LOCKED} reparte las filas: la suma de lo que publicó cada réplica es exactamente la cantidad de eventos del
 *       outbox, y en Kafka hay un mensaje por evento (ni duplicados ni faltantes).</li>
 *   <li><b>Escalamiento por particiones</b>: los tópicos tienen más de una partición, los mensajes se reparten entre
 *       todas, y cada nominación cae en la misma partición en los tres tópicos (key = nomination_id: orden por
 *       nominación).</li>
 *   <li><b>Trazabilidad</b>: el correlation_id del POST está en cada fila del historial y en los headers de
 *       nomination.requested.v1 y nomination.result.v1; el historial respeta la máquina de estados.</li>
 * </ul>
 * Determinismo: se espera a estados y a filas de la base, y los tópicos se leen hasta el high watermark
 * ({@link KafkaTopicProbe}) una vez que todo terminó: "exactamente uno" no depende de esperar a que algo no llegue.
 */
@Tag("integration")
@Tag("E10")
@DisplayName("E10 - Pico de volumen: ráfaga concurrente con réplicas del relay, sin perder trazabilidad ni garantías")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        // Las "réplicas" del relay son hilos del test: sin el ciclo programado, la contabilidad es exacta.
        "nominations.outbox.relay.enabled=false",
        // Lotes chicos: más ciclos y más competencia entre réplicas por las mismas filas.
        "nominations.outbox.relay.batch-size=20",
        "nominations.abm-mock.response-delay=100ms"})
@Import({TestcontainersConfiguration.class, AbmFlowIntegrationTest.AbmRequestRecorder.class})
class E10PeakVolumeIntegrationTest {

    private static final int ENTITIES = 8;
    private static final int NOMINATIONS_PER_ENTITY = 30;
    private static final int NOMINATIONS = ENTITIES * NOMINATIONS_PER_ENTITY;
    /** Uno de cada cinco request_id se envía dos veces a la vez (reintento del canal en plena carrera). */
    private static final int RETRY_EVERY = 5;
    private static final int CLIENT_THREADS = 32;
    private static final int RELAY_REPLICAS = 4;
    private static final Duration FINAL_STATE_TIMEOUT = Duration.ofSeconds(45);
    private static final String ACCOUNT_ID = NominationEventFlowIntegrationTest.ACCOUNT_ID;
    private static final String DEMO_CONSUMER = "notifications-demo";
    private static final Set<String> FINAL = Set.of("APPROVED", "REJECTED");

    /** Mezcla de escenarios del simulador que terminan solos: aprobación y rechazos con distintos motivos. */
    private static final List<CardScenario> CARDS = List.of(
            new CardScenario("tok_e10_ok_", "APPROVED", null),
            new CardScenario("tok_e10_ok_", "APPROVED", null),
            new CardScenario("tok_e10_REJECT_030_", "REJECTED", "CARD_NOT_ELIGIBLE"),
            new CardScenario("tok_e10_REJECT_010_", "REJECTED", "INVALID_ACCOUNT"));

    @LocalServerPort
    private int port;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private OutboxRelay relay;
    @Autowired
    private AbmFlowIntegrationTest.AbmRequestRecorder abmRequests;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void burstOfConcurrentRequestsWithParallelRelayReplicasKeepsEveryGuarantee() throws Exception {
        List<Planned> planned = plan();
        Map<String, String> tokens = new HashMap<>();
        planned.forEach(p -> tokens.computeIfAbsent(p.entity(), TestTokens::bearer));

        // ---------------------------------------------------------- réplicas del relay
        AtomicBoolean stop = new AtomicBoolean(false);
        List<AtomicInteger> publishedByReplica = new ArrayList<>();
        AtomicInteger relayErrors = new AtomicInteger();
        ExecutorService replicas = Executors.newFixedThreadPool(RELAY_REPLICAS);
        for (int i = 0; i < RELAY_REPLICAS; i++) {
            AtomicInteger published = new AtomicInteger();
            publishedByReplica.add(published);
            replicas.submit(new ReplicaLoop(relay, stop, published, relayErrors));
        }

        try {
            // ------------------------------------------------------ ráfaga de POST
            List<Call> calls = new ArrayList<>();
            for (Planned p : planned) {
                calls.add(new Call(p, false));
                if (p.index() % RETRY_EVERY == 0) {
                    calls.add(new Call(p, true));
                }
            }
            Collections.shuffle(calls);

            long burstStart = System.nanoTime();
            List<Response> responses = runConcurrently(calls, call -> post(call, tokens.get(call.planned().entity())));
            long burstMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - burstStart);

            // Todas 202 (alta o replay), ninguna 5xx ni 4xx.
            assertThat(responses).hasSize(calls.size())
                    .allSatisfy(r -> assertThat(r.status()).as("HTTP de %s", r.call()).isEqualTo(202));
            assertThat(responses).allSatisfy(r ->
                    assertThat(r.correlationIdHeader()).isEqualTo(r.call().planned().correlationId()));

            // Idempotencia en la carrera: por request_id, un único nomination_id y una sola respuesta "no replay".
            Map<UUID, List<Response>> byRequest = responses.stream()
                    .collect(Collectors.groupingBy(r -> r.call().planned().requestId()));
            assertThat(byRequest).hasSize(NOMINATIONS);
            byRequest.forEach((requestId, group) -> {
                assertThat(group.stream().map(Response::nominationId).distinct()).as("request_id %s", requestId)
                        .hasSize(1);
                assertThat(group.stream().filter(r -> !r.replayed())).as("altas de %s", requestId).hasSize(1);
            });
            Map<UUID, Planned> byNomination = new HashMap<>();
            byRequest.forEach((requestId, group) -> byNomination.put(group.getFirst().nominationId(),
                    group.getFirst().call().planned()));
            assertThat(byNomination).hasSize(NOMINATIONS);
            Set<UUID> ids = byNomination.keySet();

            // ------------------------------------------------------ todas llegan a estado final
            long drainStart = System.nanoTime();
            await().atMost(FINAL_STATE_TIMEOUT).pollInterval(Duration.ofMillis(250))
                    .until(() -> countInStatus(planned, FINAL) == NOMINATIONS);
            // ...y su nomination.result ya fue publicado y procesado por el consumidor de ejemplo.
            await().atMost(FINAL_STATE_TIMEOUT).pollInterval(Duration.ofMillis(250))
                    .until(() -> processedResults(planned) == NOMINATIONS);
            long drainMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - drainStart);

            // Reintentos tardíos (ya resueltas): replay, sin evento nuevo.
            List<Call> lateRetries = planned.stream().filter(p -> p.index() % 10 == 1)
                    .map(p -> new Call(p, true)).toList();
            List<Response> late = runConcurrently(lateRetries, call -> post(call, tokens.get(call.planned().entity())));
            assertThat(late).allSatisfy(r -> {
                assertThat(r.status()).isEqualTo(202);
                assertThat(r.replayed()).isTrue();
            });

            // ------------------------------------------------------ base: sin duplicados, estado esperado
            assertThat(jdbc.queryForObject("SELECT count(*) FROM nominations WHERE entity_id IN ("
                    + entitiesIn(planned) + ")", Integer.class)).isEqualTo(NOMINATIONS);
            Map<UUID, Map<String, Object>> rows = jdbc.queryForList(
                            "SELECT id, status, rejection_reason, correlation_id FROM nominations WHERE entity_id IN ("
                                    + entitiesIn(planned) + ")").stream()
                    .collect(Collectors.toMap(r -> (UUID) r.get("id"), r -> r));
            assertThat(rows.keySet()).isEqualTo(ids);
            byNomination.forEach((id, p) -> {
                assertThat(rows.get(id).get("status")).as("estado de %s", id).isEqualTo(p.card().status());
                assertThat(rows.get(id).get("rejection_reason")).isEqualTo(p.card().reason());
                assertThat(rows.get(id).get("correlation_id")).isEqualTo(p.correlationId());
            });
            // La API pública devuelve lo mismo (muestra: una nominación de cada diez).
            byNomination.entrySet().stream().filter(e -> e.getValue().index() % 10 == 0).forEach(e ->
                    assertThat(getStatus(e.getKey(), tokens.get(e.getValue().entity())))
                            .isEqualTo(e.getValue().card().status()));

            // Outbox: exactamente requested + result por nominación, todo publicado.
            Map<UUID, Map<String, UUID>> outbox = outboxEvents(ids);
            assertThat(outbox).hasSize(NOMINATIONS)
                    .allSatisfy((id, events) -> assertThat(events.keySet())
                            .containsExactlyInAnyOrder("nomination.requested", "nomination.result"));

            // ABM recibió cada nominación una sola vez.
            assertThat(ids).allSatisfy(id -> assertThat(abmRequests.count(id)).as("altas en ABM de %s", id)
                    .isEqualTo(1));

            // Historial: correlation_id del POST en cada fila y transiciones válidas.
            Map<UUID, List<Map<String, Object>>> histories = histories(ids);
            byNomination.forEach((id, p) -> {
                List<Map<String, Object>> history = histories.get(id);
                assertThat(history).allSatisfy(row ->
                        assertThat(row.get("correlation_id")).isEqualTo(p.correlationId()));
                List<String> statuses = history.stream().map(r -> (String) r.get("to_status")).toList();
                assertThat(statuses).as("historial de %s", id).isIn(
                        List.of("RECEIVED", "PENDING_ABM", p.card().status()),
                        List.of("RECEIVED", p.card().status()));
                assertThat(history.getFirst()).containsEntry("source", "API");
                assertThat(history.getLast()).containsEntry("source", "ABM_RESPONSE");
            });

            // ------------------------------------------------------ réplicas: nadie publicó dos veces
            stop.set(true);
            replicas.shutdown();
            assertThat(replicas.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
            List<Integer> perReplica = publishedByReplica.stream().map(AtomicInteger::get).toList();
            assertThat(relayErrors.get()).isZero();
            assertThat(perReplica.stream().mapToInt(Integer::intValue).sum())
                    .as("publicados por las réplicas %s", perReplica).isEqualTo(2 * NOMINATIONS);
            assertThat(perReplica.stream().filter(n -> n > 0).count())
                    .as("réplicas que publicaron algo %s", perReplica).isGreaterThanOrEqualTo(2);

            // ------------------------------------------------------ Kafka: uno y solo uno por evento, repartidos
            int partitions = partitionCount(KafkaTopics.NOMINATION_REQUESTED);
            assertThat(partitions).isGreaterThan(1);
            assertThat(partitionCount(KafkaTopics.NOMINATION_RESULT)).isEqualTo(partitions);

            Map<UUID, ConsumerRecord<String, String>> requested =
                    singleMessagePerNomination(KafkaTopics.NOMINATION_REQUESTED, ids);
            Map<UUID, ConsumerRecord<String, String>> results =
                    singleMessagePerNomination(KafkaTopics.NOMINATION_RESULT, ids);
            Map<UUID, Integer> abmResponsePartition = abmResponsePartitions(ids);

            byNomination.forEach((id, p) -> {
                ConsumerRecord<String, String> req = requested.get(id);
                ConsumerRecord<String, String> res = results.get(id);
                assertThat(header(req, KafkaTopics.HEADER_CORRELATION_ID)).isEqualTo(p.correlationId());
                assertThat(header(res, KafkaTopics.HEADER_CORRELATION_ID)).isEqualTo(p.correlationId());
                assertThat(header(req, KafkaTopics.HEADER_EVENT_ID))
                        .isEqualTo(outbox.get(id).get("nomination.requested").toString());
                assertThat(header(res, KafkaTopics.HEADER_EVENT_ID))
                        .isEqualTo(outbox.get(id).get("nomination.result").toString());
                assertThat(json(res.value()).get("status").asText()).isEqualTo(p.card().status());
                // Misma key → misma partición en los tres tópicos: el orden por nominación se conserva de punta a punta.
                assertThat(res.partition()).as("partición de %s", id).isEqualTo(req.partition());
                assertThat(abmResponsePartition.get(id)).isEqualTo(req.partition());
            });

            Map<Integer, Long> distribution = requested.values().stream()
                    .collect(Collectors.groupingBy(ConsumerRecord::partition, Collectors.counting()));
            assertThat(distribution.keySet()).as("particiones usadas %s", distribution).hasSize(partitions);

            System.out.printf("""
                    [E10] %d nominaciones (%d POST, %d reintentos concurrentes, %d tardíos) de %d entidades, %d hilos.
                    [E10] ráfaga de POST: %d ms (%.0f req/s); drenado hasta estado final + resultado consumido: %d ms.
                    [E10] relay: %d réplicas, publicados por réplica %s; %d particiones, nomination.requested por partición %s
                    """, NOMINATIONS, calls.size(), calls.size() - NOMINATIONS, late.size(), ENTITIES, CLIENT_THREADS,
                    burstMillis, calls.size() * 1000.0 / Math.max(1, burstMillis), drainMillis,
                    RELAY_REPLICAS, perReplica, partitions, new java.util.TreeMap<>(distribution));
        } finally {
            stop.set(true);
            replicas.shutdownNow();
        }
    }

    // ---------------------------------------------------------------- plan de carga

    record CardScenario(String prefix, String status, String reason) {
    }

    record Planned(int index, String entity, UUID requestId, String cardId, CardScenario card, String correlationId) {
    }

    record Call(Planned planned, boolean retry) {
    }

    record Response(Call call, int status, UUID nominationId, boolean replayed, String correlationIdHeader) {
    }

    private static List<Planned> plan() {
        List<Planned> planned = new ArrayList<>();
        int index = 0;
        for (int e = 0; e < ENTITIES; e++) {
            String entity = NominationEventFlowIntegrationTest.newEntity();
            for (int n = 0; n < NOMINATIONS_PER_ENTITY; n++, index++) {
                CardScenario card = CARDS.get(index % CARDS.size());
                planned.add(new Planned(index, entity, UUID.randomUUID(), card.prefix() + String.format("%04d", index),
                        card, "it-e10-" + UUID.randomUUID()));
            }
        }
        return planned;
    }

    /** Un loop de "instancia" del relay: llama a relayOnce sin pausa mientras haya trabajo y acumula lo publicado. */
    private record ReplicaLoop(OutboxRelay relay, AtomicBoolean stop, AtomicInteger published,
                               AtomicInteger errors) implements Runnable {
        @Override
        public void run() {
            while (!stop.get()) {
                try {
                    int n = relay.relayOnce();
                    published.addAndGet(n);
                    if (n == 0) {
                        TimeUnit.MILLISECONDS.sleep(25);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    errors.incrementAndGet();
                }
            }
        }
    }

    // ---------------------------------------------------------------- HTTP

    private <T, R> List<R> runConcurrently(List<T> tasks, Function<T, R> action) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(CLIENT_THREADS);
        try {
            List<Callable<R>> callables = tasks.stream().<Callable<R>>map(t -> () -> action.apply(t)).toList();
            List<R> results = new ArrayList<>();
            for (Future<R> f : pool.invokeAll(callables, 60, TimeUnit.SECONDS)) {
                results.add(f.get());
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    private Response post(Call call, String bearer) {
        Planned p = call.planned();
        String body = """
                {"request_id":"%s","customer_id":"CUST-%06d","account_id":"%s","card_id":"%s","alias":"E10"}
                """.formatted(p.requestId(), p.index(), ACCOUNT_ID, p.cardId());
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/nominations"))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", bearer)
                .header("Content-Type", "application/json")
                .header(ApiHeaders.CORRELATION_ID, p.correlationId())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            UUID nominationId = response.statusCode() == 202
                    ? UUID.fromString(json(response.body()).get("nomination_id").asText()) : null;
            return new Response(call, response.statusCode(), nominationId,
                    "true".equals(response.headers().firstValue(ApiHeaders.IDEMPOTENT_REPLAYED).orElse(null)),
                    response.headers().firstValue(ApiHeaders.CORRELATION_ID).orElse(null));
        } catch (Exception e) {
            throw new IllegalStateException("POST falló: " + call, e);
        }
    }

    private String getStatus(UUID nominationId, String bearer) {
        HttpRequest request = HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/v1/nominations/" + nominationId))
                .header("Authorization", bearer).GET().build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            return json(response.body()).get("status").asText();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private JsonNode json(String body) {
        try {
            return objectMapper.readTree(body);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------------------------------------------------------------- base

    private static String entitiesIn(List<Planned> planned) {
        return planned.stream().map(Planned::entity).distinct().map(e -> "'" + e + "'")
                .collect(Collectors.joining(","));
    }

    private int countInStatus(List<Planned> planned, Set<String> statuses) {
        String in = statuses.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        return jdbc.queryForObject("SELECT count(*) FROM nominations WHERE entity_id IN (" + entitiesIn(planned)
                + ") AND status IN (" + in + ")", Integer.class);
    }

    private int processedResults(List<Planned> planned) {
        return jdbc.queryForObject("""
                SELECT count(*) FROM consumer_processed_events c
                  JOIN outbox_events o ON o.id = c.event_id
                  JOIN nominations n ON n.id = o.aggregate_id
                 WHERE c.consumer = ? AND o.event_type = 'nomination.result' AND n.entity_id IN (%s)"""
                .formatted(entitiesIn(planned)), Integer.class, DEMO_CONSUMER);
    }

    private Map<UUID, Map<String, UUID>> outboxEvents(Set<UUID> ids) {
        Map<UUID, Map<String, UUID>> events = new HashMap<>();
        jdbc.query("SELECT aggregate_id, event_type, id, published_at FROM outbox_events WHERE aggregate_id = ANY (?)",
                ps -> ps.setArray(1, ps.getConnection().createArrayOf("uuid", ids.toArray())),
                rs -> {
                    assertThat(rs.getTimestamp("published_at")).as("evento publicado").isNotNull();
                    UUID previous = events.computeIfAbsent(rs.getObject("aggregate_id", UUID.class), k -> new HashMap<>())
                            .put(rs.getString("event_type"), rs.getObject("id", UUID.class));
                    assertThat(previous).as("evento duplicado en el outbox").isNull();
                });
        return events;
    }

    private Map<UUID, List<Map<String, Object>>> histories(Set<UUID> ids) {
        Map<UUID, List<Map<String, Object>>> histories = new HashMap<>();
        jdbc.query("""
                        SELECT nomination_id, to_status, source, correlation_id FROM nomination_history
                         WHERE nomination_id = ANY (?) ORDER BY nomination_id, occurred_at, id""",
                ps -> ps.setArray(1, ps.getConnection().createArrayOf("uuid", ids.toArray())),
                rs -> {
                    Map<String, Object> row = new HashMap<>();
                    row.put("to_status", rs.getString("to_status"));
                    row.put("source", rs.getString("source"));
                    row.put("correlation_id", rs.getString("correlation_id"));
                    histories.computeIfAbsent(rs.getObject("nomination_id", UUID.class), k -> new ArrayList<>()).add(row);
                });
        assertThat(histories.keySet()).isEqualTo(ids);
        return histories;
    }

    // ---------------------------------------------------------------- Kafka

    private int partitionCount(String topic) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS)
                    .get(topic).partitions().size();
        }
    }

    /**
     * Lee el tópico hasta el high watermark y exige exactamente un mensaje por nominación: ni faltantes ni duplicados
     * (event_id distintos en todo el conjunto).
     */
    private Map<UUID, ConsumerRecord<String, String>> singleMessagePerNomination(String topic, Set<UUID> ids) {
        Map<UUID, ConsumerRecord<String, String>> single = new HashMap<>();
        Set<String> eventIds = new HashSet<>();
        try (var probe = new KafkaTopicProbe(kafka.getBootstrapServers(), topic)) {
            for (UUID id : ids) {
                List<ConsumerRecord<String, String>> records = probe.recordsWithKey(id);
                assertThat(records).as("mensajes en %s para %s", topic, id).hasSize(1);
                single.put(id, records.getFirst());
                eventIds.add(header(records.getFirst(), KafkaTopics.HEADER_EVENT_ID));
            }
        }
        assertThat(eventIds).hasSize(ids.size());
        return single;
    }

    private Map<UUID, Integer> abmResponsePartitions(Set<UUID> ids) {
        Map<UUID, Integer> partitions = new HashMap<>();
        try (var probe = new KafkaTopicProbe(kafka.getBootstrapServers(), KafkaTopics.ABM_RESPONSES)) {
            for (UUID id : ids) {
                List<ConsumerRecord<String, String>> records = probe.recordsWithKey(id);
                assertThat(records).as("respuestas de ABM para %s", id).hasSize(1);
                partitions.put(id, records.getFirst().partition());
            }
        }
        return partitions;
    }
}
