# Observabilidad

Propuesta de observabilidad de `prisma-nominations-api`: métricas, logs, trazas, tablero y alertas accionables. Las
alertas y qué hacer ante cada una están en el [runbook](operations.md#alertas); los artefactos, en
[`ops/`](../ops) (Prometheus, reglas de alerta, Grafana) y en [`docker-compose.yml`](../docker-compose.yml).

| Pilar | Herramienta (demo) | Qué aporta |
|-------|--------------------|-----------|
| Métricas | Micrometer → `/actuator/prometheus` → Prometheus (scrape 15s) | Tasas, latencias (histogramas), backlog, estado del CB |
| Alertas | Reglas de Prometheus ([`alerts.yml`](../ops/prometheus/alerts.yml)) | Síntomas para el usuario + indicadores tempranos, cada una con runbook |
| Tableros | Grafana, tablero provisionado "Nominaciones – visión operativa" | Visión de la cadena completa: API → outbox → Kafka → ABM → resultado |
| Trazas | Micrometer Tracing (OpenTelemetry) → OTLP HTTP → Jaeger | Dónde se fue el tiempo de una nominación, a través de HTTP y Kafka |
| Logs | Logback: `NIVEL [correlation_id] [trace_id,span_id]` en cada línea (texto) o JSON ECS con el perfil `json-logs` | Detalle del caso puntual; nunca `account_id` completo ni PAN |
| Auditoría | `nomination_history` (solo inserción) | Fuente de verdad de cada cambio de estado, con `source` y `correlation_id` |

## Levantar el stack

```bash
docker compose up -d                       # postgres, kafka, kafka-ui, jaeger, prometheus, grafana
cd app && mvn spring-boot:run              # la app corre en el host (8080) y Prometheus la scrapea
```

| Servicio | URL | Notas |
|----------|-----|-------|
| Grafana | http://localhost:3000 | `admin` / `admin` (lectura anónima habilitada). El tablero es la home. Si el 3000 está ocupado: `GRAFANA_PORT=3001 docker compose up -d` |
| Prometheus | http://localhost:9090 | Alertas en `/alerts`, targets en `/targets`. Sin Alertmanager en la demo |
| Jaeger | http://localhost:16686 | Servicio `prisma-nominations-api`. Recibe OTLP en 4318 (HTTP) y 4317 (gRPC) |
| Kafka UI | http://localhost:8081 | Tópicos, DLT, consumer groups y lag |
| Métricas crudas | http://localhost:8080/actuator/prometheus | Sin token en la demo (en producción: red interna o credenciales de scrape) |

Prometheus llega a la app por `host.docker.internal:8080` (en Linux, vía `extra_hosts: host-gateway`). Si el target
figura `DOWN` en `/targets`, la app no está corriendo o no escucha en 8080. Para recargar reglas sin reiniciar:
`curl -X POST http://localhost:9090/-/reload`.

## SLIs y SLOs propuestos

| SLI | Definición (PromQL resumida) | SLO | Alerta |
|-----|------------------------------|-----|--------|
| Disponibilidad de la API | `1 - 5xx / total` de `http_server_requests_seconds_count` (sin `/actuator`, sin `/abm-mock`) | 99,9% mensual (≈ 43 min de presupuesto) | `ApiErrorRateHigh` (> 1% en 5m) |
| Latencia de aceptación | p95 de `http_server_requests_seconds` para `POST /v1/nominations` | p95 < 300ms | `ApiLatencyP95High` |
| Tiempo de resolución end to end | p95 de `nominations_resolution_time_seconds` (alta → `APPROVED`/`REJECTED`) | p95 < 5 min (depende del SLA de ABM; con el simulador, segundos) | `ResolutionLatencyP95High` |
| Frescura del outbox | `outbox_oldest_pending_age_seconds` | < 5s el 99% del tiempo | `OutboxBacklogGrowing` (> 60s durante 5m) |
| Completitud | nominaciones que terminan en estado final sin intervención (sin `ABM_TIMEOUT` ni DLT) | ≥ 99,5% | `AbmTimeoutsIncreasing`, `DltMessagesIncreasing` |

Los 4xx no consumen presupuesto de error (son errores del cliente), pero un aumento de
`nominations_idempotency_conflicts_total` de una entidad indica un cliente con bug y se mira en el tablero. En
producción las alertas de SLO pasarían a **burn rate multi-ventana** (p.ej. 14,4× en 1h y 5m para `critical`, 6× en 6h
y 30m para `warning`) en lugar de umbrales fijos.

## ¿Qué indicadores permiten detectar degradación antes de un incidente?

El incidente para el negocio es "las nominaciones no se resuelven" o "la API rechaza altas". Casi todo lo que lleva
ahí se ve antes en un indicador intermedio de la cadena asincrónica, mucho antes de que el usuario lo note:

| Indicador | Qué anticipa | Umbral | Alerta |
|-----------|--------------|--------|--------|
| `abm_submissions_total{outcome="UNAVAILABLE"}` / total | Apertura del circuit breaker y ola de `ABM_TIMEOUT` (~6 min después, al agotar la capa 2) | > 20% en 5m | `AbmUnavailableRateHigh` |
| `resilience4j_circuitbreaker_failure_rate{name="abm"}` | Apertura del CB (abre al 50%) | > 25% durante 2m | `AbmFailureRateRising` |
| `resilience4j_retry_calls_total{kind="successful_with_retry"}` en alza | ABM inestable aunque todavía "funciona": más latencia y más carga sobre ABM | tendencia (tablero) | — |
| `nominations_open{status="PENDING_ABM"}` creciendo | ABM acepta pero no responde: en 15m el sweeper pasa todo a `ABM_TIMEOUT` | > 50 y creciendo 10m | `NominationsStuckPendingAbm` |
| `outbox_oldest_pending_age_seconds` | Kafka o el relay trabados: nada llega a ABM ni a los canales | > 60s durante 5m | `OutboxBacklogGrowing` |
| `outbox_failing` | Publicación rota (tópico, tamaño, réplicas) antes de que el backlog sea visible | > 0 durante 5m | `OutboxPublishFailing` |
| `kafka_consumer_fetch_manager_records_lag_max` | Resolución lenta por consumer bloqueado (ABM lento en capa 1) o falta de capacidad | > 1000 durante 10m | `ConsumerLagHigh` |
| `hikaricp_connections_pending` | Latencia del POST, 5xx y outbox detenido (todo pasa por PostgreSQL) | > 0 durante 5m | `DbPoolSaturated` |
| p95 de `POST /v1/nominations` | Saturación de la base antes de que haya errores | > 300ms durante 10m | `ApiLatencyP95High` |
| `abm_submissions_total{outcome="CONTRACT_ERROR"}` | Cambio de contrato no coordinado con ABM: cada pedido afectado termina en `ABM_TIMEOUT` | > 0 en 15m | `AbmContractErrors` |
| `abm_responses_total{outcome="CONFLICT"}` | Inconsistencia de datos en ABM (riesgo de estado incorrecto informado al cliente) | > 0 en 1h | `AbmResponseConflicts` |
| `kafka_dlt_messages` creciendo | Fallas que ya no se recuperan solas | cualquier aumento en 10m | `DltMessagesIncreasing` |
| `process_cpu_usage`, `jvm_gc_pause_seconds`, heap | Degradación del proceso (fugas, GC) antes de latencia visible | tendencia (tablero) | — |

Las alertas de **síntoma** (`ApiErrorRateHigh`, `CircuitBreakerAbmOpen`, `ResolutionLatencyP95High`,
`NominationsApiDown`) confirman el incidente; las de **causa temprana** (resto) permiten actuar antes. Todas tienen
runbook y ninguna salta por comportamiento esperado: `DUPLICATE` de ABM no alerta (reentrega normal), los consumers de
tópicos `-retry` se excluyen del lag y los 5xx simulados de `/abm-mock` no cuentan como errores de la API.

## Tablero "Nominaciones – visión operativa"

Provisionado desde [`ops/grafana/dashboards/nominations.json`](../ops/grafana/dashboards/nominations.json), datasource
`prometheus` (uid fijo). Variable `entity` para filtrar por entidad. Filas:

| Fila | Paneles |
|------|---------|
| Resumen (SLIs) | Disponibilidad 5m, p95 de aceptación, p95 de resolución, edad del outbox, estado del CB, alertas disparadas |
| Tráfico y negocio | Altas/s por entidad, replays y conflictos de idempotencia, resueltas por estado y motivo, requests por uri/status, tasa de 5xx |
| Latencia | p50/p95/p99 de resolución end to end y de `POST /v1/nominations`, p95 de resolución por estado, pool Hikari |
| ABM | Estado del CB, envíos por outcome, tasa de UNAVAILABLE vs failure rate del CB, reintentos en proceso, respuestas por outcome, timeouts por `source` y reprocesos |
| Asincronía | Outbox pendiente / fallando, edad del pendiente más viejo, lag máximo por consumer, mensajes acumulados y nuevos en DLT |
| Nominaciones abiertas | Abiertas por estado (`RECEIVED`, `PENDING_ABM`, `ABM_TIMEOUT`) |
| JVM y proceso | CPU, heap, pausas de GC |

Leído de arriba hacia abajo sigue el camino de una nominación: si la resolución se demora, la fila de asincronía y la
de ABM dicen en qué tramo.

## Correlación: logs ↔ trazas ↔ historial

Tres identificadores, cada uno con su alcance:

| Id | Origen | Dónde aparece |
|----|--------|---------------|
| `correlation_id` | Header `X-Correlation-Id` del cliente (o generado y devuelto) | Logs (`[correlation_id]`), `nomination_history`, outbox, headers de Kafka, DLT |
| `trace_id` | Micrometer Tracing (W3C `traceparent`) | Logs (`[trace_id,span_id]`), Jaeger, header `traceparent` en HTTP y en Kafka |
| `nomination_id` | La API al crear | Todo lo anterior + API (`/v1/nominations/{id}`, `/history`) |

Recorrido típico ante una alerta o un reclamo:

1. **Del tablero al caso**: el panel muestra el tramo con problemas (p.ej. timeouts `source=SWEEPER`). En los logs
   del período se toma un `correlation_id` / `nomination_id` afectado (o del reclamo del cliente).
2. **Historial** (qué pasó): `nomination_history` por `correlation_id` da cada transición con `source` y `detail`
   (consulta en [operations.md](operations.md#diagnosticar-con-el-correlation-id)).
3. **Logs** (por qué): filtrando por `correlation_id` se ven intentos a ABM, tópicos de retry, CB y DLT; cada línea
   trae también el `trace_id`.
4. **Traza** (dónde se fue el tiempo): con el `trace_id`, Jaeger → *Search* → *Trace ID*. Se ven el POST, la
   escritura en PostgreSQL, la publicación del outbox, el consumo en el ABM Adapter, la llamada HTTP a ABM con sus
   reintentos y el procesamiento de la respuesta. En Jaeger también se puede buscar por operación y
   duración (p.ej. `http post /v1/nominations`, *Min Duration* 300ms) para encontrar casos lentos.

Una nominación atraviesa varios procesos asincrónicos: el relay del outbox y la respuesta de ABM (que llega por Kafka
minutos después) pueden quedar en trazas distintas pero enlazadas; el `correlation_id` es el hilo que las une siempre,
incluso cuando ABM no propaga `traceparent`.

## A x100 de volumen

| Tema | Hoy (demo) | A x100 |
|------|------------|--------|
| Cardinalidad de métricas | `entity_id` como etiqueta (decenas de entidades), `reason` acotado por enum | Mantener etiquetas **acotadas**: nunca `nomination_id`, `account_id` ni `correlation_id` como label. Si las entidades crecen a miles, agregar por segmento o dejar `entity_id` solo en contadores de negocio. `http.server.requests` con `uri` de plantilla (ya es así) |
| Histogramas | `percentiles-histogram` para HTTP y resolución (decenas de buckets) | Limitar buckets con `minimum/maximum-expected-value` y SLOs (`slo` buckets), o histogramas nativos de Prometheus |
| Muestreo de trazas | `probability: 1.0` | Head sampling 5–10% + **tail sampling** en un OpenTelemetry Collector: conservar 100% de trazas con error, lentas (> SLO) o que pasaron por DLT/`ABM_TIMEOUT`. Jaeger con almacenamiento persistente (Elasticsearch/OpenSearch, Cassandra) o Tempo |
| Retención | Prometheus 7d local, Jaeger en memoria | Métricas: 15d en Prometheus + largo plazo en Thanos/Mimir con downsampling (13 meses para SLOs). Trazas: 7d. Logs: 30d calientes, archivado según normativa (auditoría real en `nomination_history`) |
| Logs | Texto a stdout | JSON estructurado con `trace_id`, `span_id`, `correlation_id`, `nomination_id`, `entity_id`; envío a Loki/ELK; nivel INFO con muestreo de logs repetitivos (reintentos) |
| Prometheus | Uno solo, scrape estático | Service discovery (Kubernetes), HA en pares, recording rules para las consultas del tablero y de las alertas (p.ej. `job:abm_unavailable_ratio:rate5m`), federación o remote write |
| Alertas | Umbrales fijos, sin Alertmanager | Alertmanager con ruteo por `severity`/equipo (ABM vs plataforma), agrupación e inhibición (si `CircuitBreakerAbmOpen` está activa, silenciar `AbmUnavailableRateHigh` y `ConsumerLagHigh`), alertas de SLO por burn rate |
| Gauges con consultas a la DB | `nominations_open` y `outbox_*` consultan PostgreSQL en cada scrape | Cachear el valor (refresco cada 15–30s) y usar índices parciales por estado, para que el scrape no cargue la base; con varias instancias, calcularlos en una sola (o leer `max` en las consultas, como ya hacen las alertas) |
| Umbrales | Absolutos (p.ej. lag > 1000, `PENDING_ABM` > 50) | Relativos al throughput (lag en segundos de atraso, `PENDING_ABM` / altas por minuto) |
