# Runbook de operación

Guía para operar la API de nominaciones y su integración con ABM: qué alerta, qué hacer ante cada alerta, cómo
diagnosticar una nominación con su correlation id y cómo recuperarla. Contexto de diseño en el
[README](../README.md#resiliencia-e6), contrato de eventos en [`events.md`](events.md) y propuesta de observabilidad
(SLOs, indicadores tempranos, tablero, trazas) en [`observability.md`](observability.md).

## Alertas

Definidas en [`ops/prometheus/alerts.yml`](../ops/prometheus/alerts.yml); cada una lleva en `annotations.runbook` el
enlace a su sección de abajo. Se ven en http://localhost:9090/alerts y en el tablero de Grafana
"Nominaciones – visión operativa". `critical` = actuar ya; `warning` = actuar en el día o anticipa un incidente.

| Alerta | Severidad | Condición (resumida) | Runbook |
|--------|-----------|----------------------|---------|
| `NominationsApiDown` | critical | `up == 0` durante 1m | [→](#nominationsapidown) |
| `ApiErrorRateHigh` | critical | 5xx / total > 1% en 5m (sin actuator ni abm-mock) | [→](#apierrorratehigh) |
| `ApiLatencyP95High` | warning | p95 de `POST /v1/nominations` > 300ms durante 10m | [→](#apilatencyp95high) |
| `DbPoolSaturated` | warning | `hikaricp_connections_pending` > 0 durante 5m | [→](#dbpoolsaturated) |
| `CircuitBreakerAbmOpen` | critical | CB `abm` en `open` durante 1m | [→](#circuitbreakerabmopen) |
| `AbmUnavailableRateHigh` | warning | envíos `UNAVAILABLE` / total > 20% en 5m | [→](#abmunavailableratehigh) |
| `AbmFailureRateRising` | warning | failure rate del CB > 25% (abre al 50%) | [→](#abmunavailableratehigh) |
| `AbmContractErrors` | warning | algún `CONTRACT_ERROR` en 15m | [→](#abmcontracterrors) |
| `AbmTimeoutsIncreasing` | warning | > 5 pasajes a `ABM_TIMEOUT` en 15m (por `source`) | [→](#abmtimeoutsincreasing) |
| `NominationsInAbmTimeout` | warning | nominaciones en `ABM_TIMEOUT` durante 30m | [→](#abmtimeoutsincreasing) |
| `NominationsStuckPendingAbm` | warning | `PENDING_ABM` > 50 y creciendo durante 10m | [→](#nominationsstuckpendingabm) |
| `AbmResponseConflicts` | warning | alguna respuesta `CONFLICT` en 1h | [→](#abmresponseconflicts) |
| `ResolutionLatencyP95High` | warning | p95 alta → resultado > 5m durante 15m | [→](#resolutionlatencyp95high) |
| `OutboxBacklogGrowing` | warning | pendiente más viejo > 60s durante 5m | [→](#outboxbackloggrowing) |
| `OutboxPublishFailing` | critical | `outbox_failing` > 0 durante 5m | [→](#outboxpublishfailing) |
| `DltMessagesIncreasing` | warning | mensajes nuevos en algún DLT en 10m | [→](#dltmessagesincreasing) |
| `ConsumerLagHigh` | warning | lag máx. > 1000 durante 10m (sin tópicos `-retry`) | [→](#consumerlaghigh) |

## Runbooks por alerta

Cada sección: qué significa, cómo confirmarlo y qué hacer. El diagnóstico fino de una nominación puntual está en
[Diagnosticar con el correlation id](#diagnosticar-con-el-correlation-id) y las acciones en [Recuperar](#recuperar).

### NominationsApiDown

- **Significa**: Prometheus no puede leer `/actuator/prometheus`. La API puede estar caída o colgada.
- **Confirmar**: `GET /actuator/health/liveness` y `/readiness`; logs de arranque (Flyway, conexión a PostgreSQL/Kafka).
- **Hacer**: reiniciar la instancia si la liveness falla; si es arranque, revisar migraciones y conectividad. El estado
  vive en PostgreSQL y en Kafka: al volver, el relay publica lo pendiente del outbox y los consumers retoman su offset.

### ApiErrorRateHigh

- **Significa**: más del 1% de las respuestas de la API son 5xx (SLO de disponibilidad 99,9%). Los 4xx (validación,
  409 de idempotencia) no cuentan: son errores del cliente.
- **Confirmar**: panel "Requests HTTP por uri y status" (qué endpoint); logs ERROR con su `correlation_id` y la traza
  en Jaeger (span de JDBC lento o con error).
- **Hacer**: si es PostgreSQL → ver [DbPoolSaturated](#dbpoolsaturated). Si es una excepción no mapeada en un endpoint
  puntual → bug, rollback del último despliegue. ABM caído **no** debe producir 5xx en la API (el POST no llama a ABM).

### ApiLatencyP95High

- **Significa**: el p95 de `POST /v1/nominations` supera 300ms. El POST solo escribe en PostgreSQL (nominación +
  historial + outbox en una TX), así que la causa casi siempre es la base.
- **Confirmar**: panel "Pool de PostgreSQL" (conexiones esperando), trazas lentas en Jaeger filtrando
  `http.route=/v1/nominations` y `minDuration=300ms`, CPU del proceso y pausas de GC.
- **Hacer**: buscar locks o queries lentas (`pg_stat_activity`), revisar el tamaño del pool, escalar instancias.

### DbPoolSaturated

- **Significa**: hay hilos esperando conexión del pool Hikari hace 5m. Precede a latencia alta, 5xx y a un outbox que
  no avanza.
- **Confirmar**: `hikaricp_connections_active` en el máximo; `SELECT * FROM pg_stat_activity WHERE state <> 'idle'`.
- **Hacer**: cortar la query o el lock que retiene conexiones; si es carga legítima, escalar pool/DB (ojo: con hilos
  virtuales la concurrencia la limita el pool, no los hilos).

### CircuitBreakerAbmOpen

- **Significa**: el CB `abm` está `OPEN` hace más de 1 minuto: ABM está caído o muy lento. No se le envía carga.
- **Confirmar**: `GET /actuator/health` con token de operador → `components.circuitBreakers.details.abm` (`state`,
  `failureRate`, `slowCallRate`); métrica `resilience4j_circuitbreaker_state{name="abm"}`; log ERROR `Circuit breaker de ABM CLOSED -> OPEN`; panel "Envíos a ABM por outcome".
- **Hacer**: escalar al equipo de ABM con la hora de apertura. Del lado de la API no hay acción inmediata: ver
  "ABM caído, CB abierto" en [Recuperar](#recuperar). Al cerrarse, vigilar
  [AbmTimeoutsIncreasing](#abmtimeoutsincreasing) y reprocesar lo que haya quedado en `ABM_TIMEOUT`.

### AbmUnavailableRateHigh

- **Significa**: más del 20% de los envíos a ABM fallan por indisponibilidad (5xx, 408/429, timeout, conexión), o la
  ventana del CB ya acumula > 25% de fallas (`AbmFailureRateRising`). Es la **alerta temprana** del CB (abre al 50%).
- **Confirmar**: paneles "Tasa de UNAVAILABLE y fallas del CB" y "Reintentos en proceso"; logs
  `Reintentando envío a ABM: … causa_anterior=…` (timeout vs 5xx vs conexión).
- **Hacer**: avisar a ABM antes de que el CB abra. Si la causa es lentitud (timeouts) y no errores, revisar
  `read-timeout` vs la latencia real de ABM.

### AbmContractErrors

- **Significa**: ABM rechazó pedidos por contrato (4xx de validación o 2xx sin `abm_operation_id`). No se reintentan:
  van directo al DLT y la nominación queda en `ABM_TIMEOUT` con detalle "ABM rechazó el pedido por contrato".
- **Confirmar**: headers `kafka_exception-message` del mensaje en `nomination.requested.v1-dlt`; historial de la
  nominación.
- **Hacer**: corregir la causa (dato de entrada, versión del contrato) **antes** de reprocesar: reprocesar sin cambios
  vuelve a fallar igual.

### AbmTimeoutsIncreasing

- **Significa**: nominaciones pasan a `ABM_TIMEOUT` (`AbmTimeoutsIncreasing`) o siguen ahí sin respuesta tardía
  (`NominationsInAbmTimeout`). `source=ABM_ADAPTER`: se agotaron los reintentos (ABM no disponible).
  `source=SWEEPER`: ABM aceptó pero no respondió dentro del SLA de 15m.
- **Confirmar**: `SELECT id, updated_at FROM nominations WHERE status = 'ABM_TIMEOUT' ORDER BY updated_at`;
  historial de cada una (`detail`).
- **Hacer**: confirmar con ABM si las procesó (por `nomination_id`). Si responde tarde, se resuelven solas. Si no,
  reprocesar en lote y con el CB cerrado (ver [Recuperar](#recuperar)).

### NominationsStuckPendingAbm

- **Significa**: crecen las nominaciones en `PENDING_ABM`: ABM las aceptó pero sus respuestas no llegan (o no se
  procesan). En 15m el sweeper las pasará a `ABM_TIMEOUT` en masa.
- **Confirmar**: lag del consumer `abm-response-processor` (si hay lag, el problema es nuestro; si no, ABM no publica
  en `abm.responses.v1`); mensajes en `abm.responses.v1-dlt`.
- **Hacer**: si el consumer está trabado, revisar sus logs y el DLT; si ABM no publica, escalar a ABM. Si el
  sweeper no corre (log ERROR `Sweeper: falló el ciclo`), revisar la conexión a PostgreSQL.

### AbmResponseConflicts

- **Significa**: ABM respondió para una nominación ya resuelta un resultado distinto (p.ej. `APPROVED` y después
  `REJECTED`). La API conserva el primero e ignora el segundo; es una inconsistencia del lado de ABM.
- **Confirmar**: log WARN `Respuesta de ABM contradictoria, se ignora` (trae `nomination_id` y `correlation_id`) e
  historial de la nominación.
- **Hacer**: abrir un incidente con ABM con los ids; decidir con negocio cuál es el estado correcto. No hay
  corrección automática.

### ResolutionLatencyP95High

- **Significa**: el p95 entre el alta y el resultado final supera 5 minutos.
- **Confirmar**: recorrer la cadena en el tablero: edad del outbox → lag de `abm-adapter` → reintentos y CB de ABM →
  `PENDING_ABM` creciendo → lag de `abm-response-processor`. La traza en Jaeger de una nominación lenta muestra en qué
  tramo se fue el tiempo.
- **Hacer**: según el tramo, seguir el runbook de la alerta correspondiente.

### OutboxBacklogGrowing

- **Significa**: el evento pendiente más viejo del outbox tiene más de 60s (normal: < 1s, el relay corre cada 500ms).
  Nada nuevo llega a ABM ni a los consumidores de `nomination.result` mientras dure.
- **Confirmar**: `SELECT count(*), min(created_at) FROM outbox_events WHERE published_at IS NULL`; logs del relay;
  salud de Kafka (Kafka UI).
- **Hacer**: si Kafka está caído, recuperarlo (el relay publica solo al volver, en orden por agregado). Si el relay
  está apagado (`nominations.outbox.relay.enabled`) o la DB está saturada, ver [DbPoolSaturated](#dbpoolsaturated).

### OutboxPublishFailing

- **Significa**: hay eventos del outbox que acumulan intentos fallidos de publicación (E8).
- **Confirmar**: `SELECT id, aggregate_id, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND
  attempts >= 5`.
- **Hacer**: `last_error` dice la causa: tópico inexistente, mensaje demasiado grande, broker sin réplicas
  (`NOT_ENOUGH_REPLICAS`), autenticación. Corregirla; el relay reintenta solo.

### DltMessagesIncreasing

- **Significa**: llegaron mensajes a un DLT (`topic` en la alerta). En `nomination.requested.v1-dlt`, nominaciones que
  agotaron los reintentos (→ `ABM_TIMEOUT`), errores de contrato o poison pills. En `abm.responses.v1-dlt` /
  `nomination.result.v1-dlt`, mensajes que un consumer no pudo procesar.
- **Confirmar**: headers del DLT (ver [Diagnosticar](#diagnosticar-con-el-correlation-id), paso 3).
- **Hacer**: según la causa, ver [Recuperar](#recuperar) y [`events.md`](events.md#reprocesar-desde-el-dlt).

### ConsumerLagHigh

- **Significa**: un consumer acumula más de 1000 mensajes de atraso hace 10m. Anticipa demoras de resolución. Los
  consumers de tópicos `-retry` se excluyen: tienen lag por diseño mientras esperan su demora.
- **Confirmar**: Kafka UI → Consumers; ¿el consumer está vivo? ¿hubo rebalanceos? ¿ABM está lento (la capa 1 retiene
  el mensaje hasta ~6,6s)?
- **Hacer**: si es ABM, ver [AbmUnavailableRateHigh](#abmunavailableratehigh); si es volumen, escalar instancias
  (hasta 6 = particiones por tópico).

## Señales sin métrica (consultas de apoyo)

| Señal | Consulta | Qué significa |
|-------|----------|---------------|
| `PENDING_ABM` viejas | `SELECT count(*) FROM nominations WHERE status = 'PENDING_ABM' AND updated_at < now() - interval '15 minutes'` | > 0 durante más de 2 ciclos del sweeper: el sweeper no corre (log ERROR `Sweeper: falló el ciclo`) |
| `RECEIVED` viejas | `SELECT count(*) FROM nominations WHERE status = 'RECEIVED' AND updated_at < now() - interval '10 minutes'` | Normal mientras la capa 2 reintenta (hasta ~6 min); más allá, el adapter o el relay no avanzan |

## Diagnosticar con el correlation id

El `X-Correlation-Id` del POST (o el generado, que vuelve en la respuesta) viaja a historial, outbox, headers de
Kafka y a cada línea de log (`[correlation_id]` después del nivel).

1. **Historial** (fuente de verdad, solo inserción):
   ```sql
   SELECT h.occurred_at, h.from_status, h.to_status, h.source, h.detail
     FROM nomination_history h
    WHERE h.correlation_id = 'demo-fail-0001'          -- o: h.nomination_id = '<uuid>'
    ORDER BY h.id;
   ```
   `source` dice quién cambió el estado: `API`, `ABM_ADAPTER` (envío o DLT), `ABM_RESPONSE`, `SWEEPER`, `OPERATOR`.
   `detail` distingue "Reintentos agotados: ABM no disponible", "ABM rechazó el pedido por contrato" y
   "Sin respuesta de ABM dentro del SLA (…)". También: `GET /v1/nominations/{id}/history` con un token de esa entidad (`scripts/mint-token.sh <entity_id>`).
2. **Logs**: filtrar por el correlation id. Se ven la creación, cada intento a ABM
   (`Reintentando envío a ABM: … intento=2/3, causa_anterior=…`), el paso por los tópicos de retry
   (`Reintento de envío a ABM: … tópico=nomination.requested.v1-retry-0`), el CB y el procesamiento del DLT.
   Nunca aparecen `account_id` completo ni el PAN.
3. **Headers del DLT** (`nomination.requested.v1-dlt`): `kafka_exception-cause-fqcn` (causa raíz),
   `kafka_exception-message`, `kafka_original-topic` / `-partition` / `-offset`, `retry_topic-attempts`, más los
   propios (`event_id`, `correlation_id`). En los otros DLT el prefijo es `kafka_dlt-`. Ejemplo local:
   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic nomination.requested.v1-dlt --from-beginning --timeout-ms 5000 \
     --property print.key=true --property print.headers=true
   ```
4. **Circuit breaker**: `GET /actuator/health` con token de operador (estado, `failureRate`, `slowCallRate`, `notPermittedCalls`).
5. **Traza**: con el `trace_id` de cualquier línea de log, abrir la traza en Jaeger (http://localhost:16686) y ver
   cada tramo (HTTP, outbox, Kafka, ABM). Ver [`observability.md`](observability.md#correlación-logs--trazas--historial).

## Recuperar

| Situación | Acción |
|-----------|--------|
| ABM caído, CB abierto | Nada que hacer del lado de la API: el CB pasa solo a HALF_OPEN a los 30s y la capa 2 reintenta a 10s, 1m y 5m. Las que agoten todo quedan en `ABM_TIMEOUT` |
| Nominación en `ABM_TIMEOUT` | Confirmar con ABM (por `nomination_id` / `abm_operation_id` del log) si la procesó. Si respondió tarde, se resuelve sola (`ABM_TIMEOUT → APPROVED/REJECTED`). Si no, **reprocesar** (abajo) |
| Error de contrato con ABM (`ABM rechazó el pedido por contrato`) | Corregir la causa (dato o versión del contrato) antes de reprocesar: reprocesar sin cambios vuelve a fallar igual |
| Poison pill o nominación inexistente en el DLT | No hay estado que cambiar. Analizar el productor; descartar o, si se corrigió un bug del consumidor, republicar |
| Mensajes en `abm.responses.v1-dlt` / `nomination.result.v1-dlt` | Corregir la causa y republicar desde el DLT con sus headers (los consumidores son idempotentes). Ver [`events.md`](events.md#reprocesar-desde-el-dlt) |

**Reproceso por endpoint interno** (uso de operador; la fase de seguridad exige rol):

```bash
curl -i -X POST http://localhost:8080/internal/v1/nominations/<nomination_id>/reprocess \
  -H 'X-Correlation-Id: ops-reprocess-0001'
```

- `202`: `ABM_TIMEOUT → RECEIVED` + nuevo `nomination.requested` en el outbox (una TX, historial con source
  `OPERATOR`). El circuito normal la reenvía a ABM; ABM es idempotente por `nomination_id`, no se crea otro alta.
- `409 INVALID_STATE_TRANSITION`: ya no está en `ABM_TIMEOUT` (p.ej. ABM respondió tarde). No hay nada que hacer.
- `404 NOMINATION_NOT_FOUND`: el id no existe.

Para un lote (p.ej. después de una caída larga de ABM), iterar sobre
`SELECT id FROM nominations WHERE status = 'ABM_TIMEOUT' AND updated_at > '<inicio del incidente>'`, de a poco y
con el CB cerrado, para no volver a saturar a ABM.

**Republicar desde `nomination.requested.v1-dlt`** no reemplaza al reproceso: el adapter solo envía desde
`RECEIVED`, así que un mensaje republicado de una nominación en `ABM_TIMEOUT` se descarta como reentrega. Sirve solo
para mensajes que nunca cambiaron el estado (poison pill corregido, nominación que no existía por un bug).
