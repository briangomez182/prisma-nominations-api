# Runbook de operación

Guía breve para operar la integración con ABM: qué alertar, cómo diagnosticar una nominación con su correlation id y
cómo recuperarla. Contexto de diseño en el [README](../README.md#resiliencia-e6) y contrato de eventos en
[`events.md`](events.md). Las métricas y alertas formales llegan con la fase de observabilidad; mientras tanto, cada
señal se puede obtener con las consultas y endpoints de abajo.

## Qué alertar

| Señal | Cómo se ve | Umbral sugerido | Qué significa |
|-------|------------|-----------------|---------------|
| Circuit breaker de ABM abierto | `GET /actuator/health` → `components.circuitBreakers.details.abm.details.state` = `OPEN` (`status` = `CIRCUIT_OPEN`); log ERROR `Circuit breaker de ABM CLOSED -> OPEN` | Cualquier apertura; crítica si dura > 5 min | ABM caído o muy lento. Los pedidos se acumulan en los tópicos de retry sin cargar a ABM |
| Mensajes en `nomination.requested.v1-dlt` | Lag / end offset del tópico (Kafka UI, `kafka-consumer-groups`); log WARN `Nominación pasada a ABM_TIMEOUT` | > 0 en 5 min | Nominaciones que agotaron la recuperación automática (o error de contrato) |
| Mensajes en `abm.responses.v1-dlt` / `nomination.result.v1-dlt` | End offset del tópico | > 0 | Respuesta de ABM inválida o consumidor con error persistente |
| Nominaciones en `ABM_TIMEOUT` | `SELECT count(*) FROM nominations WHERE status = 'ABM_TIMEOUT'` | > 0 sostenido | Esperan respuesta tardía o reproceso |
| `PENDING_ABM` viejas | `SELECT count(*) FROM nominations WHERE status = 'PENDING_ABM' AND updated_at < now() - interval '15 minutes'` | > 0 durante más de 2 ciclos del sweeper | El sweeper no está corriendo (o está fallando: log ERROR `Sweeper: falló el ciclo`) |
| `RECEIVED` viejas | `SELECT count(*) FROM nominations WHERE status = 'RECEIVED' AND updated_at < now() - interval '10 minutes'` | > 0 | Normal mientras la capa 2 reintenta (hasta ~6 min); más allá, el adapter o el relay no avanzan |
| Outbox con reintentos | `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND attempts >= 5` | > 0 | Kafka no acepta publicaciones (E8); ver `last_error` |
| Respuesta de ABM en conflicto | log WARN `Respuesta de ABM contradictoria, se ignora` | Cualquiera | ABM respondió dos resultados distintos para la misma nominación |

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
   "Sin respuesta de ABM dentro del SLA (…)". También: `GET /v1/nominations/{id}/history` con su `X-Entity-Id`.
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
4. **Circuit breaker**: `GET /actuator/health` (estado, `failureRate`, `slowCallRate`, `notPermittedCalls`).

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
