# Contrato de eventos (AsyncAPI-lite)

Eventos que publica el servicio de nominaciones. Se escriben en el **outbox** (`outbox_events`) en la misma
transacción que el cambio de estado y un relay los publica en Kafka (D6, D7). Este documento es el contrato
para los consumidores; los cambios dentro de una versión son solo aditivos (D14). También documenta el tópico de
entrada `abm.responses.v1`, cuyo contrato fija ABM.

## Tópicos

| Tópico | Productor | Consumidores | Visibilidad | DLT |
|--------|-----------|--------------|-------------|-----|
| `nomination.requested.v1` | API de nominaciones (outbox) | ABM Adapter (grupo `abm-adapter`) | Interno (lleva `account_id` completo) | `nomination.requested.v1-dlt` |
| `abm.responses.v1` | ABM (en la demo, el simulador `abm-mock`) | API de nominaciones (grupo `abm-response-processor`) | Interno | `abm.responses.v1-dlt` |
| `nomination.result.v1` | API de nominaciones (outbox) | Notificaciones, canales, BI (ejemplo: grupo `notifications-demo`) | Público (datos enmascarados) | `nomination.result.v1-dlt` |

- **Particiones:** `nominations.kafka.partitions` (6 por defecto). Es la unidad de paralelismo de cada consumer group.
- **Replicación:** `nominations.kafka.replication-factor` (1 local; en producción ≥ 3 con `min.insync.replicas=2`).
- **DLT:** convención de Spring Kafka, `<tópico>-dlt`, con la **misma cantidad de particiones** que el original
  (el mensaje fallido va a la misma partición). Los crea `KafkaTopicsConfig` al arrancar.
- **Tópicos de retry (solo ABM Adapter):** `nomination.requested.v1-retry-0`, `-retry-1`, `-retry-2` (uno por valor
  de `nominations.abm.adapter.retry-delays`), mismas particiones, internos. Los crea Spring Kafka al arrancar.
  Grupos: `abm-adapter-retry-0` / `-1` / `-2` y `abm-adapter-dlt` para el DLT. Ver
  [ABM Adapter: reintentos no bloqueantes](#abm-adapter-reintentos-no-bloqueantes-y-dlt).

## Mensaje (eventos del servicio)

| Parte | Valor |
|-------|-------|
| **Key** | `nomination_id` (UUID como texto). Todos los eventos de una nominación van a la misma partición: orden garantizado por nominación. |
| **Value** | JSON UTF-8, `snake_case`. Los campos nulos se omiten. |
| **Header `event_id`** | UUID del evento (= id de la fila del outbox). Clave de deduplicación. |
| **Header `event_type`** | `nomination.requested` o `nomination.result`. |
| **Header `schema_version`** | Versión del payload dentro del tópico, como texto (`"1"`). |
| **Header `correlation_id`** | El de la request HTTP original (`X-Correlation-Id`), para trazar de punta a punta (D13). |

Los headers repiten `event_id`, `event_type` y `schema_version` del payload para poder enrutar o deduplicar sin
parsear el cuerpo. Si un header falta, el consumidor usa el campo del payload.

## `nomination.requested` (v1)

Pedido de alta hacia ABM. Interno: lleva `account_id` completo porque ABM lo necesita (en producción, con ACL y
cifrado en tránsito). `card_id` es siempre un **token**, nunca el PAN.

| Campo | Tipo | Obligatorio | Descripción |
|-------|------|-------------|-------------|
| `event_id` | UUID | sí | Identificador único del evento |
| `event_type` | string | sí | `nomination.requested` |
| `schema_version` | int | sí | `1` |
| `nomination_id` | UUID | sí | Nominación (también es la key) |
| `entity_id` | string | sí | Entidad financiera |
| `request_id` | UUID | sí | Idempotency key del canal |
| `customer_id` | string | sí | Cliente |
| `account_id` | string | sí | Cuenta completa (CBU/CVU) |
| `card_id` | string | sí | Token de la tarjeta |
| `alias` | string | no | Alias de la cuenta |
| `correlation_id` | string | sí | Correlation id |
| `occurred_at` | ISO-8601 UTC | sí | Momento del evento |

```json
{
  "event_id": "8f1d2c3b-4a5e-4f60-9b7a-1c2d3e4f5a6b",
  "event_type": "nomination.requested",
  "schema_version": 1,
  "nomination_id": "3b9e7c1a-2f4d-4e8b-a6c5-0d1e2f3a4b5c",
  "entity_id": "0072",
  "request_id": "6a7b8c9d-0e1f-4a2b-8c3d-4e5f6a7b8c9d",
  "customer_id": "CUST-000123",
  "account_id": "0720000088000037654321",
  "card_id": "tok_4f9a2c7b8d1e",
  "alias": "CUENTA SUELDO",
  "correlation_id": "web-2f9c1a7e",
  "occurred_at": "2026-10-09T12:00:00.123Z"
}
```

### Consumidor: ABM Adapter (grupo `abm-adapter`)

Lee `nomination_id` (payload, o la key como respaldo) y `correlation_id`; el resto del payload (incluido
`account_id`) ni se lee ni se loguea: el adapter carga la nominación de la base. Solo llama a ABM si la nominación
sigue en `RECEIVED` (una reentrega no genera un segundo envío) y ABM es idempotente por `nomination_id`. Después
pasa la nominación a `PENDING_ABM`. Ver la política de errores en [Errores y Dead Letter Topics](#errores-y-dead-letter-topics).

## `nomination.result` (v1)

Resultado **final** de una nominación (`APPROVED` o `REJECTED`). Se publica **uno solo** por nominación (índice
único parcial en `outbox_events`). Sin datos sensibles: cuenta y tarjeta enmascaradas, motivo normalizado (nunca el
código propio de ABM). `ABM_TIMEOUT` no publica resultado: si ABM responde tarde, se publica el resultado real.

| Campo | Tipo | Obligatorio | Descripción |
|-------|------|-------------|-------------|
| `event_id` | UUID | sí | Identificador único del evento |
| `event_type` | string | sí | `nomination.result` |
| `schema_version` | int | sí | `1` |
| `nomination_id` | UUID | sí | Nominación (también es la key) |
| `entity_id` | string | sí | Entidad financiera |
| `request_id` | UUID | sí | Idempotency key del canal |
| `status` | enum | sí | `APPROVED` \| `REJECTED` |
| `rejection_reason` | enum | solo si `REJECTED` | `INVALID_ACCOUNT` \| `INVALID_CARD` \| `CARD_NOT_ELIGIBLE` \| `ACCOUNT_BLOCKED` \| `ALREADY_NOMINATED` \| `OTHER` |
| `account_id` | string | sí | Cuenta **enmascarada** (`****7654`) |
| `card_id` | string | sí | Token **enmascarado** (`****8d1e`) |
| `correlation_id` | string | sí | Correlation id |
| `occurred_at` | ISO-8601 UTC | sí | Momento del evento |

```json
{
  "event_id": "c4d5e6f7-0819-4a2b-9c3d-5e6f7a8b9c0d",
  "event_type": "nomination.result",
  "schema_version": 1,
  "nomination_id": "3b9e7c1a-2f4d-4e8b-a6c5-0d1e2f3a4b5c",
  "entity_id": "0072",
  "request_id": "6a7b8c9d-0e1f-4a2b-8c3d-4e5f6a7b8c9d",
  "status": "REJECTED",
  "rejection_reason": "INVALID_CARD",
  "account_id": "****7654",
  "card_id": "****8d1e",
  "correlation_id": "web-2f9c1a7e",
  "occurred_at": "2026-10-09T12:00:04.512Z"
}
```

## `abm.responses.v1` (entrada, contrato de ABM)

Respuesta asincrónica de ABM a un alta aceptada (`POST {nominations.abm.base-url}/v1/nominations` → 202). La
publica ABM, no este servicio: los nombres de campos son los de ABM. Lo consume `AbmResponseListener` (grupo
`abm-response-processor`), que aplica estado + historial + `nomination.result` en el outbox en **una** transacción.

| Parte | Valor |
|-------|-------|
| **Key** | `nomination_id` |
| **Value** | JSON UTF-8, `snake_case` |
| **Header `correlation_id`** | El que viajó en el pedido. Si falta, se usa el del payload. |

No trae `event_id`: la deduplicación la da la máquina de estados (D9, ver abajo).

| Campo | Tipo | Obligatorio | Descripción |
|-------|------|-------------|-------------|
| `abm_operation_id` | string | no | Id de la operación en ABM (el mismo que devolvió el 202). Solo se loguea. |
| `nomination_id` | UUID | sí | Nominación |
| `request_id` | UUID | sí | Debe coincidir con el de la nominación; si no, se trata como respuesta ajena → DLT |
| `correlation_id` | string | no | Respaldo del header |
| `result` | enum | sí | `APPROVED` \| `REJECTED` (sin distinguir mayúsculas) |
| `reason_code` | string | solo si `REJECTED` | Código propio de ABM |
| `reason_description` | string | no | Texto de ABM; no se usa |
| `responded_at` | ISO-8601 UTC | no | Momento de la respuesta en ABM |

```json
{
  "abm_operation_id": "ABM-OP-1f0c2d3e-…",
  "nomination_id": "3b9e7c1a-2f4d-4e8b-a6c5-0d1e2f3a4b5c",
  "request_id": "6a7b8c9d-0e1f-4a2b-8c3d-4e5f6a7b8c9d",
  "correlation_id": "web-2f9c1a7e",
  "result": "REJECTED",
  "reason_code": "ABM-030",
  "reason_description": "Tarjeta no habilitada para nominación",
  "responded_at": "2026-10-09T12:00:04.123Z"
}
```

**Normalización del motivo.** El código de ABM no sale del adapter: se guarda en `nominations.abm_reason_code`
(solo auditoría) y la API y `nomination.result` exponen únicamente `rejection_reason`.

| `reason_code` (ABM) | Significado en ABM | `rejection_reason` |
|---------------------|--------------------|--------------------|
| `ABM-010` | Cuenta inexistente o inválida | `INVALID_ACCOUNT` |
| `ABM-020` | Tarjeta inexistente o inválida | `INVALID_CARD` |
| `ABM-030` | Tarjeta no habilitada para nominación | `CARD_NOT_ELIGIBLE` |
| `ABM-051` | Cuenta bloqueada | `ACCOUNT_BLOCKED` |
| `ABM-060` | La cuenta ya está nominada a la tarjeta | `ALREADY_NOMINATED` |
| otro o ausente | — | `OTHER` (WARN para sumarlo a la tabla) |

**Idempotencia (E7, D9).** Si la nominación ya está en un estado final, una respuesta con el mismo resultado es
`DUPLICATE` (ACK sin efectos: ni historial ni evento) y una con resultado distinto es `CONFLICT` (no se modifica,
WARN). Dos respuestas simultáneas: la segunda pierde por lock optimista o por el índice único de `nomination.result`
en el outbox, y se reevalúa como `DUPLICATE`. Una respuesta puede llegar antes de que el adapter confirme el envío:
`RECEIVED → APPROVED/REJECTED` directo es válido.

## Garantías de entrega

- **At-least-once.** El outbox garantiza que ningún evento se pierde si la transacción confirmó, pero un evento
  puede publicarse más de una vez (caída del relay entre el envío y la marca de publicado, ack que excede
  `nominations.outbox.relay.send-timeout` aunque el broker lo haya aceptado, reentrega tras un rebalanceo).
  **El consumidor debe deduplicar por `event_id`.**
- **Orden por nominación**, no global: misma key → misma partición, y el relay publica un solo evento pendiente por
  nominación a la vez (el siguiente espera a que el anterior se publique). Entre nominaciones distintas no hay orden.
- **Desacople (E9).** Si un consumidor está caído, los eventos quedan en el tópico (retención) y al volver
  retoma desde su último offset confirmado. La API sigue aceptando nominaciones sin depender de él.

### Deduplicación recomendada (consumidor de referencia: `NominationResultNotifier`)

1. Leer `event_id` del header (fallback al payload).
2. En **una transacción**: `INSERT INTO consumer_processed_events (consumer, event_id, processed_at) ... ON CONFLICT DO NOTHING`
   y el efecto. Si el insert no afecta filas, es un duplicado: no hacer nada y confirmar el offset.
3. Confirmar el offset después de procesar (`enable-auto-commit: false`, ack por registro).

La tabla de dedup se puede purgar más allá de la retención del tópico: pasado ese plazo ya no puede llegar un duplicado.

## Versionado y evolución

- **Dentro de `.v1` solo cambios aditivos:** campos nuevos opcionales o valores nuevos que el consumidor pueda
  ignorar. `schema_version` sube (2, 3…) para indicar qué se agregó, pero el payload sigue siendo legible por un
  consumidor v1.
- **Consumidores tolerantes (tolerant reader):** ignorar campos desconocidos y leer solo lo que necesitan. Si
  `schema_version` es mayor a la que conocen, procesar igual (la compatibilidad la garantiza el tópico) y avisar
  para actualizarse. Si falta `schema_version`, asumir `1`. Un `event_type` desconocido en el tópico se ignora.
- **Cambio incompatible** (quitar o renombrar un campo, cambiar su tipo o semántica): tópico nuevo
  **`nomination.result.v2`** publicado **en paralelo** con `.v1` hasta que todos los consumidores migren; luego se
  deja de publicar `.v1`.
- Evolución prevista para producción: Schema Registry (Avro/JSON Schema) con compatibilidad `BACKWARD` para
  validar estas reglas en el pipeline en lugar de por convención.

## Errores y Dead Letter Topics

Política del consumidor de referencia (`KafkaConsumerConfig`):

| Error | Tratamiento |
|-------|-------------|
| Transitorio (base caída, timeout) | 3 intentos con backoff fijo (`nominations.demo-consumer.max-attempts`, `.backoff`); agotados → DLT |
| Poison pill (JSON inválido, falta `event_id` / `nomination_id` / `status`) | Directo al DLT, sin reintentos |

ABM Adapter, `nomination.requested.v1` → `nomination.requested.v1-dlt` (`AbmAdapterConsumerConfig`):

| Error | Tratamiento |
|-------|-------------|
| Falla técnica de ABM (timeout, conexión, 5xx, 408, 429, circuit breaker abierto) u otro error inesperado | Capa 1 en proceso (Resilience4j: 3 intentos + CB) y luego capa 2: tópicos de retry no bloqueantes (10s, 1m, 5m); agotados → DLT → `ABM_TIMEOUT` |
| Rechazo de contrato de ABM (resto de 4xx, 2xx sin `abm_operation_id`) | Directo al DLT → `ABM_TIMEOUT` ("ABM rechazó el pedido por contrato") |
| Poison pill (JSON inválido, sin `nomination_id`) o nominación inexistente | Directo al DLT, solo log ERROR |

Detalle en [ABM Adapter: reintentos no bloqueantes y DLT](#abm-adapter-reintentos-no-bloqueantes-y-dlt).

Consumer de respuestas, `abm.responses.v1` → `abm.responses.v1-dlt` (`AbmResponseConsumerConfig`):

| Error | Tratamiento |
|-------|-------------|
| Transitorio (base caída) | `nominations.abm.response-consumer.max-attempts` intentos con backoff fijo; agotados → DLT |
| Poison pill (JSON inválido, sin `nomination_id` / `request_id` / `result`, `result` desconocido) | Directo al DLT |
| Nominación inexistente o `request_id` ajeno; transición inválida | Directo al DLT |

Un rechazo funcional de ABM (`REJECTED`) **no** es un error: se aplica como cualquier respuesta (D10).

El mensaje llega al DLT **sin modificar** (misma key, mismo valor, headers originales) más los headers de
diagnóstico de Spring Kafka. El offset del original avanza: un mensaje roto no bloquea la partición. Los nombres de
esos headers dependen de cómo se armó el reintento:

| DLT | Mecanismo | Headers de diagnóstico |
|-----|-----------|------------------------|
| `nomination.result.v1-dlt`, `abm.responses.v1-dlt` | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` (reintento bloqueante) | `kafka_dlt-exception-fqcn`, `kafka_dlt-exception-cause-fqcn`, `kafka_dlt-exception-message`, `kafka_dlt-exception-stacktrace`, `kafka_dlt-original-topic`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset`, `kafka_dlt-original-timestamp`, `kafka_dlt-original-consumer-group` |
| `nomination.requested.v1-dlt` | Tópicos de retry (`RetryTopicConfiguration`) | **Sin** el prefijo `dlt-`: `kafka_exception-fqcn`, `kafka_exception-cause-fqcn`, `kafka_exception-message`, `kafka_exception-stacktrace`, `kafka_original-topic`, `kafka_original-partition`, `kafka_original-offset`, `kafka_original-timestamp`, `kafka_original-timestamp-type`; además `retry_topic-attempts`, `retry_topic-original-timestamp` y `retry_topic-backoff-timestamp`. Excepción: el grupo sigue siendo `kafka_dlt-original-consumer-group` |

En `nomination.requested.v1-dlt`, `kafka_original-topic` es el tópico **principal** (`nomination.requested.v1`), no
el último de retry; `kafka_exception-fqcn` es siempre `ListenerExecutionFailedException`, y lo que importa es
`kafka_exception-cause-fqcn`, la causa raíz que usa el handler del DLT para decidir (`AbmUnavailableException`,
`AbmContractException`, `InvalidEventException`, `NominationNotFoundException`). Ejemplo real con el CB abierto:
`kafka_exception-message: Listener failed; ABM no disponible: circuit breaker abierto (estado OPEN)`.

### ABM Adapter: reintentos no bloqueantes y DLT

```
nomination.requested.v1 ──falla técnica──▶ -retry-0 (10s) ──▶ -retry-1 (1m) ──▶ -retry-2 (5m) ──▶ -dlt
        └──── contrato / poison pill / nominación inexistente (sin reintentos) ──────────────────────▲
```

- Cada pasada (principal y cada retry) llama a ABM a través de la **capa 1** (`ResilientAbmClient`: hasta 3 intentos
  con backoff corto, dentro de un circuit breaker). Solo si la capa 1 se agota (o el CB está abierto) el mensaje
  pasa al tópico de retry siguiente.
- El reenvío conserva key (`nomination_id`), valor y headers (`event_id`, `correlation_id`) y va a la misma
  partición. El tópico de retry no consume el mensaje hasta que vence su espera (la partición de ese tópico se pausa;
  la del principal sigue avanzando: un ABM caído no frena las demás nominaciones, E10).
- **DLT**: lo consume `NominationRequestedDltHandler` (grupo `abm-adapter-dlt`) según `kafka_exception-cause-fqcn`:
  falla técnica agotada → `ABM_TIMEOUT` con "Reintentos agotados: ABM no disponible"; contrato → `ABM_TIMEOUT` con
  "ABM rechazó el pedido por contrato"; poison pill o nominación inexistente → solo log ERROR. Si ABM ya respondió
  mientras tanto, no se toca nada. `ABM_TIMEOUT` **no** publica `nomination.result`.
- Si el handler del DLT falla, se loguea y el mensaje queda en el DLT (sin bucle de republicación).

### Reprocesar desde el DLT

**Nominaciones en `ABM_TIMEOUT` (DLT de `nomination.requested.v1`).** La vía preferida no es republicar el mensaje
sino el endpoint de operación: `POST /internal/v1/nominations/{id}/reprocess` (202). Pasa la nominación a
`RECEIVED` y encola un `nomination.requested` **nuevo** (nuevo `event_id`) en el outbox, en una TX: queda
auditado en el historial (source `OPERATOR`) y respeta la máquina de estados (409 `INVALID_STATE_TRANSITION` si
ya no está en `ABM_TIMEOUT`, p.ej. porque ABM respondió tarde). Republicar a mano el mensaje del DLT a
`nomination.requested.v1` **no** sirve para una nominación en `ABM_TIMEOUT`: el adapter solo envía desde `RECEIVED`
y lo descarta como reentrega. Runbook completo en [`operations.md`](operations.md).

**Resto de los DLT** (`nomination.result.v1-dlt`, `abm.responses.v1-dlt`):

1. Diagnosticar con los headers `kafka_dlt-*` y corregir la causa (bug del consumidor, dato, infraestructura).
2. Volver a publicar los mensajes del DLT en el tópico original **con sus headers** (en especial `event_id`):
   como el consumidor deduplica por `event_id`, reprocesar es seguro aunque alguno ya se hubiera aplicado.
   Ejemplo local con `kcat`:
   ```bash
   kcat -b localhost:9092 -C -t nomination.result.v1-dlt -e -J \
     | jq -c '{key, payload, headers}' # revisar y luego producir a nomination.result.v1 con -k y -H
   ```
3. Alternativa para un consumidor que se equivocó en masa: resetear el offset de su grupo
   (`kafka-consumer-groups --reset-offsets --to-datetime ... --group notifications-demo --topic nomination.result.v1`).
   También es seguro por la deduplicación.
