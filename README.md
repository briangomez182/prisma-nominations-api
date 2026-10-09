# prisma-nominations-api

Solución de nominación de cuentas y tarjetas para entidades financieras, creada con Java 21 y Spring Boot 3.
API REST asincrónica, arquitectura hexagonal, procesamiento con Kafka, persistencia transaccional en PostgreSQL,
idempotencia de punta a punta y patrón Transactional Outbox. Integración simulada con ABM.

> Caso ficticio. No contiene datos reales de clientes, tarjetas, cuentas ni credenciales.

## Flujo end to end

```
Canal ──POST /v1/nominations──▶ Nominations API ──(1 TX: nominación + historial + outbox)──▶ PostgreSQL
           ◀── 202 Accepted ───┘                                                              │
                                                                         Outbox relay ◀───────┘
                                                                              │
                                                                              ▼
                                                              Kafka: nomination.requested.v1
                                                                              │
                                                                              ▼
                                                     ABM Adapter (retry · timeout · circuit breaker · DLQ)
                                                                              │ HTTP
                                                                              ▼
                                                                    ABM (asincrónico, minutos)
                                                                              │
                                                              Kafka: abm.responses.v1
                                                                              │
                                                                              ▼
                                ABM Response Consumer (dedup) ──(1 TX: estado + historial + outbox)──▶ PostgreSQL
                                                                              │
                                                                         Outbox relay
                                                                              │
                                                                              ▼
                                                     Kafka: nomination.result.v1 ──▶ Consumidores
```

## Decisiones de arquitectura

| # | Tema | Decisión | Alternativa descartada | Por qué |
|---|------|----------|------------------------|---------|
| D1 | Estilo | **Monolito modular hexagonal** (puertos y adaptadores), un único desplegable | Microservicios separados (API, adapter ABM, consumer) | Menos piezas para operar y demostrar. Cada adaptador está aislado y se puede extraer a su propio servicio sin tocar el dominio. |
| D2 | Respuesta de la API | **202 Accepted** + `Location: /v1/nominations/{id}` + `nomination_id` y `correlation_id` | 201 sincrónico esperando a ABM | ABM tarda minutos: bloquear al consumidor es inviable. La API nunca llama a ABM. |
| D3 | Fuente de verdad | **PostgreSQL**: estado actual + historial solo de inserción + outbox | NoSQL | Transacciones ACID entre estado, historial y evento; `UNIQUE`, `CHECK` y lock optimista en un solo motor. |
| D4 | Idempotencia de ingreso | Clave **(entity_id, request_id)** con `UNIQUE` en la base. Un repetido devuelve la nominación existente, sin reenviar. | Cache con TTL (Redis) | La base garantiza unicidad aun con concurrencia. La clave vive lo mismo que la nominación (sin TTL que pueda expirar antes de un reintento tardío). |
| D5 | Consistencia estado ↔ evento | **Transactional Outbox**: el evento se inserta en la misma transacción que el cambio de estado | Dual write (guardar y publicar) | Si la TX hace rollback no hay evento; si Kafka está caído el evento espera en la tabla (E8). |
| D6 | Relay del outbox | Demo: **polling** con `SELECT … FOR UPDATE SKIP LOCKED`, un evento pendiente por nominación por ciclo (orden) y marca de publicado después del ack (at-least-once). Producción: **Debezium (CDC)** | — | El contrato (tabla outbox) es el mismo; cambiar el relay no toca el dominio. Polling evita levantar Kafka Connect en la demo. |
| D7 | Mensajería | **Kafka**, key = `nomination_id` (orden por nominación). Tópicos `nomination.requested.v1`, `abm.responses.v1`, `nomination.result.v1` + DLT | Cola administrada (SQS, RabbitMQ) | Retención y replay por offset: un consumidor caído retoma donde quedó (E9). Escala por particiones (E10). |
| D8 | Integración ABM | Adapter que consume `nomination.requested.v1` y llama a ABM por HTTP; la respuesta vuelve por `abm.responses.v1` | Callback HTTP directo a la API | Desacopla y absorbe picos. En la demo, ABM es un mock (`nominations.abm-mock.enabled`) que responde con demora configurable: aprueba, rechaza, no responde o duplica. |
| D9 | Idempotencia de retorno ABM | La respuesta trae `nomination_id` + `request_id` + `correlation_id`. Si la nominación ya está en estado final → ACK sin efectos. `@Version` para respuestas simultáneas. | Tabla de dedup aparte | La máquina de estados ya sabe si la respuesta fue procesada (E7). |
| D10 | Rechazo vs falla técnica | **Rechazo funcional** (respuesta de ABM con motivo) → `REJECTED`, no se reintenta. **Falla técnica** (timeout, 5xx, conexión) → reintento con backoff; agotado → DLT + `ABM_TIMEOUT` | Reintentar todo | Reintentar un rechazo funcional no cambia el resultado y duplica carga. Los 4xx de contrato tampoco se reintentan. |
| D11 | Datos sensibles | `card_id` llega **tokenizado**: se rechaza cualquier valor con forma de PAN. `account_id` se expone y loguea **enmascarado**. | Recibir PAN y tokenizar adentro | La plataforma queda fuera del alcance PCI. En producción: cifrado de columna con KMS. |
| D12 | Seguridad | **OAuth2 client credentials (JWT)**: `entity_id` sale del token, no del body. mTLS en el gateway. | API key | Aislamiento por entidad: una entidad no puede crear ni consultar nominaciones de otra. En la demo: JWT HS256 con clave de demo ([Seguridad](#seguridad)). |
| D13 | Trazabilidad | Header `X-Correlation-Id` (se genera si no viene) → MDC → historial → outbox → headers de Kafka | — | Permite reconstruir la operación desde el canal hasta el evento final. |
| D14 | Versionado | API con `/v1` en la URL. Eventos con sufijo `.v1` en el tópico + campo `schema_version`. Solo cambios aditivos dentro de una versión. | Schema Registry (Avro) en la demo | Compatible hacia atrás sin infraestructura extra. Schema Registry queda como evolución para producción. |
| D15 | Concurrencia | **Virtual threads** (Java 21) para la API y los consumers | WebFlux reactivo | Código imperativo simple con alta concurrencia de I/O. |

## Máquina de estados

```
RECEIVED ──▶ PENDING_ABM ──▶ APPROVED | REJECTED          (finales: publican nomination.result)
   │              │
   └──────────────┴──▶ ABM_TIMEOUT ──▶ APPROVED | REJECTED   (respuesta tardía de ABM)
                            └──────▶ RECEIVED               (reproceso controlado)
```

| Estado | Significado | ¿Publica resultado? |
|--------|-------------|---------------------|
| `RECEIVED` | Persistida, pedido a ABM en el outbox | No |
| `PENDING_ABM` | ABM aceptó el pedido, se espera su respuesta | No |
| `APPROVED` | ABM aprobó | Sí |
| `REJECTED` | ABM rechazó por regla funcional; motivo normalizado + código original de ABM | Sí |
| `ABM_TIMEOUT` | Falla técnica: reintentos agotados o sin respuesta dentro del SLA | No: alerta + DLQ. No es final, admite respuesta tardía o reproceso. |

- `RECEIVED → APPROVED/REJECTED` directo es válido: ABM puede responder antes de que el adapter confirme el envío.
- Una respuesta de ABM sobre una nominación ya final devuelve `DUPLICATE` (mismo resultado → ACK sin efectos) o `CONFLICT` (resultado distinto → no se modifica y se alerta).
- `ABM_TIMEOUT` no publica resultado para que el evento siga siendo **único**: si ABM responde tarde, se publica el resultado real.

## Modelo de datos

| Tabla | Rol | Garantías |
|-------|-----|-----------|
| `nominations` | Estado actual, una fila por nominación | `UNIQUE (entity_id, request_id)` · `CHECK` de estados · `CHECK` rechazo ⇔ motivo · `version` (lock optimista) |
| `nomination_history` | Una fila por transición: origen, detalle, correlation_id | Solo inserción: un trigger bloquea `UPDATE` y `DELETE` |
| `outbox_events` | Eventos pendientes de publicar | Índice único parcial: **un solo** `nomination.result` por nominación |
| `consumer_processed_events` | Dedup del consumidor de ejemplo: un `event_id` procesado por consumer group | `PRIMARY KEY (consumer, event_id)` |

Datos sensibles: `card_token` nunca es un PAN (el dominio rechaza 13 a 19 dígitos). `account_id` se persiste porque ABM lo necesita, pero `toString()` y las respuestas lo muestran enmascarado (`****7654`).

Retención: la clave de idempotencia vive lo mismo que la nominación. Las filas publicadas del outbox se purgan periódicamente.

## Mensajería y outbox

Contrato de eventos (payloads, headers, versionado, DLT y reproceso): [`docs/events.md`](docs/events.md).

| Tópico | Key | Productor | Consumidor | DLT |
|--------|-----|-----------|------------|-----|
| `nomination.requested.v1` | `nomination_id` | API (outbox → relay) | ABM Adapter `abm-adapter` | `nomination.requested.v1-dlt` (grupo `abm-adapter-dlt`) |
| `nomination.requested.v1-retry-0` / `-1` / `-2` | `nomination_id` | ABM Adapter (reintento no bloqueante) | ABM Adapter `abm-adapter-retry-0` / `-1` / `-2` | — (agotados → `nomination.requested.v1-dlt`) |
| `abm.responses.v1` | `nomination_id` | ABM (en la demo, el simulador) | ABM Response Consumer `abm-response-processor` | `abm.responses.v1-dlt` |
| `nomination.result.v1` | `nomination_id` | API (outbox → relay) | Consumidor de ejemplo `notifications-demo` (+ canales, BI) | `nomination.result.v1-dlt` |

Headers de todo mensaje: `event_id`, `event_type`, `schema_version`, `correlation_id`. Los tópicos, sus DLT y los
tópicos de retry del ABM Adapter (mismas particiones) los crea la app al arrancar.

**Relay (D6).** Un ciclo programado (`fixed-delay`) toma un lote de `outbox_events` pendientes con
`FOR UPDATE SKIP LOCKED`, así varias instancias corren en paralelo sin tomar la misma fila (E10).

- **Orden por nominación:** en cada ciclo solo entra el evento pendiente **más antiguo de cada nominación**; el
  siguiente espera a que ese se publique. Junto con key = `nomination_id` (misma partición), los consumidores ven
  los eventos de una nominación en orden.
- **Marca después del ack:** la fila se marca `published_at` recién con el ack de Kafka (`acks=all`, productor
  idempotente, espera acotada por `send-timeout`).
- **Corte de lote ante falla:** si un envío falla se registra `attempts` y `last_error` y se corta el lote; el
  ciclo siguiente reintenta. El evento nunca se pierde: Kafka caído solo demora la publicación (E8).
- **At-least-once:** si el proceso cae entre el ack y el commit, el evento se vuelve a publicar. Los consumidores
  deduplican por `event_id` (el de ejemplo, en `consumer_processed_events` en la misma TX que el efecto).
- **Purga:** los eventos publicados se borran pasada la retención, por tandas; los pendientes nunca se borran.

Consumidor de ejemplo (`notifications-demo`): commit de offset por registro después de procesar; error transitorio
→ reintentos con backoff y luego DLT; mensaje imposible de procesar (poison pill) → DLT directo.

| Property | Default | Para qué |
|----------|---------|----------|
| `nominations.kafka.partitions` / `.replication-factor` | `6` / `1` | Particiones (paralelismo de consumers) y réplicas de los tópicos |
| `nominations.outbox.relay.enabled` | `true` | Ciclo programado del relay (`false` en tests o cuando publica Debezium) |
| `nominations.outbox.relay.fixed-delay` | `500ms` | Pausa entre ciclos |
| `nominations.outbox.relay.batch-size` | `100` | Eventos que toma (y bloquea) cada ciclo |
| `nominations.outbox.relay.send-timeout` | `5s` | Espera máxima del ack de Kafka por evento |
| `nominations.outbox.retention` | `7d` | Antigüedad de un evento publicado antes de purgarlo |
| `nominations.outbox.purge.enabled` / `.fixed-delay` / `.initial-delay` | `true` / `1h` / `1m` | Purga del outbox |
| `nominations.demo-consumer.enabled` / `.group-id` | `true` / `notifications-demo` | Consumidor de ejemplo de `nomination.result.v1` |
| `nominations.demo-consumer.max-attempts` / `.backoff` | `3` / `1s` | Reintentos ante error transitorio antes del DLT |
| `nominations.abm.base-url` | `http://localhost:${local.server.port}/abm-mock` | URL de ABM (se resuelve en cada llamada; en la demo, el simulador de la misma app) |
| `nominations.abm.connect-timeout` / `.read-timeout` | `2s` / `2s` | Timeouts del cliente HTTP de ABM (read-timeout < slow-call del CB, 3s) |
| `nominations.abm.adapter.enabled` / `.group-id` | `true` / `abm-adapter` | ABM Adapter: consumer de `nomination.requested.v1` que envía a ABM |
| `nominations.abm.adapter.retry-delays` | `10s,1m,5m` | Capa 2: espera de cada tópico de retry (`-retry-0`, `-1`, `-2`); agotados → DLT + `ABM_TIMEOUT` |
| `nominations.abm.sweeper.enabled` / `.fixed-delay` | `true` / `30s` | Ciclo del sweeper del SLA de ABM |
| `nominations.abm.sweeper.response-sla` / `.batch-size` | `15m` / `100` | `PENDING_ABM` sin respuesta más allá del SLA → `ABM_TIMEOUT` (source `SWEEPER`); tamaño de lote |
| `resilience4j.retry.instances.abm.*` | 3 intentos, 200ms ×2 + jitter 0,5 | Capa 1: retry en proceso, solo sobre `AbmUnavailableException` |
| `resilience4j.circuitbreaker.instances.abm.*` | ventana 20, mín. 10, 50% fallas u 80% lentas (>3s), 30s abierto, 3 en HALF_OPEN | Capa 1: circuit breaker; estado en `/actuator/health` |
| `nominations.abm.response-consumer.enabled` / `.group-id` | `true` / `abm-response-processor` | Consumer de `abm.responses.v1` |
| `nominations.abm.response-consumer.max-attempts` / `.backoff` | `3` / `1s` | Reintentos ante error transitorio antes del DLT |
| `nominations.abm-mock.enabled` | `true` (en `application.yml`) | Simulador de ABM; en producción `false` |
| `nominations.abm-mock.response-delay` / `.duplicate-gap` / `.slow-http-delay` | `2s` / `300ms` / `10s` | Demora de la respuesta, separación de las copias en DUP y demora del HTTP en SLOW |

Tests: `OutboxRelayIntegrationTest` (relay, orden, SKIP LOCKED, fallas de Kafka, purga),
`NominationResultNotifierIntegrationTest` (dedup, DLT, evolución del esquema) y
`NominationEventFlowIntegrationTest` / `NominationEventFlowRelayDownIntegrationTest` (flujo de punta a punta: E1,
E3, E8, E9 y trazabilidad del `correlation_id`).

## Integración con ABM

```
nomination.requested.v1 ─▶ ABM Adapter ──HTTP POST /v1/nominations──▶ ABM ── 202 {abm_operation_id}
                              │ (RECEIVED → PENDING_ABM)                │
                              │                                  (minutos después)
                              ▼                                         ▼
                                              abm.responses.v1 ◀────────┘
                                                     │
                     ABM Response Consumer ◀─────────┘  (1 TX: estado final + historial + nomination.result)
```

- **Envío (ida):** el ABM Adapter consume `nomination.requested.v1` y llama a ABM **fuera de toda transacción**;
  con el 202 pasa la nominación a `PENDING_ABM` en una TX corta. ABM puede responder antes de esa confirmación:
  `RECEIVED → APPROVED/REJECTED` directo es válido.
- **Idempotencia de ida:** solo se envía si la nominación sigue en `RECEIVED` (una reentrega del evento no
  reenvía) y ABM es idempotente por `nomination_id` (mismo `abm_operation_id`, sin segunda alta), lo que cubre
  la ventana de una caída entre el 202 y el commit.
- **Idempotencia de vuelta (E7):** la respuesta trae `nomination_id` + `request_id` + `correlation_id`. Sobre una
  nominación ya final, el mismo resultado es `DUPLICATE` (ACK sin efectos) y uno distinto es `CONFLICT` (no se
  modifica, WARN). El índice único del outbox garantiza **un solo** `nomination.result` por nominación.
- **Rechazo funcional vs falla técnica (D10):** un `REJECTED` de ABM es una respuesta válida: estado `REJECTED`,
  motivo normalizado (`ABM-030` → `CARD_NOT_ELIGIBLE`) y el código original solo en la base para auditoría; no se
  reintenta. Una falla técnica (timeout, conexión, 5xx, 408, 429, circuit breaker abierto) se reintenta en dos
  capas y, agotada, va a `nomination.requested.v1-dlt` y la nominación pasa a `ABM_TIMEOUT`. Un 4xx de contrato
  va directo al DLT. Detalle en [Resiliencia (E6)](#resiliencia-e6).
- **Simulador:** el escenario se elige por el `card_id` (`tok_demo_ok_01` aprueba, `REJECT[_010|_020|_030|_060]`
  rechaza, `DUP` responde dos veces, `SILENT` no responde, `FAIL` da 503, `SLOW` excede el read-timeout). Detalle
  en [`docs/abm-mock.md`](docs/abm-mock.md); contrato de `abm.responses.v1` en [`docs/events.md`](docs/events.md);
  requests de demo en [`docs/requests.http`](docs/requests.http).

Tests: `AbmFlowIntegrationTest` (E4, E5, E7, SILENT, sin doble envío y trazabilidad, todo encendido con servidor
HTTP real), `AbmMockIntegrationTest`, `NominationRequestedListenerIntegrationTest`,
`AbmResponseListenerIntegrationTest`. Los contextos de test con MockMvc (sin servidor HTTP) apagan el ABM Adapter.

## Resiliencia (E6)

ABM puede estar caído, lento o no responder nunca. La recuperación está en **dos capas** más dos mecanismos de cierre:

```
capa 1 (en proceso, ResilientAbmClient)
  nomination.requested ─▶ Retry "abm" (3 intentos) ─▶ CircuitBreaker "abm" ─▶ HTTP (read-timeout 2s) ─▶ ABM
                              │
                              │ AbmUnavailableException (capa 1 agotada o CB abierto)
                              ▼
capa 2 (Kafka, no bloqueante, AbmAdapterConsumerConfig)
  -retry-0 (10s) ─▶ -retry-1 (1m) ─▶ -retry-2 (5m) ─▶ -dlt ─▶ ABM_TIMEOUT (source ABM_ADAPTER)

sweeper
  PENDING_ABM sin respuesta > 15m ─▶ ABM_TIMEOUT (source SWEEPER)

desde ABM_TIMEOUT
  respuesta tardía de ABM ─────────────────────────▶ APPROVED/REJECTED + un único nomination.result
  POST /internal/v1/nominations/{id}/reprocess ────▶ RECEIVED + nuevo nomination.requested
```

| Capa | Qué | Dónde | Tiempos (default) |
|------|-----|-------|-------------------|
| 1 — en proceso | Timeout de lectura/conexión | `AbmHttpClient` | `read-timeout` 2s, `connect-timeout` 2s |
| 1 — en proceso | Retry corto, solo `AbmUnavailableException` | `ResilientAbmClient` (`resilience4j.retry.instances.abm`) | 3 intentos, espera 200ms → 400ms (exponencial, ±50% jitter) |
| 1 — en proceso | Circuit breaker alrededor de cada intento HTTP, dentro del Retry: `Retry(CircuitBreaker(http))` | `ResilientAbmClient` (`resilience4j.circuitbreaker.instances.abm`) | Abre con 50% de fallas u 80% de llamadas lentas (>3s) sobre las últimas 20 (mín. 10); 30s abierto; 3 llamadas de prueba en HALF_OPEN |
| 2 — Kafka | Reintento no bloqueante por tópicos de retry | `AbmAdapterConsumerConfig` (`nominations.abm.adapter.retry-delays`) | `-retry-0` 10s, `-retry-1` 1m, `-retry-2` 5m; luego DLT |
| Cierre | DLT → `ABM_TIMEOUT` (sin `nomination.result`) | `NominationRequestedDltHandler` → `MarkAbmFailureService` | Al agotar la capa 2 (~6 min) o directo si es contrato |
| Cierre | Sweeper del SLA de respuesta | `StaleNominationSweeper` → `SweepStaleNominationsService` | `PENDING_ABM` > `response-sla` (15m), ciclo cada 30s |

**Qué se reintenta y qué no** (D10). La clasificación la hace `AbmHttpClient`; el Retry y la capa 2 solo miran la excepción:

| Situación | Excepción | ¿Reintenta? | Destino |
|-----------|-----------|-------------|---------|
| 5xx, 408, 429 | `AbmUnavailableException` | Sí (capa 1 y capa 2) | Agotado → DLT → `ABM_TIMEOUT` |
| Timeout de lectura o de conexión, conexión rechazada o cortada | `AbmUnavailableException` | Sí (capa 1 y capa 2) | Agotado → DLT → `ABM_TIMEOUT` |
| Circuit breaker abierto (no se llama a ABM) | `AbmUnavailableException` | Capa 1 no (corta en el acto); capa 2 sí | Agotado → DLT → `ABM_TIMEOUT` |
| 4xx de contrato (400, 422, …) o 2xx sin `abm_operation_id` | `AbmContractException` | No (y el CB la ignora) | DLT directo → `ABM_TIMEOUT` ("rechazó el pedido por contrato") |
| Poison pill (JSON inválido, sin `nomination_id`) | `InvalidEventException` | No | DLT directo, solo log ERROR (no hay estado) |
| Nominación inexistente | `NominationNotFoundException` | No | DLT directo, solo log ERROR |
| **Rechazo funcional** de ABM (`REJECTED` por `abm.responses.v1`) | — (no es un error) | No | `REJECTED` + `nomination.result`; el HTTP fue 202 y el CB lo cuenta como éxito |

**Rechazo funcional vs falla técnica.** Un rechazo es una **respuesta** de ABM, por el canal de respuestas, con un
motivo de negocio: reintentar daría el mismo resultado. Una falla técnica es la **ausencia** de respuesta (o un 5xx):
no dice nada sobre la nominación, por eso se reintenta y, agotada, el estado es `ABM_TIMEOUT` ("no sabemos"), no
`REJECTED`.

**Peor caso de bloqueo de la capa 1 (~6,6s).** Un mensaje ocupa al consumer a lo sumo 3 × 2s de read-timeout +
~0,6s de esperas (200ms + 400ms, con jitter hasta ~0,9s). El read-timeout (2s) queda por debajo del umbral de
slow-call (3s) a propósito: una llamada que corta por timeout cuenta como **falla** del CB. Los reintentos largos
(10s, 1m, 5m) **no bloquean la partición**: el mensaje se reenvía a un tópico de retry y la partición principal sigue
con las demás nominaciones (E10); el tópico de retry lo consume recién cuando vence su espera. Si el CB está abierto,
cada intento falla en microsegundos (fail fast) y ABM no recibe carga mientras se recupera; las esperas de la capa 2
(1m, 5m) superan los 30s de CB abierto, así que el reintento siguiente lo encuentra en HALF_OPEN/CLOSED.
Con los defaults, una sola nominación contra un ABM caído ya genera hasta 12 llamadas (4 pasadas × 3 intentos):
pasada la 10ª el CB abre y el resto de las pasadas (de esa y de las demás nominaciones) falla sin llamar a ABM.

**Recuperación.**

- **DLT → `ABM_TIMEOUT`**: agotada la capa 2, el handler del DLT pasa la nominación a `ABM_TIMEOUT` (source
  `ABM_ADAPTER`, detalle "Reintentos agotados: ABM no disponible"). No publica `nomination.result`.
- **Sweeper**: ABM aceptó (`PENDING_ABM`) pero no respondió dentro del SLA → `ABM_TIMEOUT` (source `SWEEPER`).
  Seguro en multi-instancia por lock optimista.
- **Respuesta tardía**: `ABM_TIMEOUT` no es final. Si ABM responde después, se aplica (`ABM_TIMEOUT → APPROVED/REJECTED`)
  y recién ahí se publica el **único** `nomination.result` (escenario SLOW).
- **Reproceso controlado** (operador): `POST /internal/v1/nominations/{id}/reprocess` → `ABM_TIMEOUT → RECEIVED` +
  nuevo `nomination.requested` en una TX; responde 202. Desde otro estado: 409 `INVALID_STATE_TRANSITION`. ABM es
  idempotente por `nomination_id`, así que reenviar nunca crea un segundo alta.
- **Republicar desde el DLT**: ver [`docs/events.md`](docs/events.md#reprocesar-desde-el-dlt). Runbook de
  alertas, diagnóstico y recuperación: [`docs/operations.md`](docs/operations.md).

**Circuit breaker en Actuator.** `GET /actuator/health` → `components.circuitBreakers.details.abm` con `status`
(`UP` / `CIRCUIT_OPEN` / `CIRCUIT_HALF_OPEN`) y `details.state`, `failureRate`, `slowCallRate`, llamadas fallidas y no
permitidas. El CB abierto **no** baja el estado general de la app (ABM caído no es motivo para sacar la instancia
del balanceador). Las transiciones se loguean (ERROR al abrir).

Tests: `ResilientAbmClientTest` / `ResilientAbmClientHttpTest` (capa 1: qué se reintenta, CB, timeout real),
`AbmCircuitBreakerHealthIntegrationTest` (cableado y Actuator), `AbmAdapterRetryIntegrationTest` (capa 2: tópicos de
retry, DLT, contrato, poison pill, partición no bloqueada), `AbmTimeoutRecoveryIntegrationTest` (sweeper, reproceso,
respuesta tardía) y `AbmResilienceIntegrationTest` (E2E con el simulador real y tiempos comprimidos: FAIL, SLOW con
respuesta tardía, SILENT + sweeper + reproceso, apertura y recuperación del CB, rechazo funcional sin reintentos).

## Seguridad

OAuth2 Resource Server con **JWT**. La entidad financiera sale del claim `entity_id` del token, nunca del request: una entidad no puede crear ni ver nominaciones de otra (responde 404, no 403, para no revelar existencia).

| Ruta | Requisito |
|------|-----------|
| `POST /v1/nominations` | scope `nominations:write` + `entity_id` válido |
| `GET /v1/nominations/**` | scope `nominations:read` + `entity_id` válido |
| `/internal/**` (reproceso) | scope `nominations:operate` |
| Actuator (salvo health, info, prometheus) | scope `nominations:operate`; el detalle de `/actuator/health` también |
| `/actuator/health`, `/actuator/info`, `/actuator/prometheus`, Swagger, `/abm-mock/**` | públicos **solo en la demo** (en producción: red interna / puerto de management; el mock no existe) |
| Cualquier otra ruta | denegada |

- 401 y 403 salen en el mismo formato RFC 9457 (`UNAUTHORIZED` / `FORBIDDEN`) con `correlation_id` y `WWW-Authenticate: Bearer`, sin revelar el motivo concreto del rechazo.
- Stateless, sin sesiones ni CSRF (API sin cookies), headers de seguridad por defecto de Spring Security.
- Datos sensibles: `card_id` solo tokenizado (se rechaza un PAN), `account_id` enmascarado en respuestas, eventos públicos y logs; mensajes de error sin valores recibidos.

Token de demo (HS256, `iss=prisma-nominations-demo`, `exp` obligatorio):

```bash
scripts/mint-token.sh ENT01                          # canal: write + read, 1 h
scripts/mint-token.sh "" nominations:operate         # operador
curl -H "Authorization: Bearer $(scripts/mint-token.sh ENT01)" http://localhost:8080/v1/nominations/<id>
```

La clave de demo está en `application.yml` y se sobreescribe con `NOMINATIONS_SECURITY_JWT_SECRET`. **En producción:** `issuer-uri`/JWKS del IdP corporativo (claves asimétricas rotables) validando también `aud`, mTLS en el gateway, rate limiting por entidad en el gateway.

## Observabilidad

Detalle completo, SLIs/SLOs e indicadores que anticipan degradación: [`docs/observability.md`](docs/observability.md). Runbook por alerta: [`docs/operations.md`](docs/operations.md).

| Pilar | Implementación |
|-------|----------------|
| **Métricas** | Micrometer → `/actuator/prometheus`. De negocio vía el puerto `NominationMetrics` (la aplicación no depende de Micrometer): `nominations_received_total`, `nominations_resolved_total{status,reason}`, `nominations_resolution_time_seconds` (histograma end to end), `abm_submissions_total{outcome}`, `abm_responses_total{outcome}`, `nominations_abm_timeouts_total{source}`, `nominations_open{status}`, `outbox_pending` / `outbox_oldest_pending_age_seconds` / `outbox_failing`, `kafka_dlt_messages{topic}`. Más HTTP, Resilience4j, lag de consumers Kafka, Hikari y JVM. Nunca ids por operación como tag. |
| **Trazas** | Micrometer Tracing + OpenTelemetry (W3C `traceparent`) exportando OTLP a Jaeger. Una sola traza de punta a punta, **también a través del outbox**: el `traceparent` se guarda con el evento y el relay lo continúa. |
| **Logs** | `correlation_id` + `trace_id`/`span_id` en cada línea. JSON ECS con `SPRING_PROFILES_ACTIVE=json-logs`. Sin datos sensibles. |
| **Correlación** | `correlation_id` (id de negocio estable: respuesta HTTP, historial, eventos) + `trace_id` (técnico: spans y logs). |
| **Alertas** | 17 reglas Prometheus en [`ops/prometheus/alerts.yml`](ops/prometheus/alerts.yml), cada una con su runbook: circuit breaker abierto, tasa de ABM no disponible, outbox atrasado o fallando, mensajes en DLT, nominaciones trabadas, latencia p95, 5xx, lag de consumers, conflictos de ABM. |
| **Tablero** | Grafana "Nominaciones – visión operativa" ([`ops/grafana/dashboards/nominations.json`](ops/grafana/dashboards/nominations.json)), provisionado automáticamente. |

## Escalamiento (E10)

`E10PeakVolumeIntegrationTest` dispara una ráfaga concurrente de casi 300 POST desde 8 entidades, con reintentos
simultáneos del mismo `request_id`, y corre el relay del outbox como 4 réplicas en paralelo. Verifica que:

- todas las respuestas son 202, sin 5xx, y cada `request_id` da una sola nominación;
- todas llegan a estado final en unos 2 s;
- hay exactamente un `nomination.requested.v1` y un `nomination.result.v1` por nominación, leídos hasta el high
  watermark: `FOR UPDATE SKIP LOCKED` reparte las filas entre réplicas sin publicar nada dos veces;
- el `correlation_id` está en el historial y en los headers de ambos eventos;
- los mensajes se reparten entre las 6 particiones y cada nominación cae en la misma partición en los tres
  tópicos, así que el orden por nominación se conserva.

`scripts/load-test.sh` reproduce el pico en vivo junto con Grafana. En una laptop, 2000 altas con concurrencia 50
dieron p95 de 116 ms y drenaron en menos de 9 s.

**Qué cambia al multiplicar por cien** (de 10k a 1M por día: unas 12 req/s de media, 100–200 req/s en pico):

- La API es stateless y escala horizontalmente detrás del balanceador.
- El relay escala por instancias gracias a SKIP LOCKED. En producción, publicación por lotes (enviar el lote y
  esperar los futures juntos) o Debezium (CDC).
- Los consumidores escalan por particiones: subir a 24–48 particiones, replication factor 3 con `min.insync.replicas=2`,
  y hacer configurable la concurrencia de los listeners (hoy 1 consumer por listener por instancia).
- Pool de Hikari y `batch-size` del relay fijados explícitamente. Purga o particionado por fecha de `outbox_events`
  y `nomination_history`.
- La idempotencia y el orden no dependen de la cantidad de instancias: los garantizan la UNIQUE
  `(entity_id, request_id)`, la deduplicación por `event_id` y la key `nomination_id`.
- El circuit breaker y los reintentos no bloqueantes aíslan la presión sobre ABM.
- La capacidad se vigila con el backlog del outbox, las nominaciones abiertas, el lag de consumers y el p95.

## Estructura

```
app/src/main/java/com/prisma/nominations
├── domain/            # Nominación, máquina de estados, reglas. Java puro.
├── application/       # Casos de uso (service) y puertos (port.in / port.out)
└── infrastructure/    # Adaptadores: web, persistence, messaging, abm, config
```

## Cómo correr

Requisitos: Docker. Para correr la app en el host o los tests, además Java 21 y Maven.

```bash
# Modo A: todo en Docker (app + Postgres + Kafka + observabilidad), con el perfil demo
docker compose up -d --build            # la app queda healthy en ~20 s; APP_PORT=9080 para cambiar el puerto
docker compose logs -f app              # logs JSON (ECS) con correlationId y traceId

# Modo B: app en el host (desarrollo) y el resto en Docker
APP_RUNS_ON=host docker compose up -d postgres kafka kafka-ui jaeger prometheus grafana
cd app && mvn spring-boot:run

# Sin compose: Postgres y Kafka con Testcontainers
cd app && mvn spring-boot:test-run

# Bajar todo (-v borra también los datos)
docker compose down
```

`APP_RUNS_ON=host` solo cambia el target de Prometheus (de `app:8080` a `host.docker.internal:8080`). Si Prometheus
ya estaba levantado en el otro modo: `APP_RUNS_ON=host docker compose up -d prometheus`.

La imagen (`app/Dockerfile`) es multi-stage: build con Maven, runtime JRE 21 Alpine, jar por capas, usuario no root,
`MaxRAMPercentage=75` y healthcheck contra `/actuator/health`.

- API: http://localhost:8080 (con token: ver [Seguridad](#seguridad)) · Swagger: http://localhost:8080/swagger-ui.html
- Health: http://localhost:8080/actuator/health · Métricas: http://localhost:8080/actuator/prometheus
- Kafka UI: http://localhost:8081
- Grafana: http://localhost:3000 (admin/admin; si el puerto está ocupado: `GRAFANA_PORT=3001 docker compose up -d`)
- Prometheus: http://localhost:9090 (alertas en `/alerts`, targets en `/targets`) · Jaeger: http://localhost:16686

## Pruebas

Cada escenario del enunciado (E1–E10) tiene sus tests etiquetados con `@Tag("En")`. Los que levantan Spring y
Testcontainers llevan además `@Tag("integration")` y requieren Docker.

```bash
cd app
mvn test -Dgroups=E6                    # un escenario
mvn test -Dgroups='E4 | E5 | E7'        # varios
mvn test -DexcludedGroups=integration   # solo unitarios, sin Docker
mvn test                                # suite completa (~3-4 min)
```

Matriz escenario → diseño → tests → cómo verlo en la demo: [`docs/scenarios.md`](docs/scenarios.md).

## Demo

Guion de la presentación (10–12 min, qué decir y mostrar en cada escenario, plan B): [`docs/demo.md`](docs/demo.md).

```bash
docker compose up -d --build          # la app corre con el perfil demo (tiempos comprimidos)
scripts/demo.sh                       # E1 → E10 con checklist ✓/✗ al final
scripts/demo.sh E6                    # un solo escenario
DEMO_PAUSE=1 scripts/demo.sh          # Enter entre pasos (modo presentación)
scripts/load-test.sh 2000 50          # pico de volumen para ver en Grafana
```

El perfil `demo` (`application-demo.yml`) comprime los tiempos para mostrarlos en vivo: ABM responde en 3 s, el
sweeper marca timeout a los 30 s y el circuit breaker se reabre a los 10 s. E8 (Kafka caído → outbox) y E9
(consumidor caído → lag y recuperación) apagan servicios del compose; sin compose se marcan como saltados y su
evidencia es `mvn test -Dgroups=E8` / `-Dgroups=E9`.

## API

Contrato completo: [`docs/openapi.yaml`](docs/openapi.yaml) · Swagger UI: http://localhost:8080/swagger-ui.html · Ejemplos: [`docs/requests.http`](docs/requests.http) (IntelliJ HTTP Client).

| Método | Ruta | Respuesta |
|--------|------|-----------|
| `POST` | `/v1/nominations` | **202** + `Location` + `Idempotent-Replayed: true\|false`. Body con `status: RECEIVED` |
| `GET` | `/v1/nominations/{id}` | **200** estado actual (`account_id` y `card_id` enmascarados) |
| `GET` | `/v1/nominations/{id}/history` | **200** transiciones en orden cronológico |
| `POST` | `/internal/v1/nominations/{id}/reprocess` | **202** reproceso de una nominación en `ABM_TIMEOUT` (scope `nominations:operate`; ver [Resiliencia](#resiliencia-e6)) |

Headers:

- `Authorization: Bearer <JWT>` (obligatorio): ver [Seguridad](#seguridad). La entidad sale del claim `entity_id`: aísla los datos (otra entidad → 404) y forma parte de la clave de idempotencia `(entity_id, request_id)`.
- `X-Correlation-Id` (opcional): si falta o no cumple `[A-Za-z0-9._-]{1,64}` se genera. Vuelve siempre en la respuesta.
- `Idempotent-Replayed` (respuesta del POST): `true` si se devolvió una nominación ya existente para el mismo `request_id`.

Errores: RFC 9457 (`application/problem+json`) con `code`, `correlation_id`, `timestamp` y `errors[{field, message}]` (campos en snake_case, nunca con el valor recibido). `type` = `https://api.prisma.example/problems/<code-en-kebab-case>`.

| `code` | HTTP | Cuándo |
|--------|------|--------|
| `VALIDATION_ERROR` | 400 | Campos faltantes o inválidos; `card_id` con forma de PAN |
| `MALFORMED_REQUEST` | 400 | Body que no es JSON válido o campo con tipo incorrecto (p.ej. `request_id` no UUID) |
| `MISSING_HEADER` | 400 | Falta un header obligatorio (la API pública ya no lo emite: la entidad sale del JWT) |
| `UNAUTHORIZED` | 401 | Sin token, o token vencido, mal firmado o de otro emisor (`WWW-Authenticate: Bearer`) |
| `FORBIDDEN` | 403 | Token válido sin el scope requerido o sin `entity_id` válido |
| `INVALID_PARAMETER` | 400 | Parámetro de ruta con formato inválido (`{id}` no UUID) |
| `BAD_REQUEST` | 400 | Otro 400 resuelto por Spring MVC |
| `REQUEST_ERROR` | 4xx | Otro error de cliente sin código específico |
| `NOMINATION_NOT_FOUND` | 404 | La nominación no existe o pertenece a otra entidad |
| `RESOURCE_NOT_FOUND` | 404 | Ruta inexistente |
| `METHOD_NOT_ALLOWED` | 405 | Método HTTP no soportado en la ruta |
| `NOT_ACCEPTABLE` | 406 | `Accept` que la API no puede producir |
| `IDEMPOTENCY_CONFLICT` | 409 | `request_id` ya usado por la entidad con otro contenido |
| `CONCURRENT_MODIFICATION` | 409 | Lock optimista: otra operación modificó la nominación; reintentar |
| `INVALID_STATE_TRANSITION` | 409 | Reproceso de una nominación que no está en `ABM_TIMEOUT` |
| `PAYLOAD_TOO_LARGE` | 413 | Solicitud que excede el tamaño permitido |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | `Content-Type` distinto de JSON |
| `SERVICE_UNAVAILABLE` | 503 | Servicio no disponible temporalmente; reintentar |
| `INTERNAL_ERROR` | 500 | Error inesperado (se loguea con stack; la respuesta no expone detalles) |

`docs/openapi.yaml` se regenera desde el test de integración (requiere Docker):
`cd app && mvn test -Dtest='NominationApiIntegrationTest*'` (también se actualiza con cualquier `mvn test`).

## Avance

- [x] Fase 1 — Bootstrap y decisiones
- [x] Fase 2 — Dominio y base
- [x] Fase 3 — API
- [x] Fase 4 — Outbox + Kafka
- [x] Fase 5 — ABM mock + respuesta
- [x] Fase 6 — Resiliencia
- [x] Fase 7 — Seguridad y observabilidad
- [x] Fase 8 — E2E y entrega
