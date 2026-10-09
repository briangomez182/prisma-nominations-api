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
| D12 | Seguridad | **OAuth2 client credentials (JWT)**: `entity_id` sale del token, no del body. mTLS en el gateway. | API key | Aislamiento por entidad: una entidad no puede crear ni consultar nominaciones de otra. En la demo: JWT firmado con clave simétrica. |
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
| `nomination.requested.v1` | `nomination_id` | API (outbox → relay) | ABM Adapter `abm-adapter` | `nomination.requested.v1-dlt` |
| `abm.responses.v1` | `nomination_id` | ABM (en la demo, el simulador) | ABM Response Consumer `abm-response-processor` | `abm.responses.v1-dlt` |
| `nomination.result.v1` | `nomination_id` | API (outbox → relay) | Consumidor de ejemplo `notifications-demo` (+ canales, BI) | `nomination.result.v1-dlt` |

Headers de todo mensaje: `event_id`, `event_type`, `schema_version`, `correlation_id`. Los tópicos y sus DLT (mismas
particiones) los crea la app al arrancar.

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
| `nominations.abm.connect-timeout` / `.read-timeout` | `2s` / `5s` | Timeouts del cliente HTTP de ABM |
| `nominations.abm.adapter.enabled` / `.group-id` | `true` / `abm-adapter` | ABM Adapter: consumer de `nomination.requested.v1` que envía a ABM |
| `nominations.abm.adapter.max-attempts` / `.backoff` | `3` / `1s` | Intentos ante falla técnica de ABM antes del DLT (fase 6: Resilience4j) |
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
  reintenta. Una falla técnica (timeout, conexión, 5xx, 408, 429) se reintenta con backoff y, agotada, va a
  `nomination.requested.v1-dlt` (fase 6: circuit breaker y `ABM_TIMEOUT`). Un 4xx de contrato va directo al DLT.
- **Simulador:** el escenario se elige por el `card_id` (`tok_demo_ok_01` aprueba, `REJECT[_010|_020|_030|_060]`
  rechaza, `DUP` responde dos veces, `SILENT` no responde, `FAIL` da 503, `SLOW` excede el read-timeout). Detalle
  en [`docs/abm-mock.md`](docs/abm-mock.md); contrato de `abm.responses.v1` en [`docs/events.md`](docs/events.md);
  requests de demo en [`docs/requests.http`](docs/requests.http).

Tests: `AbmFlowIntegrationTest` (E4, E5, E7, SILENT, sin doble envío y trazabilidad, todo encendido con servidor
HTTP real), `AbmMockIntegrationTest`, `NominationRequestedListenerIntegrationTest`,
`AbmResponseListenerIntegrationTest`. Los contextos de test con MockMvc (sin servidor HTTP) apagan el ABM Adapter.

## Estructura

```
app/src/main/java/com/prisma/nominations
├── domain/            # Nominación, máquina de estados, reglas. Java puro.
├── application/       # Casos de uso (service) y puertos (port.in / port.out)
└── infrastructure/    # Adaptadores: web, persistence, messaging, abm, config
```

## Cómo correr

Requisitos: Java 21, Maven y Docker.

```bash
# Opción A: infraestructura con docker-compose y la app local
docker compose up -d
cd app && mvn spring-boot:run

# Opción B: sin compose, Postgres y Kafka con Testcontainers
cd app && mvn spring-boot:test-run

# Tests (requieren Docker corriendo)
cd app && mvn verify
```

- API: http://localhost:8080
- Health: http://localhost:8080/actuator/health
- Kafka UI: http://localhost:8081

## API

Contrato completo: [`docs/openapi.yaml`](docs/openapi.yaml) · Swagger UI: http://localhost:8080/swagger-ui.html · Ejemplos: [`docs/requests.http`](docs/requests.http) (IntelliJ HTTP Client).

| Método | Ruta | Respuesta |
|--------|------|-----------|
| `POST` | `/v1/nominations` | **202** + `Location` + `Idempotent-Replayed: true\|false`. Body con `status: RECEIVED` |
| `GET` | `/v1/nominations/{id}` | **200** estado actual (`account_id` y `card_id` enmascarados) |
| `GET` | `/v1/nominations/{id}/history` | **200** transiciones en orden cronológico |

Headers:

- `X-Entity-Id` (obligatorio): entidad que llama. Aísla los datos (otra entidad → 404) y forma parte de la clave de idempotencia `(entity_id, request_id)`. Transitorio: en la fase de seguridad sale del JWT.
- `X-Correlation-Id` (opcional): si falta o no cumple `[A-Za-z0-9._-]{1,64}` se genera. Vuelve siempre en la respuesta.
- `Idempotent-Replayed` (respuesta del POST): `true` si se devolvió una nominación ya existente para el mismo `request_id`.

Errores: RFC 9457 (`application/problem+json`) con `code`, `correlation_id`, `timestamp` y `errors[{field, message}]` (campos en snake_case, nunca con el valor recibido). `type` = `https://api.prisma.example/problems/<code-en-kebab-case>`.

| `code` | HTTP | Cuándo |
|--------|------|--------|
| `VALIDATION_ERROR` | 400 | Campos faltantes o inválidos; `card_id` con forma de PAN |
| `MALFORMED_REQUEST` | 400 | Body que no es JSON válido o campo con tipo incorrecto (p.ej. `request_id` no UUID) |
| `MISSING_HEADER` | 400 | Falta `X-Entity-Id` |
| `INVALID_PARAMETER` | 400 | Parámetro de ruta con formato inválido (`{id}` no UUID) |
| `BAD_REQUEST` | 400 | Otro 400 resuelto por Spring MVC |
| `REQUEST_ERROR` | 4xx | Otro error de cliente sin código específico |
| `NOMINATION_NOT_FOUND` | 404 | La nominación no existe o pertenece a otra entidad |
| `RESOURCE_NOT_FOUND` | 404 | Ruta inexistente |
| `METHOD_NOT_ALLOWED` | 405 | Método HTTP no soportado en la ruta |
| `NOT_ACCEPTABLE` | 406 | `Accept` que la API no puede producir |
| `IDEMPOTENCY_CONFLICT` | 409 | `request_id` ya usado por la entidad con otro contenido |
| `CONCURRENT_MODIFICATION` | 409 | Lock optimista: otra operación modificó la nominación; reintentar |
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
- [ ] Fase 6 — Resiliencia
- [ ] Fase 7 — Seguridad y observabilidad
- [ ] Fase 8 — E2E y entrega
