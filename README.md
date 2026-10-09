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
| D6 | Relay del outbox | Demo: **polling** con `SELECT … FOR UPDATE SKIP LOCKED`. Producción: **Debezium (CDC)** | — | El contrato (tabla outbox) es el mismo; cambiar el relay no toca el dominio. Polling evita levantar Kafka Connect en la demo. |
| D7 | Mensajería | **Kafka**, key = `nomination_id` (orden por nominación). Tópicos `nomination.requested.v1`, `abm.responses.v1`, `nomination.result.v1` + DLT | Cola administrada (SQS, RabbitMQ) | Retención y replay por offset: un consumidor caído retoma donde quedó (E9). Escala por particiones (E10). |
| D8 | Integración ABM | Adapter que consume `nomination.requested.v1` y llama a ABM por HTTP; la respuesta vuelve por `abm.responses.v1` | Callback HTTP directo a la API | Desacopla y absorbe picos. En la demo, ABM es un mock (perfil `abm-mock`) que responde con demora configurable: aprueba, rechaza, no responde o duplica. |
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

Datos sensibles: `card_token` nunca es un PAN (el dominio rechaza 13 a 19 dígitos). `account_id` se persiste porque ABM lo necesita, pero `toString()` y las respuestas lo muestran enmascarado (`****7654`).

Retención: la clave de idempotencia vive lo mismo que la nominación. Las filas publicadas del outbox se purgan periódicamente.

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

## Avance

- [x] Fase 1 — Bootstrap y decisiones
- [x] Fase 2 — Dominio y base
- [ ] Fase 3 — API
- [ ] Fase 4 — Outbox + Kafka
- [ ] Fase 5 — ABM mock + respuesta
- [ ] Fase 6 — Resiliencia
- [ ] Fase 7 — Seguridad y observabilidad
- [ ] Fase 8 — E2E y entrega
