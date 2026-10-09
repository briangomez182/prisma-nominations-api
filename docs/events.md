# Contrato de eventos (AsyncAPI-lite)

Eventos que publica el servicio de nominaciones. Se escriben en el **outbox** (`outbox_events`) en la misma
transacción que el cambio de estado y un relay los publica en Kafka (D6, D7). Este documento es el contrato
para los consumidores; los cambios dentro de una versión son solo aditivos (D14).

## Tópicos

| Tópico | Productor | Consumidores | Visibilidad | DLT |
|--------|-----------|--------------|-------------|-----|
| `nomination.requested.v1` | API de nominaciones (outbox) | ABM Adapter | Interno (lleva `account_id` completo) | `nomination.requested.v1-dlt` |
| `abm.responses.v1` | ABM Adapter | API de nominaciones | Interno | `abm.responses.v1-dlt` |
| `nomination.result.v1` | API de nominaciones (outbox) | Notificaciones, canales, BI | Público (datos enmascarados) | `nomination.result.v1-dlt` |

- **Particiones:** `nominations.kafka.partitions` (6 por defecto). Es la unidad de paralelismo de cada consumer group.
- **Replicación:** `nominations.kafka.replication-factor` (1 local; en producción ≥ 3 con `min.insync.replicas=2`).
- **DLT:** convención de Spring Kafka, `<tópico>-dlt`, con la **misma cantidad de particiones** que el original
  (el mensaje fallido va a la misma partición). Los crea `KafkaTopicsConfig` al arrancar.

## Mensaje

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

El mensaje llega al DLT **sin modificar** (misma key, mismo valor, headers originales) más los headers de
diagnóstico de Spring Kafka (`kafka_dlt-exception-fqcn`, `kafka_dlt-exception-message`,
`kafka_dlt-original-topic`, `kafka_dlt-original-partition`, `kafka_dlt-original-offset`, …). El offset del
original avanza: un mensaje roto no bloquea la partición.

### Reprocesar desde el DLT

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
