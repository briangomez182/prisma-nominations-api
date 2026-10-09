# Simulador de ABM

ABM es un sistema externo con procesamiento asincrónico (su respuesta puede demorar minutos). Para la demo y los
tests, la app incluye un **simulador** en el paquete `com.prisma.nominations.abmmock`, que se comporta como otro
sistema: no importa nada del dominio, de la aplicación ni de la infraestructura del servicio de nominaciones, y
solo comparte con él el contrato (HTTP + tópico Kafka).

Se activa con `nominations.abm-mock.enabled=true`. En producción no existe. No figura en OpenAPI/Swagger.

## Contrato

### Alta: `POST /abm-mock/v1/nominations`

Headers: `Content-Type: application/json`, `X-Correlation-Id`.

```json
{
  "nomination_id": "6b1f3c1e-…",
  "request_id": "0c7d…",
  "correlation_id": "c0ffee00-…",
  "entity_id": "ENT01",
  "customer_id": "CUST-000123",
  "account_id": "0001234567890987654",
  "card_id": "tok_demo_ok_01",
  "alias": "CUENTA SUELDO"
}
```

| HTTP | Cuándo | Body |
|------|--------|------|
| `202` | Pedido aceptado | `{"abm_operation_id":"ABM-OP-<uuid>","status":"ACCEPTED"}` |
| `400` | Faltan `nomination_id`, `request_id`, `account_id` o `card_id`, o el JSON es inválido | `{"error":"INVALID_REQUEST"\|"MALFORMED_REQUEST","message":"…"}` |
| `503` | Escenario FAIL | `{"error":"SERVICE_UNAVAILABLE","message":"ABM no disponible"}` |

**Idempotente por `nomination_id`**: repetir el alta devuelve el mismo `abm_operation_id` y no programa una
segunda respuesta.

### Respuesta asincrónica: Kafka `abm.responses.v1`

Luego de `response-delay`. Key = `nomination_id`, header `correlation_id`.

```json
{
  "abm_operation_id": "ABM-OP-…",
  "nomination_id": "6b1f3c1e-…",
  "request_id": "0c7d…",
  "correlation_id": "c0ffee00-…",
  "result": "REJECTED",
  "reason_code": "ABM-051",
  "reason_description": "Cuenta bloqueada",
  "responded_at": "2026-10-09T12:00:02.123Z"
}
```

`reason_code` solo viene en `REJECTED`. En `APPROVED`, `reason_description` es `"Nominación aprobada"`.

## Escenarios

Se eligen por el `card_id` (sin distinguir mayúsculas, "contiene"). Si hay varias marcas gana la primera en
este orden: FAIL, SLOW, SILENT, DUP, REJECT.

| `card_id` contiene | HTTP | Respuesta en Kafka | Caso | Ejemplo para la demo |
|--------------------|------|--------------------|------|----------------------|
| (ninguna marca) | 202 | `APPROVED` | E4 | `tok_demo_ok_01` |
| `REJECT` | 202 | `REJECTED` `ABM-051` Cuenta bloqueada | E5 | `tok_demo_REJECT_01` |
| `REJECT_010` | 202 | `REJECTED` `ABM-010` Cuenta inexistente o inválida | E5 | `tok_demo_REJECT_010` |
| `REJECT_020` | 202 | `REJECTED` `ABM-020` Tarjeta inexistente o inválida | E5 | `tok_demo_REJECT_020` |
| `REJECT_030` | 202 | `REJECTED` `ABM-030` Tarjeta no habilitada para nominación | E5 | `tok_demo_REJECT_030` |
| `REJECT_060` | 202 | `REJECTED` `ABM-060` La cuenta ya está nominada a la tarjeta | E5 | `tok_demo_REJECT_060` |
| `DUP` | 202 | `APPROVED` **dos veces** (idéntica), separadas `duplicate-gap` | E7 | `tok_demo_DUP_01` |
| `SILENT` | 202 | ninguna | E6 (sweeper) | `tok_demo_SILENT_01` |
| `FAIL` | 503 siempre | ninguna | E6 (reintentos, circuit breaker) | `tok_demo_FAIL_01` |
| `SLOW` | 202 recién después de `slow-http-delay` | `APPROVED` | E6 (read-timeout); luego, respuesta tardía | `tok_demo_SLOW_01` |

En SLOW el simulador sí registra el pedido (después de la demora) y aprueba: el cliente ya cortó por timeout,
pero ABM lo procesó. Sirve para mostrar una respuesta que llega tarde.

## Configuración (`nominations.abm-mock.*`)

| Property | Default | Descripción |
|----------|---------|-------------|
| `enabled` | `false` | Activa el simulador |
| `response-delay` | `2s` | Demora de la respuesta por Kafka (en la vida real, minutos) |
| `duplicate-gap` | `300ms` | Separación entre las dos copias en DUP |
| `slow-http-delay` | `10s` | Demora del HTTP en SLOW; debe superar el read-timeout del cliente de ABM |

## Limitaciones

- Registro en memoria: al reiniciar se pierden la idempotencia y las respuestas pendientes (como un ABM que se
  cae; lo cubre el sweeper).
- No reintenta publicar una respuesta que falló: queda logueado en WARN.
- Los logs muestran `account_id` enmascarado (`***************7654`).
