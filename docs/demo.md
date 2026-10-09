# Guion de la demo en vivo

Demo de 10–12 minutos de los escenarios obligatorios E1–E10 para la presentación ante el panel técnico. El
script [`scripts/demo.sh`](../scripts/demo.sh) ejecuta cada paso, muestra request, status y lo relevante de la
respuesta, y cierra con un checklist por escenario. Este documento dice **qué digo, qué ejecuto y qué muestro** en
cada paso, y qué preguntas puede disparar.

Evidencia automatizada de cada escenario (tests): [`scenarios.md`](scenarios.md). Decisiones de diseño (D1–D15):
[README](../README.md#decisiones-de-arquitectura).

## Resumen

| # | Escenario | Duración en vivo | Pantalla principal | Decisión que muestra |
|---|-----------|------------------|--------------------|----------------------|
| — | Arquitectura en 1 diagrama | 1 min | README (flujo end to end) | D1, D2, D5 |
| E1 | Alta válida | 45 s | Terminal + Swagger | D2 (202), D12 (entidad del token) |
| E2 | Datos faltantes | 30 s | Terminal | Validación en el borde, D11 (PAN) |
| E3 | Repetida | 1 min | Terminal | D4 (UNIQUE en la base) |
| E4 | ABM aprueba | 1 min | Terminal + Kafka UI + Jaeger | D7, D8, D13 |
| E5 | ABM rechaza | 30 s | Terminal | D10 (rechazo ≠ falla), motivo normalizado |
| E6 | ABM no responde | 2 min | Terminal + Grafana (fila ABM) | Resiliencia en 2 capas, sweeper, reproceso |
| E7 | Respuesta duplicada | 45 s | Terminal + Kafka UI | D9 (la máquina de estados deduplica) |
| E8 | Falla al publicar | 1 min 30 s | Terminal + Grafana (Asincronía) | D5, D6 (outbox) |
| E9 | Consumidor caído | 1 min 30 s | Terminal + Kafka UI (lag) | D7 (retención y offsets) |
| E10 | Pico de volumen | 1 min | Grafana | D15, particiones, POST que solo escribe en la base |

Con el perfil `demo` el script completo tarda ~3 minutos (E6 ~55 s; E8 y E9 dependen de lo que tarden Kafka y la app
en volver). El resto del tiempo es lo que se dice.

## Preparación (15 minutos antes)

```bash
# 1. Stack completo: la app corre con SPRING_PROFILES_ACTIVE=demo,json-logs (lo pone el compose)
docker compose up -d --build
# Si el 3000 está ocupado: GRAFANA_PORT=3001 docker compose up -d --build

# 2. Esperar a que la app esté UP
until curl -sf http://localhost:8080/actuator/health/readiness >/dev/null; do sleep 2; done

# 3. Ensayo rápido (también "calienta" la JVM, Kafka y el pool: la primera nominación es la más lenta)
scripts/demo.sh E4 E5

# 4. Token para pegar en Swagger (Authorize → bearer) — vigencia 1 h
scripts/mint-token.sh ENT01 | pbcopy
```

- **Pestañas del navegador, en este orden**: README (diagrama) · Swagger http://localhost:8080/swagger-ui.html ·
  Kafka UI http://localhost:8081 · Jaeger http://localhost:16686 · Grafana http://localhost:3000 (o 3001) con el
  tablero "Nominaciones – visión operativa" en *Last 15 minutes*, refresco 5s · Prometheus http://localhost:9090/alerts.
- **Terminal**: fuente grande, dos paneles. Arriba `DEMO_PAUSE=1 scripts/demo.sh` (espera Enter entre pasos);
  abajo, logs filtrados: `docker compose logs -f app | grep --line-buffered demo-` (los correlation ids de la demo
  son `demo-eN-<hora>`).
- El perfil `demo` ([`application-demo.yml`](../app/src/main/resources/application-demo.yml)) comprime los tiempos:

  | Property | Default | Demo | Para qué |
  |----------|---------|------|----------|
  | `nominations.abm-mock.response-delay` | `2s` | `3s` | Ver `RECEIVED → PENDING_ABM → APPROVED` en el polling |
  | `nominations.abm-mock.slow-http-delay` | `10s` | `50s` | SLOW agota los reintentos antes de que ABM conteste: se ve `ABM_TIMEOUT → APPROVED` |
  | `nominations.abm.adapter.retry-delays` | `10s,1m,5m` | `3s,5s,10s` | FAIL termina en `ABM_TIMEOUT` en ~20 s (y no en ~6 min) |
  | `nominations.abm.sweeper.response-sla` / `.fixed-delay` | `15m` / `30s` | `30s` / `5s` | SILENT pasa a `ABM_TIMEOUT` en ~30–35 s |
  | `resilience4j.circuitbreaker.instances.abm.wait-duration-in-open-state` | `30s` | `10s` | Ver el CB abrir y volver a HALF_OPEN en vivo |
  | `nominations.metrics.db-gauges.cache-ttl` / `.kafka-dlt.refresh-interval` | `15s` / `30s` | `5s` / `10s` | Que Grafana reaccione durante E8/E10 |

  Solo cambian tiempos: lógica, tópicos y garantías son los mismos. Los retry-delays suman 18 s, más que los 10 s de
  CB abierto, por la misma razón que en producción (1m y 5m > 30s): una nominación sana que cae durante la apertura
  encuentra el CB en HALF_OPEN en su último reintento.

## Guion

> Formato de cada paso: **Digo** (1–2 frases que conectan con una decisión) · **Ejecuto** · **Muestro** ·
> **Preguntas posibles** con respuesta corta.

### 0. Arquitectura (1 min, README)

- **Digo**: "La API nunca llama a ABM. Acepta, persiste estado + historial + evento en una sola transacción y
  responde 202. Todo lo demás es asincrónico por Kafka, y cada salto es idempotente."
- **Muestro**: el diagrama "Flujo end to end" y la máquina de estados del README.

### E1 — Alta válida (45 s)

- **Digo**: "El canal recibe 202 con `Location` en milisegundos: ABM tarda minutos y no puede bloquear al canal. La
  entidad sale del token, no del body."
- **Ejecuto**: `DEMO_PAUSE=1 scripts/demo.sh` (arranca por E1). Opcional: el mismo POST desde Swagger.
- **Muestro**: `202`, `Location`, `Idempotent-Replayed: false`, `account_id`/`card_id` enmascarados; el GET
  inmediato; el GET con el token de ENT02 → **404**.
- **Preguntas posibles**:
  - *¿Por qué 202 y no 201?* El recurso existe pero el resultado de negocio todavía no; 201 sugeriría que la
    nominación está hecha. El canal consulta `Location` o escucha `nomination.result.v1`.
  - *¿Por qué 404 y no 403 a otra entidad?* Para no revelar que el id existe (enumeración).
  - *¿Cómo se autentica el canal en producción?* OAuth2 client credentials contra el IdP (JWKS, firma asimétrica) +
    mTLS en el gateway; el HS256 es solo de demo (D12).

### E2 — Datos faltantes (30 s)

- **Digo**: "Errores en RFC 9457 con código estable y campo en snake_case; nunca devolvemos el valor recibido.
  Un `card_id` con forma de PAN se rechaza: la plataforma queda fuera del alcance PCI."
- **Muestro**: `400 VALIDATION_ERROR` con `errors[]`, el PAN rechazado sin eco, `MALFORMED_REQUEST`, `401` sin token.
  El script verifica con `nominations_received_total` que no se creó nada.
- **Preguntas posibles**:
  - *¿Dónde se valida?* Bean Validation en el borde (forma) y el dominio (reglas: `CardToken` rechaza 13–19 dígitos);
    la base tiene además `CHECK`s.

### E3 — Solicitud repetida (1 min)

- **Digo**: "La clave de idempotencia es (entidad, request_id) con `UNIQUE` en PostgreSQL, no un cache: vive lo
  mismo que la nominación y funciona aun con concurrencia."
- **Muestro**: el mismo POST → mismo `nomination_id` con `Idempotent-Replayed: true`; mismo `request_id` con otra
  tarjeta → `409 IDEMPOTENCY_CONFLICT`; **5 POST en paralelo** → un solo id, una sola respuesta con `false`.
- **Preguntas posibles**:
  - *¿Por qué no Redis con TTL?* Un reintento tardío del canal después del TTL crearía un duplicado; la base ya
    está en la transacción, Redis sería otra pieza a mantener consistente (D4).
  - *¿Y si dos requests iguales llegan a la vez?* Uno inserta, el otro choca con el `UNIQUE`, relee y devuelve la
    existente (por eso las 5 respuestas son 202 con el mismo id).

### E4 — ABM aprueba (1 min)

- **Digo**: "El ABM Adapter consume `nomination.requested.v1`, llama a ABM fuera de toda transacción y pasa a
  `PENDING_ABM`. La respuesta vuelve por `abm.responses.v1` y el resultado sale por el outbox."
- **Muestro**: el polling `RECEIVED → PENDING_ABM → APPROVED` y el historial con `source`. En **Kafka UI**: los tres
  tópicos con key = `nomination_id`. En **Jaeger**: servicio `prisma-nominations-api`, la traza del POST que sigue
  por el relay del outbox, el adapter y la llamada HTTP a ABM.
- **Preguntas posibles**:
  - *¿Por qué key = nomination_id?* Orden por nominación (misma partición) y paralelismo entre nominaciones (D7).
  - *¿Qué pasa si ABM responde antes de que el adapter confirme el envío?* `RECEIVED → APPROVED` directo es válido.
  - *¿Cómo se sigue una operación?* `correlation_id` (negocio: historial, eventos, logs) + `trace_id` (técnico).

### E5 — ABM rechaza (30 s)

- **Digo**: "Un rechazo es una respuesta válida de negocio, no un error: no se reintenta. El consumidor ve un motivo
  normalizado; el código de ABM queda solo en la base para auditoría."
- **Muestro**: `REJECTED` con `rejection_reason: CARD_NOT_ELIGIBLE` (ABM-030), el código ABM no aparece.
- **Preguntas posibles**:
  - *¿Por qué normalizar?* Desacopla a los canales del catálogo de ABM: si ABM cambia códigos, cambia un mapper
    (`AbmReasonCodeMapper`), no cada canal. Un código desconocido cae en `OTHER`.

### E6 — ABM no responde (2 min)

- **Digo**: "Tres fallas técnicas distintas a la vez. Capa 1 en proceso: timeout de 2 s, 3 intentos y circuit
  breaker. Capa 2 en Kafka: tópicos de retry que **no bloquean la partición**. Agotado: DLT y `ABM_TIMEOUT`, que no
  es final: admite respuesta tardía o reproceso."
- **Ejecuto**: el script lanza FAIL (503), SLOW (contesta a los 50 s) y SILENT (acepta y nunca responde) y sigue los
  tres + el estado del CB en una tabla. Cuando SILENT vence el SLA, hace el **reproceso** con token de operador.
- **Muestro**: la tabla (`RECEIVED → ABM_TIMEOUT` para FAIL; CB `CLOSED → OPEN → HALF_OPEN`; SLOW
  `ABM_TIMEOUT → APPROVED`; SILENT `PENDING_ABM → ABM_TIMEOUT [SWEEPER] → RECEIVED [OPERATOR] → PENDING_ABM`),
  los historiales y el `409` al reprocesar una que ya no está en `ABM_TIMEOUT`. En **Grafana** (fila ABM): estado del
  CB, envíos `UNAVAILABLE`, timeouts por `source`. En **Kafka UI**: `nomination.requested.v1-retry-0/1/2` y `-dlt`.
- **Preguntas posibles**:
  - *¿Por qué `ABM_TIMEOUT` y no `REJECTED`?* No sabemos qué pasó en ABM; rechazar sería inventar un resultado.
    Por eso tampoco publica `nomination.result`: el evento sigue siendo único (el de SLOW sale recién con el APPROVED).
  - *¿El reproceso no duplica el alta en ABM?* ABM es idempotente por `nomination_id` (mismo `abm_operation_id`).
  - *¿Por qué el read-timeout (2 s) es menor que el slow-call del CB (3 s)?* Para que un timeout cuente como falla
    del CB y el peor caso de la capa 1 quede acotado (~6,6 s por mensaje).
  - *¿Por qué no reintentar 10 veces en el consumer?* Bloquearía la partición y frenaría a las nominaciones sanas;
    los tópicos de retry difieren solo al mensaje que falla.
  - *¿Las alertas?* `CircuitBreakerAbmOpen` (1 min sostenido) y `DltMessagesIncreasing` en Prometheus `/alerts`,
    cada una con su runbook en [`operations.md`](operations.md).

### E7 — Respuesta duplicada de ABM (45 s)

- **Digo**: "ABM manda la misma respuesta dos veces. No hace falta una tabla de dedup: la máquina de estados ya sabe
  que la nominación es final, y el índice único del outbox garantiza un solo `nomination.result`."
- **Muestro**: una sola transición a `APPROVED` en el historial; `abm_responses_total{outcome="DUPLICATE"}` +1; con
  compose, el `SELECT` del outbox con un único `nomination.result`. En **Kafka UI**: 2 mensajes en
  `abm.responses.v1` para esa key y 1 en `nomination.result.v1`.
- **Preguntas posibles**:
  - *¿Y si la segunda respuesta dice lo contrario?* `CONFLICT`: no se modifica, WARN y alerta `AbmResponseConflicts`.
  - *¿Y dos respuestas simultáneas?* Lock optimista (`@Version`): una gana, la otra reintenta y ve `DUPLICATE`.

### E8 — Falla al publicar el evento (1 min 30 s, requiere compose)

- **Digo**: "Si guardáramos y después publicáramos, una caída de Kafka nos dejaría estado sin evento. Con el outbox,
  el evento se escribe en la misma transacción y el relay lo publica cuando puede."
- **Ejecuto**: el script hace `docker compose stop kafka`, POST, consulta el outbox con `psql`, `docker compose start
  kafka` y sigue la nominación hasta `APPROVED`.
- **Muestro**: `202` con Kafka caído; la fila en `outbox_events` con `published_at` nulo, `attempts` y `last_error`;
  en **Grafana** (Asincronía) `outbox_pending` y la edad del más viejo; al volver Kafka, `publicado = t` y `APPROVED`.
- **Preguntas posibles**:
  - *¿Y si la app se cae entre el ack de Kafka y marcar publicado?* Se republica (at-least-once); los consumidores
    deduplican por `event_id`.
  - *¿Por qué polling y no Debezium?* Mismo contrato (tabla outbox); en producción CDC con Debezium evita el polling,
    acá evita levantar Kafka Connect (D6). `SKIP LOCKED` permite varias instancias del relay.
  - *¿Orden?* El relay toma solo el pendiente más viejo de cada nominación por ciclo + key por nominación.

### E9 — Consumidor caído y recuperación (1 min 30 s, requiere la app en el compose)

- **Digo**: "Kafka retiene los mensajes: un consumidor caído no pierde nada, retoma desde su último offset
  commiteado y, como la entrega es at-least-once, deduplica por `event_id`."
- **Ejecuto**: el script detiene la app (`docker compose stop app`: en la demo el consumidor `notifications-demo`
  vive en el mismo desplegable, D1), publica 3 `nomination.result.v1` con el productor de consola de Kafka (el
  "otro productor"), muestra el **lag = 3**, levanta la app y verifica que los 3 quedan procesados una vez
  (`consumer_processed_events`) y el lag vuelve a 0.
- **Muestro**: **Kafka UI → Consumers → `notifications-demo`** con lag 3 y luego 0.
- **Preguntas posibles**:
  - *¿Por qué publicar a mano?* Porque productor y consumidor comparten proceso en la demo; en producción el
    consumidor es otro servicio y la API sigue publicando mientras está caído. Lo mismo, sin apagar la app, lo
    prueba `E9ConsumerOutageIntegrationTest` deteniendo solo el listener.
  - *¿Y si el consumidor falla procesando?* Reintentos con backoff y luego `nomination.result.v1-dlt`; un poison
    pill va directo al DLT. Commit de offset por registro, después de procesar.
  - *¿Qué pasa si la retención vence antes de que vuelva?* Se pierde el mensaje para ese consumidor: la retención se
    dimensiona con el peor tiempo de recuperación + alerta `ConsumerLagHigh`.

### E10 — Pico de volumen (1 min)

- **Digo**: "El POST solo escribe en PostgreSQL; todo lo pesado es asincrónico y escala por particiones. Con hilos
  virtuales la API no se queda sin threads esperando I/O."
- **Ejecuto**: el script corre `scripts/load-test.sh 300 30` (300 altas de 4 entidades, 30 en paralelo, ~50%
  aprueba / ~50% rechaza, 10% reintentos del canal). Para un pico más visible: `scripts/load-test.sh 2000 50`.
- **Muestro**: el resumen del load test (p50/p95, throughput, todas en estado final); en **Grafana** altas/s, p95 del
  POST, lag por consumer y `nominations_open` drenando a 0.
- **Preguntas posibles**:
  - *¿Cuál es el cuello de botella?* La base (una TX por alta) y, aguas abajo, ABM: el CB y el lag absorben su ritmo
    sin afectar la aceptación. El relay escala por instancias (`SKIP LOCKED`), los consumers hasta 6 particiones.
  - *¿Rate limiting?* Por entidad en el gateway, no en la API.

### Cierre (30 s)

- **Muestro**: el resumen ✓/✗ del script y el tablero de Grafana completo.
- **Digo**: "Cada escenario tiene además un test automatizado: `mvn test -Dgroups=E6` corre la evidencia de E6"
  (mapa en [`scenarios.md`](scenarios.md)).

## Plan B (si algo falla en vivo)

| Falla | Qué hago |
|-------|----------|
| Un escenario no termina en el tiempo esperado | Seguir con el siguiente (`scripts/demo.sh E7`) y volver al final; mostrar el historial del caso con `GET /v1/nominations/{id}/history` (dice exactamente en qué quedó) |
| La app no levanta en el compose | `docker compose logs app`; alternativa sin compose: `cd app && mvn spring-boot:test-run -Dspring-boot.run.arguments=--spring.profiles.active=demo` (Postgres y Kafka con Testcontainers) y `scripts/demo.sh` (E8/E9 quedan saltados) |
| E8 o E9 no se pueden mostrar (sin compose, Docker lento) | Correr el test del escenario: `cd app && mvn test -Dgroups='E8 & integration'` / `-Dgroups='E9 & integration'` y explicar el test (Kafka caído con el relay real; listener detenido y retomado) |
| E6 tarda demasiado o el CB no abre | `mvn test -Dgroups=E6` (`AbmResilienceIntegrationTest`: FAIL, SLOW con respuesta tardía, SILENT + sweeper + reproceso, apertura y recuperación del CB) |
| Kafka quedó caído después de E8 | `docker compose start kafka`; el outbox drena solo (mostrarlo es parte del punto) |
| Grafana sin datos | Prometheus http://localhost:9090/targets: el job `prisma-nominations-api` tiene que estar `UP`; si no, mostrar `/actuator/prometheus` crudo |
| Jaeger sin la traza | Buscar por servicio `prisma-nominations-api` y operación `http post /v1/nominations`; si no, mostrar el `trace_id` en los logs JSON |
| Token vencido (1 h) en Swagger | `scripts/mint-token.sh ENT01` de nuevo (el script genera los suyos en cada corrida) |
| Sin red / sin Docker | Capturas del resumen del script y del tablero tomadas en el ensayo + `mvn test -Dgroups='E1 | E2 | E3'` (unitarios, sin Docker no corren los de integración) |

## Referencia del script

```bash
scripts/demo.sh                    # E1 → E10 de corrido, checklist al final (exit 1 si algún escenario falla)
scripts/demo.sh E6                 # un escenario; varios: scripts/demo.sh E4 E5 E7
DEMO_PAUSE=1 scripts/demo.sh       # Enter entre pasos
BASE_URL=http://localhost:18080 scripts/demo.sh   # otra instancia (E8/E9 se saltean salvo DEMO_COMPOSE=1)
```

E8 y E9 usan `docker compose` (stop/start de `kafka` y de la app, `psql` y las herramientas de Kafka dentro de los
contenedores). Con `DEMO_COMPOSE=auto` (default) solo se ejecutan si el compose del repo tiene `kafka` corriendo y
`BASE_URL` apunta al puerto 8080; si no, se marcan como saltados con el motivo. `DEMO_APP_SERVICE` fuerza el
nombre del servicio de la app (default: se detecta, `app`).
