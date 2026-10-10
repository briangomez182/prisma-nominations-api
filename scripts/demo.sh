#!/usr/bin/env bash
# Demo guiada de los escenarios obligatorios E1–E10 contra la API de nominaciones.
#
# Uso:
#   scripts/demo.sh                 # E1 → E10 de corrido
#   scripts/demo.sh E6              # un solo escenario (o varios: scripts/demo.sh E4 E5 E7)
#   DEMO_PAUSE=1 scripts/demo.sh    # espera Enter entre pasos (modo presentación)
#
# Variables opcionales:
#   BASE_URL           URL de la app. Default: http://localhost:8080
#   DEMO_PAUSE         1 = pausa entre pasos. Default: 0
#   DEMO_COMPOSE       auto | 1 | 0. E8 y E9 apagan servicios del docker-compose (kafka, la app). En "auto" solo se
#                      usan si el compose tiene kafka corriendo y BASE_URL apunta al puerto 8080. Default: auto
#   DEMO_APP_SERVICE   servicio de la app en el compose (E9). Default: se detecta (el que no es infraestructura)
#   LOAD_TEST_ARGS     argumentos para scripts/load-test.sh en E10 (default: pico chico, ver scenario_e10)
#   NO_COLOR           cualquier valor = sin colores
#
# La app tiene que correr con el perfil "demo" (application-demo.yml: SLA del sweeper 30s, retries 3s/5s/10s,
# respuesta de ABM a los 3s). Con los defaults E6 tarda ~20 min. Tokens: los genera scripts/mint-token.sh.
# Requiere: bash, curl, jq, openssl (y docker compose para E8/E9).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
BASE_URL="${BASE_URL%/}"
DEMO_PAUSE="${DEMO_PAUSE:-0}"
DEMO_COMPOSE="${DEMO_COMPOSE:-auto}"
ALL_SCENARIOS="E1 E2 E3 E4 E5 E6 E7 E8 E9 E10"
RUN_ID="$(date +%H%M%S)"

# ── presentación ────────────────────────────────────────────────────────────────────────────────────────────
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  B=$'\e[1m'; DIM=$'\e[2m'; R=$'\e[31m'; G=$'\e[32m'; Y=$'\e[33m'; BL=$'\e[34m'; M=$'\e[35m'; C=$'\e[36m'; N=$'\e[0m'
  JQ_COLOR=-C
else
  B=; DIM=; R=; G=; Y=; BL=; M=; C=; N=
  JQ_COLOR=-M
fi

title()  { printf '\n%s━━━ %s ━━━%s\n' "${B}${M}" "$*" "${N}"; }
say()    { printf '%s\n' "$*"; }
step()   { printf '\n%s▸ %s%s\n' "${B}" "$*" "${N}"; }
look()   { printf '%s  » %s%s\n' "${C}" "$*" "${N}"; }
note()   { printf '%s  %s%s\n' "${DIM}" "$*" "${N}"; }
ok()     { printf '%s  ✓ %s%s\n' "${G}" "$*" "${N}"; }
warn()   { printf '%s  ! %s%s\n' "${Y}" "$*" "${N}"; }
fail()   { printf '%s  ✗ %s%s\n' "${R}" "$*" "${N}"; SC_FAILS=$((SC_FAILS + 1)); }
indent() { sed 's/^/    /'; }

pause() {
  [[ "${DEMO_PAUSE}" == "1" ]] || return 0
  printf '%s  [Enter para seguir]%s' "${DIM}" "${N}"
  { read -r _ </dev/tty; } 2>/dev/null || true
}

usage() { sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'; }

# ── requisitos ──────────────────────────────────────────────────────────────────────────────────────────────
for bin in curl jq openssl; do
  command -v "${bin}" >/dev/null || { echo "Falta '${bin}' en el PATH" >&2; exit 1; }
done

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nominations-demo.XXXXXX")"
trap 'rm -rf "${TMP_DIR}"' EXIT

new_uuid() {
  if command -v uuidgen >/dev/null; then uuidgen | tr '[:upper:]' '[:lower:]'
  else cat /proc/sys/kernel/random/uuid; fi
}

# ── HTTP ────────────────────────────────────────────────────────────────────────────────────────────────────
# api METHOD PATH [BODY] [TOKEN]  → HTTP_STATUS, RESP (body) y headers en ${TMP_DIR}/headers.
# TOKEN: default ${TOKEN} (ENT01); "" = sin Authorization. CORR (variable) = X-Correlation-Id.
api() {
  local method="$1" path="$2" body="${3:-}" token="${4-${TOKEN}}"
  local args=(-sS -X "${method}" -o "${TMP_DIR}/body" -D "${TMP_DIR}/headers" -w '%{http_code}' --max-time 20
              -H 'Accept: application/json, application/problem+json')
  [[ -n "${token}" ]] && args+=(-H "Authorization: Bearer ${token}")
  [[ -n "${CORR:-}" ]] && args+=(-H "X-Correlation-Id: ${CORR}")
  [[ -n "${body}" ]] && args+=(-H 'Content-Type: application/json' --data "${body}")
  HTTP_STATUS="$(curl "${args[@]}" "${BASE_URL}${path}" 2>/dev/null)" || HTTP_STATUS="000"
  RESP="$(cat "${TMP_DIR}/body" 2>/dev/null || true)"
}

header() { grep -i "^$1:" "${TMP_DIR}/headers" 2>/dev/null | tail -n 1 | cut -d: -f2- | tr -d '\r' | sed 's/^ *//'; }
field()  { jq -r "$1 // empty" <<<"${RESP}" 2>/dev/null || true; }

# Muestra el request (con body compacto) y la respuesta (status + lo relevante filtrado con jq).
call() {
  local method="$1" path="$2" body="${3:-}" token="${4-${TOKEN}}" filter="${5:-.}"
  local auth="Bearer ENT01"
  [[ "${token}" == "${OPERATOR_TOKEN:-x}" ]] && auth="Bearer operador"
  [[ "${token}" == "${OTHER_TOKEN:-x}" ]] && auth="Bearer ENT02"
  [[ -z "${token}" ]] && auth="sin token"
  printf '  %s→ %s %s%s %s(%s%s)%s\n' "${BL}" "${method}" "${path}" "${N}" "${DIM}" "${auth}" \
    "${CORR:+, X-Correlation-Id: ${CORR}}" "${N}"
  [[ -n "${body}" ]] && { jq -c . <<<"${body}" 2>/dev/null || printf '%s\n' "${body}"; } | indent
  api "${method}" "${path}" "${body}" "${token}"
  local color="${G}"
  [[ "${HTTP_STATUS}" =~ ^[45] ]] && color="${Y}"
  [[ "${HTTP_STATUS}" =~ ^(5|000) ]] && color="${R}"
  local extra=""
  [[ -n "$(header Location)" ]] && extra+="  Location: $(header Location)"
  [[ -n "$(header Idempotent-Replayed)" ]] && extra+="  Idempotent-Replayed: $(header Idempotent-Replayed)"
  printf '  %s← %s%s%s\n' "${color}${B}" "${HTTP_STATUS}" "${N}" "${DIM}${extra}${N}"
  if [[ -n "${RESP}" ]]; then
    jq "${JQ_COLOR}" "${filter}" <<<"${RESP}" 2>/dev/null | indent || printf '%s\n' "${RESP}" | indent
  fi
}

expect_eq() { # descripción valor esperado
  if [[ "$2" == "$3" ]]; then ok "$1: $2"; else fail "$1: esperado '$3', obtenido '${2:-<vacío>}'"; fi
}

nomination_body() { # card_id [request_id]
  jq -nc --arg r "${2:-$(new_uuid)}" --arg c "$1" \
    '{request_id: $r, customer_id: "CUST-000123", account_id: "0001234567890987654", card_id: $c, alias: "CUENTA SUELDO"}'
}

get_status() { api GET "/v1/nominations/$1"; field .status; }

# wait_status ID REGEX_FINAL TIMEOUT [ETIQUETA]: polling cada 1s mostrando cada cambio de estado.
wait_status() {
  local id="$1" final="$2" timeout="$3" label="${4:-}" start="${SECONDS}" prev="" st
  while :; do
    st="$(get_status "${id}")"
    if [[ "${st}" != "${prev}" ]]; then
      printf '  %s%3ss%s  %s%s%s%s\n' "${DIM}" "$((SECONDS - start))" "${N}" "${label:+${label} }" "${B}" "${st:-?}" "${N}"
      prev="${st}"
    fi
    [[ "${st}" =~ ^(${final})$ ]] && { LAST_STATUS="${st}"; return 0; }
    (( SECONDS - start >= timeout )) && { LAST_STATUS="${st}"; return 1; }
    sleep 1
  done
}

show_history() { # id
  api GET "/v1/nominations/$1/history"
  jq -r '.items[] | "\(.occurred_at | sub("\\.[0-9]+Z$"; "Z") | .[11:19])  \(.from_status // "∅") → \(.to_status)  [\(.source)]  \(.detail // "")"' \
    <<<"${RESP}" 2>/dev/null | indent
}

count_history_to() { # id estado
  api GET "/v1/nominations/$1/history"
  jq --arg s "$2" '[.items[] | select(.to_status == $s)] | length' <<<"${RESP}" 2>/dev/null || echo 0
}

# Suma de una métrica de /actuator/prometheus (público en la demo). metric NOMBRE [FILTRO_DE_ETIQUETAS]
metric() {
  curl -s --max-time 5 "${BASE_URL}/actuator/prometheus" 2>/dev/null \
    | grep -E "^$1(\{|$| )" | grep -E -- "${2:-.}" | awk '{s += $NF} END {printf "%d", s}'
}

cb_state() {
  api GET /actuator/health "" "${OPERATOR_TOKEN}"
  field '.components.circuitBreakers.details.abm.details.state'
}

# ── docker compose (E8 y E9) ────────────────────────────────────────────────────────────────────────────────
INFRA_SERVICES=" postgres kafka kafka-ui jaeger prometheus grafana "
compose() { docker compose -f "${ROOT_DIR}/docker-compose.yml" "$@"; }

COMPOSE_OK=0
COMPOSE_WHY=""
detect_compose() {
  if [[ "${DEMO_COMPOSE}" == "0" ]]; then COMPOSE_WHY="DEMO_COMPOSE=0"; return; fi
  if ! command -v docker >/dev/null || ! docker compose version >/dev/null 2>&1; then
    COMPOSE_WHY="docker compose no está disponible"; return
  fi
  if ! compose ps --services --status running 2>/dev/null | grep -qx kafka; then
    COMPOSE_WHY="el docker-compose del repo no tiene kafka corriendo (docker compose up -d)"; return
  fi
  if [[ "${DEMO_COMPOSE}" == "auto" && ! "${BASE_URL}" =~ :8080$ ]]; then
    COMPOSE_WHY="BASE_URL no es el puerto 8080 del compose (forzar con DEMO_COMPOSE=1)"; return
  fi
  COMPOSE_OK=1
}

app_service() {
  if [[ -n "${DEMO_APP_SERVICE:-}" ]]; then echo "${DEMO_APP_SERVICE}"; return; fi
  compose ps --services --status running 2>/dev/null | while read -r s; do
    [[ "${INFRA_SERVICES}" == *" ${s} "* ]] || echo "${s}"
  done | head -n 1
}

psql_q() { compose exec -T postgres psql -U nominations -d nominations -P pager=off -c "$1"; }
psql_val() { compose exec -T postgres psql -U nominations -d nominations -tA -c "$1" | tr -d '[:space:]'; }
kafka_cli() { local tool="$1"; shift; compose exec -T kafka "/opt/kafka/bin/${tool}" --bootstrap-server localhost:9092 "$@"; }

wait_kafka_up() {
  local start="${SECONDS}"
  until kafka_cli kafka-broker-api-versions.sh >/dev/null 2>&1; do
    (( SECONDS - start > 90 )) && return 1
    sleep 2
  done
}

wait_app_up() {
  local start="${SECONDS}"
  until [[ "$(curl -s --max-time 3 "${BASE_URL}/actuator/health" | jq -r '.status // empty' 2>/dev/null)" == "UP" ]]; do
    (( SECONDS - start > 180 )) && return 1
    sleep 2
  done
}

group_lag() { # consumer group → lag total
  kafka_cli kafka-consumer-groups.sh --describe --group "$1" 2>/dev/null \
    | awk '$6 ~ /^[0-9]+$/ {s += $6} END {printf "%d", s}'
}

wait_group_lag() { # consumer group, lag esperado, segundos máx → último lag leído
  local lag start="${SECONDS}"
  lag="$(group_lag "$1")"
  until [[ "${lag}" == "$2" ]] || (( SECONDS - start > $3 )); do
    sleep 2; lag="$(group_lag "$1")"
  done
  printf '%s' "${lag}"
}

# ── resultados ──────────────────────────────────────────────────────────────────────────────────────────────
SC_FAILS=0
record() { printf -v "RESULT_$1" '%s' "$2"; printf -v "DETAIL_$1" '%s' "${3:-}"; }
finish() { # escenario descripción
  local s="$1"
  if (( SC_FAILS == 0 )); then record "${s}" ok "$2"; else record "${s}" fail "$2 (${SC_FAILS} chequeos fallidos)"; fi
}
skip() { warn "SALTADO: $2"; record "$1" skip "$2"; }

# ── escenarios ──────────────────────────────────────────────────────────────────────────────────────────────
scenario_e1() {
  title "E1 — Nominación válida: 202 inmediato, la API no espera a ABM"
  say "La API persiste nominación + historial + evento (outbox) en UNA transacción y responde 202 con Location."
  look "Swagger: ${BASE_URL}/swagger-ui.html → POST /v1/nominations"
  local rid; rid="$(new_uuid)"
  E1_REQUEST_ID="${rid}"
  E1_BODY="$(nomination_body "tok_demo_ok_e1_${RUN_ID}" "${rid}")"
  step "Alta válida"
  CORR="demo-e1-${RUN_ID}" call POST /v1/nominations "${E1_BODY}" "${TOKEN}" \
    '{nomination_id, status, account_id, card_id, correlation_id}'
  expect_eq "HTTP" "${HTTP_STATUS}" 202
  expect_eq "status" "$(field .status)" RECEIVED
  expect_eq "Idempotent-Replayed" "$(header Idempotent-Replayed)" false
  [[ "$(header Location)" == /v1/nominations/* ]] && ok "Location presente" || fail "falta Location"
  E1_ID="$(field .nomination_id)"
  note "account_id y card_id vuelven enmascarados (D11). El correlation_id viaja a historial, outbox, Kafka y logs."
  pause
  step "Consulta inmediata por Location (el estado avanza solo, asincrónico)"
  call GET "/v1/nominations/${E1_ID}" "" "${TOKEN}" '{status, updated_at}'
  expect_eq "HTTP" "${HTTP_STATUS}" 200
  step "Aislamiento por entidad: otra entidad (token de ENT02) consulta la misma nominación"
  call GET "/v1/nominations/${E1_ID}" "" "${OTHER_TOKEN}" '{status, code, detail}'
  expect_eq "HTTP (404, no 403: no revela que existe)" "${HTTP_STATUS}" 404
  look "Jaeger: buscar el correlation id demo-e1-${RUN_ID} en los logs → trace_id → una sola traza API → outbox → Kafka → ABM"
  finish E1 "202 + Location en el acto; 404 a otra entidad"
}

scenario_e2() {
  title "E2 — Datos faltantes o inválidos: 400 sin efectos"
  say "Validación en el borde: RFC 9457 con code y errors[] en snake_case; nunca se devuelve el valor recibido."
  local before; before="$(metric nominations_received_total)"
  step "Faltan campos obligatorios"
  CORR="demo-e2-${RUN_ID}" call POST /v1/nominations '{"alias":"CUENTA SUELDO"}' "${TOKEN}" '{code, errors}'
  expect_eq "HTTP" "${HTTP_STATUS}" 400
  expect_eq "code" "$(field .code)" VALIDATION_ERROR
  pause
  step "card_id con forma de PAN (número de prueba público de Visa): la plataforma queda fuera de PCI"
  call POST /v1/nominations "$(nomination_body 4111111111111111)" "${TOKEN}" '{code, errors}'
  expect_eq "HTTP" "${HTTP_STATUS}" 400
  if grep -q 4111111111111111 <<<"${RESP}"; then fail "la respuesta repite el PAN"; else ok "la respuesta no repite el PAN"; fi
  step "JSON inválido"
  call POST /v1/nominations '{ "request_id": "no-es-un-uuid", ' "${TOKEN}" '{code, detail}'
  expect_eq "HTTP" "${HTTP_STATUS}" 400
  expect_eq "code" "$(field .code)" MALFORMED_REQUEST
  step "Sin token"
  call POST /v1/nominations "$(nomination_body tok_demo_ok_e2)" "" '{code, correlation_id}'
  expect_eq "HTTP" "${HTTP_STATUS}" 401
  local after; after="$(metric nominations_received_total)"
  expect_eq "nominaciones creadas por estos requests (nominations_received_total)" "$((after - before))" 0
  finish E2 "400 VALIDATION_ERROR / MALFORMED_REQUEST, 401 sin token, nada persistido"
}

scenario_e3() {
  title "E3 — Solicitud repetida: idempotencia por (entity_id, request_id)"
  say "El canal reintenta con el mismo request_id: se devuelve la MISMA nominación, sin reenviar a ABM. UNIQUE en la base."
  local rid body id
  if [[ -n "${E1_ID:-}" ]]; then
    rid="${E1_REQUEST_ID}"; body="${E1_BODY}"; id="${E1_ID}"
    note "Se repite el POST de E1 (request_id ${rid})."
  else
    rid="$(new_uuid)"; body="$(nomination_body "tok_demo_ok_e3_${RUN_ID}" "${rid}")"
    step "Alta original"
    call POST /v1/nominations "${body}" "${TOKEN}" '{nomination_id, status}'
    id="$(field .nomination_id)"
  fi
  step "Mismo POST otra vez"
  CORR="demo-e3-${RUN_ID}" call POST /v1/nominations "${body}" "${TOKEN}" '{nomination_id, status}'
  expect_eq "HTTP" "${HTTP_STATUS}" 202
  expect_eq "nomination_id (el mismo)" "$(field .nomination_id)" "${id}"
  expect_eq "Idempotent-Replayed" "$(header Idempotent-Replayed)" true
  pause
  step "Mismo request_id con OTRO contenido (otra tarjeta)"
  call POST /v1/nominations "$(nomination_body tok_otra_tarjeta "${rid}")" "${TOKEN}" '{code, detail}'
  expect_eq "HTTP" "${HTTP_STATUS}" 409
  expect_eq "code" "$(field .code)" IDEMPOTENCY_CONFLICT
  pause
  step "Carrera: 5 POST idénticos en paralelo con un request_id nuevo"
  local rid2 body2 i
  rid2="$(new_uuid)"; body2="$(nomination_body "tok_demo_ok_e3_race_${RUN_ID}" "${rid2}")"
  for i in 1 2 3 4 5; do
    curl -s -o "${TMP_DIR}/race_${i}.json" -D "${TMP_DIR}/race_${i}.h" -X POST "${BASE_URL}/v1/nominations" \
      -H "Authorization: Bearer ${TOKEN}" -H 'Content-Type: application/json' --data "${body2}" &
  done
  wait
  local ids replayed_false
  ids="$(cat "${TMP_DIR}"/race_*.json | jq -r '.nomination_id // "ERROR"' | sort -u)"
  replayed_false="$(cat "${TMP_DIR}"/race_*.h | grep -ic '^Idempotent-Replayed: false' || true)"
  printf '%s\n' "${ids}" | sed 's/^/    nomination_id: /'
  expect_eq "nomination_id distintos entre los 5" "$(printf '%s\n' "${ids}" | wc -l | tr -d ' ')" 1
  expect_eq "respuestas con Idempotent-Replayed: false (una sola creó)" "${replayed_false}" 1
  look "Grafana → Tráfico y negocio → replays y conflictos de idempotencia"
  finish E3 "replay devuelve la misma nominación; 409 con otro contenido; 5 concurrentes → 1 alta"
}

scenario_e4() {
  title "E4 — ABM aprueba"
  say "La API no llama a ABM: el ABM Adapter consume nomination.requested.v1, ABM contesta por abm.responses.v1."
  CORR="demo-e4-${RUN_ID}" call POST /v1/nominations "$(nomination_body "tok_demo_ok_${RUN_ID}")" "${TOKEN}" '{nomination_id, status}'
  expect_eq "HTTP" "${HTTP_STATUS}" 202
  local id; id="$(field .nomination_id)"
  step "Polling del estado (ABM responde a los ~3s en el perfil demo)"
  if wait_status "${id}" 'APPROVED|REJECTED|ABM_TIMEOUT' 40; then expect_eq "estado final" "${LAST_STATUS}" APPROVED
  else fail "no llegó a un estado final (último: ${LAST_STATUS:-?})"; fi
  step "Historial (solo inserción)"
  show_history "${id}"
  look "Kafka UI: nomination.requested.v1 → abm.responses.v1 → nomination.result.v1 (key = ${id})"
  look "Jaeger: servicio prisma-nominations-api, la traza del POST sigue por el outbox y el adapter"
  finish E4 "RECEIVED → PENDING_ABM → APPROVED"
}

scenario_e5() {
  title "E5 — ABM rechaza: motivo normalizado, sin reintentos"
  say "Un rechazo es una RESPUESTA de negocio (no un error): REJECTED + motivo normalizado; el código ABM queda solo en la base."
  CORR="demo-e5-${RUN_ID}" call POST /v1/nominations "$(nomination_body "tok_demo_REJECT_030_${RUN_ID}")" "${TOKEN}" '{nomination_id, status}'
  local id; id="$(field .nomination_id)"
  step "Polling del estado"
  if wait_status "${id}" 'APPROVED|REJECTED|ABM_TIMEOUT' 40; then expect_eq "estado final" "${LAST_STATUS}" REJECTED
  else fail "no llegó a un estado final (último: ${LAST_STATUS:-?})"; fi
  call GET "/v1/nominations/${id}" "" "${TOKEN}" '{status, rejection_reason}'
  expect_eq "rejection_reason" "$(field .rejection_reason)" CARD_NOT_ELIGIBLE
  if grep -q 'ABM-030' <<<"${RESP}"; then fail "se expone el código interno ABM-030"; else ok "el código ABM-030 no se expone"; fi
  show_history "${id}"
  look "Grafana → Tráfico y negocio → resueltas por estado y motivo (reason=CARD_NOT_ELIGIBLE)"
  finish E5 "REJECTED / CARD_NOT_ELIGIBLE (ABM-030 no expuesto)"
}

scenario_e6() {
  title "E6 — ABM no responde: timeout, reintentos y recuperación"
  say "Tres fallas técnicas a la vez: FAIL (503), SLOW (contesta tarde, > read-timeout) y SILENT (acepta y nunca responde)."
  say "Capa 1: retry corto + circuit breaker. Capa 2: tópicos de retry no bloqueantes → DLT → ABM_TIMEOUT. Sweeper por SLA."
  look "Grafana → fila ABM (CB, envíos UNAVAILABLE, timeouts por source) · Kafka UI → nomination.requested.v1-retry-*/-dlt"
  local fail_id slow_id silent_id
  step "Tres altas"
  CORR="demo-e6-fail-${RUN_ID}" call POST /v1/nominations "$(nomination_body "tok_demo_FAIL_${RUN_ID}")" "${TOKEN}" '{nomination_id, status}'
  fail_id="$(field .nomination_id)"
  CORR="demo-e6-slow-${RUN_ID}" call POST /v1/nominations "$(nomination_body "tok_demo_SLOW_${RUN_ID}")" "${TOKEN}" '{nomination_id, status}'
  slow_id="$(field .nomination_id)"
  CORR="demo-e6-silent-${RUN_ID}" call POST /v1/nominations "$(nomination_body "tok_demo_SILENT_${RUN_ID}")" "${TOKEN}" '{nomination_id, status}'
  silent_id="$(field .nomination_id)"
  pause
  step "Seguimiento (FAIL ~20–25s, SILENT ~30–35s + reproceso, SLOW ~55s)"
  local start="${SECONDS}" pf="" ps="" pi="" pcb="" f s i cb reprocessed=0 cb_opened=0 slow_timeout_seen=0
  while :; do
    f="$(get_status "${fail_id}")"; s="$(get_status "${slow_id}")"; i="$(get_status "${silent_id}")"; cb="$(cb_state)"
    if [[ "${f}|${s}|${i}|${cb}" != "${pf}|${ps}|${pi}|${pcb}" ]]; then
      printf '  %s%3ss%s  FAIL %s%-12s%s SLOW %s%-12s%s SILENT %s%-12s%s CB %s\n' "${DIM}" "$((SECONDS - start))" "${N}" \
        "${B}" "${f}" "${N}" "${B}" "${s}" "${N}" "${B}" "${i}" "${N}" "${cb:-?}"
      pf="${f}"; ps="${s}"; pi="${i}"; pcb="${cb}"
    fi
    [[ "${cb}" == "OPEN" ]] && cb_opened=1
    [[ "${s}" == "ABM_TIMEOUT" ]] && slow_timeout_seen=1
    if [[ "${i}" == "ABM_TIMEOUT" && "${reprocessed}" == 0 ]]; then
      printf '  %s→ SILENT venció el SLA (sweeper). Reproceso por un operador (scope nominations:operate):%s\n' "${Y}" "${N}"
      CORR="demo-e6-reprocess-${RUN_ID}" call POST "/internal/v1/nominations/${silent_id}/reprocess" "" "${OPERATOR_TOKEN}" '{status}'
      expect_eq "reproceso HTTP" "${HTTP_STATUS}" 202
      reprocessed=1; pi="RECEIVED"
    fi
    if [[ "${f}" == "ABM_TIMEOUT" && "${s}" =~ ^(APPROVED|REJECTED)$ && "${reprocessed}" == 1 && "${i}" == "PENDING_ABM" ]]; then break; fi
    (( SECONDS - start >= 120 )) && break
    sleep 1
  done
  expect_eq "FAIL" "${f}" ABM_TIMEOUT
  expect_eq "SLOW (respuesta tardía de ABM aplicada)" "${s}" APPROVED
  expect_eq "SILENT reprocesada (ABM idempotente → mismo alta)" "${i}" PENDING_ABM
  if (( slow_timeout_seen )); then ok "SLOW pasó por ABM_TIMEOUT antes de APPROVED"
  else note "SLOW no llegó a verse en ABM_TIMEOUT (ABM respondió antes de agotar la capa 2)"; fi
  (( cb_opened )) && ok "el circuit breaker abrió y se recuperó solo" || note "el CB no llegó a abrir (depende de la ventana de 20 llamadas)"
  pause
  step "Historiales"
  say "  FAIL:";   show_history "${fail_id}"
  say "  SLOW:";   show_history "${slow_id}"
  say "  SILENT:"; show_history "${silent_id}"
  expect_eq "SLOW: transiciones a APPROVED (un único nomination.result)" "$(count_history_to "${slow_id}" APPROVED)" 1
  step "Reproceso de una nominación que ya no está en ABM_TIMEOUT (SLOW, ya APPROVED) → 409"
  call POST "/internal/v1/nominations/${slow_id}/reprocess" "" "${OPERATOR_TOKEN}" '{code, detail}'
  expect_eq "HTTP" "${HTTP_STATUS}" 409
  step "Esperando que el circuit breaker salga de OPEN (no afectar a los escenarios siguientes)"
  local t0="${SECONDS}"
  while [[ "$(cb_state)" == "OPEN" ]] && (( SECONDS - t0 < 30 )); do sleep 1; done
  note "CB: $(cb_state) · DLT acumulados (kafka_dlt_messages): $(metric kafka_dlt_messages)"
  look "Prometheus /alerts: CircuitBreakerAbmOpen y DltMessagesIncreasing (necesitan 1–10 min sostenidos para disparar)"
  finish E6 "FAIL → DLT → ABM_TIMEOUT · SLOW → ABM_TIMEOUT → APPROVED · SILENT → sweeper → reproceso"
}

scenario_e7() {
  title "E7 — ABM responde dos veces"
  say "La máquina de estados ya sabe si la respuesta fue procesada: la segunda es DUPLICATE → ACK sin efectos."
  local dup_before; dup_before="$(metric abm_responses_total 'outcome="DUPLICATE"')"
  CORR="demo-e7-${RUN_ID}" call POST /v1/nominations "$(nomination_body "tok_demo_DUP_${RUN_ID}")" "${TOKEN}" '{nomination_id, status}'
  local id; id="$(field .nomination_id)"
  if wait_status "${id}" 'APPROVED|REJECTED|ABM_TIMEOUT' 40; then expect_eq "estado final" "${LAST_STATUS}" APPROVED
  else fail "no llegó a un estado final (último: ${LAST_STATUS:-?})"; fi
  sleep 2   # la segunda copia llega 300ms después de la primera
  show_history "${id}"
  expect_eq "transiciones a APPROVED" "$(count_history_to "${id}" APPROVED)" 1
  local dup_after; dup_after="$(metric abm_responses_total 'outcome="DUPLICATE"')"
  expect_eq "respuestas DUPLICATE nuevas (abm_responses_total)" "$((dup_after - dup_before))" 1
  if (( COMPOSE_OK )); then
    step "Outbox: un solo nomination.result (índice único parcial)"
    psql_q "SELECT event_type, published_at IS NOT NULL AS publicado FROM outbox_events WHERE aggregate_id = '${id}' ORDER BY created_at" | indent
    expect_eq "nomination.result en el outbox" \
      "$(psql_val "SELECT count(*) FROM outbox_events WHERE aggregate_id = '${id}' AND event_type = 'nomination.result'")" 1
  fi
  look "Kafka UI: abm.responses.v1 tiene 2 mensajes con key ${id}; nomination.result.v1, uno solo"
  finish E7 "dos respuestas, una transición, un resultado"
}

scenario_e8() {
  title "E8 — Falla al publicar el evento: consistencia vía outbox"
  say "Con Kafka caído la API sigue aceptando: el evento queda en outbox_events (misma TX) y el relay lo publica al volver."
  if (( ! COMPOSE_OK )); then skip E8 "requiere docker compose: ${COMPOSE_WHY}. Alternativa: mvn test -Dgroups=E8"; return; fi
  step "docker compose stop kafka"
  compose stop kafka 2>&1 | indent
  pause
  CORR="demo-e8-${RUN_ID}" call POST /v1/nominations "$(nomination_body "tok_demo_ok_e8_${RUN_ID}")" "${TOKEN}" '{nomination_id, status}'
  expect_eq "HTTP con Kafka caído" "${HTTP_STATUS}" 202
  local id; id="$(field .nomination_id)"
  sleep 8
  call GET "/v1/nominations/${id}" "" "${TOKEN}" '{status}'
  expect_eq "estado" "$(field .status)" RECEIVED
  step "El evento espera en el outbox (attempts y last_error del relay)"
  psql_q "SELECT event_type, published_at, attempts, left(last_error, 60) AS last_error FROM outbox_events WHERE aggregate_id = '${id}'" | indent
  expect_eq "eventos pendientes de esta nominación" \
    "$(psql_val "SELECT count(*) FROM outbox_events WHERE aggregate_id = '${id}' AND published_at IS NULL")" 1
  note "outbox_pending (métrica, cache 5s): $(metric outbox_pending)"
  look "Grafana → Asincronía: outbox pendiente / edad del más viejo / outbox_failing"
  pause
  step "docker compose start kafka"
  compose start kafka 2>&1 | indent
  wait_kafka_up || fail "kafka no volvió en 90s"
  step "El relay publica y el flujo sigue solo"
  if wait_status "${id}" 'APPROVED|REJECTED|ABM_TIMEOUT' 120; then expect_eq "estado final" "${LAST_STATUS}" APPROVED
  else fail "no llegó a un estado final (último: ${LAST_STATUS:-?})"; fi
  psql_q "SELECT event_type, published_at IS NOT NULL AS publicado, attempts FROM outbox_events WHERE aggregate_id = '${id}' ORDER BY created_at" | indent
  finish E8 "202 con Kafka caído; evento en outbox; publicado al volver; APPROVED"
}

scenario_e9() {
  title "E9 — Consumidor caído y recuperación"
  say "Kafka retiene los mensajes: un consumidor caído retoma desde su último offset commiteado y deduplica por event_id."
  if (( ! COMPOSE_OK )); then skip E9 "requiere docker compose: ${COMPOSE_WHY}. Alternativa: mvn test -Dgroups=E9"; return; fi
  local svc; svc="$(app_service)"
  if [[ -z "${svc}" ]]; then
    skip E9 "la app no corre dentro del compose (sin servicio para detener; DEMO_APP_SERVICE). Alternativa: mvn test -Dgroups=E9"
    return
  fi
  note "En la demo el consumidor (notifications-demo) vive en el mismo desplegable (D1): detener el consumidor = detener '${svc}'."
  note "Mientras está caído, 'otro productor' publica 3 nomination.result.v1 (en producción la API es otro servicio)."
  # Los escenarios anteriores pueden dejar resultados en vuelo: se arranca con el consumidor al día.
  local before; before="$(wait_group_lag notifications-demo 0 60)"
  step "Lag del grupo notifications-demo antes: ${before}"
  step "docker compose stop ${svc}"
  compose stop "${svc}" 2>&1 | indent
  local ids=() i eid nid
  for i in 1 2 3; do
    eid="$(new_uuid)"; nid="$(new_uuid)"; ids+=("${eid}")
    printf '%s|{"event_id":"%s","event_type":"nomination.result","schema_version":1,"nomination_id":"%s","status":"APPROVED","correlation_id":"demo-e9-%s"}\n' \
      "${nid}" "${eid}" "${nid}" "${RUN_ID}"
  done > "${TMP_DIR}/e9.txt"
  kafka_cli kafka-console-producer.sh --topic nomination.result.v1 \
    --property parse.key=true --property 'key.separator=|' < "${TMP_DIR}/e9.txt" >/dev/null 2>&1 \
    || fail "no se pudo publicar en nomination.result.v1"
  local lag; lag="$(wait_group_lag notifications-demo 3 15)"
  expect_eq "lag de notifications-demo con el consumidor caído" "${lag}" 3
  look "Kafka UI → Consumers → notifications-demo: lag 3"
  pause
  step "docker compose start ${svc}"
  compose start "${svc}" 2>&1 | indent
  wait_app_up || fail "la app no volvió en 180s"
  local in_list; in_list="'$(IFS=,; printf '%s' "${ids[*]}" | sed "s/,/','/g")'"
  local start="${SECONDS}" n=0
  until [[ "${n}" == 3 ]] || (( SECONDS - start > 60 )); do
    n="$(psql_val "SELECT count(*) FROM consumer_processed_events WHERE event_id IN (${in_list})")"; sleep 2
  done
  expect_eq "eventos procesados al volver (consumer_processed_events)" "${n}" 3
  expect_eq "lag de notifications-demo después" "$(wait_group_lag notifications-demo 0 30)" 0
  finish E9 "lag 3 con el consumidor caído → 0 al volver, cada evento procesado una vez"
}

scenario_e10() {
  title "E10 — Pico de volumen"
  say "El POST solo escribe en PostgreSQL (una TX); el procesamiento lo absorbe Kafka (6 particiones, key = nomination_id)."
  look "Grafana → Tráfico y negocio (altas/s), Latencia (p95 del POST), Asincronía (lag y outbox pendiente)"
  local before; before="$(metric nominations_resolved_total)"
  if [[ -x "${SCRIPT_DIR}/load-test.sh" ]]; then
    local args="${LOAD_TEST_ARGS:-300 30}"
    step "scripts/load-test.sh ${args}  (altas de 4 entidades, ~50% aprueba / ~50% rechaza, 10% reintentos del canal)"
    # shellcheck disable=SC2086
    if BASE_URL="${BASE_URL}" "${SCRIPT_DIR}/load-test.sh" ${args} 2>&1 | indent; then
      ok "load-test.sh: sin errores HTTP y todas en estado final"
    else
      fail "load-test.sh terminó con error"
    fi
    note "p95 de POST /v1/nominations y lag por consumer en Grafana; ningún 5xx esperado"
    finish E10 "pico absorbido: todas aceptadas y resueltas"
    return
  else
    local n=200 conc=20
    step "scripts/load-test.sh no existe: pico simple de ${n} altas con ${conc} en paralelo"
    local t0="${SECONDS}"
    local prefix; prefix="$(new_uuid | cut -c1-24)"   # request_id = prefijo + 12 dígitos del número de alta
    seq -f '%012g' 1 "${n}" | xargs -P "${conc}" -I{} curl -s -o /dev/null -w '%{http_code}\n' -X POST "${BASE_URL}/v1/nominations" \
      -H "Authorization: Bearer ${TOKEN}" -H 'Content-Type: application/json' \
      --data "{\"request_id\":\"${prefix}{}\",\"customer_id\":\"CUST-000123\",\"account_id\":\"0001234567890987654\",\"card_id\":\"tok_demo_ok_e10\"}" \
      > "${TMP_DIR}/e10.txt" || true
    local accepted; accepted="$(grep -c '^202$' "${TMP_DIR}/e10.txt" || true)"
    note "$(sort "${TMP_DIR}/e10.txt" | uniq -c | tr '\n' ' ') en $((SECONDS - t0))s"
    expect_eq "altas aceptadas (202)" "${accepted}" "${n}"
  fi
  step "Esperando que se resuelvan (nominations_resolved_total: APPROVED + REJECTED)"
  local start="${SECONDS}" resolved=0 prev=-1
  while (( SECONDS - start < 90 )); do
    resolved=$(( $(metric nominations_resolved_total) - before ))
    if [[ "${resolved}" != "${prev}" ]]; then
      printf '  %s%3ss%s  resueltas: %s/%s\n' "${DIM}" "$((SECONDS - start))" "${N}" "${resolved}" "${accepted}"
      prev="${resolved}"
    fi
    (( resolved >= accepted )) && break
    sleep 2
  done
  (( resolved >= accepted )) && ok "las ${accepted} altas se resolvieron" || fail "resueltas ${resolved}/${accepted} en 90s"
  note "p95 de POST /v1/nominations y lag por consumer en Grafana; ningún 5xx esperado"
  finish E10 "pico absorbido: todas aceptadas y resueltas"
}

# ── main ────────────────────────────────────────────────────────────────────────────────────────────────────
SELECTED=()
for arg in "$@"; do
  case "${arg}" in
    -h|--help) usage; exit 0 ;;
    [Ee][0-9]|[Ee]10) SELECTED+=("$(printf '%s' "${arg}" | tr '[:lower:]' '[:upper:]')") ;;
    *) echo "Escenario desconocido: ${arg} (E1..E10)" >&2; exit 1 ;;
  esac
done
[[ ${#SELECTED[@]} -gt 0 ]] || read -r -a SELECTED <<<"${ALL_SCENARIOS}"

if [[ "$(curl -s --max-time 5 "${BASE_URL}/actuator/health" | jq -r '.status // empty' 2>/dev/null)" != "UP" ]]; then
  echo "La app no responde UP en ${BASE_URL}/actuator/health. Levantarla con el perfil demo (docker compose up -d --build)." >&2
  exit 1
fi
TOKEN="$("${SCRIPT_DIR}/mint-token.sh" ENT01)"
OTHER_TOKEN="$("${SCRIPT_DIR}/mint-token.sh" ENT02)"
OPERATOR_TOKEN="$("${SCRIPT_DIR}/mint-token.sh" "" nominations:operate)"
detect_compose

title "Demo prisma-nominations-api · ${BASE_URL} · run ${RUN_ID}"
note "Escenarios: ${SELECTED[*]} · docker compose para E8/E9: $([[ ${COMPOSE_OK} == 1 ]] && echo sí || echo "no (${COMPOSE_WHY})")"
note "Correlation ids: demo-eN-${RUN_ID} (buscarlos en logs / historial)"
look "Swagger ${BASE_URL}/swagger-ui.html · Kafka UI :8081 · Jaeger :16686 · Prometheus :9090 · Grafana :3000"
DEMO_START="${SECONDS}"

for sc in "${SELECTED[@]}"; do
  SC_FAILS=0
  t0="${SECONDS}"
  "scenario_$(printf '%s' "${sc}" | tr '[:upper:]' '[:lower:]')" || fail "el escenario terminó con error"
  dvar="DURATION_${sc}"; printf -v "${dvar}" '%s' "$((SECONDS - t0))"
  rvar="RESULT_${sc}"
  [[ -n "${!rvar:-}" ]] || finish "${sc}" "terminó con error"
  pause
done

title "Resumen ($((SECONDS - DEMO_START))s)"
FAILED=0
for sc in "${SELECTED[@]}"; do
  rvar="RESULT_${sc}"; dvar="DETAIL_${sc}"; tvar="DURATION_${sc}"
  case "${!rvar}" in
    ok)   printf '  %s✓%s %-4s %s %s(%ss)%s\n' "${G}" "${N}" "${sc}" "${!dvar}" "${DIM}" "${!tvar}" "${N}" ;;
    skip) printf '  %s–%s %-4s saltado: %s\n' "${Y}" "${N}" "${sc}" "${!dvar}" ;;
    *)    printf '  %s✗%s %-4s %s %s(%ss)%s\n' "${R}" "${N}" "${sc}" "${!dvar}" "${DIM}" "${!tvar}" "${N}"; FAILED=1 ;;
  esac
done
exit "${FAILED}"
