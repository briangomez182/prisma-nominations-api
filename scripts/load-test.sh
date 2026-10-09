#!/usr/bin/env bash
# Prueba de carga simple para mostrar E10 (pico de volumen) en vivo, junto con el tablero de Grafana.
#
# Uso:
#   scripts/load-test.sh [total] [concurrencia]
#     total         solicitudes nuevas a enviar (request_id distintos). Default: 500
#     concurrencia  POST simultáneos. Default: 20
#
# Ejemplos:
#   scripts/load-test.sh                          # 500 altas, 20 en paralelo, contra http://localhost:8080
#   scripts/load-test.sh 2000 50
#   BASE_URL=http://localhost:18082 RETRY_PCT=0 scripts/load-test.sh 300 30
#
# Qué hace:
#   1. Genera un token por entidad con scripts/mint-token.sh (ENTITIES entidades: LOAD01, LOAD02, ...).
#   2. Envía las altas con card_id variado: ~50% aprueba, ~50% rechaza (escenarios REJECT del simulador de ABM,
#      ver docs/abm-mock.md). Un RETRY_PCT% se reenvía con el mismo request_id, en la misma ráfaga (reintento del
#      canal): debe volver 202 con Idempotent-Replayed: true y el mismo nomination_id.
#   3. Resume: aceptadas, replays, errores (por código HTTP), latencia p50/p95/máx del POST y throughput.
#   4. Consulta GET /v1/nominations/{id} hasta que todas lleguen a estado final (APPROVED/REJECTED) o venza
#      WAIT_SECONDS, e informa cuántas terminaron y el tiempo de drenado.
#
# Variables opcionales:
#   BASE_URL      default http://localhost:8080
#   ENTITIES      entidades (canales) distintas. Default: 4
#   RETRY_PCT     % de solicitudes reenviadas con el mismo request_id. Default: 10
#   WAIT_SECONDS  espera máxima del estado final. Default: 120
#
# Código de salida: 0 si no hubo errores HTTP y todas llegaron a estado final; 1 en otro caso.
# Requiere: bash, curl, jq, awk, xargs, uuidgen, perl (estándar en macOS) y openssl (para mint-token.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOTAL="${1:-500}"
CONCURRENCY="${2:-20}"
BASE_URL="${BASE_URL:-http://localhost:8080}"
ENTITIES="${ENTITIES:-4}"
RETRY_PCT="${RETRY_PCT:-10}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"

for n in "${TOTAL}" "${CONCURRENCY}" "${ENTITIES}" "${RETRY_PCT}" "${WAIT_SECONDS}"; do
  [[ "${n}" =~ ^[0-9]+$ ]] || { echo "Parámetro numérico inválido: ${n}" >&2; exit 2; }
done
(( TOTAL > 0 && CONCURRENCY > 0 && ENTITIES > 0 && ENTITIES <= 99 && RETRY_PCT <= 100 )) || { echo "Valores fuera de rango" >&2; exit 2; }
for tool in curl jq awk xargs uuidgen perl; do
  command -v "${tool}" >/dev/null || { echo "Falta ${tool}" >&2; exit 2; }
done

WORK="$(mktemp -d "${TMPDIR:-/tmp}/load-test.XXXXXX")"
trap 'rm -rf "${WORK}"' EXIT
mkdir -p "${WORK}/tokens" "${WORK}/out"

curl -sf -o /dev/null "${BASE_URL}/actuator/health/readiness" \
  || { echo "La API no responde en ${BASE_URL} (actuator/health/readiness)" >&2; exit 2; }

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time*1000'; }

# ------------------------------------------------------------------ tokens y plan
for ((e = 1; e <= ENTITIES; e++)); do
  entity="$(printf 'LOAD%02d' "${e}")"
  "${SCRIPT_DIR}/mint-token.sh" "${entity}" > "${WORK}/tokens/${entity}"
done

RUN="$(uuidgen | tr -d '-' | tr 'A-Z' 'a-z' | cut -c1-8)"
CARDS=(ok REJECT_030 ok REJECT_010 ok REJECT)   # mitad aprueba, mitad rechaza
PLAN="${WORK}/plan.txt"                           # call_id entity request_id card_id correlation_id
: > "${PLAN}"
for ((i = 0; i < TOTAL; i++)); do
  entity="$(printf 'LOAD%02d' $(( i % ENTITIES + 1 )))"
  request_id="$(uuidgen | tr 'A-Z' 'a-z')"
  card="tok_load_${CARDS[$(( i % ${#CARDS[@]} ))]}_${RUN}_${i}"
  correlation="load-${RUN}-${i}"
  echo "${i} ${entity} ${request_id} ${card} ${correlation}" >> "${PLAN}"
  if (( (i * RETRY_PCT) % 100 < RETRY_PCT )); then   # ~RETRY_PCT% repartido en toda la corrida
    echo "${i}r ${entity} ${request_id} ${card} ${correlation}" >> "${PLAN}"
  fi
done
# Mezcla: los reintentos viajan en la misma ráfaga que el original, no al final.
awk 'BEGIN{srand()} {print rand() "\t" $0}' "${PLAN}" | sort -k1,1 | cut -f2- > "${PLAN}.shuffled"
CALLS="$(wc -l < "${PLAN}" | tr -d ' ')"

# ------------------------------------------------------------------ POST concurrentes
post_one() {
  local call_id="$1" entity="$2" request_id="$3" card="$4" correlation="$5"
  local body
  body="$(printf '{"request_id":"%s","customer_id":"CUST-LOAD","account_id":"0001234567890987654","card_id":"%s","alias":"LOAD TEST"}' \
    "${request_id}" "${card}")"
  local meta
  meta="$(curl -s -o "${WORK}/out/${call_id}.body" -D "${WORK}/out/${call_id}.headers" \
    -w '%{http_code} %{time_total}' --max-time 30 \
    -X POST "${BASE_URL}/v1/nominations" \
    -H "Authorization: Bearer $(cat "${WORK}/tokens/${entity}")" \
    -H 'Content-Type: application/json' \
    -H "X-Correlation-Id: ${correlation}" \
    --data "${body}")" || meta="000 0"
  echo "${meta}" > "${WORK}/out/${call_id}.meta"
}
export -f post_one
export WORK BASE_URL

echo "Enviando ${CALLS} POST (${TOTAL} altas + $(( CALLS - TOTAL )) reintentos) a ${BASE_URL} con concurrencia ${CONCURRENCY}..."
START_MS="$(now_ms)"
xargs -P "${CONCURRENCY}" -L 1 bash -c 'post_one "$@"' _ < "${PLAN}.shuffled"
END_MS="$(now_ms)"
ELAPSED_MS=$(( END_MS - START_MS ))

# ------------------------------------------------------------------ resumen de la ráfaga
RESULTS="${WORK}/results.txt"   # call_id entity http_code seconds replayed nomination_id
: > "${RESULTS}"
while read -r call_id entity _ _ _; do
  read -r code seconds < "${WORK}/out/${call_id}.meta"
  replayed="$(awk -F': *' 'tolower($1)=="idempotent-replayed"{gsub("\r","",$2); print $2}' \
    "${WORK}/out/${call_id}.headers" 2>/dev/null || true)"
  nomination_id="-"
  if [[ "${code}" == "202" ]]; then
    nomination_id="$(jq -r '.nomination_id // "-"' "${WORK}/out/${call_id}.body" 2>/dev/null || echo -)"
  fi
  echo "${call_id} ${entity} ${code} ${seconds} ${replayed:-false} ${nomination_id}" >> "${RESULTS}"
done < "${PLAN}"

ACCEPTED="$(awk '$3=="202" && $5!="true"' "${RESULTS}" | wc -l | tr -d ' ')"
REPLAYED="$(awk '$3=="202" && $5=="true"' "${RESULTS}" | wc -l | tr -d ' ')"
ERRORS="$(awk '$3!="202"' "${RESULTS}" | wc -l | tr -d ' ')"
UNIQUE_IDS="$(awk '$3=="202"{print $6}' "${RESULTS}" | sort -u | wc -l | tr -d ' ')"
# Un reintento debe resolver al mismo nomination_id que su original.
MISMATCHED="$(awk '$3=="202"{id=$1; sub(/r$/,"",id); if (id in seen && seen[id]!=$6) bad++; seen[id]=$6} END{print bad+0}' "${RESULTS}")"

echo
echo "== Ráfaga =="
printf '  POST enviados        %s en %.1f s (%.0f req/s)\n' "${CALLS}" "$(echo "${ELAPSED_MS}" | awk '{print $1/1000}')" \
  "$(awk -v n="${CALLS}" -v ms="${ELAPSED_MS}" 'BEGIN{print (ms>0? n*1000/ms : 0)}')"
echo "  aceptadas (202)      ${ACCEPTED}"
echo "  replays (202)        ${REPLAYED}   (Idempotent-Replayed: true)"
echo "  nominaciones únicas  ${UNIQUE_IDS} de ${TOTAL} esperadas; reintentos con otro id: ${MISMATCHED}"
echo "  errores              ${ERRORS}"
if (( ERRORS > 0 )); then
  awk '$3!="202"{c[$3]++} END{for (k in c) printf "    HTTP %s: %d\n", k, c[k]}' "${RESULTS}"
fi
awk '{print $4}' "${RESULTS}" | sort -n | awk '
  {t[NR]=$1}
  END{
    if (NR==0) exit;
    p50=t[int((NR-1)*0.50)+1]; p95=t[int((NR-1)*0.95)+1];
    printf "  latencia POST        p50 %.0f ms · p95 %.0f ms · máx %.0f ms\n", p50*1000, p95*1000, t[NR]*1000
  }'

# ------------------------------------------------------------------ estado final por GET
PENDING="${WORK}/pending.txt"    # entity nomination_id (únicos)
awk '$3=="202" && $6!="-"{print $2, $6}' "${RESULTS}" | sort -u -k2,2 > "${PENDING}"
TRACKED="$(wc -l < "${PENDING}" | tr -d ' ')"
: > "${WORK}/final.txt"

get_one() {
  local entity="$1" id="$2" status
  status="$(curl -s --max-time 10 -H "Authorization: Bearer $(cat "${WORK}/tokens/${entity}")" \
    "${BASE_URL}/v1/nominations/${id}" | jq -r '.status // "UNKNOWN"' 2>/dev/null || echo UNKNOWN)"
  echo "${entity} ${id} ${status}"
}
export -f get_one

echo
echo "Esperando estado final de ${TRACKED} nominaciones (máx ${WAIT_SECONDS} s)..."
DRAIN_START_MS="$(now_ms)"
DEADLINE=$(( $(date +%s) + WAIT_SECONDS ))
while [[ -s "${PENDING}" ]]; do
  xargs -P "${CONCURRENCY}" -L 1 bash -c 'get_one "$@"' _ < "${PENDING}" > "${WORK}/statuses.txt"
  awk '$3=="APPROVED" || $3=="REJECTED"' "${WORK}/statuses.txt" >> "${WORK}/final.txt"
  awk '$3!="APPROVED" && $3!="REJECTED"{print $1, $2}' "${WORK}/statuses.txt" > "${PENDING}"
  done_count="$(wc -l < "${WORK}/final.txt" | tr -d ' ')"
  printf '\r  finales: %s/%s' "${done_count}" "${TRACKED}"
  [[ -s "${PENDING}" ]] || break
  (( $(date +%s) < DEADLINE )) || break
  sleep 2
done
DRAIN_MS=$(( $(now_ms) - DRAIN_START_MS ))
echo

FINAL_COUNT="$(wc -l < "${WORK}/final.txt" | tr -d ' ')"
APPROVED="$(awk '$3=="APPROVED"' "${WORK}/final.txt" | wc -l | tr -d ' ')"
REJECTED="$(awk '$3=="REJECTED"' "${WORK}/final.txt" | wc -l | tr -d ' ')"
NOT_FINAL="$(wc -l < "${PENDING}" | tr -d ' ')"

echo
echo "== Procesamiento asincrónico =="
echo "  en estado final      ${FINAL_COUNT} de ${TRACKED} (APPROVED ${APPROVED} · REJECTED ${REJECTED})"
printf '  tiempo de drenado    %.1f s desde el fin de la ráfaga\n' "$(echo "${DRAIN_MS}" | awk '{print $1/1000}')"
if (( NOT_FINAL > 0 )); then
  echo "  sin estado final     ${NOT_FINAL}:"
  awk '{c[$3]++} END{for (k in c) printf "    %s: %d\n", k, c[k]}' "${WORK}/statuses.txt"
fi
echo
echo "Ver en Grafana: tasa de altas, latencia p95, nominations_open y outbox pendiente (docs/observability.md)."

(( ERRORS == 0 && MISMATCHED == 0 && UNIQUE_IDS == TOTAL && NOT_FINAL == 0 ))
