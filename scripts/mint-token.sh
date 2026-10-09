#!/usr/bin/env bash
# Genera un JWT HS256 de DEMO para la API de nominaciones (reemplaza al IdP corporativo en local).
#
# Uso:
#   scripts/mint-token.sh [entity_id] [scopes] [ttl_segundos]
#     entity_id     entidad financiera (claim entity_id). Vacío ("") = token sin entidad (p.ej. operador).
#                   Default: ENT01
#     scopes        separados por espacio. Default: "nominations:write nominations:read"
#     ttl_segundos  vigencia. Default: 3600 (1 h)
#
# Ejemplos:
#   TOKEN=$(scripts/mint-token.sh ENT01)
#   curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/nominations/<id>
#   OPERATOR=$(scripts/mint-token.sh "" nominations:operate)     # /internal/** y actuator
#
# Variables opcionales:
#   NOMINATIONS_SECURITY_JWT_SECRET  clave base64 (la misma que use la app). Default: la de demo de application.yml
#   JWT_ISSUER (default prisma-nominations-demo) · JWT_SUBJECT (default demo-channel)
#
# La clave de demo NO es productiva: en producción los tokens los emite el IdP (client credentials) con firma
# asimétrica y la API los valida contra su JWKS. Requiere: bash, openssl, od, tr.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_YML="${SCRIPT_DIR}/../app/src/main/resources/application.yml"

ENTITY_ID="${1-ENT01}"
SCOPES="${2:-nominations:write nominations:read}"
TTL="${3:-3600}"
ISSUER="${JWT_ISSUER:-prisma-nominations-demo}"
SUBJECT="${JWT_SUBJECT:-demo-channel}"

SECRET="${NOMINATIONS_SECURITY_JWT_SECRET:-}"
if [[ -z "${SECRET}" ]]; then
  # Default del placeholder ${NOMINATIONS_SECURITY_JWT_SECRET:<clave>} en application.yml (una sola fuente).
  SECRET="$(sed -n 's/.*secret: *\${NOMINATIONS_SECURITY_JWT_SECRET:\([^}]*\)}.*/\1/p' "${APP_YML}" | head -n 1)"
fi
[[ -n "${SECRET}" ]] || { echo "No se encontró la clave (NOMINATIONS_SECURITY_JWT_SECRET o ${APP_YML})" >&2; exit 1; }

# Valores acotados: van dentro del JSON sin escapar.
[[ -z "${ENTITY_ID}" || "${ENTITY_ID}" =~ ^[A-Za-z0-9_-]{1,20}$ ]] || { echo "entity_id inválido: [A-Za-z0-9_-]{1,20}" >&2; exit 1; }
[[ "${SCOPES}" =~ ^[A-Za-z0-9:._\ -]*$ ]] || { echo "scopes inválidos" >&2; exit 1; }
[[ "${TTL}" =~ ^[0-9]+$ ]] || { echo "ttl inválido" >&2; exit 1; }
[[ "${ISSUER}${SUBJECT}" =~ ^[A-Za-z0-9:._/-]+$ ]] || { echo "issuer/subject inválidos" >&2; exit 1; }

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

NOW="$(date +%s)"
EXP=$((NOW + TTL))
ENTITY_CLAIM=""
[[ -n "${ENTITY_ID}" ]] && ENTITY_CLAIM=",\"entity_id\":\"${ENTITY_ID}\""

HEADER='{"alg":"HS256","typ":"JWT"}'
PAYLOAD="{\"iss\":\"${ISSUER}\",\"sub\":\"${SUBJECT}\",\"iat\":${NOW},\"exp\":${EXP},\"scope\":\"${SCOPES}\"${ENTITY_CLAIM}}"

SIGNING_INPUT="$(printf '%s' "${HEADER}" | b64url).$(printf '%s' "${PAYLOAD}" | b64url)"
KEY_HEX="$(printf '%s' "${SECRET}" | openssl base64 -d -A | od -An -v -tx1 | tr -d ' \n')"
SIGNATURE="$(printf '%s' "${SIGNING_INPUT}" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:${KEY_HEX}" -binary | b64url)"

printf '%s.%s\n' "${SIGNING_INPUT}" "${SIGNATURE}"
