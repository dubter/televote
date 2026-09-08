#!/usr/bin/env bash
# Создаёт демо-опрос и печатает ссылки.
#
# Опрос сразу открывается вручную: сервис запрещает создавать опрос, который
# открывается раньше чем через час (ёмкость под эфир поднимается заранее),
# поэтому для стенда переводим его в open отдельным вызовом.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${APP_PORT:=8080}"
: "${GRAFANA_PORT:=3000}"
: "${ADMIN_LOGIN:=admin}"
: "${ADMIN_PASSWORD:=dev-only-change-me}"
: "${SLUG:=demo}"

BASE="http://localhost:${APP_PORT}"
API="${BASE}/api/v1/admin"

token=$(curl -fsS -X POST "${API}/login" -H 'Content-Type: application/json' \
  -d "{\"login\":\"${ADMIN_LOGIN}\",\"password\":\"${ADMIN_PASSWORD}\"}" \
  | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')

if [ -z "$token" ]; then
  echo "не удалось войти в админку — проверь ADMIN_PASSWORD" >&2
  exit 1
fi

opens=$(date -u -v+2H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '+2 hours' '+%Y-%m-%dT%H:%M:%SZ')
closes=$(date -u -v+3H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '+3 hours' '+%Y-%m-%dT%H:%M:%SZ')

curl -fsS -X POST "${API}/polls" -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${token}" -d @- >/dev/null <<JSON || true
{
  "slug": "${SLUG}",
  "question": "Кто победит в финале?",
  "type": "single",
  "options": ["Первый", "Второй", "Третий"],
  "opens_at": "${opens}",
  "closes_at": "${closes}",
  "expected_audience": 100000000,
  "expected_conversion": 0.3
}
JSON

curl -fsS -X POST "${API}/polls/${SLUG}/open" -H "Authorization: Bearer ${token}" >/dev/null || true

cat <<INFO

  Голосование   ${BASE}/p/${SLUG}
  QR-код        ${BASE}/p/${SLUG}/qr.png
  Админка       ${BASE}/admin            ${ADMIN_LOGIN} / ${ADMIN_PASSWORD}
  Grafana       http://localhost:${GRAFANA_PORT}

  make smoke    проверить дедуп между инстансами
  make load     нагрузочный тест
  make down     остановить стенд

INFO
