#!/usr/bin/env bash
# Общая обвязка chaos-сценариев.
#
# Каждый сценарий заканчивается СВЕРКОЙ АГРЕГАТА, а не проверкой «сервис
# отвечает». Отвечать 202 и молча терять голоса — ровно тот отказ, ради
# которого эти сценарии и написаны.
set -euo pipefail

: "${APP_PORT:=8080}"
: "${ADMIN_LOGIN:=admin}"
: "${ADMIN_PASSWORD:=dev-only-change-me}"

BASE="http://localhost:${APP_PORT}"
API="${BASE}/api/v1"
COMPOSE="docker compose -f deploy/docker-compose.yml"

say()  { printf '  %s\n' "$1"; }
fail() { printf '\n  ПРОВАЛ: %s\n\n' "$1" >&2; exit 1; }

admin_token() {
  curl -fsS -X POST "${API}/admin/login" -H 'Content-Type: application/json' \
    -d "{\"login\":\"${ADMIN_LOGIN}\",\"password\":\"${ADMIN_PASSWORD}\"}" \
    | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
}

# create_poll <slug> <token> — опрос, открытый прямо сейчас.
create_poll() {
  local slug=$1 token=$2
  local opens closes
  opens=$(date -u -v-1M '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '-1 minute' '+%Y-%m-%dT%H:%M:%SZ')
  closes=$(date -u -v+1H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '+1 hour' '+%Y-%m-%dT%H:%M:%SZ')

  curl -fsS -X POST "${API}/admin/polls" -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${token}" -d @- >/dev/null <<JSON
{"slug":"${slug}","question":"chaos","type":"single","options":["да","нет"],
 "opens_at":"${opens}","closes_at":"${closes}",
 "expected_audience":10000,"expected_conversion":0.3}
JSON
  curl -fsS -X POST "${API}/admin/polls/${slug}/open" -H "Authorization: Bearer ${token}" >/dev/null
  sleep 3   # конфиг разъезжается по инстансам фоновым рефрешером
}

# cast_votes <slug> <from> <to> — возвращает число принятых (202).
cast_votes() {
  local slug=$1 from=$2 to=$3 ok=0 code
  for i in $(seq "$from" "$to"); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${API}/polls/${slug}/vote" \
      -H 'Content-Type: application/json' \
      -d "{\"choices\":[0],\"voter\":\"chaos-${slug}-${i}\"}" || echo 000)
    [ "$code" = "202" ] && ok=$((ok+1))
  done
  echo "$ok"
}

# ballots <slug> <token>
ballots() {
  curl -fsS "${API}/admin/polls/$1/results" -H "Authorization: Bearer $2" \
    | sed -n 's/.*"ballots":\([0-9]*\).*/\1/p'
}

# await_drain <slug> <token> <expected> — ждёт, пока агрегат догонит приём.
await_drain() {
  local slug=$1 token=$2 want=$3 got=0
  for _ in $(seq 1 40); do
    sleep 3
    got=$(ballots "$slug" "$token")
    [ "${got:-0}" -ge "$want" ] && break
  done
  echo "${got:-0}"
}
