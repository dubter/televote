#!/usr/bin/env bash
set -euo pipefail

: "${APP_PORT:=8080}"
: "${APP1_PORT:=8081}"
: "${APP2_PORT:=8082}"
: "${ADMIN_LOGIN:=admin}"
: "${ADMIN_PASSWORD:=dev-only-change-me}"

BASE="http://localhost:${APP_PORT}"
API="${BASE}/api/v1"
COMPOSE="docker compose -f deploy/docker-compose.yml"

say()  { printf '  %s\n' "$1"; }
fail() { printf '\n  ПРОВАЛ: %s\n\n' "$1" >&2; exit 1; }

utc() { date -u "$@" '+%Y-%m-%dT%H:%M:%SZ'; }

window_start() { utc -v-1M 2>/dev/null || date -u -d '-1 minute' '+%Y-%m-%dT%H:%M:%SZ'; }
window_end()   { utc -v+1H 2>/dev/null || date -u -d '+1 hour'   '+%Y-%m-%dT%H:%M:%SZ'; }

admin_token() {
  curl -fsS -X POST "${API}/admin/login" -H 'Content-Type: application/json' \
    -d "{\"login\":\"${ADMIN_LOGIN}\",\"password\":\"${ADMIN_PASSWORD}\"}" \
    | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
}

create_poll() {
  local slug=$1 token=$2 question=${3:-chaos}
  curl -fsS -X POST "${API}/admin/polls" -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${token}" -d @- >/dev/null <<JSON
{"slug":"${slug}","question":"${question}","type":"single","options":["да","нет"],
 "opens_at":"$(window_start)","closes_at":"$(window_end)",
 "expected_audience":10000,"expected_conversion":0.3}
JSON
  curl -fsS -X POST "${API}/admin/polls/${slug}/open" -H "Authorization: Bearer ${token}" >/dev/null
  sleep 3
  say "опрос ${slug} создан и открыт"
}

vote_on() {
  local port=$1 slug=$2 voter=$3
  curl -s -o /dev/null -w '%{http_code}' -X POST \
    "http://localhost:${port}/api/v1/polls/${slug}/vote" \
    -H 'Content-Type: application/json' \
    -d "{\"choices\":[0],\"voter\":\"${voter}\"}"
}

cast_votes() {
  local slug=$1 from=$2 to=$3 ok=0 code
  for i in $(seq "$from" "$to"); do
    code=$(vote_on "$APP_PORT" "$slug" "chaos-${slug}-${i}" || echo 000)
    [ "$code" = "202" ] && ok=$((ok+1))
  done
  echo "$ok"
}

ballots() {
  curl -fsS "${API}/admin/polls/$1/results" -H "Authorization: Bearer $2" \
    | sed -n 's/.*"ballots":\([0-9]*\).*/\1/p'
}

await_drain() {
  local slug=$1 token=$2 want=$3 got=0
  for _ in $(seq 1 40); do
    sleep 3
    got=$(ballots "$slug" "$token")
    [ "${got:-0}" -ge "$want" ] && break
  done
  echo "${got:-0}"
}
