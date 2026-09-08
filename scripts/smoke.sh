#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

: "${APP_PORT:=8080}"
: "${APP1_PORT:=8081}"
: "${APP2_PORT:=8082}"
: "${ADMIN_LOGIN:=admin}"
: "${ADMIN_PASSWORD:=dev-only-change-me}"

BASE="http://localhost:${APP_PORT}"
API="${BASE}/api/v1"
SLUG="smoke-$$"
VOTER="smoke-voter-$$-$(date +%s)"
step=0

ok()   { step=$((step+1)); printf '  %d. %s\n' "$step" "$1"; }
fail() { printf '\n  ПРОВАЛ: %s\n' "$1" >&2; exit 1; }

token=$(curl -fsS -X POST "${API}/admin/login" -H 'Content-Type: application/json' \
  -d "{\"login\":\"${ADMIN_LOGIN}\",\"password\":\"${ADMIN_PASSWORD}\"}" \
  | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
[ -n "$token" ] || fail "не удалось войти в админку"

opens=$(date -u -v-1M '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '-1 minute' '+%Y-%m-%dT%H:%M:%SZ')
closes=$(date -u -v+1H '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date -u -d '+1 hour' '+%Y-%m-%dT%H:%M:%SZ')

curl -fsS -X POST "${API}/admin/polls" -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${token}" -d @- >/dev/null <<JSON || fail "опрос не создан"
{"slug":"${SLUG}","question":"smoke?","type":"single","options":["да","нет"],
 "opens_at":"${opens}","closes_at":"${closes}",
 "expected_audience":1000,"expected_conversion":0.3}
JSON
ok "опрос создан"

curl -fsS -X POST "${API}/admin/polls/${SLUG}/open" -H "Authorization: Bearer ${token}" >/dev/null \
  || fail "опрос не открылся"
ok "опрос открыт"

sleep 3

vote_on() {
  curl -s -o /dev/null -w '%{http_code}' -X POST \
    "http://localhost:$1/api/v1/polls/${SLUG}/vote" \
    -H 'Content-Type: application/json' \
    -d "{\"choices\":[0],\"voter\":\"${VOTER}\"}"
}

code=$(vote_on "$APP1_PORT"); [ "$code" = "202" ] || fail "app-1 ответил $code вместо 202"
ok "голос принят на app-1 (202 accepted, а не 200: посчитает консьюмер)"

code=$(vote_on "$APP2_PORT"); [ "$code" = "202" ] || fail "app-2 ответил $code вместо 202"
ok "тот же голосующий принят на app-2 — приём не дедуплицирует, это делает консьюмер"

ballots=""
for _ in $(seq 1 30); do
  sleep 1
  ballots=$(curl -fsS "${API}/admin/polls/${SLUG}/results" -H "Authorization: Bearer ${token}" \
    | sed -n 's/.*"ballots":\([0-9]*\).*/\1/p')
  [ "${ballots:-0}" -ge 1 ] && break
done

[ "${ballots:-0}" = "1" ] || fail "в результатах ${ballots:-0} бюллетеней вместо 1 — дедуп между инстансами не сработал"
ok "после дренажа ровно ОДИН голос — дедуп не зависит от инстанса"

curl -fsS -X POST "${API}/admin/polls/${SLUG}/close" -H "Authorization: Bearer ${token}" >/dev/null

closed=""
for _ in $(seq 1 15); do
  sleep 1
  code=$(vote_on "$APP1_PORT")
  if [ "$code" = "409" ] || [ "$code" = "404" ]; then closed=$code; break; fi
done
[ -n "$closed" ] || fail "голос после закрытия принят с кодом $code"
ok "голос после закрытия отвергнут ($closed)"

code=$(curl -s -o /dev/null -w '%{http_code}' "${API}/admin/polls")
[ "$code" = "401" ] || fail "админка без токена ответила $code вместо 401"
ok "админка без токена недоступна (401)"

printf '\n  Smoke пройден.\n\n'
