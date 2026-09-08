#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

SLUG="smoke-$$"
VOTER="smoke-voter-$$-$(date +%s)"
step=0
ok() { step=$((step+1)); printf '  %d. %s\n' "$step" "$1"; }

token=$(admin_token)
[ -n "$token" ] || fail "не удалось войти в админку"

create_poll "$SLUG" "$token" "smoke?"
ok "опрос создан и открыт"

code=$(vote_on "$APP1_PORT" "$SLUG" "$VOTER")
[ "$code" = "202" ] || fail "app-1 ответил $code вместо 202"
ok "голос принят на app-1 (202 accepted, а не 200: посчитает консьюмер)"

code=$(vote_on "$APP2_PORT" "$SLUG" "$VOTER")
[ "$code" = "202" ] || fail "app-2 ответил $code вместо 202"
ok "тот же голосующий принят на app-2 — приём не дедуплицирует, это делает консьюмер"

count=""
for _ in $(seq 1 30); do
  sleep 1
  count=$(ballots "$SLUG" "$token")
  [ "${count:-0}" -ge 1 ] && break
done
[ "${count:-0}" = "1" ] || fail "в результатах ${count:-0} бюллетеней вместо 1 — дедуп между инстансами не сработал"
ok "после дренажа ровно ОДИН голос — дедуп не зависит от инстанса"

curl -fsS -X POST "${API}/admin/polls/${SLUG}/close" -H "Authorization: Bearer ${token}" >/dev/null

closed=""
for _ in $(seq 1 15); do
  sleep 1
  code=$(vote_on "$APP1_PORT" "$SLUG" "$VOTER")
  if [ "$code" = "409" ] || [ "$code" = "404" ]; then closed=$code; break; fi
done
[ -n "$closed" ] || fail "голос после закрытия принят с кодом $code"
ok "голос после закрытия отвергнут ($closed)"

code=$(curl -s -o /dev/null -w '%{http_code}' "${API}/admin/polls")
[ "$code" = "401" ] || fail "админка без токена ответила $code вместо 401"
ok "админка без токена недоступна (401)"

printf '\n  Smoke пройден.\n\n'
