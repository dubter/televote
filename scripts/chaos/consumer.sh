#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/chaos/lib.sh

printf '\n  chaos: перезапуск консьюмера и ребаланс группы\n\n'

TOKEN=$(admin_token); [ -n "$TOKEN" ] || fail "не удалось войти в админку"
SLUG="chaos-consumer-$$"
create_poll "$SLUG" "$TOKEN"
say "опрос создан"

sent=$(cast_votes "$SLUG" 1 100)
say "принято: ${sent}"

victim=$($COMPOSE ps -q consumer | head -1)
docker restart -t 0 "$victim" >/dev/null 2>&1
say "реплика консьюмера перезапущена — группа ребалансится, часть сообщений придёт повторно"
sleep 12

got=$(await_drain "$SLUG" "$TOKEN" "$sent")
[ "$got" = "$sent" ] || fail "посчитано ${got} вместо ${sent} — ребаланс исказил счёт"

printf '\n  Пройден: %s принято, %s посчитано — повторная доставка не удвоила счёт.\n\n' "$sent" "$got"
