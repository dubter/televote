#!/usr/bin/env bash
# Падение Postgres под нагрузкой.
#
# Postgres вне горячего пути: его отказ обязан остановить админку, но не приём
# и не подсчёт. Проверяем именно это, а не то, что сервис отвечает.
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/chaos/lib.sh

printf '\n  chaos: падение Postgres\n\n'

TOKEN=$(admin_token); [ -n "$TOKEN" ] || fail "не удалось войти в админку"
SLUG="chaos-pg-$$"
create_poll "$SLUG" "$TOKEN"
say "опрос создан"

before=$(cast_votes "$SLUG" 1 40)
say "принято до отказа: ${before}"

$COMPOSE stop -t 0 postgres >/dev/null 2>&1
say "Postgres остановлен"

during=$(cast_votes "$SLUG" 41 100)
[ "$during" -gt 0 ] || fail "приём остановился без Postgres — он оказался на горячем пути"
say "принято без Postgres: ${during} — приём его не касается"

$COMPOSE start postgres >/dev/null 2>&1
say "Postgres возвращён"
sleep 12

total=$((before + during))
got=$(await_drain "$SLUG" "$TOKEN" "$total")
[ "$got" = "$total" ] || fail "посчитано ${got} из ${total} — снапшоты не догнали после возврата"

printf '\n  Пройден: %s принято, %s посчитано — снапшоты возобновились.\n\n' "$total" "$got"
