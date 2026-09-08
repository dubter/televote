#!/usr/bin/env bash
# Падение мастера Redis под нагрузкой.
#
# Проверяем не то, что сервис отвечает, а то, что после failover СУММА
# СЧЁТЧИКОВ СХОДИТСЯ с числом принятых голосов. Kafka держит голоса, пока
# шард недоступен, поэтому отказ Redis обязан оказаться задержкой, а не потерей.
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/chaos/lib.sh

printf '\n  chaos: падение мастера Redis\n\n'

TOKEN=$(admin_token); [ -n "$TOKEN" ] || fail "не удалось войти в админку"
SLUG="chaos-redis-$$"
create_poll "$SLUG" "$TOKEN"
say "опрос создан"

before=$(cast_votes "$SLUG" 1 60)
say "принято до отказа: ${before}"

victim=$($COMPOSE exec -T redis-1 redis-cli cluster info 2>/dev/null | grep -q 'cluster_state:ok' && echo redis-1 || echo redis-1)
$COMPOSE stop -t 0 "$victim" >/dev/null 2>&1
say "нода ${victim} остановлена"

during=$(cast_votes "$SLUG" 61 140)
say "принято во время отказа: ${during} (приём не зависит от Redis — голоса ждут в Kafka)"

$COMPOSE start "$victim" >/dev/null 2>&1
say "нода ${victim} возвращена"
sleep 10

after=$(cast_votes "$SLUG" 141 200)
total=$((before + during + after))
say "принято всего: ${total}"

got=$(await_drain "$SLUG" "$TOKEN" "$total")
[ "$got" = "$total" ] || fail "посчитано ${got} из ${total} — голоса потеряны при failover"

printf '\n  Пройден: %s голосов принято, %s посчитано.\n\n' "$total" "$got"
