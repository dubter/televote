#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

redis_master() {
  printf '\n  chaos: падение мастера Redis\n\n'
  local slug="chaos-redis-$$" token=$1
  create_poll "$slug" "$token"

  local before during after total got
  before=$(cast_votes "$slug" 1 60)
  say "принято до отказа: ${before}"

  $COMPOSE stop -t 0 redis-1 >/dev/null 2>&1
  say "нода redis-1 остановлена"

  during=$(cast_votes "$slug" 61 140)
  say "принято во время отказа: ${during} — приём не зависит от Redis, голоса ждут в Kafka"

  $COMPOSE start redis-1 >/dev/null 2>&1
  say "нода redis-1 возвращена"
  sleep 10

  after=$(cast_votes "$slug" 141 200)
  total=$((before + during + after))

  got=$(await_drain "$slug" "$token" "$total")
  [ "$got" = "$total" ] || fail "посчитано ${got} из ${total} — голоса потеряны при failover"
  printf '\n  Пройден: %s принято, %s посчитано.\n' "$total" "$got"
}

consumer_rebalance() {
  printf '\n  chaos: перезапуск консьюмера и ребаланс группы\n\n'
  local slug="chaos-consumer-$$" token=$1
  create_poll "$slug" "$token"

  local sent got victim
  sent=$(cast_votes "$slug" 1 100)
  say "принято: ${sent}"

  victim=$($COMPOSE ps -q consumer | head -1)
  docker restart -t 0 "$victim" >/dev/null 2>&1
  say "реплика консьюмера перезапущена — часть сообщений придёт повторно"
  sleep 12

  got=$(await_drain "$slug" "$token" "$sent")
  [ "$got" = "$sent" ] || fail "посчитано ${got} вместо ${sent} — ребаланс исказил счёт"
  printf '\n  Пройден: %s принято, %s посчитано — повторная доставка не удвоила счёт.\n' "$sent" "$got"
}

postgres_down() {
  printf '\n  chaos: падение Postgres\n\n'
  local slug="chaos-pg-$$" token=$1
  create_poll "$slug" "$token"

  local before during total got
  before=$(cast_votes "$slug" 1 40)
  say "принято до отказа: ${before}"

  $COMPOSE stop -t 0 postgres >/dev/null 2>&1
  say "Postgres остановлен"

  during=$(cast_votes "$slug" 41 100)
  [ "$during" -gt 0 ] || fail "приём остановился без Postgres — он оказался на горячем пути"
  say "принято без Postgres: ${during} — приём его не касается"

  $COMPOSE start postgres >/dev/null 2>&1
  say "Postgres возвращён"
  sleep 12

  total=$((before + during))
  got=$(await_drain "$slug" "$token" "$total")
  [ "$got" = "$total" ] || fail "посчитано ${got} из ${total} — снапшоты не догнали"
  printf '\n  Пройден: %s принято, %s посчитано — снапшоты возобновились.\n' "$total" "$got"
}

TOKEN=$(admin_token)
[ -n "$TOKEN" ] || fail "не удалось войти в админку"

for scenario in ${*:-redis consumer postgres}; do
  case "$scenario" in
    redis)    redis_master "$TOKEN" ;;
    consumer) consumer_rebalance "$TOKEN" ;;
    postgres) postgres_down "$TOKEN" ;;
    *)        fail "неизвестный сценарий: ${scenario}" ;;
  esac
done
printf '\n'
