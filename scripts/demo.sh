#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

: "${GRAFANA_PORT:=3000}"
: "${SLUG:=demo-$(date -u +%H%M%S)}"

DEADLINE=$(( $(date +%s) + ${WAIT_TIMEOUT:-180} ))

wait_ready() {
  local name=$1 port=$2
  until curl -fsS "http://localhost:${port}/readyz" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
      printf '\n%s не поднялся. Последние логи:\n' "$name"
      $COMPOSE logs --tail=40 "$name" || true
      exit 1
    fi
    printf '.'
    sleep 2
  done
  printf ' %s готов\n' "$name"
}

printf 'Ждём готовности'
wait_ready app-1 "$APP1_PORT"
wait_ready app-2 "$APP2_PORT"
wait_ready lb "$APP_PORT"

token=$(admin_token)
[ -n "$token" ] || fail "не удалось войти в админку — проверь ADMIN_PASSWORD"

create_poll "$SLUG" "$token" "Кто победит в финале?" '["Первый","Второй","Третий"]' 100000000

cat <<INFO

  Голосование   ${BASE}/p/${SLUG}
  QR-код        ${BASE}/p/${SLUG}/qr.png
  Админка       ${BASE}/admin            ${ADMIN_LOGIN} / ${ADMIN_PASSWORD}
  Grafana       http://localhost:${GRAFANA_PORT}/d/televote

  make smoke    проверить дедуп между инстансами
  make load     нагрузочный тест
  make down     остановить стенд

INFO
