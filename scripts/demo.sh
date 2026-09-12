#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/lib.sh

: "${GRAFANA_PORT:=3000}"
: "${SLUG:=demo}"

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

publish_poll() {
  local slug=$1 created opened
  created=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${API}/admin/polls" \
    -H 'Content-Type: application/json' -H "Authorization: Bearer ${token}" -d @- <<JSON
{"slug":"${slug}","question":"Кто победит в финале?","type":"single",
 "options":["Первый","Второй","Третий"],
 "opens_at":"$(window_start)","closes_at":"$(window_end)",
 "expected_audience":100000000,"expected_conversion":0.3}
JSON
  )
  case "$created" in
    200|201|409) ;;
    *) fail "опрос ${slug} не создан: HTTP ${created}" ;;
  esac

  opened=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${API}/admin/polls/${slug}/open" \
    -H "Authorization: Bearer ${token}")
  case "$opened" in
    200|204|409) ;;
    *) fail "опрос ${slug} не открыт: HTTP ${opened}" ;;
  esac

  for _ in 1 2 3 4 5 6 7 8 9 10; do
    curl -fsS "${API}/polls/${slug}" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}

if ! publish_poll "$SLUG"; then
  printf 'Опрос %s уже закрыт, создаю новый\n' "$SLUG"
  SLUG="${SLUG}-$(date -u +%H%M%S)"
  publish_poll "$SLUG" || fail "опрос ${SLUG} не отдаётся публичным API"
fi

cat <<INFO

  Голосование   ${BASE}/p/${SLUG}
  QR-код        ${BASE}/p/${SLUG}/qr.png
  Админка       ${BASE}/admin            ${ADMIN_LOGIN} / ${ADMIN_PASSWORD}
  Grafana       http://localhost:${GRAFANA_PORT}/d/televote

  make smoke    проверить дедуп между инстансами
  make load     нагрузочный тест
  make down     остановить стенд

INFO
