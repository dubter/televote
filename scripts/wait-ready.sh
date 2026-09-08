#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

: "${APP1_PORT:=8081}"
: "${APP2_PORT:=8082}"
DEADLINE=$(( $(date +%s) + ${WAIT_TIMEOUT:-180} ))

wait_one() {
  local name=$1 port=$2
  until curl -fsS "http://localhost:${port}/readyz" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$DEADLINE" ]; then
      printf '\n%s не поднялся. Последние логи:\n' "$name"
      docker compose -f deploy/docker-compose.yml logs --tail=40 "$name" || true
      exit 1
    fi
    printf '.'
    sleep 2
  done
  printf ' %s готов\n' "$name"
}

printf 'Ждём готовности'
wait_one app-1 "$APP1_PORT"
wait_one app-2 "$APP2_PORT"
