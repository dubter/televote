#!/usr/bin/env bash
# Сверяет docs/specs/acceptance.md с реальными именами тестов в репозитории.
# Трассировка не поддерживается руками — она вычисляется отсюда.
# Портируемо: bash 3.2 (macOS) и выше.
set -euo pipefail
cd "$(dirname "$0")/.."

SPEC=docs/specs/acceptance.md
[ -f "$SPEC" ] || { echo "нет $SPEC"; exit 1; }

tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT

# Имена тестов, обещанные спецификацией
{ grep -ohE '\bTest(FR|NFR)[0-9_]+[A-Za-z0-9_]*' "$SPEC" || true; } | sort -u > "$tmp/promised"
# Имена тестов, реально объявленные в коде
{ grep -rhoE '^func Test[A-Za-z0-9_]+' --include='*_test.go' . 2>/dev/null || true; } \
  | awk '{print $2}' | sort -u > "$tmp/actual"

comm -23 "$tmp/promised" "$tmp/actual" > "$tmp/missing"

# Покрытие требований: у каждого ID хотя бы один тест с его префиксом
: > "$tmp/uncovered"
for r in FR1 FR2 FR3 FR4 FR5 FR6 FR7 FR8 NFR1 NFR2 NFR3 NFR4 NFR5 NFR6 NFR7 NFR8 NFR9; do
  grep -q "^Test${r}_" "$tmp/actual" || echo "$r" >> "$tmp/uncovered"
done

n_promised=$(wc -l < "$tmp/promised" | tr -d ' ')
n_actual=$(wc -l < "$tmp/actual" | tr -d ' ')
n_missing=$(wc -l < "$tmp/missing" | tr -d ' ')
n_uncovered=$(wc -l < "$tmp/uncovered" | tr -d ' ')

printf 'Обещано спецификацией : %s\n' "$n_promised"
printf 'Объявлено в коде      : %s\n' "$n_actual"

if [ "$n_missing" -gt 0 ]; then
  printf '\nОБЕЩАНЫ, НО НЕ НАПИСАНЫ (%s):\n' "$n_missing"
  sed 's/^/  /' "$tmp/missing"
fi
if [ "$n_uncovered" -gt 0 ]; then
  printf '\nТРЕБОВАНИЯ БЕЗ ЕДИНОГО ТЕСТА (%s):\n' "$n_uncovered"
  sed 's/^/  /' "$tmp/uncovered"
fi

if [ "$n_missing" -gt 0 ] || [ "$n_uncovered" -gt 0 ]; then
  printf '\nСпецификация и код разошлись.\n'
  exit 1
fi
printf '\nВсе требования покрыты исполняемыми тестами.\n'
