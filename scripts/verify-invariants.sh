#!/usr/bin/env bash
# Для каждого инварианта: ломаем реализацию, ждём КРАСНОГО теста, откатываем.
# Зелёный тест после мутации означает, что инвариант не защищён.
set -uo pipefail
cd "$(dirname "$0")/.."

MANIFEST=scripts/invariants.tsv
pass=0 fail=0 skip=0

printf '%-28s %-42s %s\n' "ИНВАРИАНТ" "ТЕСТ" "РЕЗУЛЬТАТ"
printf '%.0s─' {1..92}; printf '\n'

while IFS=$'\t' read -r name file mutation test; do
  [[ $name == \#* || -z ${name:-} ]] && continue

  if [[ ! -f $file ]]; then
    printf '%-28s %-42s %s\n' "$name" "$test" "пропуск — нет $file"
    ((skip++)); continue
  fi

  cp "$file" "$file.bak"
  if ! sed -i.tmp -E "$mutation" "$file" 2>/dev/null || cmp -s "$file" "$file.bak"; then
    mv "$file.bak" "$file"; rm -f "$file.tmp"
    printf '%-28s %-42s %s\n' "$name" "$test" "пропуск — мутация не применилась"
    ((skip++)); continue
  fi
  rm -f "$file.tmp"

  # Тест ОБЯЗАН упасть на сломанной реализации
  if go test -count=1 -run "^${test}$" ./... >/dev/null 2>&1; then
    printf '%-28s %-42s %s\n' "$name" "$test" "ПРОВАЛ — тест зелёный на сломанном коде"
    ((fail++))
  else
    printf '%-28s %-42s %s\n' "$name" "$test" "ok — покраснел"
    ((pass++))
  fi

  mv "$file.bak" "$file"
done < "$MANIFEST"

printf '\nзащищено %d · не защищено %d · пропущено %d\n' "$pass" "$fail" "$skip"
((fail == 0)) || { printf 'Инварианты без защиты — это дефект, а не стиль.\n'; exit 1; }
