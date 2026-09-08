# Точка входа в проект. Начни с `make demo`.
.DEFAULT_GOAL := help
SHELL := /bin/bash
# Без --env-file: все переменные стенда имеют дефолты прямо в compose
# (${VAR:-default}), поэтому `make demo` работает на чистой машине без
# подготовки. Переопределить порт — обычная переменная окружения:
#   APP_PORT=9090 make demo
COMPOSE := docker compose -f deploy/docker-compose.yml
MODULE := $(shell head -1 go.mod 2>/dev/null | cut -d' ' -f2)

.PHONY: help
help: ## показать эту справку
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ─── стенд ────────────────────────────────────────────────────────────────
.PHONY: demo
demo: ## поднять всё, создать демо-опрос, напечатать ссылки
	@$(COMPOSE) up -d --build --wait-timeout 300
	@scripts/wait-ready.sh
	@scripts/seed-demo.sh

.PHONY: up
up: ## поднять стенд без демо-данных
	@$(COMPOSE) up -d --build

.PHONY: down
down: ## остановить стенд и удалить тома
	@$(COMPOSE) down -v

.PHONY: logs
logs: ## хвост логов приложения
	@$(COMPOSE) logs -f app-1 app-2

.PHONY: redis-cli
redis-cli: ## redis-cli внутри сети кластера (снаружи будут MOVED в недоступные IP)
	@$(COMPOSE) exec redis-1 redis-cli -c

# ─── проверки ─────────────────────────────────────────────────────────────
.PHONY: test
test: ## unit-тесты с детектором гонок
	@go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## integration на testcontainers (Redis Cluster + Postgres)
	@go test -race -count=1 -tags=integration -timeout=10m ./...

.PHONY: cover
cover: ## покрытие
	@go test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## golangci-lint v2
	@golangci-lint run

.PHONY: vuln
vuln: ## проверка уязвимостей в зависимостях
	@go run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: check
check: lint test vuln ## всё, что гоняет CI

.PHONY: smoke
smoke: ## end-to-end: дедуп проверяется между двумя инстансами
	@scripts/smoke.sh

.PHONY: chaos
chaos: ## сценарии отказов; каждый заканчивается сверкой агрегата
	@scripts/chaos/redis-master.sh
	@scripts/chaos/consumer.sh
	@scripts/chaos/postgres.sh

.PHONY: load
load: ## k6: стоимость одного голоса + сверка суммы счётчиков
	@$(COMPOSE) --profile load run --rm \
	  -e BASE_URL=http://lb:8080 -e PEAK_RPS=$${PEAK_RPS:-500} \
	  k6 run /scripts/vote.js

# ─── спецификация ─────────────────────────────────────────────────────────
.PHONY: verify-requirements
verify-requirements: ## сверить имена тестов с docs/specs/acceptance.md
	@scripts/verify-requirements.sh

.PHONY: verify-invariants
verify-invariants: ## сломать каждый инвариант и убедиться, что тест краснеет
	@scripts/verify-invariants.sh

.PHONY: test-acceptance
test-acceptance: ## приёмочные тесты через HTTP (нужен поднятый стенд)
	@go test -count=1 -tags=acceptance -timeout=10m ./test/acceptance/...

# ─── разработка ───────────────────────────────────────────────────────────
.PHONY: build
build: ## собрать бинарь
	@CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/televote ./cmd/televote

.PHONY: migrate
migrate: ## применить миграции к локальной БД
	@go run github.com/pressly/goose/v3/cmd/goose@latest -dir migrations postgres "$$POSTGRES_DSN" up

.PHONY: module
module: ## сменить module path: make module OWNER=yourname
	@test -n "$(OWNER)" || (echo "укажи OWNER: make module OWNER=yourname" && exit 1)
	@grep -rl 'github.com/OWNER/televote' --include='*.go' --include='*.mod' --include='*.yml' . \
	  | xargs sed -i '' 's|github.com/OWNER/televote|github.com/$(OWNER)/televote|g'
	@echo "module path: github.com/$(OWNER)/televote"

.PHONY: tidy
tidy: ## go mod tidy + форматирование
	@go mod tidy && gofmt -w -s .
