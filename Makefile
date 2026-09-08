# Точка входа в проект. Начни с `make demo`.
.DEFAULT_GOAL := help
SHELL := /bin/bash
# Без --env-file: все переменные стенда имеют дефолты прямо в compose
# (${VAR:-default}), поэтому `make demo` работает на чистой машине без
# подготовки. Переопределить порт — обычная переменная окружения:
#   APP_PORT=9090 make demo
COMPOSE := docker compose -f deploy/docker-compose.yml
# Версия совпадает с CI: линтер, который проходит локально, обязан пройти и там.
LINTER := golangci/golangci-lint:v2.13.2
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
	@$(COMPOSE) logs -f app-1 app-2 consumer snapshot

.PHONY: redis-cli
redis-cli: ## redis-cli внутри сети кластера (снаружи будут MOVED в недоступные IP)
	@$(COMPOSE) exec redis-1 redis-cli -c

# ─── проверки ─────────────────────────────────────────────────────────────
.PHONY: test
test: ## unit-тесты с детектором гонок
	@go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## integration на настоящих Postgres и Redis (testcontainers)
	@go test -count=1 -tags=integration -timeout=12m ./...

.PHONY: cover
cover: ## покрытие
	@go test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## golangci-lint в докере, версия та же, что в CI
	@docker run --rm -v "$(PWD)":/app -v "$(HOME)/go/pkg/mod":/go/pkg/mod \
	  -w /app $(LINTER) golangci-lint run --timeout 10m

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
	  -e SLUG=load-$$(date +%s) \
	  k6 run /scripts/vote.js

# ─── разработка ───────────────────────────────────────────────────────────
.PHONY: build
build: ## собрать бинарь
	@CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/televote ./cmd/televote

.PHONY: migrate
migrate: ## применить миграции к локальной БД
	@go run github.com/pressly/goose/v3/cmd/goose@latest -dir migrations postgres "$$POSTGRES_DSN" up

.PHONY: tidy
tidy: ## go mod tidy + форматирование
	@go mod tidy && gofmt -w -s .
