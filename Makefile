.DEFAULT_GOAL := help
SHELL := /bin/bash
COMPOSE := docker compose -f deploy/docker-compose.yml
LINTER := golangci/golangci-lint:v2.13.2

.PHONY: help
help: ## показать эту справку
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: demo
demo: ## поднять всё, создать демо-опрос, напечатать ссылки
	@$(COMPOSE) up -d --build --wait-timeout 300
	@scripts/demo.sh

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

.PHONY: test
test: ## unit-тесты с детектором гонок
	@go test -race -count=1 ./...

.PHONY: test-integration
test-integration: ## integration на настоящих Postgres и Redis (testcontainers)
	@go test -count=1 -tags=integration -timeout=12m ./...

.PHONY: cover
cover: ## покрытие без сгенерированных моков
	@go test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
	@grep -v '/mocks/' coverage.out > coverage.filtered && mv coverage.filtered coverage.out
	@go tool cover -func=coverage.out | tail -1

.PHONY: mocks
mocks: ## перегенерировать моки (mockgen)
	@go run go.uber.org/mock/mockgen@latest -version >/dev/null 2>&1 || true
	@PATH="$(HOME)/go/bin:$$PATH" go generate ./...

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
	@scripts/chaos.sh

.PHONY: load
load: ## k6: стоимость одного голоса + сверка суммы счётчиков
	@$(COMPOSE) --profile load run --rm \
	  -e BASE_URL=http://lb:8080 -e PEAK_RPS=$${PEAK_RPS:-500} \
	  -e SLUG=load-$$(date +%s) \
	  k6 run /scripts/vote.js

.PHONY: build
build: ## собрать все бинарники
	@go build -o bin/ ./cmd/...

.PHONY: migrate
migrate: ## применить миграции к локальной БД
	@go run github.com/pressly/goose/v3/cmd/goose@latest -dir migrations postgres "$$POSTGRES_DSN" up

.PHONY: tidy
tidy: ## go mod tidy + форматирование
	@go mod tidy && gofmt -w -s .
