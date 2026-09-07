# ─── сборка ───────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS build
WORKDIR /src

# Слой зависимостей кэшируется отдельно от кода
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Статический бинарь: distroless-образ ниже не содержит libc
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w -X main.version=$(git describe --tags --always 2>/dev/null || echo dev)" \
      -o /out/televote ./cmd/televote

# ─── рантайм ──────────────────────────────────────────────────────────────
# distroless: без shell, без пакетного менеджера, минимум поверхности атаки
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/televote /televote
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/televote"]
