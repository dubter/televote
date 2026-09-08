# ─── сборка ───────────────────────────────────────────────────────────────
FROM golang:1.27-alpine AS build
WORKDIR /src

# Слой зависимостей кэшируется отдельно от кода
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Статический бинарь: distroless-образ ниже не содержит libc
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/televote ./cmd/televote && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
      -o /out/migrate ./cmd/migrate

# ─── рантайм ──────────────────────────────────────────────────────────────
# distroless: без shell, без пакетного менеджера, минимум поверхности атаки
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/televote /televote
COPY --from=build /out/migrate /migrate
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/televote"]
