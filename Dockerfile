# ─── сборка ───────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS build
WORKDIR /src

# Слой зависимостей кэшируется отдельно от кода
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Статический бинарь: distroless-образ ниже не содержит libc
ARG VERSION=dev
ENV LDFLAGS="-s -w -X github.com/dubter/televote/internal/app.Version=${VERSION}"
RUN set -e; \
    for c in televote televote-api televote-consumer migrate; do \
      CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/$c ./cmd/$c; \
    done

# ─── рантайм ──────────────────────────────────────────────────────────────
# distroless: без shell, без пакетного менеджера, минимум поверхности атаки
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/televote /televote
COPY --from=build /out/televote-api /televote-api
COPY --from=build /out/televote-consumer /televote-consumer
COPY --from=build /out/migrate /migrate
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/televote"]
