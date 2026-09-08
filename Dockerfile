FROM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
ENV LDFLAGS="-s -w -X github.com/dubter/televote/internal/app.Version=${VERSION}"
RUN set -e; \
    for c in televote-api televote-consumer televote-snapshot migrate; do \
      CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="$LDFLAGS" -o /out/$c ./cmd/$c; \
    done

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/televote-api /televote-api
COPY --from=build /out/televote-consumer /televote-consumer
COPY --from=build /out/televote-snapshot /televote-snapshot
COPY --from=build /out/migrate /migrate
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/televote-api"]
