FROM golang:1.26-alpine AS build
RUN apk add --no-cache git
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN set -e; \
    for c in api consumer snapshot migrate; do \
      CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=true -ldflags="-s -w" -o /out/$c ./cmd/$c; \
    done

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/api /api
COPY --from=build /out/consumer /consumer
COPY --from=build /out/snapshot /snapshot
COPY --from=build /out/migrate /migrate
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/api"]
