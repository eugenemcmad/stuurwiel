# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
	&& CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker-nats-kafka ./cmd/worker-nats-kafka \
	&& CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker-kafka-rabbit ./cmd/worker-kafka-rabbit \
	&& CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/worker-rabbit-nats ./cmd/worker-rabbit-nats

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
	&& adduser -D -u 65532 appuser
COPY --from=build /out/api /out/worker-nats-kafka /out/worker-kafka-rabbit /out/worker-rabbit-nats /usr/local/bin/
USER appuser
WORKDIR /
