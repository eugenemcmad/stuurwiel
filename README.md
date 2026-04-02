# Stuurwiel

This project is a **sandbox for exercising broker interactions**: publish paths, consumers, reconnect behaviour, and cross-broker relay in one runnable stack.

Ring relay over **NATS → Kafka → RabbitMQ → NATS …**: a publish **API** (REST + gRPC), three **edge workers** that consume from one broker and optionally forward to the next, and **stochastic forwarding** (`forward_probability`).

## Requirements

- Go **1.26+** (see `go.mod`)
- **Docker** for Compose-based runs and e2e

## Quick start (Docker Compose)

```bash
docker compose up -d
```

- **HTTP API**: `http://localhost:8080` — `GET /healthz`, `POST /v1/publish/{broker}`, `GET /openapi.yaml`, `GET /swagger` (Swagger UI)
- **gRPC**: `localhost:9090` — publish RPC (see `api/grpc/stuurwiel/v1`)
- Brokers: NATS `4222`, Kafka `9092`, RabbitMQ `5672` (management UI `15672`)

After changing only Go services (rebuild app images):

```bash
make docker-build-go
make up-go
```

## Configuration

Precedence: **defaults** → optional **YAML** → **environment** (env wins).

- If `CONFIG_PATH` is set, that file is loaded; otherwise `./config.yaml` or `./config.yml` in the working directory is used if present.

Copy `config.example.yaml` to `config.yaml` and adjust. Important keys:

| Area | Notes |
|------|--------|
| `forward_probability` | Probability to forward to the next broker (otherwise message is logged as completed and not relayed). |
| `worker_concurrency` | Concurrent handlers per edge worker (default `10`). |
| `http_addr` / `grpc_addr` | API listen addresses. |
| `reconnect_*` / `max_reconnect_attempts` | Backoff and limits for dial/session recovery. |

## Message format

Domain JSON (`Content-Type: application/json` for REST):

```json
{ "event_id": 1, "text": "hello" }
```

`event_id` must be **≥ 0** (domain validation). Workers **append** to `text` when forwarding (e.g. ` -> Kafka`). New messages are not invented inside workers; they originate from the API or upstream broker payloads.

## Architecture (Clean / DDD-oriented)

Dependency direction: **`domain`** (no I/O) → **`internal/application`** (use cases, ports) → **`internal/infrastructure`** (adapters) → **`cmd`** (composition).

- **Domain**: `Msg`, `Broker`, ring helpers, `ErrUnknownBroker` / `ErrInvalidMsg`, `Msg.Validate`, `AppendHopLabel` hop text.
- **Application**: `publish.Publisher` port, `PublishRouter`, `publish.Service` (decode/validate/publish for HTTP/gRPC), `relay` edge + policies.
- **Infrastructure**: broker clients, `transport` (thin HTTP/gRPC handlers calling `publish.Service`), `logging.GroupMsg` for structured logs without `slog` on domain types.

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Liveness probe |
| `POST` | `/v1/publish/{broker}` | Publish JSON body; `broker` is `nats`, `kafka`, or `rabbitmq` |

Successful publish returns **204 No Content**.

gRPC: register `MessageService` / `Publish` with `broker` and payload fields (`api/grpc/stuurwiel/v1`).

## Development

```bash
make build      # binaries under bin/ (api, worker-*)
make test       # go test ./...
make check      # fmt, tidy, vet, test
make generate   # protobuf (go generate ./...)
make k8s-validate   # kubeconform on infra/k8s/overlays/dev and overlays/prod (kubectl + go)
make k8s-apply      # kubectl apply -k infra/k8s/overlays/dev
make nomad-validate # nomad job validate on infra/nomad overlays dev + prod (nomad CLI)
make nomad-apply    # nomad job run (default overlay infra/nomad/overlays/dev)
make api-lint       # buf lint + buf breaking (see api/README.md)
make test-race      # go test -race (cgo; C toolchain required — see `go help build`)
make test-integration   # go test -tags=integration (for future broker-backed tests)
```

Integration tests that need live brokers can live in `*_test.go` files with `//go:build integration` and run with `make test-integration` (default `go test` does not set that tag).

Full image:

```bash
make docker     # docker build -t stuurwiel:local .
```

## Kubernetes

Kustomize: `infra/k8s/base/`, overlays under `infra/k8s/overlays/` (default **`dev`**). See [infra/k8s/README.md](infra/k8s/README.md).

## Nomad

Job file `infra/nomad/base/stuurwiel.nomad.hcl`, variable overlays under `infra/nomad/overlays/` (default **`dev`**). See [infra/nomad/README.md](infra/nomad/README.md).

## Integration test (e2e)

Smoke test against a running stack (REST + gRPC, multiple brokers):

```bash
make e2e        # compose up → ./e2e (manage-compose) → observe window → compose down
```

If Compose is already up:

```bash
make e2e-only
```

Observability window and compose directory: `E2E_OBSERVE_AFTER` / `-observe-after`, `E2E_COMPOSE_DIR` / `-compose-dir`. Default HTTP/gRPC base URLs can be overridden via `E2E_HTTP` / `E2E_GRPC`.

The driver lives in **`e2e/`** (`go run ./e2e`), not under `cmd/`.

## Layout

| Path | Role |
|------|------|
| `cmd/api` | HTTP + gRPC publish server |
| `cmd/worker-*` | One relay edge per binary (NATS→Kafka, Kafka→Rabbit, Rabbit→NATS) |
| `internal/application` | `PublishRouter`, `publish.Service`, relay use case |
| `internal/domain` | `Msg`, broker ring identifiers (`domain.Broker`, `ParseBrokerName`, hop suffix) |
| `internal/infrastructure` | NATS, Kafka, RabbitMQ adapters, HTTP/gRPC |
| `internal/config` | YAML + env loading |
| `api/grpc/stuurwiel/v1` | gRPC / protobuf definitions |
| `api/http` | Optional `.http` samples for REST (IDE clients) |
| `api/README.md` | What lives under `api/` and how to regenerate protos |
| `infra/k8s/base`, `infra/k8s/overlays/*` | Kubernetes manifests (Kustomize base + overlays) |
| `infra/nomad/base`, `infra/nomad/overlays/*` | Nomad job (`stuurwiel`) + var overlays for app image |
