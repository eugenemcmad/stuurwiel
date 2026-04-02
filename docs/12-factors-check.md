# 12-Factor — current checkup (Stuurwiel)

**Snapshot date:** 2026-04-05. Refresh after material changes to runtime, packaging, or deployment docs.

Reference: [The Twelve-Factor App](https://12factor.net/).

## Current state

| # | Factor | Status | Current state | Gap vs strict factor | Acceptable / follow-up |
|---|--------|--------|---------------|----------------------|-------------------------|
| 1 | Codebase | **OK** | Single Git repo; `cmd/api`, `cmd/worker-*`, shared `internal/`. | — | — |
| 2 | Dependencies | **OK** | `go.mod` / `go.sum`; Docker base images pinned in `Dockerfile`; broker clients from module. | — | — |
| 3 | Config | **OK** | Defaults → optional YAML (`CONFIG_PATH` or `./config.yaml`) → env overrides (`internal/config`). Secrets not baked into images; wired via env / K8s Secret / Nomad in manifests. | — | — |
| 4 | Backing services | **OK** | NATS, Kafka, RabbitMQ as attached resources; URLs entirely from config. | — | — |
| 5 | Build, release, run | **Partial** | **Build:** `make build` / image via `Dockerfile` (immutable binaries in image). **Run:** compose / K8s / Nomad from repo. **Release:** no automated image registry push in CI; “release” is local `make docker` + manual / external deploy. | Twelve-factor “release” often implies a single versioned artifact moving through stages; here the pipeline stops at validated source + optional local image. | Acceptable for a sandbox / lab repo. **Follow-up:** add publish job + version tag when a registry and promotion process exist. |
| 6 | Processes | **OK** | App processes are stateless; message state lives in brokers. | — | — |
| 7 | Port binding | **OK** | HTTP/gRPC addresses from config (`http_addr`, `grpc_addr`); services bind ports, no Unix-socket–only requirement. | — | — |
| 8 | Concurrency | **OK** | Out-of-process: more replicas (Compose / K8s / Nomad). In-process: `worker_concurrency` per edge process. | — | — |
| 9 | Disposability | **OK** | Signal handling and graceful shutdown in `cmd/*`; broker reconnect with backoff in `internal/runtime/reconnect`. | — | — |
| 10 | Dev/prod parity | **Partial** | Same env keys and same `Dockerfile` pattern across paths. | Broker topology differs (e.g. single-node Kafka in compose vs production-style notes in `infra/k8s/README.md` / `infra/nomad/README.md`). | Documented trade-offs; acceptable until production SLAs are fixed. **Follow-up:** tighten manifests when target environment is known. |
| 11 | Logs | **OK** | `slog` to stderr; no required log file path for normal operation. | — | If log shipping is added later: sidecar / agent; no rotation logic inside the app (`docs` note from earlier review). |
| 12 | Admin processes | **OK** | No DB migrations inside long-running servers; `e2e` and `make` targets are separate from `cmd/*` services. | — | — |

### Notes (unchanged facts)

- Broker URLs in application config are not compile-time constants (tests/mocks aside).
- **Factor 11 (shipping):** aggregation remains an environment concern; the process stays a stdout stream.

---

## Updating this snapshot

Replace the **Snapshot date** when re-running the check. For any row moved to **Partial**, fill **Gap** and **Acceptable / follow-up**; empty gaps are not allowed for **Partial**. If the gap is closed, return the row to **OK**.
