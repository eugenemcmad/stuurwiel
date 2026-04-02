# API layout

- **`grpc/stuurwiel/v1/`** — gRPC **protobuf** contract (`stuurwiel.proto`) and generated **`stuurwiel.pb.go`**, **`stuurwiel_grpc.pb.go`** (do not edit generated files by hand).
- **`grpc/buf.yaml`** — [Buf](https://buf.build/) lint and **breaking** checks (`WIRE` rules: wire-compatible changes; not raw file-path diffs). Run via **`make api-lint`** from the repo root (uses `go run` for the Buf CLI).
- **`http/`** — optional **`.http`** samples for REST (IDE clients only; not used by the build).
- **OpenAPI** — embedded spec at **`GET /openapi.yaml`** and Swagger UI at **`GET /swagger`** on the HTTP server (`internal/infrastructure/transport/openapi.yaml`).

Regenerate protos from the repository root:

```bash
make generate   # go generate ./...
```

Requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` on `PATH` (see the main [README](../README.md) and `generate.go`).

Go imports use **`stuurwiel/api/grpc/stuurwiel/v1`** (module `stuurwiel`).

### Breaking vs `HEAD~1`

`make api-lint` runs `buf breaking` against the parent git commit. If there is no parent (single commit), breaking is skipped. **Requires `git`** and **two commits** for the breaking step.
