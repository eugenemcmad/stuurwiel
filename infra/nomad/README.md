# HashiCorp Nomad

Job specification for the same stack as `docker-compose.yml`: Kafka (KRaft), NATS, RabbitMQ, `api`, three workers. One **job** (`stuurwiel`) with a single **task group** (`stack`) so all tasks share one network namespace; broker and app URLs use **`127.0.0.1`** (see below).

## Structure

| Path | Role |
|------|------|
| `base/stuurwiel.nomad.hcl` | Root job file; `variable "app_image"` for api/worker images |
| `overlays/dev/vars.hcl` | Dev image tag (default `stuurwiel:local`) |
| `overlays/prod/vars.hcl` | Production image (pin registry + tag) |

Validate and deploy from the repo root via **Makefile** (`make nomad-validate`, `make nomad-apply`).

## Prerequisites

- **Nomad** client on `PATH` (`nomad version`), and a cluster where the **Docker** task driver is enabled (Linux nodes).
- Image **`stuurwiel:local`** (or the value in `vars.hcl`) available to the node: `make docker`, load into the node’s image store, or reference a pushed image in `overlays/prod/vars.hcl`.

## Networking note

Tasks in the same Nomad task group share a network namespace. Connection strings mirror compose wiring but use loopback: `nats://127.0.0.1:4222`, `127.0.0.1:29092` for Kafka, `amqp://guest:guest@127.0.0.1:5672/` for RabbitMQ. There is no `depends_on` between tasks: apps may restart until brokers are ready (same idea as [infra/k8s/README.md](../k8s/README.md) rollout notes).

## Validate (no cluster required)

```bash
make nomad-validate
```

Runs `nomad job validate` against **`infra/nomad/base/stuurwiel.nomad.hcl`** with **`infra/nomad/overlays/dev/vars.hcl`** and **`infra/nomad/overlays/prod/vars.hcl`**. The CLI checks syntax and job structure; the Docker driver is not fully validated without a local agent (you may see a notice about that).

## Deploy

```bash
make docker
make nomad-apply
```

`make nomad-apply` runs `nomad job run -var-file=$(NOMAD_OVERLAY)/vars.hcl infra/nomad/base/stuurwiel.nomad.hcl` with **`NOMAD_OVERLAY` defaulting to `infra/nomad/overlays/dev`**. For production:

```bash
make nomad-apply NOMAD_OVERLAY=infra/nomad/overlays/prod
```

(After editing `overlays/prod/vars.hcl` with your registry image.)

## API from your workstation

Use Nomad **alloc** / **service** port mapping or `nomad alloc exec`; the job maps HTTP **8080** and gRPC **9090** on the host for the `api` task’s `http` / `grpc` ports when scheduled.

## Hardening (later)

Not in the default job: health checks per task, `spread` / `affinity`, persistent volumes for Kafka/Rabbit, Connect or mesh, multi-replica `api`, secrets via Vault, Nomad Variables for `RABBIT_URL`, production Kafka/Rabbit operators or external services.
