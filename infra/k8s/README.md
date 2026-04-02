# Kubernetes

[Kustomize](https://kustomize.io/) manifests for the same stack as `docker-compose.yml`: NATS, Kafka (KRaft), RabbitMQ, `api`, three workers. Namespace: **`stuurwiel`**.

## Structure

| Path | Role |
|------|------|
| `base/kustomization.yaml` | Entrypoint; sets namespace `stuurwiel` |
| `base/namespace.yaml` | Namespace |
| `base/configmap.yaml` | `stuurwiel-brokers` (URLs, subject, topic, queue) |
| `base/secret.yaml` | `stuurwiel-secrets` (`RABBIT_URL`; replace for non-dev) |
| `base/nats.yaml`, `rabbitmq.yaml`, `kafka.yaml` | Broker `Deployment` + `Service` |
| `base/api.yaml` | API `Deployment` + `Service` (HTTP `8080`, gRPC `9090`, `GET /healthz`) |
| `base/worker-nats-kafka.yaml` | NATS → Kafka |
| `base/worker-kafka-rabbit.yaml` | Kafka → RabbitMQ |
| `base/worker-rabbit-nats.yaml` | RabbitMQ → NATS |
| `overlays/dev/kustomization.yaml` | Default overlay (includes `base` as-is) |
| `overlays/prod/kustomization.yaml` | Pin `images`, patches, stricter resources |

Deploy from the repo root via **Makefile** (`make k8s-apply`). Raw base without an overlay: `kubectl apply -k infra/k8s/base`.

## Prerequisites

- `kubectl` and a working context
- Image **`stuurwiel:local`** visible to the cluster (`make docker`), e.g. `minikube image load`, `kind load docker-image`, or push and reference from `overlays/prod`

## Validate (no cluster)

Requires `kubectl` (built-in Kustomize) and **Go** on `PATH`:

```bash
make k8s-validate
```

Runs [kubeconform](https://github.com/yannh/kubeconform) on **`infra/k8s/overlays/dev` and `infra/k8s/overlays/prod`** (`go run …`, strict OpenAPI validation). On GitHub, `.github/workflows/ci.yml` runs `make k8s-validate` (and `make check` in another job).

## Deploy

```bash
make docker
make k8s-apply
```

`make k8s-apply` runs `kubectl apply -k infra/k8s/overlays/dev`. For another overlay: `make k8s-apply K8S_OVERLAY=infra/k8s/overlays/prod` (after editing that overlay).

## API from your workstation

```bash
kubectl port-forward -n stuurwiel svc/api 8080:8080 9090:9090
```

REST: `http://localhost:8080` — `GET /healthz`, `POST /v1/publish/{broker}`. gRPC: `localhost:9090` (`api/grpc/stuurwiel/v1`).

## Implementation notes

- **App pods (`api`, workers):** `securityContext` matches the image user (`65532`, non-root); `revisionHistoryLimit: 2` on all `Deployment`s. Broker images are unchanged (run as their default users).
- **Kafka:** single-replica `Deployment`, `hostname: kafka`, listeners aligned with `docker-compose`; topic `stuurwiel-messages` created in startup probes. For real production, use an operator or managed Kafka and persistent volumes.
- **RabbitMQ:** `RABBITMQ_SERVER_ADDITIONAL_ERLANG_ARGS` allows default `guest` from other pods (dev-style only).
- **Config:** env from `ConfigMap`/`Secret` matches app precedence (env over file; see project config docs).
- **Rollout:** there is no `depends_on` like in Compose. Applying everything at once is OK: the `api` may restart until Kafka/NATS/RabbitMQ are ready; workers use the same reconnect settings as in compose. To reduce restarts, apply broker manifests first, then `api` and workers (optional).

## Hardening (later)

Not in the default manifests: PVCs for broker data, Ingress or `LoadBalancer` for `api`, `NetworkPolicy`, TLS for brokers and gRPC, `PodDisruptionBudget` if `api` is multi-replica, Prometheus `ServiceMonitor`, GitOps (Flux/Argo CD), cluster e2e via `port-forward` or in-cluster Job with URLs pointing at `Service` DNS.
