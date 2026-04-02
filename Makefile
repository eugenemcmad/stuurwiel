# Stuurwiel — common tasks.
#
# When you add ./cmd/<name>:
#   1. Append <name> to CMDS below
#   2. Update Dockerfile (add go build … /out/<name>)
#   3. Update docker-compose.yml if it should run in compose
#   4. docker-build-go / up-go — only app images from Dockerfile
#
# Config: ./config.yaml in cwd, or CONFIG_PATH; see config.example.yaml; env overrides file.

export CGO_ENABLED := 0

BIN_DIR   := bin
LDFLAGS   := -s -w
GOFLAGS   := -trimpath

# Pause after e2e publishes so workers can relay (visible in logs); then `make e2e` runs docker compose down.
E2E_OBSERVE_AFTER ?= 45s

# Single list — `build` iterates; keep Dockerfile in sync when you change this.
CMDS := api \
	worker-nats-kafka \
	worker-kafka-rabbit \
	worker-rabbit-nats

.PHONY: help
help:
	@echo "Targets:"
	@echo "  make build       Build all binaries into $(BIN_DIR)/ (uses CMDS)"
	@echo "  make test        go test ./..."
	@echo "  make check       fmt, tidy, vet, test"
	@echo "  make api-lint    buf lint + buf breaking vs git HEAD~1 (needs buf via go run)"
	@echo "  make test-race   go test -race ./..."
	@echo "  make test-integration  go test -tags=integration (broker-backed tests when added)"
	@echo "  make generate    go generate ./... (protobuf)"
	@echo "  make docker      same as docker-build (spec §8)"
	@echo "  make docker-build      full image: docker build -t stuurwiel:local ."
	@echo "  make docker-build-go   only Go app images (compose services: $(CMDS)), not kafka/nats/rabbit/zk"
	@echo "  make up-go             docker compose up -d --build for Go services only (after stack is up)"
	@echo "  make up | down | logs   docker compose"
	@echo "  make e2e         compose up + e2e + observe + compose down (E2E_OBSERVE_AFTER=$(E2E_OBSERVE_AFTER))"
	@echo "  make e2e-only    only ./e2e (compose unchanged; uses E2E_OBSERVE_AFTER)"
	@echo "  make k8s-validate  kubeconform on infra/k8s/overlays/dev and infra/k8s/overlays/prod (needs kubectl + go)"
	@echo "  make k8s-apply     kubectl apply -k infra/k8s/overlays/dev (override: K8S_OVERLAY=...)"
	@echo "  make nomad-validate  nomad job validate on dev + prod var overlays (needs nomad CLI)"
	@echo "  make nomad-apply     nomad job run (override: NOMAD_OVERLAY=infra/nomad/overlays/prod)"
	@echo "  make clean       remove $(BIN_DIR)/"

.PHONY: all
all: build test ## Build binaries then run tests

.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	@for c in $(CMDS); do \
		echo "go build -> $(BIN_DIR)/$$c"; \
		go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BIN_DIR)/$$c ./cmd/$$c || exit $$?; \
	done

.PHONY: test
test:
	go test ./... -count=1

# The race detector requires cgo; see `go doc -race`.
.PHONY: test-race
test-race:
	CGO_ENABLED=1 go test ./... -count=1 -race

# Optional: tests that need real brokers use //go:build integration in *_test.go files.
.PHONY: test-integration
test-integration:
	go test -tags=integration ./... -count=1

# Proto lint + breaking change detection vs parent commit (full SHA; needs 2+ commits in repo).
BUF_MOD := github.com/bufbuild/buf/cmd/buf@v1.47.2
BUF_PARENT := $(shell git rev-parse HEAD~1 2>/dev/null)

.PHONY: api-lint
ifeq ($(BUF_PARENT),)
api-lint:
	cd api/grpc && go run $(BUF_MOD) lint
	@echo "api-lint: skipping buf breaking (no parent commit)"
else
api-lint:
	cd api/grpc && go run $(BUF_MOD) lint
	cd api/grpc && go run $(BUF_MOD) breaking --against "../../.git#commit=$(BUF_PARENT)"
endif

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: generate
generate:
	go generate ./...

.PHONY: check
check: fmt tidy vet test

.PHONY: docker docker-build
docker: docker-build
docker-build:
	docker build -t stuurwiel:local .

# Rebuild only Docker images that compile this repo (same Dockerfile). Does not rebuild kafka, nats, rabbitmq.
.PHONY: docker-build-go
docker-build-go: docker-check
	docker compose build $(CMDS)

# Recreate only Go app containers with fresh build (brokers unchanged). Use when compose stack already runs.
.PHONY: up-go
up-go: docker-check
	docker compose up -d --build $(CMDS)

.PHONY: up
up:
	docker compose up -d

# Docker: stack up → e2e → observe → stack down (tech-spec item 12). Integration driver: ./e2e (-manage-compose).
.PHONY: e2e
e2e: docker-check
	go run ./e2e -manage-compose -wait=5m -observe-after=$(E2E_OBSERVE_AFTER)

# Same smoke test without starting/stopping compose (stack must already be running).
.PHONY: e2e-only
e2e-only:
	go run ./e2e -wait=5m -observe-after=$(E2E_OBSERVE_AFTER)

# Fails if the Docker daemon is not reachable.
.PHONY: docker-check
docker-check:
	@docker info >/dev/null 2>&1 || (echo >&2 "Docker daemon is not reachable. Start the Docker service and retry."; exit 1)

.PHONY: down
down:
	docker compose down

.PHONY: logs
logs:
	docker compose logs -f

# Validates rendered manifests without a cluster (kubeconform + upstream OpenAPI schemas).
KUBECONFORM_MOD := github.com/yannh/kubeconform/cmd/kubeconform@v0.6.7
KUBECONFORM_K8S := 1.31.0

.PHONY: k8s-validate
k8s-validate:
	kubectl kustomize infra/k8s/overlays/dev | go run $(KUBECONFORM_MOD) -strict -summary -kubernetes-version $(KUBECONFORM_K8S) -
	kubectl kustomize infra/k8s/overlays/prod | go run $(KUBECONFORM_MOD) -strict -summary -kubernetes-version $(KUBECONFORM_K8S) -

# Default overlay matches k8s-validate. Example: make k8s-apply K8S_OVERLAY=infra/k8s/overlays/prod
K8S_OVERLAY ?= infra/k8s/overlays/dev

.PHONY: k8s-apply
k8s-apply:
	kubectl apply -k $(K8S_OVERLAY)

# Nomad job spec (tech-spec §16): base job + per-overlay vars.
NOMAD_JOB := infra/nomad/base/stuurwiel.nomad.hcl
NOMAD_OVERLAY ?= infra/nomad/overlays/dev
NOMAD_VARFILE := $(NOMAD_OVERLAY)/vars.hcl

.PHONY: nomad-validate
nomad-validate:
	nomad job validate -var-file=infra/nomad/overlays/dev/vars.hcl $(NOMAD_JOB)
	nomad job validate -var-file=infra/nomad/overlays/prod/vars.hcl $(NOMAD_JOB)

.PHONY: nomad-apply
nomad-apply:
	nomad job run -var-file=$(NOMAD_VARFILE) $(NOMAD_JOB)

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)
