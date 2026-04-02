# Architecture — current checkup (SOLID, Clean / Hexagonal, DDD)

**Snapshot date:** 2026-04-05. Refresh after large refactors under `internal/` or new edges/transports.

## Summary

| Area | Verdict | In one sentence |
|------|---------|-----------------|
| SOLID | **OK** | Packages split by role; publishers are pluggable; domain stays free of broker SDKs. |
| Clean / hexagonal | **OK** | Domain → application (use cases + ports) → infrastructure adapters; HTTP/gRPC are driving adapters. |
| DDD (lightweight) | **OK** | `Msg` and broker language are centralized in `domain`; relay behaviour lives in `application/relay`. |

No **hard-fail** condition is present today (see dependency rule below).

---

## SOLID — current state

| Principle | Status | Evidence | Gap / nuance | Acceptable / follow-up |
|-------------|--------|----------|----------------|-------------------------|
| **S** Single responsibility | **OK** | `cmd/*` wires only; `domain` = model + validation; `application` = publish + relay; `infrastructure` = brokers + transport + config. | — | — |
| **O** Open/closed | **OK** | New broker side: implement `publish.Publisher`, register in router; new edge: new `cmd` + compose/k8s/nomad entry. | — | — |
| **L** Liskov | **OK** | Tests swap mock publishers behind `Publisher`. | — | — |
| **I** Interface segregation | **OK** | `Publisher`, `MessageSource`, `ForwardPolicy`, etc., stay small. | — | — |
| **D** Dependency inversion | **OK** | `domain` imports stdlib only (`encoding/json`, `fmt`, `errors`, tests); application uses ports; `infrastructure` implements them. | — | — |

---

## Clean architecture & hexagonal — current state

| Layer | Status | Evidence |
|-------|--------|----------|
| **Domain** | **OK** | No imports from `infrastructure` or `application`. |
| **Application** | **OK** | `publish` and `relay` depend on `domain` + narrow interfaces; no direct `nats` / `kafka` / `amqp` imports in use cases. |
| **Infrastructure** | **OK** | Broker clients and `transport` implement ports; errors mapped where needed. |
| **Driving adapters (HTTP, gRPC)** | **OK** | Handlers delegate to `publish.Service`; status mapping via `MapPublishError` keeps HTTP concerns at the edge while errors remain typed in domain/application. |

**Observation:** `application/relay` imports `application/publish` for `MessageSink = publish.Publisher`. That is an explicit reuse of the publish port for the “next hop” sink, not a leak of unrelated use-case logic.

---

## DDD (lightweight) — current state

| Idea | Status | Evidence |
|------|--------|----------|
| **Ubiquitous language** | **OK** | `Msg`, broker names, relay path language align across `domain`, proto, and docs. |
| **Entity / payload boundary** | **OK** | `Msg.Validate` in domain; relay appends hop text per rules. |
| **Bounded contexts** | **OK** | `application/publish` vs `application/relay` are separate; dependency is only on the shared `Publisher` port (see above). |
| **Anti-corruption** | **OK** | JSON / proto decoded into `domain.Msg` before rules. |

---

## Dependency rule — current state

**Allowed (as implemented):** `cmd` → application, infrastructure, config · `application` → domain · `infrastructure` → domain, application ports · `domain` → standard library (plus tests).

**Hard fail (not present):** `domain` importing `broker`, `transport`, or `config`; handlers containing relay probability or routing instead of use cases.

---

## Updating this snapshot

Bump **Snapshot date** on each review. If a **Partial** appears in a future edit, document **Gap** and **Acceptable / follow-up** in the same style as `docs/12-factors-check.md`.
