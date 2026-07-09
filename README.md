# Letopis

[![CI](https://github.com/max-trifonov/letopis/actions/workflows/ci.yml/badge.svg)](https://github.com/max-trifonov/letopis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.25-blue.svg)](go.mod)

**Letopis** is a multi-tenant service that stores a complete, tamper-evident history of changes to entities in external systems — CRM records, contracts, orders, or anything else you track. It accepts full states or ready-made diffs, reconstructs any past state on demand, and fires webhook rules when history changes.

- Website & docs: https://letopis.tech
- License: Apache 2.0

---

## Why Letopis?

Most databases store only the latest state. Letopis stores *every* change — who made it, when, and what exactly changed — and gives you that history through a simple REST API.

| Feature | Description |
|---|---|
| **Diff ingestion** | Send full state or a ready-made diff; the server normalizes either form |
| **Point-in-time reads** | Reconstruct any entity at any past version or timestamp |
| **Hash-chain integrity** | SHA-256-chained history — undetectable tampering or deletion |
| **Rules + webhooks** | Conditional, retried, signed HTTP callbacks on every change |
| **Business flows** | Link events across collections into causal DAGs with activities |
| **Multi-tenancy** | Hard MongoDB-database isolation per tenant, identified by API key |
| **Reliability modes** | Per-request choice: `strict` (sync 201), `durable` (Redis-queued 202), `fast` (in-memory 202) |
| **Plugin system** | In-process `pre-store`/`post-store`/`action` hooks; `pkg/ext` is semver-stable |

---

## Quick start

**Prerequisites:** Go 1.25+, MongoDB 7+, Redis 6+.

Grab a prebuilt binary from [Releases](https://github.com/max-trifonov/letopis/releases) (linux/darwin/windows, amd64/arm64), or build from source:

```sh
git clone https://github.com/max-trifonov/letopis.git
cd letopis
make build

cp config.example.yaml config.yaml   # edit tenants.*.keys at minimum
./bin/letopis serve
```

The binary searches for `config.yaml` in the working directory, then next to itself; `--config` overrides. `LETOPIS_*` environment variables override the file.

The default role is `all` (HTTP + gRPC + worker in one process), which is the right choice for development and small installs. For production you can split `api` and `worker` processes.

### First write

```sh
# Set your API key from config.yaml
TOKEN=hm_dev_plaintext

# Ingest a state change (server computes the diff)
curl -s -X POST http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/state \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"state": {"title": "Acme Corp", "amount": 5000, "stage": "prospect"}}'

# Read it back
curl -s http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/state \
  -H "Authorization: Bearer $TOKEN"
```

---

## Docker

Two compose flavours are provided:

```sh
# Development — builds from the working tree, everything in one stack.
docker compose -f docker-compose.dev.yml up --build

# Production — released image + your config.yaml + infra.
cp config.example.yaml config.yaml   # configure tenants/keys
docker compose up -d
```

Both compose files wire `LETOPIS_*` environment variables so the config file itself stays host-agnostic. Build a local image: `make docker`.

---

## Configuration

Configuration is a YAML file plus environment variable overrides. See [`config.example.yaml`](config.example.yaml) for the full reference.

The minimal production config sets:

| Section | Key fields |
|---|---|
| `mongodb.uri` | Connection URI for the default MongoDB cluster |
| `redis.addr` | Redis address (used for the durable queue and idempotency) |
| `tenants` | One entry per tenant with `id` and at least one API key (SHA-256 hash preferred) |
| `webhooks.secrets` | Named signing secrets referenced by rules |

---

## System endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Liveness probe |
| `GET /readyz` | Readiness — aggregated dependency checks |
| `GET /metrics` | Prometheus metrics |
| `GET /version` | Build info (version, commit, date) |
| gRPC `:9090` | `letopis.v1.SystemService`, standard health, reflection |

Both servers support TLS; set `server.http.tls` / `server.grpc.tls` in the config.

---

## Documentation

| Doc | Contents |
|---|---|
| [Getting started](doc/getting-started.md) | Installation, configuration reference, tenant setup |
| [Concepts](doc/concepts.md) | Collections, entities, reliability modes, snapshots, multi-tenancy |
| [Write API](doc/write-api.md) | Ingest state/diff/delete, batch, idempotency, tickets |
| [Read API](doc/read-api.md) | History, current state, point-in-time reconstruction, collections, flows |
| [Admin API](doc/admin-api.md) | Collection config, rules, webhooks, dead-letter queue |
| [Client SDKs](doc/sdks.md) | Official Laravel and Node.js SDKs |

Russian: [doc/ru/](doc/ru/)

## Client SDKs

| SDK | Stack | Package |
|---|---|---|
| [letopis-laravel-sdk](https://github.com/max-trifonov/letopis-laravel-sdk) | PHP 8.2+ / Laravel 11-12 | `letopis/laravel-sdk` (Packagist) |
| [letopis-node-sdk](https://github.com/max-trifonov/letopis-node-sdk) | Node.js 18+ / TypeScript | `letopis-node` (npm) |

OpenAPI 3.1 spec: [`api/openapi/letopis.v1.yaml`](api/openapi/letopis.v1.yaml)

---

## Development

```sh
make test             # go test -race ./...
make test-integration # requires Docker — integration + e2e (testcontainers)
make lint             # golangci-lint + buf lint
make proto            # regenerate gRPC bindings (buf + go tool plugins)
make bench            # diff/pipeline CPU benchmarks
```

The OpenAPI spec is validated by a regular Go test — `go test ./api/openapi/` — no Docker needed, so a malformed spec fails the build like any other test failure.

The integration suite (`-tags=integration`) runs MongoDB and Redis via testcontainers. It covers storage invariants, REST e2e, and point-in-time perf (snapshot + event-tail reconstruction must stay under 200 ms, FR-3.2/NFR-1.5).

---

## Project layout

```
cmd/letopis/       entry point (thin; wires internal/app)
internal/
  app/             dependency wiring
  config/          YAML + env config loader
  diff/            diff engine (own package, no dependencies)
  domain/          domain types and port interfaces
  service/         ingest / read / rules / snapshot business logic
  storage/         MongoDB + Redis adapters
  queue/           queue abstraction; memory and redis-streams drivers
  worker/          async event processing loop
  plugin/          plugin host + hash-chain plugin
  rules/           rules compilation and evaluation
  delivery/        webhook dispatcher + SSRF guard
  metrics/         Prometheus metrics
  tenant/          tenant resolution
pkg/ext/           public extension API (semver-stable)
api/
  openapi/         OpenAPI 3.1 spec + spec test
  proto/           Protobuf definitions
```

---

## Contributing

Contributions are welcome. A few ground rules:

- **DCO** — every commit must be signed off (`git commit -s`), certifying the [Developer Certificate of Origin](https://developercertificate.org/).
- **Tests first** — the project follows test-driven development; tests go in before the implementation.
- **Discuss first** — significant changes deserve an issue before you invest time in code; the architecture follows recorded design decisions.
- **CI must pass** — `make test lint` before pushing; proto changes need `make proto` with the generated code committed.

See [CONTRIBUTING.md](CONTRIBUTING.md) for details. The only public Go API is `pkg/ext`; internal packages are not stable.

This project follows a [Code of Conduct](CODE_OF_CONDUCT.md). To report a security issue, see [SECURITY.md](SECURITY.md) rather than opening a public issue.

---

## License

Apache 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). "Letopis" is a trademark; see [TRADEMARK.md](TRADEMARK.md) for usage guidelines.
