# Getting started

This guide covers installation, configuration, and running your first Letopis instance.

## Prerequisites

| Dependency | Minimum version | Notes |
|---|---|---|
| Go | 1.25 | Required to build from source |
| MongoDB | 7.0 | One database per tenant is provisioned automatically |
| Redis | 6.0 | Used for the durable event queue, idempotency keys, and rule-cache invalidation |
| Docker | any | Optional; used for the compose stacks and integration tests |

---

## Install

### Prebuilt binaries

Download an archive for your platform (linux/darwin/windows, amd64/arm64) from [GitHub Releases](https://github.com/max-trifonov/letopis/releases). Each archive bundles the `letopis` binary with `LICENSE`, `NOTICE`, `config.example.yaml`, and `docker-compose.deps.yml`.

### From source

```sh
git clone https://github.com/max-trifonov/letopis.git
cd letopis
make build          # produces bin/letopis
```

The binary embeds the version, commit hash, and build date via `ldflags`; `make build` sets them from `git describe` automatically.

---

## Docker

A pre-built image is published at `ghcr.io/max-trifonov/letopis`. To build locally:

```sh
make docker          # tags ghcr.io/max-trifonov/letopis:latest
```

Two compose flavours are provided:

### Development stack

```sh
docker compose -f docker-compose.dev.yml up --build
```

Builds the image from the working tree and brings up MongoDB, Redis, and the service together. Environment variables and a `.env` file can override any `LETOPIS_*` setting.

### Production stack

```sh
cp config.example.yaml config.yaml   # edit before starting
docker compose up -d
```

Uses the published image. MongoDB and Redis are included in the compose file for convenience; in production you will typically point `mongodb.uri` and `redis.addr` at your managed infrastructure and remove them from the compose file.

---

## Configuration

Letopis is configured by a YAML file. The binary searches for `config.yaml` in:

1. The path given by `--config` (highest priority)
2. The current working directory
3. The directory containing the binary

**The config file is required; the server will not start without it.**

After the file is loaded, any `LETOPIS_*` environment variable overrides its corresponding key:

| Environment variable | Config key |
|---|---|
| `LETOPIS_ROLE` | `role` |
| `LETOPIS_HTTP_ADDR` | `server.http.addr` |
| `LETOPIS_GRPC_ADDR` | `server.grpc.addr` |
| `LETOPIS_MONGODB_URI` | `mongodb.uri` |
| `LETOPIS_REDIS_ADDR` | `redis.addr` |
| `LETOPIS_REDIS_PASSWORD` | `redis.password` |
| `LETOPIS_REDIS_DB` | `redis.db` |
| `LETOPIS_LOG_LEVEL` | `log.level` |

### Full configuration reference

```yaml
# Role: api (HTTP/gRPC only), worker (pipeline only), all (both — default).
role: all

server:
  http:
    addr: ":8080"
    tls:
      enabled: false
      cert_file: ""   # path to PEM certificate
      key_file: ""    # path to PEM private key
  grpc:
    addr: ":9090"
    tls:
      enabled: false
      cert_file: ""
      key_file: ""

# Default MongoDB cluster. Tenants may override this individually.
mongodb:
  uri: "mongodb://localhost:27017"

redis:
  addr: "localhost:6379"
  password: ""
  db: 0

# Event queue (ADR-003).
# memory driver: in-process only; valid for role=all.
# redis-streams: default; works across api/worker processes.
# Changing shard count on a live queue requires draining it first.
queue:
  driver: redis-streams        # memory | redis-streams
  shards: 16
  stream_prefix: "letopis:ingest"
  consumer_group: "workers"

# Rules engine — compiled rules are cached per collection.
# A rule change is broadcast over Redis pub/sub and invalidated immediately.
# cache_ttl_seconds bounds staleness when Redis pub/sub is unavailable.
rules:
  cache_ttl_seconds: 30

# Webhook delivery.
webhooks:
  default_timeout_ms: 5000
  max_attempts: 5
  backoff:
    base_ms: 500
    max_ms: 30000
  delivery_shards: 0           # 0 = same as queue.shards
  secrets:
    whsec_default: "change-me-in-production"
  ssrf:
    allow_private: false       # set true only when receivers are on your private network
    allow_http: false          # set true only in dev; production requires https

log:
  level: info    # debug | info | warn | error
  format: json   # json | text

collections:
  auto_create: true   # set false to require explicit PUT /config before first write

tenants:
  - id: acme
    database:
      uri: ""    # optional: pin to a different cluster
      name: ""   # optional: explicit database name (default: hm_t_acme)
    keys:
      - key_hash: "sha256:<hex>"   # preferred: store only the SHA-256
        scopes: [write, read]      # write | read | admin
        collections: ["crm.*"]     # glob mask; "*" = all
      - key: "hm_dev_plaintext"    # plaintext accepted for dev; logs a warning
        scopes: [admin]
        collections: ["*"]
```

---

## Tenant and API key setup

Each tenant gets its own MongoDB database (`hm_t_{id}` on the default cluster, or the database you specify). Collections and indexes are provisioned automatically on first write when `collections.auto_create: true`.

### Key scopes

| Scope | Allowed operations |
|---|---|
| `write` | Ingest events (state, diff, delete, batch) |
| `read` | Read history, current state, point-in-time, collections, flows |
| `admin` | Collection config, rules CRUD, DLQ management + everything in `read` |

A key's `collections` mask is a glob pattern. `"crm.*"` allows access to `crm.deals`, `crm.contacts`, etc. `"*"` allows all collections.

### Generating a key hash

Store key hashes instead of plaintext. To hash a key:

```sh
echo -n "your-secret-key" | sha256sum | awk '{print "sha256:"$1}'
```

Use the resulting `sha256:<hex>` as the `key_hash` value in config.

---

## Running roles

A single binary can serve all roles or be split for scale:

```sh
# All-in-one (default)
./bin/letopis serve

# API server only (no worker)
./bin/letopis serve --config config.yaml
# with role: api in config, or:
LETOPIS_ROLE=api ./bin/letopis serve

# Worker only
LETOPIS_ROLE=worker ./bin/letopis serve
```

Run multiple `api` instances behind a load balancer and one or more `worker` instances consuming from the Redis Streams queue.

> **Note:** The `memory` queue driver only works with `role: all`. For any split-process setup, use `redis-streams`.

---

## TLS

Both the HTTP and gRPC servers support TLS. Set the `cert_file` and `key_file` paths and `enabled: true`:

```yaml
server:
  http:
    addr: ":443"
    tls:
      enabled: true
      cert_file: "/etc/letopis/tls/server.crt"
      key_file:  "/etc/letopis/tls/server.key"
  grpc:
    addr: ":9443"
    tls:
      enabled: true
      cert_file: "/etc/letopis/tls/server.crt"
      key_file:  "/etc/letopis/tls/server.key"
```

---

## Health and observability

| Endpoint | Protocol | Description |
|---|---|---|
| `/healthz` | HTTP | Liveness — always 200 if the process is up |
| `/readyz` | HTTP | Readiness — checks MongoDB and Redis connectivity |
| `/metrics` | HTTP | Prometheus metrics |
| `/version` | HTTP | JSON build info |
| `:9090` | gRPC | `letopis.v1.SystemService` + standard health service + reflection |

Prometheus metrics include queue depth, consumer lag, ingest rates per tenant and mode, backpressure totals, and delivery failure counts. Sample alert rules are in [`deploy/prometheus/letopis-alerts.yml`](../deploy/prometheus/letopis-alerts.yml).

---

## Client SDKs

If your application is PHP/Laravel or Node.js/TypeScript, skip hand-rolled HTTP calls
and use an official SDK instead:

```bash
composer require letopis/laravel-sdk   # Laravel 11/12
npm install letopis-node               # Node.js 18+
```

See [Client SDKs](sdks.md) for setup and usage.

---

## Next steps

- [Concepts](concepts.md) — understand collections, reliability modes, and multi-tenancy
- [Write API](write-api.md) — start ingesting events
- [Read API](read-api.md) — query history and reconstruct past states
- [Admin API](admin-api.md) — configure collections, rules, and webhooks
- [Client SDKs](sdks.md) — official Laravel and Node.js SDKs
