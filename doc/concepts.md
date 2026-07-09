# Concepts

This page explains the core ideas behind Letopis. Read this before diving into the API.

---

## Collections and entities

A **collection** is a named group of entities of the same kind — for example, `crm.deals`, `docs.contracts`, or `payments.invoices`. Collection names are lowercase, dot-separated identifiers (`^[a-z0-9]+(?:\.[a-z0-9]+)*$`). Letopis does not interpret the collection name; it is just a namespace you choose.

An **entity** is a single tracked object within a collection, identified by a string `entity_id` that is unique within the collection. The `entity_id` is meaningful only to your system; Letopis stores it as an opaque string.

Collections and their physical MongoDB collections are provisioned automatically on first write when `collections.auto_create: true` (the default). Disable this to require explicit `PUT /collections/{c}/config` before writing.

---

## Events and diffs

Every change to an entity is recorded as an **event**. An event carries:

- A **diff** — the list of field-level changes (`add`, `change`, `remove`) with path, old value, and new value.
- A **version** — a monotonically increasing integer, per entity, assigned in write order.
- Metadata: `author_id`, `source`, `ts_source`, `ts_received`, `ts_stored`, and an arbitrary `meta` object.

The diff format is Letopis-native: each element is `{path, op, old, new}`. The `old` field (absent in RFC 6902 JSON Patch) makes each change self-contained. The read API can export diffs as JSON Patch (`?format=json-patch`) for compatibility.

### State vs diff ingest

You can push changes in two ways:

- **Full state** (`POST /state`): send the entire current object; Letopis computes the diff against the last known state.
- **Ready-made diff** (`POST /diff`): send the change list directly; Letopis validates the format and stores it as-is.

Both produce identical events in storage. Use full-state ingest when your source system does not know what changed; use diff ingest when you track changes yourself.

---

## Reliability modes

Every write can choose one of three reliability modes, either as a collection default or overridden per request with the `X-Letopis-Mode` header.

| Mode | Response | Guarantee | Use case |
|---|---|---|---|
| `strict` | `201 Created` | Written to MongoDB before responding | Audited writes that must be confirmed synchronously |
| `durable` | `202 Accepted` + ticket | Queued to Redis Streams; worker writes to MongoDB | Default; survives API restarts without data loss |
| `fast` | `202 Accepted` + ticket | Queued in-process (falls back to `durable` when the role is split) | Maximum throughput; loses queued events on crash |

The default mode is **`durable`** for new collections. You can change the per-collection default via `PUT /collections/{c}/config`.

**Tickets:** Async modes return a `ticket_id`. Poll `GET /tickets/{id}` to track: `accepted → processing → stored` (or `failed` with a reason). Tickets expire after a configurable TTL (default 24 h).

---

## Snapshots and point-in-time reconstruction

Letopis stores every event append-only. For an entity at version 10,000, reading point-in-time state by replaying all events from genesis would be slow. **Snapshots** solve this.

Every `N` events (configured per collection; default 100), Letopis materializes the entity's current state as a snapshot. Point-in-time reads then only replay the *tail* of events from the nearest snapshot at or below the requested version. The benchmark result: p99 under 7 ms for histories with 10,000 versions.

Snapshots are best-effort: they never block the write path, and their absence affects only read performance, not correctness.

---

## Hash-chain integrity

The **hash-chain plugin** (`hash_chain`) adds tamper-evidence to an entity's history. When enabled for a collection:

```
hash₁ = sha256("letopis:genesis:v1:" + collection_name)  // genesis seed
hash₂ = sha256(hash₁ ‖ canonical(event₁))
hash₃ = sha256(hash₂ ‖ canonical(event₂))
…
```

Each event stores its `hash` and `prev_hash`. Because the chain is linear per entity, silently removing, inserting, or modifying any event breaks all hashes that follow. The `:verify` endpoint detects the first divergence.

The canonical projection covers `{op, entity_id, changes, author_id, source, ts_source, flow}` but excludes `version` — so retries that assign a different version do not break the chain.

Hash chains are per-entity. GDPR deletion (`purge`) removes the entity's entire chain and records a tombstone; other entities are unaffected.

---

## Multi-tenancy

Each tenant owns an isolated MongoDB database (`hm_t_{id}` on the default cluster, or any URI/database you configure). Collections, events, snapshots, rules, tickets, and DLQ entries never mix between tenants. The tenant is identified by the **API key**; it never appears in URL paths.

Within a tenant's database, each logical collection maps to three physical MongoDB collections:

| Physical collection | Contents |
|---|---|
| `ev_{name}` | Append-only events |
| `sn_{name}` | Snapshots |
| `cur_{name}` | Current materialized state (one document per entity) |

Plus system collections:

| Physical collection | Contents |
|---|---|
| `ev__system` | Audit log (config changes, rule changes, purge tombstones) |
| `ev__flow` | Business-flow activities |
| `_rules` | Rule definitions |
| `_collections` | Explicit collection config |
| `_dlq` | Webhook dead-letter entries |

---

## Business flows and activities

An **activity** is a business-process event that is not an entity change — for example, "started invoice approval", "triggered recalculation", "sent notification". Activities live in `ev__flow`.

A **flow** is a causal graph of events and activities linked by a shared `flow_id`. Any change event or activity can carry a `flow` block with `flow_id`, `caused_by` (references to upstream events or activities), and `step` (a label). References are stored as-is; Letopis does not validate that the referenced entity or event exists — causal edges across async systems are allowed to be dangling and are resolved at read time.

`GET /flows/{flow_id}` returns all nodes in received order with their `caused_by` edges, forming a traversable DAG.

---

## Rules and webhooks

A **rule** is attached to a collection and consists of:

- A **condition** — a nested tree of `all`/`any`/`not` combinators and leaf operators (`eq`, `ne`, `in`, `gt`, `gte`, `lt`, `lte`, `regex`, `exists`) evaluated against the event's fields, plus `changes` match with glob path support (`items.*.price`).
- One or more **actions**: `webhook` (signed HMAC-SHA256 HTTP POST) or `log` (structured log entry).

Rules are evaluated **after** the event is written and never slow the write path. Webhook delivery is at-least-once with configurable exponential backoff. Exhausted deliveries go to the rule's dead-letter queue (DLQ), visible via `GET /collections/{c}/rules/{id}/dlq` and redeliverable via `:redeliver`.

A webhook request is signed: the `X-HM-Signature` header contains `sha256=` + `hex(HMAC-SHA256(secret, body))`. The secret is resolved by `secret_ref` from the instance config's `webhooks.secrets` map, not stored in the rules themselves.

---

## Plugin system

The plugin system exposes three hooks in the ingest pipeline:

| Hook | When | Can do |
|---|---|---|
| `pre-store` | Before writing to MongoDB | Enrich event, add metadata, reject write |
| `post-store` | After writing to MongoDB | React, index, trigger external systems |
| `action` | When a rule fires an action | Implement custom action types |

Plugins are in-process Go code compiled with the service. Register them via `pkg/ext.Registry` before the server starts. The only public API is `pkg/ext`; it is semver-stable. Internal packages are not stable across releases.

The `hash_chain` plugin is bundled and registered by default (active only when enabled per collection via config).

A plugin error triggers the configured `fail_mode`:

- `open` (default): log the error and continue — the event is written without the plugin's contribution.
- `closed`: reject the write with `422 Unprocessable Entity`.
