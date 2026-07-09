# Admin API

Admin endpoints require a Bearer token with the `admin` scope.

```
Authorization: Bearer <api-key>
```

Admin-scoped keys also implicitly hold `read` access. The collection mask on the key still applies — an admin key with `collections: ["crm.*"]` cannot manage `payments.*` collections.

---

## Collection configuration

### Get collection config

```
GET /api/v1/collections/{collection}/config
```

Returns the collection's effective configuration (defaults applied) and a list of fields that are defaults rather than stored choices.

Returns `404` for a collection that has never been configured explicitly (auto-created collections via first write do not have a stored config record until an explicit `PUT`).

**Response** (200):

```json
{
  "config": {
    "reliability_mode": "durable",
    "snapshot_interval": 100,
    "retention": {"type": "forever"},
    "max_event_size_bytes": 1048576,
    "first_event_op": "create",
    "ordering": {"mode": "received"},
    "plugins": {
      "hash_chain": {
        "enabled": false,
        "fail_mode": "open",
        "params": {}
      }
    }
  },
  "defaults": ["reliability_mode", "snapshot_interval", "retention", "max_event_size_bytes", "first_event_op", "ordering"]
}
```

The `defaults` array names every field whose value is an applied default rather than an explicit stored choice. Use this to differentiate "the operator chose durable" from "durable happened to be the default".

### Set collection config

```
PUT /api/v1/collections/{collection}/config
```

Persists the configuration, provisions the physical MongoDB collections and indexes (idempotent), invalidates the resolver cache so changes take effect immediately, and writes an audit entry to `ev__system`.

Fields you omit fall back to defaults. An unknown enum value or a non-positive number returns `400`.

**Request body**

```json
{
  "reliability_mode": "strict",
  "snapshot_interval": 50,
  "retention": {
    "type": "days",
    "days": 90
  },
  "max_event_size_bytes": 524288,
  "first_event_op": "update",
  "ordering": {
    "mode": "received"
  },
  "plugins": {
    "hash_chain": {
      "enabled": true,
      "fail_mode": "closed"
    }
  }
}
```

**Config fields**

| Field | Type | Default | Description |
|---|---|---|---|
| `reliability_mode` | string | `durable` | Default per-request mode: `strict`, `durable`, or `fast` |
| `snapshot_interval` | integer | 100 | Snapshot every N events; 0 disables snapshots |
| `retention.type` | string | `forever` | Retention policy: `forever`, `days`, or `versions` |
| `retention.days` | integer | — | Required when `type: days` |
| `retention.keep` | integer | — | Required when `type: versions`; keeps the N most recent versions |
| `max_event_size_bytes` | integer | 1048576 | Per-event body size limit (1 MiB default) |
| `first_event_op` | string | `create` | Op for the first state-ingest to a new entity: `create` or `update` |
| `ordering.mode` | string | `received` | Event ordering: `received` (default) or `source` (reserved, not yet implemented) |
| `plugins` | object | `{}` | Per-plugin config keyed by plugin name |

**Plugin config**

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | boolean | `false` | Enable the plugin for this collection |
| `fail_mode` | string | `open` | On plugin error: `open` (write anyway, log) or `closed` (reject the write) |
| `params` | object | `{}` | Plugin-specific parameters |

**Response** (200):

```json
{
  "config": {
    "reliability_mode": "strict",
    "snapshot_interval": 50,
    ...
  }
}
```

---

## Rules

Rules are evaluated after every successful write and can trigger webhook deliveries or log entries.

### Create a rule

```
POST /api/v1/collections/{collection}/rules
```

Validates the condition by compiling it and validates all actions, then stores the rule. A name already used in the collection returns `409`. The collection is taken from the path.

**Request body**

```json
{
  "name": "notify-on-close",
  "enabled": true,
  "condition": {
    "all": [
      {"field": "op", "eq": "update"},
      {"field": "changes", "match": {"path": "stage", "new": "closed-won"}}
    ]
  },
  "actions": [
    {
      "type": "webhook",
      "url": "https://hooks.example.com/crm-events",
      "secret_ref": "whsec_default",
      "timeout_ms": 3000,
      "retry": {
        "max_attempts": 3,
        "backoff": "exponential"
      }
    }
  ]
}
```

**Response** (201):

```json
{
  "rule": {
    "id": "rule_01J3...",
    "name": "notify-on-close",
    "enabled": true,
    "version": 1,
    "updated_at": "2026-06-01T10:00:00Z",
    "condition": {...},
    "actions": [...]
  }
}
```

### List rules

```
GET /api/v1/collections/{collection}/rules
```

**Response** (200):

```json
{
  "rules": [
    {"id": "rule_01J3...", "name": "notify-on-close", "enabled": true, "version": 1, ...}
  ]
}
```

### Get a rule

```
GET /api/v1/collections/{collection}/rules/{ruleId}
```

Returns `404` for an unknown rule ID.

### Update a rule

```
PUT /api/v1/collections/{collection}/rules/{ruleId}
```

Replaces the rule's body and bumps its version. Same validation as create. Returns `404` for an unknown rule, `409` for a name conflict with another rule.

### Delete a rule

```
DELETE /api/v1/collections/{collection}/rules/{ruleId}
```

Returns `204 No Content` on success, `404` if the rule does not exist.

---

## Rule conditions

A condition is a tree of nodes. Each node is one of:

- A **combinator** — `all` (AND), `any` (OR), or `not`.
- A **scalar leaf** — `field` plus exactly one operator.
- A **change-match leaf** — `field: "changes"` plus a `match` object.

### Combinators

```json
{"all": [<condition>, ...]}   // all must be true (empty all = true)
{"any": [<condition>, ...]}   // at least one must be true (empty any = false)
{"not": <condition>}          // negation
```

### Scalar leaf operators

```json
{"field": "op", "eq": "update"}
{"field": "author_id", "ne": "system"}
{"field": "source", "in": ["crm-backend", "import-job"]}
{"field": "op", "exists": true}
```

| Operator | Type | Description |
|---|---|---|
| `eq` | any | Strict equality |
| `ne` | any | Not equal |
| `in` | array | Value is in the list |
| `gt` | number | Greater than |
| `gte` | number | Greater than or equal |
| `lt` | number | Less than |
| `lte` | number | Less than or equal |
| `regex` | string | Regular expression match (against string fields) |
| `exists` | boolean | Field is present (`true`) or absent (`false`) |

Addressable fields: `op`, `entity_id`, `author_id`, `source`.

### Change-match leaf

Matches when at least one element in the event's `changes` array satisfies all specified criteria. The `path` is a glob where `*` matches exactly one dot-notation segment.

```json
{
  "field": "changes",
  "match": {
    "path": "items.*.price",
    "op": "change",
    "old": 100,
    "new": 200
  }
}
```

| Field | Required | Description |
|---|---|---|
| `path` | Yes | Glob path, e.g. `status`, `address.city`, `items.*.price` |
| `op` | No | `add`, `change`, or `remove` |
| `old` | No | Exact match against the change's old value |
| `new` | No | Exact match against the change's new value |

### Condition examples

**Any field under `items` changed:**

```json
{"field": "changes", "match": {"path": "items.*"}}
```

**Status changed to "closed-won" by a specific author:**

```json
{
  "all": [
    {"field": "author_id", "eq": "user-42"},
    {"field": "changes", "match": {"path": "status", "new": "closed-won"}}
  ]
}
```

**Delete event from any source except "archive-job":**

```json
{
  "all": [
    {"field": "op", "eq": "delete"},
    {"field": "source", "ne": "archive-job"}
  ]
}
```

---

## Rule actions

### Webhook action

```json
{
  "type": "webhook",
  "url": "https://hooks.example.com/endpoint",
  "secret_ref": "whsec_default",
  "timeout_ms": 5000,
  "retry": {
    "max_attempts": 5,
    "backoff": "exponential"
  }
}
```

| Field | Required | Description |
|---|---|---|
| `url` | Yes | Target URL (must use HTTPS in production) |
| `secret_ref` | No | Key into `webhooks.secrets` in config; if absent, the request is unsigned |
| `timeout_ms` | No | Per-attempt timeout; falls back to `webhooks.default_timeout_ms` |
| `retry.max_attempts` | No | Falls back to `webhooks.max_attempts` |
| `retry.backoff` | No | `exponential` (only supported value) |

**Webhook request format**

The server POSTs JSON to your endpoint:

```json
{
  "event": { ... },
  "rule": {"id": "rule_01J3...", "name": "notify-on-close"}
}
```

Headers sent with every webhook:

| Header | Description |
|---|---|
| `X-HM-Signature` | `sha256=` + hex(HMAC-SHA256(secret, body)) |
| `X-HM-Delivery` | Stable delivery ID across retries (for deduplication) |
| `X-HM-Rule` | Rule ID |

Verify the signature on your receiver:

```python
import hmac, hashlib

def verify(body: bytes, header: str, secret: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)
```

Delivery is at-least-once. Your endpoint must be idempotent on `X-HM-Delivery`.

### Log action

```json
{
  "type": "log",
  "level": "warn"
}
```

Writes a structured log entry containing the event and rule ID. `level` can be any string; the service logs it as-is.

---

## Dead-letter queue (DLQ)

When a webhook exhausts its retries, it lands in the rule's dead-letter queue.

### List dead letters

```
GET /api/v1/collections/{collection}/rules/{ruleId}/dlq
```

**Query parameters**

| Parameter | Default | Description |
|---|---|---|
| `limit` | 100 | Page size (max 1,000) |
| `cursor` | — | Opaque pagination token |

**Response** (200):

```json
{
  "items": [
    {
      "id": "dlq_01J3...",
      "rule_id": "rule_01J3...",
      "collection": "crm.deals",
      "delivery_id": "dlv_01J3...",
      "url": "https://hooks.example.com/crm-events",
      "secret_ref": "whsec_default",
      "attempts": 5,
      "last_error": "timeout",
      "failed_at": "2026-06-02T10:00:00Z",
      "body": { ... }
    }
  ],
  "next_cursor": null
}
```

`body` is the exact JSON payload that was (or would have been) POSTed. `delivery_id` is the stable ID sent as `X-HM-Delivery` — use it to deduplicate on your receiver after redelivery.

### Redeliver dead letters

```
POST /api/v1/collections/{collection}/rules/{ruleId}/dlq:redeliver
```

Re-enqueues dead letters for another delivery attempt and removes them from the DLQ on successful re-enqueue.

**Request body** (optional):

```json
{
  "ids": ["dlq_01J3...", "dlq_01J4..."]
}
```

Omit `ids` (or send an empty body) to redeliver all of the rule's dead letters.

**Response** (202):

```json
{"requeued": 3}
```

Re-enqueued deliveries follow the same retry policy as the original. If they fail again, they return to the DLQ. A redelivered delivery has the same `delivery_id`.

Returns `404` if any specified ID is unknown or already redelivered.

---

## Audit log

All admin actions are recorded in the tenant's `ev__system` collection:

- `collection.config.updated` — when `PUT /collections/{c}/config` succeeds
- `rule.created`, `rule.updated`, `rule.deleted` — on rule CRUD

Audit entries are stored with the tenant ID as actor and are accessible via standard MongoDB queries (there is no API for the audit log in the current version).
