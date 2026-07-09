# Read API

All read endpoints live under `/api/v1` and require a Bearer token with at least the `read` scope.

```
Authorization: Bearer <api-key>
```

---

## Entity history

```
GET /api/v1/collections/{collection}/entities/{entityId}/history
```

Returns a paginated list of events for an entity, newest-first by default.

**Query parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `from` | RFC3339 | — | Return events received at or after this time |
| `to` | RFC3339 | — | Return events received before or at this time |
| `author_id` | string | — | Filter to events from this author |
| `op` | string | — | Filter by operation: `create`, `update`, or `delete` |
| `path` | string | — | Filter to events that touch this field path or any path nested under it |
| `source` | string | — | Filter by source system identifier |
| `limit` | integer | 100 | Page size (1–1,000) |
| `cursor` | string | — | Opaque pagination token from a previous `next_cursor` |
| `order_by` | string | `version` | Sort key: `version`, `ts_source`, or `ts_received` |
| `order` | string | `desc` | `asc` or `desc` |
| `format` | string | `native` | Diff format: `native` (Letopis format) or `json-patch` (RFC 6902) |

**Response** (200):

```json
{
  "entity_id": "deal-1",
  "next_cursor": "eyJ2IjoxMH0=",
  "events": [
    {
      "version": 4,
      "op": "update",
      "ts_received": "2026-06-02T09:00:01Z",
      "ts_stored":   "2026-06-02T09:00:01Z",
      "ts_source":   "2026-06-02T09:00:00Z",
      "author_id": "user-42",
      "source": "crm-backend",
      "meta": {},
      "integrity": {
        "hash":      "sha256:9f86d0...",
        "prev_hash": "sha256:2c2640..."
      },
      "changes": [
        {"path": "amount", "op": "change", "old": 5000, "new": 7500},
        {"path": "stage",  "op": "change", "old": "prospect", "new": "qualified"}
      ]
    }
  ]
}
```

The `integrity` field is present only when the `hash_chain` plugin is enabled for the collection.

When `format=json-patch`, each element in `changes` follows RFC 6902: `{"op": "replace", "path": "/amount", "value": 7500}`. The native format is richer (it includes `old`) and should be preferred for display.

**Pagination:** Pass the `next_cursor` value from the previous response as the `cursor` parameter. When `next_cursor` is `null`, you have reached the last page.

---

## Current state

```
GET /api/v1/collections/{collection}/entities/{entityId}/state
```

Returns the entity's current materialized state.

**Response** (200):

```json
{
  "entity_id": "deal-1",
  "version": 4,
  "ts": "2026-06-02T09:00:01Z",
  "deleted": false,
  "state": {
    "title": "Acme Corp",
    "amount": 7500,
    "stage": "qualified"
  }
}
```

Returns `404` if the entity has never been written (or has been purged).

---

## Point-in-time state

```
GET /api/v1/collections/{collection}/entities/{entityId}/state?version=N
GET /api/v1/collections/{collection}/entities/{entityId}/state?at=<RFC3339>
```

Reconstructs the entity's state at a past version or received-time cutoff. The `version` and `at` parameters are mutually exclusive.

| Parameter | Description |
|---|---|
| `version` | Reconstruct at this version (clamped to the latest if higher) |
| `at` | Reconstruct as of this received-time cutoff (RFC3339); returns the state after the last event with `ts_received ≤ at` |

`?at` always uses `ts_received` (the write-order timestamp, ADR-011). The `?at_source` variant is reserved for a future ordering mode and returns `400` if used.

**Response** (200) adds `reconstructed_from` to distinguish from a current-state response:

```json
{
  "entity_id": "deal-1",
  "version": 2,
  "ts": "2026-06-01T10:00:01Z",
  "deleted": false,
  "state": {
    "title": "Acme Corp",
    "amount": 5000,
    "stage": "prospect"
  },
  "reconstructed_from": {
    "snapshot_version": null,
    "events_applied": 2
  }
}
```

`snapshot_version` is the version of the snapshot used as the base, or `null` when the reconstruction started from genesis (no snapshot available). `events_applied` is the number of tail events replayed onto that base.

Returns `404` if the entity does not exist, or if `?at` is before the entity's first event.
Returns `400` if both `version` and `at` are provided, if `version < 1`, or if `at_source` is used.

---

## List collections

```
GET /api/v1/collections
```

Returns all collections the API key can access (filtered by the key's collection mask), with basic statistics.

**Response** (200):

```json
{
  "collections": [
    {
      "name": "crm.deals",
      "entities": 1243,
      "events": 18750,
      "last_event_at": "2026-06-02T09:00:01Z",
      "config": {
        "reliability_mode": "durable",
        "snapshot_interval": 100,
        "retention": {"type": "forever"},
        "max_event_size_bytes": 1048576,
        "first_event_op": "create",
        "ordering": {"mode": "received"}
      }
    }
  ]
}
```

`entities` is the exact count of distinct entities (from the `cur_*` collection). `events` is an estimated count (MongoDB `estimatedDocumentCount`, fast and non-blocking). `last_event_at` is the stored timestamp of the most recent event, or `null` for a collection with no events.

Auto-created collections (written to without an explicit `PUT /config`) are included. The `config` shown has defaults applied.

---

## Business flows

### Record an activity

```
POST /api/v1/activities
```

Records a business-process event (not an entity change) and associates it with a flow. Requires `write` scope.

**Request body**

```json
{
  "type": "invoice.approval.started",
  "flow_id": "flow-invoice-cycle-42",
  "author_id": "user-7",
  "source": "approval-service",
  "ts_source": "2026-06-02T11:00:00Z",
  "caused_by": [
    {"collection": "invoices", "entity_id": "inv-99", "version": 3}
  ],
  "refs": [
    {"collection": "crm.deals", "entity_id": "deal-1"}
  ],
  "data": {
    "approver_email": "manager@example.com"
  },
  "meta": {}
}
```

| Field | Description |
|---|---|
| `activity_id` | Optional; the server mints a ULID if absent |
| `type` | Semantic label (opaque to Letopis) |
| `flow_id` | The flow to attach to; a new flow is created if absent |
| `caused_by` | Upstream events or activities that caused this one |
| `refs` | Related entities (informational, not causal) |
| `data` | Arbitrary payload |

**Response** (201):

```json
{
  "activity_id": "act_01J3...",
  "flow_id": "flow-invoice-cycle-42"
}
```

### Get a flow

```
GET /api/v1/flows/{flowId}
```

Returns all nodes of a flow (events and activities) in received order, with their causal edges.

**Query parameters**

| Parameter | Default | Description |
|---|---|---|
| `limit` | 100 | Page size (1–1,000) |
| `cursor` | — | Opaque pagination token |

**Response** (200):

```json
{
  "flow_id": "flow-invoice-cycle-42",
  "next_cursor": null,
  "nodes": [
    {
      "kind": "event",
      "ts_received": "2026-06-01T10:00:01Z",
      "collection": "invoices",
      "entity_id": "inv-99",
      "version": 3,
      "op": "update",
      "step": "amount-adjusted",
      "caused_by": []
    },
    {
      "kind": "activity",
      "ts_received": "2026-06-02T11:00:01Z",
      "activity_id": "act_01J3...",
      "type": "invoice.approval.started",
      "caused_by": [
        {"collection": "invoices", "entity_id": "inv-99", "version": 3}
      ],
      "refs": [
        {"collection": "crm.deals", "entity_id": "deal-1"}
      ],
      "data": {"approver_email": "manager@example.com"}
    }
  ]
}
```

Each node carries `kind: event | activity`. Event nodes include `collection`, `entity_id`, `version`, `op`, and `step`. Activity nodes include `activity_id`, `type`, `refs`, and `data`. Both carry `caused_by` and `ts_received`.

---

## Examples

### Fetch the last 10 changes to a field

```sh
curl "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/history?path=stage&limit=10" \
  -H "Authorization: Bearer $TOKEN"
```

### Reconstruct state one week ago

```sh
curl "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/state?at=2026-05-26T00:00:00Z" \
  -H "Authorization: Bearer $TOKEN"
```

### Paginate through history

```sh
# First page
RESP=$(curl -s "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/history?limit=50" \
  -H "Authorization: Bearer $TOKEN")
CURSOR=$(echo $RESP | jq -r .next_cursor)

# Next page
curl "http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/history?limit=50&cursor=$CURSOR" \
  -H "Authorization: Bearer $TOKEN"
```
