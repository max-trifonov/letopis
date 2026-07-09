# Write API

All write endpoints live under `/api/v1` and require a Bearer token with the `write` scope.

```
Authorization: Bearer <api-key>
```

The tenant is derived from the key. It never appears in the URL.

---

## Authentication errors

| Status | Meaning |
|---|---|
| `401 Unauthorized` | Missing or invalid API key |
| `403 Forbidden` | Key lacks the required scope or collection access |

---

## Reliability mode

Every write endpoint accepts an optional `X-Letopis-Mode` header that overrides the collection's default mode for this request:

| Value | Response | Guarantee |
|---|---|---|
| `strict` | `201` after MongoDB write | Synchronous; confirmed before responding |
| `durable` | `202` + ticket | Queued to Redis Streams; worker writes asynchronously |
| `fast` | `202` + ticket | In-memory queue; maximum throughput, lower durability |

Absent the header, the collection's configured `reliability_mode` applies (default `durable`).

`202` responses return a `ticket_id` you can poll at `GET /tickets/{id}`.

---

## Idempotency

To prevent duplicate events on retry, pass a client-generated key via:

- The `Idempotency-Key` header, **or**
- The `event_id` field in the request body (`event_id` takes precedence when both are present).

If the same key arrives again within the dedup window (default 24 h, configurable), the server replays the original response:

- `200 {"status": "duplicate"}` if the original already landed.
- The same `202` ticket body if the original was accepted but not yet stored.

No second event is written.

---

## Ingest full state

```
POST /api/v1/collections/{collection}/entities/{entityId}/state
```

Send the entity's complete current state. The server computes the diff against the last known state. If the entity is new, all fields are recorded as `add` operations and the event is typed `create` (or `update` if `first_event_op: update` is configured for the collection).

**Path parameters**

| Parameter | Description |
|---|---|
| `collection` | Collection name, e.g. `crm.deals` (`^[a-z0-9]+(?:\.[a-z0-9]+)*$`) |
| `entityId` | Entity identifier, any non-empty string |

**Headers**

| Header | Required | Description |
|---|---|---|
| `Authorization` | Yes | `Bearer <key>` with `write` scope |
| `X-Letopis-Mode` | No | Override reliability mode |
| `Idempotency-Key` | No | Client-generated dedup key |

**Request body**

```json
{
  "state": {
    "title": "Acme Corp",
    "amount": 5000,
    "stage": "prospect"
  },
  "op": "update",
  "event_id": "evt-001",
  "author_id": "user-42",
  "source": "crm-backend",
  "ts_source": "2026-06-01T10:00:00Z",
  "expected_version": 3,
  "meta": {
    "ip": "10.0.0.1",
    "session": "abc123"
  },
  "flow": {
    "flow_id": "flow-deal-onboarding",
    "step": "qualification",
    "caused_by": [
      {"activity_id": "act-111"}
    ]
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `state` | object | Yes | The full current state of the entity |
| `op` | string | No | `create` or `update`; inferred if absent (see `first_event_op`) |
| `event_id` | string | No | Client-assigned ID for idempotency |
| `author_id` | string | No | Opaque identifier of who made the change |
| `source` | string | No | Identifier of the source system |
| `ts_source` | RFC3339 | No | Timestamp of the change in the source system |
| `expected_version` | integer | No | Optimistic lock: fails with `409` if the entity's current version differs |
| `meta` | object | No | Arbitrary metadata stored with the event |
| `flow` | object | No | Business-flow block (see [Concepts — Business flows](concepts.md)) |

**Responses**

| Status | Body | Meaning |
|---|---|---|
| `201` | `WriteResult` | Written synchronously (`strict` mode) |
| `202` | `Accepted` | Queued asynchronously (`durable`/`fast`) |
| `200` | `{"status": "no_changes"}` | State is identical to last known — no event recorded |
| `200` | `{"status": "duplicate"}` | Idempotent replay of an already-stored event |
| `400` | `Error` | Malformed request (invalid collection name, missing `state`, etc.) |
| `409` | `Error` | `expected_version` did not match |
| `413` | `Error` | Body exceeds `max_event_size_bytes` |
| `429` | `Error` | Queue at capacity; retry after `Retry-After` seconds |
| `503` | `Error` | Queue accept failed; retry later |

**WriteResult** (201):

```json
{
  "entity_id": "deal-1",
  "version": 4,
  "changes_count": 2
}
```

**Accepted** (202):

```json
{
  "ticket_id": "tkt_01J3...",
  "status": "accepted"
}
```

---

## Ingest a diff

```
POST /api/v1/collections/{collection}/entities/{entityId}/diff
```

Send a ready-made diff. Useful when your source system already computes what changed. The server validates the format and stores it as-is.

**Request body**

```json
{
  "changes": [
    {"path": "amount",  "op": "change", "old": 5000, "new": 7500},
    {"path": "stage",   "op": "change", "old": "prospect", "new": "qualified"}
  ],
  "op": "update",
  "author_id": "user-42",
  "source": "crm-backend",
  "ts_source": "2026-06-02T09:00:00Z"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `changes` | array | No* | List of field changes |
| `state` | object | No* | Full state (allowed when `op: create` instead of `changes`) |
| `op` | string | No | `create`, `update`, or `delete` |

\* One of `changes` or `state` must be present (except for `op: delete`, which uses the delete endpoint).

**Change object**

| Field | Type | Description |
|---|---|---|
| `path` | string | Dot-notation field path, e.g. `address.city` or `items.0.price` |
| `op` | string | `add`, `change`, or `remove` |
| `old` | any | Previous value (omit for `add`) |
| `new` | any | New value (omit for `remove`) |

Responses are identical to the state endpoint.

---

## Record a deletion

```
POST /api/v1/collections/{collection}/entities/{entityId}/delete
```

Records the entity as deleted. The history is preserved; the entity can be "reincarnated" by a subsequent create. The current-state record is marked `deleted: true`.

**Request body** (optional)

```json
{
  "author_id": "user-42",
  "source": "crm-backend",
  "ts_source": "2026-06-03T12:00:00Z",
  "meta": {"reason": "merged with deal-2"}
}
```

All fields are optional. Responses are the same as the state endpoint (no `413` or `409` with `expected_version` conflict).

---

## Batch ingest

```
POST /api/v1/events:batch
```

Accept up to 1,000 events across any collections and entities in a single request. The batch is **not atomic**: each event is validated independently; invalid items are rejected and returned in the response; the rest are published normally.

A single `X-Letopis-Mode` header applies to all events in the batch.

**Request body**

```json
{
  "events": [
    {
      "collection": "crm.deals",
      "entity_id": "deal-1",
      "type": "state",
      "payload": {
        "state": {"amount": 8000},
        "author_id": "user-42"
      }
    },
    {
      "collection": "crm.contacts",
      "entity_id": "contact-99",
      "type": "diff",
      "payload": {
        "changes": [{"path": "email", "op": "change", "old": "old@x.com", "new": "new@x.com"}]
      }
    }
  ]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `events` | array | Yes | 1–1,000 events |
| `events[].collection` | string | Yes | Target collection |
| `events[].entity_id` | string | Yes | Target entity |
| `events[].type` | string | Yes | `state`, `diff`, or `delete` |
| `events[].payload` | object | Yes | Same body as the per-event endpoint for the given type |

**Response** (always `202`):

```json
{
  "ticket_id": "tkt_01J3...",
  "accepted": 2,
  "rejected": [
    {
      "index": 2,
      "error": {
        "code": "invalid_type",
        "message": "unknown event type: patch"
      }
    }
  ]
}
```

The `ticket_id` status is `accepted` when all events were accepted, `partial` when some were rejected. Per-event processing is not individually tracked — the ticket covers the accepted batch as a unit.

**Limits**

| Limit | Value |
|---|---|
| Max events per batch | 1,000 (returns `400` if exceeded) |
| Max total body size | 32 MiB (returns `413` if exceeded) |

---

## Ticket status

```
GET /api/v1/tickets/{ticketId}
```

Poll the status of an async write accepted with `202`.

**Response** (200):

```json
{
  "ticket_id": "tkt_01J3...",
  "status": "stored",
  "entity_collection": "crm.deals",
  "entity_id": "deal-1",
  "created_at": "2026-06-01T10:00:00Z",
  "updated_at": "2026-06-01T10:00:01Z"
}
```

| `status` | Meaning |
|---|---|
| `accepted` | Received and queued; not yet picked up by a worker |
| `processing` | Worker is writing to MongoDB |
| `stored` | Successfully written |
| `failed` | Write failed; `error` field contains the reason |
| `partial` | Batch: some events stored, some failed |

Tickets expire after a configurable TTL (default 24 h). An expired or unknown ticket returns `404`.

---

## Common error format

All error responses use the same envelope:

```json
{"error": "human-readable reason"}
```

For batch rejects, a richer object is used:

```json
{
  "index": 2,
  "error": {
    "code": "too_large",
    "message": "payload exceeds max_event_size_bytes"
  }
}
```

---

## Examples

### Strict write with optimistic lock

```sh
curl -X POST http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/state \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Letopis-Mode: strict" \
  -H "Content-Type: application/json" \
  -d '{
    "state": {"amount": 9000, "stage": "closed-won"},
    "expected_version": 7,
    "author_id": "user-42"
  }'
```

### Idempotent diff ingest

```sh
curl -X POST http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/diff \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: my-system-txn-id-abc" \
  -H "Content-Type: application/json" \
  -d '{
    "changes": [{"path": "amount", "op": "change", "old": 9000, "new": 10000}],
    "author_id": "user-42"
  }'
```

### Delete and check the ticket

```sh
# Delete
RESP=$(curl -s -X POST http://localhost:8080/api/v1/collections/crm.deals/entities/deal-1/delete \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"author_id":"admin"}' -H "Content-Type: application/json")
TICKET=$(echo $RESP | jq -r .ticket_id)

# Poll until stored
curl http://localhost:8080/api/v1/tickets/$TICKET \
  -H "Authorization: Bearer $TOKEN"
```
