// Shared helpers for the S2-07 load scenarios: target resolution, auth headers,
// the ~2KB ready diff payload (NFR-1.3) and entity-id spread.
//
// A "ready diff" (not full state) is sent on purpose: it isolates the accept
// path from diff-computation CPU, so the measured 202 latency reflects
// validation + enqueue only (NFR-1.1). In durable/fast modes the diff-apply
// check is deferred to the worker, so repeating the same diff per entity does
// not affect accept latency or throughput.

import http from 'k6/http';
import { check } from 'k6';

export const BASE_URL = __ENV.LETOPIS_BASE_URL || 'http://localhost:8080';
export const WRITE_KEY = __ENV.LETOPIS_WRITE_KEY || 'load_write_key';
export const COLLECTION = __ENV.LETOPIS_COLLECTION || 'crm.deals';

// ENTITIES spreads writes across many entities (and therefore all 16 shards),
// so per-entity serialization (FR-1.10) never becomes the bottleneck under load.
export const ENTITIES = Number(__ENV.LETOPIS_ENTITIES || 5000);

export function headers(mode) {
  const h = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${WRITE_KEY}`,
  };
  if (mode) {
    h['X-Letopis-Mode'] = mode;
  }
  return h;
}

// entityID picks an entity from the configured spread, keyed off the VU and
// iteration so a run touches a wide, repeatable set.
export function entityID() {
  const n = (__VU * 100000 + __ITER) % ENTITIES;
  return `deal-${n}`;
}

export function diffURL(entity) {
  return `${BASE_URL}/api/v1/collections/${COLLECTION}/entities/${entity}/diff`;
}

// readyDiff is a ~2KB update diff matching the benchmark payload in
// internal/service/bench_test.go, so the k6 wire size and the Go CPU numbers
// describe the same event shape.
export function readyDiff() {
  const note =
    'reconciled against upstream CRM export; tier promotion approved by region lead';
  return JSON.stringify({
    op: 'update',
    author_id: 'svc-sync',
    source: 'crm',
    changes: [
      { path: 'status', op: 'change', old: 'processing', new: 'shipped' },
      { path: 'customer.tier', op: 'change', old: 'gold', new: 'platinum' },
      { path: 'customer.account_manager', op: 'change', old: 'amgr-12', new: 'amgr-30' },
      { path: 'total', op: 'change', old: 1299.5, new: 1450.0 },
      { path: 'currency', op: 'change', old: 'USD', new: 'EUR' },
      { path: 'shipping.carrier', op: 'change', old: 'dhl', new: 'fedex' },
      { path: 'shipping.tracking', op: 'add', new: 'FX-99-2841-0033-7712' },
      { path: 'shipping.address.city', op: 'change', old: 'Berlin', new: 'Munich' },
      { path: 'shipping.address.postcode', op: 'change', old: '10115', new: '80331' },
      { path: 'items.1.price', op: 'change', old: 901.5, new: 950.0 },
      { path: 'items.2.qty', op: 'change', old: 4, new: 6 },
      { path: 'tags.2', op: 'add', new: 'expedited' },
      { path: 'notes', op: 'change', old: '', new: note },
      { path: 'review.note', op: 'change', old: '', new: note },
      { path: 'review.reviewer', op: 'change', old: 'ops-1', new: 'ops-7' },
      { path: 'review.checklist', op: 'add', new: ['credit', 'stock', 'fraud', 'export-licence'] },
      { path: 'billing.method', op: 'change', old: 'invoice', new: 'card' },
      { path: 'billing.terms', op: 'change', old: 'net-30', new: 'net-15' },
      { path: 'fulfilment.warehouse', op: 'change', old: 'wh-eu-1', new: 'wh-eu-3' },
      { path: 'fulfilment.priority', op: 'change', old: 'standard', new: 'high' },
    ],
  });
}

// seedState is the base document each entity is created with before the load
// loop sends diffs against it. It carries every parent object the ready diff
// descends into, so the diff applies cleanly and the event lands in Mongo (the
// precondition for measuring the 202→stored lag, NFR-1.6).
export function seedState() {
  return {
    status: 'processing',
    total: 1299.5,
    currency: 'USD',
    customer: { id: 'cust-77', tier: 'gold', account_manager: 'amgr-12' },
    shipping: { carrier: 'dhl', address: { city: 'Berlin', postcode: '10115' } },
    items: [
      { sku: 'A-1', qty: 2, price: 199.0 },
      { sku: 'B-7', qty: 1, price: 901.5 },
      { sku: 'C-3', qty: 4, price: 49.75 },
    ],
    tags: ['priority', 'export'],
    notes: '',
    review: { note: '', reviewer: 'ops-1' },
    billing: { method: 'invoice', terms: 'net-30' },
    fulfilment: { warehouse: 'wh-eu-1', priority: 'standard' },
  };
}

// seedEntities creates the entity set synchronously (strict mode) so every
// entity exists in Mongo before the load loop runs. Called from a scenario's
// setup(); k6 runs it once on a single VU. Idempotent across re-runs — a repeat
// create simply appends a version.
export function seedEntities(count) {
  const body = JSON.stringify({ op: 'create', source: 'seed', state: seedState() });
  const h = headers('strict');
  let ok = 0;
  for (let i = 0; i < count; i++) {
    const res = http.post(diffURL(`deal-${i}`), body, { headers: h });
    if (res.status === 201) {
      ok++;
    }
  }
  return { seeded: ok, requested: count };
}

// expectAccepted asserts an async accept (durable/fast return 202).
export function expectAccepted(res) {
  return check(res, {
    'status is 202': (r) => r.status === 202,
    'body carries ticket_id': (r) => {
      try {
        return JSON.parse(r.body).ticket_id !== undefined;
      } catch (_) {
        return false;
      }
    },
  });
}
