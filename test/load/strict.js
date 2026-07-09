// Strict-mode scenario (NFR-1.2: synchronous accept latency p99<50ms).
//
// Strict commits to Mongo with write concern majority before answering 201, so
// this scenario measures the full synchronous write path, not just enqueue. It
// runs at a moderate, fixed concurrency rather than a forced arrival rate:
// strict throughput is bounded by Mongo, so the question NFR-1.2 asks is "is the
// committed-write latency acceptable", not "how many per second".
//
//   docker compose -f docker-compose.load.yml run --rm k6 run /scripts/strict.js
//   LETOPIS_VUS=100 ... run /scripts/strict.js

import http from 'k6/http';
import { diffURL, entityID, headers, readyDiff, seedEntities, ENTITIES } from './lib/common.js';
import { check } from 'k6';

const VUS = Number(__ENV.LETOPIS_VUS || 50);
const DURATION = __ENV.LETOPIS_DURATION || '60s';

export const options = {
  scenarios: {
    strict: {
      executor: 'constant-vus',
      vus: VUS,
      duration: DURATION,
    },
  },
  thresholds: {
    // NFR-1.2: synchronous (committed) accept latency.
    'http_req_duration{expected_response:true}': ['p(99)<50'],
    'http_req_failed': ['rate<0.01'],
    'checks': ['rate>0.99'],
  },
};

const body = readyDiff();
const h = headers('strict');

export function setup() {
  return seedEntities(ENTITIES);
}

export default function () {
  const res = http.post(diffURL(entityID()), body, { headers: h });
  check(res, { 'status is 201': (r) => r.status === 201 });
}
