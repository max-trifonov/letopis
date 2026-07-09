// Fast-mode accept scenario (NFR-1.1 latency, NFR-1.3 throughput).
//
// Identical shape to durable.js but with X-Letopis-Mode: fast, so the accept
// goes onto the in-process memory queue (architecture §2) instead of Redis
// Streams. Expected to match or beat durable latency since the publish is a
// channel send rather than a network round-trip; the batcher flushes via
// insertMany behind it.
//
//   docker compose -f docker-compose.load.yml run --rm k6 run /scripts/fast.js

import http from 'k6/http';
import { diffURL, entityID, headers, readyDiff, expectAccepted, seedEntities, ENTITIES } from './lib/common.js';

const RATE = Number(__ENV.LETOPIS_RATE || 10000);
const DURATION = __ENV.LETOPIS_DURATION || '60s';
const PRE_VUS = Number(__ENV.LETOPIS_VUS || 400);

export const options = {
  scenarios: {
    fast: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_VUS,
      maxVUs: PRE_VUS * 4,
    },
  },
  thresholds: {
    'http_req_duration{expected_response:true}': ['p(50)<2', 'p(99)<10'],
    'http_req_failed': ['rate<0.01'],
    'checks': ['rate>0.99'],
  },
};

const body = readyDiff();
const h = headers('fast');

export function setup() {
  return seedEntities(ENTITIES);
}

export default function () {
  const res = http.post(diffURL(entityID()), body, { headers: h });
  expectAccepted(res);
}
