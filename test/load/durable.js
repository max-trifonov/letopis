// Durable-mode accept scenario (NFR-1.1 latency, NFR-1.3 throughput).
//
// Holds a constant arrival rate (default 10000 req/s, the NFR-1.3 target) for a
// fixed window and asserts the 202 accept latency stays within NFR-1.1
// (p50<2ms, p99<10ms). Sustaining the target rate with the failure threshold
// green is the NFR-1.3 evidence; the worker drains the Redis-Streams backlog
// behind the accept, observed via the lag scenario.
//
//   docker compose -f docker-compose.load.yml run --rm k6 run /scripts/durable.js
//   LETOPIS_RATE=15000 ... run /scripts/durable.js   # push past the target

import http from 'k6/http';
import { diffURL, entityID, headers, readyDiff, expectAccepted, seedEntities, ENTITIES } from './lib/common.js';

const RATE = Number(__ENV.LETOPIS_RATE || 10000);
const DURATION = __ENV.LETOPIS_DURATION || '60s';
const PRE_VUS = Number(__ENV.LETOPIS_VUS || 400);

export const options = {
  scenarios: {
    durable: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_VUS,
      maxVUs: PRE_VUS * 4,
    },
  },
  thresholds: {
    // NFR-1.1: async accept latency. expected_response keeps 4xx/5xx out of the
    // percentile so a backpressure 429 does not flatter the numbers.
    'http_req_duration{expected_response:true}': ['p(50)<2', 'p(99)<10'],
    'http_req_failed': ['rate<0.01'],
    'checks': ['rate>0.99'],
  },
};

const body = readyDiff();
const h = headers('durable');

export function setup() {
  return seedEntities(ENTITIES);
}

export default function () {
  const res = http.post(diffURL(entityID()), body, { headers: h });
  expectAccepted(res);
}
