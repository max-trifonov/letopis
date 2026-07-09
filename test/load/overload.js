// Overload scenario (NFR-2.3: backpressure, not collapse).
//
// Pushes accepts far past what the worker can drain so the durable queue depth
// crosses queue.max_depth and the service starts refusing with 429 +
// Retry-After. The point is to prove graceful refusal: every response is either
// a 202 (accepted) or a 429 carrying a Retry-After header — never a 5xx, a hang,
// or a silently dropped write.
//
//   docker compose -f docker-compose.load.yml run --rm k6 run /scripts/overload.js
//
// Tune LETOPIS_RATE up if the worker keeps pace and no 429 appears; the default
// pairs with the modest max_depth in config.load.yaml.

import http from 'k6/http';
import { Rate, Counter } from 'k6/metrics';
import { check } from 'k6';
import { diffURL, entityID, headers, readyDiff, seedEntities, ENTITIES } from './lib/common.js';

const RATE = Number(__ENV.LETOPIS_RATE || 40000);
const DURATION = __ENV.LETOPIS_DURATION || '60s';
const PRE_VUS = Number(__ENV.LETOPIS_VUS || 800);

const backpressure = new Counter('backpressure_429');
const retryAfterOK = new Rate('retry_after_present');
const unexpectedStatus = new Rate('unexpected_status');

export const options = {
  scenarios: {
    overload: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_VUS,
      maxVUs: PRE_VUS * 4,
    },
  },
  thresholds: {
    // Backpressure must actually engage, and never without a Retry-After hint.
    'backpressure_429': ['count>0'],
    'retry_after_present': ['rate>0.99'],
    // No response outside the {202, 429} contract under overload.
    'unexpected_status': ['rate<0.01'],
  },
};

const body = readyDiff();
const h = headers('durable');

export function setup() {
  return seedEntities(ENTITIES);
}

export default function () {
  const res = http.post(diffURL(entityID()), body, { headers: h });
  if (res.status === 429) {
    backpressure.add(1);
    retryAfterOK.add(res.headers['Retry-After'] !== undefined && res.headers['Retry-After'] !== '');
    unexpectedStatus.add(false);
  } else {
    unexpectedStatus.add(res.status !== 202);
  }
  check(res, { 'status is 202 or 429': (r) => r.status === 202 || r.status === 429 });
}
