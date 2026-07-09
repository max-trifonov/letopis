# Load tests (S2-07)

Reproducible load scenarios that confirm the async pipeline (S2-01…06) meets
NFR-1 with numbers rather than assumptions:

| NFR | Target | Scenario |
|---|---|---|
| NFR-1.1 | 202 accept latency p50<2ms, p99<10ms | `durable.js`, `fast.js` |
| NFR-1.2 | strict (committed) latency p99<50ms | `strict.js` |
| NFR-1.3 | throughput ≥10 000 events/s (async, ~2KB event) | `durable.js`, `fast.js` |
| NFR-1.6 | 202→stored lag p99<1s | `consumer_lag_seconds` on `/metrics` during a `durable.js` run |
| NFR-2.3 | overload → 429 + Retry-After, no loss/hang | `overload.js` |

Tooling: [k6](https://k6.io) for the HTTP path; Go benchmarks
(`internal/service/bench_test.go`, `internal/diff/bench_test.go`) for the
CPU-side cost of the codec and diff engine. The latest measured baseline lives
in `docs/perf/`.

## Why a ready diff and not full state

Every scenario posts a ready ~2KB diff to
`POST /api/v1/collections/{c}/entities/{e}/diff`. Sending a diff (not full
state) keeps diff-computation CPU out of the accept path, so the measured
latency is validation + enqueue (NFR-1.1). Each entity is seeded once in
`setup()` with a full base document so the diff applies cleanly and the event
lands in Mongo — the precondition for the 202→stored lag measurement.

Writes spread across `LETOPIS_ENTITIES` (default 5000) entities so they hash
across all 16 shards and per-entity serialization (FR-1.10) is never the
bottleneck.

## Running

From the repo root (needs Docker + Compose):

```sh
make load-up           # build the service image, start Letopis + Mongo + Redis
make load-durable      # NFR-1.1 / NFR-1.3, durable mode
make load-fast         # NFR-1.1 / NFR-1.3, fast mode
make load-strict       # NFR-1.2, synchronous mode
make load-overload     # NFR-2.3, backpressure
make load-down         # stop and drop volumes
```

While a run is in flight, scrape the pipeline metrics for the lag (NFR-1.6):

```sh
curl -s localhost:8080/metrics | grep -E 'letopis_(consumer_lag_seconds|queue_depth)'
```

k6 prints the summary to the console and writes `results/<scenario>.json`
(thresholds encode the NFR targets, so a non-zero k6 exit means a missed NFR).

## Knobs (environment)

| Var | Default | Meaning |
|---|---|---|
| `LETOPIS_RATE` | 10000 (40000 for overload) | target arrival rate, req/s |
| `LETOPIS_DURATION` | `60s` | run length |
| `LETOPIS_VUS` | 400 (50 strict) | preallocated / fixed virtual users |
| `LETOPIS_ENTITIES` | 5000 | distinct entities (shard spread) |

The k6 container reads `LETOPIS_BASE_URL` and `LETOPIS_WRITE_KEY` from the
compose file; against a non-compose target, set them yourself and run k6
directly:

```sh
LETOPIS_BASE_URL=https://host:8080 LETOPIS_WRITE_KEY=… k6 run durable.js
```
