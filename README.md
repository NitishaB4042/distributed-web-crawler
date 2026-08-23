# Distributed Web Crawler

A horizontally-scalable web crawler built in Python, backed by Redis. Many
stateless workers share one frontier and dedup set, stay polite to each host,
survive crashes without losing work, and scale near-linearly with worker count.

Built in five phases, each independently runnable and tested.

![Throughput scales with worker count](phase5/benchmark.png)

*Measured throughput: 31 → 265 pages/sec across 1–16 workers. The gap from the
ideal-linear line at higher worker counts is expected — Redis coordination
overhead and per-domain rate limiting cap per-host concurrency (see Phase 5).*

---

## What it does

- **Crawls** a site breadth-first, extracting and following links.
- **Coordinates** many stateless workers through Redis — add or kill workers
  freely; all state lives in Redis.
- **Deduplicates** URLs with an atomic check-and-add, after canonicalizing them
  so equivalent URLs collapse to one key.
- **Stays polite** with per-domain token-bucket rate limiting and robots.txt
  enforcement.
- **Survives crashes** via a claim/ack pattern with dead-worker recovery,
  retries with exponential backoff, and a dead-letter queue.
- **Is observable** through a live Streamlit dashboard and a reproducible
  scaling benchmark.

## Architecture

```
                ┌─────────────────────────────────────────┐
                │                  REDIS                   │
                │                                          │
   ┌────────┐   │  frontier (list)   visited (set)         │
   │ Worker │◄──┤  inflight (zset)   buckets (per-domain)   │
   ├────────┤   │  dlq (list)        robots cache           │
   │ Worker │◄──┤  stats (hash)      samples (time series)  │
   ├────────┤   └─────────────────────────────────────────┘
   │ Worker │              ▲                    ▲
   └────────┘              │                    │
        │            ┌───────────┐       ┌─────────────┐
        │            │Recoverer  │       │ Dashboard   │
   fetch+parse       │(requeues  │       │ (Streamlit) │
   +enqueue links    │ dead jobs)│       └─────────────┘
                     └───────────┘
```

Workers are identical and stateless. The recoverer requeues jobs whose claims
expired (a presumed-dead worker). The dashboard reads metrics samples. None of
them hold state locally — Redis is the single source of truth.

## Phases

| Phase | Adds | Key concept |
|-------|------|-------------|
| **1** | single-process async loop | URL canonicalization |
| **2** | Redis-backed shared state | atomic dedup, stateless workers |
| **3** | politeness layer | token bucket (atomic Lua), robots.txt |
| **4** | fault tolerance | claim pattern, at-least-once, dead-letter queue |
| **5** | metrics + benchmark | observability, horizontal-scaling proof |

Each phase folder has its own README with the details and design rationale.

## Quick start

```bash
pip install -r requirements.txt

# Use a free cloud Redis (Upstash / Redis Cloud) — no install needed on a tablet
export REDIS_URL="rediss://default:<password>@<host>.upstash.io:6379"

# Run the full crawler (Phase 5 = Phase 4 + live metrics)
python phase5/run_phase5.py --seed https://quotes.toscrape.com \
    --workers 4 --recover --sample --max-pages 200

# In another terminal / Colab cell: live dashboard
streamlit run phase5/dashboard.py
```

> On a tablet: run the workers in one Colab cell and the dashboard in another,
> both pointed at the same `REDIS_URL`. No local terminal required.

## Running the benchmark

```bash
python phase5/benchmark.py --pages 400 --workers 1 2 4 8 16 --fetch-latency 0.03
```

Runs the real Phase 4 crawler over a deterministic in-process mock site with
simulated network latency, at increasing worker counts. Writes
`benchmark.png` and `benchmark_results.json`.

## Tests

Every phase ships tests. Phases 2–5 need a reachable Redis.

```bash
python phase1/test_phase1.py   # no Redis needed (mocked)
python phase2/test_phase2.py
python phase3/test_phase3.py
python phase4/test_phase4.py
```

Highlights:
- **Phase 2** — 5 concurrent workers crawl an 11-page cross-linked site exactly
  once (no duplicates).
- **Phase 3** — 50 concurrent token-bucket takes on a 10-token bucket grant
  *exactly* 10 (proves the Lua script is race-free).
- **Phase 4** — workers randomly crash mid-job; with the recoverer running,
  every reachable page is still crawled (the no-loss invariant).

## Tech stack

- `aiohttp` — async HTTP fetching (crawling is I/O-bound; async gives real
  concurrency despite the GIL)
- `selectolax` — fast, lightweight HTML parsing
- `redis` — shared frontier, dedup, rate-limit buckets, metrics
- `streamlit` + `pandas` — live dashboard
- `matplotlib` — benchmark plot

## Design decisions worth knowing

- **Atomic Lua scripts** for dedup, rate limiting, and claim/recover, so
  read-modify-write is race-free across workers without explicit locks.
- **At-least-once + idempotent consumer**: a worker may finish then die before
  acking, so a URL can be crawled twice. That's fine — re-fetching overwrites
  the same result. This mirrors how SQS works (claim = receive,
  visibility timeout, ack = delete, DLQ = dead-letter queue).
- **Sub-linear scaling is honest.** The benchmark models network latency
  because that's what a crawler actually parallelizes; the curve bends away
  from linear at high worker counts due to Redis coordination and per-domain
  limits. Removing that bottleneck = sharding the frontier by domain hash,
  partitioning the dedup filter, and moving to Kafka.

## Scaling further (to ~1B pages)

- Shard the frontier by domain hash so one domain can't bottleneck others.
- Replace the Redis set with a partitioned **bloom filter** for the visited set
  (trades rare false positives — occasionally skipping a real page — for huge
  memory savings; never re-crawls).
- Swap Redis lists for **Kafka** for a durable, high-throughput queue.
- Separate fetch and parse tiers so each scales independently.
