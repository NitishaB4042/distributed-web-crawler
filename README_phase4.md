# Distributed Web Crawler — Phase 4

Adds **fault tolerance**: the claim pattern, dead-worker recovery, retries with
exponential backoff, and a dead-letter queue. This is the phase that turns the
project from a script into a system — workers can crash and no URL is lost.

## Files
- `crawler_phase4.py` — fault-tolerant crawler
- `test_phase4.py` — tests (need a running Redis)

## The problem
In Phase 2/3 a worker did `BRPOP` — pop-and-forget. If it crashed *after*
popping a URL but *before* finishing, that URL vanished. At scale workers do
crash, so silent loss is unacceptable.

## The fix — claim pattern (at-least-once)
1. **CLAIM** — atomically move a URL off the frontier into an in-flight ZSET,
   scored with a deadline (`now + visibility_timeout`). The URL is reserved,
   not lost.
2. **PROCESS** — fetch + parse.
3. **ACK** — on success, remove from in-flight. Done.
4. **RECOVER** — a coordinator periodically requeues in-flight jobs whose
   deadline has passed (presumed-dead worker).

All three Redis ops (claim, ack, recover) are **Lua scripts**, so they're
atomic. That's what makes `recover` safe to run from multiple coordinators at
once — each expired job is requeued exactly once.

### Why at-least-once, not exactly-once?
A worker can finish, then die before its ACK lands. The recoverer will requeue
that URL, so it may be crawled twice. That's fine: **crawling is idempotent** —
re-fetching a page just overwrites the same result. Use the vocabulary
"at-least-once + idempotent consumer" in interviews; it's exactly right.

## Retries & dead-letter queue
`fetch()` now classifies failures:
- **5xx / 429 / network errors** → retryable. Requeue with `attempts+1` after
  exponential backoff (1s, 2s, 4s, 8s).
- **4xx / non-HTML** → permanent. Don't retry.

After `MAX_ATTEMPTS` (default 4), the URL goes to the **dead-letter queue**
(`crawl:dlq`) for inspection instead of retrying forever.

## Setup & run
```bash
pip install aiohttp selectolax redis
export REDIS_URL="rediss://...upstash.io:6379"   # or local

# run workers AND the background recoverer in one process
python crawler_phase4.py --seed https://quotes.toscrape.com \
    --workers 4 --recover --visibility-timeout 30 --max-pages 50
```
On separate machines/cells, run extra `--workers` processes and one process
with `--recover`. Only one recoverer is needed, but more is harmless (it's
idempotent).

```bash
python crawler_phase4.py --report   # crawled / retried / recovered / dlq / in-flight
python crawler_phase4.py --reset
```

## Test
```bash
python test_phase4.py
```
- `dead_worker_recovery` — a claimed-but-unacked job is requeued after its
  deadline.
- `ack_prevents_recovery` — an acked job is *not* requeued.
- `concurrent_recover_idempotent` — 4 coordinators racing on 5 expired jobs
  requeue each exactly once (no double-requeue).
- `retry_then_dlq` — a permanently-failing URL is retried 3× then dead-lettered.
- `no_loss_under_crashes` — **the headline test**: 4 workers randomly crash
  mid-job (~30% of claims); with the recoverer running, every reachable page is
  still crawled. Proves the no-loss invariant.

## New Redis keys
- `crawl:inflight` — ZSET, member=job JSON, score=deadline (ms)
- `crawl:dlq` — LIST of dead-lettered jobs
- jobs are now JSON `{"u":url,"d":depth,"a":attempts}` (Phase 2/3 used
  `"depth|url"`; we needed the attempt counter for retries)
- `crawl:stats` adds `retried`, `recovered`, `dead_lettered`

## Interview notes
- **Visibility timeout** is the tuning knob: too short → healthy-but-slow
  workers get their jobs stolen (duplicate work); too long → slow recovery
  after a real crash. This is the exact trade SQS exposes.
- **Why a ZSET for in-flight?** Scored by deadline, so "find everything expired"
  is a single `ZRANGEBYSCORE -inf now` — O(log n + m).
- **Why JSON jobs?** The retry counter must travel with the URL. The member
  bytes are the ZSET key, so `json.dumps(sort_keys=True)` keeps them stable.
- **This mirrors real systems**: claim = SQS receive, visibility_timeout = SQS
  visibility timeout, ack = delete-message, DLQ = SQS dead-letter queue.

## Still deferred
- Metrics dashboard + benchmarks → **Phase 5** (the last phase)
