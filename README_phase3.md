# Distributed Web Crawler — Phase 3

Adds the **politeness layer**: a per-domain token-bucket rate limiter (atomic
Lua script) and robots.txt enforcement. A naive crawler hammers one host and
gets IP-banned; this one is well-behaved.

## Files
- `crawler_phase3.py` — crawler with rate limiting + robots.txt
- `test_phase3.py` — tests (need a running Redis)

## What changed from Phase 2
- **Token bucket per domain.** Before fetching, a worker must `take()` a token
  for that URL's domain. The bucket refills at `--rate` tokens/sec, capped at
  `--burst`. If empty, the URL is requeued and the worker moves on.
- **Atomic Lua script.** The refill-and-take happens inside one Redis Lua
  script, so it executes atomically. Without this, two workers could both read
  "1 token left" and both proceed (a check-then-act race) — blowing the limit.
- **robots.txt.** Fetched once per domain, cached in Redis (1h TTL), and every
  URL is checked with `can_fetch()` before queueing/fetching. A `Crawl-delay`
  directive lowers that domain's refill rate.

## The token bucket (interview centerpiece)
`TokenBucket.take(domain)` runs `TOKEN_BUCKET_LUA`, which:
1. reads the domain's `{tokens, ts}` (a fresh bucket starts full),
2. refills `tokens += elapsed_seconds * rate`, capped at `burst`,
3. if `tokens >= 1`: decrement and return *allowed*; else return *denied* plus
   `wait_ms` until the next token.

Buckets `PEXPIRE` after 60s idle so one-off domains don't leak keys.

This `TokenBucket` class + Lua script is reusable as the standalone
rate-limiter portfolio project — same code, different front door.

## Setup & run
```bash
pip install aiohttp selectolax redis
export REDIS_URL="rediss://default:<password>@<host>.upstash.io:6379"  # or local

python crawler_phase3.py --seed https://quotes.toscrape.com \
    --workers 4 --rate 2 --burst 4 --max-pages 50
```
`--rate 2 --burst 4` = at most ~2 requests/sec/domain, bursting to 4.

Other commands:
```bash
python crawler_phase3.py --report   # stats incl. throttle + robots-blocked counts
python crawler_phase3.py --reset    # clear all state (incl. per-domain buckets)
```

## Test
```bash
python test_phase3.py
```
- `token_bucket_atomicity` — 50 concurrent takes on a burst-10 bucket grant
  **exactly 10** (proves no race).
- `token_bucket_refill` — ~5 tokens accrue after 1s at 5/sec.
- `denied_reports_wait` — a denied take returns `wait_ms > 0`.
- `robots_blocking` — a `Disallow: /private` rule blocks matching URLs.

## New Redis keys
- `crawl:bucket:{domain}` — HASH `{tokens, ts}`, the per-domain bucket
- `crawl:robots:{domain}` — cached robots.txt body (1h TTL)
- `crawl:crawldelay` — HASH of domain -> Crawl-delay seconds
- `crawl:stats` now also tracks `throttled` and `robots_blocked`

## Interview notes
- **Why Lua, not Python-side check?** Atomicity. Redis runs a script start to
  finish without interleaving other commands, so the read-modify-write is
  race-free across all workers. A Python `get` then `set` is not.
- **Why per-domain, not global?** Politeness is a per-host concern — you can
  crawl 100 domains in parallel but must be gentle with each one.
- **Why requeue on throttle instead of blocking?** The worker stays useful: it
  drops the throttled URL back and could pick up work for another domain rather
  than sleeping on one slow host. (Current version backs off briefly; a
  priority/delayed queue is the next refinement.)

## Still deferred
- Crash recovery / claim pattern / retries / dead-letter → **Phase 4**
- Metrics dashboard + benchmarks → **Phase 5**
