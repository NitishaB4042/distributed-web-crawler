# Distributed Web Crawler — Phase 2

Frontier and visited-set now live in **Redis**, so multiple stateless workers
cooperate without crawling the same URL twice. This is the first genuinely
*distributed* milestone.

## Files
- `crawler_phase2.py` — the distributed crawler
- `test_phase2.py` — tests (need a running Redis)

## What changed from Phase 1
| Phase 1 | Phase 2 |
|---|---|
| in-memory `deque` frontier | Redis `LIST` (`RPUSH` / `BRPOP`) |
| in-memory `set` visited | Redis `SET` |
| dedup via `if x in set` | **atomic** `SADD` (returns 1 only on first insert) |
| one process | many stateless workers, any number of processes |

The atomic `SADD` is the core correctness fix: when two workers discover the
same new URL at the same instant, only the one whose `SADD` returns `1`
enqueues it. Everyone else gets `0` and skips. No duplicates, no locks needed.

## Setup

You need a Redis instance. On a tablet, use a free cloud tier (no install):
- **Upstash** (`https://upstash.com`) — free tier, gives a `rediss://` URL
- **Redis Cloud** (`https://redis.com/try-free`) — free 30 MB tier

Then:
```bash
pip install aiohttp selectolax redis
export REDIS_URL="rediss://default:<password>@<host>.upstash.io:6379"
```
(Note: Upstash uses `rediss://` — double-s — for TLS. Local Redis is
`redis://localhost:6379`.)

## Run a crawl

```bash
# Seed the start URL and run one worker
python crawler_phase2.py --seed https://quotes.toscrape.com --workers 1 --max-pages 50

# In another terminal/Colab cell: add more workers against the SAME Redis
python crawler_phase2.py --workers 1
```

On a tablet without multiple terminals, run several workers in one process:
```bash
python crawler_phase2.py --seed https://quotes.toscrape.com --workers 4 --max-pages 50
```

Useful commands:
```bash
python crawler_phase2.py --report   # print current stats
python crawler_phase2.py --reset    # clear all crawl state
```

## Test
```bash
python test_phase2.py
```
Tests use a mocked site (no external network) but **do need a reachable
Redis** (`REDIS_URL` or localhost). The headline test runs 5 concurrent
workers over a heavily cross-linked 11-page site and asserts each page is
crawled exactly once.

## Redis keys used
- `crawl:frontier` — LIST of `"depth|url"` entries (the queue)
- `crawl:visited` — SET of canonical URLs (dedup)
- `crawl:root_netloc` — STRING, the on-site domain
- `crawl:stats` — HASH with `crawled` / `failed` counters

Change the `CRAWL_NS` env var to run isolated crawls side by side.


## Still deferred
- Bloom filter for memory-efficient dedup at scale → optional Phase 2.5
- Per-domain rate limiting + robots.txt → **Phase 3**
- Crash recovery / claim pattern / retries → **Phase 4**
- Metrics dashboard + benchmarks → **Phase 5**

> Note: the current visited-set is a plain Redis SET, which is fine up to
> millions of URLs. The bloom-filter upgrade (and its false-positive tradeoff)
> is described in the study guide and becomes worthwhile only at very large
> scale.
