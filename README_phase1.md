# Distributed Web Crawler — Phase 1

Single-process async crawler. This phase proves the **core loop** works before
any distribution: fetching, link extraction, and (most importantly) URL
canonicalization. No Redis, no multiple workers yet.

## Files
- `crawler_phase1.py` — the crawler
- `test_phase1.py` — offline tests (no network needed)

## Setup
```bash
pip install aiohttp selectolax
```

## Run
```bash
python crawler_phase1.py https://example.com --max-pages 30 --max-depth 2
```

Options: `--max-pages` (stop after N pages), `--max-depth` (link depth from
start), `--delay` (seconds between requests — naive politeness, replaced in
Phase 3).

## Test
```bash
python test_phase1.py
```
Uses a mocked 3-page site, so it runs anywhere with no network.

## What this phase demonstrates
- **URL canonicalization** — `/a`, `/a/`, `/a#frag`, `HOST` casing, and
  query-param ordering all collapse to one key. This is the #1 source of
  crawler bugs (infinite loops, double-counting); getting it right here is the
  point of Phase 1.
- **Async fetching** with `aiohttp` (I/O-bound, so the GIL is a non-issue).
- **Link extraction** with `selectolax` (faster/lighter than BeautifulSoup).
- **On-site scoping** — stays on the start domain and its subdomains.
- **Dedup** against the canonical URL key.
- **Depth + page limits** and a stats report.

## What's deliberately NOT here yet
- Shared state / multiple workers → **Phase 2** (move frontier + visited to Redis)
- Bloom filter for dedup at scale → **Phase 2**
- Per-domain rate limiting + robots.txt → **Phase 3**
- Crash recovery, retries, dead-letter → **Phase 4**
- Metrics dashboard + benchmarks → **Phase 5**

## Note on testing locally
Some networks/proxies block crawl-practice sites with a 403. If you see that,
the code is fine — try it on Google Colab (no domain allowlist) against
`https://books.toscrape.com` or `https://quotes.toscrape.com`, which are built
for crawling practice.
