"""
Phase 2 — Distributed crawler with Redis-backed shared state.

What changed from Phase 1:
  - Frontier (the URL queue) now lives in a Redis LIST, not an in-memory deque.
  - Visited set now lives in Redis, not an in-memory set.
  - The "have I seen this URL?" check is now ATOMIC (SADD returns 1 only for
    the worker that actually inserted it) — this is the core correctness fix
    that lets many workers cooperate without crawling the same URL twice.
  - Workers are STATELESS. Run as many as you like against the same Redis;
    add or kill them freely. That is the horizontal-scaling story.

Run a crawl (seed + one or more workers):

    # terminal 1 — seed the frontier and then work
    python crawler_phase2.py --seed https://quotes.toscrape.com --workers 1

    # terminals 2..N — just add more workers against the same Redis
    python crawler_phase2.py --workers 1

Reset everything:
    python crawler_phase2.py --reset

Dependencies:
    pip install aiohttp selectolax redis
"""

import asyncio
import argparse
import os
import time
import socket
from urllib.parse import urljoin, urldefrag, urlsplit, urlunsplit

import aiohttp
import redis.asyncio as aioredis
from selectolax.parser import HTMLParser


# ---------------------------------------------------------------------------
# Redis key names (one namespace so --reset is easy and crawls don't collide)
# ---------------------------------------------------------------------------
NS = os.environ.get("CRAWL_NS", "crawl")
K_FRONTIER = f"{NS}:frontier"      # LIST of "depth|url" entries
K_VISITED  = f"{NS}:visited"       # SET of canonical URLs (dedup)
K_ROOT     = f"{NS}:root_netloc"   # STRING: the on-site domain
K_STATS    = f"{NS}:stats"         # HASH: crawled / failed counters


def redis_url() -> str:
    # Works with Upstash / Redis Cloud / local. Set REDIS_URL in your env.
    # Examples:
    #   redis://localhost:6379
    #   rediss://default:<password>@<host>.upstash.io:6379   (note rediss = TLS)
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


# ---------------------------------------------------------------------------
# URL canonicalization (identical logic to Phase 1 — the rules don't change)
# ---------------------------------------------------------------------------
def canonicalize(base: str, link: str) -> str | None:
    try:
        absolute = urljoin(base, link)
        absolute, _ = urldefrag(absolute)
        parts = urlsplit(absolute)
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return None
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")
    query = parts.query
    if query:
        query = "&".join(sorted(query.split("&")))
    return urlunsplit((scheme, netloc, path, query, ""))


def same_registered_domain(url: str, root_netloc: str) -> bool:
    netloc = urlsplit(url).netloc.lower()
    return netloc == root_netloc or netloc.endswith("." + root_netloc)


# ---------------------------------------------------------------------------
# Frontier encoding: store depth alongside the URL as "depth|url"
# ---------------------------------------------------------------------------
def encode(url: str, depth: int) -> str:
    return f"{depth}|{url}"

def decode(item: str) -> tuple[str, int]:
    depth_str, url = item.split("|", 1)
    return url, int(depth_str)


# ---------------------------------------------------------------------------
# Fetch + parse (same as Phase 1)
# ---------------------------------------------------------------------------
async def fetch(session: aiohttp.ClientSession, url: str, timeout: float = 10.0):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if "text/html" not in resp.headers.get("Content-Type", ""):
                return None, resp.status
            return await resp.text(), resp.status
    except (asyncio.TimeoutError, aiohttp.ClientError):
        return None, None


def extract_links(html: str, base_url: str) -> list[str]:
    tree = HTMLParser(html)
    out = []
    for node in tree.css("a[href]"):
        href = node.attributes.get("href")
        if href:
            canon = canonicalize(base_url, href)
            if canon:
                out.append(canon)
    return out


# ---------------------------------------------------------------------------
# Seeding: put the start URL into Redis and record the on-site domain
# ---------------------------------------------------------------------------
async def seed(r: aioredis.Redis, start_url: str):
    start = canonicalize(start_url, start_url)
    if start is None:
        raise ValueError(f"Invalid start URL: {start_url}")
    root = urlsplit(start).netloc.lower()

    # Atomic check-and-add: only push to the frontier if newly added to visited.
    added = await r.sadd(K_VISITED, start)
    if added:
        await r.rpush(K_FRONTIER, encode(start, 0))
    await r.set(K_ROOT, root)
    print(f"[seed] {start}  (on-site domain: {root})")


# ---------------------------------------------------------------------------
# Worker: stateless. Pull from frontier, fetch, parse, enqueue new links.
# ---------------------------------------------------------------------------
async def worker(r: aioredis.Redis, max_pages: int, max_depth: int,
                 delay: float, idle_timeout: float):
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    root = (await r.get(K_ROOT))
    if root is None:
        print("[worker] no root domain set — seed a URL first.")
        return
    headers = {"User-Agent": "Phase2Crawler/0.2 (learning project)"}

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            # Stop if the whole crawl has hit the page budget.
            crawled = int(await r.hget(K_STATS, "crawled") or 0)
            if crawled >= max_pages:
                print(f"[worker {worker_id}] page budget reached ({max_pages}).")
                return

            # BRPOP blocks until an item is available or idle_timeout elapses.
            popped = await r.brpop(K_FRONTIER, timeout=int(idle_timeout))
            if popped is None:
                print(f"[worker {worker_id}] frontier empty — exiting.")
                return
            _key, item = popped
            url, depth = decode(item)

            html, status = await fetch(session, url)
            if html is None:
                await r.hincrby(K_STATS, "failed", 1)
                print(f"[{worker_id}] FAIL {status} {url}")
                continue

            n = await r.hincrby(K_STATS, "crawled", 1)
            new_links = 0
            if depth < max_depth:
                for link in extract_links(html, url):
                    if not same_registered_domain(link, root):
                        continue
                    # ATOMIC dedup: SADD returns 1 only for the worker that
                    # actually inserted the URL. Everyone else gets 0 and skips.
                    if await r.sadd(K_VISITED, link):
                        await r.rpush(K_FRONTIER, encode(link, depth + 1))
                        new_links += 1

            print(f"[{worker_id}] OK {status} d{depth} (#{n}) {url}  (+{new_links} new)")
            await asyncio.sleep(delay)   # naive politeness; Phase 3 replaces this


async def report(r: aioredis.Redis):
    crawled = int(await r.hget(K_STATS, "crawled") or 0)
    failed  = int(await r.hget(K_STATS, "failed") or 0)
    visited = await r.scard(K_VISITED)
    frontier = await r.llen(K_FRONTIER)
    root = await r.get(K_ROOT)
    print("\n" + "=" * 60)
    print(f"  On-site domain : {root}")
    print(f"  Pages crawled  : {crawled}")
    print(f"  Pages failed   : {failed}")
    print(f"  Unique URLs    : {visited}")
    print(f"  Frontier left  : {frontier}")
    print("=" * 60)


async def reset(r: aioredis.Redis):
    await r.delete(K_FRONTIER, K_VISITED, K_ROOT, K_STATS)
    print("[reset] cleared all crawl state.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main():
    ap = argparse.ArgumentParser(description="Phase 2 distributed crawler (Redis)")
    ap.add_argument("--seed", metavar="URL", help="seed this start URL before working")
    ap.add_argument("--workers", type=int, default=1, help="async workers in THIS process")
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--max-depth", type=int, default=2)
    ap.add_argument("--delay", type=float, default=0.2)
    ap.add_argument("--idle-timeout", type=float, default=5, help="exit after N idle secs")
    ap.add_argument("--reset", action="store_true", help="clear all crawl state and exit")
    ap.add_argument("--report", action="store_true", help="print stats and exit")
    args = ap.parse_args()

    r = aioredis.from_url(redis_url(), decode_responses=True)
    try:
        if args.reset:
            await reset(r)
            return
        if args.report:
            await report(r)
            return
        if args.seed:
            await seed(r, args.seed)

        if args.workers > 0:
            t0 = time.time()
            tasks = [
                asyncio.create_task(
                    worker(r, args.max_pages, args.max_depth, args.delay, args.idle_timeout)
                )
                for _ in range(args.workers)
            ]
            await asyncio.gather(*tasks)
            elapsed = time.time() - t0
            await report(r)
            crawled = int(await r.hget(K_STATS, "crawled") or 0)
            if elapsed > 0:
                print(f"  Rate           : {crawled / elapsed:.2f} pages/sec "
                      f"({args.workers} worker(s) this process)")
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
