"""
Phase 3 — Politeness layer: per-domain rate limiting + robots.txt.

What changed from Phase 2:
  - Before fetching, a worker must take a TOKEN from that domain's bucket.
    The bucket refills at a fixed rate (e.g. 2 tokens/sec). If empty, the URL
    is requeued and the worker moves on — no single host gets hammered.
  - The take-a-token operation is an ATOMIC Lua script. This matters: without
    atomicity, two workers can both read "1 token left" and both proceed,
    blowing past the rate limit (a check-then-act race).
  - robots.txt is fetched once per domain, cached in Redis, and every URL is
    checked with can_fetch() before it is queued or fetched. Crawl-delay is
    honored by lowering that domain's refill rate.

This is also a standalone rate-limiter component — the TokenBucket class +
Lua script can be lifted straight into the separate rate-limiter project.

Run:
    python crawler_phase3.py --seed https://quotes.toscrape.com --workers 4 \
        --rate 2 --burst 4 --max-pages 50

Dependencies:
    pip install aiohttp selectolax redis
"""

import asyncio
import argparse
import os
import time
import socket
from urllib.parse import urljoin, urldefrag, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import aiohttp
import redis.asyncio as aioredis
from selectolax.parser import HTMLParser


NS = os.environ.get("CRAWL_NS", "crawl")
K_FRONTIER = f"{NS}:frontier"
K_VISITED  = f"{NS}:visited"
K_ROOT     = f"{NS}:root_netloc"
K_STATS    = f"{NS}:stats"
K_BUCKET   = f"{NS}:bucket"        # per-domain: "{NS}:bucket:{domain}" HASH(tokens, ts)
K_ROBOTS   = f"{NS}:robots"        # per-domain cache: "{NS}:robots:{domain}"
K_ROBODELAY= f"{NS}:crawldelay"    # per-domain crawl-delay (seconds), HASH


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


# ---------------------------------------------------------------------------
# Canonicalization (unchanged from Phase 1/2)
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


def domain_of(url: str) -> str:
    return urlsplit(url).netloc.lower()


def same_registered_domain(url: str, root_netloc: str) -> bool:
    netloc = domain_of(url)
    return netloc == root_netloc or netloc.endswith("." + root_netloc)


def encode(url: str, depth: int) -> str:
    return f"{depth}|{url}"

def decode(item: str) -> tuple[str, int]:
    depth_str, url = item.split("|", 1)
    return url, int(depth_str)


# ---------------------------------------------------------------------------
# TOKEN BUCKET — atomic refill-and-take as a Redis Lua script.
#
# State per domain: a HASH with fields {tokens, ts}.
#   tokens = fractional tokens currently available
#   ts     = last refill timestamp (ms)
#
# On each call we:
#   1. compute elapsed time since ts
#   2. add elapsed * rate tokens, capped at burst
#   3. if >= 1 token: subtract 1, return 1 (ALLOWED)
#      else:          return 0 (DENIED) + how long until a token is ready
#
# Because Redis runs the whole script atomically (single-threaded execution),
# no two workers can both see the same last token. This is the crux.
# ---------------------------------------------------------------------------
TOKEN_BUCKET_LUA = """
local key    = KEYS[1]
local rate   = tonumber(ARGV[1])   -- tokens per second
local burst  = tonumber(ARGV[2])   -- max tokens (bucket capacity)
local now_ms = tonumber(ARGV[3])   -- current time in ms

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
  tokens = burst                   -- new bucket starts full
  ts = now_ms
end

-- refill based on elapsed time
local elapsed = math.max(0, now_ms - ts) / 1000.0
tokens = math.min(burst, tokens + elapsed * rate)
ts = now_ms

local allowed = 0
local wait_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  -- time until one token accrues
  wait_ms = math.ceil((1 - tokens) / rate * 1000)
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', ts)
-- expire idle buckets so we don't leak keys for one-off domains
redis.call('PEXPIRE', key, 60000)

return {allowed, wait_ms}
"""


class TokenBucket:
    """Per-domain distributed token bucket backed by an atomic Lua script."""
    def __init__(self, r: aioredis.Redis, rate: float, burst: float):
        self.r = r
        self.rate = rate
        self.burst = burst
        self._sha = None

    async def _ensure_loaded(self):
        if self._sha is None:
            self._sha = await self.r.script_load(TOKEN_BUCKET_LUA)

    async def take(self, domain: str, rate: float | None = None) -> tuple[bool, int]:
        """Try to take one token for `domain`. Returns (allowed, wait_ms)."""
        await self._ensure_loaded()
        key = f"{K_BUCKET}:{domain}"
        now_ms = int(time.time() * 1000)
        r = rate if rate is not None else self.rate
        try:
            allowed, wait_ms = await self.r.evalsha(
                self._sha, 1, key, r, self.burst, now_ms)
        except aioredis.ResponseError:
            # script cache was flushed (e.g. failover) — reload and retry once
            self._sha = None
            await self._ensure_loaded()
            allowed, wait_ms = await self.r.evalsha(
                self._sha, 1, key, r, self.burst, now_ms)
        return bool(allowed), int(wait_ms)


# ---------------------------------------------------------------------------
# robots.txt — fetch once per domain, cache in Redis, honor Crawl-delay.
# ---------------------------------------------------------------------------
UA = "Phase3Crawler/0.3 (learning project)"


async def load_robots(r: aioredis.Redis, session: aiohttp.ClientSession,
                      domain: str, scheme: str = "https") -> RobotFileParser:
    """Return a RobotFileParser for `domain`, using a Redis-cached body."""
    cache_key = f"{K_ROBOTS}:{domain}"
    cached = await r.get(cache_key)
    rp = RobotFileParser()

    if cached is not None:
        rp.parse(cached.splitlines())
    else:
        body = ""
        url = f"{scheme}://{domain}/robots.txt"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    body = await resp.text()
        except (asyncio.TimeoutError, aiohttp.ClientError):
            body = ""                      # no robots.txt -> allow all
        # cache for 1 hour (even an empty body, to avoid refetching)
        await r.set(cache_key, body, ex=3600)
        rp.parse(body.splitlines())

        # record Crawl-delay if present, so the bucket can slow this domain
        delay = rp.crawl_delay(UA)
        if delay:
            await r.hset(K_ROBODELAY, domain, float(delay))
    return rp


async def crawl_delay_rate(r: aioredis.Redis, domain: str, default_rate: float) -> float:
    """If robots set a Crawl-delay, convert it to a (lower) refill rate."""
    d = await r.hget(K_ROBODELAY, domain)
    if d:
        delay = float(d)
        if delay > 0:
            return min(default_rate, 1.0 / delay)
    return default_rate


# ---------------------------------------------------------------------------
# Fetch + parse (unchanged)
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
# Seed (unchanged from Phase 2)
# ---------------------------------------------------------------------------
async def seed(r: aioredis.Redis, start_url: str):
    start = canonicalize(start_url, start_url)
    if start is None:
        raise ValueError(f"Invalid start URL: {start_url}")
    root = domain_of(start)
    if await r.sadd(K_VISITED, start):
        await r.rpush(K_FRONTIER, encode(start, 0))
    await r.set(K_ROOT, root)
    print(f"[seed] {start}  (on-site domain: {root})")


# ---------------------------------------------------------------------------
# Worker — now with politeness checks before each fetch.
# ---------------------------------------------------------------------------
async def worker(r: aioredis.Redis, bucket: TokenBucket, max_pages: int,
                 max_depth: int, default_rate: float, idle_timeout: float):
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    root = await r.get(K_ROOT)
    if root is None:
        print("[worker] no root domain set — seed a URL first.")
        return
    headers = {"User-Agent": UA}

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            crawled = int(await r.hget(K_STATS, "crawled") or 0)
            if crawled >= max_pages:
                print(f"[worker {worker_id}] page budget reached ({max_pages}).")
                return

            popped = await r.brpop(K_FRONTIER, timeout=int(idle_timeout))
            if popped is None:
                print(f"[worker {worker_id}] frontier empty — exiting.")
                return
            _key, item = popped
            url, depth = decode(item)
            domain = domain_of(url)
            scheme = urlsplit(url).scheme

            # --- robots.txt check ---
            rp = await load_robots(r, session, domain, scheme)
            if not rp.can_fetch(UA, url):
                await r.hincrby(K_STATS, "robots_blocked", 1)
                print(f"[{worker_id}] ROBOTS-BLOCKED {url}")
                continue

            # --- rate limit: take a token for this domain ---
            rate = await crawl_delay_rate(r, domain, default_rate)
            allowed, wait_ms = await bucket.take(domain, rate=rate)
            if not allowed:
                # requeue at the FRONT so it's retried soon, and back off briefly
                await r.rpush(K_FRONTIER, encode(url, depth))
                await r.hincrby(K_STATS, "throttled", 1)
                await asyncio.sleep(min(wait_ms, 1000) / 1000.0)
                continue

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
                    if await r.sadd(K_VISITED, link):
                        await r.rpush(K_FRONTIER, encode(link, depth + 1))
                        new_links += 1

            print(f"[{worker_id}] OK {status} d{depth} (#{n}) {url}  (+{new_links} new)")


async def report(r: aioredis.Redis):
    crawled = int(await r.hget(K_STATS, "crawled") or 0)
    failed  = int(await r.hget(K_STATS, "failed") or 0)
    throttled = int(await r.hget(K_STATS, "throttled") or 0)
    blocked = int(await r.hget(K_STATS, "robots_blocked") or 0)
    visited = await r.scard(K_VISITED)
    frontier = await r.llen(K_FRONTIER)
    root = await r.get(K_ROOT)
    print("\n" + "=" * 60)
    print(f"  On-site domain  : {root}")
    print(f"  Pages crawled   : {crawled}")
    print(f"  Pages failed    : {failed}")
    print(f"  Throttle events : {throttled}")
    print(f"  Robots-blocked  : {blocked}")
    print(f"  Unique URLs     : {visited}")
    print(f"  Frontier left   : {frontier}")
    print("=" * 60)


async def reset(r: aioredis.Redis):
    # delete all namespaced keys, including per-domain bucket/robots keys
    keys = [K_FRONTIER, K_VISITED, K_ROOT, K_STATS, K_ROBODELAY]
    async for k in r.scan_iter(match=f"{K_BUCKET}:*"):
        keys.append(k)
    async for k in r.scan_iter(match=f"{K_ROBOTS}:*"):
        keys.append(k)
    if keys:
        await r.delete(*keys)
    print("[reset] cleared all crawl state.")


async def main():
    ap = argparse.ArgumentParser(description="Phase 3 crawler (politeness layer)")
    ap.add_argument("--seed", metavar="URL")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--max-depth", type=int, default=2)
    ap.add_argument("--rate", type=float, default=2.0, help="tokens/sec per domain")
    ap.add_argument("--burst", type=float, default=4.0, help="bucket capacity per domain")
    ap.add_argument("--idle-timeout", type=float, default=5)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    r = aioredis.from_url(redis_url(), decode_responses=True)
    try:
        if args.reset:
            await reset(r); return
        if args.report:
            await report(r); return
        if args.seed:
            await seed(r, args.seed)

        if args.workers > 0:
            bucket = TokenBucket(r, rate=args.rate, burst=args.burst)
            t0 = time.time()
            tasks = [asyncio.create_task(
                        worker(r, bucket, args.max_pages, args.max_depth,
                               args.rate, args.idle_timeout))
                     for _ in range(args.workers)]
            await asyncio.gather(*tasks)
            elapsed = time.time() - t0
            await report(r)
            crawled = int(await r.hget(K_STATS, "crawled") or 0)
            if elapsed > 0:
                print(f"  Rate            : {crawled / elapsed:.2f} pages/sec")
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
