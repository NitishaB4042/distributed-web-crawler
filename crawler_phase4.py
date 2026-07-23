"""
Phase 4 — Fault tolerance: claim pattern, dead-worker recovery, retries, DLQ.

The problem Phase 4 solves:
  In Phase 2/3 a worker did BRPOP (pop-and-forget). If it crashed AFTER popping
  a URL but BEFORE finishing, that URL was lost forever. At scale, workers DO
  crash, so silent loss is unacceptable.

The fix — the CLAIM pattern (at-least-once processing):
  1. CLAIM: atomically move a URL off the frontier into an "in-flight" set,
     stamped with a deadline (now + visibility_timeout). The URL is not lost;
     it is reserved.
  2. PROCESS: fetch + parse.
  3. ACK: on success, remove the URL from in-flight. Done.
  4. RECOVER: a coordinator periodically scans in-flight for entries whose
     deadline has passed (the worker presumably died) and requeues them.

Plus:
  - Retries with exponential backoff for transient failures (timeouts, 5xx).
  - A dead-letter queue (DLQ) for URLs that fail past max attempts.

Trade-off: this gives AT-LEAST-ONCE, not exactly-once. A worker can finish then
die before ACK, so a URL may be processed twice. That is fine because crawling
is IDEMPOTENT — re-fetching a page just overwrites the same result.

Run a crawl with a background recoverer:
    python crawler_phase4.py --seed https://quotes.toscrape.com \
        --workers 4 --recover --visibility-timeout 30 --max-pages 50

Dependencies: pip install aiohttp selectolax redis
"""

import asyncio
import argparse
import os
import time
import socket
import json
from urllib.parse import urljoin, urldefrag, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import aiohttp
import redis.asyncio as aioredis
from selectolax.parser import HTMLParser


NS = os.environ.get("CRAWL_NS", "crawl")
K_FRONTIER = f"{NS}:frontier"      # LIST of job JSON (the ready queue)
K_INFLIGHT = f"{NS}:inflight"      # ZSET member=job JSON, score=deadline (ms)
K_VISITED  = f"{NS}:visited"
K_ROOT     = f"{NS}:root_netloc"
K_STATS    = f"{NS}:stats"
K_DLQ      = f"{NS}:dlq"           # LIST of dead-lettered job JSON
K_BUCKET   = f"{NS}:bucket"
K_ROBOTS   = f"{NS}:robots"
K_ROBODELAY= f"{NS}:crawldelay"

UA = "Phase4Crawler/0.4 (learning project)"


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


# ---------------------------------------------------------------------------
# Canonicalization (unchanged)
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


# ---------------------------------------------------------------------------
# Job encoding: a job carries url, depth, and attempt count, as JSON.
# (Phase 2/3 used "depth|url"; we upgrade to JSON to carry the attempt count
#  needed for retries. The exact bytes are the ZSET member, so they must be
#  stable — json.dumps with sort_keys gives deterministic output.)
# ---------------------------------------------------------------------------
def make_job(url: str, depth: int, attempts: int = 0) -> str:
    return json.dumps({"u": url, "d": depth, "a": attempts}, sort_keys=True)

def parse_job(blob: str) -> tuple[str, int, int]:
    j = json.loads(blob)
    return j["u"], j["d"], j["a"]


# ---------------------------------------------------------------------------
# CLAIM — atomic move frontier -> inflight with a deadline.
#   LMOVE-like pop, then ZADD into inflight with score = now + timeout.
#   Done in one Lua script so a crash between the two can't drop the job.
# ---------------------------------------------------------------------------
CLAIM_LUA = """
local frontier = KEYS[1]
local inflight = KEYS[2]
local deadline = tonumber(ARGV[1])
local job = redis.call('RPOP', frontier)
if job == false then
  return false
end
redis.call('ZADD', inflight, deadline, job)
return job
"""

# ACK — remove a specific job from inflight (success path).
ACK_LUA = """
return redis.call('ZREM', KEYS[1], ARGV[1])
"""

# RECOVER — move all jobs in inflight whose deadline <= now back to frontier.
# Returns the count recovered. Atomic so two coordinators can't double-requeue.
RECOVER_LUA = """
local inflight = KEYS[1]
local frontier = KEYS[2]
local now = tonumber(ARGV[1])
local expired = redis.call('ZRANGEBYSCORE', inflight, '-inf', now)
local n = 0
for _, job in ipairs(expired) do
  redis.call('ZREM', inflight, job)
  redis.call('LPUSH', frontier, job)
  n = n + 1
end
return n
"""


class Queue:
    """Reliable queue with claim / ack / recover semantics."""
    def __init__(self, r: aioredis.Redis):
        self.r = r
        self._claim = self._ack = self._recover = None

    async def _load(self):
        if self._claim is None:
            self._claim = await self.r.script_load(CLAIM_LUA)
            self._ack = await self.r.script_load(ACK_LUA)
            self._recover = await self.r.script_load(RECOVER_LUA)

    async def push(self, job: str):
        await self.r.lpush(K_FRONTIER, job)

    async def claim(self, visibility_timeout: float) -> str | None:
        await self._load()
        deadline = int((time.time() + visibility_timeout) * 1000)
        job = await self.r.evalsha(self._claim, 2, K_FRONTIER, K_INFLIGHT, deadline)
        return job

    async def ack(self, job: str):
        await self._load()
        await self.r.evalsha(self._ack, 1, K_INFLIGHT, job)

    async def recover(self) -> int:
        await self._load()
        now = int(time.time() * 1000)
        return int(await self.r.evalsha(self._recover, 2, K_INFLIGHT, K_FRONTIER, now))


# ---------------------------------------------------------------------------
# Token bucket (from Phase 3, condensed) + robots
# ---------------------------------------------------------------------------
TOKEN_BUCKET_LUA = """
local key=KEYS[1]; local rate=tonumber(ARGV[1]); local burst=tonumber(ARGV[2]); local now_ms=tonumber(ARGV[3])
local d=redis.call('HMGET',key,'tokens','ts'); local tokens=tonumber(d[1]); local ts=tonumber(d[2])
if tokens==nil then tokens=burst; ts=now_ms end
local elapsed=math.max(0,now_ms-ts)/1000.0
tokens=math.min(burst,tokens+elapsed*rate); ts=now_ms
local allowed=0; local wait_ms=0
if tokens>=1 then tokens=tokens-1; allowed=1 else wait_ms=math.ceil((1-tokens)/rate*1000) end
redis.call('HMSET',key,'tokens',tokens,'ts',ts); redis.call('PEXPIRE',key,60000)
return {allowed,wait_ms}
"""

class TokenBucket:
    def __init__(self, r, rate, burst):
        self.r = r; self.rate = rate; self.burst = burst; self._sha = None
    async def take(self, domain, rate=None):
        if self._sha is None:
            self._sha = await self.r.script_load(TOKEN_BUCKET_LUA)
        key = f"{K_BUCKET}:{domain}"; now_ms = int(time.time()*1000)
        rt = rate if rate is not None else self.rate
        allowed, wait_ms = await self.r.evalsha(self._sha, 1, key, rt, self.burst, now_ms)
        return bool(allowed), int(wait_ms)


async def load_robots(r, session, domain, scheme="https"):
    cache_key = f"{K_ROBOTS}:{domain}"
    cached = await r.get(cache_key)
    rp = RobotFileParser()
    if cached is not None:
        rp.parse(cached.splitlines())
    else:
        body = ""
        try:
            async with session.get(f"{scheme}://{domain}/robots.txt",
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    body = await resp.text()
        except (asyncio.TimeoutError, aiohttp.ClientError):
            body = ""
        await r.set(cache_key, body, ex=3600)
        rp.parse(body.splitlines())
        delay = rp.crawl_delay(UA)
        if delay:
            await r.hset(K_ROBODELAY, domain, float(delay))
    return rp


async def crawl_delay_rate(r, domain, default_rate):
    d = await r.hget(K_ROBODELAY, domain)
    if d and float(d) > 0:
        return min(default_rate, 1.0 / float(d))
    return default_rate


# ---------------------------------------------------------------------------
# Fetch + parse. fetch now returns a third value: retryable (bool).
# ---------------------------------------------------------------------------
async def fetch(session, url, timeout=10.0):
    """Returns (html_or_None, status_or_None, retryable)."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status >= 500:
                return None, resp.status, True            # server error -> retry
            if resp.status == 429:
                return None, resp.status, True            # too many requests -> retry
            if resp.status >= 400:
                return None, resp.status, False           # 4xx -> permanent
            if "text/html" not in resp.headers.get("Content-Type", ""):
                return None, resp.status, False           # not HTML -> don't retry
            return await resp.text(), resp.status, False
    except (asyncio.TimeoutError, aiohttp.ClientError):
        return None, None, True                           # network blip -> retry


def extract_links(html, base_url):
    tree = HTMLParser(html); out = []
    for node in tree.css("a[href]"):
        href = node.attributes.get("href")
        if href:
            c = canonicalize(base_url, href)
            if c:
                out.append(c)
    return out


# ---------------------------------------------------------------------------
# Seed
# ---------------------------------------------------------------------------
async def seed(r, q: Queue, start_url):
    start = canonicalize(start_url, start_url)
    if start is None:
        raise ValueError(f"Invalid start URL: {start_url}")
    root = domain_of(start)
    if await r.sadd(K_VISITED, start):
        await q.push(make_job(start, 0, 0))
    await r.set(K_ROOT, root)
    print(f"[seed] {start}  (on-site domain: {root})")


# ---------------------------------------------------------------------------
# Worker — claim, process, ack; retry on transient failure, DLQ on exhaustion.
# ---------------------------------------------------------------------------
MAX_ATTEMPTS = 4
BASE_BACKOFF = 1.0     # seconds: 1, 2, 4, 8 ...


async def worker(r, q: Queue, bucket: TokenBucket, max_pages, max_depth,
                 default_rate, visibility_timeout, idle_timeout):
    worker_id = f"{socket.gethostname()}-{os.getpid()}-{id(asyncio.current_task())%1000}"
    root = await r.get(K_ROOT)
    if root is None:
        print("[worker] no root domain set — seed first."); return
    headers = {"User-Agent": UA}
    idle_deadline = time.time() + idle_timeout

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            if int(await r.hget(K_STATS, "crawled") or 0) >= max_pages:
                print(f"[{worker_id}] page budget reached."); return

            job = await q.claim(visibility_timeout)
            if job is None:
                # nothing ready right now; exit if idle too long, else wait
                if time.time() >= idle_deadline:
                    print(f"[{worker_id}] idle — exiting."); return
                await asyncio.sleep(0.2)
                continue
            idle_deadline = time.time() + idle_timeout

            url, depth, attempts = parse_job(job)
            domain = domain_of(url); scheme = urlsplit(url).scheme

            # robots
            rp = await load_robots(r, session, domain, scheme)
            if not rp.can_fetch(UA, url):
                await r.hincrby(K_STATS, "robots_blocked", 1)
                await q.ack(job)                    # blocked = done, not retried
                continue

            # rate limit
            rate = await crawl_delay_rate(r, domain, default_rate)
            allowed, wait_ms = await bucket.take(domain, rate=rate)
            if not allowed:
                # not this URL's fault — return it to the queue and ack the claim
                await q.ack(job)
                await q.push(job)
                await r.hincrby(K_STATS, "throttled", 1)
                await asyncio.sleep(min(wait_ms, 1000) / 1000.0)
                continue

            html, status, retryable = await fetch(session, url)

            if html is None:
                if retryable and attempts + 1 < MAX_ATTEMPTS:
                    backoff = BASE_BACKOFF * (2 ** attempts)
                    await q.ack(job)                                  # release claim
                    await asyncio.sleep(min(backoff, 8))
                    await q.push(make_job(url, depth, attempts + 1))  # requeue w/ +1
                    await r.hincrby(K_STATS, "retried", 1)
                    print(f"[{worker_id}] RETRY {status} (attempt {attempts+1}) {url}")
                else:
                    await q.ack(job)
                    await r.rpush(K_DLQ, make_job(url, depth, attempts + 1))
                    await r.hincrby(K_STATS, "dead_lettered", 1)
                    print(f"[{worker_id}] DLQ {status} {url}")
                continue

            # success
            n = await r.hincrby(K_STATS, "crawled", 1)
            new_links = 0
            if depth < max_depth:
                for link in extract_links(html, url):
                    if not same_registered_domain(link, root):
                        continue
                    if await r.sadd(K_VISITED, link):
                        await q.push(make_job(link, depth + 1, 0))
                        new_links += 1
            await q.ack(job)                          # ACK only after success
            print(f"[{worker_id}] OK {status} d{depth} (#{n}) {url}  (+{new_links} new)")


# ---------------------------------------------------------------------------
# Coordinator — background loop that requeues expired (dead-worker) claims.
# ---------------------------------------------------------------------------
async def recoverer(r, q: Queue, interval: float, stop_after_idle: float):
    idle_deadline = time.time() + stop_after_idle
    while True:
        n = await q.recover()
        if n:
            await r.hincrby(K_STATS, "recovered", n)
            print(f"[recoverer] requeued {n} expired claim(s)")
            idle_deadline = time.time() + stop_after_idle
        # stop when frontier + inflight are both empty for a while
        if (await r.llen(K_FRONTIER)) == 0 and (await r.zcard(K_INFLIGHT)) == 0:
            if time.time() >= idle_deadline:
                print("[recoverer] queues drained — exiting."); return
        await asyncio.sleep(interval)


async def report(r):
    async def gi(f): return int(await r.hget(K_STATS, f) or 0)
    print("\n" + "=" * 60)
    print(f"  On-site domain  : {await r.get(K_ROOT)}")
    print(f"  Pages crawled   : {await gi('crawled')}")
    print(f"  Retried         : {await gi('retried')}")
    print(f"  Recovered       : {await gi('recovered')}  (dead-worker requeues)")
    print(f"  Dead-lettered   : {await gi('dead_lettered')}")
    print(f"  Throttled       : {await gi('throttled')}")
    print(f"  Robots-blocked  : {await gi('robots_blocked')}")
    print(f"  Unique URLs     : {await r.scard(K_VISITED)}")
    print(f"  Frontier left   : {await r.llen(K_FRONTIER)}")
    print(f"  In-flight       : {await r.zcard(K_INFLIGHT)}")
    print(f"  DLQ size        : {await r.llen(K_DLQ)}")
    print("=" * 60)


async def reset(r):
    keys = [K_FRONTIER, K_INFLIGHT, K_VISITED, K_ROOT, K_STATS, K_DLQ, K_ROBODELAY]
    async for k in r.scan_iter(match=f"{K_BUCKET}:*"): keys.append(k)
    async for k in r.scan_iter(match=f"{K_ROBOTS}:*"): keys.append(k)
    if keys: await r.delete(*keys)
    print("[reset] cleared all crawl state.")


async def main():
    ap = argparse.ArgumentParser(description="Phase 4 crawler (fault tolerance)")
    ap.add_argument("--seed", metavar="URL")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--max-depth", type=int, default=2)
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--burst", type=float, default=4.0)
    ap.add_argument("--visibility-timeout", type=float, default=30,
                    help="seconds a claim is held before it's considered dead")
    ap.add_argument("--recover", action="store_true",
                    help="run the background dead-worker recoverer in this process")
    ap.add_argument("--recover-interval", type=float, default=5)
    ap.add_argument("--idle-timeout", type=float, default=5)
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    r = aioredis.from_url(redis_url(), decode_responses=True)
    q = Queue(r)
    try:
        if args.reset: await reset(r); return
        if args.report: await report(r); return
        if args.seed: await seed(r, q, args.seed)

        if args.workers > 0:
            bucket = TokenBucket(r, args.rate, args.burst)
            t0 = time.time()
            tasks = [asyncio.create_task(
                        worker(r, q, bucket, args.max_pages, args.max_depth,
                               args.rate, args.visibility_timeout, args.idle_timeout))
                     for _ in range(args.workers)]
            if args.recover:
                tasks.append(asyncio.create_task(
                    recoverer(r, q, args.recover_interval, args.idle_timeout * 2)))
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
