"""
Tests for Phase 3 — requires a running Redis (REDIS_URL or localhost).
Run: python test_phase3.py
"""
import asyncio
from unittest.mock import patch
import crawler_phase3 as c


def test_token_bucket_atomicity():
    """50 concurrent takes on a fresh burst=10 bucket -> exactly 10 allowed."""
    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        bucket = c.TokenBucket(r, rate=5, burst=10)
        await r.delete(f"{c.K_BUCKET}:atom.test")
        res = await asyncio.gather(*[bucket.take("atom.test") for _ in range(50)])
        allowed = sum(1 for a, _ in res if a)
        assert allowed == 10, allowed
        await r.delete(f"{c.K_BUCKET}:atom.test")
        await r.aclose()
    asyncio.run(go())
    print("  token_bucket_atomicity (exactly 10/50): PASS")


def test_token_bucket_refill():
    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        bucket = c.TokenBucket(r, rate=5, burst=10)
        await r.delete(f"{c.K_BUCKET}:refill.test")
        # drain
        await asyncio.gather(*[bucket.take("refill.test") for _ in range(10)])
        await asyncio.sleep(1.0)            # ~5 tokens refill at 5/sec
        res = await asyncio.gather(*[bucket.take("refill.test") for _ in range(50)])
        allowed = sum(1 for a, _ in res if a)
        assert 4 <= allowed <= 6, allowed
        await r.delete(f"{c.K_BUCKET}:refill.test")
        await r.aclose()
    asyncio.run(go())
    print("  token_bucket_refill (~5 after 1s): PASS")


def test_denied_reports_wait():
    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        bucket = c.TokenBucket(r, rate=1, burst=1)
        await r.delete(f"{c.K_BUCKET}:wait.test")
        a1, _ = await bucket.take("wait.test")     # consumes the one token
        a2, wait = await bucket.take("wait.test")  # denied
        assert a1 is True and a2 is False and wait > 0, (a1, a2, wait)
        await r.delete(f"{c.K_BUCKET}:wait.test")
        await r.aclose()
    asyncio.run(go())
    print("  denied_reports_wait (wait_ms > 0): PASS")


def test_robots_blocking():
    robots = "User-agent: *\nDisallow: /private\n"
    pages = {
        "http://site.test/":  '<a href="/ok">ok</a><a href="/private/x">no</a>',
        "http://site.test/ok": '<a href="/">home</a>',
        "http://site.test/private/x": "secret",
    }

    async def fake_fetch(session, url, timeout=10.0):
        await asyncio.sleep(0.005)
        return (pages[url], 200) if url in pages else (None, 404)

    async def fake_load_robots(r, session, domain, scheme="https"):
        rp = c.RobotFileParser(); rp.parse(robots.splitlines()); return rp

    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await c.reset(r)
        await c.seed(r, "http://site.test/")
        bucket = c.TokenBucket(r, rate=100, burst=100)
        with patch.object(c, "fetch", fake_fetch), \
             patch.object(c, "load_robots", fake_load_robots):
            tasks = [asyncio.create_task(c.worker(r, bucket, 100, 3, 100, 2))
                     for _ in range(3)]
            await asyncio.gather(*tasks)
        crawled = int(await r.hget(c.K_STATS, "crawled") or 0)
        blocked = int(await r.hget(c.K_STATS, "robots_blocked") or 0)
        assert crawled == 2, crawled
        assert blocked >= 1, blocked
        await c.reset(r)
        await r.aclose()
    asyncio.run(go())
    print("  robots_blocking (/private disallowed): PASS")


if __name__ == "__main__":
    print("Running Phase 3 tests (needs Redis):")
    test_token_bucket_atomicity()
    test_token_bucket_refill()
    test_denied_reports_wait()
    test_robots_blocking()
    print("All tests passed.")
