"""
Tests for Phase 4 — requires a running Redis (REDIS_URL or localhost).
Run: python test_phase4.py
"""
import asyncio
import random
from unittest.mock import patch
import crawler_phase4 as c


def test_dead_worker_recovery():
    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await c.reset(r); q = c.Queue(r)
        await q.push(c.make_job("http://x.com/a", 0, 0))
        job = await q.claim(visibility_timeout=0.5)
        assert job is not None
        assert (await r.llen(c.K_FRONTIER)) == 0
        assert (await r.zcard(c.K_INFLIGHT)) == 1
        await asyncio.sleep(0.6)                 # "crash": never ack
        assert (await q.recover()) == 1
        assert (await r.llen(c.K_FRONTIER)) == 1
        await c.reset(r); await r.aclose()
    asyncio.run(go())
    print("  dead_worker_recovery: PASS")


def test_ack_prevents_recovery():
    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await c.reset(r); q = c.Queue(r)
        await q.push(c.make_job("http://x.com/b", 0, 0))
        job = await q.claim(visibility_timeout=0.5)
        await q.ack(job)
        await asyncio.sleep(0.6)
        assert (await q.recover()) == 0
        assert (await r.llen(c.K_FRONTIER)) == 0
        await c.reset(r); await r.aclose()
    asyncio.run(go())
    print("  ack_prevents_recovery: PASS")


def test_concurrent_recover_idempotent():
    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await c.reset(r); q = c.Queue(r)
        for i in range(5):
            await q.push(c.make_job(f"http://x.com/{i}", 0, 0))
        for _ in range(5):
            await q.claim(0.3)
        await asyncio.sleep(0.4)
        counts = await asyncio.gather(*[q.recover() for _ in range(4)])
        assert sum(counts) == 5, counts          # each requeued exactly once
        assert (await r.llen(c.K_FRONTIER)) == 5
        await c.reset(r); await r.aclose()
    asyncio.run(go())
    print("  concurrent_recover_idempotent: PASS")


def test_retry_then_dlq():
    pages = {
        "http://site.test/":     '<a href="/good">g</a><a href="/bad">b</a>',
        "http://site.test/good": '<a href="/">home</a>',
    }

    async def fake_fetch(session, url, timeout=10.0):
        await asyncio.sleep(0.005)
        if url == "http://site.test/bad":
            return None, 503, True               # always transient
        return (pages[url], 200, False) if url in pages else (None, 404, False)

    async def fake_robots(r, s, d, scheme="https"):
        rp = c.RobotFileParser(); rp.parse(["User-agent: *", "Allow: /"]); return rp

    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await c.reset(r); q = c.Queue(r)
        await c.seed(r, q, "http://site.test/")
        bucket = c.TokenBucket(r, 1000, 1000)
        c.BASE_BACKOFF = 0.01
        with patch.object(c, "fetch", fake_fetch), \
             patch.object(c, "load_robots", fake_robots):
            tasks = [asyncio.create_task(c.worker(r, q, bucket, 100, 3, 1000, 5, 2))
                     for _ in range(2)]
            await asyncio.gather(*tasks)
        assert int(await r.hget(c.K_STATS, "crawled") or 0) == 2
        assert int(await r.hget(c.K_STATS, "retried") or 0) == c.MAX_ATTEMPTS - 1
        assert (await r.llen(c.K_DLQ)) == 1
        await c.reset(r); await r.aclose()
    asyncio.run(go())
    print("  retry_then_dlq: PASS")


def test_no_loss_under_crashes():
    N = 15
    pages = {"http://site.test/": "".join(f'<a href="/p{i}">x</a>' for i in range(N))}
    for i in range(N):
        pages[f"http://site.test/p{i}"] = '<a href="/">home</a>'
    crashes = {"n": 0}

    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await c.reset(r); q = c.Queue(r)
        await c.seed(r, q, "http://site.test/")

        async def fake_fetch(session, url, timeout=10.0):
            await asyncio.sleep(0.002)
            return (pages[url], 200, False) if url in pages else (None, 404, False)
        async def fake_robots(r2, s, d, scheme="https"):
            rp = c.RobotFileParser(); rp.parse(["User-agent: *", "Allow: /"]); return rp

        async def crashy(wid):
            root = await r.get(c.K_ROOT)
            loop = asyncio.get_event_loop()
            async with c.aiohttp.ClientSession() as session:
                idle = loop.time() + 3
                while True:
                    if int(await r.hget(c.K_STATS, "crawled") or 0) >= 200: return
                    job = await q.claim(visibility_timeout=0.5)
                    if job is None:
                        if loop.time() >= idle: return
                        await asyncio.sleep(0.05); continue
                    idle = loop.time() + 3
                    url, depth, att = c.parse_job(job)
                    if random.random() < 0.30:
                        crashes["n"] += 1; continue       # die without ack
                    html, st, _ = await fake_fetch(session, url)
                    if html is None: await q.ack(job); continue
                    await r.hincrby(c.K_STATS, "crawled", 1)
                    if depth < 3:
                        for link in c.extract_links(html, url):
                            if c.same_registered_domain(link, root) and \
                               await r.sadd(c.K_VISITED, link):
                                await q.push(c.make_job(link, depth + 1, 0))
                    await q.ack(job)

        with patch.object(c, "fetch", fake_fetch), \
             patch.object(c, "load_robots", fake_robots):
            tasks = [asyncio.create_task(crashy(i)) for i in range(4)]
            tasks.append(asyncio.create_task(c.recoverer(r, q, 0.3, 3)))
            await asyncio.gather(*tasks)

        crawled = int(await r.hget(c.K_STATS, "crawled") or 0)
        assert crawled == N + 1, f"LOST PAGES: {crawled} of {N+1}"
        await c.reset(r); await r.aclose()

    asyncio.run(go())
    print(f"  no_loss_under_crashes ({crashes['n']} crashes survived): PASS")


if __name__ == "__main__":
    print("Running Phase 4 tests (needs Redis):")
    test_dead_worker_recovery()
    test_ack_prevents_recovery()
    test_concurrent_recover_idempotent()
    test_retry_then_dlq()
    test_no_loss_under_crashes()
    print("All tests passed.")
