"""
Offline tests for Phase 2 — requires a running Redis (local or cloud).
Set REDIS_URL if not using localhost. Run: python test_phase2.py
"""
import asyncio
from unittest.mock import patch
import crawler_phase2 as c


def build_mock_site(n=10):
    pages = {"http://site.test/": "".join(f'<a href="/p{i}">p{i}</a>' for i in range(n))}
    for i in range(n):
        nbrs = [f"/p{(i+1)%n}", f"/p{(i+2)%n}", "/"]
        pages[f"http://site.test/p{i}"] = "".join(f'<a href="{x}">x</a>' for x in nbrs)
    return pages


def test_concurrent_no_duplicates():
    pages = build_mock_site(10)

    async def fake_fetch(session, url, timeout=10.0):
        await asyncio.sleep(0.01)            # force worker interleaving
        return (pages[url], 200) if url in pages else (None, 404)

    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await c.reset(r)
        await c.seed(r, "http://site.test/")
        with patch.object(c, "fetch", fake_fetch):
            tasks = [asyncio.create_task(
                        c.worker(r, max_pages=100, max_depth=5, delay=0, idle_timeout=2))
                     for _ in range(5)]
            await asyncio.gather(*tasks)
        crawled = int(await r.hget(c.K_STATS, "crawled") or 0)
        visited = await r.scard(c.K_VISITED)
        assert crawled == 11, crawled        # home + p0..p9
        assert visited == 11, visited
        await c.reset(r)
        await r.aclose()

    asyncio.run(go())
    print("  concurrent_no_duplicates (5 workers, 11 pages once): PASS")


def test_atomic_sadd_semantics():
    """The correctness primitive: SADD returns 1 only on first insert."""
    async def go():
        r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
        await r.delete("t:set")
        first = await r.sadd("t:set", "http://x.com/a")
        second = await r.sadd("t:set", "http://x.com/a")
        assert first == 1 and second == 0, (first, second)
        await r.delete("t:set")
        await r.aclose()
    asyncio.run(go())
    print("  atomic_sadd_semantics (1 then 0): PASS")


def test_encode_decode():
    assert c.decode(c.encode("http://x.com/a|b", 3)) == ("http://x.com/a|b", 3)
    print("  encode_decode (url containing '|' survives): PASS")


if __name__ == "__main__":
    print("Running Phase 2 tests (needs Redis):")
    test_atomic_sadd_semantics()
    test_encode_decode()
    test_concurrent_no_duplicates()
    print("All tests passed.")
