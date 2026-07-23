"""
Offline tests for Phase 1 — no network required.
Run: python test_phase1.py
"""
import asyncio
from unittest.mock import patch
import crawler_phase1 as c


def test_canonicalize():
    base = "http://X.COM/blog/post"
    eq = lambda a, b: canon(base, a) == canon(base, b)
    canon = c.canonicalize
    # trailing slash, host case, fragment all collapse
    assert canon(base, "http://x.com/a") == "http://x.com/a"
    assert canon(base, "http://x.com/a/") == "http://x.com/a"
    assert canon(base, "http://X.COM/a") == "http://x.com/a"
    assert canon(base, "http://x.com/a#top") == "http://x.com/a"
    # relative resolution
    assert canon(base, "/a") == "http://x.com/a"
    assert canon(base, "../a") == "http://x.com/a"
    # query param ordering
    assert canon(base, "http://x.com/p?b=2&a=1") == canon(base, "http://x.com/p?a=1&b=2")
    # non-http dropped
    assert canon(base, "mailto:hi@x.com") is None
    assert canon(base, "javascript:void(0)") is None
    print("  canonicalize: PASS")


def test_extract_links():
    html = '<a href="/x">1</a><a href="x/">2</a><a>no href</a><a href="mailto:a@b.com">m</a>'
    links = c.extract_links(html, "http://x.com/dir/")
    assert links == ["http://x.com/x", "http://x.com/dir/x"], links
    print("  extract_links: PASS")


def test_crawl_loop():
    pages = {
        "http://site.test/":  '<a href="/a">A</a><a href="/b">B</a><a href="/a/">dup</a>',
        "http://site.test/a": '<a href="/">home</a><a href="/c">C</a>',
        "http://site.test/b": '<a href="http://other.com/x">offsite</a>',
        "http://site.test/c": '<a href="/a">back</a>',
    }

    async def fake_fetch(session, url, timeout=10.0):
        return (pages[url], 200) if url in pages else (None, 404)

    async def go():
        with patch.object(c, "fetch", fake_fetch):
            cr = c.Crawler("http://site.test/", max_pages=50, max_depth=3, delay=0)
            await cr.run()
            assert cr.crawled == 4, cr.crawled
            assert not any("other.com" in u for u in cr.visited)
    asyncio.run(go())
    print("  crawl_loop (dedup + on-site + depth): PASS")


if __name__ == "__main__":
    print("Running Phase 1 tests:")
    test_canonicalize()
    test_extract_links()
    test_crawl_loop()
    print("All tests passed.")
