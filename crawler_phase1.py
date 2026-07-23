"""
Phase 1 — Single-process async web crawler.

Goal of this phase: get the CORE LOOP correct before any distribution.
No Redis, no multiple workers. Just prove that fetching, parsing, and
(most importantly) URL canonicalization work in isolation.

Run:
    python crawler_phase1.py https://example.com --max-pages 30 --max-depth 2

Dependencies:
    pip install aiohttp selectolax
"""

import asyncio
import argparse
import time
from collections import deque
from urllib.parse import urljoin, urldefrag, urlsplit, urlunsplit

import aiohttp
from selectolax.parser import HTMLParser


# ---------------------------------------------------------------------------
# URL canonicalization — the most important function in Phase 1.
# These must all collapse to the SAME key, or the crawler loops / double-counts:
#   http://x.com/a   http://x.com/a/   http://X.COM/a   http://x.com/a#top
# ---------------------------------------------------------------------------
def canonicalize(base: str, link: str) -> str | None:
    """Resolve `link` (possibly relative) against `base` and normalize it.

    Returns a canonical absolute URL, or None if the link isn't crawlable
    (non-http scheme, mailto:, javascript:, etc.).
    """
    try:
        absolute = urljoin(base, link)        # resolve relative -> absolute
        absolute, _ = urldefrag(absolute)     # drop the #fragment
        parts = urlsplit(absolute)
    except ValueError:
        return None

    if parts.scheme not in ("http", "https"):
        return None
    if not parts.netloc:
        return None

    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()

    # Normalize path: collapse empty path to "/", and strip a trailing slash
    # on non-root paths so /a and /a/ are treated as one.
    path = parts.path or "/"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")

    # Sort query params so ?b=1&c=2 and ?c=2&b=1 match. Keep query, drop fragment.
    query = parts.query
    if query:
        pairs = sorted(query.split("&"))
        query = "&".join(pairs)

    return urlunsplit((scheme, netloc, path, query, ""))


def same_registered_domain(url: str, root_netloc: str) -> bool:
    """Keep the crawl on the starting site (and its subdomains)."""
    netloc = urlsplit(url).netloc.lower()
    return netloc == root_netloc or netloc.endswith("." + root_netloc)


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------
async def fetch(session: aiohttp.ClientSession, url: str, timeout: float = 10.0):
    """Fetch a URL. Returns (html_text, status) or (None, status/None) on failure."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            ctype = resp.headers.get("Content-Type", "")
            if "text/html" not in ctype:
                return None, resp.status          # skip PDFs, images, etc.
            return await resp.text(), resp.status
    except asyncio.TimeoutError:
        return None, None
    except aiohttp.ClientError:
        return None, None


def extract_links(html: str, base_url: str) -> list[str]:
    """Pull all <a href> links from the page and canonicalize them."""
    tree = HTMLParser(html)
    out = []
    for node in tree.css("a[href]"):
        href = node.attributes.get("href")
        if not href:
            continue
        canon = canonicalize(base_url, href)
        if canon:
            out.append(canon)
    return out


# ---------------------------------------------------------------------------
# The crawler
# ---------------------------------------------------------------------------
class Crawler:
    def __init__(self, start_url: str, max_pages: int = 30,
                 max_depth: int = 2, delay: float = 0.5):
        start = canonicalize(start_url, start_url)
        if start is None:
            raise ValueError(f"Invalid start URL: {start_url}")

        self.start = start
        self.root_netloc = urlsplit(start).netloc.lower()
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.delay = delay                       # politeness placeholder (Phase 3 replaces this)

        self.frontier: deque[tuple[str, int]] = deque([(start, 0)])
        self.visited: set[str] = set([start])    # mark queued URLs as seen immediately
        self.crawled = 0
        self.failed = 0

    async def run(self):
        headers = {"User-Agent": "Phase1Crawler/0.1 (learning project)"}
        async with aiohttp.ClientSession(headers=headers) as session:
            while self.frontier and self.crawled < self.max_pages:
                url, depth = self.frontier.popleft()

                html, status = await fetch(session, url)
                if html is None:
                    self.failed += 1
                    print(f"[FAIL {status}] {url}")
                    continue

                self.crawled += 1
                links = extract_links(html, url)
                new_links = 0

                if depth < self.max_depth:
                    for link in links:
                        if link in self.visited:
                            continue              # dedup against canonical key
                        if not same_registered_domain(link, self.root_netloc):
                            continue              # stay on-site
                        self.visited.add(link)
                        self.frontier.append((link, depth + 1))
                        new_links += 1

                print(f"[OK  {status}] d{depth} {url}  "
                      f"({len(links)} links, {new_links} new)")

                await asyncio.sleep(self.delay)   # naive politeness; replaced in Phase 3

    def report(self):
        print("\n" + "=" * 60)
        print(f"  Start URL    : {self.start}")
        print(f"  Pages crawled: {self.crawled}")
        print(f"  Pages failed : {self.failed}")
        print(f"  Unique URLs  : {len(self.visited)}")
        print(f"  Frontier left: {len(self.frontier)}")
        print("=" * 60)


async def main():
    ap = argparse.ArgumentParser(description="Phase 1 single-process crawler")
    ap.add_argument("start_url")
    ap.add_argument("--max-pages", type=int, default=30)
    ap.add_argument("--max-depth", type=int, default=2)
    ap.add_argument("--delay", type=float, default=0.5)
    args = ap.parse_args()

    crawler = Crawler(args.start_url, args.max_pages, args.max_depth, args.delay)
    t0 = time.time()
    await crawler.run()
    elapsed = time.time() - t0

    crawler.report()
    if elapsed > 0:
        print(f"  Rate         : {crawler.crawled / elapsed:.2f} pages/sec")


if __name__ == "__main__":
    asyncio.run(main())
