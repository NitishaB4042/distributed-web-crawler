"""
Phase 5 — Benchmark: throughput vs worker count.

Proves the horizontal-scaling claim with real numbers. Runs the Phase 4
crawler over a deterministic in-process mock site (no external network, so
results are reproducible) with increasing worker counts and records
pages/second for each.

Output:
    - prints a table
    - writes benchmark_results.json
    - writes benchmark.png (throughput vs workers)

Run:
    python benchmark.py --pages 400 --workers 1 2 4 8

Needs a running Redis (REDIS_URL or localhost).
"""

import asyncio
import argparse
import json
import time
from unittest.mock import patch

import crawler_phase4 as c


def build_site(n_pages: int) -> dict:
    """A connected mock site of n_pages pages.

    The homepage links to a 'hub' set, and every page links forward to several
    others, so the entire site is reachable within a few hops (keeps the crawl
    from stalling at the depth limit).
    """
    pages = {}
    root = "http://bench.test/"
    # homepage links to the first chunk of pages directly
    hub = min(n_pages, 50)
    pages[root] = "".join(f'<a href="/p{i}">p{i}</a>' for i in range(hub))
    for i in range(n_pages):
        # each page links forward to spread reachability across the whole site
        targets = [f"/p{(i + k) % n_pages}" for k in (1, 7, 31, 113)] + ["/"]
        pages[f"http://bench.test/p{i}"] = "".join(f'<a href="{t}">x</a>' for t in targets)
    return pages, root


async def run_once(n_workers: int, pages: dict, root: str, page_budget: int,
                   fetch_latency: float) -> tuple[float, int]:
    """Run one crawl with n_workers; return (pages/second, crawled)."""
    r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
    await c.reset(r)
    q = c.Queue(r)
    await c.seed(r, q, root)

    # Mock fetch with realistic network latency. This is the key: real crawlers
    # are I/O-bound (waiting on the network), and async lets many fetches wait
    # concurrently. Modeling that latency is what makes the scaling test honest.
    async def fake_fetch(session, url, timeout=10.0):
        await asyncio.sleep(fetch_latency)
        return (pages[url], 200, False) if url in pages else (None, 404, False)

    async def fake_robots(r2, s, d, scheme="https"):
        rp = c.RobotFileParser(); rp.parse(["User-agent: *", "Allow: /"]); return rp

    # High rate limit so the bucket doesn't gate the benchmark.
    bucket = c.TokenBucket(r, rate=1_000_000, burst=1_000_000)

    with patch.object(c, "fetch", fake_fetch), \
         patch.object(c, "load_robots", fake_robots):
        t0 = time.time()
        tasks = [asyncio.create_task(
                    c.worker(r, q, bucket, page_budget, max_depth=10,
                             default_rate=1_000_000, visibility_timeout=30,
                             idle_timeout=1))
                 for _ in range(n_workers)]
        await asyncio.gather(*tasks)
        elapsed = time.time() - t0

    crawled = int(await r.hget(c.K_STATS, "crawled") or 0)
    await c.reset(r)
    await r.aclose()
    return crawled / elapsed if elapsed > 0 else 0.0, crawled


async def main():
    ap = argparse.ArgumentParser(description="Throughput vs workers benchmark")
    ap.add_argument("--pages", type=int, default=400)
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    ap.add_argument("--trials", type=int, default=2, help="runs per worker count, averaged")
    ap.add_argument("--fetch-latency", type=float, default=0.03,
                    help="simulated per-fetch network latency in seconds (default 30ms)")
    args = ap.parse_args()

    pages, root = build_site(args.pages)
    results = []

    print(f"\nBenchmark: {args.pages}-page mock site, {args.trials} trial(s) each, "
          f"{int(args.fetch_latency*1000)}ms fetch latency\n")
    print(f"  {'workers':>8} | {'pages/sec':>10} | {'crawled':>8}")
    print("  " + "-" * 34)
    for w in args.workers:
        rates = []
        crawled_n = 0
        for _ in range(args.trials):
            rate, crawled_n = await run_once(w, pages, root, args.pages + 1,
                                             args.fetch_latency)
            rates.append(rate)
        avg = sum(rates) / len(rates)
        results.append({"workers": w, "pages_per_sec": round(avg, 1), "crawled": crawled_n})
        print(f"  {w:>8} | {avg:>10.1f} | {crawled_n:>8}")

    # speedup relative to 1 worker
    base = results[0]["pages_per_sec"] or 1
    print("\n  Speedup vs 1 worker:")
    for row in results:
        print(f"    {row['workers']}x workers -> {row['pages_per_sec']/base:.2f}x throughput")

    with open("benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\n  wrote benchmark_results.json")

    # plot
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        ws = [r["workers"] for r in results]
        ps = [r["pages_per_sec"] for r in results]
        ideal = [base * w for w in ws]
        fig, ax = plt.subplots(figsize=(7, 4.5))
        ax.plot(ws, ps, "o-", color="#2E75B6", linewidth=2.2, markersize=7, label="measured")
        ax.plot(ws, ideal, "--", color="#A6A6A6", linewidth=1.5, label="ideal (linear)")
        ax.set_xlabel("Workers"); ax.set_ylabel("Throughput (pages/sec)")
        ax.set_title("Crawler throughput scales with worker count")
        ax.grid(True, alpha=0.3); ax.legend()
        ax.set_xticks(ws)
        fig.tight_layout()
        fig.savefig("benchmark.png", dpi=130)
        print("  wrote benchmark.png")
    except ImportError:
        print("  (matplotlib not installed — skipped benchmark.png)")


if __name__ == "__main__":
    asyncio.run(main())
