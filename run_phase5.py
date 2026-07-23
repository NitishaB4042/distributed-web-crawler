"""
Phase 5 — Crawler runner with live metrics sampling.

Thin wrapper over the Phase 4 crawler that also runs the background metrics
sampler, so the Streamlit dashboard has data to plot. Everything else (claim
pattern, recovery, retries, rate limiting, robots) is reused unchanged from
crawler_phase4.

Run a crawl that feeds the dashboard:
    python run_phase5.py --seed https://quotes.toscrape.com \
        --workers 4 --recover --sample --max-pages 200

Then in another terminal/cell:
    streamlit run dashboard.py
"""

import asyncio
import argparse

import crawler_phase4 as c
import metrics


async def main():
    ap = argparse.ArgumentParser(description="Phase 5 crawler runner (with metrics)")
    ap.add_argument("--seed", metavar="URL")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--max-pages", type=int, default=200)
    ap.add_argument("--max-depth", type=int, default=3)
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--burst", type=float, default=4.0)
    ap.add_argument("--visibility-timeout", type=float, default=30)
    ap.add_argument("--recover", action="store_true")
    ap.add_argument("--recover-interval", type=float, default=5)
    ap.add_argument("--idle-timeout", type=float, default=5)
    ap.add_argument("--sample", action="store_true", help="run the metrics sampler")
    ap.add_argument("--sample-interval", type=float, default=0.5)
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    r = c.aioredis.from_url(c.redis_url(), decode_responses=True)
    q = c.Queue(r)
    try:
        if args.reset:
            await c.reset(r)
            await r.delete(metrics.K_SAMPLES)
            print("[reset] cleared crawl state + samples.")
            return
        if args.seed:
            await c.seed(r, q, args.seed)

        bucket = c.TokenBucket(r, args.rate, args.burst)
        stop = asyncio.Event()

        tasks = [asyncio.create_task(
                    c.worker(r, q, bucket, args.max_pages, args.max_depth,
                             args.rate, args.visibility_timeout, args.idle_timeout))
                 for _ in range(args.workers)]
        if args.recover:
            tasks.append(asyncio.create_task(
                c.recoverer(r, q, args.recover_interval, args.idle_timeout * 2)))

        sampler_task = None
        if args.sample:
            # clear stale samples so the chart starts fresh
            await r.delete(metrics.K_SAMPLES)
            sampler_task = asyncio.create_task(
                metrics.sampler(r, args.sample_interval, stop))

        await asyncio.gather(*tasks)   # wait for workers (+recoverer) to drain
        stop.set()
        if sampler_task:
            await metrics.snapshot(r)  # final sample
            await sampler_task

        await c.report(r)
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
