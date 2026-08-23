# Distributed Web Crawler — Phase 5 (Final)

Adds the **metrics layer, live dashboard, and scaling benchmark** — the pieces
that make the project legible to a recruiter in ten seconds and let you defend
the horizontal-scaling claim with real numbers.

## Files
- `metrics.py` — time-series sampler (snapshots into a capped Redis list)
- `run_phase5.py` — Phase 4 crawler + background sampler (adds `--sample`)
- `dashboard.py` — Streamlit live dashboard
- `benchmark.py` — throughput-vs-workers benchmark, writes `benchmark.png`
- `requirements.txt` — for Hugging Face Spaces / local install

## Run it end to end
```bash
pip install -r requirements.txt
export REDIS_URL="rediss://...upstash.io:6379"   # or local redis

# 1) start a crawl that feeds the dashboard
python run_phase5.py --seed https://quotes.toscrape.com \
    --workers 4 --recover --sample --max-pages 200

# 2) in another terminal/Colab cell, open the dashboard
streamlit run dashboard.py
```

## The benchmark (your strongest single artifact)
```bash
python benchmark.py --pages 400 --workers 1 2 4 8 16 --fetch-latency 0.03
```
Runs the real Phase 4 crawler over a deterministic in-process mock site with
simulated network latency, at increasing worker counts. Produces:
- a printed table + speedup summary
- `benchmark_results.json`
- `benchmark.png` (measured throughput vs the ideal-linear line)

### Why simulated latency?
Real crawlers are **I/O-bound** — they spend almost all their time waiting on
the network. The benchmark models that with a per-fetch delay so it measures
what actually scales (concurrent waiting), not CPU. With an instant mock fetch,
the workload collapses to pure Redis round-trips and shows no scaling — which is
itself the honest reason the latency model is necessary.

### Reading the curve
Throughput rises steeply at first, then bends away from the ideal line at
higher worker counts. That gap is **real and expected**: Redis coordination
overhead and the single-domain rate limit cap per-host concurrency. Being able
to explain *why* scaling is sub-linear is a stronger interview signal than a
suspiciously perfect straight line.

## Metrics design
- `metrics.snapshot()` appends `{t, crawled, frontier, inflight}` to
  `crawl:samples`, capped at 2000 entries via `LTRIM`.
- Instantaneous throughput = Δcrawled / Δt between consecutive samples.
- `dashboard_state()` returns all counters + the sample series in one call.

## Deploying the dashboard to Hugging Face Spaces
1. New Space → Streamlit SDK.
2. Upload `dashboard.py`, `metrics.py`, `requirements.txt`.
3. Add `REDIS_URL` as a Space **secret** (your Upstash `rediss://` URL).
4. The Space auto-runs `streamlit run dashboard.py`. Run the crawler from Colab
   pointed at the same Redis, and the dashboard updates live.



## The full project, recapped
| Phase | Adds | Key concept |
|-------|------|-------------|
| 1 | single-process loop | URL canonicalization |
| 2 | Redis-backed state | atomic dedup, stateless workers |
| 3 | politeness layer | token bucket (atomic Lua), robots.txt |
| 4 | fault tolerance | claim pattern, at-least-once, DLQ |
| 5 | metrics + benchmark | observability, horizontal-scaling proof |
