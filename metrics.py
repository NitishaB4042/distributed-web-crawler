"""
Phase 5 — Metrics layer.

A tiny sampler that periodically snapshots crawl progress into a Redis time
series, so the dashboard and benchmark can plot throughput over time.

We store samples in a capped Redis LIST (crawl:samples) as JSON:
    {"t": epoch_seconds, "crawled": N, "frontier": M, "inflight": K}

Throughput between two samples = (crawled_b - crawled_a) / (t_b - t_a).
"""

import json
import time
import asyncio
import os

import redis.asyncio as aioredis

NS = os.environ.get("CRAWL_NS", "crawl")
K_STATS    = f"{NS}:stats"
K_VISITED  = f"{NS}:visited"
K_FRONTIER = f"{NS}:frontier"
K_INFLIGHT = f"{NS}:inflight"
K_SAMPLES  = f"{NS}:samples"
K_DLQ      = f"{NS}:dlq"

MAX_SAMPLES = 2000   # cap so the list never grows unbounded


async def snapshot(r: aioredis.Redis) -> dict:
    """Take one metrics snapshot and append it to the capped samples list."""
    crawled = int(await r.hget(K_STATS, "crawled") or 0)
    frontier = await r.llen(K_FRONTIER)
    inflight = await r.zcard(K_INFLIGHT)
    sample = {"t": round(time.time(), 3), "crawled": crawled,
              "frontier": frontier, "inflight": inflight}
    await r.rpush(K_SAMPLES, json.dumps(sample))
    await r.ltrim(K_SAMPLES, -MAX_SAMPLES, -1)
    return sample


async def get_samples(r: aioredis.Redis) -> list[dict]:
    raw = await r.lrange(K_SAMPLES, 0, -1)
    return [json.loads(x) for x in raw]


async def sampler(r: aioredis.Redis, interval: float, stop_event: asyncio.Event):
    """Background task: snapshot every `interval` seconds until stopped."""
    while not stop_event.is_set():
        await snapshot(r)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def dashboard_state(r: aioredis.Redis) -> dict:
    """Everything the dashboard needs in one round-trip-ish call."""
    async def gi(f): return int(await r.hget(K_STATS, f) or 0)
    samples = await get_samples(r)
    # compute current throughput from the last two samples
    rate = 0.0
    if len(samples) >= 2:
        a, b = samples[-2], samples[-1]
        dt = b["t"] - a["t"]
        if dt > 0:
            rate = (b["crawled"] - a["crawled"]) / dt
    return {
        "crawled": await gi("crawled"),
        "failed": await gi("failed"),
        "retried": await gi("retried"),
        "recovered": await gi("recovered"),
        "dead_lettered": await gi("dead_lettered"),
        "throttled": await gi("throttled"),
        "robots_blocked": await gi("robots_blocked"),
        "unique": await r.scard(K_VISITED),
        "frontier": await r.llen(K_FRONTIER),
        "inflight": await r.zcard(K_INFLIGHT),
        "dlq": await r.llen(K_DLQ),
        "rate": round(rate, 2),
        "samples": samples,
    }
