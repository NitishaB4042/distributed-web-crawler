"""
Phase 5 — Live crawl dashboard (Streamlit).

A real-time view of the crawl: headline counters, queue depths, and a
throughput-over-time chart. This is the 10-second "wow" for anyone looking at
the project.

Run (in its own terminal/Colab cell):
    streamlit run dashboard.py

Set REDIS_URL in the environment first if not using localhost.

Notes for Hugging Face Spaces deployment:
    - Add a requirements.txt with: streamlit, redis, pandas
    - Set REDIS_URL as a Space secret (your Upstash rediss:// URL)
    - The Space runs `streamlit run dashboard.py` automatically
"""

import os
import json
import time

import redis
import pandas as pd
import streamlit as st

NS = os.environ.get("CRAWL_NS", "crawl")
K_STATS    = f"{NS}:stats"
K_VISITED  = f"{NS}:visited"
K_FRONTIER = f"{NS}:frontier"
K_INFLIGHT = f"{NS}:inflight"
K_SAMPLES  = f"{NS}:samples"
K_DLQ      = f"{NS}:dlq"
K_ROOT     = f"{NS}:root_netloc"


def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


@st.cache_resource
def get_client():
    # synchronous client here — the dashboard is read-only and simple
    return redis.from_url(redis_url(), decode_responses=True)


def gi(r, field):
    return int(r.hget(K_STATS, field) or 0)


def load_state(r):
    samples = [json.loads(x) for x in r.lrange(K_SAMPLES, 0, -1)]
    rate = 0.0
    if len(samples) >= 2:
        a, b = samples[-2], samples[-1]
        dt = b["t"] - a["t"]
        if dt > 0:
            rate = (b["crawled"] - a["crawled"]) / dt
    return {
        "root": r.get(K_ROOT),
        "crawled": gi(r, "crawled"),
        "failed": gi(r, "failed"),
        "retried": gi(r, "retried"),
        "recovered": gi(r, "recovered"),
        "dead_lettered": gi(r, "dead_lettered"),
        "throttled": gi(r, "throttled"),
        "robots_blocked": gi(r, "robots_blocked"),
        "unique": r.scard(K_VISITED),
        "frontier": r.llen(K_FRONTIER),
        "inflight": r.zcard(K_INFLIGHT),
        "dlq": r.llen(K_DLQ),
        "rate": round(rate, 2),
        "samples": samples,
    }


st.set_page_config(page_title="Crawler Dashboard", page_icon="🕷️", layout="wide")
st.title("🕷️ Distributed Web Crawler — Live Dashboard")

auto = st.sidebar.checkbox("Auto-refresh (2s)", value=True)
st.sidebar.caption(f"Redis: {redis_url().split('@')[-1]}")
st.sidebar.caption(f"Namespace: {NS}")

try:
    r = get_client()
    r.ping()
except Exception as e:
    st.error(f"Cannot reach Redis at {redis_url()} — {e}")
    st.stop()

s = load_state(r)

st.caption(f"Crawling: **{s['root'] or '(not seeded)'}**")

# headline metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Pages crawled", f"{s['crawled']:,}")
c2.metric("Throughput", f"{s['rate']:.1f}/s")
c3.metric("Unique URLs seen", f"{s['unique']:,}")
c4.metric("In-flight", f"{s['inflight']:,}")

c5, c6, c7, c8 = st.columns(4)
c5.metric("Frontier (queued)", f"{s['frontier']:,}")
c6.metric("Retried", f"{s['retried']:,}")
c7.metric("Recovered", f"{s['recovered']:,}", help="dead-worker requeues")
c8.metric("Dead-lettered", f"{s['dlq']:,}")

st.divider()

# throughput-over-time chart
samples = s["samples"]
if len(samples) >= 2:
    df = pd.DataFrame(samples)
    t0 = df["t"].iloc[0]
    df["seconds"] = df["t"] - t0
    # instantaneous rate between consecutive samples
    df["rate"] = df["crawled"].diff() / df["t"].diff()
    df = df.dropna()

    left, right = st.columns(2)
    with left:
        st.subheader("Throughput over time")
        st.line_chart(df.set_index("seconds")[["rate"]], height=280)
    with right:
        st.subheader("Queue depth over time")
        st.line_chart(df.set_index("seconds")[["frontier", "inflight"]], height=280)

    st.subheader("Cumulative pages crawled")
    st.area_chart(df.set_index("seconds")[["crawled"]], height=220)
else:
    st.info("Waiting for samples… start a crawl with the sampler running "
            "(`--sample` flag on the crawler, or run the sampler task).")

# secondary counters
st.divider()
b1, b2, b3 = st.columns(3)
b1.metric("Throttled (rate-limited)", f"{s['throttled']:,}")
b2.metric("Robots-blocked", f"{s['robots_blocked']:,}")
b3.metric("Failed fetches", f"{s['failed']:,}")

if auto:
    time.sleep(2)
    st.rerun()
