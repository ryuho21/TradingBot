"""
trainer.py  —  Institutional Pattern Library Builder
Replaces pt_trainer.py with:
- Async KuCoin history fetching (no blocking sleep)
- Trains PatternLibrary + HMM regime model for each coin + timeframe
- Writes trainer_last_training_time.txt for freshness gate compatibility
- Publishes trainer_status.json for GUI
"""
import asyncio
import json
import logging
import os
import sys
import time
from typing import List, Optional

import aiohttp
import numpy as np

log = logging.getLogger("trainer")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

KUCOIN_REST  = "https://api.kucoin.com"
TF_CHOICES   = ["1hour", "2hour", "4hour", "8hour", "12hour", "1day", "1week"]
TF_SECS = {
    "1hour":   3600,  "2hour":   7200,  "4hour":  14400,
    "8hour":  28800,  "12hour": 43200,  "1day":  86400,
    "1week": 604800,
}
MAX_CANDLES  = 1500
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))


def coin_folder(sym: str) -> str:
    return BASE_DIR if sym.upper() == "BTC" else os.path.join(BASE_DIR, sym.upper())


# ── Async KuCoin fetch ────────────────────────────────────────────────────────
async def fetch_candles(session: aiohttp.ClientSession, coin: str, tf: str,
                        n: int = MAX_CANDLES) -> np.ndarray:
    """
    Returns a 2D numpy array: columns [ts, open, close, high, low, volume].
    Rows ordered oldest → newest.
    """
    end   = int(time.time())
    start = end - TF_SECS[tf] * n

    rows = []
    # KuCoin returns at most ~1500 per call; paginate if needed
    while True:
        url = (f"{KUCOIN_REST}/api/v1/market/candles"
               f"?symbol={coin}&type={tf}&startAt={start}&endAt={end}")
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json()
            data = d.get("data") or []
        except Exception as e:
            log.warning("Fetch error %s %s: %s", coin, tf, e)
            break

        if not data:
            break
        for row in data:
            try:
                rows.append([float(x) for x in row[:6]])
            except Exception:
                pass

        # KuCoin returns newest-first; if we got < 1500 rows we're done
        if len(data) < 1499:
            break

        # Advance window back
        end = int(rows[-1][0])  # oldest ts in this batch
        if end <= start:
            break
        await asyncio.sleep(0.1)  # rate limit courtesy

    if not rows:
        return np.empty((0, 6))

    arr = np.array(rows, dtype=np.float64)  # newest first
    arr = arr[::-1]                          # → oldest first
    return arr


# ── Train a single coin/tf ────────────────────────────────────────────────────
async def train_symbol_tf(session: aiohttp.ClientSession,
                           sym: str, tf: str,
                           engine) -> int:
    """Fetches candles and calls brain.train(). Returns candle count."""
    from brain import IntelligenceEngine
    coin = f"{sym}-USDT"
    arr  = await fetch_candles(session, coin, tf)
    if arr.shape[0] < 10:
        log.warning("Insufficient candles for %s %s (%d)", sym, tf, arr.shape[0])
        return 0

    opens  = arr[:, 1]
    closes = arr[:, 2]
    highs  = arr[:, 3]
    lows   = arr[:, 4]

    engine.train(sym, tf, opens, closes, highs, lows)
    return int(arr.shape[0])


# ── Write legacy gate files ───────────────────────────────────────────────────
def _write_status(folder: str, coin: str, state: str, started: int,
                  finished: Optional[int] = None):
    d = {"coin": coin, "state": state,
         "started_at": started, "timestamp": int(time.time())}
    if finished:
        d["finished_at"] = finished
    try:
        with open(os.path.join(folder, "trainer_status.json"), "w") as f:
            json.dump(d, f)
    except Exception:
        pass


def _write_freshness(folder: str):
    ts = int(time.time())
    try:
        with open(os.path.join(folder, "trainer_last_training_time.txt"), "w") as f:
            f.write(str(ts))
    except Exception:
        pass


# ── Entry point ───────────────────────────────────────────────────────────────
from typing import Optional

async def train(coins: List[str], engine=None):
    """
    Main training coroutine. Can be called standalone or imported by main.py.

    Usage:
        python trainer.py BTC ETH XRP
    """
    # Lazy import so brain.py isn't loaded unless training runs
    from brain import IntelligenceEngine
    if engine is None:
        engine = IntelligenceEngine(coins)

    started = int(time.time())
    total_coins = len(coins)

    log.info("=== PowerTrader Institutional Trainer ===")
    log.info("Coins: %s | Timeframes: %s", coins, TF_CHOICES)

    async with aiohttp.ClientSession() as session:
        for i, sym in enumerate(coins):
            folder  = coin_folder(sym)
            os.makedirs(folder, exist_ok=True)
            _write_status(folder, sym, "TRAINING", started)

            log.info("[%d/%d] Training %s …", i + 1, total_coins, sym)
            tf_counts = {}

            # Fan-out: train all timeframes concurrently for this coin
            tasks = {
                tf: asyncio.create_task(train_symbol_tf(session, sym, tf, engine))
                for tf in TF_CHOICES
            }
            for tf, task in tasks.items():
                try:
                    count = await task
                    tf_counts[tf] = count
                    log.info("  %s %s: %d candles → library size %d",
                             sym, tf, count,
                             engine._lib(sym, tf).size())
                except Exception as e:
                    log.error("  %s %s failed: %s", sym, tf, e)

            finished = int(time.time())
            _write_freshness(folder)
            _write_status(folder, sym, "FINISHED", started, finished)
            elapsed = finished - started
            log.info("  ✓ %s done in %ds  tf_candles=%s", sym, elapsed, tf_counts)

    log.info("All coins trained. Engine ready.")
    return engine


if __name__ == "__main__":
    coins = [a.strip().upper() for a in sys.argv[1:] if a.strip()] or ["BTC"]
    asyncio.run(train(coins))
