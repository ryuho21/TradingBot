"""
data_hub.py  —  OKX WebSocket Hub + KuCoin Historical Backfill + InstrumentCache

CHANGELOG — POST-ROADMAP TRACK 17: DCA / EXIT-PATH DECISION-TRACE EXPANSION
═══════════════════════════════════════════════════════════════════════════════
[TRACK17-SCHEMA]  _TRACK17_DT_MIGRATION constant added:
               ALTER TABLE decision_trace ADD COLUMN pnl_pct REAL
               ALTER TABLE decision_trace ADD COLUMN hold_time_secs REAL
               Applied in _conn_obj() immediately after the TRACK16 block,
               using the same duplicate-column-ignore pattern.  Old rows carry
               NULL for both columns (correct — no data loss).  Safe no-op on
               DBs where the columns already exist.

[TRACK17-INSERT]  insert_decision_trace() updated to accept and persist
               optional "pnl_pct" (float) and "hold_time_secs" (float) keys.
               "source_path": "close" — records from _record_close
               "source_path": "dca"   — records from _maybe_dca (successful add)
               Absent keys → NULL stored (backward-compatible with all
               pre-Track-17 callers).  Fail-open contract unchanged.

CHANGELOG — POST-ROADMAP TRACK 16: DECISION-TRACE EXPANSION (EXPRESS PATH)
═══════════════════════════════════════════════════════════════════════════════
[TRACK16-DT-SCHEMA]  _TRACK16_DT_MIGRATION constant added:
               ALTER TABLE decision_trace ADD COLUMN source_path TEXT
               Applied in _conn_obj() immediately after the TRACK10
               executescript block, using the same duplicate-column-ignore
               pattern as all prior additive migrations.  Old rows carry
               NULL source_path (correct — no data loss).  Safe no-op on
               all DBs where the column already exists.

[TRACK16-DT-INSERT]  insert_decision_trace() updated to accept and persist
               an optional "source_path" key from the record dict.
               "source_path": "entry"   — records from _maybe_enter
               "source_path": "express" — records from trigger_atomic_express_trade
               Absent key → NULL stored (backward-compatible with all
               pre-Track-16 callers).  No behavioral change; fail-open
               contract unchanged.

Phase 37 additions:
  [P37-VPIN]   Volume-Clock & VPIN Calculation — TapeBuffer now maintains a
               VolumeBucket state machine.  Every trade accumulates buy/sell
               volume until the bucket total crosses P37_VPIN_BUCKET_SIZE (1.0
               BTC default), at which point VPIN = |BuyVol - SellVol| / TotalVol
               is computed and appended to a rolling 50-bucket history.  A
               ToxicityScore ∈ [0.0, 1.0] is derived from where the latest VPIN
               sits in the empirical CDF of the history window.  DataHub exposes
               get_flow_toxicity(symbol) so VetoArbitrator and the Executor can
               gate entries and trigger emergency exits without polling the tape.

Phase 36.2 additions:
  [P36.2-SELFHEAL]  force_immediate_refresh(symbol) — bypasses the 1800-second
                    InstrumentCache timer to immediately fetch fresh buyLmt /
                    sellLmt price bands for a specific symbol via REST.  Called
                    by the Executor whenever an sCode 51006 rejection is detected
                    so the next order has accurate price-limit data.

Phase 36.1 additions:
  [P36.1-DETECT]  Spoof Toxicity Store — DataHub now maintains a per-symbol
                  spoof probability dict (_p36_spoof_probs).  The Executor's
                  Mimic Order Engine writes detected spoof probabilities via
                  update_spoof_toxicity(); the VetoArbitrator reads them via
                  get_spoof_probability() to gate new entries.

Phase 15 additions:
  [P15-3] CoinbaseCDPCredentials  — startup PEM validation
  [P15-1] CoinbaseOracle          — initialised and task-launched in DataHub.start()
  [P15-4] GlobalTapeAggregator    — initialised in DataHub.start()
  [P15-2] DataHub.cycle_oracle_cancel()  — called by Executor when
          signal.cancel_buys_flag is True; cancels all open OKX BUY limit orders.
  [P15]   get_oracle_signal(), is_hv_mode(), hv_stop_extra_pct(),
          get_global_tape_status() added to DataHub public API.
  [P15]   P15 telemetry embedded in get_status_snapshot().

Phase 12 additions (preserved):
  [P12-1] OrderBook.obi()  [P12-2] TapeBuffer + trades channel
  [P12-3] Pennying helper  [P12-4] get_trade_velocity()

Phase 8 (preserved): SQLite WAL + timeout=30 + NORMAL sync
Phase 5 (preserved): Sentiment & Liquidation Tracking
Phase 4 (preserved): Flash-Crash Circuit Breaker — VolatilityGuard
Phase 3 (preserved): books5 channel → OrderBook
Phase 1/2 (preserved): InstrumentCache, CandleBuffer, Database, Cache,
                        OKXMarketFeed, OKXPrivateFeed

PATCH — Task 4 additions:
  [TASK-4-A] DataHub.__init__: _risk_executor and _risk_cb reference slots.
  [TASK-4-B] DataHub.get_risk_status_snapshot(): unified Phase 4/7/15/20
             risk state for the GUI — surfaces block_reason, cb_tripped,
             zombie_active, emergency_pause, oracle_connected, hv_mode.
  [TASK-4-C] Wire point in main_p20.py (comment only — see main_p20.py):
               hub._risk_executor = executor
               hub._risk_cb       = cb

Stage 6.1 additions:
  [S6.1-DBPATH]   Database canonical path is hub_data/powertrader.db (absolute),
                  co-located with all other hub_data artifacts.  hub_data/ is
                  created automatically on first use.  The resolved path is always
                  absolute — constructed via Path(__file__).resolve().parent /
                  "hub_data" / "powertrader.db" — so there is no ambiguity
                  regardless of the process working directory.
                  datahub.db is NOT used anywhere in this module.

Stage 7A additions:
  [S7A-DBPATH]    Explicit absolute-path guarantee documented and enforced in
                  Database.__init__.  _conn_obj() logs and re-raises on any
                  failure so callers and the boot ensure-ready step can detect it.

Stage R1 additions:
  [R1-A-DBPATH]   Database.__init__ ALWAYS constructs the canonical absolute path
                  from Path(__file__).resolve().parent / "hub_data" / "powertrader.db".
                  datahub.db is NEVER used.  hub_data/ is mkdir'd at __init__ time
                  (not lazily) so the directory is guaranteed to exist before any
                  caller attempts to open it.  Path is logged immediately at
                  construction — before any connection is made.

Stage CI-1 additions:
  [CI-1-A]  Candle verified as @dataclass — no structural change required.
  [CI-1-B]  Database.upsert_candles(candles) — batch upsert in a single
            transaction/commit; empty list is a safe no-op; sqlite errors are
            logged and re-raised (no silent loss).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import aiohttp
import redis.asyncio as aioredis

from okx_gateway import (
    OKX_DEMO_MODE,
    OKX_WS_PUBLIC, OKX_WS_PRIVATE, OKX_WS_BIZ,
    OKX_DEMO_WS_PUB, OKX_DEMO_WS_PRIV, OKX_DEMO_WS_BIZ,
    OKXRestClient, OKXWebSocket,
)

log = logging.getLogger("data_hub")
logging.getLogger("data_hub").addHandler(logging.NullHandler())

# ── [P0-FIX-3/11] Shared safe env parsing from pt_utils ──────────────────────
from pt_utils import _env_float, _env_int, atomic_write_json

# ── Symbol / string sanitization helper ───────────────────────────────────────
def _sanitize_sym(raw: str) -> str:
    """
    Strip leading/trailing whitespace and literal single/double quotes from a
    symbol string loaded from .env.  Covers cases like:
        FLASH_CRASH_SYMBOL="BTC-USDT"  →  BTC-USDT
        P35_HEDGE_SYMBOL='BTC-USDT-SWAP'  →  BTC-USDT-SWAP
    """
    return raw.strip().strip("'\"").strip()


# _clean_env is the canonical name used across all modules (executor, data_hub,
# portfolio_manager).  It is identical to _sanitize_sym and handles the
# OKX_PASSPHRASE="$Khalil21z" vs. symbol-quoting conflict: only the surrounding
# quote characters are removed; the content (including $ signs) is preserved.
_clean_env = _sanitize_sym


# ── General config ─────────────────────────────────────────────────────────────
REDIS_URL            = os.environ.get("REDIS_URL",             "redis://localhost:6379/0")
KUCOIN_REST          = "https://api.kucoin.com"
INSTRUMENT_CACHE_TTL = _env_int("INSTRUMENT_CACHE_TTL", 3600)
FLASH_CRASH_DROP_PCT = _env_float("FLASH_CRASH_DROP_PCT", 5.0)
FLASH_CRASH_WINDOW   = _env_float("FLASH_CRASH_WINDOW_SECS", 30.0)
EMERGENCY_PAUSE_SECS = _env_float("EMERGENCY_PAUSE_SECS", 300.0)

# [P5-1]
SENTIMENT_WINDOW_SECS = _env_float("SENTIMENT_WINDOW_SECS", 60.0)

# [P12-2]
TAPE_BUFFER_SECS          = _env_float("P12_TAPE_BUFFER_SECS", 60.0)
TAPE_MAX_EVENTS           = _env_int("P12_TAPE_MAX_EVENTS", 10000)
TAPE_VELOCITY_WINDOW_SECS = _env_float("P12_TAPE_VELOCITY_WINDOW", 5.0)
SWEEP_DIP_PCT             = _env_float("P12_SWEEP_DIP_PCT", 0.3)
SWEEP_BUY_VOL_USD         = _env_float("P12_SWEEP_BUY_VOL_USD", 250000.0)
SWEEP_WINDOW_SECS         = _env_float("P12_SWEEP_WINDOW_SECS", 10.0)

# [P12-1]
OBI_LEVELS    = _env_int("P12_OBI_LEVELS", 10)
OB_MAX_LEVELS = _env_int("P12_OB_MAX_LEVELS", 25)

# [P15-3] Coinbase CDP credentials
COINBASE_API_KEY        = os.environ.get("COINBASE_API_KEY",    "")
COINBASE_API_SECRET_RAW = os.environ.get("COINBASE_API_SECRET", "")

# [TASK-4] Minimum equity value treated as valid for risk snapshot purposes.
# Matches P7_CB_MIN_VALID_EQUITY so all risk subsystems use the same floor.
_RISK_MIN_VALID_EQUITY = _env_float("P7_CB_MIN_VALID_EQUITY", 10.0)

# ── [P34.1] Synthetic Mid-Price Discovery ────────────────────────────────────
P34_OKX_WEIGHT      = _env_float("P34_OKX_WEIGHT", 0.5)
P34_COINBASE_WEIGHT = _env_float("P34_COINBASE_WEIGHT", 0.5)
P34_STALENESS_SECS  = _env_float("P34_STALENESS_SECS", 2.0)

# ── [P37-VPIN] Volume-Clock & Flow Toxicity config ────────────────────────────
P37_VPIN_BUCKET_SIZE = _env_float("P37_VPIN_BUCKET_SIZE", 1.0)
_P37_VPIN_WINDOW     = _env_int("P37_VPIN_WINDOW", 50)

# ── [P38-OFI] Predictive Order Flow Imbalance config ──────────────────────────
# P38_OFI_LEVELS  : how many depth levels to include in the delta computation.
#                   5 covers ~80% of typical institutional book activity without
#                   noise from deep passive liquidity.
# P38_OFI_WINDOW  : number of snapshots to keep in the per-symbol deque.
#                   2 = only the most recent pair (sufficient for single-tick OFI).
#                   Higher values enable rolling-average smoothing in the future.
# P38_WALL_MIN_VOL: minimum size (in base coin) for a passive level to be
#                   classified as a "wall" — below this it is noise, not intent.
#                   Default 0.5 BTC / 5 ETH equivalent; adjust per instrument.
P38_OFI_LEVELS   = _env_int("P38_OFI_LEVELS", 5)
P38_OFI_WINDOW   = _env_int("P38_OFI_WINDOW", 3)
P38_WALL_MIN_VOL = _env_float("P38_WALL_MIN_VOL", 0.5)

# ── [P42-SHADOW] Global Market Correlation Sentinel config ────────────────────
# Polling interval in seconds between Yahoo Finance REST requests.
# Default 30 s — fast enough to catch a 1-minute equity crash within 1 cycle.
P42_POLL_INTERVAL_SECS = _env_float("P42_POLL_INTERVAL_SECS", 30.0)
# Lookback window (seconds) for SPY / DXY delta calculation.
# Default 300 s (5 minutes) — matches the roadmap spec exactly.
P42_LOOKBACK_SECS      = _env_float("P42_LOOKBACK_SECS", 300.0)
# Maximum data age before the veto gate is considered stale and skipped (fail-open).
P42_MAX_STALE_SECS     = _env_float("P42_MAX_STALE_SECS", 300.0)
# Yahoo Finance tickers — free, no API key required.
P42_SPY_TICKER = os.environ.get("P42_SPY_TICKER", "SPY")
P42_DXY_TICKER = os.environ.get("P42_DXY_TICKER", "DX-Y.NYB")
# ── [/P42-SHADOW] ─────────────────────────────────────────────────────────────

# ── [TRACK12-PRUNE] decision_trace retention / pruning config ─────────────────
# DT_RETAIN_DAYS         : rows older than this many days are deleted on each
#                          prune pass.  Default 30 days — generous forensic
#                          window; well above the dashboard's maximum days_back
#                          filter (currently 90d slider cap, but the table only
#                          grows from Track 10 onwards so 30d is safe for fresh
#                          installs and configurable for older ones).
# DT_PRUNE_INTERVAL_SECS : how often the background prune loop runs.
#                          Default 6 hours (21 600 s) — keeps the table bounded
#                          without hammering the DB.  Set to a lower value (e.g.
#                          3 600) in environments with very high signal frequency.
DT_RETAIN_DAYS         = _env_int("DT_RETAIN_DAYS",         30)
DT_PRUNE_INTERVAL_SECS = _env_int("DT_PRUNE_INTERVAL_SECS", 21_600)
# ── [/TRACK12-PRUNE] ──────────────────────────────────────────────────────────

_WS_SUPPORTED_BARS: Dict[str, str] = {
    "1hour": "1H", "2hour": "2H", "4hour": "4H",
    "1day":  "1Dutc", "1week": "1Wutc",
}
_AGGREGATED_TFS = {"8hour", "12hour"}
ALL_TFS = ["1hour", "2hour", "4hour", "8hour", "12hour", "1day", "1week"]

_KUCOIN_FETCH_TFS: Dict[str, str] = {
    "1hour": "1hour", "2hour": "2hour", "4hour": "4hour",
    "1day":  "1day",  "1week": "1week",
}
_TF_SECS: Dict[str, int] = {
    "1hour": 3600, "2hour": 7200,  "4hour": 14400,
    "8hour": 28800, "12hour": 43200, "1day": 86400, "1week": 604800,
}


# ══════════════════════════════════════════════════════════════════════════════
# [P42-SHADOW] Global Market Sentinel — SPY / DXY Macro Crash Monitor
# ══════════════════════════════════════════════════════════════════════════════

class GlobalMarketSentinel:
    """
    [P42-SHADOW] Phase 42 Shadow Correlation Matrix — macro market feed.

    Polls the Yahoo Finance free REST endpoint every P42_POLL_INTERVAL_SECS
    seconds (default 30 s) for SPY and DXY minute-bar data.  No API key is
    required — this uses the public v8/finance/chart endpoint.

    Public API (thread-safe, async-safe):
        get_spy_5m_pct()          → float  (5-min SPY % change, negative = falling)
        get_dxy_5m_pct()          → float  (5-min DXY % change, positive = rising)
        get_data_age_secs()       → float  (seconds since last successful poll)
        get_status_snapshot()     → dict   (dashboard telemetry)

    Fail-open design:
        All network failures are silently swallowed.  The Executor / VetoArbitrator
        check data_age_secs > P42_MAX_STALE_SECS (default 300 s) and skip the
        veto gate when the feed is stale, so a Yahoo Finance outage cannot halt
        trading.

    Integration:
        Instantiated and task-launched by DataHub.start().
        DataHub exposes get_global_market_status() which proxies this class.
    """

    _YF_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    _PARAMS = "?interval=1m&range=12m"   # 12-minute bar range gives us 5-min delta

    def __init__(self) -> None:
        # Rolling price history: deque of (ts_epoch: float, close: float)
        # One entry per polling cycle per ticker.
        self._spy_history: deque = deque(maxlen=20)   # 20 × 30 s = 10 min
        self._dxy_history: deque = deque(maxlen=20)

        # ── [P42-CORR] BTC price history for SPY/BTC rolling correlation ──────
        # Fed externally by DataHub.feed_btc_price_for_correlation() each cycle.
        # maxlen matches _spy_history so aligned-window correlation is trivial.
        self._btc_history: deque = deque(maxlen=20)
        # Cached Pearson r ∈ [-1, +1].  Starts at 1.0 (assume correlated until
        # proven otherwise) so the SPY-drop-blocker is active on cold-start.
        self._spy_btc_corr: float = 1.0
        # ── [/P42-CORR] ──────────────────────────────────────────────────────

        # Computed 5-minute deltas — updated after each successful poll.
        self._spy_5m_pct: float = 0.0
        self._dxy_5m_pct: float = 0.0

        # Timestamp of the last *successful* data fetch (epoch seconds).
        self._last_success_ts: float = 0.0

        # Human-readable status string for dashboard display.
        self._last_status: str = "INITIALISING"
        self._running: bool = False

        # aiohttp session — created lazily in _poll_loop so it lives inside
        # the running event loop (avoids "no current event loop" errors).
        self._session: Optional[Any] = None

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_spy_5m_pct(self) -> float:
        """Return SPY 5-minute % price change. Negative = falling."""
        return self._spy_5m_pct

    def get_dxy_5m_pct(self) -> float:
        """Return DXY 5-minute % price change. Positive = rising dollar."""
        return self._dxy_5m_pct

    def get_data_age_secs(self) -> float:
        """Seconds since the last successful Yahoo Finance poll."""
        if self._last_success_ts == 0.0:
            return 9999.0
        return time.time() - self._last_success_ts

    # ── [P42-CORR] ────────────────────────────────────────────────────────────

    def record_btc_price(self, price: float) -> None:
        """
        [P42-CORR] Feed a live BTC price sample into the correlation window.

        Called by DataHub.feed_btc_price_for_correlation() each executor cycle
        whenever a fresh OKX/Binance BTC mid-price is available.  Each call
        triggers a Pearson-r recomputation against the aligned SPY return series.

        Parameters
        ----------
        price : float — current BTC/USDT mid-price (bid+ask)/2 or last trade.
                        Ignored if <= 0.
        """
        if price <= 0:
            return
        now = time.time()
        self._btc_history.append((now, float(price)))
        self._recompute_spy_btc_corr()

    def get_spy_btc_corr(self) -> float:
        """
        [P42-CORR] Return the cached rolling Pearson r between SPY and BTC.

        ∈ [-1, +1].  1.0 means perfectly positively correlated (SPY-blocker
        is fully active).  Values < P42_CORR_DECOUPLE_THRESHOLD (default 0.3)
        indicate that crypto is trading independently of equity markets and
        the SPY-drop-blocker should stand down.
        """
        return self._spy_btc_corr

    def _recompute_spy_btc_corr(self) -> None:
        """
        [P42-CORR] Pearson-r over the aligned SPY/BTC return windows.

        Algorithm
        ---------
        1. Extract log-returns from the most recent N samples of each series
           where N = min(len(spy), len(btc)).
        2. Require at least 5 matched return pairs — below that, leave the
           cached value unchanged (avoids thrashing on cold-start noise).
        3. Compute Pearson r without any scipy dependency (pure Python/stdlib).
        4. Clamp result to [-1, +1] and update self._spy_btc_corr.

        Fail-safe: any arithmetic error leaves the cached value unchanged so
        the gate defaults to the last known state rather than crashing.
        """
        try:
            spy_px = [p for _, p in self._spy_history]
            btc_px = [p for _, p in self._btc_history]
            n = min(len(spy_px), len(btc_px))
            if n < 6:
                return   # not enough data — keep prior cached value

            # Aligned slices: use the LAST n samples from each.
            spy_w = spy_px[-n:]
            btc_w = btc_px[-n:]

            # Log-returns: r_i = ln(p_i / p_{i-1})
            spy_r = [math.log(spy_w[i] / spy_w[i-1])
                     for i in range(1, n) if spy_w[i-1] > 0 and spy_w[i] > 0]
            btc_r = [math.log(btc_w[i] / btc_w[i-1])
                     for i in range(1, n) if btc_w[i-1] > 0 and btc_w[i] > 0]

            m = min(len(spy_r), len(btc_r))
            if m < 5:
                return

            spy_r = spy_r[-m:]
            btc_r = btc_r[-m:]

            # Pearson r — pure Python (no numpy/scipy dependency).
            mean_s = sum(spy_r) / m
            mean_b = sum(btc_r) / m
            cov    = sum((spy_r[i] - mean_s) * (btc_r[i] - mean_b) for i in range(m))
            std_s  = math.sqrt(sum((x - mean_s) ** 2 for x in spy_r))
            std_b  = math.sqrt(sum((x - mean_b) ** 2 for x in btc_r))

            if std_s < 1e-12 or std_b < 1e-12:
                return   # degenerate case (flat series) — keep prior value

            r = max(-1.0, min(1.0, cov / (std_s * std_b)))
            self._spy_btc_corr = round(r, 4)
            log.debug(
                "[P42-CORR] SPY/BTC rolling Pearson r=%.4f (n=%d return pairs)",
                self._spy_btc_corr, m,
            )
        except Exception as exc:
            log.debug("[P42-CORR] _recompute_spy_btc_corr error (non-fatal): %s", exc)

    # ── [/P42-CORR] ───────────────────────────────────────────────────────────

    def get_status_snapshot(self) -> dict:
        """[P42-SHADOW] Serialisable snapshot for DataHub.get_global_market_status()."""
        age = self.get_data_age_secs()
        return {
            "spy_5m_pct":        round(self._spy_5m_pct,  4),
            "dxy_5m_pct":        round(self._dxy_5m_pct,  4),
            "data_age_secs":     round(age, 1),
            "feed_stale":        age > P42_MAX_STALE_SECS,
            "status":            self._last_status,
            "spy_ticker":        P42_SPY_TICKER,
            "dxy_ticker":        P42_DXY_TICKER,
            "poll_interval_secs": P42_POLL_INTERVAL_SECS,
            "lookback_secs":     P42_LOOKBACK_SECS,
            # ── [P42-CORR] Rolling SPY/BTC Pearson r ─────────────────────────
            # < P42_CORR_DECOUPLE_THRESHOLD (0.3) → SPY-drop-blocker stands down
            "spy_btc_corr":      self._spy_btc_corr,
            "btc_samples":       len(self._btc_history),
        }

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Launch the background polling loop as an asyncio task."""
        if self._running:
            return
        self._running = True
        asyncio.create_task(self._poll_loop(), name="p42_global_market_sentinel")
        log.info(
            "[P42-SHADOW] GlobalMarketSentinel started. "
            "Polling SPY=%s DXY=%s every %.0fs (lookback=%.0fs).",
            P42_SPY_TICKER, P42_DXY_TICKER,
            P42_POLL_INTERVAL_SECS, P42_LOOKBACK_SECS,
        )

    def stop(self) -> None:
        self._running = False

    # ── Internal polling loop ──────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Main polling coroutine — runs until stop() is called."""
        try:
            import aiohttp as _aiohttp
            self._session = _aiohttp.ClientSession(
                timeout=_aiohttp.ClientTimeout(total=10.0),
                headers={"User-Agent": "Mozilla/5.0 PowerTraderAI/42"},
            )
        except Exception as exc:
            log.warning("[P42-SHADOW] aiohttp unavailable — sentinel disabled: %s", exc)
            self._last_status = f"DISABLED (aiohttp: {exc})"
            return

        log.debug("[P42-SHADOW] Poll loop started.")
        while self._running:
            try:
                await self._poll_ticker(P42_SPY_TICKER, self._spy_history, "SPY")
                await self._poll_ticker(P42_DXY_TICKER, self._dxy_history, "DXY")
                self._compute_deltas()
                self._last_success_ts = time.time()
                self._last_status = (
                    f"OK spy_5m={self._spy_5m_pct:+.3f}% "
                    f"dxy_5m={self._dxy_5m_pct:+.3f}%"
                )
                log.debug(
                    "[P42-SHADOW] Poll OK: SPY_5m=%+.3f%% DXY_5m=%+.3f%%",
                    self._spy_5m_pct, self._dxy_5m_pct,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._last_status = f"POLL_ERROR: {exc}"
                log.debug("[P42-SHADOW] Poll error (non-fatal): %s", exc)
            await asyncio.sleep(P42_POLL_INTERVAL_SECS)

        try:
            if self._session and not self._session.closed:
                await self._session.close()
        except Exception as _exc:
            log.debug("[DATA_HUB] cleanup: %s", _exc)
        log.debug("[P42-SHADOW] Poll loop exited.")

    async def _poll_ticker(
        self, ticker: str, history: deque, label: str,
    ) -> None:
        """Fetch the latest minute-bar for `ticker` and append to `history`."""
        url = self._YF_URL.format(ticker=ticker) + self._PARAMS
        try:
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    log.debug(
                        "[P42-SHADOW] %s HTTP %d — skipping.", label, resp.status,
                    )
                    return
                raw = await resp.json(content_type=None)
        except Exception as exc:
            log.debug("[P42-SHADOW] %s fetch error: %s", label, exc)
            return

        try:
            result  = raw["chart"]["result"][0]
            meta    = result.get("meta", {})
            # Use regularMarketPrice as the most recent price.
            current = float(meta.get("regularMarketPrice") or 0)
            ts_now  = float(meta.get("regularMarketTime") or time.time())
            if current <= 0:
                return
            history.append((ts_now, current))
            log.debug("[P42-SHADOW] %s current=%.4f ts=%d", label, current, int(ts_now))
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            log.debug("[P42-SHADOW] %s parse error: %s", label, exc)

    def _compute_deltas(self) -> None:
        """
        Recompute 5-minute percentage deltas from the rolling price history.

        Uses the oldest observation within the last P42_LOOKBACK_SECS window
        as the baseline, and the most recent observation as the current price.
        This gives a true 5-min change without bias from polling timing jitter.
        """
        self._spy_5m_pct = self._delta(self._spy_history)
        self._dxy_5m_pct = self._delta(self._dxy_history)

    @staticmethod
    def _delta(history: deque) -> float:
        """Return (newest - oldest_within_window) / oldest × 100, or 0.0."""
        if len(history) < 2:
            return 0.0
        now = time.time()
        cutoff = now - P42_LOOKBACK_SECS
        # Find the oldest entry still within the lookback window.
        baseline_px: Optional[float] = None
        for ts, px in history:
            if ts >= cutoff:
                baseline_px = px
                break   # history is time-ordered oldest→newest
        if baseline_px is None or baseline_px <= 0:
            return 0.0
        newest_px = history[-1][1]
        if newest_px <= 0:
            return 0.0
        return (newest_px - baseline_px) / baseline_px * 100.0


# ══════════════════════════════════════════════════════════════════════════════
# [P15-3] Coinbase CDP Credential Validator
# ══════════════════════════════════════════════════════════════════════════════

class CoinbaseCDPCredentials:
    """
    [P15-3] Validates Coinbase Advanced Trade V3 credentials at startup.

    PEM handling:
      COINBASE_API_SECRET in .env is stored as a single line with literal \\n.
      reconstruct_pem() converts these back to real newlines and optionally
      restores missing PEM headers.

    Degrades gracefully:
      • cryptography not installed → skips PEM parse, marks valid if non-empty.
      • Bad PEM → logs error, marks invalid; oracle runs unauthenticated.
      • Missing keys → marks invalid; oracle runs unauthenticated (public ch).

    ⚠  READ-ONLY validator. Does not place orders.
    """

    def __init__(
        self,
        api_key:        str = COINBASE_API_KEY,
        api_secret_raw: str = COINBASE_API_SECRET_RAW,
    ):
        self.api_key = api_key.strip()
        self.pem_key = self._reconstruct_pem(api_secret_raw)
        self._valid  = self._validate()

    @staticmethod
    def _reconstruct_pem(raw: str) -> str:
        if not raw:
            return ""
        pem = raw.strip().replace("\\n", "\n")
        if "-----BEGIN" not in pem:
            pem = (
                "-----BEGIN EC PRIVATE KEY-----\n"
                + pem.strip()
                + "\n-----END EC PRIVATE KEY-----"
            )
        return pem

    def _validate(self) -> bool:
        if not self.api_key:
            log.info(
                "[P15-3] COINBASE_API_KEY not set — "
                "Coinbase Oracle will run unauthenticated (public channels only)."
            )
            return False
        if not self.pem_key or "BEGIN" not in self.pem_key:
            log.warning(
                "[P15-3] COINBASE_API_SECRET appears malformed — "
                "Coinbase Oracle will run unauthenticated."
            )
            return False
        try:
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            load_pem_private_key(self.pem_key.encode(), password=None)
            log.info(
                "[P15-3] Coinbase CDP credentials validated OK. key_preview=%s…",
                self.api_key[:32],
            )
            return True
        except ImportError:
            log.warning(
                "[P15-3] 'cryptography' not installed — skipping PEM parse validation. "
                "Run: pip install cryptography"
            )
            return bool(self.pem_key)
        except Exception as exc:
            log.error("[P15-3] PEM validation failed: %s", exc)
            return False

    def is_valid(self) -> bool:
        return self._valid

    def status_snapshot(self) -> dict:
        return {
            "api_key_set":    bool(self.api_key),
            "pem_key_set":    bool(self.pem_key),
            "credentials_ok": self._valid,
            "key_preview":    (self.api_key[:24] + "…") if self.api_key else "",
        }


# ══════════════════════════════════════════════════════════════════════════════
# Core data structures (Phases 1-12, fully preserved)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Tick:
    symbol: str
    bid:    float
    ask:    float
    last:   float
    ts:     float = field(default_factory=time.time)


# [CI-1-A] Candle is a @dataclass — verified, no structural change required.
@dataclass
class Candle:
    symbol:    str
    tf:        str
    ts:        int
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float
    confirmed: bool = False


@dataclass
class AccountSnapshot:
    ts:            float
    total_equity:  float
    buying_power:  float
    margin_ratio:  float
    positions_raw: list


@dataclass
class InstrumentMeta:
    inst_id:   str
    min_sz:    float
    sz_inst:   float
    tick_sz:   float
    inst_type: str
    ct_val:    float = 0.0
    # [P36.1-PRICEGUARD] OKX exchange-mandated price bands.
    buyLmt:    float = 0.0
    sellLmt:   float = 0.0


@dataclass
class OrderBook:
    symbol: str
    bids:   List[Tuple[float, float]]
    asks:   List[Tuple[float, float]]
    ts:     float = field(default_factory=time.time)

    def total_bid_volume(self, levels: int = 3) -> float:
        return sum(sz for _, sz in self.bids[:levels])

    def total_ask_volume(self, levels: int = 3) -> float:
        return sum(sz for _, sz in self.asks[:levels])

    def weighted_ask_price(self, qty: float, levels: int = 3) -> float:
        remaining, cost = qty, 0.0
        for px, sz in self.asks[:levels]:
            fill = min(sz, remaining)
            cost += fill * px
            remaining -= fill
            if remaining <= 0:
                break
        return 0.0 if remaining > 0 else cost / qty

    def weighted_bid_price(self, qty: float, levels: int = 3) -> float:
        remaining, cost = qty, 0.0
        for px, sz in self.bids[:levels]:
            fill = min(sz, remaining)
            cost += fill * px
            remaining -= fill
            if remaining <= 0:
                break
        return 0.0 if remaining > 0 else cost / qty

    def slippage_pct(self, qty: float, side: str, levels: int = 3) -> float:
        if not self.asks or not self.bids:
            return float("inf")
        if side == "buy":
            ref  = self.asks[0][0]
            vwap = self.weighted_ask_price(qty, levels)
        else:
            ref  = self.bids[0][0]
            vwap = self.weighted_bid_price(qty, levels)
        if vwap <= 0 or ref <= 0:
            return float("inf")
        return abs(vwap - ref) / ref * 100.0

    def obi(self, levels: int = OBI_LEVELS) -> float:
        """[P12-1] Order Book Imbalance ∈ [-1, +1]."""
        bid_sz = sum(sz for _, sz in self.bids[:levels])
        ask_sz = sum(sz for _, sz in self.asks[:levels])
        total  = bid_sz + ask_sz
        if total <= 0:
            return 0.0
        return (bid_sz - ask_sz) / total

    def best_passive_wall(
        self, side: str, tick_sz: float, levels: int = 3,
    ) -> Optional[Tuple[float, float]]:
        """[P12-3] Largest passive wall within top `levels` for pennying."""
        if tick_sz <= 0:
            return None
        sides = self.asks[:levels] if side == "buy" else self.bids[:levels]
        if not sides:
            return None
        return max(sides, key=lambda x: x[1])


# ── [P38-OFI] Predictive Order Flow Imbalance Monitor ────────────────────────

class OBIMonitor:
    """
    [P38-OFI] Predictive Order Book Imbalance — Order Flow Imbalance tracker.

    Standard OBI (P12-1) is a single-snapshot ratio of bid vs ask volume.
    OFI is the *delta* between successive snapshots — it measures whether
    passive walls are being **stacked** or **pulled** between ticks.

    Mathematical model
    ──────────────────
    For each depth level l in [0, P38_OFI_LEVELS):

        bid_delta[l] = curr_bid_size[price] − prev_bid_size[price]
        ask_delta[l] = curr_ask_size[price] − prev_ask_size[price]

    Prices are matched across snapshots.  A price that appears in the current
    snapshot but not the previous one contributes its full size as a positive
    delta (wall appearing).  A price that existed in the previous snapshot but
    not the current one contributes −prev_size (wall disappearing / pulled).

    Raw OFI = Σ bid_delta − Σ ask_delta (positive = buy-side pressure)
    Normalized OFI = raw_ofi / total_curr_depth  ∈ [−1.0, +1.0]

    Wall-Pull Detection
    ───────────────────
    A "wall pull" is when a passive level with size ≥ P38_WALL_MIN_VOL
    disappears entirely in a single tick.  Pulled bid walls before a long
    entry and pulled ask walls before a short entry are manipulation red flags
    (fake support / resistance removed to bait directional entries).

    Thread / coroutine safety
    ─────────────────────────
    update() is called from DataHub._on_book() inside the async event loop.
    get_ofi_snapshot() is called from executor._cycle(), also async.
    No concurrent writes → no lock required (single asyncio event loop).
    """

    def __init__(self) -> None:
        # Rolling deque of recent OrderBook snapshots.
        # maxlen = P38_OFI_WINDOW + 1 so we always have prev + curr available.
        self._snapshots:      deque = deque(maxlen=P38_OFI_WINDOW + 1)
        self._last_ofi:       float = 0.0
        self._bid_wall_pulled: bool = False
        self._ask_wall_pulled: bool = False
        self._update_count:   int   = 0

    # ── Public interface ───────────────────────────────────────────────────────

    def update(self, book: "OrderBook") -> None:
        """
        Push a new OrderBook snapshot and recompute OFI against the previous.
        No-op until at least two snapshots have been received.
        """
        if self._snapshots:
            prev = self._snapshots[-1]
            try:
                self._last_ofi = self._compute_ofi(prev, book)
                self._detect_wall_pulls(prev, book)
            except Exception as exc:
                log.debug("[P38-OFI] OBIMonitor.update error: %s", exc)
        self._snapshots.append(book)
        self._update_count += 1

    def get_ofi_snapshot(self) -> Tuple[float, bool, bool]:
        """
        Return the current OFI state as a three-tuple:
            (ofi_score, bid_wall_pulled, ask_wall_pulled)

        ofi_score ∈ [−1.0, +1.0]:
            > 0  → net buying pressure (bid walls building / ask walls thinning)
            < 0  → net selling pressure (ask walls building / bid walls thinning)

        bid_wall_pulled → True when a large bid level vanished in the last tick.
        ask_wall_pulled → True when a large ask level vanished in the last tick.
        """
        return self._last_ofi, self._bid_wall_pulled, self._ask_wall_pulled

    @property
    def ready(self) -> bool:
        """True once at least two snapshots have been processed."""
        return self._update_count >= 2

    # ── Private ───────────────────────────────────────────────────────────────

    def _compute_ofi(self, prev: "OrderBook", curr: "OrderBook") -> float:
        """Price-matched OFI across top P38_OFI_LEVELS depth levels."""
        prev_bids: Dict[float, float] = {
            px: sz for px, sz in prev.bids[:P38_OFI_LEVELS]
        }
        curr_bids: Dict[float, float] = {
            px: sz for px, sz in curr.bids[:P38_OFI_LEVELS]
        }
        prev_asks: Dict[float, float] = {
            px: sz for px, sz in prev.asks[:P38_OFI_LEVELS]
        }
        curr_asks: Dict[float, float] = {
            px: sz for px, sz in curr.asks[:P38_OFI_LEVELS]
        }

        # Bid delta: positive when bid-side volume grows (buying pressure)
        all_bid_px = set(prev_bids) | set(curr_bids)
        bid_delta  = sum(
            curr_bids.get(px, 0.0) - prev_bids.get(px, 0.0)
            for px in all_bid_px
        )

        # Ask delta: positive when ask-side volume grows (selling pressure)
        all_ask_px = set(prev_asks) | set(curr_asks)
        ask_delta  = sum(
            curr_asks.get(px, 0.0) - prev_asks.get(px, 0.0)
            for px in all_ask_px
        )

        # Normalise by current total depth so cross-symbol values are comparable
        total_depth = (
            sum(sz for _, sz in curr.bids[:P38_OFI_LEVELS])
            + sum(sz for _, sz in curr.asks[:P38_OFI_LEVELS])
        )
        if total_depth <= 0.0:
            return 0.0

        raw = bid_delta - ask_delta
        return max(-1.0, min(1.0, raw / total_depth))

    def _detect_wall_pulls(self, prev: "OrderBook", curr: "OrderBook") -> None:
        """
        Detect disappearance of large passive walls between snapshots.
        A level is considered a wall when its previous size ≥ P38_WALL_MIN_VOL.
        """
        curr_bid_px: set = {px for px, _ in curr.bids[:P38_OFI_LEVELS]}
        curr_ask_px: set = {px for px, _ in curr.asks[:P38_OFI_LEVELS]}

        self._bid_wall_pulled = any(
            sz >= P38_WALL_MIN_VOL and px not in curr_bid_px
            for px, sz in prev.bids[:P38_OFI_LEVELS]
        )
        self._ask_wall_pulled = any(
            sz >= P38_WALL_MIN_VOL and px not in curr_ask_px
            for px, sz in prev.asks[:P38_OFI_LEVELS]
        )


# ── [P12-2] Tape Buffer ────────────────────────────────────────────────────────

@dataclass
class TapeEvent:
    ts:    float
    side:  str
    qty:   float
    price: float
    usd:   float


class TapeBuffer:
    def __init__(
        self,
        buffer_secs:  float = TAPE_BUFFER_SECS,
        max_events:   int   = TAPE_MAX_EVENTS,
        velocity_win: float = TAPE_VELOCITY_WINDOW_SECS,
    ):
        self._buffer_secs  = buffer_secs
        self._velocity_win = velocity_win
        self._events: Deque[TapeEvent] = deque(maxlen=max_events)
        self._lock = asyncio.Lock()

        # ── [P37-VPIN] Volume-Clock bucket state ─────────────────────────────
        self._vpin_buy_vol:   float = 0.0
        self._vpin_sell_vol:  float = 0.0
        self._vpin_total_vol: float = 0.0
        self._vpin_buckets: deque = deque(maxlen=_P37_VPIN_WINDOW)
        self._vpin_toxicity: float = 0.0

    async def add(self, side: str, qty: float, price: float) -> None:
        async with self._lock:
            self._events.append(
                TapeEvent(ts=time.time(), side=side, qty=qty,
                          price=price, usd=qty * price)
            )
            try:
                _qty = max(0.0, float(qty))
                if side == "buy":
                    self._vpin_buy_vol  += _qty
                else:
                    self._vpin_sell_vol += _qty
                self._vpin_total_vol += _qty

                if self._vpin_total_vol >= P37_VPIN_BUCKET_SIZE:
                    _total = self._vpin_total_vol
                    _buy   = self._vpin_buy_vol
                    _sell  = self._vpin_sell_vol

                    if _total > 0.0:
                        _vpin = abs(_buy - _sell) / _total
                        _vpin = max(0.0, min(1.0, _vpin))
                    else:
                        _vpin = 0.0

                    self._vpin_buckets.append(_vpin)
                    self._vpin_toxicity = self._compute_toxicity_score(_vpin)

                    self._vpin_buy_vol   = 0.0
                    self._vpin_sell_vol  = 0.0
                    self._vpin_total_vol = 0.0
            except Exception as _exc:
                log.warning("[DATA_HUB] suppressed: %s", _exc)

    def _compute_toxicity_score(self, latest_vpin: float) -> float:
        """[P37-VPIN] Derive ToxicityScore ∈ [0.0, 1.0] from empirical CDF."""
        history = list(self._vpin_buckets)
        if len(history) < 2:
            return 0.0
        try:
            _vpin_c = max(0.0, min(1.0, latest_vpin))
            _below  = sum(1 for v in history if v <= _vpin_c)
            _score  = _below / len(history)
            return round(max(0.0, min(1.0, _score)), 4)
        except Exception:
            return 0.0

    def get_flow_toxicity(self) -> float:
        """[P37-VPIN] Return the current ToxicityScore ∈ [0.0, 1.0]."""
        return self._vpin_toxicity

    def _recent(self, window_secs: float) -> List[TapeEvent]:
        cutoff = time.time() - window_secs
        result = []
        for ev in reversed(self._events):
            if ev.ts < cutoff:
                break
            result.append(ev)
        return result

    async def trade_velocity(self) -> float:
        async with self._lock:
            recent = self._recent(self._velocity_win)
        return len(recent) / self._velocity_win if recent else 0.0

    async def volume_cluster(self, side: str, window_secs: float) -> float:
        async with self._lock:
            recent = self._recent(window_secs)
        return sum(ev.usd for ev in recent if ev.side == side)

    async def is_liquidity_sweep(
        self,
        direction:   str,
        dip_px:      float,
        current_px:  float,
        dip_pct:     float = SWEEP_DIP_PCT,
        buy_vol_usd: float = SWEEP_BUY_VOL_USD,
        window_secs: float = SWEEP_WINDOW_SECS,
    ) -> bool:
        if dip_px <= 0 or current_px <= 0:
            return False
        if direction == "long":
            if not (dip_px <= current_px * (1 - dip_pct / 100)):
                return False
            buy_vol = await self.volume_cluster("buy", window_secs)
            result  = buy_vol >= buy_vol_usd
            if result:
                log.info(
                    "[P12-2] Sweep (LONG) %s: buy_vol=$%.0f >= $%.0f",
                    direction, buy_vol, buy_vol_usd,
                )
            return result
        elif direction == "short":
            if not (dip_px >= current_px * (1 + dip_pct / 100)):
                return False
            sell_vol = await self.volume_cluster("sell", window_secs)
            result   = sell_vol >= buy_vol_usd
            if result:
                log.info(
                    "[P12-2] Sweep (SHORT): sell_vol=$%.0f >= $%.0f",
                    sell_vol, buy_vol_usd,
                )
            return result
        return False

    async def snapshot_stats(self) -> dict:
        async with self._lock:
            r5  = self._recent(5.0)
            r60 = self._recent(60.0)
        return {
            "velocity_tps":  round(len(r5) / 5.0, 2),
            "buy_vol_60s":   round(sum(ev.usd for ev in r60 if ev.side == "buy"),  2),
            "sell_vol_60s":  round(sum(ev.usd for ev in r60 if ev.side == "sell"), 2),
            "total_events":  len(self._events),
        }


# ── [P5-1] Sentiment ──────────────────────────────────────────────────────────

@dataclass
class SentimentSnapshot:
    funding_rate:  float
    net_liq_delta: float
    long_liq_usd:  float
    short_liq_usd: float
    ts:            float = field(default_factory=time.time)


class SentimentBuffer:
    def __init__(self, window_secs: float = SENTIMENT_WINDOW_SECS):
        self._window       = window_secs
        self._funding_rate = 0.0
        self._liq_events:  Deque[Tuple[float, float, str]] = deque(maxlen=10_000)
        self._lock         = asyncio.Lock()
        self._cancelling_symbols = set()

    async def cycle_oracle_cancel(self, symbol: str):
        if symbol in self._cancelling_symbols:
            return
        self._cancelling_symbols.add(symbol)
        try:
            log.info(f"🚨 [P15] WHALE NUKE: Cancelling all BUY orders for {symbol}")
            await self.rest.cancel_all_orders(symbol, side="buy")
            asyncio.create_task(self._clear_nuke_lock(symbol, 30))
        except Exception as e:
            log.error(f"Nuke failed for {symbol}: {e}")
            self._cancelling_symbols.discard(symbol)

    async def _clear_nuke_lock(self, symbol: str, delay: float):
        await asyncio.sleep(delay)
        self._cancelling_symbols.discard(symbol)

    async def update_funding(self, rate: float) -> None:
        async with self._lock:
            self._funding_rate = rate

    async def add_liquidation(self, usd_val: float, side: str) -> None:
        async with self._lock:
            self._liq_events.append((time.time(), usd_val, side))

    async def snapshot(self) -> SentimentSnapshot:
        async with self._lock:
            now    = time.time()
            cutoff = now - self._window
            ll  = sum(v for ts, v, s in self._liq_events if ts >= cutoff and s == "long")
            sl  = sum(v for ts, v, s in self._liq_events if ts >= cutoff and s == "short")
            while self._liq_events and self._liq_events[0][0] < cutoff:
                self._liq_events.popleft()
            return SentimentSnapshot(
                funding_rate=self._funding_rate,
                net_liq_delta=ll - sl,
                long_liq_usd=ll, short_liq_usd=sl, ts=now,
            )


# ── [P4-1] Volatility Guard ────────────────────────────────────────────────────

class VolatilityGuard:
    def __init__(
        self,
        drop_pct:    float = FLASH_CRASH_DROP_PCT,
        window_secs: float = FLASH_CRASH_WINDOW,
        pause_secs:  float = EMERGENCY_PAUSE_SECS,
    ):
        self._drop_pct   = drop_pct
        self._window     = window_secs
        self._pause_secs = pause_secs
        self._price_hist: Dict[str, Deque[Tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=500)
        )
        self.emergency_pause:     bool  = False
        self._pause_triggered_at: float = 0.0
        self._triggered_by:       str   = ""

    def update(self, symbol: str, price: float) -> bool:
        try:
            if price <= 0:
                return False
            now  = time.time()
            if self.emergency_pause and (now - self._pause_triggered_at) >= self._pause_secs:
                log.info(
                    "VolatilityGuard: pause RESET after %.0f s (triggered by %s)",
                    self._pause_secs, self._triggered_by,
                )
                self.emergency_pause     = False
                self._pause_triggered_at = 0.0
                self._triggered_by       = ""

            hist = self._price_hist[symbol]
            hist.append((now, price))
            cutoff = now - self._window
            while hist and hist[0][0] < cutoff:
                hist.popleft()
            if len(hist) < 2:
                return False

            oldest_px = hist[0][1]
            drop_pct  = (oldest_px - price) / oldest_px * 100.0
            if drop_pct >= self._drop_pct and not self.emergency_pause:
                self.emergency_pause     = True
                self._pause_triggered_at = now
                self._triggered_by       = symbol
                log.critical(
                    "🚨 FLASH CRASH: %s dropped %.2f%% in <%.0f s. "
                    "emergency_pause=True for %.0f s.",
                    symbol, drop_pct, self._window, self._pause_secs,
                )
                return True
        except Exception as exc:
            log.error("VolatilityGuard.update error: %s", exc)
        return False

    @property
    def pause_remaining_secs(self) -> float:
        if not self.emergency_pause:
            return 0.0
        return max(0.0, self._pause_secs - (time.time() - self._pause_triggered_at))


# ── Instrument Cache ───────────────────────────────────────────────────────────

class InstrumentCache:
    def __init__(self, rest: OKXRestClient):
        self._rest        = rest
        self._cache:      Dict[str, InstrumentMeta] = {}
        self._lock        = asyncio.Lock()
        self._fetched_at: Dict[str, float] = {}
        self._price_limits: Dict[str, Tuple[float, float]] = {}
        self._price_limits_lock: asyncio.Lock = asyncio.Lock()

    @staticmethod
    def _pf(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    async def _fetch(self, inst_type: str) -> None:
        now  = time.time()
        last = self._fetched_at.get(inst_type, 0.0)
        if now - last < INSTRUMENT_CACHE_TTL:
            return
        log.info("InstrumentCache: fetching %s specs from OKX…", inst_type)
        try:
            d    = await self._rest.get(
                "/api/v5/public/instruments",
                params={"instType": inst_type}, auth=False,
            )
            rows = d.get("data") or []
            async with self._lock:
                for row in rows:
                    iid = row.get("instId", "")
                    if not iid:
                        continue
                    self._cache[iid] = InstrumentMeta(
                        inst_id   = iid,
                        min_sz    = self._pf(row.get("minSz"),  0.0),
                        sz_inst   = self._pf(row.get("lotSz"),  0.0),
                        tick_sz   = self._pf(row.get("tickSz"), 0.0),
                        inst_type = inst_type,
                        ct_val    = self._pf(row.get("ctVal"),  0.0),
                    )
                self._fetched_at[inst_type] = now
            log.info("InstrumentCache: cached %d %s instruments.", len(rows), inst_type)
        except Exception as exc:
            log.error("InstrumentCache: failed to fetch %s: %s", inst_type, exc)

    async def warm(self, symbols: List[str], include_swap: bool = True) -> None:
        await self._fetch("SPOT")
        if include_swap:
            await self._fetch("SWAP")

    async def get_instrument_info(
        self, symbol: str, swap: bool = False,
    ) -> Optional[InstrumentMeta]:
        inst_id   = f"{symbol.upper()}-USDT" + ("-SWAP" if swap else "")
        inst_type = "SWAP" if swap else "SPOT"
        async with self._lock:
            if inst_id in self._cache:
                return self._cache[inst_id]
        await self._fetch(inst_type)
        async with self._lock:
            meta = self._cache.get(inst_id)
            if meta is None:
                log.warning("InstrumentCache: no metadata for %s.", inst_id)
            return meta

    async def refresh_price_limits(
        self,
        symbols: List[str],
        include_swap: bool = True,
    ) -> None:
        """[P36.1-PRICEGUARD] Fetch OKX price bands (buyLmt/sellLmt)."""
        inst_types = ["SPOT"]
        if include_swap:
            inst_types.append("SWAP")

        for inst_type in inst_types:
            try:
                d    = await self._rest.get(
                    "/api/v5/market/tickers",
                    params={"instType": inst_type}, auth=False,
                )
                rows = d.get("data") or []
                updated = 0
                async with self._price_limits_lock:
                    for row in rows:
                        iid     = row.get("instId", "")
                        buy_lmt = float(row.get("buyLmt")  or 0)
                        sell_lmt= float(row.get("sellLmt") or 0)
                        if iid and (buy_lmt > 0 or sell_lmt > 0):
                            self._price_limits[iid] = (buy_lmt, sell_lmt)
                            meta = self._cache.get(iid)
                            if meta is not None:
                                meta.buyLmt  = buy_lmt
                                meta.sellLmt = sell_lmt
                            updated += 1
                log.info(
                    "[P36.1-PRICEGUARD] refresh_price_limits: "
                    "updated %d %s price limit entries.", updated, inst_type,
                )
            except Exception as exc:
                log.warning(
                    "[P36.1-PRICEGUARD] refresh_price_limits %s failed: %s",
                    inst_type, exc,
                )

    def get_price_limits(self, inst_id: str) -> Tuple[float, float]:
        """[P36.1-PRICEGUARD] Return cached (buyLmt, sellLmt); (0.0, 0.0) if unknown."""
        return self._price_limits.get(inst_id, (0.0, 0.0))

    @staticmethod
    def _decimals(step: float) -> int:
        s = f"{step:.10f}".rstrip("0")
        return len(s.split(".")[1]) if "." in s else 0

    @staticmethod
    def round_to_lot(sz: float, meta: InstrumentMeta) -> float:
        if meta.sz_inst <= 0:
            return sz
        dec    = InstrumentCache._decimals(meta.sz_inst)
        factor = 10 ** dec
        return round(math.floor(sz * factor / round(meta.sz_inst * factor)) * meta.sz_inst, dec)

    @staticmethod
    def clamp_to_min(sz: float, meta: InstrumentMeta, tag: str = "") -> float:
        if meta.min_sz > 0 and sz < meta.min_sz:
            log.warning("[%s] qty %.8f < minSz %.8f — bumping.", tag, sz, meta.min_sz)
            return meta.min_sz
        return sz

    @staticmethod
    def round_price(price: float, meta: InstrumentMeta) -> float:
        if meta.tick_sz <= 0:
            return price
        dec = InstrumentCache._decimals(meta.tick_sz)
        return round(round(price / meta.tick_sz) * meta.tick_sz, dec)

    def apply_constraints(self, sz: float, meta: InstrumentMeta, tag: str = "") -> float:
        sz = self.clamp_to_min(sz, meta, tag)
        sz = self.round_to_lot(sz, meta)
        sz = self.clamp_to_min(sz, meta, tag + "_post")
        return sz


# ── Candle aggregation ─────────────────────────────────────────────────────────

def aggregate_candles(
    candles_1h: List[Candle], period_hours: int, symbol: str,
) -> List[Candle]:
    if not candles_1h:
        return []
    period_secs = period_hours * 3600
    tf_label    = f"{period_hours}hour"
    buckets: Dict[int, List[Candle]] = defaultdict(list)
    for c in candles_1h:
        buckets[(c.ts // period_secs) * period_secs].append(c)
    result: List[Candle] = []
    for bts in sorted(buckets):
        grp = sorted(buckets[bts], key=lambda c: c.ts)
        result.append(Candle(
            symbol=symbol, tf=tf_label, ts=bts,
            open=grp[0].open, high=max(c.high for c in grp),
            low=min(c.low for c in grp), close=grp[-1].close,
            volume=sum(c.volume for c in grp),
            confirmed=len(grp) >= period_hours,
        ))
    return result


# ── Candle Buffer ──────────────────────────────────────────────────────────────

class CandleBuffer:
    def __init__(self, maxlen: int = 2000):
        self._confirmed: List[Candle] = []
        self._live: Optional[Candle]  = None
        self._maxlen                  = maxlen
        self._lock                    = asyncio.Lock()

    async def update(self, candle: Candle) -> None:
        async with self._lock:
            if candle.confirmed:
                if self._live and self._live.ts == candle.ts:
                    self._live = None
                if self._confirmed and self._confirmed[0].ts == candle.ts:
                    self._confirmed[0] = candle
                else:
                    self._confirmed.insert(0, candle)
                if len(self._confirmed) > self._maxlen:
                    self._confirmed = self._confirmed[:self._maxlen]
            else:
                self._live = candle

    async def snapshot(self, n: int = 500) -> List[Candle]:
        async with self._lock:
            result = []
            if self._live is not None:
                result.append(self._live)
            result.extend(self._confirmed[:n - len(result)])
            return result

    async def confirmed_list(self) -> List[Candle]:
        async with self._lock:
            return list(self._confirmed)

    async def bulk_load(self, candles: List[Candle]) -> None:
        async with self._lock:
            self._confirmed = sorted(candles, key=lambda c: c.ts, reverse=True)
            if len(self._confirmed) > self._maxlen:
                self._confirmed = self._confirmed[:self._maxlen]


# ── SQLite persistence ─────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
    symbol TEXT NOT NULL, tf TEXT NOT NULL, ts INTEGER NOT NULL,
    open REAL, high REAL, low REAL, close REAL, volume REAL,
    PRIMARY KEY (symbol, tf, ts)
);
CREATE INDEX IF NOT EXISTS idx_candles_lookup ON candles(symbol, tf, ts DESC);
CREATE TABLE IF NOT EXISTS trades (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           INTEGER,
    symbol       TEXT, side TEXT, qty REAL, price REAL,
    cost_basis   REAL, pnl_pct REAL, realized_usd REAL,
    tag          TEXT, order_id TEXT UNIQUE, inst_type TEXT
);
CREATE TABLE IF NOT EXISTS account_snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           INTEGER,
    total_equity REAL, buying_power REAL, margin_ratio REAL
);
"""

# [PHASE1] Additive-only migration SQL.
# Adds trade_id and exit_reason_type columns to an existing trades table.
# IF NOT EXISTS guards make this safe to run on every boot — no-op if already present.
# Old rows receive NULL for both columns; no data is rewritten.
_PHASE1_MIGRATION = """
ALTER TABLE trades ADD COLUMN trade_id             TEXT;
ALTER TABLE trades ADD COLUMN exit_reason_type     TEXT;
ALTER TABLE trades ADD COLUMN exit_reason_category TEXT;
"""

# [PHASE4] Additive-only migration SQL.
# Adds is_close_leg column: 1 = canonical close row, 0 = open/add/noise row.
# DEFAULT 0 means historical rows are treated as non-close, which is safe
# because KPI filters select is_close_leg = 1 (opt-in truth).
# Old rows are never rewritten; new writes populate the column explicitly.
_PHASE4_MIGRATION = """
ALTER TABLE trades ADD COLUMN is_close_leg INTEGER DEFAULT 0;
"""

# [PHASE6] Additive-only migration SQL — root-cause loss diagnostics.
# Adds three diagnostic columns to close-leg rows only:
#   entry_confidence — Signal.confidence at position entry (NULL on historical rows)
#   entry_kelly_f    — Signal.kelly_f at position entry (NULL on historical rows)
#   hold_time_secs   — seconds held from entry_ts to close fill (NULL on historical rows)
# All three default to NULL so historical rows remain valid; KPI filters are
# unaffected (they key on is_close_leg, not on these columns).
_PHASE6_MIGRATION = """
ALTER TABLE trades ADD COLUMN entry_confidence REAL;
ALTER TABLE trades ADD COLUMN entry_kelly_f    REAL;
ALTER TABLE trades ADD COLUMN hold_time_secs   REAL;
"""

# [PHASE10] Additive-only migration SQL — execution friction persistence.
# Adds two friction columns to close-leg rows only:
#   close_slippage_bps  — signed; positive = adverse (fill worse than expected bid/ask).
#                         NULL for pre-Phase-10 rows, pre-Phase-15 HEDGE_TRIM rows,
#                         or any row where tick data was unavailable at close time.
#                         Phase 15 added tick-based friction for HEDGE_TRIM and
#                         liquidation exits; post-Phase-15 rows may be non-NULL.
#   spread_at_close_bps — raw bid-ask spread in bps at close order submission time.
#                         NULL where tick was unavailable at close time.
# Both default to NULL; historical rows are never rewritten.  KPI filters key on
# is_close_leg=1 and are unaffected by the new columns.
_PHASE10_MIGRATION = """
ALTER TABLE trades ADD COLUMN close_slippage_bps  REAL;
ALTER TABLE trades ADD COLUMN spread_at_close_bps REAL;
"""

# [POST-ROADMAP-DT] Additive-only migration — entry-context signal fields.
# Adds three columns to close-leg rows only:
#   entry_regime    — Signal.regime at position entry (e.g. "bull", "bear", "chop")
#   entry_direction — Signal.direction at position entry ("long" | "short")
#   entry_z_score   — Signal.z_score at position entry (float)
# All three default to NULL; historical rows are never rewritten.  KPI filters
# key on is_close_leg=1 and are unaffected by the new columns.
_DT_MIGRATION = """
ALTER TABLE trades ADD COLUMN entry_regime    TEXT;
ALTER TABLE trades ADD COLUMN entry_direction TEXT;
ALTER TABLE trades ADD COLUMN entry_z_score   REAL;
"""

# [TRACK16-DT] Additive-only migration — add source_path column to
# decision_trace.  Distinguishes "entry" (_maybe_enter) from "express"
# (trigger_atomic_express_trade) records.
#   source_path TEXT — "entry" | "express"; NULL on pre-Track-16 rows.
# Same duplicate-column-ignore pattern used by all prior migrations.
_TRACK16_DT_MIGRATION = """
ALTER TABLE decision_trace ADD COLUMN source_path TEXT;
"""

# [TRACK17-SCHEMA] Additive migration: pnl_pct and hold_time_secs columns for
# close and DCA lifecycle trace records.  NULL on all pre-Track-17 rows.
_TRACK17_DT_MIGRATION = """
ALTER TABLE decision_trace ADD COLUMN pnl_pct REAL;
ALTER TABLE decision_trace ADD COLUMN hold_time_secs REAL;
"""

# [TRACK10-DT] New decision_trace table — cross-session history for all
# _maybe_enter admission outcomes.  This is a NEW table (not an ALTER TABLE),
# so CREATE TABLE IF NOT EXISTS is fully idempotent on every boot — no
# duplicate-column risk, no data loss on existing installations.
#
# Schema rationale:
#   ts, symbol, direction, regime, confidence, z_score, kelly_f — core signal
#     snapshot fields, captured before any signal mutation.
#   mtf_aligned, sniper_boost, oracle_boosted — boolean flags (stored as INTEGER
#     0/1 for SQLite compatibility).
#   whale_mult — float multiplier from oracle signal.
#   outcome — "ADMITTED" | "BLOCKED" | "SHADOW"
#   gate_name — gate that blocked, or "PASS" / "SHADOW" if admitted.
#   reason — brief human-readable detail (max ~120 chars in executor).
#   llm_verdict — LLM verdict string if consulted, else empty string.
#   p32_p_success — VetoArbitrator p_success when computed, else NULL.
#   sizing_usd — final notional if admitted, else NULL.
#   risk_off, p39_snipe — boolean modifier flags (INTEGER 0/1).
#   p39_size_boost, conviction_mult — float modifier values.
#     Flattened from the "modifiers" dict so all fields are directly queryable.
#
# Indexes:
#   idx_dt_ts       — descending ts for time-ordered dashboard queries.
#   idx_dt_sym_ts   — (symbol, ts DESC) for per-symbol filtering.
#   idx_dt_outcome  — (outcome, ts DESC) for outcome-bucket aggregation.
_TRACK10_DT_SCHEMA = """
CREATE TABLE IF NOT EXISTS decision_trace (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    ts               REAL    NOT NULL,
    symbol           TEXT    NOT NULL,
    direction        TEXT,
    regime           TEXT,
    confidence       REAL,
    z_score          REAL,
    kelly_f          REAL,
    mtf_aligned      INTEGER,
    sniper_boost     INTEGER,
    oracle_boosted   INTEGER,
    whale_mult       REAL,
    outcome          TEXT    NOT NULL,
    gate_name        TEXT    NOT NULL,
    reason           TEXT,
    llm_verdict      TEXT,
    p32_p_success    REAL,
    sizing_usd       REAL,
    risk_off         INTEGER,
    p39_snipe        INTEGER,
    p39_size_boost   REAL,
    conviction_mult  REAL
);
CREATE INDEX IF NOT EXISTS idx_dt_ts      ON decision_trace(ts DESC);
CREATE INDEX IF NOT EXISTS idx_dt_sym_ts  ON decision_trace(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_dt_outcome ON decision_trace(outcome, ts DESC);
"""


class Database:
    """
    [S6.1-DBPATH / S7A-DBPATH / R1-A-DBPATH]

    Canonical DB path: hub_data/powertrader.db — expressed as an ABSOLUTE path
    constructed from the resolved directory of this module file:

        Path(__file__).resolve().parent / "hub_data" / "powertrader.db"

    This path is ALWAYS absolute regardless of the process working directory.
    datahub.db is NEVER used anywhere in this codebase.

    hub_data/ is created at __init__ time (not lazily) so the directory is
    guaranteed to exist before any caller attempts to open the DB.

    The resolved absolute path is logged immediately in __init__ — before any
    connection is made — so it appears in the boot log for easy verification
    against the dashboard's expected path.

    On connection failure _conn_obj() logs the error and re-raises (fail-closed)
    so callers and the boot ensure-ready step can detect it immediately.

    Schema tables: candles, trades (extended by Phase 1 migration with
    trade_id, exit_reason_type, exit_reason_category columns),
    account_snapshots, decision_trace (Track 10 — cross-session admission
    outcome history, created via idempotent CREATE TABLE IF NOT EXISTS).

    [CI-1-B] upsert_candles(candles) — batch upsert in a single
    transaction/commit.  Empty list is a safe no-op.  sqlite errors are logged
    and re-raised so the caller is never left with silent data loss.

    [TRACK10-DT] insert_decision_trace(record) — fail-open INSERT for one
    admission-outcome record from _maybe_enter.  Never raises; failures are
    logged at DEBUG so the admission pipeline is never blocked by observability
    writes.  Called only via asyncio.ensure_future() from executor.py.
    """

    def __init__(self, path: Optional[str] = None):
        # [R1-A-DBPATH] module_dir is the resolved absolute directory that
        # contains this file — never the process CWD.
        module_dir = Path(__file__).resolve().parent

        if path and str(path).strip():
            p = Path(str(path).strip()).expanduser()
            if not p.is_absolute():
                p = (module_dir / p).resolve()
            else:
                p = p.resolve()
            self._path = str(p)
        else:
            # [R1-A-DBPATH] Canonical absolute path:
            #   <module_dir>/hub_data/powertrader.db
            # datahub.db is NEVER used.
            hub_dir = module_dir / "hub_data"
            try:
                # mkdir at __init__ time — not lazily — so the directory is
                # guaranteed to exist before any connection is attempted.
                hub_dir.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                log.error(
                    "[R1-A-DBPATH] Cannot create hub_data directory %s: %s",
                    hub_dir, exc,
                )
                raise
            self._path = str((hub_dir / "powertrader.db").resolve())

        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

        # [R1-A-DBPATH] Log the absolute DB path immediately at construction
        # time — before any connection — so it appears in the very first log
        # lines and can be compared directly with the dashboard's expected path.
        log.info("[R1-A-DBPATH] Database absolute path (canonical): %s", self._path)

    def _conn_obj(self) -> sqlite3.Connection:
        """
        Return the shared SQLite connection, creating it on first call.
        Applies WAL pragmas and schema; commits immediately.

        [R1-A-DBPATH] Fail-closed: on any failure the error is logged and
        re-raised so the caller (and the boot ensure-ready step) can detect it.
        The connection slot is reset to None so a subsequent call will retry.
        """
        if self._conn is None:
            try:
                self._conn = sqlite3.connect(
                    self._path, check_same_thread=False, timeout=30
                )
                self._conn.row_factory = sqlite3.Row
                self._conn.execute("PRAGMA journal_mode=WAL;")
                self._conn.execute("PRAGMA synchronous=NORMAL;")
                self._conn.executescript(_SCHEMA)
                self._conn.commit()
                # [PHASE1] Additive migration: add trade_id, exit_reason_type,
                # exit_reason_category columns to trades table if not present.
                # SQLite does not support IF NOT EXISTS on ALTER TABLE, so we
                # catch OperationalError per column and ignore "duplicate column"
                # errors — any other error is logged at ERROR and re-raised so
                # the outer handler sets _conn=None and fails boot (fail-closed).
                for _col_sql in _PHASE1_MIGRATION.strip().splitlines():
                    _col_sql = _col_sql.strip()
                    if not _col_sql:
                        continue
                    try:
                        self._conn.execute(_col_sql)
                    except Exception as _mig_exc:
                        _msg = str(_mig_exc).lower()
                        if "duplicate column" in _msg:
                            pass   # already present — safe no-op
                        else:
                            log.error(
                                "[PHASE1] trades migration failed (non-recoverable): %s — %s",
                                _col_sql, _mig_exc,
                            )
                            raise
                self._conn.commit()
                log.info(
                    "[R1-A-DBPATH] Database opened and schema applied: %s",
                    self._path,
                )
                # [PHASE4] Additive migration: add is_close_leg column.
                # Same pattern as Phase 1 — catch duplicate-column errors
                # and ignore them; any other error is fatal.
                for _col_sql in _PHASE4_MIGRATION.strip().splitlines():
                    _col_sql = _col_sql.strip()
                    if not _col_sql:
                        continue
                    try:
                        self._conn.execute(_col_sql)
                    except Exception as _mig4_exc:
                        _msg4 = str(_mig4_exc).lower()
                        if "duplicate column" in _msg4:
                            pass   # already present — safe no-op
                        else:
                            log.error(
                                "[PHASE4] trades migration failed (non-recoverable): %s — %s",
                                _col_sql, _mig4_exc,
                            )
                            raise
                self._conn.commit()
                # [PHASE6] Additive migration: add entry_confidence, entry_kelly_f,
                # hold_time_secs diagnostic columns to close-leg rows.
                # Same pattern as Phase 1/4 — duplicate-column ignored, other errors fatal.
                for _col_sql in _PHASE6_MIGRATION.strip().splitlines():
                    _col_sql = _col_sql.strip()
                    if not _col_sql:
                        continue
                    try:
                        self._conn.execute(_col_sql)
                    except Exception as _mig6_exc:
                        _msg6 = str(_mig6_exc).lower()
                        if "duplicate column" in _msg6:
                            pass   # already present — safe no-op
                        else:
                            log.error(
                                "[PHASE6] trades migration failed (non-recoverable): %s — %s",
                                _col_sql, _mig6_exc,
                            )
                            raise
                self._conn.commit()
                # [PHASE10] Additive migration: add close_slippage_bps and
                # spread_at_close_bps friction columns to close-leg rows.
                # Same pattern as Phases 1/4/6 — duplicate-column errors are
                # silently ignored; any other error is fatal (fail-closed boot).
                for _col_sql in _PHASE10_MIGRATION.strip().splitlines():
                    _col_sql = _col_sql.strip()
                    if not _col_sql:
                        continue
                    try:
                        self._conn.execute(_col_sql)
                    except Exception as _mig10_exc:
                        _msg10 = str(_mig10_exc).lower()
                        if "duplicate column" in _msg10:
                            pass   # already present — safe no-op
                        else:
                            log.error(
                                "[PHASE10] trades migration failed (non-recoverable): %s — %s",
                                _col_sql, _mig10_exc,
                            )
                            raise
                self._conn.commit()
                # [POST-ROADMAP-DT] Additive migration: add entry_regime,
                # entry_direction, entry_z_score signal-context columns to
                # close-leg rows.  Same pattern as Phases 1/4/6/10 —
                # duplicate-column errors are silently ignored; any other
                # error is fatal (fail-closed boot).
                for _col_sql in _DT_MIGRATION.strip().splitlines():
                    _col_sql = _col_sql.strip()
                    if not _col_sql:
                        continue
                    try:
                        self._conn.execute(_col_sql)
                    except Exception as _mig_dt_exc:
                        _msg_dt = str(_mig_dt_exc).lower()
                        if "duplicate column" in _msg_dt:
                            pass   # already present — safe no-op
                        else:
                            log.error(
                                "[POST-ROADMAP-DT] trades migration failed "
                                "(non-recoverable): %s — %s",
                                _col_sql, _mig_dt_exc,
                            )
                            raise
                self._conn.commit()
                # [TRACK10-DT] Create decision_trace table and its indexes if
                # not already present.  Uses executescript() with CREATE TABLE
                # IF NOT EXISTS and CREATE INDEX IF NOT EXISTS — fully idempotent
                # on every boot.  No ALTER TABLE on existing tables; no risk to
                # existing rows.  A failure here is fatal (fail-closed) because
                # it means the DB is inaccessible for writes — the same policy
                # as the _SCHEMA application above.
                try:
                    self._conn.executescript(_TRACK10_DT_SCHEMA)
                    self._conn.commit()
                    log.info("[TRACK10-DT] decision_trace table ready: %s", self._path)
                except Exception as _mig10dt_exc:
                    log.error(
                        "[TRACK10-DT] decision_trace schema failed (non-recoverable): %s",
                        _mig10dt_exc,
                    )
                    raise
                # [TRACK16-DT] Additive migration: add source_path column to
                # decision_trace.  Same duplicate-column-ignore pattern as all
                # prior migrations.  Old rows carry NULL source_path, which is
                # correct — callers that do not supply it also get NULL.
                for _col_sql in _TRACK16_DT_MIGRATION.strip().splitlines():
                    _col_sql = _col_sql.strip()
                    if not _col_sql:
                        continue
                    try:
                        self._conn.execute(_col_sql)
                    except Exception as _mig16_exc:
                        _msg16 = str(_mig16_exc).lower()
                        if "duplicate column" in _msg16:
                            pass   # already present — safe no-op
                        else:
                            log.error(
                                "[TRACK16-DT] decision_trace migration failed "
                                "(non-recoverable): %s — %s",
                                _col_sql, _mig16_exc,
                            )
                            raise
                self._conn.commit()
                # [TRACK17-SCHEMA] Additive migration: pnl_pct and
                # hold_time_secs columns for close/DCA trace records.
                # Same duplicate-column-ignore pattern as Track 16.
                for _col_sql in _TRACK17_DT_MIGRATION.strip().splitlines():
                    _col_sql = _col_sql.strip()
                    if not _col_sql:
                        continue
                    try:
                        self._conn.execute(_col_sql)
                    except Exception as _mig17_exc:
                        _msg17 = str(_mig17_exc).lower()
                        if "duplicate column" in _msg17:
                            pass   # already present — safe no-op
                        else:
                            log.error(
                                "[TRACK17-SCHEMA] decision_trace migration failed "
                                "(non-recoverable): %s — %s",
                                _col_sql, _mig17_exc,
                            )
                            raise
                self._conn.commit()
            except Exception:
                log.error(
                    "[R1-A-DBPATH] Database._conn_obj: failed to open/init %s",
                    self._path, exc_info=True,
                )
                self._conn = None
                raise
        return self._conn

    async def _run(self, fn: Callable) -> Any:
        loop = asyncio.get_event_loop()
        async with self._lock:
            return await loop.run_in_executor(None, fn)

    async def upsert_candle(self, c: Candle) -> None:
        def _fn():
            conn = self._conn_obj()
            conn.execute(
                "INSERT OR REPLACE INTO candles"
                "(symbol,tf,ts,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?,?)",
                (c.symbol, c.tf, c.ts, c.open, c.high, c.low, c.close, c.volume),
            )
            conn.commit()
        await self._run(_fn)

    async def upsert_candles(self, candles: List[Candle]) -> None:
        """
        [CI-1-B] Batch-upsert a list of Candle objects in a single transaction.

        Behaviour:
          • Empty list → safe no-op (no DB round-trip).
          • All rows are inserted/replaced in one executemany() call and
            committed in a single conn.commit() so the write is atomic.
          • sqlite3.Error and unexpected exceptions are logged with full
            context (symbol, tf range, count) and re-raised — no silent loss.
        """
        if not candles:
            return

        def _fn():
            conn = self._conn_obj()
            rows = [
                (c.symbol, c.tf, c.ts, c.open, c.high, c.low, c.close, c.volume)
                for c in candles
            ]
            try:
                conn.executemany(
                    "INSERT OR REPLACE INTO candles"
                    "(symbol,tf,ts,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?,?)",
                    rows,
                )
                conn.commit()
                log.debug(
                    "[CI-1-B] upsert_candles: committed %d rows "
                    "(symbol=%s tf=%s ts_range=[%d,%d])",
                    len(rows),
                    candles[0].symbol,
                    candles[0].tf,
                    candles[0].ts,
                    candles[-1].ts,
                )
            except sqlite3.Error as _exc:
                log.error(
                    "[CI-1-B] upsert_candles: sqlite error — "
                    "count=%d symbol=%s tf=%s: %s",
                    len(rows),
                    candles[0].symbol if candles else "?",
                    candles[0].tf if candles else "?",
                    _exc,
                )
                raise
            except Exception as _exc:
                log.error(
                    "[CI-1-B] upsert_candles: unexpected error — "
                    "count=%d symbol=%s tf=%s: %s",
                    len(rows),
                    candles[0].symbol if candles else "?",
                    candles[0].tf if candles else "?",
                    _exc,
                )
                raise

        await self._run(_fn)

    async def insert_trade(self, t: dict) -> None:
        """Insert a trade record; uses INSERT OR IGNORE on duplicate order_id.

        [PHASE1] Accepts optional keys:
          trade_id             — links open and close rows for one round-trip
          exit_reason_type     — ExitReason.value string (typed exit reason)
          exit_reason_category — ExitReasonCategory.value string (coarse bucket)
        [PHASE4] Accepts optional key:
          is_close_leg         — 1 for canonical close rows, 0 (default) for all others
        [PHASE6] Accepts optional keys (close-leg only; NULL for open/noise rows):
          entry_confidence     — Signal.confidence at position entry
          entry_kelly_f        — Signal.kelly_f at position entry
          hold_time_secs       — seconds held from entry_ts to close fill
        [PHASE10] Accepts optional keys (close-leg only; NULL for HEDGE_TRIM and
          pre-Phase-10 rows):
          close_slippage_bps   — signed execution slippage at close (positive = adverse)
          spread_at_close_bps  — bid-ask spread in bps at close order submission time
        [POST-ROADMAP-DT] Accepts optional keys (close-leg only; NULL for historical rows):
          entry_regime         — Signal.regime at position entry
          entry_direction      — Signal.direction at position entry
          entry_z_score        — Signal.z_score at position entry
        These keys are ignored gracefully if absent (old callers unaffected).
        """
        def _fn():
            try:
                conn = self._conn_obj()
                conn.execute(
                    "INSERT OR IGNORE INTO trades"
                    "(ts,symbol,side,qty,price,cost_basis,pnl_pct,"
                    "realized_usd,tag,order_id,inst_type,"
                    "trade_id,exit_reason_type,exit_reason_category,"
                    "is_close_leg,"
                    "entry_confidence,entry_kelly_f,hold_time_secs,"
                    "close_slippage_bps,spread_at_close_bps,"
                    "entry_regime,entry_direction,entry_z_score) VALUES"
                    "(:ts,:symbol,:side,:qty,:price,:cost_basis,:pnl_pct,"
                    ":realized_usd,:tag,:order_id,:inst_type,"
                    ":trade_id,:exit_reason_type,:exit_reason_category,"
                    ":is_close_leg,"
                    ":entry_confidence,:entry_kelly_f,:hold_time_secs,"
                    ":close_slippage_bps,:spread_at_close_bps,"
                    ":entry_regime,:entry_direction,:entry_z_score)",
                    {
                        "ts":                   t.get("ts"),
                        "symbol":               t.get("symbol"),
                        "side":                 t.get("side"),
                        "qty":                  t.get("qty"),
                        "price":                t.get("price"),
                        "cost_basis":           t.get("cost_basis"),
                        "pnl_pct":              t.get("pnl_pct"),
                        "realized_usd":         t.get("realized_usd"),
                        "tag":                  t.get("tag"),
                        "order_id":             t.get("order_id"),
                        "inst_type":            t.get("inst_type"),
                        # [PHASE1] New columns — None when not supplied (old callers).
                        "trade_id":             t.get("trade_id"),
                        "exit_reason_type":     t.get("exit_reason_type"),
                        "exit_reason_category": t.get("exit_reason_category"),
                        # [PHASE4] is_close_leg: default 0 (non-close/open/add rows).
                        "is_close_leg":         int(t.get("is_close_leg") or 0),
                        # [PHASE6] Close-leg diagnostic columns — None for non-close rows.
                        "entry_confidence":     t.get("entry_confidence"),
                        "entry_kelly_f":        t.get("entry_kelly_f"),
                        "hold_time_secs":       t.get("hold_time_secs"),
                        # [PHASE10] Friction columns — None for HEDGE_TRIM and
                        # pre-Phase-10 rows; old callers default gracefully to None.
                        "close_slippage_bps":   t.get("close_slippage_bps"),
                        "spread_at_close_bps":  t.get("spread_at_close_bps"),
                        # [POST-ROADMAP-DT] Entry-context columns — None for historical
                        # rows and non-close rows; old callers default gracefully to None.
                        "entry_regime":         t.get("entry_regime"),
                        "entry_direction":      t.get("entry_direction"),
                        "entry_z_score":        t.get("entry_z_score"),
                    },
                )
                conn.commit()
            except sqlite3.Error as _exc:
                log.error(
                    "Database.insert_trade: sqlite error for order_id=%s: %s",
                    t.get("order_id", "?"), _exc,
                )
                raise
            except Exception as _exc:
                log.error(
                    "Database.insert_trade: unexpected error for order_id=%s: %s",
                    t.get("order_id", "?"), _exc,
                )
                raise
        await self._run(_fn)

    async def patch_open_attribution(
        self,
        order_id: str,
        trade_id: str,
        tag: str,
    ) -> int:
        """[PHASE4] Enrich an open-leg row that was written by _on_fill_event before
        the canonical entry path had a chance to set the position.

        UPDATEs the row matching order_id with:
          - trade_id (links this row to the close leg via round-trip id)
          - tag      (canonical entry tag, e.g. "LONG_ENTRY")
          - is_close_leg = 0 (explicit; default but made unambiguous)

        Returns the number of rows updated (0 = row not yet written; 1 = patched).
        If 0 is returned, the caller's subsequent INSERT OR IGNORE acts as the
        fallback write.  This method never raises on a missing row.
        """
        def _fn() -> int:
            try:
                conn  = self._conn_obj()
                cur   = conn.execute(
                    "UPDATE trades SET trade_id=?, tag=?, is_close_leg=0"
                    " WHERE order_id=?",
                    (trade_id, tag, order_id),
                )
                conn.commit()
                return cur.rowcount
            except sqlite3.Error as _exc:
                log.error(
                    "Database.patch_open_attribution: sqlite error order_id=%s: %s",
                    order_id, _exc,
                )
                raise
        return await self._run(_fn)

    async def patch_close_attribution(
        self,
        order_id: str,
        trade_id: str,
        tag: str,
        exit_reason_type: str,
        exit_reason_category: str,
        pnl_pct: float,
        realized_usd: float,
        entry_confidence: Optional[float] = None,
        entry_kelly_f: Optional[float] = None,
        hold_time_secs: Optional[float] = None,
        close_slippage_bps: Optional[float] = None,
        spread_at_close_bps: Optional[float] = None,
        entry_regime: Optional[str] = None,
        entry_direction: Optional[str] = None,
        entry_z_score: Optional[float] = None,
    ) -> int:
        """[PHASE4] Enrich a close-leg row that was written by _on_fill_event with
        full attribution: trade linkage, PnL, and typed exit reason.

        [PHASE6] Also writes entry_confidence, entry_kelly_f, hold_time_secs when
        supplied (None values leave those columns unchanged / NULL).

        [PHASE10] Also writes close_slippage_bps, spread_at_close_bps when supplied.
        Both default to None (NULL) so all existing callers are unaffected.
        [PHASE15] HEDGE_TRIM and liquidation rows now supply non-None values when a
        pre-order tick was available; None is passed only when tick fetch returned no data.

        [POST-ROADMAP-DT] Also writes entry_regime, entry_direction, entry_z_score
        when supplied.  All three default to None so all existing callers are unaffected.
        Populated from Signal fields at position entry; NULL on historical rows.

        UPDATEs the row matching order_id with all canonical close-leg fields.
        Sets is_close_leg = 1 so KPI filters can identify it as a finalized close.

        Returns rows updated (0 = fill-event row not yet written; 1 = patched).
        Callers should INSERT OR IGNORE as fallback when 0 is returned.
        """
        def _fn() -> int:
            try:
                conn  = self._conn_obj()
                cur   = conn.execute(
                    "UPDATE trades SET"
                    "  trade_id=?, tag=?, is_close_leg=1,"
                    "  exit_reason_type=?, exit_reason_category=?,"
                    "  pnl_pct=?, realized_usd=?,"
                    "  entry_confidence=?, entry_kelly_f=?, hold_time_secs=?,"
                    "  close_slippage_bps=?, spread_at_close_bps=?,"
                    "  entry_regime=?, entry_direction=?, entry_z_score=?"
                    " WHERE order_id=?",
                    (
                        trade_id, tag, exit_reason_type, exit_reason_category,
                        pnl_pct, realized_usd,
                        entry_confidence, entry_kelly_f, hold_time_secs,
                        close_slippage_bps, spread_at_close_bps,
                        entry_regime, entry_direction, entry_z_score,
                        order_id,
                    ),
                )
                conn.commit()
                return cur.rowcount
            except sqlite3.Error as _exc:
                log.error(
                    "Database.patch_close_attribution: sqlite error order_id=%s: %s",
                    order_id, _exc,
                )
                raise
        return await self._run(_fn)

    async def patch_close_friction(
        self,
        trade_id: str,
        entry_confidence: Optional[float],
        entry_kelly_f: Optional[float],
        hold_time_secs: Optional[float],
    ) -> int:
        """[PHASE6] Back-fill diagnostic columns on a close-leg row already written
        via INSERT (i.e. when patch_close_attribution returned 0 and the fallback
        INSERT path was used).  Matches on trade_id + is_close_leg=1.

        This is a best-effort write; failures are logged at DEBUG and do not raise,
        so the critical close path is never blocked by a diagnostic update failure.

        Returns rows updated (0 = row not found or already populated).
        """
        def _fn() -> int:
            try:
                conn = self._conn_obj()
                cur  = conn.execute(
                    "UPDATE trades SET"
                    "  entry_confidence=?, entry_kelly_f=?, hold_time_secs=?"
                    " WHERE trade_id=? AND is_close_leg=1",
                    (entry_confidence, entry_kelly_f, hold_time_secs, trade_id),
                )
                conn.commit()
                return cur.rowcount
            except sqlite3.Error as _exc:
                log.debug(
                    "Database.patch_close_friction: sqlite error trade_id=%s: %s",
                    trade_id, _exc,
                )
                return 0
        return await self._run(_fn)

    async def insert_snapshot(self, s: dict) -> None:
        """Insert an account snapshot into account_snapshots."""
        def _fn():
            try:
                conn = self._conn_obj()
                conn.execute(
                    "INSERT INTO account_snapshots"
                    "(ts,total_equity,buying_power,margin_ratio)"
                    " VALUES(:ts,:total_equity,:buying_power,:margin_ratio)",
                    s,
                )
                conn.commit()
            except sqlite3.Error as _exc:
                log.error(
                    "Database.insert_snapshot: sqlite error ts=%s equity=%.4f: %s",
                    s.get("ts", "?"), s.get("total_equity", 0.0), _exc,
                )
                raise
            except Exception as _exc:
                log.error(
                    "Database.insert_snapshot: unexpected error ts=%s: %s",
                    s.get("ts", "?"), _exc,
                )
                raise
        await self._run(_fn)

    async def insert_decision_trace(self, record: dict) -> None:
        """[TRACK10-DT] Insert one admission-outcome record into decision_trace.

        This method is FAIL-OPEN by design: all exceptions are caught and logged
        at DEBUG level.  It must never raise into the caller — it is fired via
        asyncio.ensure_future() from _maybe_enter and must not affect admission
        logic under any circumstances.

        This intentionally differs from insert_trade (which raises on error)
        because decision_trace rows are observability data, not authoritative
        fill truth.

        Accepts the standard Track 09/10 record dict with keys:
            ts, symbol, direction, regime, confidence, z_score, kelly_f,
            mtf_aligned, sniper_boost, oracle_boosted, whale_mult,
            outcome, gate_name, reason, llm_verdict, p32_p_success,
            sizing_usd, modifiers (dict with risk_off, p39_snipe,
            p39_size_boost, conviction_mult).

        [TRACK16-DT] Also accepts optional source_path (str):
            "entry"   — from _maybe_enter (standard signal path)
            "express" — from trigger_atomic_express_trade (oracle/whale path)
            None / absent — pre-Track-16 callers; stored as NULL.

        [TRACK17-DT] Also accepts optional lifecycle fields:
            "source_path": "close" — from _record_close (any exit)
            "source_path": "dca"   — from _maybe_dca (successful add)
            "pnl_pct"       (float) — realized PnL pct for close rows; NULL otherwise.
            "hold_time_secs" (float) — hold duration for close rows; NULL otherwise.
            Absent keys → NULL stored (backward-compatible).

        The modifiers dict is flattened into individual columns for
        direct SQL queryability.
        """
        def _fn() -> None:
            try:
                conn = self._conn_obj()
                mods = record.get("modifiers") or {}
                conn.execute(
                    "INSERT INTO decision_trace"
                    "(ts,symbol,direction,regime,confidence,z_score,kelly_f,"
                    "mtf_aligned,sniper_boost,oracle_boosted,whale_mult,"
                    "outcome,gate_name,reason,llm_verdict,p32_p_success,"
                    "sizing_usd,risk_off,p39_snipe,p39_size_boost,conviction_mult,"
                    "source_path,pnl_pct,hold_time_secs)"
                    " VALUES"
                    "(:ts,:symbol,:direction,:regime,:confidence,:z_score,:kelly_f,"
                    ":mtf_aligned,:sniper_boost,:oracle_boosted,:whale_mult,"
                    ":outcome,:gate_name,:reason,:llm_verdict,:p32_p_success,"
                    ":sizing_usd,:risk_off,:p39_snipe,:p39_size_boost,:conviction_mult,"
                    ":source_path,:pnl_pct,:hold_time_secs)",
                    {
                        "ts":             record.get("ts"),
                        "symbol":         record.get("symbol"),
                        "direction":      record.get("direction"),
                        "regime":         record.get("regime"),
                        "confidence":     record.get("confidence"),
                        "z_score":        record.get("z_score"),
                        "kelly_f":        record.get("kelly_f"),
                        "mtf_aligned":    int(bool(record.get("mtf_aligned"))),
                        "sniper_boost":   int(bool(record.get("sniper_boost"))),
                        "oracle_boosted": int(bool(record.get("oracle_boosted"))),
                        "whale_mult":     record.get("whale_mult"),
                        "outcome":        record.get("outcome", ""),
                        "gate_name":      record.get("gate_name", ""),
                        "reason":         (record.get("reason") or "")[:120],
                        "llm_verdict":    record.get("llm_verdict") or "",
                        "p32_p_success":  record.get("p32_p_success"),
                        "sizing_usd":     record.get("sizing_usd"),
                        # Flattened modifier columns
                        "risk_off":        int(bool(mods.get("risk_off", False))),
                        "p39_snipe":       int(bool(mods.get("p39_snipe", False))),
                        "p39_size_boost":  mods.get("p39_size_boost"),
                        "conviction_mult": mods.get("conviction_mult"),
                        # [TRACK16-DT] Source path — NULL when caller omits the key.
                        "source_path":     record.get("source_path"),
                        # [TRACK17-DT] Lifecycle fields — NULL for non-close rows.
                        "pnl_pct":         record.get("pnl_pct"),
                        "hold_time_secs":  record.get("hold_time_secs"),
                    },
                )
                conn.commit()
            except Exception as _dt_exc:
                # Fail-open: observability writes must never surface into
                # the admission pipeline.  DEBUG only — not ERROR.
                log.debug(
                    "[TRACK10-DT] insert_decision_trace: swallowed error "
                    "sym=%s outcome=%s: %s",
                    record.get("symbol", "?"),
                    record.get("outcome", "?"),
                    _dt_exc,
                )
        try:
            await self._run(_fn)
        except Exception as _outer_exc:
            log.debug(
                "[TRACK10-DT] insert_decision_trace: _run failed sym=%s: %s",
                record.get("symbol", "?"), _outer_exc,
            )

    async def prune_decision_trace(self, retain_days: int) -> int:
        """[TRACK12-PRUNE] Delete decision_trace rows older than retain_days.

        Executes a single parameterized DELETE against the ts column index
        (idx_dt_ts), which makes the operation a fast index range scan rather
        than a full table scan.

        This method is FAIL-OPEN by design: all exceptions are caught and
        logged at WARNING level.  It never raises into its caller — neither
        the background _dt_prune_loop nor the on-boot call in DataHub.start()
        must be able to fail because of a prune error.

        Parameters
        ----------
        retain_days : int
            Rows with ts < (now - retain_days * 86400) are deleted.
            Must be > 0; values ≤ 0 are treated as a no-op with a warning.

        Returns
        -------
        int
            Number of rows deleted, or 0 on any error / no-op.
        """
        if retain_days <= 0:
            log.warning(
                "[TRACK12-PRUNE] prune_decision_trace called with retain_days=%d "
                "(≤ 0) — skipping to avoid deleting all history.",
                retain_days,
            )
            return 0

        def _fn() -> int:
            try:
                conn  = self._conn_obj()
                cutoff = time.time() - retain_days * 86_400.0
                conn.execute(
                    "DELETE FROM decision_trace WHERE ts < :cutoff",
                    {"cutoff": cutoff},
                )
                conn.commit()
                return conn.total_changes
            except Exception as _prune_exc:
                log.warning(
                    "[TRACK12-PRUNE] prune_decision_trace DELETE failed "
                    "(retain_days=%d): %s",
                    retain_days, _prune_exc,
                )
                return 0

        try:
            deleted: int = await self._run(_fn)
            return deleted if isinstance(deleted, int) else 0
        except Exception as _outer_exc:
            log.warning(
                "[TRACK12-PRUNE] prune_decision_trace: _run failed "
                "(retain_days=%d): %s",
                retain_days, _outer_exc,
            )
            return 0


class Cache:
    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._local: Dict[str, str]            = {}
        self._use_local                         = False

    async def connect(self) -> None:
        try:
            self._redis = await aioredis.from_url(
                REDIS_URL, decode_responses=True, socket_connect_timeout=3,
            )
            await self._redis.ping()
            log.info("Redis connected: %s", REDIS_URL)
        except Exception as exc:
            log.warning("Redis unavailable (%s) — using in-process dict.", exc)
            self._use_local = True

    async def set(self, key: str, value: Any, ex: int = 300) -> None:
        v = json.dumps(value)
        if self._use_local:
            self._local[key] = v
        else:
            try:
                await self._redis.set(key, v, ex=ex)
            except Exception:
                self._local[key] = v

    async def get(self, key: str) -> Any:
        try:
            raw = self._local.get(key) if self._use_local else await self._redis.get(key)
            return json.loads(raw) if raw else None
        except Exception:
            return None

    async def publish(self, channel: str, msg: Any) -> None:
        if not self._use_local and self._redis:
            try:
                await self._redis.publish(channel, json.dumps(msg))
            except Exception as _exc:
                log.debug("[DATA_HUB] cleanup: %s", _exc)


# ── OKX Market Feed ────────────────────────────────────────────────────────────

class OKXMarketFeed:
    def __init__(
        self,
        symbols:    List[str],
        on_tick:    Callable,
        on_candle:  Callable,
        on_book:    Callable,
        on_funding: Callable,
        on_liq:     Callable,
        on_trade:   Callable,
        demo: bool = OKX_DEMO_MODE,
    ):
        self.inst_ids      = [f"{s.upper()}-USDT"      for s in symbols]
        self.swap_inst_ids = [f"{s.upper()}-USDT-SWAP" for s in symbols]

        self.on_tick    = on_tick
        self.on_candle  = on_candle
        self.on_book    = on_book
        self.on_funding = on_funding
        self.on_liq     = on_liq
        self.on_trade   = on_trade

        pub_url = OKX_DEMO_WS_PUB if demo else OKX_WS_PUBLIC
        biz_url = OKX_DEMO_WS_BIZ if demo else OKX_WS_BIZ

        ticker_subs  = [{"channel": "tickers",      "instId": i} for i in self.inst_ids]
        book_subs    = [{"channel": "books5",        "instId": i} for i in self.inst_ids]
        funding_subs = [{"channel": "funding-rate",  "instId": i} for i in self.swap_inst_ids]
        liq_subs     = [{"channel": "liquidation-orders", "instType": "SWAP"}]
        trade_subs   = [{"channel": "trades",        "instId": i} for i in self.inst_ids]

        self._ticker_ws = OKXWebSocket(pub_url, self._handle, private=False)
        self._ticker_ws.subscribe(
            ticker_subs + book_subs + funding_subs + liq_subs + trade_subs
        )

        candle_subs = [
            {"channel": f"candle{bar}", "instId": inst}
            for inst in self.inst_ids
            for bar in _WS_SUPPORTED_BARS.values()
        ]
        self._candle_ws = OKXWebSocket(biz_url, self._handle, private=False)
        self._candle_ws.subscribe(candle_subs)

    async def run(self) -> None:
        await asyncio.gather(self._ticker_ws.run(), self._candle_ws.run())

    def stop(self) -> None:
        self._ticker_ws.stop()
        self._candle_ws.stop()

    async def _handle(self, msg: dict) -> None:
        event = msg.get("event")
        if event in ("subscribe", "login"):
            return
        if event == "error":
            log.error("OKX WS error code=%s: %s", msg.get("code"), msg.get("msg"))
            return
        ch      = msg.get("arg", {}).get("channel", "")
        data    = msg.get("data", [])
        inst_id = msg.get("arg", {}).get("instId", "")
        if not data:
            return
        if   ch == "tickers":           await self._handle_ticker(data, inst_id)
        elif ch == "books5":            await self._handle_book(data, inst_id)
        elif ch.startswith("candle"):   await self._handle_candle(ch, inst_id, data)
        elif ch == "funding-rate":      await self._handle_funding(data, inst_id)
        elif ch == "liquidation-orders":await self._handle_liquidation(data)
        elif ch == "trades":            await self._handle_trades(data, inst_id)

    async def _handle_ticker(self, data: list, inst_id: str) -> None:
        for d in data:
            try:
                await self.on_tick(Tick(
                    symbol=inst_id,
                    bid=float(d.get("bidPx") or d.get("bid1Px") or 0),
                    ask=float(d.get("askPx") or d.get("ask1Px") or 0),
                    last=float(d.get("last", 0)),
                ))
            except (ValueError, KeyError) as exc:
                log.debug("Ticker parse %s: %s", inst_id, exc)

    async def _handle_book(self, data: list, inst_id: str) -> None:
        for d in data:
            try:
                raw_bids = [(float(r[0]), float(r[1])) for r in d.get("bids", [])]
                raw_asks = [(float(r[0]), float(r[1])) for r in d.get("asks", [])]
                bids     = sorted(raw_bids, key=lambda x: -x[0])[:OB_MAX_LEVELS]
                asks     = sorted(raw_asks, key=lambda x:  x[0])[:OB_MAX_LEVELS]
                ts       = float(d.get("ts", time.time() * 1000)) / 1000.0
                await self.on_book(OrderBook(symbol=inst_id, bids=bids, asks=asks, ts=ts))
            except Exception as exc:
                log.debug("books5 parse %s: %s", inst_id, exc)

    async def _handle_candle(self, channel: str, inst_id: str, data: list) -> None:
        bar = channel[len("candle"):]
        tf  = next((k for k, v in _WS_SUPPORTED_BARS.items() if v == bar), None)
        if tf is None:
            return
        for row in data:
            try:
                await self.on_candle(Candle(
                    symbol=inst_id, tf=tf, ts=int(int(row[0]) / 1000),
                    open=float(row[1]), high=float(row[2]),
                    low=float(row[3]),  close=float(row[4]),
                    volume=float(row[5]),
                    confirmed=len(row) > 8 and row[8] == "1",
                ))
            except (ValueError, IndexError) as exc:
                log.debug("Candle parse %s: %s", channel, exc)

    async def _handle_funding(self, data: list, inst_id: str) -> None:
        for d in data:
            try:
                rate    = float(d.get("fundingRate") or 0)
                swap_id = d.get("instId", inst_id)
                spot_id = swap_id.replace("-SWAP", "")
                await self.on_funding(spot_id, rate)
            except Exception as exc:
                log.debug("Funding parse %s: %s", inst_id, exc)

    async def _handle_liquidation(self, data: list) -> None:
        for d in data:
            try:
                swap_id  = d.get("instId", "")
                spot_id  = swap_id.replace("-SWAP", "")
                okx_side = d.get("side", "")
                sz       = float(d.get("sz")   or 0)
                bk_px    = float(d.get("bkPx") or 0)
                if sz <= 0 or bk_px <= 0:
                    continue
                our_side = "long" if okx_side == "sell" else "short"
                await self.on_liq(spot_id, sz * bk_px, our_side)
            except Exception as exc:
                log.debug("Liquidation parse: %s", exc)

    async def _handle_trades(self, data: list, inst_id: str) -> None:
        for d in data:
            try:
                side = d.get("side", "")
                px   = float(d.get("px")  or 0)
                sz   = float(d.get("sz")  or 0)
                if px <= 0 or sz <= 0 or side not in ("buy", "sell"):
                    continue
                await self.on_trade(inst_id, side, sz, px)
            except Exception as exc:
                log.debug("Trade tape parse %s: %s", inst_id, exc)


# ── OKX Private Feed ───────────────────────────────────────────────────────────

class OKXPrivateFeed:
    def __init__(
        self,
        on_account: Callable,
        on_fill:    Callable,
        demo: bool = OKX_DEMO_MODE,
    ):
        self.on_account = on_account
        self.on_fill    = on_fill
        priv_url        = OKX_DEMO_WS_PRIV if demo else OKX_WS_PRIVATE
        self._ws        = OKXWebSocket(priv_url, self._handle, private=True)
        self._ws.subscribe([
            {"channel": "account"},
            {"channel": "orders", "instType": "ANY"},
        ])

    async def run(self) -> None:
        await self._ws.run()

    def stop(self) -> None:
        self._ws.stop()

    async def _handle(self, msg: dict) -> None:
        if "event" in msg:
            return
        ch   = msg.get("arg", {}).get("channel", "")
        data = msg.get("data", [])
        if not data:
            return
        if ch == "account":
            for d in data:
                try:
                    snap = AccountSnapshot(
                        ts=time.time(),
                        total_equity=float(d.get("adjEq") or 0),
                        buying_power=next(
                            (float(x.get("availBal", 0))
                             for x in d.get("details", [])
                             if x.get("ccy") == "USDT"), 0.0,
                        ),
                        margin_ratio=float(d.get("mgnRatio") or 0),
                        positions_raw=d.get("details", []),
                    )
                    await self.on_account(snap)
                except Exception as exc:
                    log.error("Account push parse error: %s", exc)
        elif ch == "orders":
            for d in data:
                if d.get("state") in ("filled", "partially_filled"):
                    try:
                        await self.on_fill(d)
                    except Exception as exc:
                        log.error("Fill handler error: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# DataHub  —  Central event bus (Phases 1-12 + Phase 15 + TASK-4)
# ══════════════════════════════════════════════════════════════════════════════

class DataHub:
    """
    Central event bus and data store.

    Phase 15 additions:
      self.cb_creds    : CoinbaseCDPCredentials — validated at start()
      self.oracle      : CoinbaseOracle         — started and task-launched in start()
      self.global_tape : GlobalTapeAggregator   — initialised in start()

    Public P15 API:
      get_oracle_signal(symbol)      → OracleSignal | None
      is_hv_mode()                   → bool
      hv_stop_extra_pct()            → float
      get_global_tape_status()       → dict
      cycle_oracle_cancel(symbols)   → coroutine; cancels OKX buy limits when
                                       called by Executor on cancel_buys_flag=True

    TASK-4 additions:
      self._risk_executor : Executor | None — injected by main_p20 after wiring
      self._risk_cb       : CircuitBreaker | None — injected by main_p20 after wiring
      get_risk_status_snapshot()     → dict — unified Phase 4/7/15/20 risk state

    R1-A additions:
      Database is always constructed with the canonical absolute path
      Path(__file__).resolve().parent / "hub_data" / "powertrader.db".
      datahub.db is never used.  The absolute path is logged at DataHub.__init__
      time and again inside Database.__init__ so it appears in the first log
      lines of every boot.
    """

    def __init__(
        self,
        symbols:    List[str],
        timeframes: List[str],
        demo: bool = OKX_DEMO_MODE,
    ):
        self.symbols    = [_sanitize_sym(s).upper() for s in symbols if _sanitize_sym(s)]
        self.timeframes = timeframes
        self.demo       = demo

        # [R1-A-DBPATH] Database() with no path argument always resolves to the
        # canonical absolute path: <module_dir>/hub_data/powertrader.db.
        # datahub.db is NEVER used.
        self.db               = Database()
        self.cache            = Cache()
        self.rest             = OKXRestClient()
        self.instrument_cache = InstrumentCache(self.rest)
        self.volatility_guard = VolatilityGuard()

        # [R1-A-DBPATH] Log the resolved DB path at DataHub construction time
        # (Database.__init__ also logs it, but this provides a second boot-log
        # reference with "DataHub" context for easy grep).
        log.info(
            "[R1-A-DBPATH] DataHub: using DB at absolute path: %s",
            self.db._path,
        )

        self._sentiment: Dict[str, SentimentBuffer] = {
            f"{s}-USDT": SentimentBuffer() for s in self.symbols
        }
        self._tapes: Dict[str, TapeBuffer] = {
            f"{s}-USDT": TapeBuffer() for s in self.symbols
        }

        self._buffers:  Dict[Tuple[str, str], CandleBuffer] = {}
        self._buf_lock  = asyncio.Lock()
        self._books:    Dict[str, OrderBook] = {}
        self._book_lock = asyncio.Lock()

        self._subs: Dict[str, List[Callable]] = defaultdict(list)
        self._last_account: Optional[AccountSnapshot] = None

        self._market_feed = OKXMarketFeed(
            symbols    = symbols,
            on_tick    = self._on_tick,
            on_candle  = self._on_candle,
            on_book    = self._on_book,
            on_funding = self._on_funding,
            on_liq     = self._on_liq,
            on_trade   = self._on_trade,
            demo       = demo,
        )
        self._private_feed = OKXPrivateFeed(
            on_account=self._on_account, on_fill=self._on_fill, demo=demo,
        )

        self.cb_creds: CoinbaseCDPCredentials = CoinbaseCDPCredentials()

        self.oracle      = None
        self.global_tape = None

        self._oracle_task: Optional[asyncio.Task] = None

        self._risk_executor = None
        self._risk_cb       = None

        self._p36_spoof_probs: Dict[str, float] = {}
        self._p36_spoof_lock: asyncio.Lock = asyncio.Lock()

        # ── [P42-SHADOW] Global Market Sentinel ───────────────────────────────
        # Polls Yahoo Finance for SPY / DXY every 30 s (configurable).
        # Started as an asyncio task in DataHub.start().
        # Public API: get_global_market_status(), get_spy_5m_pct(), get_dxy_5m_pct().
        self.p42_sentinel: GlobalMarketSentinel = GlobalMarketSentinel()
        # ── [/P42-SHADOW] ─────────────────────────────────────────────────────

        # ── [P38-OFI] Per-symbol Order Flow Imbalance monitors ────────────────
        # Keyed by instrument ID (e.g. "BTC-USDT") matching self._books keys.
        # OBIMonitor.update() is called from _on_book() every WebSocket tick.
        self._p38_obi: Dict[str, OBIMonitor] = {
            f"{s}-USDT": OBIMonitor()
            for s in self.symbols
        }
        # ── [/P38-OFI] ────────────────────────────────────────────────────────

        self._binance_trade_ts: deque = deque()
        self._binance_tps_window: float = _env_float("BINANCE_TPS_WINDOW_SECS", 10.0)

    # ── [STAGE-3] Binance TPS API ─────────────────────────────────────────────

    def record_binance_trade(self) -> None:
        """[STAGE-3] Record a single Binance trade tick and recompute rolling TPS."""
        try:
            now = time.time()
            self._binance_trade_ts.append(now)
            cutoff = now - self._binance_tps_window
            while self._binance_trade_ts and self._binance_trade_ts[0] < cutoff:
                self._binance_trade_ts.popleft()
        except Exception as exc:
            log.debug("[STAGE-3] record_binance_trade error (non-fatal): %s", exc)

    def get_binance_tps(self) -> float:
        """[STAGE-3] Return current Binance trades-per-second over rolling window."""
        try:
            now = time.time()
            cutoff = now - self._binance_tps_window
            while self._binance_trade_ts and self._binance_trade_ts[0] < cutoff:
                self._binance_trade_ts.popleft()
            if self._binance_tps_window > 0:
                return len(self._binance_trade_ts) / self._binance_tps_window
            return 0.0
        except Exception:
            return 0.0

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _sentiment_buf(self, inst_id: str) -> Optional[SentimentBuffer]:
        key = inst_id.replace("-SWAP", "")
        if "-USDT" not in key:
            key = f"{key}-USDT"
        return self._sentiment.get(key)

    def _tape_buf(self, inst_id: str) -> Optional[TapeBuffer]:
        key = inst_id.replace("-SWAP", "")
        if "-USDT" not in key:
            key = f"{key}-USDT"
        return self._tapes.get(key)

    async def _get_buf(self, inst: str, tf: str) -> CandleBuffer:
        key = (inst, tf)
        if key not in self._buffers:
            async with self._buf_lock:
                if key not in self._buffers:
                    self._buffers[key] = CandleBuffer()
        return self._buffers[key]

    def subscribe(self, channel: str, cb: Callable) -> None:
        self._subs[channel].append(cb)

    async def _emit(self, channel: str, data: Any) -> None:
        for cb in self._subs[channel]:
            try:
                await cb(data)
            except Exception as exc:
                log.error("Subscriber [%s] error: %s", channel, exc)

    # ── Feed callbacks ─────────────────────────────────────────────────────────

    async def _on_tick(self, tick: Tick) -> None:
        try:
            self.volatility_guard.update(tick.symbol, tick.last or tick.bid)
        except Exception as exc:
            log.debug("VolatilityGuard error (ignored): %s", exc)
        await self.cache.set(f"tick:{tick.symbol}", {
            "bid": tick.bid, "ask": tick.ask, "last": tick.last, "ts": tick.ts,
        }, ex=60)
        await self._emit("tick", tick)

    async def _on_candle(self, candle: Candle) -> None:
        buf = await self._get_buf(candle.symbol, candle.tf)
        await buf.update(candle)
        if candle.confirmed:
            await self.db.upsert_candle(candle)
        await self._emit("candle", candle)
        if candle.tf == "1hour":
            await self._rebuild_aggregated(candle.symbol)

    async def _on_book(self, book: OrderBook) -> None:
        async with self._book_lock:
            self._books[book.symbol] = book
        # [P38-OFI] Feed the delta-OFI monitor on every book tick.
        # No lock needed — _on_book runs exclusively in the asyncio event loop.
        mon = self._p38_obi.get(book.symbol)
        if mon is not None:
            mon.update(book)
        await self._emit("book", book)

    async def _on_funding(self, spot_inst_id: str, rate: float) -> None:
        buf = self._sentiment_buf(spot_inst_id)
        if buf is None:
            return
        try:
            await buf.update_funding(rate)
        except Exception as exc:
            log.debug("_on_funding %s: %s", spot_inst_id, exc)

    async def _on_liq(self, spot_inst_id: str, usd_val: float, side: str) -> None:
        buf = self._sentiment_buf(spot_inst_id)
        if buf is None:
            return
        try:
            await buf.add_liquidation(usd_val, side)
        except Exception as exc:
            log.debug("_on_liq %s: %s", spot_inst_id, exc)

    async def _on_trade(self, inst_id: str, side: str, qty: float, price: float) -> None:
        buf = self._tape_buf(inst_id)
        if buf is None:
            return
        try:
            await buf.add(side, qty, price)
            await self._emit("tape", {
                "inst_id": inst_id, "side": side, "qty": qty, "price": price,
            })
        except Exception as exc:
            log.debug("_on_trade %s: %s", inst_id, exc)

    async def _rebuild_aggregated(self, inst: str) -> None:
        h1_buf       = await self._get_buf(inst, "1hour")
        h1_confirmed = await h1_buf.confirmed_list()
        if not h1_confirmed:
            return
        for period_hours, tf_label in [(8, "8hour"), (12, "12hour")]:
            agg     = aggregate_candles(h1_confirmed, period_hours, inst)
            agg_buf = await self._get_buf(inst, tf_label)
            for c in agg:
                await agg_buf.update(c)
            if agg:
                await self._emit("candle", agg[-1])

    async def _on_account(self, snap: AccountSnapshot) -> None:
        if snap.total_equity > _RISK_MIN_VALID_EQUITY or self._last_account is None:
            self._last_account = snap
        else:
            log.debug(
                "[GHOST-STATE] DataHub._on_account: "
                "equity=%.4f <= floor=%.2f — "
                "_last_account NOT overwritten (ghost push, still broadcasting).",
                snap.total_equity, _RISK_MIN_VALID_EQUITY,
            )
        await self.cache.set("account:snapshot", {
            "ts": snap.ts, "total_equity": snap.total_equity,
            "buying_power": snap.buying_power, "margin_ratio": snap.margin_ratio,
        }, ex=120)
        await self.db.insert_snapshot({
            "ts": int(snap.ts), "total_equity": snap.total_equity,
            "buying_power": snap.buying_power, "margin_ratio": snap.margin_ratio,
        })
        await self._emit("account", snap)

    async def _on_fill(self, fill: dict) -> None:
        log.info(
            "Fill: ordId=%s instId=%s side=%s fillSz=%s fillPx=%s",
            fill.get("ordId"), fill.get("instId"),
            fill.get("side"), fill.get("fillSz"), fill.get("fillPx"),
        )
        await self._emit("fill", fill)

    # ── Public API — OKX data ──────────────────────────────────────────────────

    async def get_tick(self, symbol: str) -> Optional[Tick]:
        symbol = _sanitize_sym(symbol)
        # [P35.1-FIX] Build a valid OKX instId without blindly appending -USDT.
        # Previously "BTC-USDT-SWAP" → "BTC-USDT-SWAP-USDT" (invalid → REST 400 → None).
        # Rule: if symbol already carries a recognised OKX suffix or has 2+ dashes
        # it is already a fully-qualified instId; otherwise append spot quote pair.
        _s = symbol.upper().strip()
        _OKX_SUFFIXES = ("-USDT", "-USDC", "-USD", "-SWAP", "-FUTURES")
        if any(_s.endswith(sfx) for sfx in _OKX_SUFFIXES) or _s.count("-") >= 2:
            inst = _s          # already a fully-qualified OKX instId (swap/futures/spot)
        else:
            inst = f"{_s}-USDT"  # bare base symbol e.g. "BTC" → "BTC-USDT"
        cached = await self.cache.get(f"tick:{inst}")
        if cached:
            try:
                return Tick(symbol=inst, **cached)
            except Exception as exc:
                log.debug("get_tick: cached Tick construction failed %s: %s", inst, exc)
        try:
            d    = await self.rest.get("/api/v5/market/ticker", params={"instId": inst})
            data = d.get("data") or []
            if not data or len(data) == 0:
                log.warning(
                    "Tick REST fallback: empty data list for %s — returning None.", symbol
                )
                return None
            row = data[0]
            return Tick(
                symbol=inst,
                bid=float(row.get("bidPx") or 0),
                ask=float(row.get("askPx") or 0),
                last=float(row.get("last", 0)),
            )
        except Exception as exc:
            log.warning("Tick REST fallback failed %s: %s", symbol, exc)
            return None

    async def get_price(self, symbol: str) -> Optional[float]:
        """Return the last-traded price for symbol, or None on any failure."""
        try:
            tick = await self.get_tick(symbol)
            if tick is None:
                log.warning("[PRICE-CHECK] get_price: no tick for %s — returning None.", symbol)
                return None
            price = tick.last or tick.bid or tick.ask
            if not price or price <= 0:
                log.warning(
                    "[PRICE-CHECK] get_price: zero/negative price for %s "
                    "(last=%.4f bid=%.4f ask=%.4f) — returning None.",
                    symbol, tick.last, tick.bid, tick.ask,
                )
                return None
            return float(price)
        except Exception as exc:
            log.warning("[PRICE-CHECK] get_price error for %s: %s", symbol, exc)
            return None

    async def get_candles(self, symbol: str, tf: str, n: int = 500) -> List[Candle]:
        inst = f"{symbol.upper()}-USDT"
        buf  = await self._get_buf(inst, tf)
        return await buf.snapshot(n)

    async def get_order_book(self, symbol: str) -> Optional[OrderBook]:
        inst = f"{symbol.upper()}-USDT"
        async with self._book_lock:
            book = self._books.get(inst)
        if book is not None:
            return book
        try:
            d   = await self.rest.get(
                "/api/v5/market/books",
                params={"instId": inst, "sz": "5"}, auth=False,
            )
            row      = (d.get("data") or [{}])[0]
            raw_bids = [(float(r[0]), float(r[1])) for r in row.get("bids", [])]
            raw_asks = [(float(r[0]), float(r[1])) for r in row.get("asks", [])]
            bids     = sorted(raw_bids, key=lambda x: -x[0])[:OB_MAX_LEVELS]
            asks     = sorted(raw_asks, key=lambda x:  x[0])[:OB_MAX_LEVELS]
            return OrderBook(symbol=inst, bids=bids, asks=asks)
        except Exception as exc:
            log.warning("Order book REST fallback %s: %s", symbol, exc)
            return None

    async def get_obi(self, symbol: str, levels: int = OBI_LEVELS) -> float:
        book = await self.get_order_book(symbol)
        if book is None:
            return 0.0
        val = book.obi(levels)
        log.debug("[P12-1] OBI %s top-%d: %.4f", symbol, levels, val)
        return val

    def get_tape(self, symbol: str) -> Optional[TapeBuffer]:
        return self._tape_buf(symbol)

    async def get_trade_velocity(self, symbol: str) -> float:
        buf = self._tape_buf(symbol)
        if buf is None:
            return 0.0
        return await buf.trade_velocity()

    async def get_flow_toxicity(self, symbol: str) -> float:
        """
        [P37-VPIN / P47] Return Volume-Clock ToxicityScore ∈ [0.0, 1.0] for symbol.

        [P47] When a Rust bridge reference has been injected (hub._p47_bridge) and
        P47_ENABLE=1, the bridge-computed value is returned when fresh
        (age ≤ P47_STALENESS_SECS).  Falls back to Python VPIN math silently.
        """
        # ── [P47] Bridge-computed fast path ───────────────────────────────────
        try:
            _bridge47 = getattr(self, "_p47_bridge", None)
            if _bridge47 is not None:
                _tox47, _ofi47, _bp, _ap, _fresh = _bridge47.get_p47_microstructure(symbol)
                if _fresh:
                    if getattr(_bridge47, "_p47_enabled", False) and \
                            __import__("os").environ.get("P47_LOG_SOURCE", "0") == "1":
                        __import__("logging").getLogger("data_hub").debug(
                            "[P47] get_flow_toxicity %s: source=BRIDGE tox=%.4f", symbol, _tox47
                        )
                    return _tox47
        except Exception as _p47_exc:
            __import__("logging").getLogger("data_hub").debug(
                "[P47] bridge toxicity fallback %s: %s", symbol, _p47_exc
            )
        # ── [/P47] fall through to Python VPIN ────────────────────────────────
        try:
            buf = self._tape_buf(symbol)
            if buf is None:
                return 0.0
            return buf.get_flow_toxicity()
        except Exception as exc:
            log.debug("[P37-VPIN] get_flow_toxicity error %s: %s", symbol, exc)
            return 0.0

    # ── [P42-SHADOW] Global Market Sentinel public API ─────────────────────────

    def get_global_market_status(self) -> dict:
        """
        [P42-SHADOW] Return a serialisable snapshot of the GlobalMarketSentinel state.

        Used by the Executor's _cycle() to inject SPY/DXY deltas into the
        VetoArbitrator each loop iteration, and by the Dashboard to display the
        macro veto status tile.

        Returns a dict with keys:
            spy_5m_pct        : float  — SPY 5-min % change (negative = falling)
            dxy_5m_pct        : float  — DXY 5-min % change (positive = rising $)
            data_age_secs     : float  — seconds since last successful poll
            feed_stale        : bool   — True when age > P42_MAX_STALE_SECS
            status            : str    — human-readable status string
            spy_ticker        : str
            dxy_ticker        : str
            poll_interval_secs: float
            lookback_secs     : float
        """
        try:
            return self.p42_sentinel.get_status_snapshot()
        except Exception as exc:
            log.debug("[P42-SHADOW] get_global_market_status error: %s", exc)
            return {
                "spy_5m_pct": 0.0, "dxy_5m_pct": 0.0,
                "data_age_secs": 9999.0, "feed_stale": True,
                "status": f"ERROR: {exc}",
                "spy_ticker": P42_SPY_TICKER, "dxy_ticker": P42_DXY_TICKER,
                "poll_interval_secs": P42_POLL_INTERVAL_SECS,
                "lookback_secs": P42_LOOKBACK_SECS,
            }

    def get_spy_5m_pct(self) -> float:
        """[P42-SHADOW] SPY 5-minute percentage change. Negative = equity falling."""
        try:
            return self.p42_sentinel.get_spy_5m_pct()
        except Exception:
            return 0.0

    def get_dxy_5m_pct(self) -> float:
        """[P42-SHADOW] DXY 5-minute percentage change. Positive = dollar rising."""
        try:
            return self.p42_sentinel.get_dxy_5m_pct()
        except Exception:
            return 0.0

    def get_p42_data_age_secs(self) -> float:
        """[P42-SHADOW] Seconds since last successful Yahoo Finance poll."""
        try:
            return self.p42_sentinel.get_data_age_secs()
        except Exception:
            return 9999.0

    def feed_btc_price_for_correlation(self, price: float) -> None:
        """
        [P42-CORR] Push the current BTC mid-price into GlobalMarketSentinel's
        rolling correlation window.

        Called by the Executor each _cycle() after reading the live BTC tick so
        the Pearson-r between SPY returns and BTC returns is always fresh.  No-op
        when the sentinel is unavailable or price <= 0.

        Parameters
        ----------
        price : float — current BTC/USDT mid-price (bid+ask)/2 or last trade.
        """
        try:
            if price > 0:
                self.p42_sentinel.record_btc_price(price)
        except Exception as exc:
            log.debug("[P42-CORR] feed_btc_price_for_correlation error: %s", exc)

    # ── [/P42-SHADOW] ─────────────────────────────────────────────────────────

    async def get_ofi_score(
        self, symbol: str
    ) -> tuple:
        """
        [P38-OFI / P47] Return the current Order Flow Imbalance snapshot for symbol.

        Returns
        -------
        Tuple[ofi_score, bid_wall_pulled, ask_wall_pulled]

        ofi_score ∈ [-1.0, +1.0]:
            Positive → net buy-side delta pressure.
            Negative → net sell-side delta pressure.

        [P47] When a Rust bridge reference has been injected (hub._p47_bridge) and
        P47_ENABLE=1, the bridge-computed OFI is returned when fresh
        (age ≤ P47_STALENESS_SECS).  Falls back to Python OBIMonitor silently.

        Returns (0.0, False, False) when no monitor exists or fewer than two
        snapshots have been received (cold-start guard — safe neutral value).
        """
        # ── [P47] Bridge-computed fast path ───────────────────────────────────
        try:
            _bridge47 = getattr(self, "_p47_bridge", None)
            if _bridge47 is not None:
                _tox47, _ofi47, _bp47, _ap47, _fresh = _bridge47.get_p47_microstructure(symbol)
                if _fresh:
                    if getattr(_bridge47, "_p47_enabled", False) and \
                            __import__("os").environ.get("P47_LOG_SOURCE", "0") == "1":
                        __import__("logging").getLogger("data_hub").debug(
                            "[P47] get_ofi_score %s: source=BRIDGE ofi=%.4f "
                            "bid_pull=%s ask_pull=%s", symbol, _ofi47, _bp47, _ap47
                        )
                    return _ofi47, _bp47, _ap47
        except Exception as _p47_exc:
            __import__("logging").getLogger("data_hub").debug(
                "[P47] bridge OFI fallback %s: %s", symbol, _p47_exc
            )
        # ── [/P47] fall through to Python OBIMonitor ──────────────────────────
        try:
            inst = f"{symbol.upper()}-USDT"
            mon  = self._p38_obi.get(inst)
            if mon is None or not mon.ready:
                return 0.0, False, False
            return mon.get_ofi_snapshot()
        except Exception as exc:
            log.debug("[P38-OFI] get_ofi_score error %s: %s", symbol, exc)
            return 0.0, False, False

    async def get_sentiment(self, symbol: str) -> SentimentSnapshot:
        buf = self._sentiment_buf(symbol)
        if buf is None:
            return SentimentSnapshot(
                funding_rate=0.0, net_liq_delta=0.0,
                long_liq_usd=0.0, short_liq_usd=0.0,
            )
        return await buf.snapshot()

    def get_account(self) -> Optional[AccountSnapshot]:
        return self._last_account

    # ── [P15] Public API — Oracle / HV Mode ───────────────────────────────────

    def get_oracle_signal(self, symbol: str):
        """[P15-2] Return the latest valid OracleSignal for symbol, or None."""
        if self.oracle is None:
            return None
        try:
            return self.oracle.latest_signal(symbol)
        except Exception as exc:
            log.debug("[P15] get_oracle_signal %s: %s", symbol, exc)
            return None

    async def get_global_mid_price(self, symbol: str) -> Optional[float]:
        """[P34.1-SYNTH] Synthetic mid-price: weighted average of OKX and Coinbase."""
        now = time.time()
        okx_mid: Optional[float] = None
        cb_mid:  Optional[float] = None
        cb_ts:   Optional[float] = None

        try:
            book = await self.get_order_book(symbol)
            if book and book.bids and book.asks:
                okx_mid = (book.bids[0][0] + book.asks[0][0]) / 2.0
        except Exception as exc:
            log.debug("[P34.1-SYNTH] OKX order-book error %s: %s", symbol, exc)

        try:
            if self.oracle is not None:
                px_entry = self.oracle.latest_price(symbol)
                if px_entry is not None:
                    cb_mid, cb_ts = px_entry
        except Exception as exc:
            log.debug("[P34.1-SYNTH] Coinbase price error %s: %s", symbol, exc)

        okx_stale = okx_mid is None
        cb_stale  = cb_mid is None or cb_ts is None or (now - cb_ts) > P34_STALENESS_SECS

        if okx_stale and cb_stale:
            log.debug("[P34.1-SYNTH] Both sources stale for %s — returning None", symbol)
            return None
        if okx_stale:
            log.info("[P34.1-SYNTH] %s OKX stale → 100%% Coinbase mid=%.4f", symbol, cb_mid)
            return cb_mid
        if cb_stale:
            log.info("[P34.1-SYNTH] %s Coinbase stale → 100%% OKX mid=%.4f", symbol, okx_mid)
            return okx_mid

        global_mid = (okx_mid * P34_OKX_WEIGHT) + (cb_mid * P34_COINBASE_WEIGHT)
        log.debug(
            "[P34.1-SYNTH] %s global_mid=%.4f (OKX=%.4f w=%.2f  CB=%.4f w=%.2f  cb_age=%.2fs)",
            symbol, global_mid, okx_mid, P34_OKX_WEIGHT, cb_mid, P34_COINBASE_WEIGHT,
            now - cb_ts,
        )
        return global_mid

    def is_hv_mode(self) -> bool:
        """[P15-4] True when GlobalTapeAggregator signals High Volatility."""
        if self.global_tape is None:
            return False
        try:
            return self.global_tape.is_high_volatility_mode()
        except Exception as exc:
            log.debug("[P15-4] is_hv_mode error: %s", exc)
            return False

    def hv_stop_extra_pct(self) -> float:
        """[P15-4] Extra stop-gap % to widen when in HV Mode."""
        if self.global_tape is None:
            return 0.0
        try:
            return self.global_tape.hv_stop_extra_pct()
        except Exception:
            return 0.0

    def get_global_tape_status(self) -> dict:
        """[P15-4] Full GlobalTapeAggregator status dict for status cache."""
        _binance_tps = self.get_binance_tps()

        base: dict = {
            "coinbase_tps":         0.0,
            "binance_tps":          _binance_tps,
            "combined_tps":         _binance_tps,
            "high_volatility_mode": False,
            "hv_stop_extra_pct":    0.0,
            "cb_credentials":       self.cb_creds.status_snapshot(),
        }
        if self.global_tape is None:
            return base
        try:
            snap = self.global_tape.status_snapshot()
            snap["cb_credentials"] = self.cb_creds.status_snapshot()
            snap["binance_tps"]  = _binance_tps
            snap["combined_tps"] = snap.get("coinbase_tps", 0.0) + _binance_tps
            return snap
        except Exception as exc:
            log.debug("[P15-4] get_global_tape_status error: %s", exc)
            return base

    # ── [TASK-4-B] Unified risk state snapshot ────────────────────────────────

    def get_risk_status_snapshot(self) -> dict:
        """[TASK-4-B] Unified Phase 4/7/15/20 risk state for the GUI."""
        snap: dict = {
            "cb_tripped":           False,
            "cb_drawdown_pct":      0.0,
            "cb_peak_equity":       0.0,
            "cb_latest_equity":     0.0,
            "cb_tripped_at":        0.0,
            "cb_in_grace_period":   False,
            "zombie_active":        False,
            "zombie_pct":           0.0,
            "peak_equity":          0.0,
            "current_equity":       0.0,
            "drawdown_pct":         0.0,
            "emergency_pause":      False,
            "pause_remaining_secs": 0.0,
            "oracle_connected":     False,
            "hv_mode":              False,
            "block_reason":         "none",
        }

        cb = self._risk_cb
        if cb is not None:
            try:
                snap["cb_tripped"] = bool(cb.is_tripped)
                raw_dd = getattr(cb, "last_drawdown_pct", None)
                snap["cb_drawdown_pct"] = float(raw_dd) if raw_dd is not None else 0.0
                raw_pk = getattr(cb, "peak_equity", None)
                snap["cb_peak_equity"] = float(raw_pk) if raw_pk is not None else 0.0
                raw_lt = getattr(cb, "latest_equity", None)
                snap["cb_latest_equity"] = float(raw_lt) if raw_lt is not None else 0.0
                raw_ta = getattr(cb, "tripped_at", None)
                snap["cb_tripped_at"] = float(raw_ta) if raw_ta is not None else 0.0
                if hasattr(cb, "_in_grace") and callable(cb._in_grace):
                    try:
                        snap["cb_in_grace_period"] = bool(cb._in_grace())
                    except Exception as _exc:
                        log.warning("[DATA_HUB] suppressed: %s", _exc)
            except Exception as exc:
                log.debug("[TASK-4] get_risk_status_snapshot CB read: %s", exc)

        executor = self._risk_executor
        if executor is not None:
            try:
                snap["zombie_active"] = bool(getattr(executor, "_p20_zombie_mode", False))
                raw_eq = getattr(executor, "_equity", None)
                cur_eq = float(raw_eq) if raw_eq is not None else 0.0
                snap["current_equity"] = cur_eq if cur_eq >= _RISK_MIN_VALID_EQUITY else 0.0
                brain = getattr(executor, "brain", None)
                if brain is not None:
                    raw_hwm = getattr(brain, "peak_equity", None)
                    hwm = float(raw_hwm) if raw_hwm is not None else 0.0
                    snap["peak_equity"] = hwm
                    valid_eq = snap["current_equity"]
                    if hwm > 0.0 and valid_eq > 0.0:
                        try:
                            dd = (hwm - valid_eq) / hwm * 100.0
                            snap["drawdown_pct"] = round(float(dd), 4)
                        except (TypeError, ZeroDivisionError):
                            pass
                risk_mgr = getattr(executor, "_p20_risk_manager", None)
                if risk_mgr is not None:
                    raw_zpct = getattr(risk_mgr, "_zombie_pct", None)
                    snap["zombie_pct"] = float(raw_zpct) if raw_zpct is not None else 0.0
            except Exception as exc:
                log.debug("[TASK-4] get_risk_status_snapshot executor read: %s", exc)

        try:
            snap["emergency_pause"] = bool(self.volatility_guard.emergency_pause)
            snap["pause_remaining_secs"] = float(self.volatility_guard.pause_remaining_secs)
        except Exception as exc:
            log.debug("[TASK-4] get_risk_status_snapshot VolatilityGuard: %s", exc)

        try:
            if self.oracle is not None:
                snap["oracle_connected"] = bool(getattr(self.oracle, "_ws_connected", False))
        except Exception as exc:
            log.debug("[TASK-4] get_risk_status_snapshot oracle: %s", exc)

        try:
            snap["hv_mode"] = self.is_hv_mode()
        except Exception as exc:
            log.debug("[TASK-4] get_risk_status_snapshot hv_mode: %s", exc)

        if snap["zombie_active"]:
            snap["block_reason"] = (
                f"P20_ZOMBIE_MODE "
                f"(drawdown={snap['drawdown_pct']:.2f}% "
                f">= {snap['zombie_pct']:.1f}%)"
            )
        elif snap["cb_tripped"]:
            snap["block_reason"] = (
                f"P7_CIRCUIT_BREAKER "
                f"(hourly_dd={snap['cb_drawdown_pct']:.2f}%)"
            )
        elif snap["emergency_pause"]:
            snap["block_reason"] = (
                f"P4_FLASH_CRASH_PAUSE "
                f"(resumes_in={snap['pause_remaining_secs']:.0f}s)"
            )
        elif snap["hv_mode"]:
            snap["block_reason"] = "P15_HIGH_VOLATILITY_MODE"
        else:
            snap["block_reason"] = "none"

        return snap

    # ── [P36.1-DETECT] Spoof Toxicity Public API ──────────────────────────────

    async def update_spoof_toxicity(self, symbol: str, spoof_prob: float) -> None:
        """[P36.1-DETECT] Write a spoof probability EMA sample for a symbol."""
        _alpha = _env_float("P36_SPOOF_EMA_ALPHA", 0.5)
        try:
            spoof_prob = max(0.0, min(1.0, float(spoof_prob)))
            sym_upper  = symbol.upper()
            async with self._p36_spoof_lock:
                prev = self._p36_spoof_probs.get(sym_upper, 0.0)
                updated = _alpha * spoof_prob + (1.0 - _alpha) * prev
                self._p36_spoof_probs[sym_upper] = round(updated, 4)
            log.info(
                "[P36.1-DETECT] update_spoof_toxicity %s: "
                "raw_prob=%.4f prev_ema=%.4f → new_ema=%.4f (alpha=%.2f)",
                sym_upper, spoof_prob, prev, updated, _alpha,
            )
        except Exception as exc:
            log.warning("[P36.1-DETECT] update_spoof_toxicity error %s: %s", symbol, exc)

    async def get_spoof_probability(self, symbol: str) -> float:
        """[P36.1-DETECT] Return current spoof probability EMA ∈ [0.0, 1.0]."""
        try:
            sym_upper = symbol.upper().replace("-USDT", "").replace("-SWAP", "")
            async with self._p36_spoof_lock:
                return self._p36_spoof_probs.get(sym_upper, 0.0)
        except Exception as exc:
            log.debug("[P36.1-DETECT] get_spoof_probability error %s: %s", symbol, exc)
            return 0.0

    # ── [P36.2-SELFHEAL] Self-Healing Triggered Refresh ──────────────────────

    async def force_immediate_refresh(self, symbol: str) -> None:
        """[P36.2-SELFHEAL] Bypass the timer and fetch fresh price limits immediately."""
        sym_upper = _clean_env(symbol).upper().replace("-USDT", "").replace("-SWAP", "")
        spot_id   = f"{sym_upper}-USDT"
        swap_id   = f"{sym_upper}-USDT-SWAP"
        targets   = {spot_id, swap_id}

        log.info(
            "[P36.2-SELFHEAL] force_immediate_refresh triggered for %s "
            "(spot=%s swap=%s) — fetching fresh price limits now.",
            sym_upper, spot_id, swap_id,
        )

        refreshed_any = False
        for inst_type, inst_id in [("SPOT", spot_id), ("SWAP", swap_id)]:
            try:
                d = await self.rest.get(
                    "/api/v5/market/tickers",
                    params={"instType": inst_type}, auth=False,
                )
                rows = d.get("data") or []
                async with self.instrument_cache._price_limits_lock:
                    for row in rows:
                        iid = row.get("instId", "")
                        if iid not in targets:
                            continue
                        try:
                            buy_lmt  = float(row.get("buyLmt")  or 0)
                            sell_lmt = float(row.get("sellLmt") or 0)
                        except (TypeError, ValueError):
                            continue
                        if buy_lmt > 0 or sell_lmt > 0:
                            self.instrument_cache._price_limits[iid] = (buy_lmt, sell_lmt)
                            meta = self.instrument_cache._cache.get(iid)
                            if meta is not None:
                                meta.buyLmt  = buy_lmt
                                meta.sellLmt = sell_lmt
                            log.info(
                                "[P36.2-SELFHEAL] %s: price limits refreshed "
                                "buyLmt=%.8f sellLmt=%.8f",
                                iid, buy_lmt, sell_lmt,
                            )
                            refreshed_any = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "[P36.2-SELFHEAL] REST fetch failed for %s %s: %s (non-fatal).",
                    inst_type, sym_upper, exc,
                )

        if not refreshed_any:
            log.warning(
                "[P36.2-SELFHEAL] %s: no fresh price limits found in REST response "
                "— clamping will use previously cached values.", sym_upper,
            )

    # ── [P15-2] Oracle cancel ──────────────────────────────────────────────────

    async def cycle_oracle_cancel(self, symbols: List[str]) -> None:
        """[P15-2] Cancel ALL open OKX BUY limit orders for the given symbols.

        [P15-2-THROTTLE] Per-symbol REST call guard: skips the full cancel sweep
        if called within 20s of the previous call for the same symbol.  Prevents
        hammering /api/v5/trade/orders-pending on every executor cycle (~0.5s)
        while a 30s whale signal is live.  The window is 20s (< 30s whale TTL)
        so a genuine new whale within the TTL window still fires once.
        """
        import time as _t
        _now = _t.time()
        _CANCEL_COOLDOWN_SECS = 20.0
        if not hasattr(self, "_oracle_cancel_ts"):
            self._oracle_cancel_ts: dict = {}

        # Filter to only symbols not recently cancelled
        symbols_due = [
            s for s in symbols
            if _now - self._oracle_cancel_ts.get(s.upper(), 0.0) >= _CANCEL_COOLDOWN_SECS
        ]
        if not symbols_due:
            log.debug(
                "[P15-2] cycle_oracle_cancel: ALL symbols throttled (< %.0fs since last cancel) "
                "— skipping REST sweep for %s", _CANCEL_COOLDOWN_SECS, symbols,
            )
            return

        for s in symbols_due:
            self._oracle_cancel_ts[s.upper()] = _now

        log.warning(
            "[P15-2] cycle_oracle_cancel: Coinbase Whale SELL — "
            "cancelling all OKX BUY limit orders for %s (throttled symbols skipped)", symbols_due,
        )
        for inst_type in ("SPOT", "SWAP"):
            try:
                d      = await self.rest.get(
                    "/api/v5/trade/orders-pending",
                    params={"instType": inst_type},
                )
                orders = d.get("data") or []
                for o in orders:
                    inst_id = o.get("instId", "")
                    sym = inst_id.split("-")[0]
                    if symbols and sym not in [s.upper() for s in symbols]:
                        continue
                    if o.get("side") != "buy":
                        continue
                    try:
                        await self.rest.post(
                            "/api/v5/trade/cancel-order",
                            {"instId": inst_id, "ordId": o["ordId"]},
                        )
                        log.info(
                            "[P15-2] Cancelled OKX BUY order %s %s",
                            inst_id, o["ordId"],
                        )
                    except Exception as exc:
                        log.warning(
                            "[P15-2] Cancel failed %s %s: %s",
                            inst_id, o.get("ordId"), exc,
                        )
            except Exception as exc:
                log.error(
                    "[P15-2] cycle_oracle_cancel fetch %s orders failed: %s",
                    inst_type, exc,
                )

    # ── KuCoin backfill ────────────────────────────────────────────────────────

    async def _backfill_kucoin(self) -> None:
        log.info("Starting KuCoin candle backfill for %s…", self.symbols)
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(
            timeout=timeout, headers={"Accept": "application/json"},
        ) as sess:
            for sym in self.symbols:
                inst = f"{sym}-USDT"
                for tf, kc_tf in _KUCOIN_FETCH_TFS.items():
                    n     = 1500
                    end   = int(time.time())
                    start = end - _TF_SECS.get(tf, 3600) * n
                    url   = (
                        f"{KUCOIN_REST}/api/v1/market/candles"
                        f"?symbol={inst}&type={kc_tf}&startAt={start}&endAt={end}"
                    )
                    try:
                        async with sess.get(url) as r:
                            d = await r.json(content_type=None)
                        candles = []
                        for row in d.get("data") or []:
                            try:
                                candles.append(Candle(
                                    symbol=inst, tf=tf, ts=int(row[0]),
                                    open=float(row[1]),  close=float(row[2]),
                                    high=float(row[3]),  low=float(row[4]),
                                    volume=float(row[5]), confirmed=True,
                                ))
                            except (ValueError, IndexError):
                                pass
                        buf = await self._get_buf(inst, tf)
                        await buf.bulk_load(candles)
                        # [FIX-KUCOIN-DB] Also persist to DB so dashboard's
                        # _query() can see this data immediately.  Previously
                        # bulk_load() was memory-only and the dashboard showed
                        # "NO CANDLES" until CI-2 REST completed its first sweep.
                        if candles:
                            try:
                                await self.db.upsert_candles(candles)
                            except Exception as _db_exc:
                                log.warning(
                                    "KuCoin backfill DB upsert %s %s: %s",
                                    sym, tf, _db_exc,
                                )
                        log.info("Backfill %s %s: %d candles", sym, tf, len(candles))
                    except Exception as exc:
                        log.warning("KuCoin backfill %s %s: %s", sym, tf, exc)
                    await asyncio.sleep(0.15)
        for sym in self.symbols:
            try:
                await self._rebuild_aggregated(f"{sym}-USDT")
            except Exception as exc:
                log.error(
                    "[TASK-4] _rebuild_aggregated failed for %s — skipping: %s", sym, exc,
                )

    # ── Startup ────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.cache.connect()
        await self.instrument_cache.warm(self.symbols, include_swap=True)
        try:
            await self.instrument_cache.refresh_price_limits(
                self.symbols, include_swap=True,
            )
            log.info(
                "[P36.1-PRICEGUARD] Initial price limits loaded for %d symbols.",
                len(self.symbols),
            )
        except Exception as _pl_exc:
            log.warning(
                "[P36.1-PRICEGUARD] Initial price limit load failed (non-fatal): %s",
                _pl_exc,
            )
        await self._backfill_kucoin()

        asyncio.create_task(self._market_feed.run(),  name="okx_market_feed")
        asyncio.create_task(self._private_feed.run(), name="okx_private_feed")

        try:
            d = await self.rest.get("/api/v5/account/balance", params={"ccy": "USDT"})
            if d.get("data"):
                detail = d["data"][0]
                avail  = next(
                    (float(x.get("availBal", 0))
                     for x in detail.get("details", []) if x.get("ccy") == "USDT"),
                    0.0,
                )
                snap = AccountSnapshot(
                    ts=time.time(),
                    total_equity=float(detail.get("adjEq") or 0),
                    buying_power=avail,
                    margin_ratio=0.0,
                    positions_raw=detail.get("details", []),
                )
                await self._on_account(snap)
                log.info(
                    "Initial account: equity=$%.2f avail=$%.2f",
                    snap.total_equity, snap.buying_power,
                )
        except Exception as exc:
            log.warning("Initial account REST failed: %s. Relying on WS push.", exc)

        from arbitrator import CoinbaseOracle, GlobalTapeAggregator, Arbitrator

        oracle = CoinbaseOracle(symbols=self.symbols)
        # [FIX-DUAL-ORACLE] Store oracle but do NOT call oracle.start() here.
        # main() wires the whale callback and starts the oracle AFTER hub.start()
        # returns.  Previously DataHub.start() called oracle.start() AND main()
        # called it again → double WebSocket connection on the same object.
        self._oracle_task = None   # will be set by main() after oracle.start()
        self.oracle = oracle

        _stub_arb = Arbitrator(symbols=self.symbols)
        self.global_tape = GlobalTapeAggregator(oracle=oracle, arbitrator=_stub_arb)

        # ── [P42-SHADOW] Start the Global Market Sentinel ─────────────────────
        try:
            await self.p42_sentinel.start()
            log.info("[P42-SHADOW] GlobalMarketSentinel armed. SPY/DXY macro veto active.")
        except Exception as _p42_start_exc:
            log.warning(
                "[P42-SHADOW] GlobalMarketSentinel start failed (non-fatal — "
                "macro veto disabled until next restart): %s", _p42_start_exc,
            )
        # ── [/P42-SHADOW] ─────────────────────────────────────────────────────

        log.info(
            "DataHub live. demo=%s symbols=%s "
            "[P15] Oracle=%s GlobalTape=ready CB_auth=%s",
            self.demo, self.symbols,
            "started", self.cb_creds.is_valid(),
        )

        asyncio.create_task(
            self._instrument_cache_refresh_loop(),
            name="instrument_cache_refresh",
        )

        # ── [TRACK12-PRUNE] On-boot prune + periodic prune loop ───────────────
        # One immediate prune on startup clears any existing backlog accumulated
        # before Track 12 was deployed, without waiting for the first loop tick.
        # Wrapped in try/except so a prune failure can never abort startup.
        try:
            _boot_deleted = await self.db.prune_decision_trace(DT_RETAIN_DAYS)
            log.info(
                "[TRACK12-PRUNE] Boot prune: deleted %d decision_trace row(s) "
                "older than %d day(s).",
                _boot_deleted, DT_RETAIN_DAYS,
            )
        except Exception as _boot_prune_exc:
            log.warning(
                "[TRACK12-PRUNE] Boot prune failed (non-fatal): %s",
                _boot_prune_exc,
            )
        asyncio.create_task(
            self._dt_prune_loop(),
            name="dt_prune_loop",
        )
        # ── [/TRACK12-PRUNE] ──────────────────────────────────────────────────

    async def _instrument_cache_refresh_loop(
        self,
        interval_secs: float = 86_400.0,
    ) -> None:
        """[FIX-INSTRUMENT-CACHE] Refresh SPOT and SWAP instrument metadata periodically."""
        if OKX_DEMO_MODE:
            interval_secs = 1_800.0
            log.info(
                "[FIX-INSTRUMENT-CACHE] Demo mode — "
                "InstrumentCache + price-limit refresh interval set to 30 min (1800 s)."
            )
        else:
            log.info(
                "[FIX-INSTRUMENT-CACHE] InstrumentCache refresh loop started "
                "(interval=%.0fh).", interval_secs / 3600.0,
            )
        while True:
            await asyncio.sleep(interval_secs)
            try:
                log.info("[FIX-INSTRUMENT-CACHE] Refreshing InstrumentCache (SPOT + SWAP)…")
                async with self.instrument_cache._lock:
                    self.instrument_cache._fetched_at.clear()
                await self.instrument_cache.warm(self.symbols, include_swap=True)
                log.info(
                    "[FIX-INSTRUMENT-CACHE] InstrumentCache refreshed OK — %d instruments cached.",
                    len(self.instrument_cache._cache),
                )
                await self.instrument_cache.refresh_price_limits(
                    self.symbols, include_swap=True,
                )
            except asyncio.CancelledError:
                log.info("[FIX-INSTRUMENT-CACHE] InstrumentCache refresh loop cancelled.")
                return
            except Exception as exc:
                log.error(
                    "[FIX-INSTRUMENT-CACHE] InstrumentCache refresh failed: %s "
                    "(will retry in %.0fs).",
                    exc, interval_secs,
                )

    async def _dt_prune_loop(self) -> None:
        """[TRACK12-PRUNE] Periodic background pruning of old decision_trace rows.

        Sleeps for DT_PRUNE_INTERVAL_SECS (default 6 h), then calls
        db.prune_decision_trace(DT_RETAIN_DAYS).  Runs indefinitely until the
        task is cancelled (clean shutdown).

        Design constraints:
          - All exceptions except CancelledError are caught and logged at
            WARNING so the loop continues on the next interval.
          - CancelledError is re-raised so asyncio task cancellation (on
            DataHub.close()) propagates correctly.
          - The loop never calls prune_decision_trace directly — it delegates
            entirely to the fail-open Database method, so no DB exception can
            ever escape this loop.
        """
        log.info(
            "[TRACK12-PRUNE] dt_prune_loop started "
            "(interval=%ds, retain=%dd).",
            DT_PRUNE_INTERVAL_SECS, DT_RETAIN_DAYS,
        )
        while True:
            try:
                await asyncio.sleep(DT_PRUNE_INTERVAL_SECS)
            except asyncio.CancelledError:
                log.info("[TRACK12-PRUNE] dt_prune_loop cancelled.")
                return
            try:
                _deleted = await self.db.prune_decision_trace(DT_RETAIN_DAYS)
                log.info(
                    "[TRACK12-PRUNE] Periodic prune: deleted %d decision_trace "
                    "row(s) older than %d day(s).",
                    _deleted, DT_RETAIN_DAYS,
                )
            except asyncio.CancelledError:
                log.info("[TRACK12-PRUNE] dt_prune_loop cancelled during prune.")
                return
            except Exception as _exc:
                log.warning(
                    "[TRACK12-PRUNE] dt_prune_loop: unexpected error "
                    "(will retry in %ds): %s",
                    DT_PRUNE_INTERVAL_SECS, _exc,
                )

    async def close(self) -> None:
        self._market_feed.stop()
        self._private_feed.stop()
        if self.oracle is not None:
            await self.oracle.stop()
        if self._oracle_task and not self._oracle_task.done():
            self._oracle_task.cancel()
            try:
                await self._oracle_task
            except asyncio.CancelledError:
                pass
        await self.rest.close()
        log.info("DataHub closed.")
