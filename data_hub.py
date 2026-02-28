"""
data_hub.py  —  OKX WebSocket Hub + KuCoin Historical Backfill + InstrumentCache

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
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
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
INSTRUMENT_CACHE_TTL = int  (os.environ.get("INSTRUMENT_CACHE_TTL",    "3600"))
FLASH_CRASH_DROP_PCT = float(os.environ.get("FLASH_CRASH_DROP_PCT",    "5.0"))
FLASH_CRASH_WINDOW   = float(os.environ.get("FLASH_CRASH_WINDOW_SECS", "30.0"))
EMERGENCY_PAUSE_SECS = float(os.environ.get("EMERGENCY_PAUSE_SECS",    "300.0"))

# [P5-1]
SENTIMENT_WINDOW_SECS = float(os.environ.get("SENTIMENT_WINDOW_SECS", "60.0"))

# [P12-2]
TAPE_BUFFER_SECS          = float(os.environ.get("P12_TAPE_BUFFER_SECS",     "60.0"))
TAPE_MAX_EVENTS           = int  (os.environ.get("P12_TAPE_MAX_EVENTS",      "10000"))
TAPE_VELOCITY_WINDOW_SECS = float(os.environ.get("P12_TAPE_VELOCITY_WINDOW", "5.0"))
SWEEP_DIP_PCT             = float(os.environ.get("P12_SWEEP_DIP_PCT",        "0.3"))
SWEEP_BUY_VOL_USD         = float(os.environ.get("P12_SWEEP_BUY_VOL_USD",    "250000"))
SWEEP_WINDOW_SECS         = float(os.environ.get("P12_SWEEP_WINDOW_SECS",    "10.0"))

# [P12-1]
OBI_LEVELS    = int(os.environ.get("P12_OBI_LEVELS",    "10"))
OB_MAX_LEVELS = int(os.environ.get("P12_OB_MAX_LEVELS", "25"))

# [P15-3] Coinbase CDP credentials
COINBASE_API_KEY        = os.environ.get("COINBASE_API_KEY",    "")
COINBASE_API_SECRET_RAW = os.environ.get("COINBASE_API_SECRET", "")

# [TASK-4] Minimum equity value treated as valid for risk snapshot purposes.
# Matches P7_CB_MIN_VALID_EQUITY so all risk subsystems use the same floor.
_RISK_MIN_VALID_EQUITY = float(os.environ.get("P7_CB_MIN_VALID_EQUITY", "10.0"))

# ── [P34.1] Synthetic Mid-Price Discovery ────────────────────────────────────
P34_OKX_WEIGHT      = float(os.environ.get("P34_OKX_WEIGHT",     "0.5"))
P34_COINBASE_WEIGHT = float(os.environ.get("P34_COINBASE_WEIGHT", "0.5"))
P34_STALENESS_SECS  = float(os.environ.get("P34_STALENESS_SECS",  "2.0"))

# ── [P37-VPIN] Volume-Clock & Flow Toxicity config ────────────────────────────
# Bucket size (BTC-equivalent units).  When cumulative trade volume across both
# sides reaches this value, a VPIN sample is sealed and appended to the rolling
# window.  Default 1.0 BTC = one volume-clock tick per bucket.
P37_VPIN_BUCKET_SIZE = float(os.environ.get("P37_VPIN_BUCKET_SIZE", "1.0"))
# Rolling window depth for VPIN history (last N sealed buckets).
# 50 buckets provides a statistically meaningful CDF with low memory overhead.
_P37_VPIN_WINDOW     = int(os.environ.get("P37_VPIN_WINDOW", "50"))

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
    # buyLmt  = maximum price allowed for a BUY order (upper band).
    # sellLmt = minimum price allowed for a SELL order (lower band).
    # Populated by InstrumentCache.refresh_price_limits() via the OKX
    # ticker endpoint.  Default 0.0 = unknown / not yet fetched.
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
        # Active bucket accumulators (reset after each bucket completion).
        self._vpin_buy_vol:   float = 0.0   # buy-side volume in current bucket
        self._vpin_sell_vol:  float = 0.0   # sell-side volume in current bucket
        self._vpin_total_vol: float = 0.0   # total volume in current bucket
        # Sealed VPIN samples — rolling window of last _P37_VPIN_WINDOW buckets.
        # Each entry is a float ∈ [0.0, 1.0]: abs(buy-sell)/total for one bucket.
        self._vpin_buckets: deque = deque(maxlen=_P37_VPIN_WINDOW)
        # ToxicityScore derived from CDF of _vpin_buckets.  Updated every time
        # a new bucket is sealed.  0.0 when < 2 buckets have been sealed.
        self._vpin_toxicity: float = 0.0
        # ── [/P37-VPIN] ──────────────────────────────────────────────────────
    async def add(self, side: str, qty: float, price: float) -> None:
        async with self._lock:
            # [P37-VPIN] Source validation: reject malformed tape events so VPIN
            # cannot be polluted by non-finite values or unknown sides.
            try:
                _side = str(side).strip().lower()
                if _side.startswith("b"):
                    _side = "buy"
                elif _side.startswith("s"):
                    _side = "sell"
                else:
                    return
                _qty = float(qty)
                _px  = float(price)
                if _qty <= 0.0 or _px <= 0.0:
                    return
                import math as _math
                if not (_math.isfinite(_qty) and _math.isfinite(_px)):
                    return
            except Exception:
                return

            self._events.append(
                TapeEvent(ts=time.time(), side=_side, qty=_qty,
                          price=_px, usd=_qty * _px)
            )
            # ── [P37-VPIN] Volume-Clock accumulation ─────────────────────────
            # Classify trade direction into the active bucket.
            try:
                if _side == "buy":
                    self._vpin_buy_vol  += _qty
                else:
                    self._vpin_sell_vol += _qty
                self._vpin_total_vol += _qty

                # Bucket completion: seal when cumulative volume crosses threshold.
                if self._vpin_total_vol >= P37_VPIN_BUCKET_SIZE:
                    _total = self._vpin_total_vol
                    _buy   = self._vpin_buy_vol
                    _sell  = self._vpin_sell_vol

                    # VPIN formula with zero-division guard.
                    if _total > 0.0:
                        _vpin = abs(_buy - _sell) / _total
                        _vpin = max(0.0, min(1.0, _vpin))  # clamp to [0, 1]
                    else:
                        _vpin = 0.0

                    self._vpin_buckets.append(_vpin)
                    self._vpin_toxicity = self._compute_toxicity_score(_vpin)

                    # Reset bucket accumulators for the next volume-clock tick.
                    self._vpin_buy_vol   = 0.0
                    self._vpin_sell_vol  = 0.0
                    self._vpin_total_vol = 0.0
            except Exception:
                pass  # VPIN accumulation is always non-fatal
            # ── [/P37-VPIN] ──────────────────────────────────────────────────


    def _compute_toxicity_score(self, latest_vpin: float) -> float:
        """
        [P37-VPIN] Derive ToxicityScore ∈ [0.0, 1.0] from where latest_vpin
        sits in the empirical CDF of the current _vpin_buckets history.

        CDF position = fraction of historical buckets with VPIN ≤ latest_vpin.
        A score of 0.90 means that 90% of recent volume-clock buckets had lower
        order-flow imbalance than the current one — i.e. the current bucket is
        in the 90th percentile of toxicity.

        Safeguards
        ----------
        • Empty or single-bucket history → returns 0.0 (insufficient data).
        • latest_vpin clamped to [0.0, 1.0] before CDF lookup.
        • Zero-division impossible: denominator is always len(_vpin_buckets) > 0.
        """
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
        """
        [P37-VPIN] Return the current ToxicityScore ∈ [0.0, 1.0].

        Thread-safe: reads the cached _vpin_toxicity scalar which is updated
        atomically inside the asyncio.Lock in add().  Callers must not hold the
        lock themselves when calling this method (it is a simple attribute read).

        Returns 0.0 until at least two volume-clock buckets have been sealed,
        so the VetoArbitrator never acts on a single-sample artefact.
        """
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
        # [P36.1-PRICEGUARD] Per-instrument price limits fetched from OKX tickers.
        # Key = inst_id (e.g. "BTC-USDT"), Value = (buyLmt, sellLmt).
        # Populated by refresh_price_limits(); consumed by executor._execute_order
        # and the Whale Sniper to prevent OKX Error 51006.
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

    # ── [P36.1-PRICEGUARD] Price Limit Cache ─────────────────────────────────

    async def refresh_price_limits(
        self,
        symbols: List[str],
        include_swap: bool = True,
    ) -> None:
        """
        [P36.1-PRICEGUARD] Fetch OKX exchange-enforced price bands (buyLmt /
        sellLmt) from the /api/v5/market/tickers endpoint and store them in the
        _price_limits cache keyed by inst_id.

        These limits change throughout the day as OKX adjusts its price bands.
        In demo mode the executor refreshes them every 30 minutes (1800 s);
        in live mode every 24 hours is sufficient.

        Parameters
        ----------
        symbols      : list of base symbols, e.g. ["BTC", "ETH"]
        include_swap : if True, also fetch SWAP ticker limits
        """
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
                            # Also propagate into InstrumentMeta if cached
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
        """
        [P36.1-PRICEGUARD] Return the cached (buyLmt, sellLmt) for an
        instrument.  Returns (0.0, 0.0) when the limits have not yet been
        fetched — callers must treat a 0.0 as 'unknown / skip clamping'.

        Parameters
        ----------
        inst_id : full OKX instrument ID, e.g. "BTC-USDT" or "BTC-USDT-SWAP"

        Returns
        -------
        Tuple[float, float] — (buyLmt, sellLmt)
        """
        return self._price_limits.get(inst_id, (0.0, 0.0))

    # ── [/P36.1-PRICEGUARD] ──────────────────────────────────────────────────

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


class Database:
    def __init__(self, path: str = "powertrader.db"):
        self._path = path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    def _conn_obj(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._path, check_same_thread=False, timeout=30)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            self._conn.executescript(_SCHEMA)
            self._conn.commit()
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

    async def insert_trade(self, t: dict) -> None:
        def _fn():
            conn = self._conn_obj()
            conn.execute(
                "INSERT OR IGNORE INTO trades"
                "(ts,symbol,side,qty,price,cost_basis,pnl_pct,"
                "realized_usd,tag,order_id,inst_type) VALUES"
                "(:ts,:symbol,:side,:qty,:price,:cost_basis,:pnl_pct,"
                ":realized_usd,:tag,:order_id,:inst_type)",
                t,
            )
            conn.commit()
        await self._run(_fn)

    async def insert_snapshot(self, s: dict) -> None:
        def _fn():
            conn = self._conn_obj()
            conn.execute(
                "INSERT INTO account_snapshots"
                "(ts,total_equity,buying_power,margin_ratio)"
                " VALUES(:ts,:total_equity,:buying_power,:margin_ratio)",
                s,
            )
            conn.commit()
        await self._run(_fn)


# ── Redis cache ────────────────────────────────────────────────────────────────

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
            except Exception:
                pass


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
    """

    def __init__(
        self,
        symbols:    List[str],
        timeframes: List[str],
        demo: bool = OKX_DEMO_MODE,
    ):
        # Sanitize all incoming symbols: strip whitespace and literal quote chars
        # that can survive .env parsing (e.g. SYMBOLS="BTC,ETH" → ['BTC', 'ETH']).
        self.symbols    = [_sanitize_sym(s).upper() for s in symbols if _sanitize_sym(s)]
        self.timeframes = timeframes
        self.demo       = demo

        self.db               = Database()
        self.cache            = Cache()
        self.rest             = OKXRestClient()
        self.instrument_cache = InstrumentCache(self.rest)
        self.volatility_guard = VolatilityGuard()

        # [P5-1] Sentiment buffers
        self._sentiment: Dict[str, SentimentBuffer] = {
            f"{s}-USDT": SentimentBuffer() for s in self.symbols
        }
        # [P12-2] Tape buffers
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

        # [P15-3] Credential validator — always initialised so status_snapshot is safe
        self.cb_creds: CoinbaseCDPCredentials = CoinbaseCDPCredentials()

        # [P15-1] Oracle and [P15-4] GlobalTapeAggregator
        # These are None until start() is called (requires event loop).
        self.oracle      = None   # CoinbaseOracle | None
        self.global_tape = None   # GlobalTapeAggregator | None

        self._oracle_task: Optional[asyncio.Task] = None

        # [TASK-4-A] Risk state references — injected by main_p20 after both
        # objects exist (after gate.install() in main_p20.py::main()).
        # Wire with:
        #   hub._risk_executor = executor
        #   hub._risk_cb       = cb
        self._risk_executor = None   # Executor | None
        self._risk_cb       = None   # CircuitBreaker | None

        # ── [P36.1-DETECT] Spoof Toxicity Store ──────────────────────────────
        # Per-symbol spoof probability written by the Executor's Mimic Order
        # Engine (_p36_run_mimic_test) and consumed by VetoArbitrator.
        # Values decay toward 0.0 on every clean (non-spoof) cycle and spike
        # to 1.0 when a spoof wall evaporation is detected.
        # Protected by an asyncio.Lock for thread-safe concurrent access.
        self._p36_spoof_probs: Dict[str, float] = {}
        self._p36_spoof_lock: asyncio.Lock = asyncio.Lock()
        # ── [/P36.1-DETECT] ──────────────────────────────────────────────────

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
        # [GHOST-STATE] Validate equity before storing and broadcasting.
        # If the WS delivers a sub-floor ghost value, we still emit it so that
        # downstream subscribers (executor._on_account_update) can apply their
        # own state-aware validation — but we do NOT overwrite _last_account
        # with a ghost value, which would corrupt the public API.
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
        # Sanitize incoming symbol — strip whitespace and literal quotes that
        # may survive from .env parsing (e.g.  P35_HEDGE_SYMBOL="BTC-USDT-SWAP").
        symbol = _sanitize_sym(symbol)
        inst   = f"{symbol.upper()}-USDT"
        cached = await self.cache.get(f"tick:{inst}")
        if cached:
            try:
                return Tick(symbol=inst, **cached)
            except Exception as exc:
                log.debug("get_tick: cached Tick construction failed %s: %s", inst, exc)
        try:
            d    = await self.rest.get("/api/v5/market/ticker", params={"instId": inst})
            data = d.get("data") or []
            # ── [P36.1-FALLBACK] Prevent raw index access on empty lists ──────
            # An empty data list is a valid OKX response for unknown instruments
            # and must never cause an IndexError that crashes the data thread.
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
        """
        [ROBUSTNESS] Return the last-traded price for *symbol*, or ``None`` when
        the tick cannot be retrieved.

        Thin wrapper around ``get_tick`` that:
          • Sanitizes the symbol (strips whitespace + literal quotes from .env).
          • Catches ALL exceptions so a failure for the hedge symbol never crashes
            the DataHub cycle.
          • Returns None — never raises — so callers can do a simple null-check.

        Designed for use by ``PortfolioGovernor._p35_evaluate_and_hedge`` and any
        other location that needs a scalar price for the hedge symbol.
        """
        try:
            tick = await self.get_tick(symbol)
            if tick is None:
                log.warning(
                    "[PRICE-CHECK] get_price: no tick for %s — returning None.", symbol,
                )
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
        [P37-VPIN] Return the Volume-Clock ToxicityScore ∈ [0.0, 1.0] for a
        symbol.

        ToxicityScore is the CDF position of the most-recently-sealed VPIN
        bucket within the rolling 50-bucket history.  A score near 1.0 indicates
        that the current order-flow imbalance (abs(BuyVol-SellVol)/TotalVol) is
        at the extreme upper tail of recent history — a reliable signal of
        informed, directional institutional trading.

        Safeguards
        ----------
        • Returns 0.0 for an unknown symbol (no tape buffer).
        • Returns 0.0 until at least two buckets are sealed (insufficient data).
        • All exceptions are caught and yield 0.0 so the caller is never blocked.

        Parameters
        ----------
        symbol : str — base symbol, e.g. "BTC" or "BTC-USDT"

        Returns
        -------
        float ∈ [0.0, 1.0] — 0.0 = clean (uninformed) flow, 1.0 = maximally
        toxic (perfectly one-sided institutional pressure).
        """
        try:
            buf = self._tape_buf(symbol)
            if buf is None:
                return 0.0
            return buf.get_flow_toxicity()
        except Exception as exc:
            log.debug("[P37-VPIN] get_flow_toxicity error %s: %s", symbol, exc)
            return 0.0

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
        """
        [P15-2] Return the latest valid OracleSignal for symbol, or None.
        Safe to call even before oracle is initialised.
        """
        if self.oracle is None:
            return None
        try:
            return self.oracle.latest_signal(symbol)
        except Exception as exc:
            log.debug("[P15] get_oracle_signal %s: %s", symbol, exc)
            return None

    async def get_global_mid_price(self, symbol: str) -> Optional[float]:
        """
        [P34.1-SYNTH] Synthetic mid-price: weighted average of OKX and Coinbase.

        Weights are configured via P34_OKX_WEIGHT / P34_COINBASE_WEIGHT env vars
        (default 50/50).  If one source is stale (>P34_STALENESS_SECS), the weight
        shifts 100% to the fresh source.  Returns None only when both sources are
        unavailable or stale.

        Staleness definition:
            OKX    — order book returned no best bid/ask
            Coinbase — oracle.latest_price() returns None or timestamp too old
        """
        now = time.time()
        okx_mid: Optional[float] = None
        cb_mid:  Optional[float] = None
        cb_ts:   Optional[float] = None

        # ── OKX mid-price ────────────────────────────────────────────────────
        try:
            book = await self.get_order_book(symbol)
            if book and book.bids and book.asks:
                okx_mid = (book.bids[0][0] + book.asks[0][0]) / 2.0
        except Exception as exc:
            log.debug("[P34.1-SYNTH] OKX order-book error %s: %s", symbol, exc)

        # ── Coinbase last-trade mid-price ────────────────────────────────────
        try:
            if self.oracle is not None:
                px_entry = self.oracle.latest_price(symbol)
                if px_entry is not None:
                    cb_mid, cb_ts = px_entry
        except Exception as exc:
            log.debug("[P34.1-SYNTH] Coinbase price error %s: %s", symbol, exc)

        # ── Staleness flags ──────────────────────────────────────────────────
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

        # ── Both fresh: weighted average ─────────────────────────────────────
        global_mid = (okx_mid * P34_OKX_WEIGHT) + (cb_mid * P34_COINBASE_WEIGHT)
        log.debug(
            "[P34.1-SYNTH] %s global_mid=%.4f (OKX=%.4f w=%.2f  CB=%.4f w=%.2f  "
            "cb_age=%.2fs)",
            symbol, global_mid,
            okx_mid, P34_OKX_WEIGHT,
            cb_mid,  P34_COINBASE_WEIGHT,
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
        base: dict = {
            "coinbase_tps":         0.0,
            "binance_tps":          0.0,
            "combined_tps":         0.0,
            "high_volatility_mode": False,
            "hv_stop_extra_pct":    0.0,
            "cb_credentials":       self.cb_creds.status_snapshot(),
        }
        if self.global_tape is None:
            return base
        try:
            snap = self.global_tape.status_snapshot()
            snap["cb_credentials"] = self.cb_creds.status_snapshot()
            return snap
        except Exception as exc:
            log.debug("[P15-4] get_global_tape_status error: %s", exc)
            return base

    # ── [TASK-4-B] Unified risk state snapshot ────────────────────────────────

    def get_risk_status_snapshot(self) -> dict:
        """
        [TASK-4-B] Returns a unified risk-state dict that merges:
          • Phase 4  — VolatilityGuard (emergency_pause, pause_remaining_secs)
          • Phase 7  — CircuitBreaker  (cb_tripped, cb_drawdown_pct, …)
          • Phase 15 — CoinbaseOracle  (oracle_connected, hv_mode)
          • Phase 20 — Zombie Mode / HWM (zombie_active, drawdown_pct, …)

        This is the single source of truth for the GUI's "block reason" banner
        and for any diagnostic tooling that needs to know WHY entries are gated.

        All fields default to safe, falsy values when the referenced objects
        have not yet been injected or have not yet received a real reading —
        no field ever contains None; every numeric field is a float.

        Wire in main_p20.py immediately after gate.install():
            hub._risk_executor = executor
            hub._risk_cb       = cb

        Returns
        -------
        dict
            cb_tripped           bool   CircuitBreaker has fired
            cb_drawdown_pct      float  last measured hourly drawdown %
            cb_peak_equity       float  CB high-water mark
            cb_latest_equity     float  CB latest equity reading
            cb_tripped_at        float  unix ts of last trip (0.0 = never)
            cb_in_grace_period   bool   startup grace window still active
            zombie_active        bool   P20 Zombie Mode is active
            zombie_pct           float  configured zombie trigger threshold %
            peak_equity          float  brain HWM (P20 source of truth)
            current_equity       float  executor._equity (last valid reading)
            drawdown_pct         float  (HWM - equity) / HWM * 100
            emergency_pause      bool   VolatilityGuard flash-crash pause
            pause_remaining_secs float  seconds until emergency_pause lifts
            oracle_connected     bool   Coinbase WS is live
            hv_mode              bool   GlobalTapeAggregator HV flag
            block_reason         str    primary human-readable block reason
        """
        # ── Safe defaults — no field is ever None ─────────────────────────────
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

        # ── Phase 7: CircuitBreaker ────────────────────────────────────────────
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

                # GracedCircuitBreaker exposes _in_grace(); plain CB does not.
                if hasattr(cb, "_in_grace") and callable(cb._in_grace):
                    try:
                        snap["cb_in_grace_period"] = bool(cb._in_grace())
                    except Exception:
                        pass
            except Exception as exc:
                log.debug("[TASK-4] get_risk_status_snapshot CB read: %s", exc)

        # ── Phase 20: Zombie Mode / HWM / Executor equity ─────────────────────
        executor = self._risk_executor
        if executor is not None:
            try:
                snap["zombie_active"] = bool(
                    getattr(executor, "_p20_zombie_mode", False)
                )

                raw_eq = getattr(executor, "_equity", None)
                cur_eq = float(raw_eq) if raw_eq is not None else 0.0
                # Only surface equity values that passed the min-valid floor.
                if cur_eq >= _RISK_MIN_VALID_EQUITY:
                    snap["current_equity"] = cur_eq
                else:
                    snap["current_equity"] = 0.0

                # brain.peak_equity may be a guarded property — read safely.
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
                log.debug(
                    "[TASK-4] get_risk_status_snapshot executor read: %s", exc
                )

        # ── Phase 4: VolatilityGuard ───────────────────────────────────────────
        try:
            snap["emergency_pause"] = bool(self.volatility_guard.emergency_pause)
            snap["pause_remaining_secs"] = float(
                self.volatility_guard.pause_remaining_secs
            )
        except Exception as exc:
            log.debug("[TASK-4] get_risk_status_snapshot VolatilityGuard: %s", exc)

        # ── Phase 15: Oracle connectivity ─────────────────────────────────────
        try:
            if self.oracle is not None:
                snap["oracle_connected"] = bool(
                    getattr(self.oracle, "_ws_connected", False)
                )
        except Exception as exc:
            log.debug("[TASK-4] get_risk_status_snapshot oracle: %s", exc)

        # ── Phase 15-4: HV Mode ───────────────────────────────────────────────
        try:
            snap["hv_mode"] = self.is_hv_mode()
        except Exception as exc:
            log.debug("[TASK-4] get_risk_status_snapshot hv_mode: %s", exc)

        # ── Unified block_reason — priority order ─────────────────────────────
        # Zombie Mode > CircuitBreaker > EmergencyPause > HV Mode > none
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
        """
        [P36.1-DETECT] Write a spoof probability sample for a symbol.

        Called by the Executor's _p36_run_mimic_test() after each Passive Spoof
        Test completes.  The value is stored in _p36_spoof_probs and consumed by
        VetoArbitrator.set_spoof_probability() on the next compute_p_success()
        call.

        A simple Exponential Moving Average (alpha=0.5) is applied so that a
        single clean cycle quickly reduces an elevated spoof signal rather than
        requiring manual reset.  Alpha can be adjusted via the P36_SPOOF_EMA_ALPHA
        env var (defaults to 0.5).

        Parameters
        ----------
        symbol     : str   — base symbol, e.g. "BTC"
        spoof_prob : float — detected probability ∈ [0.0, 1.0];
                             1.0 = wall evaporated immediately (confirmed spoof),
                             0.0 = wall held (not a spoof this cycle).
        """
        _alpha = float(os.environ.get("P36_SPOOF_EMA_ALPHA", "0.5"))
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
        """
        [P36.1-DETECT] Return the current spoof probability EMA for a symbol.

        Parameters
        ----------
        symbol : str — base symbol, e.g. "BTC"

        Returns
        -------
        float — EMA spoof probability ∈ [0.0, 1.0].  0.0 for an unknown symbol.
        """
        try:
            sym_upper = symbol.upper().replace("-USDT", "").replace("-SWAP", "")
            async with self._p36_spoof_lock:
                return self._p36_spoof_probs.get(sym_upper, 0.0)
        except Exception as exc:
            log.debug("[P36.1-DETECT] get_spoof_probability error %s: %s", symbol, exc)
            return 0.0

    # ── [P36.2-SELFHEAL] Self-Healing Triggered Refresh ──────────────────────

    async def force_immediate_refresh(self, symbol: str) -> None:
        """
        [P36.2-SELFHEAL] Bypass the 1800-second timer and immediately fetch
        fresh buyLmt / sellLmt price bands for ``symbol`` from the OKX REST API.

        Called by the Executor whenever an sCode 51006 rejection is detected so
        the next order has accurate, up-to-date price-limit data without waiting
        for the next scheduled InstrumentCache refresh cycle.

        Strategy
        --------
        1. Build the OKX inst_id strings for SPOT and SWAP variants of symbol.
        2. Call /api/v5/market/tickers for both instTypes and filter to just the
           two relevant instruments (avoids fetching the full ticker list when
           only a single symbol is needed).
        3. Update _price_limits cache and the InstrumentMeta.buyLmt / sellLmt
           fields atomically under the price_limits_lock.
        4. Log the refreshed bands so operators can confirm the update.

        All exceptions are caught internally; failure is non-fatal (the Price
        Limit Guard will use the previous cached values on the next order).

        Parameters
        ----------
        symbol : str — base symbol, e.g. "BTC" or "ETH"
        """
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
                            # Propagate into InstrumentMeta if available.
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

    # ── [/P36.2-SELFHEAL] ────────────────────────────────────────────────────

    # ── [P15-2] Oracle cancel ──────────────────────────────────────────────────

    async def cycle_oracle_cancel(self, symbols: List[str]) -> None:
        """
        [P15-2] Called by Executor when master.cancel_buys_flag is True.

        Cancels ALL open OKX BUY limit orders for the given symbols via REST.
        Failures per symbol are caught and logged without stopping the loop.

        ⚠  This only cancels OKX orders. It does NOT interact with Coinbase.
        """
        log.warning(
            "[P15-2] cycle_oracle_cancel: Coinbase Whale SELL — "
            "cancelling all OKX BUY limit orders for %s", symbols,
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
                        log.info("Backfill %s %s: %d candles", sym, tf, len(candles))
                    except Exception as exc:
                        log.warning("KuCoin backfill %s %s: %s", sym, tf, exc)
                    await asyncio.sleep(0.15)
        # [TASK-4] Per-symbol exception isolation: a failure rebuilding aggregated
        # candles for one instrument must never block the remaining symbols.
        for sym in self.symbols:
            try:
                await self._rebuild_aggregated(f"{sym}-USDT")
            except Exception as exc:
                log.error(
                    "[TASK-4] _rebuild_aggregated failed for %s — skipping, "
                    "bot continues: %s", sym, exc,
                )

    # ── Startup ────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self.cache.connect()
        await self.instrument_cache.warm(self.symbols, include_swap=True)
        # [P36.1-PRICEGUARD] Populate initial buyLmt / sellLmt price bands
        # at startup so the Price Limit Guard has data on the very first order.
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
                "[P36.1-PRICEGUARD] Initial price limit load failed "
                "(non-fatal — limits will be fetched on first refresh cycle): %s",
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

        # [P15] Initialise Coinbase Oracle and GlobalTapeAggregator
        from arbitrator import CoinbaseOracle, GlobalTapeAggregator, Arbitrator

        oracle = CoinbaseOracle(symbols=self.symbols)
        await oracle.start()
        self._oracle_task = asyncio.create_task(
            oracle.run(), name="p15_coinbase_oracle",
        )
        self.oracle = oracle

        _stub_arb = Arbitrator(symbols=self.symbols)
        self.global_tape = GlobalTapeAggregator(oracle=oracle, arbitrator=_stub_arb)

        log.info(
            "DataHub live. demo=%s symbols=%s "
            "[P15] Oracle=%s GlobalTape=ready CB_auth=%s",
            self.demo, self.symbols,
            "started", self.cb_creds.is_valid(),
        )

        # [FIX-INSTRUMENT-CACHE] Schedule a background task that refreshes the
        # InstrumentCache every 24 hours so exchange-side changes to minSz /
        # tickSize are picked up without restarting the bot.
        asyncio.create_task(
            self._instrument_cache_refresh_loop(),
            name="instrument_cache_refresh",
        )

    async def _instrument_cache_refresh_loop(
        self,
        interval_secs: float = 86_400.0,  # 24 hours (overridden by demo mode below)
    ) -> None:
        """
        [FIX-INSTRUMENT-CACHE] Background task that refreshes SPOT and SWAP
        instrument metadata from OKX every 24 hours (or 30 minutes in demo mode).

        Exchange-side changes to minSz, lotSz, or tickSize are reflected
        automatically without requiring a bot restart.  A short initial delay
        skips the first refresh (warm() is called during start()).

        [P36.1-PRICEGUARD] Also refreshes buyLmt / sellLmt price bands every
        cycle so the executor's Price Limit Guard has fresh data.

        Demo-Mode Refresh:
            OKX_DEMO_MODE == "1" → interval forced to 1800 s (30 minutes).
            In demo trading the price bands update far more frequently than in
            production and stale limits cause a spike in sCode 51006 rejections.
        """
        # [P36.1-PRICEGUARD] In demo mode, refresh far more often so price
        # limits stay accurate and sCode 51006 rejections are minimised.
        if OKX_DEMO_MODE:
            interval_secs = 1_800.0  # 30 minutes
            log.info(
                "[FIX-INSTRUMENT-CACHE] Demo mode detected — "
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
                log.info(
                    "[FIX-INSTRUMENT-CACHE] Refreshing InstrumentCache "
                    "(SPOT + SWAP) …"
                )
                # Force re-fetch by clearing the TTL timestamps so _fetch()
                # does not short-circuit.
                async with self.instrument_cache._lock:
                    self.instrument_cache._fetched_at.clear()
                await self.instrument_cache.warm(self.symbols, include_swap=True)
                log.info(
                    "[FIX-INSTRUMENT-CACHE] InstrumentCache refreshed OK — "
                    "%d instruments cached.",
                    len(self.instrument_cache._cache),
                )
                # [P36.1-PRICEGUARD] Also refresh buyLmt / sellLmt price bands
                # so the Price Limit Guard has fresh data for every cycle.
                await self.instrument_cache.refresh_price_limits(
                    self.symbols, include_swap=True,
                )
            except asyncio.CancelledError:
                log.info(
                    "[FIX-INSTRUMENT-CACHE] InstrumentCache refresh loop cancelled."
                )
                return
            except Exception as exc:
                log.error(
                    "[FIX-INSTRUMENT-CACHE] InstrumentCache refresh failed: %s "
                    "(will retry in %.0fs).",
                    exc, interval_secs,
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
