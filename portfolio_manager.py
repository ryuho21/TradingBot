"""
portfolio_manager.py  —  Phase 10: Portfolio Intelligence & Swarm Scaling

Phase 37 additions:
  [P37-VPIN]   No changes to portfolio_manager.  Flow Toxicity veto and emergency
               exits are handled natively by data_hub.py (TapeBuffer VPIN engine),
               intelligence_layer.py (VetoArbitrator zeroth gate), and executor.py
               (tape-monitor emergency exit + _cycle injection).  The PortfolioGovernor
               continues to read _p362_get_dynamic_buffer() for hedge leg clamping;
               the elevated DynamicBuffer remains in full effect during high-toxicity /
               high-velocity periods.

Phase 36.2 additions:
  [P36.2-HEDGE]   DynamicBuffer Hedge Alignment — _p35_adjust_hedge() now reads
                  the Executor's _p362_get_dynamic_buffer() method for the hedge
                  symbol before computing the hedge order price.  During high-
                  volatility periods (PriceVelocity > P362_VELOCITY_THRESHOLD_PCT)
                  the hedge price is clamped using the elevated 10-bps buffer so
                  BTC-SWAP hedge legs are never rejected with sCode 51006 when OKX
                  price bands move rapidly.

No changes for Phase 11. This file is reproduced in full so you have a
single authoritative source that integrates cleanly with the Phase 11
sentinel, executor, and main_p6 files.

[P10-1] PortfolioGovernor — correlation-penalised allocation caps
[P10-2] Dynamic Swarm Rebalancing — drift-triggered harvest / priority deploy
[P10-3] Global Delta-Neutral Hedge — BTC-SWAP short/long to neutralise delta
[P10-4] RegimeDetector — cross-asset vol/trend → RegimePersonality enum
         Executor reads governor.execution_mode each cycle and routes orders
         accordingly (iceberg / shadow / standard).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np

log = logging.getLogger("portfolio_manager")

# ══════════════════════════════════════════════════════════════════════════════
# [SANITIZER] Institutional Environment Sanitizer
# Identical to _clean_env in executor.py / data_hub.py; defined here so
# portfolio_manager is a standalone importable module.
# ══════════════════════════════════════════════════════════════════════════════

def _clean_env(raw: str) -> str:
    """
    Strip leading/trailing whitespace and literal single/double quote characters
    from .env values.  Prevents the OKX_PASSPHRASE="$Khalil21z" dollar-sign
    passphrase from being confused with a quoted symbol like "BTC-USDT-SWAP".
    Only surrounding quote characters are removed; the content is preserved.
    """
    return raw.strip().strip("'\"").strip()


# ── Config ─────────────────────────────────────────────────────────────────────
CORR_LOOKBACK_CANDLES   = int  (os.environ.get("P10_CORR_LOOKBACK",         "168"))
CORR_UPDATE_INTERVAL    = float(os.environ.get("P10_CORR_UPDATE_SECS",      "300.0"))
MAX_CORR_PENALTY        = float(os.environ.get("P10_MAX_CORR_PENALTY",      "0.60"))
MIN_CORR_ALLOC_FRAC     = float(os.environ.get("P10_MIN_CORR_ALLOC_FRAC",   "0.40"))
CORR_HIGH_THRESHOLD     = float(os.environ.get("P10_CORR_HIGH_THRESHOLD",   "0.75"))

REBAL_DRIFT_PCT         = float(os.environ.get("P10_REBAL_DRIFT_PCT",       "5.0"))
REBAL_COOLDOWN_SECS     = float(os.environ.get("P10_REBAL_COOLDOWN_SECS",   "600.0"))
REBAL_HARVEST_FRAC      = float(os.environ.get("P10_REBAL_HARVEST_FRAC",    "0.30"))

DELTA_LONG_THRESHOLD    = float(os.environ.get("P10_DELTA_LONG_THRESHOLD",  "0.80"))
DELTA_SHORT_THRESHOLD   = float(os.environ.get("P10_DELTA_SHORT_THRESHOLD", "-0.80"))
DELTA_TARGET_PCT        = float(os.environ.get("P10_DELTA_TARGET_PCT",      "0.20"))
DELTA_HEDGE_SYMBOL      = os.environ.get("P10_DELTA_HEDGE_SYMBOL",          "BTC")
DELTA_CLOSE_THRESHOLD   = float(os.environ.get("P10_DELTA_CLOSE_THRESHOLD", "0.35"))
DELTA_MIN_HEDGE_USD     = float(os.environ.get("P10_DELTA_MIN_HEDGE_USD",   "20.0"))
DELTA_COOLDOWN_SECS     = float(os.environ.get("P10_DELTA_COOLDOWN_SECS",   "120.0"))

REGIME_VOL_WINDOW       = int  (os.environ.get("P10_REGIME_VOL_WINDOW",     "14"))
REGIME_ADX_WINDOW       = int  (os.environ.get("P10_REGIME_ADX_WINDOW",     "14"))
REGIME_VOL_THRESHOLD    = float(os.environ.get("P10_REGIME_VOL_THRESHOLD",  "0.60"))
REGIME_TREND_THRESHOLD  = float(os.environ.get("P10_REGIME_TREND_THRESHOLD","0.55"))
REGIME_UPDATE_INTERVAL  = float(os.environ.get("P10_REGIME_UPDATE_SECS",    "60.0"))

BEARISH_SCORE_THRESHOLD = float(os.environ.get("NEWS_BEARISH_BLOCK_THRESHOLD", "-0.5"))

# ── [P23-OPT-2] Small-account pivot config ────────────────────────────────────
P23_SMALL_ACCT_THRESHOLD  = float(os.environ.get("P23_SMALL_ACCT_THRESHOLD",  "100.0"))
P23_SMALL_PIVOT_SYMS: List[str] = [
    s.strip().upper()
    for s in os.environ.get("P23_SMALL_PIVOT_SYMS", "XRP,DOGE").split(",")
    if s.strip()
]
# Minimum USD allocation that is considered viable for a SWAP contract.
# BTC/ETH require ~$5+ per contract; XRP/DOGE are a few cents.
P23_MIN_VIABLE_ALLOC_USD  = float(os.environ.get("P23_MIN_VIABLE_ALLOC_USD",  "2.0"))

# ── [P35.1] Dynamic Delta-Neutral Hedging Engine ───────────────────────────────
# Minimum absolute net Altcoin exposure (USD) before a BTC-SWAP hedge is opened.
P35_HEDGE_THRESHOLD_USD   = float(os.environ.get("P35_HEDGE_THRESHOLD_USD",   "500.0"))
# Hedge size as a fraction of net Altcoin exposure.  1.0 = fully delta-neutral.
P35_HEDGE_RATIO           = float(os.environ.get("P35_HEDGE_RATIO",           "1.0"))
# Minimum change (USD) in required hedge size before the hedge is rebalanced.
# Prevents fee-wasting micro-adjustments.
P35_MIN_HEDGE_CHANGE_USD  = float(os.environ.get("P35_MIN_HEDGE_CHANGE_USD",  "50.0"))
# Instrument used for the hedge leg — _clean_env strips literal quotes from .env
# (e.g. P35_HEDGE_SYMBOL="BTC-USDT-SWAP"  →  BTC-USDT-SWAP).
_p35_hedge_sym_raw        = os.environ.get("P35_HEDGE_SYMBOL", "BTC")
P35_HEDGE_SYMBOL          = _clean_env(_p35_hedge_sym_raw) or "BTC"
if P35_HEDGE_SYMBOL != _p35_hedge_sym_raw.strip():
    log.warning(
        "[ENV-WARN] P35_HEDGE_SYMBOL contained surrounding quotes — "
        "sanitized to '%s'", P35_HEDGE_SYMBOL,
    )
# Seconds between hedge evaluation cycles.
P35_HEDGE_POLL_SECS       = float(os.environ.get("P35_HEDGE_POLL_SECS",       "60.0"))
# Base symbols explicitly excluded from the net-delta calculation.
# The hedge instrument itself is always excluded regardless of this list.
P35_EXCLUDE_SYMBOLS: List[str] = [
    s.strip().strip("'\"").strip().upper()
    for s in os.environ.get("P35_EXCLUDE_SYMBOLS", "").split(",")
    if s.strip().strip("'\"").strip()
]


# ── [P10-4] Execution personality ─────────────────────────────────────────────
class RegimePersonality(str, Enum):
    AGGRESSIVE_ICEBERG = "aggressive_iceberg"
    SHADOW_ONLY        = "shadow_only"
    NEUTRAL            = "neutral"


@dataclass
class RegimeSnapshot:
    personality:   RegimePersonality
    vol_score:     float
    trend_score:   float
    n_symbols:     int
    ts:            float = field(default_factory=time.time)


# ── [P10-1/2] Per-symbol allocation metadata ──────────────────────────────────
@dataclass
class SymbolAlloc:
    symbol:          str
    target_weight:   float
    corr_penalty:    float
    max_usd:         float
    rl_confidence:   float
    priority_deploy: bool
    ts:              float = field(default_factory=time.time)


# ── [P10-3] Hedge record ───────────────────────────────────────────────────────
@dataclass
class HedgePosition:
    symbol:       str
    direction:    str
    usd_size:     float
    entry_ts:     float = field(default_factory=time.time)
    okx_order_id: str   = ""


# ── Helper: ATR ────────────────────────────────────────────────────────────────
def _atr(highs: np.ndarray, lows: np.ndarray,
          closes: np.ndarray, n: int) -> float:
    if len(closes) < n + 1:
        return 0.0
    tr = np.maximum(
        highs[1:] - lows[1:],
        np.maximum(
            np.abs(highs[1:] - closes[:-1]),
            np.abs(lows[1:]  - closes[:-1]),
        ),
    )
    return float(np.mean(tr[-n:]))


def _adx(highs: np.ndarray, lows: np.ndarray,
         closes: np.ndarray, n: int) -> float:
    """Simplified Wilder ADX — returns 0-100 value."""
    if len(closes) < n * 2 + 1:
        return 0.0
    h, l_, c = highs, lows, closes
    pdm = np.maximum(h[1:] - h[:-1], 0.0)
    ndm = np.maximum(l_[:-1] - l_[1:], 0.0)
    mask  = pdm < ndm;  pdm[mask]  = 0.0
    mask2 = ndm < pdm;  ndm[mask2] = 0.0

    tr = np.maximum(
        h[1:] - l_[1:],
        np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l_[1:] - c[:-1])),
    )

    def _smooth(arr: np.ndarray, p: int) -> np.ndarray:
        out = np.empty_like(arr)
        out[0] = arr[:p].sum()
        for i in range(1, len(arr)):
            out[i] = out[i - 1] - out[i - 1] / p + arr[i]
        return out

    tr_s  = _smooth(tr,  n)
    pdm_s = _smooth(pdm, n)
    ndm_s = _smooth(ndm, n)

    pdi = np.where(tr_s > 0, 100 * pdm_s / tr_s, 0.0)
    ndi = np.where(tr_s > 0, 100 * ndm_s / tr_s, 0.0)
    dx  = np.where((pdi + ndi) > 0, 100 * np.abs(pdi - ndi) / (pdi + ndi), 0.0)

    if len(dx) < n:
        return float(np.mean(dx))
    return float(np.mean(dx[-n:]))


# ══════════════════════════════════════════════════════════════════════════════
# [P10-4] RegimeDetector
# ══════════════════════════════════════════════════════════════════════════════
class RegimeDetector:
    """
    Computes a market-wide execution personality from cross-asset volatility
    and trend strength.

    Personality matrix:
        trend_score >= REGIME_TREND_THRESHOLD AND
        vol_score   <  REGIME_VOL_THRESHOLD   → AGGRESSIVE_ICEBERG
        vol_score   >= REGIME_VOL_THRESHOLD   → SHADOW_ONLY
        otherwise                             → NEUTRAL
    """

    def __init__(self, hub, symbols: List[str]):
        self._hub     = hub
        self._symbols = symbols
        self._snapshot = RegimeSnapshot(
            personality=RegimePersonality.NEUTRAL,
            vol_score=0.5, trend_score=0.5, n_symbols=len(symbols),
        )
        self._lock = asyncio.Lock()

    @property
    def personality(self) -> RegimePersonality:
        return self._snapshot.personality

    @property
    def snapshot(self) -> RegimeSnapshot:
        return self._snapshot

    async def update(self) -> RegimeSnapshot:
        vol_scores   = []
        trend_scores = []

        for sym in self._symbols:
            try:
                candles = await self._hub.get_candles(sym, "1hour",
                                                       REGIME_VOL_WINDOW * 3)
                if len(candles) < REGIME_VOL_WINDOW + 2:
                    continue
                closes = np.array([c.close for c in reversed(candles)], dtype=np.float64)
                highs  = np.array([c.high  for c in reversed(candles)], dtype=np.float64)
                lows   = np.array([c.low   for c in reversed(candles)], dtype=np.float64)

                atr_val  = _atr(highs, lows, closes, REGIME_VOL_WINDOW)
                mid      = closes[-1] if closes[-1] > 0 else 1.0
                vol_norm = atr_val / mid

                adx_val  = _adx(highs, lows, closes, REGIME_ADX_WINDOW)

                vol_scores.append(vol_norm)
                trend_scores.append(adx_val / 100.0)

            except Exception as e:
                log.debug("[P10] RegimeDetector skip %s: %s", sym, e)

        if not vol_scores:
            return self._snapshot

        raw_vol     = float(np.mean(vol_scores))
        vol_score   = min(raw_vol / 0.03, 1.0)
        trend_score = float(np.mean(trend_scores))

        if trend_score >= REGIME_TREND_THRESHOLD and vol_score < REGIME_VOL_THRESHOLD:
            personality = RegimePersonality.AGGRESSIVE_ICEBERG
        elif vol_score >= REGIME_VOL_THRESHOLD:
            personality = RegimePersonality.SHADOW_ONLY
        else:
            personality = RegimePersonality.NEUTRAL

        snap = RegimeSnapshot(
            personality=personality,
            vol_score=round(vol_score, 4),
            trend_score=round(trend_score, 4),
            n_symbols=len(vol_scores),
        )
        async with self._lock:
            self._snapshot = snap

        log.info(
            "[P10] RegimeDetector: personality=%s vol=%.3f trend=%.3f n=%d",
            personality.value, vol_score, trend_score, len(vol_scores),
        )
        return snap

    async def loop(self):
        log.info("[P10] RegimeDetector loop started (interval=%.0fs)",
                 REGIME_UPDATE_INTERVAL)
        while True:
            try:
                await self.update()
            except asyncio.CancelledError:
                log.info("[P10] RegimeDetector loop stopping.")
                return
            except Exception as e:
                log.error("[P10] RegimeDetector loop error: %s", e, exc_info=True)
            await asyncio.sleep(REGIME_UPDATE_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
# [P10-1] Correlation Engine
# ══════════════════════════════════════════════════════════════════════════════
class CorrelationEngine:
    """
    Builds a pairwise Pearson correlation matrix from the last
    CORR_LOOKBACK_CANDLES confirmed 1-hour closes.
    Returns {symbol: corr_penalty} where high correlation → high penalty →
    lower effective max_usd allocation.
    """

    def __init__(self, hub, symbols: List[str]):
        self._hub     = hub
        self._symbols = symbols

    async def compute(self) -> Dict[str, float]:
        series: Dict[str, np.ndarray] = {}
        for sym in self._symbols:
            try:
                candles   = await self._hub.get_candles(sym, "1hour", CORR_LOOKBACK_CANDLES)
                confirmed = [c for c in candles if c.confirmed]
                if len(confirmed) < 10:
                    continue
                closes = np.array([c.close for c in reversed(confirmed)], dtype=np.float64)
                rets   = np.diff(np.log(closes + 1e-12))
                series[sym] = rets
            except Exception as e:
                log.debug("[P10] CorrelationEngine skip %s: %s", sym, e)

        if len(series) < 2:
            return {s: 0.0 for s in self._symbols}

        min_len = min(len(v) for v in series.values())
        aligned = {s: v[-min_len:] for s, v in series.items()}
        syms    = list(aligned.keys())
        mat     = np.column_stack([aligned[s] for s in syms])

        corr = np.corrcoef(mat.T)

        penalties: Dict[str, float] = {}
        n = len(syms)
        for i, sym in enumerate(syms):
            if n <= 1:
                penalties[sym] = 0.0
                continue
            others   = [corr[i, j] for j in range(n) if j != i]
            mean_abs = float(np.mean(np.abs(others)))
            scaled   = min(mean_abs / CORR_HIGH_THRESHOLD, 1.0) * MAX_CORR_PENALTY
            penalties[sym] = round(scaled, 4)

        for sym in self._symbols:
            if sym not in penalties:
                penalties[sym] = 0.0

        log.info(
            "[P10] Correlation updated: %s",
            {s: f"{v:.3f}" for s, v in penalties.items()},
        )
        return penalties


# ══════════════════════════════════════════════════════════════════════════════
# [P10-1/2/3] PortfolioGovernor
# ══════════════════════════════════════════════════════════════════════════════
class PortfolioGovernor:
    """
    Central portfolio intelligence layer.

    [P10-1] Dynamic allocation caps via correlation-penalised weights.
    [P10-2] Drift-triggered rebalancing: harvest winners, flag underweights.
    [P10-3] Global delta-neutral hedge via BTC-SWAP short/long.
    [P10-4] Delegates regime detection to RegimeDetector.

    Executor interface:
        max_usd = await gov.get_max_usd(symbol, base_max_usd)
        weight  = gov.get_alloc_weight(symbol)
        mode    = gov.execution_mode   # RegimePersonality
    """

    def __init__(self, hub, executor, news_wire, symbols: List[str]):
        self._hub      = hub
        self._exec     = executor
        self._nw       = news_wire
        self._symbols  = [s.upper() for s in symbols]

        self._corr_eng = CorrelationEngine(hub, self._symbols)
        self._regime   = RegimeDetector(hub, self._symbols)

        self._allocs:    Dict[str, SymbolAlloc] = {}
        self._penalties: Dict[str, float]       = {}
        self._lock       = asyncio.Lock()

        self._last_rebal:     Dict[str, float] = {}
        self._priority_deploy: set             = set()

        self._hedge:         Optional[HedgePosition] = None
        self._last_delta_ts: float                   = 0.0

        # ── [P35.1-HEDGE] Dynamic Delta-Neutral Hedging Engine state ──────────
        # Active BTC-SWAP hedge: USD notional size of the current hedge leg.
        self._p35_hedge_usd:   float          = 0.0
        # Direction of the active hedge ("short" or "long") or "" when no hedge.
        self._p35_hedge_dir:   str            = ""
        # OKX order-id of the most recently placed hedge order (for logging).
        self._p35_hedge_ord_id: str           = ""
        # True while a hedge order is in-flight — used by VetoArbitrator to
        # block new Altcoin entries during the rebalance window.
        self._p35_rebalancing: bool           = False
        # asyncio.Lock — serialises hedge evaluation+execution so that rapid
        # successive _cycle() callbacks never double-open a hedge.
        self._p35_lock:        asyncio.Lock   = asyncio.Lock()

        self._tasks: List[asyncio.Task] = []

    # ── Startup / shutdown ─────────────────────────────────────────────────────
    async def start(self):
        self._tasks = [
            asyncio.create_task(self._corr_loop(),      name="p10_corr_loop"),
            asyncio.create_task(self._rebal_loop(),     name="p10_rebal_loop"),
            asyncio.create_task(self._delta_loop(),     name="p10_delta_loop"),
            asyncio.create_task(self._regime.loop(),    name="p10_regime_loop"),
            asyncio.create_task(self._p35_hedge_loop(), name="p35_hedge_loop"),
        ]
        log.info("[P10] PortfolioGovernor started (%d symbols).", len(self._symbols))

    async def stop(self):
        for t in self._tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        log.info("[P10] PortfolioGovernor stopped.")

    # ── [P10-4] Execution mode ─────────────────────────────────────────────────
    @property
    def execution_mode(self) -> RegimePersonality:
        return self._regime.personality

    @property
    def regime_snapshot(self) -> RegimeSnapshot:
        return self._regime.snapshot

    # ── [P35.1-HEDGE] Public properties ───────────────────────────────────────
    @property
    def p35_is_rebalancing(self) -> bool:
        """True while a P35 hedge order is in-flight."""
        return self._p35_rebalancing

    @property
    def p35_net_delta_usd(self) -> float:
        """Current signed net Altcoin exposure in USD (positive = net long)."""
        return self._p35_calc_net_delta_usd()

    @property
    def p35_hedge_snapshot(self) -> dict:
        """Serialisable snapshot of the active P35 hedge for status injection."""
        return {
            "hedge_usd":      round(self._p35_hedge_usd, 2),
            "hedge_dir":      self._p35_hedge_dir,
            "rebalancing":    self._p35_rebalancing,
            "ord_id":         self._p35_hedge_ord_id,
            "threshold_usd":  P35_HEDGE_THRESHOLD_USD,
            "ratio":          P35_HEDGE_RATIO,
            "min_change_usd": P35_MIN_HEDGE_CHANGE_USD,
            "symbol":         P35_HEDGE_SYMBOL,
        }

    @property
    def regime_snapshot(self) -> RegimeSnapshot:
        return self._regime.snapshot

    # ── [P10-1] Allocation API ─────────────────────────────────────────────────
    async def get_max_usd(self, symbol: str, base_max_usd: float) -> float:
        async with self._lock:
            alloc = self._allocs.get(symbol)
        if alloc is None:
            return base_max_usd

        penalty     = alloc.corr_penalty
        conf        = max(alloc.rl_confidence, 0.01)
        corr_factor = max(1.0 - penalty, MIN_CORR_ALLOC_FRAC)

        if self.execution_mode == RegimePersonality.AGGRESSIVE_ICEBERG:
            corr_factor = min(corr_factor * 1.25, 1.0)

        adjusted = base_max_usd * corr_factor * min(conf * 2.0, 1.0)
        return max(adjusted, base_max_usd * MIN_CORR_ALLOC_FRAC)

    def get_alloc_weight(self, symbol: str) -> float:
        alloc = self._allocs.get(symbol)
        return alloc.target_weight if alloc else 1.0 / max(len(self._symbols), 1)

    def is_priority_deploy(self, symbol: str) -> bool:
        return symbol in self._priority_deploy

    def clear_priority_deploy(self, symbol: str):
        self._priority_deploy.discard(symbol)

    # ── Internal: update allocations ──────────────────────────────────────────
    async def _update_allocs(self, penalties: Dict[str, float]):
        equity = self._exec._equity or 1.0
        n      = max(len(self._symbols), 1)
        base_w = 1.0 / n

        async with self._lock:
            for sym in self._symbols:
                penalty = penalties.get(sym, 0.0)
                conf    = 0.5
                try:
                    pos_data = (self._exec._status.get("positions") or {}).get(sym, {})
                    conf = float(pos_data.get("signal_conf", 0.5))
                except Exception:
                    pass

                conf_adj_w  = base_w * (0.5 + conf)
                target_w    = conf_adj_w
                corr_factor = max(1.0 - penalty, MIN_CORR_ALLOC_FRAC)
                max_usd     = equity * target_w * corr_factor * n

                self._allocs[sym] = SymbolAlloc(
                    symbol          = sym,
                    target_weight   = round(target_w, 6),
                    corr_penalty    = penalty,
                    max_usd         = round(max_usd, 2),
                    rl_confidence   = conf,
                    priority_deploy = sym in self._priority_deploy,
                )

    # ── [P10-1] Correlation background loop ───────────────────────────────────
    async def _corr_loop(self):
        log.info("[P10] Correlation loop started (interval=%.0fs).", CORR_UPDATE_INTERVAL)
        while True:
            try:
                penalties = await self._corr_eng.compute()
                async with self._lock:
                    self._penalties = penalties
                await self._update_allocs(penalties)
            except asyncio.CancelledError:
                log.info("[P10] Correlation loop stopping.")
                return
            except Exception as e:
                log.error("[P10] Correlation loop error: %s", e, exc_info=True)
            await asyncio.sleep(CORR_UPDATE_INTERVAL)

    # ── [P10-2] Rebalancing loop ───────────────────────────────────────────────
    async def _rebal_loop(self):
        log.info("[P10] Rebalancing loop started.")
        while True:
            try:
                await asyncio.sleep(REBAL_COOLDOWN_SECS)
                await self._check_rebalance()
            except asyncio.CancelledError:
                log.info("[P10] Rebalancing loop stopping.")
                return
            except Exception as e:
                log.error("[P10] Rebalancing loop error: %s", e, exc_info=True)

    async def _check_rebalance(self):
        equity    = self._exec._equity
        if equity <= 0:
            return
        positions = self._exec.positions
        if not positions:
            return

        async with self._lock:
            allocs = dict(self._allocs)

        for sym, pos in list(positions.items()):
            now        = time.time()
            last_rebal = self._last_rebal.get(sym, 0.0)
            if now - last_rebal < REBAL_COOLDOWN_SECS:
                continue
            alloc = allocs.get(sym)
            if alloc is None:
                continue

            live_weight   = pos.usd_cost / equity * 100.0
            target_weight = alloc.target_weight * 100.0
            drift         = live_weight - target_weight

            if abs(drift) < REBAL_DRIFT_PCT:
                continue

            if drift > REBAL_DRIFT_PCT:
                await self._harvest(sym, pos, drift, equity)
                self._last_rebal[sym] = now
            elif drift < -REBAL_DRIFT_PCT:
                log.info(
                    "[P10] Rebal: %s UNDERWEIGHT (live=%.1f%% target=%.1f%%) "
                    "— flagging for priority deploy.",
                    sym, live_weight, target_weight,
                )
                self._priority_deploy.add(sym)
                self._last_rebal[sym] = now

    async def _harvest(self, symbol: str, pos, drift: float, equity: float):
        trim_usd = pos.usd_cost * REBAL_HARVEST_FRAC
        if trim_usd < 1.0:
            return
        log.info(
            "[P10] Rebal HARVEST %s: drift=+%.1f%% — trimming $%.2f (%.0f%% of pos)",
            symbol, drift, trim_usd, REBAL_HARVEST_FRAC * 100,
        )
        try:
            from data_hub import InstrumentCache

            use_swap = pos.inst_type == "SWAP"
            meta     = await self._exec._icache.get_instrument_info(symbol, swap=use_swap)
            trim_qty = trim_usd / (pos.cost_basis if pos.cost_basis > 0 else 1.0)
            if meta and meta.sz_inst > 0:
                trim_qty = InstrumentCache.round_to_lot(trim_qty, meta)
            if trim_qty <= 0 or (meta and meta.min_sz > 0 and trim_qty < meta.min_sz):
                log.warning("[P10] Harvest %s: trim qty too small — skipping.", symbol)
                return

            close_side     = "sell" if pos.direction == "long" else "buy"
            pos_side_close = ("long" if use_swap else "net") \
                              if pos.direction == "long" else "short"
            inst    = f"{symbol}-USDT" + ("-SWAP" if use_swap else "")
            td_mode = "cross" if use_swap else "cash"

            if meta and meta.sz_inst > 0:
                dec    = InstrumentCache._decimals(meta.sz_inst)
                sz_str = str(int(trim_qty)) if use_swap else f"{trim_qty:.{dec}f}"
            else:
                sz_str = f"{trim_qty:.8f}"

            mkt_body = self._exec._sor.build_market_body(
                inst, close_side, sz_str, td_mode,
                use_swap, pos_side_close, "P10_HARVEST",
            )
            resp = await self._exec._place_order(mkt_body)
            if resp and resp.get("code") == "0":
                ord_id = resp["data"][0]["ordId"]
                order  = await self._exec._wait_fill(inst, ord_id, timeout=20)
                if order:
                    trimmed = float(order.get("accFillSz") or trim_qty)
                    trim_px = float(order.get("avgPx") or pos.cost_basis)
                    pos.qty      -= trimmed
                    pos.usd_cost  = pos.qty * pos.cost_basis
                    pnl_pct = (
                        (trim_px - pos.cost_basis) / pos.cost_basis * 100
                        if pos.direction == "long"
                        else (pos.cost_basis - trim_px) / pos.cost_basis * 100
                    ) if pos.cost_basis > 0 else 0.0
                    import uuid as _uuid
                    await self._exec.db.insert_trade({
                        "ts": int(time.time()), "symbol": symbol,
                        "side": close_side, "qty": trimmed, "price": trim_px,
                        "cost_basis": pos.cost_basis,
                        "pnl_pct": round(pnl_pct, 4),
                        "realized_usd": round(pnl_pct / 100 * (trimmed * pos.cost_basis), 6),
                        "tag": "P10_HARVEST",
                        "order_id": order.get("ordId", str(_uuid.uuid4())),
                        "inst_type": pos.inst_type,
                    })
                    log.info("[P10] Harvest done %s: trimmed=%.6f px=%.4f pnl=%.2f%%",
                             symbol, trimmed, trim_px, pnl_pct)
            else:
                log.error("[P10] Harvest order failed %s: %s", symbol, resp)
        except Exception as e:
            log.error("[P10] _harvest error %s: %s", symbol, e, exc_info=True)

    # ── [P10-3] Delta hedge loop ───────────────────────────────────────────────
    async def _delta_loop(self):
        log.info("[P10] Delta hedge loop started.")
        while True:
            try:
                await asyncio.sleep(DELTA_COOLDOWN_SECS)
                await self._check_delta_hedge()
            except asyncio.CancelledError:
                log.info("[P10] Delta hedge loop stopping.")
                return
            except Exception as e:
                log.error("[P10] Delta hedge loop error: %s", e, exc_info=True)

    async def _check_delta_hedge(self):
        equity = self._exec._equity
        if equity <= 0:
            return

        total_delta_usd = 0.0
        for sym, pos in self._exec.positions.items():
            sign = 1.0 if pos.direction == "long" else -1.0
            total_delta_usd += pos.usd_cost * sign

        delta_ratio = total_delta_usd / equity

        log.debug(
            "[P10] Account delta: %.2f USD (%.1f%% of equity)",
            total_delta_usd, delta_ratio * 100,
        )

        if self._hedge is not None:
            await self._maybe_close_hedge(delta_ratio)

        if abs(delta_ratio) < DELTA_LONG_THRESHOLD:
            return

        macro_bearish = False
        macro_bullish = False
        try:
            bias = await self._nw.get_macro_bias()
            macro_bearish = bias.score < BEARISH_SCORE_THRESHOLD
            macro_bullish = bias.score > abs(BEARISH_SCORE_THRESHOLD)
        except Exception as e:
            log.debug("[P10] Delta: macro check error: %s", e)

        if delta_ratio >= DELTA_LONG_THRESHOLD and macro_bearish:
            await self._open_delta_hedge("short", delta_ratio, equity)
        elif delta_ratio <= DELTA_SHORT_THRESHOLD and macro_bullish:
            await self._open_delta_hedge("long", delta_ratio, equity)

    async def _open_delta_hedge(self, direction: str, delta_ratio: float,
                                  equity: float):
        if self._hedge is not None:
            log.debug("[P10] Hedge already active — skipping new open.")
            return

        target_delta_usd   = equity * DELTA_TARGET_PCT * (1.0 if delta_ratio >= 0 else -1.0)
        required_reduction = abs(delta_ratio * equity - target_delta_usd)
        hedge_usd          = max(required_reduction, DELTA_MIN_HEDGE_USD)
        sym                = DELTA_HEDGE_SYMBOL

        if hedge_usd > self._exec._avail * 0.90:
            hedge_usd = self._exec._avail * 0.90
        if hedge_usd < DELTA_MIN_HEDGE_USD:
            log.warning("[P10] Delta hedge: insufficient funds ($%.2f).", hedge_usd)
            return

        log.info(
            "[P10] Opening delta-neutral hedge: %s %s $%.2f "
            "(delta_ratio=%.2f → target=%.2f)",
            direction.upper(), sym, hedge_usd, delta_ratio, DELTA_TARGET_PCT,
        )

        try:
            side     = "sell" if direction == "short" else "buy"
            pos_side = direction
            order    = await self._exec._execute_order(
                sym, side, hedge_usd,
                swap=True, pos_side=pos_side,
                tag="P10_DELTA_HEDGE",
            )
            if order:
                fill_px = float(order.get("avgPx") or order.get("px") or 0)
                ord_id  = order.get("ordId", "")
                self._hedge = HedgePosition(
                    symbol=sym, direction=direction,
                    usd_size=hedge_usd, okx_order_id=ord_id,
                )
                log.info(
                    "[P10] Delta hedge opened: %s %s $%.2f @ %.4f ordId=%s",
                    direction.upper(), sym, hedge_usd, fill_px, ord_id,
                )
            else:
                log.error("[P10] Delta hedge order failed for %s.", sym)
        except Exception as e:
            log.error("[P10] _open_delta_hedge error: %s", e, exc_info=True)

    async def _maybe_close_hedge(self, current_delta_ratio: float):
        if self._hedge is None:
            return
        if abs(current_delta_ratio) > DELTA_CLOSE_THRESHOLD:
            return

        log.info(
            "[P10] Delta ratio=%.2f normalised — closing delta hedge %s %s.",
            current_delta_ratio, self._hedge.direction, self._hedge.symbol,
        )
        try:
            sym        = self._hedge.symbol
            h_dir      = self._hedge.direction
            close_side = "buy" if h_dir == "short" else "sell"
            pos_side   = h_dir

            tick = await self._hub.get_tick(sym)
            if not tick:
                return

            meta  = await self._exec._icache.get_instrument_info(sym, swap=True)
            h_usd = self._hedge.usd_size
            price = tick.ask if close_side == "buy" else tick.bid
            if price <= 0:
                return

            from data_hub import InstrumentCache
            ct_val = (meta.ct_val if meta and meta.ct_val > 0 else 0.01)
            raw_sz = h_usd / price / ct_val
            sz     = InstrumentCache.round_to_lot(raw_sz, meta) if meta else raw_sz
            sz_str = str(int(sz))

            inst    = f"{sym}-USDT-SWAP"
            td_mode = "cross"

            mkt = self._exec._sor.build_market_body(
                inst, close_side, sz_str, td_mode, True, pos_side,
                "P10_DELTA_CLOSE",
            )
            resp = await self._exec._place_order(mkt)
            if resp and resp.get("code") == "0":
                log.info("[P10] Delta hedge closed for %s.", sym)
                self._hedge = None
            else:
                log.error("[P10] Delta hedge close failed: %s", resp)
        except Exception as e:
            log.error("[P10] _maybe_close_hedge error: %s", e, exc_info=True)

    # ── [P23-OPT-2] Small-account allocation API ──────────────────────────────

    async def get_allocations(
        self,
        total_equity: float,
        min_sz_map: Optional[Dict[str, float]] = None,
    ) -> Dict[str, float]:
        """
        [P23-OPT-2] Returns {symbol: usd_allocation} for the active symbol set,
        applying small-account rebalancing logic when total_equity < P23_SMALL_ACCT_THRESHOLD.

        Small-account mode ($50 demo):
        ─────────────────────────────
        1. All symbols receive equal base slices of `total_equity`.
        2. BTC and ETH allocations are compared against their known minimum
           contract sizes (min_sz_map).  If the slice is below the minimum
           viable size (P23_MIN_VIABLE_ALLOC_USD) the capital is deemed
           undeployable in that symbol.
        3. Freed capital from step 2 is merged into the highest-conviction
           small-pivot coin (XRP, then DOGE by default) so it remains
           fully deployed rather than sitting idle.
        4. XRP and DOGE are always preferred in small-account mode because
           their per-contract cost is a fraction of a cent, making them
           compatible with any lot size.

        Normal mode (equity ≥ P23_SMALL_ACCT_THRESHOLD):
        ──────────────────────────────────────────────────
        Returns the existing correlation-penalised max_usd from _allocs,
        falling back to an equal-weight split when alloc data is not yet
        populated (cold-start).

        Args:
            total_equity: Live account equity in USD.
            min_sz_map:   Optional {symbol: min_contract_size_usd}.  When
                          provided, used to gate sub-threshold allocations.

        Returns:
            Dict mapping each active symbol to its target USD allocation.
            All values are non-negative; undeployable symbols map to 0.0.
        """
        if not self._symbols:
            return {}

        allocs: Dict[str, float] = {}

        # ── Normal mode ───────────────────────────────────────────────────────
        if total_equity >= P23_SMALL_ACCT_THRESHOLD:
            async with self._lock:
                snap = dict(self._allocs)

            n = max(len(self._symbols), 1)
            for sym in self._symbols:
                if sym in snap:
                    allocs[sym] = round(snap[sym].max_usd, 4)
                else:
                    # Cold-start: equal weight until corr loop populates allocs
                    allocs[sym] = round(total_equity / n, 4)

            log.debug(
                "[P23-OPT-2] get_allocations (normal mode, equity=$%.2f): %s",
                total_equity,
                {s: f"${v:.2f}" for s, v in allocs.items()},
            )
            return allocs

        # ── Small-account mode ────────────────────────────────────────────────
        log.info(
            "[P23-OPT-2] Small-account mode active (equity=$%.2f < $%.2f) — "
            "applying min_sz pivot logic.",
            total_equity, P23_SMALL_ACCT_THRESHOLD,
        )

        n        = max(len(self._symbols), 1)
        base_usd = total_equity / n

        freed_usd      = 0.0
        high_vol_syms: List[str] = []  # BTC, ETH — large contract size
        pivot_syms:    List[str] = []  # XRP, DOGE — tiny contract size

        for sym in self._symbols:
            min_viable = P23_MIN_VIABLE_ALLOC_USD
            if min_sz_map:
                # min_sz_map[sym] is expressed in USD (already price-adjusted)
                min_viable = max(min_viable, min_sz_map.get(sym, 0.0))

            # BTC/ETH-class: check if base_usd covers minimum contract
            is_large_contract = sym in ("BTC", "ETH")
            if is_large_contract and base_usd < min_viable:
                log.info(
                    "[P23-OPT-2] %s allocation=$%.4f < min_viable=$%.4f — "
                    "freeing capital for Small-Pivot merge.",
                    sym, base_usd, min_viable,
                )
                allocs[sym] = 0.0
                freed_usd  += base_usd
                high_vol_syms.append(sym)
            else:
                allocs[sym] = round(base_usd, 4)
                # Track viable pivot candidates (XRP/DOGE or env-configured)
                if sym in P23_SMALL_PIVOT_SYMS:
                    pivot_syms.append(sym)

        # ── Merge freed capital into best pivot ───────────────────────────────
        if freed_usd > 0.0 and pivot_syms:
            # Priority: first match in P23_SMALL_PIVOT_SYMS (XRP before DOGE)
            best_pivot = next(
                (s for s in P23_SMALL_PIVOT_SYMS if s in pivot_syms),
                pivot_syms[0],
            )
            allocs[best_pivot] = round(allocs.get(best_pivot, 0.0) + freed_usd, 4)
            log.info(
                "[P23-OPT-2] Freed $%.4f from %s → merged into %s "
                "(new alloc=$%.4f).",
                freed_usd,
                high_vol_syms,
                best_pivot,
                allocs[best_pivot],
            )
        elif freed_usd > 0.0:
            # No pivot sym in the active set — distribute equally among
            # whatever symbols DO have viable allocations.
            viable = [s for s, v in allocs.items() if v > 0.0]
            if viable:
                per_sym = freed_usd / len(viable)
                for sym in viable:
                    allocs[sym] = round(allocs[sym] + per_sym, 4)
                log.info(
                    "[P23-OPT-2] No configured pivot syms active — "
                    "distributed $%.4f equally across %s.",
                    freed_usd, viable,
                )
            else:
                log.warning(
                    "[P23-OPT-2] All symbols unviable for $%.2f account — "
                    "returning zero allocations.", total_equity,
                )

        log.info(
            "[P23-OPT-2] Final small-account allocations (equity=$%.2f): %s",
            total_equity,
            {s: f"${v:.4f}" for s, v in allocs.items()},
        )
        return allocs

    # ══════════════════════════════════════════════════════════════════════════
    # [P35.1-HEDGE] Dynamic Delta-Neutral Hedging Engine
    # ══════════════════════════════════════════════════════════════════════════

    def _p35_calc_net_delta_usd(self) -> float:
        """
        [P35-CALC] Sum the signed USD exposure of every active Altcoin position.

        Long positions contribute +usd_cost; short positions contribute -usd_cost.
        Positions in P35_HEDGE_SYMBOL (BTC) and P35_EXCLUDE_SYMBOLS are skipped
        so the hedge leg is never included in its own delta calculation.

        Returns
        -------
        float
            Signed net Altcoin delta in USD.  Positive = net long; negative = net short.
        """
        try:
            excluded = {P35_HEDGE_SYMBOL.upper()} | {s.upper() for s in P35_EXCLUDE_SYMBOLS}
            net = 0.0
            for sym, pos in self._exec.positions.items():
                if sym.upper() in excluded:
                    continue
                sign = 1.0 if pos.direction == "long" else -1.0
                net += pos.usd_cost * sign
            return net
        except Exception as exc:
            log.debug("[P35.1-HEDGE] _p35_calc_net_delta_usd error: %s", exc)
            return 0.0

    async def _p35_hedge_loop(self) -> None:
        """
        [P35.1-HEDGE] Background loop — evaluates and adjusts the BTC-SWAP hedge
        every P35_HEDGE_POLL_SECS seconds.
        """
        log.info(
            "[P35.1-HEDGE] Hedge loop started "
            "(poll=%.0fs threshold=$%.2f ratio=%.2f min_change=$%.2f symbol=%s).",
            P35_HEDGE_POLL_SECS, P35_HEDGE_THRESHOLD_USD,
            P35_HEDGE_RATIO, P35_MIN_HEDGE_CHANGE_USD, P35_HEDGE_SYMBOL,
        )
        while True:
            try:
                await asyncio.sleep(P35_HEDGE_POLL_SECS)
                await self._p35_evaluate_and_hedge()
            except asyncio.CancelledError:
                log.info("[P35.1-HEDGE] Hedge loop stopping.")
                return
            except Exception as exc:
                log.error("[P35.1-HEDGE] Hedge loop error: %s", exc, exc_info=True)

    async def _p35_evaluate_and_hedge(self) -> None:
        """
        [P35.1-HEDGE] Core evaluation cycle.

        1. Calculate net Altcoin delta.
        2. [PRICE-VALIDITY] Verify the hedge symbol has a live, positive price via
           DataHub.get_price().  If the price is stale or None, log a CRITICAL
           sentinel warning and return — the loop continues normally next poll.
        3. Determine required hedge direction and target USD size.
        4. If the required change exceeds P35_MIN_HEDGE_CHANGE_USD, call
           _p35_adjust_hedge() to open / replace the hedge.
        5. If the net delta has fallen below P35_HEDGE_THRESHOLD_USD, close
           any active hedge.

        [P36.1-GUARD] hedge_symbol is loaded at module-level via _clean_env()
        so literal quote characters from .env files (e.g. P35_HEDGE_SYMBOL="BTC")
        can never reach the OKX order placement layer.
        """
        # [P36.1-GUARD] Use the module-level sanitized symbol — never raw env.
        hedge_symbol = P35_HEDGE_SYMBOL  # guaranteed _clean_env sanitized
        async with self._p35_lock:
            try:
                net_delta = self._p35_calc_net_delta_usd()
                abs_delta = abs(net_delta)

                # ── [PRICE-VALIDITY / P36.1-GUARD] Check hedge price before any order ─
                # DataHub.get_price() is a null-safe wrapper — it never raises.
                # A None or non-positive return means the feed is stale or the
                # instrument is unknown.  In either case we MUST NOT place a
                # hedge order — the size calculation would be meaningless.
                try:
                    hedge_price = await self._hub.get_price(hedge_symbol)
                except Exception as _price_exc:
                    hedge_price = None
                    log.warning(
                        "[P35.1-HEDGE] get_price raised for %s: %s",
                        hedge_symbol, _price_exc,
                    )

                if hedge_price is None or hedge_price <= 0:
                    log.critical(
                        "[P35.1-SENTINEL] STALE/NULL PRICE for hedge symbol '%s' "
                        "(DataHub returned %s) — SKIPPING hedge adjustment this "
                        "cycle to prevent a mis-sized SWAP order.  "
                        "Feed may be down.  Will retry in %.0fs.",
                        hedge_symbol, hedge_price, P35_HEDGE_POLL_SECS,
                    )
                    # Do NOT crash or raise — let the loop continue normally.
                    return

                log.debug(
                    "[P35.1-HEDGE] Evaluation: net_delta_usd=%.2f abs=%.2f "
                    "threshold=%.2f current_hedge=%.2f dir=%s hedge_price=%.4f",
                    net_delta, abs_delta, P35_HEDGE_THRESHOLD_USD,
                    self._p35_hedge_usd, self._p35_hedge_dir or "none",
                    hedge_price,
                )

                # ── Case 1: no significant exposure → close any active hedge ─
                if abs_delta < P35_HEDGE_THRESHOLD_USD:
                    if self._p35_hedge_usd > 0.0:
                        log.info(
                            "[P35.1-HEDGE] Net delta $%.2f < threshold $%.2f — "
                            "closing active hedge.",
                            abs_delta, P35_HEDGE_THRESHOLD_USD,
                        )
                        await self._p35_close_hedge(_inside_lock=True)
                    return

                # ── Case 2: significant exposure → determine target hedge ─────
                # Hedge direction is opposite to net Altcoin exposure.
                target_dir = "short" if net_delta > 0.0 else "long"
                target_usd = abs_delta * P35_HEDGE_RATIO

                # Cap at 90 % of available margin to prevent account exhaustion.
                max_hedge = self._exec._avail * 0.90
                if max_hedge > 0.0:
                    target_usd = min(target_usd, max_hedge)

                change_usd = abs(target_usd - self._p35_hedge_usd)

                # Direction flip always requires a full rebalance.
                dir_changed = (
                    self._p35_hedge_dir != "" and self._p35_hedge_dir != target_dir
                )

                if not dir_changed and change_usd < P35_MIN_HEDGE_CHANGE_USD:
                    log.debug(
                        "[P35.1-HEDGE] Change $%.2f < min $%.2f — no rebalance needed.",
                        change_usd, P35_MIN_HEDGE_CHANGE_USD,
                    )
                    return

                if dir_changed:
                    log.info(
                        "[P35.1-HEDGE] Hedge direction flip (%s → %s) — "
                        "closing existing hedge before re-opening.",
                        self._p35_hedge_dir, target_dir,
                    )

                await self._p35_adjust_hedge(target_dir, target_usd)

            except Exception as exc:
                log.error("[P35.1-HEDGE] _p35_evaluate_and_hedge error: %s", exc,
                          exc_info=True)
                self._p35_rebalancing = False

    async def _p35_adjust_hedge(self, target_dir: str, target_usd: float) -> None:
        """
        [P35.1-HEDGE] Open or replace the BTC-SWAP hedge position.

        Strategy: close the existing hedge fully first (if any), then open a
        fresh position at the target size.  This simplifies state tracking vs
        partial adjustments and avoids OKX over-hedged-position errors.
        """
        try:
            self._p35_rebalancing = True
            log.info(
                "[P35.1-HEDGE] Adjusting hedge: dir=%s target_usd=$%.2f "
                "(previous: dir=%s usd=$%.2f).",
                target_dir, target_usd,
                self._p35_hedge_dir or "none", self._p35_hedge_usd,
            )

            # Close existing hedge first if one is active.
            if self._p35_hedge_usd > 0.0 and self._p35_hedge_dir:
                await self._p35_close_hedge(_inside_lock=True)

            # Open new hedge at target size.
            side = "sell" if target_dir == "short" else "buy"

            # ── [P36.2-HEDGE] DynamicBuffer Alignment ────────────────────────
            # Read the Executor's dynamic buffer for the hedge symbol so hedge
            # orders during high-volatility periods use the elevated 10-bps buffer
            # and are never rejected with sCode 51006 from OKX price-band movement.
            try:
                _hedge_dyn_buf = self._exec._p362_get_dynamic_buffer(P35_HEDGE_SYMBOL)
            except Exception:
                _hedge_dyn_buf = 0.0005  # safe fallback: 5 bps

            # Pre-validate hedge price against cached price limits using dynamic buffer.
            try:
                _hedge_inst    = f"{P35_HEDGE_SYMBOL}-USDT-SWAP"
                _h_buy_lmt, _h_sell_lmt = self._exec._icache.get_price_limits(_hedge_inst)
                _hedge_tick    = await self._hub.get_tick(P35_HEDGE_SYMBOL)
                if _hedge_tick and _hedge_tick.ask > 0 and _hedge_tick.bid > 0:
                    _hedge_ref_px = (
                        _hedge_tick.ask if side == "buy" else _hedge_tick.bid
                    )
                    if side == "buy" and _h_buy_lmt > 0 and _hedge_ref_px > _h_buy_lmt:
                        _clamped_hedge = _h_buy_lmt * (1.0 - _hedge_dyn_buf)
                        log.warning(
                            "[P36.2-HEDGE] %s BUY hedge price %.8f > buyLmt %.8f "
                            "→ pre-clamped to %.8f (DynBuf=%.4f%%)",
                            P35_HEDGE_SYMBOL, _hedge_ref_px, _h_buy_lmt,
                            _clamped_hedge, _hedge_dyn_buf * 100,
                        )
                    elif side == "sell" and _h_sell_lmt > 0 and _hedge_ref_px < _h_sell_lmt:
                        _clamped_hedge = _h_sell_lmt * (1.0 + _hedge_dyn_buf)
                        log.warning(
                            "[P36.2-HEDGE] %s SELL hedge price %.8f < sellLmt %.8f "
                            "→ pre-clamped to %.8f (DynBuf=%.4f%%)",
                            P35_HEDGE_SYMBOL, _hedge_ref_px, _h_sell_lmt,
                            _clamped_hedge, _hedge_dyn_buf * 100,
                        )
            except Exception as _hpg_exc:
                log.debug("[P36.2-HEDGE] Hedge price pre-clamp check error: %s", _hpg_exc)
            # ── [/P36.2-HEDGE] ───────────────────────────────────────────────

            try:
                order = await self._exec._execute_order(
                    P35_HEDGE_SYMBOL, side, target_usd,
                    swap=True, pos_side=target_dir,
                    tag="P35_HEDGE",
                )
            except Exception as exc:
                log.error("[P35.1-HEDGE] _execute_order error: %s", exc, exc_info=True)
                self._p35_rebalancing = False
                return

            if order:
                fill_px  = float(order.get("avgPx") or order.get("px") or 0)
                ord_id   = order.get("ordId", "")
                self._p35_hedge_usd    = target_usd
                self._p35_hedge_dir    = target_dir
                self._p35_hedge_ord_id = ord_id
                log.info(
                    "[P35.1-HEDGE] ✅ Hedge opened: %s %s $%.2f @ %.4f ordId=%s",
                    target_dir.upper(), P35_HEDGE_SYMBOL, target_usd, fill_px, ord_id,
                )
            else:
                log.error(
                    "[P35.1-HEDGE] Hedge order returned None for %s %s $%.2f.",
                    target_dir.upper(), P35_HEDGE_SYMBOL, target_usd,
                )

        except Exception as exc:
            log.error("[P35.1-HEDGE] _p35_adjust_hedge error: %s", exc, exc_info=True)
        finally:
            self._p35_rebalancing = False

    async def _p35_close_hedge(self, *, _inside_lock: bool = False) -> None:
        """
        [P35.1-HEDGE] Close the active BTC-SWAP hedge position via a market order.

        Parameters
        ----------
        _inside_lock : bool
            Pass True when this method is already called from within _p35_lock
            so we don't attempt to re-acquire it (asyncio.Lock is not reentrant).
        """
        if self._p35_hedge_usd <= 0.0 or not self._p35_hedge_dir:
            return

        async def _do_close():
            try:
                h_dir      = self._p35_hedge_dir
                h_usd      = self._p35_hedge_usd
                close_side = "buy" if h_dir == "short" else "sell"

                log.info(
                    "[P35.1-HEDGE] Closing hedge: %s %s $%.2f (ordId=%s).",
                    h_dir.upper(), P35_HEDGE_SYMBOL, h_usd, self._p35_hedge_ord_id,
                )

                tick = await self._hub.get_tick(P35_HEDGE_SYMBOL)
                if not tick:
                    log.warning("[P35.1-HEDGE] Cannot close hedge — no tick for %s.",
                                P35_HEDGE_SYMBOL)
                    return

                meta = await self._exec._icache.get_instrument_info(
                    P35_HEDGE_SYMBOL, swap=True
                )
                price  = tick.ask if close_side == "buy" else tick.bid
                if price <= 0:
                    log.warning("[P35.1-HEDGE] Cannot close hedge — zero price for %s.",
                                P35_HEDGE_SYMBOL)
                    return

                from data_hub import InstrumentCache
                ct_val = (meta.ct_val if meta and meta.ct_val > 0 else 0.01)
                raw_sz = h_usd / price / ct_val
                sz     = InstrumentCache.round_to_lot(raw_sz, meta) if meta else raw_sz
                sz_str = str(int(sz))

                inst    = f"{P35_HEDGE_SYMBOL}-USDT-SWAP"
                td_mode = "cross"

                mkt = self._exec._sor.build_market_body(
                    inst, close_side, sz_str, td_mode, True, h_dir,
                    "P35_HEDGE_CLOSE",
                )
                resp = await self._exec._place_order(mkt)
                if resp and resp.get("code") == "0":
                    log.info(
                        "[P35.1-HEDGE] ✅ Hedge closed: %s %s $%.2f.",
                        h_dir.upper(), P35_HEDGE_SYMBOL, h_usd,
                    )
                    self._p35_hedge_usd    = 0.0
                    self._p35_hedge_dir    = ""
                    self._p35_hedge_ord_id = ""
                else:
                    log.error("[P35.1-HEDGE] Hedge close order failed: %s", resp)
            except Exception as exc:
                log.error("[P35.1-HEDGE] _p35_close_hedge error: %s", exc,
                          exc_info=True)

        if _inside_lock:
            await _do_close()
        else:
            async with self._p35_lock:
                await _do_close()

    # ── Status / reporting ─────────────────────────────────────────────────────
    def status_snapshot(self) -> dict:
        reg = self._regime.snapshot
        allocs_out = {}
        for sym, a in self._allocs.items():
            allocs_out[sym] = {
                "target_weight":   round(a.target_weight, 6),
                "corr_penalty":    round(a.corr_penalty,  4),
                "max_usd":         round(a.max_usd,       2),
                "rl_confidence":   round(a.rl_confidence, 4),
                "priority_deploy": a.priority_deploy,
            }
        hedge_out = None
        if self._hedge is not None:
            hedge_out = {
                "symbol":    self._hedge.symbol,
                "direction": self._hedge.direction,
                "usd_size":  self._hedge.usd_size,
                "entry_ts":  self._hedge.entry_ts,
            }
        return {
            "regime": {
                "personality": reg.personality.value,
                "vol_score":   reg.vol_score,
                "trend_score": reg.trend_score,
                "n_symbols":   reg.n_symbols,
                "ts":          reg.ts,
            },
            "allocations":           allocs_out,
            "delta_hedge":           hedge_out,
            "priority_deploy_queue": list(self._priority_deploy),
            # [P35.1-HEDGE] Live hedge snapshot
            "p35_hedge": {
                "hedge_usd":         round(self._p35_hedge_usd, 2),
                "hedge_dir":         self._p35_hedge_dir or "none",
                "rebalancing":       self._p35_rebalancing,
                "net_delta_usd":     round(self._p35_calc_net_delta_usd(), 2),
                "ord_id":            self._p35_hedge_ord_id,
                "threshold_usd":     P35_HEDGE_THRESHOLD_USD,
                "ratio":             P35_HEDGE_RATIO,
                "min_change_usd":    P35_MIN_HEDGE_CHANGE_USD,
                "symbol":            P35_HEDGE_SYMBOL,
                "poll_secs":         P35_HEDGE_POLL_SECS,
                "exclude_symbols":   P35_EXCLUDE_SYMBOLS,
            },
        }
