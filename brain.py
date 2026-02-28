"""
brain.py  —  Market Intelligence Engine  Phase 32 Institutional Predator Suite

Phase 32 additions (this release):
  [P32-SLIP-ADAPT] Per-symbol Slippage-Adaptive Tracker — SlippagePerSymbolTracker
                   tracks the last P32_SLIP_WINDOW_TRADES (default 3) realized fills
                   per symbol.  When the rolling average exceeds P32_SLIP_LIMIT_BPS
                   (10 bps), the Executor forces LIMIT_ONLY for P32_SLIP_LIMIT_HOURS
                   (4 h) to stop market-order bleeding.  The tracker is accessible at
                   IntelligenceEngine.p32_slip_tracker.

Phase 31 Institutional Tactical Suite (all preserved):
  [P31-WHALE-TRAIL] Dynamic Whale-Trail — Signal.whale_multiplier field added.
                    IntelligenceEngine.analyze() populates whale_multiplier from
                    the oracle signal (P15 whale mult) so the Executor's trailing
                    stop gap scales with whale sweep volume:
                      • High whale_multiplier (>= 10×) → wider gap (let winners run).
                      • Fading whale_multiplier (→ 1×)  → aggressive gap tightening.
                    aggregate_signals() carries the max whale_multiplier across TFs.

Phase 30.5 Titan Upgrade (all preserved — see below):
  [P30.5-REGIME] BayesianTracker Regime-Aware Decay
  [P27-OKX]     OKXPostAdapter preserved

Phase 30.5 additions (this release — history):
  [P30.5-REGIME] BayesianTracker Regime-Aware Decay — BayesianTracker.update()
                 now accepts a ``current_regime`` parameter.  When the HMM
                 detects a regime shift, a 20 % "Soft Reset" (decay toward
                 the Beta prior) is applied before the new observation is
                 incorporated, eliminating past-regime bias.
  [P27-OKX]     OKXPostAdapter preserved — corrects the
                 ``OKXRestClient.post() got an unexpected keyword argument 'data'``
                 error found in logs.

Phase 26 additions (all preserved):
  [P26-SLIP] SlippageCircuitBreaker — LIMIT_ONLY when avg_slip > 500 bps.
  [P26-VELOCITY] BayesianTracker Signal Velocity — early-sample 2× weighting.

Phase 20 additions (all preserved):
  [P20-1] High-Water Mark (HWM) tracking.
  [P20-3] Walk-Forward Optimization (Shadow Training).

Phase 19 additions (all preserved):
  [P19-1] HMM State Persistence via save_state() / load_state().

Phase 6 RL (merged from brain_p6.py):
  [P6-RL] RegimeWeightTable + per-regime multiplier learning.
          - RegimeStats / RegimeWeightTable classes.
          - IntelligenceEngine.update_weights(regime, pnl)
          - IntelligenceEngine.get_regime_weights()
          - save_state() / load_state() persist the full RegimeWeightTable.

Phase 15 additions (all preserved): EnsembleJudge Oracle Synergy
Phase 14 additions (all preserved): EnsembleJudge, FAISS PatternLibrary
Phase 13 (all preserved): BayesianTracker, Strategy Self-Censorship
Phase 11 (all preserved): MTF Log Dampening via sentinel
Phase 4  (all preserved): Multi-Timeframe (MTF) Alignment
Phase 3  (all preserved): regime_modifier, chop_sniper_only, trail_multiplier
[TASK-2] Sniper Multiplier
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import math
import os
import pickle
import time
from decimal import Decimal, getcontext
from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional, Tuple

import numpy as np

getcontext().prec = 8

log = logging.getLogger("brain")



# ── Safe env parsing ──────────────────────────────────────────────────────────
def _env_float(key: str, default: float) -> float:
    """Parse float env var safely; fall back to default on bad values."""
    raw = os.environ.get(key, "")
    if raw is None or raw == "":
        return float(default)
    try:
        return float(str(raw).strip())
    except Exception:
        log.warning("Invalid float for %s=%r; using default=%s", key, raw, default)
        return float(default)

def _env_int(key: str, default: int) -> int:
    """Parse int env var safely; fall back to default on bad values."""
    raw = os.environ.get(key, "")
    if raw is None or raw == "":
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        log.warning("Invalid int for %s=%r; using default=%s", key, raw, default)
        return int(default)



def _D(val) -> Decimal:
    try:
        if isinstance(val, Decimal):
            return val
        if val is None:
            return Decimal("0")
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return Decimal("0")
        return Decimal(str(val))
    except Exception:
        return Decimal("0")

# ══════════════════════════════════════════════════════════════════════════════
# [P27-OKX] OKX Gateway Post Adapter
# ══════════════════════════════════════════════════════════════════════════════
# ROOT CAUSE: OKXRestClient.post() does NOT accept a 'data=' keyword argument.
# Its signature is:  post(path: str, body: dict) → dict
# Any call-site that used `client.post(path, data=payload)` raised:
#   TypeError: OKXRestClient.post() got an unexpected keyword argument 'data'
#
# FIX PATTERN:  Replace every  client.post(path, data=payload)
#               with           okx_post(client, path, payload)
#               or             client.post(path, body=payload)
#
# This module exposes OKXPostAdapter as a thin wrapper that normalises the
# call signature so existing code doesn't need to be hunted call-by-call.
# ──────────────────────────────────────────────────────────────────────────────

class OKXPostAdapter:
    """
    [P27-OKX] Adapter that wraps an OKXRestClient and exposes a unified
    ``post(path, body)`` interface, masking the historic misuse of ``data=``.

    Usage (drop-in replacement at any call site)::

        # BEFORE (broken):
        response = okx_client.post("/api/v5/trade/order", data=order_payload)

        # AFTER (fixed via adapter):
        adapter  = OKXPostAdapter(okx_client)
        response = adapter.post("/api/v5/trade/order", order_payload)

        # OR directly with the corrected keyword:
        response = okx_client.post("/api/v5/trade/order", body=order_payload)

    The adapter delegates to the underlying client's ``post`` method using
    the ``body=`` keyword, which is the only accepted parameter name in the
    OKXRestClient signature.  It also catches and re-raises with a structured
    log entry so slippage tracking in SlippageCircuitBreaker can react.
    """

    def __init__(self, okx_client) -> None:
        self._client = okx_client

    def post(self, path: str, payload: dict) -> dict:
        """
        [P27-OKX] Safe post — always uses ``body=`` keyword, never ``data=``.

        Parameters
        ----------
        path    : str   — OKX REST API path, e.g. "/api/v5/trade/order"
        payload : dict  — Request body (order params, etc.)

        Returns
        -------
        dict : Parsed OKX response.

        Raises
        ------
        RuntimeError wrapping the original exception so callers can catch it
        without importing OKX-specific exception types.
        """
        try:
            # ── Correct call: body= not data= ─────────────────────────────────
            return self._client.post(path, body=payload)
        except TypeError as exc:
            # If the underlying client still fails on body= (e.g. different SDK
            # version uses positional only), try positional as last resort.
            if "body" in str(exc) or "unexpected keyword" in str(exc):
                log.warning(
                    "[P27-OKX] body= keyword rejected by client (%s); "
                    "retrying with positional arg.", exc,
                )
                try:
                    return self._client.post(path, payload)
                except Exception as exc2:
                    raise RuntimeError(f"[P27-OKX] OKX post failed: {exc2}") from exc2
            raise RuntimeError(f"[P27-OKX] OKX post TypeError: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"[P27-OKX] OKX post error: {exc}") from exc

    def place_order(self, inst_id: str, side: str, ord_type: str,
                    sz: str, px: str = "", **extra) -> dict:
        """
        [P27-OKX] Convenience wrapper for /api/v5/trade/order.

        Builds the OKX order body from explicit parameters to prevent callers
        from accidentally passing ``data=`` at a higher level of abstraction.
        """
        body: dict = {
            "instId":  inst_id,
            "tdMode":  extra.pop("tdMode", "cash"),
            "side":    side,
            "ordType": ord_type,
            "sz":      sz,
        }
        if px:
            body["px"] = px
        body.update(extra)
        log.debug("[P27-OKX] place_order body=%s", body)
        return self.post("/api/v5/trade/order", body)


def okx_post(client, path: str, payload: dict) -> dict:
    """
    [P27-OKX] Module-level convenience function — one-liner fix for any
    call-site that can import from brain:

        from brain import okx_post
        result = okx_post(okx_client, "/api/v5/trade/order", order_dict)

    Internally delegates to OKXPostAdapter.post() which uses ``body=``.
    """
    return OKXPostAdapter(client).post(path, payload)

try:
    import faiss  # type: ignore
    _FAISS_OK = True
except ImportError:
    _FAISS_OK = False
    log.warning("faiss not installed — falling back to numpy dot-product search.")

try:
    from hmmlearn import hmm as _hmm  # type: ignore
    _HMM_OK = True
except ImportError:
    _HMM_OK = False
    log.warning("hmmlearn not installed — using rule-based regime fallback.")

try:
    from scipy.special import betainc as _betainc  # type: ignore
    _SCIPY_OK = True
except ImportError:
    _SCIPY_OK = False
    log.warning("scipy not installed — BayesianTracker using mean-based fallback.")

try:
    from sentinel import mtf_conflict_logger as _mtf_log
    _SENTINEL_OK = True
except ImportError:
    _SENTINEL_OK = False
    _mtf_log = None

_mtf_fallback_state: Dict[str, bool] = {}

# ── [P19-1] State persistence config ──────────────────────────────────────────
BRAIN_STATE_PATH = os.environ.get(
    "BRAIN_STATE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "brain_state.pkl"),
)

# ── Sniper config ──────────────────────────────────────────────────────────────
SNIPER_CONFIDENCE_THRESHOLD = _env_float("SNIPER_CONFIDENCE_THRESHOLD", 0.8)
SNIPER_MIN_MULTIPLIER       = _env_float("SNIPER_MIN_MULTIPLIER", 1.5)
SNIPER_MAX_MULTIPLIER       = _env_float("SNIPER_MAX_MULTIPLIER", 2.0)
MAX_ALLOC_PCT               = _env_float("MAX_ALLOC_PCT", 0.2)

# ── [P6-RL] RL config ─────────────────────────────────────────────────────────
RL_LEARN_RATE = _env_float("RL_LEARN_RATE", 0.05)
RL_MAX_MULT   = _env_float("RL_MAX_MULT", 2.0)
RL_MIN_MULT   = _env_float("RL_MIN_MULT", 0.3)
RL_MIN_TRADES = _env_int("RL_MIN_TRADES", 3)
_REGIMES      = ("bull", "bear", "chop")

# ── [P13] Bayesian config ──────────────────────────────────────────────────────
P13_TRUST_CIRCUIT_BREAKER = _env_float("P13_TRUST_CIRCUIT_BREAKER", 0.35)
P13_TRUST_MIN_SAMPLES     = _env_int("P13_TRUST_MIN_SAMPLES", 5)
P13_TRUST_PRIOR_ALPHA     = _env_float("P13_TRUST_PRIOR_ALPHA", 2.0)
P13_TRUST_PRIOR_BETA      = _env_float("P13_TRUST_PRIOR_BETA", 2.0)
# [P26-VELOCITY] Number of early samples that receive a boosted update weight
# so the trust factor moves away from the 0.5 neutral zone faster.
P13_TRUST_VELOCITY_SAMPLES = _env_int("P13_TRUST_VELOCITY_SAMPLES", 5)
P13_TRUST_VELOCITY_WEIGHT  = _env_float("P13_TRUST_VELOCITY_WEIGHT", 2.0)

# ── [P26] Slippage Circuit Breaker config ────────────────────────────────────
P26_SLIP_GUARD_BPS    = _env_float("P26_SLIP_GUARD_BPS", 500.0)
P26_BBO_CLAMP_BPS     = _env_float("P26_BBO_CLAMP_BPS", 1.0)
P26_SLIP_WINDOW       = _env_int("P26_SLIP_WINDOW", 20)

# ── [P14] Meta-Optimizer config ────────────────────────────────────────────────
P14_CONVICTION_BOOST       = _env_float("P14_CONVICTION_BOOST", 1.2)
P14_CONVICTION_PENALTY     = _env_float("P14_CONVICTION_PENALTY", 0.5)
P14_EMA_FAST               = _env_int("P14_EMA_FAST", 20)
P14_EMA_SLOW               = _env_int("P14_EMA_SLOW", 50)
P14_RSI_PERIOD             = _env_int("P14_RSI_PERIOD", 14)
P14_RSI_OVERBOUGHT         = _env_float("P14_RSI_OVERBOUGHT", 65.0)
P14_RSI_OVERSOLD           = _env_float("P14_RSI_OVERSOLD", 35.0)
P14_ATR_PERIOD             = _env_int("P14_ATR_PERIOD", 14)
P14_PRUNE_INTERVAL_SECS    = _env_float("P14_PRUNE_INTERVAL_SECS", str(6 * 3600))
P14_PRUNE_WEIGHT_THRESHOLD = _env_float("P14_PRUNE_WEIGHT_THRESHOLD", 0.2)
P14_PRUNE_MAX_PATTERNS     = _env_int("P14_PRUNE_MAX_PATTERNS", 5000)

# ── [P15-2] Oracle conviction config ──────────────────────────────────────────
P15_ORACLE_BOOST          = _env_float("P15_ORACLE_BOOST", 1.5)
P15_ORACLE_PENALTY        = _env_float("P15_ORACLE_PENALTY", 0.6)
P15_ORACLE_MIN_WHALE_MULT = _env_float("P15_ORACLE_MIN_WHALE_MULT", 10.0)

_P15_SOFT_PENALTY_FACTOR = 0.5

# ── [P20-3] Walk-Forward Shadow Training config ────────────────────────────────
P20_SHADOW_INTERVAL_SECS  = _env_float("P20_SHADOW_INTERVAL_SECS", str(24 * 3600))
P20_SHADOW_LOOKBACK_HOURS = _env_int("P20_SHADOW_LOOKBACK_HOURS", 48)
P20_SHADOW_SWAP_THRESHOLD = _env_float("P20_SHADOW_SWAP_THRESHOLD", 1.1)
P20_SHADOW_AUTO_SWAP      = os.environ.get("P20_SHADOW_AUTO_SWAP", "0").strip() == "1"
P20_SHADOW_MIN_OBS        = _env_int("P20_SHADOW_MIN_OBS", 60)

# ── Regime modifiers ───────────────────────────────────────────────────────────
_REGIME_KELLY_MODIFIER: Dict[str, float] = {
    "bull": 1.0, "bear": 0.5, "chop": 0.3,
}
_REGIME_TRAIL_TIGHTEN: Dict[str, float] = {
    "bull": 1.0, "bear": 0.7, "chop": 0.8,
}


# ══════════════════════════════════════════════════════════════════════════════
# [P6-RL] Regime Weight Table (merged from brain_p6.py)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class RegimeStats:
    """Per-regime RL statistics and learned confidence multiplier."""
    multiplier:   float = 1.0
    wins:         int   = 0
    losses:       int   = 0
    total_pnl:    float = 0.0
    last_updated: float = field(default_factory=time.time)

    @property
    def total_trades(self) -> int:
        return self.wins + self.losses

    @property
    def win_rate(self) -> float:
        t = self.total_trades
        return self.wins / t if t > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "multiplier":   round(self.multiplier, 4),
            "wins":         self.wins,
            "losses":       self.losses,
            "total_trades": self.total_trades,
            "win_rate":     round(self.win_rate, 4),
            "total_pnl":    round(self.total_pnl, 4),
            "last_updated": self.last_updated,
        }


class RegimeWeightTable:
    """
    [P6-RL] Tracks per-regime RL multipliers with asyncio.Lock protection.

    The multiplier is applied to both signal.confidence and signal.kelly_f
    inside IntelligenceEngine.analyze() / aggregate_signals().

    Clamped to [RL_MIN_MULT, RL_MAX_MULT] at all times.
    Does not deviate from 1.0 until RL_MIN_TRADES trades have been recorded
    for that regime (cold-start protection).
    """

    def __init__(self) -> None:
        self._stats: Dict[str, RegimeStats] = {r: RegimeStats() for r in _REGIMES}
        self._lock = asyncio.Lock()
        # Sync-readable snapshot updated after every async update()
        self._cache: Dict[str, float] = {r: 1.0 for r in _REGIMES}

    # ── Async write path (called from Orchestrator after position close) ───────
    async def update(self, regime: str, pnl: float) -> None:
        regime = regime.lower()
        if regime not in self._stats:
            log.debug("RegimeWeightTable: unknown regime '%s' — skipping.", regime)
            return

        async with self._lock:
            s = self._stats[regime]
            s.total_pnl    += pnl
            s.last_updated  = time.time()

            if pnl > 0:
                s.wins += 1
            else:
                s.losses += 1

            if s.total_trades < RL_MIN_TRADES:
                log.debug(
                    "RL [%s]: trade %d/%d — holding multiplier at %.3f (cold start)",
                    regime, s.total_trades, RL_MIN_TRADES, s.multiplier,
                )
                self._cache[regime] = s.multiplier
                return

            old = s.multiplier
            if pnl > 0:
                s.multiplier = min(s.multiplier + RL_LEARN_RATE, RL_MAX_MULT)
                arrow = "↑"
            else:
                s.multiplier = max(s.multiplier - RL_LEARN_RATE, RL_MIN_MULT)
                arrow = "↓"

            self._cache[regime] = s.multiplier

            log.info(
                "RL weight update [%s]: pnl=%.2f%% mult %.4f → %.4f %s "
                "(wins=%d losses=%d win_rate=%.1f%%)",
                regime, pnl, old, s.multiplier, arrow,
                s.wins, s.losses, s.win_rate * 100,
            )

    # ── Sync read path (zero-cost, used inside analyze()) ─────────────────────
    def get_multiplier_sync(self, regime: str) -> float:
        """Fast sync read; at worst one trade cycle stale — acceptable."""
        return self._cache.get(regime.lower(), 1.0)

    # ── Async snapshot for GUI / status reporting ──────────────────────────────
    async def get_all(self) -> Dict[str, dict]:
        async with self._lock:
            return {r: s.to_dict() for r, s in self._stats.items()}

    async def reset_regime(self, regime: str) -> None:
        async with self._lock:
            if regime.lower() in self._stats:
                self._stats[regime.lower()] = RegimeStats()
                self._cache[regime.lower()] = 1.0
                log.info("RL: reset regime '%s' to default multiplier 1.0", regime)

    async def reset_all(self) -> None:
        async with self._lock:
            self._stats = {r: RegimeStats() for r in _REGIMES}
            self._cache = {r: 1.0 for r in _REGIMES}
            log.info("RL: all regime weights reset to 1.0")

    # ── Serialisation helpers (for save_state / load_state) ───────────────────
    def get_state(self) -> dict:
        """Return a pickle-safe dict (no Lock object)."""
        return {
            "stats": {
                r: {
                    "multiplier":   s.multiplier,
                    "wins":         s.wins,
                    "losses":       s.losses,
                    "total_pnl":    s.total_pnl,
                    "last_updated": s.last_updated,
                }
                for r, s in self._stats.items()
            }
        }

    def set_state(self, state: dict) -> None:
        """Restore from a dict produced by get_state()."""
        for r, d in state.get("stats", {}).items():
            if r in self._stats:
                s = self._stats[r]
                s.multiplier   = float(d.get("multiplier",   1.0))
                s.wins         = int  (d.get("wins",         0))
                s.losses       = int  (d.get("losses",       0))
                s.total_pnl    = float(d.get("total_pnl",    0.0))
                s.last_updated = float(d.get("last_updated", time.time()))
                self._cache[r] = s.multiplier
        log.info(
            "RL: regime weights restored — %s",
            {r: round(self._cache[r], 4) for r in _REGIMES},
        )


# ══════════════════════════════════════════════════════════════════════════════
# Data structures
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Signal:
    symbol:           str
    tf:               str
    direction:        str
    prob:             float
    regime:           str
    kelly_f:          float
    kelly_raw:        float
    z_score:          float
    confidence:       float
    sniper_boost:     bool
    regime_modifier:  float
    chop_sniper_only: bool
    trail_multiplier: float
    mtf_aligned:      bool  = True
    trust_factor:     float = 1.0
    high_conviction:  bool  = False
    conviction_votes: int   = 0
    oracle_boosted:   bool  = False
    cancel_buys_flag: bool  = False
    # [P31-WHALE-TRAIL] Whale oracle multiplier at signal creation time.
    # Populated by IntelligenceEngine.analyze() from the EnsembleJudge oracle
    # signal; kept at 1.0 (neutral) when no oracle signal is present.
    whale_multiplier: float = 1.0
    ts:               float = field(default_factory=time.time)


@dataclass
class PatternMatch:
    idx:        int
    similarity: float
    high_diff:  float
    low_diff:   float
    close_diff: float


class ConvictionResult(NamedTuple):
    multiplier:           float
    high_conviction:      bool
    votes:                int
    ema_direction:        str
    rsi_direction:        str
    oracle_boost_applied: bool
    cancel_pending_buys:  bool


# ══════════════════════════════════════════════════════════════════════════════
# [P14-1 + P15-2] EnsembleJudge
# ══════════════════════════════════════════════════════════════════════════════

class EnsembleJudge:

    @staticmethod
    def _ema(arr: np.ndarray, period: int) -> float:
        if len(arr) < period:
            return float(np.mean(arr))
        k   = 2.0 / (period + 1)
        ema = float(np.mean(arr[:period]))
        for price in arr[period:]:
            ema = price * k + ema * (1.0 - k)
        return ema

    @staticmethod
    def _rsi(closes: np.ndarray, period: int) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = np.diff(closes[-(period + 1):])
        avg_g  = float(np.mean(np.where(deltas > 0, deltas,  0.0)))
        avg_l  = float(np.mean(np.where(deltas < 0, -deltas, 0.0)))
        if avg_l == 0:
            return 100.0
        return 100.0 - (100.0 / (1.0 + avg_g / avg_l))

    @staticmethod
    def _atr(closes: np.ndarray, period: int) -> float:
        if len(closes) < period + 1:
            return float(np.std(closes)) if len(closes) > 1 else 0.0
        return float(np.mean(np.abs(np.diff(closes[-(period + 1):]))))

    @staticmethod
    def _whale_types() -> Tuple[str, str]:
        try:
            from arbitrator import ORACLE_WHALE_BUY, ORACLE_WHALE_SELL
            return ORACLE_WHALE_BUY, ORACLE_WHALE_SELL
        except ImportError:
            return "WHALE_SWEEP_BUY", "WHALE_SWEEP_SELL"

    def evaluate(
        self,
        closes:            np.ndarray,
        primary_direction: str,
        oracle_signal=None,
    ) -> ConvictionResult:
        WHALE_BUY, WHALE_SELL = self._whale_types()

        min_req = max(P14_EMA_SLOW, P14_RSI_PERIOD + 1, P14_ATR_PERIOD + 1)
        oracle_valid = (
            oracle_signal is not None
            and oracle_signal.is_valid()
            and oracle_signal.multiplier >= P15_ORACLE_MIN_WHALE_MULT
        )
        if len(closes) < min_req or primary_direction == "neutral":
            cancel_buys = (
                oracle_valid
                and oracle_signal.signal_type == WHALE_SELL
                and primary_direction != "short"
            )
            if cancel_buys:
                log.warning(
                    "[P15-2] Whale SELL on %s while primary='%s' (TA cold) "
                    "→ cancel_pending_buys=True",
                    getattr(oracle_signal, "symbol", "?"), primary_direction,
                )
            return ConvictionResult(
                multiplier=1.0, high_conviction=False, votes=0,
                ema_direction="neutral", rsi_direction="neutral",
                oracle_boost_applied=False, cancel_pending_buys=cancel_buys,
            )

        fast_ema = self._ema(closes, P14_EMA_FAST)
        slow_ema = self._ema(closes, P14_EMA_SLOW)
        ema_band = slow_ema * 0.0005
        if   fast_ema > slow_ema + ema_band: ema_dir = "long"
        elif fast_ema < slow_ema - ema_band: ema_dir = "short"
        else:                                ema_dir = "neutral"

        rsi_val   = self._rsi(closes, P14_RSI_PERIOD)
        atr_val   = self._atr(closes, P14_ATR_PERIOD)
        mid_price = float(closes[-1])
        atr_ratio = (atr_val / mid_price) if mid_price > 0 else 0.0
        vol_scale = min(1.0, atr_ratio / 0.02)
        adj_ob    = P14_RSI_OVERBOUGHT - 5.0 * vol_scale
        adj_os    = P14_RSI_OVERSOLD   + 5.0 * vol_scale
        if   rsi_val < adj_os: rsi_dir = "long"
        elif rsi_val > adj_ob: rsi_dir = "short"
        else:                  rsi_dir = "neutral"

        votes = int(ema_dir == primary_direction) + int(rsi_dir == primary_direction)
        ema_conflicts = ema_dir != "neutral" and ema_dir != primary_direction
        rsi_conflicts = rsi_dir != "neutral" and rsi_dir != primary_direction
        hard_conflict = ema_conflicts and rsi_conflicts

        sym_label = getattr(oracle_signal, "symbol", "?") if oracle_signal else "?"

        oracle_aligned = (
            oracle_valid and (
                (oracle_signal.signal_type == WHALE_BUY  and primary_direction == "long")
                or
                (oracle_signal.signal_type == WHALE_SELL and primary_direction == "short")
            )
        )
        oracle_opposing = (
            oracle_valid and (
                (oracle_signal.signal_type == WHALE_SELL and primary_direction == "long")
                or
                (oracle_signal.signal_type == WHALE_BUY  and primary_direction == "short")
            )
        )

        oracle_boost_applied = False
        cancel_buys          = False

        if votes == 2 and oracle_aligned:
            raw_mult             = P14_CONVICTION_BOOST * P15_ORACLE_BOOST
            multiplier           = min(1.0, raw_mult)
            high_conv            = True
            oracle_boost_applied = True
            log.warning(
                "[P15] Oracle: Coinbase Whale detected (+50%% Conviction) "
                "| %s %s EMA=%s RSI=%s WHALE=%s votes=2 mult=%.2f",
                sym_label, primary_direction.upper(),
                ema_dir, rsi_dir, oracle_signal.signal_type, multiplier,
            )
        elif votes == 2:
            multiplier = P14_CONVICTION_BOOST
            high_conv  = True
            log.info(
                "[P14-1] High Conviction: EMA=%s RSI=%s PRIMARY=%s mult=%.2f",
                ema_dir, rsi_dir, primary_direction, multiplier,
            )
        elif hard_conflict and oracle_opposing:
            multiplier  = P14_CONVICTION_PENALTY * P15_ORACLE_PENALTY
            high_conv   = False
            cancel_buys = (primary_direction == "long")
            log.warning(
                "[P15-2] Oracle OPPOSING + hard TA conflict %s [%s]: "
                "EMA=%s RSI=%s WHALE=%s → mult=%.2f cancel_buys=%s",
                sym_label, primary_direction.upper(),
                ema_dir, rsi_dir, oracle_signal.signal_type,
                multiplier, cancel_buys,
            )
        elif hard_conflict:
            multiplier = P14_CONVICTION_PENALTY
            high_conv  = False
            log.info(
                "[P14-1] Low Conviction (TA conflict): EMA=%s RSI=%s ≠ %s | mult=%.2f",
                ema_dir, rsi_dir, primary_direction, multiplier,
            )
        elif oracle_opposing:
            multiplier  = 1.0 - (1.0 - P14_CONVICTION_PENALTY) * _P15_SOFT_PENALTY_FACTOR
            high_conv   = False
            cancel_buys = (primary_direction == "long")
            log.warning(
                "[P15-2] Coinbase Whale opposing %s [%s]: "
                "soft penalty mult=%.2f cancel_pending_buys=%s",
                sym_label, primary_direction.upper(), multiplier, cancel_buys,
            )
        else:
            multiplier = 1.0
            high_conv  = False
            log.debug(
                "[P14-1] Neutral: EMA=%s RSI=%s PRIMARY=%s votes=%d oracle=%s",
                ema_dir, rsi_dir, primary_direction, votes,
                oracle_signal.signal_type if oracle_valid else "none",
            )

        return ConvictionResult(
            multiplier=multiplier, high_conviction=high_conv, votes=votes,
            ema_direction=ema_dir, rsi_direction=rsi_dir,
            oracle_boost_applied=oracle_boost_applied,
            cancel_pending_buys=cancel_buys,
        )


# ══════════════════════════════════════════════════════════════════════════════
# [P26] Slippage Circuit Breaker
# ══════════════════════════════════════════════════════════════════════════════

class SlippageCircuitBreaker:
    """
    [P26] Tracks a rolling window of execution slippage (in bps) and enforces
    a circuit breaker when average slippage exceeds P26_SLIP_GUARD_BPS.

    When tripped
    ───────────
    • ``execution_mode`` returns ``"LIMIT_ONLY"`` — the execution layer must
      refuse market orders and route exclusively via limit orders.
    • ``bbo_clamp`` returns ``P26_BBO_CLAMP_BPS`` (default 1.0) — the
      p16_bbo_offset_bps must be clamped to this value to reduce queue
      aggression until slippage normalises.

    The breaker resets automatically when the rolling average falls back below
    P26_SLIP_GUARD_BPS after new, lower-slippage fills are recorded.

    Usage (from Orchestrator/Executor after each fill)
    ──────────────────────────────────────────────────
        engine.slippage_guard.record_slip(symbol, slip_bps)
        if engine.slippage_guard.tripped:
            bbo = engine.slippage_guard.bbo_clamp          # 1.0
            mode = engine.slippage_guard.execution_mode    # "LIMIT_ONLY"
    """

    def __init__(self) -> None:
        self._slip_window: List[float] = []
        self._window_size: int         = P26_SLIP_WINDOW
        self._guard_bps:   float       = P26_SLIP_GUARD_BPS
        self._clamp_bps:   float       = P26_BBO_CLAMP_BPS
        self._tripped:     bool        = False

    # ── Public API ─────────────────────────────────────────────────────────────

    def record_slip(self, symbol: str, slip_bps: float) -> None:
        """
        Record a new fill's slippage observation (in bps, positive = adverse).

        Maintains a rolling window of P26_SLIP_WINDOW observations.  After
        every update the circuit-breaker state is re-evaluated.
        """
        self._slip_window.append(float(slip_bps))
        if len(self._slip_window) > self._window_size:
            self._slip_window.pop(0)
        self._evaluate()

    @property
    def avg_slip_bps(self) -> float:
        """Rolling average slippage in bps (0.0 when no data yet)."""
        if not self._slip_window:
            return 0.0
        return float(sum(self._slip_window) / len(self._slip_window))

    @property
    def tripped(self) -> bool:
        """True when the circuit breaker is currently active."""
        return self._tripped

    @property
    def execution_mode(self) -> str:
        """Return ``"LIMIT_ONLY"`` when tripped, else ``"NORMAL"``."""
        return "LIMIT_ONLY" if self._tripped else "NORMAL"

    @property
    def bbo_clamp(self) -> float:
        """
        Return the clamped p16_bbo_offset_bps value.

        When tripped: P26_BBO_CLAMP_BPS (1.0 by default).
        When normal:  returns a large sentinel (9999.0) meaning "no clamp".
        """
        return self._clamp_bps if self._tripped else 9999.0

    def to_dict(self) -> dict:
        """Serialisable snapshot for status/dashboard reporting."""
        return {
            "tripped":        self._tripped,
            "avg_slip_bps":   round(self.avg_slip_bps, 2),
            "guard_bps":      self._guard_bps,
            "execution_mode": self.execution_mode,
            "bbo_clamp":      self._clamp_bps if self._tripped else None,
            "window_size":    self._window_size,
            "observations":   len(self._slip_window),
        }

    # ── Internal ────────────────────────────────────────────────────────────────

    def _evaluate(self) -> None:
        """Re-evaluate the breaker state after each new observation."""
        avg = self.avg_slip_bps
        was_tripped = self._tripped
        self._tripped = avg > self._guard_bps
        if self._tripped and not was_tripped:
            log.warning(
                "[P26-SLIP] Circuit breaker TRIPPED: avg_slip=%.1f bps > %.1f bps "
                "→ LIMIT_ONLY mode, p16_bbo_offset clamped to %.1f bps",
                avg, self._guard_bps, self._clamp_bps,
            )
        elif not self._tripped and was_tripped:
            log.info(
                "[P26-SLIP] Circuit breaker RESET: avg_slip=%.1f bps <= %.1f bps "
                "→ returning to NORMAL execution mode",
                avg, self._guard_bps,
            )


# ══════════════════════════════════════════════════════════════════════════════
# [P13-1] Bayesian Trust Tracker
# ══════════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════════
# [P32-SLIP-ADAPT] Per-Symbol Slippage-Adaptive Tracker
# ══════════════════════════════════════════════════════════════════════════════

class SlippagePerSymbolTracker:
    """
    [P32-SLIP-ADAPT] Tracks realized slippage on a per-symbol basis and
    enforces a LIMIT_ONLY window when the rolling average exceeds the threshold.

    Unlike the global SlippageCircuitBreaker (which affects all symbols), this
    tracker is surgical — it only suppresses market orders for the specific
    symbol exhibiting bleeding slippage.

    Attributes
    ----------
    _slip_history : dict[str, deque]
        Rolling window of slippage observations (bps) per symbol.
    _limit_only_until : dict[str, float]
        Expiry timestamp for each symbol's LIMIT_ONLY lockout.
    """

    # These defaults mirror the P32 env vars parsed in executor.py so the
    # tracker can be used standalone (e.g. in unit tests) without the executor.
    WINDOW_TRADES: int   = _env_int("P32_SLIP_WINDOW_TRADES", 3)
    LIMIT_BPS:     float = _env_float("P32_SLIP_LIMIT_BPS", 10.0)
    LIMIT_HOURS:   float = _env_float("P32_SLIP_LIMIT_HOURS", 4.0)

    def __init__(self) -> None:
        self._slip_history:    Dict[str, object] = {}  # symbol → collections.deque
        self._limit_only_until: Dict[str, float] = {}

    def record(self, symbol: str, expected_px: float,
               fill_px: float, side: str) -> None:
        """
        Record a realized fill and update LIMIT_ONLY status.

        Parameters
        ----------
        symbol      : str   — instrument base symbol (e.g. "BTC")
        expected_px : float — reference price (e.g. limit price or mid-market)
        fill_px     : float — actual fill price
        side        : str   — "buy" or "sell"
        """
        if expected_px <= 0 or fill_px <= 0:
            return
        if side == "buy":
            slip_bps = (fill_px - expected_px) / expected_px * 10_000.0
        else:
            slip_bps = (expected_px - fill_px) / expected_px * 10_000.0

        from collections import deque as _deque
        if symbol not in self._slip_history:
            self._slip_history[symbol] = _deque(maxlen=self.WINDOW_TRADES)
        self._slip_history[symbol].append(slip_bps)

        window = list(self._slip_history[symbol])
        if len(window) >= self.WINDOW_TRADES:
            avg = sum(window) / len(window)
            if avg > self.LIMIT_BPS:
                expiry = time.time() + self.LIMIT_HOURS * 3600.0
                self._limit_only_until[symbol] = expiry
                log.warning(
                    "[P32-SLIP-ADAPT] SlippagePerSymbolTracker %s: "
                    "avg_slip=%.2f bps > %.1f bps → LIMIT_ONLY for %.1f h",
                    symbol, avg, self.LIMIT_BPS, self.LIMIT_HOURS,
                )

    def is_limit_only(self, symbol: str) -> bool:
        """Return True if the symbol is currently in LIMIT_ONLY lockout."""
        expiry = self._limit_only_until.get(symbol, 0.0)
        if expiry > time.time():
            return True
        if symbol in self._limit_only_until:
            del self._limit_only_until[symbol]
        return False

    def avg_slip_bps(self, symbol: str) -> float:
        """Rolling average slippage in bps for a symbol (0.0 if no data)."""
        history = self._slip_history.get(symbol)
        if not history:
            return 0.0
        window = list(history)
        return float(sum(window) / len(window)) if window else 0.0

    def to_dict(self) -> dict:
        """Serialisable snapshot for status/dashboard reporting."""
        now = time.time()
        return {
            "per_symbol": {
                sym: {
                    "avg_slip_bps":  round(self.avg_slip_bps(sym), 2),
                    "limit_only":    self.is_limit_only(sym),
                    "limit_until":   round(self._limit_only_until.get(sym, 0.0), 1),
                    "limit_remaining_secs": round(
                        max(0.0, self._limit_only_until.get(sym, 0.0) - now), 1
                    ),
                }
                for sym in set(list(self._slip_history.keys()) +
                               list(self._limit_only_until.keys()))
            },
            "window_trades": self.WINDOW_TRADES,
            "limit_bps":     self.LIMIT_BPS,
            "limit_hours":   self.LIMIT_HOURS,
        }


class BayesianTracker:

    def __init__(self) -> None:
        self._params: Dict[str, List[float]] = {}
        # [P30.5-REGIME] Track last known regime per (symbol, tf) key so we can
        # detect transitions and apply the 20 % Soft Reset decay.
        self._last_regime: Dict[str, str] = {}

    def _key(self, symbol: str, tf: str) -> str:
        return f"{symbol.upper()}:{tf}"

    def _ensure(self, symbol: str, tf: str) -> List[float]:
        k = self._key(symbol, tf)
        if k not in self._params:
            self._params[k] = [P13_TRUST_PRIOR_ALPHA, P13_TRUST_PRIOR_BETA]
        return self._params[k]

    def _apply_regime_decay(self, symbol: str, tf: str, current_regime: str) -> None:
        """
        [P30.5-REGIME] Soft Reset — 20 % decay on regime shift.

        When the HMM detects a new regime that differs from the last observed
        regime for this (symbol, tf) pair, the accumulated alpha/beta posterior
        is pulled back 20 % toward the symmetric prior (α₀=P13_TRUST_PRIOR_ALPHA,
        β₀=P13_TRUST_PRIOR_BETA).  This prevents past-regime wins/losses from
        biasing trust decisions in the new market environment.

        Mathematically:
            α_new = α_prior + 0.80 × (α_old − α_prior)
            β_new = β_prior + 0.80 × (β_old − β_prior)

        The decay is applied BEFORE the current observation is incorporated so
        the new regime's first data-point has the correct (decayed) posterior
        as its starting point.
        """
        k = self._key(symbol, tf)
        regime_norm = (current_regime or "").lower()
        last        = self._last_regime.get(k, "")
        if last and last != regime_norm and k in self._params:
            ab = self._params[k]
            _DECAY = 0.80   # retain 80 %, discard 20 %
            ab[0] = P13_TRUST_PRIOR_ALPHA + _DECAY * (ab[0] - P13_TRUST_PRIOR_ALPHA)
            ab[1] = P13_TRUST_PRIOR_BETA  + _DECAY * (ab[1] - P13_TRUST_PRIOR_BETA)
            log.info(
                "[P30.5-REGIME] BayesianTracker soft-reset %s: regime %s→%s "
                "α=%.3f β=%.3f (20%% decay applied)",
                k, last, regime_norm, ab[0], ab[1],
            )
        self._last_regime[k] = regime_norm

    def update(self, symbol: str, tf: str, win: bool,
               current_regime: str = "") -> None:
        """
        Record a trade outcome and update the Beta distribution.

        [P26-VELOCITY] The first P13_TRUST_VELOCITY_SAMPLES (default 5)
        observations are weighted by P13_TRUST_VELOCITY_WEIGHT (default 2.0)
        so the trust factor moves away from the 0.5 neutral zone faster during
        the cold-start period.  Beyond that window each sample has weight 1.0
        (standard Bayesian update).

        This resolves the "Signal Stagnation" issue where trust factors remain
        stuck at 0.5 with 0 samples because the symmetric Beta(2, 2) prior
        requires many observations before the posterior mean diverges from 0.5.

        [P30.5-REGIME] If current_regime is supplied and differs from the last
        recorded regime for this key, a 20 % Soft Reset decay is applied to the
        posterior BEFORE incorporating this observation.
        """
        # [P30.5-REGIME] Decay on regime shift (before update so it's consistent)
        if current_regime:
            self._apply_regime_decay(symbol, tf, current_regime)

        ab = self._ensure(symbol, tf)
        n_samples = self.sample_count(symbol, tf)
        # [P26-VELOCITY] Higher weight for early samples
        weight = (
            P13_TRUST_VELOCITY_WEIGHT
            if n_samples < P13_TRUST_VELOCITY_SAMPLES
            else 1.0
        )
        ab[0 if win else 1] += weight

    def trust_factor(self, symbol: str, tf: str) -> float:
        ab   = self._ensure(symbol, tf)
        a, b = ab[0], ab[1]
        if _SCIPY_OK:
            return float(1.0 - _betainc(a, b, 0.5))
        return float(a / (a + b))

    def sample_count(self, symbol: str, tf: str) -> int:
        ab = self._params.get(self._key(symbol, tf))
        if ab is None:
            return 0
        return max(0, int(round(ab[0] + ab[1] - P13_TRUST_PRIOR_ALPHA - P13_TRUST_PRIOR_BETA)))

    def is_censored(self, symbol: str, tf: str) -> bool:
        if self.sample_count(symbol, tf) < P13_TRUST_MIN_SAMPLES:
            return False
        return self.trust_factor(symbol, tf) < P13_TRUST_CIRCUIT_BREAKER

    def snapshot(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for key, (a, b) in self._params.items():
            sym, tf    = key.split(":", 1)
            tf_trust   = self.trust_factor(sym, tf)
            tf_samples = self.sample_count(sym, tf)
            label = (
                "Censored" if tf_samples >= P13_TRUST_MIN_SAMPLES and tf_trust < P13_TRUST_CIRCUIT_BREAKER
                else "Weak"    if tf_trust < 0.50
                else "Neutral" if tf_trust < 0.65
                else "Strong"
            )
            out[key] = {
                "alpha":        round(a, 2),
                "beta":         round(b, 2),
                "trust_factor": round(tf_trust, 4),
                "samples":      tf_samples,
                "label":        label,
            }
        return out


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def _rolling_vol(closes: np.ndarray, window: int = 20) -> float:
    if len(closes) < window + 1:
        return 1.0
    lr = np.diff(np.log(closes[-(window + 1):]))
    return float(np.std(lr) * np.sqrt(365 * 24))


def _z_score(value: float, arr: np.ndarray) -> float:
    if len(arr) < 2:
        return 0.0
    mu, sigma = float(np.mean(arr)), float(np.std(arr))
    return (value - mu) / sigma if sigma > 0 else 0.0


def apply_sniper_multiplier(
    kelly_f: float, confidence: float, symbol: str, tf: str,
) -> Tuple[float, bool]:
    if confidence <= SNIPER_CONFIDENCE_THRESHOLD:
        return kelly_f, False
    span       = 1.0 - SNIPER_CONFIDENCE_THRESHOLD
    progress   = (confidence - SNIPER_CONFIDENCE_THRESHOLD) / span if span > 0 else 1.0
    multiplier = SNIPER_MIN_MULTIPLIER + progress * (SNIPER_MAX_MULTIPLIER - SNIPER_MIN_MULTIPLIER)
    capped     = min(kelly_f * multiplier, MAX_ALLOC_PCT)
    log.info(
        "🎯 SNIPER BOOST %s [%s]: kelly %.4f × %.2f → %.4f conf=%.3f",
        symbol, tf, kelly_f, multiplier, capped, confidence,
    )
    return capped, True


def mtf_align_filter(h1: Optional["Signal"], h4: Optional["Signal"]) -> bool:
    if h1 is None or h4 is None:
        return True
    d1, d4  = h1.direction, h4.direction
    aligned = True if (d1 == "neutral" or d4 == "neutral") else (d1 == d4)
    symbol  = h1.symbol

    if _SENTINEL_OK and _mtf_log is not None:
        _mtf_log.update(symbol, d1, d4, aligned)
    else:
        prev = _mtf_fallback_state.get(symbol)
        if prev != aligned:
            _mtf_fallback_state[symbol] = aligned
            if not aligned:
                log.info("MTF CONFLICT %s: 1H=%s vs 4H=%s → neutral", symbol, d1, d4)
            else:
                log.info("MTF RESTORED %s: 1H/4H agree (%s)", symbol, d1)
    return aligned


# ══════════════════════════════════════════════════════════════════════════════
# Pattern Library  [P14-3]
# ══════════════════════════════════════════════════════════════════════════════

class PatternLibrary:
    DIM = 1

    def __init__(self, dim: int = 1):
        self.dim    = dim
        self._vecs: Optional[np.ndarray] = None
        self._meta: List[dict]            = []
        if _FAISS_OK:
            self._index = faiss.IndexFlatL2(dim)
        else:
            self._index = None

    def _normalize(self, v: np.ndarray) -> np.ndarray:
        n = np.linalg.norm(v)
        return v / n if n > 0 else v

    def add(
        self, vec: np.ndarray,
        high_diff: float, low_diff: float, close_diff: float,
        weight: float = 1.0,
    ) -> None:
        v = self._normalize(vec.astype(np.float32).reshape(1, self.dim))
        if _FAISS_OK:
            self._index.add(v)
        self._vecs = v if self._vecs is None else np.vstack([self._vecs, v])
        self._meta.append({
            "high_diff":  high_diff,
            "low_diff":   low_diff,
            "close_diff": close_diff,
            "weight":     weight,
        })

    def update_weight(self, idx: int, delta: float, clip: float = 2.0) -> None:
        if 0 <= idx < len(self._meta):
            self._meta[idx]["weight"] = float(
                np.clip(self._meta[idx]["weight"] + delta, 0.0, clip)
            )

    def search(
        self, vec: np.ndarray, k: int = 20, threshold: float = 0.97,
    ) -> List[PatternMatch]:
        if self._vecs is None or not self._meta:
            return []
        v = self._normalize(vec.astype(np.float32).reshape(1, self.dim))
        k = min(k, len(self._meta))

        if _FAISS_OK:
            dists, idxs = self._index.search(v, k)
            dists, idxs = dists[0], idxs[0]
            sims = 1.0 / (1.0 + dists)
        else:
            dots = (self._vecs @ v.T).flatten()
            idxs = np.argsort(dots)[::-1][:k]
            sims = dots[idxs]

        return [
            PatternMatch(
                idx=int(i),
                similarity=float(s),
                high_diff =self._meta[i]["high_diff"]  * self._meta[i]["weight"],
                low_diff  =self._meta[i]["low_diff"]   * self._meta[i]["weight"],
                close_diff=self._meta[i]["close_diff"] * self._meta[i]["weight"],
            )
            for s, i in zip(sims, idxs)
            if s >= threshold and 0 <= int(i) < len(self._meta)
        ]

    def size(self) -> int:
        return len(self._meta)

    def prune_low_weight(
        self,
        weight_threshold: float = P14_PRUNE_WEIGHT_THRESHOLD,
        max_patterns:     int   = P14_PRUNE_MAX_PATTERNS,
    ) -> int:
        if not self._meta:
            return 0
        before    = len(self._meta)
        survivors = [
            (i, m) for i, m in enumerate(self._meta)
            if m["weight"] >= weight_threshold
        ]
        if len(survivors) > max_patterns:
            survivors.sort(key=lambda x: x[1]["weight"], reverse=True)
            survivors = survivors[:max_patterns]
        removed = before - len(survivors)
        if removed == 0:
            return 0

        new_meta: List[dict]            = []
        new_vecs_list: List[np.ndarray] = []
        for orig_idx, meta_entry in survivors:
            new_meta.append(meta_entry)
            if self._vecs is not None and orig_idx < len(self._vecs):
                new_vecs_list.append(self._vecs[orig_idx])

        self._meta = new_meta
        self._vecs = np.vstack(new_vecs_list) if new_vecs_list else None
        if _FAISS_OK:
            self._index = faiss.IndexFlatL2(self.dim)
            if self._vecs is not None:
                self._index.add(self._vecs.astype(np.float32))

        log.info(
            "[P14-3] PatternLibrary pruned: removed=%d survivors=%d "
            "(threshold=%.2f max=%d)",
            removed, len(self._meta), weight_threshold, max_patterns,
        )
        return removed

    def get_state(self) -> dict:
        return {"dim": self.dim, "vecs": self._vecs, "meta": self._meta}

    def set_state(self, state: dict) -> None:
        self.dim   = state["dim"]
        self._meta = state["meta"]
        self._vecs = state["vecs"]
        if _FAISS_OK:
            self._index = faiss.IndexFlatL2(self.dim)
            if self._vecs is not None and len(self._vecs):
                self._index.add(self._vecs.astype(np.float32))
        else:
            self._index = None


# ══════════════════════════════════════════════════════════════════════════════
# Regime Detector
# ══════════════════════════════════════════════════════════════════════════════

class RegimeDetector:
    N_STATES = 3
    MIN_OBS  = 60

    def __init__(self):
        self._model        = None
        self._fitted       = False
        self._state_labels = ["chop", "bull", "bear"]

    def fit(self, closes: np.ndarray) -> None:
        if len(closes) < self.MIN_OBS:
            return
        lr = np.diff(np.log(closes)).reshape(-1, 1)
        if _HMM_OK:
            model = _hmm.GaussianHMM(
                n_components=self.N_STATES,
                covariance_type="diag",
                n_iter=100,
                random_state=42,
            )
            try:
                model.fit(lr)
                means     = model.means_.flatten()
                order     = np.argsort(means)
                label_map = {order[0]: "bear", order[1]: "chop", order[2]: "bull"}
                self._state_labels = [label_map[i] for i in range(self.N_STATES)]
                self._model  = model
                self._fitted = True
            except Exception as exc:
                log.warning("HMM fit failed: %s", exc)

    def predict(self, closes: np.ndarray) -> str:
        if self._fitted and self._model and len(closes) > 2:
            try:
                lr    = np.diff(np.log(closes)).reshape(-1, 1)
                state = self._model.predict(lr)[-1]
                return self._state_labels[state]
            except Exception:
                pass
        return self._rule_based(closes)

    @staticmethod
    def _rule_based(closes: np.ndarray) -> str:
        if len(closes) < 20:
            return "chop"
        s_ma = float(np.mean(closes[-5:]))
        l_ma = float(np.mean(closes[-20:]))
        vol  = _rolling_vol(closes)
        if vol > 1.5:            return "bear"
        if s_ma > l_ma * 1.005: return "bull"
        if s_ma < l_ma * 0.995: return "bear"
        return "chop"

    def get_state(self) -> dict:
        return {
            "model":        self._model,
            "fitted":       self._fitted,
            "state_labels": self._state_labels,
        }

    def set_state(self, state: dict) -> None:
        self._model        = state["model"]
        self._fitted       = state["fitted"]
        self._state_labels = state["state_labels"]


# ══════════════════════════════════════════════════════════════════════════════
# [P20-3] Shadow Regime Detector result
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ShadowTrainResult:
    symbol:           str
    live_sharpe:      float
    shadow_sharpe:    float
    swap_recommended: bool
    swapped:          bool
    ts:               float = field(default_factory=time.time)


def _compute_sharpe(closes: np.ndarray, detector: RegimeDetector, lookback: int = 48) -> float:
    if len(closes) < lookback + 2:
        return 0.0
    window = closes[-(lookback + 2):]
    pnls: List[float] = []
    for i in range(1, len(window) - 1):
        ctx = window[:i + 1]
        try:
            regime = detector.predict(ctx)
        except Exception:
            regime = "chop"
        pos = {"bull": 1.0, "bear": -1.0, "chop": 0.0}.get(regime, 0.0)
        ret = (window[i + 1] - window[i]) / window[i] if window[i] > 0 else 0.0
        pnls.append(pos * ret)
    if not pnls:
        return 0.0
    arr  = np.array(pnls, dtype=np.float64)
    mean = float(np.mean(arr))
    std  = float(np.std(arr))
    if std < 1e-10:
        return 0.0
    return float(mean / std * np.sqrt(365 * 24))


def _cpu_shadow_train(closes: np.ndarray, live_state: dict) -> dict:
    shadow = RegimeDetector()
    shadow.fit(closes)
    live = RegimeDetector()
    live.set_state(live_state)
    return {
        "shadow_state":  shadow.get_state(),
        "shadow_sharpe": _compute_sharpe(closes, shadow),
        "live_sharpe":   _compute_sharpe(closes, live),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Bayesian Fusion
# ══════════════════════════════════════════════════════════════════════════════

class BayesianFusion:

    def __init__(self):
        self._priors: Dict[str, Dict[str, List[float]]] = {}

    def _ensure(self, k: str) -> None:
        if k not in self._priors:
            self._priors[k] = {
                "long":    [1.0, 1.0],
                "short":   [1.0, 1.0],
                "neutral": [1.0, 1.0],
            }

    def update(self, symbol: str, tf: str, direction: str, success: bool) -> None:
        k = f"{symbol}:{tf}"
        self._ensure(k)
        ab = self._priors[k].get(direction, [1.0, 1.0])
        ab[0 if success else 1] += 1
        self._priors[k][direction] = ab

    def posterior_mean(self, symbol: str, tf: str, direction: str) -> float:
        k = f"{symbol}:{tf}"
        self._ensure(k)
        a, b = self._priors[k][direction]
        return a / (a + b)

    def best_direction(
        self, symbol: str, tf: str, candidate: str, p_candidate: float,
    ) -> Tuple[str, float]:
        prior   = self.posterior_mean(symbol, tf, candidate)
        blended = 0.70 * p_candidate + 0.30 * prior
        return candidate, round(blended, 4)


# ══════════════════════════════════════════════════════════════════════════════
# Kelly Criterion
# ══════════════════════════════════════════════════════════════════════════════

def fractional_kelly(
    win_prob: float, win_loss_ratio: float, fraction: float = 0.25,
) -> float:
    if win_loss_ratio <= 0 or win_prob <= 0:
        return 0.0
    p, q, b = win_prob, 1 - win_prob, win_loss_ratio
    return round(min(max(0.0, (p * b - q) / b * fraction), 1.0), 4)


# ══════════════════════════════════════════════════════════════════════════════
# Correlation Guard
# ══════════════════════════════════════════════════════════════════════════════

class CorrelationGuard:

    def __init__(self, threshold: float = 0.99, window: int = 50):
        self.threshold = threshold
        self.window    = window
        self._returns: Dict[str, list] = {}

    def update(self, symbol: str, price: float) -> None:
        if symbol not in self._returns:
            self._returns[symbol] = []
        self._returns[symbol].append(price)
        if len(self._returns[symbol]) > self.window + 1:
            self._returns[symbol] = self._returns[symbol][-(self.window + 1):]

    def max_correlation(self) -> float:
        symbols = [s for s, d in self._returns.items() if len(d) >= 10]
        if len(symbols) < 2:
            return 0.0
        n = min(self.window, min(len(self._returns[s]) for s in symbols)) - 1
        if n < 20:
            return 0.0
        mat = np.array([
            np.diff(np.log(np.array(self._returns[s])[-n - 1:]))
            for s in symbols
        ])
        if any(np.std(row) < 1e-10 for row in mat):
            return 0.0
        corr = np.corrcoef(mat)
        np.fill_diagonal(corr, 0.0)
        return float(np.nanmax(np.abs(corr)))

    def is_frozen(self) -> bool:
        mc     = self.max_correlation()
        frozen = mc > self.threshold
        if frozen:
            log.warning(
                "Correlation guard: max_corr=%.3f > %.2f — new entries FROZEN",
                mc, self.threshold,
            )
        return frozen

    def position_correlation(self, symbol: str, open_positions: List[str]) -> float:
        candidates = [s for s in open_positions if s != symbol and s in self._returns]
        if not candidates or symbol not in self._returns:
            return 0.0
        sym_ret = self._returns.get(symbol, [])
        if len(sym_ret) < 10:
            return 0.0
        max_corr = 0.0
        for cand in candidates:
            cand_ret = self._returns.get(cand, [])
            n = min(len(sym_ret), len(cand_ret)) - 1
            if n < 10:
                continue
            s_arr = np.diff(np.log(np.array(sym_ret[-n - 1:])))
            c_arr = np.diff(np.log(np.array(cand_ret[-n - 1:])))
            if np.std(s_arr) < 1e-10 or np.std(c_arr) < 1e-10:
                continue
            corr = float(np.corrcoef(s_arr, c_arr)[0, 1])
            max_corr = max(max_corr, abs(corr))
        return max_corr


# ══════════════════════════════════════════════════════════════════════════════
# Master Intelligence Engine
# ══════════════════════════════════════════════════════════════════════════════

TF_CHOICES = ["1hour", "2hour", "4hour", "8hour", "12hour", "1day", "1week"]


class IntelligenceEngine:

    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.tfs     = TF_CHOICES

        self._libs:    Dict[tuple, PatternLibrary] = {}
        self._regimes: Dict[str, RegimeDetector]   = {}
        self._bayes    = BayesianFusion()
        self._corr     = CorrelationGuard()
        self.trust     = BayesianTracker()
        self._judge    = EnsembleJudge()

        # [P6-RL] Regime weight table — now lives here, not in brain_p6
        self._rl_table = RegimeWeightTable()

        # [P26] Slippage Circuit Breaker — record fills via
        # self.slippage_guard.record_slip(symbol, slip_bps) from the Orchestrator.
        self.slippage_guard = SlippageCircuitBreaker()

        # [P32-SLIP-ADAPT] Per-symbol slippage tracker — tracks the last
        # P32_SLIP_WINDOW_TRADES (default 3) fills per symbol and enforces a
        # LIMIT_ONLY lockout when the rolling average exceeds P32_SLIP_LIMIT_BPS.
        # The Executor reads/writes this via brain.p32_slip_tracker.
        self.p32_slip_tracker = SlippagePerSymbolTracker()

        self._wins:    Dict[tuple, int]   = {}
        self._losses:  Dict[tuple, int]   = {}
        self._win_mag: Dict[tuple, float] = {}

        self._prune_task:        Optional[asyncio.Task] = None
        self._shadow_train_task: Optional[asyncio.Task] = None

        # [P20-1] High-Water Mark — backed by a guarded property.
        # Use _peak_equity_raw for internal raw storage; external code reads/writes
        # via the .peak_equity property which enforces the "never regress to zero"
        # rule introduced by the Cold-Start grace fix.
        self._peak_equity_raw: float = 0.0
        # Once a valid (> 0) equity has been observed the HWM is considered
        # "seeded". After seeding, any attempt to lower the HWM below the
        # current value — including setting it back to 0 — is silently ignored.
        # This prevents the "Zero State" cold-start hallucination where a
        # transient $0/$1 equity reading would reset the HWM and trigger a
        # spurious 99 % drawdown / Zombie Mode entry.
        self._hwm_seeded: bool = False

        # [P20-3] Shadow train bookkeeping
        self._shadow_results:    Dict[str, ShadowTrainResult] = {}
        self._shadow_candles_fn  = None

        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="p20_shadow_hmm",
        )

        self.oracle = None

    # ── [P20-1] peak_equity property — Cold-Start guard ───────────────────────

    @property
    def peak_equity(self) -> float:
        """Return the current High-Water Mark."""
        return self._peak_equity_raw

    @peak_equity.setter
    def peak_equity(self, value: float) -> None:
        """
        Guarded HWM setter.

        Rules:
          1. Values ≤ 0 are always rejected — they are not valid equity readings
             and most likely represent a DataHub cold-start race (WebSocket not
             yet delivered the first account push).
          2. Once the HWM has been seeded with a valid value (_hwm_seeded=True),
             the HWM may only increase — attempts to lower it (e.g. by assigning
             a stale/zero reading) are silently ignored.  The only way to reset
             the HWM is via the explicit reset_hwm() method below, which is
             called exclusively during unit tests and controlled re-starts.

        This logic prevents the "Zero State" cold-start hallucination:
          - Startup sequence: executor._equity = 0  →  attempt to seed HWM = 0
          - Old code: brain.peak_equity = 0, then first cycle sees equity = $1
            transient → drawdown = (HWM 136 − 1) / 136 = 99% → Zombie Mode ✗
          - New code: the assignment of 0 is rejected; HWM stays at its last
            persisted value until a real non-zero equity is delivered.
        """
        fval = float(value)
        if fval <= 0:
            # Never allow an invalid reading to corrupt the HWM.
            log.debug(
                "[P20-1] peak_equity setter: rejected zero/negative value %.4f "
                "(HWM remains %.2f, seeded=%s)",
                fval, self._peak_equity_raw, self._hwm_seeded,
            )
            return
        if not self._hwm_seeded:
            # First valid seed — accept unconditionally.
            self._peak_equity_raw = fval
            self._hwm_seeded = True
            log.info("[P20-1] HWM seeded for the first time: %.2f", fval)
        elif fval > self._peak_equity_raw:
            # Normal HWM advance — equity hit a new all-time high.
            self._peak_equity_raw = fval
            log.debug("[P20-1] HWM advanced: %.2f", fval)
        else:
            # Reject any attempt to lower a seeded HWM (cold-start protection).
            log.debug(
                "[P20-1] peak_equity setter: rejected %.4f < current HWM %.2f "
                "(cold-start guard active)",
                fval, self._peak_equity_raw,
            )

    def reset_hwm(self, new_value: float = 0.0) -> None:
        """
        Explicitly reset the High-Water Mark.  Bypasses the cold-start guard.
        Use ONLY in tests or a deliberate account-reset workflow.
        """
        self._peak_equity_raw = float(new_value)
        self._hwm_seeded = new_value > 0
        log.warning("[P20-1] HWM explicitly reset to %.2f (seeded=%s)",
                    self._peak_equity_raw, self._hwm_seeded)

    # ── [P6-RL] Public RL interface ────────────────────────────────────────────

    async def update_weights(self, regime: str, pnl: float) -> None:
        """
        [P6-RL] Update the RL weight for the given regime based on trade outcome.
        Call this after every position close (replaces RLBrain.update_weights).
        Also delegates to record_outcome for Bayesian updating when tf is known.
        """
        await self._rl_table.update(regime, pnl)

    async def get_regime_weights(self) -> Dict[str, dict]:
        """[P6-RL] Full snapshot of regime stats for GUI / status reporting."""
        return await self._rl_table.get_all()

    async def reset_regime_weight(self, regime: str) -> None:
        """[P6-RL] Reset a single regime back to multiplier=1.0."""
        await self._rl_table.reset_regime(regime)

    async def reset_all_weights(self) -> None:
        """[P6-RL] Reset all regime weights to 1.0."""
        await self._rl_table.reset_all()

    # ── [P20-3] Candle source injection ───────────────────────────────────────
    def set_candle_source(self, fn) -> None:
        self._shadow_candles_fn = fn
        log.info("[P20-3] Shadow candle source injected.")

    # ── [P19-1] State Persistence ──────────────────────────────────────────────

    def save_state(self, path: str = BRAIN_STATE_PATH) -> bool:
        """
        [P19-1 / P20-1 / P20-3 / P6-RL] Persist all learnt state to disk.

        Saved components (additions vs original):
          - [P6-RL]  RegimeWeightTable state (all regime multipliers + stats)
          - [P20-1]  peak_equity (HWM)
          - [P20-3]  shadow_results metadata
          - All original P19-1 components unchanged
        """
        try:
            shadow_meta = {
                sym: {
                    "live_sharpe":      r.live_sharpe,
                    "shadow_sharpe":    r.shadow_sharpe,
                    "swap_recommended": r.swap_recommended,
                    "swapped":          r.swapped,
                    "ts":               r.ts,
                }
                for sym, r in self._shadow_results.items()
            }
            state = {
                "version":   "P20-1",
                "ts":        time.time(),
                "symbols":   list(self.symbols),
                "regimes":   {
                    sym: det.get_state()
                    for sym, det in self._regimes.items()
                },
                "libs":      {
                    f"{sym}:{tf}": lib.get_state()
                    for (sym, tf), lib in self._libs.items()
                },
                "bayes_priors":  self._bayes._priors,
                "trust_params":  self.trust._params,
                # [P30.5-REGIME] Persist last-known regime so soft-reset survives restarts
                "trust_last_regime": self.trust._last_regime,
                "wins":          {f"{s}:{t}": v for (s, t), v in self._wins.items()},
                "losses":        {f"{s}:{t}": v for (s, t), v in self._losses.items()},
                "win_mag":       {f"{s}:{t}": v for (s, t), v in self._win_mag.items()},
                # [P20-1] HWM — only persist if a valid equity has been observed.
                # Persisting 0.0 would cause the next startup to treat the HWM
                # as unseeded and accept any first equity value (including a
                # hallucinated $1.00) as the new all-time high, re-introducing
                # the Zero State bug.
                "peak_equity":   self._peak_equity_raw if self._peak_equity_raw > 0 else None,
                "hwm_seeded":    self._hwm_seeded if self._peak_equity_raw > 0 else False,
                # [P20-3] Shadow metadata
                "shadow_results": shadow_meta,
                # [P6-RL] Regime weight table
                "rl_regime_weights": self._rl_table.get_state(),
            }
            tmp = path + ".tmp"
            with open(tmp, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, path)
            lib_count     = len(self._libs)
            regime_count  = len(self._regimes)
            pattern_total = sum(lib.size() for lib in self._libs.values())
            log.info(
                "[P19-1] Brain state saved → %s "
                "(regimes=%d libs=%d total_patterns=%d "
                "peak_equity=%.2f rl_weights=%s)",
                path, regime_count, lib_count, pattern_total,
                self.peak_equity,
                {r: round(self._rl_table.get_multiplier_sync(r), 4) for r in _REGIMES},
            )
            return True
        except Exception as exc:
            log.error("[P19-1] save_state failed: %s", exc, exc_info=True)
            return False

    def load_state(self, path: str = BRAIN_STATE_PATH) -> bool:
        """
        [P19-1 / P20-1 / P20-3 / P6-RL] Restore learnt state from disk.
        Returns True on success, False on any failure.
        """
        if not os.path.exists(path):
            log.info("[P19-1] No brain state file found at %s — will train fresh.", path)
            return False
        try:
            with open(path, "rb") as f:
                state = pickle.load(f)

            if not isinstance(state, dict) or state.get("version") not in ("P19-1", "P20-1"):
                log.warning(
                    "[P19-1] Brain state version mismatch or corrupt at %s — "
                    "ignoring and retraining.", path,
                )
                return False

            age_hours = (time.time() - state.get("ts", 0)) / 3600.0
            log.info(
                "[P19-1] Loading brain state from %s (age=%.1fh symbols=%s)",
                path, age_hours, state.get("symbols", []),
            )

            for sym, det_state in state.get("regimes", {}).items():
                det = self._regime_det(sym)
                det.set_state(det_state)

            for key, lib_state in state.get("libs", {}).items():
                parts = key.split(":", 1)
                if len(parts) != 2:
                    continue
                sym, tf = parts
                lib = self._lib(sym, tf)
                lib.set_state(lib_state)

            self._bayes._priors = state.get("bayes_priors", {})
            self.trust._params      = state.get("trust_params", {})
            # [P30.5-REGIME] Restore last-known regime for soft-reset continuity
            self.trust._last_regime = state.get("trust_last_regime", {})

            for key, v in state.get("wins", {}).items():
                parts = key.split(":", 1)
                if len(parts) == 2:
                    self._wins[(parts[0], parts[1])] = v
            for key, v in state.get("losses", {}).items():
                parts = key.split(":", 1)
                if len(parts) == 2:
                    self._losses[(parts[0], parts[1])] = v
            for key, v in state.get("win_mag", {}).items():
                parts = key.split(":", 1)
                if len(parts) == 2:
                    self._win_mag[(parts[0], parts[1])] = v

            # [P20-1] HWM — restore raw + seeded flag directly so the
            # cold-start guard is in the correct state from the first cycle.
            # [P20-1] HWM — restore raw + seeded flag.
            # peak_equity is None when save_state intentionally omitted a zero
            # value; treat that identically to a missing key (HWM unseeded).
            _raw_pe         = state.get("peak_equity")
            restored_hwm    = float(_raw_pe) if (_raw_pe is not None and float(_raw_pe) > 0) else 0.0
            restored_seeded = bool(state.get("hwm_seeded", restored_hwm > 0))
            self._peak_equity_raw = restored_hwm
            self._hwm_seeded      = restored_seeded
            if restored_hwm > 0:
                log.info("[P20-1] High-Water Mark restored: %.2f (seeded=%s)",
                         restored_hwm, restored_seeded)

            # [P20-3] Shadow metadata
            for sym, meta in state.get("shadow_results", {}).items():
                self._shadow_results[sym] = ShadowTrainResult(
                    symbol=sym,
                    live_sharpe=meta.get("live_sharpe", 0.0),
                    shadow_sharpe=meta.get("shadow_sharpe", 0.0),
                    swap_recommended=meta.get("swap_recommended", False),
                    swapped=meta.get("swapped", False),
                    ts=meta.get("ts", 0.0),
                )

            # [P6-RL] Regime weight table
            rl_state = state.get("rl_regime_weights")
            if rl_state:
                self._rl_table.set_state(rl_state)

            pattern_total = sum(lib.size() for lib in self._libs.values())
            log.info(
                "[P19-1] Brain state restored: "
                "regimes=%d libs=%d total_patterns=%d "
                "peak_equity=%.2f rl_weights=%s",
                len(self._regimes), len(self._libs), pattern_total,
                self.peak_equity,
                {r: round(self._rl_table.get_multiplier_sync(r), 4) for r in _REGIMES},
            )
            return True

        except Exception as exc:
            log.error(
                "[P19-1] load_state failed (%s): %s — will retrain.",
                path, exc, exc_info=True,
            )
            self._libs        = {}
            self._regimes     = {}
            self._bayes       = BayesianFusion()
            self.trust        = BayesianTracker()
            self._wins        = {}
            self._losses      = {}
            self._win_mag     = {}
            self.reset_hwm(0.0)   # explicit bypass — full state reset on corrupt load
            # Reset RL table to clean state on corrupt load
            self._rl_table    = RegimeWeightTable()
            return False

    # ── [P14-3] Background prune loop ─────────────────────────────────────────
    async def _prune_loop(self) -> None:
        log.info(
            "[P14-3] Pattern prune loop started (interval=%.0fh / %.0fs)",
            P14_PRUNE_INTERVAL_SECS / 3600, P14_PRUNE_INTERVAL_SECS,
        )
        while True:
            try:
                await asyncio.sleep(P14_PRUNE_INTERVAL_SECS)
            except asyncio.CancelledError:
                log.info("[P14-3] Prune loop cancelled.")
                return
            total = 0
            for (sym, tf), lib in list(self._libs.items()):
                try:
                    total += lib.prune_low_weight(
                        P14_PRUNE_WEIGHT_THRESHOLD, P14_PRUNE_MAX_PATTERNS,
                    )
                except Exception as exc:
                    log.error("[P14-3] Prune error %s/%s: %s", sym, tf, exc)
            log.info(
                "[P14-3] Housekeeping complete: total_removed=%d across %d libs",
                total, len(self._libs),
            )

    # ── [P20-3] Shadow Training loop ──────────────────────────────────────────
    async def _shadow_train_loop(self) -> None:
        log.info(
            "[P20-3] Shadow train loop started "
            "(interval=%.0fh  lookback=%dh  swap_threshold=%.2f  auto_swap=%s)",
            P20_SHADOW_INTERVAL_SECS / 3600,
            P20_SHADOW_LOOKBACK_HOURS,
            P20_SHADOW_SWAP_THRESHOLD,
            P20_SHADOW_AUTO_SWAP,
        )
        await asyncio.sleep(600)

        while True:
            try:
                await self._run_shadow_train_cycle()
            except asyncio.CancelledError:
                log.info("[P20-3] Shadow train loop cancelled.")
                return
            except Exception as exc:
                log.error("[P20-3] Shadow train loop error: %s", exc, exc_info=True)

            try:
                await asyncio.sleep(P20_SHADOW_INTERVAL_SECS)
            except asyncio.CancelledError:
                log.info("[P20-3] Shadow train loop cancelled during sleep.")
                return

    async def _run_shadow_train_cycle(self) -> None:
        if self._shadow_candles_fn is None:
            log.warning("[P20-3] No candle source set — skipping shadow train cycle.")
            return

        n_candles = max(P20_SHADOW_LOOKBACK_HOURS + 4, RegimeDetector.MIN_OBS + 4)
        loop = asyncio.get_event_loop()

        for sym in list(self.symbols):
            try:
                candles = await self._shadow_candles_fn(sym, "1hour", n_candles)
                if not candles or len(candles) < P20_SHADOW_MIN_OBS:
                    log.debug(
                        "[P20-3] Insufficient candles for %s (%d) — skipping.",
                        sym, len(candles) if candles else 0,
                    )
                    continue

                closes = np.array(
                    [c.close for c in reversed(candles)], dtype=np.float64
                )
                shadow_closes = closes[-P20_SHADOW_LOOKBACK_HOURS:]
                if len(shadow_closes) < P20_SHADOW_MIN_OBS:
                    log.debug("[P20-3] %s: shadow window too small (%d) — skipping.",
                              sym, len(shadow_closes))
                    continue

                live_det   = self._regime_det(sym)
                live_state = live_det.get_state()

                result_dict = await loop.run_in_executor(
                    self._executor,
                    _cpu_shadow_train,
                    shadow_closes,
                    live_state,
                )

                shadow_sharpe = result_dict["shadow_sharpe"]
                live_sharpe   = result_dict["live_sharpe"]
                swap_rec      = (
                    shadow_sharpe > live_sharpe * P20_SHADOW_SWAP_THRESHOLD
                    and shadow_sharpe > 0.0
                )
                swapped = False

                if swap_rec:
                    log.warning(
                        "[P20-3] ⚡ SWAP RECOMMENDED for %s: "
                        "shadow_sharpe=%.4f > live_sharpe=%.4f × %.2f (=%.4f). "
                        "auto_swap=%s",
                        sym, shadow_sharpe, live_sharpe,
                        P20_SHADOW_SWAP_THRESHOLD,
                        live_sharpe * P20_SHADOW_SWAP_THRESHOLD,
                        P20_SHADOW_AUTO_SWAP,
                    )
                    if P20_SHADOW_AUTO_SWAP:
                        shadow_det = RegimeDetector()
                        shadow_det.set_state(result_dict["shadow_state"])
                        self._regimes[sym] = shadow_det
                        swapped = True
                        log.warning(
                            "[P20-3] AUTO-SWAP executed for %s: "
                            "live HMM replaced with shadow HMM.", sym,
                        )
                else:
                    log.info(
                        "[P20-3] Shadow train %s: shadow_sharpe=%.4f  live_sharpe=%.4f "
                        "— live model retained.",
                        sym, shadow_sharpe, live_sharpe,
                    )

                self._shadow_results[sym] = ShadowTrainResult(
                    symbol=sym,
                    live_sharpe=live_sharpe,
                    shadow_sharpe=shadow_sharpe,
                    swap_recommended=swap_rec,
                    swapped=swapped,
                )

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("[P20-3] Shadow train error for %s: %s", sym, exc, exc_info=True)

    def shadow_train_snapshot(self) -> Dict[str, dict]:
        return {
            sym: {
                "live_sharpe":    round(r.live_sharpe,   4),
                "shadow_sharpe":  round(r.shadow_sharpe, 4),
                "swap_recommended": r.swap_recommended,
                "swapped":        r.swapped,
                "age_secs":       round(time.time() - r.ts, 1),
            }
            for sym, r in self._shadow_results.items()
        }

    def start_background_tasks(self) -> None:
        if self._prune_task is None or self._prune_task.done():
            self._prune_task = asyncio.create_task(
                self._prune_loop(), name="p14_prune_loop",
            )
            log.info("[P14-3] Background prune task created.")

        if self._shadow_train_task is None or self._shadow_train_task.done():
            self._shadow_train_task = asyncio.create_task(
                self._shadow_train_loop(), name="p20_shadow_train",
            )
            log.info("[P20-3] Shadow train background task created.")

    async def stop_background_tasks(self) -> None:
        for task in (self._prune_task, self._shadow_train_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._executor.shutdown(wait=False)
        log.info("[P14-3 / P20-3] Background tasks stopped.")

    # ── Internal helpers ───────────────────────────────────────────────────────
    def _lib(self, sym: str, tf: str) -> PatternLibrary:
        k = (sym, tf)
        if k not in self._libs:
            self._libs[k] = PatternLibrary(dim=PatternLibrary.DIM)
        return self._libs[k]

    def _regime_det(self, sym: str) -> RegimeDetector:
        if sym not in self._regimes:
            self._regimes[sym] = RegimeDetector()
        return self._regimes[sym]

    def position_correlation(self, symbol: str, open_symbols: List[str]) -> float:
        return self._corr.position_correlation(symbol, open_symbols)

    # ── Training ───────────────────────────────────────────────────────────────
    def train(
        self, symbol: str, tf: str,
        opens: np.ndarray, closes: np.ndarray,
        highs: np.ndarray, lows: np.ndarray,
    ) -> None:
        if len(closes) < 10:
            return
        lib = self._lib(symbol, tf)
        rd  = self._regime_det(symbol)
        rd.fit(closes)
        for i in range(1, len(closes)):
            if opens[i] == 0:
                continue
            pct = (closes[i] - opens[i]) / abs(opens[i]) * 100
            vec = np.array([pct], dtype=np.float32)
            if i + 1 < len(closes) and opens[i + 1] != 0:
                nxt = opens[i + 1]
                lib.add(
                    vec,
                    (highs[i + 1]  - nxt) / abs(nxt),
                    (lows[i + 1]   - nxt) / abs(nxt),
                    (closes[i + 1] - nxt) / abs(nxt),
                )
        log.info("Trained %s %s: library_size=%d", symbol, tf, lib.size())

    # ── Per-timeframe analysis [P14-1 + P15-2 + P6-RL] ───────────────────────
    def analyze(self, symbol: str, tf: str, candles) -> Optional[Signal]:
        """
        Per-timeframe signal generation.

        [P6-RL] After all existing computation, scales signal.confidence and
        signal.kelly_f by the current RL multiplier for the detected regime.
        Scaling is applied AFTER the sniper multiplier so that RL conviction
        amplifies or reduces the final sizing naturally.
        """
        if not candles or len(candles) < 5:
            return None
        c = candles[0]
        if c.open == 0:
            return None

        cur_pct = (c.close - c.open) / abs(c.open) * 100
        vec     = np.array([cur_pct], dtype=np.float32)
        closes  = np.array([x.close for x in reversed(candles)], dtype=np.float64)

        regime           = self._regime_det(symbol).predict(closes)
        regime_modifier  = _REGIME_KELLY_MODIFIER.get(regime, 1.0)
        trail_multiplier = _REGIME_TRAIL_TIGHTEN.get(regime, 1.0)
        chop_sniper_only = (regime == "chop")

        lib     = self._lib(symbol, tf)
        matches = lib.search(vec, k=20, threshold=0.90)
        self._corr.update(symbol, float(c.close))

        tf_trust = self.trust.trust_factor(symbol, tf)

        if self.trust.is_censored(symbol, tf):
            log.info(
                "[P13] Signal %s [%s] suppressed (trust=%.4f) → neutral",
                symbol, tf, tf_trust,
            )
            return Signal(
                symbol=symbol, tf=tf, direction="neutral", prob=0.5,
                regime=regime, kelly_f=0.0, kelly_raw=0.0, z_score=0.0,
                confidence=0.0, sniper_boost=False,
                regime_modifier=regime_modifier,
                chop_sniper_only=chop_sniper_only,
                trail_multiplier=trail_multiplier,
                mtf_aligned=True, trust_factor=tf_trust,
                high_conviction=False, conviction_votes=0,
                oracle_boosted=False, cancel_buys_flag=False,
            )

        if not matches:
            return Signal(
                symbol=symbol, tf=tf, direction="neutral", prob=0.5,
                regime=regime, kelly_f=0.0, kelly_raw=0.0, z_score=0.0,
                confidence=0.0, sniper_boost=False,
                regime_modifier=regime_modifier,
                chop_sniper_only=chop_sniper_only,
                trail_multiplier=trail_multiplier,
                mtf_aligned=True, trust_factor=tf_trust,
                high_conviction=False, conviction_votes=0,
                oracle_boosted=False, cancel_buys_flag=False,
            )

        total_w   = sum(m.similarity for m in matches) or 1.0
        avg_close = sum(m.close_diff * m.similarity for m in matches) / total_w

        if   avg_close > 0.001:  direction = "long"
        elif avg_close < -0.001: direction = "short"
        else:                    direction = "neutral"

        if   direction == "long":  raw_p = sum(1 for m in matches if m.close_diff > 0) / len(matches)
        elif direction == "short": raw_p = sum(1 for m in matches if m.close_diff < 0) / len(matches)
        else:                      raw_p = 0.5

        if   regime == "chop":                          raw_p = 0.5 + (raw_p - 0.5) * 0.4
        elif regime == "bear" and direction == "long":  raw_p *= 0.6
        elif regime == "bull" and direction == "short": raw_p *= 0.6

        _, prob = self._bayes.best_direction(symbol, tf, direction, raw_p)

        recent_pcts = np.array(
            [(x.close - x.open) / max(abs(x.open), 1e-9) * 100 for x in candles[:50]],
            dtype=np.float64,
        )
        z = _z_score(cur_pct, recent_pcts)

        k         = (symbol, tf)
        wins      = self._wins.get(k, 1)
        losses    = self._losses.get(k, 1)
        w_mag     = self._win_mag.get(k, 1.0)
        kelly_raw = fractional_kelly(wins / (wins + losses), w_mag, fraction=0.25)

        regime_bonus   = {"bull": 0.1, "bear": 0.1, "chop": -0.15}
        raw_confidence = float(min(1.0, max(0.0,
            prob * 0.6 + kelly_raw * 0.2 + regime_bonus.get(regime, 0.0) + 0.1
        )))

        oracle_sig = None
        if self.oracle is not None:
            try:
                oracle_sig = self.oracle.latest_signal(symbol)
            except Exception as exc:
                log.debug("[P15-2] Oracle signal fetch error %s: %s", symbol, exc)

        conviction = self._judge.evaluate(closes, direction, oracle_signal=oracle_sig)

        # [P31-WHALE-TRAIL] Extract oracle whale multiplier for dynamic trail scaling.
        _whale_mult = 1.0
        try:
            if oracle_sig is not None and oracle_sig.is_valid():
                _whale_mult = float(oracle_sig.multiplier)
        except Exception:
            _whale_mult = 1.0

        convinced_conf = float(min(1.0, max(0.0, raw_confidence * conviction.multiplier)))
        if conviction.multiplier != 1.0:
            log.debug(
                "[P14-1/P15-2] %s [%s] mult=%.2f raw=%.4f→%.4f "
                "votes=%d oracle_boost=%s cancel_buys=%s",
                symbol, tf, conviction.multiplier,
                raw_confidence, convinced_conf,
                conviction.votes,
                conviction.oracle_boost_applied,
                conviction.cancel_pending_buys,
            )

        confidence = float(min(1.0, max(0.0, convinced_conf * tf_trust)))

        kelly_adj      = kelly_raw * regime_modifier
        kelly_f, boost = apply_sniper_multiplier(kelly_adj, confidence, symbol, tf)

        # ── [P6-RL] Apply regime multiplier ────────────────────────────────────
        # Neutral signals (direction == "neutral") are not scaled — there is
        # nothing to amplify when we are not taking a position.
        if direction != "neutral":
            rl_mult = self._rl_table.get_multiplier_sync(regime)
            if rl_mult != 1.0:
                scaled_conf  = float(min(1.0, confidence * rl_mult))
                scaled_kelly = float(min(MAX_ALLOC_PCT, kelly_f * rl_mult))
                log.debug(
                    "[P6-RL] %s [%s] regime=%s rl_mult=%.3f "
                    "conf %.4f→%.4f kelly %.4f→%.4f",
                    symbol, tf, regime, rl_mult,
                    confidence, scaled_conf, kelly_f, scaled_kelly,
                )
                confidence = scaled_conf
                kelly_f    = scaled_kelly

        return Signal(
            symbol=symbol, tf=tf, direction=direction, prob=prob,
            regime=regime, kelly_f=kelly_f, kelly_raw=kelly_raw,
            z_score=z, confidence=confidence, sniper_boost=boost,
            regime_modifier=regime_modifier,
            chop_sniper_only=chop_sniper_only,
            trail_multiplier=trail_multiplier,
            mtf_aligned=True,
            trust_factor=tf_trust,
            high_conviction=conviction.high_conviction,
            conviction_votes=conviction.votes,
            oracle_boosted=conviction.oracle_boost_applied,
            cancel_buys_flag=conviction.cancel_pending_buys,
            whale_multiplier=_whale_mult,
        )

    # ── Multi-timeframe aggregation ────────────────────────────────────────────
    def aggregate_signals(self, symbol: str, signals: Dict[str, Signal]) -> Signal:
        tf_weight = {
            "1hour": 0.5,  "2hour": 0.7,  "4hour": 1.0,
            "8hour": 1.2,  "12hour": 1.5, "1day": 2.0, "1week": 2.5,
        }
        long_score = short_score = total_w = 0.0

        for tf, sig in signals.items():
            w = tf_weight.get(tf, 1.0) * sig.confidence
            if   sig.direction == "long":  long_score  += sig.prob * w
            elif sig.direction == "short": short_score += sig.prob * w
            total_w += w

        if total_w == 0:
            return list(signals.values())[0]

        long_score  /= total_w
        short_score /= total_w

        if   long_score  > short_score and long_score  > 0.5: direction, prob = "long",  long_score
        elif short_score > long_score  and short_score > 0.5: direction, prob = "short", short_score
        else:                                                  direction, prob = "neutral", max(long_score, short_score)

        best_sig             = max(signals.values(), key=lambda s: s.confidence)
        agg_regime_modifier  = min(s.regime_modifier  for s in signals.values())
        agg_trail_multiplier = min(s.trail_multiplier for s in signals.values())
        agg_chop_sniper_only = any(s.chop_sniper_only for s in signals.values())
        agg_kelly_raw        = max(s.kelly_raw         for s in signals.values())
        agg_kelly_adj        = agg_kelly_raw * agg_regime_modifier

        trust_vals = [s.trust_factor for s in signals.values() if s.trust_factor > 0]
        agg_trust  = float(np.prod(trust_vals) ** (1.0 / len(trust_vals))) if trust_vals else 1.0

        agg_high_conviction  = any(s.high_conviction  for s in signals.values())
        agg_conviction_votes = max((s.conviction_votes for s in signals.values()), default=0)

        agg_oracle_boosted = any(s.oracle_boosted   for s in signals.values())
        agg_cancel_buys    = any(s.cancel_buys_flag for s in signals.values())
        # [P31-WHALE-TRAIL] Aggregate: take the maximum whale_multiplier seen
        # across all timeframes so the trail gap stays wide as long as ANY TF
        # is flagging an active whale sweep.
        agg_whale_mult = max(
            (s.whale_multiplier for s in signals.values()), default=1.0
        )

        agg_conf = float(min(1.0, max(0.0,
            prob * max(s.confidence for s in signals.values()) * 1.2
        )))
        agg_kelly_f, agg_boosted = apply_sniper_multiplier(
            agg_kelly_adj, agg_conf, symbol, "aggregate",
        )
        conf = min(1.0, prob * agg_kelly_raw * 4 + 0.1)

        h1_sig  = signals.get("1hour")
        h4_sig  = signals.get("4hour")
        aligned = mtf_align_filter(h1_sig, h4_sig)
        if not aligned:
            direction = "neutral"
            prob      = 0.5

        # [P6-RL] Scale aggregate confidence + kelly by RL multiplier
        if direction != "neutral":
            agg_regime = best_sig.regime
            rl_mult    = self._rl_table.get_multiplier_sync(agg_regime)
            if rl_mult != 1.0:
                conf        = float(min(1.0, conf * rl_mult))
                agg_kelly_f = float(min(MAX_ALLOC_PCT, agg_kelly_f * rl_mult))
                log.debug(
                    "[P6-RL] aggregate %s regime=%s rl_mult=%.3f "
                    "conf→%.4f kelly→%.4f",
                    symbol, agg_regime, rl_mult, conf, agg_kelly_f,
                )

        return Signal(
            symbol=symbol, tf="aggregate",
            direction=direction, prob=round(prob, 4),
            regime=best_sig.regime,
            kelly_f=agg_kelly_f, kelly_raw=agg_kelly_raw,
            z_score=best_sig.z_score,
            confidence=round(conf, 4),
            sniper_boost=agg_boosted,
            regime_modifier=agg_regime_modifier,
            chop_sniper_only=agg_chop_sniper_only,
            trail_multiplier=agg_trail_multiplier,
            mtf_aligned=aligned,
            trust_factor=round(agg_trust, 4),
            high_conviction=agg_high_conviction,
            conviction_votes=agg_conviction_votes,
            oracle_boosted=agg_oracle_boosted,
            cancel_buys_flag=agg_cancel_buys,
            whale_multiplier=round(agg_whale_mult, 4),
        )

    def record_outcome(
        self, symbol: str, tf: str, direction: str, pnl_pct: float,
        regime: str = "",
    ) -> None:
        success = pnl_pct > 0
        self._bayes.update(symbol, tf, direction, success)
        # [P30.5-REGIME] Pass regime so BayesianTracker can apply soft-reset on shift
        self.trust.update(symbol, tf, win=success, current_regime=regime)
        log.debug(
            "[P13] Trust update %s [%s]: win=%s → trust=%.4f samples=%d regime=%s",
            symbol, tf, success,
            self.trust.trust_factor(symbol, tf),
            self.trust.sample_count(symbol, tf),
            regime or "?",
        )
        k = (symbol, tf)
        if success:
            self._wins[k]    = self._wins.get(k, 0) + 1
            self._win_mag[k] = self._win_mag.get(k, 1.0) * 0.9 + abs(pnl_pct) * 0.1
        else:
            self._losses[k] = self._losses.get(k, 0) + 1

    def correlation_freeze(self) -> bool:
        return self._corr.is_frozen()

    @staticmethod
    def dca_z_trigger(
        current_price: float, cost_basis: float,
        price_history: np.ndarray, threshold: float = -2.0,
    ) -> bool:
        if cost_basis <= 0 or len(price_history) < 10:
            return False
        pnl_pct  = float((_D(current_price) - _D(cost_basis)) / _D(cost_basis) * Decimal("100"))
        hist_pnl = (price_history - cost_basis) / cost_basis * 100
        z        = _z_score(pnl_pct, hist_pnl)
        if z <= threshold:
            log.info("DCA Z-trigger: pnl=%.2f%% Z=%.3f ≤ %.1f", pnl_pct, z, threshold)
            return True
        return False
