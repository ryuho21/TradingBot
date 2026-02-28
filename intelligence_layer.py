"""
intelligence_layer.py  —  Phase 37: Predictive Microstructure (VPIN/Flow Toxicity)
            (Dynamic Clamping · Multi-Level Mimicry · Golden Build Monitor ·
             VADER SNR · Accuracy Enrichment · JSON Sanitization re.DOTALL ·
             Dynamic Correlation Haircut · Cross-Exchange Price Velocity Lead/Lag ·
             Unified Veto Arbitrator · Exhaustion Gap Filter ·
             Volume-Clock VPIN Flow Toxicity Veto)

Phase 37 additions (this release):
  [P37-VPIN]  Flow Toxicity Veto — VetoArbitrator gains a new ZEROTH gate that
              checks ToxicityScore (set via set_flow_toxicity()) before all other
              veto logic.  When ToxicityScore > P37_TOXICITY_THRESHOLD (default
              0.80), compute_p_success() returns 0.0 immediately with reason:
              "VETO: High Flow Toxicity (Informed Selling Detected)".
              Entropy Escalation: if ToxicityScore is above threshold AND
              entropy_norm is below P37_LOW_ENTROPY_THRESHOLD (0.35), the veto
              reason is escalated to flag deliberate institutional positioning
              distinct from random order-flow noise.

Phase 36.2 additions (this release):
  [P36.2-PRESERVE] All Phase 36.1 VetoArbitrator features fully preserved and
                   unchanged: Spoof Toxicity EMA via set_spoof_probability(),
                   Multi-Level Mimicry veto gate, Exhaustion Gap Filter, and
                   P36_SPOOF_VETO_THRESHOLD gate.  No new logic added here —
                   intelligence_layer remains the authoritative gating layer.

Phase 36.1 additions (all preserved):
  [P36-VETO]      Manipulation Veto — VetoArbitrator now tracks a per-symbol
                  SpoofProbability injected by the Executor via
                  set_spoof_probability().  When spoof_prob > 0.8, compute_p_success()
                  returns 0.0 immediately with reason:
                  "VETO: Market Manipulation Detected (Spoofing)".
                  This pre-check runs BEFORE all other veto logic (P35/P34/P33)
                  so no individual component weighting can override a confirmed
                  manipulation signal.

Phase 33.1 additions (all preserved):
  [P33-REVERSION] Exhaustion Gap Filter — VetoArbitrator now tracks Whale Sweep
                  events via record_sweep_event() and price velocity samples via
                  record_price_velocity().  When a Whale Sweep occurred and the
                  PriceVelocity dropped by > 50% within the following 500ms, the
                  VetoArbitrator blocks the entry with a HARD VETO (p_success forced
                  to 0.0) to guard against mean-reversion traps.  The detection
                  window and velocity drop threshold are configurable via
                  P33_EXHAUSTION_WINDOW_MS and P33_EXHAUSTION_VELOCITY_DROP_PCT.

Phase 32 additions (all preserved):
  [P32-VETO-ARB] Unified Veto Arbitrator — VetoArbitrator class aggregates
                 Entropy, Correlation Haircuts, and Price Velocity into a single
                 ``p_success`` score.  The Executor blocks any order when
                 p_success < 0.65 (configurable via P32_VETO_ARB_THRESHOLD).
                 VetoArbitrator uses a trimmed-mean model: drop the lowest
                 component, average the remaining two so one bad axis cannot
                 block a genuinely strong signal.

Phase 31 additions (all preserved):
  [P31-VELOCITY] Cross-Exchange Alpha — PriceVelocityMonitor tracks per-exchange
                 price events with millisecond timestamps.  If Coinbase price
                 movement leads OKX by more than P31_VELOCITY_LAG_MS milliseconds
                 in the same direction as the proposed trade, the conviction
                 multiplier receives a P31_VELOCITY_BOOST (default +15 %).
                 The boost decays after P31_VELOCITY_TTL_MS to prevent stale
                 lead/lag signals from over-influencing later cycles.
                 PriceVelocityMonitor is accessible at LLMContextVeto.velocity_monitor
                 so the Executor can feed exchange ticks directly.

Phase 30.5 Titan Upgrade (all preserved):
  [P30.5-SNR] VADER SNR enrichment   [P26-JSON] Robust JSON sanitization

Phase 27 additions:
  [P27-SNR] VADER Data Enrichment — AgentVote gains two new signal-quality
             fields: ``accuracy`` (float 0–1) and ``snr`` (float 0–∞).
             • VADER/keyword fallback: accuracy=0.5, snr=0.0  (explicit neutral)
             • LLM path: accuracy derived from |score−0.5|×2+0.5; snr∝conviction
             Both fields are propagated into every ``council_detail`` dict entry
             and aggregated into each ``recent_results`` snapshot row so the
             Dashboard gauge renderer never encounters a missing-key error.

Phase 26 additions (all preserved):
  [P26-JSON] Robust JSON Sanitization — 2-retry brace-extraction before
             json.loads; immediate VADER fallback on failure.
  [P26-HAIRCUT] Dynamic Correlation Haircut — 0.85–0.95: haircut (ALLOW 25%
             size); ≥0.95: HARD VETO.

Phase 24.1 additions (all preserved):
  [P24.1-IL] Entropy Starvation Fix — warm-up Nominal Uncertainty (0.5 norm).

Phase 24 additions (all preserved):
  [P24-IL-1] NarrativeResult.entropy_normalized
  [P24-IL-2] Reliable Returns — all NarrativeResult paths carry valid entropy.

All Phase 22/23 components are fully preserved.
Phase 22: [P22-1] OpenRouter Hub · [P22-2] Entropy Shield · [P22-3] Regime Trail
All Phase 17–21 components are fully preserved.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import threading

import aiohttp

log = logging.getLogger("p17_intelligence")

# ── [SANITIZER] Institutional Environment Sanitizer ───────────────────────────
# Consistent with executor.py / data_hub.py / portfolio_manager.py.
# Must be defined before all _safe_il_* helpers and env-var constants.
def _clean_env(raw: str) -> str:
    """
    Strip surrounding whitespace and literal quote characters from .env values.
    Preserves the value content (including $ in passwords); only quote wrappers
    such as OKX_PASSPHRASE="$Khalil21z" or P35_HEDGE_SYMBOL="BTC-USDT-SWAP"
    are removed, yielding $Khalil21z and BTC-USDT-SWAP respectively.
    """
    return raw.strip().strip("'\"").strip()


# ── [P25] Veto Audit config ────────────────────────────────────────────────────
VETO_AUDIT_PATH = os.environ.get(
    "VETO_AUDIT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "hub_data", "veto_audit.json"),
)
_veto_audit_lock = threading.Lock()


# ── Defensive env loader — prevents startup crash on missing/malformed keys ────
def _safe_il_float(key: str, default: float) -> float:
    """
    Defensive float loader for intelligence_layer constants.

    Uses _clean_env to strip whitespace and literal quote characters (from .env
    files where values are wrapped in quotes, e.g. P36_SPOOF_VETO_THRESHOLD="0.8").
    Logs a WARNING and returns *default* when:
      • The variable is absent from the environment.
      • The value cannot be parsed as a float.
    Never raises — the bot must always be able to import this module.
    """
    raw = _clean_env(os.environ.get(key, ""))
    if not raw:
        log.warning(
            "[IL-ENV-WARN] %s not set — using safe default %.4f", key, default
        )
        return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        log.warning(
            "[IL-ENV-WARN] %s='%s' is not a valid float — "
            "using safe default %.4f", key, raw, default,
        )
        return default


def _safe_il_str(key: str, default: str) -> str:
    """Defensive string loader — uses _clean_env to strip whitespace and literal quotes."""
    raw = os.environ.get(key, default)
    return _clean_env(raw) or default


# ── Phase 17–21 config ────────────────────────────────────────────────────────
P17_VETO_THRESHOLD        = _safe_il_float("P17_NARRATIVE_VETO_THRESHOLD",  0.3)
P17_BOOST_THRESHOLD       = _safe_il_float("P17_NARRATIVE_BOOST_THRESHOLD", 0.8)
P17_BOOST_FACTOR          = _safe_il_float("P17_NARRATIVE_BOOST_FACTOR",    0.25)
P17_CATASTROPHE_THRESHOLD = _safe_il_float("P17_CATASTROPHE_THRESHOLD",     0.1)
P17_SCRAPER_MODE          = _safe_il_str("P17_SCRAPER_MODE",  "mock").lower()
P17_SCRAPER_FEED_URL      = _safe_il_str("P17_SCRAPER_FEED_URL", "")
P17_SCRAPER_TIMEOUT_MS    = _safe_il_float("P17_SCRAPER_TIMEOUT_MS",        10000.0)
P17_CACHE_TTL_SECS        = _safe_il_float("P17_CACHE_TTL_SECS",            30.0)
P17_MAX_PORTFOLIO_HEAT    = _safe_il_float("P17_MAX_PORTFOLIO_HEAT",        0.0)

P20_COUNCIL_OBI_VETO_THRESHOLD     = _safe_il_float("P20_COUNCIL_OBI_VETO_THRESHOLD",     -0.5)
P20_COUNCIL_WALL_RATIO             = _safe_il_float("P20_COUNCIL_WALL_RATIO",              4.0)
P20_COUNCIL_CORR_VETO_THRESHOLD    = _safe_il_float("P20_COUNCIL_CORR_VETO_THRESHOLD",    0.85)
P20_COUNCIL_CORR_WARN_THRESHOLD    = _safe_il_float("P20_COUNCIL_CORR_WARN_THRESHOLD",    0.65)
# [P26-HAIRCUT] Hard VETO threshold raised to 0.95; 0.85–0.95 → DYNAMIC_HAIRCUT.
P20_COUNCIL_CORR_HAIRCUT_THRESHOLD = _safe_il_float("P20_COUNCIL_CORR_HAIRCUT_THRESHOLD", 0.95)
P20_COUNCIL_CORR_HAIRCUT_FACTOR    = _safe_il_float("P20_COUNCIL_CORR_HAIRCUT_FACTOR",    0.25)

# ── [P22-1] OpenRouter config ──────────────────────────────────────────────────
OPENROUTER_API_KEY   = _safe_il_str("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL     = _safe_il_str("OPENROUTER_MODEL",   "stepfun/step-3.5-flash:free")
OPENROUTER_TIMEOUT_MS= _safe_il_float("OPENROUTER_TIMEOUT_MS", 10000.0)
OPENROUTER_SITE_URL  = _safe_il_str("OPENROUTER_SITE_URL",  "https://github.com/powertrader")
OPENROUTER_SITE_NAME = _safe_il_str("OPENROUTER_SITE_NAME", "PowerTrader AI")
_OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"

# ── [P22-2] Entropy Shield config ─────────────────────────────────────────────
P22_ENTROPY_HIGH_THRESHOLD   = _safe_il_float("P22_ENTROPY_HIGH_THRESHOLD",   3.5)
P22_ENTROPY_CONFIDENCE_BOOST = _safe_il_float("P22_ENTROPY_CONFIDENCE_BOOST", 0.20)

# ── [P22-3] Regime-Adaptive Take-Profit config ────────────────────────────────
P22_BULL_TRAIL_WIDEN    = _safe_il_float("P22_BULL_TRAIL_WIDEN",    0.005)
P22_CHOP_TRAIL_TIGHTEN  = _safe_il_float("P22_CHOP_TRAIL_TIGHTEN",  0.002)

_NEUTRAL = 0.5
_CONVICTION_MIN = 0.5
_CONVICTION_MAX = 2.0

# ── [P31-VELOCITY] Cross-Exchange Price Velocity Lead/Lag config ───────────────
P31_VELOCITY_LAG_MS   = _safe_il_float("P31_VELOCITY_LAG_MS",    200.0)
P31_VELOCITY_BOOST    = _safe_il_float("P31_VELOCITY_BOOST",     1.15)
P31_VELOCITY_TTL_MS   = _safe_il_float("P31_VELOCITY_TTL_MS",    5000.0)
P31_VELOCITY_MIN_MOVE = _safe_il_float("P31_VELOCITY_MIN_MOVE",  0.0002)

# ── [P33-REVERSION] Exhaustion Gap Filter config ──────────────────────────────
P33_EXHAUSTION_WINDOW_MS         = _safe_il_float("P33_EXHAUSTION_WINDOW_MS",         500.0)
P33_EXHAUSTION_VELOCITY_DROP_PCT = _safe_il_float("P33_EXHAUSTION_VELOCITY_DROP_PCT", 0.50)
P33_EXHAUSTION_SWEEP_TTL_MS      = _safe_il_float("P33_EXHAUSTION_SWEEP_TTL_MS",      2000.0)

# ── [P34.1-SKEW] Price Skew Veto config ──────────────────────────────────────
# Source: P34_MAX_SKEW_BPS in .env (default 50 bps).
P34_MAX_SKEW_BPS = _safe_il_float("P34_MAX_SKEW_BPS", 50.0)

# ── [P35.1] Hedge symbol — read and sanitize so VetoArbitrator can reference it ─
# _clean_env strips literal quotes that .env parsers may leave (e.g. P35_HEDGE_SYMBOL="BTC-USDT-SWAP").
_p35_il_hedge_raw = os.environ.get("P35_HEDGE_SYMBOL", "BTC")
P35_HEDGE_SYMBOL  = _clean_env(_p35_il_hedge_raw) or "BTC"

# ── [P36.1-DETECT] Manipulation Veto config ───────────────────────────────────
# Spoof probability EMA above which VetoArbitrator forces p_success=0.0.
# Source: P36_SPOOF_VETO_THRESHOLD in .env (default 0.80).
P36_SPOOF_VETO_THRESHOLD = _safe_il_float("P36_SPOOF_VETO_THRESHOLD", 0.8)
# Reaction window (ms) used by the Executor mimic test — exposed here so the
# VetoArbitrator can apply the same timing budget for its own staleness checks.
P36_SPOOF_REACTION_MS    = _safe_il_float("P36_SPOOF_REACTION_MS",   400.0)
# Master enable switch mirrored here for cross-module consistency checks.
P36_ENABLE_MIMICRY       = os.environ.get("P36_ENABLE_MIMICRY", "1").strip().strip("'\"") == "1"

# ── [P37-VPIN] Flow Toxicity Veto config ─────────────────────────────────────
# CDF-based ToxicityScore above which VetoArbitrator blocks ALL new entries.
# A score of 0.80 means the current VPIN bucket is in the top 20% of recent
# history — consistent with informed, directional institutional selling.
# Source: P37_TOXICITY_THRESHOLD in .env (default 0.80).
P37_TOXICITY_THRESHOLD = _safe_il_float("P37_TOXICITY_THRESHOLD", 0.80)
# When toxicity is above P37_TOXICITY_THRESHOLD AND entropy_norm is LOW
# (< P37_LOW_ENTROPY_THRESHOLD), the veto is flagged as ESCALATED — a
# deliberate, non-random institutional move rather than noise-driven imbalance.
# Default 0.35: entropy_norm ≤ 0.35 → orderly, directional market (not chaotic).
P37_LOW_ENTROPY_THRESHOLD = _safe_il_float("P37_LOW_ENTROPY_THRESHOLD", 0.35)


# ══════════════════════════════════════════════════════════════════════════════
# [P31-VELOCITY] Cross-Exchange Price Velocity Lead/Lag Monitor
# ══════════════════════════════════════════════════════════════════════════════

class PriceVelocityMonitor:
    """
    [P31-VELOCITY] Tracks the last meaningful price move for each registered
    exchange and detects when one exchange (e.g. Coinbase) directionally leads
    another (e.g. OKX).

    Thread/coroutine safe — reads are lock-free (Python GIL protected).

    Usage::

        monitor = PriceVelocityMonitor()
        monitor.record("coinbase", current_btc_price)
        monitor.record("okx",      current_btc_price)
        boost = monitor.conviction_boost("long")   # 1.15 if CB leads OKX, else 1.0
    """

    def __init__(self) -> None:
        # {exchange: [(price, ts_ms), (price, ts_ms)]}  — last 2 ticks
        self._history: Dict[str, List[tuple]] = {}
        # Cached lead/lag event: (direction, ts_ms, lead_exchange, lag_exchange)
        self._last_lead: Optional[tuple] = None

    def record(self, exchange: str, price: float,
               ts_ms: Optional[float] = None) -> None:
        """
        Register a new price observation for an exchange.

        Parameters
        ----------
        exchange : str   e.g. ``"coinbase"`` or ``"okx"``
        price    : float Latest mid/last price.
        ts_ms    : float, optional  Monotonic timestamp in milliseconds.
        """
        if price <= 0:
            return
        now_ms = ts_ms if ts_ms is not None else time.monotonic() * 1_000.0
        buf    = self._history.setdefault(exchange, [])
        buf.append((price, now_ms))
        if len(buf) > 2:
            buf.pop(0)
        # Re-evaluate lead/lag whenever OKX tick arrives (the lagging leg).
        if exchange.lower() == "okx":
            self._evaluate_lead_lag()

    def _evaluate_lead_lag(self) -> None:
        """Compare Coinbase and OKX momentum windows, cache result."""
        try:
            cb_buf  = self._history.get("coinbase", [])
            okx_buf = self._history.get("okx", [])
            if len(cb_buf) < 2 or len(okx_buf) < 2:
                return

            cb_prev_px,  _          = cb_buf[0]
            cb_now_px,   cb_now_ts  = cb_buf[1]
            okx_prev_px, _          = okx_buf[0]
            okx_now_px,  okx_now_ts = okx_buf[1]

            def _dir(prev: float, cur: float) -> str:
                frac = abs(cur - prev) / max(cur, 1e-9)
                if frac < P31_VELOCITY_MIN_MOVE:
                    return "flat"
                return "up" if cur > prev else "down"

            cb_dir  = _dir(cb_prev_px,  cb_now_px)
            okx_dir = _dir(okx_prev_px, okx_now_px)

            if cb_dir == "flat" or okx_dir == "flat":
                return

            # Coinbase leads: CB moved in same direction as OKX, but its
            # timestamp predates OKX by at least P31_VELOCITY_LAG_MS.
            if cb_dir == okx_dir and (okx_now_ts - cb_now_ts) >= P31_VELOCITY_LAG_MS:
                trade_dir = "long" if cb_dir == "up" else "short"
                self._last_lead = (trade_dir, okx_now_ts, "coinbase", "okx")
                log.debug(
                    "[P31-VELOCITY] Lead detected: coinbase(%s)->okx lag=%.1fms dir=%s",
                    cb_dir, okx_now_ts - cb_now_ts, trade_dir,
                )
        except Exception as exc:
            log.debug("[P31-VELOCITY] _evaluate_lead_lag error: %s", exc)

    def conviction_boost(self, direction: str) -> float:
        """
        Return P31_VELOCITY_BOOST if a fresh Coinbase-leads-OKX signal exists
        for the requested ``direction``.  Returns 1.0 otherwise.
        """
        if self._last_lead is None:
            return 1.0
        lead_dir, lead_ts, _, _ = self._last_lead
        now_ms = time.monotonic() * 1_000.0
        if now_ms - lead_ts > P31_VELOCITY_TTL_MS:
            self._last_lead = None
            return 1.0
        return P31_VELOCITY_BOOST if lead_dir == direction else 1.0

    def status_snapshot(self) -> dict:
        """Return a serialisable snapshot for diagnostics / dashboard."""
        snap: dict = {"exchanges": {}, "last_lead": None}
        for exch, buf in self._history.items():
            if buf:
                snap["exchanges"][exch] = {
                    "price": round(buf[-1][0], 6),
                    "ts_ms": round(buf[-1][1], 1),
                }
        if self._last_lead is not None:
            lead_dir, lead_ts, lead_ex, lag_ex = self._last_lead
            now_ms = time.monotonic() * 1_000.0
            snap["last_lead"] = {
                "direction":     lead_dir,
                "age_ms":        round(now_ms - lead_ts, 1),
                "lead_exchange": lead_ex,
                "lag_exchange":  lag_ex,
                "active":        (now_ms - lead_ts) <= P31_VELOCITY_TTL_MS,
            }
        return snap


# ── Keyword corpus (preserved from P17) ───────────────────────────────────────
_KEYWORD_WEIGHTS: List[Tuple[str, float]] = [
    ("etf approval",         +0.35),
    ("spot etf",             +0.30),
    ("institutional buy",    +0.28),
    ("mass adoption",        +0.25),
    ("regulatory clarity",   +0.22),
    ("all-time high",        +0.20),
    ("breakout",             +0.18),
    ("partnership",          +0.15),
    ("upgrade",              +0.12),
    ("bullish",              +0.10),
    ("accumulation",         +0.10),
    ("rally",                +0.10),
    ("buy signal",           +0.12),
    ("interest",             +0.05),
    ("growth",               +0.05),
    ("recover",              +0.06),
    ("lawsuit",              -0.10),
    ("investigation",        -0.10),
    ("correction",           -0.08),
    ("sell-off",             -0.10),
    ("bearish",              -0.10),
    ("resistance",           -0.06),
    ("ban",                  -0.20),
    ("hack",                 -0.22),
    ("exploit",              -0.20),
    ("regulation crackdown", -0.25),
    ("sec charges",          -0.28),
    ("insolvency",           -0.30),
    ("bankruptcy",           -0.30),
    ("rug pull",             -0.35),
    ("de-listing",           -0.28),
    ("exchange collapse",    -0.50),
    ("market crash",         -0.45),
    ("systemic risk",        -0.40),
    ("contagion",            -0.38),
    ("black swan",           -0.42),
    ("protocol exploit",     -0.40),
    ("critical vulnerability", -0.40),
]

_MOCK_HEADLINES: List[str] = [
    "Crypto market shows steady recovery amid macro uncertainty",
    "Institutional interest in digital assets continues to grow",
    "Regulatory clarity sought by major exchanges worldwide",
    "Bitcoin dominance stable as altcoins consolidate",
    "DeFi protocol upgrade improves throughput by 30%",
]

_VOTE_ALLOW   = "ALLOW"
_VOTE_BLOCK   = "BLOCK"
_VOTE_ABSTAIN = "ABSTAIN"


# ══════════════════════════════════════════════════════════════════════════════
# [P22-2] Shannon Entropy Shield
# ══════════════════════════════════════════════════════════════════════════════

def compute_shannon_entropy(returns: List[float], bins: int = 10) -> float:
    """
    Computes Shannon entropy (bits) over a discretised return distribution.

    Parameters
    ----------
    returns : list of float
        Sequence of percentage price returns (e.g. from 60-second window).
    bins    : int
        Number of histogram buckets.  Defaults to 10.

    Returns
    -------
    float
        Entropy in bits.  0.0 if the distribution is fully deterministic;
        higher values indicate more directionless / noisy price action.

    [P24.1-IL] Warm-Up Guard (n < 20)
    ------------------------------------
    Returns ``_ENTROPY_WARMUP_NOMINAL`` (0.5 normalised ≈ 1.66 bits) instead
    of 0.0 when fewer than 20 samples are available.  This prevents:
      * Dashboard entropy gauges from displaying a flat "dead" 0.0 reading
        during the first seconds of operation before candle history arrives.
      * The ``entropy_confidence_gate`` from treating warm-up silence as a
        "fully deterministic" distribution and suppressing all entries.
    A value of 0.5 (normalised) is mid-scale "Nominal Uncertainty" — the bot
    treats the market as moderately noisy until it has real data to measure.
    """
    # [P24.1-IL] Minimum sample guard — return Nominal Uncertainty during warm-up.
    _ENTROPY_MIN_SAMPLES = 20
    if len(returns) < _ENTROPY_MIN_SAMPLES:
        # 0.5 × log2(10) ≈ 1.6609 bits — mid-scale "Nominal Uncertainty"
        _nominal_bits = round(_MAX_ENTROPY_BITS * 0.5, 4)
        log.debug(
            "[P24.1-IL] compute_shannon_entropy: only %d samples (need %d) — "
            "returning Nominal Uncertainty %.4f bits (entropy_normalized=0.5).",
            len(returns), _ENTROPY_MIN_SAMPLES, _nominal_bits,
        )
        return _nominal_bits
    try:
        mn, mx = min(returns), max(returns)
        if mx == mn:
            return 0.0
        width  = (mx - mn) / bins
        counts: Dict[int, int] = {}
        for r in returns:
            bucket = min(int((r - mn) / width), bins - 1)
            counts[bucket] = counts.get(bucket, 0) + 1
        total = len(returns)
        entropy = 0.0
        for cnt in counts.values():
            p = cnt / total
            if p > 0:
                entropy -= p * math.log2(p)
        return round(entropy, 4)
    except Exception as exc:
        log.debug("[P22-2] Shannon entropy error: %s", exc)
        return 0.0


_MAX_ENTROPY_BITS: float = math.log2(10)   # ≈ 3.321928 bits — 10-bucket maximum


def normalize_entropy(entropy_bits: float) -> float:
    """
    [P24-IL-1] Scale raw Shannon entropy (bits) to a 0.0–1.0 range.

    Uses _MAX_ENTROPY_BITS = log2(10) ≈ 3.32 bits as the practical ceiling
    for a 10-bucket histogram.  Values above the ceiling are clamped to 1.0.
    Returns 0.0 for zero or negative input (safe for degenerate distributions).
    """
    if entropy_bits <= 0.0 or _MAX_ENTROPY_BITS <= 0.0:
        return 0.0
    return round(min(entropy_bits / _MAX_ENTROPY_BITS, 1.0), 4)


def entropy_confidence_gate(
    base_confidence: float,
    entropy: float,
    threshold: float = P22_ENTROPY_HIGH_THRESHOLD,
    boost: float     = P22_ENTROPY_CONFIDENCE_BOOST,
) -> Tuple[float, bool]:
    """
    Returns (required_confidence, entropy_triggered).

    If entropy > threshold the required minimum confidence is raised by
    `boost` fraction (default +20 %).  The caller compares signal.confidence
    against the returned threshold to decide whether to suppress the entry.
    """
    if entropy > threshold:
        return round(base_confidence * (1.0 + boost), 4), True
    return base_confidence, False


# ══════════════════════════════════════════════════════════════════════════════
# [P22-3] Regime-Adaptive Trailing Gap
# ══════════════════════════════════════════════════════════════════════════════

def regime_trail_adjustment(regime: str) -> float:
    """
    Returns the trailing-gap adjustment in absolute percentage points.

    Bull regime  → +P22_BULL_TRAIL_WIDEN   (wider, lets winners run)
    Chop regime  → -P22_CHOP_TRAIL_TIGHTEN (tighter, banks profit faster)
    Other        →  0.0
    """
    r = (regime or "").lower()
    if r == "bull":
        return P22_BULL_TRAIL_WIDEN
    if r == "chop":
        return -P22_CHOP_TRAIL_TIGHTEN
    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# [P22-1] OpenRouter async client
# ══════════════════════════════════════════════════════════════════════════════

class OpenRouterClient:
    """
    Thin async wrapper around the OpenRouter /v1/chat/completions endpoint.

    Design decisions
    ----------------
    * Single shared aiohttp.ClientSession per instance (created lazily).
    * Timeout is the P17 hard cap (OPENROUTER_TIMEOUT_MS / 1000).
    * All errors are caught and returned as (None, error_str) so the caller
      can degrade gracefully to the keyword-heuristic fallback.
    * Latency is always measured and returned, even on failure.
    """

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._timeout = aiohttp.ClientTimeout(
            total=OPENROUTER_TIMEOUT_MS / 1_000.0
        )
        self._api_key = OPENROUTER_API_KEY
        self._model   = OPENROUTER_MODEL
        if not self._api_key:
            log.warning(
                "[P22-1] OPENROUTER_API_KEY not set — "
                "LLM scoring will fall back to keyword heuristics."
            )
        else:
            log.info(
                "[P22-1] OpenRouterClient ready — model=%s timeout=%.0fms",
                self._model, OPENROUTER_TIMEOUT_MS,
            )

    def _session_or_create(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={
                    "Authorization":  f"Bearer {self._api_key}",
                    "HTTP-Referer":   OPENROUTER_SITE_URL,
                    "X-Title":        OPENROUTER_SITE_NAME,
                    "Content-Type":   "application/json",
                },
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def score_headlines(
        self,
        symbol:    str,
        direction: str,
        headlines: List[str],
    ) -> Tuple[Optional[float], float, Optional[str]]:
        """
        Calls OpenRouter to score headlines for a given symbol + direction.

        Returns
        -------
        (score, latency_ms, error)
            score      : float ∈ [0, 1] or None on failure
            latency_ms : float — wall-clock time of the API call
            error      : str or None — structured error description
        """
        if not self._api_key:
            return None, 0.0, "no_api_key"

        prompt = (
            f"You are a crypto trading sentiment analyst.\n"
            f"Symbol: {symbol}  |  Proposed direction: {direction.upper()}\n"
            f"Rate the aggregate sentiment of these headlines on a scale from 0.00 "
            f"(extremely bearish / dangerous) to 1.00 (extremely bullish / safe).\n"
            f"Headlines:\n"
            + "\n".join(f"- {h}" for h in headlines[:10])
            + "\n\nRespond with ONLY a JSON object: "
            '{\"score\": <float 0-1>, \"reason\": \"<one sentence>\"}'
        )

        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 60,
            "temperature": 0.1,
        }

        t0 = time.monotonic()
        try:
            sess = self._session_or_create()
            async with sess.post(_OPENROUTER_ENDPOINT, json=payload) as resp:
                latency_ms = (time.monotonic() - t0) * 1_000
                if resp.status != 200:
                    body = await resp.text()
                    err  = f"http_{resp.status}: {body[:120]}"
                    log.warning("[P22-1] OpenRouter %s: %s", symbol, err)
                    return None, latency_ms, err

                data = await resp.json(content_type=None)

            raw_content = (
                data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                    .strip()
            )

            # [P30.5-JSON] Robust JSON Sanitization — re.DOTALL brace-hunt.
            # Two-retry loop: first strip markdown fences, then use a regex
            # that works across newlines (re.DOTALL) to find the outermost
            # JSON object even when the model embeds it in prose paragraphs.
            parsed = None
            last_parse_exc = None
            for _attempt in range(2):
                attempt_content = raw_content.replace("```json", "").replace("```", "").strip()
                # [P30.5-JSON] re.DOTALL finds the outermost {...} spanning newlines
                _brace_match = re.search(r"\{.*\}", attempt_content, re.DOTALL)
                if _brace_match:
                    attempt_content = _brace_match.group(0)
                else:
                    # Fallback to the old char-index approach
                    brace_start = attempt_content.find("{")
                    brace_end   = attempt_content.rfind("}")
                    if brace_start != -1 and brace_end != -1 and brace_end > brace_start:
                        attempt_content = attempt_content[brace_start : brace_end + 1]
                try:
                    parsed = json.loads(attempt_content)
                    break
                except (json.JSONDecodeError, ValueError) as exc:
                    last_parse_exc = exc
                    log.debug(
                        "[P30.5-JSON] OpenRouter parse attempt %d failed for %s: %s",
                        _attempt + 1, symbol, exc,
                    )

            if parsed is None:
                # [P30.5-JSON] After 2 retries: fall back to VADER-keyword engine
                # immediately so the Council always has valid data.
                latency_ms = (time.monotonic() - t0) * 1_000
                log.warning(
                    "[P30.5-JSON] OpenRouter %s: JSON parse failed after 2 retries "
                    "(%s) — falling back to keyword engine immediately.",
                    symbol, last_parse_exc,
                )
                return None, latency_ms, f"parse_error:{last_parse_exc}"

            score = float(parsed["score"])
            score = max(0.0, min(1.0, score))
            log.info(
                "[P22-1] OpenRouter %s %s: score=%.4f latency=%.1fms reason=%s",
                symbol, direction, score, latency_ms, parsed.get("reason", ""),
            )
            return score, latency_ms, None

        except asyncio.TimeoutError:
            latency_ms = (time.monotonic() - t0) * 1_000
            log.warning(
                "[P22-1][P23-OPT-1] AI FALLBACK: OpenRouter latency exceeded %.1fms "
                "for %s — switching to Technical Mode.",
                latency_ms, symbol,
            )
            return None, latency_ms, "timeout"
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            latency_ms = (time.monotonic() - t0) * 1_000
            log.warning("[P22-1] OpenRouter parse error for %s: %s", symbol, exc)
            return None, latency_ms, f"parse_error:{exc}"
        except Exception as exc:
            latency_ms = (time.monotonic() - t0) * 1_000
            log.warning("[P22-1] OpenRouter unexpected error for %s: %s", symbol, exc)
            return None, latency_ms, f"error:{exc}"


# ── Singleton client shared across all agents ──────────────────────────────────
_openrouter_client: Optional[OpenRouterClient] = None


def get_openrouter_client() -> OpenRouterClient:
    global _openrouter_client
    if _openrouter_client is None:
        _openrouter_client = OpenRouterClient()
    return _openrouter_client


# ══════════════════════════════════════════════════════════════════════════════
# Agent vote dataclass
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class AgentVote:
    agent:    str
    vote:     str        # ALLOW | BLOCK | ABSTAIN
    score:    float      # ∈ [0, 1]; 0.5 = neutral
    reason:   str   = ""
    # [P27-SNR] Signal quality fields — always populated so the Dashboard
    # gauge renderer never encounters a KeyError / missing key.
    # accuracy : model confidence in the given score (0.5 = random / fallback)
    # snr      : signal-to-noise ratio proxy (0.0 when no LLM; higher = cleaner)
    accuracy: float = 0.5
    snr:      float = 0.0


# ══════════════════════════════════════════════════════════════════════════════
# NarrativeResult  (extended with P22 fields)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class NarrativeResult:
    symbol:                str
    direction:             str
    score:                 float
    verdict:               str
    boost_factor:          float
    conviction_multiplier: float
    headlines_used:        int
    latency_ms:            float
    timed_out:             bool
    council_detail:        List[dict] = field(default_factory=list)
    # [P22-1]
    llm_used:              bool  = False
    llm_error:             Optional[str] = None
    # [P22-2]
    entropy:               float = 0.0
    entropy_triggered:     bool  = False
    # [P24-IL-1] Normalized entropy: raw bits scaled to 0.0–1.0.
    # Uses log2(10) ≈ 3.32 bits as the practical maximum for a 10-bucket
    # histogram so the Dashboard sparkline renders on a consistent axis.
    entropy_normalized:    float = 0.0
    # [P22-3]
    trailing_gap_adjustment: float = 0.0
    ts:                    float = field(default_factory=time.time)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers (preserved)
# ══════════════════════════════════════════════════════════════════════════════

def _compute_conviction_multiplier(score: float, veto_threshold: float) -> float:
    if score <= veto_threshold:
        return _CONVICTION_MIN
    denom = 1.0 - veto_threshold
    if denom <= 0:
        return _CONVICTION_MIN
    raw = _CONVICTION_MIN + (_CONVICTION_MAX - _CONVICTION_MIN) * (
        (score - veto_threshold) / denom
    )
    return max(_CONVICTION_MIN, min(_CONVICTION_MAX, raw))


# ══════════════════════════════════════════════════════════════════════════════
# [P25] Veto Audit — Thread-safe append helper
# ══════════════════════════════════════════════════════════════════════════════

def _append_veto_audit_file(
    symbol:  str,
    reason:  str,
    details: str = "",
) -> None:
    """
    [P25] Atomically append one row to veto_audit.json.

    Uses a module-level threading.Lock so simultaneous calls from the async
    executor (running in a single thread via asyncio) and any background
    threads cannot corrupt the file.

    File format: JSON array of audit records.
    """
    row = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "symbol":    symbol,
        "reason":    reason,
        "details":   details,
    }
    tmp_path = VETO_AUDIT_PATH + ".tmp"
    with _veto_audit_lock:
        try:
            os.makedirs(os.path.dirname(VETO_AUDIT_PATH), exist_ok=True)
            existing: list = []
            if os.path.exists(VETO_AUDIT_PATH):
                try:
                    with open(VETO_AUDIT_PATH, "r", encoding="utf-8") as f:
                        raw = f.read().strip()
                    if raw:
                        existing = json.loads(raw)
                    if not isinstance(existing, list):
                        existing = []
                except Exception:
                    existing = []
            existing.append(row)
            # Keep last 10 000 rows to prevent unbounded growth
            if len(existing) > 10_000:
                existing = existing[-10_000:]
            payload = json.dumps(existing, indent=None, separators=(",", ":"))
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp_path, VETO_AUDIT_PATH)
        except Exception as exc:
            log.warning("[P25] _append_veto_audit_file failed: %s", exc)
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass


def _keyword_score(headlines: List[str], direction: str) -> float:
    """Preserved Phase 17 keyword-heuristic baseline."""
    corpus = " ".join(headlines).lower()
    raw    = 0.5
    for kw, w in _KEYWORD_WEIGHTS:
        if kw in corpus:
            raw += w if direction == "long" else -w
    return max(0.0, min(1.0, raw))


# ══════════════════════════════════════════════════════════════════════════════
# IntelligenceScraper (preserved from P17/P18/P20)
# ══════════════════════════════════════════════════════════════════════════════

class IntelligenceScraper:
    """Async headline ingestion with per-symbol TTL caching."""

    def __init__(self, mode: str = P17_SCRAPER_MODE, feed_url: str = P17_SCRAPER_FEED_URL):
        self.mode     = mode
        self.feed_url = feed_url
        self._cache: Dict[str, Tuple[float, List[str]]] = {}
        self._timeout_secs = P17_SCRAPER_TIMEOUT_MS / 1_000.0
        log.info(
            "[P17] IntelligenceScraper init: mode=%s ttl=%.0fs timeout=%.0fms",
            mode, P17_CACHE_TTL_SECS, P17_SCRAPER_TIMEOUT_MS,
        )

    async def get_headlines(self, symbol: str) -> List[str]:
        now    = time.time()
        cached = self._cache.get(symbol)
        if cached and (now - cached[0]) < P17_CACHE_TTL_SECS:
            return cached[1]
        headlines = await self._fetch(symbol)
        self._cache[symbol] = (now, headlines)
        return headlines
def cache_age_secs(self, symbol: str, now: Optional[float] = None) -> Optional[float]:
    """
    [P30.5-SNR] Return the age (seconds) of the cached headlines for `symbol`.

    This is used by VADER/keyword fallback scoring to apply a confidence
    penalty when LLM enrichment fails: stale headlines imply lower SNR.
    Returns None when no cache entry exists.
    """
    try:
        now_ts = float(now if now is not None else time.time())
        cached = self._cache.get(symbol)
        if not cached:
            return None
        ts = float(cached[0])
        age = max(0.0, now_ts - ts)
        return age
    except Exception:
        return None

    def invalidate(self, symbol: str):
        self._cache.pop(symbol, None)

    async def _fetch(self, symbol: str) -> List[str]:
        try:
            return await asyncio.wait_for(
                self._fetch_inner(symbol),
                timeout=self._timeout_secs,
            )
        except asyncio.TimeoutError:
            log.debug("[P17] Scraper timeout for %s — using mock", symbol)
            return list(_MOCK_HEADLINES)
        except Exception as exc:
            log.debug("[P17] Scraper fetch error for %s: %s — using mock", symbol, exc)
            return list(_MOCK_HEADLINES)

    async def _fetch_inner(self, symbol: str) -> List[str]:
        if self.mode == "mock" or not self.feed_url:
            return list(_MOCK_HEADLINES)
        if self.mode == "json":
            return await self._fetch_json(symbol)
        if self.mode == "rss":
            return await self._fetch_rss(symbol)
        return list(_MOCK_HEADLINES)

    async def _fetch_json(self, symbol: str) -> List[str]:
        try:
            import aiohttp as _aiohttp
        except ImportError:
            return list(_MOCK_HEADLINES)
        url = self.feed_url.format(symbol=symbol.lower())
        async with _aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=_aiohttp.ClientTimeout(
                total=self._timeout_secs
            )) as r:
                data = await r.json()
        raw = data.get("headlines") or data.get("articles") or []
        if isinstance(raw, list):
            titles = []
            for item in raw:
                if isinstance(item, str):
                    titles.append(item)
                elif isinstance(item, dict):
                    titles.append(item.get("title") or item.get("headline") or "")
            return [t for t in titles if t] or list(_MOCK_HEADLINES)
        return list(_MOCK_HEADLINES)

    async def _fetch_rss(self, symbol: str) -> List[str]:
        try:
            import aiohttp as _aiohttp
        except ImportError:
            return list(_MOCK_HEADLINES)
        import re
        url = self.feed_url.format(symbol=symbol.lower())
        async with _aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=_aiohttp.ClientTimeout(
                total=self._timeout_secs
            )) as r:
                text = await r.text()
        titles  = re.findall(r"<title>(.*?)</title>", text, re.DOTALL)
        cleaned = [t.strip() for t in titles[1:] if t.strip()]
        return cleaned or list(_MOCK_HEADLINES)


# ══════════════════════════════════════════════════════════════════════════════
# [P20-2] Council sub-agents  (NarrativeAgent upgraded to use OpenRouter)
# ══════════════════════════════════════════════════════════════════════════════

class NarrativeAgent:
    """
    [P20-2 / P22-1] Agent 1: Sentiment scoring.

    Scoring order:
      1. Attempt OpenRouter LLM call (OPENROUTER_API_KEY present + within budget).
      2. Fall back to keyword-heuristic if LLM unavailable or times out.

    [FIX-AI-RELIABILITY] Distinguishes two failure modes:
      - "Neutral AI Sentiment": LLM returned a valid score in the neutral zone.
        Vote is ALLOW with score ≈ 0.5, reason prefix = "neutral".
      - "AI Connection Error": LLM call failed (timeout, parse error, no key).
        In this case the keyword heuristic score is ONLY used if it is above
        P17_VETO_THRESHOLD.  If the heuristic also returns a suspicious low
        score (< P17_VETO_THRESHOLD) when an LLM error occurred, we emit an
        ABSTAIN vote so the other two agents decide rather than forcing a VETO
        based on potentially stale/mock headlines.  This prevents the keyword
        fallback from producing a 0.05 "Catastrophe Veto" when the AI is merely
        unreachable.

    Latency is always tracked.  LLM errors are stored in the vote reason
    so the dashboard can surface them without null-pointer issues.
    """

    def __init__(self, scraper: IntelligenceScraper) -> None:
        self._scraper = scraper
        self._or_client = get_openrouter_client()

    async def vote(self, symbol: str, direction: str) -> AgentVote:
        try:
            headlines = await self._scraper.get_headlines(symbol)

            # [P30.5-SNR] Headline age for fallback confidence penalty.
            _now_ts = time.time()
            _age_s  = self._scraper.cache_age_secs(symbol, now=_now_ts)
        except Exception as exc:
            log.debug("[P22-1/NarrativeAgent] Scraper error %s: %s — ABSTAIN", symbol, exc)
            return AgentVote(
                agent="narrative", vote=_VOTE_ABSTAIN, score=0.5,
                reason=f"scraper_error:{exc}",
                accuracy=0.5, snr=0.0,   # [P27-SNR] fallback quality markers
            )

        # [P22-1] Try OpenRouter first
        llm_score, llm_latency_ms, llm_error = await self._or_client.score_headlines(
            symbol, direction, headlines
        )

        ai_connected = llm_error is None

        if llm_score is not None:
            # LLM returned a valid score — use it directly.
            score      = llm_score
            score_src  = f"openrouter:{OPENROUTER_MODEL}"
            # [P27-SNR] LLM accuracy: how far the score deviates from 0.5 (neutral).
            # A score near 0 or 1 is high-conviction; near 0.5 is uncertain.
            accuracy   = float(min(1.0, abs(score - 0.5) * 2.0 + 0.5))
            snr        = float(min(10.0, abs(score - 0.5) * 10.0))
        else:
            # [FIX-AI-RELIABILITY] LLM failed.  Compute keyword score but
            # treat it as advisory only.  If it falls below the veto threshold,
            # emit ABSTAIN rather than a spurious block.
            kw_score  = _keyword_score(headlines, direction)
            score_src = f"keyword_fallback (llm_err={llm_error})"            # [P27-SNR][P30.5-SNR] Keyword/VADER fallback: apply a confidence penalty
            # based on headline age. Stale headlines imply lower signal-to-noise.
            accuracy = 0.5
            try:
                # Freshness ∈ [0,1]; treat missing age as half-fresh.
                _fresh = 0.5 if _age_s is None else max(0.0, 1.0 - min(_age_s / 3600.0, 1.0))
                # Base SNR from deviation vs neutral, scaled by freshness.
                snr = float(min(10.0, abs(kw_score - 0.5) * 10.0) * _fresh)
            except Exception:
                snr = 0.0


            if kw_score < P17_VETO_THRESHOLD:
                # Keyword score is low, but we can't trust it when the AI is
                # unreachable — the headlines may be mock/stale.  ABSTAIN and
                # let the other council agents decide.
                log.info(
                    "[FIX-AI-RELIABILITY] NarrativeAgent %s %s: "
                    "AI error=%s AND keyword_score=%.4f < veto=%.2f — "
                    "emitting ABSTAIN instead of spurious BLOCK.",
                    symbol, direction, llm_error, kw_score, P17_VETO_THRESHOLD,
                )
                reason = (
                    f"ai_connection_error | kw_score={kw_score:.4f} | "
                    f"src={score_src} | llm_lat={llm_latency_ms:.1f}ms | "
                    f"n_headlines={len(headlines)} | llm_err={llm_error}"
                )
                return AgentVote(
                    agent="narrative", vote=_VOTE_ABSTAIN, score=0.5,
                    reason=reason,
                    accuracy=0.5, snr=snr,   # [P27-SNR][P30.5-SNR] age-penalized fallback
                )
            score = kw_score

        reason_parts = [
            f"score={score:.4f}",
            f"src={score_src}",
            f"llm_lat={llm_latency_ms:.1f}ms",
            f"n_headlines={len(headlines)}",
            f"headline_age_s={(_age_s if _age_s is not None else -1):.0f}",  # [P30.5-SNR]
            f"ai_connected={ai_connected}",
            f"accuracy={accuracy:.4f}",   # [P27-SNR]
            f"snr={snr:.4f}",             # [P27-SNR]
        ]
        if llm_error:
            reason_parts.append(f"llm_err={llm_error}")

        reason = " | ".join(reason_parts)

        if score < P17_CATASTROPHE_THRESHOLD:
            # Only apply CATASTROPHE_VETO if the AI was actually connected and
            # gave a genuine assessment.  If AI errored and keyword gave a low
            # score, we already ABSTAIN'd above — this path is only reachable
            # when the LLM itself returned a catastrophically low score.
            if not ai_connected:
                # Defensive guard — should not reach here, but just in case.
                return AgentVote(agent="narrative", vote=_VOTE_ABSTAIN, score=0.5,
                                 reason=f"ai_error_safe_guard | {reason}",
                                 accuracy=0.5, snr=0.0)
            return AgentVote(agent="narrative", vote=_VOTE_BLOCK, score=score,
                             reason=f"catastrophe | {reason}",
                             accuracy=accuracy, snr=snr)
        if score < P17_VETO_THRESHOLD:
            return AgentVote(agent="narrative", vote=_VOTE_BLOCK, score=score,
                             reason=f"veto | {reason}",
                             accuracy=accuracy, snr=snr)
        if score > P17_BOOST_THRESHOLD:
            return AgentVote(agent="narrative", vote=_VOTE_ALLOW, score=score,
                             reason=f"boost | {reason}",
                             accuracy=accuracy, snr=snr)
        return AgentVote(agent="narrative", vote=_VOTE_ALLOW, score=score,
                         reason=f"neutral | {reason}",
                         accuracy=accuracy, snr=snr)


class MicrostructureAgent:
    """[P20-2] Agent 2: OBI + liquidity wall analysis (unchanged from P20)."""

    def __init__(self, hub=None):
        self._hub = hub

    def set_hub(self, hub) -> None:
        self._hub = hub

    async def vote(self, symbol: str, direction: str) -> AgentVote:
        if self._hub is None:
            return AgentVote(agent="microstructure", vote=_VOTE_ABSTAIN,
                             score=0.5, reason="no_hub")
        try:
            obi = await self._hub.get_obi(symbol, 10)
        except Exception as exc:
            log.debug("[P20-2/MicrostructureAgent] OBI error %s: %s", symbol, exc)
            return AgentVote(agent="microstructure", vote=_VOTE_ABSTAIN,
                             score=0.5, reason=f"obi_error:{exc}")

        ms_score = (obi + 1.0) / 2.0

        if direction == "long" and obi < P20_COUNCIL_OBI_VETO_THRESHOLD:
            vote   = _VOTE_BLOCK
            reason = f"obi={obi:.4f} < long_veto={P20_COUNCIL_OBI_VETO_THRESHOLD}"
        elif direction == "short" and obi > abs(P20_COUNCIL_OBI_VETO_THRESHOLD):
            vote   = _VOTE_BLOCK
            reason = f"obi={obi:.4f} > short_veto={abs(P20_COUNCIL_OBI_VETO_THRESHOLD)}"
        else:
            vote   = _VOTE_ALLOW
            reason = f"obi={obi:.4f} direction={direction}"

        try:
            book = await self._hub.get_order_book(symbol)
            if book and book.bids and book.asks:
                bid_vol = sum(sz for _, sz in book.bids[:5])
                ask_vol = sum(sz for _, sz in book.asks[:5])
                if direction == "long" and ask_vol > 0:
                    ratio = ask_vol / max(bid_vol, 1e-9)
                    if ratio >= P20_COUNCIL_WALL_RATIO:
                        vote     = _VOTE_BLOCK
                        reason   = f"sell_wall ratio={ratio:.2f}>={P20_COUNCIL_WALL_RATIO}"
                        ms_score = max(0.0, ms_score - 0.3)
                elif direction == "short" and bid_vol > 0:
                    ratio = bid_vol / max(ask_vol, 1e-9)
                    if ratio >= P20_COUNCIL_WALL_RATIO:
                        vote     = _VOTE_BLOCK
                        reason   = f"buy_wall ratio={ratio:.2f}>={P20_COUNCIL_WALL_RATIO}"
                        ms_score = max(0.0, ms_score - 0.3)
        except Exception as exc:
            log.debug("[P20-2/MicrostructureAgent] Wall check error %s: %s", symbol, exc)

        return AgentVote(agent="microstructure", vote=vote,
                         score=float(ms_score), reason=reason)


class CorrelationAgent:
    """[P20-2] Agent 3: Directional correlation with open positions (unchanged)."""

    def __init__(self, brain=None, executor=None):
        self._brain    = brain
        self._executor = executor

    def set_dependencies(self, brain, executor) -> None:
        self._brain    = brain
        self._executor = executor

    async def vote(self, symbol: str, direction: str) -> AgentVote:
        if self._brain is None or self._executor is None:
            return AgentVote(agent="correlation", vote=_VOTE_ABSTAIN,
                             score=0.5, reason="no_brain_or_executor")

        open_positions = list(getattr(self._executor, "positions", {}).keys())
        if not open_positions:
            return AgentVote(agent="correlation", vote=_VOTE_ALLOW,
                             score=0.5, reason="no_open_positions")

        try:
            corr = self._brain.position_correlation(symbol, open_positions)
        except Exception as exc:
            log.debug("[P20-2/CorrelationAgent] Error %s: %s", symbol, exc)
            return AgentVote(agent="correlation", vote=_VOTE_ABSTAIN,
                             score=0.5, reason=f"error:{exc}")

        corr_score = 1.0 - corr

        if corr >= P20_COUNCIL_CORR_HAIRCUT_THRESHOLD:
            # [P26-HAIRCUT] Hard VETO only above 0.95 correlation.
            log.warning(
                "[P26-HAIRCUT] CorrelationAgent HARD VETO %s: corr=%.4f >= %.2f",
                symbol, corr, P20_COUNCIL_CORR_HAIRCUT_THRESHOLD,
            )
            return AgentVote(agent="correlation", vote=_VOTE_BLOCK,
                             score=corr_score,
                             reason=(
                                 f"HARD_VETO corr={corr:.4f}>="
                                 f"{P20_COUNCIL_CORR_HAIRCUT_THRESHOLD} "
                                 f"(p26_dynamic_haircut)"
                             ))
        if corr >= P20_COUNCIL_CORR_VETO_THRESHOLD:
            # [P26-HAIRCUT] Correlation in the 0.85–0.95 band → DYNAMIC_HAIRCUT.
            # Allow the trade but signal a 75 % size reduction via a depressed
            # score.  The score is clamped to P20_COUNCIL_CORR_HAIRCUT_FACTOR
            # (default 0.25) so conviction-multiplier arithmetic naturally
            # reduces allocation without a hard block.
            haircut_score = float(min(corr_score, P20_COUNCIL_CORR_HAIRCUT_FACTOR))
            log.info(
                "[P26-HAIRCUT] CorrelationAgent DYNAMIC_HAIRCUT %s: "
                "corr=%.4f in [%.2f, %.2f) → ALLOW score=%.4f (75%% size cut)",
                symbol, corr,
                P20_COUNCIL_CORR_VETO_THRESHOLD, P20_COUNCIL_CORR_HAIRCUT_THRESHOLD,
                haircut_score,
            )
            return AgentVote(agent="correlation", vote=_VOTE_ALLOW,
                             score=haircut_score,
                             reason=(
                                 f"DYNAMIC_HAIRCUT corr={corr:.4f} "
                                 f"in [{P20_COUNCIL_CORR_VETO_THRESHOLD:.2f},"
                                 f"{P20_COUNCIL_CORR_HAIRCUT_THRESHOLD:.2f}) "
                                 f"haircut_score={haircut_score:.4f}"
                             ))
        if corr >= P20_COUNCIL_CORR_WARN_THRESHOLD:
            return AgentVote(agent="correlation", vote=_VOTE_ABSTAIN,
                             score=corr_score,
                             reason=f"corr={corr:.4f} warn_level")
        return AgentVote(agent="correlation", vote=_VOTE_ALLOW,
                         score=corr_score,
                         reason=f"corr={corr:.4f} acceptable")


# ══════════════════════════════════════════════════════════════════════════════
# [P20-2] Council Quorum logic (preserved)
# ══════════════════════════════════════════════════════════════════════════════

def _council_quorum(
    votes:       List[AgentVote],
    raw_scores:  List[float],
    direction:   str,
    symbol:      str,
) -> Tuple[str, float, float]:
    for v, s in zip(votes, raw_scores):
        if v.agent == "narrative" and s < P17_CATASTROPHE_THRESHOLD:
            return "CATASTROPHE_VETO", s, 0.0

    allow_count   = sum(1 for v in votes if v.vote == _VOTE_ALLOW)
    block_count   = sum(1 for v in votes if v.vote == _VOTE_BLOCK)
    active_scores = [s for v, s in zip(votes, raw_scores) if v.vote != _VOTE_ABSTAIN]
    mean_score    = float(sum(active_scores) / len(active_scores)) if active_scores else 0.5

    if block_count >= 2:
        log.info(
            "[P20-2] Council VETO %s %s: block_count=%d votes=[%s]",
            symbol, direction, block_count,
            ", ".join(f"{v.agent}={v.vote}" for v in votes),
        )
        return "VETO", mean_score, 0.0

    if allow_count >= 2:
        if mean_score > P17_BOOST_THRESHOLD:
            return "BOOST", mean_score, P17_BOOST_FACTOR
        return "NEUTRAL", mean_score, 0.0

    log.debug(
        "[P20-2] Council TIE/ABSTAIN %s %s → NEUTRAL "
        "allow=%d block=%d abstain=%d",
        symbol, direction, allow_count, block_count,
        len(votes) - allow_count - block_count,
    )
    return "NEUTRAL", mean_score, 0.0


# ══════════════════════════════════════════════════════════════════════════════
# [P32] Unified Veto Arbitrator
# ══════════════════════════════════════════════════════════════════════════════

class VetoArbitrator:
    """
    [P32] Unified Veto Arbitrator — aggregates Entropy, Correlation Haircuts,
    and Price Velocity into a single ``p_success`` score.

    Any order with ``p_success < 0.65`` is blocked by the Executor before it
    reaches the exchange.

    [P37-VPIN] Flow Toxicity Veto (Phase 37 addition):
    When set_flow_toxicity() records a ToxicityScore > P37_TOXICITY_THRESHOLD
    (default 0.80), compute_p_success() immediately returns 0.0 with the reason:
    "VETO: High Flow Toxicity (Informed Selling Detected)".
    This check is the ZEROTH gate — it runs before ALL other veto logic (P36,
    P35, P34, P33) so no individual component weighting can override a confirmed
    Volume-Clock informed-flow signal.

    Entropy Escalation: When ToxicityScore > P37_TOXICITY_THRESHOLD AND the
    current market Shannon Entropy (entropy_norm) is BELOW P37_LOW_ENTROPY_THRESHOLD
    (default 0.35), the veto is escalated with reason prefix "ESCALATED VETO:".
    Low entropy means the market is orderly and directional — the imbalance is
    deliberate institutional positioning, not random noise.

    [P36.1-DETECT] Manipulation Veto (Phase 36.1 addition):
    When set_spoof_probability() records a spoof_prob > P36_SPOOF_VETO_THRESHOLD
    (default 0.8), compute_p_success() immediately returns 0.0 with the reason:
    "VETO: Market Manipulation Detected (Spoofing)".
    This check is the first gate — it runs before all other veto logic (P35,
    P34, P33) so no weighting can override a confirmed manipulation signal.

    [P33-REVERSION] Exhaustion Gap Filter (Phase 33.1 addition):
    When a Whale Sweep event is recorded via record_sweep_event() and the
    subsequent PriceVelocity sample (within P33_EXHAUSTION_WINDOW_MS = 500ms)
    shows a drop of more than P33_EXHAUSTION_VELOCITY_DROP_PCT (50%) relative
    to the velocity-at-sweep, compute_p_success() returns 0.0 as a hard block.
    This guards against mean-reversion traps where the sweep exhausts directional
    momentum — the most dangerous form of false-breakout entry.

    Scoring model
    ─────────────
    p_success is the trimmed mean of three sub-scores (P32):

    1. Entropy Score
       Low entropy (orderly market)  → high score (good conditions)
       High entropy (chaotic market) → low score (poor conditions)
       entropy_score = 1.0 − entropy_normalized          ∈ [0, 1]

    2. Correlation Score
       Passed directly from the Executor's position_correlation()
       measurement:  0.0 = fully correlated (bad), 1.0 = uncorrelated (good)

    3. Velocity Score
       velocity_boost = 1.0  → neutral   (velocity_score = 0.5)
       velocity_boost > 1.0  → favourable (velocity_score > 0.5)
       velocity_score = min(1.0, (velocity_boost − 1.0) * 5.0 + 0.5)

    [P37-VPIN] Flow Toxicity Veto runs ZEROTH (before all other checks).
    [P36.1] Manipulation Veto runs FIRST.
    [P33-REVERSION] Exhaustion pre-check runs SECOND and short-circuits to 0.0.
    """

    # Class-level guard threshold — adjustable via env var at module load time.
    P_SUCCESS_THRESHOLD: float = float(
        os.environ.get("P32_VETO_ARB_THRESHOLD", "0.65")
    )

    def __init__(self) -> None:
        # [P33-REVERSION] Sweep event ring-buffer.
        # Each entry: (sweep_ts_ms: float, direction: str, velocity_at_sweep: float)
        self._p33_sweep_events: deque = deque(maxlen=32)

        # [P33-REVERSION] Last recorded price velocity sample.
        # Updated by the Executor's tape monitor loop via record_price_velocity().
        # Stored as (sample_ts_ms: float, velocity: float).
        self._p33_latest_velocity: tuple = (0.0, 0.0)  # (ts_ms, velocity)

        # [P34.1-SKEW] DataHub reference for global mid-price access.
        # Injected post-construction via set_hub() by the Executor.
        self._p34_hub: Optional[Any] = None

        # [P35.1-HEDGE] Dynamic Delta-Neutral Hedge state flags.
        # Injected each cycle by the Executor via set_p35_state().
        # _p35_hedge_rebalancing : True while a hedge order is in-flight.
        # _p35_btc_toxic         : True when BTC tape toxicity exceeds threshold.
        self._p35_hedge_rebalancing: bool = False
        self._p35_btc_toxic:         bool = False

        # ── [P36.1-DETECT] Manipulation Veto state ───────────────────────────
        # Per-symbol spoof probability injected by the Executor via
        # set_spoof_probability() after each Mimic Order Test cycle.
        # When this value exceeds P36_SPOOF_VETO_THRESHOLD (default 0.8),
        # compute_p_success() short-circuits to 0.0 before all other checks.
        self._p36_spoof_prob: float = 0.0
        self._p36_spoof_symbol: str = ""   # symbol for which the current prob applies
        # ── [/P36.1-DETECT] ──────────────────────────────────────────────────

        # ── [P37-VPIN] Flow Toxicity Veto state ──────────────────────────────
        # Volume-Clock ToxicityScore injected by the Executor each cycle via
        # set_flow_toxicity().  When this score exceeds P37_TOXICITY_THRESHOLD
        # (default 0.80), compute_p_success() short-circuits to 0.0 with reason:
        # "VETO: High Flow Toxicity (Informed Selling Detected)".
        # This is the ZEROTH gate — runs before ALL other veto checks.
        self._p37_toxicity_score: float = 0.0
        self._p37_toxicity_symbol: str  = ""
        # Last entropy_norm captured at veto time — used for Entropy Escalation
        # logging so the reason string distinguishes institutional vs noisy flow.
        self._p37_last_entropy_norm: float = 0.5
        # ── [/P37-VPIN] ──────────────────────────────────────────────────────

        log.debug("[P33-REVERSION] VetoArbitrator.__init__: exhaustion filter armed "
                  "(window=%.0fms drop=%.0f%%) | [P34-SKEW] max_skew=%.1f bps | "
                  "[P35.1-HEDGE] hedge veto armed | [P36.1-DETECT] spoof veto armed "
                  "(threshold=%.2f) | [P37-VPIN] flow toxicity veto armed "
                  "(threshold=%.2f)",
                  P33_EXHAUSTION_WINDOW_MS, P33_EXHAUSTION_VELOCITY_DROP_PCT * 100.0,
                  P34_MAX_SKEW_BPS, P36_SPOOF_VETO_THRESHOLD,
                  P37_TOXICITY_THRESHOLD)

    # ── [P34.1-SKEW] DataHub injection ───────────────────────────────────────

    def set_hub(self, hub: Any) -> None:
        """
        [P34.1-SKEW] Inject the DataHub instance so compute_p_success can
        call hub.get_global_mid_price() for the Price Skew Veto pre-check.

        Called by the Executor immediately after VetoArbitrator instantiation.
        """
        self._p34_hub = hub
        log.debug("[P34-SKEW] VetoArbitrator.set_hub: hub injected (type=%s)",
                  type(hub).__name__)

    # ── [P35.1-HEDGE] State injection ────────────────────────────────────────

    def set_p35_state(self, rebalancing: bool, btc_toxic: bool) -> None:
        """
        [P35.1-HEDGE] Inject live P35 hedge state from the Executor each cycle.

        Called by Executor._cycle() immediately after updating the governor mode
        so VetoArbitrator always has an up-to-date view of hedge activity and
        BTC toxicity before the next compute_p_success() call.

        Parameters
        ----------
        rebalancing : bool
            True while a P35 BTC-SWAP hedge order is in-flight.  When True,
            compute_p_success() returns 0.0 to block new Altcoin entries and
            prevent exposure from widening during the rebalance window.
        btc_toxic   : bool
            True when the P33 Toxic Flow Sniffer reports BTC tape toxicity
            above P33_TOXICITY_THRESHOLD.  When True, compute_p_success()
            returns 0.0 as BTC price action may be driven by informed order-flow
            that makes new Altcoin entries particularly dangerous.
        """
        try:
            self._p35_hedge_rebalancing = bool(rebalancing)
            self._p35_btc_toxic         = bool(btc_toxic)
            log.debug(
                "[P35.1-HEDGE] set_p35_state: rebalancing=%s btc_toxic=%s",
                self._p35_hedge_rebalancing, self._p35_btc_toxic,
            )
        except Exception as exc:
            log.debug("[P35.1-HEDGE] set_p35_state error: %s", exc)

    # ── [P37-VPIN] Flow Toxicity injection ───────────────────────────────────

    def set_flow_toxicity(self, symbol: str, toxicity_score: float) -> None:
        """
        [P37-VPIN] Inject the current Volume-Clock ToxicityScore for a symbol.

        Called by the Executor each cycle after reading DataHub.get_flow_toxicity()
        so compute_p_success() always has the freshest score before the next
        entry evaluation.

        When toxicity_score exceeds P37_TOXICITY_THRESHOLD (env: P37_TOXICITY_THRESHOLD,
        default 0.80), compute_p_success() will return 0.0 immediately, blocking
        the entry with reason "VETO: High Flow Toxicity (Informed Selling Detected)".

        If toxicity is also above threshold while entropy_norm is LOW
        (< P37_LOW_ENTROPY_THRESHOLD), the veto is escalated, indicating a
        deliberate, non-random institutional positioning event.

        Parameters
        ----------
        symbol         : str   — base symbol, e.g. "BTC"
        toxicity_score : float — ToxicityScore CDF ∈ [0.0, 1.0] from TapeBuffer
        """
        try:
            self._p37_toxicity_score  = max(0.0, min(1.0, float(toxicity_score)))
            self._p37_toxicity_symbol = str(symbol).upper()
            log.debug(
                "[P37-VPIN] set_flow_toxicity %s: score=%.4f "
                "(threshold=%.2f  will_veto=%s)",
                self._p37_toxicity_symbol, self._p37_toxicity_score,
                P37_TOXICITY_THRESHOLD,
                self._p37_toxicity_score > P37_TOXICITY_THRESHOLD,
            )
        except Exception as exc:
            log.debug("[P37-VPIN] set_flow_toxicity error: %s", exc)

    # ── [P36.1-DETECT] Spoof Probability injection ────────────────────────────

    def set_spoof_probability(self, symbol: str, spoof_prob: float) -> None:
        """
        [P36.1-DETECT] Inject the current spoof probability for a symbol.

        Called by the Executor each TWAP cycle after _p36_run_mimic_test()
        produces a new measurement.  The stored value is consumed on the very
        next compute_p_success() call for the same symbol.

        If spoof_prob exceeds P36_SPOOF_VETO_THRESHOLD (env: P36_SPOOF_VETO_THRESHOLD,
        default 0.8), compute_p_success() will return 0.0 immediately, blocking
        the entry with reason "VETO: Market Manipulation Detected (Spoofing)".

        Parameters
        ----------
        symbol     : str   — base symbol, e.g. "BTC"
        spoof_prob : float — spoof probability EMA from DataHub ∈ [0.0, 1.0]
        """
        try:
            self._p36_spoof_prob   = max(0.0, min(1.0, float(spoof_prob)))
            self._p36_spoof_symbol = str(symbol).upper()
            log.debug(
                "[P36.1-DETECT] set_spoof_probability %s: spoof_prob=%.4f "
                "(veto_threshold=%.2f  will_veto=%s)",
                self._p36_spoof_symbol, self._p36_spoof_prob,
                P36_SPOOF_VETO_THRESHOLD,
                self._p36_spoof_prob > P36_SPOOF_VETO_THRESHOLD,
            )
        except Exception as exc:
            log.debug("[P36.1-DETECT] set_spoof_probability error: %s", exc)

    # ── [P33-REVERSION] Sweep & velocity intake ──────────────────────────────

    def record_sweep_event(self, direction: str, velocity_at_sweep: float) -> None:
        """
        [P33-REVERSION] Register a Whale Sweep event for exhaustion tracking.

        Called by the Executor immediately after _check_liquidity_sweep() returns
        True so the VetoArbitrator has a reference velocity snapshot.

        Parameters
        ----------
        direction         : str — "long" or "short"
        velocity_at_sweep : float — trades-per-second at the moment of sweep
                            detection (from DataHub.get_trade_velocity).
        """
        try:
            now_ms = time.monotonic() * 1_000.0
            self._p33_sweep_events.append(
                (now_ms, direction, float(velocity_at_sweep))
            )
            log.debug(
                "[P33-REVERSION] Sweep event recorded: dir=%s vel=%.2f tps ts_ms=%.1f",
                direction, velocity_at_sweep, now_ms,
            )
        except Exception as exc:
            log.debug("[P33-REVERSION] record_sweep_event error: %s", exc)

    def record_price_velocity(self, velocity: float) -> None:
        """
        [P33-REVERSION] Update the latest price velocity sample.

        Called periodically by the Executor's tape monitor loop so the
        exhaustion check always has a fresh velocity reference to compare
        against the velocity recorded at sweep time.

        Parameters
        ----------
        velocity : float — current trades-per-second from TapeBuffer.trade_velocity()
        """
        try:
            now_ms = time.monotonic() * 1_000.0
            self._p33_latest_velocity = (now_ms, max(0.0, float(velocity)))
        except Exception as exc:
            log.debug("[P33-REVERSION] record_price_velocity error: %s", exc)

    # ── [P33-REVERSION] Exhaustion detection ─────────────────────────────────

    def _p33_check_exhaustion(self) -> bool:
        """
        [P33-REVERSION] Return True if an exhaustion gap is detected.

        Detection logic
        ───────────────
        1. Iterate recent sweep events (P33_EXHAUSTION_SWEEP_TTL_MS cutoff).
        2. For each sweep event that is at most P33_EXHAUSTION_WINDOW_MS old:
           • velocity_now is the latest sample if it is fresher than the sweep.
           • If velocity_now < velocity_at_sweep * (1 − P33_EXHAUSTION_VELOCITY_DROP_PCT)
             → exhaustion confirmed → return True.
        3. Clean up sweep events older than P33_EXHAUSTION_SWEEP_TTL_MS as a
           side-effect to prevent unbounded memory growth.

        Returns False when no exhaustion is detected (normal path).
        """
        try:
            now_ms = time.monotonic() * 1_000.0
            sweep_ttl_cutoff  = now_ms - P33_EXHAUSTION_SWEEP_TTL_MS
            window_cutoff     = now_ms - P33_EXHAUSTION_WINDOW_MS
            vel_ts, vel_now   = self._p33_latest_velocity

            # Discard stale entries to keep the deque lean.
            while self._p33_sweep_events:
                oldest_ts = self._p33_sweep_events[0][0]
                if oldest_ts < sweep_ttl_cutoff:
                    self._p33_sweep_events.popleft()
                else:
                    break

            for sweep_ts, sweep_dir, vel_at_sweep in self._p33_sweep_events:
                # Only consider sweeps within the 500ms exhaustion window.
                if sweep_ts < window_cutoff:
                    continue
                # The velocity sample must be AFTER the sweep (otherwise we're
                # comparing pre-sweep velocity to itself, which is meaningless).
                if vel_ts <= sweep_ts:
                    continue
                # Require a meaningful reference velocity to avoid divide-by-zero
                # and false positives on near-zero velocity baselines.
                if vel_at_sweep <= 0.1:
                    continue

                velocity_threshold = vel_at_sweep * (1.0 - P33_EXHAUSTION_VELOCITY_DROP_PCT)
                if vel_now < velocity_threshold:
                    log.warning(
                        "[P33-REVERSION] EXHAUSTION GAP detected: "
                        "sweep dir=%s vel_at_sweep=%.2f → vel_now=%.2f "
                        "(drop=%.1f%% > threshold=%.0f%%) "
                        "sweep_age_ms=%.0f — HARD VETO applied.",
                        sweep_dir, vel_at_sweep, vel_now,
                        (1.0 - vel_now / vel_at_sweep) * 100.0,
                        P33_EXHAUSTION_VELOCITY_DROP_PCT * 100.0,
                        now_ms - sweep_ts,
                    )
                    return True
        except Exception as exc:
            log.debug("[P33-REVERSION] _p33_check_exhaustion error: %s", exc)
        return False

    # ── Core scoring ─────────────────────────────────────────────────────────

    def compute_p_success(
        self,
        recent_returns:    List[float],
        correlation_score: float = 0.5,
        velocity_boost:    float = 1.0,
        local_price:       float = 0.0,
        global_mid_price:  Optional[float] = None,
    ) -> float:
        """
        Compute the composite ``p_success`` score.

        [P36.1-DETECT] Manipulation Veto runs FIRST (before all other checks).
        If the current spoof_probability (set via set_spoof_probability()) exceeds
        P36_SPOOF_VETO_THRESHOLD (default 0.8), returns 0.0 immediately with
        reason "VETO: Market Manipulation Detected (Spoofing)".

        [P34.1-SKEW] Price Skew Veto runs SECOND.
        If the OKX local price deviates from global_mid_price by more than
        P34_MAX_SKEW_BPS basis points, returns 0.0 immediately.

        [P33-REVERSION] Exhaustion Gap Filter runs THIRD as a hard pre-check.
        If an exhaustion gap is detected (whale sweep followed by >50% velocity
        collapse within 500ms), this method immediately returns 0.0, bypassing
        the trimmed-mean calculation so no individual component can override
        the mean-reversion veto.

        Parameters
        ----------
        recent_returns    : list of float — percentage returns for entropy calc
        correlation_score : float ∈ [0, 1] — 0=fully correlated, 1=uncorrelated
        velocity_boost    : float — conviction_boost from PriceVelocityMonitor
                            (1.0 = neutral, > 1.0 = Coinbase leads OKX)
        local_price       : float — current OKX execution price (for skew check)
        global_mid_price  : float | None — synthetic mid from DataHub (P34.1-SYNTH)

        Returns
        -------
        float ∈ [0, 1] — 0.0 on toxicity/manipulation/skew/exhaustion veto; trimmed mean otherwise.
        """
        # ── [P37-VPIN] Flow Toxicity Veto — MUST run before ALL other checks ──
        # If the Volume-Clock ToxicityScore (set via set_flow_toxicity()) exceeds
        # P37_TOXICITY_THRESHOLD, informed selling / institutional flow is detected.
        # Block the entry unconditionally so no other factor can override the signal.
        try:
            if self._p37_toxicity_score > P37_TOXICITY_THRESHOLD:
                # Compute entropy for escalation check.
                # If the caller provided recent_returns, derive entropy_norm now;
                # otherwise fall back to the cached value from the last full scoring.
                _en_norm = 0.5  # neutral default
                try:
                    _en_bits = compute_shannon_entropy(recent_returns)
                    _en_norm = normalize_entropy(_en_bits)
                except Exception:
                    pass
                self._p37_last_entropy_norm = _en_norm

                _escalated = _en_norm < P37_LOW_ENTROPY_THRESHOLD
                _veto_reason = (
                    "ESCALATED VETO: High Flow Toxicity + Low Entropy "
                    "(Deliberate Institutional Positioning Detected)"
                    if _escalated
                    else "VETO: High Flow Toxicity (Informed Selling Detected)"
                )

                log.warning(
                    "[P37-VPIN] %s symbol=%s toxicity=%.4f > threshold=%.2f "
                    "entropy_norm=%.4f (escalated=%s) — "
                    "compute_p_success → 0.0 (blocking all new entries).",
                    _veto_reason,
                    self._p37_toxicity_symbol, self._p37_toxicity_score,
                    P37_TOXICITY_THRESHOLD, _en_norm, _escalated,
                )
                _append_veto_audit_file(
                    symbol  = self._p37_toxicity_symbol,
                    reason  = _veto_reason,
                    details = (
                        f"toxicity={self._p37_toxicity_score:.4f} "
                        f"threshold={P37_TOXICITY_THRESHOLD:.2f} "
                        f"entropy_norm={_en_norm:.4f} "
                        f"escalated={_escalated} [P37-VPIN]"
                    ),
                )
                return 0.0
        except Exception as exc:
            log.debug("[P37-VPIN] flow toxicity pre-check error: %s", exc)
        # ── [/P37-VPIN] ──────────────────────────────────────────────────────

        # ── [P36.1-DETECT] Manipulation Veto — MUST run before ALL other checks ─
        # If the Mimic Order Engine confirmed active spoofing for this symbol,
        # force p_success=0.0 and record the reason in the veto audit log.
        try:
            if self._p36_spoof_prob > P36_SPOOF_VETO_THRESHOLD:
                log.warning(
                    "[P36.1-DETECT] VETO: Market Manipulation Detected (Spoofing) "
                    "symbol=%s spoof_prob=%.4f > threshold=%.2f — "
                    "compute_p_success → 0.0 (blocking entry, bypassing all other checks).",
                    self._p36_spoof_symbol, self._p36_spoof_prob, P36_SPOOF_VETO_THRESHOLD,
                )
                _append_veto_audit_file(
                    symbol  = self._p36_spoof_symbol,
                    reason  = "VETO: Market Manipulation Detected (Spoofing)",
                    details = (
                        f"spoof_prob={self._p36_spoof_prob:.4f} "
                        f"threshold={P36_SPOOF_VETO_THRESHOLD:.2f} "
                        f"[P36.1-DETECT]"
                    ),
                )
                return 0.0
        except Exception as exc:
            log.debug("[P36.1-DETECT] manipulation veto pre-check error: %s", exc)
        # ── [/P36.1-DETECT] ──────────────────────────────────────────────────

        # ── [P35.1-HEDGE] Hedge-Aware Veto — MUST run before all other checks ─
        # Block new Altcoin entries if a hedge rebalance is in-flight or if
        # BTC tape toxicity is elevated.  Both conditions are set by the
        # Executor each cycle via set_p35_state().
        try:
            if self._p35_hedge_rebalancing:
                log.warning(
                    "[P35.1-HEDGE] VETO: P35 hedge rebalance in-flight — "
                    "compute_p_success → 0.0 (blocking new Altcoin entries).",
                )
                return 0.0
            if self._p35_btc_toxic:
                log.warning(
                    "[P35.1-HEDGE] VETO: BTC Toxic Flow detected (P33 sniffer) — "
                    "compute_p_success → 0.0 (blocking new Altcoin entries).",
                )
                return 0.0
        except Exception as exc:
            log.debug("[P35.1-HEDGE] hedge-aware veto pre-check error: %s", exc)
        # ── [/P35.1-HEDGE] ───────────────────────────────────────────────────

        # ── [P34.1-SKEW] Price Skew Veto — runs AFTER P36 Manipulation Veto ──
        try:
            if (
                local_price > 0.0
                and global_mid_price is not None
                and global_mid_price > 0.0
            ):
                skew_bps = abs(local_price - global_mid_price) / global_mid_price * 10_000.0
                if skew_bps > P34_MAX_SKEW_BPS:
                    log.warning(
                        "[P34-SKEW] VETO: Price Skew Detected "
                        "local=%.6f global=%.6f skew=%.2f bps > %.1f bps — "
                        "compute_p_success → 0.0 (local manipulation risk)",
                        local_price, global_mid_price, skew_bps, P34_MAX_SKEW_BPS,
                    )
                    return 0.0
        except Exception as exc:
            log.debug("[P34-SKEW] skew check error: %s", exc)
        # ── [/P34.1-SKEW] ────────────────────────────────────────────────────

        # ── [P33-REVERSION] Hard pre-check — exhaustion veto ──────────────
        try:
            if self._p33_check_exhaustion():
                log.info(
                    "[P33-REVERSION] compute_p_success → 0.0 "
                    "(exhaustion gap hard veto bypasses trimmed mean)",
                )
                return 0.0
        except Exception as exc:
            log.debug("[P33-REVERSION] exhaustion pre-check exception: %s", exc)
        # ── [/P33-REVERSION] ─────────────────────────────────────────────

        # Component 1 — Entropy Score
        try:
            entropy_bits = compute_shannon_entropy(recent_returns)
            entropy_norm = normalize_entropy(entropy_bits)
        except Exception:
            entropy_norm = 0.5  # neutral fallback
        entropy_score = max(0.0, min(1.0, 1.0 - entropy_norm))

        # Component 2 — Correlation Score (passed directly)
        corr_score = max(0.0, min(1.0, float(correlation_score)))

        # Component 3 — Velocity Score
        # velocity_boost=1.0 → 0.5 (neutral)
        # velocity_boost=1.15 → 0.5 + 0.15*5 = 0.5 + 0.75 = 1.0 (capped)
        vel_score = min(1.0, (float(velocity_boost) - 1.0) * 5.0 + 0.5)
        vel_score = max(0.0, vel_score)

        # Trimmed mean: drop the lowest sub-score, average the other two.
        # This prevents a single-axis failure from unilaterally blocking a trade
        # while still requiring two healthy dimensions.
        components = sorted([entropy_score, corr_score, vel_score])
        p_success  = (components[1] + components[2]) / 2.0

        log.debug(
            "[P32-VETO-ARB] p_success=%.4f "
            "(entropy_score=%.4f corr_score=%.4f vel_score=%.4f) "
            "entropy_norm=%.4f vel_boost=%.4f",
            p_success, entropy_score, corr_score, vel_score,
            entropy_norm, velocity_boost,
        )
        return round(p_success, 4)

    def is_blocked(
        self,
        recent_returns:    List[float],
        correlation_score: float = 0.5,
        velocity_boost:    float = 1.0,
    ) -> bool:
        """Return True when p_success falls below the configured threshold."""
        return self.compute_p_success(
            recent_returns, correlation_score, velocity_boost
        ) < self.P_SUCCESS_THRESHOLD


# ══════════════════════════════════════════════════════════════════════════════
# LLMContextVeto  — Phase 22 edition
# ══════════════════════════════════════════════════════════════════════════════

class LLMContextVeto:
    """
    Council of Judges (P20-2) with P22 extensions:

      [P22-1] NarrativeAgent routes through OpenRouter; latency always
              reported; errors carried in council_detail for dashboard.
      [P22-2] Shannon Entropy computed over recent candle returns and
              attached to NarrativeResult.  Callers inspect
              result.entropy_triggered to apply confidence gating.
      [P22-3] trailing_gap_adjustment populated from regime_trail_adjustment().
    """

    def __init__(
        self,
        scraper:    Optional[IntelligenceScraper] = None,
        hub=None,
        brain=None,
        executor=None,
    ):
        self._scraper  = scraper or IntelligenceScraper()

        self._narrative_agent      = NarrativeAgent(self._scraper)
        self._microstructure_agent = MicrostructureAgent(hub)
        self._correlation_agent    = CorrelationAgent(brain, executor)

        # [P31-VELOCITY] Cross-exchange lead/lag monitor — callers inject price
        # events via self.velocity_monitor.record("coinbase"|"okx", price).
        self.velocity_monitor = PriceVelocityMonitor()

        self._audit: List[NarrativeResult] = []
        self._audit_maxlen       = 50
        self._veto_count:        int = 0
        self._boost_count:       int = 0
        self._catastrophe_count: int = 0
        self._timeout_count:     int = 0

        log.info(
            "[P17/P18/P20/P22] LLMContextVeto (Council of Judges + OpenRouter) init: "
            "model=%s  veto=%.2f  boost=%.2f  catastrophe=%.2f  "
            "entropy_thresh=%.2f  bull_widen=%.4f  chop_tighten=%.4f",
            OPENROUTER_MODEL,
            P17_VETO_THRESHOLD, P17_BOOST_THRESHOLD, P17_CATASTROPHE_THRESHOLD,
            P22_ENTROPY_HIGH_THRESHOLD, P22_BULL_TRAIL_WIDEN, P22_CHOP_TRAIL_TIGHTEN,
        )

    # ── Dependency injection ───────────────────────────────────────────────────
    def set_hub(self, hub) -> None:
        self._microstructure_agent.set_hub(hub)
        log.info("[P20-2] MicrostructureAgent hub injected.")

    def set_brain_and_executor(self, brain, executor) -> None:
        self._correlation_agent.set_dependencies(brain, executor)
        log.info("[P20-2] CorrelationAgent brain/executor injected.")

    # ── Public API ─────────────────────────────────────────────────────────────
    async def score(
        self,
        symbol:           str,
        direction:        str,
        drawdown_killed:  bool = False,
        cancel_buys_flag: bool = False,
        recent_returns:   Optional[List[float]] = None,
        regime:           str = "chop",
    ) -> NarrativeResult:
        """
        Run the Council of Judges and return a NarrativeResult.

        Parameters
        ----------
        recent_returns : list of float, optional
            Percentage returns for the rolling window (used for Entropy Shield).
        regime : str
            Current HMM regime label (used for trailing-gap adjustment).
        """
        if cancel_buys_flag:
            return self._neutral(
                symbol, direction, latency_ms=0.0,
                reason="oracle_cancel_buys",
                regime=regime, recent_returns=recent_returns,
            )

        if drawdown_killed:
            return self._neutral(
                symbol, direction, latency_ms=0.0,
                reason="circuit_breaker_killed",
                regime=regime, recent_returns=recent_returns,
            )

        t0        = time.monotonic()
        timed_out = False
        try:
            result = await asyncio.wait_for(
                self._council_score(symbol, direction, recent_returns, regime),
                timeout=P17_SCRAPER_TIMEOUT_MS / 1_000.0,
            )
        except asyncio.TimeoutError:
            timed_out  = True
            self._timeout_count += 1
            elapsed    = (time.monotonic() - t0) * 1_000
            log.warning(
                "[P23-OPT-1] AI FALLBACK: OpenRouter latency exceeded %.1fms "
                "for %s council — switching to Technical Mode (neutral 0.5).",
                elapsed, symbol,
            )
            result = self._neutral(
                symbol, direction, latency_ms=elapsed,
                reason="timeout", timed_out=True,
                regime=regime, recent_returns=recent_returns,
            )
        except Exception as exc:
            elapsed = (time.monotonic() - t0) * 1_000
            log.debug("[P17/P22] %s council error (%.1f ms): %s", symbol, elapsed, exc)
            result  = self._neutral(
                symbol, direction, latency_ms=elapsed, reason=str(exc),
                regime=regime, recent_returns=recent_returns,
            )

        # [P25] Veto Audit — log suppressions caused by council or entropy
        try:
            if result.verdict in ("VETO", "CATASTROPHE_VETO"):
                reason_label = (
                    "Catastrophe Veto"
                    if result.verdict == "CATASTROPHE_VETO"
                    else "Regime Veto"
                )
                block_agents = [
                    f"{d['agent']}:{d['vote']}"
                    for d in result.council_detail
                    if d.get("vote") == "BLOCK"
                ]
                details_str = (
                    f"score={result.score:.4f} council=[{'; '.join(block_agents)}]"
                    f" latency={result.latency_ms:.1f}ms"
                )
                _append_veto_audit_file(symbol, reason_label, details_str)

            if result.entropy_triggered:
                entropy_val = getattr(result, "entropy", 0.0)
                _append_veto_audit_file(
                    symbol,
                    "Entropy Shield Active",
                    f"Entropy {entropy_val:.4f} > {P22_ENTROPY_HIGH_THRESHOLD:.2f} "
                    f"threshold — confidence floor raised by "
                    f"{P22_ENTROPY_CONFIDENCE_BOOST * 100:.0f}%",
                )
        except Exception as _audit_exc:
            log.debug("[P25] Veto audit append error: %s", _audit_exc)

        self._record(result)
        return result

    def compute_conviction_size(
        self,
        base_usd:      float,
        result:        NarrativeResult,
        avail:         float,
        max_alloc_pct: float,
        equity:        float,
    ) -> float:
        m = result.conviction_multiplier
        if m <= 1.0:
            return base_usd
        scaled    = base_usd * m
        alloc_cap = avail * max_alloc_pct
        scaled    = min(scaled, alloc_cap)
        if P17_MAX_PORTFOLIO_HEAT > 0.0 and equity > 0:
            heat_cap = equity * P17_MAX_PORTFOLIO_HEAT
            scaled   = min(scaled, heat_cap)
        if scaled < base_usd:
            return base_usd
        log.info(
            "[P18] CONVICTION SIZE %s: base=$%.2f → final=$%.2f (mult=%.3fx)",
            result.symbol, base_usd, scaled, m,
        )
        return scaled

    # Backward-compat shim
    def compute_boosted_size(
        self, base_usd: float, result: NarrativeResult,
        avail: float, max_alloc_pct: float, equity: float,
    ) -> float:
        return self.compute_conviction_size(base_usd, result, avail, max_alloc_pct, equity)

    def status_snapshot(self) -> dict:
        recent = self._audit[-10:]
        return {
            "mode":              P17_SCRAPER_MODE,
            "council_enabled":   True,
            "openrouter_model":  OPENROUTER_MODEL,
            "openrouter_active": bool(OPENROUTER_API_KEY),
            "veto_threshold":    P17_VETO_THRESHOLD,
            "boost_threshold":   P17_BOOST_THRESHOLD,
            "boost_factor":      P17_BOOST_FACTOR,
            "catastrophe_threshold":    P17_CATASTROPHE_THRESHOLD,
            "max_portfolio_heat":        P17_MAX_PORTFOLIO_HEAT,
            "p20_obi_veto_threshold":    P20_COUNCIL_OBI_VETO_THRESHOLD,
            "p20_wall_ratio":            P20_COUNCIL_WALL_RATIO,
            "p20_corr_veto_threshold":   P20_COUNCIL_CORR_VETO_THRESHOLD,
            "p20_corr_warn_threshold":   P20_COUNCIL_CORR_WARN_THRESHOLD,
            "p20_corr_haircut_threshold": P20_COUNCIL_CORR_HAIRCUT_THRESHOLD,
            "p20_corr_haircut_factor":   P20_COUNCIL_CORR_HAIRCUT_FACTOR,
            "p22_entropy_threshold":     P22_ENTROPY_HIGH_THRESHOLD,
            "p22_entropy_boost":         P22_ENTROPY_CONFIDENCE_BOOST,
            "p22_bull_trail_widen":      P22_BULL_TRAIL_WIDEN,
            "p22_chop_trail_tighten":    P22_CHOP_TRAIL_TIGHTEN,
            # [P25] Veto audit path exposed for dashboard
            "p25_veto_audit_path":       VETO_AUDIT_PATH,
            "counters": {
                "veto":        self._veto_count,
                "boost":       self._boost_count,
                "catastrophe": self._catastrophe_count,
                "timeout":     self._timeout_count,
            },
            "recent_results": [
                {
                    "symbol":                  r.symbol,
                    "direction":               r.direction,
                    "score":                   round(r.score, 4),
                    "verdict":                 r.verdict,
                    "boost_factor":            r.boost_factor,
                    "conviction_multiplier":   round(r.conviction_multiplier, 4),
                    "latency_ms":              round(r.latency_ms, 2),
                    "timed_out":               r.timed_out,
                    "age_secs":                round(time.time() - r.ts, 1),
                    "council_detail":          r.council_detail,
                    "llm_used":                r.llm_used,
                    "llm_error":               r.llm_error,
                    "entropy":                 round(r.entropy, 4),
                    # [P24-IL-1] Normalized entropy in snapshot for dashboard
                    "entropy_normalized":      round(getattr(r, "entropy_normalized", normalize_entropy(r.entropy)), 4),
                    "entropy_triggered":       r.entropy_triggered,
                    "trailing_gap_adjustment": round(r.trailing_gap_adjustment, 5),
                    # [P27-SNR] Signal quality aggregated across council agents
                    "accuracy": round(
                        sum(cd.get("accuracy", 0.5) for cd in r.council_detail) / max(len(r.council_detail), 1),
                        4,
                    ) if r.council_detail else 0.5,
                    "snr": round(
                        sum(cd.get("snr", 0.0) for cd in r.council_detail) / max(len(r.council_detail), 1),
                        4,
                    ) if r.council_detail else 0.0,
                }
                for r in reversed(recent)
            ],
        }

    # ── [P22] Internal: full council orchestration ─────────────────────────────
    async def _council_score(
        self,
        symbol:         str,
        direction:      str,
        recent_returns: Optional[List[float]],
        regime:         str,
    ) -> NarrativeResult:
        t0 = time.monotonic()

        # Run all three agents concurrently
        agent_votes: List[AgentVote] = list(
            await asyncio.gather(
                asyncio.create_task(self._narrative_agent.vote(symbol, direction)),
                asyncio.create_task(self._microstructure_agent.vote(symbol, direction)),
                asyncio.create_task(self._correlation_agent.vote(symbol, direction)),
                return_exceptions=False,
            )
        )

        raw_scores = [v.score for v in agent_votes]
        latency_ms = (time.monotonic() - t0) * 1_000

        verdict, mean_score, boost_factor = _council_quorum(
            agent_votes, raw_scores, direction, symbol,
        )

        conv_mult = _compute_conviction_multiplier(mean_score, P17_VETO_THRESHOLD)
        if verdict == "CATASTROPHE_VETO":
            conv_mult = 0.0
            self._catastrophe_count += 1
            log.warning(
                "[P20-2] CATASTROPHE_VETO %s %s score=%.4f lat=%.1fms",
                symbol, direction, mean_score, latency_ms,
            )
        elif verdict == "VETO":
            self._veto_count += 1
        elif verdict == "BOOST":
            self._boost_count += 1

        # ── [P31-VELOCITY] Apply cross-exchange lead/lag conviction boost ──────
        # Only boost non-vetoed signals so we never amplify a blocked entry.
        if verdict not in ("VETO", "CATASTROPHE_VETO"):
            _vel_boost = self.velocity_monitor.conviction_boost(direction)
            if _vel_boost != 1.0:
                old_mult  = conv_mult
                conv_mult = min(_CONVICTION_MAX, conv_mult * _vel_boost)
                log.info(
                    "[P31-VELOCITY] %s %s: Coinbase-lead boost applied "
                    "conv_mult %.4f → %.4f (boost=%.3f)",
                    symbol, direction, old_mult, conv_mult, _vel_boost,
                )
        # ── [/P31-VELOCITY] ──────────────────────────────────────────────────

        # [P22-1] Detect whether LLM was actually used / errored
        llm_used  = False
        llm_error: Optional[str] = None
        for v in agent_votes:
            if v.agent == "narrative":
                llm_used = "openrouter:" in v.reason
                # [FIX-AI-RELIABILITY] Detect AI connection error separately
                # from genuine neutral sentiment so the dashboard can distinguish.
                if "ai_connection_error" in v.reason:
                    try:
                        llm_error = v.reason.split("llm_err=", 1)[1].split(" |")[0].strip()
                        if not llm_error:
                            llm_error = "connection_error"
                    except Exception:
                        llm_error = "connection_error"
                elif "llm_err=" in v.reason:
                    try:
                        llm_error = v.reason.split("llm_err=", 1)[1].split(" |")[0].strip()
                    except Exception:
                        llm_error = "parse_failed"
                break

        # [P22-2] Entropy Shield
        entropy         = compute_shannon_entropy(recent_returns or [])
        _, ent_triggered = entropy_confidence_gate(0.5, entropy)

        # [P22-3] Regime-Adaptive trailing gap
        trail_adj = regime_trail_adjustment(regime)

        council_detail = [
            {
                "agent":    v.agent,
                "vote":     v.vote,
                "score":    round(v.score, 4),
                "reason":   v.reason,
                # [P27-SNR] Signal quality — always present so Dashboard gauges never KeyError
                "accuracy": round(getattr(v, "accuracy", 0.5), 4),
                "snr":      round(getattr(v, "snr",      0.0), 4),
            }
            for v in agent_votes
        ]

        # headlines_used metadata
        n_headlines = 0
        try:
            cached = self._scraper._cache.get(symbol)
            n_headlines = len(cached[1]) if cached else 0
        except Exception:
            pass

        return NarrativeResult(
            symbol=symbol,
            direction=direction,
            score=mean_score,
            verdict=verdict,
            boost_factor=boost_factor,
            conviction_multiplier=conv_mult,
            headlines_used=n_headlines,
            latency_ms=latency_ms,
            timed_out=False,
            council_detail=council_detail,
            llm_used=llm_used,
            llm_error=llm_error,
            entropy=entropy,
            entropy_triggered=ent_triggered,
            # [P24-IL-1] Normalized entropy for smoother dashboard sparklines.
            entropy_normalized=normalize_entropy(entropy),
            trailing_gap_adjustment=trail_adj,
        )

    def _neutral(
        self,
        symbol:         str,
        direction:      str,
        latency_ms:     float,
        reason:         str = "",
        timed_out:      bool = False,
        regime:         str = "chop",
        recent_returns: Optional[List[float]] = None,
    ) -> NarrativeResult:
        entropy       = compute_shannon_entropy(recent_returns or [])
        _, ent_trig   = entropy_confidence_gate(0.5, entropy)
        trail_adj     = regime_trail_adjustment(regime)
        llm_error_val = reason if reason else None
        return NarrativeResult(
            symbol=symbol, direction=direction, score=_NEUTRAL,
            verdict="NEUTRAL", boost_factor=0.0,
            conviction_multiplier=_compute_conviction_multiplier(_NEUTRAL, P17_VETO_THRESHOLD),
            headlines_used=0, latency_ms=latency_ms,
            timed_out=timed_out,
            council_detail=[{
                "agent": "fallback", "vote": _VOTE_ABSTAIN,
                "score": 0.5, "reason": reason,
                # [P30.5-SNR] Always inject accuracy and snr so Dashboard gauges
                # never encounter None or a missing key — even on VADER fallback.
                "accuracy": 0.5,
                "snr":      0.0,
            }],
            llm_used=False,
            llm_error=llm_error_val,
            entropy=entropy,
            entropy_triggered=ent_trig,
            # [P24-IL-1] Always populate normalized entropy so dashboard never divides by zero.
            entropy_normalized=normalize_entropy(entropy),
            trailing_gap_adjustment=trail_adj,
        )

    def _record(self, result: NarrativeResult) -> None:
        self._audit.append(result)
        if len(self._audit) > self._audit_maxlen:
            self._audit.pop(0)
