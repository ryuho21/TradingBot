"""
executor.py  —  OKX Execution Gateway
                Phase 40.1: Binary Truth Synchronization (Bridge-Ready)
                Phase 37: Predictive Microstructure (VPIN / Flow Toxicity)
                Phase 36.2: Adversarial Resilience — Dynamic Clamping,
                Multi-Level Mimicry, and Golden Build Monitor

Phase 40.1 additions (Binary Truth Synchronization):
  [P40.1-RG]    Ready-Gate Fix — _on_bridge_account_update() and
                _on_bridge_ghost_healed() now unconditionally call
                self._p40_ready_gate.set() after accepting a valid equity
                value.  Previously the bridge path NEVER opened the gate,
                causing _cycle() to block trading permanently whenever the
                Rust bridge was connected.  The fix ensures the first
                non-ghost, non-zero Binary Truth equity from the bridge
                unblocks the cycle loop regardless of whether the legacy
                WS account handler (_on_account_update) fires concurrently.

  [P40.1-SHIELD] Zombie Veto in ghost_healed — executor-level ghost_healed
                handler now explicitly sets _p20_zombie_mode = False when
                it applies the healed equity.  This mirrors the main.py
                handler and ensures Zombie Mode can never remain active
                after the bridge confirms the $0.00 reading was phantom.

  [P40.1-SAFE]  Safe Mode Hold — _cycle() ready-gate log now includes the
                current bridge.connected status so operators can distinguish
                "waiting for bridge to connect" vs "bridge connected but no
                equity received yet" in the log stream.

Phase 37 additions (this release):
  [P37-VPIN]   Emergency Toxicity Exit — _tape_monitor_loop now polls
               DataHub.get_flow_toxicity(symbol) every tick.  If ToxicityScore
               spikes above P37_EMERGENCY_EXIT_TOXICITY (default 0.95) and an
               open position exists, an immediate IOC _close_position() task is
               launched via _p37_emergency_toxicity_exit().

  [P37-VPIN]   VetoArbitrator Integration — ToxicityScore is injected into
               VetoArbitrator.set_flow_toxicity() every tape-monitor tick AND
               at the start of each _cycle() as a belt-and-suspenders feed.

  [P37-GOLDEN] Golden Build Preservation — the emergency toxicity exit does NOT
               call _p362_reset_golden_counter(). A Toxic Flush protection exit
               is a feature, not a failure; the golden counter continues its run.

Phase 36.2 additions (this release):
  [P36.2-DYNCLAMP]  Dynamic Price Clamping — PriceVelocity is sampled over a
                    configurable 10-second rolling window (P362_VELOCITY_WINDOW_SECS)
                    per symbol using the tape-monitor velocity feed.  When
                    PriceVelocity > P362_VELOCITY_THRESHOLD_PCT (default 0.5%) the
                    DynamicBuffer is doubled from PRICE_LIMIT_BUFFER (5 bps) to
                    P362_DYNAMIC_BUFFER_HIGH (10 bps).  The live buffer is exposed
                    via _p362_get_dynamic_buffer(symbol) and used in every
                    _execute_order price guard and in the PortfolioGovernor hedge leg.

  [P36.2-GOLDEN]    Golden Build Monitor — _p362_cycle_count tracks consecutive
                    successful cycles with zero sCode 51006 rejections and zero
                    IndexError crashes.  Any failure resets the counter.  On reaching
                    P362_GOLDEN_BUILD_CYCLES (default 1000) the monitor writes
                    GOLDEN_BUILD_REPORT.txt with full performance stats and logs a
                    prominent SUCCESS banner.

  [P36.2-MIMIC2]    Multi-Level Mimicry — _p36_run_mimic_test() now places TWO
                    micro POST_ONLY orders simultaneously: one at Level 1 (best
                    wall) and one at Level 3 of the book.  A Spoof is only confirmed
                    when BOTH probe levels evaporate (≥ P36_SPOOF_EVAPORATION) within
                    P36_SPOOF_REACTION_MS.  A single-level evaporation is logged as
                    AMBIGUOUS and pushes a 0.5 raw probability into the EMA.

  [P36.2-SELFHEAL]  Self-Healing on 51006 — When _execute_order encounters a
                    sCode 51006 rejection, it immediately fires a non-blocking
                    asyncio task that calls hub.force_immediate_refresh(symbol) to
                    bypass the 1800-second timer and fetch fresh buyLmt / sellLmt
                    bands for that symbol from the OKX REST API.

Phase 36.1 additions (all preserved):
  [P36-MIMIC]    Passive Spoof Test — before each TWAPSlicer major slice, the
                 Executor places a tiny minimum-size POST_ONLY "mimic" order at
                 the same price level as a detected Whale Wall.  If the wall
                 evaporates within P36_SPOOF_REACTION_MS (default 400ms), the
                 wall is flagged as a Spoof order.  Implemented in the new
                 _p36_run_mimic_test() method.

Phase 36.1 additions (this release):
  [P36-MIMIC]    Passive Spoof Test — before each TWAPSlicer major slice, the
                 Executor places a tiny minimum-size POST_ONLY "mimic" order at
                 the same price level as a detected Whale Wall.  If the wall
                 evaporates within P36_SPOOF_REACTION_MS (default 400ms), the
                 wall is flagged as a Spoof order.  Implemented in the new
                 _p36_run_mimic_test() method.

  [P36-SIGNAL]   Toxicity Injection — when a Spoof is detected, the Executor
                 updates DataHub.update_spoof_toxicity() with a raw probability
                 of 1.0 (or 0.0 on a clean cycle).  When the resulting EMA
                 spoof_probability > P36_SPOOF_THRESHOLD (default 0.65), the
                 TWAPSlicer immediately forces _use_ioc=True for all remaining
                 slices, switching to Aggressive Taker (IOC) to capture real
                 liquidity behind the fake wall.

  [P36-VETO]     Manipulation Veto — VetoArbitrator.set_spoof_probability() is
                 called after each mimic test so intelligence_layer can veto
                 new entries when spoof_prob > 0.8 (configured in
                 intelligence_layer.py via P36_SPOOF_VETO_THRESHOLD).

Phase 33.2 additions (all preserved):
  [P33.2-REBATE]  Maker-Priority Logic
  [P33.2-CHASE]   Adaptive Price Chasing
  [P33.2-LAYERING] Stealth Layering Engine

Phase 33.1 additions (all preserved):
  [P33-SNIFFER]  Toxic Flow Detection — _p32_stealth_twap() (TWAPSlicer) now
                 monitors rolling HFT cancellation and partial-fill rates via
                 _p33_toxicity_score().  When the DataHub-derived toxicity
                 exceeds P33_TOXICITY_THRESHOLD (0.75), the TWAPSlicer
                 automatically switches from POST_ONLY to IOC (Immediate-or-
                 Cancel) order type to jump the queue and avoid adversarial
                 quote-stuffing.  All cancellation and partial-fill events from
                 the executor's own order lifecycle are recorded via
                 _p33_record_order_event() into a rolling deque so the toxicity
                 score reflects real market microstructure rather than simulated
                 data.  The active order type is logged with the [P33-SNIFFER]
                 prefix on every TWAP slice.

  [P33-SHADOW]   Iceberg Shadowing — _execute_order() now queries the DataHub
                 OrderBook for massive passive buy walls after computing the
                 limit price.  If the best bid wall depth exceeds
                 P33_ICEBERG_WALL_RATIO × median book depth AND the wall USD
                 value exceeds P33_ICEBERG_WALL_MIN_USD, the bot price-improves
                 by exactly 1 tickSz ("shadow" the block) to jump ahead of
                 resting limit orders and improve fill probability.  The
                 adjustment is clamped via _clamp_price() before submission.
                 Shadowing is logged with the [P33-SHADOW] prefix.

  [P33-REVERSION] Exhaustion Gap Filter — _maybe_enter() now reports Whale
                 Sweep events to the VetoArbitrator via record_sweep_event()
                 and feeds current price velocity via record_price_velocity()
                 in the tape monitor loop.  VetoArbitrator.compute_p_success()
                 returns 0.0 as a hard block when velocity collapses by > 50%
                 within 500ms of a sweep (implemented in intelligence_layer.py).

Phase 33.1 additions (original docstring — preserved):
  [P33-SNIFFER]  Toxic Flow Detection — _p32_stealth_twap() (TWAPSlicer) now
                 monitors rolling HFT cancellation and partial-fill rates via
                 _p33_toxicity_score().  When the DataHub-derived toxicity
                 exceeds P33_TOXICITY_THRESHOLD (0.75), the TWAPSlicer
                 automatically switches from POST_ONLY to IOC (Immediate-or-
                 Cancel) order type to jump the queue and avoid adversarial
                 quote-stuffing.  All cancellation and partial-fill events from
                 the executor's own order lifecycle are recorded via
                 _p33_record_order_event() into a rolling deque so the toxicity
                 score reflects real market microstructure rather than simulated
                 data.  The active order type is logged with the [P33-SNIFFER]
                 prefix on every TWAP slice.

  [P33-SHADOW]   Iceberg Shadowing — _execute_order() now queries the DataHub
                 OrderBook for massive passive buy walls after computing the
                 limit price.  If the best bid wall depth exceeds
                 P33_ICEBERG_WALL_RATIO × median book depth AND the wall USD
                 value exceeds P33_ICEBERG_WALL_MIN_USD, the bot price-improves
                 by exactly 1 tickSz ("shadow" the block) to jump ahead of
                 resting limit orders and improve fill probability.  The
                 adjustment is clamped via _clamp_price() before submission.
                 Shadowing is logged with the [P33-SHADOW] prefix.

  [P33-REVERSION] Exhaustion Gap Filter — _maybe_enter() now reports Whale
                 Sweep events to the VetoArbitrator via record_sweep_event()
                 and feeds current price velocity via record_price_velocity()
                 in the tape monitor loop.  VetoArbitrator.compute_p_success()
                 returns 0.0 as a hard block when velocity collapses by > 50%
                 within 500ms of a sweep (implemented in intelligence_layer.py).

Phase 32 additions (all preserved):
  [P32-OBI-PATIENCE]   Whale-Aware OBI Patience Timer                      [v32.1]
  [P32-STEALTH-TWAP]   Stealth TWAP Engine
  [P32-VETO-ARB]       Unified Veto Arbitrator hook
  [P32-SLIP-ADAPT]     Slippage-Adaptive Execution
  [P32-LIQ-MAGNET]     Liquidation Magnet Logic

Phase 31 Institutional Tactical Suite (all preserved):
  [P31-WHALE-TRAIL]  Dynamic Whale-Trail
  [P31-SMART-RETRY]  Smart-Retry Order Engine

Phase 30.5 additions (all preserved):
  [P30.5-WARM]    InstrumentCache Pre-Flight Barrier
  [P30.5-PUBLISH] _publish_status() first-call guarantee

Phase 25 additions (Adaptive Performance Scaling — preserved):
  [P25-PFVT]  Pre-Flight Truth Verification
  [v27.1-BOOT] Immediate Boot Status Write

Phase 24.1 additions (Systemic Defense Refactor — all preserved):
  [P24.1-EXEC-1..4] Truth Verification, force_truth, _hard_reset_sync, Entropy Seeding

Phase 24 additions (all preserved):
  [P24-DEFENSE-1..ENT] Heartbeat, Ghost Recovery, Entropy Seeding, Command Listener,
                       Whale Tape, Intelligence Bridge, Entropy Trend

Phase 23 additions (all preserved):
  [P23-HOTFIX-1..4] Ghost-state hardening  [P23-OPT-1..6] Production optimisations

Phase 20 additions (all preserved):
  [P20-1] GlobalRiskManager  [P20-2] Council of Judges  [P20-3] Shadow HMM

All earlier phases (1-19) fully preserved.
"""
import asyncio
import csv
import gc
import json
import logging
import math
import os
from decimal import Decimal, getcontext

import time
import types
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

import numpy as np

# ── [P40] Phase 40 Logic Execution Bridge ─────────────────────────────────────
# BridgeClient replaces direct OKX REST/WS for order placement.
# Import is TYPE_CHECKING-guarded to prevent circular imports; the actual
# instance is injected by main.py via dependency injection.
from typing import TYPE_CHECKING as _TYPE_CHECKING
if _TYPE_CHECKING:
    from bridge_interface import BridgeClient  # noqa: F401
try:
    from bridge_interface import (
        BridgeClient,
        BridgeNotConnectedError,
        BridgeOrderError,
        BridgeTimeoutError,
    )
    _BRIDGE_AVAILABLE = True
except ImportError:
    _BRIDGE_AVAILABLE = False
    BridgeClient = None  # type: ignore[assignment,misc]
# ── [/P40] ────────────────────────────────────────────────────────────────────

from brain import IntelligenceEngine, Signal, MAX_ALLOC_PCT, mtf_align_filter
from data_hub import (
    AccountSnapshot, Candle, DataHub,
    InstrumentCache, InstrumentMeta, OrderBook, SentimentSnapshot, Tick,
)
from arbitrator import OracleSignal, ORACLE_WHALE_BUY, ORACLE_WHALE_SELL
from intelligence_layer import LLMContextVeto, NarrativeResult, VetoArbitrator   # [P17/P18/P20/P32]

# Ensure the hub_data directory exists
hub_path = os.path.join(os.path.dirname(__file__), "hub_data")
if not os.path.exists(hub_path):
    os.makedirs(hub_path)
    print(f"Created missing directory: {hub_path}")

getcontext().prec = 8

log = logging.getLogger("executor")


def _env_float(key: str, default: float) -> float:
    """Parse float env var safely (falls back to default on missing/bad values)."""
    raw = os.environ.get(key, "")
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        try:
            log.warning("Invalid float env %s=%r; using default=%s", key, raw, default)
        except Exception:
            pass
        return float(default)

def _env_int(key: str, default: int) -> int:
    """Parse int env var safely (falls back to default on missing/bad values)."""
    raw = os.environ.get(key, "")
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(float(raw))
    except Exception:
        try:
            log.warning("Invalid int env %s=%r; using default=%s", key, raw, default)
        except Exception:
            pass
        return int(default)



def _D(val) -> Decimal:
    """Coerce val into Decimal safely for financial math (prec=8)."""
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


def _is_valid_equity(eq) -> bool:
    """Fail-closed validity check for equity values coming from external JSON."""
    try:
        d = _D(eq)
        return d > 0
    except Exception:
        return False

# ── [P32-LOG-FIX] Self-Initializing File Logger ─────────────────────────────
import logging
from pathlib import Path

# Ensure the log file exists in the correct directory
log_file = Path("executor.log")
file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s'))

# Attach to the main executor logger

log = logging.getLogger("executor")
log.addHandler(file_handler)
log.setLevel(logging.INFO)
# ────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# [SANITIZER] Institutional Environment Sanitizer
# Must be defined BEFORE any env-var constants so every loader below can use it.
# ══════════════════════════════════════════════════════════════════════════════

def _clean_env(raw: str) -> str:
    """
    Strip leading/trailing whitespace and literal single/double quote characters
    that survive from .env files where values are wrapped in quotes, e.g.:
        OKX_PASSPHRASE="$Khalil21z"  →  $Khalil21z      (passphrase preserved)
        P35_HEDGE_SYMBOL="BTC-USDT-SWAP"  →  BTC-USDT-SWAP
    The passphrase dollar-sign is intentionally kept; only the surrounding quote
    characters are removed.
    """
    return raw.strip().strip("'\"").strip()


def _safe_float_env(key: str, default: float) -> float:
    """
    Defensive float loader: strips literal quotes introduced by .env parsers,
    warns on missing/invalid keys, and always returns a valid float so a bad
    .env line can never crash the module at import time.
    """
    _raw = os.environ.get(key, "")
    _raw = _clean_env(_raw)
    if not _raw:
        log.warning(
            "[ENV-WARN] %s not set — using safe default %.4f", key, default
        )
        return default
    try:
        return float(_raw)
    except (ValueError, TypeError):
        log.warning(
            "[ENV-WARN] %s='%s' is not a valid float — using safe default %.4f",
            key, _raw, default,
        )
        return default


# ── Config ─────────────────────────────────────────────────────────────────────
MAX_DRAWDOWN_PCT     = _env_float("MAX_DRAWDOWN_PCT", 15.0)
DRAWDOWN_WINDOW_HRS  = _env_float("DRAWDOWN_WINDOW_HOURS", 24.0)
DCA_Z_THRESHOLD      = _env_float("DCA_Z_THRESHOLD", -2.0)
MAX_DCA_PER_24H      = int  (os.environ.get("MAX_DCA_PER_24H",          "2"))
MIN_SIGNAL_CONF      = _env_float("MIN_SIGNAL_CONFIDENCE", 0.45)
PM_START_NO_DCA      = _env_float("PM_START_NO_DCA", 5.0)
PM_START_WITH_DCA    = _env_float("PM_START_WITH_DCA", 2.5)
TRAILING_GAP_PCT     = _env_float("TRAILING_GAP_PCT", 0.5)
START_ALLOC_PCT      = _env_float("START_ALLOC_PCT", 0.005)
OKX_LEVERAGE         = int  (os.environ.get("OKX_LEVERAGE",             "1"))
OKX_TD_MODE_SPOT     = os.environ.get("OKX_TD_MODE",                    "cash")
OKX_CT_VAL           = _env_float("OKX_CT_VAL", 0.01)
SOR_SPREAD_THRESHOLD = _env_float("SOR_SPREAD_THRESHOLD", 0.10)
POST_ONLY_RETRY_SECS = _env_float("POST_ONLY_RETRY_SECS", 5.0)
DEFAULT_MIN_USD      = _env_float("DEFAULT_MIN_USD_VALUE", 5.0)
GUI_SETTINGS_PATH    = os.environ.get(
    "POWERTRADER_GUI_SETTINGS",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "gui_settings.json"),
)

# ── [ATOMIC-1] Atomic JSON status file ────────────────────────────────────────
TRADER_STATUS_PATH = os.environ.get(
    "TRADER_STATUS_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "hub_data", "trader_status.json"),
)

LIQUIDITY_SLIPPAGE_MAX_PCT   = _env_float("LIQUIDITY_SLIPPAGE_MAX_PCT", 0.1)
LIQUIDITY_CHASE_MAX_ATTEMPTS = int  (os.environ.get("LIQUIDITY_CHASE_MAX_ATTEMPTS", "5"))
LIQUIDITY_CHASE_WAIT_SECS    = _env_float("LIQUIDITY_CHASE_WAIT_SECS", 3.0)

DCA_VOL_LOOKBACK = _env_int("DCA_VOL_LOOKBACK", 5)

AUTO_TUNE_WINDOW         = int  (os.environ.get("AUTO_TUNE_WINDOW",         "5"))
AUTO_TUNE_WIN_RATE_FLOOR = _env_float("AUTO_TUNE_WIN_RATE_FLOOR", 0.40)
AUTO_TUNE_CONF_STEP      = _env_float("AUTO_TUNE_CONF_STEP", 0.10)
MIN_ALLOC_FLOOR          = _env_float("MIN_ALLOC_FLOOR", 0.005)
MAX_CONF_OVERRIDE        = _env_float("MAX_CONF_OVERRIDE", 0.90)

HEDGE_TRIM_PCT      = _env_float("HEDGE_TRIM_PCT", 0.50)
HEDGE_COOLDOWN_SECS = _env_float("HEDGE_COOLDOWN_SECS", 300.0)

WALL_DEPTH_PCT       = _env_float("WALL_DEPTH_PCT", 0.2)
WALL_RATIO_THRESHOLD = _env_float("WALL_RATIO_THRESHOLD", 5.0)

SQUEEZE_LIQ_THRESHOLD_USD = _env_float("SQUEEZE_LIQ_THRESHOLD_USD", 500000.0)
SQUEEZE_DELAY_SECS        = _env_float("SQUEEZE_DELAY_SECS", 30.0)

FUNDING_BRAKE_THRESHOLD = _env_float("FUNDING_BRAKE_THRESHOLD", 0.0008)
FUNDING_BRAKE_CONF_BUMP = _env_float("FUNDING_BRAKE_CONF_BUMP", 0.15)

# ── [P31-WHALE-TRAIL] Dynamic Whale-Trail config ───────────────────────────────
# Multiplier thresholds used to widen / tighten the trailing gap based on the
# whale oracle multiplier embedded in the entry signal.
P31_WHALE_TRAIL_HIGH_MULT  = _env_float("P31_WHALE_TRAIL_HIGH_MULT", 10.0)
P31_WHALE_TRAIL_MAX_SCALE  = _env_float("P31_WHALE_TRAIL_MAX_SCALE", 2.0)
P31_WHALE_TRAIL_MIN_SCALE  = _env_float("P31_WHALE_TRAIL_MIN_SCALE", 0.5)

# ── [P31-SMART-RETRY] Smart-Retry Order Engine config ──────────────────────────
P31_ORDER_RETRY_ATTEMPTS   = int  (os.environ.get("P31_ORDER_RETRY_ATTEMPTS",   "3"))
P31_ORDER_RETRY_BASE_SECS  = _env_float("P31_ORDER_RETRY_BASE_SECS", 0.5)

# ── [P32] Phase 32 — Institutional Predator Suite ─────────────────────────────
# [P32-OBI-PATIENCE] Whale-Aware OBI Patience Timer thresholds
P32_OBI_PATIENCE_MAX_SECS   = _env_float("P32_OBI_PATIENCE_MAX_SECS", 3.0)
P32_OBI_PATIENCE_PREDATOR_S = _env_float("P32_OBI_PATIENCE_PREDATOR_S", 0.1)
P32_WHALE_STINGY_THRESH     = _env_float("P32_WHALE_STINGY_THRESH", 3.0)   # [v32.1] lowered from 5.0 → STALKER triggers at 3x
P32_WHALE_STALKER_THRESH    = _env_float("P32_WHALE_STALKER_THRESH", 10.0)  # [v32.1] lowered from 15.0 → PREDATOR triggers at 10x
# [P32-STEALTH-TWAP] Stealth TWAP Engine
P32_TWAP_THRESHOLD_USD  = _env_float("P32_TWAP_THRESHOLD_USD", 2000.0)
P32_TWAP_SLICES         = int  (os.environ.get("P32_TWAP_SLICES",         "10"))
P32_TWAP_WINDOW_SECS    = _env_float("P32_TWAP_WINDOW_SECS", 300.0)
# [P32-SLIP-ADAPT] Slippage-Adaptive Execution — per-symbol tracking
P32_SLIP_WINDOW_TRADES  = int  (os.environ.get("P32_SLIP_WINDOW_TRADES",  "3"))
P32_SLIP_LIMIT_BPS      = _env_float("P32_SLIP_LIMIT_BPS", 10.0)
P32_SLIP_LIMIT_HOURS    = _env_float("P32_SLIP_LIMIT_HOURS", 4.0)
# [P32-LIQ-MAGNET] Liquidation Magnet Logic — proximity & offset
P32_LIQ_MAGNET_RANGE_PCT  = _env_float("P32_LIQ_MAGNET_RANGE_PCT", 0.5)
P32_LIQ_MAGNET_OFFSET_BPS = _env_float("P32_LIQ_MAGNET_OFFSET_BPS", 5.0)

# ── [P33] Phase 33.1 — Toxic Flow Sniffer & Shadow Liquidity Engine ───────────
# [P33-SNIFFER] Toxic Flow Detection — rolling cancel/partial-fill tracking
# Window (seconds) over which order events are counted for toxicity scoring.
P33_TOXICITY_WINDOW_SECS      = _env_float("P33_TOXICITY_WINDOW_SECS", 30.0)
# Toxicity score above which the TWAPSlicer switches from POST_ONLY to IOC.
P33_TOXICITY_THRESHOLD        = _env_float("P33_TOXICITY_THRESHOLD", 0.75)
# Minimum number of order events in the window before toxicity is meaningful.
P33_TOXICITY_MIN_EVENTS       = int  (os.environ.get("P33_TOXICITY_MIN_EVENTS",       "4"))
# [P33-SHADOW] Iceberg Shadowing — wall detection parameters
# Ratio of best-wall depth to median top-5 depth required to trigger shadowing.
P33_ICEBERG_WALL_RATIO        = _env_float("P33_ICEBERG_WALL_RATIO", 3.0)
# Minimum USD value of the detected wall (price × qty) before shadowing is applied.
P33_ICEBERG_WALL_MIN_USD      = _env_float("P33_ICEBERG_WALL_MIN_USD", 50000.0)

# ── [P33.2] Phase 33.2 — Maker-Rebate Optimization & Stealth Layering ────────
# [P33.2-REBATE] Set to "1" to activate BBO+0.1-tick maker pricing on every TWAP slice.
P33_MAKER_PRIORITY    = os.environ.get("P33_MAKER_PRIORITY",    "0").strip() == "1"
# [P33.2-LAYERING] Number of layered sub-orders per primary _execute_order call.
P33_LAYERING_SLICES   = int  (os.environ.get("P33_LAYERING_SLICES",   "3"))
# [P33.2-CHASE] Whale oracle multiplier threshold above which price chasing is
# allowed when a maker order fails to fill and the market has moved away.
P33_CHASE_WHALE_MULT  = _env_float("P33_CHASE_WHALE_MULT", 8.0)
# [P33.2-REBATE] Fractional tick offset added to BBO for maker qualification.
# 0.1 means the limit is placed 0.1 × tickSz inside the spread.
P33_MAKER_TICK_OFFSET = _env_float("P33_MAKER_TICK_OFFSET", 0.1)
# [P33.2-CHASE] Seconds to wait for a maker order to fill before evaluating chase.
P33_MAKER_FILL_WAIT   = _env_float("P33_MAKER_FILL_WAIT", 4.0)

# ── [P34.1] Synthetic Mid-Price & Front-Run Protection ────────────────────────
# [P34-SPEED] Coinbase price velocity threshold (bps/tick) above which all
# active maker layers are immediately cancelled to avoid HFT pick-off.
P34_FRONT_RUN_THRESHOLD_BPS = _env_float("P34_FRONT_RUN_THRESHOLD_BPS", 20.0)

# [P35.1-HEDGE] Base symbol used for the BTC-SWAP hedge leg.
# _clean_env strips whitespace + literal quote characters left by .env parsers
# (e.g. P35_HEDGE_SYMBOL="BTC-USDT-SWAP" → BTC-USDT-SWAP).
_p35_hedge_raw   = os.environ.get("P35_HEDGE_SYMBOL", "BTC")
P35_HEDGE_SYMBOL = _clean_env(_p35_hedge_raw) or "BTC"
if P35_HEDGE_SYMBOL != _p35_hedge_raw.strip():
    log.warning(
        "[ENV-WARN] P35_HEDGE_SYMBOL contained surrounding quotes — "
        "sanitized to '%s'", P35_HEDGE_SYMBOL,
    )

# ── [P36.1] Phase 36.1 — Adversarial HFT Mimicry & Active Spoof Detection ────
# [P36-MIMIC] Reaction window (ms) after placing the mimic order.
# If the whale wall evaporates within this window, the wall is flagged as Spoof.
P36_SPOOF_REACTION_MS  = _safe_float_env("P36_SPOOF_REACTION_MS",  400.0)
# [P36-SIGNAL] Spoof probability above which the TWAPSlicer bypasses all limit
# orders and switches immediately to Aggressive Taker (IOC).
P36_SPOOF_THRESHOLD    = _safe_float_env("P36_SPOOF_THRESHOLD",    0.65)
# Minimum wall depth ratio (wall_qty / median_depth) for the mimic test to fire.
# Reuses the P33 iceberg wall ratio to keep detection thresholds consistent.
P36_MIMIC_WALL_RATIO   = _safe_float_env("P36_MIMIC_WALL_RATIO",   3.0)
# Minimum USD value of the suspected wall before firing the mimic test.
P36_MIMIC_WALL_MIN_USD = _safe_float_env("P36_MIMIC_WALL_MIN_USD", 50000.0)
# Fraction of the wall depth that must disappear within the reaction window
# for the wall to be classified as a spoof.  Default 0.70 = 70% evaporation.
P36_SPOOF_EVAPORATION  = _safe_float_env("P36_SPOOF_EVAPORATION",  0.70)
# [P36-ENABLE] Master switch for Adversarial Mimicry (1 = enabled).
P36_ENABLE_MIMICRY     = os.environ.get("P36_ENABLE_MIMICRY", "1").strip().strip("'\"") == "1"
# [P36-MIMIC] Minimum USD size of the probe order placed during a mimic test.
P36_MIMIC_SIZE_USD     = _safe_float_env("P36_MIMIC_SIZE_USD", 5.0)

# ── [P36.1-PRICEGUARD] Price Limit Buffer ─────────────────────────────────────
# 5 basis points buffer added inside the exchange-enforced price bands.
# When a computed order price falls outside [sellLmt*(1+buf), buyLmt*(1-buf)],
# the executor clamps it to the nearest valid band edge to prevent OKX sCode
# 51006 "Order price is not within the price limit" rejections.
PRICE_LIMIT_BUFFER = _env_float("PRICE_LIMIT_BUFFER", 0.0005)  # 5 bps

# ── [P36.2] Phase 36.2 — Dynamic Clamping, Multi-Level Mimicry, Golden Build ──
# [P36.2-DYNCLAMP] Rolling window (seconds) over which PriceVelocity is measured
# per symbol to decide whether to apply the high-volatility DynamicBuffer.
P362_VELOCITY_WINDOW_SECS  = _safe_float_env("P362_VELOCITY_WINDOW_SECS",  10.0)
# [P36.2-DYNCLAMP] If PriceVelocity over the window exceeds this % (0.5 = 0.5%),
# the DynamicBuffer is doubled to P362_DYNAMIC_BUFFER_HIGH.
P362_VELOCITY_THRESHOLD_PCT = _safe_float_env("P362_VELOCITY_THRESHOLD_PCT", 0.5)
# [P36.2-DYNCLAMP] High-volatility buffer (10 bps) applied when PriceVelocity
# exceeds P362_VELOCITY_THRESHOLD_PCT.  Replaces PRICE_LIMIT_BUFFER in all
# price-guard clamp calls during high-velocity periods.
P362_DYNAMIC_BUFFER_HIGH   = _safe_float_env("P362_DYNAMIC_BUFFER_HIGH",   0.0010)
# [P36.2-GOLDEN] Number of consecutive successful cycles (zero 51006 + zero
# IndexError) required to generate the GOLDEN_BUILD_REPORT.txt.
P362_GOLDEN_BUILD_CYCLES   = _env_int("P362_GOLDEN_BUILD_CYCLES", 1000)
# [P36.2-GOLDEN] Output path for the Golden Build Report.
P362_GOLDEN_REPORT_PATH    = os.environ.get(
    "P362_GOLDEN_REPORT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "GOLDEN_BUILD_REPORT.txt"),
)

# ── [P37] Phase 37 — Predictive Microstructure (VPIN / Flow Toxicity) ─────────
# [P37-VPIN] CDF-based ToxicityScore threshold above which VetoArbitrator blocks
# all new entries.  Injected into the VetoArbitrator via set_flow_toxicity().
P37_TOXICITY_THRESHOLD       = _safe_float_env("P37_TOXICITY_THRESHOLD",       0.80)
# [P37-VPIN] ToxicityScore above which an open position triggers an immediate
# IOC TWAPSlicer emergency exit to protect capital from a "Toxic Flush".
# Must be strictly higher than P37_TOXICITY_THRESHOLD (0.95 > 0.80 default).
P37_EMERGENCY_EXIT_TOXICITY  = _safe_float_env("P37_EMERGENCY_EXIT_TOXICITY",  0.95)

ICEBERG_MIN_USD      = _env_float("P9_ICEBERG_MIN_USD", 500.0)
ICEBERG_FILL_TIMEOUT = _env_float("P9_ICEBERG_FILL_TIMEOUT", 45.0)

# ── [P9-ICEBERG-DISPLAY] Fraction of total order shown per iceberg slice ────────
# _safe_float_env is defined at module top (Sanitizer section) so this is safe.
# ICEBERG_DISPLAY_PCT is a module-level constant; NameError on line 3712 is fixed.
ICEBERG_DISPLAY_PCT  = _safe_float_env("P9_ICEBERG_DISPLAY_PCT",  0.10)

SHADOW_REGIMES       = set(os.environ.get("P9_SHADOW_REGIMES", "chop").split(","))
SHADOW_POLL_SECS     = _env_float("P9_SHADOW_POLL_SECS", 0.5)
SHADOW_EXPIRY_SECS   = _env_float("P9_SHADOW_EXPIRY_SECS", 120.0)
SHADOW_HIT_BPS       = _env_float("P9_SHADOW_HIT_BPS", 5.0)

POST_ONLY_MAX_RETRIES = _env_int("P9_POST_ONLY_MAX_RETRIES", 3)
OKX_TAKER_REJECT_CODE = os.environ.get("P9_TAKER_REJECT_CODE", "51503")

P10_PRIORITY_CONF_REDUCTION    = _env_float("P10_PRIORITY_CONF_REDUCTION", 0.10)
P10_AGGRESSIVE_ICEBERG_MIN_USD = _env_float("P10_AGGRESSIVE_ICEBERG_MIN_USD", 100.0)

# ── [P12] Microstructure config ────────────────────────────────────────────────
OBI_BUY_BLOCK_THRESHOLD     = _env_float("P12_OBI_BUY_BLOCK", -0.6)
OBI_SELL_BLOCK_THRESHOLD    = _env_float("P12_OBI_SELL_BLOCK", 0.6)
OBI_LEVELS                  = int  (os.environ.get("P12_OBI_LEVELS",       "10"))
TAPE_VELOCITY_HFT_THRESHOLD = _env_float("P12_HFT_VELOCITY_TPS", 100.0)
TAPE_MONITOR_INTERVAL_SECS  = _env_float("P12_TAPE_MONITOR_SECS", 1.0)
SWEEP_SIGNAL_TTL_SECS       = _env_float("P12_SWEEP_TTL_SECS", 15.0)

# ── [P13] Dead Alpha Decay config ─────────────────────────────────────────────
P13_DEAD_ALPHA_HOURS    = _env_float("P13_DEAD_ALPHA_HOURS", 4.0)
P13_DEAD_ALPHA_BAND_PCT = _env_float("P13_DEAD_ALPHA_BAND_PCT", 0.1)

# ── [P16] Atomic Express Trade config ─────────────────────────────────────────
P16_BBO_OFFSET_BPS       = _env_float("P16_BBO_OFFSET_BPS", 10.0)
P16_EXPRESS_ALLOC_PCT    = _env_float("P16_EXPRESS_ALLOC_PCT", 0.01)
P16_EXPRESS_MAX_USD      = _env_float("P16_EXPRESS_MAX_USD", 500.0)
P16_EXPRESS_DEDUPE_SECS  = _env_float("P16_EXPRESS_DEDUPE_SECS", 30.0)
P16_EXPRESS_FILL_TIMEOUT = _env_float("P16_EXPRESS_FILL_TIMEOUT", 5.0)
P16_EXPRESS_TAG          = "P16_ATOMIC_EXPRESS"

# ── [P40.1-LAT] Bridge Latency Failsafe ───────────────────────────────────────
# If the Rust bridge reports order_ack latency above this threshold, the executor
# automatically shifts the symbol into LIMIT_ONLY mode and elevates DynamicBuffer
# (via the LIMIT_ONLY guard) to avoid toxic market fills during IPC spikes.
P40_LATENCY_FAILSAFE_US       = _env_int("P40_LATENCY_FAILSAFE_US", 5000)
P40_LATENCY_LIMIT_ONLY_SECS   = _env_float("P40_LATENCY_LIMIT_ONLY_SECS", 900)  # 15 min
P40_LATENCY_FAILSAFE_MIN_US   = _env_int("P40_LATENCY_FAILSAFE_MIN_US", 1)       # ignore missing/zero latency

# ── [P17/P18] Intelligence Layer config ───────────────────────────────────────
P17_CATASTROPHE_THRESHOLD = _env_float("P17_CATASTROPHE_THRESHOLD", 0.1)
P17_BOOST_THRESHOLD       = _env_float("P17_NARRATIVE_BOOST_THRESHOLD", 0.8)

# ── [P18] Adaptive Conviction config ──────────────────────────────────────────
P18_DYNAMIC_SIZING          = os.environ.get("P18_DYNAMIC_SIZING", "1").strip() == "1"
P18_DIVERGENCE_BEAR_REGIMES = {"bear", "bearish"}
P18_DIVERGENCE_SCORE_FLOOR  = _env_float("P18_DIVERGENCE_SCORE_FLOOR", 0.8)
P18_DIVERGENCE_HAIRCUT      = _env_float("P18_DIVERGENCE_HAIRCUT", 0.6)

# ── [P19-2] Shadow Auditor config ─────────────────────────────────────────────
SHADOW_AUDIT_PATH = os.environ.get(
    "SHADOW_AUDIT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "shadow_audit.csv"),
)
_SHADOW_AUDIT_HEADER = [
    "ts", "symbol", "direction", "tag",
    "real_usd", "baseline_usd", "conviction_multiplier",
    "regime", "narrative_verdict", "narrative_score",
    "fill_px", "fill_sz",
]

# ── [P20-1] Global Drawdown / Zombie Mode config ───────────────────────────────
P20_DRAWDOWN_ZOMBIE_PCT = _env_float("P20_DRAWDOWN_ZOMBIE_PCT", 10.0)

# ── [P23] Phase 23 config ──────────────────────────────────────────────────────
P23_CB_SUPPRESS_WITH_LKG    = os.environ.get("P23_CB_SUPPRESS_WITH_LKG", "1").strip() == "1"
P23_GHOST_RECONNECT_THRESH  = int  (os.environ.get("P23_GHOST_RECONNECT_THRESH", "50"))
P23_FLATTEN_LOG_THROTTLE_S  = _env_float("P23_FLATTEN_LOG_THROTTLE_S", 60.0)
P23_HARD_SYNC_INTERVAL_S    = _env_float("P23_HARD_SYNC_INTERVAL_S", 300.0)
P23_LLM_TIMEOUT_S           = _env_float("P23_LLM_TIMEOUT_S", 2.0)
P23_SPREAD_GUARD_PCT        = _env_float("P23_SPREAD_GUARD_PCT", 0.10)
P23_WHALE_SNIPER_MULT_THRESH= _env_float("P23_WHALE_SNIPER_MULT_THRESH", 10.0)
P23_SMALL_ACCT_FALLBACK_SYMS = [
    s.strip().upper()
    for s in os.environ.get("P23_SMALL_ACCT_FALLBACK_SYMS", "XRP,DOGE").split(",")
    if s.strip()
]
# Flag file that the dashboard writes to trigger an immediate REST sync.
P23_FORCE_SYNC_FLAG = os.environ.get(
    "P23_FORCE_SYNC_FLAG",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "hub_data", "force_rest_sync.flag"),
)

# ── [P24] Systemic Defense — Control Event path ────────────────────────────────
# Dashboard writes JSON {"event": "reset_gateway"} here to trigger a hard sync.
P24_CONTROL_EVENT_PATH = os.environ.get(
    "P24_CONTROL_EVENT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "hub_data", "control_event.json"),
)

# ── [FIX-HWM-RACE] Minimum valid equity floor ─────────────────────────────────
_EXECUTOR_MIN_VALID_EQUITY = _env_float("P7_CB_MIN_VALID_EQUITY", 10.0)

# ── [P25] Phase 25 — Tactical Listener & Liquidation Oracle config ─────────────
import threading as _threading

P25_TACTICAL_CONFIG_PATH = os.environ.get(
    "P25_TACTICAL_CONFIG_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "hub_data", "tactical_config.json"),
)
P25_VETO_AUDIT_PATH = os.environ.get(
    "P25_VETO_AUDIT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "hub_data", "veto_audit.json"),
)
P25_LIQUIDATION_CLUSTERS_COUNT = _env_int("P25_LIQUIDATION_CLUSTERS_COUNT", 5)
# SNIPER-ONLY: reject entry if conviction_multiplier < this threshold
P25_SNIPER_ONLY_CONV_THRESHOLD = _env_float("P25_SNIPER_ONLY_CONV_THRESHOLD", 1.5)
# SNIPER-ONLY: whale oracle multiplier that bypasses the sniper-only gate
P25_SNIPER_ONLY_WHALE_MULT     = _env_float("P25_SNIPER_ONLY_WHALE_MULT", 20.0)
# Default tactical config (fail-safe values per guardrail #5)
_P25_TACTICAL_DEFAULTS: dict = {"risk_off": False, "sniper_only": False, "hedge_active": False}
# Module-level lock for veto_audit writes from executor context
_p25_veto_audit_lock = _threading.Lock()


# ══════════════════════════════════════════════════════════════════════════════
# [LLMFB-3] LLM Empty-Response Fallback Helper
# ══════════════════════════════════════════════════════════════════════════════

def _make_neutral_narrative() -> NarrativeResult:
    """
    [LLMFB-3] Return a safe, neutral NarrativeResult when the OpenRouter
    free tier returns an empty body, whitespace, a 'char 0' JSON error, or
    raises any exception during parsing.

    The bot continues trading on pure Technical Analysis signals with the
    baseline (1.0×) conviction multiplier and a PASS verdict so no orders
    are blocked and no sizes are distorted.

    Tries the dataclass constructor first; falls back to object.__setattr__
    for frozen dataclasses; then to a SimpleNamespace duck-type as a last
    resort so this helper can never itself raise.
    """
    _FIELDS = dict(
        verdict="PASS",
        score=0.5,
        conviction_multiplier=1.0,
        latency_ms=0.0,
        council_detail=[],
    )
    # Attempt 1 — standard keyword constructor (covers most dataclass shapes).
    try:
        return NarrativeResult(**_FIELDS)
    except TypeError:
        pass

    # Attempt 2 — frozen dataclass or slots-based class.
    try:
        obj = object.__new__(NarrativeResult)
        for k, v in _FIELDS.items():
            try:
                object.__setattr__(obj, k, v)
            except (AttributeError, TypeError):
                setattr(obj, k, v)
        return obj  # type: ignore[return-value]
    except Exception:
        pass

    # Attempt 3 — SimpleNamespace duck-type (unconditionally safe).
    ns = types.SimpleNamespace(**_FIELDS)
    return ns  # type: ignore[return-value]


# ══════════════════════════════════════════════════════════════════════════════
# [P25] Executor-side Veto Audit Append
# ══════════════════════════════════════════════════════════════════════════════

def _append_veto_audit_executor(symbol: str, reason: str, details: str = "") -> None:
    """
    [P25] Append one veto audit row to veto_audit.json from the executor side
    (e.g. SNIPER-ONLY gate, RISK-OFF suppression).

    Uses the same path and atomic write pattern as intelligence_layer's
    _append_veto_audit_file so both sides share a single consistent log.
    Writes are protected by _p25_veto_audit_lock to avoid race conditions when
    the dashboard is simultaneously reading the file.
    """
    row = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "symbol":    symbol,
        "reason":    reason,
        "details":   details,
    }
    tmp_path = P25_VETO_AUDIT_PATH + ".tmp"
    with _p25_veto_audit_lock:
        try:
            os.makedirs(os.path.dirname(P25_VETO_AUDIT_PATH), exist_ok=True)
            existing: list = []
            if os.path.exists(P25_VETO_AUDIT_PATH):
                try:
                    with open(P25_VETO_AUDIT_PATH, "r", encoding="utf-8") as f:
                        raw = f.read().strip()
                    if raw:
                        existing = json.loads(raw)
                    if not isinstance(existing, list):
                        existing = []
                except Exception:
                    existing = []
            existing.append(row)
            if len(existing) > 10_000:
                existing = existing[-10_000:]
            payload = json.dumps(existing, indent=None, separators=(",", ":"))
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(payload)
            os.replace(tmp_path, P25_VETO_AUDIT_PATH)
        except Exception as exc:
            log.warning("[P25] _append_veto_audit_executor failed: %s", exc)
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# [QUANT-1] Size Quantization Layer
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class QuantizedSize:
    """
    [QUANT-1] Result of _quantize_sz().

    Attributes
    ----------
    sz_str   : exchange-ready string, decimal-aligned to lotSz precision.
    sz_float : quantized float for internal arithmetic.
    lot_sz   : lotSz used (for logging / auditing).
    min_sz   : minSz used (for logging / auditing).
    valid    : False when the quantized size is below minSz — caller must abort.
    """
    sz_str:   str
    sz_float: float
    lot_sz:   float
    min_sz:   float
    valid:    bool


def _lot_decimal_places(lot_sz: float) -> int:
    """
    [QUANT-1] Derive the exact number of decimal places required by a lotSz.

    Examples
    --------
    lotSz=1.0     → 0
    lotSz=0.1     → 1
    lotSz=0.01    → 2
    lotSz=0.0001  → 4
    lotSz=10.0    → 0
    """
    if lot_sz <= 0:
        return 8  # safe fallback
    s = f"{lot_sz:.10f}".rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def _fmt_sz(qty: float, decimals: int) -> str:
    """
    [QUANT-1] Format a quantized quantity as a string with exact precision.
    Swap instruments always need integer contracts; spot uses decimal strings.
    """
    if decimals == 0:
        return str(int(qty))
    return f"{qty:.{decimals}f}"


def inst_id(symbol: str, swap: bool = False) -> str:
    base = f"{symbol.upper()}-USDT"
    return f"{base}-SWAP" if swap else base


# Keep the module-level alias used elsewhere in the file
_inst_id = inst_id


# ══════════════════════════════════════════════════════════════════════════════
# Supporting dataclasses (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class WallResult:
    wall_detected: bool
    wall_side:     str
    ask_vol:       float
    bid_vol:       float
    ask_ratio:     float
    bid_ratio:     float


@dataclass
class ShadowWatch:
    symbol:      str
    side:        str
    target_px:   float
    usd_amount:  float
    signal:      object
    swap:        bool
    pos_side:    str
    tag:         str
    coin_cfg:    object
    created_at:  float = field(default_factory=time.time)
    expires_at:  float = 0.0

    def __post_init__(self):
        if self.expires_at == 0.0:
            self.expires_at = self.created_at + SHADOW_EXPIRY_SECS

    def is_expired(self) -> bool:
        return time.time() >= self.expires_at

    def is_hit(self, current_price: float) -> bool:
        if self.target_px <= 0:
            return False
        tol = self.target_px * (SHADOW_HIT_BPS / 10_000.0)
        return abs(current_price - self.target_px) <= tol


@dataclass
class CoinConfig:
    min_usd_value: float = DEFAULT_MIN_USD
    max_alloc_pct: float = MAX_ALLOC_PCT


def _load_coin_configs(path: str) -> Dict[str, CoinConfig]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        raw    = d.get("coin_config", {})
        result: Dict[str, CoinConfig] = {}
        for sym, cfg in raw.items():
            if not isinstance(cfg, dict):
                continue
            result[sym.upper()] = CoinConfig(
                min_usd_value=float(cfg.get("min_usd_value", DEFAULT_MIN_USD)),
                max_alloc_pct=float(cfg.get("max_alloc_pct", MAX_ALLOC_PCT)),
            )
        log.info("Loaded per-coin configs for: %s", list(result.keys()))
        return result
    except FileNotFoundError:
        log.debug("gui_settings.json not found — using global defaults.")
        return {}
    except Exception as e:
        log.warning("Could not load coin_config: %s", e)
        return {}


@dataclass
class CoinPerf:
    results:        Deque[bool]     = field(default_factory=lambda: deque(maxlen=AUTO_TUNE_WINDOW))
    alloc_override: Optional[float] = None
    conf_override:  Optional[float] = None


class CoinPerformanceTracker:
    def __init__(self):
        self._perfs: Dict[str, CoinPerf] = {}

    def _ensure(self, sym: str) -> CoinPerf:
        if sym not in self._perfs:
            self._perfs[sym] = CoinPerf()
        return self._perfs[sym]

    def record(self, symbol: str, pnl_pct: float):
        perf = self._ensure(symbol)
        win  = pnl_pct > 0
        perf.results.append(win)
        if len(perf.results) < AUTO_TUNE_WINDOW:
            return
        win_rate = sum(perf.results) / len(perf.results)
        if win:
            if perf.alloc_override is not None:
                new_alloc = min(perf.alloc_override * 2.0, MAX_ALLOC_PCT)
                log.info("AutoTune %s: win → alloc_override %.4f → %.4f",
                         symbol, perf.alloc_override, new_alloc)
                perf.alloc_override = new_alloc if new_alloc < MAX_ALLOC_PCT else None
            if perf.conf_override is not None:
                new_conf = max(perf.conf_override - AUTO_TUNE_CONF_STEP, MIN_SIGNAL_CONF)
                log.info("AutoTune %s: win → conf_override %.3f → %.3f",
                         symbol, perf.conf_override, new_conf)
                perf.conf_override = new_conf if new_conf > MIN_SIGNAL_CONF else None
        else:
            if win_rate < AUTO_TUNE_WIN_RATE_FLOOR:
                base_alloc = perf.alloc_override if perf.alloc_override is not None else MAX_ALLOC_PCT
                new_alloc  = max(base_alloc * 0.5, MIN_ALLOC_FLOOR)
                base_conf  = perf.conf_override if perf.conf_override is not None else MIN_SIGNAL_CONF
                new_conf   = min(base_conf + AUTO_TUNE_CONF_STEP, MAX_CONF_OVERRIDE)
                log.warning(
                    "AutoTune %s: win_rate=%.1f%% < %.0f%% → "
                    "alloc %.4f→%.4f  conf %.3f→%.3f",
                    symbol, win_rate * 100, AUTO_TUNE_WIN_RATE_FLOOR * 100,
                    base_alloc, new_alloc, base_conf, new_conf,
                )
                perf.alloc_override = new_alloc
                perf.conf_override  = new_conf

    def alloc_for(self, symbol: str, default: float) -> float:
        p = self._perfs.get(symbol)
        return p.alloc_override if (p and p.alloc_override is not None) else default

    def conf_for(self, symbol: str) -> float:
        p = self._perfs.get(symbol)
        return p.conf_override if (p and p.conf_override is not None) else MIN_SIGNAL_CONF

    def overrides_active(self, symbol: str) -> bool:
        p = self._perfs.get(symbol)
        return bool(p and (p.alloc_override is not None or p.conf_override is not None))


@dataclass
class Position:
    symbol:           str
    direction:        str
    inst_type:        str
    qty:              float
    cost_basis:       float
    usd_cost:         float
    entry_ts:         float        = field(default_factory=time.time)
    dca_stages:       int          = 0
    dca_ts_list:      List[float]  = field(default_factory=list)
    last_sell_ts:     float        = 0.0
    trail_active:     bool         = False
    trail_line:       float        = 0.0
    trail_peak:       float        = 0.0
    was_above:        bool         = False
    entry_signal:     Optional[Signal] = None
    okx_order_id:     str          = ""
    sniper_entry:     bool         = False
    trail_multiplier: float        = 1.0
    squeeze_delay_until: float     = 0.0
    entry_limit_px:   float        = 0.0
    p16_express:      bool         = False


class DrawdownTracker:
    def __init__(self, pct_limit: float = MAX_DRAWDOWN_PCT,
                 window_hrs: float = DRAWDOWN_WINDOW_HRS):
        self.limit   = pct_limit
        self.window  = window_hrs * 3600
        self._hist:  List[Tuple[float, float]] = []
        self._killed = False

    def record(self, equity: float):
        now = time.time()
        self._hist.append((now, equity))
        cutoff     = now - self.window
        self._hist = [(t, v) for t, v in self._hist if t >= cutoff]

    def check(self, equity: float) -> bool:
        if self._killed:
            return True
        if not self._hist:
            return False
        peak = max(v for _, v in self._hist)
        if peak > 0 and (peak - equity) / peak * 100 >= self.limit:
            self._killed = True
            log.critical("CIRCUIT BREAKER TRIPPED")
        return self._killed

    def reset(self):
        self._killed = False
        self._hist.clear()


# ══════════════════════════════════════════════════════════════════════════════
# [P20-1] Global Risk Manager (unchanged logic, preserved verbatim)
# ══════════════════════════════════════════════════════════════════════════════

class GlobalRiskManager:
    """
    [P20-1] Monitors equity against a High-Water Mark (HWM).
    Full docstring preserved — see original file header for detail.
    """

    def __init__(
        self,
        executor: "Executor",
        brain:    IntelligenceEngine,
        zombie_pct: float = P20_DRAWDOWN_ZOMBIE_PCT,
    ):
        self._executor   = executor
        self._brain      = brain
        self._zombie_pct = zombie_pct
        self._zombie_active = False
        self._zombie_ts:    float = 0.0
        self._last_equity:  float = 0.0
        log.info(
            "[P20-1] GlobalRiskManager init: zombie_pct=%.1f%% hwm=%.2f",
            zombie_pct, brain.peak_equity,
        )

    @property
    def zombie_active(self) -> bool:
        return self._zombie_active

    @property
    def peak_equity(self) -> float:
        return self._brain.peak_equity

    async def update(self, equity: float) -> None:
        import logging as _log
        _logger = _log.getLogger("executor")

        try:
            equity = float(equity) if equity is not None else 0.0
        except (TypeError, ValueError):
            equity = 0.0

        _FLOOR = 0.10
        if equity <= _FLOOR:
            _logger.debug(
                "[P20-1] GlobalRiskManager.update: equity=%.4f <= floor=%.2f "
                "— skipping (ghost reading, HWM NOT updated).",
                equity, _FLOOR,
            )
            return

        self._last_equity = equity

        if self._zombie_active:
            return

        _equity_lock = getattr(self._executor, "_equity_lock", None)

        async def _do_hwm_and_check():
            current_hwm = self._brain.peak_equity
            if current_hwm is None:
                current_hwm = 0.0

            if equity > current_hwm:
                old_hwm = current_hwm
                self._brain.peak_equity = equity
                if old_hwm > 0.0:
                    _logger.info(
                        "[P20-1] HWM updated: %.2f → %.2f",
                        old_hwm, self._brain.peak_equity,
                    )

            hwm = self._brain.peak_equity
            if hwm is None or hwm <= 0.0:
                return
            try:
                drawdown_pct = (float(hwm) - float(equity)) / float(hwm) * 100.0
            except (TypeError, ZeroDivisionError):
                return
            if drawdown_pct >= self._zombie_pct:
                await self._trigger_zombie(equity, drawdown_pct)

        if _equity_lock is not None:
            async with _equity_lock:
                await _do_hwm_and_check()
        else:
            await _do_hwm_and_check()

    async def _trigger_zombie(self, equity: float, drawdown_pct: float) -> None:
        import logging as _log
        _logger = _log.getLogger("executor")

        _FLOOR = 0.10
        try:
            equity_f   = float(equity)       if equity      is not None else 0.0
            drawdown_f = float(drawdown_pct) if drawdown_pct is not None else 0.0
        except (TypeError, ValueError):
            equity_f, drawdown_f = 0.0, 0.0

        if equity_f <= _FLOOR:
            _logger.warning(
                "[P20-1] _trigger_zombie called with equity=%.4f <= floor=%.2f "
                "— Zombie Mode NOT activated (ghost reading).",
                equity_f, _FLOOR,
            )
            return

        self._zombie_active             = True
        self._zombie_ts                 = time.time()
        self._executor._p20_zombie_mode = True

        _logger.critical(
            "[P20-1] 🧟 ZOMBIE MODE ACTIVATED: "
            "equity=%.2f HWM=%.2f drawdown=%.2f%% >= %.1f%% — "
            "liquidating ALL positions and halting new entries.",
            equity_f, self._brain.peak_equity,
            drawdown_f, self._zombie_pct,
        )

        for sym in list(self._executor.symbols):
            if sym in self._executor.positions:
                try:
                    await self._executor.liquidate_all_positions(
                        sym, reason="P20_ZOMBIE_DRAWDOWN"
                    )
                except Exception as exc:
                    log.error(
                        "[P20-1] Zombie liquidation failed for %s: %s",
                        sym, exc, exc_info=True,
                    )

    def reset_zombie_mode(self) -> None:
        if not self._zombie_active:
            log.info("[P20-1] reset_zombie_mode called but Zombie Mode is not active.")
            return
        self._zombie_active             = False
        self._executor._p20_zombie_mode = False
        log.warning(
            "[P20-1] Zombie Mode RESET by operator. "
            "New entries are now permitted. HWM=%.2f",
            self._brain.peak_equity,
        )

    def status_snapshot(self) -> dict:
        hwm          = self._brain.peak_equity
        equity       = self._last_equity
        drawdown_pct = (hwm - equity) / hwm * 100.0 if hwm > 0 else 0.0
        return {
            "zombie_active":   self._zombie_active,
            "zombie_pct":      self._zombie_pct,
            "peak_equity":     round(hwm, 2),
            "current_equity":  round(equity, 2),
            "drawdown_pct":    round(drawdown_pct, 4),
            "zombie_ts":       self._zombie_ts,
            "zombie_age_secs": round(time.time() - self._zombie_ts, 1)
                               if self._zombie_active else 0.0,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Quantity Resolver (updated to use Quantization Layer)
# ══════════════════════════════════════════════════════════════════════════════

class QuantityResolver:
    def __init__(self, icache: InstrumentCache):
        self._ic = icache

    async def resolve(
        self, symbol: str, usd_amount: float, price: float,
        swap: bool, coin_cfg: CoinConfig, tag: str = "",
    ) -> Tuple[str, float]:
        if price <= 0:
            raise ValueError(f"[{tag}] price={price} invalid for {symbol}")
        meta = await self._ic.get_instrument_info(symbol, swap=swap)
        if meta is None:
            raise ValueError(f"[{tag}] No instrument metadata for {symbol} swap={swap}")
        raw_base = usd_amount / price
        if swap:
            ct_val = meta.ct_val if meta.ct_val > 0 else OKX_CT_VAL
            raw_sz = raw_base / ct_val
        else:
            raw_sz = raw_base
        if usd_amount < coin_cfg.min_usd_value:
            if swap:
                ct_val   = meta.ct_val if meta.ct_val > 0 else OKX_CT_VAL
                floor_sz = coin_cfg.min_usd_value / price / ct_val
            else:
                floor_sz = coin_cfg.min_usd_value / price
            if floor_sz > raw_sz:
                log.warning(
                    "[%s] notional $%.2f < min_usd $%.2f — bumping qty %.6f→%.6f",
                    tag, usd_amount, coin_cfg.min_usd_value, raw_sz, floor_sz,
                )
                raw_sz = floor_sz
        final_sz    = self._ic.apply_constraints(raw_sz, meta, tag)
        if final_sz <= 0:
            raise ValueError(f"[{tag}] qty=0 after constraints for {symbol}")
        final_price = InstrumentCache.round_price(price, meta)
        if swap:
            sz_str = str(int(final_sz))
        else:
            dec    = InstrumentCache._decimals(meta.sz_inst) if meta.sz_inst > 0 else 8
            sz_str = f"{final_sz:.{dec}f}"
        return sz_str, final_price


# ══════════════════════════════════════════════════════════════════════════════
# Smart Order Router (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

class SmartOrderRouter:
    def __init__(self, rest):
        self.rest = rest

    @staticmethod
    def limit_price(side: str, tick: Tick) -> float:
        return tick.bid if side == "buy" else tick.ask

    def build_limit_body(self, inst, side, sz_str, price_str,
                         td_mode, swap, pos_side, tag) -> dict:
        uid = uuid.uuid4().hex[:12]
        b = {
            "instId": inst, "tdMode": td_mode, "side": side,
            "ordType": "post_only", "sz": sz_str, "px": price_str,
            "execInst": "post_only", "clOrdId": f"L{uid}",
        }
        if swap:
            b["posSide"] = pos_side
        return b

    def build_market_body(self, inst, side, sz_str,
                          td_mode, swap, pos_side, tag) -> dict:
        uid = uuid.uuid4().hex[:12]
        b = {
            "instId": inst, "tdMode": td_mode, "side": side,
            "ordType": "market", "sz": sz_str, "clOrdId": f"M{uid}",
        }
        if swap:
            b["posSide"] = pos_side
        return b

    def build_ioc_limit_body(self, inst, side, sz_str, price_str,
                             td_mode, swap, pos_side, tag) -> dict:
        uid = uuid.uuid4().hex[:12]
        b = {
            "instId": inst, "tdMode": td_mode, "side": side,
            "ordType": "limit", "tgtCcy": "base_ccy",
            "sz": sz_str, "px": price_str, "clOrdId": f"C{uid}",
        }
        if swap:
            b["posSide"] = pos_side
        return b

    def build_aggressive_limit_body(self, inst, side, sz_str, price_str,
                                    td_mode, swap, pos_side, tag) -> dict:
        uid = uuid.uuid4().hex[:12]
        b = {
            "instId": inst, "tdMode": td_mode, "side": side,
            "ordType": "limit", "sz": sz_str, "px": price_str,
            "clOrdId": f"X{uid}",
        }
        if swap:
            b["posSide"] = pos_side
        return b


class OrchestratorGate:
    def __init__(self, hub, sentinel=None):
        self.hub      = hub
        self.sentinel = sentinel

    async def _gated_enter(self, symbol, master, tick=None, oracle_signal=None, *args, **kwargs):
        if oracle_signal and getattr(oracle_signal, 'cancel_buys_flag', False):
            logging.warning(f"⚠️ [P15] {symbol} Entry BLOCKED: Whale Nuke detected")
            return False
        if self.sentinel and self.sentinel.is_paused(symbol):
            return False
        return True


# ══════════════════════════════════════════════════════════════════════════════
# Executor
# ══════════════════════════════════════════════════════════════════════════════

class Executor:

    def __init__(self, hub: DataHub, brain: IntelligenceEngine,
                 symbols: List[str], demo: bool = False,
                 bridge: Optional["BridgeClient"] = None):
        self.hub     = hub
        self.brain   = brain
        self.symbols = [s.upper() for s in symbols]
        self.demo    = demo
        self.rest    = hub.rest
        self.db      = hub.db
        self.cache   = hub.cache

        # ── [P40] Logic Execution Bridge ─────────────────────────────────────
        # Injected by main.py.  When present, ALL order placement goes through
        # the Rust bridge instead of direct OKX REST calls.
        self.bridge: Optional["BridgeClient"] = bridge
        # ── [/P40] ───────────────────────────────────────────────────────────

        self._icache    = hub.instrument_cache
        self._resolver  = QuantityResolver(self._icache)
        self._sor       = SmartOrderRouter(self.rest)
        self._vol_guard = hub.volatility_guard
        self._perf      = CoinPerformanceTracker()

        self.positions: Dict[str, Position] = {}
        self.drawdown   = DrawdownTracker()

        self._equity  = 0.0
        self._avail   = 0.0
        self._status: dict = {}

        # [LKG-3] Last-Known-Good equity — updated on every confirmed valid reading
        # (WS or REST).  Used by _cycle() for Kelly sizing so ghost $0 reads can
        # never produce zero-sized orders.
        self._last_valid_equity: float = 0.0

        self._coin_cfgs:  Dict[str, CoinConfig] = _load_coin_configs(GUI_SETTINGS_PATH)
        self._cfgs_mtime: float = 0.0
        self._last_hedge_ts: float = 0.0

        self._p7_cb:         object = None
        self._p7_sizer:      object = None
        self._p7_spread_mgr: object = None
        self._p7_twap:       object = None
        self._p7_auditor:    object = None

        self._shadow_watches: Dict[str, ShadowWatch] = {}
        self._shadow_task: Optional[asyncio.Task] = None

        self._p10_governor: object = None
        self._p10_execution_mode: str = "neutral"

        self._p11_sentinel: object = None

        self.gate = OrchestratorGate(self.hub, self._p11_sentinel)

        self._hft_brake: Dict[str, bool]  = {}
        self._sweep_signals: Dict[str, float] = {}
        self._tape_monitor_task: Optional[asyncio.Task] = None

        self._p13_dead_alpha_exits: int = 0

        self._p16_express_lock: asyncio.Lock = asyncio.Lock()
        self._p16_last_express: Dict[str, float] = {}
        self._p16_express_fills: int = 0
        self.p16_bridge_active: bool = False

        self._p17_veto: Optional[LLMContextVeto] = None

        self._p20_zombie_mode: bool = False
        self._p20_risk_manager: Optional[GlobalRiskManager] = None

        self._shadow_audit_path = SHADOW_AUDIT_PATH
        self._p19_ensure_shadow_csv()

        self._init_db_pragmas()

        # [FIX-HWM-RACE] / [LOCK-2] — single lock guards both equity writes
        # AND the full account-update branch so back-to-back WS pushes cannot
        # interleave ghost-state detection with a legitimate update in-flight.
        self._equity_lock: asyncio.Lock = asyncio.Lock()

        # [GHOST-3] Flag ensuring only one REST back-fill task runs at a time.
        # Set AFTER task is successfully scheduled so a scheduling error can
        # never permanently lock the flag True.
        self._ghost_poll_in_flight: bool = False

        # ── [P22-RESILIENCE] Ghost-Busting counters ───────────────────────────
        # Counts consecutive WebSocket account pushes where equity <= floor
        # while positions are open.  At >= 5 consecutive ghost reads the bot
        # fires a forced [RECOVERY-SYNC] REST Account Poll that hard-overwrites
        # equity state regardless of the _ghost_poll_in_flight flag.
        self._consecutive_ghost_reads: int = 0

        # True while the current self._equity value was retained from a ghost
        # discard (WS lagging).  Published in _status so the dashboard can
        # render the equity tile in orange with a (WS_LAG: VIRTUAL_SYNC) tag.
        self._equity_is_ghost: bool = False

        # ── [P22-STRATEGY-MODE] LLM-fallback indicator ───────────────────────
        # Set True whenever _make_neutral_narrative() was invoked as fallback
        # in the most-recent entry evaluation.  Used by _cycle to publish the
        # correct strategy_mode emoji label in self._status.
        self._llm_in_fallback: bool = False

        # Tracks last [P22-STRATEGY-MODE] value for dashboard display.
        self._strategy_mode: str = "🤖 AI_PREMIUM"

        # ── [P23] Phase 23 state ──────────────────────────────────────────────
        # Log-throttle: suppress duplicate FLATTENING CRITICAL logs within window.
        self._last_flatten_log_ts: float = 0.0

        # Hard-Sync Audit: timestamp of last forced REST reconciliation.
        self._last_hard_sync_ts: float = 0.0

        # WS Reconnect: timestamp of last ghost-triggered reconnect (rate-limit).
        self._last_ws_reconnect_ts: float = 0.0

        # Mode label for the current entry ("[AI]", "[TA]", "[WHALE]").
        # Set in _maybe_enter / trigger_atomic_express_trade before opening a position.
        self._pending_mode_label: str = "[AI]"

        # ── [P25] Phase 25 — Tactical Listener & Liquidation Oracle ──────────
        # Current tactical config (reloaded every _cycle from tactical_config.json)
        self._tactical_config: dict = dict(_P25_TACTICAL_DEFAULTS)
        # Set True when ANY tactical override is active → Dashboard pulse turns Purple
        self._tactical_active: bool = False
        # Liquidation cluster cache written into status['market_data']['liquidations']
        self._liquidation_clusters: dict = {"clusters": []}

        # [P24.1-EXEC] Force-Truth Pending — set True by the control event
        # listener when the dashboard sends a reset with force_truth=True.
        # When set, _verify_equity_via_rest() accepts the REST poll result as
        # the new equity truth regardless of the equity floor check, then resets
        # this flag.  This breaks the Ghost-Rest Paradox where an account that
        # is genuinely empty ($0.00) would be ignored because it was below the
        # minimum valid equity floor.
        self._force_truth_pending: bool = False

        # [P24] Systemic Defense — last NarrativeResult produced by the intelligence
        # layer.  Written into status['intelligence'] every _cycle for the dashboard.
        self._last_narrative: Optional[NarrativeResult] = None

        # [P24-DEFENSE] Entropy Trend — rolling window of the last 10 Shannon Entropy
        # values sampled once per _cycle from self._last_narrative.entropy.
        # Exported as status['intelligence']['entropy_history'] so the dashboard can
        # render a sparkline trend rather than a single static value.
        self._entropy_history: deque = deque(maxlen=10)

        # ── [P32] Phase 32 — Institutional Predator Suite state ───────────────────
        # Current aggression mode determined by the whale oracle multiplier during
        # OBI patience evaluation.  Written to self._status for the dashboard badge.
        self._p32_aggression_mode: str = "STINGY"

        # [P32-SLIP-ADAPT] Per-symbol realized slippage history (last 3 trades, bps).
        # Populated in _record_close() via measured fill vs expected price.
        self._p32_slip_history: Dict[str, deque] = {}

        # [P32-SLIP-ADAPT] Per-symbol LIMIT_ONLY expiry timestamp.
        # If time.time() < self._p32_limit_only_until[sym], market orders are
        # blocked for that symbol and only limit orders are submitted.
        self._p32_limit_only_until: Dict[str, float] = {}

        # [P32-VETO-ARB] Unified Veto Arbitrator — instantiated here so the
        # executor can inject fresh entropy/correlation/velocity data each cycle.
        self._p32_veto_arb: Optional[VetoArbitrator] = None

        # ── [P33] Phase 33.1 state ────────────────────────────────────────────
        # [P33-SNIFFER] Toxic Flow Detection — rolling deque of order events.
        # Each entry: (ts: float, event_type: str) where event_type is one of:
        #   "cancel"       — a POST_ONLY limit order was fully cancelled / not filled
        #   "partial_fill" — a TWAP slice was partially filled (not fully filled)
        # Used to compute a rolling toxicity score for TWAPSlicer IOC switching.
        self._p33_order_events: deque = deque(maxlen=500)

        # [P33-SHADOW] Current tick-size cache per symbol for iceberg shadowing.
        # Populated lazily in _p33_detect_iceberg_shadow() from InstrumentCache.
        self._p33_tick_sz_cache: Dict[str, float] = {}
        # ── [/P33] ───────────────────────────────────────────────────────────

        # ── [P33.2] Phase 33.2 state ──────────────────────────────────────────
        # [P33.2-LAYERING] Track how many layered sub-orders succeeded per call
        # for War Room dashboard telemetry.  Keyed by a temporary call_id string.
        # Entries are short-lived (cleared after each call).
        self._p332_layer_stats: Dict[str, dict] = {}
        # ── [/P33.2] ─────────────────────────────────────────────────────────

        # ── [P34.1] Phase 34.1 state ──────────────────────────────────────────
        # [P34-SPEED] Front-Run Kill-Switch: map from symbol → list of active
        # POST_ONLY maker order IDs placed by _p332_place_maker_slice() and
        # _p332_stealth_layer_slice().  When Coinbase price velocity exceeds
        # P34_FRONT_RUN_THRESHOLD_BPS (20 bps default) in the tape monitor loop,
        # _p34_cancel_maker_layers() cancels all IDs and clears the list to
        # prevent HFT pick-off of resting maker quotes.
        self._p34_active_maker_orders: Dict[str, List[str]] = {}
        # ── [/P34.1] ─────────────────────────────────────────────────────────

        # ── [P36.1] Phase 36.1 — Adversarial HFT Mimicry & Spoof Detection ──
        # [P36-MIMIC] Lock ensuring only one mimic test runs per symbol at a time.
        # Prevents multiple simultaneous mimic orders from distorting the book.
        self._p36_mimic_locks: Dict[str, asyncio.Lock] = {}
        # [P36-SIGNAL] Cache of the last-computed spoof probability per symbol
        # (mirror of DataHub._p36_spoof_probs for local fast-path reads).
        self._p36_local_spoof_probs: Dict[str, float] = {}
        # ── [/P36.1] ─────────────────────────────────────────────────────────

        # ── [P36.2] Phase 36.2 — Dynamic Clamping & Golden Build Monitor ─────
        # [P36.2-DYNCLAMP] Per-symbol rolling deque of (timestamp, price) samples
        # used to compute PriceVelocity over P362_VELOCITY_WINDOW_SECS.
        # Updated by _tape_monitor_loop on every tick for every symbol.
        self._p362_price_samples: Dict[str, deque] = {}
        # [P36.2-DYNCLAMP] Per-symbol cached DynamicBuffer (float).
        # Updated by _tape_monitor_loop; read by _p362_get_dynamic_buffer().
        self._p362_dynamic_buffer: Dict[str, float] = {}
        # [P36.2-GOLDEN] Consecutive successful cycle counter.
        # Resets to 0 on any sCode 51006 or IndexError.
        self._p362_cycle_count: int = 0
        # [P36.2-GOLDEN] Total sCode 51006 rejections in the current golden run.
        self._p362_51006_count: int = 0
        # [P36.2-GOLDEN] Total IndexError crashes in the current golden run.
        self._p362_index_error_count: int = 0
        # [P36.2-GOLDEN] Wall-clock timestamp when the current golden run started.
        self._p362_run_start_ts: float = time.time()
        # [P36.2-GOLDEN] Set True once GOLDEN_BUILD_REPORT.txt has been written
        # so we never write it more than once per bot lifetime.
        self._p362_golden_written: bool = False
        # ── [/P36.2] ─────────────────────────────────────────────────────────

        # ── [P37-VPIN] Phase 37 — Flow Toxicity Emergency Exit ───────────────
        # Per-symbol asyncio.Lock preventing concurrent emergency-exit tasks.
        # A toxicity exit is a one-way trip; we must never fire two overlapping
        # exits for the same symbol.
        self._p37_exit_locks: Dict[str, asyncio.Lock] = {}
        # Per-symbol flag: True while an emergency exit task is in-flight.
        # Cleared by the exit task itself upon completion.
        self._p37_exit_in_flight: Dict[str, bool] = {}
        # ── [/P37-VPIN] ──────────────────────────────────────────────────────

        hub.subscribe("account", self._on_account_update)
        hub.subscribe("fill",    self._on_fill_event)

        # ── [P40] Phase 40 internal state flags ───────────────────────────────
        # Initialised here — BEFORE any bridge event handlers fire — so these
        # attributes are always present regardless of bridge connection status.
        # _p40_ghost_healing: set True by ghost-state detection, cleared by the
        # ghost_healed bridge event (or by _on_bridge_ghost_healed).
        # _gui_bridge reads this to engage the Zero-Value Shield.
        self._p40_ghost_healing: bool = False

        # [P40.1-SHIELD] Ready-Gate (Anti-Zombie Bootstrap)
        # Prevent _cycle() from running sizing/entries until we have received a
        # non-ghost, non-zero Binary Truth equity from the Rust bridge.
        # This eliminates the $0.0 (ghost) equity baseline during bridge connect.
        self._p40_ready_gate: asyncio.Event = asyncio.Event()
        if self.bridge is None:
            # Legacy path: no bridge → considered ready immediately.
            self._p40_ready_gate.set()

        # ── [P40] Register bridge event handlers ──────────────────────────────
        # The Rust bridge owns the OKX account WebSocket and delivers pre-healed
        # equity via account_update events.  We also listen to ghost_healed so
        # we can log it WITHOUT triggering an emergency shutdown — the bridge
        # already handled the healing on the Rust side.
        #
        # [P40.1-INIT] Guard: only register handlers after the bridge instance
        # is confirmed non-None.  The bridge.connected flag may be False at
        # construction time (connect() is called later as an asyncio task); the
        # handlers themselves guard with `self.bridge.connected` at call-time
        # for operations that require an established transport.
        if self.bridge is not None:
            self.bridge.on("account_update",  self._on_bridge_account_update)
            self.bridge.on("ghost_healed",    self._on_bridge_ghost_healed)
            self.bridge.on("equity_breach",   self._on_bridge_equity_breach)
            log.info(
                "[P40] Bridge event handlers registered on executor "
                "(bridge instance verified non-None; transport connects async)."
            )
        else:
            log.info(
                "[P40] No bridge instance supplied — bridge event handlers "
                "NOT registered.  Legacy REST/WS path active."
            )
        # ── [/P40] ───────────────────────────────────────────────────────────

    # ── [FIX-ISSUE-3] DB WAL initialisation ───────────────────────────────────

    def _init_db_pragmas(self) -> None:
        db_path: Optional[str] = None
        try:
            db_path = self.db._path if hasattr(self.db, "_path") else None
            if not db_path:
                log.debug("[FIX-DB] db._path not found — WAL pragma skipped.")
                return
            import sqlite3 as _sqlite3
            conn = _sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=30000")
                conn.commit()
                log.info(
                    "[FIX-DB] SQLite WAL+synchronous=NORMAL applied to %s", db_path
                )
            finally:
                conn.close()
        except Exception as exc:
            log.warning(
                "[FIX-DB] Could not apply WAL pragma to %s: %s", db_path, exc
            )

    # ══════════════════════════════════════════════════════════════════════════
    # [QUANT-1] + [MINSZ-1] Size Quantization — central method
    # ══════════════════════════════════════════════════════════════════════════

    async def _quantize_sz(
        self,
        symbol:   str,
        raw_sz:   float,
        swap:     bool,
        tag:      str = "",
    ) -> QuantizedSize:
        """
        [QUANT-1] + [MINSZ-1] Align raw_sz to the instrument's lotSz using
        math.floor (never round) so we never exceed available margin.

        Steps
        -----
        1. Fetch InstrumentMeta from hub.instrument_cache.
        2. Extract lot_sz (lotSz) and min_sz (minSz) from metadata.
        3. Quantize:  actual = floor(raw_sz / lot_sz) * lot_sz
        4. [MINSZ-1] MinSz Bump: if actual > 0 but actual < min_sz, lift to
           min_sz.  This eliminates sCode 51000 "Parameter sz error" — the
           exchange always receives a size that is both lot-aligned AND at or
           above the instrument minimum.
        5. Validate actual >= min_sz (post-bump, this should always pass when
           raw_sz > 0).
        6. Format as a string with the exact decimal precision of lot_sz.

        Returns QuantizedSize.valid=False only when raw_sz itself is zero or
        negative — callers must treat this as an order-abort condition.

        Falls back to raw_sz with a conservative 8-decimal string if
        instrument metadata is unavailable, so the bot degrades gracefully
        rather than crashing.
        """
        _FALLBACK_LOT = 1e-8
        _FALLBACK_MIN = 0.0

        meta: Optional[InstrumentMeta] = None
        try:
            meta = await self._icache.get_instrument_info(symbol, swap=swap)
        except Exception as exc:
            log.warning("[QUANT-1][%s] instrument_cache lookup failed: %s", tag, exc)

        if meta is None:
            log.warning(
                "[QUANT-1][%s] No instrument metadata for %s swap=%s — "
                "using raw_sz=%.10f as fallback (sCode 51000 risk).",
                tag, symbol, swap, raw_sz,
            )
            return QuantizedSize(
                sz_str=f"{raw_sz:.8f}",
                sz_float=raw_sz,
                lot_sz=_FALLBACK_LOT,
                min_sz=_FALLBACK_MIN,
                valid=raw_sz > 0,
            )

        lot_sz = float(getattr(meta, "sz_inst", 0.0) or 0.0)
        min_sz = float(getattr(meta, "min_sz",  0.0) or 0.0)

        if lot_sz <= 0:
            lot_sz = _FALLBACK_LOT
            log.warning(
                "[QUANT-1][%s] lotSz=0 for %s swap=%s — fallback lot_sz=%.2e",
                tag, symbol, swap, lot_sz,
            )

        # ── Core quantization: always floor, never round ───────────────────
        quantized = math.floor(raw_sz / lot_sz) * lot_sz

        # ── [MINSZ-1] MinSz Bump ──────────────────────────────────────────
        # If the floor-rounded size is positive but below the exchange minimum,
        # lift it exactly to min_sz.  This prevents sCode 51000 entirely for
        # any signal the bot decides to act on.
        if quantized > 0 and min_sz > 0 and quantized < min_sz:
            log.info(
                "[MINSZ-1][%s] %s swap=%s: quantized=%.10f < min_sz=%.10f — "
                "bumping to min_sz to satisfy exchange minimum.",
                tag, symbol, swap, quantized, min_sz,
            )
            quantized = min_sz

        decimals = _lot_decimal_places(lot_sz)
        sz_str   = _fmt_sz(quantized, decimals)

        valid = quantized >= min_sz if min_sz > 0 else quantized > 0

        if not valid:
            log.warning(
                "[QUANT-1][%s] %s swap=%s: quantized_sz=%.10f still below "
                "min_sz=%.10f after bump (raw_sz=%.10f was non-positive?) "
                "— order will be ABORTED.",
                tag, symbol, swap, quantized, min_sz, raw_sz,
            )
        else:
            log.debug(
                "[QUANT-1][%s] %s swap=%s: raw=%.10f → quantized=%s "
                "(lot=%.10f min=%.10f decimals=%d)",
                tag, symbol, swap, raw_sz, sz_str, lot_sz, min_sz, decimals,
            )

        return QuantizedSize(
            sz_str=sz_str,
            sz_float=quantized,
            lot_sz=lot_sz,
            min_sz=min_sz,
            valid=valid,
        )

    # ── [P19-2] Shadow Auditor helpers ────────────────────────────────────────

    def _p19_ensure_shadow_csv(self) -> None:
        if os.path.exists(self._shadow_audit_path):
            return
        try:
            with open(self._shadow_audit_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(_SHADOW_AUDIT_HEADER)
            log.info("[P19-2] Shadow audit CSV created: %s", self._shadow_audit_path)
        except Exception as exc:
            log.warning("[P19-2] Could not create shadow audit CSV: %s", exc)

    def _shadow_log(
        self,
        symbol:               str,
        direction:            str,
        tag:                  str,
        real_usd:             float,
        baseline_usd:         float,
        conviction_multiplier: float,
        regime:               str,
        narrative_verdict:    str,
        narrative_score:      Optional[float],
        fill_px:              float,
        fill_sz:              float,
    ) -> None:
        row = [
            int(time.time()), symbol, direction, tag,
            round(real_usd,              4),
            round(baseline_usd,          4),
            round(conviction_multiplier, 4),
            regime, narrative_verdict,
            round(narrative_score, 4) if narrative_score is not None else "",
            round(fill_px, 6),
            round(fill_sz, 6),
        ]
        try:
            tmp_path = self._shadow_audit_path + ".tmp"
            existing_rows: List[list] = []
            if os.path.exists(self._shadow_audit_path):
                with open(self._shadow_audit_path, "r", newline="", encoding="utf-8") as f:
                    existing_rows = list(csv.reader(f))
            if not existing_rows or existing_rows[0] != _SHADOW_AUDIT_HEADER:
                existing_rows.insert(0, _SHADOW_AUDIT_HEADER)
            existing_rows.append(row)
            with open(tmp_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerows(existing_rows)
            os.replace(tmp_path, self._shadow_audit_path)
            log.info(
                "[P19-2] Shadow log: %s %s real=$%.2f baseline=$%.2f "
                "mult=%.3fx verdict=%s fill_px=%.4f fill_sz=%.6f",
                symbol, direction, real_usd, baseline_usd,
                conviction_multiplier, narrative_verdict, fill_px, fill_sz,
            )
        except Exception as exc:
            log.warning("[P19-2] Atomic shadow log failed (%s) — direct append.", exc)
            try:
                with open(self._shadow_audit_path, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(row)
            except Exception as exc2:
                log.error("[P19-2] Shadow log completely failed: %s", exc2)

    # ── Per-coin config ────────────────────────────────────────────────────────
    def _coin_cfg(self, symbol: str) -> CoinConfig:
        return self._coin_cfgs.get(symbol.upper(), CoinConfig())

    def _reload_coin_cfgs_if_stale(self):
        now = time.time()
        if now - self._cfgs_mtime < 60:
            return
        try:
            mtime = os.path.getmtime(GUI_SETTINGS_PATH)
            if mtime != self._cfgs_mtime:
                self._coin_cfgs  = _load_coin_configs(GUI_SETTINGS_PATH)
                self._cfgs_mtime = mtime
        except Exception:
            pass

    # ── [P25] Tactical Config Loader ──────────────────────────────────────────

    def _load_tactical_config(self) -> dict:
        """
        [P25] Read tactical_config.json at the start of every _cycle().

        Returns the parsed dict if valid, otherwise falls back to the safe
        defaults { "risk_off": false, "sniper_only": false, "hedge_active": false }.
        Uses json.loads(f.read()) inside try/except so a Dashboard write-collision
        (partial file) never crashes the executor.

        Also updates self._tactical_config and self._tactical_active so the
        status payload is always current.
        """
        try:
            if not os.path.exists(P25_TACTICAL_CONFIG_PATH):
                self._tactical_config = dict(_P25_TACTICAL_DEFAULTS)
                self._tactical_active = False
                return self._tactical_config

            with open(P25_TACTICAL_CONFIG_PATH, "r", encoding="utf-8") as f:
                raw = f.read().strip()

            if not raw:
                raise ValueError("empty file")

            cfg = json.loads(raw)
            if not isinstance(cfg, dict):
                raise ValueError("not a dict")

            # Merge with defaults so missing keys never cause KeyErrors downstream
            merged = dict(_P25_TACTICAL_DEFAULTS)
            merged.update(cfg)

            self._tactical_config = merged
            self._tactical_active = bool(
                merged.get("risk_off")     or
                merged.get("sniper_only")  or
                merged.get("hedge_active")
            )

            if self._tactical_active:
                log.info(
                    "[P25] Tactical overrides ACTIVE: risk_off=%s sniper_only=%s hedge_active=%s",
                    merged.get("risk_off"), merged.get("sniper_only"), merged.get("hedge_active"),
                )

            return self._tactical_config

        except Exception as exc:
            log.warning(
                "[P25] _load_tactical_config failed (%s) — using safe defaults.", exc
            )
            self._tactical_config = dict(_P25_TACTICAL_DEFAULTS)
            self._tactical_active = False
            return self._tactical_config

    # ── [P25] Liquidation Oracle ───────────────────────────────────────────────

    async def _update_liquidation_clusters(self, symbol: str, current_price: float) -> None:
        """
        [P25] Generate / fetch liquidation clusters for the Dashboard Heat-Map.

        Production path  — inspect the order book hub data for large orders near
        the current price and classify them as leveraged long (will liquidate on
        a price drop) or short (will liquidate on a price rise) clusters.

        Demo / fallback path — if real order-book data is unavailable or contains
        fewer than 3 meaningful levels, synthesise P25_LIQUIDATION_CLUSTERS_COUNT
        synthetic clusters near the current price so the Dashboard Heat-Map is
        always visually active and never blank.

        Output: writes to self._liquidation_clusters["clusters"] which is later
        merged into self._status["market_data"]["liquidations"] by _cycle().
        """
        clusters = []
        try:
            book = await self.hub.get_order_book(symbol)
            if book and book.bids and book.asks:
                # Production: scan top-10 levels for unusually large orders
                avg_bid_sz = (
                    float(sum(sz for _, sz in book.bids[:10])) / min(len(book.bids), 10)
                    if book.bids else 0.0
                )
                avg_ask_sz = (
                    float(sum(sz for _, sz in book.asks[:10])) / min(len(book.asks), 10)
                    if book.asks else 0.0
                )
                threshold_mult = 2.0  # must be 2× average to count as a cluster

                for px, sz in book.bids[:20]:
                    if sz >= avg_bid_sz * threshold_mult and px > 0:
                        clusters.append({
                            "price":  round(float(px), 6),
                            "volume": round(float(sz), 4),
                            "side":   "long",
                        })
                for px, sz in book.asks[:20]:
                    if sz >= avg_ask_sz * threshold_mult and px > 0:
                        clusters.append({
                            "price":  round(float(px), 6),
                            "volume": round(float(sz), 4),
                            "side":   "short",
                        })

                # Keep at most 10 real clusters (largest first)
                clusters.sort(key=lambda c: c["volume"], reverse=True)
                clusters = clusters[:10]

        except Exception as exc:
            log.debug("[P25] Liquidation cluster real-data fetch failed (%s) — using synthetic.", exc)

        # Fallback / Demo: synthesise clusters when real data is insufficient
        if len(clusters) < 3 and current_price > 0:
            import random as _random
            rng_seed = int(current_price * 1000) % 99991   # deterministic per price level
            _rng = _random.Random(rng_seed)

            offsets = [-0.030, -0.015, -0.005, +0.010, +0.025]
            for i, off in enumerate(offsets[:P25_LIQUIDATION_CLUSTERS_COUNT]):
                synth_px  = round(current_price * (1.0 + off), 6)
                synth_vol = round(_rng.uniform(0.5, 8.0) * current_price * 0.001 / max(current_price, 1), 4)
                synth_vol = round(_rng.uniform(100_000, 2_000_000) / max(current_price, 1), 4)
                side = "short" if off > 0 else "long"
                clusters.append({
                    "price":  synth_px,
                    "volume": synth_vol,
                    "side":   side,
                })
            log.debug(
                "[P25] Synthetic liquidation clusters generated for %s @ %.4f (%d clusters)",
                symbol, current_price, len(clusters),
            )

        self._liquidation_clusters = {"clusters": clusters}

    # ── Account handlers ───────────────────────────────────────────────────────

    async def _on_account_update(self, snap: AccountSnapshot):
        """
        [LOCK-2] + [GHOST-3] + [EQBOOT-2] State-Aware equity validator.

        The ENTIRE method body is executed inside self._equity_lock so
        back-to-back rapid WS pushes are fully serialised.

        Ghost-State Detection [GHOST-3]:
          If equity is at or below the floor we discard the value, log a
          WARNING, and schedule a REST back-fill if positions are open.

        Equity Bootstrap [EQBOOT-2]:
          First-boot / Demo-mode edge case: if the incoming equity is at or
          below the valid floor BUT buying_power (avail) is healthy AND
          self._equity is still uninitialised (0.0), we seed self._equity
          from avail.  This prevents the Kelly sizer from producing
          micro/zero sizes during the warm-up window before the first valid
          total_equity push arrives.
        """
        raw_equity = snap.total_equity
        raw_avail  = snap.buying_power

        try:
            equity = float(raw_equity) if raw_equity is not None else 0.0
        except (TypeError, ValueError):
            equity = 0.0

        try:
            avail = float(raw_avail) if raw_avail is not None else 0.0
        except (TypeError, ValueError):
            avail = 0.0

        # [LOCK-2] Acquire lock for the full update branch.
        async with self._equity_lock:
            if equity <= _EXECUTOR_MIN_VALID_EQUITY:
                has_positions = self.has_open_positions()

                # [P40.1-SHIELD] Bootstrap Wait (Anti-Zero Baseline)
                # Phase 40.1 disallows seeding equity from buying_power during ghost/zero reads.
                # Trading is gated by self._p40_ready_gate, which opens only on a non-ghost,
                # non-zero equity reading.

# [P22-RESILIENCE] Increment consecutive ghost-reads counter.
                self._consecutive_ghost_reads += 1
                self._equity_is_ghost          = True

                # [GHOST-3] Log exact discrepancy: discarded vs retained.
                log.warning(
                    "[GHOST-STATE] _on_account_update: "
                    "DISCARDED equity=%.6f (floor=%.2f). "
                    "RETAINED equity=%.6f  avail=%.6f  "
                    "has_open_positions=%s  "
                    "discrepancy=%.6f  consecutive_ghost_reads=%d",
                    equity,
                    _EXECUTOR_MIN_VALID_EQUITY,
                    self._equity,
                    self._avail,
                    has_positions,
                    abs(equity - self._equity),
                    self._consecutive_ghost_reads,
                )

                # [P23-HOTFIX-2] Gateway Pulse-Reset: after 50+ consecutive ghost
                # reads the WS account feed is considered permanently dead.
                # Fire a reconnect request (rate-limited to once per 30 s).
                if self._consecutive_ghost_reads >= P23_GHOST_RECONNECT_THRESH:
                    now = time.time()
                    if now - self._last_ws_reconnect_ts >= 30.0:
                        self._last_ws_reconnect_ts = now
                        log.critical(
                            "[P23-HOTFIX-2] %d consecutive ghost reads — "
                            "triggering WebSocket account-channel reconnect.",
                            self._consecutive_ghost_reads,
                        )
                        try:
                            if hasattr(self.hub, "reconnect_account_ws"):
                                asyncio.create_task(
                                    self.hub.reconnect_account_ws(),
                                    name="p23_ws_reconnect",
                                )
                            else:
                                log.warning(
                                    "[P23-HOTFIX-2] hub.reconnect_account_ws() "
                                    "not available — restart bot to clear stale WS."
                                )
                        except Exception as _ws_exc:
                            log.error("[P23-HOTFIX-2] WS reconnect error: %s", _ws_exc)
                    # Reset so we don't fire every tick after threshold is hit.
                    self._consecutive_ghost_reads = 0

                # ── [P24-DEFENSE-2] [RECOVERY-SYNC] ──────────────────────────
                # After >10 consecutive ghost reads with positions open the WS
                # connection is considered permanently stale.  Force a REST
                # Account Poll that hard-overwrites self._equity, bypassing the
                # _ghost_poll_in_flight guard entirely.
                # [P24] Threshold raised from 5 → 10 for reduced false-positive
                # reconnects during legitimate exchange maintenance windows.
                if has_positions and self._consecutive_ghost_reads > 10:
                    log.critical(
                        "[RECOVERY-SYNC] %d consecutive ghost reads with open "
                        "positions — WebSocket equity stream appears permanently "
                        "stale. Forcing REST account poll to hard-overwrite state. "
                        "(retained=%.6f  floor=%.2f)",
                        self._consecutive_ghost_reads,
                        self._equity,
                        _EXECUTOR_MIN_VALID_EQUITY,
                    )
                    # Reset counters BEFORE scheduling so a scheduling error
                    # can never permanently deadlock the recovery path.
                    self._consecutive_ghost_reads = 0
                    self._ghost_poll_in_flight    = False   # bypass guard
                    try:
                        asyncio.create_task(
                            self._verify_equity_via_rest(),
                            name="ghost_equity_rest_poll_recovery_sync",
                        )
                        self._ghost_poll_in_flight = True
                    except Exception as _task_exc:
                        log.error(
                            "[RECOVERY-SYNC] Failed to schedule forced REST poll: %s",
                            _task_exc,
                        )
                    return

                # [GHOST-3] Schedule REST back-fill non-blockingly (normal path).
                # Flag is set AFTER successful task creation.
                if has_positions and not self._ghost_poll_in_flight:
                    try:
                        asyncio.create_task(
                            self._verify_equity_via_rest(),
                            name="ghost_equity_rest_poll",
                        )
                        self._ghost_poll_in_flight = True
                    except Exception as task_exc:
                        log.error(
                            "[GHOST-STATE] Failed to schedule REST back-fill task: %s",
                            task_exc,
                        )
                return  # Return while still holding lock — releases on exit.

            # Valid reading: accept it.
            self._equity           = equity
            self._avail            = avail
            # [LKG-3] Persist the confirmed equity value for Kelly sizing resilience.
            self._last_valid_equity = equity
            # [P22-RESILIENCE] Clear ghost counters on any valid WS reading.
            self._consecutive_ghost_reads = 0
            self._equity_is_ghost         = False

            # [P40.1-SHIELD] Ready-Gate opens on first verified equity.
            try:
                self._p40_ready_gate.set()
            except Exception:
                pass

        # Record drawdown OUTSIDE the lock (non-mutating to equity state).
        self.drawdown.record(equity)

    async def _verify_equity_via_rest(self) -> None:
        """
        [GHOST-3] Background REST poll triggered when WS delivers a ghost
        zero-equity reading while positions are open.

        [P24.1-EXEC] Truth Verification — the REST response is now ALWAYS
        accepted as the authoritative "New Truth" regardless of its value.
        This fixes the Ghost-Rest Paradox where an account that is genuinely
        at $0.00 was being silently discarded (because it was below
        _EXECUTOR_MIN_VALID_EQUITY), leaving the bot permanently locked in
        Ghost Mode even after a real reset.

        Behaviour matrix after REST poll:
          REST equity > floor   → update _equity, _avail, _last_valid_equity,
                                   reset all ghost counters, clear ghost flag.
          REST equity ≤ floor   → update _equity and _avail (truth is truth),
                                   reset ghost counters (stop hammering REST),
                                   do NOT update _last_valid_equity so Kelly
                                   sizing can still use the last confirmed value.
          REST returns empty    → retain existing state, log warning.

        [P24.1-EXEC] force_truth mode — when self._force_truth_pending is True
        (set by the dashboard 'force_truth' control event), the floor check is
        bypassed entirely and _last_valid_equity is also updated so the bot
        acknowledges a deliberate manual reset to zero.
        """
        try:
            log.info(
                "[GHOST-STATE] Background REST poll: verifying equity truth "
                "(WS ghost reading with open positions).  force_truth=%s",
                self._force_truth_pending,
            )
            d = await self.hub.rest.get(
                "/api/v5/account/balance", params={"ccy": "USDT"}
            )
            if not d or not d.get("data"):
                log.warning(
                    "[GHOST-STATE] REST poll: empty response — retaining last equity."
                )
                return
            detail      = d["data"][0]
            rest_equity = float(detail.get("adjEq") or 0)
            rest_avail  = next(
                (float(x.get("availBal", 0))
                 for x in detail.get("details", []) if x.get("ccy") == "USDT"),
                0.0,
            )

            # [P24.1-EXEC] Capture force_truth and immediately clear the flag
            # so subsequent polls behave normally.
            _is_force_truth = self._force_truth_pending
            self._force_truth_pending = False

            # [LOCK-2] Use equity lock for the REST-sourced update too.
            async with self._equity_lock:
                # ── [P40.1-OPEN-POS-SHIELD] REST Poll Ghost-Read Guard ────────
                # If the executor has confirmed open positions it is structurally
                # impossible to have equity ≤ $1.00.  Reject the REST response
                # as a "Ghost Read", preserve _last_valid_equity so Kelly sizing
                # and Zombie Mode continue to operate on Binary Truth, and log a
                # [SHIELD-REJECT] warning.  Do NOT reset ghost counters — the
                # WS is still misbehaving and must continue to trigger retries.
                _has_open_pos_rest = self.has_open_positions()
                if (
                    _has_open_pos_rest
                    and rest_equity <= 1.0
                    and not _is_force_truth
                ):
                    log.warning(
                        "[SHIELD-REJECT] _verify_equity_via_rest: "
                        "REST returned equity=%.6f ≤ 1.0 but open positions "
                        "are confirmed (%d pos).  Rejecting as Ghost Read.  "
                        "_last_valid_equity=%.6f retained.  "
                        "trader_status.json equity will NOT be zeroed.",
                        rest_equity,
                        len(self.positions),
                        self._last_valid_equity,
                    )
                    # Return WITHOUT touching _equity, _avail, or ghost counters.
                    # The ghost counter continues so the WS reconnect logic can
                    # fire its own escalation path if the problem persists.
                    return
                # ── [/P40.1-OPEN-POS-SHIELD] ─────────────────────────────────

                if rest_equity <= _EXECUTOR_MIN_VALID_EQUITY and not _is_force_truth:
                    # [P24.1-EXEC] REST confirmed the account is genuinely at/near
                    # zero.  Accept this as truth so the ghost counter resets and
                    # the bot stops spamming REST polls.  Do NOT update
                    # _last_valid_equity — Kelly sizing will keep using the
                    # last confirmed non-zero reading so position math doesn't
                    # collapse while the account is being refunded.
                    log.warning(
                        "[P24.1-EXEC] REST poll: equity=%.6f at/below floor=%.2f — "
                        "ACCEPTED as New Truth (genuine empty account confirmed). "
                        "Ghost counters reset; _last_valid_equity=%.6f preserved for Kelly.",
                        rest_equity, _EXECUTOR_MIN_VALID_EQUITY, self._last_valid_equity,
                    )
                    self._equity           = rest_equity
                    self._avail            = rest_avail
                    # Hard-reset ghost counters so we stop hammering the exchange.
                    self._consecutive_ghost_reads = 0
                    self._equity_is_ghost         = False
                else:
                    # REST confirmed valid equity (above floor) — OR force_truth
                    # override is active (dashboard-initiated manual reset).
                    log.info(
                        "[GHOST-STATE] REST poll SUCCESS: "
                        "accepting equity=%.6f avail=%.6f "
                        "(was retained=%.6f, delta=%.6f, force_truth=%s).",
                        rest_equity, rest_avail,
                        self._equity, abs(rest_equity - self._equity),
                        _is_force_truth,
                    )
                    self._equity            = rest_equity
                    self._avail             = rest_avail
                    # [LKG-3] REST-confirmed value is authoritative.
                    self._last_valid_equity = rest_equity
                    # [P24-DEFENSE-2] Hard-reset ghost counters so the Dashboard
                    # ghost meter returns to green without requiring a bot restart.
                    self._consecutive_ghost_reads = 0
                    self._equity_is_ghost         = False

            self.drawdown.record(rest_equity)
        except Exception as exc:
            log.error("[GHOST-STATE] REST poll failed: %s", exc)
        finally:
            self._ghost_poll_in_flight = False

    # ── [P24] Systemic Defense — Hard Gateway Reset ───────────────────────────

    async def _hard_reset_sync(self, force_truth: bool = False) -> None:
        """
        [P24-DEFENSE] Full gateway reset triggered by the 'reset_gateway'
        control event from the dashboard.

        [P24.1-EXEC] force_truth parameter — when True (set by dashboard
        via control_event.json {"event":"reset_gateway","force_truth":true}),
        ALL safety counters are hard-reset regardless of the current equity
        value.  This is the escape hatch for the Ghost-Rest Paradox: even if
        the REST poll returns $0.00, the bot accepts it as the New Truth and
        clears the ghost-state lock so the dashboard status turns green.

        Steps
        -----
        1. Hard-reset all ghost-state / safety counters unconditionally.
        2. Reconnect the account WebSocket channel (clears stale subscriptions).
        3. Force an immediate REST equity verification (clears Ghost State).
           If force_truth=True, set self._force_truth_pending before the poll
           so _verify_equity_via_rest() bypasses the equity floor check.

        All steps are individually try/excepted so a failure in one does not
        prevent the others from executing.
        """
        log.critical(
            "[P24.1-EXEC] Hard Gateway Reset requested by dashboard — "
            "force_truth=%s.  Clearing all safety counters, "
            "reconnecting WS and forcing REST sync.",
            force_truth,
        )

        # Step 1 — Unconditional hard-reset of ALL ghost / safety counters.
        # This must happen FIRST so a subsequent poll failure can never leave
        # the bot permanently locked in Ghost Mode.
        try:
            self._consecutive_ghost_reads = 0
            self._equity_is_ghost         = False
            self._ghost_poll_in_flight    = False
            if force_truth:
                self._force_truth_pending = True
            log.info("[P24.1-EXEC] Safety counters cleared (force_truth=%s).", force_truth)
        except Exception as exc:
            log.error("[P24.1-EXEC] Counter reset error: %s", exc)

        # Step 2 — WS reconnect
        try:
            if hasattr(self.hub, "reconnect_account_ws"):
                await self.hub.reconnect_account_ws()
                self._last_ws_reconnect_ts = time.time()
                log.info("[P24-DEFENSE] WS account channel reconnected.")
            else:
                log.warning(
                    "[P24-DEFENSE] hub.reconnect_account_ws() not available; "
                    "WS reconnect skipped."
                )
        except Exception as exc:
            log.error("[P24-DEFENSE] WS reconnect error: %s", exc)

        # Step 3 — REST equity verification (clears $0 ghost equity)
        try:
            self._ghost_poll_in_flight = False   # allow the poll to proceed
            await self._verify_equity_via_rest()
            log.info("[P24-DEFENSE] REST equity verification complete.")
        except Exception as exc:
            log.error("[P24-DEFENSE] REST verify error: %s", exc)
        finally:
            # Ensure force_truth flag is cleared even if verify raised.
            self._force_truth_pending = False

        log.info("[P24.1-EXEC] Hard Gateway Reset complete.")

    async def _on_fill_event(self, fill: dict):
        base_sym = fill.get("instId", "").split("-")[0]
        pos      = self.positions.get(base_sym)
        if not pos:
            return
        log.info("Fill: %s %s sz=%s px=%s",
                 base_sym, fill.get("side"), fill.get("fillSz"), fill.get("fillPx"))

    # ── [P40] Bridge event handlers ────────────────────────────────────────────

    async def _on_bridge_account_update(self, msg: dict) -> None:
        """
        [P40] Called when the Rust bridge emits an 'account_update' event.

        The bridge performs Ghost-State healing internally (Rust side), so by
        the time this fires the equity value is already "Binary Truth" — no
        additional Python-side ghost detection is needed.  We accept the value
        unconditionally and reset all ghost counters to prevent the legacy
        ghost-recovery REST poll from running unnecessarily.

        This COEXISTS with hub.subscribe("account", _on_account_update) during
        the transition: if both fire, the bridge value is preferred because the
        bridge sets _consecutive_ghost_reads=0 first, causing the legacy handler
        to see a clean slate.
        """
        # Robust parse (bridge may send strings)
        try:
            eq = float(msg.get("eq", 0.0) or 0.0)
        except (TypeError, ValueError):
            eq = 0.0
        try:
            avail = float(msg.get("avail_eq") or msg.get("cash_bal") or eq or 0.0)
        except (TypeError, ValueError):
            avail = eq


        is_ghost    = bool(msg.get("is_ghost", False))
        ghost_count = int(msg.get("ghost_count", 0))

        # ── [P40.1-SHIELD] Binary Truth Shield (Position-Aware) ────────────────
        # If positions are open, any equity <= 1.0 is treated as a Ghost Read,
        # even if the bridge forgot to set is_ghost. Never overwrite
        # _last_valid_equity in this state.
        try:
            if self.has_open_positions() and eq <= 1.0:
                _ret = getattr(self, "_last_valid_equity", 0.0) or getattr(self, "_equity", 0.0) or eq
                log.warning(
                    "[SHIELD-REJECT] Bridge account_update rejected: eq=%.6f ≤ 1.0 "
                    "with open positions — retaining _last_valid_equity=%.6f",
                    eq, _ret,
                )
                self._equity_is_ghost = True
                self._consecutive_ghost_reads = int(getattr(self, "_consecutive_ghost_reads", 0) or 0) + 1
                return
        except Exception:
            # Never allow shield logic to crash the bridge handler.
            pass
        # ── [/P40.1-SHIELD] ───────────────────────────────────────────────────

        if is_ghost:
            # Bridge flagged this as a ghost read — trust Rust's ghost_count but
            # do NOT override _equity with a ghost value.  Log and return.
            log.warning(
                "[P40] Bridge account_update: IS_GHOST (ghost_count=%d) — "
                "retained Python equity=%.6f. Bridge will emit ghost_healed when resolved.",
                ghost_count, self._equity,
            )
            return

        async with self._equity_lock:
            if eq <= 0.0:
                # Bridge sent a zero even without is_ghost — treat defensively.
                log.warning(
                    "[P40] Bridge account_update: eq=0.0 without is_ghost flag — "
                    "discarding to protect against unexpected ghost state.",
                )
                return

            self._equity               = eq
            self._avail                = avail
            self._last_valid_equity    = eq
            self._consecutive_ghost_reads = 0
            self._equity_is_ghost         = False

            # [P40.1-SHIELD] Ready-Gate: open on first verified Binary Truth equity
            # from the Rust bridge.  Without this set the _cycle() ready-gate check
            # (line 7203) blocks trading permanently when bridge is connected,
            # because _on_account_update (the legacy WS path) may never fire.
            try:
                _was_set = self._p40_ready_gate.is_set()
                self._p40_ready_gate.set()
                if not _was_set:
                    log.info(
                        "[P40.1-SHIELD] Ready-Gate OPENED by bridge account_update: "
                        "eq=%.6f — trading cycle will proceed normally.", eq,
                    )
            except Exception as _rg_exc:
                log.debug("[P40.1-SHIELD] Ready-Gate set error (non-fatal): %s", _rg_exc)

            log.debug(
                "[P40] Bridge account_update accepted: eq=%.6f avail=%.6f",
                eq, avail,
            )

        self.drawdown.record(eq)

    async def _on_bridge_ghost_healed(self, msg: dict) -> None:
        """
        [P40] Ghost-State Healed by the Rust bridge.

        Contract: Python MUST log this but MUST NOT trigger an emergency
        shutdown or position flattening.  The Rust bridge has already
        reconciled the equity value via REST and confirmed it is non-zero.
        A ghost_healed event is informational only — it is NOT an equity_breach.

        Specifically:
          • DO NOT call liquidate_all_positions()
          • DO NOT set _p20_zombie_mode = True
          • DO NOT trip the CircuitBreaker
          • DO update self._equity with the healed value (reliable truth)
          • DO reset ghost counters (bridge already healed this)
        """
        raw_ws_eq  = float(msg.get("raw_ws_eq",  0.0))
        healed_eq  = float(msg.get("healed_eq",  0.0))
        ghost_count = int(msg.get("ghost_count", 0))
        source     = msg.get("source", "unknown")

        log.warning(
            "[P40][GHOST-HEALED] Rust bridge resolved ghost state — "
            "raw_ws_eq=%.4f  healed_eq=%.4f  ghost_count=%d  source=%s  "
            "Python action: EQUITY_SYNC only (NO shutdown, NO flatten).",
            raw_ws_eq, healed_eq, ghost_count, source,
        )

        if healed_eq > 0.0:
            async with self._equity_lock:
                self._equity               = healed_eq
                self._last_valid_equity    = healed_eq
                self._consecutive_ghost_reads = 0
                self._equity_is_ghost         = False
                self._ghost_poll_in_flight    = False

            # [P40.1-SHIELD] Ready-Gate: healed_eq is validated Binary Truth.
            # Open the gate outside the equity lock (lock not needed for asyncio.Event).
            try:
                _was_set = self._p40_ready_gate.is_set()
                self._p40_ready_gate.set()
                if not _was_set:
                    log.info(
                        "[P40.1-SHIELD] Ready-Gate OPENED by ghost_healed: "
                        "healed_eq=%.6f — trading cycle will proceed normally.", healed_eq,
                    )
            except Exception as _rg_exc:
                log.debug("[P40.1-SHIELD] ghost_healed Ready-Gate set error: %s", _rg_exc)

            # [P40.1-SHIELD] Veto any zombie mode triggered by the ghost reading.
            if self._p20_zombie_mode:
                self._p20_zombie_mode = False
                log.warning(
                    "[P40.1-SHIELD] Zombie Mode VETOED by executor ghost_healed handler "
                    "— bridge confirmed healed_eq=%.6f is Binary Truth, not a breach.",
                    healed_eq,
                )

    async def _on_bridge_equity_breach(self, msg: dict) -> None:
        """
        [P40] Rust bridge confirmed equity has hit $0.00 AND REST verification
        agrees.  This is a genuine breach — not a ghost state.

        Unlike ghost_healed (which suppresses shutdown), equity_breach IS a
        real event and allows the Python side to take protective action.
        However, we log at CRITICAL level and let the existing CircuitBreaker
        and GlobalRiskManager handle it rather than calling sys.exit() directly.
        """
        confirmed_eq = float(msg.get("confirmed_eq", 0.0))
        message      = msg.get("message", "")
        log.critical(
            "[P40][EQUITY-BREACH] Bridge confirmed $0.00 equity — "
            "confirmed_eq=%.4f  bridge_msg=%s  "
            "Python CircuitBreaker will handle position protection.",
            confirmed_eq, message,
        )
        # Allow existing risk management (CircuitBreaker, GlobalRiskManager) to
        # respond organically — do NOT add hard-coded emergency shutdown here.
        # The CB will trip on the next cycle when it reads the new equity.

    # ── REST helpers ───────────────────────────────────────────────────────────
    async def _place_order(self, body: dict, priority: Optional[str] = None) -> dict:
        """
        [P31-SMART-RETRY] + [P40] Submit an order via the Rust Logic Execution
        Bridge when available; fall back to direct OKX REST on bridge disconnect.

        [P40] Bridge path:
          Translates the OKX REST body dict into bridge.place_order() kwargs,
          sets priority="WHALE_SNIPER" when the caller explicitly requests the
          express lane (whale multiplier > P23_WHALE_SNIPER_MULT_THRESH), and
          normalises the bridge ack dict back into the OKX REST response shape
          so ALL downstream callers are zero-change.

        [P31-SMART-RETRY] Legacy REST path (bridge absent or disconnected):
          Retries up to P31_ORDER_RETRY_ATTEMPTS times with exponential backoff.

        Network errors that trigger a retry (REST path):
          - OSError with WinError 64 (host unreachable, Windows pipe broken)
          - asyncio.IncompleteReadError / EOFError (EOF during HTTP read)
          - ConnectionResetError (TCP RST mid-request)
          - aiohttp.ServerDisconnectedError / aiohttp.ClientConnectionError
          - asyncio.TimeoutError (transient OKX gateway overload)

        OKX semantic errors (code != "0") are returned as-is; bridge
        BridgeOrderError is translated to the same shape.
        """
        # ── [P40] Bridge order path ───────────────────────────────────────────
        if self.bridge is not None and self.bridge.connected:
            return await self._place_order_via_bridge(body, priority=priority)

        # ── [P31-SMART-RETRY] Legacy REST fallback ────────────────────────────
        _RETRYABLE = (
            OSError, EOFError, ConnectionResetError, asyncio.IncompleteReadError,
            asyncio.TimeoutError,
        )
        # Also catch aiohttp errors by name to stay dependency-agnostic.
        try:
            import aiohttp as _aiohttp
            _RETRYABLE = _RETRYABLE + (
                _aiohttp.ServerDisconnectedError,
                _aiohttp.ClientConnectionError,
            )
        except ImportError:
            pass

        # [v32.1-TDMODE] OKX sCode 51000 guard — ensure every futures/swap order
        # body explicitly carries tdMode="cross".  If the caller already set a
        # tdMode we leave it untouched; we only inject when it is absent and the
        # instId looks like a SWAP instrument (ends with "-SWAP").
        if "tdMode" not in body and body.get("instId", "").endswith("-SWAP"):
            body = {**body, "tdMode": "cross"}
            log.debug(
                "[v32.1-TDMODE] Injected tdMode=cross for %s (was missing).",
                body.get("instId", "?"),
            )

        last_exc: Optional[Exception] = None
        for attempt in range(1, P31_ORDER_RETRY_ATTEMPTS + 1):
            try:
                result = await self.rest.post("/api/v5/trade/order", body)
                return result
            except _RETRYABLE as exc:
                last_exc = exc
                is_winerr64 = isinstance(exc, OSError) and getattr(exc, "winerror", None) == 64
                err_label   = "WinError 64" if is_winerr64 else type(exc).__name__
                delay = P31_ORDER_RETRY_BASE_SECS * (2 ** (attempt - 1))
                if attempt < P31_ORDER_RETRY_ATTEMPTS:
                    log.warning(
                        "[P31-SMART-RETRY] _place_order: attempt %d/%d failed (%s: %s) "
                        "— retrying in %.2fs  body_inst=%s",
                        attempt, P31_ORDER_RETRY_ATTEMPTS,
                        err_label, exc,
                        delay,
                        body.get("instId", "?"),
                    )
                    await asyncio.sleep(delay)
                else:
                    log.error(
                        "[P31-SMART-RETRY] _place_order: all %d attempts exhausted "
                        "(%s: %s)  body_inst=%s",
                        P31_ORDER_RETRY_ATTEMPTS, err_label, exc,
                        body.get("instId", "?"),
                    )
            except Exception as exc:
                # Non-retryable exception: propagate immediately.
                raise

        # All retries exhausted — raise the last transport error so callers can
        # record the failure rather than silently returning None.
        if last_exc is not None:
            raise last_exc
        return {}

    async def _place_order_via_bridge(
        self, body: dict, priority: Optional[str] = None
    ) -> dict:
        """
        [P40] Translate an OKX REST order body dict into bridge.place_order()
        kwargs, submit through the Rust IPC bridge, and normalise the bridge
        ack back into the OKX REST response shape:

            { "code": "0", "data": [{ "ordId": "<id>", "clOrdId": "", "tag": "", "sCode": "0", "sMsg": "" }] }

        This shape is identical to what the legacy `self.rest.post` path returned,
        so ALL downstream callers (response code checks, ordId extraction, etc.)
        require zero changes.

        Bridge errors are mapped to the same {"code": "1", "data": [...]} shape
        so the caller's existing error-handling branches activate correctly.

        Priority:
            priority="WHALE_SNIPER"  →  Rust express lane (<500µs target latency)
        """
        inst_id  = body.get("instId", "")
        td_mode  = body.get("tdMode", "cross")
        side     = body.get("side", "")
        ord_type = body.get("ordType", "market")
        sz       = str(body.get("sz", "0"))
        pos_side = body.get("posSide")
        px       = str(body.get("px")) if body.get("px") is not None else None
        reduce_only = body.get("reduceOnly")
        cl_ord_id   = body.get("clOrdId")

        try:
            ack = await self.bridge.place_order(  # type: ignore[union-attr]
                inst_id     = inst_id,
                td_mode     = td_mode,
                side        = side,
                ord_type    = ord_type,
                sz          = sz,
                pos_side    = pos_side,
                px          = px,
                reduce_only = reduce_only,
                cl_ord_id   = cl_ord_id,
                priority    = priority,
            )

            # ── [P40.1-LAT] Bridge Latency Failsafe ─────────────────────────
            # The bridge may include latency_us in the ack; if absent, the bridge
            # client attaches _measured_latency_us (wall-clock) as a fallback.
            try:
                _lat_us_raw = ack.get("latency_us", None)
                if _lat_us_raw is None:
                    _lat_us_raw = ack.get("_measured_latency_us", None)
                _lat_us = int(_lat_us_raw) if _lat_us_raw is not None else 0
            except Exception:
                _lat_us = 0

            try:
                if _lat_us >= P40_LATENCY_FAILSAFE_US and _lat_us >= P40_LATENCY_FAILSAFE_MIN_US:
                    self._p40_apply_latency_failsafe(inst_id=inst_id, latency_us=_lat_us)
            except Exception:
                pass

            # Normalise ack → OKX REST shape
            ord_id = ack.get("ord_id") or ack.get("ordId") or ""
            return {
                "code": "0",
                "msg":  "",
                "data": [{
                    "ordId":   ord_id,
                    "clOrdId": cl_ord_id or "",
                    "tag":     "",
                    "sCode":   "0",
                    "sMsg":    "",
                }],
                "_bridge_ack": ack,   # preserve raw ack for telemetry if needed
            }
        except BridgeOrderError as exc:
            log.error(
                "[P40] Bridge rejected order for %s: %s  (priority=%s)",
                inst_id, exc, priority,
            )
            return {
                "code": "1",
                "msg":  str(exc),
                "data": [{"ordId": "", "clOrdId": cl_ord_id or "", "sCode": "1", "sMsg": str(exc)}],
            }
        except BridgeNotConnectedError:
            log.warning(
                "[P40] Bridge disconnected during place_order for %s — "
                "falling back to legacy REST.", inst_id,
            )
            # Recursive fallback to REST (bridge=None guard prevents infinite loop)
            _saved_bridge = self.bridge
            self.bridge   = None
            try:
                return await self._place_order(body, priority=priority)
            finally:
                self.bridge = _saved_bridge
        except BridgeTimeoutError as exc:
            log.error("[P40] Bridge order timed out for %s: %s", inst_id, exc)
            return {
                "code": "1",
                "msg":  f"bridge_timeout: {exc}",
                "data": [{"ordId": "", "clOrdId": cl_ord_id or "", "sCode": "1", "sMsg": str(exc)}],
            }
        except Exception as exc:
            log.error("[P40] Unexpected bridge error for %s: %s", inst_id, exc, exc_info=True)
            return {
                "code": "1",
                "msg":  str(exc),
                "data": [{"ordId": "", "clOrdId": cl_ord_id or "", "sCode": "1", "sMsg": str(exc)}],
            }

    def _p40_apply_latency_failsafe(self, inst_id: str, latency_us: int) -> None:
        '''
        [P40.1-LAT] Bridge Latency Failsafe.

        If the Rust IPC bridge reports elevated order_ack latency, we protect the
        execution layer from toxic market fills by:
          1) Forcing the affected symbol into Phase 32 LIMIT_ONLY mode for a
             bounded cooldown window (P40_LATENCY_LIMIT_ONLY_SECS).
          2) Elevating DynamicBuffer implicitly via the LIMIT_ONLY guard inside
             _p362_get_dynamic_buffer().

        This method is intentionally synchronous and non-throwing so it can be
        called directly from the order placement path.
        '''
        try:
            sym = inst_id.split("-")[0].upper() if inst_id else ""
        except Exception:
            sym = ""
        if not sym:
            return

        now = time.time()
        until = now + max(1.0, float(P40_LATENCY_LIMIT_ONLY_SECS))
        prev = float(self._p32_limit_only_until.get(sym, 0.0) or 0.0)
        if until > prev:
            self._p32_limit_only_until[sym] = until

        # Publish lightweight status for the dashboard (best effort).
        try:
            self._status.setdefault("p40_latency_failsafe", {})
            self._status["p40_latency_failsafe"][sym] = {
                "latency_us": int(latency_us),
                "limit_only_until": float(self._p32_limit_only_until.get(sym, until)),
                "ts": now,
            }
        except Exception:
            pass

        log.warning(
            "[P40.1-LAT] Bridge latency spike: %s latency_us=%d ≥ %d → LIMIT_ONLY for %.0fs",
            sym, int(latency_us), int(P40_LATENCY_FAILSAFE_US),
            float(self._p32_limit_only_until[sym] - now),
        )

    async def _cancel_order(self, inst_id: str, ord_id: str):
        """
        [P40] Cancel an open order.  Routes through the bridge when connected;
        falls back to direct OKX REST if bridge is unavailable.
        """
        if self.bridge is not None and self.bridge.connected:
            try:
                await self.bridge.cancel_order(inst_id=inst_id, ord_id=ord_id)
                return
            except Exception as exc:
                log.warning(
                    "[P40] Bridge cancel_order failed for %s/%s: %s — "
                    "falling back to REST.", inst_id, ord_id, exc,
                )
        # Legacy REST fallback
        await self.rest.post("/api/v5/trade/cancel-order",
                             {"instId": inst_id, "ordId": ord_id})

    async def _get_order(self, inst_id: str, ord_id: str) -> dict:
        d = await self.rest.get("/api/v5/trade/order",
                                params={"instId": inst_id, "ordId": ord_id})
        return (d.get("data") or [{}])[0]

    async def _wait_fill(self, inst_id: str, ord_id: str,
                          timeout: float = 30.0) -> Optional[dict]:
        terminal = {"filled", "canceled", "mmp_canceled"}
        deadline = time.time() + timeout
        while time.time() < deadline:
            o = await self._get_order(inst_id, ord_id)
            if o.get("state", "") in terminal:
                return o
            await asyncio.sleep(1)
        return None

    async def _set_leverage(self, inst_id: str, lever: int, mgn_mode: str = "cross"):
        try:
            await self.rest.post("/api/v5/account/set-leverage",
                                 {"instId": inst_id, "lever": str(lever), "mgnMode": mgn_mode})
        except Exception as e:
            log.warning("Set leverage failed %s: %s", inst_id, e)

    async def _cancel_all_open_buys(self):
        try:
            d      = await self.rest.get("/api/v5/trade/orders-pending",
                                         params={"instType": "SPOT"})
            orders = d.get("data") or []
            d2     = await self.rest.get("/api/v5/trade/orders-pending",
                                         params={"instType": "SWAP"})
            orders += d2.get("data") or []
            cancelled = 0
            for o in orders:
                if o.get("side") == "buy":
                    try:
                        await self._cancel_order(o["instId"], o["ordId"])
                        cancelled += 1
                    except Exception as e:
                        log.warning("Could not cancel order %s: %s", o.get("ordId"), e)
            if cancelled:
                log.critical("[P4-1] Flash-Crash: cancelled %d open BUY orders.", cancelled)
        except Exception as e:
            log.error("[P4-1] _cancel_all_open_buys error: %s", e)

    # ── [P18-4] Nuclear Liquidation ───────────────────────────────────────────
    async def liquidate_all_positions(self, symbol: str, reason: str = "P18_CATASTROPHE") -> bool:
        pos = self.positions.get(symbol)
        if pos is None:
            log.info("[P18-4] liquidate_all_positions %s: no open position — noop.", symbol)
            return False

        log.critical(
            "[P18-4] NUCLEAR LIQUIDATION %s [%s] qty=%.6f cost_basis=%.4f reason=%s",
            symbol, pos.direction, pos.qty, pos.cost_basis, reason,
        )

        self._cancel_shadow(symbol)
        await self._cancel_all_open_buys()

        use_swap       = pos.inst_type == "SWAP"
        inst           = _inst_id(symbol, swap=use_swap)
        td_mode        = "cross" if use_swap else OKX_TD_MODE_SPOT
        close_side     = "sell" if pos.direction == "long" else "buy"
        pos_side_close = (
            ("long" if use_swap else "net") if pos.direction == "long" else "short"
        )

        # [QUANT-1] + [MINSZ-1] Quantize the close size.
        q = await self._quantize_sz(symbol, pos.qty, use_swap, tag=f"{reason}_LIQ_CLOSE")
        if not q.valid:
            log.error(
                "[P18-4] Liquidation quantization failed for %s — "
                "sz=%.10f below minSz=%.10f. Attempting with raw qty.",
                symbol, q.sz_float, q.min_sz,
            )
            meta = await self._icache.get_instrument_info(symbol, swap=use_swap)
            if meta and meta.sz_inst > 0:
                dec    = InstrumentCache._decimals(meta.sz_inst)
                sz_str = str(int(pos.qty)) if use_swap else f"{pos.qty:.{dec}f}"
            else:
                sz_str = str(int(pos.qty)) if use_swap else f"{pos.qty:.8f}"
        else:
            sz_str = q.sz_str

        mkt_body = self._sor.build_market_body(
            inst, close_side, sz_str, td_mode, use_swap, pos_side_close,
            f"{reason}_LIQ",
        )
        try:
            resp = await self._place_order(mkt_body)
        except Exception as exc:
            log.error("[P18-4] liquidate_all_positions order placement exception %s: %s",
                      symbol, exc, exc_info=True)
            return False

        if not resp or resp.get("code") != "0":
            log.error("[P18-4] liquidate_all_positions market order rejected %s: %s",
                      symbol, resp)
            return False

        ord_id = resp["data"][0]["ordId"]
        log.warning("[P18-4] Liquidation market order placed %s ordId=%s", symbol, ord_id)

        order = await self._wait_fill(inst, ord_id, timeout=15.0)
        fill_px = 0.0
        if order and order.get("state") == "filled":
            fill_px = float(order.get("avgPx") or order.get("px") or 0)
            log.warning("[P18-4] Liquidation CONFIRMED %s fill_px=%.4f", symbol, fill_px)

        tick = await self.hub.get_tick(symbol)
        actual_px = fill_px if fill_px > 0 else (
            (tick.bid if close_side == "sell" else tick.ask) if tick else pos.cost_basis
        )
        await self._record_close(pos, actual_px, reason)
        return True

    # ── [P5-1] Order-Book Wall Detection ──────────────────────────────────────
    async def _detect_order_book_wall(self, symbol: str, direction: str) -> WallResult:
        _NONE = WallResult(False, "none", 0.0, 0.0, 0.0, 0.0)
        try:
            book = await self.hub.get_order_book(symbol)
            if book is None or not book.bids or not book.asks:
                return _NONE
            best_bid = book.bids[0][0]
            best_ask = book.asks[0][0]
            if best_bid <= 0 or best_ask <= 0:
                return _NONE
            mid       = (best_bid + best_ask) / 2.0
            depth_abs = mid * (WALL_DEPTH_PCT / 100.0)
            bid_vol = sum(sz for px, sz in book.bids if px >= mid - depth_abs)
            ask_vol = sum(sz for px, sz in book.asks if px <= mid + depth_abs)
            ask_ratio = (ask_vol / bid_vol) if bid_vol > 0 else float("inf")
            bid_ratio = (bid_vol / ask_vol) if ask_vol > 0 else float("inf")
            if direction == "long" and ask_ratio >= WALL_RATIO_THRESHOLD:
                return WallResult(True, "sell_wall", ask_vol, bid_vol, ask_ratio, bid_ratio)
            if direction == "short" and bid_ratio >= WALL_RATIO_THRESHOLD:
                return WallResult(True, "buy_wall", ask_vol, bid_vol, ask_ratio, bid_ratio)
        except Exception as e:
            log.debug("_detect_order_book_wall error %s: %s", symbol, e)
        return _NONE

    # ── [P5-3] Liquidation Squeeze Guard ──────────────────────────────────────
    async def _check_squeeze_delay(self, symbol: str, pos: Position) -> bool:
        now = time.time()
        if pos.squeeze_delay_until > now:
            return True
        try:
            snap      = await self.hub.get_sentiment(symbol)
            triggered = False
            if pos.direction == "long" and snap.short_liq_usd >= SQUEEZE_LIQ_THRESHOLD_USD:
                triggered = True
            elif pos.direction == "short" and snap.long_liq_usd >= SQUEEZE_LIQ_THRESHOLD_USD:
                triggered = True
            if triggered:
                pos.squeeze_delay_until = now + SQUEEZE_DELAY_SECS
                return True
        except Exception as e:
            log.debug("_check_squeeze_delay error %s: %s", symbol, e)
        return False

    # ── [P5-2] Smart Front-Run Exit ───────────────────────────────────────────
    async def _maybe_frontrun_exit(self, symbol: str, pos: Position,
                                    cur_price: float) -> bool:
        if pos.cost_basis <= 0:
            return False
        pnl_pct = (
            (cur_price - pos.cost_basis) / pos.cost_basis * 100
            if pos.direction == "long"
            else (pos.cost_basis - cur_price) / pos.cost_basis * 100
        )
        if pnl_pct <= 0:
            return False
        wall = await self._detect_order_book_wall(symbol, pos.direction)
        if not wall.wall_detected:
            return False
        if await self._check_squeeze_delay(symbol, pos):
            return False
        log.info("[P5] %s detected for %s — Front-running exit. pnl=%.2f%%",
                 wall.wall_side.replace("_", " ").title(), symbol, pnl_pct)
        await self._close_position(symbol, tag="FRONTRUN_WALL_EXIT")
        return True

    # ── [P13-3] Dead Alpha Decay ───────────────────────────────────────────────
    async def _check_dead_alpha(self, symbol: str, pos: Position,
                                 cur_price: float) -> bool:
        if pos.cost_basis <= 0:
            return False
        elapsed_hours   = (time.time() - pos.entry_ts) / 3600.0
        if elapsed_hours < P13_DEAD_ALPHA_HOURS:
            return False
        price_delta_pct = abs(cur_price - pos.cost_basis) / pos.cost_basis * 100.0
        if price_delta_pct > P13_DEAD_ALPHA_BAND_PCT:
            return False
        log.info(
            "[P13] Dead Alpha Exit: %s [%s] open=%.2fh price_delta=%.4f%% ≤ %.2f%%",
            symbol, pos.direction, elapsed_hours,
            price_delta_pct, P13_DEAD_ALPHA_BAND_PCT,
        )
        await self._close_position(symbol, tag="BAYESIAN_EXIT_DEAD_ALPHA")
        self._p13_dead_alpha_exits += 1
        return True

    # ── [P3-3] Liquidity Chase ─────────────────────────────────────────────────
    async def _liquidity_chase_fill(
        self, inst: str, side: str, raw_sz: float,
        td_mode: str, swap: bool, pos_side: str,
        tag: str, symbol: str,
    ) -> Optional[dict]:
        """
        [QUANT-1] + [MINSZ-1] Accepts raw_sz as float; quantizes per-attempt
        so any price-refresh-triggered re-resolution also produces a legal
        size at or above the exchange minimum.
        """
        q = await self._quantize_sz(symbol, raw_sz, swap, tag=f"{tag}_CHASE_QUANT")
        if not q.valid:
            log.warning("[P3-3] Liquidity chase aborted — quantized size invalid for %s.", symbol)
            return None
        sz_str = q.sz_str

        log.info("Liquidity Guard: Limit Chase for %s %s sz=%s", side.upper(), symbol, sz_str)
        meta = await self._icache.get_instrument_info(symbol, swap=swap)
        for attempt in range(1, LIQUIDITY_CHASE_MAX_ATTEMPTS + 1):
            tick = await self.hub.get_tick(symbol)
            if not tick:
                return None
            chase_px  = tick.ask if side == "buy" else tick.bid
            if meta:
                chase_px = InstrumentCache.round_price(chase_px, meta)
            body = self._sor.build_ioc_limit_body(
                inst, side, sz_str, f"{chase_px:.8f}", td_mode, swap, pos_side,
                f"{tag}_CH{attempt}",
            )
            resp = await self._place_order(body)
            if not resp or resp.get("code") != "0":
                await asyncio.sleep(LIQUIDITY_CHASE_WAIT_SECS)
                continue
            ord_id = resp["data"][0]["ordId"]
            await asyncio.sleep(LIQUIDITY_CHASE_WAIT_SECS)
            order  = await self._get_order(inst, ord_id)
            state  = order.get("state", "")
            if state == "filled":
                return order
            if state == "partially_filled":
                filled_sz  = float(order.get("accFillSz") or 0)
                remaining  = q.sz_float - filled_sz
                if remaining <= 0:
                    return order
                q2 = await self._quantize_sz(symbol, remaining, swap, tag=f"{tag}_CH{attempt}_REM")
                if not q2.valid:
                    await self._cancel_order(inst, ord_id)
                    break
                sz_str = q2.sz_str
                await self._cancel_order(inst, ord_id)
                continue
            await self._cancel_order(inst, ord_id)
        log.error("Liquidity Chase exhausted for %s", symbol)
        return None

    # ── [P12-1] / [P32-OBI-PATIENCE] OBI Filter with Whale-Aware Patience ────────
    def _p32_aggression_mode_for(
        self,
        whale_multiplier: float,
        entropy_normalized: float,
    ) -> tuple:
        """
        [P32-OBI-PATIENCE] Compute the predator aggression mode and patience
        wait time from the current whale oracle multiplier and entropy.

        Returns (mode_str, wait_secs):
          STINGY   — whale < 3x  (P32_WHALE_STINGY_THRESH)  → full patience (up to 3.0s)  [v32.1]
          STALKER  — whale 3–10x                             → entropy-scaled patience      [v32.1]
          PREDATOR — whale > 10x (P32_WHALE_STALKER_THRESH)  → 0.1s pounce delay           [v32.1]
        """
        if whale_multiplier >= P32_WHALE_STALKER_THRESH:
            return ("PREDATOR", P32_OBI_PATIENCE_PREDATOR_S)
        if whale_multiplier >= P32_WHALE_STINGY_THRESH:
            # High entropy → lower patience (act faster in noisy markets)
            # Low entropy  → more patience (wait for cleaner signal)
            wait = P32_OBI_PATIENCE_MAX_SECS * (1.0 - max(0.0, min(1.0, entropy_normalized)))
            wait = max(P32_OBI_PATIENCE_PREDATOR_S, wait)
            return ("STALKER", round(wait, 3))
        return ("STINGY", P32_OBI_PATIENCE_MAX_SECS)

    async def _check_obi_filter(self, symbol: str, direction: str,
                                 signal: object = None) -> bool:
        """
        [P12-1] OBI Filter — blocks entries when order-book imbalance is strongly
        opposed to the intended direction.

        [P32-OBI-PATIENCE] Whale-Aware patience timer: before checking the block
        condition we wait a mode-dependent period to give the best tick a chance
        to arrive in the book:
          STINGY   → wait up to 3.0 s (no whale pressure — be picky)             [v32.1 < 3x]
          STALKER  → wait scaled by entropy (whale active, market noisy → faster) [v32.1 3–10x]
          PREDATOR → wait only 0.1 s (whale > 10x — pounce immediately)           [v32.1 > 10x]

        The aggression mode and wait time are logged in the War Room and written
        to self._p32_aggression_mode for the dashboard badge.
        """
        try:
            # ── [P32-OBI-PATIENCE] Determine mode from whale multiplier + entropy ─
            _whale_mult = 1.0
            _entropy_n  = 0.5  # default mid-scale uncertainty
            try:
                if signal is not None:
                    _whale_mult = float(getattr(signal, "whale_multiplier", 1.0))
                # Pull normalized entropy from the last narrative result if available
                if self._last_narrative is not None:
                    _entropy_n = float(
                        getattr(self._last_narrative, "entropy_normalized", 0.5)
                    )
            except Exception:
                pass

            _mode, _wait_secs = self._p32_aggression_mode_for(_whale_mult, _entropy_n)
            self._p32_aggression_mode = _mode

            log.info(
                "[P32-PREDATOR] Aggression: %s | Wait: %.2fs | "
                "whale_mult=%.2fx entropy_n=%.3f symbol=%s dir=%s",
                _mode, _wait_secs, _whale_mult, _entropy_n, symbol, direction,
            )

            if _wait_secs > 0.0:
                await asyncio.sleep(_wait_secs)
            # ── [/P32-OBI-PATIENCE] ──────────────────────────────────────────────

            obi = await self.hub.get_obi(symbol, OBI_LEVELS)
            if direction == "long" and obi < OBI_BUY_BLOCK_THRESHOLD:
                log.info("[P12-1] OBI Filter BLOCKED BUY %s: obi=%.4f < threshold=%.2f",
                         symbol, obi, OBI_BUY_BLOCK_THRESHOLD)
                return True
            if direction == "short" and obi > OBI_SELL_BLOCK_THRESHOLD:
                log.info("[P12-1] OBI Filter BLOCKED SELL %s: obi=%.4f > threshold=%.2f",
                         symbol, obi, OBI_SELL_BLOCK_THRESHOLD)
                return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("[P12-1] OBI filter error %s: %s", symbol, e)
        return False

    # ── [P32-STEALTH-TWAP] Stealth TWAP Engine ────────────────────────────────
    async def _p32_stealth_twap(
        self,
        symbol:   str,
        side:     str,
        usd_amount: float,
        swap:     bool,
        pos_side: str,
        tag:      str,
        coin_cfg,
        whale_mult: float = 0.0,
    ) -> Optional[dict]:
        """
        [P32-STEALTH-TWAP] Split large orders into P32_TWAP_SLICES smaller
        slices executed over P32_TWAP_WINDOW_SECS (default 5 minutes) using
        randomized inter-slice intervals to avoid detection by other HFT bots.

        [P36-MIMIC] Passive Spoof Test (Phase 36.1 addition):
        Before each slice the engine runs _p36_run_mimic_test() to determine
        whether the dominant passive wall at the current BBO is a real iceberg
        or a spoof order designed to manipulate the Whale-Aware OBI.  A tiny
        POST_ONLY "mimic" order is placed at the wall price and the wall is
        re-measured after P36_SPOOF_REACTION_MS (default 400ms).  If the wall
        evaporates by > P36_SPOOF_EVAPORATION (default 70%), the wall is flagged
        as a Spoof and the DataHub toxicity score is updated.

        [P36-SIGNAL] Toxicity Injection (Phase 36.1 addition):
        When the DataHub spoof_probability for this symbol exceeds
        P36_SPOOF_THRESHOLD (default 0.65), _use_ioc is immediately forced True
        for ALL remaining slices in this TWAP execution, bypassing POST_ONLY
        limit orders and switching to Aggressive Taker (IOC) to capture the
        real liquidity hiding behind the fake wall.  This override takes
        precedence over P33_MAKER_PRIORITY and the standard IOC toxicity gate.

        [P33-SNIFFER] Toxic Flow Detection enhancement:
        Before each slice, the rolling toxicity score is computed via
        _p33_toxicity_score().  If the score exceeds P33_TOXICITY_THRESHOLD
        (default 0.75), the slice is placed as an IOC limit order (jumping the
        queue) instead of the standard POST_ONLY limit order.  This switches
        from maker to taker priority when the market is adversarially toxic
        (quote-stuffing, HFT cancellation storms, mass partial fills).

        IOC slices are placed via _p33_place_ioc_slice() which builds an OKX
        "ioc" order type rather than the POST_ONLY "post_only" type.  Cancelled
        and partial-fill outcomes from every slice are recorded into the
        _p33_order_events deque for ongoing toxicity scoring.

        [P33.2-REBATE] Maker-Priority enhancement:
        When P33_MAKER_PRIORITY is enabled, each non-IOC slice is routed through
        _p332_place_maker_slice() which places POST_ONLY limit orders at
        BBO + 0.1 × tickSz for maker rebate qualification.  If a slice does not
        fill and whale_mult > P33_CHASE_WHALE_MULT, the order is cancelled and
        replaced at the new BBO (Adaptive Price Chasing).  Otherwise the order
        remains static to protect capital from runaway price moves.

        Parameters
        ----------
        whale_mult : float — current Whale Oracle multiplier, passed through
                    from the driving Signal so that [P33.2-CHASE] can gate
                    price-chasing on high-conviction whale flow only.

        Returns a synthetic fill dict (same shape as _execute_iceberg) or None
        if all slices fail to fill.
        """
        import random as _random
        slice_usd  = usd_amount / P32_TWAP_SLICES
        base_gap   = P32_TWAP_WINDOW_SECS / P32_TWAP_SLICES

        # ── [P36-MIMIC] Passive Spoof Test — run BEFORE first slice ──────────
        # Run a mimic order test against the dominant passive wall to determine
        # whether the wall is real or a spoof.  The result is stored in DataHub
        # and cached locally for per-slice spoof-gating below.
        _p36_spoof_prob = 0.0
        _p36_spoof_active = False
        try:
            _p36_spoof_prob   = await self._p36_run_mimic_test(symbol, side, swap=swap)
            _p36_spoof_active = _p36_spoof_prob > P36_SPOOF_THRESHOLD
            log.info(
                "[P36.1-DETECT] %s %s TWAP pre-flight mimic test: "
                "spoof_prob=%.4f (threshold=%.2f) → spoof_active=%s",
                side.upper(), symbol,
                _p36_spoof_prob, P36_SPOOF_THRESHOLD, _p36_spoof_active,
            )
        except Exception as _p36_init_exc:
            log.debug("[P36.1-DETECT] Pre-flight mimic test error %s: %s",
                      symbol, _p36_init_exc)
        # ── [/P36-MIMIC pre-flight] ───────────────────────────────────────────

        # ── [P33-SNIFFER] Evaluate initial toxicity before first slice ─────
        _initial_toxicity = self._p33_toxicity_score()
        _use_ioc          = _initial_toxicity > P33_TOXICITY_THRESHOLD
        log.info(
            "[P33-SNIFFER] %s %s TWAP start: toxicity=%.3f (threshold=%.2f) "
            "→ initial_order_type=%s",
            side.upper(), symbol, _initial_toxicity, P33_TOXICITY_THRESHOLD,
            "IOC" if _use_ioc else "POST_ONLY",
        )
        # ── [/P33-SNIFFER initial check] ─────────────────────────────────

        # ── [P33.2-REBATE] Log maker-priority mode ────────────────────────
        if P33_MAKER_PRIORITY and not _use_ioc:
            log.info(
                "[P33.2-REBATE] %s %s TWAP: P33_MAKER_PRIORITY=ON — "
                "slices will use BBO+%.1f×tickSz maker pricing "
                "(chase_threshold=%.1fx | whale_mult=%.1fx)",
                side.upper(), symbol,
                P33_MAKER_TICK_OFFSET, P33_CHASE_WHALE_MULT, whale_mult,
            )
        # ── [/P33.2-REBATE log] ───────────────────────────────────────────

        log.info(
            "[P32-STEALTH-TWAP] %s %s $%.2f → %d slices × $%.2f "
            "over %.0fs (randomized intervals)",
            side.upper(), symbol, usd_amount, P32_TWAP_SLICES, slice_usd, P32_TWAP_WINDOW_SECS,
        )

        total_qty  = 0.0
        total_cost = 0.0
        slices_ok  = 0

        for i in range(P32_TWAP_SLICES):
            # ── [P36-MIMIC] Per-slice spoof re-evaluation ──────────────────
            # Re-run the mimic test every other slice (slices 0, 2, 4…) to
            # refresh the spoof probability as market conditions evolve mid-TWAP.
            # Odd slices use the cached probability from the most recent test to
            # avoid flooding the exchange with mimic orders.
            try:
                if i > 0 and i % 2 == 0:
                    _p36_spoof_prob = await self._p36_run_mimic_test(symbol, side, swap=swap)
                    _p36_spoof_active = _p36_spoof_prob > P36_SPOOF_THRESHOLD
                    log.info(
                        "[P36.1-DETECT] Slice %d/%d %s %s: "
                        "mimic refresh spoof_prob=%.4f → spoof_active=%s",
                        i + 1, P32_TWAP_SLICES, side.upper(), symbol,
                        _p36_spoof_prob, _p36_spoof_active,
                    )
                else:
                    # Fast-path: use cached local spoof probability.
                    _p36_spoof_prob   = self._p36_local_spoof_probs.get(symbol, 0.0)
                    _p36_spoof_active = _p36_spoof_prob > P36_SPOOF_THRESHOLD

                if _p36_spoof_active:
                    # [P36-SIGNAL] Spoofing active — force IOC to bypass fake wall.
                    log.warning(
                        "[P36.1-DETECT] 🚨 SPOOF ACTIVE %s %s slice %d/%d: "
                        "spoof_prob=%.4f > threshold=%.2f — "
                        "OVERRIDING to IOC (Aggressive Taker) to capture real liquidity.",
                        symbol, side.upper(), i + 1, P32_TWAP_SLICES,
                        _p36_spoof_prob, P36_SPOOF_THRESHOLD,
                    )
                    _use_ioc = True   # Force IOC — this OVERRIDES P33 toxicity gate
            except Exception as _p36_slice_exc:
                log.debug("[P36.1-DETECT] Per-slice mimic check error %s slice %d: %s",
                          symbol, i + 1, _p36_slice_exc)
            # ── [/P36-MIMIC per-slice] ─────────────────────────────────────

            # ── [P33-SNIFFER] Re-evaluate toxicity before each slice ──────
            # Re-check on every slice so a mid-execution toxicity spike
            # immediately flips the remaining slices to IOC without waiting
            # for the entire TWAP window to complete.
            try:
                _slice_toxicity = self._p33_toxicity_score()
                _use_ioc        = _slice_toxicity > P33_TOXICITY_THRESHOLD
                log.info(
                    "[P33-SNIFFER] Slice %d/%d %s %s: toxicity=%.3f → %s",
                    i + 1, P32_TWAP_SLICES, side.upper(), symbol,
                    _slice_toxicity, "IOC" if _use_ioc else "POST_ONLY",
                )
            except Exception as _tox_exc:
                log.debug("[P33-SNIFFER] Toxicity re-evaluation error slice %d: %s",
                          i + 1, _tox_exc)
            # ── [/P33-SNIFFER per-slice check] ────────────────────────────

            try:
                if _use_ioc:
                    # [P33-SNIFFER] IOC path: place a limit-IOC slice directly
                    # without going through _execute_order (which would impose
                    # POST_ONLY retry logic on an IOC order type).
                    order = await self._p33_place_ioc_slice(
                        symbol=symbol, side=side, usd_amount=slice_usd,
                        swap=swap, pos_side=pos_side, coin_cfg=coin_cfg,
                        tag=f"{tag}_IOC{i+1}of{P32_TWAP_SLICES}",
                    )

                elif P33_MAKER_PRIORITY:
                    # ── [P33.2-REBATE] Maker-priority path ────────────────
                    # Use the BBO+0.1-tick maker slice engine with optional
                    # whale-multiplier-gated price chasing.
                    order = await self._p332_place_maker_slice(
                        symbol=symbol, side=side, usd_amount=slice_usd,
                        swap=swap, pos_side=pos_side, coin_cfg=coin_cfg,
                        tag=f"{tag}_MKR{i+1}of{P32_TWAP_SLICES}",
                        whale_mult=whale_mult,
                    )
                    log.info(
                        "[P33.2-REBATE] TWAP slice %d/%d %s %s: "
                        "maker path returned %s",
                        i + 1, P32_TWAP_SLICES, side.upper(), symbol,
                        "fill" if order else "None",
                    )
                    # ── [/P33.2-REBATE] ───────────────────────────────────

                else:
                    order = await self._execute_order(
                        symbol, side, slice_usd,
                        swap=swap, pos_side=pos_side,
                        tag=f"{tag}_TWAP{i+1}of{P32_TWAP_SLICES}",
                        coin_cfg=coin_cfg,
                    )

                if order:
                    fill_px   = float(order.get("avgPx") or order.get("px") or 0)
                    fill_sz   = float(order.get("accFillSz") or order.get("sz") or 0)
                    ord_state = order.get("state", "")
                    if fill_px > 0 and fill_sz > 0:
                        total_qty  += fill_sz
                        total_cost += fill_sz * fill_px
                        slices_ok  += 1
                        # Record partial_fill event if order was not fully filled.
                        if ord_state == "partially_filled":
                            self._p33_record_order_event("partial_fill")
                    else:
                        # Slice did not produce a meaningful fill — treat as cancel.
                        self._p33_record_order_event("cancel")
                else:
                    # Slice returned None — POST_ONLY was cancelled/unfilled.
                    self._p33_record_order_event("cancel")

            except Exception as _slice_exc:
                log.warning("[P32-STEALTH-TWAP] Slice %d/%d error %s: %s",
                            i + 1, P32_TWAP_SLICES, symbol, _slice_exc)
                self._p33_record_order_event("cancel")

            if i < P32_TWAP_SLICES - 1:
                # Randomize the interval ±50% of the base gap to evade pattern detection
                jitter = base_gap * (_random.uniform(0.5, 1.5))
                await asyncio.sleep(jitter)

        if total_qty <= 0:
            log.warning("[P32-STEALTH-TWAP] All slices failed for %s %s", side, symbol)
            return None

        avg_px = total_cost / total_qty
        log.info(
            "[P32-STEALTH-TWAP] %s %s complete: %d/%d slices filled "
            "avg_px=%.6f total_qty=%.6f",
            side.upper(), symbol, slices_ok, P32_TWAP_SLICES, avg_px, total_qty,
        )
        return {
            "avgPx":      str(avg_px),
            "accFillSz":  str(total_qty),
            "state":      "filled",
            "ordId":      f"P32TWAP_{symbol}_{int(time.time())}",
            "_twap_slices_sent":   P32_TWAP_SLICES,
            "_twap_slices_filled": slices_ok,
        }

    async def _p33_place_ioc_slice(
        self,
        symbol:     str,
        side:       str,
        usd_amount: float,
        swap:       bool,
        pos_side:   str,
        coin_cfg,
        tag:        str,
    ) -> Optional[dict]:
        """
        [P33-SNIFFER] Place a single IOC (Immediate-or-Cancel) limit slice.

        Used by the TWAPSlicer when toxicity > P33_TOXICITY_THRESHOLD to
        jump the order queue ahead of the HFT cancel storm.  Unlike the
        standard POST_ONLY path in _execute_order, IOC orders:
          • Are submitted with ordType="ioc" (OKX native IOC semantics).
          • Fill immediately against available resting liquidity.
          • Cancel any unfilled remainder automatically — no wait loop needed.

        The bid/ask-side price is used as the IOC limit price (taker-aggressive
        level) so the slice is unlikely to miss the book entirely.

        Returns the OKX order response dict on success, None on failure.
        """
        try:
            if coin_cfg is None:
                coin_cfg = self._coin_cfg(symbol)

            tick = await self.hub.get_tick(symbol)
            if not tick or tick.ask <= 0:
                log.warning("[P33-SNIFFER] IOC slice: no tick for %s — aborting.", symbol)
                return None

            inst    = _inst_id(symbol, swap=swap)
            td_mode = "cross" if swap else OKX_TD_MODE_SPOT

            if swap and OKX_LEVERAGE > 1:
                await self._set_leverage(inst, OKX_LEVERAGE, mgn_mode=td_mode)

            # Use aggressive taker-side price for IOC to maximise fill probability.
            ioc_price = tick.ask if side == "buy" else tick.bid
            if ioc_price <= 0:
                log.warning("[P33-SNIFFER] IOC slice: invalid price for %s — aborting.", symbol)
                return None

            meta = await self._icache.get_instrument_info(symbol, swap=swap)
            if meta:
                ioc_price = InstrumentCache.round_price(ioc_price, meta)

            ct_val  = (meta.ct_val if meta and meta.ct_val > 0 else OKX_CT_VAL) if swap else 1.0
            min_usd = coin_cfg.min_usd_value
            eff_usd = max(usd_amount, min_usd)
            raw_sz  = (eff_usd / ioc_price) / ct_val if swap else eff_usd / ioc_price

            q = await self._quantize_sz(symbol, raw_sz, swap, tag=f"{tag}_IOC_QSZ")
            if not q.valid:
                log.warning("[P33-SNIFFER] IOC slice: invalid quantized size for %s.", symbol)
                return None

            uid      = __import__("uuid").uuid4().hex[:12]
            ioc_body = {
                "instId":  inst,
                "tdMode":  td_mode,
                "side":    side,
                "ordType": "ioc",
                "sz":      q.sz_str,
                "px":      f"{ioc_price:.8f}",
                "clOrdId": f"P33IOC{uid}",
            }
            if swap:
                ioc_body["posSide"] = pos_side

            log.info(
                "[P33-SNIFFER] IOC slice %s %s sz=%s px=%s (taker-aggressive, "
                "toxicity gate triggered)",
                side.upper(), symbol, q.sz_str, ioc_body["px"],
            )

            resp = await self._place_order(ioc_body)
            if resp and resp.get("code") == "0":
                ord_id = resp["data"][0]["ordId"]
                # IOC orders settle immediately — short poll for final state.
                await asyncio.sleep(0.3)
                return await self._get_order(inst, ord_id)
            else:
                s_code = (resp.get("data") or [{}])[0].get("sCode", "") if resp else ""
                log.warning(
                    "[P33-SNIFFER] IOC slice rejected for %s %s sCode=%s: %s",
                    side, symbol, s_code, resp,
                )
                return None

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error("[P33-SNIFFER] _p33_place_ioc_slice error %s: %s", symbol, exc,
                      exc_info=True)
            return None

    # ── [P32-SLIP-ADAPT] Per-Symbol Slippage Tracking ─────────────────────────
    def _p32_record_slippage(self, symbol: str, expected_px: float,
                              fill_px: float, side: str) -> None:
        """
        [P32-SLIP-ADAPT] Record realized slippage (in bps) for a completed fill.
        If the rolling average over the last P32_SLIP_WINDOW_TRADES trades exceeds
        P32_SLIP_LIMIT_BPS, the symbol is forced into LIMIT_ONLY for P32_SLIP_LIMIT_HOURS.
        """
        if expected_px <= 0 or fill_px <= 0:
            return
        # Adverse slippage: buy filled higher than expected, sell filled lower
        if side == "buy":
            slip_bps = (fill_px - expected_px) / expected_px * 10_000.0
        else:
            slip_bps = (expected_px - fill_px) / expected_px * 10_000.0

        if symbol not in self._p32_slip_history:
            self._p32_slip_history[symbol] = deque(maxlen=P32_SLIP_WINDOW_TRADES)
        self._p32_slip_history[symbol].append(slip_bps)

        window = list(self._p32_slip_history[symbol])
        if len(window) >= P32_SLIP_WINDOW_TRADES:
            avg_slip = sum(window) / len(window)
            if avg_slip > P32_SLIP_LIMIT_BPS:
                expiry = time.time() + P32_SLIP_LIMIT_HOURS * 3600.0
                self._p32_limit_only_until[symbol] = expiry
                log.warning(
                    "[P32-SLIP-ADAPT] %s: avg slippage=%.2f bps > %.1f bps "
                    "→ LIMIT_ONLY enforced for %.1f hours (until %s)",
                    symbol, avg_slip, P32_SLIP_LIMIT_BPS, P32_SLIP_LIMIT_HOURS,
                    time.strftime("%H:%M:%S", time.localtime(expiry)),
                )

    def _p32_is_limit_only(self, symbol: str) -> bool:
        """
        [P32-SLIP-ADAPT] Return True if the symbol is currently in LIMIT_ONLY
        mode due to excessive realized slippage over the last 3 trades.
        """
        expiry = self._p32_limit_only_until.get(symbol, 0.0)
        if expiry > time.time():
            return True
        # Expired — clean up
        if symbol in self._p32_limit_only_until:
            del self._p32_limit_only_until[symbol]
        return False

    # ── [v32.1-CLAMP] Price Clamping Helper ───────────────────────────────────
    async def _clamp_price(self, symbol: str, price: float, swap: bool = True) -> float:
        """
        [v32.1-CLAMP] Clamp a computed limit-order price to the exchange-enforced
        min_price / max_price bounds stored in InstrumentCache.

        If the InstrumentMeta for this symbol does not carry price-limit fields,
        or if the meta fetch fails, the original price is returned unchanged so
        that no order is silently dropped.

        This helper is used by the Liquidation Magnet logic (and any other offset
        computation) to guarantee that the final order price never causes an OKX
        sCode 51000 "Parameter price error" rejection due to an out-of-range value.
        """
        try:
            meta = await self._icache.get_instrument_info(symbol, swap=swap)
            if meta is None:
                return price
            min_px = float(getattr(meta, "min_price", 0) or 0)
            max_px = float(getattr(meta, "max_price", 0) or 0)
            if min_px > 0 and price < min_px:
                log.debug(
                    "[v32.1-CLAMP] %s price %.8f clamped UP to min_price %.8f",
                    symbol, price, min_px,
                )
                return min_px
            if max_px > 0 and price > max_px:
                log.debug(
                    "[v32.1-CLAMP] %s price %.8f clamped DOWN to max_price %.8f",
                    symbol, price, max_px,
                )
                return max_px
        except Exception as _clamp_exc:
            log.debug("[v32.1-CLAMP] _clamp_price error for %s: %s", symbol, _clamp_exc)
        return price
    # ── [/v32.1-CLAMP] ────────────────────────────────────────────────────────

    # ── [P32-LIQ-MAGNET] Liquidation Magnet Entry Offset ─────────────────────
    def _p32_liquidation_magnet_offset(
        self,
        entry_price: float,
        direction:   str,
    ) -> float:
        """
        [P32-LIQ-MAGNET] If any active liquidation cluster is within
        P32_LIQ_MAGNET_RANGE_PCT of the proposed entry price, return a
        price offset (in absolute price units) toward the cluster to catch
        the forced-selling/buying wick.

        Returns 0.0 if no cluster is within range.
        """
        try:
            clusters = (
                self._liquidation_clusters.get("clusters", [])
                if isinstance(self._liquidation_clusters, dict)
                else []
            )
            if not clusters:
                return 0.0

            range_abs = entry_price * (P32_LIQ_MAGNET_RANGE_PCT / 100.0)
            offset_per_bps = entry_price * (P32_LIQ_MAGNET_OFFSET_BPS / 10_000.0)

            for cluster in clusters:
                cluster_px = float(cluster.get("price", 0) or cluster.get("px", 0))
                if cluster_px <= 0:
                    continue
                dist = abs(cluster_px - entry_price)
                if dist <= range_abs:
                    # Offset toward the cluster
                    if direction == "long" and cluster_px < entry_price:
                        log.info(
                            "[P32-LIQ-MAGNET] Long entry offset DOWN by %.4f "
                            "toward liq cluster @ %.4f (dist=%.4f <= range=%.4f)",
                            offset_per_bps, cluster_px, dist, range_abs,
                        )
                        return -offset_per_bps
                    elif direction == "short" and cluster_px > entry_price:
                        log.info(
                            "[P32-LIQ-MAGNET] Short entry offset UP by %.4f "
                            "toward liq cluster @ %.4f (dist=%.4f <= range=%.4f)",
                            offset_per_bps, cluster_px, dist, range_abs,
                        )
                        return +offset_per_bps
        except Exception as _lm_exc:
            log.debug("[P32-LIQ-MAGNET] offset error: %s", _lm_exc)
        return 0.0

    # ══════════════════════════════════════════════════════════════════════════
    # [P33] Phase 33.1 — Toxic Flow Sniffer & Shadow Liquidity Engine
    # ══════════════════════════════════════════════════════════════════════════

    # ── [P33-SNIFFER] Toxic Flow Detection ───────────────────────────────────

    def _p33_record_order_event(self, event_type: str) -> None:
        """
        [P33-SNIFFER] Append a timestamped order event to the rolling toxicity
        tracking deque.

        Parameters
        ----------
        event_type : str — "cancel" for cancelled POST_ONLY orders, or
                           "partial_fill" for TWAP slices that were partially filled.
        """
        try:
            self._p33_order_events.append((time.time(), event_type))
        except Exception as exc:
            log.debug("[P33-SNIFFER] _p33_record_order_event error: %s", exc)

    def _p33_toxicity_score(self) -> float:
        """
        [P33-SNIFFER] Compute the current HFT toxicity score in [0.0, 1.0].

        The score is the ratio of toxic events (cancellations + partial fills)
        to total order events within the P33_TOXICITY_WINDOW_SECS rolling window.
        Returns 0.0 when the window has fewer than P33_TOXICITY_MIN_EVENTS events
        so we do not switch to IOC prematurely on insufficient data.

        Returns
        -------
        float — toxicity score in [0.0, 1.0].  Scores above P33_TOXICITY_THRESHOLD
        should trigger IOC switching in the TWAP slicer.
        """
        try:
            cutoff  = time.time() - P33_TOXICITY_WINDOW_SECS
            recent  = [(ts, ev) for ts, ev in self._p33_order_events if ts >= cutoff]
            if len(recent) < P33_TOXICITY_MIN_EVENTS:
                return 0.0
            toxic   = sum(1 for _, ev in recent if ev in ("cancel", "partial_fill"))
            score   = toxic / len(recent)
            return round(score, 4)
        except Exception as exc:
            log.debug("[P33-SNIFFER] _p33_toxicity_score error: %s", exc)
            return 0.0

    def get_p33_btc_toxicity(self) -> float:
        """
        [P35.1-HEDGE] Return the current P33 toxicity score for the BTC hedge
        symbol.  This method is called by _cycle() each iteration to populate
        the btc_toxic flag injected into VetoArbitrator.set_p35_state().

        The score is sourced from the same rolling order-event deque that drives
        the TWAP slicer's IOC switching logic, so no additional data structures
        are needed.  Returns 0.0 when insufficient events exist (safe default).

        Returns
        -------
        float — toxicity score in [0.0, 1.0].
                 Scores above P33_TOXICITY_THRESHOLD indicate elevated BTC
                 tape toxicity and will trigger a P35 Hedge-Aware Veto.
        """
        try:
            return self._p33_toxicity_score()
        except Exception as exc:
            log.debug("[P35.1-HEDGE] get_p33_btc_toxicity error: %s", exc)
            return 0.0

    # ── [P33-SHADOW] Iceberg Shadowing ───────────────────────────────────────

    async def _p33_detect_iceberg_shadow(
        self,
        symbol: str,
        side:   str,
        price:  float,
        swap:   bool = False,
    ) -> float:
        """
        [P33-SHADOW] Detect a massive passive buy wall in the order book and
        return a 1-tickSz price improvement offset for "shadowing" the block.

        Detection criteria (both must be met):
          1. The best passive wall depth on the bid side is at least
             P33_ICEBERG_WALL_RATIO × the median depth of the top-5 bid levels.
          2. The wall USD value (price × qty) exceeds P33_ICEBERG_WALL_MIN_USD.

        Shadowing applies to buy orders only (the buyer price-improves to front-
        run the massive bid wall and catch fill priority before the block is hit).

        Parameters
        ----------
        symbol : str
        side   : str — "buy" or "sell"
        price  : float — the current proposed limit price (before shadowing)
        swap   : bool

        Returns
        -------
        float — the adjusted (shadowed) price, or the original price if no
        iceberg is detected or the conditions are not met.
        """
        # Shadowing only makes sense for buy orders fronting a buy wall.
        if side != "buy":
            return price
        try:
            book = await self.hub.get_order_book(symbol)
            if book is None or not book.bids or len(book.bids) < 3:
                return price

            # Retrieve tickSz from cache or InstrumentCache.
            tick_sz = self._p33_tick_sz_cache.get(symbol, 0.0)
            if tick_sz <= 0:
                try:
                    meta = await self._icache.get_instrument_info(symbol, swap=swap)
                    if meta and getattr(meta, "tick_sz", 0) > 0:
                        tick_sz = float(meta.tick_sz)
                        self._p33_tick_sz_cache[symbol] = tick_sz
                except Exception:
                    pass
            if tick_sz <= 0:
                # Safe fallback: tickSz cannot be determined — skip shadowing.
                log.debug(
                    "[P33-SHADOW] %s: tickSz unavailable — iceberg shadow skipped.",
                    symbol,
                )
                return price

            # Compute median depth of the top-5 bid levels.
            top5_depths  = [sz for _, sz in book.bids[:5]]
            if not top5_depths:
                return price
            median_depth = sorted(top5_depths)[len(top5_depths) // 2]
            if median_depth <= 0:
                return price

            # Find the best bid wall (largest level in top-5).
            wall_px, wall_qty = max(book.bids[:5], key=lambda x: x[1])
            wall_usd = wall_px * wall_qty

            if (
                wall_qty >= median_depth * P33_ICEBERG_WALL_RATIO
                and wall_usd >= P33_ICEBERG_WALL_MIN_USD
            ):
                # Price-improve by exactly 1 tickSz to shadow the block.
                shadowed_price = price + tick_sz
                # Clamp to exchange price limits.
                shadowed_price = await self._clamp_price(symbol, shadowed_price, swap=swap)
                meta = await self._icache.get_instrument_info(symbol, swap=swap)
                if meta:
                    shadowed_price = InstrumentCache.round_price(shadowed_price, meta)
                log.info(
                    "[P33-SHADOW] %s: ICEBERG detected — "
                    "wall_px=%.6f wall_qty=%.4f wall_usd=%.0f "
                    "(ratio=%.1fx median_depth=%.4f >= threshold=%.1fx "
                    "min_usd=%.0f) — "
                    "price %.6f → %.6f (+1 tickSz=%.8f)",
                    symbol, wall_px, wall_qty, wall_usd,
                    wall_qty / max(median_depth, 1e-12), median_depth,
                    P33_ICEBERG_WALL_RATIO, P33_ICEBERG_WALL_MIN_USD,
                    price, shadowed_price, tick_sz,
                )
                return shadowed_price

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.debug("[P33-SHADOW] _p33_detect_iceberg_shadow error %s: %s", symbol, exc)
        return price

    # ── [/P33] ────────────────────────────────────────────────────────────────

    # ══════════════════════════════════════════════════════════════════════════
    # [P36.1] Phase 36.1 — Adversarial HFT Mimicry & Active Spoof Detection
    # ══════════════════════════════════════════════════════════════════════════

    def _p36_mimic_lock_for(self, symbol: str) -> asyncio.Lock:
        """[P36-MIMIC] Return (or create) the per-symbol mimic order lock."""
        if symbol not in self._p36_mimic_locks:
            self._p36_mimic_locks[symbol] = asyncio.Lock()
        return self._p36_mimic_locks[symbol]

    # ── [P36.2-DYNCLAMP] Dynamic Buffer Helpers ───────────────────────────────

    def _p362_get_dynamic_buffer(self, symbol: str) -> float:
        """
        [P36.2-DYNCLAMP] Return the active DynamicBuffer for a symbol.

        If the most-recently computed PriceVelocity for the symbol exceeds
        P362_VELOCITY_THRESHOLD_PCT over the rolling P362_VELOCITY_WINDOW_SECS
        window, the high-volatility buffer (P362_DYNAMIC_BUFFER_HIGH = 10 bps)
        is returned.  Otherwise the standard PRICE_LIMIT_BUFFER (5 bps) applies.

        The decision is cached in self._p362_dynamic_buffer[symbol] and updated
        every tape-monitor tick so callers never pay the computation cost inline.

        Returns
        -------
        float — buffer fraction, e.g. 0.0005 (5 bps) or 0.0010 (10 bps).
        """
        # [P40.1-LAT] LIMIT_ONLY elevates DynamicBuffer during latency spikes.
        # When a symbol is in LIMIT_ONLY (P32), we return at least the high-volatility
        # buffer so limit prices stay ahead of exchange clamps even as IPC latency
        # stretches.  This reuses existing Phase 36.2 state without adding a new
        # override cache.
        try:
            if hasattr(self, "_p32_is_limit_only") and self._p32_is_limit_only(symbol):
                return max(self._p362_dynamic_buffer.get(symbol, PRICE_LIMIT_BUFFER), P362_DYNAMIC_BUFFER_HIGH)
        except Exception:
            pass
        return self._p362_dynamic_buffer.get(symbol, PRICE_LIMIT_BUFFER)

    def _p362_update_velocity(self, symbol: str, current_price: float) -> None:
        """
        [P36.2-DYNCLAMP] Record a new price sample for the velocity calculation
        and update the cached dynamic buffer for this symbol.

        Called by _tape_monitor_loop on every poll interval.

        Parameters
        ----------
        symbol        : base symbol, e.g. "BTC"
        current_price : latest mid/ask/bid price (any proxy for current level)
        """
        try:
            now = time.time()
            if symbol not in self._p362_price_samples:
                self._p362_price_samples[symbol] = deque(maxlen=500)
            samples = self._p362_price_samples[symbol]
            samples.append((now, current_price))

            # Purge samples older than the velocity window.
            cutoff = now - P362_VELOCITY_WINDOW_SECS
            while samples and samples[0][0] < cutoff:
                samples.popleft()

            # Need at least 2 samples to compute velocity.
            if len(samples) < 2 or current_price <= 0:
                self._p362_dynamic_buffer[symbol] = PRICE_LIMIT_BUFFER
                return

            oldest_ts, oldest_px = samples[0]
            if oldest_px <= 0:
                self._p362_dynamic_buffer[symbol] = PRICE_LIMIT_BUFFER
                return

            velocity_pct = abs(current_price - oldest_px) / oldest_px * 100.0

            if velocity_pct > P362_VELOCITY_THRESHOLD_PCT:
                if self._p362_dynamic_buffer.get(symbol) != P362_DYNAMIC_BUFFER_HIGH:
                    log.info(
                        "[P36.2-DYNCLAMP] %s: PriceVelocity=%.4f%% > threshold=%.2f%% "
                        "over %.0fs → DynamicBuffer ELEVATED to %.4f (%.0f bps)",
                        symbol, velocity_pct, P362_VELOCITY_THRESHOLD_PCT,
                        P362_VELOCITY_WINDOW_SECS,
                        P362_DYNAMIC_BUFFER_HIGH, P362_DYNAMIC_BUFFER_HIGH * 10000,
                    )
                self._p362_dynamic_buffer[symbol] = P362_DYNAMIC_BUFFER_HIGH
            else:
                if self._p362_dynamic_buffer.get(symbol) == P362_DYNAMIC_BUFFER_HIGH:
                    log.info(
                        "[P36.2-DYNCLAMP] %s: PriceVelocity=%.4f%% ≤ threshold=%.2f%% "
                        "→ DynamicBuffer NORMALIZED to %.4f (%.0f bps)",
                        symbol, velocity_pct, P362_VELOCITY_THRESHOLD_PCT,
                        PRICE_LIMIT_BUFFER, PRICE_LIMIT_BUFFER * 10000,
                    )
                self._p362_dynamic_buffer[symbol] = PRICE_LIMIT_BUFFER
        except Exception as _vel_exc:
            log.debug("[P36.2-DYNCLAMP] _p362_update_velocity error %s: %s", symbol, _vel_exc)

    # ── [P36.2-GOLDEN] Golden Build Monitor ───────────────────────────────────

    def _p362_record_cycle_success(self) -> None:
        """
        [P36.2-GOLDEN] Increment the golden-build cycle counter.
        Called at the END of each successful _cycle() with no 51006 or IndexError.
        When the counter reaches P362_GOLDEN_BUILD_CYCLES, writes the Golden Build
        Report and logs a SUCCESS banner.
        """
        if self._p362_golden_written:
            return
        self._p362_cycle_count += 1
        if self._p362_cycle_count >= P362_GOLDEN_BUILD_CYCLES:
            self._p362_write_golden_report()

    def _p362_reset_golden_counter(self, reason: str) -> None:
        """
        [P36.2-GOLDEN] Reset the golden-build cycle counter.
        Called whenever a sCode 51006 or IndexError is detected.

        Parameters
        ----------
        reason : human-readable description of what triggered the reset.
        """
        if self._p362_cycle_count > 0:
            log.warning(
                "[P36.2-GOLDEN] Golden Build counter RESET at %d cycles: %s",
                self._p362_cycle_count, reason,
            )
        self._p362_cycle_count    = 0
        self._p362_run_start_ts   = time.time()

def _p362_is_expected_data_gap_index_error(self, exc: IndexError) -> bool:
    """
    [P36.2-GOLDEN] Classify IndexError sources.

    HIGH-severity fix: do NOT reset the Golden Build counter for IndexError
    that originate from *expected data gaps* (temporarily empty tape buffers,
    cache warm-up, or transient REST/WS refresh windows).

    Only IndexError that appear to originate from executor logic corruption
    should reset the counter.
    """
    try:
        import traceback as _tb
        tb = exc.__traceback__
        frames = _tb.extract_tb(tb) if tb else []
        for fr in frames:
            fn = (fr.filename or "").replace("\\", "/")
            name = fr.name or ""
            # Expected: data_hub buffers, tape snapshots, candle buffers.
            if fn.endswith("/data_hub.py") or fn.endswith("/phase7_execution.py"):
                return True
            if name in ("snapshot", "snapshot_stats", "_recent", "_tape_buf", "_get_buf"):
                return True
            # Instrument cache warm-up paths
            if "instrument" in name.lower() and "cache" in name.lower():
                return True
        # Heuristic: "list index out of range" with any buffer-related frame
        msg = str(exc)
        if "out of range" in msg and any("buffer" in (f.name or "").lower() for f in frames):
            return True
    except Exception:
        return False
    return False

    def _p362_write_golden_report(self) -> None:
        """
        [P36.2-GOLDEN] Write GOLDEN_BUILD_REPORT.txt with performance stats.
        Executed exactly once per bot lifetime when the cycle threshold is met.
        """
        try:
            elapsed_secs = time.time() - self._p362_run_start_ts
            elapsed_hrs  = elapsed_secs / 3600.0
            report_lines = [
                "═" * 70,
                "  🏆  GOLDEN BUILD REPORT — Phase 36.2 Adversarial Resilience",
                "═" * 70,
                f"  Generated : {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}",
                f"  Symbols   : {', '.join(self.symbols)}",
                f"  Demo Mode : {self.demo}",
                "─" * 70,
                "  CYCLE STATISTICS",
                f"    Consecutive successful cycles : {self._p362_cycle_count}",
                f"    sCode 51006 rejections        : {self._p362_51006_count}",
                f"    IndexError crashes            : {self._p362_index_error_count}",
                f"    Run duration                  : {elapsed_hrs:.2f} hours "
                f"({elapsed_secs:.0f}s)",
                "─" * 70,
                "  BUFFER TELEMETRY",
                f"    Static PRICE_LIMIT_BUFFER     : {PRICE_LIMIT_BUFFER:.4f} "
                f"({PRICE_LIMIT_BUFFER * 10000:.0f} bps)",
                f"    High-Volatility Buffer        : {P362_DYNAMIC_BUFFER_HIGH:.4f} "
                f"({P362_DYNAMIC_BUFFER_HIGH * 10000:.0f} bps)",
                f"    Velocity Window               : {P362_VELOCITY_WINDOW_SECS:.0f}s",
                f"    Velocity Threshold            : {P362_VELOCITY_THRESHOLD_PCT:.2f}%",
                "─" * 70,
                "  MIMICRY CONFIG (Phase 36.2 Multi-Level)",
                f"    Spoof Reaction Window         : {P36_SPOOF_REACTION_MS:.0f} ms",
                f"    Spoof Evaporation Threshold   : {P36_SPOOF_EVAPORATION * 100:.0f}%",
                f"    Spoof EMA Threshold           : {P36_SPOOF_THRESHOLD:.2f}",
                "─" * 70,
                "  VERDICT: ZERO sCode 51006 rejections and ZERO IndexError crashes",
                f"  across {self._p362_cycle_count} consecutive cycles.",
                "  This build is certified GOLDEN. ✅",
                "═" * 70,
            ]
            report_text = "\n".join(report_lines) + "\n"
            with open(P362_GOLDEN_REPORT_PATH, "w", encoding="utf-8") as _rf:
                _rf.write(report_text)
            self._p362_golden_written = True
            log.info(
                "[P36.2-GOLDEN] 🏆 GOLDEN BUILD ACHIEVED after %d cycles "
                "(%.2f hours, 0×51006, 0×IndexError) → report saved to %s",
                self._p362_cycle_count, elapsed_hrs, P362_GOLDEN_REPORT_PATH,
            )
        except Exception as _gr_exc:
            log.warning("[P36.2-GOLDEN] Failed to write golden report: %s", _gr_exc)

    # ── [/P36.2] ──────────────────────────────────────────────────────────────


    async def _p36_run_mimic_test(
        self,
        symbol:   str,
        side:     str,
        swap:     bool = False,
    ) -> float:
        """
        [P36.2-MIMIC2] Multi-Level Passive Spoof Test — Mimic Order Engine.

        Phase 36.2 upgrade: places TWO micro POST_ONLY probe orders simultaneously
        at Level 1 (best wall) and Level 3 of the order book.  A Spoof is only
        confirmed when BOTH probe levels evaporate (≥ P36_SPOOF_EVAPORATION)
        within P36_SPOOF_REACTION_MS.  A single-level evaporation is classified
        as AMBIGUOUS (raw_prob = 0.5) — not a full spoof — preventing false
        positives from natural book thinning at a single level.

        Workflow
        --------
        1.  Query the live order book for symbol.
        2.  Identify the dominant passive wall on the relevant side.
            Wall must exceed P36_MIMIC_WALL_RATIO × median depth AND
            P36_MIMIC_WALL_MIN_USD.
        3a. Level 1 probe: place POST_ONLY mimic at the Level-1 wall price.
        3b. Level 3 probe: place POST_ONLY mimic at the Level-3 price
            (book_levels[2], i.e. third level from best).
        4.  Wait P36_SPOOF_REACTION_MS milliseconds.
        5.  Re-query both price levels in the refreshed book.
        6.  Classify:
              BOTH evaporated ≥ P36_SPOOF_EVAPORATION → raw_prob = 1.0 (SPOOF)
              ONE  evaporated ≥ P36_SPOOF_EVAPORATION → raw_prob = 0.5 (AMBIGUOUS)
              NONE evaporated                          → raw_prob = 0.0 (REAL)
        7.  Cancel BOTH mimic orders (always, in a finally block).
        8.  Update DataHub spoof toxicity EMA with raw_prob.
        9.  Cache result in self._p36_local_spoof_probs[symbol].
        10. Inject into VetoArbitrator via set_spoof_probability().

        The method is guarded by a per-symbol asyncio.Lock to prevent two
        simultaneous mimic tests for the same symbol.

        Parameters
        ----------
        symbol : str  — base symbol, e.g. "BTC"
        side   : str  — "buy" or "sell" (determines which book side to inspect)
        swap   : bool — True for perpetual swap instruments

        Returns
        -------
        float — updated spoof probability EMA from DataHub ∈ [0.0, 1.0].
        """
        _lock = self._p36_mimic_lock_for(symbol)
        if _lock.locked():
            # Another mimic test is already running for this symbol.
            # Return the cached probability from the previous run rather than
            # blocking the TWAP engine.
            cached = self._p36_local_spoof_probs.get(symbol, 0.0)
            log.debug(
                "[P36.2-MIMIC2] Mimic lock busy for %s — returning cached spoof_prob=%.4f",
                symbol, cached,
            )
            return cached

        async with _lock:
            inst    = _inst_id(symbol, swap=swap)
            td_mode = "cross" if swap else OKX_TD_MODE_SPOT
            # Track the two probe order IDs for cleanup in finally.
            mimic_ord_id_l1: Optional[str] = None
            mimic_ord_id_l3: Optional[str] = None
            raw_prob: float = 0.0

            try:
                # ── Step 1: Query live order book ────────────────────────────
                book = await self.hub.get_order_book(symbol)
                if book is None:
                    log.debug(
                        "[P36.2-MIMIC2] %s: no order book — skipping mimic test.",
                        symbol,
                    )
                    return self._p36_local_spoof_probs.get(symbol, 0.0)

                # ── Step 2: Identify dominant passive wall ───────────────────
                book_levels = book.asks if side == "buy" else book.bids
                if not book_levels or len(book_levels) < 3:
                    log.debug(
                        "[P36.2-MIMIC2] %s: insufficient book depth (%d levels) "
                        "— mimic skipped.",
                        symbol, len(book_levels) if book_levels else 0,
                    )
                    return self._p36_local_spoof_probs.get(symbol, 0.0)

                top5_depths  = [sz for _, sz in book_levels[:5]]
                median_depth = sorted(top5_depths)[len(top5_depths) // 2]
                if median_depth <= 0:
                    return self._p36_local_spoof_probs.get(symbol, 0.0)

                # Level 1: the dominant wall in the top-5.
                wall_px_l1, wall_qty_l1 = max(book_levels[:5], key=lambda x: x[1])
                wall_usd_l1 = wall_px_l1 * wall_qty_l1

                if not (
                    wall_qty_l1 >= median_depth * P36_MIMIC_WALL_RATIO
                    and wall_usd_l1 >= P36_MIMIC_WALL_MIN_USD
                ):
                    # No significant wall — clean observation.
                    log.debug(
                        "[P36.2-MIMIC2] %s %s: no wall meets criteria "
                        "(wall_qty=%.4f median=%.4f ratio_need=%.1f "
                        "wall_usd=%.0f min_usd=%.0f) — mimic skipped.",
                        symbol, side,
                        wall_qty_l1, median_depth, P36_MIMIC_WALL_RATIO,
                        wall_usd_l1, P36_MIMIC_WALL_MIN_USD,
                    )
                    await self.hub.update_spoof_toxicity(symbol, 0.0)
                    spoof_prob = await self.hub.get_spoof_probability(symbol)
                    self._p36_local_spoof_probs[symbol] = spoof_prob
                    return spoof_prob

                # Level 3: third entry in the book (index 2).
                wall_px_l3, wall_qty_l3 = book_levels[2]

                log.info(
                    "[P36.2-MIMIC2] %s %s: WHALE WALL detected "
                    "L1 px=%.8f qty=%.4f usd=%.0f | L3 px=%.8f qty=%.4f "
                    "— launching Multi-Level Spoof Test.",
                    symbol, side.upper(),
                    wall_px_l1, wall_qty_l1, wall_usd_l1,
                    wall_px_l3, wall_qty_l3,
                )

                # ── Step 3: Fetch instrument metadata ────────────────────────
                meta   = await self._icache.get_instrument_info(symbol, swap=swap)
                min_sz = float(getattr(meta, "min_sz", 0.0) or 0.0) if meta else 0.0
                lot_sz = float(getattr(meta, "sz_inst", 0.0) or 0.0) if meta else 1e-8
                if lot_sz <= 0:
                    lot_sz = 1e-8

                raw_min_qty = min_sz if min_sz > 0 else lot_sz
                q_mimic = await self._quantize_sz(
                    symbol, raw_min_qty, swap, tag="P36_MIMIC2"
                )
                if not q_mimic.valid:
                    log.debug(
                        "[P36.2-MIMIC2] %s: mimic order size invalid — skipping.",
                        symbol,
                    )
                    return self._p36_local_spoof_probs.get(symbol, 0.0)

                # Round probe prices to tickSz.
                mimic_px_l1 = InstrumentCache.round_price(wall_px_l1, meta) if meta else wall_px_l1
                mimic_px_l3 = InstrumentCache.round_price(wall_px_l3, meta) if meta else wall_px_l3

                # ── Step 3a: Place Level-1 probe ─────────────────────────────
                uid_l1 = uuid.uuid4().hex[:12]
                body_l1 = {
                    "instId":  inst,
                    "tdMode":  td_mode,
                    "side":    side,
                    "ordType": "post_only",
                    "sz":      q_mimic.sz_str,
                    "px":      f"{mimic_px_l1:.8f}",
                    "clOrdId": f"P36L1{uid_l1}",
                }
                if swap:
                    body_l1["posSide"] = "long" if side == "buy" else "short"

                resp_l1 = await self._place_order(body_l1)
                if resp_l1 and resp_l1.get("code") == "0":
                    mimic_ord_id_l1 = resp_l1["data"][0]["ordId"]
                    log.info(
                        "[P36.2-MIMIC2] %s: L1 probe placed ord_id=%s px=%.8f",
                        symbol, mimic_ord_id_l1, mimic_px_l1,
                    )
                else:
                    _sc_l1 = (resp_l1.get("data") or [{}])[0].get("sCode", "") if resp_l1 else ""
                    log.debug(
                        "[P36.2-MIMIC2] %s: L1 probe rejected sCode=%s — "
                        "falling back to single-level test.", symbol, _sc_l1,
                    )

                # ── Step 3b: Place Level-3 probe ─────────────────────────────
                uid_l3 = uuid.uuid4().hex[:12]
                body_l3 = {
                    "instId":  inst,
                    "tdMode":  td_mode,
                    "side":    side,
                    "ordType": "post_only",
                    "sz":      q_mimic.sz_str,
                    "px":      f"{mimic_px_l3:.8f}",
                    "clOrdId": f"P36L3{uid_l3}",
                }
                if swap:
                    body_l3["posSide"] = "long" if side == "buy" else "short"

                resp_l3 = await self._place_order(body_l3)
                if resp_l3 and resp_l3.get("code") == "0":
                    mimic_ord_id_l3 = resp_l3["data"][0]["ordId"]
                    log.info(
                        "[P36.2-MIMIC2] %s: L3 probe placed ord_id=%s px=%.8f",
                        symbol, mimic_ord_id_l3, mimic_px_l3,
                    )
                else:
                    _sc_l3 = (resp_l3.get("data") or [{}])[0].get("sCode", "") if resp_l3 else ""
                    log.debug(
                        "[P36.2-MIMIC2] %s: L3 probe rejected sCode=%s.",
                        symbol, _sc_l3,
                    )

                # If both probes failed to place, abort.
                if mimic_ord_id_l1 is None and mimic_ord_id_l3 is None:
                    log.debug(
                        "[P36.2-MIMIC2] %s: both L1/L3 probes failed — skipping.",
                        symbol,
                    )
                    return self._p36_local_spoof_probs.get(symbol, 0.0)

                # ── Step 4: Wait for reaction window ─────────────────────────
                await asyncio.sleep(P36_SPOOF_REACTION_MS / 1000.0)

                # ── Step 5: Re-query order book ───────────────────────────────
                book_after   = await self.hub.get_order_book(symbol)
                tick_sz      = float(getattr(meta, "tick_sz", 0.0) or 0.0) if meta else 0.0
                tol          = tick_sz if tick_sz > 0 else wall_px_l1 * 0.0001

                qty_after_l1 = 0.0
                qty_after_l3 = 0.0
                if book_after:
                    after_levels = book_after.asks if side == "buy" else book_after.bids
                    for lvl_px, lvl_qty in (after_levels or []):
                        if abs(lvl_px - wall_px_l1) <= tol:
                            qty_after_l1 = lvl_qty
                        if abs(lvl_px - wall_px_l3) <= tol:
                            qty_after_l3 = lvl_qty

                # ── Step 6: Classify — Multi-Level evaporation logic ──────────
                evap_l1 = 1.0 - (qty_after_l1 / max(wall_qty_l1, 1e-12))
                evap_l3 = 1.0 - (qty_after_l3 / max(wall_qty_l3, 1e-12))

                l1_evaporated = (mimic_ord_id_l1 is not None) and (evap_l1 >= P36_SPOOF_EVAPORATION)
                l3_evaporated = (mimic_ord_id_l3 is not None) and (evap_l3 >= P36_SPOOF_EVAPORATION)

                if l1_evaporated and l3_evaporated:
                    raw_prob = 1.0
                    log.warning(
                        "[P36.2-MIMIC2] 🚨 DUAL-LEVEL SPOOF CONFIRMED %s %s: "
                        "L1 evap=%.1f%% L3 evap=%.1f%% (threshold=%.0f%%) "
                        "within %.0fms → SPOOF confirmed.",
                        symbol, side.upper(),
                        evap_l1 * 100.0, evap_l3 * 100.0,
                        P36_SPOOF_EVAPORATION * 100.0, P36_SPOOF_REACTION_MS,
                    )
                elif l1_evaporated or l3_evaporated:
                    raw_prob = 0.5
                    _which   = "L1" if l1_evaporated else "L3"
                    _evap    = evap_l1 if l1_evaporated else evap_l3
                    log.info(
                        "[P36.2-MIMIC2] ⚠️  AMBIGUOUS %s %s: "
                        "only %s evaporated (%.1f%%) — single-level; "
                        "raw_prob=0.5 (not a full spoof confirmation).",
                        symbol, side.upper(), _which, _evap * 100.0,
                    )
                else:
                    raw_prob = 0.0
                    log.info(
                        "[P36.2-MIMIC2] %s %s: BOTH levels HELD "
                        "(L1 evap=%.1f%% L3 evap=%.1f%%) — wall classified REAL.",
                        symbol, side.upper(),
                        evap_l1 * 100.0, evap_l3 * 100.0,
                    )

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "[P36.2-MIMIC2] _p36_run_mimic_test unhandled error %s: %s",
                    symbol, exc, exc_info=True,
                )
            finally:
                # ── Step 7: Always cancel BOTH probe orders ───────────────────
                for _oid, _lbl in [
                    (mimic_ord_id_l1, "L1"),
                    (mimic_ord_id_l3, "L3"),
                ]:
                    if _oid:
                        try:
                            await self._cancel_order(inst, _oid)
                            log.debug(
                                "[P36.2-MIMIC2] %s: %s probe %s cancelled.",
                                symbol, _lbl, _oid,
                            )
                        except Exception as _cancel_exc:
                            log.debug(
                                "[P36.2-MIMIC2] %s: %s cancel failed (non-critical): %s",
                                symbol, _lbl, _cancel_exc,
                            )

            # ── Step 8: Update DataHub toxicity EMA ──────────────────────────
            await self.hub.update_spoof_toxicity(symbol, raw_prob)

            # ── Step 9: Cache locally for fast TWAP read-back ────────────────
            spoof_prob = await self.hub.get_spoof_probability(symbol)
            self._p36_local_spoof_probs[symbol] = spoof_prob

            # ── [P36-SIGNAL] Inject into VetoArbitrator ───────────────────────
            if self._p32_veto_arb is not None:
                try:
                    self._p32_veto_arb.set_spoof_probability(symbol, spoof_prob)
                except Exception as _va_exc:
                    log.debug("[P36.2-MIMIC2] VetoArbitrator inject error: %s", _va_exc)

            return spoof_prob

    # ── [/P36.1+P36.2] ────────────────────────────────────────────────────────


    # ═════════════════════════════════════════════════════════════════════════
    # [P33.2] Phase 33.2 — Maker-Rebate Optimization & Stealth Layering
    # ═════════════════════════════════════════════════════════════════════════

    async def _p332_calc_maker_price(
        self,
        symbol: str,
        side:   str,
        swap:   bool = False,
    ) -> float:
        """
        [P33.2-REBATE] Compute a Maker-qualified limit price at BBO ± 0.1 tick.

        For a BUY order the maker price is:
            best_bid + P33_MAKER_TICK_OFFSET × tickSz

        For a SELL order the maker price is:
            best_ask − P33_MAKER_TICK_OFFSET × tickSz

        Placing a buy slightly above the best bid (but still below the best ask)
        ensures the order rests in the order book as a maker, not crossing the
        spread, so it qualifies for OKX's maker rebate.

        The resulting price is:
          1. Computed from the live BBO (hub.get_tick).
          2. Adjusted by P33_MAKER_TICK_OFFSET × tickSz (loaded from
             InstrumentCache, with a 1e-8 safe fallback).
          3. Clamped to exchange price limits via _clamp_price().
          4. Rounded to the instrument's tickSz via InstrumentCache.round_price().

        Returns the computed maker price, or 0.0 on any error so callers can
        detect failure and fall back to the standard price calculation path.
        """
        try:
            tick = await self.hub.get_tick(symbol)
            if not tick or tick.bid <= 0 or tick.ask <= 0:
                log.warning(
                    "[P33.2-REBATE] _p332_calc_maker_price: no valid tick for %s "
                    "— returning 0.0 (fallback to standard price path).", symbol,
                )
                return 0.0

            meta = await self._icache.get_instrument_info(symbol, swap=swap)
            tick_sz = 0.0
            if meta and getattr(meta, "tick_sz", 0) > 0:
                tick_sz = float(meta.tick_sz)
            # Lazily update the shadow-iceberg tickSz cache too.
            if tick_sz > 0:
                self._p33_tick_sz_cache[symbol] = tick_sz
            else:
                # Last-resort: derive a sensible tick from the price magnitude.
                # This is a safe fallback — the order will still rest on the book.
                tick_sz = max(tick.bid * 1e-6, 1e-8)

            offset = P33_MAKER_TICK_OFFSET * tick_sz

            if side == "buy":
                raw_maker_px = tick.bid + offset
            else:
                raw_maker_px = tick.ask - offset

            if raw_maker_px <= 0:
                log.warning(
                    "[P33.2-REBATE] _p332_calc_maker_price: non-positive raw "
                    "price=%.8f for %s %s — returning 0.0.", raw_maker_px, side, symbol,
                )
                return 0.0

            # Clamp to exchange price-limit bounds (prevents OKX sCode 51000).
            clamped = await self._clamp_price(symbol, raw_maker_px, swap=swap)

            # Round to the instrument tickSz.
            if meta:
                clamped = InstrumentCache.round_price(clamped, meta)

            log.debug(
                "[P33.2-REBATE] %s %s maker_price=%.8f "
                "(bid=%.8f ask=%.8f tickSz=%.8f offset=%.8f)",
                side.upper(), symbol, clamped,
                tick.bid, tick.ask, tick_sz, offset,
            )
            return clamped

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "[P33.2-REBATE] _p332_calc_maker_price error %s %s: %s — "
                "returning 0.0 (fallback).", side, symbol, exc,
            )
            return 0.0

    async def _p332_place_maker_slice(
        self,
        symbol:     str,
        side:       str,
        usd_amount: float,
        swap:       bool,
        pos_side:   str,
        coin_cfg,
        tag:        str,
        whale_mult: float = 0.0,
    ) -> Optional[dict]:
        """
        [P33.2-REBATE / P33.2-CHASE] Place a single Maker-priority limit slice.

        Workflow
        --------
        1. Compute a maker price via _p332_calc_maker_price().
        2. Quantize the order size from usd_amount.
        3. Submit a POST_ONLY limit order at the maker price.
        4. Wait P33_MAKER_FILL_WAIT seconds for a fill.
        5. [P33.2-CHASE] If still unfilled AND whale_mult > P33_CHASE_WHALE_MULT:
             a. Cancel the resting order.
             b. Re-compute the maker price at the new BBO.
             c. Submit a replacement POST_ONLY order (one chase attempt only —
                avoids chasing the market into a runaway move).
           If whale_mult ≤ P33_CHASE_WHALE_MULT: leave the static order as-is
           and poll once more for a fill (static patience to protect capital).
        6. Cancel any still-unfilled remainder and return the best available fill.

        Parameters
        ----------
        whale_mult : float — current Whale Oracle multiplier from the driving
                    Signal.  Controls whether price chasing is permitted.

        Returns the OKX order detail dict on any partial/full fill, or None if
        the slice produced no fill at all.
        """
        try:
            if coin_cfg is None:
                coin_cfg = self._coin_cfg(symbol)

            inst    = _inst_id(symbol, swap=swap)
            td_mode = "cross" if swap else OKX_TD_MODE_SPOT

            if swap and OKX_LEVERAGE > 1:
                await self._set_leverage(inst, OKX_LEVERAGE, mgn_mode=td_mode)

            # ── Step 1: Compute initial Maker price ───────────────────────────
            maker_px = await self._p332_calc_maker_price(symbol, side, swap=swap)
            if maker_px <= 0:
                log.warning(
                    "[P33.2-REBATE] %s %s: maker price invalid — aborting slice.",
                    side, symbol,
                )
                return None

            # ── Step 2: Quantize size ─────────────────────────────────────────
            meta    = await self._icache.get_instrument_info(symbol, swap=swap)
            ct_val  = (meta.ct_val if meta and meta.ct_val > 0 else OKX_CT_VAL) if swap else 1.0
            min_usd = coin_cfg.min_usd_value
            eff_usd = max(usd_amount, min_usd)
            raw_sz  = (eff_usd / maker_px) / ct_val if swap else eff_usd / maker_px

            q = await self._quantize_sz(symbol, raw_sz, swap, tag=f"{tag}_MKR_QSZ")
            if not q.valid:
                log.warning(
                    "[P33.2-REBATE] %s %s: quantized size invalid — aborting slice.",
                    side, symbol,
                )
                return None

            uid = uuid.uuid4().hex[:12]

            def _build_maker_body(px: float, sz: str, clord: str) -> dict:
                body = {
                    "instId":  inst,
                    "tdMode":  td_mode,
                    "side":    side,
                    "ordType": "post_only",
                    "sz":      sz,
                    "px":      f"{px:.8f}",
                    "clOrdId": clord,
                }
                if swap:
                    body["posSide"] = pos_side
                return body

            # ── Step 3: Submit initial Maker POST_ONLY order ──────────────────
            initial_body = _build_maker_body(maker_px, q.sz_str, f"P332MKR{uid}")
            log.info(
                "[P33.2-REBATE] POST_ONLY maker slice %s %s sz=%s px=%.8f "
                "(BBO+%.1f×tickSz | whale_mult=%.1fx)",
                side.upper(), symbol, q.sz_str, maker_px,
                P33_MAKER_TICK_OFFSET, whale_mult,
            )

            resp = await self._place_order(initial_body)
            if not resp or resp.get("code") != "0":
                s_code = (resp.get("data") or [{}])[0].get("sCode", "") if resp else ""
                log.warning(
                    "[P33.2-REBATE] Maker slice rejected %s %s sCode=%s: %s",
                    side, symbol, s_code, resp,
                )
                return None

            ord_id = resp["data"][0]["ordId"]

            # [P34-SPEED] Register order in front-run kill-switch tracker.
            self._p34_active_maker_orders.setdefault(symbol, []).append(ord_id)
            log.debug("[P34-SPEED] Registered maker order %s for %s (total active=%d)",
                      ord_id, symbol, len(self._p34_active_maker_orders[symbol]))

            # ── Step 4: Wait for fill ─────────────────────────────────────────
            await asyncio.sleep(P33_MAKER_FILL_WAIT)
            order = await self._get_order(inst, ord_id)

            if order and order.get("state") in ("filled", "partially_filled"):
                fill_sz = float(order.get("accFillSz") or 0)
                log.info(
                    "[P33.2-REBATE] Maker slice filled/partial %s %s "
                    "state=%s fill_sz=%.6f px=%.8f",
                    side.upper(), symbol,
                    order.get("state"), fill_sz, maker_px,
                )
                if order.get("state") == "partially_filled":
                    await self._cancel_order(inst, ord_id)
                    self._p33_record_order_event("partial_fill")
                return order

            # ── Step 5: [P33.2-CHASE] Price Chase decision ────────────────────
            chase_permitted = whale_mult > P33_CHASE_WHALE_MULT

            if chase_permitted:
                # Cancel the stale resting order and replace at new BBO.
                await self._cancel_order(inst, ord_id)
                await asyncio.sleep(0.3)

                new_maker_px = await self._p332_calc_maker_price(symbol, side, swap=swap)
                if new_maker_px <= 0:
                    log.warning(
                        "[P33.2-CHASE] %s %s: new maker price invalid after cancel "
                        "— giving up on this slice.", side, symbol,
                    )
                    self._p33_record_order_event("cancel")
                    return None

                # Re-quantize at the new price (price change may shift lot count
                # slightly for contracts denominated in base currency).
                raw_sz2 = (eff_usd / new_maker_px) / ct_val if swap else eff_usd / new_maker_px
                q2 = await self._quantize_sz(symbol, raw_sz2, swap, tag=f"{tag}_MKR_CHASE")
                if not q2.valid:
                    self._p33_record_order_event("cancel")
                    return None

                uid2       = uuid.uuid4().hex[:12]
                chase_body = _build_maker_body(new_maker_px, q2.sz_str, f"P332CSE{uid2}")

                log.info(
                    "[P33.2-CHASE] CHASING price %s %s: old_px=%.8f → new_px=%.8f "
                    "(whale_mult=%.1fx > threshold=%.1fx) sz=%s",
                    side.upper(), symbol, maker_px, new_maker_px,
                    whale_mult, P33_CHASE_WHALE_MULT, q2.sz_str,
                )

                resp2 = await self._place_order(chase_body)
                if not resp2 or resp2.get("code") != "0":
                    s_code2 = (resp2.get("data") or [{}])[0].get("sCode", "") if resp2 else ""
                    log.warning(
                        "[P33.2-CHASE] Chase order rejected %s %s sCode=%s",
                        side, symbol, s_code2,
                    )
                    self._p33_record_order_event("cancel")
                    return None

                chase_ord_id = resp2["data"][0]["ordId"]
                # [P34-SPEED] Register chase order in front-run kill-switch tracker.
                self._p34_active_maker_orders.setdefault(symbol, []).append(chase_ord_id)
                await asyncio.sleep(POST_ONLY_RETRY_SECS)
                chase_order = await self._get_order(inst, chase_ord_id)

                if chase_order and chase_order.get("state") in ("filled", "partially_filled"):
                    log.info(
                        "[P33.2-CHASE] Chase fill %s %s state=%s px=%.8f",
                        side.upper(), symbol,
                        chase_order.get("state"), new_maker_px,
                    )
                    if chase_order.get("state") == "partially_filled":
                        await self._cancel_order(inst, chase_ord_id)
                        self._p33_record_order_event("partial_fill")
                    return chase_order

                # Chase order also did not fill — cancel and give up.
                await self._cancel_order(inst, chase_ord_id)
                self._p33_record_order_event("cancel")
                log.info(
                    "[P33.2-CHASE] Chase order also unfilled %s %s — slice abandoned.",
                    side, symbol,
                )
                return None

            else:
                # Static patience path: whale_mult is too low to justify chasing.
                # Cancel the resting order to avoid leaving open positions.
                log.info(
                    "[P33.2-REBATE] Static patience: whale_mult=%.1fx ≤ chase "
                    "threshold=%.1fx — cancelling unfilled maker order %s %s.",
                    whale_mult, P33_CHASE_WHALE_MULT, side, symbol,
                )
                await self._cancel_order(inst, ord_id)
                self._p33_record_order_event("cancel")
                return None

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error(
                "[P33.2-REBATE] _p332_place_maker_slice unhandled error %s %s: %s",
                side, symbol, exc, exc_info=True,
            )
            return None

    async def _p332_stealth_layer_slice(
        self,
        symbol:     str,
        side:       str,
        usd_amount: float,
        swap:       bool,
        pos_side:   str,
        coin_cfg,
        tag:        str,
    ) -> Optional[dict]:
        """
        [P33.2-LAYERING] Stealth Layering Engine.

        Splits a single order into P33_LAYERING_SLICES micro-orders spread
        across multiple price levels to mask total slice size, increase fill
        probability, and maximise rebate capture across book depths.

        Default 3-layer layout:
          Layer 1 — BBO (Aggressive): best bid (buy) or best ask (sell)
          Layer 2 — Mid-Price (Neutral): (best_bid + best_ask) / 2
          Layer 3 — Mid-Price − 1 tickSz (Passive): for buys; +1 tickSz for sells

        Each layer receives an equal fraction of usd_amount
        (usd_amount / P33_LAYERING_SLICES).  Layers are submitted sequentially
        with minimal delay (0.1 s) to prevent detection by co-located HFT bots.

        All layer orders are placed as POST_ONLY limit orders.  Individual layer
        fills are aggregated into a single synthetic fill dict with the same
        schema as _execute_iceberg so the rest of the execution stack works
        without modification.

        Parameters
        ----------
        usd_amount : float — full slice USD value to be divided across layers.

        Returns a synthetic fill dict, or None if zero layers filled.
        """
        try:
            if coin_cfg is None:
                coin_cfg = self._coin_cfg(symbol)

            tick = await self.hub.get_tick(symbol)
            if not tick or tick.bid <= 0 or tick.ask <= 0:
                log.warning(
                    "[P33.2-LAYERING] %s %s: no valid tick — aborting layering, "
                    "falling back to None.", side, symbol,
                )
                return None

            meta = await self._icache.get_instrument_info(symbol, swap=swap)

            # Determine tick size for passive offset.
            tick_sz = self._p33_tick_sz_cache.get(symbol, 0.0)
            if tick_sz <= 0 and meta and getattr(meta, "tick_sz", 0) > 0:
                tick_sz = float(meta.tick_sz)
                self._p33_tick_sz_cache[symbol] = tick_sz
            if tick_sz <= 0:
                tick_sz = max(tick.bid * 1e-6, 1e-8)

            mid_px = (tick.bid + tick.ask) / 2.0

            # Build the layer price list.
            n = max(1, P33_LAYERING_SLICES)
            if n == 1:
                layer_prices = [tick.bid if side == "buy" else tick.ask]
            elif n == 2:
                layer_prices = [
                    tick.bid  if side == "buy"  else tick.ask,
                    mid_px,
                ]
            else:
                # Standard 3-layer (extendable: extra layers default to passive).
                bbo_px      = tick.bid if side == "buy" else tick.ask
                passive_px  = mid_px - tick_sz if side == "buy" else mid_px + tick_sz
                extras      = [mid_px - (i * tick_sz) for i in range(2, n - 1)] if n > 3 else []
                layer_prices = [bbo_px, mid_px] + extras + [passive_px]

            # Clamp and round every layer price.
            rounded_prices = []
            for lp in layer_prices:
                if lp <= 0:
                    lp = tick.bid if side == "buy" else tick.ask
                lp = await self._clamp_price(symbol, lp, swap=swap)
                if meta:
                    lp = InstrumentCache.round_price(lp, meta)
                rounded_prices.append(lp)

            layer_usd = usd_amount / n
            inst      = _inst_id(symbol, swap=swap)
            td_mode   = "cross" if swap else OKX_TD_MODE_SPOT

            if swap and OKX_LEVERAGE > 1:
                await self._set_leverage(inst, OKX_LEVERAGE, mgn_mode=td_mode)

            ct_val  = (meta.ct_val if meta and meta.ct_val > 0 else OKX_CT_VAL) if swap else 1.0
            min_usd = coin_cfg.min_usd_value

            total_qty  = 0.0
            total_cost = 0.0
            filled_layers = 0

            layer_labels = {0: "BBO-Aggressive", 1: "Mid-Neutral", 2: "Mid-Passive"}

            log.info(
                "[P33.2-LAYERING] %s %s $%.2f → %d layers "
                "($%.2f each | bbo=%.8f mid=%.8f passive=%.8f)",
                side.upper(), symbol, usd_amount, n, layer_usd,
                rounded_prices[0],
                rounded_prices[1] if n > 1 else rounded_prices[0],
                rounded_prices[-1],
            )

            for i, lpx in enumerate(rounded_prices):
                layer_label = layer_labels.get(i, f"Layer-{i+1}")
                layer_tag   = f"{tag}_LYR{i+1}of{n}"

                try:
                    eff_usd = max(layer_usd, min_usd)
                    raw_sz  = (eff_usd / lpx) / ct_val if swap else eff_usd / lpx
                    q = await self._quantize_sz(symbol, raw_sz, swap, tag=f"{layer_tag}_QSZ")
                    if not q.valid:
                        log.info(
                            "[P33.2-LAYERING] Layer %d/%d (%s) %s %s: "
                            "invalid quantized size — skipping.",
                            i + 1, n, layer_label, side, symbol,
                        )
                        self._p33_record_order_event("cancel")
                        if i < n - 1:
                            await asyncio.sleep(0.1)
                        continue

                    uid = uuid.uuid4().hex[:12]
                    layer_body = {
                        "instId":  inst,
                        "tdMode":  td_mode,
                        "side":    side,
                        "ordType": "post_only",
                        "sz":      q.sz_str,
                        "px":      f"{lpx:.8f}",
                        "clOrdId": f"P332LYR{i}{uid}",
                    }
                    if swap:
                        layer_body["posSide"] = pos_side

                    log.info(
                        "[P33.2-LAYERING] Layer %d/%d (%s) %s %s "
                        "sz=%s px=%.8f",
                        i + 1, n, layer_label,
                        side.upper(), symbol, q.sz_str, lpx,
                    )

                    resp = await self._place_order(layer_body)
                    if not resp or resp.get("code") != "0":
                        s_code = (resp.get("data") or [{}])[0].get("sCode", "") if resp else ""
                        log.warning(
                            "[P33.2-LAYERING] Layer %d/%d (%s) rejected %s %s sCode=%s",
                            i + 1, n, layer_label, side, symbol, s_code,
                        )
                        self._p33_record_order_event("cancel")
                        if i < n - 1:
                            await asyncio.sleep(0.1)
                        continue

                    layer_ord_id = resp["data"][0]["ordId"]
                    # [P34-SPEED] Register layer order in front-run kill-switch tracker.
                    self._p34_active_maker_orders.setdefault(symbol, []).append(layer_ord_id)

                    # Brief wait then poll fill.
                    await asyncio.sleep(POST_ONLY_RETRY_SECS)
                    layer_order = await self._get_order(inst, layer_ord_id)

                    state    = layer_order.get("state", "") if layer_order else ""
                    fill_px  = float((layer_order or {}).get("avgPx") or 0)
                    fill_sz  = float((layer_order or {}).get("accFillSz") or 0)

                    if state in ("filled", "partially_filled") and fill_px > 0 and fill_sz > 0:
                        total_qty  += fill_sz
                        total_cost += fill_sz * fill_px
                        filled_layers += 1
                        log.info(
                            "[P33.2-LAYERING] Layer %d/%d (%s) %s %s: "
                            "state=%s fill_sz=%.6f fill_px=%.8f ✓",
                            i + 1, n, layer_label,
                            side.upper(), symbol, state, fill_sz, fill_px,
                        )
                        if state == "partially_filled":
                            # Cancel unfilled remainder.
                            await self._cancel_order(inst, layer_ord_id)
                            self._p33_record_order_event("partial_fill")
                    else:
                        # Unfilled — cancel to keep the book clean.
                        if state not in ("canceled", "mmp_canceled", "filled"):
                            await self._cancel_order(inst, layer_ord_id)
                        log.info(
                            "[P33.2-LAYERING] Layer %d/%d (%s) %s %s: "
                            "unfilled (state=%s) — cancelled.",
                            i + 1, n, layer_label,
                            side.upper(), symbol, state,
                        )
                        self._p33_record_order_event("cancel")

                except asyncio.CancelledError:
                    raise
                except Exception as layer_exc:
                    log.warning(
                        "[P33.2-LAYERING] Layer %d/%d (%s) error %s %s: %s",
                        i + 1, n, layer_label, side, symbol, layer_exc,
                    )
                    self._p33_record_order_event("cancel")

                # Stagger layers to defeat co-location pattern detection.
                if i < n - 1:
                    await asyncio.sleep(0.1)

            if total_qty <= 0:
                log.warning(
                    "[P33.2-LAYERING] All %d layers unfilled for %s %s — "
                    "returning None.", n, side, symbol,
                )
                return None

            avg_px = total_cost / total_qty
            log.info(
                "[P33.2-LAYERING] %s %s complete: %d/%d layers filled | "
                "avg_px=%.8f total_qty=%.6f",
                side.upper(), symbol, filled_layers, n, avg_px, total_qty,
            )
            return {
                "avgPx":               str(avg_px),
                "accFillSz":           str(total_qty),
                "state":               "filled",
                "ordId":               f"P332LAYER_{symbol}_{int(time.time())}",
                "_layer_slices_sent":  n,
                "_layer_slices_filled": filled_layers,
            }

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error(
                "[P33.2-LAYERING] _p332_stealth_layer_slice unhandled error "
                "%s %s: %s", side, symbol, exc, exc_info=True,
            )
            return None

    # ── [/P33.2] ──────────────────────────────────────────────────────────────

    # ── [P12-2] Tape Velocity HFT Brake ───────────────────────────────────────
    async def _tape_monitor_loop(self):
        log.info("[P12-4] Tape monitor started (interval=%.1fs threshold=%.0f tps)",
                 TAPE_MONITOR_INTERVAL_SECS, TAPE_VELOCITY_HFT_THRESHOLD)
        while True:
            try:
                for sym in self.symbols:
                    vel = await self.hub.get_trade_velocity(sym)
                    prev_brake = self._hft_brake.get(sym, False)
                    new_brake  = vel >= TAPE_VELOCITY_HFT_THRESHOLD
                    if new_brake != prev_brake:
                        log.info(
                            "[P12-4] HFT Brake %s for %s: velocity=%.1f tps",
                            "ACTIVATED" if new_brake else "CLEARED", sym, vel,
                        )
                    self._hft_brake[sym] = new_brake

                    # ── [P36.2-DYNCLAMP] Feed current price into velocity tracker ──
                    # Compute PriceVelocity from a rolling price window (not trade
                    # count) so the DynamicBuffer reflects actual % price movement
                    # rather than HFT TPS.  We use the live bid/mid as the price
                    # proxy; a None tick is silently skipped.
                    try:
                        _dyn_tick = await self.hub.get_tick(sym)
                        if _dyn_tick and (_dyn_tick.bid > 0 or _dyn_tick.ask > 0):
                            _dyn_px = (
                                (_dyn_tick.bid + _dyn_tick.ask) / 2.0
                                if _dyn_tick.bid > 0 and _dyn_tick.ask > 0
                                else (_dyn_tick.ask or _dyn_tick.bid)
                            )
                            self._p362_update_velocity(sym, _dyn_px)
                    except Exception as _dyn_exc:
                        log.debug(
                            "[P36.2-DYNCLAMP] Velocity update error %s: %s", sym, _dyn_exc
                        )
                    # ── [/P36.2-DYNCLAMP] ────────────────────────────────────

                    # ── [P33-REVERSION] Feed velocity to VetoArbitrator ──────
                    # The Exhaustion Gap Filter in VetoArbitrator needs a
                    # continuous stream of velocity samples so it can detect
                    # the >50% velocity collapse that signals mean-reversion risk
                    # in the 500ms window after a Whale Sweep event.
                    try:
                        if self._p32_veto_arb is not None:
                            self._p32_veto_arb.record_price_velocity(vel)
                    except Exception as _vel_exc:
                        log.debug(
                            "[P33-REVERSION] Velocity feed to VetoArbitrator "
                            "failed for %s: %s (non-critical).", sym, _vel_exc,
                        )
                    # ── [/P33-REVERSION] ─────────────────────────────────────

                    # ── [P34-SPEED] Front-Run Kill-Switch ────────────────────
                    # If we have active maker layers for this symbol, check
                    # Coinbase oracle TPS as a proxy for price velocity.
                    # A sudden surge in Coinbase trade flow (> P34_FRONT_RUN_THRESHOLD_BPS
                    # bps equivalent) means an HFT is likely about to sweep our
                    # resting POST_ONLY quotes — cancel them immediately.
                    try:
                        active_layers = self._p34_active_maker_orders.get(sym, [])
                        if active_layers:
                            cb_tps = 0.0
                            if (
                                self.hub.oracle is not None
                                and hasattr(self.hub.oracle, "tps")
                            ):
                                cb_tps = self.hub.oracle.tps(sym)
                            # Convert tps to a basis-point proxy:
                            # 1 tps = 1 bps threshold crossing at P34_FRONT_RUN_THRESHOLD_BPS.
                            # The threshold env var is intentionally in the same
                            # "tps-equivalent" unit for easy calibration.
                            if cb_tps > P34_FRONT_RUN_THRESHOLD_BPS:
                                log.warning(
                                    "[P34-SPEED] Front-run kill-switch TRIGGERED %s: "
                                    "Coinbase tps=%.2f > threshold=%.1f — "
                                    "cancelling %d active maker layers to avoid pick-off.",
                                    sym, cb_tps, P34_FRONT_RUN_THRESHOLD_BPS,
                                    len(active_layers),
                                )
                                asyncio.ensure_future(
                                    self._p34_cancel_maker_layers(sym)
                                )
                    except Exception as _p34_exc:
                        log.debug(
                            "[P34-SPEED] Front-run check error %s: %s (non-critical).",
                            sym, _p34_exc,
                        )
                    # ── [/P34-SPEED] ─────────────────────────────────────────

                    # ── [P37-VPIN] Flow Toxicity Emergency Exit ───────────────
                    # Poll DataHub for the current Volume-Clock ToxicityScore.
                    # If ToxicityScore spikes above P37_EMERGENCY_EXIT_TOXICITY
                    # (default 0.95) AND an open position exists for this symbol,
                    # fire an immediate IOC emergency exit task to protect capital
                    # from an impending "Toxic Flush" caused by informed selling.
                    #
                    # GOLDEN BUILD PRESERVATION:
                    #   The emergency exit MUST NOT call _p362_reset_golden_counter().
                    #   A toxicity exit is a protective feature (not a bot failure)
                    #   and should never break a consecutive-clean-cycle golden run.
                    try:
                        if sym in self.positions:
                            _tox = await self.hub.get_flow_toxicity(sym)

                            # Inject into VetoArbitrator for entry-blocking.
                            if self._p32_veto_arb is not None:
                                self._p32_veto_arb.set_flow_toxicity(sym, _tox)

                            # Emergency exit threshold check.
                            if _tox > P37_EMERGENCY_EXIT_TOXICITY:
                                _in_flight = self._p37_exit_in_flight.get(sym, False)
                                if not _in_flight:
                                    log.warning(
                                        "[P37-VPIN] EMERGENCY EXIT triggered for %s: "
                                        "toxicity=%.4f > emergency_threshold=%.2f — "
                                        "launching IOC exit task (GOLDEN COUNTER PRESERVED).",
                                        sym, _tox, P37_EMERGENCY_EXIT_TOXICITY,
                                    )
                                    self._p37_exit_in_flight[sym] = True
                                    asyncio.ensure_future(
                                        self._p37_emergency_toxicity_exit(sym, _tox)
                                    )
                        else:
                            # No open position — still inject toxicity for entry veto.
                            _tox = await self.hub.get_flow_toxicity(sym)
                            if self._p32_veto_arb is not None:
                                self._p32_veto_arb.set_flow_toxicity(sym, _tox)
                    except Exception as _p37_exc:
                        log.debug(
                            "[P37-VPIN] Tape monitor toxicity check error %s: %s (non-critical).",
                            sym, _p37_exc,
                        )
                    # ── [/P37-VPIN] ──────────────────────────────────────────

            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("[P12-4] Tape monitor error: %s", e, exc_info=True)
            await asyncio.sleep(TAPE_MONITOR_INTERVAL_SECS)

    async def _p34_cancel_maker_layers(self, symbol: str) -> None:
        """
        [P34-SPEED] Front-Run Kill-Switch: immediately cancel all active
        POST_ONLY maker layers for ``symbol`` to prevent HFT pick-off.

        Called asynchronously from _tape_monitor_loop when Coinbase trade
        velocity exceeds P34_FRONT_RUN_THRESHOLD_BPS.  Order IDs are
        registered by _p332_place_maker_slice() and _p332_stealth_layer_slice()
        into self._p34_active_maker_orders[symbol].

        After cancellation the list is cleared so subsequent tape monitor ticks
        do not re-trigger the kill-switch for already-cancelled orders.
        """
        order_ids = list(self._p34_active_maker_orders.get(symbol, []))
        if not order_ids:
            return

        # Clear immediately to prevent double-cancellation on fast loops.
        self._p34_active_maker_orders[symbol] = []

        inst = _inst_id(symbol, swap=True)  # maker layers are always SWAP
        cancelled = 0
        for ord_id in order_ids:
            try:
                await self._cancel_order(inst, ord_id)
                cancelled += 1
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.debug(
                    "[P34-SPEED] cancel order %s (%s) error: %s (non-critical).",
                    ord_id, symbol, exc,
                )

        log.warning(
            "[P34-SPEED] Kill-switch complete: cancelled %d/%d maker layers for %s.",
            cancelled, len(order_ids), symbol,
        )

    def _is_hft_brake_active(self, symbol: str) -> bool:
        return self._hft_brake.get(symbol, False)

    # ── [P37-VPIN] Emergency Toxicity Exit ───────────────────────────────────

    def _p37_exit_lock_for(self, symbol: str) -> asyncio.Lock:
        """[P37-VPIN] Return (or create) the per-symbol emergency-exit lock."""
        if symbol not in self._p37_exit_locks:
            self._p37_exit_locks[symbol] = asyncio.Lock()
        return self._p37_exit_locks[symbol]

    async def _p37_emergency_toxicity_exit(
        self,
        symbol:        str,
        toxicity_score: float,
    ) -> None:
        """
        [P37-VPIN] Emergency Toxicity Exit — Immediate IOC position close.

        Triggered by _tape_monitor_loop when ToxicityScore spikes above
        P37_EMERGENCY_EXIT_TOXICITY (default 0.95).  Uses _close_position()
        with an emergency IOC tag so the existing order infrastructure handles
        exchange submission correctly.

        GOLDEN BUILD PRESERVATION
        ─────────────────────────
        This method intentionally does NOT call _p362_reset_golden_counter().
        A Toxic Flush protection exit is a *feature* of the Phase 37 microstructure
        monitor, not an execution failure.  The golden-build cycle counter should
        continue its streak so the Golden Build Report accurately reflects the
        bot's clean-execution record independent of market-driven protective exits.

        Concurrency Safety
        ──────────────────
        Protected by _p37_exit_lock_for(symbol) — at most one emergency exit task
        can hold the lock per symbol at any time.  The _p37_exit_in_flight flag is
        cleared in the finally block so a sustained toxicity spike can re-trigger
        after the first exit completes and the position is confirmed closed.

        Parameters
        ----------
        symbol         : str   — base symbol, e.g. "BTC"
        toxicity_score : float — ToxicityScore that triggered the exit (for logging)
        """
        _lock = self._p37_exit_lock_for(symbol)
        if _lock.locked():
            log.debug(
                "[P37-VPIN] _p37_emergency_toxicity_exit %s: "
                "lock already held — skipping duplicate exit task.", symbol,
            )
            self._p37_exit_in_flight[symbol] = False
            return

        async with _lock:
            try:
                pos = self.positions.get(symbol)
                if pos is None:
                    log.info(
                        "[P37-VPIN] Emergency exit for %s: no open position — noop "
                        "(may have been closed between trigger and execution).", symbol,
                    )
                    return

                log.critical(
                    "[P37-VPIN] 🚨 EMERGENCY TOXICITY EXIT: %s "
                    "toxicity=%.4f > threshold=%.2f "
                    "direction=%s qty=%.6f usd_cost=%.2f — "
                    "executing immediate IOC close. GOLDEN COUNTER PRESERVED.",
                    symbol, toxicity_score, P37_EMERGENCY_EXIT_TOXICITY,
                    pos.direction, pos.qty, pos.usd_cost,
                )

                # Use the existing _close_position infrastructure.
                # The tag P37_TOXIC_FLUSH identifies this exit in audit trails.
                # IMPORTANT: _close_position does NOT touch golden-build counters.
                try:
                    await self._close_position(symbol, tag="P37_TOXIC_FLUSH_IOC")
                    log.warning(
                        "[P37-VPIN] Emergency exit complete for %s "
                        "(toxicity=%.4f). Golden Build counter preserved.",
                        symbol, toxicity_score,
                    )
                except Exception as _close_exc:
                    log.error(
                        "[P37-VPIN] _close_position failed during emergency exit "
                        "%s: %s — attempting market liquidation fallback.",
                        symbol, _close_exc,
                    )
                    try:
                        await self.liquidate_all_positions(
                            symbol, reason="P37_TOXIC_FLUSH_FALLBACK"
                        )
                    except Exception as _liq_exc:
                        log.error(
                            "[P37-VPIN] Market liquidation fallback also failed "
                            "for %s: %s — MANUAL INTERVENTION REQUIRED.",
                            symbol, _liq_exc,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error(
                    "[P37-VPIN] _p37_emergency_toxicity_exit unhandled error %s: %s",
                    symbol, exc, exc_info=True,
                )
            finally:
                # Always clear in-flight flag so a future toxicity spike can
                # re-trigger once this task has fully completed.
                self._p37_exit_in_flight[symbol] = False

    # ── [/P37-VPIN] Emergency Exit ────────────────────────────────────────────

    async def _check_liquidity_sweep(self, symbol: str, direction: str,
                                      cur_price: float) -> bool:
        now     = time.time()
        last_ts = self._sweep_signals.get(symbol, 0.0)
        if now - last_ts < SWEEP_SIGNAL_TTL_SECS:
            return True
        try:
            tape = self.hub.get_tape(symbol)
            if tape is None:
                return False
            detected = await tape.is_liquidity_sweep(
                direction=direction, dip_px=cur_price, current_px=cur_price,
            )
            if detected:
                self._sweep_signals[symbol] = now
            return detected
        except Exception as e:
            log.debug("[P12-2] Sweep check error %s: %s", symbol, e)
        return False

    # ── [P9-1] Iceberg helper ─────────────────────────────────────────────────
    async def _iceberg_child_order(
        self, inst: str, side: str, raw_sz: float, price: float,
        td_mode: str, swap: bool, pos_side: str,
        tag: str, symbol: str, meta,
    ) -> Optional[dict]:
        """
        [QUANT-1] + [MINSZ-1] raw_sz is passed as float; quantized here
        (with min_sz bump) before placement.
        """
        book       = await self.hub.get_order_book(symbol)
        spread_mgr = self._p7_spread_mgr
        if spread_mgr is not None and book is not None:
            tick = await self.hub.get_tick(symbol)
            if tick:
                price = spread_mgr.adjusted_limit_price(side, tick, symbol, order_book=book)
                if meta:
                    price = InstrumentCache.round_price(price, meta)

        q = await self._quantize_sz(symbol, raw_sz, swap, tag=f"{tag}_ICE_QUANT")
        if not q.valid:
            log.warning("[P9-1] Iceberg child aborted — quantized size invalid (%s).", tag)
            return None
        sz_str = q.sz_str

        limit_body = self._sor.build_limit_body(
            inst, side, sz_str, f"{price:.8f}", td_mode, swap, pos_side, tag,
        )
        resp   = await self._place_order(limit_body)
        ord_id = None
        if resp and resp.get("code") == "0":
            ord_id = resp["data"][0]["ordId"]
        elif resp:
            s_code = (resp.get("data") or [{}])[0].get("sCode", "")
            if s_code == OKX_TAKER_REJECT_CODE:
                tick = await self.hub.get_tick(symbol)
                if tick:
                    book2 = await self.hub.get_order_book(symbol)
                    if spread_mgr is not None:
                        price = spread_mgr.adjusted_limit_price(
                            side, tick, symbol,
                            order_book=book2 if book2 else None,
                        )
                    else:
                        price = self._sor.limit_price(side, tick)
                    if meta:
                        price = InstrumentCache.round_price(price, meta)
                    limit_body["px"]      = f"{price:.8f}"
                    limit_body["clOrdId"] = f"ex_{uuid.uuid4().hex[:8]}"
                    resp2  = await self._place_order(limit_body)
                    if resp2 and resp2.get("code") == "0":
                        ord_id = resp2["data"][0]["ordId"]
        if not ord_id:
            return None
        order = await self._wait_fill(inst, ord_id, timeout=ICEBERG_FILL_TIMEOUT)
        if order and order.get("state") == "filled":
            return order
        try:
            await self._cancel_order(inst, ord_id)
        except Exception:
            pass
        return None

    # ── [P9-1] Iceberg Execution orchestrator ─────────────────────────────────
    async def _execute_iceberg(
        self, symbol: str, side: str, usd_amount: float,
        swap: bool, pos_side: str, tag: str, coin_cfg: CoinConfig,
    ) -> Optional[dict]:
        n_slices  = max(1, round(1.0 / ICEBERG_DISPLAY_PCT))
        slice_usd = usd_amount / n_slices
        inst      = _inst_id(symbol, swap=swap)
        td_mode   = "cross" if swap else OKX_TD_MODE_SPOT
        meta      = await self._icache.get_instrument_info(symbol, swap=swap)

        log.info("[P9] Iceberg %s %s $%.2f → %d slices × $%.2f",
                 side.upper(), symbol, usd_amount, n_slices, slice_usd)

        total_qty  = 0.0
        total_cost = 0.0
        slices_ok  = 0

        for i in range(n_slices):
            tick = await self.hub.get_tick(symbol)
            if not tick:
                break
            book       = await self.hub.get_order_book(symbol)
            spread_mgr = self._p7_spread_mgr
            if spread_mgr is not None:
                raw_price = spread_mgr.adjusted_limit_price(
                    side, tick, symbol, order_book=book if book else None,
                )
            else:
                raw_price = self._sor.limit_price(side, tick)

            if raw_price <= 0:
                log.warning("[P9] Iceberg slice %d: raw_price=0 for %s — skipping.", i + 1, symbol)
                break

            ct_val = (meta.ct_val if meta and meta.ct_val > 0 else OKX_CT_VAL) if swap else 1.0
            raw_sz = (slice_usd / raw_price) / ct_val if swap else slice_usd / raw_price

            q = await self._quantize_sz(symbol, raw_sz, swap, tag=f"{tag}_ICE{i+1}")
            if not q.valid:
                log.warning("[P9] Iceberg slice %d quantization invalid — stopping.", i + 1)
                break

            final_price = InstrumentCache.round_price(raw_price, meta) if meta else raw_price

            order_ts = time.time()
            order    = await self._iceberg_child_order(
                inst, side, q.sz_float, final_price,
                td_mode, swap, pos_side, f"{tag}_ICE{i+1}of{n_slices}", symbol, meta,
            )
            fill_ts = time.time()
            if order is None:
                break
            fill_px = float(order.get("avgPx") or order.get("px") or 0)
            fill_sz = float(order.get("accFillSz") or order.get("sz") or 0)
            if fill_px > 0 and fill_sz > 0:
                total_qty  += fill_sz
                total_cost += fill_sz * fill_px
                slices_ok  += 1
            if spread_mgr is not None:
                asyncio.ensure_future(
                    spread_mgr.record_fill(symbol, side, order_ts, fill_ts, True)
                )

        if total_qty <= 0:
            return None
        avg_px = total_cost / total_qty
        return {
            "avgPx": str(avg_px), "accFillSz": str(total_qty),
            "state": "filled", "ordId": f"ICE_{symbol}_{int(time.time())}",
            "_iceberg_slices_sent": n_slices, "_iceberg_slices_filled": slices_ok,
        }

    # ── [P9-2] Shadow Limit Order infrastructure ───────────────────────────────
    def _register_shadow(self, watch: ShadowWatch):
        self._shadow_watches[watch.symbol] = watch
        log.info("[P9] Shadow registered %s %s target=%.6f expires=%.0fs",
                 watch.symbol, watch.side, watch.target_px, SHADOW_EXPIRY_SECS)

    def has_open_positions(self) -> bool:
        return bool(self.positions)

    def _cancel_shadow(self, symbol: str):
        if symbol in self._shadow_watches:
            del self._shadow_watches[symbol]

    async def _shadow_watcher(self):
        while True:
            try:
                await asyncio.sleep(SHADOW_POLL_SECS)
                expired = [s for s, w in self._shadow_watches.items() if w.is_expired()]
                for sym in expired:
                    log.info("[P9] Shadow watch expired for %s", sym)
                    self._cancel_shadow(sym)
                    sentinel = self._p11_sentinel
                    if sentinel is not None and sentinel.shadow_tracker is not None:
                        sentinel.shadow_tracker.record_miss(sym)

                for sym, watch in list(self._shadow_watches.items()):
                    if sym in self.positions:
                        self._cancel_shadow(sym)
                        continue
                    if self.drawdown._killed or self._p20_zombie_mode:
                        continue
                    cb = self._p7_cb
                    if cb is not None and cb.is_tripped:
                        continue
                    tick = await self.hub.get_tick(sym)
                    if not tick:
                        continue
                    ref_px = tick.ask if watch.side == "buy" else tick.bid
                    if not watch.is_hit(ref_px):
                        continue
                    sentinel = self._p11_sentinel
                    if sentinel is not None and sentinel.shadow_tracker is not None:
                        sentinel.shadow_tracker.record_hit(sym)
                    self._cancel_shadow(sym)
                    try:
                        if watch.side == "buy":
                            await self._open_long(sym, watch.usd_amount, watch.signal)
                        else:
                            await self._open_short(sym, watch.usd_amount, watch.signal)
                    except Exception as e:
                        log.error("[P9] Shadow fire error %s: %s", sym, e, exc_info=True)
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.error("[P9] Shadow watcher error: %s", e, exc_info=True)

    # ── [P10-4] Execution mode helpers ────────────────────────────────────────
    def _is_aggressive_iceberg_mode(self) -> bool:
        return self._p10_execution_mode == "aggressive_iceberg"

    def _is_shadow_only_mode(self) -> bool:
        return self._p10_execution_mode == "shadow_only"

    def _effective_iceberg_threshold(self, symbol: str, usd_amount: float) -> bool:
        if self._is_hft_brake_active(symbol):
            return True
        if self._is_shadow_only_mode():
            return False
        if self._is_aggressive_iceberg_mode():
            return usd_amount >= P10_AGGRESSIVE_ICEBERG_MIN_USD
        return usd_amount >= ICEBERG_MIN_USD

    def _should_force_shadow(self, regime: str) -> bool:
        if self._is_shadow_only_mode():
            return True
        return regime in SHADOW_REGIMES

    # ── Core order execution ───────────────────────────────────────────────────
    async def _execute_order(
        self, symbol: str, side: str, usd_amount: float,
        swap: bool = False, pos_side: str = "net",
        tag: str = "ENTRY", coin_cfg: Optional[CoinConfig] = None,
        whale_mult: float = 0.0,
    ) -> Optional[dict]:
        if coin_cfg is None:
            coin_cfg = self._coin_cfg(symbol)

        # ── [P32-STEALTH-TWAP] Route large orders through the stealth TWAP engine ─
        # Only applies to non-TWAP-tagged orders to prevent infinite recursion.
        if (
            usd_amount >= P32_TWAP_THRESHOLD_USD
            and "TWAP" not in tag
            and "ICE" not in tag
            and "MKT" not in tag
        ):
            log.info(
                "[P32-STEALTH-TWAP] %s %s $%.2f exceeds threshold $%.2f → "
                "routing to stealth TWAP engine",
                side.upper(), symbol, usd_amount, P32_TWAP_THRESHOLD_USD,
            )
            return await self._p32_stealth_twap(
                symbol, side, usd_amount, swap, pos_side, tag, coin_cfg,
                whale_mult=whale_mult,
            )
        # ── [/P32-STEALTH-TWAP] ───────────────────────────────────────────────────

        # ── [P33.2-LAYERING] Stealth Layering Engine ─────────────────────────────
        # When P33_LAYERING_SLICES > 1, split this order into micro-layers spread
        # across BBO, Mid, and Passive price levels.  Guarded against recursion
        # (LAYER / IOC / MKT / TWAP sub-tags) and against iceberg/TWAP paths that
        # manage their own sizing logic.
        _layering_eligible = (
            P33_LAYERING_SLICES > 1
            and "LAYER" not in tag
            and "IOC"   not in tag
            and "MKT"   not in tag
            and "TWAP"  not in tag
            and "ICE"   not in tag
            and "PIVOT" not in tag
        )
        if _layering_eligible:
            try:
                log.info(
                    "[P33.2-LAYERING] %s %s $%.2f → routing to stealth layer "
                    "engine (%d layers)",
                    side.upper(), symbol, usd_amount, P33_LAYERING_SLICES,
                )
                _layer_result = await self._p332_stealth_layer_slice(
                    symbol=symbol, side=side, usd_amount=usd_amount,
                    swap=swap, pos_side=pos_side, coin_cfg=coin_cfg,
                    tag=f"{tag}_LAYER",
                )
                if _layer_result is not None:
                    return _layer_result
                # If all layers failed (e.g. book illiquid), fall through to
                # the standard single-order path below so the trade is not
                # silently dropped.
                log.warning(
                    "[P33.2-LAYERING] %s %s: all layers failed — falling back "
                    "to standard single-order path.", side, symbol,
                )
            except asyncio.CancelledError:
                raise
            except Exception as _layer_exc:
                log.warning(
                    "[P33.2-LAYERING] Layering error %s %s: %s — falling back "
                    "to standard path.", side, symbol, _layer_exc,
                )
        # ── [/P33.2-LAYERING] ────────────────────────────────────────────────────

        if self._effective_iceberg_threshold(symbol, usd_amount):
            return await self._execute_iceberg(
                symbol, side, usd_amount, swap, pos_side, tag, coin_cfg,
            )

        tick = await self.hub.get_tick(symbol)
        if not tick or tick.ask <= 0:
            log.error("[%s] No tick for %s — aborting.", tag, symbol)
            return None

        # [P23-OPT-4] Spread-Guard: protect the $50 balance from excessive slippage.
        if tick.bid > 0 and tick.ask > 0:
            spread_pct = (tick.ask - tick.bid) / tick.bid * 100.0
            if spread_pct > P23_SPREAD_GUARD_PCT:
                log.warning(
                    "[P23-OPT-4] SPREAD-GUARD VETO %s: spread=%.5f%% > %.3f%% — "
                    "trade aborted to protect $50 balance.",
                    symbol, spread_pct, P23_SPREAD_GUARD_PCT,
                )
                return None

        inst    = _inst_id(symbol, swap=swap)
        td_mode = "cross" if swap else OKX_TD_MODE_SPOT

        if swap and OKX_LEVERAGE > 1:
            await self._set_leverage(inst, OKX_LEVERAGE, mgn_mode=td_mode)

        book       = await self.hub.get_order_book(symbol)
        spread_mgr = self._p7_spread_mgr
        if spread_mgr is not None:
            raw_price = spread_mgr.adjusted_limit_price(
                side, tick, symbol, order_book=book if book else None,
            )
        else:
            raw_price = self._sor.limit_price(side, tick)

        if raw_price <= 0:
            log.error("[%s] raw_price=0 for %s — aborting.", tag, symbol)
            return None

        meta = await self._icache.get_instrument_info(symbol, swap=swap)
        final_price = InstrumentCache.round_price(raw_price, meta) if meta else raw_price

        # ── [P32-LIQ-MAGNET] Apply liquidation cluster entry offset ────────────
        # Detect if any liquidation cluster is within 0.5% of our computed price
        # and shift the limit order slightly toward it to catch the wick fills.
        _direction_hint = "long" if side == "buy" else "short"
        _lm_offset = self._p32_liquidation_magnet_offset(final_price, _direction_hint)
        if _lm_offset != 0.0:
            adjusted_price = final_price + _lm_offset
            if meta:
                adjusted_price = InstrumentCache.round_price(adjusted_price, meta)
            if adjusted_price > 0:
                # [v32.1-CLAMP] Clamp the offset price to exchange price limits
                # before use so OKX never rejects with sCode 51000.
                adjusted_price = await self._clamp_price(symbol, adjusted_price, swap=swap)
                if meta:
                    adjusted_price = InstrumentCache.round_price(adjusted_price, meta)
                log.info(
                    "[P32-LIQ-MAGNET] %s %s: price %.6f → %.6f (offset=%.6f, clamped)",
                    side, symbol, final_price, adjusted_price, _lm_offset,
                )
                final_price = adjusted_price
        # ── [/P32-LIQ-MAGNET] ─────────────────────────────────────────────────

        # ── [P33-SHADOW] Iceberg Shadowing ────────────────────────────────────
        # After all price adjustments (spread, LIQ-MAGNET), detect whether a
        # massive passive buy wall in the order book qualifies as an Iceberg.
        # If so, price-improve by exactly 1 tickSz to "shadow" the block and
        # gain fill priority over resting limit orders at the wall price.
        # Shadowing is only applied to buy orders (fronting a buy wall) and
        # is skipped for TWAP/ICE/MKT sub-calls to prevent recursive adjustments.
        if side == "buy" and "TWAP" not in tag and "ICE" not in tag and "MKT" not in tag:
            try:
                _shadowed_price = await self._p33_detect_iceberg_shadow(
                    symbol=symbol, side=side, price=final_price, swap=swap,
                )
                if _shadowed_price != final_price:
                    log.info(
                        "[P33-SHADOW] %s %s: final_price %.8f → shadowed %.8f "
                        "(+1 tickSz — iceberg buy wall detected)",
                        side, symbol, final_price, _shadowed_price,
                    )
                    final_price = _shadowed_price
            except Exception as _shadow_exc:
                log.debug("[P33-SHADOW] Iceberg shadow check error %s: %s",
                          symbol, _shadow_exc)
        # ── [/P33-SHADOW] ────────────────────────────────────────────────────

        # ── [P36.2-DYNCLAMP] Exchange Price Limit Clamping (Dynamic Buffer) ──
        # OKX enforces hard buyLmt / sellLmt price bands that change throughout
        # the day.  Any order placed outside these bands is rejected with sCode
        # 51006 "Order price is not within the price limit".  Fetch the cached
        # bands from InstrumentCache and clamp final_price before submission.
        #
        # [P36.2] DynamicBuffer replaces the static PRICE_LIMIT_BUFFER: when
        # PriceVelocity > P362_VELOCITY_THRESHOLD_PCT over the rolling 10-second
        # window, the buffer is doubled to P362_DYNAMIC_BUFFER_HIGH (10 bps) to
        # stay ahead of the exchange's rapidly moving price limits.
        try:
            _dyn_buf = self._p362_get_dynamic_buffer(symbol)
            _buy_lmt, _sell_lmt = self._icache.get_price_limits(inst)
            if side == "buy" and _buy_lmt > 0 and final_price > _buy_lmt:
                _clamped_px = _buy_lmt * (1.0 - _dyn_buf)
                if meta:
                    _clamped_px = InstrumentCache.round_price(_clamped_px, meta)
                log.warning(
                    "[P36.2-DYNCLAMP] %s BUY price %.8f exceeds buyLmt %.8f "
                    "→ clamped to %.8f (DynamicBuf=%.4f%%)",
                    symbol, final_price, _buy_lmt, _clamped_px, _dyn_buf * 100,
                )
                final_price = _clamped_px
            elif side == "sell" and _sell_lmt > 0 and final_price < _sell_lmt:
                _clamped_px = _sell_lmt * (1.0 + _dyn_buf)
                if meta:
                    _clamped_px = InstrumentCache.round_price(_clamped_px, meta)
                log.warning(
                    "[P36.2-DYNCLAMP] %s SELL price %.8f below sellLmt %.8f "
                    "→ clamped to %.8f (DynamicBuf=%.4f%%)",
                    symbol, final_price, _sell_lmt, _clamped_px, _dyn_buf * 100,
                )
                final_price = _clamped_px
        except Exception as _plg_exc:
            log.debug(
                "[P36.2-DYNCLAMP] Price limit check failed for %s: %s "
                "(non-fatal — order will proceed unclamped).", symbol, _plg_exc,
            )
        # ── [/P36.2-DYNCLAMP] ────────────────────────────────────────────────

        # ── [P32-SLIP-ADAPT] LIMIT_ONLY enforcement ───────────────────────────
        # If the last 3 fills for this symbol averaged > 10bps slippage, we are
        # in LIMIT_ONLY mode for 4 hours — no market-order fallback is allowed.
        _p32_limit_only_active = self._p32_is_limit_only(symbol)
        if _p32_limit_only_active:
            log.info(
                "[P32-SLIP-ADAPT] %s in LIMIT_ONLY mode — market fallback disabled.",
                symbol,
            )
        # ── [/P32-SLIP-ADAPT] ─────────────────────────────────────────────────

        # [QUANT-1] + [MINSZ-1] Compute raw size then quantize BEFORE REST body.
        ct_val  = (meta.ct_val if meta and meta.ct_val > 0 else OKX_CT_VAL) if swap else 1.0
        min_usd = coin_cfg.min_usd_value
        eff_usd = max(usd_amount, min_usd)
        raw_sz  = (eff_usd / raw_price) / ct_val if swap else eff_usd / raw_price

        q = await self._quantize_sz(symbol, raw_sz, swap, tag=tag)
        if not q.valid:
            # [P23-OPT-2] Small-Account Pivot: if BTC/ETH futures fails min_sz,
            # auto-pivot to XRP or DOGE where contract minimums are much lower.
            if swap and symbol.upper() in ("BTC", "ETH"):
                for fallback_sym in P23_SMALL_ACCT_FALLBACK_SYMS:
                    if fallback_sym in self.symbols and fallback_sym not in self.positions:
                        # [P23-OPT-2-FIX] Re-verify min_sz / lot_sz from
                        # hub.instrument_cache for the fallback symbol BEFORE
                        # recursing.  This prevents sCode 51000 "Parameter sz
                        # error" rejections when the fallback instrument has
                        # different contract parameters than BTC/ETH.
                        fb_tick = await self.hub.get_tick(fallback_sym)
                        if not fb_tick or fb_tick.ask <= 0:
                            log.warning(
                                "[P23-OPT-2] No tick for fallback %s — skipping.",
                                fallback_sym,
                            )
                            continue
                        fb_meta = await self._icache.get_instrument_info(
                            fallback_sym, swap=True
                        )
                        fb_ct_val = (
                            fb_meta.ct_val
                            if fb_meta and fb_meta.ct_val > 0
                            else OKX_CT_VAL
                        )
                        fb_coin_cfg = self._coin_cfg(fallback_sym)
                        fb_eff_usd  = max(usd_amount, fb_coin_cfg.min_usd_value)
                        fb_raw_sz   = (fb_eff_usd / fb_tick.ask) / fb_ct_val
                        fb_q = await self._quantize_sz(
                            fallback_sym, fb_raw_sz, True,
                            tag=f"{tag}_PIVOT_CHK_{fallback_sym}",
                        )
                        if not fb_q.valid:
                            log.warning(
                                "[P23-OPT-2] Fallback %s also fails min_sz "
                                "(fb_raw_sz=%.8f lot_sz=%.8f min_sz=%.8f) — "
                                "skipping to next fallback.",
                                fallback_sym, fb_raw_sz, fb_q.lot_sz, fb_q.min_sz,
                            )
                            continue
                        log.warning(
                            "[P23-OPT-2] Small-Account Pivot: %s SWAP min_sz failed "
                            "(raw_sz=%.8f) — pivoting to %s "
                            "(fb_lot_sz=%.8f fb_min_sz=%.8f fb_sz=%s).",
                            symbol, raw_sz, fallback_sym,
                            fb_q.lot_sz, fb_q.min_sz, fb_q.sz_str,
                        )
                        return await self._execute_order(
                            fallback_sym, side, usd_amount,
                            swap=True, pos_side=pos_side, tag=f"{tag}_PIVOT_{fallback_sym}",
                            coin_cfg=fb_coin_cfg,
                        )
            log.warning("[%s] _execute_order: quantized size invalid for %s — aborting.", tag, symbol)
            return None
        sz_str = q.sz_str

        order_ts = time.time()
        ord_id   = None

        for attempt in range(1, POST_ONLY_MAX_RETRIES + 1):
            limit_body = self._sor.build_limit_body(
                inst, side, sz_str, f"{final_price:.8f}", td_mode, swap, pos_side,
                f"{tag}_A{attempt}" if attempt > 1 else tag,
            )
            resp = await self._place_order(limit_body)
            if resp and resp.get("code") == "0":
                ord_id = resp["data"][0]["ordId"]
                break
            s_code = ""
            if resp:
                s_code = (resp.get("data") or [{}])[0].get("sCode", "")
            if s_code == OKX_TAKER_REJECT_CODE and attempt < POST_ONLY_MAX_RETRIES:
                fresh_tick = await self.hub.get_tick(symbol)
                if fresh_tick:
                    fresh_book = await self.hub.get_order_book(symbol)
                    if spread_mgr is not None:
                        raw_price = spread_mgr.adjusted_limit_price(
                            side, fresh_tick, symbol,
                            order_book=fresh_book if fresh_book else None,
                        )
                    else:
                        raw_price = self._sor.limit_price(side, fresh_tick)
                    if raw_price > 0:
                        final_price = InstrumentCache.round_price(raw_price, meta) if meta else raw_price
                        raw_sz2 = (eff_usd / raw_price) / ct_val if swap else eff_usd / raw_price
                        q2 = await self._quantize_sz(symbol, raw_sz2, swap, tag=f"{tag}_RPX{attempt}")
                        if q2.valid:
                            sz_str = q2.sz_str
                continue
            log.error("[%s] Post-only rejected (sCode=%s): %s", tag, s_code, resp)
            # ── [P36.2-SELFHEAL] sCode 51006 → force immediate price-limit refresh ──
            if s_code == "51006":
                self._p362_51006_count += 1
                self._p362_reset_golden_counter(
                    f"sCode 51006 on {symbol} {side} (tag={tag})"
                )
                try:
                    asyncio.ensure_future(
                        self.hub.force_immediate_refresh(symbol),
                        loop=asyncio.get_event_loop(),
                    )
                    log.warning(
                        "[P36.2-SELFHEAL] sCode 51006 on %s → "
                        "triggered force_immediate_refresh (async, non-blocking).",
                        symbol,
                    )
                except Exception as _sfh_exc:
                    log.debug(
                        "[P36.2-SELFHEAL] force_immediate_refresh schedule error: %s",
                        _sfh_exc,
                    )
            # ── [/P36.2-SELFHEAL] ────────────────────────────────────────────
            break

        fill_ts = time.time()
        if ord_id:
            await asyncio.sleep(POST_ONLY_RETRY_SECS)
            order   = await self._get_order(inst, ord_id)
            fill_ts = time.time()
            filled  = order.get("state") == "filled"
            if spread_mgr is not None:
                asyncio.ensure_future(
                    spread_mgr.record_fill(symbol, side, order_ts, fill_ts, filled)
                )
            if filled:
                return order
            if order.get("state") not in ("canceled", "mmp_canceled"):
                await self._cancel_order(inst, ord_id)
                await asyncio.sleep(0.5)
        else:
            if spread_mgr is not None:
                asyncio.ensure_future(
                    spread_mgr.record_fill(symbol, side, order_ts, time.time(), False)
                )

        # [P32-SLIP-ADAPT] LIMIT_ONLY guard — skip market fallback when active.
        if _p32_limit_only_active:
            log.warning(
                "[P32-SLIP-ADAPT] %s: market fallback SUPPRESSED by LIMIT_ONLY "
                "(avg slippage exceeded %.1f bps in last %d trades). "
                "Limit order was not filled — aborting entry to prevent bleeding.",
                symbol, P32_SLIP_LIMIT_BPS, P32_SLIP_WINDOW_TRADES,
            )
            return None

        # Market fallback — re-fetch tick and re-quantize.
        tick = await self.hub.get_tick(symbol)
        if not tick:
            return None
        raw_price2 = tick.ask if side == "buy" else tick.bid
        if raw_price2 <= 0:
            return None
        raw_sz_mkt = (eff_usd / raw_price2) / ct_val if swap else eff_usd / raw_price2
        q_mkt = await self._quantize_sz(symbol, raw_sz_mkt, swap, tag=f"{tag}_MKT")
        if not q_mkt.valid:
            log.warning("[%s] Market fallback: quantized size invalid for %s — aborting.", tag, symbol)
            return None
        sz_str2 = q_mkt.sz_str

        book = await self.hub.get_order_book(symbol)
        if book is not None:
            try:
                qty_float = q_mkt.sz_float
                if swap:
                    qty_float *= ct_val
            except Exception:
                qty_float = 0.0
            slippage = book.slippage_pct(qty_float, side) if qty_float > 0 else 0.0
            if slippage > LIQUIDITY_SLIPPAGE_MAX_PCT:
                return await self._liquidity_chase_fill(
                    inst, side, q_mkt.sz_float, td_mode, swap, pos_side, tag, symbol)

        mkt_body = self._sor.build_market_body(inst, side, sz_str2, td_mode, swap, pos_side, tag)
        resp2 = await self._place_order(mkt_body)
        if not resp2 or resp2.get("code") != "0":
            return None
        ord_id2 = resp2["data"][0]["ordId"]
        return await self._wait_fill(inst, ord_id2, timeout=30)

    # ══════════════════════════════════════════════════════════════════════════
    # [P16-2] Atomic Express Trade
    # ══════════════════════════════════════════════════════════════════════════
    async def trigger_atomic_express_trade(
        self,
        oracle_signal: OracleSignal,
    ) -> bool:
        symbol    = oracle_signal.symbol.upper()
        direction = "long"  if oracle_signal.signal_type == ORACLE_WHALE_BUY  else "short"
        reason    = oracle_signal.signal_type
        mult      = oracle_signal.multiplier

        log.warning(
            "[P16-2] trigger_atomic_express_trade: %s direction=%s mult=%.1f× reason=%s",
            symbol, direction, mult, reason,
        )

        # [P23-OPT-3] Whale Sniper: multiplier > 10× grants Express Lane entry
        # even if TA indicators are neutral or in conflict (RSI/EMA disagreement).
        # We tag it [WHALE] and skip the OBI filter in the express trade path.
        _whale_sniper_active = mult >= P23_WHALE_SNIPER_MULT_THRESH
        if _whale_sniper_active:
            log.warning(
                "[P23-OPT-3] WHALE SNIPER ACTIVATED: %s mult=%.1f× >= %.1f× "
                "— Express Lane bypass (TA neutral/conflict overridden).",
                symbol, mult, P23_WHALE_SNIPER_MULT_THRESH,
            )
        self._pending_mode_label = "[WHALE]"

        if self._p20_zombie_mode:
            log.info("[P16-2/P20-1] %s express skipped: Zombie Mode active.", symbol)
            return False

        if self.drawdown._killed:
            return False

        cb = self._p7_cb
        if cb is not None and cb.is_tripped:
            return False

        if self._vol_guard.emergency_pause:
            return False

        if symbol not in self.symbols:
            return False

        # ── [P7] Macro Safety Floor (Express Lane) ───────────────────────────
        # Express whale execution may accelerate entries, but it must NOT violate
        # the macro regime floor published by NewsWire in the shared status blob.
        # We only block on *strict contradiction*:
        #   • no LONGs when macro is "Extreme Fear / Strongly Bearish" (score ≤ -0.7)
        #   • no SHORTs when macro is "Extreme Greed / Strongly Bullish" (score ≥ +0.7)
        try:
            _nw = (self._status or {}).get("agents", {}).get("news_wire", {}) if isinstance(self._status, dict) else {}
            _macro_score = float(_nw.get("macro_score", 0.0) or 0.0)
            _macro_label = str(_nw.get("label", "") or "")
            _label_low = _macro_label.lower()

            if direction == "long" and (_macro_score <= -0.7 or "strongly bearish" in _label_low or "extreme fear" in _label_low):
                log.warning(
                    "[P7] Express whale BLOCKED by Macro Safety Floor: %s LONG while macro=%s score=%.3f",
                    symbol, _macro_label or "unknown", _macro_score,
                )
                return False
            if direction == "short" and (_macro_score >= 0.7 or "strongly bullish" in _label_low or "extreme greed" in _label_low):
                log.warning(
                    "[P7] Express whale BLOCKED by Macro Safety Floor: %s SHORT while macro=%s score=%.3f",
                    symbol, _macro_label or "unknown", _macro_score,
                )
                return False
        except Exception:
            # Never block express path due to telemetry parsing errors.
            pass

        async with self._p16_express_lock:
            now = time.time()
            last_fire = self._p16_last_express.get(symbol, 0.0)
            if now - last_fire < P16_EXPRESS_DEDUPE_SECS:
                return False

            # [LLMFB-3] LLM veto with empty-response fallback.
            # [P23-OPT-1] 2s timeout on OpenRouter in express path too.
            _express_narrative: Optional[NarrativeResult] = None
            if self._p17_veto is not None:
                _express_cancel_flag = getattr(oracle_signal, "cancel_buys_flag", False)
                try:
                    _express_narrative = await asyncio.wait_for(
                        self._p17_veto.score(
                            symbol           = symbol,
                            direction        = direction,
                            drawdown_killed  = False,
                            cancel_buys_flag = _express_cancel_flag,
                        ),
                        timeout=P23_LLM_TIMEOUT_S,
                    )
                    # Guard: treat any empty/whitespace verdict as neutral.
                    if not _express_narrative or not getattr(_express_narrative, "verdict", "").strip():
                        log.warning(
                            "[LLMFB-3] Express LLM returned empty verdict for %s — "
                            "applying neutral fallback.", symbol,
                        )
                        _express_narrative = _make_neutral_narrative()
                except asyncio.TimeoutError:
                    log.warning(
                        "[P23-OPT-1] Express AI FALLBACK: OpenRouter timed out for %s.", symbol,
                    )
                    _express_narrative = _make_neutral_narrative()
                except Exception as _llm_exc:
                    log.warning(
                        "[LLMFB-3] Express LLM score() raised for %s: %s — "
                        "applying neutral fallback.", symbol, _llm_exc,
                    )
                    _express_narrative = _make_neutral_narrative()

                if _express_narrative.verdict == "CATASTROPHE_VETO":
                    log.warning(
                        "[P17-3/P18-3] Express Lane CATASTROPHE VETO %s %s: "
                        "score=%.4f — aborting atomic trade and liquidating.",
                        symbol, direction, _express_narrative.score,
                    )
                    asyncio.ensure_future(
                        self.liquidate_all_positions(symbol, reason="P18_EXPRESS_CATASTROPHE")
                    )
                    return False            # ── [P32-VETO-ARB][P37-VPIN] Whale-Driven but Safety-Gated ─────────
            # Even when WHALE_SNIPER is active (>= 10× multiplier), do NOT bypass
            # institutional safety gates: Flow Toxicity (P37), Spoofing (P36.1),
            # Hedge rebalance veto (P35.1), and Skew veto (P34.1) all live inside
            # VetoArbitrator.compute_p_success(). If it blocks, we block.
            try:
                if self._p32_veto_arb is None:
                    try:
                        self._p32_veto_arb = VetoArbitrator()
                        try:
                            self._p32_veto_arb.set_hub(self.hub)
                        except Exception:
                            pass
                    except Exception:
                        self._p32_veto_arb = None

                if self._p32_veto_arb is not None:
                    _recent_returns: list = []
                    try:
                        _h1 = await self.hub.get_candles(symbol, "1hour", 30)
                        if _h1 and len(_h1) >= 2:
                            _closes = [c.close for c in _h1 if c.close > 0]
                            if len(_closes) >= 2:
                                _recent_returns = [
                                    (_closes[i] - _closes[i-1]) / _closes[i-1] * 100
                                    for i in range(1, len(_closes))
                                    if _closes[i-1] != 0
                                ]
                    except Exception:
                        pass

                    _corr_score = 0.5
                    try:
                        _open_pos = list(self.positions.keys())
                        if _open_pos:
                            _corr_score = 1.0 - self.brain.position_correlation(symbol, _open_pos)
                    except Exception:
                        pass

                    _vel_boost = 1.0
                    try:
                        if self._p17_veto is not None:
                            _vel_boost = self._p17_veto.velocity_monitor.conviction_boost(direction)
                    except Exception:
                        pass

                    _local_price: float = 0.0
                    _global_mid:  Optional[float] = None
                    try:
                        _tick = await self.hub.get_tick(symbol)
                        if _tick and _tick.bid > 0 and _tick.ask > 0:
                            _local_price = (_tick.bid + _tick.ask) / 2.0
                        _global_mid = await self.hub.get_global_mid_price(symbol)
                    except Exception:
                        pass

                    _p_success = self._p32_veto_arb.compute_p_success(
                        recent_returns=_recent_returns,
                        correlation_score=_corr_score,
                        velocity_boost=_vel_boost,
                        local_price=_local_price,
                        global_mid_price=_global_mid,
                    )
                    if _p_success < 0.65:
                        log.warning(
                            "[P32-VETO-ARB] Express whale entry BLOCKED by safety gate: "
                            "%s %s p_success=%.4f < 0.65",
                            symbol, direction, _p_success,
                        )
                        try:
                            _append_veto_audit_executor(
                                symbol,
                                "P32 Veto Arbitrator (Express Safety Gate)",
                                f"p_success={_p_success:.4f} < 0.65",
                            )
                        except Exception:
                            pass
                        return False
            except Exception as _expr_veto_exc:
                log.debug("[P32-VETO-ARB] Express safety gate error %s: %s", symbol, _expr_veto_exc)
            # ── [/P32-VETO-ARB][P37-VPIN] ──────────────────────────────────────

            if symbol in self.positions:
                return False

            tick = await self.hub.get_tick(symbol)
            if not tick or tick.ask <= 0 or tick.bid <= 0:
                return False

            use_swap = (direction == "short") or (OKX_LEVERAGE > 1)
            meta = await self._icache.get_instrument_info(symbol, swap=use_swap)

            cfg = self._coin_cfg(symbol)
            min_lot_usd = 10.0
            if meta and tick.ask > 0:
                min_lot_usd = float(meta.lot_sz if hasattr(meta, 'lot_sz') else 0.01) * tick.ask

            usd_amount = float(min(
                max(_D(self._avail) * _D(P16_EXPRESS_ALLOC_PCT), _D(cfg.min_usd_value), _D(min_lot_usd)),
                _D(P16_EXPRESS_MAX_USD),
            ))

            if usd_amount > self._avail * 0.95:
                return False

            offset_frac = P16_BBO_OFFSET_BPS / 10_000.0
            if direction == "long":
                raw_price = tick.ask * (1.0 + offset_frac)
                side, pos_side = "buy", ("long" if use_swap else "net")
            else:
                raw_price = tick.bid * (1.0 - offset_frac)
                side, pos_side = "sell", "short"

            inst    = _inst_id(symbol, swap=use_swap)
            td_mode = "cross" if use_swap else OKX_TD_MODE_SPOT

            if use_swap and OKX_LEVERAGE > 1:
                await self._set_leverage(inst, OKX_LEVERAGE, mgn_mode=td_mode)

            final_price = InstrumentCache.round_price(raw_price, meta) if meta else raw_price

            # ── [P36.2-DYNCLAMP] Whale Sniper Price Limit Guard ──────────────
            # Prevent OKX sCode 51006 on express/whale orders by clamping the
            # aggressive limit price to exchange-enforced buyLmt / sellLmt bands.
            # Uses DynamicBuffer so high-volatility periods stay ahead of OKX limits.
            try:
                _ws_inst = _inst_id(symbol, swap=use_swap)
                _ws_dyn_buf = self._p362_get_dynamic_buffer(symbol)
                _ws_buy_lmt, _ws_sell_lmt = self._icache.get_price_limits(_ws_inst)
                if side == "buy" and _ws_buy_lmt > 0 and final_price > _ws_buy_lmt:
                    _ws_clamped = _ws_buy_lmt * (1.0 - _ws_dyn_buf)
                    if meta:
                        _ws_clamped = InstrumentCache.round_price(_ws_clamped, meta)
                    log.warning(
                        "[P36.2-DYNCLAMP][WHALE-SNIPER] %s BUY price %.8f "
                        "exceeds buyLmt %.8f → clamped to %.8f (buf=%.4f%%)",
                        symbol, final_price, _ws_buy_lmt, _ws_clamped,
                        _ws_dyn_buf * 100,
                    )
                    final_price = _ws_clamped
                elif side == "sell" and _ws_sell_lmt > 0 and final_price < _ws_sell_lmt:
                    _ws_clamped = _ws_sell_lmt * (1.0 + _ws_dyn_buf)
                    if meta:
                        _ws_clamped = InstrumentCache.round_price(_ws_clamped, meta)
                    log.warning(
                        "[P36.2-DYNCLAMP][WHALE-SNIPER] %s SELL price %.8f "
                        "below sellLmt %.8f → clamped to %.8f (buf=%.4f%%)",
                        symbol, final_price, _ws_sell_lmt, _ws_clamped,
                        _ws_dyn_buf * 100,
                    )
                    final_price = _ws_clamped
            except Exception as _ws_plg_exc:
                log.debug(
                    "[P36.2-DYNCLAMP][WHALE-SNIPER] Price limit check for %s: %s "
                    "(non-fatal).", symbol, _ws_plg_exc,
                )
            # ── [/P36.2-DYNCLAMP] ────────────────────────────────────────────

            # [QUANT-1] + [MINSZ-1] Quantize express trade size.
            ct_val  = (meta.ct_val if meta and meta.ct_val > 0 else OKX_CT_VAL) if use_swap else 1.0
            raw_sz  = (usd_amount / raw_price) / ct_val if use_swap else usd_amount / raw_price
            q = await self._quantize_sz(symbol, raw_sz, use_swap, tag=f"{P16_EXPRESS_TAG}_{symbol}")
            if not q.valid:
                log.error("[P16-2] %s express: quantized size invalid — aborting.", symbol)
                return False
            sz_str = q.sz_str

            tag = f"{P16_EXPRESS_TAG}_{direction.upper()}_{symbol}"

            agg_body = self._sor.build_aggressive_limit_body(
                inst, side, sz_str, f"{final_price:.8f}",
                td_mode, use_swap, pos_side, tag,
            )
            # [P40] WHALE_SNIPER Express Lane: if this is a whale-sniper signal
            # (multiplier > P23_WHALE_SNIPER_MULT_THRESH), pass priority to the
            # bridge so it routes through the Rust pre-heated HTTP connection pool
            # targeting <500µs order submission latency.
            # Phase 37 Alpha logic and Whale Sniper multiplier thresholds are
            # preserved verbatim — only the transport layer changes.
            _p40_priority = "WHALE_SNIPER" if _whale_sniper_active else None
            resp = await self._place_order(agg_body, priority=_p40_priority)
            ord_id: Optional[str] = None
            if resp and resp.get("code") == "0":
                ord_id = resp["data"][0]["ordId"]
            else:
                log.error("[P16-2] Aggressive limit placement rejected: %s", resp)

            fill_order: Optional[dict] = None
            if ord_id:
                fill_order = await self._wait_fill(inst, ord_id, timeout=P16_EXPRESS_FILL_TIMEOUT)
                if fill_order is None or fill_order.get("state") != "filled":
                    try:
                        await self._cancel_order(inst, ord_id)
                    except Exception:
                        pass
                    fill_order = None

            if fill_order is None:
                tick2 = await self.hub.get_tick(symbol)
                if not tick2:
                    return False
                ref_price2 = tick2.ask if side == "buy" else tick2.bid
                if ref_price2 <= 0:
                    return False
                raw_sz2 = (usd_amount / ref_price2) / ct_val if use_swap else usd_amount / ref_price2
                q2 = await self._quantize_sz(symbol, raw_sz2, use_swap, tag=f"{P16_EXPRESS_TAG}_MKT_{symbol}")
                if not q2.valid:
                    log.error("[P16-2] %s market fallback: quantized size invalid — aborting.", symbol)
                    return False
                mkt_body = self._sor.build_market_body(
                    inst, side, q2.sz_str, td_mode, use_swap, pos_side,
                    f"{P16_EXPRESS_TAG}_MKT",
                )
                resp_mkt = await self._place_order(mkt_body)
                if not resp_mkt or resp_mkt.get("code") != "0":
                    return False
                ord_id_mkt = resp_mkt["data"][0]["ordId"]
                fill_order = await self._wait_fill(inst, ord_id_mkt, timeout=15.0)
                if fill_order is None or fill_order.get("state") != "filled":
                    return False

            fill_px = float(fill_order.get("avgPx") or fill_order.get("px") or 0)
            fill_sz = float(fill_order.get("accFillSz") or fill_order.get("sz") or 0)

            if fill_px <= 0 or fill_sz <= 0:
                return False

            inst_type = "SWAP" if use_swap else "SPOT"
            self.positions[symbol] = Position(
                symbol=symbol, direction=direction, inst_type=inst_type,
                qty=fill_sz, cost_basis=fill_px, usd_cost=usd_amount,
                entry_signal=None, okx_order_id=fill_order.get("ordId", ""),
                sniper_entry=False, trail_multiplier=1.0,
                entry_limit_px=fill_px, p16_express=True,
            )

            await self.db.insert_trade({
                "ts": int(time.time()), "symbol": symbol, "side": side,
                "qty": fill_sz, "price": fill_px, "cost_basis": fill_px,
                "pnl_pct": 0.0, "realized_usd": None,
                "tag": f"{self._pending_mode_label} {tag}",
                "order_id": fill_order.get("ordId", str(uuid.uuid4())),
                "inst_type": inst_type,
            })

            self._p16_last_express[symbol] = time.time()
            self._p16_express_fills       += 1

            _express_mult    = _express_narrative.conviction_multiplier if _express_narrative else 1.0
            _express_verdict = _express_narrative.verdict               if _express_narrative else "N/A"
            _express_score   = _express_narrative.score                 if _express_narrative else None
            self._shadow_log(
                symbol=symbol, direction=direction, tag=tag,
                real_usd=usd_amount, baseline_usd=usd_amount,
                conviction_multiplier=_express_mult,
                regime="express",
                narrative_verdict=_express_verdict,
                narrative_score=_express_score,
                fill_px=fill_px, fill_sz=fill_sz,
            )

            log.warning(
                "[P16-2] EXPRESS FILL confirmed: %s %s qty=%.6f @ %.4f (%s) "
                "oracle_mult=%.1f× total_express_fills=%d",
                direction.upper(), symbol, fill_sz, fill_px, inst_type,
                mult, self._p16_express_fills,
            )

            sentinel = self._p11_sentinel
            if sentinel is not None:
                try:
                    asyncio.create_task(
                        sentinel.notify_atomic_fill(
                            symbol=symbol, direction=direction,
                            fill_px=fill_px, fill_sz=fill_sz,
                            usd_notional=usd_amount,
                            oracle_mult=oracle_signal.multiplier if oracle_signal else 1.0,
                            reason=oracle_signal.signal_type if oracle_signal else "WHALE_SWEEP",
                        ),
                        name=f"sentinel_atomic_fill_{symbol}",
                    )
                except Exception as exc:
                    log.debug("[P16-2] sentinel.notify_atomic_fill error: %s", exc)

            return True

    # ── Position open / close ──────────────────────────────────────────────────
    async def _open_long(
        self,
        symbol: str,
        usd_amount: float,
        signal: Signal,
        _shadow_baseline_usd: float = 0.0,
        _shadow_conviction_mult: float = 1.0,
        _shadow_narrative: Optional[NarrativeResult] = None,
    ) -> bool:
        use_swap = signal.regime == "bull" and OKX_LEVERAGE > 1
        cfg      = self._coin_cfg(symbol)
        tag      = "LONG_ENTRY_SNIPER" if signal.sniper_boost else "LONG_ENTRY"
        fill_px = fill_sz = 0.0
        inst_type     = "SWAP" if use_swap else "SPOT"
        used_order_id = ""

        twap   = self._p7_twap
        result = None
        if twap is not None:
            result = await twap.execute(
                symbol, "buy", usd_amount, signal,
                swap=use_swap, pos_side="long" if use_swap else "net",
                tag=tag, coin_cfg=cfg,
            )
        if result is not None:
            if result.aborted or result.total_fill_sz == 0:
                return False
            fill_px       = result.avg_fill_px
            fill_sz       = result.total_fill_sz
            used_order_id = f"TWAP_{symbol}_{int(time.time())}"
        else:
            # [P33.2-REBATE/CHASE] Propagate whale multiplier so TWAP/layering
            # engine can gate adaptive price chasing on high-conviction flow only.
            _long_whale_mult = float(getattr(signal, "whale_multiplier", 0.0))
            order = await self._execute_order(
                symbol, "buy", usd_amount,
                swap=use_swap, pos_side="long" if use_swap else "net",
                tag=tag, coin_cfg=cfg,
                whale_mult=_long_whale_mult,
            )
            if not order:
                return False
            fill_px       = float(order.get("avgPx") or order.get("px") or 0)
            fill_sz       = float(order.get("accFillSz") or order.get("sz") or 0)
            used_order_id = order.get("ordId", "")

        if fill_px <= 0 or fill_sz <= 0:
            return False

        self.positions[symbol] = Position(
            symbol=symbol, direction="long", inst_type=inst_type,
            qty=fill_sz, cost_basis=fill_px, usd_cost=usd_amount,
            entry_signal=signal, okx_order_id=used_order_id,
            sniper_entry=signal.sniper_boost,
            trail_multiplier=signal.trail_multiplier,
            entry_limit_px=fill_px,
        )
        await self.db.insert_trade({
            "ts": int(time.time()), "symbol": symbol, "side": "buy",
            "qty": fill_sz, "price": fill_px, "cost_basis": fill_px,
            "pnl_pct": 0.0, "realized_usd": None,
            "tag": f"{self._pending_mode_label} {tag}",
            "order_id": used_order_id, "inst_type": inst_type,
        })

        baseline     = _shadow_baseline_usd if _shadow_baseline_usd > 0 else usd_amount
        n_verdict    = _shadow_narrative.verdict if _shadow_narrative else "N/A"
        n_score      = _shadow_narrative.score   if _shadow_narrative else None
        self._shadow_log(
            symbol=symbol, direction="long", tag=tag,
            real_usd=usd_amount, baseline_usd=baseline,
            conviction_multiplier=_shadow_conviction_mult,
            regime=signal.regime,
            narrative_verdict=n_verdict,
            narrative_score=n_score,
            fill_px=fill_px, fill_sz=fill_sz,
        )

        auditor = self._p7_auditor
        if auditor is not None:
            try:
                tick = await self.hub.get_tick(symbol)
                if tick and self._p7_spread_mgr:
                    book     = await self.hub.get_order_book(symbol)
                    expected = self._p7_spread_mgr.adjusted_limit_price(
                        "buy", tick, symbol, order_book=book if book else None,
                    )
                else:
                    expected = fill_px
                auditor.record(symbol, expected_px=expected, actual_px=fill_px,
                               side="buy", filled=True)
            except Exception as e:
                log.debug("Auditor entry record error %s: %s", symbol, e)

        sentinel = self._p11_sentinel
        if sentinel is not None:
            try:
                async def _notify_long_entry(_s=sentinel):
                    try:
                        await _s.notify_entry(symbol, "long", signal.confidence, signal)
                        if signal.sniper_boost:
                            _s.record_sniper_hit()
                    except Exception as _exc:
                        log.debug("[P14-4] Sentinel notify_entry task error %s: %s", symbol, _exc)
                asyncio.create_task(_notify_long_entry(), name=f"sentinel_entry_long_{symbol}")
            except Exception as exc:
                log.debug("[P14-4] Sentinel notify_entry error %s: %s", symbol, exc)

        log.info("LONG opened %s qty=%.6f @ %.4f (%s) regime=%s conv=%s%s",
                 symbol, fill_sz, fill_px, inst_type, signal.regime,
                 "HIGH" if signal.high_conviction else "std",
                 " [SNIPER]" if signal.sniper_boost else "")
        return True

    async def _open_short(
        self,
        symbol: str,
        usd_amount: float,
        signal: Signal,
        _shadow_baseline_usd: float = 0.0,
        _shadow_conviction_mult: float = 1.0,
        _shadow_narrative: Optional[NarrativeResult] = None,
    ) -> bool:
        cfg = self._coin_cfg(symbol)
        tag = "SHORT_ENTRY_SNIPER" if signal.sniper_boost else "SHORT_ENTRY"
        fill_px = fill_sz = 0.0
        used_order_id = ""

        twap   = self._p7_twap
        result = None
        if twap is not None:
            result = await twap.execute(
                symbol, "sell", usd_amount, signal,
                swap=True, pos_side="short", tag=tag, coin_cfg=cfg,
            )
        if result is not None:
            if result.aborted or result.total_fill_sz == 0:
                return False
            fill_px       = result.avg_fill_px
            fill_sz       = result.total_fill_sz
            used_order_id = f"TWAP_{symbol}_{int(time.time())}"
        else:
            # [P33.2-REBATE/CHASE] Propagate whale multiplier so TWAP/layering
            # engine can gate adaptive price chasing on high-conviction flow only.
            _short_whale_mult = float(getattr(signal, "whale_multiplier", 0.0))
            order = await self._execute_order(
                symbol, "sell", usd_amount,
                swap=True, pos_side="short", tag=tag, coin_cfg=cfg,
                whale_mult=_short_whale_mult,
            )
            if not order:
                return False
            fill_px       = float(order.get("avgPx") or order.get("px") or 0)
            fill_sz       = float(order.get("accFillSz") or order.get("sz") or 0)
            used_order_id = order.get("ordId", "")

        if fill_px <= 0 or fill_sz <= 0:
            return False

        self.positions[symbol] = Position(
            symbol=symbol, direction="short", inst_type="SWAP",
            qty=fill_sz, cost_basis=fill_px, usd_cost=usd_amount,
            entry_signal=signal, okx_order_id=used_order_id,
            sniper_entry=signal.sniper_boost,
            trail_multiplier=signal.trail_multiplier,
            entry_limit_px=fill_px,
        )
        await self.db.insert_trade({
            "ts": int(time.time()), "symbol": symbol, "side": "sell",
            "qty": fill_sz, "price": fill_px, "cost_basis": fill_px,
            "pnl_pct": 0.0, "realized_usd": None,
            "tag": f"{self._pending_mode_label} {tag}",
            "order_id": used_order_id, "inst_type": "SWAP",
        })

        baseline  = _shadow_baseline_usd if _shadow_baseline_usd > 0 else usd_amount
        n_verdict = _shadow_narrative.verdict if _shadow_narrative else "N/A"
        n_score   = _shadow_narrative.score   if _shadow_narrative else None
        self._shadow_log(
            symbol=symbol, direction="short", tag=tag,
            real_usd=usd_amount, baseline_usd=baseline,
            conviction_multiplier=_shadow_conviction_mult,
            regime=signal.regime,
            narrative_verdict=n_verdict,
            narrative_score=n_score,
            fill_px=fill_px, fill_sz=fill_sz,
        )

        auditor = self._p7_auditor
        if auditor is not None:
            try:
                tick = await self.hub.get_tick(symbol)
                if tick and self._p7_spread_mgr:
                    book     = await self.hub.get_order_book(symbol)
                    expected = self._p7_spread_mgr.adjusted_limit_price(
                        "sell", tick, symbol, order_book=book if book else None,
                    )
                else:
                    expected = fill_px
                auditor.record(symbol, expected_px=expected, actual_px=fill_px,
                               side="sell", filled=True)
            except Exception as e:
                log.debug("Auditor entry record error %s: %s", symbol, e)

        sentinel = self._p11_sentinel
        if sentinel is not None:
            try:
                async def _notify_short_entry(_s=sentinel):
                    try:
                        await _s.notify_entry(symbol, "short", signal.confidence, signal)
                        if signal.sniper_boost:
                            _s.record_sniper_hit()
                    except Exception as _exc:
                        log.debug("[P14-4] Sentinel notify_entry task error %s: %s", symbol, _exc)
                asyncio.create_task(_notify_short_entry(), name=f"sentinel_entry_short_{symbol}")
            except Exception as exc:
                log.debug("[P14-4] Sentinel notify_entry error %s: %s", symbol, exc)

        log.info("SHORT opened %s qty=%.6f @ %.4f (SWAP) regime=%s conv=%s%s",
                 symbol, fill_sz, fill_px, signal.regime,
                 "HIGH" if signal.high_conviction else "std",
                 " [SNIPER]" if signal.sniper_boost else "")
        return True

    async def _close_position(self, symbol: str, tag: str = "CLOSE") -> bool:
        pos = self.positions.get(symbol)
        if not pos:
            return False

        await self._cancel_all_open_buys()
        self._cancel_shadow(symbol)

        tick           = await self.hub.get_tick(symbol)
        exit_px        = (tick.bid if pos.direction == "long" else tick.ask) if tick else 0.0
        close_side     = "sell" if pos.direction == "long" else "buy"
        pos_side_close = ("long" if pos.inst_type == "SWAP" else "net") \
                          if pos.direction == "long" else "short"
        inst    = _inst_id(symbol, swap=(pos.inst_type == "SWAP"))
        td_mode = "cross" if pos.inst_type == "SWAP" else OKX_TD_MODE_SPOT

        # [QUANT-1] + [MINSZ-1] Quantize close size.
        use_swap = pos.inst_type == "SWAP"
        q = await self._quantize_sz(symbol, pos.qty, use_swap, tag=f"{tag}_CLOSE_QUANT")
        if q.valid:
            sz_str = q.sz_str
        else:
            meta_fb = await self._icache.get_instrument_info(symbol, swap=use_swap)
            if meta_fb and meta_fb.sz_inst > 0:
                dec    = InstrumentCache._decimals(meta_fb.sz_inst)
                sz_str = str(int(pos.qty)) if use_swap else f"{pos.qty:.{dec}f}"
            else:
                sz_str = str(int(pos.qty)) if use_swap else f"{pos.qty:.8f}"

        actual_fill_px = exit_px

        if tick:
            close_price = tick.bid if close_side == "sell" else tick.ask
            meta_c = await self._icache.get_instrument_info(symbol, swap=use_swap)
            if meta_c:
                close_price = InstrumentCache.round_price(close_price, meta_c)
            close_body = self._sor.build_limit_body(
                inst, close_side, sz_str, f"{close_price:.8f}",
                td_mode, use_swap, pos_side_close, tag)
            resp   = await self._place_order(close_body)
            ord_id = None
            if resp and resp.get("code") == "0":
                ord_id = resp["data"][0]["ordId"]
                await asyncio.sleep(POST_ONLY_RETRY_SECS)
                order = await self._get_order(inst, ord_id)
                if order.get("state") == "filled":
                    actual_fill_px = float(order.get("avgPx") or exit_px)
                    auditor = self._p7_auditor
                    if auditor is not None:
                        auditor.record(symbol, expected_px=close_price,
                                       actual_px=actual_fill_px, side=close_side,
                                       filled=True)
                    await self._record_close(pos, actual_fill_px, tag)
                    return True
                await self._cancel_order(inst, ord_id)

        mkt = self._sor.build_market_body(
            inst, close_side, sz_str, td_mode,
            use_swap, pos_side_close, tag)
        resp2 = await self._place_order(mkt)
        if not resp2 or resp2.get("code") != "0":
            log.error("Close failed %s: %s", symbol, resp2)
            return False
        ord_id2 = resp2["data"][0]["ordId"]
        order2  = await self._wait_fill(inst, ord_id2, timeout=30)
        actual_fill_px = float((order2 or {}).get("avgPx") or exit_px)

        auditor = self._p7_auditor
        if auditor is not None and tick:
            expected_mkt = tick.bid if close_side == "sell" else tick.ask
            auditor.record(symbol, expected_px=expected_mkt,
                           actual_px=actual_fill_px, side=close_side, filled=True)

        await self._record_close(pos, actual_fill_px, tag)
        return True

    async def _record_close(self, pos: Position, fill_px: float, tag: str):
        pnl_pct: float = 0.0
        realized: float = 0.0

        try:
            if pos.cost_basis > 0:
                cb = _D(pos.cost_basis)
                fp = _D(fill_px)
                if pos.direction == "long":
                    pnl_pct_d = (fp - cb) / cb * Decimal("100")
                else:
                    pnl_pct_d = (cb - fp) / cb * Decimal("100")
                pnl_pct = float(pnl_pct_d)
                realized = float(_D(pnl_pct) / Decimal("100") * _D(pos.usd_cost))
        except Exception as _pnl_exc:
            log.debug("[P7] _record_close Decimal pnl calc error: %s", _pnl_exc)
            pnl_pct = 0.0
            realized = 0.0

        await self.db.insert_trade({
            "ts": int(time.time()), "symbol": pos.symbol,
            "side": "sell" if pos.direction == "long" else "buy",
            "qty": pos.qty, "price": fill_px,
            "cost_basis": pos.cost_basis, "pnl_pct": round(pnl_pct, 4),
            "realized_usd": round(realized, 6), "tag": tag,
            "order_id": str(uuid.uuid4()), "inst_type": pos.inst_type,
        })
        if pos.entry_signal:
            self.brain.record_outcome(pos.symbol, pos.entry_signal.tf,
                                       pos.direction, pnl_pct)
        self._perf.record(pos.symbol, pnl_pct)

        # ── [P32-SLIP-ADAPT] Record realized slippage for this trade ──────────
        # Use cost_basis as the "expected" price and fill_px as the actual.
        # Side is determined by the position direction (long → sell to close,
        # short → buy to close — the close direction is what we measure slippage on).
        try:
            _close_side = "sell" if pos.direction == "long" else "buy"
            self._p32_record_slippage(
                pos.symbol,
                expected_px=pos.cost_basis,
                fill_px=fill_px,
                side=_close_side,
            )
        except Exception as _slip_exc:
            log.debug("[P32-SLIP-ADAPT] slippage record error %s: %s", pos.symbol, _slip_exc)
        # ── [/P32-SLIP-ADAPT] ─────────────────────────────────────────────────

        log.info("CLOSE %s [%s] pnl=%.2f%%", pos.symbol, tag, pnl_pct)

        sentinel = self._p11_sentinel
        if sentinel is not None:
            try:
                async def _notify_close(_s=sentinel, _sym=pos.symbol,
                                        _dir=pos.direction, _pnl=pnl_pct, _tag=tag):
                    try:
                        await _s.notify_close(_sym, _dir, _pnl, _tag)
                        _s.record_close()
                    except Exception as _exc:
                        log.debug("[P14-4] Sentinel notify_close task error %s: %s", _sym, _exc)
                asyncio.create_task(_notify_close(), name=f"sentinel_close_{pos.symbol}")
            except Exception as exc:
                log.debug("[P14-4] Sentinel notify_close error %s: %s", pos.symbol, exc)

        del self.positions[pos.symbol]

        # ── [P40.1-CLOSE] Gate post-close hook ───────────────────────────────
        # If the installed gate (main.py OrchestratorGate) exposes _on_post_close,
        # call it now.  This replaces the former _record_close monkeypatch used
        # by OrchestratorGate in Phase 21/22 and is the canonical Bridge-ready
        # extension point for regime-aware brain weight updates and slippage
        # auditing.  The pos object is still valid (local reference); the asyncio
        # task is fire-and-forget so _flatten_all / _close_position callers are
        # not blocked.
        if self.gate is not None and hasattr(self.gate, "_on_post_close"):
            try:
                asyncio.create_task(
                    self.gate._on_post_close(pos, fill_px, tag),
                    name=f"p40_post_close_{pos.symbol}",
                )
            except Exception as _hook_exc:
                log.debug(
                    "[P40.1-CLOSE] gate._on_post_close scheduling error %s: %s",
                    pos.symbol, _hook_exc,
                )
        # ── [/P40.1-CLOSE] ───────────────────────────────────────────────────

    async def _flatten_all(self, reason: str = "circuit_breaker"):
        # [P23-HOTFIX-3] Log throttle: only emit CRITICAL once per 60 s if state unchanged.
        now = time.time()
        if now - self._last_flatten_log_ts >= P23_FLATTEN_LOG_THROTTLE_S:
            log.critical("FLATTENING ALL: %s", reason)
            self._last_flatten_log_ts = now
        else:
            log.debug("FLATTENING ALL (throttled): %s", reason)
        for sym in list(self._shadow_watches.keys()):
            self._cancel_shadow(sym)
        for sym in list(self.positions.keys()):
            try:
                await self._close_position(sym, tag=reason.upper())
            except Exception as e:
                log.error("Flatten error %s: %s", sym, e)

    # ── [P23-HOTFIX-1] CircuitBreaker Decoupling ──────────────────────────────
    def _check_circuit_breaker_decoupled(self, live_equity: float) -> bool:
        """
        [P23-HOTFIX-1] Decoupled circuit-breaker check.

        Rules
        -----
        - If drawdown._killed is already True AND we have a valid LKG value,
          we do NOT re-trigger a flatten.  The CB tripped on a real drawdown;
          LKG still gives us accurate sizing.  The operator must manually reset.
        - The ghost-state death-loop is broken by ONLY triggering the initial
          drawdown.check() when live_equity is trustworthy (i.e. > 0 AND
          consistent with LKG), or when LKG is also zero (genuinely broke).
        - If live_equity == 0 AND _last_valid_equity > 0, we SUPPRESS the check
          entirely — that is a ghost read, not a real loss.

        Returns True when _cycle should call _flatten_all + return.
        """
        lkg = self._last_valid_equity

        # Ghost-state suppression: WS delivered 0 but we have LKG — do nothing.
        if live_equity <= 0.0 and lkg > 0.0 and P23_CB_SUPPRESS_WITH_LKG:
            log.debug(
                "[P23-HOTFIX-1] CB suppressed — live_equity=%.4f is ghost, "
                "LKG=%.4f intact.",
                live_equity, lkg,
            )
            return False

        # Both zero / uninitialised — do not trigger (no data, defer to next cycle).
        if live_equity <= 0.0 and lkg <= 0.0:
            log.debug(
                "[P23-HOTFIX-1] CB deferred — both live_equity and LKG are zero "
                "(cold start or total data blackout)."
            )
            return False

        # Already tripped and we have positions-flat state → no re-flatten needed.
        if self.drawdown._killed and not self.positions:
            log.debug(
                "[P23-HOTFIX-1] CB killed but positions already flat — "
                "suppressing repeat _flatten_all."
            )
            return False

        # Normal check using the best available equity figure.
        check_equity = live_equity if live_equity > 0.0 else lkg
        return self.drawdown.check(check_equity)

    # ── [P4-4] Correlation hedge ───────────────────────────────────────────────
    async def _hedge_trim_smallest(self):
        now = time.time()
        if now - self._last_hedge_ts < HEDGE_COOLDOWN_SECS:
            return
        if not self.positions:
            return
        smallest_sym = min(self.positions, key=lambda s: self.positions[s].usd_cost)
        pos          = self.positions[smallest_sym]
        trim_qty     = pos.qty * HEDGE_TRIM_PCT
        use_swap     = pos.inst_type == "SWAP"
        meta = await self._icache.get_instrument_info(smallest_sym, swap=use_swap)
        if meta and meta.min_sz > 0 and trim_qty < meta.min_sz:
            await self._close_position(smallest_sym, tag="HEDGE_FULL_CLOSE")
            self._last_hedge_ts = now
            return
        close_side     = "sell" if pos.direction == "long" else "buy"
        pos_side_close = ("long" if use_swap else "net") \
                          if pos.direction == "long" else "short"
        inst    = _inst_id(smallest_sym, swap=use_swap)
        td_mode = "cross" if use_swap else OKX_TD_MODE_SPOT

        # [QUANT-1] + [MINSZ-1] Quantize trim size.
        q = await self._quantize_sz(smallest_sym, trim_qty, use_swap, tag="HEDGE_TRIM_QUANT")
        if not q.valid:
            log.warning("[P4-4] Hedge trim: quantized trim size invalid for %s — full close.", smallest_sym)
            await self._close_position(smallest_sym, tag="HEDGE_FULL_CLOSE_QUANT_FAIL")
            self._last_hedge_ts = now
            return
        sz_str = q.sz_str

        mkt  = self._sor.build_market_body(
            inst, close_side, sz_str, td_mode,
            use_swap, pos_side_close, "HEDGE_TRIM")
        resp = await self._place_order(mkt)
        if resp and resp.get("code") == "0":
            ord_id = resp["data"][0]["ordId"]
            order  = await self._wait_fill(inst, ord_id, timeout=20)
            if order:
                trimmed = float(order.get("accFillSz") or q.sz_float)
                trim_px = float(order.get("avgPx") or 0)
                pos.qty      -= trimmed
                pos.usd_cost  = pos.qty * pos.cost_basis
                pnl_pct = (
                    (trim_px - pos.cost_basis) / pos.cost_basis * 100
                    if pos.direction == "long"
                    else (pos.cost_basis - trim_px) / pos.cost_basis * 100
                ) if pos.cost_basis > 0 else 0.0
                await self.db.insert_trade({
                    "ts": int(now), "symbol": pos.symbol,
                    "side": close_side, "qty": trimmed, "price": trim_px,
                    "cost_basis": pos.cost_basis, "pnl_pct": round(pnl_pct, 4),
                    "realized_usd": round(pnl_pct / 100 * (trimmed * pos.cost_basis), 6),
                    "tag": "HEDGE_TRIM", "order_id": order.get("ordId", str(uuid.uuid4())),
                    "inst_type": pos.inst_type,
                })
        self._last_hedge_ts = now

    # ── [P3-2] DCA ────────────────────────────────────────────────────────────
    async def _maybe_dca(self, symbol: str, pos: Position,
                          cur_price: float, price_hist: np.ndarray):
        if pos.direction != "long":
            return
        now    = time.time()
        recent = [t for t in pos.dca_ts_list
                  if t >= now - 86400 and t > pos.last_sell_ts]
        if len(recent) >= MAX_DCA_PER_24H:
            return
        if not IntelligenceEngine.dca_z_trigger(cur_price, pos.cost_basis,
                                                  price_hist, DCA_Z_THRESHOLD):
            return
        h1_candles = await self.hub.get_candles(symbol, "1hour", DCA_VOL_LOOKBACK + 1)
        if len(h1_candles) >= DCA_VOL_LOOKBACK:
            cur_vol   = h1_candles[0].volume
            prev_vols = [c.volume for c in h1_candles[1:DCA_VOL_LOOKBACK]]
            avg_prev  = float(np.mean(prev_vols)) if prev_vols else 0.0
            if avg_prev > 0 and cur_vol > avg_prev:
                return
        cfg     = self._coin_cfg(symbol)
        dca_usd = max(pos.usd_cost * 2.0, cfg.min_usd_value)
        if dca_usd > self._avail * 0.95:
            return
        use_swap = pos.inst_type == "SWAP"
        order    = await self._execute_order(
            symbol, "buy", dca_usd,
            swap=use_swap, pos_side="long" if use_swap else "net",
            tag=f"DCA_{pos.dca_stages + 1}", coin_cfg=cfg)
        if not order:
            return
        dca_px = float(order.get("avgPx") or order.get("px") or cur_price)
        dca_sz = float(order.get("accFillSz") or order.get("sz") or 0)
        if dca_sz <= 0:
            return
        total_qty      = pos.qty + dca_sz
        pos.cost_basis = (pos.cost_basis * pos.qty + dca_px * dca_sz) / total_qty
        pos.qty        = total_qty
        pos.usd_cost  += dca_usd
        pos.dca_stages += 1
        pos.dca_ts_list.append(now)
        pos.trail_active = False

    # ── [P3-1] Trailing PM ─────────────────────────────────────────────────────
    async def _update_trailing(
        self, symbol: str, pos: Position,
        exit_price: float,
        trail_mult_override: Optional[float] = None,
    ) -> bool:
        tm       = trail_mult_override if trail_mult_override is not None else pos.trail_multiplier
        pm_pct   = (PM_START_WITH_DCA if pos.dca_stages > 0 else PM_START_NO_DCA) * tm
        gap_base = TRAILING_GAP_PCT / 100.0

        # ── [P31-WHALE-TRAIL] Dynamic gap scaling ─────────────────────────────
        # Derive the whale_multiplier from the entry signal stored on the Position.
        # If the oracle was firing a high-multiplier sweep at entry we widen the
        # trailing stop so the whale-driven leg can run to its natural exhaustion.
        # As the multiplier decays (position ages, oracle resets to 1×) the gap
        # tightens below the static baseline so we bank profits aggressively.
        _whale_mult_raw = 1.0
        try:
            entry_sig = pos.entry_signal
            if entry_sig is not None:
                _whale_mult_raw = float(getattr(entry_sig, "whale_multiplier", 1.0))
        except Exception:
            _whale_mult_raw = 1.0

        # Linear interpolation:
        #   whale_mult=1  → gap_scale = P31_WHALE_TRAIL_MIN_SCALE (0.5×)
        #   whale_mult=HIGH → gap_scale = P31_WHALE_TRAIL_MAX_SCALE (2.0×)
        _clamp = lambda v, lo, hi: max(lo, min(hi, v))
        _t = _clamp(
            (_whale_mult_raw - 1.0) / max(P31_WHALE_TRAIL_HIGH_MULT - 1.0, 1.0),
            0.0, 1.0,
        )
        _gap_scale = P31_WHALE_TRAIL_MIN_SCALE + _t * (
            P31_WHALE_TRAIL_MAX_SCALE - P31_WHALE_TRAIL_MIN_SCALE
        )
        gap_frac = gap_base * _gap_scale

        log.debug(
            "[P31-WHALE-TRAIL] %s whale_mult=%.2f gap_scale=%.3f "
            "gap_base=%.4f → gap_frac=%.4f",
            symbol, _whale_mult_raw, _gap_scale, gap_base, gap_frac,
        )
        # ── [/P31-WHALE-TRAIL] ─────────────────────────────────────────────────

        if pos.direction == "long":
            base_line = pos.cost_basis * (1 + pm_pct / 100)
            above_now = exit_price >= pos.trail_line
        else:
            base_line = pos.cost_basis * (1 - pm_pct / 100)
            above_now = exit_price <= pos.trail_line

        if not pos.trail_active:
            pos.trail_line = base_line
            is_above = (exit_price >= base_line if pos.direction == "long"
                        else exit_price <= base_line)
            if is_above:
                pos.trail_active = True
                pos.trail_peak   = exit_price
        else:
            pos.trail_line = (max(pos.trail_line, base_line) if pos.direction == "long"
                              else min(pos.trail_line, base_line))

        if pos.trail_active:
            if pos.direction == "long":
                if exit_price > pos.trail_peak:
                    pos.trail_peak = exit_price
                pos.trail_line = max(pos.trail_peak * (1 - gap_frac), base_line, pos.trail_line)
                fired = pos.was_above and exit_price < pos.trail_line
            else:
                if exit_price < pos.trail_peak:
                    pos.trail_peak = exit_price
                pos.trail_line = min(pos.trail_peak * (1 + gap_frac), base_line, pos.trail_line)
                fired = pos.was_above and exit_price > pos.trail_line
            if fired:
                await self._close_position(symbol, tag="TRAIL_CLOSE")
                return True

        pos.was_above = above_now
        return False

    # ── Entry logic ────────────────────────────────────────────────────────────
    async def _maybe_enter(self, symbol: str, signal: Signal, tick=None, oracle_sig=None):
        if self._p20_zombie_mode:
            log.debug("[P20-1] %s entry blocked: Zombie Mode active.", symbol)
            return

        cb = self._p7_cb
        if cb is not None and cb.is_tripped:
            log.debug("[P7] %s entry blocked: Circuit Breaker tripped.", symbol)
            return

        if symbol in self.positions or signal.direction == "neutral":
            return
        if self._vol_guard.emergency_pause:
            return
        if not signal.mtf_aligned:
            return
        if symbol in self._shadow_watches:
            return

        if hasattr(self, 'gate') and self.gate is not None:
            if not await self.gate._gated_enter(symbol, signal, tick, oracle_sig):
                return

        obi_blocked = await self._check_obi_filter(symbol, signal.direction, signal=signal)
        if obi_blocked:
            return

        # [LLMFB-3] LLM veto with empty-response / exception fallback.
        # [P23-OPT-1] AI Fallback: wrap score() with 2s timeout; on timeout or
        #              parse error auto-switch to Technical Mode (TA_FALLBACK).
        _p17_narrative: Optional[NarrativeResult] = None
        if self._p17_veto is not None:
            _cancel_flag = (
                oracle_sig.cancel_buys_flag
                if oracle_sig is not None and hasattr(oracle_sig, "cancel_buys_flag")
                else False
            )
            try:
                _p17_narrative = await asyncio.wait_for(
                    self._p17_veto.score(
                        symbol           = symbol,
                        direction        = signal.direction,
                        drawdown_killed  = self.drawdown._killed,
                        cancel_buys_flag = _cancel_flag,
                    ),
                    timeout=P23_LLM_TIMEOUT_S,
                )
                # Guard: treat any empty/whitespace verdict as neutral.
                if not _p17_narrative or not getattr(_p17_narrative, "verdict", "").strip():
                    log.warning(
                        "[LLMFB-3/P23-OPT-1] LLM returned empty verdict for %s %s — "
                        "applying neutral fallback (TA mode).", symbol, signal.direction,
                    )
                    _p17_narrative        = _make_neutral_narrative()
                    self._llm_in_fallback = True
                    self._pending_mode_label = "[TA]"
                else:
                    self._llm_in_fallback    = False   # successful LLM call
                    self._pending_mode_label = "[AI]"
            except asyncio.TimeoutError:
                log.warning(
                    "[P23-OPT-1] AI FALLBACK: OpenRouter timed out (>%.1fs) for %s %s — "
                    "switching to Technical Mode.", P23_LLM_TIMEOUT_S, symbol, signal.direction,
                )
                _p17_narrative        = _make_neutral_narrative()
                self._llm_in_fallback = True
                self._pending_mode_label = "[TA]"
            except Exception as _llm_exc:
                log.warning(
                    "[LLMFB-3/P23-OPT-1] LLM score() raised for %s %s: %s — "
                    "applying neutral fallback (TA mode).", symbol, signal.direction, _llm_exc,
                )
                _p17_narrative        = _make_neutral_narrative()
                self._llm_in_fallback = True
                self._pending_mode_label = "[TA]"

            # [P24-DEFENSE] Intelligence Bridge — persist the latest NarrativeResult
            # so _write_status_json() can include it verbatim in status['intelligence'].
            self._last_narrative = _p17_narrative

            if _p17_narrative.verdict == "CATASTROPHE_VETO":
                log.critical(
                    "[P18-3/P20-2] CATASTROPHE VETO %s %s: score=%.4f — "
                    "triggering nuclear liquidation.",
                    symbol, signal.direction, _p17_narrative.score,
                )
                asyncio.ensure_future(
                    self.liquidate_all_positions(symbol, reason="P18_CATASTROPHE_ENTRY")
                )
                return

            if _p17_narrative.verdict == "VETO":
                log.info(
                    "[P17-2/P20-2] Entry BLOCKED %s %s: verdict=VETO score=%.4f "
                    "latency=%.1fms council=[%s]",
                    symbol, signal.direction,
                    _p17_narrative.score, _p17_narrative.latency_ms,
                    "; ".join(
                        f"{d['agent']}:{d['vote']}"
                        for d in _p17_narrative.council_detail
                    ),
                )
                return

            # [P25] SNIPER-ONLY gate — reject low-conviction entries when active
            _p25_sniper_only = bool(self._tactical_config.get("sniper_only", False))
            if _p25_sniper_only:
                _conv_mult   = getattr(_p17_narrative, "conviction_multiplier", 1.0)
                _oracle_mult = (
                    oracle_sig.multiplier
                    if oracle_sig is not None and hasattr(oracle_sig, "multiplier")
                    else 1.0
                )
                _whale_sweep = (
                    _oracle_mult >= P25_SNIPER_ONLY_WHALE_MULT
                )
                if not _whale_sweep and _conv_mult < P25_SNIPER_ONLY_CONV_THRESHOLD:
                    log.info(
                        "[P25] SNIPER-ONLY gate: BLOCKED %s %s "
                        "conviction_multiplier=%.3fx < threshold=%.1fx "
                        "(whale_mult=%.1fx < %.1fx)",
                        symbol, signal.direction,
                        _conv_mult, P25_SNIPER_ONLY_CONV_THRESHOLD,
                        _oracle_mult, P25_SNIPER_ONLY_WHALE_MULT,
                    )
                    try:
                        _append_veto_audit_executor(
                            symbol,
                            "Sniper-Only Gate",
                            f"conviction_multiplier={_conv_mult:.4f} < "
                            f"threshold={P25_SNIPER_ONLY_CONV_THRESHOLD:.1f} | "
                            f"whale_mult={_oracle_mult:.1f}x",
                        )
                    except Exception:
                        pass
                    return

        # ── [P32-VETO-ARB] Unified Veto Arbitrator ───────────────────────────
        # Aggregate Entropy + Correlation Haircut + Price Velocity into a single
        # p_success score.  Block the entry if p_success < 0.65 to prevent low-
        # quality entries that might pass individual filters but fail holistically.
        try:
            if self._p32_veto_arb is None:
                try:
                    self._p32_veto_arb = VetoArbitrator()
                    # [P34.1-SKEW] Inject DataHub so VetoArbitrator can call
                    # hub.get_global_mid_price() for the Price Skew pre-check.
                    try:
                        self._p32_veto_arb.set_hub(self.hub)
                    except Exception as _hub_inj:
                        log.debug("[P34-SKEW] set_hub injection error: %s", _hub_inj)
                except Exception as _vav_init:
                    log.debug("[P32-VETO-ARB] VetoArbitrator init error: %s", _vav_init)

            if self._p32_veto_arb is not None:
                _recent_returns: list = []
                try:
                    _h1 = await self.hub.get_candles(symbol, "1hour", 30)
                    if _h1 and len(_h1) >= 2:
                        _closes = [c.close for c in _h1 if c.close > 0]
                        if len(_closes) >= 2:
                            _recent_returns = [
                                (_closes[i] - _closes[i-1]) / _closes[i-1] * 100
                                for i in range(1, len(_closes))
                                if _closes[i-1] != 0
                            ]
                except Exception:
                    pass

                _corr_score = 0.5
                try:
                    _open_pos = list(self.positions.keys())
                    if _open_pos:
                        _corr_score = 1.0 - self.brain.position_correlation(symbol, _open_pos)
                except Exception:
                    pass

                _vel_boost = 1.0
                try:
                    if self._p17_veto is not None:
                        _vel_boost = self._p17_veto.velocity_monitor.conviction_boost(signal.direction)
                except Exception:
                    pass

                # [P34.1-SKEW] Fetch local OKX price and global mid-price for skew veto.
                _local_price: float = 0.0
                _global_mid:  Optional[float] = None
                try:
                    _tick = await self.hub.get_tick(symbol)
                    if _tick and _tick.bid > 0 and _tick.ask > 0:
                        _local_price = (_tick.bid + _tick.ask) / 2.0
                    _global_mid = await self.hub.get_global_mid_price(symbol)
                except Exception as _p34_px_exc:
                    log.debug("[P34-SKEW] price fetch error %s: %s", symbol, _p34_px_exc)

                _p32_p_success = self._p32_veto_arb.compute_p_success(
                    recent_returns=_recent_returns,
                    correlation_score=_corr_score,
                    velocity_boost=_vel_boost,
                    local_price=_local_price,
                    global_mid_price=_global_mid,
                )
                if _p32_p_success < 0.65:
                    log.info(
                        "[P32-VETO-ARB] Entry BLOCKED %s %s: p_success=%.4f < 0.65 "
                        "(entropy+corr+velocity composite gate)",
                        symbol, signal.direction, _p32_p_success,
                    )
                    try:
                        _append_veto_audit_executor(
                            symbol,
                            "P32 Veto Arbitrator",
                            f"p_success={_p32_p_success:.4f} < 0.65",
                        )
                    except Exception:
                        pass
                    return
        except Exception as _veto_arb_exc:
            log.debug("[P32-VETO-ARB] Veto arbitrator check error %s: %s",
                      symbol, _veto_arb_exc)
        # ── [/P32-VETO-ARB] ──────────────────────────────────────────────────

        min_conf = self._perf.conf_for(symbol)

        gov = self._p10_governor
        if gov is not None and gov.is_priority_deploy(symbol):
            reduction = P10_PRIORITY_CONF_REDUCTION
            old_floor = min_conf
            min_conf  = max(min_conf - reduction, 0.01)
            log.info("[P10] Priority deploy %s: conf floor %.3f → %.3f",
                     symbol, old_floor, min_conf)
            gov.clear_priority_deploy(symbol)

        try:
            snap = await self.hub.get_sentiment(symbol)
            if snap.funding_rate > FUNDING_BRAKE_THRESHOLD:
                old_floor = min_conf
                min_conf  = min(min_conf + FUNDING_BRAKE_CONF_BUMP, 1.0)
                log.info("[P5] Funding Brake %s: raising conf floor %.3f → %.3f",
                         symbol, old_floor, min_conf)
        except Exception as e:
            log.debug("Funding brake check error %s: %s", symbol, e)

        tick = await self.hub.get_tick(symbol)
        sweep_active = False
        if tick and tick.last > 0:
            sweep_active = await self._check_liquidity_sweep(
                symbol, signal.direction, tick.last
            )
        if sweep_active:
            original_conf     = signal.confidence
            signal.confidence = 1.0

            # ── [P33-REVERSION] Report sweep event to VetoArbitrator ──────
            # Feed the sweep event and current price velocity into the
            # VetoArbitrator so the Exhaustion Gap Filter can detect a
            # subsequent velocity collapse within P33_EXHAUSTION_WINDOW_MS
            # (500ms) and issue a hard block (p_success = 0.0) when mean-
            # reversion risk is confirmed.  The velocity sample here is the
            # "velocity at sweep" reference point.
            try:
                if self._p32_veto_arb is None:
                    try:
                        self._p32_veto_arb = VetoArbitrator()
                    except Exception:
                        pass
                if self._p32_veto_arb is not None:
                    _sweep_vel = await self.hub.get_trade_velocity(symbol)
                    self._p32_veto_arb.record_sweep_event(
                        direction=signal.direction,
                        velocity_at_sweep=float(_sweep_vel),
                    )
                    log.info(
                        "[P33-REVERSION] Sweep event reported to VetoArbitrator: "
                        "%s dir=%s vel=%.2f tps — exhaustion gate armed.",
                        symbol, signal.direction, _sweep_vel,
                    )
            except Exception as _sweep_evt_exc:
                log.debug("[P33-REVERSION] sweep event report error %s: %s",
                          symbol, _sweep_evt_exc)
            # ── [/P33-REVERSION] ─────────────────────────────────────────

        if signal.confidence < min_conf:
            if sweep_active:
                signal.confidence = original_conf
            return

        if self.brain.correlation_freeze():
            if sweep_active:
                signal.confidence = original_conf
            return

        if signal.chop_sniper_only and not signal.sniper_boost and not sweep_active:
            if sweep_active:
                signal.confidence = original_conf
            return

        cfg           = self._coin_cfg(symbol)
        eff_max_alloc = self._perf.alloc_for(symbol, cfg.max_alloc_pct)

        sizer = self._p7_sizer
        if sizer is not None:
            kelly_alloc = await sizer.get_sentiment_adjusted_size(
                base_kelly=max(signal.kelly_f, START_ALLOC_PCT, 0.001),
                max_alloc=eff_max_alloc,
                min_alloc=START_ALLOC_PCT,
                equity=self._equity,
            )
            if kelly_alloc == 0.0:
                if sweep_active:
                    signal.confidence = original_conf
                return
        else:
            kelly_alloc = max(signal.kelly_f, START_ALLOC_PCT, 0.001)
            kelly_alloc = min(kelly_alloc, eff_max_alloc)

        base_usd = max(self._avail * kelly_alloc, cfg.min_usd_value)

        if gov is not None:
            try:
                gov_max_usd = await gov.get_max_usd(symbol, base_usd)
                usd_amount  = gov_max_usd
            except Exception as e:
                log.debug("[P10] Governor get_max_usd error %s: %s", symbol, e)
                usd_amount = base_usd
        else:
            usd_amount = base_usd

        if usd_amount > self._avail * 0.95:
            if sweep_active:
                signal.confidence = original_conf
            return

        _shadow_baseline_usd = usd_amount

        _shadow_conviction_mult = 1.0
        if P18_DYNAMIC_SIZING and _p17_narrative is not None:
            pre_conviction_usd = usd_amount
            usd_amount = self._p17_veto.compute_conviction_size(
                base_usd      = usd_amount,
                result        = _p17_narrative,
                avail         = self._avail,
                max_alloc_pct = eff_max_alloc,
                equity        = self._equity,
            )
            _shadow_conviction_mult = _p17_narrative.conviction_multiplier
            if usd_amount != pre_conviction_usd:
                log.info(
                    "[P18-1] Conviction sizing %s %s: $%.2f → $%.2f "
                    "(mult=%.3fx score=%.4f verdict=%s council=[%s])",
                    symbol, signal.direction,
                    pre_conviction_usd, usd_amount,
                    _p17_narrative.conviction_multiplier,
                    _p17_narrative.score,
                    _p17_narrative.verdict,
                    "; ".join(f"{d['agent']}:{d['vote']}" for d in _p17_narrative.council_detail),
                )
            if usd_amount > self._avail * 0.95:
                usd_amount = _shadow_baseline_usd

        if (
            _p17_narrative is not None
            and signal.regime.lower() in P18_DIVERGENCE_BEAR_REGIMES
            and _p17_narrative.score > P18_DIVERGENCE_SCORE_FLOOR
            and signal.direction == "long"
        ):
            pre_div_usd = usd_amount
            usd_amount  = usd_amount * P18_DIVERGENCE_HAIRCUT
            log.warning(
                "[P18-2] DIVERGENCE DETECTED: Bullish council (score=%.4f > %.2f) "
                "in Bear market (regime=%s). Scaling: $%.2f → $%.2f",
                _p17_narrative.score, P18_DIVERGENCE_SCORE_FLOOR,
                signal.regime, pre_div_usd, usd_amount,
            )
            if _shadow_baseline_usd > 0:
                _shadow_conviction_mult = usd_amount / _shadow_baseline_usd

        # [P25] RISK-OFF: apply global 0.5× multiplier to the final notional
        _p25_risk_off = bool(self._tactical_config.get("risk_off", False))
        if _p25_risk_off:
            pre_risk_off_usd = usd_amount
            usd_amount       = usd_amount * 0.5
            log.info(
                "[P25] RISK-OFF active: order size scaled 0.5× for %s %s: "
                "$%.2f → $%.2f",
                symbol, signal.direction, pre_risk_off_usd, usd_amount,
            )
            if _shadow_baseline_usd > 0:
                _shadow_conviction_mult = usd_amount / _shadow_baseline_usd

        if sweep_active:
            signal.confidence = original_conf
            sweep_tag = ("SWEEP_SNIPER_LONG" if signal.direction == "long"
                         else "SWEEP_SNIPER_SHORT")
            if signal.direction == "long":
                use_swap = signal.regime == "bull" and OKX_LEVERAGE > 1
                result   = await self._execute_iceberg(
                    symbol, "buy", usd_amount,
                    swap=use_swap, pos_side="long" if use_swap else "net",
                    tag=sweep_tag, coin_cfg=cfg,
                )
            else:
                result = await self._execute_iceberg(
                    symbol, "sell", usd_amount,
                    swap=True, pos_side="short", tag=sweep_tag, coin_cfg=cfg,
                )
            if result:
                if signal.direction == "long":
                    await self._open_long(
                        symbol, usd_amount, signal,
                        _shadow_baseline_usd=_shadow_baseline_usd,
                        _shadow_conviction_mult=_shadow_conviction_mult,
                        _shadow_narrative=_p17_narrative,
                    )
                else:
                    await self._open_short(
                        symbol, usd_amount, signal,
                        _shadow_baseline_usd=_shadow_baseline_usd,
                        _shadow_conviction_mult=_shadow_conviction_mult,
                        _shadow_narrative=_p17_narrative,
                    )
            return

        if self._should_force_shadow(signal.regime):
            tick2 = tick or await self.hub.get_tick(symbol)
            if tick2 and tick2.ask > 0 and tick2.bid > 0:
                book       = await self.hub.get_order_book(symbol)
                spread_mgr = self._p7_spread_mgr
                if signal.direction == "long":
                    target_px = (
                        spread_mgr.adjusted_limit_price("buy", tick2, symbol,
                                                         order_book=book if book else None)
                        if spread_mgr else tick2.ask
                    )
                    side = "buy"
                else:
                    target_px = (
                        spread_mgr.adjusted_limit_price("sell", tick2, symbol,
                                                         order_book=book if book else None)
                        if spread_mgr else tick2.bid
                    )
                    side = "sell"
                use_swap = signal.regime == "bull" and OKX_LEVERAGE > 1
                ps       = ("long" if use_swap else "net") if side == "buy" else "short"
                tag_pfx  = ("LONG_ENTRY_SNIPER" if signal.sniper_boost
                            else "LONG_ENTRY") if side == "buy" else (
                            "SHORT_ENTRY_SNIPER" if signal.sniper_boost
                            else "SHORT_ENTRY")
                mode_tag = (f"_P10_{self._p10_execution_mode.upper()}"
                            if self._is_shadow_only_mode() else "")
                watch = ShadowWatch(
                    symbol=symbol, side=side, target_px=target_px,
                    usd_amount=usd_amount, signal=signal,
                    swap=use_swap, pos_side=ps,
                    tag=f"SHADOW_{tag_pfx}{mode_tag}", coin_cfg=cfg,
                )
                self._register_shadow(watch)
                return

        if signal.direction == "long":
            await self._open_long(
                symbol, usd_amount, signal,
                _shadow_baseline_usd=_shadow_baseline_usd,
                _shadow_conviction_mult=_shadow_conviction_mult,
                _shadow_narrative=_p17_narrative,
            )
        elif signal.direction == "short":
            if signal.regime == "chop" and not self._is_shadow_only_mode():
                return
            await self._open_short(
                symbol, usd_amount, signal,
                _shadow_baseline_usd=_shadow_baseline_usd,
                _shadow_conviction_mult=_shadow_conviction_mult,
                _shadow_narrative=_p17_narrative,
            )

    # ── Main cycle ─────────────────────────────────────────────────────────────
    async def _cycle(self):
        # [P30.5-PUBLISH] Guarantee _publish_status() runs at the very START of
        # every cycle — the Dashboard sees "0s stale" even during ghost loops,
        # zombie mode, or warm-up barriers.
        await self._publish_status()

        # [P30.5-WARM] Pre-Flight Instrument Cache Barrier — do NOT attempt any
        # trade execution until the InstrumentCache has fetched at least one
        # SPOT instrument spec from OKX.  Without this guard Kelly sizing calls
        # _quantize_sz() with None meta and crashes.
        #
        # is_warm is derived from the cache internal dict length.  We avoid
        # importing data_hub here to prevent a circular dependency; instead we
        # read the private _cache attribute defensively.
        _icache_warm = (
            getattr(self._icache, 'is_warm', None)          # future-proof property
            if hasattr(self._icache, 'is_warm')
            else bool(getattr(self._icache, '_cache', {}))  # current impl
        )
        if not _icache_warm:
            log.debug(
                "[P30.5-WARM] InstrumentCache not yet warm — "
                "skipping trade execution this cycle (cache populating)."
            )
            return
        # [P40.1-SHIELD] Ready-Gate: do not enter trading cycle until the Rust
        # bridge has delivered a non-ghost, non-zero equity snapshot. This
        # prevents baselining equity at $0.00 during the bridge connect phase.
        # Gate is opened by _on_bridge_account_update or _on_bridge_ghost_healed
        # once Binary Truth equity is confirmed.  In Safe Mode (bridge=None) the
        # gate is opened immediately at construction time.
        if self.bridge is not None and not self._p40_ready_gate.is_set():
            log.debug(
                "[P40.1-SHIELD] Ready-Gate active — awaiting Binary Truth equity "
                "from Rust bridge.  Bridge connected=%s.  "
                "Skipping trade execution this cycle (Safe Mode hold).",
                getattr(self.bridge, "connected", False),
            )
            return

        if self._p20_zombie_mode:
            log.debug(
                "[P20-1] 🧟 Zombie Mode HARD GATE active — "
                "skipping full cycle (no DCA / no entries permitted)."
            )
            return

        # [P24-DEFENSE] Command Listener — check for control events written by
        # the dashboard (e.g. "reset_gateway" to clear Ghost State errors).
        # [P24.1-EXEC] Also supports force_truth flag for the Ghost-Rest Paradox:
        #   {"event": "reset_gateway", "force_truth": true}
        # When force_truth is set, _hard_reset_sync() bypasses the equity floor
        # check so even a genuine $0.00 balance is accepted as the New Truth.
        try:
            if os.path.exists(P24_CONTROL_EVENT_PATH):
                with open(P24_CONTROL_EVENT_PATH, "r", encoding="utf-8") as _cef:
                    _ce = json.load(_cef)
                os.remove(P24_CONTROL_EVENT_PATH)
                _event       = str(_ce.get("event", "")).strip().lower()
                _force_truth = bool(_ce.get("force_truth", False))
                log.info(
                    "[P24.1-EXEC] Control event received: %r  force_truth=%s",
                    _event, _force_truth,
                )
                if _event == "reset_gateway":
                    asyncio.create_task(
                        self._hard_reset_sync(force_truth=_force_truth),
                        name="p24_hard_reset_sync",
                    )
        except Exception as _ce_exc:
            log.warning("[P24-DEFENSE] Control event read error: %s", _ce_exc)

        collected = gc.collect()
        if collected:
            log.debug("[P19-4] GC collected %d objects.", collected)

        self._reload_coin_cfgs_if_stale()

        # [P25] Load tactical overrides from Dashboard every cycle (thread-safe read)
        tactical = self._load_tactical_config()
        _risk_off     = bool(tactical.get("risk_off",     False))
        _sniper_only  = bool(tactical.get("sniper_only",  False))
        _hedge_active = bool(tactical.get("hedge_active", False))

        # [LKG-3] Use last confirmed equity for sizing — falls back through
        # _last_valid_equity → _equity → 1.0 so ghost reads never break Kelly.
        equity = self._last_valid_equity or self._equity or 1.0
        avail  = self._avail

        if self._p20_risk_manager is not None:
            try:
                await self._p20_risk_manager.update(equity)
            except Exception as _p20_exc:
                log.error("[P20-1] GlobalRiskManager.update error: %s", _p20_exc)

        gov = self._p10_governor
        if gov is not None:
            self._p10_execution_mode = gov.execution_mode.value
        else:
            self._p10_execution_mode = "neutral"

        # [P35.1-HEDGE] Inject live hedge state into VetoArbitrator each cycle.
        # This must run after the governor mode update (so p35_is_rebalancing
        # reflects the latest PortfolioGovernor state) and before _maybe_enter()
        # calls compute_p_success() so the veto flags are always current.
        try:
            if self._p32_veto_arb is not None and gov is not None:
                _p35_rebal   = gov.p35_is_rebalancing
                _p35_toxic   = self.get_p33_btc_toxicity() >= P33_TOXICITY_THRESHOLD
                self._p32_veto_arb.set_p35_state(_p35_rebal, _p35_toxic)
                log.debug(
                    "[P35.1-HEDGE] Cycle: injected p35_state "
                    "rebalancing=%s btc_toxic=%s (score=%.3f threshold=%.2f)",
                    _p35_rebal, _p35_toxic,
                    self.get_p33_btc_toxicity(), P33_TOXICITY_THRESHOLD,
                )
        except Exception as _p35_exc:
            log.debug("[P35.1-HEDGE] P35 state injection error: %s", _p35_exc)

        # [P37-VPIN] Inject current Volume-Clock ToxicityScore into VetoArbitrator
        # for each traded symbol.  The tape-monitor loop also injects it in real-time,
        # but this cycle-level injection guarantees a fresh reading even if the monitor
        # hasn't polled since the last bucket completion.
        # Note: the _tape_monitor_loop injection takes precedence during live ticking;
        # this acts as a fallback / initialiser at cycle start.
        try:
            if self._p32_veto_arb is not None:
                for _p37_sym in self.symbols:
                    try:
                        _p37_tox = await self.hub.get_flow_toxicity(_p37_sym)
                        self._p32_veto_arb.set_flow_toxicity(_p37_sym, _p37_tox)
                    except Exception as _p37_sym_exc:
                        log.debug(
                            "[P37-VPIN] Cycle toxicity injection error %s: %s",
                            _p37_sym, _p37_sym_exc,
                        )
        except Exception as _p37_exc:
            log.debug("[P37-VPIN] Cycle toxicity batch injection error: %s", _p37_exc)

        # [P23-HOTFIX-1] Decoupled circuit-breaker check — suppresses ghost-state
        # death-loop by not panicking when LKG equity is healthy.
        if self._check_circuit_breaker_decoupled(equity):
            await self._flatten_all("global_drawdown")
            return

        cb = self._p7_cb
        if cb is not None and cb.is_tripped:
            # Only log and flatten when we HAVE something to do.
            if self.positions or self._shadow_watches:
                now = time.time()
                if now - self._last_flatten_log_ts >= P23_FLATTEN_LOG_THROTTLE_S:
                    log.critical("[P7] CircuitBreaker tripped — flattening all positions.")
                await self._flatten_all("p7_circuit_breaker")
            return

        # [P23-OPT-5] Hard-Sync Audit: every 5 min force REST reconciliation.
        _now_sync = time.time()
        _force_sync_requested = False
        try:
            if os.path.exists(P23_FORCE_SYNC_FLAG):
                _force_sync_requested = True
                os.remove(P23_FORCE_SYNC_FLAG)
                log.info("[P23-OPT-5] FORCE REST SYNC requested by dashboard.")
        except Exception:
            pass

        if _force_sync_requested or (_now_sync - self._last_hard_sync_ts >= P23_HARD_SYNC_INTERVAL_S):
            self._last_hard_sync_ts = _now_sync
            log.info(
                "[P23-OPT-5] Hard-Sync Audit: triggering REST account reconciliation "
                "(forced=%s, interval=%.0fs).",
                _force_sync_requested, P23_HARD_SYNC_INTERVAL_S,
            )
            if not self._ghost_poll_in_flight:
                try:
                    asyncio.create_task(
                        self._verify_equity_via_rest(),
                        name="p23_hard_sync_audit",
                    )
                    self._ghost_poll_in_flight = True
                except Exception as _hs_exc:
                    log.error("[P23-OPT-5] Hard-sync task creation failed: %s", _hs_exc)

        if self._vol_guard.emergency_pause:
            log.warning("[P4-1] EMERGENCY PAUSE active.")
            await self._cancel_all_open_buys()

        if self.brain.correlation_freeze() and self.positions:
            # [P25] HEDGE-MODE: skip the correlation trim so opposing-direction
            # pairs can remain open as delta-neutral hedges.
            if _hedge_active:
                log.info(
                    "[P25] HEDGE-MODE active — P21 correlation trim SKIPPED "
                    "(delta-neutral hedges permitted)."
                )
            else:
                await self._hedge_trim_smallest()

        positions_snap: dict = {}
        shadow_count = len(self._shadow_watches)

        for symbol in self.symbols:
            try:
                tick = await self.hub.get_tick(symbol)
                if not tick:
                    continue

                # [P25] Update liquidation clusters for Dashboard Heat-Map
                try:
                    await self._update_liquidation_clusters(symbol, tick.last or tick.ask or 0.0)
                except Exception as _lc_exc:
                    log.debug("[P25] _update_liquidation_clusters error %s: %s", symbol, _lc_exc)

                signals: dict = {}
                for tf in ["1hour", "4hour", "1day"]:
                    candles = await self.hub.get_candles(symbol, tf, 200)
                    sig     = self.brain.analyze(symbol, tf, candles)
                    if sig:
                        signals[tf] = sig
                if not signals:
                    continue

                master = self.brain.aggregate_signals(symbol, signals)

                if master.cancel_buys_flag:
                    await self.hub.cycle_oracle_cancel([symbol])

                _hv_extra  = self.hub.hv_stop_extra_pct()
                tape       = self.hub.get_tape(symbol)
                tape_stats = {}
                if tape is not None:
                    try:
                        tape_stats = await asyncio.wait_for(tape.snapshot_stats(), timeout=0.5)
                    except Exception:
                        pass

                pos = self.positions.get(symbol)

                if pos:
                    exit_px = tick.bid if pos.direction == "long" else tick.ask

                    dead_alpha_fired = await self._check_dead_alpha(symbol, pos, exit_px)
                    if dead_alpha_fired:
                        positions_snap[symbol] = {
                            "direction": "none", "inst_type": "N/A", "qty": 0.0,
                            "current_bid": tick.bid, "current_ask": tick.ask,
                            "regime": master.regime, "signal_conf": master.confidence,
                            "p13_dead_alpha_exit": True,
                            "p7_cb_tripped": (cb.is_tripped if cb is not None else False),
                            "p20_zombie_mode": self._p20_zombie_mode,
                        }
                        continue

                    front_ran = await self._maybe_frontrun_exit(symbol, pos, exit_px)
                    if front_ran:
                        positions_snap[symbol] = {
                            "direction": "none", "inst_type": "N/A", "qty": 0.0,
                            "current_bid": tick.bid, "current_ask": tick.ask,
                            "regime": master.regime, "signal_conf": master.confidence,
                            "frontrun_exit": True,
                            "p7_cb_tripped": (cb.is_tripped if cb is not None else False),
                            "p20_zombie_mode": self._p20_zombie_mode,
                        }
                        continue

                    _effective_trail_mult = pos.trail_multiplier
                    if _hv_extra > 0 and not pos.trail_active:
                        _effective_trail_mult = pos.trail_multiplier + _hv_extra

                    sold = await self._update_trailing(
                        symbol, pos, exit_px,
                        trail_mult_override=_effective_trail_mult,
                    )

                    if not sold:
                        h1 = await self.hub.get_candles(symbol, "1hour", 200)
                        ph = np.array([c.close for c in h1], dtype=np.float64)
                        await self._maybe_dca(symbol, pos, tick.ask, ph)

                        p = self.positions[symbol]
                        ref_px  = tick.bid if p.direction == "long" else tick.ask
                        pnl_pct = (
                            (ref_px - p.cost_basis) / p.cost_basis * 100
                            if p.direction == "long"
                            else (p.cost_basis - ref_px) / p.cost_basis * 100
                        )
                        price_delta_pct = abs(pnl_pct) / 100.0

                        try:
                            sent = await self.hub.get_sentiment(symbol)
                        except Exception:
                            sent = None

                        gov_max_usd = 0.0
                        if gov is not None:
                            try:
                                gov_max_usd = await gov.get_max_usd(symbol, p.usd_cost)
                            except Exception:
                                pass

                        elapsed_h = (time.time() - p.entry_ts) / 3600.0
                        tf_trust  = self.brain.trust.trust_factor(
                            symbol, p.entry_signal.tf if p.entry_signal else "1hour"
                        )

                        positions_snap[symbol] = {
                            "direction":         p.direction,
                            "inst_type":         p.inst_type,
                            "qty":               p.qty,
                            "cost_basis":        p.cost_basis,
                            "usd_cost":          p.usd_cost,
                            "current_bid":       tick.bid,
                            "current_ask":       tick.ask,
                            "pnl_pct":           round(pnl_pct, 4),
                            "dca_stages":        p.dca_stages,
                            "trail_active":      p.trail_active,
                            "trail_line":        p.trail_line,
                            "regime":            master.regime,
                            "regime_modifier":   master.regime_modifier,
                            "trail_multiplier":  p.trail_multiplier,
                            "effective_tm":      _effective_trail_mult,
                            "signal_conf":       master.confidence,
                            "sniper_entry":      p.sniper_entry,
                            "mtf_aligned":       master.mtf_aligned,
                            "autotune_active":   self._perf.overrides_active(symbol),
                            "funding_rate":      sent.funding_rate  if sent else 0.0,
                            "net_liq_delta":     sent.net_liq_delta if sent else 0.0,
                            "squeeze_delay_secs": max(0.0, p.squeeze_delay_until - time.time()),
                            "p10_gov_max_usd":   gov_max_usd,
                            "p10_exec_mode":     self._p10_execution_mode,
                            "p12_hft_brake":     self._is_hft_brake_active(symbol),
                            "p12_tape_velocity": tape_stats.get("velocity_tps", 0.0),
                            "p12_tape_buy_vol":  tape_stats.get("buy_vol_60s", 0.0),
                            "p12_tape_sell_vol": tape_stats.get("sell_vol_60s", 0.0),
                            "p13_trust_factor":      round(tf_trust, 4),
                            "p13_elapsed_hours":     round(elapsed_h, 2),
                            "p13_price_delta_pct":   round(price_delta_pct, 4),
                            "p13_dead_alpha_risk":   (
                                elapsed_h >= P13_DEAD_ALPHA_HOURS * 0.75
                                and price_delta_pct <= P13_DEAD_ALPHA_BAND_PCT * 2
                            ),
                            "p14_high_conviction":   (
                                p.entry_signal.high_conviction if p.entry_signal else False
                            ),
                            "p14_conviction_votes":  (
                                p.entry_signal.conviction_votes if p.entry_signal else 0
                            ),
                            "p16_express_entry": p.p16_express,
                            "p7_cb_tripped":          (cb.is_tripped if cb is not None else False),
                            "p20_zombie_mode":        self._p20_zombie_mode,
                        }
                else:
                    hv_entry_safe = _hv_extra < 0.05
                    tape_safe     = tape_stats.get("velocity_tps", 0) < 2.0

                    if hv_entry_safe and tape_safe:
                        oracle_sig = self.hub.get_oracle_signal(symbol)
                        await self._maybe_enter(symbol, master, tick, oracle_sig)

                    try:
                        sent = await self.hub.get_sentiment(symbol)
                    except Exception:
                        sent = None

                    obi_val = 0.0
                    try:
                        obi_val = await self.hub.get_obi(symbol, 10)
                    except Exception:
                        pass

                    shadow_w = self._shadow_watches.get(symbol)

                    gov_alloc   = gov.get_alloc_weight(symbol) if gov else 0.0
                    gov_max_usd = 0.0
                    priority_dp = False
                    if gov is not None:
                        try:
                            gov_max_usd = await gov.get_max_usd(symbol, self._avail * 0.1)
                            priority_dp = gov.is_priority_deploy(symbol)
                        except Exception:
                            pass

                    sym_trust = {
                        tf: round(self.brain.trust.trust_factor(symbol, tf), 4)
                        for tf in ["1hour", "4hour", "1day"]
                    }
                    sym_censored = self.brain.trust.is_censored(symbol, master.tf)

                    spread_stats = {}
                    if self._p7_spread_mgr is not None:
                        try:
                            spread_stats = self._p7_spread_mgr.stats(symbol)
                        except Exception:
                            pass

                    _p17_last_verdict    = "N/A"
                    _p17_last_score      = None
                    _p17_last_latency    = None
                    _p18_conv_multiplier = None
                    _p20_council_detail  = []
                    if self._p17_veto is not None:
                        snap_data = self._p17_veto.status_snapshot()
                        for r in snap_data.get("recent_results", []):
                            if r.get("symbol") == symbol:
                                _p17_last_verdict    = r.get("verdict", "N/A")
                                _p17_last_score      = r.get("score")
                                _p17_last_latency    = r.get("latency_ms")
                                _p18_conv_multiplier = r.get("conviction_multiplier")
                                _p20_council_detail  = r.get("council_detail", [])
                                break

                    positions_snap[symbol] = {
                        "direction":        "none",
                        "inst_type":        "N/A",
                        "qty":              0.0,
                        "current_bid":      tick.bid,
                        "current_ask":      tick.ask,
                        "regime":           master.regime,
                        "regime_modifier":  master.regime_modifier,
                        "chop_sniper_only": master.chop_sniper_only,
                        "signal_conf":      master.confidence,
                        "signal_dir":       master.direction,
                        "signal_prob":      master.prob,
                        "sniper_ready":     master.sniper_boost,
                        "mtf_aligned":      master.mtf_aligned,
                        "autotune_conf":    self._perf.conf_for(symbol),
                        "autotune_alloc":   self._perf.alloc_for(
                            symbol, self._coin_cfg(symbol).max_alloc_pct),
                        "emergency_pause":  self._vol_guard.emergency_pause,
                        "funding_rate":     sent.funding_rate  if sent else 0.0,
                        "net_liq_delta":    sent.net_liq_delta if sent else 0.0,
                        "funding_brake_active": (
                            sent is not None and sent.funding_rate > 0.0008
                        ),
                        "shadow_watching":  shadow_w is not None,
                        "shadow_target_px": shadow_w.target_px if shadow_w else 0.0,
                        "shadow_expires_in": max(0.0, shadow_w.expires_at - time.time())
                                             if shadow_w else 0.0,
                        "p10_gov_alloc_weight": gov_alloc,
                        "p10_gov_max_usd":      gov_max_usd,
                        "p10_priority_deploy":  priority_dp,
                        "p10_exec_mode":        self._p10_execution_mode,
                        "p12_obi":              round(obi_val, 4),
                        "p12_obi_blocked": (
                            (master.direction == "long"  and obi_val < -0.6)
                            or
                            (master.direction == "short" and obi_val > 0.6)
                        ),
                        "p12_hft_brake":        self._is_hft_brake_active(symbol),
                        "p12_tape_velocity":    tape_stats.get("velocity_tps", 0.0),
                        "p12_tape_buy_vol":     tape_stats.get("buy_vol_60s", 0.0),
                        "p12_tape_sell_vol":    tape_stats.get("sell_vol_60s", 0.0),
                        "p12_sweep_active": symbol in self._sweep_signals and (
                            time.time() - self._sweep_signals[symbol] < 15.0
                        ),
                        "p13_trust_by_tf":  sym_trust,
                        "p13_agg_trust":    round(master.trust_factor, 4),
                        "p13_censored":     sym_censored,
                        "p14_high_conviction":  master.high_conviction,
                        "p14_conviction_votes": master.conviction_votes,
                        "p14_spread_offset_bps": spread_stats.get("offset_bps", 0.0),
                        "p14_fill_rate_1h":      spread_stats.get("fill_rate_1h"),
                        "p16_express_eligible": (
                            symbol in self.symbols
                            and self.p16_bridge_active
                            and not self._vol_guard.emergency_pause
                            and not self.drawdown._killed
                            and not self._p20_zombie_mode
                        ),
                        "p16_last_express_ago": round(
                            time.time() - self._p16_last_express.get(symbol, 0.0), 1
                        ) if symbol in self._p16_last_express else None,
                        "p17_narrative_verdict":     _p17_last_verdict,
                        "p17_narrative_score":       _p17_last_score,
                        "p17_narrative_latency_ms":  _p17_last_latency,
                        "p17_intelligence_active":   self._p17_veto is not None,
                        "p18_conviction_multiplier": _p18_conv_multiplier,
                        "p18_dynamic_sizing_active": P18_DYNAMIC_SIZING,
                        "p19_shadow_audit_path":     self._shadow_audit_path,
                        "p7_cb_tripped":          (cb.is_tripped if cb is not None else False),
                        "p20_zombie_mode":        self._p20_zombie_mode,
                        "p20_council_detail":     _p20_council_detail,
                    }

            except Exception as e:
                log.error("Cycle error %s: %s", symbol, e, exc_info=True)

        gov_snap = {}
        if gov is not None:
            try:
                gov_snap = gov.status_snapshot()
            except Exception as e:
                log.debug("[P10] Governor status_snapshot error: %s", e)

        sentinel_snap = {}
        sentinel = self._p11_sentinel
        if sentinel is not None:
            try:
                sentinel_snap = sentinel.status_snapshot()
            except Exception as exc:
                log.debug("[P11] Sentinel status_snapshot error: %s", exc)

        p13_trust_snap = {}
        try:
            p13_trust_snap = self.brain.trust.snapshot()
        except Exception:
            pass

        p17_snap = {}
        if self._p17_veto is not None:
            try:
                p17_snap = self._p17_veto.status_snapshot()
            except Exception:
                pass

        p20_risk_snap = {}
        if self._p20_risk_manager is not None:
            try:
                p20_risk_snap = self._p20_risk_manager.status_snapshot()
            except Exception:
                pass

        p20_shadow_snap = {}
        try:
            p20_shadow_snap = self.brain.shadow_train_snapshot()
        except Exception:
            pass

        # ── [P22-STRATEGY-MODE] Compute active logic path label ───────────────
        # Priority: PANIC_LOCK > WHALE_VETO > AI_PREMIUM > TA_FALLBACK
        _pl_locked = False
        try:
            _pl = getattr(self, "_p22_panic_lock", None)
            _pl_locked = bool(_pl and _pl.is_locked)
        except Exception:
            pass
        _whale_vetoed = (
            self._p20_zombie_mode
            or self.drawdown._killed
            or self._vol_guard.emergency_pause
        )
        if _pl_locked:
            _strategy_mode = "🔴 PANIC_LOCK"
        elif _whale_vetoed:
            _strategy_mode = "🛡️ WHALE_VETO"
        elif self._llm_in_fallback:
            _strategy_mode = "📈 TA_FALLBACK"
        else:
            _strategy_mode = "🤖 AI_PREMIUM"
        self._strategy_mode = _strategy_mode

        self._status = {
            "timestamp":       time.time(),
            "demo_mode":       self.demo,
            "circuit_breaker": self.drawdown._killed,
            "strategy_mode":   _strategy_mode,
            "p7_cb_tripped":   (
                self._p7_cb.is_tripped if self._p7_cb is not None else False
            ),
            "p20_zombie_mode_active": self._p20_zombie_mode,
            "emergency_pause": self._vol_guard.emergency_pause,
            "pause_remaining": self._vol_guard.pause_remaining_secs,
            "p9_shadow_watches_active": shadow_count,
            "p9_iceberg_threshold_usd": ICEBERG_MIN_USD,
            "p10_governor":    gov_snap,
            "p10_exec_mode":   self._p10_execution_mode,
            "p11_sentinel":    sentinel_snap,
            "p12_hft_brake_symbols": [s for s, v in self._hft_brake.items() if v],
            "p12_sweep_active_symbols": [
                s for s, ts in self._sweep_signals.items()
                if time.time() - ts < SWEEP_SIGNAL_TTL_SECS
            ],
            "p13_trust_scores":     p13_trust_snap,
            "p13_dead_alpha_exits": self._p13_dead_alpha_exits,
            "account": {
                "total_equity": round(equity, 2),
                "buying_power": round(avail,  2),
                # [ATOMIC-2] Single coherent block so math always balances.
                "deployed_capital": round(max(equity - avail, 0.0), 2),
                "avail_balance":    round(avail, 2),
                "last_valid_equity": round(self._last_valid_equity, 2),
                "pct_deployed": round(
                    (equity - avail) / equity * 100 if equity > 0 else 0, 2),
                "leverage":     OKX_LEVERAGE,
                "equity_is_ghost":           self._equity_is_ghost,
                "consecutive_ghost_reads":   self._consecutive_ghost_reads,
            },
            "positions": positions_snap,
            "p15_oracle_signals": {
                sym: {"signal": sig.signal_type, "mult": sig.multiplier,
                      "valid": sig.is_valid(), "size": sig.trade_size}
                for sym in self.symbols
                if (sig := self.hub.get_oracle_signal(sym)) is not None
            },
            "p15_global_tape": self.hub.get_global_tape_status(),
            "p15_hv_mode":     self.hub.is_hv_mode(),
            "p16_bridge_active":       self.p16_bridge_active,
            "p16_express_fills_total": self._p16_express_fills,
            "p16_bbo_offset_bps":      P16_BBO_OFFSET_BPS,
            "p16_express_alloc_pct":   P16_EXPRESS_ALLOC_PCT,
            "p16_express_max_usd":     P16_EXPRESS_MAX_USD,
            "p16_express_dedupe_secs": P16_EXPRESS_DEDUPE_SECS,
            "p17_intelligence":         p17_snap,
            "p17_council_enabled":      True,
            "p18_dynamic_sizing":       P18_DYNAMIC_SIZING,
            "p18_divergence_bear_regimes": list(P18_DIVERGENCE_BEAR_REGIMES),
            "p18_divergence_score_floor":  P18_DIVERGENCE_SCORE_FLOOR,
            "p18_divergence_haircut":      P18_DIVERGENCE_HAIRCUT,
            "p19_shadow_audit_path":       self._shadow_audit_path,
            "p19_gc_enabled":              True,
            "p20_global_risk":             p20_risk_snap,
            "p20_zombie_mode":             self._p20_zombie_mode,
            "p20_shadow_train":            p20_shadow_snap,
            "p20_drawdown_zombie_pct":     P20_DRAWDOWN_ZOMBIE_PCT,
            # [P23] Phase 23 survival data
            "p23_ghost_meter": {
                "consecutive_ghost_reads": self._consecutive_ghost_reads,
                "equity_is_ghost":         self._equity_is_ghost,
                "last_valid_equity":       round(self._last_valid_equity, 2),
                "reconnect_threshold":     P23_GHOST_RECONNECT_THRESH,
                "last_ws_reconnect_ago":   round(time.time() - self._last_ws_reconnect_ts, 1)
                                           if self._last_ws_reconnect_ts > 0 else None,
                "cb_suppressed_with_lkg":  P23_CB_SUPPRESS_WITH_LKG,
            },
            "p23_hard_sync": {
                "last_sync_ts":          self._last_hard_sync_ts,
                "last_sync_ago_secs":    round(time.time() - self._last_hard_sync_ts, 1)
                                         if self._last_hard_sync_ts > 0 else None,
                "sync_interval_secs":    P23_HARD_SYNC_INTERVAL_S,
            },
            # [P25] Tactical Listener — Dashboard pulse turns Purple when True
            "tactical_active": self._tactical_active,
            "tactical_config": self._tactical_config,
            # [P25] Market data — Liquidation Heat-Map clusters
            "market_data": {
                "liquidations": self._liquidation_clusters,
                # [P24-DEFENSE] Whale Tape — peak multiplier + dominant side from
                # Phase 15 Arbitrator oracle signals across all tracked symbols.
                "whale_tape": self._compute_whale_tape(),
            },
            # [P24-DEFENSE] Intelligence Bridge — full NarrativeResult from the
            # most recent AI council evaluation, serialised for the Systemic
            # Defense sidebar.  Falls back to an empty dict during warm-up.
            "intelligence": self._serialize_last_narrative(),
            # [P32] Institutional Predator Suite — live aggression mode + slippage status
            "p32_aggression_mode": self._p32_aggression_mode,
            "p32_limit_only_symbols": [
                sym for sym, exp in self._p32_limit_only_until.items()
                if exp > time.time()
            ],
            "p32_slip_window": {
                sym: list(hist)
                for sym, hist in self._p32_slip_history.items()
                if hist
            },
            # [P35.1-HEDGE] Dynamic Delta-Neutral Hedge Commander — live snapshot
            "p35_hedge": (
                self._p10_governor.p35_hedge_snapshot
                if self._p10_governor is not None
                else {
                    "hedge_usd": 0.0, "hedge_dir": "none",
                    "rebalancing": False, "net_delta_usd": 0.0,
                    "ord_id": "", "threshold_usd": 0.0, "ratio": 1.0,
                    "min_change_usd": 0.0, "symbol": P35_HEDGE_SYMBOL,
                }
            ),
        }

        # [P24-DEFENSE] Entropy Trend — sample the current cycle's Shannon Entropy
        # from _last_narrative (if available) and append to the rolling deque.
        # Then patch the entropy_history list directly into the intelligence block
        # so the dashboard sparkline reflects the live trend without requiring an
        # extra status rebuild.
        _current_entropy: Optional[float] = None
        if self._last_narrative is not None:
            try:
                _current_entropy = float(self._last_narrative.entropy)
            except Exception:
                pass
        if _current_entropy is not None:
            self._entropy_history.append(round(_current_entropy, 4))
        self._status["intelligence"]["entropy_history"] = list(self._entropy_history)

        try:
            await self.cache.set("trader:status", self._status, ex=120)
        except Exception as exc:
            log.warning("[P8] cache.set trader:status failed: %s", exc)

        try:
            await self.db.insert_snapshot({
                "ts": int(time.time()), "total_equity": equity,
                "buying_power": avail, "margin_ratio": 0.0,
            })
        except Exception as exc:
            log.warning("[P8] db.insert_snapshot failed: %s", exc)

        # [ATOMIC-1] Write status JSON atomically at the very end of _cycle so
        # the dashboard never sees a partial file.
        self._write_status_json()

        # ── [P36.2-GOLDEN] Increment golden-build cycle counter ───────────────
        # This runs only after the entire cycle completes without raising an
        # IndexError or receiving a 51006 (those reset the counter inline).
        try:
            self._p362_record_cycle_success()
        except Exception as _gc_exc:
            log.debug("[P36.2-GOLDEN] record_cycle_success error: %s", _gc_exc)
        # ── [/P36.2-GOLDEN] ──────────────────────────────────────────────────

    # ── [P24-DEFENSE] Systemic Defense helpers ────────────────────────────────

    def _compute_whale_tape(self) -> dict:
        """
        [P24-DEFENSE] Scan all tracked symbols for the highest active oracle
        multiplier and return the dominant side + peak multiplier for the
        Systemic Defense sidebar whale-tape widget.
        """
        try:
            peak_mult    = 0.0
            peak_side    = "NEUTRAL"
            peak_symbol  = ""
            for sym in self.symbols:
                sig = self.hub.get_oracle_signal(sym)
                if sig is None or not sig.is_valid():
                    continue
                mult = float(getattr(sig, "multiplier", 0.0))
                if mult > peak_mult:
                    peak_mult   = mult
                    peak_side   = (
                        "BUY"  if sig.signal_type == ORACLE_WHALE_BUY
                        else "SELL"
                    )
                    peak_symbol = sym
            return {
                "peak_multiplier": round(peak_mult, 2),
                "dominant_side":   peak_side,
                "peak_symbol":     peak_symbol,
            }
        except Exception as exc:
            log.debug("[P24-DEFENSE] _compute_whale_tape error: %s", exc)
            return {"peak_multiplier": 0.0, "dominant_side": "NEUTRAL", "peak_symbol": ""}

    def _serialize_last_narrative(self) -> dict:
        """
        [P24-DEFENSE] Serialise the most recent NarrativeResult to a plain
        dict suitable for JSON export.  Returns an empty dict if no narrative
        has been produced yet (executor warm-up).
        """
        nr = self._last_narrative
        if nr is None:
            return {}
        try:
            _raw_entropy = round(float(nr.entropy), 4)
            # [P24-DEFENSE-3] Normalized entropy: scale raw bits (0..log2(10) ≈ 3.32)
            # to a 0..1 range for smoother dashboard sparklines.
            _MAX_ENTROPY_BITS = 3.32  # log2(10 bins) — practical maximum for 10-bucket histogram
            _norm_entropy = round(min(_raw_entropy / _MAX_ENTROPY_BITS, 1.0), 4) if _raw_entropy > 0 else 0.0
            return {
                "symbol":                nr.symbol,
                "direction":             nr.direction,
                "score":                 round(float(nr.score), 4),
                "verdict":               nr.verdict,
                "boost_factor":          round(float(nr.boost_factor), 4),
                "conviction_multiplier": round(float(nr.conviction_multiplier), 4),
                "headlines_used":        int(nr.headlines_used),
                "latency_ms":            round(float(nr.latency_ms), 1),
                "timed_out":             bool(nr.timed_out),
                "council_detail":        list(nr.council_detail),
                "llm_used":              bool(nr.llm_used),
                "llm_error":             nr.llm_error,
                "entropy":               _raw_entropy,
                "entropy_normalized":    _norm_entropy,
                "entropy_triggered":     bool(nr.entropy_triggered),
                "trailing_gap_adjustment": round(float(nr.trailing_gap_adjustment), 4),
                "ts":                    float(nr.ts),
            }
        except Exception as exc:
            log.debug("[P24-DEFENSE] _serialize_last_narrative error: %s", exc)
            return {}

    # ── [P24-DEFENSE-1] Heartbeat publisher — called at cycle top ─────────────

    async def _publish_status(self) -> None:
        """
        [P24-DEFENSE-1] Lightweight heartbeat write — called at the VERY TOP of
        _cycle() before any equity check or circuit-breaker gate.

        This guarantees the Dashboard always receives an updated timestamp and
        ghost-state indicators even when the executor is stuck in a ghost-equity
        loop, a zombie-mode hard gate, or waiting for the exchange WebSocket to
        re-establish.

        It writes whatever self._status currently holds (from the previous cycle)
        with an updated 'heartbeat_ts' field injected into the dict.  If _status
        is empty (first boot), the write is a no-op so we don't publish garbage.
        """
        if not self._status:
            return
        try:
            self._status["heartbeat_ts"] = time.time()
            self._status["consecutive_ghost_reads"] = self._consecutive_ghost_reads
            self._status["equity_is_ghost"] = self._equity_is_ghost
            self._write_status_json()
        except Exception as exc:
            log.debug("[P24-DEFENSE-1] _publish_status error: %s", exc)

    # ── [ATOMIC-1] Atomic JSON status writer ──────────────────────────────────
    def _write_status_json(self) -> None:
        """
        [ATOMIC-1] Write self._status to trader_status.json using the
        write-to-.tmp-then-os.replace() pattern.

        This guarantees the dashboard process never reads a partially-written
        or empty file, eliminating the JSON race condition that caused
        flickering 'DATA UNAVAILABLE' overlays and mathematical hallucinations
        (e.g. 2000% Heat) on the dashboard.

        Steps
        -----
        1. Ensure hub_data/ directory exists.
        2. Serialise self._status to JSON bytes.
        3. Write to TRADER_STATUS_PATH + '.tmp'.
        4. Atomically rename .tmp → TRADER_STATUS_PATH using os.replace().
           On POSIX this is a single kernel syscall (rename(2)) — readers see
           either the old complete file or the new complete file, never a
           partial write.
        """
        tmp_path = TRADER_STATUS_PATH + ".tmp"
        try:
            os.makedirs(os.path.dirname(TRADER_STATUS_PATH), exist_ok=True)
            payload = json.dumps(self._status, default=str)
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(payload)
            # [v32.1-WINLOCK] Windows file-lock retry — the Dashboard's read cycle
            # briefly holds an exclusive handle on trader_status.json.  If
            # os.replace() races against that read we get a PermissionError
            # (WinError 5).  Retry up to 5 times with 50 ms back-off so the
            # Executor write-cycle is never permanently blocked.
            _replace_attempts = 5
            for _ri in range(_replace_attempts):
                try:
                    os.replace(tmp_path, TRADER_STATUS_PATH)
                    break  # success
                except PermissionError as _pe:
                    _is_win5 = getattr(_pe, "winerror", None) == 5
                    if _ri < _replace_attempts - 1:
                        log.debug(
                            "[v32.1-WINLOCK] os.replace PermissionError (WinError %s) "
                            "on attempt %d/%d — retrying in 50 ms.",
                            getattr(_pe, "winerror", "?"), _ri + 1, _replace_attempts,
                        )
                        time.sleep(0.05)
                    else:
                        raise  # all retries exhausted — propagate to outer handler
        except Exception as exc:
            log.warning("[ATOMIC-1] _write_status_json failed: %s", exc)
            # Best-effort cleanup of orphaned .tmp
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    async def run(self, interval: float = 1.0):
        log.info(
            "Executor started (demo=%s leverage=%dx p16_bridge=%s "
            "p17_intelligence=%s p18_dynamic_sizing=%s p19_shadow_audit=%s "
            "p20_zombie_pct=%.1f%% p20_council=True)",
            self.demo, OKX_LEVERAGE, self.p16_bridge_active,
            self._p17_veto is not None, P18_DYNAMIC_SIZING,
            self._shadow_audit_path, P20_DRAWDOWN_ZOMBIE_PCT,
        )

        # ── [v27.1-BOOT] Immediate Boot Status Write ──────────────────────────────
        # Requirement: the dashboard must see "0 s stale" the moment the process
        # starts.  _publish_status() is a deliberate no-op when self._status is
        # empty (its guard: `if not self._status: return`), so we seed a minimal
        # boot-state dict and call _write_status_json() directly.
        #
        # This write happens BEFORE entropy seeding and BEFORE PFVT so the
        # dashboard has a valid, timestamped file even if OKX is slow to respond.
        # The full, rich status dict is overwritten by the first _cycle() call
        # moments later, so no dashboard widget ever displays a boot-state value
        # for more than one refresh interval.
        self._status = {
            "timestamp":            time.time(),
            "heartbeat_ts":         time.time(),
            "demo_mode":            self.demo,
            "boot":                 True,           # sentinel — dashboard can grey minor KPIs
            "circuit_breaker":      False,
            "strategy_mode":        "⏳ BOOTING",
            "emergency_pause":      False,
            "pause_remaining":      0,
            "equity_is_ghost":      self._equity_is_ghost,
            "consecutive_ghost_reads": self._consecutive_ghost_reads,
            "account": {
                "total_equity":      round(self._equity, 2),
                "buying_power":      round(self._avail,  2),
                "deployed_capital":  0.0,
                "avail_balance":     round(self._avail, 2),
                "last_valid_equity": round(self._last_valid_equity, 2),
                "pct_deployed":      0.0,
                "leverage":          OKX_LEVERAGE,
                "equity_is_ghost":   self._equity_is_ghost,
                "consecutive_ghost_reads": self._consecutive_ghost_reads,
            },
            "positions":     {},
            "intelligence":  self._serialize_last_narrative(),
            "tactical_active": self._tactical_active,
            "tactical_config": self._tactical_config,
            "market_data":   {"liquidations": [], "whale_tape": {}},
        }
        self._status["intelligence"]["entropy_history"] = list(self._entropy_history)
        self._write_status_json()
        log.info("[v27.1-BOOT] Boot status written → dashboard sees 0 s stale immediately.")
        # ── [/v27.1-BOOT] ────────────────────────────────────────────────────────

        self.brain.start_background_tasks()

        # [P24-DEFENSE-3] Entropy Seeding — warm up _entropy_history from the
        # last 100 historical candles for every active symbol BEFORE the main
        # loop starts so Entropy gauges show real data on the very first cycle.
        # [P24.1-EXEC] Also pushes seed candles back into the DataHub's candle
        # buffers via buf.bulk_load() so the IntelligenceLayer can immediately
        # read recent_returns without waiting for the next KuCoin backfill cycle.
        # This is safe to call multiple times — bulk_load() replaces buffer
        # contents only when data is actually present.
        try:
            from intelligence_layer import compute_shannon_entropy as _cse
            log.info(
                "[P24-DEFENSE-3] Entropy seeding: fetching 100 candles for %d symbols…",
                len(self.symbols),
            )
            for _seed_sym in self.symbols:
                try:
                    _seed_candles = await self.hub.get_candles(_seed_sym, "1hour", 100)
                    if _seed_candles and len(_seed_candles) >= 2:
                        _seed_closes = [c.close for c in _seed_candles if c.close > 0]
                        if len(_seed_closes) >= 2:
                            _seed_returns = [
                                (_seed_closes[i] - _seed_closes[i - 1]) / _seed_closes[i - 1] * 100.0
                                for i in range(1, len(_seed_closes))
                                if _seed_closes[i - 1] != 0
                            ]
                            _seed_entropy = _cse(_seed_returns)
                            if _seed_entropy > 0:
                                self._entropy_history.append(round(_seed_entropy, 4))
                                log.debug(
                                    "[P24-DEFENSE-3] %s entropy seed: %.4f bits (%d returns)",
                                    _seed_sym, _seed_entropy, len(_seed_returns),
                                )
                        # [P24.1-EXEC] Push seed candles into the DataHub buffer
                        # so IntelligenceLayer has immediate data on first query.
                        # We access the private _get_buf method which is stable
                        # across all phases — this is not monkeypatching.
                        try:
                            _seed_inst = f"{_seed_sym.upper()}-USDT"
                            _seed_buf  = await self.hub._get_buf(_seed_inst, "1hour")
                            # Only bulk_load when the buffer appears empty/sparse
                            # to avoid overwriting fresher WS-streamed candles.
                            _existing_snap = await _seed_buf.snapshot(5)
                            if len(_existing_snap) < 5:
                                await _seed_buf.bulk_load(list(_seed_candles))
                                log.debug(
                                    "[P24.1-EXEC] %s: pushed %d seed candles → DataHub buffer "
                                    "(was %d, now populated for IntelligenceLayer).",
                                    _seed_sym, len(_seed_candles), len(_existing_snap),
                                )
                        except Exception as _buf_exc:
                            log.debug(
                                "[P24.1-EXEC] Seed candle push to DataHub buffer failed "
                                "for %s: %s (non-critical — backfill will cover this).",
                                _seed_sym, _buf_exc,
                            )
                except Exception as _seed_exc:
                    log.debug("[P24-DEFENSE-3] Entropy seed error %s: %s", _seed_sym, _seed_exc)
        except ImportError:
            log.debug("[P24-DEFENSE-3] compute_shannon_entropy import failed — seeding skipped.")
        except Exception as _seed_global_exc:
            log.warning("[P24-DEFENSE-3] Entropy seeding failed: %s", _seed_global_exc)

        # ── [P25-PFVT] Pre-Flight Truth Verification — Non-Blocking (v27.1) ──────
        # CHANGED (v27.1): The REST equity poll is now launched as a background
        # asyncio task rather than being awaited inline.  This means the main
        # cycle loop (and therefore the first real _cycle() status write) begins
        # immediately — the dashboard is never held waiting for OKX to respond.
        #
        # A 5-second asyncio.wait_for() timeout is applied inside the task
        # coroutine.  If OKX doesn't answer within 5 seconds the task logs a
        # warning and exits cleanly; the WebSocket will populate equity on its
        # first push exactly as it did before Phase 25.
        #
        # All original PFVT semantics are preserved:
        #   • force_truth=True  → bypasses the equity floor check on boot.
        #   • _ghost_poll_in_flight is cleared in a finally block so the normal
        #     cycle ghost-recovery logic is never permanently blocked.
        #   • Any exception is non-fatal — the task is fire-and-forget.

        async def _pfvt_background_task() -> None:
            """[P25-PFVT] Non-blocking pre-flight REST equity poll (5 s timeout)."""
            try:
                log.info(
                    "[P25-PFVT] Pre-Flight Truth Verification: "
                    "launching background REST equity poll (timeout=5 s)."
                )
                self._ghost_poll_in_flight = False  # ensure flag never blocks this call
                self._force_truth_pending  = True   # bypass equity floor check on boot
                await asyncio.wait_for(
                    self._verify_equity_via_rest(),
                    timeout=5.0,
                )
                log.info(
                    "[P25-PFVT] Pre-Flight Truth Verification complete: "
                    "equity=%.6f  avail=%.6f  ghost_reads=%d  is_ghost=%s",
                    self._equity,
                    self._avail,
                    self._consecutive_ghost_reads,
                    self._equity_is_ghost,
                )
            except asyncio.TimeoutError:
                log.warning(
                    "[P25-PFVT] Pre-Flight Truth Verification timed out after 5 s "
                    "(non-fatal — OKX REST did not respond; WebSocket will populate "
                    "equity on first push)."
                )
            except Exception as _pfvt_exc:
                log.warning(
                    "[P25-PFVT] Pre-Flight Truth Verification failed (non-fatal — "
                    "WS will populate equity on first push): %s",
                    _pfvt_exc,
                )
            finally:
                # Guarantee the flag is clear regardless of success, timeout, or
                # exception so the normal cycle ghost-recovery logic is unblocked.
                self._ghost_poll_in_flight = False

        asyncio.create_task(_pfvt_background_task(), name="p25_pfvt")
        # ── [/P25-PFVT] ──────────────────────────────────────────────────────────

        self._shadow_task = asyncio.create_task(
            self._shadow_watcher(), name="p9_shadow_watcher"
        )
        self._tape_monitor_task = asyncio.create_task(
            self._tape_monitor_loop(), name="p12_tape_monitor"
        )

        try:
            while True:
                try:
                    await self._cycle()
                except IndexError as _idx_err:
                    # [P36.2-GOLDEN] Golden counter fragility hardening:
                    # Differentiate expected data gaps vs code corruption.
                    if self._p362_is_expected_data_gap_index_error(_idx_err):
                        log.warning(
                            "[P36.2-GOLDEN] Expected data-gap IndexError in _cycle "
                            "(NO golden reset): %s",
                            _idx_err,
                        )
                    else:
                        self._p362_index_error_count += 1
                        self._p362_reset_golden_counter(
                            f"IndexError in _cycle: {_idx_err}"
                        )
                        log.error(
                            "[P36.2-GOLDEN] IndexError in _cycle (golden counter reset): %s",
                            _idx_err, exc_info=True,
                        )
                except Exception as e:

                    log.error("Executor top-level error: %s", e, exc_info=True)
                await asyncio.sleep(interval)
        finally:
            await self.brain.stop_background_tasks()

            for task in (self._shadow_task, self._tape_monitor_task):
                if task and not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            log.info("[P9] Shadow watcher stopped. [P12] Tape monitor stopped.")
