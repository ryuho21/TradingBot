"""
engine_supervisor.py  —  Phase 52 PowerTrader AI Trading Engine Supervisor
═══════════════════════════════════════════════════════════════════════════════

Phase 52 ARCHITECTURAL ROLE
────────────────────────────
This module is the canonical trading engine supervisor.  It owns the full
async event loop, all component life-cycles, and the hub_data status writes.

Python is now a SUPERVISOR + ANALYTICS layer:
  • engine_supervisor.py  — trading engine process (this file)
  • main.py               — thin CLI launcher + mode dispatcher
  • dashboard.py          — read-only monitoring / analytics UI
  • bot_watchdog.py       — process supervisor (restarts engine_supervisor)

The Rust/C++ bridge owns the hot-path (sub-ms execution).
Python publishes "Strategic Intent" (floor/ceil/target/risk-gates) and
supervises the bridge; all execution decisions are bridge-native (P48–P51).

Exit codes:
  0  — Clean, intentional shutdown
  1  — Fatal startup error
  2  — Unrecoverable runtime error / forced shutdown after timeout

Public API
──────────
  async_main(args: argparse.Namespace) -> int
      Start the full trading engine supervisor.  args is a pre-parsed
      argparse.Namespace produced by main.py.

Preserved Invariants (ZERO DRIFT)
───────────────────────────────────
[ATOMIC-MIRROR-SYNC]     Every total_equity write also writes retained_equity.
[ANALYTICS-NULL-SHIELD]  analytics and symbols always (data or {}) — never None.
[S6.1-PRESEED]           hub_data always contains valid JSON at boot.
[S6.1-DBREADY]           DB ensure-ready step runs immediately after DataHub.
[S7A-PRESEED]            Preseed creates tif_pending.json and tactical_config.json.
[S7A-P21-EXPORT]         p21_execution ALWAYS written into trader_status.json.
[S7C-P21-GUARANTEE]      p21_execution present on every _gui_bridge write cycle.
[FIX-SINGLE-WRITER]      _gui_bridge() no longer writes trader_status.json.
                         Supervisor-enriched fields (brain, p21, p52, p54, etc.)
                         are merged into executor._status via dict.update().
                         _touch_hub_timestamp() neutered to throttle-state stub only.
                         _patch_trader_status_equity() neutered to no-op stub.
                         _on_bridge_ghost_healed_main() patches executor._status
                         in-memory directly; _on_bridge_account_update_main() and
                         _on_bridge_fill_main() update executor._status["timestamp"]
                         directly.  executor._write_status_json() (cycle-end) is the
                         sole writer of trader_status.json.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import signal
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp   # [STAGE-3] Binance public trade stream (already a dep via data_hub)
import numpy as np
from dotenv import load_dotenv

load_dotenv(override=True)

from brain           import IntelligenceEngine, BRAIN_STATE_PATH
from arbitrator      import Arbitrator, ArbOpportunity, CoinbaseOracle, GlobalTapeAggregator
from news_wire       import NewsWire, MacroBias
from data_hub        import DataHub, Candle
from executor        import Executor, Position, GlobalRiskManager

from phase7_risk      import CircuitBreaker, KellySizer
from phase7_execution import AdaptiveSpread, TWAPSlicer, SlippageAuditor

from portfolio_manager import PortfolioGovernor
from sentinel          import Sentinel, _send_telegram as _tg_send

# ── [P40] Phase 40: Logic Execution Bridge ─────────────────────────────────────
from bridge_interface import BridgeClient, BridgeSentinel
# ── [/P40] ────────────────────────────────────────────────────────────────────

from intelligence_layer import (
    IntelligenceScraper,
    LLMContextVeto,
    NarrativeResult,
    compute_shannon_entropy,
    entropy_confidence_gate,
    regime_trail_adjustment,
    OPENROUTER_MODEL,
)

# ── [P53] Multi-Node Mesh Consensus Engine ────────────────────────────────────
from mesh_consensus import (
    MeshConsensusGate,
    ConsensusResult,
    NODE_OKX,
    NODE_BINANCE,
    NODE_COINBASE,
    P53_MESH_ENABLED,
)
# ── [/P53] ───────────────────────────────────────────────────────────────────

log = logging.getLogger("engine_supervisor_p52")

# [P52] Module identity — for introspection by watchdog and dashboard.
_P52_SUPERVISOR_ROLE    = "trading_engine"
_P52_SUPERVISOR_VERSION = "52.0"

# ── Safe env parsing (prevents startup crash on invalid env vars) ───────────
def _env_float(key: str, default: float) -> float:
    raw = os.environ.get(key, "")
    if raw == "":
        return default
    try:
        return float(raw)
    except Exception:
        try:
            log.warning("Invalid float env %s=%r; using default=%s", key, raw, default)
        except Exception:
            pass
        return default

def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key, "")
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        try:
            log.warning("Invalid int env %s=%r; using default=%s", key, raw, default)
        except Exception:
            pass
        return default

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("powertrader_p52.log", encoding="utf-8"),
    ],
)
for _lib in ("aiohttp", "websockets", "urllib3", "hpack", "ccxt"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

BASE_DIR           = os.path.dirname(os.path.abspath(__file__))
GUI_SETTINGS       = os.environ.get(
    "POWERTRADER_GUI_SETTINGS", os.path.join(BASE_DIR, "gui_settings.json")
)
# [P24-PATH-1] Hard-coded — never use env override for HUB_DIR.
HUB_DIR            = os.path.join(BASE_DIR, "hub_data")

# [P24.1-PATH] Explicit HUB_DIR creation guard — ensures the directory exists
# on first boot or after accidental deletion, before any path-derived constants
# are consumed by _preseed_hub_state.
try:
    os.makedirs(HUB_DIR, exist_ok=True)
except OSError as _hub_makedirs_exc:
    logging.getLogger("main_p24").critical(
        "[P24.1-PATH] CRITICAL: Cannot create hub_data directory at %s — %s. "
        "Dashboard will not receive status updates until this is resolved.",
        HUB_DIR, _hub_makedirs_exc,
    )

RUNNER_READY_PATH    = os.path.join(HUB_DIR, "runner_ready.json")
TRADER_STATUS_PATH   = os.path.join(HUB_DIR, "trader_status.json")
TIF_PENDING_PATH     = os.path.join(HUB_DIR, "tif_pending.json")
TACTICAL_CONFIG_PATH = os.path.join(HUB_DIR, "tactical_config.json")   # [S6.1-PRESEED]
PANIC_LOCK_PATH      = os.path.join(HUB_DIR, "panic_lock.json")         # [P22-4]
VETO_AUDIT_PATH      = os.path.join(HUB_DIR, "veto_audit.json")         # [P24-SEED-2]
CONTROL_QUEUE_PATH   = os.path.join(HUB_DIR, "control_queue.jsonl")     # [P24-DEFENSE]

# [FIX-ORDERING] Must be before _atomic_write_sync/_preseed_hub_state (called at import time)
MAX_ATOMIC_RETRIES   = 5    # max rename attempts before direct-write fallback
ATOMIC_RETRY_BASE    = 0.1  # back-off multiplier (seconds per attempt)


def _atomic_write_sync(path: str, obj) -> bool:
    """
    [S7A-PRESEED] Synchronous atomic write — Windows-safe implementation.

    Root cause of [WinError 2] on Windows: `path + ".tmp"` is a well-known
    AV/scanner target that gets deleted between the write() close and the
    os.replace() call.  Fix: use tempfile.NamedTemporaryFile(delete=False) in
    the SAME directory as the target so the rename is always same-filesystem
    and the file name is randomised (AV scanners won't auto-delete it).

    Falls back to a direct overwrite if all rename attempts fail — never leaves
    the caller with a missing file.
    """
    import tempfile as _tf
    _dir = os.path.dirname(os.path.abspath(path))
    _log = logging.getLogger("main_p24")
    tmp_path: Optional[str] = None
    try:
        os.makedirs(_dir, exist_ok=True)
    except Exception:
        pass
    try:
        with _tf.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".tmp",
            dir=_dir, delete=False
        ) as _f:
            tmp_path = _f.name
            json.dump(obj, _f, indent=2)
    except Exception as exc:
        _log.error("[S7A-PRESEED] _atomic_write_sync serialise failed for %s: %s", path, exc)
        if tmp_path:
            try: os.unlink(tmp_path)
            except Exception: pass
        return False
    for _attempt in range(1, MAX_ATOMIC_RETRIES + 1):
        try:
            os.replace(tmp_path, path)
            return True
        except (PermissionError, OSError):
            if _attempt < MAX_ATOMIC_RETRIES:
                time.sleep(ATOMIC_RETRY_BASE * _attempt)
    # All renames failed — fallback: direct overwrite (non-atomic but recoverable)
    try:
        _log.warning("[S7A-PRESEED] rename failed — direct fallback write for %s", path)
        with open(path, "w", encoding="utf-8") as _f:
            json.dump(obj, _f, indent=2)
        return True
    except Exception as fb_exc:
        _log.error("[S7A-PRESEED] direct fallback write also failed for %s: %s", path, fb_exc)
    if tmp_path:
        try: os.unlink(tmp_path)
        except Exception: pass
    return False


def _preseed_hub_state() -> None:
    """
    [P24-SEED-2 / S6.1-PRESEED / S7A-PRESEED]

    Create the hub_data/ directory if missing, then atomically write
    zero-state valid JSON to all required seed files that do not yet exist.

    Files seeded (atomic tmp → replace, existing files are NEVER overwritten):
      trader_status.json   — zero-state status object         (P24-SEED-2)
      veto_audit.json      — empty audit list []               (P24-SEED-2)
      tif_pending.json     — empty pending-order map  {}       (S6.1-PRESEED / S7A)
      tactical_config.json — empty config map         {}       (S6.1-PRESEED / S7A)

    All writes use atomic tmp → os.replace so a crash mid-write never leaves a
    corrupt file.  Falls back to a direct write if the rename-from-tmp fails.
    """
    _log = logging.getLogger("main_p24")

    try:
        os.makedirs(HUB_DIR, exist_ok=True)
    except Exception as exc:
        _log.warning("[S7A-PRESEED] makedirs failed: %s", exc)

    _NOW = time.time()

    # [S7A-PRESEED] tif_pending and tactical_config are explicitly {} (empty
    # JSON objects) so TIFWatcher._load_pending() never raises FileNotFoundError
    # and the tactical config reader never sees null/missing data.
    _SEEDS: dict = {
        TRADER_STATUS_PATH: {
            "timestamp":    _NOW,
            "status":       "pre_seed",
            "demo_mode":    False,
            "account":      {
                "total_equity":    0.0,
                # [ATOMIC-MIRROR-SYNC] retained_equity always mirrors total_equity.
                "retained_equity": 0.0,
                "buying_power":    0.0,
            },
            "positions":    {},
            "p24_preseed":  True,
        },
        VETO_AUDIT_PATH:      [],
        TIF_PENDING_PATH:     {},   # [S6.1-PRESEED / S7A-PRESEED] valid empty JSON object
        TACTICAL_CONFIG_PATH: {},   # [S6.1-PRESEED / S7A-PRESEED] valid empty JSON object
    }

    for path, payload in _SEEDS.items():
        if os.path.exists(path):
            continue  # do not overwrite a live file

        # Primary: atomic tmp → replace
        if _atomic_write_sync(path, payload):
            _log.info("[S7A-PRESEED] Zero-state seeded → %s", path)
            continue

        # Fallback: direct write (if rename failed, e.g. cross-device)
        _log.warning("[S7A-PRESEED] Atomic write failed for %s — trying direct write.", path)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            _log.info("[S7A-PRESEED] Direct fallback write OK → %s", path)
        except Exception as exc2:
            _log.error(
                "[S7A-PRESEED] Both atomic and direct write failed for %s: %s", path, exc2
            )


# [P24-SEED-2 / S7A-PRESEED] Run pre-seed at import time (before any component
# is initialised).  All four hub_data files are guaranteed to contain valid JSON
# before any reader can attempt to open them.
_preseed_hub_state()

TF_ALL = ["1hour", "2hour", "4hour", "8hour", "12hour", "1day", "1week"]

NEWS_BLOCK_THRESHOLD = _env_float("NEWS_BEARISH_BLOCK_THRESHOLD", -0.5)
ARB_EXEC_PCT         = _env_float("ARB_SPREAD_EXEC_PCT", 0.2)
# MAX_ATOMIC_RETRIES and ATOMIC_RETRY_BASE moved to line ~169 (before _preseed_hub_state)

P19_STATE_SAVE_INTERVAL_SECS = _env_float("P19_STATE_SAVE_INTERVAL_SECS", float(str(60 * 30)))

P20_DRAWDOWN_ZOMBIE_PCT   = _env_float("P20_DRAWDOWN_ZOMBIE_PCT", 10.0)
P20_COLD_START_GRACE_SECS = _env_float("P20_COLD_START_GRACE_SECS", 60.0)
P20_SHADOW_INTERVAL_SECS  = _env_float("P20_SHADOW_INTERVAL_SECS", float(str(24 * 3600)))
P20_SHADOW_AUTO_SWAP      = os.environ.get("P20_SHADOW_AUTO_SWAP", "0").strip() == "1"

# [SHUTDOWN-4] Maximum seconds to wait for all components to close gracefully.
SHUTDOWN_TIMEOUT_SECS = _env_float("SHUTDOWN_TIMEOUT_SECS", 10.0)

# ── [P21] Institutional Execution parameters ──────────────────────────────────
P21_SPREAD_THROTTLE_PCT     = _env_float("P21_SPREAD_THROTTLE_PCT", 0.05)
P21_LOW_LIQUIDITY_THRESHOLD = _env_float("P21_LOW_LIQUIDITY_THRESHOLD", 0.4)
P21_MICROBURST_LEGS         = _env_int("P21_MICROBURST_LEGS", 3)
P21_MICROBURST_DELAY_SECS   = _env_float("P21_MICROBURST_DELAY_SECS", 0.8)
P21_TIF_TIMEOUT_SECS        = _env_float("P21_TIF_TIMEOUT_SECS", 10.0)
P21_TIF_POLL_SECS           = _env_float("P21_TIF_POLL_SECS", 1.0)
P21_SPREAD_CACHE_TTL        = _env_float("P21_SPREAD_CACHE_TTL", 2.0)

# ── [P22] Phase 22 parameters ─────────────────────────────────────────────────
P22_ENTROPY_HIGH_THRESHOLD   = _env_float("P22_ENTROPY_HIGH_THRESHOLD", 3.5)
P22_ENTROPY_CONFIDENCE_BOOST = _env_float("P22_ENTROPY_CONFIDENCE_BOOST", 0.2)
P22_BULL_TRAIL_WIDEN         = _env_float("P22_BULL_TRAIL_WIDEN", 0.005)
P22_CHOP_TRAIL_TIGHTEN       = _env_float("P22_CHOP_TRAIL_TIGHTEN", 0.002)
P22_PANIC_DROP_PCT           = _env_float("P22_PANIC_DROP_PCT", 1.5)
P22_PANIC_QUORUM_PCT         = _env_float("P22_PANIC_QUORUM_PCT", 0.66)
P22_PANIC_WINDOW_SECS        = _env_float("P22_PANIC_WINDOW_SECS", 60.0)
P22_PANIC_BLOCK_SECS         = _env_float("P22_PANIC_BLOCK_SECS", 300.0)
P22_MIN_SIGNAL_CONFIDENCE    = _env_float("P22_MIN_SIGNAL_CONFIDENCE", 0.55)

# ── [S7C-P21-GUARANTEE] Snapshot error log throttle interval (seconds) ────────
# Repeated p21_monitor.snapshot() failures are WARNING-logged at most once per
# this interval to prevent log spam on sustained monitor degradation.
_P21_SNAP_ERR_THROTTLE_SECS: float = 30.0

# ── [P53] Phase 53 Mesh Consensus configuration ───────────────────────────────
# These mirror defaults in mesh_consensus.py; engine_supervisor reads them
# purely for log banners — the gate itself reads from env at import time.
P53_ENABLED: bool = os.environ.get("P53_MESH_ENABLED", "1").strip() not in ("0", "false", "no")

# Interval (seconds) at which _gui_bridge feeds OKX bridge P47 data into the
# consensus gate.  Set <= 0 to feed on every _gui_bridge cycle.
_P53_FEED_INTERVAL_SECS: float = _env_float("P53_FEED_INTERVAL_SECS", 2.0)

# ── [S7A-P21-EXPORT] P21 stub factories (module-level, not inside while loop) ─
# These are used by _gui_bridge to guarantee p21_execution is always exported.

def _p21_disabled_stub() -> dict:
    """[S7A-P21-EXPORT] Stable stub emitted when P21 is disabled or monitor is None."""
    return {
        "p21_enabled":        False,
        "p21_disabled":       True,
        "p21_slippage_guard": {},
        "p21_microburst":     {},
        "p21_tif_watcher":    {},
    }


def _p21_enabled_stub() -> dict:
    """[S7A-P21-EXPORT] Stable stub emitted when P21 is enabled but snapshot unavailable."""
    return {
        "p21_enabled":        True,
        "_synthesised":       True,
        "p21_slippage_guard": {},
        "p21_microburst":     {},
        "p21_tif_watcher":    {},
    }


def _p21_validate_snap(snap: dict) -> dict:
    """[S7A-P21-EXPORT] Ensure all required sub-keys are present dicts."""
    if not isinstance(snap.get("p21_slippage_guard"), dict):
        snap["p21_slippage_guard"] = {}
    if not isinstance(snap.get("p21_microburst"), dict):
        snap["p21_microburst"] = {}
    if not isinstance(snap.get("p21_tif_watcher"), dict):
        snap["p21_tif_watcher"] = {}
    if "p21_enabled" not in snap:
        snap["p21_enabled"] = True
    return snap


def _make_neutral_narrative() -> NarrativeResult:
    """
    [LLMFB-3] Return a safe neutral NarrativeResult on empty/failed LLM response.
    """
    import types as _types
    _FIELDS = dict(
        verdict="PASS",
        score=0.5,
        conviction_multiplier=1.0,
        latency_ms=0.0,
        council_detail=[],
    )
    try:
        return NarrativeResult(**_FIELDS)
    except TypeError:
        pass
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
    ns = _types.SimpleNamespace(**_FIELDS)
    return ns  # type: ignore[return-value]

class GracedGlobalRiskManager(GlobalRiskManager):
    """[P20-1] GlobalRiskManager with a cold-start grace period."""

    def __init__(
        self,
        executor,
        brain,
        zombie_pct: float = P20_DRAWDOWN_ZOMBIE_PCT,
        grace_secs: float = P20_COLD_START_GRACE_SECS,
    ) -> None:
        super().__init__(executor=executor, brain=brain, zombie_pct=zombie_pct)
        self.start_time: float = time.time()
        self.grace_secs: float = grace_secs
        log.info(
            "[P20-1] GracedGlobalRiskManager initialised — grace: %.0f s (until %s UTC)",
            grace_secs,
            datetime.utcfromtimestamp(self.start_time + grace_secs).strftime("%H:%M:%S"),
        )

    def check_drawdown(self, current_equity: float) -> bool:
        elapsed = time.time() - self.start_time
        if elapsed < self.grace_secs:
            return False
        return super().check_drawdown(current_equity)

    def status_snapshot(self) -> dict:
        snap     = super().status_snapshot()
        elapsed  = time.time() - self.start_time
        in_grace = elapsed < self.grace_secs
        snap.update({
            "in_grace_period":      in_grace,
            "grace_secs":           self.grace_secs,
            "grace_remaining_secs": round(max(0.0, self.grace_secs - elapsed), 1),
            "grace_until_ts":       round(self.start_time + self.grace_secs, 1),
        })
        return snap

class GracedCircuitBreaker(CircuitBreaker):
    """[P7/P20] CircuitBreaker with a cold-start grace window."""

    def __init__(
        self,
        db,
        status_path: str,
        grace_secs: float = P20_COLD_START_GRACE_SECS,
    ) -> None:
        super().__init__(db, status_path)
        self.start_time: float = time.time()
        self.grace_secs: float = grace_secs
        log.info(
            "[P7] GracedCircuitBreaker initialised — grace: %.0f s (until %s UTC)",
            grace_secs,
            datetime.utcfromtimestamp(self.start_time + grace_secs).strftime("%H:%M:%S"),
        )

    def _in_grace(self) -> bool:
        return (time.time() - self.start_time) < self.grace_secs

    def update(self, equity: float) -> None:
        if self._in_grace():
            return
        super().update(equity)

    def _trip(self, *args, **kwargs):
        if self._in_grace():
            return
        return super()._trip(*args, **kwargs)

    @property
    def is_tripped(self) -> bool:
        if self._in_grace():
            return False
        return super().is_tripped

class PanicLock:
    """
    [P22-4] Monitors a rolling price window across all active coins.
    When >= quorum_pct of coins simultaneously drop > drop_pct within the
    last window_secs seconds, a SYSTEMIC PANIC is declared and all new BUY
    entries are blocked for block_secs.
    """

    def __init__(
        self,
        symbols:      List[str],
        drop_pct:     float = P22_PANIC_DROP_PCT,
        quorum_pct:   float = P22_PANIC_QUORUM_PCT,
        window_secs:  float = P22_PANIC_WINDOW_SECS,
        block_secs:   float = P22_PANIC_BLOCK_SECS,
        persist_path: str   = PANIC_LOCK_PATH,
    ) -> None:
        self._symbols      = list(symbols)
        self._drop_pct     = drop_pct
        self._quorum_pct   = quorum_pct
        self._window_secs  = window_secs
        self._block_secs   = block_secs
        self._persist_path = persist_path
        self._lock         = asyncio.Lock()

        self._price_history: Dict[str, deque] = {
            sym: deque() for sym in self._symbols
        }

        self._blocked_until: float = 0.0
        self._trigger_count: int   = 0

        log.info(
            "[P22-4] PanicLock initialised — "
            "drop=%.2f%%  quorum=%.0f%%  window=%.0fs  block=%.0fs",
            drop_pct, quorum_pct * 100, window_secs, block_secs,
        )
        self._load_state()

    def _load_state(self) -> None:
        try:
            if os.path.exists(self._persist_path):
                with open(self._persist_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                blocked_until = float(data.get("blocked_until", 0.0))
                if blocked_until > time.time():
                    self._blocked_until = blocked_until
                    rem = blocked_until - time.time()
                    log.warning(
                        "[P22-4] PanicLock restored from disk — "
                        "block active for %.0f s more (until %s UTC)",
                        rem,
                        datetime.utcfromtimestamp(blocked_until).strftime("%H:%M:%S"),
                    )
        except Exception as exc:
            log.debug("[P22-4] Could not load PanicLock state: %s", exc)

    def _save_state_sync(self) -> None:
        # [WIN-ATOMIC] Use NamedTemporaryFile instead of .tmp to avoid Windows AV/lock race
        import tempfile as _tf
        _dir = os.path.dirname(os.path.abspath(self._persist_path))
        try:
            os.makedirs(_dir, exist_ok=True)
        except Exception:
            pass
        tmp_path = None
        try:
            with _tf.NamedTemporaryFile(
                mode="w", encoding="utf-8", suffix=".tmp",
                dir=_dir, delete=False
            ) as _f:
                tmp_path = _f.name
                json.dump({
                    "blocked_until": self._blocked_until,
                    "trigger_count": self._trigger_count,
                    "saved_at":      time.time(),
                }, _f, indent=2)
            os.replace(tmp_path, self._persist_path)
        except Exception as exc:
            log.debug("[P22-4] PanicLock save error: %s", exc)
            if tmp_path:
                try: os.unlink(tmp_path)
                except Exception: pass

    async def _save_state(self) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._save_state_sync)

    async def record_price(self, symbol: str, price: float) -> None:
        now = time.time()
        async with self._lock:
            if symbol not in self._price_history:
                self._price_history[symbol] = deque()
            dq = self._price_history[symbol]
            dq.append((now, price))
            while dq and now - dq[0][0] > self._window_secs:
                dq.popleft()
        await self._check_panic()

    async def _check_panic(self) -> None:
        now = time.time()
        async with self._lock:
            if now < self._blocked_until:
                return

            panicked = 0
            total    = 0
            for sym, dq in self._price_history.items():
                if len(dq) < 2:
                    continue
                total += 1
                oldest_price = dq[0][1]
                latest_price = dq[-1][1]
                if oldest_price <= 0:
                    continue
                drop = (oldest_price - latest_price) / oldest_price * 100.0
                if drop >= self._drop_pct:
                    panicked += 1

            if total == 0:
                return

            if panicked / total >= self._quorum_pct:
                self._blocked_until  = now + self._block_secs
                self._trigger_count += 1
                log.critical(
                    "[P22-4] SYSTEMIC PANIC LOCK TRIGGERED — "
                    "%d/%d coins dropped >%.2f%% in %.0fs window.  "
                    "BUY entries BLOCKED for %.0fs (until %s UTC). "
                    "Trigger #%d.",
                    panicked, total,
                    self._drop_pct, self._window_secs,
                    self._block_secs,
                    datetime.utcfromtimestamp(self._blocked_until).strftime("%H:%M:%S"),
                    self._trigger_count,
                )

        await self._save_state()

    def is_locked(self) -> bool:
        return time.time() < self._blocked_until

    def remaining_secs(self) -> float:
        return max(0.0, self._blocked_until - time.time())

    def add_symbol(self, symbol: str) -> None:
        if symbol not in self._price_history:
            self._price_history[symbol] = deque()

    def status_snapshot(self) -> dict:
        locked   = self.is_locked()
        per_sym: Dict[str, dict] = {}
        for sym, dq in self._price_history.items():
            if len(dq) < 2:
                continue
            oldest = dq[0][1]
            latest = dq[-1][1]
            drop   = (oldest - latest) / oldest * 100.0 if oldest > 0 else 0.0
            per_sym[sym] = {
                "oldest_price": round(oldest, 4),
                "latest_price": round(latest, 4),
                "drop_pct":     round(drop, 4),
                "panicked":     drop >= self._drop_pct,
                "observations": len(dq),
            }
        return {
            "locked":         locked,
            "blocked_until":  round(self._blocked_until, 1),
            "remaining_secs": round(self.remaining_secs(), 1),
            "trigger_count":  self._trigger_count,
            "drop_threshold": self._drop_pct,
            "quorum_pct":     self._quorum_pct,
            "window_secs":    self._window_secs,
            "block_secs":     self._block_secs,
            "per_coin":       per_sym,
        }

class DynamicSlippageGuard:
    """[P21-1] Fetches real-time BBO spread and throttles order entry."""

    def __init__(
        self,
        hub,
        throttle_pct: float = P21_SPREAD_THROTTLE_PCT,
        cache_ttl:    float = P21_SPREAD_CACHE_TTL,
    ) -> None:
        self._hub          = hub
        self._throttle_pct = throttle_pct
        self._cache_ttl    = cache_ttl
        self._cache: Dict[str, Tuple[float, float, float, float, bool]] = {}
        self._throttle_count: int = 0
        self._pass_count:     int = 0
        self._spread_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        log.info("[P21-1] DynamicSlippageGuard init throttle=%.4f%%", throttle_pct)

    async def fetch_spread(self, symbol: str) -> Tuple[float, float, float]:
        inst = f"{symbol.upper()}-USDT"
        try:
            raw  = await self._hub.rest.get(
                "/api/v5/market/books", params={"instId": inst, "sz": "1"}
            )
            data = (raw.get("data") or [{}])[0]
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if not bids or not asks:
                return 0.0, 0.0, 0.0
            bid = float(bids[0][0])
            ask = float(asks[0][0])
            if bid <= 0 or ask <= 0:
                return 0.0, 0.0, 0.0
            mid        = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid * 100.0
            return bid, ask, spread_pct
        except Exception as exc:
            log.debug("[P21-1] fetch_spread(%s) error: %s", symbol, exc)
            return 0.0, 0.0, 0.0

    async def should_throttle(self, symbol: str) -> Tuple[bool, float]:
        now   = time.time()
        cache = self._cache.get(symbol)
        if cache is not None and (now - cache[0]) < self._cache_ttl:
            _, bid, ask, spread_pct, throttled = cache
        else:
            bid, ask, spread_pct = await self.fetch_spread(symbol)
            throttled = (spread_pct > self._throttle_pct) if spread_pct > 0 else False
            self._cache[symbol] = (now, bid, ask, spread_pct, throttled)

        self._spread_history[symbol].append((now, spread_pct))
        if throttled:
            self._throttle_count += 1
        else:
            self._pass_count += 1
        return throttled, spread_pct

    def latest_spread(self, symbol: str) -> float:
        cache = self._cache.get(symbol)
        return cache[3] if cache is not None else 0.0

    def status_snapshot(self) -> dict:
        return {
            "throttle_pct":   self._throttle_pct,
            "throttle_count": self._throttle_count,
            "pass_count":     self._pass_count,
            "latest_spreads": {s: round(self._cache[s][3], 6) for s in self._cache},
            "throttled_now":  {s: self._cache[s][4] for s in self._cache},
        }

    async def refresh_all(self, symbols: list) -> None:
        """[P21-DASH] Proactively refresh the spread cache for all active symbols
        so the P21-1 dashboard panel always shows live bid/ask data even when no
        order is being placed.  Skips symbols whose cache entry is still fresh
        within cache_ttl.  Does NOT increment throttle/pass counters."""
        for sym in symbols:
            try:
                now   = time.time()
                cache = self._cache.get(sym)
                if cache is None or (now - cache[0]) >= self._cache_ttl:
                    bid, ask, spread_pct = await self.fetch_spread(sym)
                    throttled = (spread_pct > self._throttle_pct) if spread_pct > 0 else False
                    self._cache[sym] = (now, bid, ask, spread_pct, throttled)
            except Exception as exc:
                log.debug("[P21-1] refresh_all(%s) error: %s", sym, exc)

class MicroBurstExecutor:
    """[P21-2] Splits a large order into N progressively-sized legs on low liquidity."""

    def __init__(
        self,
        executor,
        legs:          int   = P21_MICROBURST_LEGS,
        delay_secs:    float = P21_MICROBURST_DELAY_SECS,
        liq_threshold: float = P21_LOW_LIQUIDITY_THRESHOLD,
    ) -> None:
        self._exec          = executor
        self._legs          = max(1, legs)
        self._delay_secs    = delay_secs
        self._liq_threshold = liq_threshold
        raw_weights         = list(range(1, self._legs + 1))
        total               = sum(raw_weights)
        self._leg_fractions = [w / total for w in raw_weights]
        self._burst_count:  int = 0
        self._single_count: int = 0
        self._leg_fills:    int = 0
        log.info(
            "[P21-2] MicroBurstExecutor init legs=%d delay=%.2fs liq=%.2f",
            self._legs, self._delay_secs, self._liq_threshold,
        )

    def _is_low_liquidity(self, oracle_signal) -> bool:
        if oracle_signal is None:
            return False
        liq = getattr(oracle_signal, "liquidity_score", None)
        if liq is not None:
            return float(liq) < self._liq_threshold
        return bool(getattr(oracle_signal, "low_liquidity", False))

    async def execute(
        self,
        symbol,
        side,
        total_usd,
        swap,
        pos_side,
        tag,
        coin_cfg,
        oracle_signal=None,
        tif_watcher: "TIFWatcher | None" = None,
    ) -> bool:
        if self._is_low_liquidity(oracle_signal):
            return await self._microburst(
                symbol, side, total_usd, swap, pos_side, tag, coin_cfg, tif_watcher
            )
        return await self._single(
            symbol, side, total_usd, swap, pos_side, tag, coin_cfg, tif_watcher
        )

    async def _single(
        self, symbol, side, total_usd, swap, pos_side, tag, coin_cfg, tif_watcher
    ) -> bool:
        self._single_count += 1
        try:
            ord_id = await self._exec._execute_order(
                symbol, side, total_usd, swap=swap,
                pos_side=pos_side, tag=tag, coin_cfg=coin_cfg,
            )
            if ord_id and tif_watcher:
                tif_watcher.register(ord_id, symbol, side, total_usd, tag)
            return True
        except Exception as exc:
            log.error("[P21-2] Single execute error %s: %s", symbol, exc, exc_info=True)
            return False

    async def _microburst(
        self, symbol, side, total_usd, swap, pos_side, tag, coin_cfg, tif_watcher
    ) -> bool:
        self._burst_count += 1
        any_success = False
        for i, frac in enumerate(self._leg_fractions):
            leg_usd = total_usd * frac
            min_usd = getattr(coin_cfg, "min_usd_value", 1.0)
            if leg_usd < min_usd:
                continue
            leg_tag = f"{tag}_MB{i+1}of{self._legs}"
            try:
                ord_id = await self._exec._execute_order(
                    symbol, side, leg_usd, swap=swap,
                    pos_side=pos_side, tag=leg_tag, coin_cfg=coin_cfg,
                )
                if ord_id:
                    self._leg_fills += 1
                    any_success = True
                    if tif_watcher:
                        tif_watcher.register(ord_id, symbol, side, leg_usd, leg_tag)
            except Exception as exc:
                log.error(
                    "[P21-2] MicroBurst leg %d error %s: %s", i + 1, symbol, exc,
                    exc_info=True,
                )
            if i < self._legs - 1:
                await asyncio.sleep(self._delay_secs)
        return any_success

    def status_snapshot(self) -> dict:
        return {
            "legs":            self._legs,
            "delay_secs":      self._delay_secs,
            "liq_threshold":   self._liq_threshold,
            "leg_fractions":   [round(f, 4) for f in self._leg_fractions],
            "burst_count":     self._burst_count,
            "single_count":    self._single_count,
            "leg_fills_total": self._leg_fills,
        }

class TIFWatcher:
    """
    [P21-3] Time-in-Force watchdog — cancels unfilled orders after timeout_secs.
    State is persisted to TIF_PENDING_PATH for orphan detection across restarts.
    """

    def __init__(
        self,
        hub,
        timeout_secs: float = P21_TIF_TIMEOUT_SECS,
        poll_secs:    float = P21_TIF_POLL_SECS,
    ) -> None:
        self._hub          = hub
        self._timeout_secs = timeout_secs
        self._poll_secs    = poll_secs
        self._pending: Dict[str, dict] = {}
        self._lock         = asyncio.Lock()
        self._cancel_count:  int  = 0
        self._fill_count:    int  = 0
        self._timeout_count: int  = 0
        self._running:       bool = False
        self._task: Optional[asyncio.Task] = None
        log.info("[P21-3] TIFWatcher init timeout=%.0fs", timeout_secs)
        self._load_pending()

    def _load_pending(self) -> None:
        try:
            if os.path.exists(TIF_PENDING_PATH):
                with open(TIF_PENDING_PATH, "r", encoding="utf-8") as f:
                    orphans = json.load(f)
                if orphans:
                    log.warning(
                        "[P21-3] %d orphaned orders from previous session.", len(orphans)
                    )
        except Exception as exc:
            log.debug("[P21-3] Could not load TIF state: %s", exc)

    async def _save_pending(self) -> None:
        # [WIN-ATOMIC] NamedTemporaryFile prevents WinError 2 (AV/scanner race on .tmp)
        import tempfile as _tf
        _dir = os.path.dirname(os.path.abspath(TIF_PENDING_PATH))
        tmp_path = None
        try:
            async with self._lock:
                snap = dict(self._pending)
            try:
                os.makedirs(_dir, exist_ok=True)
            except Exception:
                pass
            with _tf.NamedTemporaryFile(
                mode="w", encoding="utf-8", suffix=".tmp",
                dir=_dir, delete=False
            ) as _f:
                tmp_path = _f.name
                json.dump(snap, _f, indent=2)
            os.replace(tmp_path, TIF_PENDING_PATH)
        except Exception as exc:
            if tmp_path:
                try: os.unlink(tmp_path)
                except Exception: pass
            log.debug("[P21-3] Could not persist TIF state: %s", exc)

    def register(self, ord_id: str, symbol: str, side: str, usd: float, tag: str) -> None:
        if isinstance(ord_id, dict):
            ord_id = str(ord_id.get("ordId", ""))
        else:
            ord_id = str(ord_id) if ord_id is not None else ""
        if not ord_id:
            return
        entry = {
            "symbol":        symbol,
            "side":          side,
            "usd":           usd,
            "tag":           tag,
            "registered_at": time.time(),
        }
        asyncio.get_event_loop().call_soon(
            lambda: asyncio.ensure_future(self._register_async(ord_id, entry))
        )

    async def _register_async(self, ord_id: str, entry: dict) -> None:
        ord_id = str(ord_id) if not isinstance(ord_id, str) else ord_id
        async with self._lock:
            self._pending[ord_id] = entry
        await self._save_pending()

    async def confirm_fill(self, ord_id: str) -> None:
        async with self._lock:
            if ord_id in self._pending:
                self._pending.pop(ord_id)
                self._fill_count += 1
        await self._save_pending()

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task    = asyncio.create_task(self._poll_loop(), name="p21_tif_watcher")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._poll_secs)
                await self._check_timeouts()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.warning("[P21-3] TIF poll error: %s", exc)

    async def _check_timeouts(self) -> None:
        now = time.time()
        async with self._lock:
            timed_out = [
                (oid, meta) for oid, meta in self._pending.items()
                if now - meta["registered_at"] >= self._timeout_secs
            ]
        for ord_id, meta in timed_out:
            self._timeout_count += 1
            await self._cancel_order(ord_id, meta)
        if timed_out:
            await self._save_pending()

    async def _cancel_order(self, ord_id: str, meta: dict) -> None:
        inst = f"{meta['symbol'].upper()}-USDT"
        try:
            resp = await self._hub.rest.post(
                "/api/v5/trade/cancel-order",
                data={"instId": inst, "ordId": ord_id},
            )
            code = (resp.get("data") or [{}])[0].get("sCode", "?")
            if str(code) == "0":
                self._cancel_count += 1
        except Exception as exc:
            log.warning("[P21-3] Cancel request failed ord_id=%s: %s", ord_id, exc)
        finally:
            async with self._lock:
                self._pending.pop(ord_id, None)

    def status_snapshot(self) -> dict:
        pending_list = []
        for oid, meta in self._pending.items():
            age = time.time() - meta["registered_at"]
            pending_list.append({
                "ord_id":      oid,
                "symbol":      meta["symbol"],
                "side":        meta["side"],
                "usd":         round(meta["usd"], 2),
                "tag":         meta["tag"],
                "age_secs":    round(age, 1),
                "pct_timeout": round(age / self._timeout_secs * 100, 1),
            })
        return {
            "timeout_secs":   self._timeout_secs,
            "pending_count":  len(self._pending),
            "pending_orders": pending_list,
            "cancel_count":   self._cancel_count,
            "fill_count":     self._fill_count,
            "timeout_count":  self._timeout_count,
        }

class P21ExecutionMonitor:
    """[P21] Aggregates status snapshots from all three P21 sub-systems."""

    def __init__(self, slippage_guard, microburst, tif_watcher) -> None:
        self._sg  = slippage_guard
        self._mb  = microburst
        self._tif = tif_watcher

    def snapshot(self) -> dict:
        return {
            "p21_slippage_guard":        self._sg.status_snapshot(),
            "p21_microburst":            self._mb.status_snapshot(),
            "p21_tif_watcher":           self._tif.status_snapshot(),
            "p21_enabled":               True,
            "p21_spread_throttle_pct":   P21_SPREAD_THROTTLE_PCT,
            "p21_liq_threshold":         P21_LOW_LIQUIDITY_THRESHOLD,
            "p21_microburst_legs":       P21_MICROBURST_LEGS,
            "p21_microburst_delay_secs": P21_MICROBURST_DELAY_SECS,
            "p21_tif_timeout_secs":      P21_TIF_TIMEOUT_SECS,
        }

class EntropyBuffer:
    """[P22-2] Time-windowed price series per symbol for Entropy Shield gating."""

    def __init__(self, window_secs: float = P22_PANIC_WINDOW_SECS) -> None:
        self._window = window_secs
        self._prices: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))

    def record(self, symbol: str, price: float) -> None:
        now = time.time()
        self._prices[symbol].append((now, price))
        dq = self._prices[symbol]
        while dq and now - dq[0][0] > self._window:
            dq.popleft()

    def get_returns(self, symbol: str) -> List[float]:
        dq = self._prices[symbol]
        if len(dq) < 2:
            return []
        prices  = [p for _, p in dq]
        returns = [
            (prices[i] - prices[i - 1]) / prices[i - 1] * 100.0
            for i in range(1, len(prices))
            if prices[i - 1] != 0
        ]
        return returns

def load_coins_from_settings() -> List[str]:
    try:
        with open(GUI_SETTINGS, "r", encoding="utf-8") as f:
            d = json.load(f)
        coins = d.get("coins", [])
        if isinstance(coins, list) and coins:
            return [str(c).strip().upper() for c in coins if str(c).strip()]
    except Exception:
        pass
    return ["BTC", "ETH", "XRP", "BNB", "DOGE"]

async def _atomic_write(path: str, obj: dict) -> bool:
    """[P8] Atomic JSON write — Windows-safe implementation.

    WHY [WinError 2] occurs on Windows:
      `path + ".tmp"` is a predictable filename that Windows Defender and
      some AV scanners quarantine/delete between the open() close and
      os.replace().  Using tempfile.NamedTemporaryFile(delete=False) in the
      SAME directory gives a randomised name (scanner-safe) and ensures a
      same-filesystem rename (no cross-drive issues).

    Fallback: if all retries fail, performs a direct non-atomic overwrite so
    the status file is NEVER left stale.
    """
    import tempfile as _tf
    _dir = os.path.dirname(os.path.abspath(path))
    tmp_path: Optional[str] = None
    try:
        os.makedirs(_dir, exist_ok=True)
    except Exception:
        pass
    try:
        with _tf.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".tmp",
            dir=_dir, delete=False
        ) as _f:
            tmp_path = _f.name
            json.dump(obj, _f, indent=2)
    except Exception as exc:
        log.error("[P8] Atomic write failed to serialise %s: %s", path, exc, exc_info=True)
        if tmp_path:
            try: os.unlink(tmp_path)
            except Exception: pass
        return False
    for attempt in range(1, MAX_ATOMIC_RETRIES + 1):
        try:
            os.replace(tmp_path, path)
            return True
        except (PermissionError, OSError) as exc:
            if attempt == MAX_ATOMIC_RETRIES:
                log.error(
                    "[P8] All %d atomic rename attempts failed for %s: %s",
                    MAX_ATOMIC_RETRIES, path, exc, exc_info=True,
                )
                # Windows fallback: direct overwrite — non-atomic but recoverable.
                # Prevents the dashboard from seeing a permanently stale status file.
                try:
                    with open(path, "w", encoding="utf-8") as _f:
                        json.dump(obj, _f, indent=2)
                    log.warning("[P8] Used direct fallback write for %s", path)
                    return True
                except Exception as fb_exc:
                    log.error("[P8] Direct fallback write also failed for %s: %s", path, fb_exc)
                if tmp_path:
                    try: os.unlink(tmp_path)
                    except Exception: pass
                return False
            await asyncio.sleep(ATOMIC_RETRY_BASE * attempt)
    if tmp_path:
        try: os.unlink(tmp_path)
        except Exception: pass
    return False

# [P40.1-HB] Throttled heartbeat state.
_P401_HB_LOCK: asyncio.Lock = asyncio.Lock()
_P401_HB_LAST_WRITE_TS: float = 0.0
_P401_HB_MIN_INTERVAL_SECS: float = _env_float("P401_HB_MIN_INTERVAL_SECS", 5.0)

async def _touch_hub_timestamp(path: str = TRADER_STATUS_PATH, *, force: bool = False) -> None:
    """[P40.1-HB] Update throttle state only.

    [FIX-SINGLE-WRITER] File read/write removed.  Call sites that previously
    relied on this function to touch the file now update executor._status
    ["timestamp"] directly.  This function is retained as a no-op stub so
    existing create_task() call sites do not need to be individually removed.
    The throttle gate is kept to preserve the _P401_HB_LAST_WRITE_TS state.
    """
    try:
        now = time.time()
        async with _P401_HB_LOCK:
            global _P401_HB_LAST_WRITE_TS
            if (not force) and (_P401_HB_LAST_WRITE_TS > 0) and (now - _P401_HB_LAST_WRITE_TS) < _P401_HB_MIN_INTERVAL_SECS:
                return
            _P401_HB_LAST_WRITE_TS = now
        # [FIX-SINGLE-WRITER] No file write — caller is responsible for
        # in-memory executor._status["timestamp"] mutation at call site.
    except Exception as _exc:
        log.debug("[P40.1-HB] _touch_hub_timestamp error (non-fatal): %s", _exc)

async def _patch_trader_status_equity(
    path: str,
    retained_equity: float,
    bridge_source: str,
    ghost_count: int,
) -> None:
    """[ATOMIC-MIRROR-SYNC] Ghost-heal equity patch — in-memory stub.

    [FIX-SINGLE-WRITER] File read/write removed.  The ghost-healed equity
    patch is now applied directly to executor._status inside
    _on_bridge_ghost_healed_main() at the call site where executor is in
    scope.  This function is retained as a no-op stub so the create_task()
    signature remains unchanged.
    """
    log.debug(
        "[ATOMIC-MIRROR-SYNC] _patch_trader_status_equity called "
        "(no-op stub — in-memory patch applied at call site): "
        "retained_equity=%.4f  source=%s  ghost_count=%d",
        retained_equity, bridge_source, ghost_count,
    )

async def _write_runner_ready(
    ready: bool, stage: str, ready_coins: List[str], total_coins: int
) -> None:
    await _atomic_write(RUNNER_READY_PATH, {
        "timestamp":   time.time(),
        "ready":       ready,
        "stage":       stage,
        "ready_coins": ready_coins,
        "total_coins": total_coins,
    })

def _validate_credentials() -> None:
    missing = [
        v for v in ("OKX_API_KEY", "OKX_SECRET", "OKX_PASSPHRASE")
        if not os.environ.get(v, "").strip()
    ]
    if missing:
        log.critical("Missing OKX credentials: %s", ", ".join(missing))
        sys.exit(1)
    if not os.environ.get("OPENROUTER_API_KEY", "").strip():
        log.warning(
            "[P22-1] OPENROUTER_API_KEY not set — "
            "LLM scoring will fall back to keyword heuristics."
        )

async def _brain_state_saver(
    brain, path: str, interval: float = P19_STATE_SAVE_INTERVAL_SECS
) -> None:
    log.info("[P19-1] Brain state saver started (interval=%.0fs)", interval)
    while True:
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            return
        try:
            ok = brain.save_state(path=path)
            if ok:
                log.info("[P19-1] Periodic brain state save → %s", path)
        except Exception as exc:
            log.warning("[P19-1] Periodic save raised: %s", exc)

def _make_whale_callback(executor: Executor):
    async def _on_whale_signal(oracle_signal) -> None:
        sym  = oracle_signal.symbol
        mult = oracle_signal.multiplier
        log.warning("[P16-WIRE] Whale signal: %s mult=%.1f×", sym, mult)
        try:
            filled = await executor.trigger_atomic_express_trade(oracle_signal)
            if filled:
                log.warning("[P16-WIRE] Express trade CONFIRMED: %s", sym)
        except Exception as exc:
            log.error(
                "[P16-WIRE] trigger_atomic_express_trade %s: %s",
                sym, exc, exc_info=True,
            )
    return _on_whale_signal

class OrchestratorGate:
    """
    [P40.1-GATE] Phase 40.1 Bridge-ready institutional gate.

    Full gate chain:
        P22-4 PanicLock  →  P7 NewsWire  →  P15 Whale Nuke  →
        P21-1 SlippageGuard  →  P22-2 Entropy Shield  →
        P7 Arb  →  P7 Kelly Panic  →  P22-3 Regime Trail  →
        P17/P20/P22-1 Council of Judges  →  P21-2 MicroBurst
    """

    def __init__(
        self,
        executor,
        brain,
        arbitrator,
        news_wire,
        hub,
        cb,
        sizer,
        auditor,
        slippage_guard,
        microburst,
        tif_watcher,
        panic_lock:  PanicLock,
        entropy_buf: EntropyBuffer,
        veto:        Optional[LLMContextVeto] = None,
        mesh_gate:   Optional[object]         = None,   # [P53]
    ) -> None:
        self._exec    = executor
        self._brain   = brain
        self._arb     = arbitrator
        self._nw      = news_wire
        self._hub     = hub
        self._cb      = cb
        self._sizer   = sizer
        self._auditor = auditor
        self._sg      = slippage_guard
        self._mb      = microburst
        self._tif     = tif_watcher
        self._panic   = panic_lock
        self._entropy = entropy_buf
        self._veto    = veto

        self._orig_gate: object = executor.gate
        self.hub                = hub
        self.sentinel           = getattr(executor.gate, "sentinel", None)

        self._arb_cooldown: dict = {}
        self._arb_cooldown_secs = _env_float("ARB_COOLDOWN_SECS", 30.0)
        self._mesh_gate         = mesh_gate   # [P53] 2/3 consensus gate
        self._whale_nuke_warn_ts: dict = {}  # [FIX-WHALENUKE] {symbol: float}

    def install(self) -> None:
        """[P40.1-GATE] Replace executor's internal gate with this full institutional gate."""
        self._exec.gate          = self
        self._exec._p7_cb        = self._cb
        self._exec._p7_sizer     = self._sizer
        self._exec._p7_auditor   = self._auditor
        log.info(
            "[P40.1] OrchestratorGate installed as executor.gate "
            "(P6+P7+P10+P21+P22 institutional — Bridge-ready, no monkeypatch)."
        )

    async def _gated_enter(
        self, symbol, master, tick=None, oracle_signal=None, *args, **kwargs
    ):
        """[P40.1-GATE] Entry gate called by Executor._maybe_enter()."""
        direction = getattr(master, "direction", "neutral")
        bias      = await self._nw.get_macro_bias()

        if direction == "long" and self._panic.is_locked():
            log.warning(
                "[P22-4] %s LONG BLOCKED by PanicLock — %.0f s remaining.",
                symbol, self._panic.remaining_secs(),
            )
            return False

        if direction == "long" and self._nw.is_bearish_block(bias):
            log.info("[Gate] %s LONG blocked by NewsWire: score=%.3f", symbol, bias.score)
            return False

        if oracle_signal and getattr(oracle_signal, "cancel_buys_flag", False):
            # [FIX-WHALENUKE] Throttle: cancel_buys_flag fires on every oracle tick
            # (~every 2s per symbol). Without throttle this spams 1 WARNING/2s.
            import time as _wnt
            _wnow = _wnt.monotonic()
            if _wnow - self._whale_nuke_warn_ts.get(symbol, 0.0) >= 30.0:
                self._whale_nuke_warn_ts[symbol] = _wnow
                log.warning("[P15] %s Entry BLOCKED: Whale Nuke [throttled: 1/30s]", symbol)
            return False

        throttled, spread_pct = await self._sg.should_throttle(symbol)
        if throttled:
            log.warning(
                "[P21-1] %s THROTTLED: spread=%.5f%% > %.4f%%",
                symbol, spread_pct, P21_SPREAD_THROTTLE_PCT,
            )
            return False

        recent_returns = self._entropy.get_returns(symbol)
        entropy        = compute_shannon_entropy(recent_returns)
        min_conf, ent_triggered = entropy_confidence_gate(
            P22_MIN_SIGNAL_CONFIDENCE, entropy,
            threshold=P22_ENTROPY_HIGH_THRESHOLD,
            boost=P22_ENTROPY_CONFIDENCE_BOOST,
        )
        signal_conf = float(getattr(master, "confidence", 1.0))
        if ent_triggered and signal_conf < min_conf:
            log.info(
                "[P22-2] %s BLOCKED by Entropy Shield: entropy=%.4f conf=%.4f < min=%.4f",
                symbol, entropy, signal_conf, min_conf,
            )
            return False

        now     = time.time()
        cd_last = self._arb_cooldown.get(symbol, 0.0)
        if now - cd_last >= self._arb_cooldown_secs:
            try:
                opp = await self._arb.get_arbitrage_opportunity(symbol)
                if opp is not None and opp.spread_pct >= ARB_EXEC_PCT:
                    log.info(
                        "[Gate] %s: Arb spread %.4f%% — prioritising arb.",
                        symbol, opp.spread_pct,
                    )
                    await self._execute_arb_leg(symbol, opp, master)
                    self._arb_cooldown[symbol] = now
                    return False
            except Exception as exc:
                log.debug("[Gate] Arb check error %s: %s", symbol, exc)

        try:
            from phase7_risk import KELLY_PANIC_BLOCK
            if bias.score <= KELLY_PANIC_BLOCK:
                log.info(
                    "[P7] %s blocked: score=%.4f ≤ panic=%.2f",
                    symbol, bias.score, KELLY_PANIC_BLOCK,
                )
                return False
        except Exception as exc:
            log.debug("[P7] Panic gate error %s: %s", symbol, exc)

        regime    = getattr(master, "regime", "chop")
        trail_adj = regime_trail_adjustment(regime)
        if trail_adj != 0.0:
            try:
                setattr(master, "p22_trail_adj", trail_adj)
                log.debug(
                    "[P22-3] %s regime=%s trail_adj=%+.5f",
                    symbol, regime, trail_adj,
                )
            except Exception:
                pass

        narrative_tag = "P22"
        if self._veto is not None:
            try:
                result = await self._veto.score(
                    symbol         = symbol,
                    direction      = direction,
                    recent_returns = recent_returns,
                    regime         = regime,
                )
                if result is None:
                    result = _make_neutral_narrative()
                if result.verdict in ("VETO", "CATASTROPHE_VETO"):
                    log.info(
                        "[P22-1] %s BLOCKED by Council: verdict=%s score=%.4f",
                        symbol, result.verdict, result.score,
                    )
                    return False
                narrative_tag = f"P22_{result.verdict}"
            except Exception as exc:
                log.debug(
                    "[P22-1/LLMFB-3] Veto score error %s — using neutral: %s",
                    symbol, exc,
                )

        # ── [P53] 2-of-3 Mesh Consensus Gate — final check before capital commitment ──
        # check_consensus() is pure in-memory (no I/O, no await) — zero latency impact.
        # With P53_MESH_ENABLED=False it always returns approved=True (backward-safe).
        if self._mesh_gate is not None:
            try:
                _cr = self._mesh_gate.check_consensus(symbol, direction)
                if not _cr.approved:
                    log.warning(
                        "[P53] %s %s BLOCKED by mesh consensus: reason=%s "
                        "nodes=%d/3 agreeing  conf=%.2f",
                        symbol, direction.upper(),
                        _cr.blocking_reason,
                        len(_cr.agreeing_nodes),
                        _cr.confidence,
                    )
                    return False
                elif _cr.confidence < 1.0:
                    log.debug(
                        "[P53] %s %s: consensus approved with conf=%.2f "
                        "nodes=%s  latency=%.1fµs",
                        symbol, direction.upper(),
                        _cr.confidence, _cr.agreeing_nodes, _cr.latency_us,
                    )
            except Exception as _p53_exc:
                log.debug("[P53] check_consensus error (fail-closed): %s", _p53_exc)
                return False   # fail-closed: block entry on gate exception
        # ── [/P53] ─────────────────────────────────────────────────────────────────

        cfg        = self._exec._coin_cfg(symbol)
        avail      = self._exec._avail
        kelly_f    = getattr(master, "kelly_f", 0.05)
        usd_amount = max(avail * kelly_f, getattr(cfg, "min_usd_value", 1.0))

        side     = "buy"  if direction == "long"  else "sell"
        pos_side = "long" if direction == "long"  else "short"

        dispatched = await self._mb.execute(
            symbol        = symbol,
            side          = side,
            total_usd     = usd_amount,
            swap          = True,
            pos_side      = pos_side,
            tag           = f"{narrative_tag}_{direction.upper()}",
            coin_cfg      = cfg,
            oracle_signal = oracle_signal,
            tif_watcher   = self._tif,
        )

        if not dispatched:
            log.warning(
                "[P21-2] MicroBurst failed %s — yielding to native executor path.",
                symbol,
            )
            return True

        return False

    async def _execute_arb_leg(self, symbol, opp, signal) -> None:
        if symbol in self._exec.positions:
            return
        cfg        = self._exec._coin_cfg(symbol)
        avail      = self._exec._avail
        usd_amount = max(avail * 0.02, cfg.min_usd_value)
        if opp.sell_exchange == "okx" or opp.buy_exchange != "okx":
            if avail < usd_amount:
                return
            await self._exec._execute_order(
                symbol, "sell", usd_amount, swap=True, pos_side="short",
                tag=f"ARB_SELL_{opp.buy_exchange.upper()[:3]}", coin_cfg=cfg,
            )
        else:
            if avail < usd_amount:
                return
            await self._exec._execute_order(
                symbol, "buy", usd_amount, swap=False, pos_side="net",
                tag=f"ARB_BUY_{opp.sell_exchange.upper()[:3]}", coin_cfg=cfg,
            )

    async def _on_post_close(self, pos: Position, fill_px: float, tag: str) -> None:
        """[P40.1-CLOSE] Native close hook called by Executor._record_close()."""
        if pos.cost_basis <= 0:
            return

        pnl_pct = (
            (fill_px - pos.cost_basis) / pos.cost_basis * 100
            if pos.direction == "long"
            else (pos.cost_basis - fill_px) / pos.cost_basis * 100
        )

        regime = "chop"
        sig    = pos.entry_signal
        if sig is not None:
            regime = sig.regime

        await self._brain.update_weights(regime, pnl_pct)

        close_side = "sell" if pos.direction == "long" else "buy"
        self._auditor.record(
            symbol      = pos.symbol,
            expected_px = pos.cost_basis,
            actual_px   = fill_px,
            side        = close_side,
        )

# ── [STAGE-3] Binance TPS Ingestor ───────────────────────────────────────────
_BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="
_BINANCE_BACKOFF_BASE = 1.0
_BINANCE_BACKOFF_MAX  = 60.0


async def _binance_tape_ingestor(hub: "DataHub", symbols: List[str]) -> None:
    """[STAGE-3] Long-running task: subscribes to Binance Combined Public Trade Stream."""
    _log = logging.getLogger("main_p22.binance_tape")

    streams = "/".join(
        f"{sym.lower().replace('-usdt', '').replace('-swap', '')}usdt@trade"
        for sym in symbols
    )
    url = _BINANCE_WS_BASE + streams
    _log.info("[STAGE-3] Binance tape ingestor starting: %d symbols url=%s", len(symbols), url)

    backoff = _BINANCE_BACKOFF_BASE
    while True:
        try:
            timeout = aiohttp.ClientTimeout(
                total=None,
                sock_connect=15.0,
                sock_read=30.0,
            )
            async with aiohttp.ClientSession(timeout=timeout) as sess:
                async with sess.ws_connect(
                    url,
                    heartbeat=20.0,
                    max_msg_size=0,
                ) as ws:
                    _log.info("[STAGE-3] Binance WS connected.")
                    backoff = _BINANCE_BACKOFF_BASE
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                frame = json.loads(msg.data)
                                data  = frame.get("data") or {}
                                if data.get("e") != "trade":
                                    continue
                                px = float(data.get("p") or 0)
                                qty = float(data.get("q") or 0)
                                if px <= 0 or qty <= 0:
                                    continue
                                hub.record_binance_trade()
                            except (json.JSONDecodeError, ValueError, TypeError):
                                pass
                            except Exception as _exc:
                                _log.debug(
                                    "[STAGE-3] Unexpected parse error (non-fatal): %s", _exc
                                )
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR,
                            aiohttp.WSMsgType.CLOSED,
                        ):
                            _log.warning(
                                "[STAGE-3] Binance WS closed (type=%s) — will reconnect in %.1f s.",
                                msg.type, backoff,
                            )
                            break
        except asyncio.CancelledError:
            _log.info("[STAGE-3] Binance tape ingestor cancelled — exiting.")
            return
        except Exception as exc:
            _log.warning(
                "[STAGE-3] Binance WS error: %s — reconnecting in %.1f s.",
                exc, backoff,
            )

        try:
            await asyncio.sleep(backoff)
        except asyncio.CancelledError:
            _log.info("[STAGE-3] Binance tape ingestor cancelled during backoff — exiting.")
            return
        backoff = min(backoff * 2.0, _BINANCE_BACKOFF_MAX)

# ── [/STAGE-3] ────────────────────────────────────────────────────────────────

# ── [CI-2] OKX REST Candle Ingestor ──────────────────────────────────────────
_OKX_CANDLE_REST_BASE = "https://www.okx.com"

# [CI-2-A] Timeframe → OKX bar-string mapping.
# Only explicitly listed TFs produce fetches; all others are skipped with a
# throttled warning so unknown future TFs never crash the loop.
_OKX_TF_BAR_MAP: Dict[str, str] = {
    "1m":    "1m",
    "1min":  "1m",
    "5m":    "5m",
    "15m":   "15m",
    "1hour":  "1H",
    "2hour":  "2H",   # [FIX-TF-MAP] was missing — TF_ALL includes 2hour
    "4hour":  "4H",
    "8hour":  "8H",   # [FIX-TF-MAP] was missing — TF_ALL includes 8hour
    "12hour": "12H",  # [FIX-TF-MAP] was missing — TF_ALL includes 12hour
    "1day":   "1D",
    "1week":  "1W",   # [FIX-TF-MAP] was missing — TF_ALL includes 1week
}

# Per-TF base poll interval (seconds).  Staggered fetch cadence keeps rate
# consumption low while keeping the DB fresh enough for the brain.
_OKX_TF_INTERVAL_SECS: Dict[str, float] = {
    "1m":    25.0,
    "1min":  25.0,
    "5m":    60.0,
    "15m":   90.0,
    "1hour":  180.0,
    "2hour":  360.0,   # [FIX-TF-MAP] added
    "4hour":  300.0,
    "8hour":  600.0,   # [FIX-TF-MAP] added
    "12hour": 900.0,   # [FIX-TF-MAP] added
    "1day":   900.0,
    "1week":  3600.0,  # [FIX-TF-MAP] added
}
_OKX_CI2_LIMIT        = 300         # candles per REST call (OKX max=300)
_OKX_CI2_BACKOFF_BASE = 2.0         # seconds for first backoff
_OKX_CI2_BACKOFF_MAX  = 60.0        # hard cap per (symbol, tf) backoff
_OKX_CI2_STAGGER      = 0.25        # inter-request stagger inside one sweep (s)
_OKX_CI2_WARN_THROTTLE = 120.0      # seconds between repeated TF-skip warnings


async def _okx_candle_ingestor_loop(
    hub:        "DataHub",
    symbols:    List[str],
    timeframes: List[str],
) -> None:
    """
    [CI-2] Long-running task: fetches OHLCV history from OKX public REST and
    upserts into the canonical DB via hub.db.upsert_candles() (batch) with a
    fallback to hub.db.upsert_candle() per-row if batch method is absent.

    One aiohttp.ClientSession is reused for the lifetime of the task.
    Per-(symbol, tf) exponential backoff is applied on any network/parse error.
    The loop never raises — all failures are caught, logged (throttled), and
    backed off.
    """
    _log = logging.getLogger("main_p22.okx_candle_ci2")
    _log.info(
        "[CI-2] OKX candle ingestor starting: symbols=%s  tfs=%s",
        symbols, timeframes,
    )

    # ── filter to supported TFs only; warn once per unknown TF ───────────────
    _tf_skip_warned: Dict[str, float] = {}
    active_tfs: List[str] = []
    for tf in timeframes:
        if tf in _OKX_TF_BAR_MAP:
            active_tfs.append(tf)
        else:
            _tf_skip_warned[tf] = 0.0  # will log on first sweep

    if not active_tfs:
        _log.warning(
            "[CI-2] No supported timeframes in %s — ingestor idle.", timeframes
        )
        return

    # Per-(symbol, tf) backoff state: float seconds, reset to base on success.
    _backoff: Dict[Tuple[str, str], float] = {}
    # [FIX-CI2-THROTTLE] Per-(sym,tf) last-fetch timestamp. Prevents refetching
    # a 1day candle every 180s just because 1hour is the minimum sweep interval.
    _last_fetch: Dict[Tuple[str, str], float] = {}

    # ── upsert helper: prefer batch, fallback to single ──────────────────────
    _has_batch: Optional[bool] = None

    async def _upsert(candles: List[Candle]) -> None:
        nonlocal _has_batch
        if not candles:
            return
        db = hub.db
        if _has_batch is None:
            _has_batch = callable(getattr(db, "upsert_candles", None))
        if _has_batch:
            await db.upsert_candles(candles)
        else:
            for c in candles:
                await db.upsert_candle(c)

    timeout = aiohttp.ClientTimeout(total=20.0, sock_connect=10.0, sock_read=15.0)
    try:
        async with aiohttp.ClientSession(
            base_url=_OKX_CANDLE_REST_BASE,
            timeout=timeout,
        ) as session:

            # ── Stagger first requests so all (sym, tf) don't fire at t=0 ───
            _stagger_offset = 0.0

            while True:
                sweep_start = time.monotonic()

                for sym in list(symbols):
                    for tf in active_tfs:
                        # ── unknown-TF throttled warning ──────────────────
                        # (active_tfs already filtered; this block covers TFs
                        #  that might appear via a dynamic symbols_ref mutation
                        #  but is a belt-and-suspenders guard.)
                        if tf not in _OKX_TF_BAR_MAP:
                            now_ts = time.time()
                            last_w = _tf_skip_warned.get(tf, 0.0)
                            if now_ts - last_w >= _OKX_CI2_WARN_THROTTLE:
                                _log.warning(
                                    "[CI-2] Unknown/unmapped TF '%s' — skipping. "
                                    "(Throttled: next warning in %.0f s)",
                                    tf, _OKX_CI2_WARN_THROTTLE,
                                )
                                _tf_skip_warned[tf] = now_ts
                            continue

                        key      = (sym, tf)
                        backoff  = _backoff.get(key, 0.0)
                        interval = _OKX_TF_INTERVAL_SECS.get(tf, 300.0)

                        # [FIX-CI2-THROTTLE] Skip if this (sym,tf) was fetched
                        # recently enough. First sweep always runs (no entry yet).
                        _now = time.time()
                        if _now - _last_fetch.get(key, 0.0) < interval:
                            continue

                        # ── per-(sym, tf) stagger on first boot ───────────
                        if _stagger_offset > 0.0:
                            try:
                                await asyncio.sleep(_stagger_offset)
                            except asyncio.CancelledError:
                                _log.info("[CI-2] Ingestor cancelled during initial stagger.")
                                return
                            _stagger_offset = 0.0   # only applies once per key

                        # ── apply backoff delay if a previous attempt failed ─
                        if backoff > 0.0:
                            try:
                                await asyncio.sleep(backoff)
                            except asyncio.CancelledError:
                                _log.info("[CI-2] Ingestor cancelled during backoff.")
                                return

                        bar      = _OKX_TF_BAR_MAP[tf]
                        inst_id  = f"{sym}-USDT"
                        params   = {
                            "instId": inst_id,
                            "bar":    bar,
                            "limit":  str(_OKX_CI2_LIMIT),
                        }

                        try:
                            async with session.get(
                                "/api/v5/market/candles",
                                params=params,
                            ) as resp:
                                if resp.status != 200:
                                    raise aiohttp.ClientResponseError(
                                        resp.request_info,
                                        resp.history,
                                        status=resp.status,
                                        message=f"HTTP {resp.status}",
                                    )
                                raw = await resp.json(content_type=None)

                            # ── parse + build Candle list ─────────────────
                            data_rows = raw.get("data") or []
                            if not data_rows:
                                _log.debug(
                                    "[CI-2] %s/%s: empty data in response.", sym, tf
                                )
                                _backoff[key] = 0.0
                                await asyncio.sleep(_OKX_CI2_STAGGER)
                                continue

                            candles: List[Candle] = []
                            for row in data_rows:
                                try:
                                    # OKX row: [ts_ms, o, h, l, c, vol, ...]
                                    ts_ms  = int(row[0])
                                    ts_sec = ts_ms // 1000
                                    candle = Candle(
                                        symbol    = sym,
                                        tf        = tf,
                                        ts        = ts_sec,
                                        open      = float(row[1]),
                                        high      = float(row[2]),
                                        low       = float(row[3]),
                                        close     = float(row[4]),
                                        volume    = float(row[5]),
                                        confirmed = True,
                                    )
                                    candles.append(candle)
                                except (IndexError, ValueError, TypeError) as _pe:
                                    _log.debug(
                                        "[CI-2] %s/%s row parse error: %s row=%s",
                                        sym, tf, _pe, row,
                                    )
                                    continue

                            if candles:
                                await _upsert(candles)
                                _last_fetch[key] = time.time()  # [FIX-CI2-THROTTLE]
                                _log.debug(
                                    "[CI-2] upserted %d candles  sym=%s  tf=%s  "
                                    "ts_range=[%d, %d]",
                                    len(candles), sym, tf,
                                    candles[-1].ts, candles[0].ts,
                                )

                            # success → reset backoff for this key
                            _backoff[key] = 0.0

                        except asyncio.CancelledError:
                            _log.info(
                                "[CI-2] Ingestor cancelled during fetch %s/%s.", sym, tf
                            )
                            return

                        except Exception as fetch_exc:
                            new_bo = min(
                                max(_backoff.get(key, 0.0) * 2.0, _OKX_CI2_BACKOFF_BASE),
                                _OKX_CI2_BACKOFF_MAX,
                            )
                            _backoff[key] = new_bo
                            _log.warning(
                                "[CI-2] Fetch error %s/%s: %s — backoff=%.1fs",
                                sym, tf, fetch_exc, new_bo,
                            )

                        # ── inter-request stagger ─────────────────────────
                        try:
                            await asyncio.sleep(_OKX_CI2_STAGGER)
                        except asyncio.CancelledError:
                            _log.info("[CI-2] Ingestor cancelled during stagger.")
                            return

                        # ── advance stagger offset for next (sym, tf) pair ─
                        _stagger_offset = _OKX_CI2_STAGGER

                # ── wait until the next sweep interval ───────────────────────
                # Use the shortest active interval so we don't miss fast TFs,
                # but each (sym, tf) pair effectively self-throttles via its
                # own per-key backoff + the poll interval embedded in the outer
                # sleep.  Sweep-level sleep is the minimum active interval.
                min_interval = min(
                    (_OKX_TF_INTERVAL_SECS.get(t, 300.0) for t in active_tfs),
                    default=180.0,
                )
                elapsed = time.monotonic() - sweep_start
                sleep_for = max(0.0, min_interval - elapsed)
                try:
                    await asyncio.sleep(sleep_for)
                except asyncio.CancelledError:
                    _log.info("[CI-2] OKX candle ingestor cancelled — exiting.")
                    return

    except asyncio.CancelledError:
        _log.info("[CI-2] OKX candle ingestor cancelled at session level — exiting.")
        return
    except Exception as outer_exc:
        _log.error(
            "[CI-2] OKX candle ingestor fatal outer error (non-crashing): %s",
            outer_exc, exc_info=True,
        )

# ── [/CI-2] ───────────────────────────────────────────────────────────────────

async def _gui_bridge(
    executor,
    brain,
    arbitrator,
    news_wire,
    symbols,
    cb,
    sizer,
    auditor,
    governor,
    sentinel,
    veto=None,
    p21_monitor=None,
    panic_lock: Optional[PanicLock] = None,
    entropy_buf: Optional[EntropyBuffer] = None,
    bridge=None,
    mesh_gate: Optional["MeshConsensusGate"] = None,   # [P53]
    interval: float = 2.0,
) -> None:
    # ── [S7C-P21-GUARANTEE] Producer-side P21 export state ────────────────────
    # Both variables are defined at function scope BEFORE the while loop so they
    # survive across every iteration.  They are NEVER re-initialised inside the
    # loop, guaranteeing LKG continuity across cycles.
    #
    #   p21_lkg          — last dict returned by a successful p21_monitor.snapshot()
    #                      call; None only until the very first successful call.
    #   p21_last_err_ts  — monotonic timestamp of the most recent throttled WARNING
    #                      log for snapshot() errors; 0.0 at boot.
    #
    # Export rules (applied every cycle, in order):
    #   A) p21_monitor is None  → _p21_disabled_stub()            (p21_enabled=False)
    #   B) snapshot() succeeds and returns a dict
    #                           → validate + export + update p21_lkg
    #   C) snapshot() returns non-dict OR raises
    #      C1) p21_lkg is populated → export p21_lkg copy
    #      C2) p21_lkg is None (boot race) → _p21_enabled_stub()  (p21_enabled=True)
    #
    # status["p21_execution"] is ALWAYS a dict — the key is NEVER omitted.
    # ─────────────────────────────────────────────────────────────────────────
    p21_lkg: Optional[dict] = None
    p21_last_err_ts: float = 0.0

    # [P52] Supervisor telemetry — stamped once at coroutine start so uptime
    # is accurate across the full lifetime of this _gui_bridge invocation.
    _p52_boot_ts: float   = time.time()
    _p52_cycle_cnt: int   = 0

    # [P53] Last timestamp at which OKX P47 data was fed into the mesh gate.
    _p53_last_feed_ts: float = 0.0

    while True:
        try:
            _p52_cycle_cnt += 1   # [P52] monotonic loop counter
            status     = dict(executor._status)
            if not isinstance(status.get("analytics"), dict):
                status["analytics"] = (status.get("analytics") or {})
            if not isinstance(status.get("positions"), dict):
                status["positions"] = (status.get("positions") or {})
            if not isinstance(status.get("symbols"), dict):
                status["symbols"] = {}
            brain_data = {}

            for sym in symbols:
                signals: dict = {}
                for tf in ["1hour", "4hour", "1day"]:
                    try:
                        candles = await executor.hub.get_candles(sym, tf, 100)
                        sig     = brain.analyze(sym, tf, candles)
                        if sig:
                            signals[tf] = {
                                "direction":  sig.direction,
                                "prob":       sig.prob,
                                "regime":     sig.regime,
                                "kelly_f":    sig.kelly_f,
                                "z_score":    sig.z_score,
                                "confidence": sig.confidence,
                            }
                    except Exception:
                        pass
                brain_data[sym] = signals

            status["brain"]              = brain_data
            status["correlation_frozen"] = brain.correlation_freeze()

            # [P49] Publish brain state to Rust bridge for P50 native migration.
            # Builds regime_map from the 1hour signals computed above.
            # Fire-and-forget coroutine — non-fatal if bridge is unavailable.
            if bridge is not None and hasattr(bridge, "publish_brain_state"):
                async def _p49_publish_brain(
                    _bridge=bridge,
                    _eq=float(getattr(bridge, "equity", 0.0)),
                    _pos=len(getattr(executor, "_open_orders", {})),
                    _syms=list(symbols),
                    _bdata=brain_data,
                    _congested=getattr(bridge, "_ipc_congested", False),
                    _ts=time.time(),
                ) -> None:
                    try:
                        _regime_map: dict = {}
                        for _sym, _sigs in _bdata.items():
                            _tf1h = _sigs.get("1hour", {})
                            if _tf1h:
                                _regime_map[_sym] = str(_tf1h.get("regime", "neutral")).lower()
                        await _bridge.publish_brain_state(
                            equity       = _eq,
                            positions    = _pos,
                            symbols      = _syms,
                            regime_map   = _regime_map,
                            cycle_ts     = _ts,
                            is_congested = _congested,
                        )
                    except Exception as _p49_pub_exc:
                        log.debug("[P49] publish_brain_state error: %s", _p49_pub_exc)
                asyncio.ensure_future(_p49_publish_brain())

            bias = await news_wire.get_macro_bias()
            status["agents"] = {
                "news_wire": {
                    "macro_score":    bias.score,
                    "label":          bias.label,
                    "source":         bias.source,
                    "top_headlines":  bias.headlines[:3],
                    "bearish_block":  bias.score < NEWS_BLOCK_THRESHOLD,
                    "age_secs":       round(time.time() - bias.ts, 1),
                    "llm_enriched":   bias.llm_enriched,
                    "llm_error":      bias.llm_error,
                    "llm_latency_ms": bias.llm_latency_ms,
                    # [T28-FINBERT] FinBERT-style local scorer metadata
                    "finbert_score":  bias.finbert_score,
                    "finbert_used":   bias.finbert_used,
                },
                "arbitrator": {"reachable_exchanges": arbitrator.reachable_exchanges()},
                "rl_brain":   await brain.get_regime_weights(),
            }

            arb_snapshot: dict = {}
            for sym in symbols:
                try:
                    opp = await arbitrator.get_arbitrage_opportunity(sym)
                    if opp:
                        arb_snapshot[sym] = {
                            "spread_pct":    opp.spread_pct,
                            "buy_exchange":  opp.buy_exchange,
                            "sell_exchange": opp.sell_exchange,
                        }
                except Exception:
                    pass
            status["agents"]["arbitrator"]["opportunities"] = arb_snapshot

            status["p7_cb_drawdown"] = {
                "drawdown_pct":  cb.last_drawdown_pct,
                "peak_equity":   cb.peak_equity,
                "latest_equity": cb.latest_equity,
                "tripped":       cb.is_tripped,
                "tripped_at":    cb.tripped_at,
            }

            try:
                scale = await sizer.get_scale_info(
                    base_kelly=0.05, max_alloc=0.20,
                    min_alloc=0.005, equity=executor._equity or 1.0,
                )
                status["p7_kelly_scale"] = {
                    "macro_score": scale.raw_macro_score,
                    "multiplier":  scale.multiplier,
                    "alloc":       scale.adjusted_alloc,
                    "blocked":     scale.blocked,
                    "label":       scale.label,
                }
            except Exception as exc:
                log.debug("[GUI] KellySizer error: %s", exc)

            status["p7_slippage"] = auditor.report()

            try:
                spread_mgr = getattr(executor, "_p7_spread_mgr", None)
                if spread_mgr is not None:
                    status["p7_spread"] = {s: spread_mgr.stats(s) for s in symbols}
            except Exception:
                pass

            if governor is not None:
                try:
                    status["p10_governor"]  = governor.status_snapshot()
                    status["p10_exec_mode"] = governor.execution_mode.value
                except Exception:
                    pass

            if sentinel is not None:
                try:
                    status["p11_sentinel"] = sentinel.status_snapshot()
                except Exception:
                    pass

            status["p16_bridge_active"]       = executor.p16_bridge_active
            status["p16_express_fills_total"] = executor._p16_express_fills
            status["p16_last_express_by_sym"] = {
                sym: round(time.time() - ts, 1)
                for sym, ts in executor._p16_last_express.items()
            }

            if veto is not None:
                try:
                    p17_snap = veto.status_snapshot()
                    status["p17_intelligence"] = p17_snap
                    status["p18_adaptive_conviction"] = {
                        "dynamic_sizing_enabled": os.environ.get("P18_DYNAMIC_SIZING", "1") == "1",
                        "latest_multipliers": {
                            r["symbol"]: r.get("conviction_multiplier")
                            for r in p17_snap.get("recent_results", [])
                            if r.get("conviction_multiplier") is not None
                        },
                    }
                except Exception as exc:
                    log.debug("[P17/P18] GUI error: %s", exc)

            status["p19_state_save_interval_secs"] = P19_STATE_SAVE_INTERVAL_SECS

            p20_risk_snap: dict = {}
            if executor._p20_risk_manager is not None:
                try:
                    p20_risk_snap = executor._p20_risk_manager.status_snapshot()
                except Exception:
                    pass

            hwm          = brain.peak_equity
            cur_equity   = executor._equity or 0.0

            _has_open_pos = executor.has_open_positions()
            if _has_open_pos and cur_equity <= 1.0:
                _retained = executor._last_valid_equity or cur_equity
                log.warning(
                    "[SHIELD-REJECT] _gui_bridge: Open positions detected but "
                    "cur_equity=%.6f ≤ 1.0 — rejecting as Ghost Read.  "
                    "Retaining _last_valid_equity=%.6f for Dashboard write.",
                    cur_equity, _retained,
                )
                cur_equity = _retained

            _bridge_ghost_active = (
                getattr(bridge, "_equity_is_ghost", False)
                or getattr(executor, "_p40_ghost_healing", False)
            )
            _is_ghost_equity = cur_equity <= 1.0 and _bridge_ghost_active

            if _is_ghost_equity:
                drawdown_pct = (hwm - brain.peak_equity) / hwm * 100.0 if hwm > 0 else 0.0
                log.warning(
                    "[P40.1-SHIELD] Ghost equity detected in _gui_bridge "
                    "(cur_equity=%.4f ≤ 1.0, bridge_ghost=%s, healing=%s) — "
                    "freezing peak_equity=%.4f, skipping drawdown update.",
                    cur_equity,
                    getattr(bridge, "_equity_is_ghost", False),
                    getattr(executor, "_p40_ghost_healing", False),
                    hwm,
                )
            else:
                drawdown_pct = (hwm - cur_equity) / hwm * 100.0 if hwm > 0 else 0.0

            _zombie_raw = executor._p20_zombie_mode
            if _is_ghost_equity and _zombie_raw:
                executor._p20_zombie_mode = False
                log.warning(
                    "[P40.1-SHIELD] Zombie Mode VETOED in _gui_bridge — "
                    "bridge confirmed ghost equity (%.4f), overriding zombie flag.",
                    cur_equity,
                )
            _grace_until = getattr(executor, "_p20_grace_until", 0.0)
            _in_grace    = time.time() < _grace_until

            try:
                shadow_hmm_metrics = brain.shadow_train_snapshot()
            except Exception:
                shadow_hmm_metrics = {}

            status["p20_global_risk"] = {
                **p20_risk_snap,
                "global_peak_equity":   round(hwm, 2),
                "current_equity":       round(cur_equity, 2),
                "drawdown_pct":         round(drawdown_pct, 4),
                "zombie_mode_status":   executor._p20_zombie_mode,
                "zombie_pct_threshold": P20_DRAWDOWN_ZOMBIE_PCT,
                "in_grace_period":      _in_grace,
                "grace_until_ts":       round(_grace_until, 1),
                "grace_remaining_secs": round(max(0.0, _grace_until - time.time()), 1),
                # [P44-DASH] Kelly fraction for dashboard render_p44_supervisor current bar
                "kelly_fraction": float(os.environ.get("KELLY_FRACTION", "0.20")),
            }
            status["p20_zombie_mode"]        = executor._p20_zombie_mode
            status["p20_shadow_hmm_metrics"] = shadow_hmm_metrics
            # [P44-DASH] kelly_fraction as a flat top-level key — render_p44_supervisor
            # reads status.get("kelly_fraction") before the nested p20_global_risk fallback.
            status["kelly_fraction"] = float(os.environ.get("KELLY_FRACTION", "0.20"))

            # ── [P21-DASH-FIX] Proactive spread refresh ───────────────────────────────
            # Without this, DynamicSlippageGuard._cache is only populated when an
            # order is placed.  Idle periods keep latest_spreads={} and the dashboard
            # P21-1 panel shows "AWAITING SPREAD DATA" indefinitely.
            # Refresh every 3 _gui_bridge cycles (~6 s) — enough to keep the dashboard
            # current without hammering the OKX BBO endpoint.  refresh_all() is a
            # read-only probe: throttle/pass counters are NOT incremented.
            if p21_monitor is not None and (_p52_cycle_cnt % 3 == 1):
                try:
                    await p21_monitor._sg.refresh_all(list(symbols))
                except Exception as _rfr_exc:
                    log.debug("[P21-DASH] spread refresh_all error: %s", _rfr_exc)
            # ── [/P21-DASH-FIX] ──────────────────────────────────────────────────────

            # ── [S7C-P21-GUARANTEE] p21_execution — ALWAYS exported every cycle ──────
            #
            # This block guarantees status["p21_execution"] is a non-None dict on
            # every single write to trader_status.json regardless of:
            #   • p21_monitor being None (P21 disabled via --no-p21)
            #   • snapshot() raising any exception
            #   • snapshot() returning a non-dict value
            #   • boot timing (no samples available yet)
            #
            # Resolution order:
            #   A) Monitor absent  → disabled stub  {p21_enabled: False, ...}
            #   B) Snapshot valid  → validated live snapshot + update LKG
            #   C) Snapshot failed, LKG available → copy of last good snapshot
            #   D) Snapshot failed, no LKG yet    → enabled stub  {p21_enabled: True, _synthesised: True, ...}
            #
            # Snapshot errors are WARNING-logged at most once per
            # _P21_SNAP_ERR_THROTTLE_SECS (30 s) to prevent log spam.
            # ──────────────────────────────────────────────────────────────────────────
            if p21_monitor is None:
                # Case A: P21 fully disabled — emit stable disabled stub.
                status["p21_execution"] = _p21_disabled_stub()
            else:
                _raw_snap: Optional[dict] = None
                try:
                    _candidate = p21_monitor.snapshot()
                    if isinstance(_candidate, dict):
                        # Case B: valid live snapshot.
                        _raw_snap = _p21_validate_snap(_candidate)
                    else:
                        # snapshot() returned unexpected type — treat as failure.
                        _now_ts = time.time()
                        if (_now_ts - p21_last_err_ts) >= _P21_SNAP_ERR_THROTTLE_SECS:
                            log.warning(
                                "[S7C-P21-GUARANTEE] p21_monitor.snapshot() returned "
                                "non-dict type %s — falling back to LKG or enabled stub. "
                                "(Throttled: next log in %.0f s.)",
                                type(_candidate).__name__,
                                _P21_SNAP_ERR_THROTTLE_SECS,
                            )
                            p21_last_err_ts = _now_ts
                except Exception as _snap_exc:
                    _now_ts = time.time()
                    if (_now_ts - p21_last_err_ts) >= _P21_SNAP_ERR_THROTTLE_SECS:
                        log.warning(
                            "[S7C-P21-GUARANTEE] p21_monitor.snapshot() raised an "
                            "exception — falling back to LKG or enabled stub. "
                            "Error: %s  (Throttled: next log in %.0f s.)",
                            _snap_exc,
                            _P21_SNAP_ERR_THROTTLE_SECS,
                            exc_info=True,
                        )
                        p21_last_err_ts = _now_ts

                if _raw_snap is not None:
                    # Case B confirmed: store as LKG and export.
                    p21_lkg = dict(_raw_snap)
                    status["p21_execution"] = _raw_snap
                elif isinstance(p21_lkg, dict) and p21_lkg:
                    # Case C: snapshot failed but LKG is available — export copy.
                    status["p21_execution"] = dict(p21_lkg)
                else:
                    # Case D: snapshot failed AND no LKG yet (boot race) — enabled stub.
                    status["p21_execution"] = _p21_enabled_stub()
            # ── [/S7C-P21-GUARANTEE] ─────────────────────────────────────────────────

            p22_snap: dict = {
                "openrouter_model":         OPENROUTER_MODEL,
                "openrouter_active":        bool(os.environ.get("OPENROUTER_API_KEY")),
                "entropy_threshold":        P22_ENTROPY_HIGH_THRESHOLD,
                "entropy_confidence_boost": P22_ENTROPY_CONFIDENCE_BOOST,
                "bull_trail_widen":         P22_BULL_TRAIL_WIDEN,
                "chop_trail_tighten":       P22_CHOP_TRAIL_TIGHTEN,
                "panic_lock":               panic_lock.status_snapshot() if panic_lock else {},
            }
            if entropy_buf is not None:
                entropy_per_coin: dict = {}
                for sym in symbols:
                    rets = entropy_buf.get_returns(sym)
                    entropy_per_coin[sym] = {
                        "entropy":   round(compute_shannon_entropy(rets), 4),
                        "n_returns": len(rets),
                    }
                p22_snap["entropy_per_coin"] = entropy_per_coin
            status["p22"] = p22_snap

            status["strategy_mode"] = getattr(executor, "_strategy_mode", "🤖 AI_PREMIUM")

            status["equity_is_ghost"]         = getattr(executor, "_equity_is_ghost",         False)
            status["consecutive_ghost_reads"]  = getattr(executor, "_consecutive_ghost_reads",  0)

            # [P40.5] IPC RTT telemetry — written on every status cycle so the
            # dashboard always has current bridge latency and congestion state.
            try:
                if bridge is not None and hasattr(bridge, "heartbeat_snapshot"):
                    status["p40_5_heartbeat"] = bridge.heartbeat_snapshot()
                else:
                    status["p40_5_heartbeat"] = {"enabled": False}
            except Exception as _hb_exc:
                log.debug("[P40.5] heartbeat_snapshot error: %s", _hb_exc)

            # [P47] Rust Microstructure snapshot — per-symbol VPIN + OFI state,
            # source (bridge vs python), freshness, and lifetime update counter.
            try:
                if bridge is not None and hasattr(bridge, "p47_snapshot"):
                    status["p47_microstructure"] = bridge.p47_snapshot()
                else:
                    status["p47_microstructure"] = {"enabled": False}
            except Exception as _p47s_exc:
                log.debug("[P47] p47_snapshot error: %s", _p47s_exc)

            # [P48] Autonomous Floating snapshot — active strategic intents and
            # per-symbol Rust floating state (current_px, spread_bps, tick_count).
            try:
                if bridge is not None and hasattr(bridge, "p48_snapshot"):
                    status["p48_floating"] = bridge.p48_snapshot()
                else:
                    status["p48_floating"] = {"enabled": False}
            except Exception as _p48s_exc:
                log.debug("[P48] p48_snapshot error: %s", _p48s_exc)

            # [P49] Hybrid Bridge Core snapshot — framing mode, capability
            # handshake state, intent_v2 active flag, brain state counters.
            try:
                if bridge is not None and hasattr(bridge, "p49_snapshot"):
                    status["p49_protocol"] = bridge.p49_snapshot()
                else:
                    status["p49_protocol"] = {"enabled": False}
            except Exception as _p49s_exc:
                log.debug("[P49] p49_snapshot error: %s", _p49s_exc)

            # [P50] Native Hot-Path snapshot — shadow agreement rate, latency
            # ladder, fallback count, last execution_decision received from Rust.
            try:
                if bridge is not None and hasattr(bridge, "p50_snapshot"):
                    status["p50_native"] = bridge.p50_snapshot()
                else:
                    status["p50_native"] = {"enabled": False}
            except Exception as _p50s_exc:
                log.debug("[P50] p50_snapshot error: %s", _p50s_exc)

            # [P51] Institutional Execution Suite snapshot — active TWAP/VWAP/
            # Iceberg algos, slice fill history, completion log, shadow agreement.
            try:
                if bridge is not None and hasattr(bridge, "p51_snapshot"):
                    status["p51_algo"] = bridge.p51_snapshot()
                else:
                    status["p51_algo"] = {"enabled": False}
            except Exception as _p51s_exc:
                log.debug("[P51] p51_snapshot error: %s", _p51s_exc)

            # [P52] Supervisor identity + health — written every cycle so the
            # dashboard and watchdog always have a fresh liveness signal.
            # Keys are stable and never removed (fail-closed defaults to False).
            try:
                _p52_now       = time.time()
                _p52_bridge_ok = (
                    bridge is not None
                    and getattr(bridge, "connected", False)
                ) if bridge is not None else False
                status["p52_supervisor"] = {
                    "enabled":       True,
                    "role":          _P52_SUPERVISOR_ROLE,     # "trading_engine"
                    "version":       _P52_SUPERVISOR_VERSION,  # "52.0"
                    "pid":           os.getpid(),
                    "boot_ts":       _p52_boot_ts,
                    "uptime_secs":   round(_p52_now - _p52_boot_ts, 1),
                    "loop_cycles":   _p52_cycle_cnt,
                    "last_cycle_ts": round(_p52_now, 3),
                    "bridge_healthy": bool(_p52_bridge_ok),
                    "demo_mode":     os.environ.get("OKX_DEMO_MODE", "0") == "1",
                    "active_symbols": list(symbols),
                }
            except Exception as _p52_exc:
                log.debug("[P52] p52_supervisor block error: %s", _p52_exc)
                status["p52_supervisor"] = {"enabled": False}

            # ── [P53] Mesh Consensus Gate — feed + snapshot ───────────────────────────
            # Feed OKX P47 VPIN data + brain signals into the mesh gate every
            # _P53_FEED_INTERVAL_SECS.  Then write p53_mesh snapshot to status so the
            # dashboard panel (Part 2) always has current node + consensus state.
            # Fail-closed: any exception → p53_mesh = {"enabled": False}.
            try:
                if mesh_gate is not None:
                    _now_p53 = time.time()
                    # Feed at interval (or every cycle if interval <= 0)
                    if _p53_last_feed_ts == 0.0 or (_now_p53 - _p53_last_feed_ts) >= _P53_FEED_INTERVAL_SECS:
                        _p53_last_feed_ts = _now_p53  # update feed timestamp

                        # ── OKX node: bridge P47 VPIN + brain direction ───────────────
                        try:
                            _p47 = status.get("p47_microstructure", {})
                            _p47_syms = _p47.get("symbols", {}) if isinstance(_p47, dict) else {}
                            for _sym in symbols:
                                _sym_key = f"{_sym}-USDT"
                                _p47_data = _p47_syms.get(_sym_key, {})
                                _vpin = float(_p47_data.get("vpin_toxicity", 0.3))
                                # Direction from brain 1h signal
                                _brain_1h = brain_data.get(_sym, {}).get("1hour", {})
                                _raw_dir  = str(_brain_1h.get("direction", "neutral")).lower()
                                _prob     = float(_brain_1h.get("prob", 0.5))
                                _conf     = float(_brain_1h.get("confidence", 0.5))
                                mesh_gate.feed_okx_vpin(
                                    symbol        = _sym,
                                    vpin_toxicity = _vpin,
                                    direction     = _raw_dir,
                                    confidence    = max(0.5, _conf),
                                )
                        except Exception as _f_okx:
                            log.debug("[P53] OKX feed error: %s", _f_okx)

                        # ── Binance node: tape volume trend as proxy signal ────────────
                        # We use the Binance tape aggregator from GlobalTapeAggregator
                        # if available, otherwise skip — never fabricate.
                        try:
                            _gtape = getattr(executor.hub, "global_tape", None)
                            for _sym in symbols:
                                _tap = None
                                if _gtape is not None:
                                    _tap_fn = getattr(_gtape, "get_binance_pressure", None)
                                    if callable(_tap_fn):
                                        _tap = _tap_fn(_sym)
                                if _tap is not None:
                                    _bn_dir  = str(getattr(_tap, "direction", "neutral")).lower()
                                    _bn_conf = float(getattr(_tap, "confidence", 0.6))
                                    _bn_vpin = float(getattr(_tap, "vpin_estimate", 0.3))
                                    mesh_gate.feed_binance_signal(
                                        symbol        = _sym,
                                        direction     = _bn_dir,
                                        vpin_estimate = _bn_vpin,
                                        confidence    = _bn_conf,
                                    )
                        except Exception as _f_bn:
                            log.debug("[P53] Binance feed error: %s", _f_bn)

                        # ── Coinbase node: oracle signal → direction + confidence ───────
                        try:
                            for _sym in symbols:
                                _osig = executor.hub.get_oracle_signal(_sym)
                                if _osig is None or not _osig.is_valid():
                                    continue
                                _cb_dir  = str(getattr(_osig, "signal_type", "neutral")).lower()
                                _cb_mult = float(getattr(_osig, "multiplier", 1.0))
                                _cb_conf = min(0.5 + (_cb_mult - 1.0) * 0.1, 0.95)
                                # Coinbase has no direct VPIN; estimate from spread if available
                                _cb_vpin = float(getattr(_osig, "vpin_estimate", 0.3))
                                mesh_gate.feed_coinbase_signal(
                                    symbol        = _sym,
                                    direction     = _cb_dir,
                                    vpin_estimate = _cb_vpin,
                                    confidence    = max(0.55, _cb_conf),
                                )
                        except Exception as _f_cb:
                            log.debug("[P53] Coinbase feed error: %s", _f_cb)

                    # Always write p53 snapshot every cycle
                    status["p53_mesh"] = mesh_gate.status_snapshot()
                else:
                    status["p53_mesh"] = {"enabled": False, "reason": "mesh_gate_not_initialised"}
            except Exception as _p53_exc:
                log.debug("[P53] mesh feed/snapshot error: %s", _p53_exc)
                status["p53_mesh"] = {"enabled": False}
            # ── [/P53] ───────────────────────────────────────────────────────────────

            # [P54] Kernel-bypass telemetry snapshot — sourced from bridge p54_stats
            # events.  Fail-closed: always writes p54_kernel_bypass even on error.
            try:
                if bridge is not None and hasattr(bridge, "p54_snapshot"):
                    status["p54_kernel_bypass"] = bridge.p54_snapshot()
                else:
                    status["p54_kernel_bypass"] = {"enabled": False,
                                                    "reason": "bridge_no_p54_snapshot"}
            except Exception as _p54s_exc:
                log.debug("[P54] p54_snapshot error: %s", _p54s_exc)
                status["p54_kernel_bypass"] = {"enabled": False}
            # ── [/P54] ───────────────────────────────────────────────────────────────

            _p17_recents  = status.get("p17_intelligence", {}).get("recent_results", [])
            _ai_lat_ms    = None
            if _p17_recents:
                _ai_lat_ms = _p17_recents[0].get("latency_ms")
            _p21_exec      = status.get("p21_execution", {})
            _exch_lat_ms   = _p21_exec.get("p21_slippage_guard", {}).get("last_roundtrip_ms")
            _nw_lat_ms     = status.get("agents", {}).get("news_wire", {}).get("llm_latency_ms")
            status["p22_latency_bar"] = {
                "ai_latency_ms":       round(_ai_lat_ms,   1) if _ai_lat_ms   is not None else None,
                "exchange_latency_ms": round(_exch_lat_ms, 1) if _exch_lat_ms is not None else None,
                "newswire_latency_ms": round(_nw_lat_ms,   1) if _nw_lat_ms   is not None else None,
            }

            _whale_tape: list = []
            for _sym in symbols:
                try:
                    _osig = executor.hub.get_oracle_signal(_sym)
                    if _osig is None:
                        continue
                    _trade_size = getattr(_osig, "trade_size", 0.0) or 0.0
                    if _trade_size <= 0:
                        continue
                    _whale_tape.append({
                        "symbol":     _sym,
                        "size_usd":   round(float(_trade_size), 2),
                        "signal":     getattr(_osig, "signal_type", "—"),
                        "multiplier": round(float(getattr(_osig, "multiplier", 1.0)), 3),
                        "valid":      bool(_osig.is_valid()),
                        "cancel_buys": bool(getattr(_osig, "cancel_buys_flag", False)),
                    })
                except Exception:
                    pass
            _whale_tape.sort(key=lambda x: x["size_usd"], reverse=True)
            status["p22_whale_tape"] = _whale_tape[:15]

            # [ATOMIC-MIRROR-SYNC] retained_equity mirrors total_equity every write.
            try:
                _acct = status.get("account")
                if isinstance(_acct, dict):
                    if "total_equity" in _acct:
                        _acct["retained_equity"] = _acct["total_equity"]
                    else:
                        _acct["retained_equity"] = round(
                            getattr(executor, "_equity", None) or 0.0, 2
                        )
            except Exception:
                pass

            try:
                if not isinstance(status.get("analytics"), dict):
                    status["analytics"] = (status.get("analytics") or {})
                if not isinstance(status.get("symbols"), dict):
                    status["symbols"] = {}
            except Exception:
                pass

            # [FIX-SINGLE-WRITER] executor._write_status_json() is now the sole
            # writer of trader_status.json.  Instead of writing the file here,
            # merge the fully-enriched status dict directly into executor._status.
            # The next executor cycle-end _write_status_json() call carries all
            # supervisor-enriched keys (brain, p21, p52, p54, etc.) to disk.
            # executor._cycle() preserves these keys across its own self._status
            # dict replacement via the _carried_supervisor carry-forward mechanism.
            #
            # The pre-write ATOMIC-MIRROR-SYNC block above (retained_equity patch)
            # is kept — it still modifies the shared account sub-dict correctly and
            # costs nothing.  The null-shields on analytics/symbols are also kept
            # as defensive guards that are harmless to leave in place.
            try:
                executor._status.update(status)
            except Exception as exc:
                log.debug("[GUI] executor._status merge error (non-fatal): %s", exc)

        except Exception as exc:
            log.debug("[GUI] Bridge error: %s", exc, exc_info=True)

        await asyncio.sleep(interval)

async def _settings_watcher(
    executor, brain, symbols_ref: List[str], interval: float = 10.0
) -> None:
    last_mtime = 0.0
    while True:
        try:
            mtime = os.path.getmtime(GUI_SETTINGS)
            if mtime != last_mtime:
                last_mtime = mtime
                new_coins  = load_coins_from_settings()
                for c in [c for c in new_coins if c not in symbols_ref]:
                    symbols_ref.append(c)
                    if c not in executor.symbols:
                        executor.symbols.append(c)
                for c in [c for c in symbols_ref if c not in new_coins]:
                    symbols_ref.remove(c)
                    if c in executor.symbols:
                        executor.symbols.remove(c)
        except Exception:
            pass
        await asyncio.sleep(interval)

async def _on_new_candle(
    candle,
    hub,
    brain,
    panic_lock: PanicLock,
    entropy_buf: EntropyBuffer,
) -> None:
    if not candle.confirmed:
        return
    try:
        sym     = candle.symbol.replace("-USDT", "").replace("-SWAP", "")
        candles = await hub.get_candles(sym, candle.tf, 5)
        if len(candles) >= 2:
            c_now, c_prev = candles[0], candles[1]
            brain.train(
                sym, candle.tf,
                opens  = np.array([c_prev.open,  c_now.open],  dtype=np.float64),
                closes = np.array([c_prev.close, c_now.close], dtype=np.float64),
                highs  = np.array([c_prev.high,  c_now.high],  dtype=np.float64),
                lows   = np.array([c_prev.low,   c_now.low],   dtype=np.float64),
            )
        close_price = candle.close if hasattr(candle, "close") else 0.0
        if close_price > 0:
            entropy_buf.record(sym, close_price)
            await panic_lock.record_price(sym, close_price)
    except Exception as exc:
        log.debug("_on_new_candle error %s %s: %s", candle.symbol, candle.tf, exc)

async def run_training(coins: List[str], brain) -> None:
    from trainer import train
    log.info("=== TRAINING PHASE ===")
    await _write_runner_ready(False, "training", [], len(coins))
    await train(coins, brain)
    await _write_runner_ready(False, "training_complete", coins, len(coins))
    log.info("=== TRAINING COMPLETE ===")

_shutdown_event = asyncio.Event()

def _handle_signal(*_) -> None:
    log.info("Shutdown signal received — setting shutdown event.")
    _shutdown_event.set()

async def _teardown(
    tif_watcher,
    oracle,
    governor,
    sentinel,
    brain,
    brain_state_path: str,
    hub,
    arb,
    news_wire,
    p21_monitor,
    panic_lock: PanicLock,
    no_p21: bool,
    bridge: Optional["BridgeClient"] = None,
    mesh_gate: Optional["MeshConsensusGate"] = None,   # [P53]
) -> None:
    """[SHUTDOWN-4] All component teardown in one coroutine."""
    # [P53] Stop mesh consensus gate first (lightweight — just cancels NTP task)
    if mesh_gate is not None:
        try:
            await mesh_gate.stop()
        except Exception as exc:
            log.debug("[P53] mesh_gate.stop() error (non-fatal): %s", exc)
    if not no_p21:
        try:
            await tif_watcher.stop()
        except Exception as exc:
            log.warning("[P21-3] tif_watcher.stop(): %s", exc)

    try:
        await oracle.stop()
    except Exception:
        pass

    try:
        _oclose = getattr(oracle, "close", None)
        if callable(_oclose):
            await _oclose()
    except Exception as exc:
        log.debug("[P40.1-TEARDOWN] oracle.close(): %s", exc)

    try:
        _osess = getattr(oracle, "_session", None) or getattr(oracle, "session", None)
        if _osess is not None and not getattr(_osess, "closed", True):
            await _osess.close()
            log.debug("[P40.1-TEARDOWN] Oracle aiohttp session closed.")
    except Exception as exc:
        log.debug("[P40.1-TEARDOWN] Oracle session close: %s", exc)

    if governor is not None:
        try:
            await governor.stop()
        except Exception:
            pass

    if sentinel is not None:
        try:
            await sentinel.stop()
        except Exception:
            pass

    try:
        await brain.stop_background_tasks()
    except Exception:
        pass

    if p21_monitor is not None:
        try:
            snap = p21_monitor.snapshot()
            log.info(
                "[P21] Final: spread_throttles=%d bursts=%d tif_cancels=%d",
                snap["p21_slippage_guard"]["throttle_count"],
                snap["p21_microburst"]["burst_count"],
                snap["p21_tif_watcher"]["cancel_count"],
            )
        except Exception:
            pass

    log.info(
        "[P22] PanicLock triggered %d time(s). Currently locked=%s",
        panic_lock._trigger_count, panic_lock.is_locked(),
    )

    try:
        brain.save_state(path=brain_state_path)
        log.info("[P19-1] Final brain state saved → %s", brain_state_path)
    except Exception as exc:
        log.warning("[P19-1] Final save raised: %s", exc)

    try:
        await hub.close()
    except Exception:
        pass

    try:
        rest_client = getattr(hub, "rest", None)
        if rest_client is not None:
            rest_session = getattr(rest_client, "_session", None) or getattr(rest_client, "session", None)
            if rest_session is not None and not getattr(rest_session, "closed", True):
                await rest_session.close()
                log.debug("[SHUTDOWN-4-FIX] DataHub REST aiohttp session closed.")
        hub_session = getattr(hub, "_session", None) or getattr(hub, "session", None)
        if hub_session is not None and not getattr(hub_session, "closed", True):
            await hub_session.close()
            log.debug("[SHUTDOWN-4-FIX] DataHub aiohttp session closed.")
    except Exception as exc:
        log.debug("[SHUTDOWN-4-FIX] aiohttp session close: %s", exc)

    try:
        _HUB_TASK_NAMES = frozenset({"okx_market_feed", "okx_private_feed"})
        current = asyncio.current_task()
        for task in asyncio.all_tasks():
            if task is current or task.done():
                continue
            tname = task.get_name()
            hub_tasks = getattr(hub, "_tasks", None)
            if tname in _HUB_TASK_NAMES or (hub_tasks and task in hub_tasks):
                task.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
                log.debug("[SHUTDOWN-4-FIX] Cancelled DataHub task: %s", tname)
    except Exception as exc:
        log.debug("[SHUTDOWN-4-FIX] Task cancellation sweep: %s", exc)

    try:
        await arb.close()
    except Exception:
        pass

    try:
        _arb_sess = getattr(arb, "_session", None) or getattr(arb, "session", None)
        if _arb_sess is not None and not getattr(_arb_sess, "closed", True):
            await _arb_sess.close()
            log.debug("[SHUTDOWN-4-FIX] Arbitrator aiohttp session closed.")
        _arb_http = getattr(arb, "http", None) or getattr(arb, "_http", None)
        if _arb_http is not None:
            _arb_http_sess = getattr(_arb_http, "_session", None) or getattr(_arb_http, "session", None)
            if _arb_http_sess is not None and not getattr(_arb_http_sess, "closed", True):
                await _arb_http_sess.close()
                log.debug("[SHUTDOWN-4-FIX] Arbitrator HTTP aiohttp session closed.")
    except Exception as exc:
        log.debug("[SHUTDOWN-4-FIX] Arbitrator session close: %s", exc)

    try:
        await news_wire.close()
    except Exception:
        pass

    try:
        _nwsess = getattr(news_wire, "_session", None) or getattr(news_wire, "session", None)
        if _nwsess is not None and not getattr(_nwsess, "closed", True):
            await _nwsess.close()
            log.debug("[SHUTDOWN-4-FIX] NewsWire aiohttp session closed.")
    except Exception as exc:
        log.debug("[SHUTDOWN-4-FIX] NewsWire session close: %s", exc)

    if bridge is not None:
        try:
            log.info("[P40] Closing Rust bridge connection…")
            await bridge.close()
            try:
                _bsess = getattr(bridge, "_session", None) or getattr(bridge, "session", None)
                if _bsess is not None and not getattr(_bsess, "closed", True):
                    await _bsess.close()
                    log.debug("[P40.1-TEARDOWN] Bridge aiohttp session closed.")
            except Exception as exc:
                log.debug("[P40.1-TEARDOWN] Bridge session close: %s", exc)
            log.info("[P40] Bridge closed.")
        except Exception as exc:
            log.warning("[P40] Bridge close error (non-fatal): %s", exc)

    log.info("PowerTrader Phase 40 stopped cleanly.")


async def async_main(args: "argparse.Namespace") -> int:
    """
    [P52] Trading engine supervisor entry point.
    args: pre-parsed argparse.Namespace from main.py (all trading flags).
    Returns 0 on clean shutdown, 2 on forced/error shutdown.
    """
    # [P52] Return int; formerly main() returned None.
    # Argparse is handled by main.py; args are passed in.


    coins = [
        c.upper().strip()
        for c in (args.coins or load_coins_from_settings())
        if c.strip()
    ]
    log.info("Active coins: %s", coins)

    if args.drawdown_limit is not None:
        os.environ["MAX_DRAWDOWN_PCT"] = str(args.drawdown_limit)
    if args.no_p18:
        os.environ["P18_DYNAMIC_SIZING"] = "0"

    brain_state_path = args.brain_state or BRAIN_STATE_PATH
    demo_mode        = os.environ.get("OKX_DEMO_MODE", "0").strip() == "1"
    if demo_mode:
        log.info("⚠️  DEMO / PAPER TRADING MODE")
    else:
        log.warning("🔴 LIVE TRADING MODE")

    if platform.system() == "Windows":
        try:
            import psutil
            psutil.Process().nice(psutil.HIGH_PRIORITY_CLASS)
        except Exception:
            pass

    _validate_credentials()
    os.makedirs(HUB_DIR, exist_ok=True)
    await _write_runner_ready(False, "starting", [], len(coins))

    # ── Core agents ────────────────────────────────────────────────────────────
    brain = IntelligenceEngine(coins)
    hub   = DataHub(coins, TF_ALL, demo=demo_mode)

    # ── [S6.1-DBREADY / S7A-DBREADY] DB ensure-ready ─────────────────────────
    try:
        _db_path = hub.db._path
        log.info("[S7A-DBREADY] Ensuring DB is ready (absolute path): %s", _db_path)
        _loop = asyncio.get_event_loop()
        await _loop.run_in_executor(None, hub.db._conn_obj)
        log.info("[S7A-DBREADY] DB initialized and schema applied: %s", _db_path)
    except Exception as _db_exc:
        log.error(
            "[S7A-DBREADY] FATAL: DB initialization failed for %s — %s. "
            "Cannot start without a working database.",
            hub.db._path, _db_exc,
        )
        raise
    # ── [/S7A-DBREADY] ───────────────────────────────────────────────────────

    # ── [P40] Phase 40: Logic Execution Bridge ────────────────────────────────
    bridge = BridgeClient(auto_start_bridge=not args.no_bridge)

    async def _on_bridge_account_update_main(msg: dict) -> None:
        eq       = float(msg.get("eq", 0.0))
        is_ghost = bool(msg.get("is_ghost", False))
        if is_ghost or eq <= 0.0:
            return
        _exec = executor
        if _exec is not None:
            if _exec.has_open_positions() and eq <= 1.0:
                _retained = getattr(_exec, '_last_valid_equity', None) or eq
                log.warning(
                    "[SHIELD-REJECT] _on_bridge_account_update_main: "
                    "open positions present but bridge eq=%.6f ≤ 1.0 — "
                    "Ghost Read rejected.  _last_valid_equity=%.6f retained.",
                    eq, _retained,
                )
                return
        if brain.peak_equity <= 0 or eq > brain.peak_equity:
            brain.peak_equity = eq
        log.debug("[P40][MAIN] Bridge account_update: eq=%.4f → brain.peak_equity=%.4f", eq, brain.peak_equity)
        # [FIX-SINGLE-WRITER] Update timestamp in-memory; cycle-end write carries it to disk.
        if executor._status:
            executor._status["timestamp"] = time.time()

    async def _on_bridge_equity_breach_main(msg: dict) -> None:
        log.critical(
            "[P40][MAIN] Bridge equity_breach: confirmed_eq=%.4f  message=%s  "
            "CircuitBreaker will trip on next cycle.",
            float(msg.get("confirmed_eq", 0.0)),
            msg.get("message", ""),
        )

    async def _on_bridge_ghost_healed_main(msg: dict) -> None:
        raw_ws_eq   = float(msg.get("raw_ws_eq",  0.0))
        healed_eq   = float(msg.get("healed_eq",  0.0))
        source      = msg.get("source", "unknown")
        ghost_count = int(msg.get("ghost_count", 0))

        log.warning(
            "[P40.1-HEAL] GHOST HEALED by Rust bridge: "
            "raw_ws_eq=%.4f  healed_eq=%.4f  source=%s  ghost_count=%d  "
            "→ Python: applying Binary Truth, NO emergency shutdown.",
            raw_ws_eq, healed_eq, source, ghost_count,
        )

        if healed_eq > 0.0:
            executor._equity = healed_eq
            log.info(
                "[ATOMIC-MIRROR-SYNC] executor._equity updated: %.4f → %.4f (retained_equity)",
                raw_ws_eq, healed_eq,
            )

        executor._p40_ghost_healing = False

        if healed_eq > 0.0 and (brain.peak_equity <= 0 or healed_eq > brain.peak_equity):
            brain.peak_equity = healed_eq
            log.info("[P40.1-HEAL] brain.peak_equity updated to healed value: %.4f", healed_eq)

        if getattr(executor, "_p20_zombie_mode", False):
            executor._p20_zombie_mode = False
            log.warning(
                "[P40.1-SHIELD] Zombie Mode VETOED — bridge confirmed $%.2f "
                "was a ghost read (raw_ws_eq=%.4f), not a real equity breach.",
                raw_ws_eq, raw_ws_eq,
            )

        retained_equity = healed_eq if healed_eq > 0.0 else (executor._equity or 0.0)

        # [FIX-SINGLE-WRITER] Patch executor._status in-memory directly.
        # executor is in closure scope here.  The cycle-end _write_status_json()
        # carries these mutations to disk within ≤ 1 s.
        try:
            _now = time.time()
            if executor._status:
                executor._status["timestamp"]               = _now
                executor._status["equity_is_ghost"]         = False
                executor._status["consecutive_ghost_reads"] = 0
                executor._status["p40_ghost_heal_source"]   = source
                executor._status["p40_ghost_heal_count"]    = ghost_count
                executor._status["p40_last_heal_ts"]        = _now
                executor._status["zombie_mode"]             = False
                executor._status["p20_zombie_mode"]         = False
                if isinstance(executor._status.get("account"), dict):
                    executor._status["account"]["total_equity"]    = retained_equity
                    executor._status["account"]["retained_equity"] = retained_equity
                if isinstance(executor._status.get("p20_global_risk"), dict):
                    executor._status["p20_global_risk"]["zombie_mode_status"] = False
                    executor._status["p20_global_risk"]["current_equity"]     = round(retained_equity, 2)
        except Exception as _patch_exc:
            log.warning("[ATOMIC-MIRROR-SYNC] in-memory ghost_healed patch error: %s", _patch_exc)

        log.info(
            "[ATOMIC-MIRROR-SYNC] retained_equity=%.4f patched into executor._status "
            "(consistent with executor._equity=%.4f)",
            retained_equity, executor._equity,
        )

    async def _on_bridge_fill_main(msg: dict) -> None:
        log.debug(
            "[P40][MAIN] Bridge order_fill: ordId=%s  fillPx=%s  fillSz=%s",
            msg.get("ordId"), msg.get("fillPx"), msg.get("fillSz"),
        )
        # [FIX-SINGLE-WRITER] Update timestamp in-memory; cycle-end write carries it to disk.
        if executor._status:
            executor._status["timestamp"] = time.time()

    # ── [P40.5] IPC RTT / Heartbeat event handlers ────────────────────────────
    # These fire from BridgeClient._heartbeat_loop() — NOT from the Rust bridge
    # wire. They are synthetic Python events driven by measured ping→pong RTT.

    async def _on_ipc_congestion(msg: dict) -> None:
        """[P40.5] RTT exceeded warn/critical threshold → Telegram alert."""
        rtt       = msg.get("rtt_ms", 0.0)
        severity  = msg.get("severity", "warning")
        threshold = msg.get("threshold", 10.0)
        icon      = "🔴" if severity == "critical" else "🟡"
        log.warning(
            "[P40.5] IPC congestion event: severity=%s  rtt=%.3f ms  threshold=%.0f ms",
            severity, rtt, threshold,
        )
        await _tg_send(
            f"{icon} <b>P40.5 IPC Congestion ({severity.upper()})</b>\n"
            f"Bridge RTT: <code>{rtt:.3f} ms</code> "
            f"≥ <code>{threshold:.0f} ms</code> threshold\n"
            "<i>Investigate process load or IPC socket pressure. "
            "Adjust BRIDGE_RTT_WARN_MS if benign.</i>"
        )

    async def _on_ipc_standdown(msg: dict) -> None:
        """[P40.5] Consecutive ping timeouts → stand-down Telegram alert."""
        n_timeouts = msg.get("consecutive_timeouts", 0)
        threshold  = msg.get("standdown_threshold", 3)
        log.critical(
            "[P40.5] IPC STAND-DOWN: %d consecutive ping timeouts (threshold=%d) "
            "— bridge may be deadlocked or overloaded.",
            n_timeouts, threshold,
        )
        await _tg_send(
            "🛑 <b>P40.5 IPC STAND-DOWN — Bridge Unresponsive</b>\n"
            f"<code>{n_timeouts}</code> consecutive ping timeouts "
            f"(threshold: <code>{threshold}</code>)\n"
            "New order submissions may fail. "
            "Check Rust bridge process and IPC socket health."
        )

    async def _on_ipc_recovered(msg: dict) -> None:
        """[P40.5] RTT recovered below warn threshold → recovery Telegram alert."""
        rtt = msg.get("rtt_ms", 0.0)
        log.info("[P40.5] IPC RTT recovered: %.3f ms — congestion cleared.", rtt)
        await _tg_send(
            "✅ <b>P40.5 IPC Link Recovered</b>\n"
            f"Bridge RTT: <code>{rtt:.3f} ms</code> — within normal range.\n"
            "Order submissions restored."
        )
    # ── [/P40.5] ─────────────────────────────────────────────────────────────

    executor = Executor(hub, brain, list(coins), demo=demo_mode, bridge=bridge)
    hub._risk_executor = executor

    bridge.on("account_update", _on_bridge_account_update_main)
    bridge.on("equity_breach",  _on_bridge_equity_breach_main)
    bridge.on("ghost_healed",   _on_bridge_ghost_healed_main)
    bridge.on("order_fill",     _on_bridge_fill_main)
    # [P40.5] IPC RTT / Heartbeat — synthetic events from _heartbeat_loop()
    bridge.on("ipc_congestion", _on_ipc_congestion)
    bridge.on("ipc_standdown",  _on_ipc_standdown)
    bridge.on("ipc_recovered",  _on_ipc_recovered)
    # [P47] Inject bridge reference into DataHub so get_flow_toxicity() /
    # get_ofi_score() can prefer Rust-computed values when fresh.
    hub._p47_bridge = bridge
    # Tell BridgeClient which symbols to subscribe for on connect.
    bridge._p47_tracked_symbols = [f"{c}-USDT" for c in coins]
    # ── [/P40] ────────────────────────────────────────────────────────────────

    arb = Arbitrator(coins)
    if not args.no_arb:
        await arb.start()

    arb.set_bridge(bridge)

    news_wire = NewsWire()
    if not args.no_news:
        await news_wire.start()

    # ── [P7] Phase 7 risk + execution helpers ─────────────────────────────────
    cb         = GracedCircuitBreaker(hub.db, TRADER_STATUS_PATH,
                                      grace_secs=P20_COLD_START_GRACE_SECS)
    sizer      = KellySizer(news_wire)
    spread_mgr = AdaptiveSpread(instrument_cache=hub.instrument_cache)
    twap       = TWAPSlicer(hub, executor)
    auditor    = SlippageAuditor()

    hub._risk_cb            = cb
    executor._p7_cb         = cb
    executor._p7_sizer      = sizer
    executor._p7_spread_mgr = spread_mgr
    executor._p7_twap       = twap
    executor._p7_auditor    = auditor

    # ── [P21] Institutional Execution Layer ───────────────────────────────────
    p21_monitor: Optional[P21ExecutionMonitor] = None

    if not args.no_p21:
        slippage_guard = DynamicSlippageGuard(hub, P21_SPREAD_THROTTLE_PCT,
                                              P21_SPREAD_CACHE_TTL)
        microburst     = MicroBurstExecutor(executor, P21_MICROBURST_LEGS,
                                            P21_MICROBURST_DELAY_SECS,
                                            P21_LOW_LIQUIDITY_THRESHOLD)
        tif_watcher    = TIFWatcher(hub, P21_TIF_TIMEOUT_SECS, P21_TIF_POLL_SECS)
        p21_monitor    = P21ExecutionMonitor(slippage_guard, microburst, tif_watcher)
        log.info("[P21] Institutional Execution Layer ACTIVE")
    else:
        slippage_guard = DynamicSlippageGuard(hub, throttle_pct=999.0)
        microburst     = MicroBurstExecutor(executor, legs=1, liq_threshold=0.0)
        tif_watcher    = TIFWatcher(hub, timeout_secs=P21_TIF_TIMEOUT_SECS)
        log.info("[P21] Institutional Layer DISABLED (stubs active)")

    # ── [P22-4] Systemic Panic Lock ───────────────────────────────────────────
    panic_lock = PanicLock(
        symbols      = coins,
        drop_pct     = P22_PANIC_DROP_PCT,
        quorum_pct   = P22_PANIC_QUORUM_PCT,
        window_secs  = P22_PANIC_WINDOW_SECS,
        block_secs   = P22_PANIC_BLOCK_SECS,
        persist_path = PANIC_LOCK_PATH,
    )
    if args.no_p22_panic:
        log.info("[P22-4] PanicLock DISABLED via --no-p22-panic")

    # ── [P22-2] Entropy buffer ────────────────────────────────────────────────
    entropy_buf = EntropyBuffer(window_secs=P22_PANIC_WINDOW_SECS)
    if args.no_p22_entropy:
        os.environ["P22_ENTROPY_CONFIDENCE_BOOST"] = "0.0"
        log.info("[P22-2] Entropy Shield DISABLED via --no-p22-entropy")

    # ── [P53] Multi-Node Mesh Consensus Gate ─────────────────────────────────
    mesh_gate = MeshConsensusGate(symbols=list(coins), enabled=P53_ENABLED)
    log.info(
        "[P53] MeshConsensusGate configured — enabled=%s  coins=%s",
        P53_ENABLED, coins,
    )
    # ── [/P53] ───────────────────────────────────────────────────────────────

    # ── [P10] Portfolio Governor ──────────────────────────────────────────────
    governor: Optional[PortfolioGovernor] = None
    if not args.no_governor:
        governor = PortfolioGovernor(
            hub=hub, executor=executor, news_wire=news_wire, symbols=coins
        )
        executor._p10_governor = governor

    # ── [P17/P18/P20-2/P22-1] Council of Judges + OpenRouter ─────────────────
    _p17_veto: Optional[LLMContextVeto] = None
    if not args.no_p17:
        try:
            _p17_scraper = IntelligenceScraper(
                mode=os.environ.get("P17_SCRAPER_MODE", "mock"),
                feed_url=os.environ.get("P17_SCRAPER_FEED_URL", ""),
            )
            _p17_veto = LLMContextVeto(
                scraper=_p17_scraper,
                hub=hub,
                brain=brain,
                executor=executor,
            )
            _p17_veto.set_hub(hub)
            _p17_veto.set_brain_and_executor(brain, executor)
            executor._p17_veto = _p17_veto
            log.info(
                "[P17/P20-2/P22-1] Council of Judges ACTIVE — OpenRouter model=%s",
                OPENROUTER_MODEL,
            )
        except Exception as exc:
            log.warning(
                "[P17/LLMFB-3] LLMContextVeto init failed — "
                "running TA-only: %s", exc,
            )
            _p17_veto = None

    # ── OrchestratorGate ──────────────────────────────────────────────────────
    gate = OrchestratorGate(
        executor       = executor,
        brain          = brain,
        arbitrator     = arb,
        news_wire      = news_wire,
        hub            = hub,
        cb             = cb,
        sizer          = sizer,
        auditor        = auditor,
        slippage_guard = slippage_guard,
        microburst     = microburst,
        tif_watcher    = tif_watcher,
        panic_lock     = panic_lock,
        entropy_buf    = entropy_buf,
        veto           = _p17_veto if not args.no_p17 else None,
        mesh_gate      = mesh_gate,   # [P53]
    )
    gate.install()

    # ── Brain state load or initial training ──────────────────────────────────
    state_loaded = False
    if not args.train_first:
        try:
            state_loaded = brain.load_state(path=brain_state_path)
        except Exception as exc:
            log.warning("[P19-1] brain.load_state() raised: %s", exc)

    await hub.start()

    if hasattr(hub, "oracle"):
        brain.oracle = hub.oracle
        arb.oracle   = hub.oracle

    hub.global_tape = GlobalTapeAggregator(oracle=hub.oracle, arbitrator=arb)

    oracle: Optional[CoinbaseOracle] = getattr(hub, "oracle", None)
    if oracle is None:
        oracle = CoinbaseOracle(coins)
        hub.oracle = oracle

    await oracle.start()

    # ── [P53] Start mesh consensus gate (NTP sync + background tasks) ────────
    await mesh_gate.start()
    log.info("[P53] MeshConsensusGate started — NTP sync initialised.")

    if not args.no_p16_bridge:
        whale_cb = _make_whale_callback(executor)
        oracle.register_whale_callback(whale_cb)
        executor.p16_bridge_active = True
        log.warning("[P16-WIRE] Express Lane ACTIVE")

    log.info("DataHub started — warming (5 s)…")
    await asyncio.sleep(5)

    for _coin in coins:
        try:
            await spread_mgr.ensure_tick_size_resolved(_coin)
        except Exception as exc:
            log.debug("[P7] tick-size resolve %s: %s", _coin, exc)

    hub.subscribe(
        "candle",
        lambda c: asyncio.create_task(
            _on_new_candle(c, hub, brain, panic_lock, entropy_buf)
        ),
    )

    await _write_runner_ready(False, "warming_up", [], len(coins))

    if not state_loaded:
        await run_training(coins, brain)
        try:
            brain.save_state(path=brain_state_path)
        except Exception as exc:
            log.warning("[P19-1] Post-training save: %s", exc)

    if governor is not None:
        await governor.start()

    sentinel: Optional[Sentinel] = None
    if not args.no_sentinel:
        sentinel = Sentinel(executors=[executor], governor=governor, hub=hub)
        executor._p11_sentinel = sentinel
        # [P40.5] Give sentinel a direct ref to the bridge so heartbeat_snapshot()
        # is available inside status_snapshot() without a circular import.
        sentinel._bridge_client = bridge
        await sentinel.start()

    if not args.no_p21:
        await tif_watcher.start()

    # ── [P20-1] GracedGlobalRiskManager + REST balance-fetch fallback ─────────
    _p20_risk_manager = None
    if not args.no_p20_zombie:
        _p20_risk_manager = GracedGlobalRiskManager(
            executor   = executor,
            brain      = brain,
            zombie_pct = P20_DRAWDOWN_ZOMBIE_PCT,
            grace_secs = P20_COLD_START_GRACE_SECS,
        )
        executor._p20_risk_manager = _p20_risk_manager
        _grace_until = time.time() + P20_COLD_START_GRACE_SECS
        executor._p20_grace_until  = _grace_until

        if brain.peak_equity <= 0:
            if executor._equity and executor._equity > 0:
                brain.peak_equity = executor._equity
            else:
                try:
                    raw = await hub.rest.get(
                        "/api/v5/account/balance", params={"ccy": "USDT"}
                    )
                    for detail in (raw.get("data") or [{}])[0].get("details") or []:
                        if detail.get("ccy") == "USDT":
                            val = float(detail.get("eq") or detail.get("cashBal") or 0)
                            if val > 0:
                                brain.peak_equity = val
                                executor._equity  = val
                                log.info(
                                    "[P20-1] REST balance seed: peak_equity=%.2f", val
                                )
                            break
                except Exception as exc:
                    log.warning("[P20-1] REST balance fetch failed: %s", exc)
    else:
        executor._p20_grace_until = 0.0

    # ── [P20-3] Shadow HMM candle source ─────────────────────────────────────
    async def _candle_source_fn(symbol, tf, n):
        return await hub.get_candles(symbol, tf, n)

    brain.set_candle_source(_candle_source_fn)
    brain.start_background_tasks()

    # ── Signal handlers ────────────────────────────────────────────────────────
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, _handle_signal)

    symbols_ref = list(coins)

    # ── Task roster ───────────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(bridge.connect(), name="p40_bridge_connect"),
        asyncio.create_task(executor.run(interval=1.0), name="executor"),
        asyncio.create_task(
            _gui_bridge(
                executor, brain, arb, news_wire, symbols_ref,
                cb, sizer, auditor, governor, sentinel,
                veto        = _p17_veto,
                p21_monitor = p21_monitor,
                panic_lock  = panic_lock,
                entropy_buf = entropy_buf,
                bridge      = bridge,
                mesh_gate   = mesh_gate,   # [P53]
            ),
            name="gui_bridge",
        ),
        asyncio.create_task(
            _settings_watcher(executor, brain, symbols_ref),
            name="settings_watcher",
        ),
        asyncio.create_task(cb.monitor_loop(),   name="p7_circuit_breaker"),
        asyncio.create_task(oracle.run(),         name="p16_coinbase_oracle_ws"),
        asyncio.create_task(
            _brain_state_saver(brain, brain_state_path, P19_STATE_SAVE_INTERVAL_SECS),
            name="p19_brain_state_saver",
        ),
    ]

    if not args.no_arb and arb.reachable_exchanges():
        tasks.append(asyncio.create_task(arb.poll_loop(), name="arbitrator_poll"))

    if not args.no_news:
        tasks.append(
            asyncio.create_task(news_wire.refresh_loop(), name="news_wire_refresh")
        )

    tasks.append(
        asyncio.create_task(
            _binance_tape_ingestor(hub, list(symbols_ref)),
            name="p15_binance_tape_ingestor",
        )
    )
    log.info("[STAGE-3] Binance tape ingestor task registered for %s", symbols_ref)

    # ── [CI-2] OKX REST candle ingestor ──────────────────────────────────────
    tasks.append(
        asyncio.create_task(
            _okx_candle_ingestor_loop(hub, list(symbols_ref), TF_ALL),
            name="ci2_okx_candle_ingestor",
        )
    )
    log.info(
        "[CI-2] OKX candle ingestor task registered — symbols=%s  tfs=%s",
        symbols_ref, TF_ALL,
    )

    log.info(
        "🚀 PowerTrader Phase 53 LIVE.  "
        "demo=%s  coins=%s  "
        "p21=%s  p22_panic=%s  p22_entropy=%s  p22_trail=True  "
        "p24_ghost_defense=True  p24_entropy_seed=True  "
        "p40_bridge=True  p40_whale_express=True  "
        "p53_mesh=%s  "
        "openrouter_model=%s",
        demo_mode, coins,
        not args.no_p21,
        not args.no_p22_panic,
        not args.no_p22_entropy,
        P53_ENABLED,
        OPENROUTER_MODEL,
    )
    await _write_runner_ready(True, "real_predictions", coins, len(coins))

    # ── Main await ────────────────────────────────────────────────────────────
    shutdown_task = asyncio.create_task(_shutdown_event.wait(), name="shutdown")
    done, pending = await asyncio.wait(
        tasks + [shutdown_task], return_when=asyncio.FIRST_COMPLETED
    )

    for t in done:
        if t is not shutdown_task and not t.cancelled():
            exc = t.exception()
            if exc:
                log.error("Task '%s' crashed: %s", t.get_name(), exc, exc_info=exc)

    log.info("Shutting down — cancelling %d pending tasks…", len(pending))
    for t in pending:
        t.cancel()
    for t in pending:
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass

    # ── [SHUTDOWN-4] Timed teardown ───────────────────────────────────────────
    try:
        await asyncio.wait_for(
            _teardown(
                tif_watcher      = tif_watcher,
                oracle           = oracle,
                governor         = governor,
                sentinel         = sentinel,
                brain            = brain,
                brain_state_path = brain_state_path,
                hub              = hub,
                arb              = arb,
                news_wire        = news_wire,
                p21_monitor      = p21_monitor,
                panic_lock       = panic_lock,
                no_p21           = args.no_p21,
                bridge           = bridge,
                mesh_gate        = mesh_gate,   # [P53]
            ),
            timeout=SHUTDOWN_TIMEOUT_SECS,
        )
    except asyncio.TimeoutError:
        log.critical(
            "[SHUTDOWN-4] TEARDOWN TIMED OUT after %.0f s — "
            "forcing sys.exit(2) so watchdog can restart.",
            SHUTDOWN_TIMEOUT_SECS,
        )
        return 2

    return 0

