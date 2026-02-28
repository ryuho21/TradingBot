"""
main.py  —  Phase 40.1: Binary Truth Synchronization

Refactor changelog (Phase 40.1 — Binary Truth Synchronization):
  [P40.1-RG]    Ready-Gate Fix — executor._on_bridge_account_update() and
                executor._on_bridge_ghost_healed() now call
                self._p40_ready_gate.set() after accepting valid Binary Truth
                equity from the Rust bridge.  Previously the bridge path
                NEVER opened the ready-gate, so _cycle() blocked trading
                permanently with bridge active.  Fix is applied in executor.py.

  [P40.1-HEAL]  Ghost-Heal Listener — _on_bridge_ghost_healed_main now
                immediately applies the Rust bridge's healed_eq to
                executor._equity, brain.peak_equity, and writes an
                atomic patch to trader_status.json so the Dashboard
                reads the correct equity within milliseconds of the
                heal event rather than waiting for the 2-second
                _gui_bridge cycle.

  [P40.1-HB]    Heartbeat Reset — _on_bridge_account_update_main,
                _on_bridge_fill_main, and _on_bridge_ghost_healed_main
                all call _touch_hub_timestamp() so every valid bridge
                message refreshes the trader_status.json timestamp and
                stops the Watchdog from declaring STALE data on a
                healthy bot.

  [P40.1-SHIELD] Zero-Value Shield (Anti-Zombie) — _gui_bridge carries
                a strict guard: if the reported equity is ≤ 1.0 AND
                the bridge has flagged it as a ghost read, peak_equity
                and the drawdown calculation are FROZEN (not updated).
                Additionally, if the bridge confirms a ghost-state is
                currently being healed, executor._p20_zombie_mode is
                explicitly forced to False so the "Zombie Mode" UI
                indicator is never triggered by phantom equity.

  [P40.1-CLI]   --no-bridge flag — explicitly added to argparse so
                Safe Mode (legacy REST/WS, no Rust binary) can be
                activated at the command line without relying on the
                hasattr() guard.  BridgeClient(auto_start_bridge=False)
                is the canonical Safe Mode path.

  [P40.1-RETAIN] Consistency — retained_equity logged by the ghost-heal
                handler exactly matches the value written to the
                Dashboard, eliminating the UI/Logic discrepancy.

  All Phase 24.1 and earlier changes preserved verbatim.

Refactor changelog (Phase 24.1 — Hard-Reset):

  [P24.1-PATH]  Explicit HUB_DIR Guarantee — os.makedirs(HUB_DIR, exist_ok=True)
                is called immediately after BASE_DIR/HUB_DIR are defined, before
                any path-derived constants are used.  This ensures the hub_data/
                directory always exists even on a clean container first boot or
                after an accidental directory deletion.  Complements the existing
                _preseed_hub_state() atomic seeding.

  All Phase 24 path unification and pre-seed logic preserved verbatim.

Refactor changelog (Phase 24 — Systemic Defense):
  [P24-PATH-1]  Path Unification — HUB_DIR is hard-coded as a sub-folder of
                the script's directory.  os.environ overwrite of POWERTRADER_HUB_DIR
                is removed.  A single source of truth block at the top of the
                module defines ALL hub paths.  No more Shadow Directory bugs.

  [P24-SEED-2]  Pre-Seed Startup — _preseed_hub_state() creates hub_data/ if
                missing and writes Zero-State valid JSON objects to
                trader_status.json and veto_audit.json at startup so the
                Dashboard never sees a null state on first boot.

  All Phase 22/23 changes preserved verbatim (see original changelog below).

Refactor changelog (Production-Readiness Pass):
  [SHUTDOWN-4] Hanging-Shutdown Guard — the entire component teardown block
               is wrapped in asyncio.wait_for(_teardown(...),
               timeout=SHUTDOWN_TIMEOUT_SECS).  If any component fails to
               close within SHUTDOWN_TIMEOUT_SECS seconds, a TimeoutError is
               caught, a CRITICAL log is emitted, and sys.exit(2) fires
               immediately so the OS process watchdog can restart the bot
               without a hanging zombie.

  [LLMFB-3]   LLM Empty-Response Fallback — every call to _p17_veto.score()
               and any other OpenRouter call-site is wrapped in try/except.
               On empty body, whitespace, 'char 0' JSON error, or any other
               exception the bot returns a neutral NarrativeResult
               (score=0.5, verdict='PASS', conviction_multiplier=1.0) and
               continues on pure TA-driven trading.  The bot will never crash
               because the AI free-tier is lagging.

  All other Phase 22 changes preserved verbatim (P22-1..P22-4).

Changes vs Phase 21:
  [P22-1] OpenRouter Unified AI Hub
  [P22-2] Entropy Shield
  [P22-3] Regime-Adaptive Take-Profit
  [P22-4] Systemic Panic Lock

All Phase 21, 20, 19, 18, 17 components fully preserved.

Exit Codes:
  0  — Clean, intentional shutdown
  1  — Fatal startup error
  2  — Unrecoverable runtime error / forced shutdown after timeout
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

import numpy as np
from dotenv import load_dotenv


load_dotenv(override=True)

from brain           import IntelligenceEngine, BRAIN_STATE_PATH
from arbitrator      import Arbitrator, ArbOpportunity, CoinbaseOracle, GlobalTapeAggregator
from news_wire       import NewsWire, MacroBias
from data_hub        import DataHub
from executor        import Executor, Position, GlobalRiskManager

from phase7_risk      import CircuitBreaker, KellySizer
from phase7_execution import AdaptiveSpread, TWAPSlicer, SlippageAuditor

from portfolio_manager import PortfolioGovernor
from sentinel          import Sentinel

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

log = logging.getLogger("main_p22")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("powertrader_p22.log", encoding="utf-8"),
    ],
)
for _lib in ("aiohttp", "websockets", "urllib3", "hpack", "ccxt"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

# ══════════════════════════════════════════════════════════════════════════════
# [P24-PATH-1] Single Source of Truth — ALL paths derived from BASE_DIR.
# HUB_DIR is hard-coded as hub_data/ inside the script directory.
# No os.environ override so Shadow Directory bugs are impossible.
# ══════════════════════════════════════════════════════════════════════════════
BASE_DIR           = os.path.dirname(os.path.abspath(__file__))
GUI_SETTINGS       = os.environ.get(
    "POWERTRADER_GUI_SETTINGS", os.path.join(BASE_DIR, "gui_settings.json")
)
# [P24-PATH-1] Hard-coded — never use env override for HUB_DIR.
HUB_DIR            = os.path.join(BASE_DIR, "hub_data")

# [P24.1-PATH] Explicit HUB_DIR creation guard — ensures the directory exists
# on first boot or after accidental deletion, before any path-derived constants
# (RUNNER_READY_PATH, TRADER_STATUS_PATH, etc.) are consumed by _preseed_hub_state.
try:
    os.makedirs(HUB_DIR, exist_ok=True)
except OSError as _hub_makedirs_exc:
    logging.getLogger("main_p24").critical(
        "[P24.1-PATH] CRITICAL: Cannot create hub_data directory at %s — %s. "
        "Dashboard will not receive status updates until this is resolved.",
        HUB_DIR, _hub_makedirs_exc,
    )

RUNNER_READY_PATH  = os.path.join(HUB_DIR, "runner_ready.json")
TRADER_STATUS_PATH = os.path.join(HUB_DIR, "trader_status.json")
TIF_PENDING_PATH   = os.path.join(HUB_DIR, "tif_pending.json")
PANIC_LOCK_PATH    = os.path.join(HUB_DIR, "panic_lock.json")   # [P22-4]
VETO_AUDIT_PATH    = os.path.join(HUB_DIR, "veto_audit.json")   # [P24-SEED-2]
CONTROL_EVENT_PATH = os.path.join(HUB_DIR, "control_event.json")# [P24-DEFENSE]

# ══════════════════════════════════════════════════════════════════════════════
# [P24-SEED-2]  Pre-Seed Hub State
# ══════════════════════════════════════════════════════════════════════════════

def _preseed_hub_state() -> None:
    """
    [P24-SEED-2] Create the hub_data/ directory if it is missing and write
    Zero-State valid JSON objects to trader_status.json and veto_audit.json.

    This is called once at module import so the Dashboard never encounters a
    missing file, a null state, or a JSONDecodeError on first boot — even before
    the executor has produced its first status cycle.

    Uses atomic write (tmp → os.replace) so a crash mid-write never leaves a
    corrupt file; falls back to a direct write if tmp-rename fails.
    """
    try:
        os.makedirs(HUB_DIR, exist_ok=True)
    except Exception as exc:
        logging.getLogger("main_p24").warning("[P24-SEED] makedirs failed: %s", exc)

    _NOW = time.time()

    _SEEDS: dict[str, object] = {
        TRADER_STATUS_PATH: {
            "timestamp":    _NOW,
            "status":       "pre_seed",
            "demo_mode":    False,
            "account":      {"total_equity": 0.0, "buying_power": 0.0},
            "positions":    {},
            "p24_preseed":  True,
        },
        VETO_AUDIT_PATH: [],
    }

    for path, payload in _SEEDS.items():
        if os.path.exists(path):
            continue  # do not overwrite a live file
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, path)
            logging.getLogger("main_p24").info(
                "[P24-SEED-2] Zero-state seeded → %s", path
            )
        except Exception as exc:
            logging.getLogger("main_p24").warning(
                "[P24-SEED-2] Seed write failed for %s: %s — direct fallback.", path, exc
            )
            try:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
            except Exception as exc2:
                logging.getLogger("main_p24").error(
                    "[P24-SEED-2] Direct fallback also failed for %s: %s", path, exc2
                )
            finally:
                try:
                    if os.path.exists(tmp):
                        os.remove(tmp)
                except Exception:
                    pass


# [P24-SEED-2] Run pre-seed at import time (before any component is initialised).
_preseed_hub_state()


TF_ALL = ["1hour", "2hour", "4hour", "8hour", "12hour", "1day", "1week"]

NEWS_BLOCK_THRESHOLD = float(os.environ.get("NEWS_BEARISH_BLOCK_THRESHOLD", "-0.5"))
ARB_EXEC_PCT         = float(os.environ.get("ARB_SPREAD_EXEC_PCT",          "0.2"))
MAX_ATOMIC_RETRIES   = 5
ATOMIC_RETRY_BASE    = 0.1

P19_STATE_SAVE_INTERVAL_SECS = float(
    os.environ.get("P19_STATE_SAVE_INTERVAL_SECS", str(60 * 30))
)

P20_DRAWDOWN_ZOMBIE_PCT   = float(os.environ.get("P20_DRAWDOWN_ZOMBIE_PCT",   "10.0"))
P20_COLD_START_GRACE_SECS = float(os.environ.get("P20_COLD_START_GRACE_SECS", "60.0"))
P20_SHADOW_INTERVAL_SECS  = float(os.environ.get("P20_SHADOW_INTERVAL_SECS",  str(24 * 3600)))
P20_SHADOW_AUTO_SWAP      = os.environ.get("P20_SHADOW_AUTO_SWAP", "0").strip() == "1"

# [SHUTDOWN-4] Maximum seconds to wait for all components to close gracefully
# before forcing sys.exit(2) and letting the watchdog restart the bot.
SHUTDOWN_TIMEOUT_SECS = float(os.environ.get("SHUTDOWN_TIMEOUT_SECS", "10.0"))

# ── [P21] Institutional Execution parameters ──────────────────────────────────
P21_SPREAD_THROTTLE_PCT     = float(os.environ.get("P21_SPREAD_THROTTLE_PCT",     "0.05"))
P21_LOW_LIQUIDITY_THRESHOLD = float(os.environ.get("P21_LOW_LIQUIDITY_THRESHOLD", "0.4"))
P21_MICROBURST_LEGS         = int  (os.environ.get("P21_MICROBURST_LEGS",         "3"))
P21_MICROBURST_DELAY_SECS   = float(os.environ.get("P21_MICROBURST_DELAY_SECS",   "0.8"))
P21_TIF_TIMEOUT_SECS        = float(os.environ.get("P21_TIF_TIMEOUT_SECS",        "10.0"))
P21_TIF_POLL_SECS           = float(os.environ.get("P21_TIF_POLL_SECS",           "1.0"))
P21_SPREAD_CACHE_TTL        = float(os.environ.get("P21_SPREAD_CACHE_TTL",        "2.0"))

# ── [P22] Phase 22 parameters ─────────────────────────────────────────────────
P22_ENTROPY_HIGH_THRESHOLD   = float(os.environ.get("P22_ENTROPY_HIGH_THRESHOLD",   "3.5"))
P22_ENTROPY_CONFIDENCE_BOOST = float(os.environ.get("P22_ENTROPY_CONFIDENCE_BOOST", "0.20"))
P22_BULL_TRAIL_WIDEN         = float(os.environ.get("P22_BULL_TRAIL_WIDEN",   "0.005"))
P22_CHOP_TRAIL_TIGHTEN       = float(os.environ.get("P22_CHOP_TRAIL_TIGHTEN", "0.002"))
P22_PANIC_DROP_PCT           = float(os.environ.get("P22_PANIC_DROP_PCT",   "1.5"))
P22_PANIC_QUORUM_PCT         = float(os.environ.get("P22_PANIC_QUORUM_PCT", "0.66"))
P22_PANIC_WINDOW_SECS        = float(os.environ.get("P22_PANIC_WINDOW_SECS","60.0"))
P22_PANIC_BLOCK_SECS         = float(os.environ.get("P22_PANIC_BLOCK_SECS", "300.0"))
P22_MIN_SIGNAL_CONFIDENCE    = float(os.environ.get("P22_MIN_SIGNAL_CONFIDENCE", "0.55"))


# ══════════════════════════════════════════════════════════════════════════════
# [LLMFB-3] Module-level neutral NarrativeResult factory
# ══════════════════════════════════════════════════════════════════════════════

def _make_neutral_narrative() -> NarrativeResult:
    """
    [LLMFB-3] Return a safe neutral NarrativeResult whenever the OpenRouter
    free-tier returns an empty body, whitespace blob, 'char 0' JSON error, or
    raises any exception during parsing.  The bot continues on pure
    TA-driven trading with a baseline 1.0× conviction multiplier and a PASS
    verdict so no orders are blocked and no sizes are distorted.

    Tries the dataclass constructor first; falls back to object.__setattr__
    for frozen dataclasses; then to a SimpleNamespace duck-type as a last
    resort so this helper itself can never raise.
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


# ══════════════════════════════════════════════════════════════════════════════
# [P20] Graced risk classes
# ══════════════════════════════════════════════════════════════════════════════

class GracedGlobalRiskManager(GlobalRiskManager):
    """
    [P20-1] GlobalRiskManager with a cold-start grace period so drawdown
    checks are suppressed during the warm-up window (default 60 s).
    The REST balance-fetch fallback is present in main() to seed peak_equity
    when the equity WebSocket hasn't delivered its first snapshot yet.
    """

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
    """
    [P7/P20] CircuitBreaker with a cold-start grace window.  During the
    grace period all trip/update calls are silently suppressed.
    """

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


# ══════════════════════════════════════════════════════════════════════════════
# [P22-4] PanicLock — Systemic Panic Detection
# ══════════════════════════════════════════════════════════════════════════════

class PanicLock:
    """
    [P22-4] Monitors a rolling price window across all active coins.

    When >= quorum_pct of coins simultaneously drop > drop_pct within the
    last window_secs seconds, a SYSTEMIC PANIC is declared and all new BUY
    entries are blocked for block_secs.  State is persisted atomically to
    persist_path so that a restart re-engages any active lock that was
    serialised before the process exited.
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

    # ── Persistence ───────────────────────────────────────────────────────────

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
        tmp = self._persist_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({
                    "blocked_until": self._blocked_until,
                    "trigger_count": self._trigger_count,
                    "saved_at":      time.time(),
                }, f, indent=2)
            os.replace(tmp, self._persist_path)
        except Exception as exc:
            log.debug("[P22-4] PanicLock save error: %s", exc)

    async def _save_state(self) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._save_state_sync)

    # ── Public API ────────────────────────────────────────────────────────────

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


# ══════════════════════════════════════════════════════════════════════════════
# [P21] Institutional Execution classes
# ══════════════════════════════════════════════════════════════════════════════

class DynamicSlippageGuard:
    """
    [P21-1] Fetches real-time BBO spread and throttles order entry when
    the spread exceeds the configured threshold.
    """

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


class MicroBurstExecutor:
    """
    [P21-2] Splits a large order into N progressively-sized legs when
    low liquidity is detected, reducing market impact.
    """

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
    [P21-3] Time-in-Force watchdog.  Registers order IDs on entry and
    cancels any that have not been reported as filled within timeout_secs.
    State is persisted to TIF_PENDING_PATH for orphan detection across
    restarts.
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
        tmp = TIF_PENDING_PATH + ".tmp"
        try:
            async with self._lock:
                snap = dict(self._pending)
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(snap, f, indent=2)
            os.replace(tmp, TIF_PENDING_PATH)
        except Exception as exc:
            log.debug("[P21-3] Could not persist TIF state: %s", exc)

    def register(self, ord_id: str, symbol: str, side: str, usd: float, tag: str) -> None:
        # [P23-FIX-TIFWATCHER] _execute_order may return a dict (full OKX
        # response) instead of a bare string.  Extract 'ordId' if so, then
        # coerce to str so the pending dict key is always hashable.
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
        # [P23-FIX-TIFWATCHER] Final coercion guard — ensures key is always str
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


# ══════════════════════════════════════════════════════════════════════════════
# [P22-2] Rolling return / Shannon entropy buffer
# ══════════════════════════════════════════════════════════════════════════════

class EntropyBuffer:
    """
    [P22-2] Maintains a time-windowed price series per symbol and exposes
    a get_returns() method used by the Entropy Shield gating logic.
    """

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


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

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
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
    except Exception as exc:
        log.error("[P8] Atomic write failed to serialise %s: %s", tmp, exc)
        return False
    for attempt in range(1, MAX_ATOMIC_RETRIES + 1):
        try:
            os.replace(tmp, path)
            return True
        except (PermissionError, OSError) as exc:
            if attempt == MAX_ATOMIC_RETRIES:
                log.error(
                    "[P8] All %d atomic write attempts failed: %s",
                    MAX_ATOMIC_RETRIES, exc,
                )
                return False
            await asyncio.sleep(ATOMIC_RETRY_BASE * attempt)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# [P40.1-HB]  Hub heartbeat helpers — called on every valid bridge message
# so the Watchdog never sees STALE data on a healthy bot.
# ══════════════════════════════════════════════════════════════════════════════

# [P40.1-HB] Throttled heartbeat state — avoids redundant I/O churn.
# Writes at most once every 5 seconds unless forced (ghost-heal / fill).
_P401_HB_LOCK: asyncio.Lock = asyncio.Lock()
_P401_HB_LAST_WRITE_TS: float = 0.0
_P401_HB_MIN_INTERVAL_SECS: float = float(os.environ.get("P401_HB_MIN_INTERVAL_SECS", "5.0"))

async def _touch_hub_timestamp(path: str = TRADER_STATUS_PATH, *, force: bool = False) -> None:
    """
    [P40.1-HB] Atomically update ONLY the ``timestamp`` field in the hub
    status file.  Reads the current file, bumps the timestamp, writes back.

    This is intentionally lightweight — it does *not* recompute any metrics.
    Its sole purpose is to prevent the Watchdog from flagging a healthy bot
    as STALE because the bridge event loop is alive but the 2-second
    _gui_bridge cycle hasn't fired yet.

    Silently swallows all exceptions so it can never crash a caller.
    """
    try:
        # [P40.1-HB] Throttle: at most once per _P401_HB_MIN_INTERVAL_SECS unless forced.
        now = time.time()
        async with _P401_HB_LOCK:
            global _P401_HB_LAST_WRITE_TS
            if (not force) and (_P401_HB_LAST_WRITE_TS > 0) and (now - _P401_HB_LAST_WRITE_TS) < _P401_HB_MIN_INTERVAL_SECS:
                return
            _P401_HB_LAST_WRITE_TS = now
        try:
            with open(path, "r", encoding="utf-8") as _f:
                _doc = json.load(_f)
        except (FileNotFoundError, json.JSONDecodeError):
            _doc = {}
        _doc["timestamp"] = time.time()
        await _atomic_write(path, _doc)
    except Exception as _exc:
        log.debug("[P40.1-HB] _touch_hub_timestamp error (non-fatal): %s", _exc)


async def _patch_trader_status_equity(
    path: str,
    retained_equity: float,
    bridge_source: str,
    ghost_count: int,
) -> None:
    """
    [P40.1-HEAL] Atomically patch ``account.total_equity`` in the hub status
    file with the Rust-bridge-healed equity value.

    This is called immediately on a ``ghost_healed`` bridge event so the
    Dashboard shows the correct equity without waiting for the next
    _gui_bridge write cycle (default 2 s).

    Fields patched:
      timestamp                   → time.time()
      account.total_equity        → retained_equity
      equity_is_ghost             → False
      consecutive_ghost_reads     → 0
      p40_ghost_heal_source       → bridge_source
      p40_ghost_heal_count        → ghost_count
      p40_last_heal_ts            → time.time()
      zombie_mode (if present)    → False  (bridge confirmed heal = no zombie)
      p20_zombie_mode (if present)→ False
    """
    try:
        try:
            with open(path, "r", encoding="utf-8") as _f:
                _doc = json.load(_f)
        except (FileNotFoundError, json.JSONDecodeError):
            _doc = {}

        _now = time.time()
        _doc["timestamp"]               = _now
        _doc["equity_is_ghost"]         = False
        _doc["consecutive_ghost_reads"] = 0
        _doc["p40_ghost_heal_source"]   = bridge_source
        _doc["p40_ghost_heal_count"]    = ghost_count
        _doc["p40_last_heal_ts"]        = _now

        # Patch nested account block if present
        if "account" not in _doc or not isinstance(_doc["account"], dict):
            _doc["account"] = {}
        _doc["account"]["total_equity"] = retained_equity

        # Veto zombie flags — ghost heal proves the equity reading was bad
        _doc["zombie_mode"]     = False
        _doc["p20_zombie_mode"] = False

        # Patch inside p20_global_risk block as well
        if isinstance(_doc.get("p20_global_risk"), dict):
            _doc["p20_global_risk"]["zombie_mode_status"] = False
            _doc["p20_global_risk"]["current_equity"]     = round(retained_equity, 2)

        await _atomic_write(path, _doc)
        log.info(
            "[P40.1-HEAL] trader_status.json patched → "
            "retained_equity=%.4f  ghost=False  zombie=False  source=%s",
            retained_equity, bridge_source,
        )
    except Exception as _exc:
        log.warning(
            "[P40.1-HEAL] _patch_trader_status_equity error (non-fatal): %s", _exc
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


# ══════════════════════════════════════════════════════════════════════════════
# [P19-1] Periodic brain state saver
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# [P16] Whale callback factory
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# [P40.1] OrchestratorGate — P21 + P22 extensions (Bridge-ready)
#
# Phase 40.1 refactor — Binary Truth Synchronization:
#
#   [P40.1-GATE]  Native installation: executor.gate = self
#                 Executor._cycle() → _maybe_enter() → self.gate._gated_enter()
#                 is the Phase 40.1 call chain.  The gate is replaced at the
#                 OrchestratorGate slot level rather than by injecting instance
#                 attributes at runtime.  This satisfies the no-monkeypatch
#                 constraint and keeps the fix native to the class architecture.
#
#   [P40.1-CLOSE] _on_post_close() hook replaces the former _record_close
#                 monkeypatch.  Executor._record_close() calls
#                 self.gate._on_post_close(pos, fill_px, tag) when the gate
#                 exposes that method (checked via hasattr).  Brain weight
#                 updates and slippage auditing are performed here, after the
#                 executor's own record-keeping is complete.
#
#   [P40.1-MB-FB] MicroBurst fallback: when dispatch fails, the gate returns
#                 True so executor's native execution path (_execute_order or
#                 TWAP) handles the fill.  _orig_enter is not referenced.
#
# All Phase 21 and Phase 22 gate logic is preserved verbatim.
# ══════════════════════════════════════════════════════════════════════════════

class OrchestratorGate:
    """
    [P40.1-GATE] Phase 40.1 Bridge-ready institutional gate.

    Installation (native — no monkeypatching):
        executor.gate = self   (replaces the lightweight OrchestratorGate
                                that Executor.__init__ builds internally)

    Full gate chain:
        P22-4 PanicLock  →  P7 NewsWire  →  P15 Whale Nuke  →
        P21-1 SlippageGuard  →  P22-2 Entropy Shield  →
        P7 Arb  →  P7 Kelly Panic  →  P22-3 Regime Trail  →
        P17/P20/P22-1 Council of Judges  →  P21-2 MicroBurst

    Close hook:
        Executor._record_close() calls self.gate._on_post_close() when
        the gate is installed.  Brain weight updates and slippage auditing
        run inside _on_post_close() after the executor's own DB write.
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

        # [P40.1-GATE] Preserve the executor's original lightweight gate so
        # we can restore it cleanly during teardown if ever required, and so
        # we inherit its sentinel reference for Whale-Nuke / pause checks.
        self._orig_gate: object = executor.gate
        self.hub                = hub
        self.sentinel           = getattr(executor.gate, "sentinel", None)

        self._arb_cooldown: dict = {}
        self._arb_cooldown_secs = float(os.environ.get("ARB_COOLDOWN_SECS", "30.0"))

    def install(self) -> None:
        """
        [P40.1-GATE] Replace executor's internal lightweight OrchestratorGate
        with this full institutional gate.

        Executor._cycle() calls self.gate._gated_enter() natively — no
        attribute injection or method wrapping is performed on Executor.
        The three P7 slots (_p7_cb, _p7_sizer, _p7_auditor) are set here
        because they are initialised to None in Executor.__init__ and are
        expected to be populated by main.py before the cycle loop starts;
        this is not monkeypatching — it is the documented setup contract.
        """
        # [P40.1-GATE] Core gate swap — single native attribute assignment.
        self._exec.gate          = self
        # P7 circuit-breaker / sizing slots (init-contract, not a patch).
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
        """
        [P40.1-GATE] Entry gate called by Executor._maybe_enter() via
        self.gate._gated_enter().  Returns False to block entry or to signal
        that MicroBurst already dispatched the order (preventing double-fill).
        Returns True only when all checks pass AND MicroBurst failed, so the
        executor's native execution path (_execute_order / TWAP) handles the fill.
        """
        direction = getattr(master, "direction", "neutral")
        bias      = await self._nw.get_macro_bias()

        # ── [P22-4] Systemic Panic Lock ───────────────────────────────────────
        if direction == "long" and self._panic.is_locked():
            log.warning(
                "[P22-4] %s LONG BLOCKED by PanicLock — %.0f s remaining.",
                symbol, self._panic.remaining_secs(),
            )
            return False

        # ── [P7] NewsWire macro block ─────────────────────────────────────────
        if direction == "long" and self._nw.is_bearish_block(bias):
            log.info("[Gate] %s LONG blocked by NewsWire: score=%.3f", symbol, bias.score)
            return False

        # ── [P15] Whale Nuke ──────────────────────────────────────────────────
        if oracle_signal and getattr(oracle_signal, "cancel_buys_flag", False):
            log.warning("[P15] %s Entry BLOCKED: Whale Nuke", symbol)
            return False

        # ── [P21-1] Dynamic Slippage Guard ────────────────────────────────────
        throttled, spread_pct = await self._sg.should_throttle(symbol)
        if throttled:
            log.warning(
                "[P21-1] %s THROTTLED: spread=%.5f%% > %.4f%%",
                symbol, spread_pct, P21_SPREAD_THROTTLE_PCT,
            )
            return False

        # ── [P22-2] Entropy Shield ────────────────────────────────────────────
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

        # ── [P7] Arbitrage gate ───────────────────────────────────────────────
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

        # ── [P7] Kelly panic block ────────────────────────────────────────────
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

        # ── [P22-3] Regime-Adaptive trail gap ─────────────────────────────────
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

        # ── [P17/P20-2/P22-1] Council of Judges ──────────────────────────────
        # [LLMFB-3] Every veto call is wrapped; empty/failed responses → PASS.
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
                # Safe fallback: treat as PASS, continue to order entry.

        # ── [P21-2] MicroBurst dispatch ───────────────────────────────────────
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
            # [P40.1-MB-FB] MicroBurst failed.  Return True so the executor's
            # native execution path (_execute_order / TWAP) places the order.
            # _orig_enter is intentionally not called — it no longer exists in
            # the Phase 40.1 hardened Executor and was a monkeypatch target.
            log.warning(
                "[P21-2] MicroBurst failed %s — yielding to native executor path.",
                symbol,
            )
            return True

        # MicroBurst dispatched the order.  Return False to prevent the executor's
        # native post-gate flow from placing a second, duplicate order.
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
        """
        [P40.1-CLOSE] Native close hook called by Executor._record_close() after
        its own DB write, brain outcome recording, and sentinel notification.

        Replaces the former _record_close monkeypatch.  Executor checks for this
        method via hasattr(self.gate, '_on_post_close') before calling it, so
        the hook is optional and safe for executor builds that do not yet include
        the Phase 40.1 hook call.

        Responsibilities (unchanged from former _gated_record_close):
          • Regime-aware brain weight update (brain.update_weights)
          • Slippage auditor recording (auditor.record)
        """
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


# ══════════════════════════════════════════════════════════════════════════════
# GUI bridge — extended with P22 telemetry
# ══════════════════════════════════════════════════════════════════════════════

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
    bridge=None,          # [P40.1-SHIELD] BridgeClient — used for ghost-state checks
    interval: float = 2.0,
) -> None:
    while True:
        try:
            status     = dict(executor._status)
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

            # [LLMFB-3] P17 snapshot — wrapped in try/except
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

            # ── [P40.1-OPEN-POS-SHIELD] Hard Guard — Ghost Read Rejection ────
            # If the executor has confirmed open positions AND the reported
            # equity is zero or effectively zero (≤ $1.00), the REST API or WS
            # has produced a "Ghost Read".  Binary Truth: you cannot have open
            # leveraged positions with zero equity.  Reject the reading,
            # preserve _last_valid_equity as the operative value, and log a
            # [SHIELD-REJECT] warning.  The Dashboard write is skipped for the
            # equity fields — all other status keys are still written normally.
            _has_open_pos = executor.has_open_positions()
            if _has_open_pos and cur_equity <= 1.0:
                _retained = executor._last_valid_equity or cur_equity
                log.warning(
                    "[SHIELD-REJECT] _gui_bridge: Open positions detected but "
                    "cur_equity=%.6f ≤ 1.0 — rejecting as Ghost Read.  "
                    "Retaining _last_valid_equity=%.6f for Dashboard write.  "
                    "trader_status.json equity fields will NOT be zeroed.",
                    cur_equity, _retained,
                )
                # Substitute the retained value so all downstream calculations
                # in this cycle use Binary Truth equity, not the ghost zero.
                cur_equity = _retained
            # ── [/P40.1-OPEN-POS-SHIELD] ─────────────────────────────────────


            # If the reported equity is ≤ $1.00 AND the bridge has flagged it
            # as a ghost read (or a heal is in progress), we MUST NOT:
            #   • Update the Peak Equity high-water mark with the ghost value
            #   • Compute a drawdown against the ghost value
            #   • Allow Zombie Mode to be triggered by phantom equity
            #
            # The bridge's healed value is already reflected in executor._equity
            # once _on_bridge_ghost_healed_main fires, so the normal path
            # resumes automatically on the next _gui_bridge tick.
            _bridge_ghost_active = (
                getattr(bridge, "_equity_is_ghost", False)
                or getattr(executor, "_p40_ghost_healing", False)
            )
            _is_ghost_equity = cur_equity <= 1.0 and _bridge_ghost_active

            if _is_ghost_equity:
                # Freeze drawdown calculation — use last known good HWM
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

            # ── [P40.1-SHIELD] Zombie Mode Veto ──────────────────────────────
            # If the bridge is actively flagging the current equity as ghost
            # data, override any zombie_mode that may have been set by the
            # GracedGlobalRiskManager based on the corrupted reading.
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
            }
            status["p20_zombie_mode"]        = executor._p20_zombie_mode
            status["p20_shadow_hmm_metrics"] = shadow_hmm_metrics

            if p21_monitor is not None:
                try:
                    status["p21_execution"] = p21_monitor.snapshot()
                except Exception as exc:
                    log.debug("[P21] GUI snapshot error: %s", exc)

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

            # ── [P22-STRATEGY-MODE] Forward logic path key ────────────────────
            status["strategy_mode"] = getattr(executor, "_strategy_mode", "🤖 AI_PREMIUM")

            # ── [P22-RESILIENCE] Forward ghost-state flags ────────────────────
            # These are also in status["account"] from executor._status but we
            # surface them at top-level for simpler dashboard reads.
            status["equity_is_ghost"]         = getattr(executor, "_equity_is_ghost",         False)
            status["consecutive_ghost_reads"]  = getattr(executor, "_consecutive_ghost_reads",  0)

            # ── [P22-TELEMETRY] Latency bar data ──────────────────────────────
            # AI latency: pull from the most-recent P17 council result.
            _p17_recents  = status.get("p17_intelligence", {}).get("recent_results", [])
            _ai_lat_ms    = None
            if _p17_recents:
                _ai_lat_ms = _p17_recents[0].get("latency_ms")
            # Exchange latency: pull from P21 slippage-guard round-trip tracking.
            _p21_exec      = status.get("p21_execution", {})
            _exch_lat_ms   = _p21_exec.get("p21_slippage_guard", {}).get("last_roundtrip_ms")
            # News-wire LLM latency fallback if council had none
            _nw_lat_ms     = status.get("agents", {}).get("news_wire", {}).get("llm_latency_ms")
            status["p22_latency_bar"] = {
                "ai_latency_ms":       round(_ai_lat_ms,   1) if _ai_lat_ms   is not None else None,
                "exchange_latency_ms": round(_exch_lat_ms, 1) if _exch_lat_ms is not None else None,
                "newswire_latency_ms": round(_nw_lat_ms,   1) if _nw_lat_ms   is not None else None,
            }

            # ── [P22-TELEMETRY] Whale tape ────────────────────────────────────
            # Collect the most recent oracle signals (all symbols) and surface
            # them as a structured "whale tape" for the dashboard renderer.
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
            # Sort biggest sweeps first
            _whale_tape.sort(key=lambda x: x["size_usd"], reverse=True)
            status["p22_whale_tape"] = _whale_tape[:15]   # cap at 15 rows

            try:
                await _atomic_write(TRADER_STATUS_PATH, status)
            except Exception as exc:
                log.error("[P8] GUI bridge write error: %s", exc)

        except Exception as exc:
            log.debug("[GUI] Bridge error: %s", exc)

        await asyncio.sleep(interval)


# ══════════════════════════════════════════════════════════════════════════════
# Settings watcher / candle callback
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# Shutdown event + signal handler
# ══════════════════════════════════════════════════════════════════════════════

_shutdown_event = asyncio.Event()


def _handle_signal(*_) -> None:
    log.info("Shutdown signal received — setting shutdown event.")
    _shutdown_event.set()


# ══════════════════════════════════════════════════════════════════════════════
# [SHUTDOWN-4] Timed component teardown coroutine
# ══════════════════════════════════════════════════════════════════════════════

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
    bridge: Optional["BridgeClient"] = None,  # [P40]
) -> None:
    """
    [SHUTDOWN-4] All component teardown lives in one coroutine so a single
    asyncio.wait_for() can enforce the global SHUTDOWN_TIMEOUT_SECS budget.
    Any component that hangs will be abandoned when the timeout fires; the
    outer caller logs CRITICAL and calls sys.exit(2) to let the watchdog
    restart cleanly.
    """
    if not no_p21:
        try:
            await tif_watcher.stop()
        except Exception as exc:
            log.warning("[P21-3] tif_watcher.stop(): %s", exc)

    try:
        await oracle.stop()
    except Exception:
        pass

    # [P40.1-TEARDOWN] Explicitly await Oracle.close() if available (prevents
    # "Event loop is closed" and dangling transports on shutdown).
    try:
        _oclose = getattr(oracle, "close", None)
        if callable(_oclose):
            await _oclose()
    except Exception as exc:
        log.debug("[P40.1-TEARDOWN] oracle.close(): %s", exc)

    # [P40.1-TEARDOWN] Close any aiohttp ClientSession held by the Oracle.
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

    # Final brain state save (most important — must happen before closing hub)
    try:
        brain.save_state(path=brain_state_path)
        log.info("[P19-1] Final brain state saved → %s", brain_state_path)
    except Exception as exc:
        log.warning("[P19-1] Final save raised: %s", exc)

    try:
        await hub.close()
    except Exception:
        pass

    # [SHUTDOWN-4-FIX] Explicitly close any aiohttp ClientSession that the
    # DataHub's REST client (or the hub itself) may still hold open.  Without
    # this, the connector keeps an open TCP connection and asyncio emits
    # "Unclosed client session" warnings that can delay clean shutdown.
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

    # [SHUTDOWN-4-FIX] Cancel any named asyncio tasks that the DataHub spawned
    # (e.g. okx_market_feed, okx_private_feed) and that were not already
    # cancelled by hub.close().  This prevents those coroutines from keeping
    # the event loop alive and triggering the SHUTDOWN_TIMEOUT_SECS guard.
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

    # [SHUTDOWN-4-FIX] Explicitly close any aiohttp ClientSession held by the
    # Arbitrator (or any of its HTTP clients). Without this, aiohttp may emit
    # "Unclosed client session" warnings and teardown can race the loop close.
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

    # [SHUTDOWN-4-FIX] Close any aiohttp ClientSession held by NewsWire (if any).
    try:
        _nwsess = getattr(news_wire, "_session", None) or getattr(news_wire, "session", None)
        if _nwsess is not None and not getattr(_nwsess, "closed", True):
            await _nwsess.close()
            log.debug("[SHUTDOWN-4-FIX] NewsWire aiohttp session closed.")
    except Exception as exc:
        log.debug("[SHUTDOWN-4-FIX] NewsWire session close: %s", exc)

    # [P40] Gracefully shut down the Rust bridge (sends shutdown msg then closes TCP).
    if bridge is not None:
        try:
            log.info("[P40] Closing Rust bridge connection…")
            await bridge.close()
            # [P40.1-TEARDOWN] Close any aiohttp ClientSession held by the bridge client.
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


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="PowerTrader Phase 22 — Unified Intelligence & Market Correlation Defense"
    )
    parser.add_argument("--coins",          nargs="+", type=str, default=None)
    parser.add_argument("--train-first",    action="store_true")
    parser.add_argument("--drawdown-limit", type=float, default=None)
    parser.add_argument("--no-arb",         action="store_true")
    parser.add_argument("--no-news",        action="store_true")
    parser.add_argument("--no-governor",    action="store_true")
    parser.add_argument("--no-sentinel",    action="store_true")
    parser.add_argument("--no-p16-bridge",  action="store_true")
    parser.add_argument("--no-p17",         action="store_true")
    parser.add_argument("--no-p18",         action="store_true")
    parser.add_argument("--no-p20-zombie",  action="store_true")
    parser.add_argument("--no-p20-shadow",  action="store_true")
    parser.add_argument("--no-p21",         action="store_true")
    parser.add_argument(
        "--no-p22-panic",
        action="store_true",
        help="Disable Systemic Panic Lock (P22-4)",
    )
    parser.add_argument("--no-p22-entropy",
        action="store_true",
        help="Disable Entropy Shield (P22-2)",
    )
    parser.add_argument(
        "--no-bridge",
        action="store_true",
        help="[P40] Disable Rust IPC bridge; run legacy REST/WS path (Safe Mode)",
    )
    parser.add_argument("--brain-state", type=str, default=None)
    args = parser.parse_args()

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

    # Raise process priority on Windows (best-effort)
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
    brain    = IntelligenceEngine(coins)
    hub      = DataHub(coins, TF_ALL, demo=demo_mode)

    # ── [P40] Phase 40: Logic Execution Bridge ────────────────────────────────
    # The BridgeClient is the new transport layer.  It owns the OKX WebSocket
    # (including Ghost-State healing) and handles order signing / submission
    # through the compiled Rust okx_bridge binary over TCP IPC.
    #
    # Initialised here and injected into Executor below so it is shared as a
    # single source of truth — no monkey-patching, no global state.
    #
    # auto_start_bridge=True: main.py launches the Rust binary automatically if
    # it is found on disk or on PATH (set BRIDGE_BINARY env to override path).
    # ── Ghost-State contract (P40) ────────────────────────────────────────────
    # • account_update  events → executor._on_bridge_account_update (accept healed eq)
    # • ghost_healed    events → executor._on_bridge_ghost_healed   (LOG ONLY, no shutdown)
    # • equity_breach   events → executor._on_bridge_equity_breach  (let CB handle it)
    # ─────────────────────────────────────────────────────────────────────────
    bridge = BridgeClient(
        auto_start_bridge=not args.no_bridge,
    )

    # Register main-level bridge handlers BEFORE executor (executor will also
    # register its own handlers in __init__ when it receives the bridge reference).
    async def _on_bridge_account_update_main(msg: dict) -> None:
        """
        [P40] main.py-level handler for bridge account_update.
        Mirrors the equity into the brain's peak_equity tracker when it is
        positive, so GlobalRiskManager drawdown checks use the Binary Truth
        equity from the bridge rather than a potentially stale WS reading.

        [P40.1-HB] Also touches the hub timestamp on every valid (non-ghost)
        account update so the Watchdog never declares STALE on a healthy bot.
        """
        eq       = float(msg.get("eq", 0.0))
        is_ghost = bool(msg.get("is_ghost", False))
        if is_ghost or eq <= 0.0:
            return  # Ghost — let _on_bridge_ghost_healed_main handle resolution
        # ── [P40.1-OPEN-POS-SHIELD] REST / Bridge Account-Update Guard ───────
        # If open positions are confirmed by the executor but the incoming
        # equity is ≤ $1.00 it is structurally impossible — reject as Ghost.
        if executor.has_open_positions() and eq <= 1.0:
            _retained = executor._last_valid_equity or eq
            log.warning(
                "[SHIELD-REJECT] _on_bridge_account_update_main: "
                "open positions present but bridge eq=%.6f ≤ 1.0 — "
                "Ghost Read rejected.  _last_valid_equity=%.6f retained.",
                eq, _retained,
            )
            return  # Do NOT update peak_equity or touch hub timestamp
        # ── [/P40.1-OPEN-POS-SHIELD] ─────────────────────────────────────────

        if brain.peak_equity <= 0 or eq > brain.peak_equity:
            brain.peak_equity = eq
        log.debug("[P40][MAIN] Bridge account_update: eq=%.4f → brain.peak_equity=%.4f", eq, brain.peak_equity)
        # [P40.1-HB] Heartbeat: prevent Watchdog STALE warning
        asyncio.create_task(_touch_hub_timestamp(TRADER_STATUS_PATH, force=True))

    async def _on_bridge_equity_breach_main(msg: dict) -> None:
        """
        [P40] main.py-level handler for bridge equity_breach.
        This is a CONFIRMED $0.00 equity (not a ghost state).  We log CRITICAL
        and allow the existing CircuitBreaker / GlobalRiskManager to respond —
        we do NOT trigger an emergency shutdown from here, because that would
        bypass the graceful teardown and leave the Rust bridge process orphaned.
        """
        log.critical(
            "[P40][MAIN] Bridge equity_breach: confirmed_eq=%.4f  message=%s  "
            "CircuitBreaker will trip on next cycle.  "
            "System entering protective mode — NOT triggering hard sys.exit().",
            float(msg.get("confirmed_eq", 0.0)),
            msg.get("message", ""),
        )

    async def _on_bridge_ghost_healed_main(msg: dict) -> None:
        """
        [P40.1-HEAL] main.py-level handler for bridge ghost_healed.

        The Rust bridge resolved a Ghost State via REST reconciliation.  This
        handler is the authoritative receiver of the healed equity value.

        Phase 40.1 actions (in order):
          1. Extract healed_eq from the bridge message.
          2. Update executor._equity with healed_eq so all downstream
             logic (Kelly sizing, Zombie check) uses Binary Truth.
          3. Set executor._p40_ghost_healing = False — healing is complete.
          4. Update brain.peak_equity with healed_eq if it represents a
             new high-water mark.
          5. Explicitly override executor._p20_zombie_mode = False because
             the bridge has confirmed the $1.00 reading was a ghost state,
             not a real equity breach.
          6. Atomically patch trader_status.json with the healed equity so
             the Dashboard does not have to wait for the next _gui_bridge
             cycle (retained_equity consistency guarantee).
          7. [P40.1-HB] Touch the hub timestamp (heartbeat).

        Python does NOT trigger emergency shutdown or position flattening
        — the Rust bridge already reconciled the state.
        """
        raw_ws_eq  = float(msg.get("raw_ws_eq",  0.0))
        healed_eq  = float(msg.get("healed_eq",  0.0))
        source     = msg.get("source", "unknown")
        ghost_count = int(msg.get("ghost_count", 0))

        log.warning(
            "[P40.1-HEAL] GHOST HEALED by Rust bridge: "
            "raw_ws_eq=%.4f  healed_eq=%.4f  source=%s  ghost_count=%d  "
            "→ Python: applying Binary Truth, NO emergency shutdown.",
            raw_ws_eq, healed_eq, source, ghost_count,
        )

        # ── 1 & 2. Push healed equity into executor ───────────────────────────
        if healed_eq > 0.0:
            executor._equity = healed_eq
            log.info(
                "[P40.1-HEAL] executor._equity updated: %.4f → %.4f (retained_equity)",
                raw_ws_eq, healed_eq,
            )

        # ── 3. Clear healing-in-progress flag ────────────────────────────────
        executor._p40_ghost_healing = False

        # ── 4. Update peak equity with healed value ───────────────────────────
        if healed_eq > 0.0 and (brain.peak_equity <= 0 or healed_eq > brain.peak_equity):
            brain.peak_equity = healed_eq
            log.info(
                "[P40.1-HEAL] brain.peak_equity updated to healed value: %.4f", healed_eq
            )

        # ── 5. Veto Zombie Mode — ghost ≠ real breach ─────────────────────────
        if getattr(executor, "_p20_zombie_mode", False):
            executor._p20_zombie_mode = False
            log.warning(
                "[P40.1-SHIELD] Zombie Mode VETOED — bridge confirmed $%.2f "
                "was a ghost read (raw_ws_eq=%.4f), not a real equity breach.",
                raw_ws_eq, raw_ws_eq,
            )

        # ── 6. Immediate atomic patch to trader_status.json ───────────────────
        # retained_equity is the single source of truth written to the Dashboard.
        retained_equity = healed_eq if healed_eq > 0.0 else (executor._equity or 0.0)
        asyncio.create_task(
            _patch_trader_status_equity(
                path            = TRADER_STATUS_PATH,
                retained_equity = retained_equity,
                bridge_source   = source,
                ghost_count     = ghost_count,
            )
        )

        log.info(
            "[P40.1-HEAL] retained_equity=%.4f written to Dashboard "
            "(consistent with executor._equity=%.4f)",
            retained_equity, executor._equity,
        )

    async def _on_bridge_fill_main(msg: dict) -> None:
        """
        [P40.1-HB] main.py-level handler for bridge order_fill.
        A confirmed fill means the bridge is alive and connected.  Touch the
        hub timestamp so the Watchdog does not declare STALE during high-
        activity periods where equity hasn't changed but fills are arriving.
        """
        log.debug(
            "[P40][MAIN] Bridge order_fill: ordId=%s  fillPx=%s  fillSz=%s",
            msg.get("ordId"), msg.get("fillPx"), msg.get("fillSz"),
        )
        # [P40.1-HB] Heartbeat
        asyncio.create_task(_touch_hub_timestamp(TRADER_STATUS_PATH))

    executor = Executor(hub, brain, list(coins), demo=demo_mode, bridge=bridge)
    hub._risk_executor = executor

    # ── [P40.1] Ghost-healing-in-progress flag ───────────────────────────────
    # executor._p40_ghost_healing is now initialised to False inside
    # Executor.__init__ (after the bridge instance is verified non-None).
    # The external set that previously lived here has been removed to eliminate
    # the UnboundLocalError race and to co-locate all _p40 flag ownership
    # inside the Executor constructor (Phase 40.1 Stability Patch).
    # _gui_bridge and _on_bridge_ghost_healed_main still read/write this
    # attribute normally — it is guaranteed present by __init__.
    # ── [/P40.1] ─────────────────────────────────────────────────────────────

    # Register main-level bridge event handlers AFTER executor exists so that
    # any handler that fires immediately (e.g. a queued account_update from a
    # re-connect) can safely dereference executor without an AttributeError.
    bridge.on("account_update", _on_bridge_account_update_main)
    bridge.on("equity_breach",  _on_bridge_equity_breach_main)
    bridge.on("ghost_healed",   _on_bridge_ghost_healed_main)
    bridge.on("order_fill",     _on_bridge_fill_main)
    # ── [/P40] ────────────────────────────────────────────────────────────────

    arb = Arbitrator(coins)
    if not args.no_arb:
        await arb.start()

    # [P40] Wire bridge's healed equity stream into the Arbitrator so it uses
    # Binary Truth for position sizing (not raw WS readings).
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

    # ── [P10] Portfolio Governor ──────────────────────────────────────────────
    governor: Optional[PortfolioGovernor] = None
    if not args.no_governor:
        governor = PortfolioGovernor(
            hub=hub, executor=executor, news_wire=news_wire, symbols=coins
        )
        executor._p10_governor = governor

    # ── [P17/P18/P20-2/P22-1] Council of Judges + OpenRouter ─────────────────
    # [LLMFB-3] Initialisation is wrapped in try/except; failure → veto=None
    # which means the bot continues on TA-only mode without crashing.
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

    if not args.no_p16_bridge:
        whale_cb = _make_whale_callback(executor)
        oracle.register_whale_callback(whale_cb)
        executor.p16_bridge_active = True
        log.warning("[P16-WIRE] Express Lane ACTIVE")

    log.info("DataHub started — warming (5 s)…")
    await asyncio.sleep(5)

    # Resolve tick sizes for all coins
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

        # [P20-1] Seed peak_equity when equity WS hasn't fired yet (EQBOOT-2)
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
        # [P40] bridge.connect() runs the persistent reconnect loop in background.
        # If the Rust binary restarts, the loop automatically re-establishes IPC
        # without crashing the Python side (BridgeClient._connect_loop handles it).
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
                bridge      = bridge,     # [P40.1-SHIELD]
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

    log.info(
        "🚀 PowerTrader Phase 40 LIVE.  "
        "demo=%s  coins=%s  "
        "p21=%s  p22_panic=%s  p22_entropy=%s  p22_trail=True  "
        "p24_ghost_defense=True  p24_entropy_seed=True  "
        "p40_bridge=True  p40_whale_express=True  "
        "openrouter_model=%s",
        demo_mode, coins,
        not args.no_p21,
        not args.no_p22_panic,
        not args.no_p22_entropy,
        OPENROUTER_MODEL,
    )
    await _write_runner_ready(True, "real_predictions", coins, len(coins))

    # ── Main await — first task to finish triggers orderly shutdown ───────────
    shutdown_task = asyncio.create_task(_shutdown_event.wait(), name="shutdown")
    done, pending = await asyncio.wait(
        tasks + [shutdown_task], return_when=asyncio.FIRST_COMPLETED
    )

    # Surface any unexpected crash from a task
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

    # ── [SHUTDOWN-4] Timed teardown — forces exit(2) if it hangs ─────────────
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
                bridge           = bridge,       # [P40]
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


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # [P40.1-HEAL][P40.1-SHIELD] Custom asyncio runner.
    #
    # asyncio.run() closes the loop immediately after main() returns and can race
    # with transport finalizers (StreamWriter/aiohttp/websockets), producing:
    #   RuntimeError: Event loop is closed
    #
    # This runner ensures:
    #   • main() returns an exit code (no sys.exit inside the loop)
    #   • all remaining tasks are cancelled and awaited (shielded) BEFORE loop.close()
    #   • async generators are shutdown cleanly
    code: int = 2
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        try:
            code = loop.run_until_complete(main())
        except SystemExit as _se:
            # main() should not call sys.exit, but preserve legacy behavior safely.
            code = int(getattr(_se, "code", 0) or 0)
        except Exception as exc:
            logging.getLogger("main_p22").critical(
                "Unhandled exception in main loop: %s", exc, exc_info=True
            )
            code = 2
    finally:
        try:
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for t in pending:
                t.cancel()
            if pending:
                timeout = float(os.environ.get("SHUTDOWN_TIMEOUT_SECS", "20"))
                loop.run_until_complete(
                    asyncio.wait_for(
                        asyncio.shield(asyncio.gather(*pending, return_exceptions=True)),
                        timeout=timeout,
                    )
                )
        except Exception:
            pass
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:
            pass
        try:
            asyncio.set_event_loop(None)
        except Exception:
            pass
        loop.close()
    sys.exit(code)
