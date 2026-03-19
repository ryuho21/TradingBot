"""
sentinel.py  —  Phase 11 / Phase 13 / Phase 14 / Phase 16: The Sentinel & Feedback Loop

Phase 16 additions:
  [P16-3] notify_atomic_fill()
    - Sends a Telegram alert with the ⚡ lightning emoji to distinguish
      institutional Express Lane fills from regular signal-driven entries.
    - Includes: symbol, direction, fill price, fill size, USD notional,
      oracle multiplier (how many × the rolling avg the whale trade was),
      and the oracle reason string (WHALE_SWEEP_BUY / WHALE_SWEEP_SELL).
    - Callable directly from executor.trigger_atomic_express_trade() after
      a confirmed fill, via asyncio.ensure_future().

Phase 14 additions (all preserved):
  [P14-4] Telegram "Conviction" Alerts
    - When a trade is opened with high_conviction=True on its signal, the
      Telegram entry notification includes a "🔥 High Conviction Signal
      (Consensus Met)" line.
    - Sentinel exposes notify_entry(symbol, direction, confidence, signal)
      which executor calls after a confirmed long/short fill.  The method
      inspects signal.high_conviction and signal.conviction_votes to compose
      the correct message.
    - notify_close(symbol, direction, pnl_pct, tag) sends a close alert with
      PnL summary.

Phase 13 additions (all preserved):
  [P13-4] Daily Performance Wrap-up via DailyReportScheduler.

Phase 11 additions (all preserved):
  [P11-1] Telegram Notifications — flash crash, regime change, equity breach.
  [P11-2] Shadow Miss AutoTune via ShadowMissTracker.
  [P11-3] MTF Log Dampening via MtfConflictLogger.
  [P11-4] Equity Protection — rolling drawdown circuit breaker.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Deque, Dict, List, Optional, Tuple

import aiohttp

if TYPE_CHECKING:
    from brain import Signal
    from executor import Executor
    from portfolio_manager import PortfolioGovernor
    from phase7_execution import AdaptiveSpread

log = logging.getLogger("sentinel")
logging.getLogger("sentinel").addHandler(logging.NullHandler())
from pt_utils import _env_float, _env_int, atomic_write_json  # [P0-UTIL]

# [P0-FIX-11] env helpers → pt_utils

# ── Config ─────────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN       = os.environ.get("TELEGRAM_BOT_TOKEN",       "YOUR_BOT_TOKEN")
TELEGRAM_CHAT_ID         = os.environ.get("TELEGRAM_CHAT_ID",         "YOUR_CHAT_ID")
TELEGRAM_ENABLED         = (
    TELEGRAM_BOT_TOKEN != "YOUR_BOT_TOKEN"
    and TELEGRAM_CHAT_ID != "YOUR_CHAT_ID"
    and bool(TELEGRAM_BOT_TOKEN)
    and bool(TELEGRAM_CHAT_ID)
)

FLASH_CRASH_DROP_PCT     = _env_float("FLASH_CRASH_DROP_PCT", 5.0)
FLASH_CRASH_WINDOW_SECS  = _env_float("FLASH_CRASH_WINDOW_SECS", 30.0)
FLASH_CRASH_SYMBOL       = os.environ.get("P11_FLASH_CRASH_SYMBOL",         "BTC")

AUTO_TUNE_WIN_RATE_FLOOR = _env_float("AUTO_TUNE_WIN_RATE_FLOOR", 0.4)
AUTO_TUNE_WINDOW         = _env_int("AUTO_TUNE_WINDOW", 5)
AUTO_TUNE_SPREAD_STEP    = _env_float("P11_SPREAD_TIGHTEN_BPS", 0.25)
SPREAD_MIN_OFFSET_BPS    = _env_float("P7_SPREAD_MIN_OFFSET_BPS", 0.5)

MAX_DRAWDOWN_PCT         = _env_float("MAX_DRAWDOWN_PCT", 15.0)
# [P11-4] Guard rails against transient equity glitches (e.g., $0/$1 samples)
P11_MIN_EQUITY_SAMPLE        = _env_float("P11_MIN_EQUITY_SAMPLE", 100.0)
P11_ANOMALY_DROP_PCT         = _env_float("P11_ANOMALY_DROP_PCT", 50.0)
P11_ANOMALY_CONFIRM_SAMPLES  = _env_int("P11_ANOMALY_CONFIRM_SAMPLES", 2)
DRAWDOWN_WINDOW_HOURS    = _env_float("DRAWDOWN_WINDOW_HOURS", 24.0)

SENTINEL_POLL_SECS       = _env_float("P11_SENTINEL_POLL_SECS", 5.0)

# ── [P13-4] Daily report config ────────────────────────────────────────────────
P13_REPORT_INTERVAL_HOURS = _env_float("P13_REPORT_INTERVAL_HOURS", 24.0)

# ── [P44-DCB] Rolling Drawdown Circuit Breaker ────────────────────────────────
# A short-window, soft-action circuit breaker that FREEZES new entries (does NOT
# flatten positions) when equity drops too fast.  Separate from [P11-4]
# EquityProtector which fires on the 24h MAX_DRAWDOWN_PCT and triggers full
# flatten + shutdown.
#
# DCB fires when equity drops >= P44_DCB_FREEZE_PCT from its rolling peak within
# P44_DCB_WINDOW_HRS.  While frozen, executors block all new entry signals.
# Auto-resets after P44_DCB_COOLDOWN_SECS when equity recovers above the trigger
# level.
P44_DCB_ENABLE       = os.environ.get("P44_DCB_ENABLE",       "1").strip().strip("'\"") == "1"
P44_DCB_WINDOW_HRS   = _env_float("P44_DCB_WINDOW_HRS",   1.0)    # rolling peak window
P44_DCB_FREEZE_PCT   = _env_float("P44_DCB_FREEZE_PCT",   4.0)    # % drop from peak to trip
P44_DCB_COOLDOWN_SECS= _env_float("P44_DCB_COOLDOWN_SECS",1800.0) # min freeze duration (s)
P44_DCB_MIN_EQUITY   = _env_float("P44_DCB_MIN_EQUITY",   10.0)   # ignore readings below this

# ── [P44-SUP] Performance Supervisor ─────────────────────────────────────────
# Monitors global rolling win-rate and regime type.  Applies soft parameter
# adjustments to live executors WITHOUT requiring a bot restart.
#
# Adjustment ladder:
#   win_rate < P44_SUP_WARN_FLOOR       → Telegram advisory only (no action)
#   win_rate < P44_SUP_PAUSE_FLOOR      → Soft-pause new entries for P44_SUP_PAUSE_SECS
#   win_rate recovers > P44_SUP_RESUME_RATE → Auto-resume, Telegram recovery alert
P44_SUP_ENABLE        = os.environ.get("P44_SUP_ENABLE",     "1").strip().strip("'\"") == "1"
P44_SUP_POLL_SECS     = _env_float("P44_SUP_POLL_SECS",     3600.0) # how often Supervisor runs
P44_SUP_MIN_TRADES    = _env_int  ("P44_SUP_MIN_TRADES",    10)     # cold-start guard
P44_SUP_WARN_FLOOR    = _env_float("P44_SUP_WARN_FLOOR",    0.45)   # advisory threshold
P44_SUP_PAUSE_FLOOR   = _env_float("P44_SUP_PAUSE_FLOOR",   0.35)   # soft-pause threshold
P44_SUP_RESUME_RATE   = _env_float("P44_SUP_RESUME_RATE",   0.50)   # auto-resume threshold
P44_SUP_PAUSE_SECS    = _env_float("P44_SUP_PAUSE_SECS",    1800.0) # max pause duration (s)
# Supervisor writes parameter recommendations to this JSON for the dashboard.
P44_SUP_OVERRIDES_PATH = os.environ.get(
    "P44_SUP_OVERRIDES_PATH",
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "hub_data", "supervisor_overrides.json"
    ),
)

# ── [P44-APPLY] Auto-Apply: .env mutation + watchdog restart ─────────────────
# When win_rate < P44_SUP_PAUSE_FLOOR, the Supervisor no longer just writes a
# JSON recommendation — it actually modifies KELLY_FRACTION in .env and calls
# sys.exit(2) so bot_watchdog.py handles the clean restart.
#
# Safety guards:
#   • P44_APPLY_ENABLE must be "1" (default "1") — set "0" to disable mutation.
#   • New KELLY_FRACTION is clamped to [P44_KELLY_FLOOR, current_value).
#   • An override_history array is appended to supervisor_overrides.json so the
#     operator can audit every automatic change.
#   • A Telegram alert is sent BEFORE sys.exit(2) so the operator gets a
#     notification even if the process dies before the next cycle.
P44_APPLY_ENABLE        = os.environ.get("P44_APPLY_ENABLE", "1").strip().strip("'\"") == "1"
P44_ENV_PATH            = os.environ.get(
    "P44_ENV_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
)
# Floor: KELLY_FRACTION will never be reduced below this value automatically.
P44_KELLY_FLOOR         = _env_float("P44_KELLY_FLOOR",         0.05)
# Each override cycle reduces KELLY_FRACTION by this multiplicative factor.
# 0.5 = halve it.  Applied at most once per bot lifetime (per restart cycle).
P44_KELLY_REDUCTION_FACTOR = _env_float("P44_KELLY_REDUCTION_FACTOR", 0.5)
# ── [/P44-APPLY] ──────────────────────────────────────────────────────────────

# ── [P54] Sentinel kernel-bypass degradation guard ───────────────────────────
# Two-tier escalation driven by bridge.p54_snapshot() each poll cycle.
#
# TIER-1 (recoverable — soft-pause + Telegram WARNING):
#   ring_util >= P54_SENTINEL_RING_WARN_PCT  OR
#   drop_delta >= P54_SENTINEL_DROP_WARN
#   -> executor._p54_soft_pause = True, Telegram WARNING, no restart.
#
# TIER-2 (critical — optional sys.exit(2), watchdog restarts):
#   ring_util >= P54_SENTINEL_RING_CRITICAL_PCT for RING_CONSEC consecutive ticks
#   OR  drop_delta >= P54_SENTINEL_DROP_TRIP
#   -> Telegram CRITICAL; if P54_SENTINEL_EXIT_ON_CRITICAL=1 -> sys.exit(2).
#
# Guard only fires once the bridge has emitted >= 1 p54_stats event.
# Fail-closed: any exception inside the guard block is swallowed (never halts).

def _env_bool(key: str, default: bool) -> bool:
    """Parse a boolean env var (0/false/no = False; 1/true/yes = True)."""
    raw = os.environ.get(key, "").strip().lower()
    if raw in ("1", "true", "yes"):
        return True
    if raw in ("0", "false", "no"):
        return False
    return default

P54_SENTINEL_ENABLE            = _env_bool ("P54_SENTINEL_ENABLE",            True)
P54_SENTINEL_RING_WARN_PCT     = _env_float("P54_SENTINEL_RING_WARN_PCT",     85.0)
P54_SENTINEL_RING_CRITICAL_PCT = _env_float("P54_SENTINEL_RING_CRITICAL_PCT", 95.0)
P54_SENTINEL_RING_CONSEC       = _env_int  ("P54_SENTINEL_RING_CONSEC",        3)
P54_SENTINEL_DROP_WARN         = _env_int  ("P54_SENTINEL_DROP_WARN",          50)
P54_SENTINEL_DROP_TRIP         = _env_int  ("P54_SENTINEL_DROP_TRIP",         500)
P54_SENTINEL_EXIT_ON_CRITICAL  = _env_bool ("P54_SENTINEL_EXIT_ON_CRITICAL",  False)
P54_SENTINEL_STATS_STALE_SECS  = _env_float("P54_SENTINEL_STATS_STALE_SECS",  30.0)
# ── [/P54-sentinel-config] ────────────────────────────────────────────────────


# ── [P11-TG-RL] Telegram rate-limiter state (module-level, survives across calls) ──
# Three-layer guard:
#   Layer 1 — Retry-After:  if Telegram returned 429, honour its retry_after window.
#   Layer 2 — Global floor: minimum gap between any two sends (TG_MIN_INTERVAL_SECS).
#   Layer 3 — Per-category: cooldown per alert type to suppress duplicate storms.
#
# category strings used by callers:
#   "equity_breach" | "flash_crash" | "dcb_trip" | "trade_entry" | "trade_close"
#   "express_fill"  | "oracle_nuke" | "startup"  | "shutdown"    | "regime_change"
#   "daily_report"  | "supervisor"  | "generic"
# ────────────────────────────────────────────────────────────────────────────────────
_TG_GLOBAL_LAST_TS:   float              = 0.0    # wall-clock of last successful send
_TG_RETRY_AFTER_TS:   float              = 0.0    # honour Telegram 429 retry_after
_TG_CATEGORY_LAST:    Dict[str, float]   = {}     # last send ts per category

_TG_MIN_INTERVAL_SECS  = _env_float("TG_MIN_INTERVAL_SECS",  30.0)  # global floor
_TG_CATEGORY_COOLDOWN  = _env_float("TG_CATEGORY_COOLDOWN",  60.0)  # per-category dedup


# ── Telegram helper ────────────────────────────────────────────────────────────
async def _send_telegram(
    text: str,
    session: Optional[aiohttp.ClientSession] = None,
    *,
    category: str = "generic",
    priority: bool = False,
) -> None:
    """
    [P11-TG-RL] Rate-limited Telegram send.

    priority=True bypasses layers 2+3 (global floor + category dedup) but still
    respects layer 1 (Retry-After ban) to avoid worsening a 429 ban.
    """
    global _TG_GLOBAL_LAST_TS, _TG_RETRY_AFTER_TS, _TG_CATEGORY_LAST

    if not TELEGRAM_ENABLED:
        return

    now = time.time()

    # Layer 1 — Retry-After: hard block while ban window is active.
    if now < _TG_RETRY_AFTER_TS:
        remaining = _TG_RETRY_AFTER_TS - now
        log.debug(
            "[P11-TG-RL] Telegram blocked by Retry-After (%.0fs remaining) — "
            "category=%s suppressed.", remaining, category
        )
        return

    if not priority:
        # Layer 2 — Global floor: enforce minimum gap between any two sends.
        if (now - _TG_GLOBAL_LAST_TS) < _TG_MIN_INTERVAL_SECS:
            log.debug(
                "[P11-TG-RL] Global rate-limit hit (%.0fs / %.0fs) — "
                "category=%s suppressed.",
                now - _TG_GLOBAL_LAST_TS, _TG_MIN_INTERVAL_SECS, category,
            )
            return

        # Layer 3 — Per-category cooldown: suppress duplicate alert floods.
        cat_last = _TG_CATEGORY_LAST.get(category, 0.0)
        if (now - cat_last) < _TG_CATEGORY_COOLDOWN:
            log.debug(
                "[P11-TG-RL] Category '%s' rate-limit hit (%.0fs / %.0fs) — suppressed.",
                category, now - cat_last, _TG_CATEGORY_COOLDOWN,
            )
            return

    url  = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    body = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    own_session = session is None
    try:
        if own_session:
            session = aiohttp.ClientSession()
        async with session.post(
            url, json=body, timeout=aiohttp.ClientTimeout(total=6)
        ) as resp:
            if resp.status == 200:
                _TG_GLOBAL_LAST_TS     = time.time()
                _TG_CATEGORY_LAST[category] = _TG_GLOBAL_LAST_TS
            elif resp.status == 429:
                # Parse Telegram's retry_after and set the ban window.
                try:
                    body_json = await resp.json(content_type=None)
                    retry_secs = float(
                        (body_json.get("parameters") or {}).get("retry_after", 60)
                    )
                except Exception:
                    retry_secs = 60.0
                _TG_RETRY_AFTER_TS = time.time() + retry_secs
                log.warning(
                    "[P11-TG-RL] Telegram 429 — honouring retry_after=%.0fs "
                    "(ban expires in %.0fs). category=%s",
                    retry_secs, retry_secs, category,
                )
            else:
                raw = await resp.text()
                log.warning("[P11] Telegram send failed: HTTP %d — %s",
                            resp.status, raw)
    except asyncio.TimeoutError:
        log.debug("[P11] Telegram send timed out.")
    except Exception as exc:
        log.debug("[P11] Telegram send error: %s", exc)
    finally:
        if own_session and session and not session.closed:
            await session.close()


# ── [P11-3] State-Change MTF Conflict Logger ──────────────────────────────────
class MtfConflictLogger:
    """
    Tracks the MTF alignment state per symbol and emits log lines only when
    the state *changes*.
    """

    def __init__(self) -> None:
        self._state: Dict[str, bool] = {}

    def update(self, symbol: str, d1: str, d4: str, aligned: bool) -> None:
        prev = self._state.get(symbol)
        if prev == aligned:
            return
        self._state[symbol] = aligned
        if not aligned:
            log.info("MTF Alignment CONFLICT %s: 1H=%s vs 4H=%s → forcing neutral",
                     symbol, d1, d4)
        else:
            log.info("MTF Alignment RESTORED %s: 1H and 4H now agree (%s/%s)",
                     symbol, d1, d4)

    def current_state(self) -> Dict[str, bool]:
        return dict(self._state)


# Module-level singleton imported by brain.py
mtf_conflict_logger = MtfConflictLogger()


# ── [P11-2] Shadow Miss Tracker ────────────────────────────────────────────────
@dataclass
class _ShadowStats:
    symbol: str
    hits:   Deque[bool] = field(
        default_factory=lambda: deque(maxlen=AUTO_TUNE_WINDOW)
    )

    @property
    def miss_rate(self) -> float:
        if not self.hits:
            return 0.0
        return 1.0 - (sum(self.hits) / len(self.hits))

    def record(self, hit: bool) -> None:
        self.hits.append(hit)


class ShadowMissTracker:
    def __init__(self, spread_mgr: "AdaptiveSpread") -> None:
        self._spread = spread_mgr
        self._stats: Dict[str, _ShadowStats] = {}

    def _ensure(self, symbol: str) -> _ShadowStats:
        if symbol not in self._stats:
            self._stats[symbol] = _ShadowStats(symbol)
        return self._stats[symbol]

    def record_hit(self, symbol: str) -> None:
        self._ensure(symbol).record(True)
        log.debug("[P11-2] Shadow HIT recorded for %s", symbol)

    def record_miss(self, symbol: str) -> None:
        stats = self._ensure(symbol)
        stats.record(False)
        log.debug("[P11-2] Shadow MISS recorded for %s (miss_rate=%.1f%%)",
                  symbol, stats.miss_rate * 100)
        self._maybe_autotune(symbol, stats)

    def _maybe_autotune(self, symbol: str, stats: _ShadowStats) -> None:
        if len(stats.hits) < AUTO_TUNE_WINDOW:
            return
        miss_threshold = 1.0 - AUTO_TUNE_WIN_RATE_FLOOR
        if stats.miss_rate <= miss_threshold:
            return
        cur = self._spread.get_offset_bps(symbol)
        new = max(cur - AUTO_TUNE_SPREAD_STEP, SPREAD_MIN_OFFSET_BPS)
        if new < cur:
            self._spread._offsets[symbol] = round(new, 4)
            log.info(
                "[P11-2] AutoTune spread %s: miss_rate=%.1f%% > %.0f%% → "
                "offset %.3f → %.3f bps (more aggressive pricing)",
                symbol, stats.miss_rate * 100, miss_threshold * 100, cur, new,
            )
        else:
            log.debug("[P11-2] AutoTune %s: already at minimum offset (%.3f bps)",
                      symbol, SPREAD_MIN_OFFSET_BPS)

    def report(self) -> Dict[str, dict]:
        return {
            sym: {"miss_rate": round(s.miss_rate, 4), "samples": len(s.hits)}
            for sym, s in self._stats.items()
        }


# ── [P11-4] Equity Protector ───────────────────────────────────────────────────
class EquityProtector:
    def __init__(self) -> None:
        self._window  = DRAWDOWN_WINDOW_HOURS * 3600.0
        self._history: Deque[Tuple[float, float]] = deque()
        self._tripped = False
        self._last_equity: float = 0.0
        self._anomaly_pending: int = 0

    @property
    def tripped(self) -> bool:
        return self._tripped

    def record(self, equity: float) -> bool:
        if self._tripped:
            return True

        # Ignore clearly invalid / transient samples (e.g., $0/$1 during reconnects).
        # This avoids false P11-4 trips from single bad ticks while still allowing
        # real drawdowns to trip after confirmation.
        if not isinstance(equity, (int, float)):
            return False
        if equity <= 0 or equity < P11_MIN_EQUITY_SAMPLE:
            return False

        # Anomaly gate: require a couple of consecutive confirmations before
        # accepting a massive single-sample equity drop.
        if self._last_equity > 0:
            drop_pct = (self._last_equity - equity) / self._last_equity * 100.0
            if drop_pct >= P11_ANOMALY_DROP_PCT:
                self._anomaly_pending += 1
                if self._anomaly_pending < max(1, P11_ANOMALY_CONFIRM_SAMPLES):
                    log.warning(
                        "[P11-4] Equity anomaly sample ignored (pending=%d/%d): "
                        "last=%.2f current=%.2f drop=%.1f%%",
                        self._anomaly_pending,
                        P11_ANOMALY_CONFIRM_SAMPLES,
                        self._last_equity,
                        equity,
                        drop_pct,
                    )
                    return False
            else:
                self._anomaly_pending = 0
        else:
            self._anomaly_pending = 0

        now = time.time()
        self._history.append((now, equity))
        self._last_equity = equity

        cutoff = now - self._window
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()
        if len(self._history) < 2:
            return False

        peak = max(v for _, v in self._history)
        if peak <= 0:
            return False

        dd_pct = (peak - equity) / peak * 100.0
        if dd_pct >= MAX_DRAWDOWN_PCT:
            log.critical(
                "[P11-4] EQUITY PROTECTION TRIPPED: "
                "peak=%.2f current=%.2f drawdown=%.2f%% >= %.2f%%",
                peak, equity, dd_pct, MAX_DRAWDOWN_PCT,
            )
            self._tripped = True
        return self._tripped



# ── [P11-1] Flash-Crash Detector ─────────────────────────────────────────────
class FlashCrashDetector:
    def __init__(self) -> None:
        self._history: Deque[Tuple[float, float]] = deque()
        self._fired   = False

    @property
    def fired(self) -> bool:
        return self._fired

    def observe(self, price: float) -> bool:
        if self._fired:
            return True
        now = time.time()
        self._history.append((now, price))
        cutoff = now - FLASH_CRASH_WINDOW_SECS
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()
        if len(self._history) < 2:
            return False
        peak = max(p for _, p in self._history)
        if peak <= 0:
            return False
        drop = (peak - price) / peak * 100.0
        if drop >= FLASH_CRASH_DROP_PCT:
            log.critical(
                "[P11-1] FLASH CRASH on %s: peak=%.6f current=%.6f "
                "drop=%.2f%% >= %.2f%% in %.0fs window",
                FLASH_CRASH_SYMBOL, peak, price,
                drop, FLASH_CRASH_DROP_PCT, FLASH_CRASH_WINDOW_SECS,
            )
            self._fired = True
        return self._fired

    def reset(self) -> None:
        self._history.clear()
        self._fired = False


# ── [P13-4] Daily Report Scheduler ────────────────────────────────────────────
class DailyReportScheduler:
    """
    Fires a Telegram performance summary every P13_REPORT_INTERVAL_HOURS.

    Metrics gathered:
      - Bayesian trust scores per (symbol, tf) from brain.BayesianTracker
      - Sniper hits (recorded externally via record_sniper_hit())
      - Dead alpha exits (read from executor._p13_dead_alpha_exits)
      - Total positions closed in the window
      - [P16] Express Lane fills in the window
    """

    def __init__(self) -> None:
        self._last_report_ts: float = time.time()
        self._sniper_hits:    int   = 0
        self._closes_today:   int   = 0
        self._p16_fills:      int   = 0          # [P16] express fills this window
        self._interval_secs: float  = P13_REPORT_INTERVAL_HOURS * 3600.0

    def record_sniper_hit(self) -> None:
        self._sniper_hits += 1

    def record_close(self) -> None:
        self._closes_today += 1

    def record_p16_fill(self) -> None:
        """[P16] Increment the Express Lane fill counter for the daily report."""
        self._p16_fills += 1

    def is_due(self) -> bool:
        return (time.time() - self._last_report_ts) >= self._interval_secs

    def _reset_counters(self) -> None:
        self._sniper_hits    = 0
        self._closes_today   = 0
        self._p16_fills      = 0
        self._last_report_ts = time.time()

    def build_report(
        self,
        trust_snapshot:   Dict[str, dict],
        dead_alpha_exits: int,
    ) -> str:
        lines: List[str] = [
            "📊 <b>P13 Daily Report</b>",
            f"Window: {P13_REPORT_INTERVAL_HOURS:.0f}h",
            "",
        ]

        sorted_items = sorted(
            trust_snapshot.items(),
            key=lambda kv: kv[1].get("trust_factor", 1.0),
        )

        for key, info in sorted_items:
            tf_trust  = info.get("trust_factor", 0.5)
            label     = info.get("label", "?")
            samples   = info.get("samples", 0)
            sym_tf    = key.replace(":", "/")
            icon = "🚫" if label == "Censored" else (
                   "⚠️" if label == "Weak"     else (
                   "✅" if label == "Strong"   else "➖"))
            lines.append(
                f"{icon} <code>[{sym_tf}]</code> Trust: "
                f"<b>{tf_trust:.2f}</b> ({label}) | n={samples}"
            )

        lines.append("")
        lines.append(f"🎯 Sniper hits: <b>{self._sniper_hits}</b>")
        lines.append(f"⏱ Dead alpha exits: <b>{dead_alpha_exits}</b>")
        lines.append(f"🔒 Total closes: <b>{self._closes_today}</b>")
        # [P16] Include Express Lane count in the daily report
        lines.append(f"⚡ Express Lane fills: <b>{self._p16_fills}</b>")

        return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# [P44-DCB] Rolling Drawdown Circuit Breaker
# ══════════════════════════════════════════════════════════════════════════════

class RollingDrawdownCB:
    """
    [P44-DCB] Short-window equity velocity circuit breaker.

    Distinct from the existing [P11-4] EquityProtector:

    ┌─────────────────┬────────────────────────┬──────────────────────────────┐
    │ Class            │ Window                 │ Action                       │
    ├─────────────────┼────────────────────────┼──────────────────────────────┤
    │ EquityProtector  │ 24h MAX_DRAWDOWN_PCT   │ Flatten all + full shutdown  │
    │ RollingDrawdownCB│ 1h P44_DCB_FREEZE_PCT  │ FREEZE new entries only      │
    └─────────────────┴────────────────────────┴──────────────────────────────┘

    Mechanism
    ---------
    Each equity sample is pushed into a rolling deque.  On every call to
    record(), the peak equity within the last P44_DCB_WINDOW_HRS is computed
    and compared against the current sample.  When the drop exceeds
    P44_DCB_FREEZE_PCT, the breaker trips:

      • is_frozen → True
      • freeze_until → wall-clock timestamp (now + P44_DCB_COOLDOWN_SECS)
      • All executors stop accepting new entry signals until auto-reset.

    Auto-reset fires when BOTH conditions are true:
      1. wall-clock >= freeze_until
      2. Current equity has recovered above (peak * (1 - P44_DCB_FREEZE_PCT/100))

    A manual reset is possible via reset() for operator override.

    Thread Safety
    -------------
    record() and is_frozen are called from the async sentinel poll loop.
    No cross-thread locking required (single writer, single reader).
    """

    def __init__(self) -> None:
        self._window_secs: float = P44_DCB_WINDOW_HRS * 3600.0
        self._history:     Deque[Tuple[float, float]] = deque()
        self._frozen:      bool  = False
        self._freeze_until: float = 0.0
        self._freeze_peak:  float = 0.0    # peak equity at time of trip
        self._freeze_floor: float = 0.0    # equity level that triggered the freeze
        self._trips:        int   = 0      # lifetime trip counter (for status_snapshot)
        self._log = logging.getLogger("sentinel.dcb")

    # ── Public interface ───────────────────────────────────────────────────────

    @property
    def is_frozen(self) -> bool:
        return self._frozen

    @property
    def freeze_until_ts(self) -> float:
        """Wall-clock timestamp when the freeze expires (0.0 if not frozen)."""
        return self._freeze_until

    @property
    def trip_count(self) -> int:
        return self._trips

    def record(self, equity: float) -> bool:
        """
        Push a new equity sample and evaluate the circuit breaker.

        Returns True when the breaker is (or becomes) frozen.
        Returns False when the breaker is clear and the equity is healthy.

        Caller should check the return value and act accordingly:
            if cb.record(equity):
                executor._dcb_frozen = True
        """
        if not P44_DCB_ENABLE:
            return False
        if equity < P44_DCB_MIN_EQUITY:
            return self._frozen   # Ignore ghost / zero readings

        now = time.time()
        self._history.append((now, equity))
        cutoff = now - self._window_secs
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()

        if self._frozen:
            return self._maybe_unfreeze(equity, now)

        if len(self._history) < 2:
            return False

        peak = max(v for _, v in self._history)
        if peak <= 0:
            return False

        dd_pct = (peak - equity) / peak * 100.0
        if dd_pct >= P44_DCB_FREEZE_PCT:
            self._frozen       = True
            self._freeze_until = now + P44_DCB_COOLDOWN_SECS
            self._freeze_peak  = peak
            self._freeze_floor = equity
            self._trips       += 1
            self._log.critical(
                "[P44-DCB] CIRCUIT BREAKER TRIPPED — "
                "peak=%.2f current=%.2f drop=%.2f%% >= %.2f%% "
                "in %.1fh window | freeze_until=+%.0fs | trip_count=%d",
                peak, equity, dd_pct, P44_DCB_FREEZE_PCT,
                P44_DCB_WINDOW_HRS, P44_DCB_COOLDOWN_SECS, self._trips,
            )
            return True
        return False

    def reset(self) -> None:
        """Manual operator override — clears the freeze immediately."""
        if self._frozen:
            self._log.warning("[P44-DCB] Manual reset called — clearing freeze.")
        self._frozen       = False
        self._freeze_until = 0.0
        self._freeze_peak  = 0.0
        self._freeze_floor = 0.0

    def snapshot(self) -> dict:
        return {
            "enabled":          P44_DCB_ENABLE,
            "frozen":           self._frozen,
            "freeze_until_ts":  self._freeze_until,
            "freeze_remaining": max(0.0, self._freeze_until - time.time()),
            "freeze_peak":      round(self._freeze_peak,  2),
            "freeze_floor":     round(self._freeze_floor, 2),
            "trip_count":       self._trips,
            "window_hrs":       P44_DCB_WINDOW_HRS,
            "freeze_pct":       P44_DCB_FREEZE_PCT,
            "cooldown_secs":    P44_DCB_COOLDOWN_SECS,
        }

    # ── Private ───────────────────────────────────────────────────────────────

    def _maybe_unfreeze(self, equity: float, now: float) -> bool:
        """
        Called when already frozen.  Auto-resets when BOTH:
          1. Cooldown has elapsed (now >= freeze_until).
          2. Equity has recovered above the trigger floor.
        """
        cooldown_done = now >= self._freeze_until
        recovered     = (
            self._freeze_peak <= 0
            or equity >= self._freeze_peak * (1.0 - P44_DCB_FREEZE_PCT / 100.0)
        )
        if cooldown_done and recovered:
            self._log.warning(
                "[P44-DCB] Auto-reset: cooldown elapsed and equity recovered "
                "to %.2f (floor was %.2f | peak was %.2f)",
                equity, self._freeze_floor, self._freeze_peak,
            )
            self.reset()
            return False
        return True   # still frozen


# ══════════════════════════════════════════════════════════════════════════════
# [P44-SUP] Performance Supervisor
# ══════════════════════════════════════════════════════════════════════════════

class PerformanceSupervisor:
    """
    [P44-SUP] Regime-aware win-rate monitor and soft parameter adapter.

    Reads the live IntelligenceEngine.global_win_rate() from each executor's
    brain and applies a two-tier response:

    Tier 1 — Advisory (win_rate < P44_SUP_WARN_FLOOR):
        Sends a Telegram advisory only.  No execution changes.

    Tier 2 — Soft Pause (win_rate < P44_SUP_PAUSE_FLOOR):
        Sets executor._dcb_frozen = True for P44_SUP_PAUSE_SECS to halt new
        entries without touching open positions.  The freeze expires
        automatically.  A Telegram alert is sent on both trip and auto-resume.

    Auto-Resume (win_rate > P44_SUP_RESUME_RATE):
        Clears the supervisor's pause flag and sends a Telegram recovery alert.

    On every poll cycle the Supervisor also writes
    hub_data/supervisor_overrides.json with the current win_rate, regime
    classification, and any active pause state.  The dashboard can read this
    to surface a "⚠️ Supervisor Active" badge.

    Regime Classification
    ---------------------
    The Supervisor reads the most recent signal regime from each executor's
    _status dict (populated by _cycle()) and classifies the market as:
      • "trending"  — ADX proxy: last regime is "bull" or "bear"
      • "choppy"    — last regime is "chop" or "neutral"
      • "unknown"   — not enough data

    Trending markets get a recommendation to widen TRAILING_GAP_PCT.
    Choppy markets get a recommendation to tighten MIN_SIGNAL_CONFIDENCE.
    These are written to supervisor_overrides.json as recommendations, NOT
    applied directly (to avoid touching module-level constants at runtime).
    """

    def __init__(self, executors: List["Executor"]) -> None:
        self._executors        = executors
        self._last_poll_ts:    float  = 0.0
        self._paused:          bool   = False
        self._pause_until:     float  = 0.0
        self._last_win_rate:   float  = 0.5
        self._last_n_trades:   int    = 0
        self._last_regime:     str    = "unknown"
        self._log = logging.getLogger("sentinel.supervisor")
        self._write_lock = threading.Lock()   # protects JSON file writes

    # ── Public interface ───────────────────────────────────────────────────────

    @property
    def is_paused(self) -> bool:
        return self._paused

    def is_due(self) -> bool:
        return (time.time() - self._last_poll_ts) >= P44_SUP_POLL_SECS

    async def tick(self, send_tg_fn) -> Optional[str]:
        """
        Run one Supervisor evaluation cycle.

        Parameters
        ----------
        send_tg_fn : coroutine callable — async send_telegram(text, session)

        Returns
        -------
        Optional[str] — Telegram message text if an alert was sent, else None.
        """
        if not P44_SUP_ENABLE:
            return None
        if not self.is_due():
            return None

        self._last_poll_ts = time.time()

        # ── 1. Collect global win-rate across all executors ──────────────────
        total_w = 0
        total_t = 0
        regime_votes: Dict[str, int] = {}

        for exc in self._executors:
            try:
                brain = getattr(exc, "brain", None)
                if brain is None:
                    continue
                wr, n = brain.global_win_rate(min_trades=P44_SUP_MIN_TRADES)
                total_w += int(round(wr * n))
                total_t += n

                # Regime: read last signal regime from status dict
                exc_regime = (
                    (exc._status or {})
                    .get("p15_oracle_signals", {})
                )
                # Walk oracle signals to tally regime labels
                for sym_data in exc_regime.values():
                    pass  # oracle signals don't carry regime

                # Primary source: most recent position regime
                for pos in getattr(exc, "positions", {}).values():
                    sig = getattr(pos, "entry_signal", None)
                    if sig is not None:
                        r = getattr(sig, "regime", "") or ""
                        if r:
                            regime_votes[r] = regime_votes.get(r, 0) + 1

            except Exception as exc_err:
                self._log.debug("[P44-SUP] Engine read error: %s", exc_err)

        if total_t < P44_SUP_MIN_TRADES:
            self._log.info(
                "[P44-SUP] Insufficient data (%d < %d trades) — skipping cycle.",
                total_t, P44_SUP_MIN_TRADES,
            )
            return None

        win_rate = total_w / total_t if total_t > 0 else 0.5
        self._last_win_rate = win_rate
        self._last_n_trades = total_t

        # ── 2. Classify current market regime ────────────────────────────────
        trending_votes = sum(v for k, v in regime_votes.items()
                             if k.lower() in {"bull", "bear"})
        choppy_votes   = sum(v for k, v in regime_votes.items()
                             if k.lower() in {"chop", "neutral"})
        if trending_votes > choppy_votes:
            regime_class = "trending"
        elif choppy_votes > trending_votes:
            regime_class = "choppy"
        else:
            regime_class = "unknown"
        self._last_regime = regime_class

        # ── 3. Auto-resume if supervisor pause expired ────────────────────────
        msg: Optional[str] = None
        if self._paused and time.time() >= self._pause_until:
            if win_rate >= P44_SUP_RESUME_RATE:
                self._paused = False
                self._log.warning(
                    "[P44-SUP] AUTO-RESUME: win_rate=%.2f%% >= %.0f%% resume threshold",
                    win_rate * 100, P44_SUP_RESUME_RATE * 100,
                )
                msg = (
                    "✅ <b>Supervisor: Entry Freeze Lifted</b>\n"
                    f"Win rate recovered to <code>{win_rate:.1%}</code> "
                    f"({total_t} trades)\n"
                    f"Regime: <code>{regime_class}</code>\n"
                    "New entries now permitted."
                )
                await send_tg_fn(msg)
                self._clear_executor_pauses()
            else:
                # Extend pause if win-rate has not recovered
                self._pause_until = time.time() + P44_SUP_PAUSE_SECS
                self._log.warning(
                    "[P44-SUP] Pause extended: win_rate=%.2f%% still below "
                    "resume threshold %.0f%%",
                    win_rate * 100, P44_SUP_RESUME_RATE * 100,
                )

        # ── 4. Apply tier response ─────────────────────────────────────────────
        elif win_rate < P44_SUP_PAUSE_FLOOR and not self._paused:
            # Tier 2: Soft pause + [P44-APPLY] auto-apply .env mutation
            self._paused      = True
            self._pause_until = time.time() + P44_SUP_PAUSE_SECS
            self._apply_executor_pauses()
            self._log.critical(
                "[P44-SUP] SOFT PAUSE: win_rate=%.2f%% < %.0f%% floor "
                "| pause for %.0fs | n_trades=%d",
                win_rate * 100, P44_SUP_PAUSE_FLOOR * 100,
                P44_SUP_PAUSE_SECS, total_t,
            )
            rec_trailing = "widen TRAILING_GAP_PCT" if regime_class == "trending" else ""
            rec_conf     = "raise MIN_SIGNAL_CONFIDENCE" if regime_class == "choppy"  else ""
            rec_line     = " | ".join(r for r in [rec_trailing, rec_conf] if r) or "monitor closely"
            msg = (
                "🛑 <b>Supervisor: Entry Freeze ACTIVE</b>\n"
                f"Win rate <code>{win_rate:.1%}</code> &lt; "
                f"pause floor <code>{P44_SUP_PAUSE_FLOOR:.0%}</code> "
                f"({total_t} trades)\n"
                f"Regime: <code>{regime_class}</code>\n"
                f"Recommendation: <i>{rec_line}</i>\n"
                f"Auto-resume in <code>{P44_SUP_PAUSE_SECS/60:.0f} min</code> "
                f"if win rate &gt; <code>{P44_SUP_RESUME_RATE:.0%}</code>"
            )
            await send_tg_fn(msg)
            # ── [P44-APPLY] ─────────────────────────────────────────────────
            # Attempt .env mutation and watchdog-restart. This call sends its
            # own Telegram alert and then calls sys.exit(2).  It only returns
            # if P44_APPLY_ENABLE is False or the .env write fails — in either
            # case execution continues normally (soft-pause is still active).
            await self.apply_overrides(win_rate, regime_class, send_tg_fn)
            # ── [/P44-APPLY] ─────────────────────────────────────────────────

        elif win_rate < P44_SUP_WARN_FLOOR and not self._paused:
            # Tier 1: Advisory only
            self._log.warning(
                "[P44-SUP] ADVISORY: win_rate=%.2f%% < warn floor %.0f%% "
                "| n_trades=%d | regime=%s",
                win_rate * 100, P44_SUP_WARN_FLOOR * 100,
                total_t, regime_class,
            )
            msg = (
                "⚠️ <b>Supervisor Advisory</b>\n"
                f"Win rate <code>{win_rate:.1%}</code> below warn threshold "
                f"<code>{P44_SUP_WARN_FLOOR:.0%}</code> "
                f"({total_t} trades)\n"
                f"Regime: <code>{regime_class}</code>\n"
                "<i>No automated action taken. Monitor manually.</i>"
            )
            await send_tg_fn(msg)

        else:
            self._log.info(
                "[P44-SUP] Healthy: win_rate=%.2f%% | n=%d | regime=%s",
                win_rate * 100, total_t, regime_class,
            )

        # ── 5. Write supervisor_overrides.json ───────────────────────────────
        self._write_overrides(win_rate, total_t, regime_class)
        return msg

    def snapshot(self) -> dict:
        return {
            "enabled":        P44_SUP_ENABLE,
            "paused":         self._paused,
            "pause_until_ts": self._pause_until,
            "pause_remaining": max(0.0, self._pause_until - time.time()),
            "last_win_rate":  round(self._last_win_rate, 4),
            "last_n_trades":  self._last_n_trades,
            "last_regime":    self._last_regime,
            "warn_floor":     P44_SUP_WARN_FLOOR,
            "pause_floor":    P44_SUP_PAUSE_FLOOR,
            "resume_rate":    P44_SUP_RESUME_RATE,
            "poll_secs":      P44_SUP_POLL_SECS,
        }

    # ── [P44-APPLY] Auto-Apply: .env mutation + watchdog restart ──────────────

    async def apply_overrides(
        self,
        win_rate:     float,
        regime_class: str,
        send_tg_fn,
    ) -> None:
        """
        [P44-APPLY] Atomically mutate .env and trigger a bot_watchdog.py restart.

        Safe Sequence
        -------------
        1. Guard: skip if P44_APPLY_ENABLE is False (operator opt-out).
        2. Read the live .env file.
        3. Find KELLY_FRACTION; compute new_kelly = max(floor, current × factor).
        4. If new_kelly == current (already at floor) log and return — no restart.
        5. Write the updated .env atomically (tmp → replace).
        6. Append an override record to supervisor_overrides.json for audit trail.
        7. Send a Telegram alert announcing the mutation (BEFORE exit so it fires).
        8. Call sys.exit(2) — bot_watchdog.py catches exit code 2 and restarts.

        The method is intentionally synchronous at the sys.exit(2) call.  Any
        co-routine awaited BEFORE the exit must complete within reasonable time;
        the Telegram send has a 6-second aiohttp timeout built in.

        Returns without exiting only when:
          • P44_APPLY_ENABLE is False.
          • The .env file cannot be read or written.
          • new_kelly is already at or below P44_KELLY_FLOOR (no-op).
        """
        if not P44_APPLY_ENABLE:
            self._log.info(
                "[P44-APPLY] apply_overrides() called but P44_APPLY_ENABLE=0 — skipped."
            )
            return

        self._log.critical(
            "[P44-APPLY] Starting .env override: win_rate=%.2f%% regime=%s "
            "KELLY_REDUCTION_FACTOR=%.2f KELLY_FLOOR=%.3f",
            win_rate * 100, regime_class,
            P44_KELLY_REDUCTION_FACTOR, P44_KELLY_FLOOR,
        )

        # ── 1. Read .env ────────────────────────────────────────────────────────
        env_path = P44_ENV_PATH
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                env_lines = f.readlines()
        except FileNotFoundError:
            self._log.warning(
                "[P44-APPLY] .env not found at %s — override aborted (no restart).",
                env_path,
            )
            return
        except Exception as exc:
            self._log.warning("[P44-APPLY] .env read error: %s — override aborted.", exc)
            return

        # ── 2. Parse current KELLY_FRACTION ────────────────────────────────────
        current_kelly: float = _env_float("KELLY_FRACTION", 0.2)
        try:
            current_kelly = _env_float("KELLY_FRACTION", 0.2)
        except (ValueError, TypeError):
            current_kelly = 0.20

        new_kelly = round(max(P44_KELLY_FLOOR, current_kelly * P44_KELLY_REDUCTION_FACTOR), 6)

        if new_kelly >= current_kelly:
            self._log.info(
                "[P44-APPLY] KELLY_FRACTION already at/below floor (%.4f >= %.4f) "
                "— no .env write, no restart.",
                new_kelly, current_kelly,
            )
            return

        # ── 3. Rewrite KELLY_FRACTION in .env lines ────────────────────────────
        kelly_key = "KELLY_FRACTION"
        new_lines: list = []
        found = False
        for line in env_lines:
            stripped = line.lstrip()
            if stripped.startswith(f"{kelly_key}=") or stripped.startswith(f"{kelly_key} ="):
                new_lines.append(f"{kelly_key}={new_kelly}\n")
                found = True
            else:
                new_lines.append(line)

        if not found:
            # Key doesn't exist — append it so it's explicit.
            new_lines.append(f"\n# [P44-APPLY] Auto-added by Supervisor\n")
            new_lines.append(f"{kelly_key}={new_kelly}\n")

        # ── 4. Atomic write ────────────────────────────────────────────────────
        tmp_env = env_path + ".tmp"
        try:
            with self._write_lock:
                with open(tmp_env, "w", encoding="utf-8") as f:
                    f.writelines(new_lines)
                os.replace(tmp_env, env_path)
        except Exception as exc:
            self._log.error(
                "[P44-APPLY] .env atomic write FAILED: %s — override aborted (no restart).",
                exc,
            )
            try:
                if os.path.exists(tmp_env):
                    os.remove(tmp_env)
            except Exception as _exc:
                log.debug("[SENTINEL] cleanup: %s", _exc)
            return

        self._log.critical(
            "[P44-APPLY] .env updated: KELLY_FRACTION %.4f → %.4f",
            current_kelly, new_kelly,
        )

        # ── 5. Append audit record to supervisor_overrides.json ────────────────
        try:
            override_record = {
                "event":          "apply_overrides",
                # [P44-DASH-COMPAT] Keys below match dashboard reader exactly:
                # render_p44_supervisor reads: ts, old_kelly, new_kelly, win_rate, regime_class
                "ts":             time.time(),        # was "timestamp" — dashboard reads "ts"
                "timestamp":      time.time(),        # keep for audit/backwards compat
                "win_rate":       round(win_rate, 4),
                "regime_class":   regime_class,       # was "regime" — dashboard reads "regime_class"
                "regime":         regime_class,       # keep for backwards compat
                "old_kelly":      current_kelly,      # was "kelly_before" — dashboard reads "old_kelly"
                "new_kelly":      new_kelly,          # was "kelly_after"  — dashboard reads "new_kelly"
                "kelly_before":   current_kelly,      # keep for backwards compat
                "kelly_after":    new_kelly,          # keep for backwards compat
                "reduction_factor": P44_KELLY_REDUCTION_FACTOR,
                "env_path":       env_path,
                "exit_code":      2,
            }
            payload: dict = {}
            if os.path.exists(P44_SUP_OVERRIDES_PATH):
                try:
                    with open(P44_SUP_OVERRIDES_PATH, "r", encoding="utf-8") as f:
                        payload = json.load(f)
                    if not isinstance(payload, dict):
                        payload = {}
                except Exception:
                    payload = {}
            history: list = payload.get("override_history", [])
            history.append(override_record)
            payload["override_history"] = history[-100:]   # keep last 100
            tmp_ov = P44_SUP_OVERRIDES_PATH + ".tmp"
            with self._write_lock:
                os.makedirs(os.path.dirname(P44_SUP_OVERRIDES_PATH), exist_ok=True)
                with open(tmp_ov, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=None, separators=(",", ":"))
                os.replace(tmp_ov, P44_SUP_OVERRIDES_PATH)
        except Exception as audit_exc:
            self._log.warning("[P44-APPLY] Audit append failed (non-fatal): %s", audit_exc)

        # ── 6. Telegram alert (sent BEFORE sys.exit so it fires) ───────────────
        tg_msg = (
            "🔧 <b>Supervisor Auto-Override APPLIED</b>\n"
            f"Win rate: <code>{win_rate:.1%}</code> "
            f"(&lt; floor <code>{P44_SUP_PAUSE_FLOOR:.0%}</code>)\n"
            f"Regime: <code>{regime_class}</code>\n"
            f"<b>KELLY_FRACTION</b>: <code>{current_kelly:.4f}</code> "
            f"→ <code>{new_kelly:.4f}</code> "
            f"(×{P44_KELLY_REDUCTION_FACTOR:.2f})\n"
            f"Bot restarting via watchdog (exit code 2)…"
        )
        try:
            await send_tg_fn(tg_msg)
        except Exception as tg_exc:
            self._log.warning("[P44-APPLY] Telegram alert failed (non-fatal): %s", tg_exc)

        # ── 7. Trigger watchdog restart ────────────────────────────────────────
        self._log.critical(
            "[P44-APPLY] Calling sys.exit(2) — bot_watchdog.py will restart the bot "
            "with new KELLY_FRACTION=%.4f",
            new_kelly,
        )
        sys.exit(2)

    # ── [/P44-APPLY] ──────────────────────────────────────────────────────────

    # ── Private ────────────────────────────────────────────────────────────────

    def _apply_executor_pauses(self) -> None:
        """Set the timed entry-freeze flag on every managed executor."""
        for exc in self._executors:
            try:
                # [P44-SUP] _dcb_frozen is checked by executor._cycle() before
                # any new entry.  It is NOT _vol_guard.emergency_pause (which
                # also cancels open orders); DCB freeze only blocks new entries.
                exc._dcb_frozen       = True
                exc._dcb_frozen_until = self._pause_until
            except Exception as e:
                self._log.debug("[P44-SUP] Could not set freeze on executor: %s", e)

    def _clear_executor_pauses(self) -> None:
        for exc in self._executors:
            try:
                exc._dcb_frozen       = False
                exc._dcb_frozen_until = 0.0
            except Exception as e:
                self._log.debug("[P44-SUP] Could not clear freeze on executor: %s", e)

    def _write_overrides(self, win_rate: float, n_trades: int, regime: str) -> None:
        # [P44-HIST-FIX] Read existing file first to preserve override_history.
        # Previously: every regular tick overwrote the file WITHOUT reading the
        # existing history, so the history panel in the dashboard was always empty
        # (only apply_overrides preserved it). Now both code paths preserve history.
        existing_history: list = []
        with self._write_lock:
            try:
                if os.path.exists(P44_SUP_OVERRIDES_PATH):
                    with open(P44_SUP_OVERRIDES_PATH, "r", encoding="utf-8") as _ef:
                        _existing = json.load(_ef)
                    existing_history = _existing.get("override_history", []) or []
            except Exception as _exc:
                log.warning("[SENTINEL] suppressed: %s", _exc)  # start fresh if file is corrupt

        payload = {
            "timestamp":       time.time(),
            "win_rate":        round(win_rate, 4),
            "n_trades":        n_trades,
            "regime_class":    regime,
            "supervisor_paused": self._paused,
            "pause_until_ts":  self._pause_until,
            "warn_floor":      P44_SUP_WARN_FLOOR,
            "pause_floor":     P44_SUP_PAUSE_FLOOR,
            "resume_rate":     P44_SUP_RESUME_RATE,
            # Preserve history from previous apply_overrides() calls
            "override_history": existing_history[-100:],
            # Regime-aware parameter recommendations (informational — not applied
            # directly to module constants.  Operator can act on these manually
            # or a future P44-FULL cycle can apply them via a hot-reload hook).
            "recommendations": {
                "trailing_gap_pct": (
                    "consider widening" if regime == "trending" else "current ok"
                ),
                "min_signal_conf": (
                    "consider raising" if regime == "choppy" else "current ok"
                ),
            },
        }
        tmp_path = P44_SUP_OVERRIDES_PATH + ".tmp"
        with self._write_lock:
            try:
                os.makedirs(
                    os.path.dirname(P44_SUP_OVERRIDES_PATH), exist_ok=True
                )
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=None, separators=(",", ":"))
                os.replace(tmp_path, P44_SUP_OVERRIDES_PATH)
            except Exception as exc:
                self._log.warning("[P44-SUP] overrides write failed: %s", exc)
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception as _exc:
                    log.debug("[SENTINEL] cleanup: %s", _exc)


# ══════════════════════════════════════════════════════════════════════════════
# The Sentinel
# ══════════════════════════════════════════════════════════════════════════════
class Sentinel:
    """
    Phase 11 + Phase 13 + Phase 14 + Phase 16 monitoring and self-optimisation layer.

    Responsibilities
    ────────────────
    [P11-1] Telegram alerts on regime changes, flash crashes, equity breach.
    [P11-2] Shadow miss auto-tuner via ShadowMissTracker.
    [P11-3] State-change MTF conflict logging via MtfConflictLogger.
    [P11-4] Rolling equity protection with clean-shutdown on breach.
    [P13-4] Daily Bayesian trust report via DailyReportScheduler.
    [P14-4] High Conviction trade alerts — "🔥 High Conviction Signal
            (Consensus Met)" prepended when signal.high_conviction is True.
    [P16-3] Atomic Express Lane fill alerts — ⚡ lightning emoji prefix,
            distinct from regular P14 entry alerts. Includes oracle multiplier
            so the operator can see exactly how large the triggering whale
            trade was relative to the 24-h rolling average.

    Public API
    ──────────
        sentinel.record_sniper_hit()              — after sniper fill
        sentinel.record_close()                   — after every trade exit
        await sentinel.notify_entry(symbol, direction, confidence, signal)
        await sentinel.notify_close(symbol, direction, pnl_pct, tag)
        await sentinel.notify_atomic_fill(...)    — [P16-3] Express Lane fill
        await sentinel.notify_oracle_nuke(symbol) — [P15] Whale Nuke alert
    """

    def __init__(
        self,
        executors: List["Executor"],
        governor:  Optional["PortfolioGovernor"],
        hub,
    ) -> None:
        self._executors = executors
        self._governor  = governor
        self._hub       = hub

        self._flash    = FlashCrashDetector()
        self._equity_p = EquityProtector()

        # ── [P44-DCB] Rolling Drawdown Circuit Breaker ───────────────────────
        self._dcb = RollingDrawdownCB()
        # ── [P44-SUP] Performance Supervisor ─────────────────────────────────
        self._supervisor = PerformanceSupervisor(executors)

        spread_mgr = (
            getattr(executors[0], "_p7_spread_mgr", None)
            if executors else None
        )
        self.shadow_tracker: Optional[ShadowMissTracker] = (
            ShadowMissTracker(spread_mgr) if spread_mgr is not None else None
        )
        if self.shadow_tracker is None:
            log.warning("[P11] ShadowMissTracker disabled: no AdaptiveSpread found on executor.")

        self.mtf_logger = mtf_conflict_logger

        # [P13-4] Daily report scheduler
        self._report_sched = DailyReportScheduler()

        self._last_personality: Optional[str] = None
        self._tg_session: Optional[aiohttp.ClientSession] = None
        self._tasks: List[asyncio.Task] = []
        self._stop_event = asyncio.Event()

        # [P11-4] One-shot guard: equity breach handler fires exactly ONCE per
        # bot lifetime.  Without this flag, EquityProtector.record() returns True
        # forever after _tripped=True and _tick() re-fires _handle_equity_breach()
        # every poll cycle — causing a Telegram 429 flood and repeated flatten calls.
        self._breach_fired: bool = False

        # [P54] Kernel-bypass degradation guard state
        # _p54_ring_consec_high : consecutive ticks at or above CRITICAL threshold
        # _p54_last_total_drops : previous total_drops for delta computation
        # _p54_warn_fired       : TIER-1 one-shot (reset when ring recovers)
        # _p54_critical_fired   : TIER-2 one-shot (prevents repeated Telegram spam)
        self._p54_ring_consec_high:  int  = 0
        self._p54_last_total_drops:  int  = 0
        self._p54_warn_fired:        bool = False
        self._p54_critical_fired:    bool = False

    # ── [P13-4] Public counters ────────────────────────────────────────────────
    def record_sniper_hit(self) -> None:
        self._report_sched.record_sniper_hit()

    def record_close(self) -> None:
        self._report_sched.record_close()

    # ── [P14-4] Trade notification helpers ────────────────────────────────────
    async def notify_entry(
        self,
        symbol:     str,
        direction:  str,
        confidence: float,
        signal:     "Signal",
    ) -> None:
        """
        [P14-4] Sends a Telegram trade-entry alert.

        If signal.high_conviction is True the message is prefixed with the
        🔥 High Conviction marker to distinguish consensus-validated entries
        from ordinary ones.

        Parameters
        ----------
        symbol      : e.g. "BTC"
        direction   : "long" or "short"
        confidence  : final signal confidence (0–1)
        signal      : the Signal dataclass from brain.py
        """
        dir_icon = "🟢" if direction == "long" else "🔴"
        regime   = getattr(signal, "regime", "unknown")
        tf       = getattr(signal, "tf", "?")
        sniper   = getattr(signal, "sniper_boost", False)
        trust    = getattr(signal, "trust_factor", 1.0)
        votes    = getattr(signal, "conviction_votes", 0)

        lines: List[str] = []

        # [P14-4] High conviction header — only when both heuristics agreed
        if getattr(signal, "high_conviction", False):
            lines.append("🔥 <b>High Conviction Signal (Consensus Met)</b>")
            lines.append(
                f"Meta-Judge votes: <b>{votes}/2</b> heuristics aligned"
            )
            lines.append("")

        lines.append(
            f"{dir_icon} <b>ENTRY</b>: <code>{symbol}</code> {direction.upper()}"
        )
        lines.append(f"Regime: <code>{regime}</code> | TF: <code>{tf}</code>")
        lines.append(
            f"Confidence: <code>{confidence:.3f}</code> | "
            f"Trust: <code>{trust:.3f}</code>"
        )
        if sniper:
            lines.append("🎯 <b>Sniper Boost Active</b>")

        msg = "\n".join(lines)
        log.info("[P14-4] Entry notification %s %s high_conviction=%s",
                 symbol, direction, getattr(signal, "high_conviction", False))
        await _send_telegram(msg, self._tg_session, category="trade_entry")

    async def notify_close(
        self,
        symbol:    str,
        direction: str,
        pnl_pct:   float,
        tag:       str,
    ) -> None:
        """
        [P14-4] Sends a Telegram trade-close alert with PnL summary.
        """
        pnl_icon = "✅" if pnl_pct >= 0 else "❌"
        dir_icon = "🟢" if direction == "long" else "🔴"
        msg = (
            f"{pnl_icon} <b>CLOSE</b>: <code>{symbol}</code> "
            f"{dir_icon} {direction.upper()}\n"
            f"PnL: <b>{pnl_pct:+.2f}%</b> | Tag: <code>{tag}</code>"
        )
        log.info("[P14-4] Close notification %s %s pnl=%.2f%%", symbol, direction, pnl_pct)
        await _send_telegram(msg, self._tg_session, category="trade_close")

    # ── [P16-3] Atomic Express Lane fill notification ─────────────────────────
    async def notify_atomic_fill(
        self,
        symbol:      str,
        direction:   str,
        fill_px:     float,
        fill_sz:     float,
        usd_amt:     float,
        oracle_mult: float,
        reason:      str,
    ) -> None:
        """
        [P16-3] Telegram alert for an Institutional Express Lane fill.

        Uses the ⚡ lightning emoji as the leading icon so the operator can
        immediately distinguish these oracle-triggered taker fills from:
          • 🟢/🔴 standard signal entries     (notify_entry)
          • 🔥 high-conviction signal entries  (notify_entry with HC flag)
          • 🐋 whale nuke cancellations        (notify_oracle_nuke)

        Parameters
        ----------
        symbol      : e.g. "BTC"
        direction   : "long" or "short"
        fill_px     : confirmed average fill price
        fill_sz     : confirmed fill size in base currency
        usd_amt     : USD notional value of the trade
        oracle_mult : how many × the rolling 24-h average the whale trade was
        reason      : oracle signal type string (e.g. "WHALE_SWEEP_BUY")
        """
        dir_icon  = "🟢" if direction == "long" else "🔴"
        notional  = fill_sz * fill_px

        lines: List[str] = [
            f"⚡ <b>EXPRESS LANE FILL</b> — Institutional Frontrun",
            f"",
            f"{dir_icon} <code>{symbol}</code> {direction.upper()}",
            f"Fill price:  <code>{fill_px:.6f}</code>",
            f"Fill size:   <code>{fill_sz:.6f}</code>",
            f"Notional:    <code>${notional:,.2f}</code> "
            f"(alloc <code>${usd_amt:,.2f}</code>)",
            f"",
            f"🐋 Oracle trigger: <code>{reason}</code>",
            f"Whale mult:  <b>{oracle_mult:.1f}×</b> rolling 24-h avg",
            f"",
            f"<i>Bypassed: Shadow Limits · Pennying · Post-Only retry · "
            f"5s poll cycle</i>",
        ]

        msg = "\n".join(lines)
        log.warning(
            "[P16-3] Atomic fill notification %s %s px=%.6f sz=%.6f mult=%.1f×",
            symbol, direction, fill_px, fill_sz, oracle_mult,
        )
        await _send_telegram(msg, self._tg_session, category="express_fill")

        # [P16] Track in daily report
        self._report_sched.record_p16_fill()

    # ── [P15] Oracle Nuke notification ────────────────────────────────────────
    async def notify_oracle_nuke(self, symbol: str) -> None:
        """[P15] Telegram Alert for Institutional Whale Nuke (sell-side)."""
        msg = (
            f"🐋 <b>INSTITUTIONAL WHALE DETECTED</b>\n"
            f"<code>{symbol}</code>: Large sweep detected on Coinbase.\n"
            f"⚠️ <b>Action:</b> All pending BUY orders nuked."
        )
        log.warning("[P15] Oracle nuke notification for %s", symbol)
        await _send_telegram(msg, self._tg_session, category="oracle_nuke")

    # ── Lifecycle ──────────────────────────────────────────────────────────────
    async def start(self) -> None:
        # [P44-BOOT-WRITE] Write a boot-state supervisor_overrides.json immediately
        # so the dashboard shows live supervisor state from the first render instead
        # of the "awaiting first supervisor cycle" placeholder.
        #
        # Root cause without this: P44_SUP_POLL_SECS defaults to 3600s and
        # P44_SUP_MIN_TRADES=10 means the first tick returns early without writing
        # the file, then is_due()=False for the next hour — so the file is never
        # created unless 1h passes AND 10 trades execute.
        #
        # The boot write uses the supervisor's default initial state (win_rate=0.5,
        # n_trades=0, regime="unknown") and will be overwritten by the first real
        # tick that has sufficient data.
        try:
            self._supervisor._write_overrides(
                self._supervisor._last_win_rate,   # 0.5 at boot
                self._supervisor._last_n_trades,   # 0
                self._supervisor._last_regime,     # "unknown"
            )
            log.info("[P44-BOOT] Boot-state supervisor_overrides.json written.")
        except Exception as _boot_err:
            log.debug("[P44-BOOT] Boot-state supervisor write skipped: %s", _boot_err)

        if TELEGRAM_ENABLED:
            self._tg_session = aiohttp.ClientSession()
            await _send_telegram(
                "🟢 <b>PowerTrader Phase 44 Sentinel ONLINE</b>\n"
                f"Watching {len(self._executors)} executor(s).\n"
                f"Flash-crash guard: <code>{FLASH_CRASH_SYMBOL}</code> "
                f"drop ≥ {FLASH_CRASH_DROP_PCT:.1f}% / {FLASH_CRASH_WINDOW_SECS:.0f}s\n"
                f"P44 DCB: freeze ≥ {P44_DCB_FREEZE_PCT:.1f}% / "
                f"{P44_DCB_WINDOW_HRS:.1f}h | cooldown {P44_DCB_COOLDOWN_SECS/60:.0f}min\n"
                f"P44 Supervisor: poll {P44_SUP_POLL_SECS/3600:.1f}h | "
                f"warn &lt;{P44_SUP_WARN_FLOOR:.0%} | pause &lt;{P44_SUP_PAUSE_FLOOR:.0%}\n"
                f"Equity protection: drawdown ≥ {MAX_DRAWDOWN_PCT:.1f}% "
                f"/ {DRAWDOWN_WINDOW_HOURS:.0f}h\n"
                f"P13 Trust reports every {P13_REPORT_INTERVAL_HOURS:.0f}h\n"
                "P14 Meta-Judge conviction alerts: ENABLED\n"
                "P16 Express Lane ⚡ alerts: ENABLED",
                self._tg_session,
                category="startup",
                priority=True,
            )
        else:
            log.info(
                "[P11] Telegram disabled (set TELEGRAM_BOT_TOKEN and "
                "TELEGRAM_CHAT_ID in .env to enable)."
            )

        self._tasks = [
            asyncio.create_task(self._poll_loop(), name="p11_sentinel_poll"),
        ]
        log.info(
            "[P11] Sentinel started: poll=%.0fs flash_crash=%s/%.1f%%/%.0fs "
            "equity_dd=%.1f%%/%.0fh telegram=%s p13_report_interval=%.0fh "
            "p14_conviction_alerts=enabled p16_express_alerts=enabled",
            SENTINEL_POLL_SECS,
            FLASH_CRASH_SYMBOL, FLASH_CRASH_DROP_PCT, FLASH_CRASH_WINDOW_SECS,
            MAX_DRAWDOWN_PCT, DRAWDOWN_WINDOW_HOURS,
            "enabled" if TELEGRAM_ENABLED else "disabled",
            P13_REPORT_INTERVAL_HOURS,
        )

    async def stop(self) -> None:
        self._stop_event.set()
        for t in self._tasks:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        if self._tg_session and not self._tg_session.closed:
            await _send_telegram(
                "🔴 <b>PowerTrader Sentinel OFFLINE</b> — bot stopped cleanly.",
                self._tg_session,
                category="shutdown",
                priority=True,
            )
            await self._tg_session.close()
        log.info("[P11] Sentinel stopped.")

    # ── Main poll loop ─────────────────────────────────────────────────────────
    async def _poll_loop(self) -> None:
        log.info("[P11] Sentinel poll loop running.")
        while not self._stop_event.is_set():
            t0 = time.monotonic()
            try:
                await self._tick()
            except asyncio.CancelledError:
                log.info("[P11] Sentinel poll loop cancelled.")
                return
            except Exception as exc:
                log.error("[P11] Sentinel tick error: %s", exc, exc_info=True)
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            if elapsed_ms > 5.0:
                log.debug("[P11] Sentinel tick took %.1f ms", elapsed_ms)
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._stop_event.wait()),
                    timeout=SENTINEL_POLL_SECS,
                )
                break
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        """
        Single monitoring cycle.
        Order: flash-crash → DCB → equity breach → regime change → supervisor → [P13] daily report.
        """
        # ── [P11-1] Flash crash ────────────────────────────────────────────────
        try:
            tick = await self._hub.get_tick(FLASH_CRASH_SYMBOL)
            if tick is not None and tick.last > 0:
                if self._flash.observe(tick.last):
                    await self._handle_flash_crash()
                    return
        except Exception as exc:
            log.debug("[P11] Flash crash tick error: %s", exc)

        # ── [P44-DCB] Rolling Drawdown Circuit Breaker ────────────────────────
        # Evaluated BEFORE the 24h EquityProtector so rapid short-window drops
        # are caught before they reach the harder flatten-all threshold.
        for exc in self._executors:
            try:
                equity = getattr(exc, "_equity", 0.0)
                if equity >= P44_DCB_MIN_EQUITY:
                    was_frozen = self._dcb.is_frozen
                    now_frozen = self._dcb.record(equity)
                    if now_frozen and not was_frozen:
                        await self._handle_dcb_trip(equity)
                    elif was_frozen and not now_frozen:
                        await self._handle_dcb_reset(equity)
                    # Propagate freeze state to executor every tick.
                    exc._dcb_frozen       = now_frozen
                    exc._dcb_frozen_until = (
                        self._dcb.freeze_until_ts if now_frozen else 0.0
                    )
            except Exception as exc_err:
                log.debug("[P44-DCB] DCB tick error: %s", exc_err)
        # ── [/P44-DCB] ────────────────────────────────────────────────────────

        # ── [P11-4] Equity protection ──────────────────────────────────────────
        # [P11-4-GUARD] _breach_fired is a one-shot flag: the breach handler fires
        # exactly ONCE per bot lifetime.  EquityProtector.record() returns True on
        # every subsequent call once _tripped=True, so without this guard the handler
        # would re-fire every poll cycle, flooding Telegram and repeatedly calling
        # _flatten_all().
        if not self._breach_fired:
            for exc in self._executors:
                equity = exc._equity
                if equity > 0 and self._equity_p.record(equity):
                    self._breach_fired = True
                    await self._handle_equity_breach(equity)
                    return
        # ── [/P11-4] ──────────────────────────────────────────────────────────

        # ── [P11-1] Regime change alert ────────────────────────────────────────
        if self._governor is not None:
            try:
                personality = self._governor.execution_mode.value
                if personality != self._last_personality:
                    await self._handle_regime_change(self._last_personality, personality)
                    self._last_personality = personality
            except Exception as exc:
                log.debug("[P11] Regime check error: %s", exc)

        # ── [P44-SUP] Performance Supervisor ──────────────────────────────────
        try:
            await self._supervisor.tick(
                send_tg_fn=lambda text: _send_telegram(text, self._tg_session, category="supervisor")
            )
        except Exception as sup_err:
            log.debug("[P44-SUP] Supervisor tick error: %s", sup_err)
        # ── [/P44-SUP] ────────────────────────────────────────────────────────

        # ── [P54] Kernel-bypass degradation guard ─────────────────────────────
        # Reads bridge.p54_snapshot() injected via sentinel._bridge_client.
        # Fail-closed: any exception is swallowed — guard never halts the bot.
        if P54_SENTINEL_ENABLE:
            try:
                _bridge54 = getattr(self, "_bridge_client", None)
                if _bridge54 is not None and hasattr(_bridge54, "p54_snapshot"):
                    _p54s        = _bridge54.p54_snapshot()
                    _p54_enabled = bool(_p54s.get("enabled", False))
                    _p54_age     = _p54s.get("age_secs")   # None until first event
                    if _p54_enabled and _p54_age is not None:
                        if _p54_age > P54_SENTINEL_STATS_STALE_SECS:
                            log.debug(
                                "[P54] p54_stats stale (%.0fs > %.0fs) — guard skipped.",
                                _p54_age, P54_SENTINEL_STATS_STALE_SECS,
                            )
                        else:
                            _ring54     = float(_p54s.get("ring_util_pct", 0.0))
                            _drops54    = int  (_p54s.get("total_drops",   0))
                            _mode54     = str  (_p54s.get("mode",   "unknown"))
                            _drop_delta = max(0, _drops54 - self._p54_last_total_drops)
                            self._p54_last_total_drops = _drops54

                            # ── TIER-1: recoverable soft-pause ─────────────
                            _t1_ring  = _ring54 >= P54_SENTINEL_RING_WARN_PCT
                            _t1_drops = _drop_delta >= P54_SENTINEL_DROP_WARN
                            if (_t1_ring or _t1_drops) and not self._p54_warn_fired:
                                self._p54_warn_fired = True
                                _t1_msg = (
                                    f"[P54-DEGRADE] Kernel-bypass degradation "
                                    f"mode={_mode54}: "
                                    f"ring={_ring54:.1f}% "
                                    f"(warn={P54_SENTINEL_RING_WARN_PCT}%) "
                                    f"drop_delta={_drop_delta} "
                                    f"(warn={P54_SENTINEL_DROP_WARN}). "
                                    f"Soft-pausing entries."
                                )
                                log.warning("%s", _t1_msg)
                                await _send_telegram(
                                    _t1_msg, self._tg_session,
                                    category="p54_degrade",
                                )
                                for _exc in self._executors:
                                    try:
                                        _exc._p54_soft_pause = True
                                    except Exception as _exc:
                                        log.warning("[SENTINEL] suppressed: %s", _exc)

                            # ── TIER-2: critical ring saturation counter ────
                            if _ring54 >= P54_SENTINEL_RING_CRITICAL_PCT:
                                self._p54_ring_consec_high += 1
                            else:
                                self._p54_ring_consec_high = 0
                                # Ring recovered — lift soft-pause if drops also clear
                                if self._p54_warn_fired and _drop_delta < P54_SENTINEL_DROP_WARN:
                                    self._p54_warn_fired = False
                                    for _exc in self._executors:
                                        try:
                                            _exc._p54_soft_pause = False
                                        except Exception as _exc:
                                            log.warning("[SENTINEL] suppressed: %s", _exc)
                                    log.info(
                                        "[P54] Ring utilisation recovered (%.1f%%) "
                                        "— soft-pause lifted.", _ring54,
                                    )

                            _trip_ring  = (
                                self._p54_ring_consec_high >= P54_SENTINEL_RING_CONSEC
                            )
                            _trip_drops = _drop_delta >= P54_SENTINEL_DROP_TRIP
                            if (_trip_ring or _trip_drops) and not self._p54_critical_fired:
                                self._p54_critical_fired = True
                                _t2_msg = (
                                    f"[P54-CRITICAL] Kernel-bypass critical: "
                                    f"ring={_ring54:.1f}% "
                                    f"(consec={self._p54_ring_consec_high}"
                                    f"/{P54_SENTINEL_RING_CONSEC}) "
                                    f"drop_delta={_drop_delta} mode={_mode54}. "
                                    + ("Calling sys.exit(2) for watchdog restart."
                                       if P54_SENTINEL_EXIT_ON_CRITICAL
                                       else "P54_SENTINEL_EXIT_ON_CRITICAL=0 — monitoring only.")
                                )
                                log.critical("%s", _t2_msg)
                                await _send_telegram(
                                    _t2_msg, self._tg_session,
                                    category="p54_critical",
                                )
                                if P54_SENTINEL_EXIT_ON_CRITICAL:
                                    import sys as _sys54
                                    _sys54.exit(2)
            except SystemExit:
                raise   # let sys.exit(2) propagate cleanly
            except Exception as _p54_guard_err:
                log.debug("[P54] Sentinel guard error (non-fatal): %s", _p54_guard_err)
        # ── [/P54] ────────────────────────────────────────────────────────────

        # ── [P13-4] Daily report ───────────────────────────────────────────────
        if self._report_sched.is_due():
            await self._send_daily_report()

    # ── [P13-4] Daily report sender ───────────────────────────────────────────
    async def _send_daily_report(self) -> None:
        trust_snapshot: Dict[str, dict] = {}
        dead_alpha_exits = 0

        for executor in self._executors:
            try:
                trust_snapshot.update(executor.brain.trust.snapshot())
                dead_alpha_exits += getattr(executor, "_p13_dead_alpha_exits", 0)
            except Exception as exc:
                log.debug("[P13-4] Could not read trust snapshot from executor: %s", exc)

        msg = self._report_sched.build_report(trust_snapshot, dead_alpha_exits)
        log.info("[P13-4] Sending daily trust report:\n%s",
                 msg.replace("<b>", "").replace("</b>", "")
                    .replace("<code>", "").replace("</code>", ""))
        await _send_telegram(msg, self._tg_session, category="daily_report")
        self._report_sched._reset_counters()

    # ── [P11-1] Flash crash handler ────────────────────────────────────────────
    async def _handle_flash_crash(self) -> None:
        log.critical(
            "[P11-1] Flash crash handler: activating emergency pause on "
            "%d executor(s) and cancelling all open buy orders.",
            len(self._executors),
        )
        msg = (
            f"🚨 <b>FLASH CRASH DETECTED — {FLASH_CRASH_SYMBOL}</b>\n"
            f"Drop ≥ <code>{FLASH_CRASH_DROP_PCT:.1f}%</code> "
            f"within <code>{FLASH_CRASH_WINDOW_SECS:.0f}s</code>\n"
            "⚠️ Emergency pause activated.\n"
            "All open buy orders cancelled. No new long entries until resumed."
        )
        await _send_telegram(msg, self._tg_session, category="flash_crash", priority=True)
        for executor in self._executors:
            try:
                executor._vol_guard.emergency_pause = True
                await executor._cancel_all_open_buys()
                log.info("[P11-1] Flash crash: emergency pause set on executor.")
            except Exception as exc:
                log.error("[P11-1] Flash crash handler error: %s", exc, exc_info=True)

    # ── [P44-DCB] Circuit Breaker handlers ────────────────────────────────────
    async def _handle_dcb_trip(self, equity: float) -> None:
        snap = self._dcb.snapshot()
        log.critical(
            "[P44-DCB] Entry freeze TRIPPED: equity=%.2f drop=%.1f%% in %.1fh | "
            "freeze for %.0fs | trip_count=%d",
            equity, snap["freeze_pct"], snap["window_hrs"],
            snap["cooldown_secs"], snap["trip_count"],
        )
        msg = (
            "🔴 <b>P44 Drawdown Circuit Breaker TRIPPED</b>\n"
            f"Equity: <code>${equity:,.2f}</code> "
            f"(peak: <code>${snap['freeze_peak']:,.2f}</code>)\n"
            f"Drop ≥ <code>{snap['freeze_pct']:.1f}%</code> within "
            f"<code>{snap['window_hrs']:.1f}h</code> window\n"
            "⛔ <b>All new entries FROZEN</b>\n"
            f"Auto-reset in <code>{snap['cooldown_secs']/60:.0f} min</code> "
            "if equity recovers."
        )
        await _send_telegram(msg, self._tg_session, category="dcb_trip")

    async def _handle_dcb_reset(self, equity: float) -> None:
        log.warning(
            "[P44-DCB] Entry freeze CLEARED: equity=%.2f recovered", equity
        )
        msg = (
            "✅ <b>P44 Drawdown Circuit Breaker Reset</b>\n"
            f"Equity recovered to <code>${equity:,.2f}</code>\n"
            "New entries are now permitted."
        )
        await _send_telegram(msg, self._tg_session, category="dcb_reset")

    # ── [P11-4] Equity breach handler ─────────────────────────────────────────
    async def _handle_equity_breach(self, equity: float) -> None:
        log.critical("[P11-4] Equity breach handler: flattening all positions.")
        msg = (
            "🛑 <b>EQUITY PROTECTION TRIPPED</b>\n"
            f"Drawdown ≥ <code>{MAX_DRAWDOWN_PCT:.1f}%</code> "
            f"within <code>{DRAWDOWN_WINDOW_HOURS:.0f}h</code>\n"
            f"Current equity: <code>${equity:,.2f}</code>\n"
            "Closing all positions and initiating clean shutdown."
        )
        await _send_telegram(msg, self._tg_session, category="equity_breach", priority=True)
        for executor in self._executors:
            try:
                await executor._flatten_all("p11_equity_protection")
                executor.drawdown._killed = True
                log.info("[P11-4] Equity breach: positions flattened on executor.")
            except Exception as exc:
                log.error("[P11-4] Equity breach flatten error: %s", exc, exc_info=True)

        try:
            # [P11-4-IMPORT] The main module is "main.py", not "main_p16.py".
            # Try the canonical name first; fall back to the legacy alias in case
            # an older deployment still uses it.
            try:
                import main as _main
            except ImportError:
                import main_p16 as _main  # type: ignore[no-redef]
            _main._shutdown_event.set()
            log.info("[P11-4] Shutdown event signalled via main._shutdown_event.")
        except Exception as exc:
            log.warning("[P11-4] Could not signal main shutdown event: %s", exc)

    # ── [P11-1] Regime change alert ────────────────────────────────────────────
    async def _handle_regime_change(self, old: Optional[str], new: str) -> None:
        _icons = {
            "aggressive_iceberg": "🚀",
            "shadow_only":        "👻",
            "neutral":            "⚖️",
        }
        icon      = _icons.get(new, "🔄")
        old_label = old if old is not None else "startup"
        log.info("[P11-1] Regime transition: %s → %s", old_label, new)

        detail = {
            "aggressive_iceberg": (
                "Trending + calm market detected.\n"
                "Iceberg execution active for larger orders."
            ),
            "shadow_only": (
                "High-volatility / choppy market detected.\n"
                "All new entries deferred to shadow limit orders."
            ),
            "neutral": (
                "Mixed market conditions.\n"
                "Standard execution pathway active."
            ),
        }.get(new, "")

        msg = (
            f"{icon} <b>Regime Change</b>\n"
            f"<code>{old_label}</code> → <code>{new}</code>\n"
            f"{detail}"
        )
        await _send_telegram(msg, self._tg_session, category="regime_change")

    # ── Status reporting ───────────────────────────────────────────────────────
    def status_snapshot(self) -> dict:
        return {
            "flash_crash_fired":    self._flash.fired,
            "flash_crash_symbol":   FLASH_CRASH_SYMBOL,
            "equity_tripped":       self._equity_p.tripped,
            "last_regime":          self._last_personality,
            "telegram_enabled":     TELEGRAM_ENABLED,
            "shadow_miss_report":   (
                self.shadow_tracker.report()
                if self.shadow_tracker is not None
                else {}
            ),
            "mtf_conflict_states":  mtf_conflict_logger.current_state(),
            # [P13-4]
            "p13_report_interval_hours":         P13_REPORT_INTERVAL_HOURS,
            "p13_sniper_hits_since_last_report":  self._report_sched._sniper_hits,
            "p13_closes_since_last_report":       self._report_sched._closes_today,
            "p13_next_report_in_secs": max(
                0.0,
                self._report_sched._interval_secs
                - (time.time() - self._report_sched._last_report_ts),
            ),
            # [P14-4]
            "p14_conviction_alerts_enabled": True,
            # [P16-3]
            "p16_express_alerts_enabled":    True,
            "p16_express_fills_this_window": self._report_sched._p16_fills,
            # [P44-DCB] Rolling Drawdown Circuit Breaker
            "p44_dcb": self._dcb.snapshot(),
            # [P44-SUP] Performance Supervisor
            "p44_supervisor": self._supervisor.snapshot(),
            # [P40.5] IPC Bridge Heartbeat — bridge reference is optional; populated
            # by main.py after construction via sentinel._bridge_client = bridge.
            "p40_5_heartbeat": (
                self._bridge_client.heartbeat_snapshot()
                if getattr(self, "_bridge_client", None) is not None
                else {"enabled": False, "note": "bridge not wired to sentinel"}
            ),
            # [P54] Kernel-bypass degradation guard live state
            "p54_guard": {
                "enabled":            P54_SENTINEL_ENABLE,
                "ring_warn_pct":      P54_SENTINEL_RING_WARN_PCT,
                "ring_critical_pct":  P54_SENTINEL_RING_CRITICAL_PCT,
                "ring_consec_thresh": P54_SENTINEL_RING_CONSEC,
                "drop_warn":          P54_SENTINEL_DROP_WARN,
                "drop_trip":          P54_SENTINEL_DROP_TRIP,
                "exit_on_critical":   P54_SENTINEL_EXIT_ON_CRITICAL,
                "ring_consec_high":   self._p54_ring_consec_high,
                "last_total_drops":   self._p54_last_total_drops,
                "warn_fired":         self._p54_warn_fired,
                "critical_fired":     self._p54_critical_fired,
            },
        }
