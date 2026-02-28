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
import logging
import os
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

# ── Config ─────────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN       = os.environ.get("TELEGRAM_BOT_TOKEN",       "YOUR_BOT_TOKEN")
TELEGRAM_CHAT_ID         = os.environ.get("TELEGRAM_CHAT_ID",         "YOUR_CHAT_ID")
TELEGRAM_ENABLED         = (
    TELEGRAM_BOT_TOKEN != "YOUR_BOT_TOKEN"
    and TELEGRAM_CHAT_ID != "YOUR_CHAT_ID"
    and bool(TELEGRAM_BOT_TOKEN)
    and bool(TELEGRAM_CHAT_ID)
)

FLASH_CRASH_DROP_PCT     = float(os.environ.get("FLASH_CRASH_DROP_PCT",     "5.0"))
FLASH_CRASH_WINDOW_SECS  = float(os.environ.get("FLASH_CRASH_WINDOW_SECS",  "30.0"))
FLASH_CRASH_SYMBOL       = os.environ.get("P11_FLASH_CRASH_SYMBOL",         "BTC")

AUTO_TUNE_WIN_RATE_FLOOR = float(os.environ.get("AUTO_TUNE_WIN_RATE_FLOOR",  "0.40"))
AUTO_TUNE_WINDOW         = int  (os.environ.get("AUTO_TUNE_WINDOW",          "5"))
AUTO_TUNE_SPREAD_STEP    = float(os.environ.get("P11_SPREAD_TIGHTEN_BPS",    "0.25"))
SPREAD_MIN_OFFSET_BPS    = float(os.environ.get("P7_SPREAD_MIN_OFFSET_BPS",  "0.5"))

MAX_DRAWDOWN_PCT         = float(os.environ.get("MAX_DRAWDOWN_PCT",          "15.0"))
DRAWDOWN_WINDOW_HOURS    = float(os.environ.get("DRAWDOWN_WINDOW_HOURS",     "24.0"))

SENTINEL_POLL_SECS       = float(os.environ.get("P11_SENTINEL_POLL_SECS",    "5.0"))

# ── [P13-4] Daily report config ────────────────────────────────────────────────
P13_REPORT_INTERVAL_HOURS = float(os.environ.get("P13_REPORT_INTERVAL_HOURS", "24.0"))


# ── Telegram helper ────────────────────────────────────────────────────────────
async def _send_telegram(
    text: str,
    session: Optional[aiohttp.ClientSession] = None,
) -> None:
    if not TELEGRAM_ENABLED:
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
            if resp.status != 200:
                log.warning("[P11] Telegram send failed: HTTP %d — %s",
                            resp.status, await resp.text())
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

    @property
    def tripped(self) -> bool:
        return self._tripped

    def record(self, equity: float) -> bool:
        if self._tripped:
            return True
        now = time.time()
        self._history.append((now, equity))
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
        await _send_telegram(msg, self._tg_session)

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
        await _send_telegram(msg, self._tg_session)

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
        await _send_telegram(msg, self._tg_session)

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
        await _send_telegram(msg, self._tg_session)

    # ── Lifecycle ──────────────────────────────────────────────────────────────
    async def start(self) -> None:
        if TELEGRAM_ENABLED:
            self._tg_session = aiohttp.ClientSession()
            await _send_telegram(
                "🟢 <b>PowerTrader Phase 16 Sentinel ONLINE</b>\n"
                f"Watching {len(self._executors)} executor(s).\n"
                f"Flash-crash guard: <code>{FLASH_CRASH_SYMBOL}</code> "
                f"drop ≥ {FLASH_CRASH_DROP_PCT:.1f}% / {FLASH_CRASH_WINDOW_SECS:.0f}s\n"
                f"Equity protection: drawdown ≥ {MAX_DRAWDOWN_PCT:.1f}% "
                f"/ {DRAWDOWN_WINDOW_HOURS:.0f}h\n"
                f"P13 Trust reports every {P13_REPORT_INTERVAL_HOURS:.0f}h\n"
                "P14 Meta-Judge conviction alerts: ENABLED\n"
                "P16 Express Lane ⚡ alerts: ENABLED",
                self._tg_session,
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
        Order: flash-crash → equity breach → regime change → [P13] daily report.
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

        # ── [P11-4] Equity protection ──────────────────────────────────────────
        for exc in self._executors:
            equity = exc._equity
            if equity > 0 and self._equity_p.record(equity):
                await self._handle_equity_breach(equity)
                return

        # ── [P11-1] Regime change alert ────────────────────────────────────────
        if self._governor is not None:
            try:
                personality = self._governor.execution_mode.value
                if personality != self._last_personality:
                    await self._handle_regime_change(self._last_personality, personality)
                    self._last_personality = personality
            except Exception as exc:
                log.debug("[P11] Regime check error: %s", exc)

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
        await _send_telegram(msg, self._tg_session)
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
        await _send_telegram(msg, self._tg_session)
        for executor in self._executors:
            try:
                executor._vol_guard.emergency_pause = True
                await executor._cancel_all_open_buys()
                log.info("[P11-1] Flash crash: emergency pause set on executor.")
            except Exception as exc:
                log.error("[P11-1] Flash crash handler error: %s", exc, exc_info=True)

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
        await _send_telegram(msg, self._tg_session)
        for executor in self._executors:
            try:
                await executor._flatten_all("p11_equity_protection")
                executor.drawdown._killed = True
                log.info("[P11-4] Equity breach: positions flattened on executor.")
            except Exception as exc:
                log.error("[P11-4] Equity breach flatten error: %s", exc, exc_info=True)

        try:
            import main_p16 as _main
            _main._shutdown_event.set()
            log.info("[P11-4] Shutdown event signalled via main_p16._shutdown_event.")
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
        await _send_telegram(msg, self._tg_session)

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
        }
