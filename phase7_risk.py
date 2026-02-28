"""
phase7_risk.py  —  Phase 7: Professional Risk Management Layer
==============================================================

Components:
  1. CircuitBreaker  — Monitors real-time hourly drawdown from the DB.
                       Flips emergency_pause in the status JSON and sets an
                       asyncio.Event that the Executor can await/check.

  2. KellySizer      — Dynamic position sizing that blends the base Kelly
                       fraction with a macro sentiment score.

Bug-fix changelog (this revision):
  [FIX-DB-NONE]  _read_snapshots() now returns [] on ANY exception path,
                 including when the table does not yet exist (training mode /
                 cold-start before the first DB write).  Columns are validated
                 individually so a NULL cell never produces a TypeError.

  [FIX-CHECK-NONE]  _check() has explicit None/zero guards on every arithmetic
                 path.  No subtraction or division is performed on a value that
                 could be None.  Returns 0.0 (not None) in every early-exit.

  [FIX-METRIC-INIT]  last_drawdown_pct, peak_equity, latest_equity are
                 initialised to 0.0 (float) at construction, not left
                 un-initialised.  This prevents AttributeError if status is
                 read before the first monitor tick.

  [FIX-SCORE-TUPLE]  _get_score() now wraps every code path in try/except and
                 is guaranteed to return Tuple[float, str] — never None,
                 never a bare float.  Unpacking can never raise TypeError.

  [FIX-OR-PARSE]  _fetch_or_score() validates that 'choices' is non-empty
                 before indexing, and that 'content' is a non-None str before
                 stripping.

  [FIX-LOCK-REENTRANT]  _check() reads self._is_tripped / self.tripped_at
                 under the lock into local variables, then releases the lock
                 BEFORE calling _trip() or _reset() — asyncio.Lock is
                 non-reentrant and a second acquisition in the same coroutine
                 would deadlock.  (Preserves original FIX-1 intent, now also
                 guards the new None-check paths.)

  [P23-GHOST-CB]  _check() now reads trader_status.json at the top of every
                 monitor cycle.  If the key "equity_is_ghost" is True, the
                 entire drawdown computation is skipped and 0.0 is returned.
                 This prevents the CircuitBreaker from tripping on a transient
                 WebSocket zero-equity reading rather than real loss.

All earlier fixes (FIX-1 through FIX-10, NEW-8) are preserved.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Optional, Tuple

try:
    from openai import AsyncOpenAI        # type: ignore[import]
    _OPENAI_SDK_AVAILABLE = True
except ImportError:
    AsyncOpenAI = None                    # type: ignore[assignment,misc]
    _OPENAI_SDK_AVAILABLE = False

log = logging.getLogger("phase7_risk")

# ── Config ─────────────────────────────────────────────────────────────────────

CB_HOURLY_DRAWDOWN_PCT  = float(os.environ.get("P7_CB_HOURLY_DRAWDOWN_PCT",  "5.0"))
CB_MONITOR_INTERVAL     = float(os.environ.get("P7_CB_MONITOR_INTERVAL",    "15.0"))
CB_LOOKBACK_SECS        = float(os.environ.get("P7_CB_LOOKBACK_SECS",      "3600.0"))
CB_RESET_AFTER_SECS     = float(os.environ.get("P7_CB_RESET_AFTER_SECS",   "1800.0"))

CB_MIN_VALID_EQUITY     = float(os.environ.get("P7_CB_MIN_VALID_EQUITY",    "10.0"))
CB_MAX_SINGLE_DROP_PCT  = float(os.environ.get("P7_CB_MAX_SINGLE_DROP_PCT", "50.0"))
CB_GRACE_SECS           = float(os.environ.get("P7_CB_GRACE_SECS",          "60.0"))

KELLY_BULL_THRESHOLD  = float(os.environ.get("P7_KELLY_BULL_THRESHOLD",  "0.3"))
KELLY_BEAR_THRESHOLD  = float(os.environ.get("P7_KELLY_BEAR_THRESHOLD", "-0.3"))
KELLY_BULL_MULTIPLIER = float(os.environ.get("P7_KELLY_BULL_MULTIPLIER", "1.40"))
KELLY_BEAR_FLOOR      = float(os.environ.get("P7_KELLY_BEAR_FLOOR",      "0.50"))
KELLY_PANIC_BLOCK     = float(os.environ.get("P7_KELLY_PANIC_BLOCK",    "-0.50"))

OPENROUTER_API_KEY  = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.environ.get(
    "OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"
)
OPENROUTER_MODEL    = os.environ.get(
    "OPENROUTER_MODEL",
    "google/gemini-2.0-flash-lite-preview-02-05:free",
)
OPENROUTER_TIMEOUT   = float(os.environ.get("OPENROUTER_TIMEOUT",   "8.0"))
OPENROUTER_CACHE_TTL = float(os.environ.get("OPENROUTER_CACHE_TTL", "60.0"))
KELLY_NW_CACHE_TTL   = float(os.environ.get("P7_KELLY_NW_CACHE_TTL", "30.0"))

# Minimum equity value that is considered a "real" reading for HWM purposes.
# Anything at or below this is treated as an uninitialised/transient value and
# skipped in all drawdown arithmetic.  Matches CB_MIN_VALID_EQUITY so the two
# systems agree on what constitutes a valid equity reading.
CB_HWM_MIN_EQUITY = CB_MIN_VALID_EQUITY


# ══════════════════════════════════════════════════════════════════════════════
# 1. CircuitBreaker
# ══════════════════════════════════════════════════════════════════════════════

class CircuitBreaker:
    """
    Monitors real-time hourly drawdown by reading equity snapshots from SQLite.

    State machine:
      NORMAL  ──[dd ≥ threshold]──▶  TRIPPED  ──[cooldown elapsed]──▶  NORMAL

    All state mutations are protected by a single asyncio.Lock.  The lock is
    released before any delegation to _trip() / _reset() (non-reentrant).
    """

    def __init__(self, db, status_path: str) -> None:
        self._db_path: str     = db._path if hasattr(db, "_path") else str(db)
        self._status_path: str = status_path

        self._lock = asyncio.Lock()

        self._is_tripped: bool = False
        self.tripped_at: float = 0.0

        self.trip_event = asyncio.Event()

        # [FIX-METRIC-INIT] Always floats, never None.
        self.last_drawdown_pct: float = 0.0
        self.peak_equity:       float = 0.0
        self.latest_equity:     float = 0.0

        self._start_time: float = time.time()

        log.info(
            "[P7] CircuitBreaker created — threshold=%.1f%% lookback=%.0fs "
            "interval=%.0fs reset_cooldown=%.0fs grace=%.0fs "
            "min_valid_equity=$%.2f max_single_drop=%.0f%%",
            CB_HOURLY_DRAWDOWN_PCT, CB_LOOKBACK_SECS, CB_MONITOR_INTERVAL,
            CB_RESET_AFTER_SECS, CB_GRACE_SECS,
            CB_MIN_VALID_EQUITY, CB_MAX_SINGLE_DROP_PCT,
        )

    # ── [P23-GHOST-CB] Status-file ghost flag reader ───────────────────────────

    def _is_equity_ghost(self) -> bool:
        """
        [P23-GHOST-CB] Returns True if trader_status.json explicitly marks
        the current equity reading as a WebSocket ghost (zero-equity lag).

        When True, _check() must skip ALL drawdown calculations and return
        0.0 immediately — the circuit breaker must NEVER trip on a ghost read.

        Reads synchronously; safe to call from a thread-pool executor.
        Silently returns False on any I/O or parse error so normal operation
        is never blocked by a missing or malformed status file.
        """
        try:
            with open(self._status_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return bool(data.get("equity_is_ghost", False))
        except Exception:
            return False

    # ── Property: is_tripped ──────────────────────────────────────────────────

    @property
    def is_tripped(self) -> bool:
        return self._is_tripped

    @is_tripped.setter
    def is_tripped(self, value: bool) -> None:
        self._is_tripped = bool(value)
        if not self._is_tripped:
            self.trip_event.clear()

    # ── Public: force_reset() ─────────────────────────────────────────────────

    async def force_reset(self) -> None:
        await self._reset(reason="force_reset() called by orchestrator")

    # ── DB read ───────────────────────────────────────────────────────────────

    def _read_snapshots(self) -> list:
        """
        Returns [(ts: float, total_equity: float), ...] for rows within
        CB_LOOKBACK_SECS, filtered to valid equity values only, sorted
        oldest-first.

        [FIX-DB-NONE]  Handles ALL failure modes gracefully:
          • Table does not yet exist (training mode / fresh DB) → returns [].
          • Any cell value is None → that row is skipped.
          • Any exception (SQLITE_BUSY, corruption, missing file) → returns [].
          • Connection is always closed in the finally block.
        """
        cutoff = time.time() - CB_LOOKBACK_SECS
        conn: Optional[sqlite3.Connection] = None
        try:
            conn = sqlite3.connect(
                self._db_path, check_same_thread=False, timeout=30.0
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")

            # Check whether the table exists before querying it.
            # During training / cold-start the schema may not have been
            # created yet, which would raise OperationalError.
            tbl_check = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='account_snapshots'"
            ).fetchone()
            if tbl_check is None:
                log.debug(
                    "[P7] CircuitBreaker: 'account_snapshots' table not yet "
                    "created — returning empty snapshot list."
                )
                return []

            rows = conn.execute(
                "SELECT ts, total_equity "
                "FROM account_snapshots "
                "WHERE ts >= ? "
                "ORDER BY ts ASC",
                (cutoff,),
            ).fetchall()

            valid: list = []
            for row in rows:
                # [FIX-DB-NONE] Validate both columns individually.
                ts_raw  = row[0]
                eq_raw  = row[1]
                if ts_raw is None or eq_raw is None:
                    continue
                try:
                    ts_f  = float(ts_raw)
                    eq_f  = float(eq_raw)
                except (TypeError, ValueError):
                    continue
                # [FIX-6a] Discard sub-threshold readings.
                if eq_f < CB_MIN_VALID_EQUITY:
                    continue
                valid.append((ts_f, eq_f))

            return valid

        except Exception as exc:
            log.warning("[P7] CircuitBreaker: DB read error — %s", exc)
            return []
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    # ── Core drawdown check ───────────────────────────────────────────────────

    async def _check(self) -> float:
        """
        Reads equity history, validates it, computes drawdown, and trips /
        resets the breaker accordingly.

        ALWAYS returns float (0.0 on empty DB, error, suppressed check, or
        any arithmetic failure).  Never raises.  Never returns None.

        [FIX-CHECK-NONE]  Every subtraction and division is guarded:
          • rows is empty → return 0.0 immediately.
          • peak is 0 or None → return 0.0 (avoids ZeroDivisionError).
          • latest is None → treated as 0.0 with an early-exit.
          • All metrics stored as float, not Optional[float].

        [FIX-LOCK-REENTRANT]  Reads state under lock into locals, then
        releases the lock before calling _trip() / _reset().
        """
        # ── [P23-GHOST-CB] Ghost-state suppression ────────────────────────────
        # If the status file reports equity_is_ghost=True the executor has
        # already detected a transient WS zero-equity reading and tagged it.
        # Skip ALL drawdown logic to prevent a false CB trip.
        loop = asyncio.get_running_loop()
        is_ghost = await loop.run_in_executor(None, self._is_equity_ghost)
        if is_ghost:
            log.debug(
                "[P23-GHOST-CB] CircuitBreaker._check suppressed — "
                "equity_is_ghost=True in status file; skipping drawdown calc."
            )
            return 0.0

        # ── Grace window ──────────────────────────────────────────────────────
        elapsed_since_start = time.time() - self._start_time
        if elapsed_since_start < CB_GRACE_SECS:
            log.debug(
                "[P7] CircuitBreaker: grace window active (%.1f s remaining).",
                CB_GRACE_SECS - elapsed_since_start,
            )
            return 0.0

        # ── Fetch rows (non-blocking) ─────────────────────────────────────────
        rows = await loop.run_in_executor(None, self._read_snapshots)

        # [FIX-CHECK-NONE] Guard 1: empty result set.
        if not rows:
            return 0.0

        # ── [FIX-6b] Single-sample anomaly guard ──────────────────────────────
        if len(rows) >= 2:
            prev_eq   = rows[-2][1]
            latest_eq = rows[-1][1]
            # Both are validated floats from _read_snapshots, but double-check.
            if (
                prev_eq is not None
                and latest_eq is not None
                and isinstance(prev_eq, float)
                and isinstance(latest_eq, float)
                and prev_eq > 0.0
            ):
                single_drop_pct = (prev_eq - latest_eq) / prev_eq * 100.0
                if single_drop_pct > CB_MAX_SINGLE_DROP_PCT:
                    log.warning(
                        "[P7] CircuitBreaker: single-sample drop %.2f%% "
                        "($%.2f → $%.2f) exceeds anomaly threshold %.0f%% "
                        "— skipping cycle.",
                        single_drop_pct, prev_eq, latest_eq,
                        CB_MAX_SINGLE_DROP_PCT,
                    )
                    return 0.0

        # ── Compute peak and drawdown ─────────────────────────────────────────
        equities = [eq for _, eq in rows if eq is not None and isinstance(eq, float)]

        # [FIX-CHECK-NONE] Guard 2: all rows filtered out.
        if not equities:
            return 0.0

        peak   = max(equities)
        latest = equities[-1]

        # [FIX-CHECK-NONE] Guard 3: zero or negative peak — can't divide.
        if peak <= 0.0:
            return 0.0

        # [FIX-CHECK-NONE] Guard 4: latest is non-positive (corrupted row).
        if latest <= 0.0:
            return 0.0

        # [GHOST-STATE] Guard 5: if latest equity is below the valid floor and
        # peak is significantly higher, this is almost certainly a ghost DB
        # snapshot that slipped through.  Suppress the CB trip.
        if latest < CB_MIN_VALID_EQUITY and peak > CB_MIN_VALID_EQUITY:
            log.warning(
                "[GHOST-STATE] CircuitBreaker._check: "
                "latest=%.4f < min_valid=%.2f while peak=%.4f — "
                "suspected ghost DB snapshot; skipping CB trip this cycle.",
                latest, CB_MIN_VALID_EQUITY, peak,
            )
            return 0.0

        drawdown_pct = (peak - latest) / peak * 100.0

        # ── Update live metrics under lock ───────────────────────────────────
        # [FIX-LOCK-REENTRANT] Capture state into locals, then drop the lock
        # BEFORE calling _trip() / _reset().
        async with self._lock:
            self.last_drawdown_pct = round(float(drawdown_pct), 4)
            self.peak_equity       = round(float(peak),         4)
            self.latest_equity     = round(float(latest),       4)
            currently_tripped   = self._is_tripped
            tripped_at_snapshot = self.tripped_at

        # ── State machine (lock NOT held) ─────────────────────────────────────
        if drawdown_pct >= CB_HOURLY_DRAWDOWN_PCT and not currently_tripped:
            await self._trip(peak, latest, drawdown_pct)
        elif currently_tripped:
            elapsed = time.time() - (tripped_at_snapshot or 0.0)
            if elapsed >= CB_RESET_AFTER_SECS:
                await self._reset(
                    reason=(
                        f"cooldown expired ({elapsed:.0f}s "
                        f"≥ {CB_RESET_AFTER_SECS:.0f}s)"
                    )
                )

        return round(float(drawdown_pct), 4)

    # ── Trip ──────────────────────────────────────────────────────────────────

    async def _trip(self, peak: float, latest: float, dd_pct: float) -> None:
        async with self._lock:
            if self._is_tripped:
                return
            self._is_tripped = True
            self.tripped_at  = time.time()
            self.trip_event.set()   # [FIX-3] set inside lock

        log.critical(
            "🛑 [P7] CircuitBreaker TRIPPED — "
            "hourly drawdown=%.2f%% (peak=$%.2f → latest=$%.2f) "
            "threshold=%.1f%%",
            dd_pct, peak, latest, CB_HOURLY_DRAWDOWN_PCT,
        )
        await self._patch_status(emergency_pause=True, circuit_breaker=True)

    # ── Reset ─────────────────────────────────────────────────────────────────

    async def _reset(self, reason: str = "cooldown elapsed") -> None:
        async with self._lock:
            if not self._is_tripped:
                return
            self._is_tripped = False
            self.tripped_at  = 0.0
            self.trip_event.clear()

        log.info("[P7] CircuitBreaker RESET — %s.", reason)
        await self._patch_status(emergency_pause=False, circuit_breaker=False)

    # ── Atomic JSON patch ─────────────────────────────────────────────────────

    async def _patch_status(self, emergency_pause: bool, circuit_breaker: bool) -> None:
        status_path = self._status_path
        last_dd     = float(self.last_drawdown_pct)
        peak_eq     = float(self.peak_equity)
        latest_eq   = float(self.latest_equity)
        tripped_at  = float(self.tripped_at)

        def _write_sync() -> None:
            tmp = status_path + ".tmp"
            try:
                try:
                    with open(status_path, "r", encoding="utf-8") as fh:
                        status: dict = json.load(fh)
                except (FileNotFoundError, json.JSONDecodeError):
                    status = {}

                status["emergency_pause"]  = emergency_pause
                status["circuit_breaker"]  = circuit_breaker
                status["p7_drawdown_pct"]  = round(last_dd,   4)
                status["p7_peak_equity"]   = round(peak_eq,   4)
                status["p7_latest_equity"] = round(latest_eq, 4)
                status["p7_tripped_at"]    = tripped_at

                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(status, fh, indent=2)
                    fh.flush()
                    os.fsync(fh.fileno())

                os.replace(tmp, status_path)

            except Exception as exc:
                log.error("[P7] CircuitBreaker: failed to patch status JSON — %s", exc)
                try:
                    os.unlink(tmp)
                except OSError:
                    pass

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _write_sync)

    # ── Background monitor loop ───────────────────────────────────────────────

    async def monitor_loop(self) -> None:
        log.info(
            "[P7] CircuitBreaker monitor started — "
            "threshold=%.1f%% lookback=%.0fs interval=%.0fs grace=%.0fs",
            CB_HOURLY_DRAWDOWN_PCT, CB_LOOKBACK_SECS,
            CB_MONITOR_INTERVAL, CB_GRACE_SECS,
        )
        while True:
            try:
                dd: float = await self._check()
                log.debug(
                    "[P7] CB check: drawdown=%.4f%% tripped=%s "
                    "peak=$%.2f latest=$%.2f",
                    dd, self.is_tripped,
                    self.peak_equity, self.latest_equity,
                )
            except Exception as exc:
                log.error(
                    "[P7] CircuitBreaker monitor unexpected error: %s", exc
                )
            await asyncio.sleep(CB_MONITOR_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
# 2. SentimentScale
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SentimentScale:
    raw_macro_score: float
    multiplier:      float
    adjusted_alloc:  float
    blocked:         bool
    label:           str
    score_source:    str = field(default="newswire")


# ══════════════════════════════════════════════════════════════════════════════
# 3. KellySizer
# ══════════════════════════════════════════════════════════════════════════════

class KellySizer:
    """
    Adjusts the base Kelly fraction using a macro sentiment score.

    Score acquisition (two-tier):
      1. NewsWire.get_macro_bias()  — fast, always tried first.
      2. OpenRouter LLM             — fallback when NewsWire unavailable.

    [FIX-SCORE-TUPLE]  _get_score() is now guaranteed to return
    Tuple[float, str] in every code path, including exception paths.
    Callers can always do:
        score, source = await sizer._get_score()
    without risk of TypeError on unpacking.
    """

    def __init__(self, news_wire) -> None:
        self._nw = news_wire

        self._nw_score:    float = 0.0
        self._nw_cache_ts: float = 0.0

        self._or_score:    float = 0.0
        self._or_cache_ts: float = 0.0

        self._or_client: Optional["AsyncOpenAI"] = None

        self._or_enabled: bool = _OPENAI_SDK_AVAILABLE and bool(OPENROUTER_API_KEY)
        if not _OPENAI_SDK_AVAILABLE:
            log.warning(
                "[P7] KellySizer: openai SDK not installed — OpenRouter "
                "fallback disabled.  pip install openai>=1.0.0"
            )
        elif not OPENROUTER_API_KEY:
            log.warning(
                "[P7] KellySizer: OPENROUTER_API_KEY not set — "
                "OpenRouter fallback disabled."
            )
        else:
            log.info(
                "[P7] KellySizer: OpenRouter fallback enabled "
                "(model=%s  timeout=%.1fs  cache_ttl=%.0fs)",
                OPENROUTER_MODEL, OPENROUTER_TIMEOUT, OPENROUTER_CACHE_TTL,
            )

    def _get_or_client(self) -> "AsyncOpenAI":
        if self._or_client is None:
            self._or_client = AsyncOpenAI(          # type: ignore[misc]
                api_key  = os.getenv("OPENROUTER_API_KEY"),
                base_url = OPENROUTER_BASE_URL,
            )
        return self._or_client

    async def _fetch_or_score(self) -> Tuple[float, str]:
        """
        [FIX-OR-PARSE]  Validates choices list and message content before
        indexing.  Returns (cached_score, "cached_fallback") on any error.
        """
        if not self._or_enabled:
            return float(self._or_score), "cached_fallback"

        if time.time() - self._or_cache_ts < OPENROUTER_CACHE_TTL:
            return float(self._or_score), "openrouter"

        prompt = (
            "You are a crypto market sentiment classifier. "
            "Based on current global macro and crypto momentum, output a SINGLE "
            "float in [-1.0, +1.0]: -1.0 = extreme fear/sell, +1.0 = extreme greed/buy. "
            "Output ONLY the number. No explanation. Example: -0.45"
        )

        try:
            client   = self._get_or_client()
            response = await asyncio.wait_for(
                client.chat.completions.create(
                    model       = OPENROUTER_MODEL,
                    messages    = [{"role": "user", "content": prompt}],
                    max_tokens  = 10,
                    temperature = 0.0,
                ),
                timeout=OPENROUTER_TIMEOUT,
            )

            # [FIX-OR-PARSE] Guard choices list.
            choices = getattr(response, "choices", None) or []
            if not choices:
                log.warning("[P7] KellySizer: OpenRouter returned empty choices.")
                return float(self._or_score), "cached_fallback"

            message = getattr(choices[0], "message", None)
            raw_content = getattr(message, "content", None) if message else None

            # [FIX-OR-PARSE] Guard None content.
            if raw_content is None:
                log.warning("[P7] KellySizer: OpenRouter message content is None.")
                return float(self._or_score), "cached_fallback"

            raw   = str(raw_content).strip()
            clean = "".join(ch for ch in raw if ch in "0123456789.-+").strip()

            if not clean:
                log.warning(
                    "[P7] KellySizer: OpenRouter non-numeric response: %r", raw
                )
                return float(self._or_score), "cached_fallback"

            try:
                parsed = float(clean)
            except ValueError:
                log.warning(
                    "[P7] KellySizer: OpenRouter float parse failed: %r", clean
                )
                return float(self._or_score), "cached_fallback"

            parsed = max(-1.0, min(1.0, parsed))

            self._or_score    = parsed
            self._or_cache_ts = time.time()
            log.debug(
                "[P7] KellySizer OpenRouter: score=%.4f (raw=%r model=%s)",
                parsed, raw, OPENROUTER_MODEL,
            )
            return float(self._or_score), "openrouter"

        except asyncio.TimeoutError:
            log.warning(
                "[P7] KellySizer: OpenRouter timed out after %.1fs — "
                "using cached score=%.4f",
                OPENROUTER_TIMEOUT, self._or_score,
            )
        except Exception as exc:
            log.warning(
                "[P7] KellySizer: OpenRouter error — %s — "
                "using cached score=%.4f",
                exc, self._or_score,
            )

        return float(self._or_score), "cached_fallback"

    async def _fetch_nw_score(self) -> Tuple[float, bool]:
        if self._nw is None:
            return 0.0, False

        if time.time() - self._nw_cache_ts < KELLY_NW_CACHE_TTL:
            return float(self._nw_score), True

        try:
            bias              = await self._nw.get_macro_bias()
            score_val         = bias.score
            # [FIX-SCORE-TUPLE] Ensure we store a float, not None.
            self._nw_score    = float(score_val) if score_val is not None else 0.0
            self._nw_cache_ts = time.time()
            return float(self._nw_score), True
        except Exception as exc:
            log.debug(
                "[P7] KellySizer: NewsWire unavailable — %s "
                "(falling through to OpenRouter)",
                exc,
            )
            return float(self._nw_score), False

    async def _get_score(self) -> Tuple[float, str]:
        """
        [FIX-SCORE-TUPLE]  Guaranteed to return Tuple[float, str] in every
        code path.  Wraps all sub-calls in try/except.  Callers must be able
        to do:
            score, source = await self._get_score()
        without any possibility of TypeError.
        """
        # Tier 1: NewsWire
        try:
            nw_score, nw_ok = await self._fetch_nw_score()
            if nw_ok:
                # Explicit type coercion before returning.
                return float(nw_score), "newswire"
        except Exception as exc:
            log.warning(
                "[P7] KellySizer: _fetch_nw_score unexpected error — %s", exc
            )

        # Tier 2: OpenRouter
        try:
            or_result = await self._fetch_or_score()
            # _fetch_or_score always returns a tuple, but guard defensively.
            if (
                or_result is not None
                and isinstance(or_result, tuple)
                and len(or_result) == 2
            ):
                or_score, or_source = or_result
                if or_score is not None:
                    return float(or_score), str(or_source)
        except Exception as exc:
            log.warning(
                "[P7] KellySizer: _fetch_or_score unexpected error — %s", exc
            )

        # Tier 3: hard neutral fallback — NEVER returns None.
        log.warning(
            "[P7] KellySizer: all score sources failed — "
            "returning fallback_default score=0.5 (neutral, no entry block)."
        )
        return 0.5, "fallback_default"

    @staticmethod
    def _score_to_multiplier(score: float) -> Tuple[float, bool]:
        # Guard against None being passed by a caller.
        if score is None:
            score = 0.0
        score = float(score)

        if score <= KELLY_PANIC_BLOCK:
            return 0.0, True
        if score <= KELLY_BEAR_THRESHOLD:
            return KELLY_BEAR_FLOOR, False
        if score >= KELLY_BULL_THRESHOLD:
            return KELLY_BULL_MULTIPLIER, False

        span     = KELLY_BULL_THRESHOLD - KELLY_BEAR_THRESHOLD
        progress = (score - KELLY_BEAR_THRESHOLD) / span if span > 0 else 0.5
        mult     = KELLY_BEAR_FLOOR + progress * (KELLY_BULL_MULTIPLIER - KELLY_BEAR_FLOOR)
        return round(mult, 4), False

    async def get_sentiment_adjusted_size(
        self,
        base_kelly: float,
        max_alloc:  float,
        min_alloc:  float,
        equity:     float,
    ) -> float:
        """
        Returns the sentiment-adjusted allocation fraction in [0.0, max_alloc].
        Returns 0.0 as a HARD BLOCK when the panic threshold is breached.

        [FIX-SCORE-TUPLE] score, source unpacking is safe — _get_score()
        always returns Tuple[float, str].
        """
        score, source          = await self._get_score()
        multiplier, is_blocked = self._score_to_multiplier(score)

        if is_blocked:
            log.info(
                "[P7] KellySizer BLOCK — score=%.4f ≤ panic_threshold=%.2f "
                "(base_kelly=%.4f  equity=$%.2f  source=%s)",
                score, KELLY_PANIC_BLOCK, base_kelly, equity, source,
            )
            return 0.0

        adjusted = max(float(min_alloc), min(float(base_kelly) * multiplier, float(max_alloc)))
        log.debug(
            "[P7] KellySizer — score=%.4f  mult=%.4f  "
            "base=%.4f → adj=%.6f  (min=%.4f  max=%.4f  equity=$%.2f  src=%s)",
            score, multiplier, base_kelly, adjusted,
            min_alloc, max_alloc, equity, source,
        )
        return round(adjusted, 6)

    async def get_scale_info(
        self,
        base_kelly: float,
        max_alloc:  float,
        min_alloc:  float,
        equity:     float,
    ) -> SentimentScale:
        score, source          = await self._get_score()
        multiplier, is_blocked = self._score_to_multiplier(score)

        if is_blocked:
            adjusted = 0.0
            label    = "🚫 PANIC BLOCK"
        else:
            adjusted = max(float(min_alloc), min(float(base_kelly) * multiplier, float(max_alloc)))
            if multiplier >= KELLY_BULL_MULTIPLIER:
                label = "🚀 Bullish Boost"
            elif multiplier <= KELLY_BEAR_FLOOR:
                label = "🐻 Bearish Reduction"
            else:
                label = "➡️  Neutral Band"

        return SentimentScale(
            raw_macro_score = round(float(score),      4),
            multiplier      = round(float(multiplier), 4),
            adjusted_alloc  = round(float(adjusted),   6),
            blocked         = is_blocked,
            label           = label,
            score_source    = source,
        )
