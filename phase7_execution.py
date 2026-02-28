"""
phase7_execution.py  —  AdaptiveSpread / TWAPSlicer / SlippageAuditor

Phase 14 additions:
  [P14-2] Dynamic Spread Auto-Tuner
    - SlippageAuditor now tracks fill / miss events with timestamps via
      record_fill_event(symbol, filled) so AdaptiveSpread can query the
      rolling fill rate for the last hour.
    - AdaptiveSpread.auto_tune(symbol) is called after every fill event:
        • fill_rate_1h < P14_FILL_RATE_LOW  (default 70%) →
              tighten spread by P14_SPREAD_TUNE_STEP bps
              (log: "[P14] Auto-Tuner: Tightening spread …")
        • fill_rate_1h == 1.0 (100% fills) →
              widen spread by P14_SPREAD_TUNE_STEP bps to capture more rebate
              (log: "[P14] Auto-Tuner: Widening spread …")
        • Otherwise: no change.
    - auto_tune() respects the existing P7_SPREAD_MIN_OFFSET_BPS /
      P7_SPREAD_MAX_OFFSET_BPS hard bounds so the tuner cannot escape the
      intended operating range.

Phase 12 additions (all preserved):
  [P12-3] "Tick-Inside" / Pennying in AdaptiveSpread

Phase 7 (all preserved):
  AdaptiveSpread   — live spread tracker + adjusted limit price
  TWAPSlicer       — time-weighted average price execution slicer
  SlippageAuditor  — post-trade slippage recording and reporting
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Deque, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from data_hub import InstrumentCache, OrderBook, Tick
    from executor import CoinConfig, Executor
    from brain   import Signal

log = logging.getLogger("phase7_execution")

# ── Config ─────────────────────────────────────────────────────────────────────
P7_SPREAD_MIN_OFFSET_BPS  = float(os.environ.get("P7_SPREAD_MIN_OFFSET_BPS",  "0.5"))
P7_SPREAD_MAX_OFFSET_BPS  = float(os.environ.get("P7_SPREAD_MAX_OFFSET_BPS",  "5.0"))
P7_SPREAD_DECAY_ALPHA     = float(os.environ.get("P7_SPREAD_DECAY_ALPHA",     "0.3"))
P7_SPREAD_FILL_REWARD     = float(os.environ.get("P7_SPREAD_FILL_REWARD",     "0.5"))
P7_SPREAD_MISS_PENALTY    = float(os.environ.get("P7_SPREAD_MISS_PENALTY",    "0.25"))

P7_TWAP_SLICES            = int  (os.environ.get("P7_TWAP_SLICES",            "5"))
P7_TWAP_INTERVAL_SECS     = float(os.environ.get("P7_TWAP_INTERVAL_SECS",     "12.0"))
P7_TWAP_JITTER_PCT        = float(os.environ.get("P7_TWAP_JITTER_PCT",        "0.2"))
P7_TWAP_ABORT_SLIPPAGE    = float(os.environ.get("P7_TWAP_ABORT_SLIPPAGE",    "0.5"))
P7_TWAP_MIN_USD           = float(os.environ.get("P7_TWAP_MIN_USD",           "200.0"))

KELLY_PANIC_BLOCK         = float(os.environ.get("KELLY_PANIC_BLOCK",         "-0.5"))

# [P12-3] Pennying config
P12_PENNY_LEVELS          = int  (os.environ.get("P12_PENNY_LEVELS",          "3"))
P12_PENNY_MIN_WALL_USD    = float(os.environ.get("P12_PENNY_MIN_WALL_USD",    "10_000"))

# [P14-2] Dynamic Spread Auto-Tuner config
P14_FILL_RATE_LOW         = float(os.environ.get("P14_FILL_RATE_LOW",         "0.70"))
P14_SPREAD_TUNE_STEP      = float(os.environ.get("P14_SPREAD_TUNE_STEP",      "0.5"))
P14_AUTOTUNE_WINDOW_SECS  = float(os.environ.get("P14_AUTOTUNE_WINDOW_SECS",  "3600.0"))
P14_AUTOTUNE_MIN_SAMPLES  = int  (os.environ.get("P14_AUTOTUNE_MIN_SAMPLES",  "5"))

# [P23-OPT-3] Fee-protection spread guard — if market bid/ask spread exceeds
# this threshold (%) force Post-Only (Maker) order type to avoid taker fees
# that would drain a $50 account during volatile spreads.
P23_SPREAD_GUARD_PCT      = float(os.environ.get("P23_SPREAD_GUARD_PCT",      "0.10"))


# ── AdaptiveSpread ─────────────────────────────────────────────────────────────
class AdaptiveSpread:
    """
    Tracks per-symbol fill/miss history and dynamically adjusts the BPS offset
    used when constructing limit-order prices.

    [P12-3] Tick-Inside / Pennying
    ─────────────────────────────
    When `adjusted_limit_price(side, tick, symbol, order_book=book)` is called
    with a live OrderBook, the method attempts to penny the largest passive wall
    within the offset range (see _penny_price for full logic).

    [P14-2] Dynamic Spread Auto-Tuner
    ──────────────────────────────────
    auto_tune(symbol) is called after every fill/miss event via record_fill().
    It reads the rolling 1-hour fill rate from the companion SlippageAuditor
    and adjusts the per-symbol BPS offset accordingly:

      • fill_rate_1h < P14_FILL_RATE_LOW  → tighten by P14_SPREAD_TUNE_STEP bps
      • fill_rate_1h == 1.0               → widen  by P14_SPREAD_TUNE_STEP bps
      • otherwise                          → no adjustment

    Both directions respect the global min/max BPS bounds.
    """

    def __init__(self, instrument_cache: "InstrumentCache"):
        self._ic         = instrument_cache
        self._offsets:   Dict[str, float] = defaultdict(
            lambda: P7_SPREAD_MIN_OFFSET_BPS
        )
        self._tick_sizes: Dict[str, float] = {}
        self._history:   Dict[str, Deque[Tuple[float, bool]]] = defaultdict(
            lambda: deque(maxlen=50)
        )

        # [P14-2] Rolling 1-hour fill event log: deque of (timestamp, filled)
        self._fill_events: Dict[str, Deque[Tuple[float, bool]]] = defaultdict(
            lambda: deque(maxlen=500)
        )

        # Reference to SlippageAuditor injected post-construction
        self._auditor: Optional["SlippageAuditor"] = None

    def attach_auditor(self, auditor: "SlippageAuditor") -> None:
        """Inject the SlippageAuditor so auto_tune() can read fill rates."""
        self._auditor = auditor

    # ── [P14-2] Fill-rate computation ─────────────────────────────────────────
    def _fill_rate_1h(self, symbol: str) -> Optional[float]:
        """
        Returns the fill rate (0.0–1.0) for `symbol` over the last
        P14_AUTOTUNE_WINDOW_SECS window, or None if there are fewer than
        P14_AUTOTUNE_MIN_SAMPLES events.
        """
        events = self._fill_events.get(symbol)
        if events is None:
            return None
        cutoff  = time.time() - P14_AUTOTUNE_WINDOW_SECS
        recent  = [(ts, f) for ts, f in events if ts >= cutoff]
        if len(recent) < P14_AUTOTUNE_MIN_SAMPLES:
            return None
        return sum(f for _, f in recent) / len(recent)

    # ── [P14-2] Auto-Tuner ────────────────────────────────────────────────────
    def auto_tune(self, symbol: str) -> None:
        """
        Adjusts the per-symbol spread offset based on the rolling 1-hour fill
        rate. Called automatically inside record_fill() after each event.

        Logging format (explicit per requirements):
          [P14] Auto-Tuner: Tightening spread <sym> to <X> bps (fill_rate=<Y>%)
          [P14] Auto-Tuner: Widening spread   <sym> to <X> bps (fill_rate=100%)
        """
        fill_rate = self._fill_rate_1h(symbol)
        if fill_rate is None:
            return  # insufficient samples

        cur = self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS)

        if fill_rate < P14_FILL_RATE_LOW:
            # Too many misses → tighten spread (price more aggressively)
            new = max(cur - P14_SPREAD_TUNE_STEP, P7_SPREAD_MIN_OFFSET_BPS)
            if new < cur:
                self._offsets[symbol] = round(new, 4)
                log.info(
                    "[P14] Auto-Tuner: Tightening spread %s to %.4f bps "
                    "(fill_rate=%.1f%% < %.0f%% threshold)",
                    symbol, new, fill_rate * 100, P14_FILL_RATE_LOW * 100,
                )
        elif fill_rate >= 1.0:
            # 100% fills → widen spread to capture more rebate
            new = min(cur + P14_SPREAD_TUNE_STEP, P7_SPREAD_MAX_OFFSET_BPS)
            if new > cur:
                self._offsets[symbol] = round(new, 4)
                log.info(
                    "[P14] Auto-Tuner: Widening spread %s to %.4f bps "
                    "(fill_rate=100%% — capturing more rebate)",
                    symbol, new,
                )

    # ── [P12-3] Tick-size pre-warm ────────────────────────────────────────────
    async def ensure_tick_size_resolved(self, symbol: str) -> float:
        if symbol in self._tick_sizes:
            return self._tick_sizes[symbol]
        try:
            meta = await self._ic.get_instrument_info(symbol, swap=False)
            if meta and meta.tick_sz > 0:
                self._tick_sizes[symbol] = meta.tick_sz
                log.debug("[P12-3] Tick size pre-warmed %s: %.8f", symbol, meta.tick_sz)
                return meta.tick_sz
        except Exception as e:
            log.debug("[P12-3] Tick size pre-warm error %s: %s", symbol, e)
        return 0.0

    def _tick_sz(self, symbol: str) -> float:
        return self._tick_sizes.get(symbol, 0.0)

    # ── Core price computation ─────────────────────────────────────────────────
    def adjusted_limit_price(
        self,
        side:       str,
        tick:       "Tick",
        symbol:     str,
        order_book: Optional["OrderBook"] = None,
    ) -> float:
        """
        Returns the adjusted limit price for `side` ("buy" or "sell").

        Without `order_book`: pure BPS-offset from best bid/ask (Phase 7).
        With `order_book`   : attempts to penny the largest passive wall
                              within the offset range (Phase 12-3).
        """
        offset_bps  = self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS)
        offset_frac = offset_bps / 10_000.0

        if side == "buy":
            base_px    = tick.ask if tick.ask > 0 else tick.last
            vanilla_px = base_px * (1 - offset_frac)
        else:
            base_px    = tick.bid if tick.bid > 0 else tick.last
            vanilla_px = base_px * (1 + offset_frac)

        # ── [P12-3] Pennying ──────────────────────────────────────────────────
        if order_book is not None:
            penny_px = self._penny_price(side, symbol, vanilla_px,
                                          base_px, offset_frac, order_book)
            if penny_px is not None:
                return penny_px
            log.debug(
                "[P12-3] Penny skipped for %s %s — using vanilla=%.8f",
                side, symbol, vanilla_px,
            )

        return vanilla_px

    def get_limit_price(
        self,
        side:       str,
        tick:       "Tick",
        symbol:     str,
        order_book: Optional["OrderBook"] = None,
    ) -> Tuple[float, bool]:
        """
        [P23-OPT-3] Fee-Protection wrapper around adjusted_limit_price().

        Returns:
            (limit_price: float, post_only: bool)

        post_only=True signals to the executor that it MUST submit this order
        as Post-Only (Maker) to avoid taker fees.  This is forced when the
        live bid/ask spread exceeds P23_SPREAD_GUARD_PCT (default 0.10%),
        protecting a $50 account from being drained by taker fees during
        volatile market conditions.

        post_only=False means the spread is within acceptable range and the
        caller may choose the order type freely.

        The limit price itself is always the AdaptiveSpread-adjusted price
        from adjusted_limit_price() regardless of the post_only flag.
        """
        limit_px = self.adjusted_limit_price(side, tick, symbol, order_book)

        # ── Spread guard ──────────────────────────────────────────────────────
        bid = tick.bid if tick.bid and tick.bid > 0 else tick.last
        ask = tick.ask if tick.ask and tick.ask > 0 else tick.last

        post_only = False
        if bid and bid > 0:
            spread_pct = (ask - bid) / bid * 100.0
            if spread_pct > P23_SPREAD_GUARD_PCT:
                post_only = True
                log.info(
                    "[P23-OPT-3] SPREAD-GUARD: %s %s spread=%.4f%% > %.2f%% "
                    "— forcing Post-Only (Maker) order type.",
                    side, symbol, spread_pct, P23_SPREAD_GUARD_PCT,
                )

        return limit_px, post_only

    def _penny_price(
        self,
        side:        str,
        symbol:      str,
        vanilla_px:  float,
        base_px:     float,
        offset_frac: float,
        book:        "OrderBook",
    ) -> Optional[float]:
        """
        Attempts to penny the largest passive wall within our spread range.
        Returns the pennied price or None to fall back to vanilla BPS price.
        """
        tick_sz = self._tick_sz(symbol)
        if tick_sz <= 0:
            return None

        wall = book.best_passive_wall(side, tick_sz, P12_PENNY_LEVELS)
        if wall is None:
            return None

        wall_px, wall_sz = wall
        mid = (book.bids[0][0] + book.asks[0][0]) / 2.0 if book.bids and book.asks else base_px
        wall_usd = wall_sz * mid

        if wall_usd < P12_PENNY_MIN_WALL_USD:
            log.debug(
                "[P12-3] Wall too small for pennying %s: $%.0f < $%.0f",
                symbol, wall_usd, P12_PENNY_MIN_WALL_USD,
            )
            return None

        if side == "buy":
            penny_candidate = wall_px - tick_sz
            if penny_candidate <= vanilla_px:
                return None
            if penny_candidate > base_px * (1 - (P7_SPREAD_MIN_OFFSET_BPS / 10_000.0)):
                return None
            log.info(
                "[P12-3] Pennying BUY %s: wall=%.8f (wall_usd=$%.0f) → "
                "penny_px=%.8f (vanilla=%.8f)",
                symbol, wall_px, wall_usd, penny_candidate, vanilla_px,
            )
            return penny_candidate

        else:  # sell
            penny_candidate = wall_px + tick_sz
            if penny_candidate >= vanilla_px:
                return None
            if penny_candidate < base_px * (1 + (P7_SPREAD_MIN_OFFSET_BPS / 10_000.0)):
                return None
            log.info(
                "[P12-3] Pennying SELL %s: wall=%.8f (wall_usd=$%.0f) → "
                "penny_px=%.8f (vanilla=%.8f)",
                symbol, wall_px, wall_usd, penny_candidate, vanilla_px,
            )
            return penny_candidate

    # ── Offset management ──────────────────────────────────────────────────────
    def get_offset_bps(self, symbol: str) -> float:
        return self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS)

    async def record_fill(self, symbol: str, side: str,
                           order_ts: float, fill_ts: float, filled: bool):
        """
        Records a fill/miss event.

        [P14-2] Also appends to the rolling 1-hour fill-event log and
        triggers auto_tune() so the spread is adjusted immediately.
        """
        hist = self._history[symbol]
        hist.append((fill_ts, filled))

        # [P14-2] Log to rolling window for auto-tuner
        self._fill_events[symbol].append((fill_ts, filled))

        if not hist:
            return

        recent_fills = [f for _, f in list(hist)[-10:]]
        fill_rate    = sum(recent_fills) / len(recent_fills)

        cur = self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS)
        if filled:
            new = max(cur - P7_SPREAD_FILL_REWARD, P7_SPREAD_MIN_OFFSET_BPS)
        else:
            new = min(cur + P7_SPREAD_MISS_PENALTY, P7_SPREAD_MAX_OFFSET_BPS)

        self._offsets[symbol] = round(new, 4)
        log.debug(
            "[P7] AdaptiveSpread %s %s: fill_rate=%.0f%% offset %.3f→%.3f bps",
            symbol, "HIT" if filled else "MISS", fill_rate * 100, cur, new,
        )

        # Cache tick size while we have a fresh instrument
        if symbol not in self._tick_sizes:
            asyncio.ensure_future(self.ensure_tick_size_resolved(symbol))

        # [P14-2] Run auto-tuner after every event (uses the 1h window)
        self.auto_tune(symbol)

    def stats(self, symbol: str) -> dict:
        hist   = list(self._history.get(symbol, []))
        recent = hist[-20:]
        fr     = sum(f for _, f in recent) / len(recent) if recent else 0.0
        fill_1h = self._fill_rate_1h(symbol)
        return {
            "offset_bps":   round(self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS), 4),
            "fill_rate":    round(fr, 4),
            "fill_rate_1h": round(fill_1h, 4) if fill_1h is not None else None,
            "samples":      len(recent),
            "tick_sz":      self._tick_sizes.get(symbol, 0.0),
        }


# ── TWAP Slicer ────────────────────────────────────────────────────────────────
@dataclass
class TwapResult:
    aborted:         bool
    abort_reason:    str
    avg_fill_px:     float
    total_fill_sz:   float
    slices_sent:     int
    slices_filled:   int


class TWAPSlicer:
    """
    Splits a large order into P7_TWAP_SLICES child orders spaced
    P7_TWAP_INTERVAL_SECS apart with random ±P7_TWAP_JITTER_PCT timing jitter.

    Aborts if the VWAP slippage versus the entry reference exceeds
    P7_TWAP_ABORT_SLIPPAGE pct.

    Only activated for orders ≥ P7_TWAP_MIN_USD.
    """

    def __init__(self, hub, executor: "Executor"):
        self._hub  = hub
        self._exec = executor

    async def execute(
        self,
        symbol:    str,
        side:      str,
        usd:       float,
        signal:    "Signal",
        swap:      bool,
        pos_side:  str,
        tag:       str,
        coin_cfg:  "CoinConfig",
    ) -> Optional[TwapResult]:
        import random

        if usd < P7_TWAP_MIN_USD:
            return None

        n_slices  = P7_TWAP_SLICES
        slice_usd = usd / n_slices

        ref_tick  = await self._hub.get_tick(symbol)
        if not ref_tick:
            return TwapResult(True, "no_tick", 0.0, 0.0, 0, 0)
        ref_px = ref_tick.ask if side == "buy" else ref_tick.bid

        total_sz   = 0.0
        total_cost = 0.0
        sent       = 0
        filled     = 0

        for i in range(n_slices):
            if i > 0:
                jitter_frac = 1 + random.uniform(-P7_TWAP_JITTER_PCT / 100,
                                                   P7_TWAP_JITTER_PCT / 100)
                await asyncio.sleep(P7_TWAP_INTERVAL_SECS * jitter_frac)

            sent  += 1
            order  = await self._exec._execute_order(
                symbol, side, slice_usd,
                swap=swap, pos_side=pos_side,
                tag=f"{tag}_TWAP{i+1}of{n_slices}",
                coin_cfg=coin_cfg,
            )
            if order is None:
                log.warning("[P7] TWAP %s slice %d/%d failed", symbol, i + 1, n_slices)
                continue

            fill_px = float(order.get("avgPx") or order.get("px") or 0)
            fill_sz = float(order.get("accFillSz") or order.get("sz") or 0)
            if fill_px <= 0 or fill_sz <= 0:
                continue

            total_sz   += fill_sz
            total_cost += fill_sz * fill_px
            filled     += 1

            if total_sz > 0:
                vwap     = total_cost / total_sz
                slip_pct = abs(vwap - ref_px) / ref_px * 100
                if slip_pct > P7_TWAP_ABORT_SLIPPAGE:
                    log.warning(
                        "[P7] TWAP abort %s: vwap=%.6f ref=%.6f slippage=%.3f%% > %.3f%%",
                        symbol, vwap, ref_px, slip_pct, P7_TWAP_ABORT_SLIPPAGE,
                    )
                    return TwapResult(
                        aborted=True,
                        abort_reason=f"slippage_{slip_pct:.3f}pct",
                        avg_fill_px=vwap,
                        total_fill_sz=total_sz,
                        slices_sent=sent,
                        slices_filled=filled,
                    )

        if total_sz <= 0:
            return TwapResult(True, "zero_fill", 0.0, 0.0, sent, filled)

        avg_px = total_cost / total_sz
        log.info(
            "[P7] TWAP complete %s %s: slices=%d/%d avg_px=%.6f total_sz=%.6f",
            side.upper(), symbol, filled, n_slices, avg_px, total_sz,
        )
        return TwapResult(
            aborted=False, abort_reason="",
            avg_fill_px=avg_px, total_fill_sz=total_sz,
            slices_sent=sent, slices_filled=filled,
        )


# ── Slippage Auditor ───────────────────────────────────────────────────────────
@dataclass
class _SlippageRecord:
    symbol:      str
    side:        str
    expected_px: float
    actual_px:   float
    slip_bps:    float
    filled:      bool  = True      # [P14-2] track whether the order filled at all
    ts:          float = field(default_factory=time.time)


class SlippageAuditor:
    """
    Records expected vs. actual fill prices and produces per-symbol
    slippage statistics for the GUI bridge.

    [P14-2] fill_rate_1h(symbol) added for the Dynamic Spread Auto-Tuner.
    record_fill_event(symbol, filled) records a raw fill/miss without a price.
    """

    def __init__(self, maxlen: int = 200):
        self._records: Deque[_SlippageRecord] = deque(maxlen=maxlen)
        # [P14-2] Separate lightweight fill-event deque for rate calculation
        self._fill_events: Dict[str, Deque[Tuple[float, bool]]] = defaultdict(
            lambda: deque(maxlen=500)
        )

    def record(self, symbol: str, expected_px: float,
               actual_px: float, side: str, filled: bool = True) -> None:
        if expected_px <= 0:
            return
        slip_bps = (actual_px - expected_px) / expected_px * 10_000
        if side == "buy":
            slip_bps = -slip_bps
        self._records.append(_SlippageRecord(
            symbol=symbol, side=side,
            expected_px=expected_px, actual_px=actual_px,
            slip_bps=slip_bps, filled=filled,
        ))
        # [P14-2] Also log to the fill-event window
        now = time.time()
        self._fill_events[symbol].append((now, filled))
        # [FIX-MEMORY-LEAK] Prune fill events older than 1 hour to prevent
        # unbounded RAM growth during 24/7 operation.
        self._prune_fill_events(symbol, now)
        log.debug(
            "[P7] SlippageAuditor %s %s: expected=%.6f actual=%.6f "
            "slip=%.2f bps filled=%s",
            symbol, side, expected_px, actual_px, slip_bps, filled,
        )

    def record_fill_event(self, symbol: str, filled: bool) -> None:
        """
        [P14-2] Lightweight variant — records only the fill/miss outcome
        without price data. Used when price information is unavailable
        (e.g. shadow limit misses).
        """
        now = time.time()
        self._fill_events[symbol].append((now, filled))
        # [FIX-MEMORY-LEAK] Prune stale events on every write.
        self._prune_fill_events(symbol, now)
    def _prune_fill_events(self, symbol: str, now: float, ttl_secs: float = 3600.0) -> None:
        """
        [FIX-MEMORY-LEAK] Remove fill events older than `ttl_secs` (default 1h).

        Phase 40.1 / Phase 37 stability hardening:
        Use in-place deque rotation (popleft) rather than clear/extend rebuilds.
        This avoids repeated list allocations and reduces latency jitter in
        tight execution paths.

        Complexity:
            O(k) where k = number of expired events (typically small).
        """
        events = self._fill_events.get(symbol)
        if not events:
            return
        cutoff = now - ttl_secs
        try:
            while events and events[0][0] < cutoff:
                events.popleft()
        except Exception:
            return


    def fill_rate_1h(self, symbol: str,
                     window_secs: float = 3600.0,
                     min_samples: int   = 5) -> Optional[float]:
        """
        [P14-2] Returns the rolling 1-hour fill rate for `symbol`, or None
        when fewer than `min_samples` events are available in the window.
        """
        events = self._fill_events.get(symbol)
        if events is None:
            return None
        cutoff = time.time() - window_secs
        recent = [f for ts, f in events if ts >= cutoff]
        if len(recent) < min_samples:
            return None
        return sum(recent) / len(recent)

    def report(self) -> dict:
        by_sym: Dict[str, List[float]] = defaultdict(list)
        for r in self._records:
            by_sym[r.symbol].append(r.slip_bps)
        return {
            sym: {
                "avg_slip_bps":  round(sum(v) / len(v), 3) if v else 0.0,
                "max_slip_bps":  round(max(v), 3)          if v else 0.0,
                "min_slip_bps":  round(min(v), 3)          if v else 0.0,
                "samples":       len(v),
                "fill_rate_1h":  self.fill_rate_1h(sym),
            }
            for sym, v in by_sym.items()
        }
