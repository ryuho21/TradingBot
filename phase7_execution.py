"""
phase7_execution.py  —  AdaptiveSpread / TWAPSlicer / SlippageAuditor

Phase 43 additions (this release):
  [P43-MAKER] OBI-Aware Bid Floating — Maker Rebate Calibration.
    AdaptiveSpread gains a new OBI-awareness layer that reads the P38
    Order Flow Imbalance state injected by the Executor each cycle via
    set_obi_state().  The float logic runs inside adjusted_limit_price()
    AFTER the vanilla BPS offset and BEFORE the P12-3 pennying step:

    Sell-Wall Float (BUY side):
      When ofi_score < -P43_OBI_WALL_THRESHOLD (default -0.5) indicating
      net sell-side pressure, OR when ask_wall_pulled=True (fake support
      evaporated on the ask side), the bid price is moved DOWN by
      P43_WALL_FLOAT_TICKS × tick_size.  This "floats" the bid deeper in
      the book — the order is less likely to be immediately hit, and when
      it does fill it captures the full maker rebate instead of crossing
      the spread.

    Bid-Wall-Pull Float (SELL side):
      When bid_wall_pulled=True (a large bid support wall was just removed),
      the ask price is moved UP by P43_WALL_FLOAT_TICKS × tick_size.
      A disappearing bid wall is a leading indicator of downward price
      movement; raising the ask avoids selling into a falling knife and
      ensures the fill is at the higher edge of the new equilibrium.

    Float guard:
      The maximum float is capped at P43_MAX_FLOAT_TICKS × tick_size to
      prevent the bid from drifting so far it never fills.  The float is
      only applied when tick_size > 0 (i.e. the instrument meta is loaded).

    Dashboard:
      get_obi_calibration_snapshot(symbol) returns the current per-symbol
      float state for the dashboard OBI Calibration tile.

Phase 14 additions (all preserved):
  [P14-2] Dynamic Spread Auto-Tuner
    - SlippageAuditor tracks fill / miss events with timestamps via
      record_fill_event(symbol, filled) so AdaptiveSpread can query the
      rolling fill rate for the last hour.
    - AdaptiveSpread.auto_tune(symbol) adjusts the per-symbol BPS offset:
        • fill_rate_1h < P14_FILL_RATE_LOW  → tighten by P14_SPREAD_TUNE_STEP
        • fill_rate_1h == 1.0               → widen  by P14_SPREAD_TUNE_STEP
    - Respects P7_SPREAD_MIN_OFFSET_BPS / P7_SPREAD_MAX_OFFSET_BPS bounds.

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
logging.getLogger("phase7_execution").addHandler(logging.NullHandler())
from pt_utils import _env_float, _env_int, atomic_write_json  # [P0-UTIL]

# [P0-FIX-11] env helpers → pt_utils



# ── Config ─────────────────────────────────────────────────────────────────────
P7_SPREAD_MIN_OFFSET_BPS  = _env_float("P7_SPREAD_MIN_OFFSET_BPS", 0.5)
P7_SPREAD_MAX_OFFSET_BPS  = _env_float("P7_SPREAD_MAX_OFFSET_BPS", 5.0)
P7_SPREAD_DECAY_ALPHA     = _env_float("P7_SPREAD_DECAY_ALPHA", 0.3)
P7_SPREAD_FILL_REWARD     = _env_float("P7_SPREAD_FILL_REWARD", 0.5)
P7_SPREAD_MISS_PENALTY    = _env_float("P7_SPREAD_MISS_PENALTY", 0.25)

P7_TWAP_SLICES            = _env_int("P7_TWAP_SLICES", 5)
P7_TWAP_INTERVAL_SECS     = _env_float("P7_TWAP_INTERVAL_SECS", 12.0)
P7_TWAP_JITTER_PCT        = _env_float("P7_TWAP_JITTER_PCT", 0.2)
P7_TWAP_ABORT_SLIPPAGE    = _env_float("P7_TWAP_ABORT_SLIPPAGE", 0.5)
P7_TWAP_MIN_USD           = _env_float("P7_TWAP_MIN_USD", 200.0)

KELLY_PANIC_BLOCK         = _env_float("KELLY_PANIC_BLOCK", -0.5)

# [P12-3] Pennying config
P12_PENNY_LEVELS          = _env_int("P12_PENNY_LEVELS", 3)
P12_PENNY_MIN_WALL_USD    = _env_float("P12_PENNY_MIN_WALL_USD", 10_000)

# [P14-2] Dynamic Spread Auto-Tuner config
P14_FILL_RATE_LOW         = _env_float("P14_FILL_RATE_LOW", 0.70)
P14_SPREAD_TUNE_STEP      = _env_float("P14_SPREAD_TUNE_STEP", 0.5)
P14_AUTOTUNE_WINDOW_SECS  = _env_float("P14_AUTOTUNE_WINDOW_SECS", 3600.0)
P14_AUTOTUNE_MIN_SAMPLES  = _env_int("P14_AUTOTUNE_MIN_SAMPLES", 5)

# [P23-OPT-3] Fee-protection spread guard — if market bid/ask spread exceeds
# this threshold (%) force Post-Only (Maker) order type to avoid taker fees
# that would drain a $50 account during volatile spreads.
P23_SPREAD_GUARD_PCT      = _env_float("P23_SPREAD_GUARD_PCT", 0.10)

# ── [P43-MAKER] OBI-Aware Bid Floating — Maker Rebate Calibration ─────────────
# P43_OBI_WALL_THRESHOLD : OFI magnitude at which a sell/buy wall is classified
#   as "significant" enough to trigger bid/ask floating.  Matches the P38 veto
#   threshold scale (OFI ∈ [-1, +1]).  Default -0.5 for sell-wall detection
#   (negative OFI = net sell pressure).  The same magnitude (0.5) is used for
#   buy-wall-pull detection on the sell side.
P43_OBI_WALL_THRESHOLD    = _env_float("P43_OBI_WALL_THRESHOLD", 0.50)
# P43_WALL_FLOAT_TICKS   : Number of tick_size increments to float the bid/ask
#   away from the BBO when a wall is detected.  Default 2 ticks.
#   2 ticks is conservative — deep enough to earn maker rebate without missing
#   the fill entirely.  Increase to 3-4 in thin markets.
P43_WALL_FLOAT_TICKS      = _env_float("P43_WALL_FLOAT_TICKS", 2.0)
# P43_MAX_FLOAT_TICKS     : Hard cap on total float distance (ticks).  Prevents
#   the bid from drifting so far below BBO that it never fills.  Default 5.
P43_MAX_FLOAT_TICKS       = _env_float("P43_MAX_FLOAT_TICKS", 5.0)
# P43_FLOAT_ON_WALL_PULL  : When True (default), also triggers bid float when
#   a large BID wall was pulled (bid_wall_pulled=True from P38 OBIMonitor).
#   A disappearing bid wall signals fake support — safer to price lower.
P43_FLOAT_ON_WALL_PULL    = (
    os.environ.get("P43_FLOAT_ON_WALL_PULL", "1").strip().strip("'\"") == "1"
)
# P43_ENABLE              : Master enable switch.  Default 1 (active).
#   Set to 0 to revert to pure BPS + Pennying behavior (P7/P12-3).
P43_ENABLE                = (
    os.environ.get("P43_ENABLE", "1").strip().strip("'\"") == "1"
)
# ── [/P43-MAKER] ──────────────────────────────────────────────────────────────

# ── [P48] Strategic Intent Envelope — Phase 48 Autonomous Floating ─────────────
# Python computes a "Strategic Intent" price envelope from L2 depth, regime, and
# volatility.  AdaptiveSpread.compute_strategic_intent() derives (floor, ceil,
# target_px, max_spread_bps) which are forwarded to the Rust bridge via
# BridgeClient.send_strategic_intent().  Rust executes sub-1ms tactical pennying
# within those bounds; Python retains KILL-SWITCH authority via
# cancel_strategic_intent() and never micro-manages the limit price again.
#
# P48_ENABLE           : Mirror of bridge_interface.P48_ENABLE — checked here so
#                        compute_strategic_intent() can guard itself without an
#                        import cycle.  Both must be True for P48 to fire.
# P48_FLOOR_BPS        : Default envelope below target_px (buy side).
# P48_CEIL_BPS         : Default envelope above target_px (sell side).
# P48_SLACK_BPS        : Directional slack on the "tight" edge (1-tick equivalent).
# P48_TTL_SECS         : Default intent TTL forwarded to Rust.
# P48_OBI_WIDEN_FACTOR : When OBI float was applied, multiply floor/ceil by this
#                        factor so the envelope covers the full float distance.
P48_ENABLE            = (
    os.environ.get("P48_ENABLE", "1").strip().strip("'\"") == "1"
)
P48_FLOOR_BPS         = _env_float("P48_FLOOR_BPS",         5.0)
P48_CEIL_BPS          = _env_float("P48_CEIL_BPS",          5.0)
P48_SLACK_BPS         = _env_float("P48_SLACK_BPS",         1.0)
P48_TTL_SECS          = _env_float("P48_TTL_SECS",         30.0)
P48_OBI_WIDEN_FACTOR  = _env_float("P48_OBI_WIDEN_FACTOR",  1.5)
# ── [/P48] ────────────────────────────────────────────────────────────────────


# ── AdaptiveSpread ─────────────────────────────────────────────────────────────
class AdaptiveSpread:
    """
    Tracks per-symbol fill/miss history and dynamically adjusts the BPS offset
    used when constructing limit-order prices.

    [P43-MAKER] OBI-Aware Bid Floating (Phase 43 addition):
    ──────────────────────────────────────────────────────
    set_obi_state(symbol, ofi_score, bid_wall_pulled, ask_wall_pulled) is called
    by the Executor each _cycle() after the P38 OFI injection.  The OBI state
    is consumed inside adjusted_limit_price():

      BUY side: when ofi_score < -P43_OBI_WALL_THRESHOLD (sell wall detected)
        OR (ask_wall_pulled=True AND P43_FLOAT_ON_WALL_PULL=1), the bid is
        floated DOWN by P43_WALL_FLOAT_TICKS × tick_size, capped at
        P43_MAX_FLOAT_TICKS × tick_size below the BPS-adjusted price.

      SELL side: when bid_wall_pulled=True AND P43_FLOAT_ON_WALL_PULL=1, the
        ask is floated UP by P43_WALL_FLOAT_TICKS × tick_size.

    [P12-3] Tick-Inside / Pennying
    ─────────────────────────────
    When `adjusted_limit_price(side, tick, symbol, order_book=book)` is called
    with a live OrderBook, the method attempts to penny the largest passive wall
    within the offset range (see _penny_price for full logic).
    P43 float is applied BEFORE pennying so the penny step can further refine
    the floated price if a wall exists at the new level.

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

        # ── [P43-MAKER] Per-symbol OBI state (injected by Executor each cycle) ─
        # _p43_ofi      : Latest OFI score ∈ [-1, +1] from P38 OBIMonitor.
        #                 Negative = net sell pressure; positive = net buy pressure.
        # _p43_bid_pull : True when a large bid wall just disappeared (fake support).
        # _p43_ask_pull : True when a large ask wall just disappeared (fake supply).
        # _p43_float_active : True if a float adjustment was applied on the last call.
        # _p43_last_float   : Float distance (in ticks) applied on the last call.
        self._p43_ofi:          Dict[str, float] = {}
        self._p43_bid_pull:     Dict[str, bool]  = {}
        self._p43_ask_pull:     Dict[str, bool]  = {}
        self._p43_float_active: Dict[str, bool]  = {}
        self._p43_last_float:   Dict[str, float] = {}
        # ── [/P43-MAKER] ──────────────────────────────────────────────────────

    def attach_auditor(self, auditor: "SlippageAuditor") -> None:
        """Inject the SlippageAuditor so auto_tune() can read fill rates."""
        self._auditor = auditor

    # ── [P43-MAKER] OBI state injection ──────────────────────────────────────

    def set_obi_state(
        self,
        symbol:          str,
        ofi_score:       float,
        bid_wall_pulled: bool,
        ask_wall_pulled: bool,
    ) -> None:
        """
        [P43-MAKER] Inject the latest P38 Order Flow Imbalance snapshot for
        a symbol.  Called by the Executor each _cycle() after the P38 OFI
        injection block so adjusted_limit_price() always has fresh OBI context.

        Parameters
        ----------
        symbol          : str   — instrument symbol (e.g. "BTC").
        ofi_score       : float — OFI ∈ [-1, +1].  Negative = sell pressure.
        bid_wall_pulled : bool  — True when a large bid wall just disappeared.
        ask_wall_pulled : bool  — True when a large ask wall just disappeared.
        """
        try:
            self._p43_ofi[symbol]      = float(ofi_score)
            self._p43_bid_pull[symbol] = bool(bid_wall_pulled)
            self._p43_ask_pull[symbol] = bool(ask_wall_pulled)
            log.debug(
                "[P43-MAKER] set_obi_state %s: ofi=%.3f bid_pull=%s ask_pull=%s",
                symbol, ofi_score, bid_wall_pulled, ask_wall_pulled,
            )
        except Exception as exc:
            log.debug("[P43-MAKER] set_obi_state error %s: %s", symbol, exc)

    def get_obi_calibration_snapshot(self, symbol: str) -> dict:
        """
        [P43-MAKER] Serialisable snapshot of the current OBI float state for
        the dashboard OBI Calibration tile.

        Returns
        -------
        dict with keys:
            ofi_score        : float  — latest P38 OFI value
            bid_wall_pulled  : bool
            ask_wall_pulled  : bool
            float_active     : bool   — True if float was applied last call
            last_float_ticks : float  — tick distance of last float (0 if none)
            offset_bps       : float  — current spread offset in bps
            p43_enabled      : bool
        """
        return {
            "ofi_score":        round(self._p43_ofi.get(symbol, 0.0), 4),
            "bid_wall_pulled":  self._p43_bid_pull.get(symbol, False),
            "ask_wall_pulled":  self._p43_ask_pull.get(symbol, False),
            "float_active":     self._p43_float_active.get(symbol, False),
            "last_float_ticks": round(self._p43_last_float.get(symbol, 0.0), 2),
            "offset_bps":       round(self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS), 4),
            "wall_threshold":   P43_OBI_WALL_THRESHOLD,
            "float_ticks":      P43_WALL_FLOAT_TICKS,
            "max_float_ticks":  P43_MAX_FLOAT_TICKS,
            "p43_enabled":      P43_ENABLE,
        }

    # ── [/P43-MAKER] ──────────────────────────────────────────────────────────
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

        [P43-MAKER] OBI-Aware Float:
        Applied AFTER BPS offset, BEFORE pennying.  When a sell wall is
        detected (ofi_score < -P43_OBI_WALL_THRESHOLD) the BUY bid is
        floated DOWN by up to P43_MAX_FLOAT_TICKS × tick_size to avoid
        being immediately hit and to earn the maker rebate.
        """
        offset_bps  = self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS)
        offset_frac = offset_bps / 10_000.0

        if side == "buy":
            base_px    = tick.ask if tick.ask > 0 else tick.last
            vanilla_px = base_px * (1 - offset_frac)
        else:
            base_px    = tick.bid if tick.bid > 0 else tick.last
            vanilla_px = base_px * (1 + offset_frac)

        # ── [P43-MAKER] OBI-Aware Bid/Ask Floating ────────────────────────────
        # Runs AFTER the vanilla BPS offset is applied, BEFORE P12-3 pennying.
        # This ensures the base price is always within the maker zone before
        # we try to penny an existing wall at the new level.
        adjusted_px = vanilla_px
        try:
            if P43_ENABLE:
                tick_sz = self._tick_sz(symbol)
                if tick_sz > 0:
                    _ofi       = self._p43_ofi.get(symbol, 0.0)
                    _bid_pull  = self._p43_bid_pull.get(symbol, False)
                    _ask_pull  = self._p43_ask_pull.get(symbol, False)

                    if side == "buy":
                        # Sell-wall or evaporating ask wall → float bid lower
                        _sell_wall = _ofi < -P43_OBI_WALL_THRESHOLD
                        _ask_ghost = _ask_pull and P43_FLOAT_ON_WALL_PULL
                        if _sell_wall or _ask_ghost:
                            _float_ticks = min(P43_WALL_FLOAT_TICKS, P43_MAX_FLOAT_TICKS)
                            _float_dist  = _float_ticks * tick_sz
                            adjusted_px  = vanilla_px - _float_dist
                            self._p43_float_active[symbol] = True
                            self._p43_last_float[symbol]   = _float_ticks
                            log.info(
                                "[P43-MAKER] BUY float %s: ofi=%.3f "
                                "(sell_wall=%s ask_ghost=%s) "
                                "bid %.8f → %.8f (−%.1f ticks × %.8f)",
                                symbol, _ofi, _sell_wall, _ask_ghost,
                                vanilla_px, adjusted_px,
                                _float_ticks, tick_sz,
                            )
                        else:
                            self._p43_float_active[symbol] = False
                            self._p43_last_float[symbol]   = 0.0

                    elif side == "sell":
                        # Disappearing bid wall → float ask higher (avoid knife)
                        _bid_ghost = _bid_pull and P43_FLOAT_ON_WALL_PULL
                        if _bid_ghost:
                            _float_ticks = min(P43_WALL_FLOAT_TICKS, P43_MAX_FLOAT_TICKS)
                            _float_dist  = _float_ticks * tick_sz
                            adjusted_px  = vanilla_px + _float_dist
                            self._p43_float_active[symbol] = True
                            self._p43_last_float[symbol]   = _float_ticks
                            log.info(
                                "[P43-MAKER] SELL float %s: bid_wall_pulled=%s "
                                "ask %.8f → %.8f (+%.1f ticks × %.8f)",
                                symbol, _bid_pull,
                                vanilla_px, adjusted_px,
                                _float_ticks, tick_sz,
                            )
                        else:
                            self._p43_float_active[symbol] = False
                            self._p43_last_float[symbol]   = 0.0
        except Exception as _p43_exc:
            log.debug("[P43-MAKER] float error %s %s: %s", side, symbol, _p43_exc)
            adjusted_px = vanilla_px
        # ── [/P43-MAKER] ──────────────────────────────────────────────────────

        # ── [P12-3] Pennying ──────────────────────────────────────────────────
        if order_book is not None:
            penny_px = self._penny_price(side, symbol, adjusted_px,
                                          base_px, offset_frac, order_book)
            if penny_px is not None:
                return penny_px
            log.debug(
                "[P12-3] Penny skipped for %s %s — using adjusted=%.8f",
                side, symbol, adjusted_px,
            )

        return adjusted_px

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

    def compute_strategic_intent(
        self,
        side:       str,
        tick:       "Tick",
        symbol:     str,
        order_book: Optional["OrderBook"] = None,
        target_px:  Optional[float] = None,
    ) -> Tuple[float, float, float, float]:
        """
        [P48] Compute the Strategic Intent envelope for Rust autonomous floating.

        Python (Strategic Supervisor) derives safe price bounds from the current
        L2 depth, OBI state, and regime context.  The Rust bridge (Tactical
        Predator) will float the limit order within [price_floor, price_ceil]
        at sub-1ms cadence until the TTL expires or Python fires the kill-switch.

        Algorithm
        ---------
        1. Determine `target_px`:
             If the caller passes `target_px` (already computed by
             adjusted_limit_price() earlier in the same cycle), use it directly.
             This avoids a double call that would corrupt P43 OBI side-effects
             (_p43_float_active / _p43_last_float) and cause a price race.
             If not provided, compute via adjusted_limit_price() — full pipeline
             runs: BPS offset → OBI float → pennying.
        2. Derive envelope width:
             base_width = P48_FLOOR_BPS (buy) or P48_CEIL_BPS (sell).
             If P43 OBI float was active on the last call, widen by
             P48_OBI_WIDEN_FACTOR to cover the full float distance so Rust can
             follow the wall price without breaching Python's bounds.
        3. Set directional bounds:
             BUY  → floor = target * (1 - width/10000), ceil = target * (1 + slack/10000)
             SELL → floor = target * (1 - slack/10000), ceil = target * (1 + width/10000)
        4. Safety clamp: ensure floor < target ≤ ceil; re-centre if violated.
        5. max_spread_bps = current AdaptiveSpread offset × 2 (maker rebate safe zone).

        Parameters
        ----------
        target_px : float, optional
            Pre-computed limit price from adjusted_limit_price() in the same
            cycle.  When provided the internal call to adjusted_limit_price() is
            skipped entirely, preserving P43 OBI state and eliminating the race.

        Returns
        -------
        (price_floor, price_ceil, target_px, max_spread_bps) — all finite positives.
        Raises ValueError if target_px cannot be computed (bad tick).
        """
        import math as _math

        # ── Step 1: target price ──────────────────────────────────────────────
        if target_px is None:
            # Caller did not supply a pre-computed price — compute it now.
            # NOTE: Only call this path when compute_strategic_intent() is used
            # in isolation (e.g. tests).  The hot-path in executor._execute_order
            # always passes the already-computed raw_price to avoid a double call.
            target_px = self.adjusted_limit_price(side, tick, symbol, order_book)

        if not _math.isfinite(target_px) or target_px <= 0:
            raise ValueError(
                f"[P48] compute_strategic_intent: bad target_px={target_px!r} "
                f"for {side} {symbol}"
            )

        # ── Step 2: envelope width — widen when OBI float was active ──────────
        if side == "buy":
            _base_width = P48_FLOOR_BPS
        else:
            _base_width = P48_CEIL_BPS

        if P43_ENABLE and self._p43_float_active.get(symbol, False):
            # OBI float pushed price away from BBO — widen envelope to match
            _float_ticks = self._p43_last_float.get(symbol, 0.0)
            tick_sz      = self._tick_sz(symbol)
            if tick_sz > 0 and _float_ticks > 0:
                _float_bps = (_float_ticks * tick_sz) / target_px * 10_000
                _base_width = max(_base_width, _float_bps * P48_OBI_WIDEN_FACTOR)

        _slack = P48_SLACK_BPS

        # ── Step 3: directional bounds ────────────────────────────────────────
        if side == "buy":
            price_floor = target_px * (1.0 - _base_width / 10_000.0)
            price_ceil  = target_px * (1.0 + _slack       / 10_000.0)
        else:  # sell
            price_floor = target_px * (1.0 - _slack       / 10_000.0)
            price_ceil  = target_px * (1.0 + _base_width  / 10_000.0)

        # ── Step 4: safety clamp — floor < target ≤ ceil ─────────────────────
        if price_floor >= price_ceil or not (price_floor < target_px <= price_ceil):
            half = _base_width / 10_000.0
            price_floor = target_px * (1.0 - half)
            price_ceil  = target_px * (1.0 + half)
            target_px   = max(price_floor, min(price_ceil, target_px))

        # ── Step 5: max spread ────────────────────────────────────────────────
        max_spread_bps = self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS) * 2.0

        log.debug(
            "[P48] compute_strategic_intent %s %s: floor=%.8f target=%.8f "
            "ceil=%.8f width_bps=%.2f max_spread=%.2f",
            side, symbol, price_floor, target_px, price_ceil,
            _base_width, max_spread_bps,
        )
        return price_floor, price_ceil, target_px, max_spread_bps

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
        base = {
            "offset_bps":   round(self._offsets.get(symbol, P7_SPREAD_MIN_OFFSET_BPS), 4),
            "fill_rate":    round(fr, 4),
            "fill_rate_1h": round(fill_1h, 4) if fill_1h is not None else None,
            "samples":      len(recent),
            "tick_sz":      self._tick_sizes.get(symbol, 0.0),
        }
        # [P43-MAKER] Append OBI calibration snapshot inline
        base["p43"] = self.get_obi_calibration_snapshot(symbol)
        return base


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
        except Exception as _prune_exc:
            log.debug("[P7E-PRUNE] _prune_fill_events(%s): unexpected error — %s", symbol, _prune_exc)
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
