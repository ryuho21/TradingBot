"""
arbitrator.py  —  Phase 6: Cross-Exchange Arbitrage Scout
          +  Phase 15: Institutional Arbitrator (Coinbase Oracle)
          +  Phase 16: Arbitrage Bridge & Atomic Execution

Phase 6:
  Cross-exchange spread detection across OKX, Binance, Bybit via ccxt.
  Requires >= 2 reachable exchanges before signalling.
  Per-exchange fault isolation — any failure is silently skipped.

Phase 15 additions:
  [P15-1] CoinbaseOracle  — Read-Only WebSocket sensor
    - Streams `market_trades` channel via native WSS (aiohttp).
    - Detects WHALE_SWEEP: single trade >= CB_WHALE_MULTIPLIER × rolling avg.
    - Maintains TradeStats rolling window per symbol (24 h default).
    - OracleSignal TTL: 30 s (configurable).
    - Minimum avg_size floor to prevent low-liquidity false positives.
    - Tracks _ws_connected to invalidate signals during reconnect windows.
    - ⚠  EXECUTION LOCK: zero Coinbase order placement. OKX-only writes.

  [P15-3] JWT/CDP Authentication — Coinbase Advanced Trade V3 (ES256)
    - COINBASE_API_KEY    : key-name path ("organisations/.../apiKeys/...")
    - COINBASE_API_SECRET : PEM EC private key; literal \\n in .env reconstructed.
    - Falls back to unauthenticated (public channels) if PyJWT/cryptography absent.

  [P15-4] GlobalTapeAggregator
    - Merges Coinbase + Binance trade velocities.
    - is_high_volatility_mode() -> bool when combined tps >= P15_GLOBAL_TAPE_HV_TPS.
    - hv_stop_extra_pct() -> extra stop-gap % for Executor to widen stops.
    - NOTE: hv_stop_extra_pct() is a per-cycle READ value. Callers must apply
      it as a temporary modifier and must NOT write it back to pos.trail_multiplier.

Phase 16 additions:
  [P16-1] on_whale_signal async callback on CoinbaseOracle

Bug-fix changelog (this revision):
  [FIX-AVG-NONE]  TradeStats.avg_size() always returns a non-negative float.
                  The return value is explicitly guarded against zero before
                  any division in _on_trade().

  [FIX-DIV-ZERO]  _on_trade() now has an explicit `effective_avg > 0` gate
                  immediately before the ratio calculation.  A zero or
                  near-zero effective_avg (even after the floor) cannot reach
                  the division.

  [FIX-TPS-NONE]  TradeStats.tps() guards window_secs > 0 (was already present)
                  and also guards the deque count path against an empty deque.

  [FIX-STATS-INIT]  CoinbaseOracle ensures the _stats dict always contains a
                  TradeStats instance for every symbol in self.symbols before
                  the WS loop starts, preventing KeyError in _on_trade().
"""
from __future__ import annotations

import asyncio
import collections
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from data_hub import DataHub                       # noqa: F401
    from brain import IntelligenceEngine               # noqa: F401
    from bridge_interface import BridgeClient          # noqa: F401  [P40]

try:
    import ujson as _json
except ImportError:
    import json as _json  # type: ignore[no-redef]
    logging.getLogger("arbitrator").warning(
        "[P15] ujson not installed — using stdlib json. "
        "Run: pip install ujson  for lower CPU on Ryzen 5."
    )

import aiohttp

log = logging.getLogger("arbitrator")

# ── Phase 6 Config ─────────────────────────────────────────────────────────────
ARB_SPREAD_MIN_PCT = float(os.environ.get("ARB_SPREAD_MIN_PCT", "0.15"))
ARB_POLL_INTERVAL  = float(os.environ.get("ARB_POLL_INTERVAL",  "5.0"))
ARB_FETCH_TIMEOUT  = float(os.environ.get("ARB_FETCH_TIMEOUT",  "8.0"))
ARB_MAX_PRICE_AGE  = float(os.environ.get("ARB_MAX_PRICE_AGE",  "30.0"))

_BINANCE_KEY    = os.environ.get("BINANCE_API_KEY",  "")
_BINANCE_SECRET = os.environ.get("BINANCE_SECRET",   "")
_BYBIT_KEY      = os.environ.get("BYBIT_API_KEY",    "")
_BYBIT_SECRET   = os.environ.get("BYBIT_SECRET",     "")
_OKX_KEY        = os.environ.get("OKX_API_KEY",      "")
_OKX_SECRET     = os.environ.get("OKX_SECRET",       "")
_OKX_PASS       = os.environ.get("OKX_PASSPHRASE",   "")
_OKX_DEMO       = os.environ.get("OKX_DEMO_MODE", "0").strip() == "1"

# ── [P15] Coinbase Oracle Config ──────────────────────────────────────────────
CB_WS_URL             = os.environ.get("CB_WS_URL",             "wss://advanced-trade-ws.coinbase.com")
CB_API_KEY            = os.environ.get("COINBASE_API_KEY",      "")
CB_API_SECRET_RAW     = os.environ.get("COINBASE_API_SECRET",   "")
CB_WHALE_MULTIPLIER   = float(os.environ.get("CB_WHALE_MULTIPLIER",   "10.0"))
CB_AVG_WINDOW_SECS    = float(os.environ.get("CB_AVG_WINDOW_SECS",    "86400.0"))
CB_RECONNECT_DELAY    = float(os.environ.get("CB_RECONNECT_DELAY",    "5.0"))
CB_SIGNAL_TTL_SECS    = float(os.environ.get("CB_SIGNAL_TTL_SECS",    "30.0"))
CB_MIN_WARM_TRADES    = int  (os.environ.get("CB_MIN_WARM_TRADES",    "30"))
CB_MIN_AVG_SIZE_BTC   = float(os.environ.get("CB_MIN_AVG_SIZE_BTC",  "0.01"))

# ── [P15-4] Global Tape / High-Volatility Mode ────────────────────────────────
GLOBAL_TAPE_HV_TPS      = float(os.environ.get("P15_GLOBAL_TAPE_HV_TPS",    "200.0"))
GLOBAL_TAPE_WINDOW_SECS = float(os.environ.get("P15_GLOBAL_TAPE_WIN_SECS",  "5.0"))
HV_MODE_STOP_WIDE_PCT   = float(os.environ.get("P15_HV_STOP_WIDE_PCT",      "0.3"))

# ── [P16] Arbitrage Bridge Config ─────────────────────────────────────────────
P16_WHALE_TRIGGER_MULT   = float(os.environ.get("P16_WHALE_TRIGGER_MULT",  "10.0"))
P16_BRIDGE_COOLDOWN_SECS = float(os.environ.get("P16_BRIDGE_COOLDOWN_SECS", "30.0"))

# ── [P15-2] Oracle conviction multipliers ────────────────────────────────────
ORACLE_WHALE_BUY  = "WHALE_SWEEP_BUY"
ORACLE_WHALE_SELL = "WHALE_SWEEP_SELL"
ORACLE_NEUTRAL    = "NEUTRAL"

WhaleCallback = Callable[["OracleSignal"], Awaitable[None]]


# ══════════════════════════════════════════════════════════════════════════════
# [P15-3] PEM reconstruction + JWT builder
# ══════════════════════════════════════════════════════════════════════════════

def reconstruct_pem(raw: str) -> str:
    if not raw:
        return ""
    pem = raw.strip().replace("\\n", "\n")
    if "-----BEGIN" not in pem:
        pem = (
            "-----BEGIN EC PRIVATE KEY-----\n"
            + pem.strip()
            + "\n-----END EC PRIVATE KEY-----"
        )
    return pem


CB_API_SECRET_PEM: str = reconstruct_pem(CB_API_SECRET_RAW)


def build_cb_jwt(api_key: str, secret_pem: str) -> str:
    if not api_key or not secret_pem:
        return ""
    try:
        import jwt as _jwt
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        now     = int(time.time())
        payload = {
            "sub": api_key,
            "iss": "cdp",
            "nbf": now,
            "exp": now + 120,
            "aud": ["public_websocket_api"],
        }
        headers = {
            "kid":   api_key,
            "nonce": uuid.uuid4().hex[:16],
        }
        private_key = load_pem_private_key(secret_pem.encode(), password=None)
        token = _jwt.encode(payload, private_key, algorithm="ES256", headers=headers)
        return token if isinstance(token, str) else token.decode()

    except ImportError:
        log.warning(
            "[P15-3] PyJWT or cryptography not installed — "
            "Coinbase Oracle running unauthenticated (public channels). "
            "Run: pip install PyJWT cryptography"
        )
        return ""
    except Exception as exc:
        log.error("[P15-3] JWT build failed: %s", exc)
        return ""


# ══════════════════════════════════════════════════════════════════════════════
# Phase 6 data structures
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class ExchangePrice:
    exchange: str
    bid:      float
    ask:      float
    ts:       float = field(default_factory=time.time)


@dataclass
class ArbOpportunity:
    symbol:        str
    buy_exchange:  str
    sell_exchange: str
    buy_price:     float
    sell_price:    float
    spread_pct:    float
    prices:        Dict[str, ExchangePrice] = field(default_factory=dict)
    ts:            float = field(default_factory=time.time)


# ══════════════════════════════════════════════════════════════════════════════
# [P15-1] Oracle data structures
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class OracleSignal:
    symbol:           str
    signal_type:      str
    trade_size:       float
    avg_size:         float
    multiplier:       float
    ts:               float = field(default_factory=time.time)
    ttl:              float = CB_SIGNAL_TTL_SECS
    cancel_buys_flag: bool  = False

    def is_valid(self) -> bool:
        return (time.time() - self.ts) < self.ttl

    def is_bullish(self) -> bool:
        return self.signal_type == ORACLE_WHALE_BUY and self.is_valid()

    def is_bearish(self) -> bool:
        return self.signal_type == ORACLE_WHALE_SELL and self.is_valid()


# ══════════════════════════════════════════════════════════════════════════════
# [P15-1] Rolling trade statistics
# ══════════════════════════════════════════════════════════════════════════════

class TradeStats:
    """
    Per-symbol rolling window of Coinbase public trade sizes.

    [FIX-AVG-NONE]  avg_size() always returns a non-negative float (0.0 when
    the deque is empty or all events are outside the window).  It never
    returns None.

    [FIX-TPS-NONE]  tps() guards both the empty-deque case and the
    zero-window case.
    """
    __slots__ = ("_window", "_events")

    def __init__(self, window_secs: float = CB_AVG_WINDOW_SECS):
        self._window: float = window_secs
        self._events: Deque[Tuple[float, float]] = collections.deque(maxlen=100_000)

    def add(self, size: float) -> None:
        """Append a new (local_timestamp, size) event."""
        # [FIX-AVG-NONE] Reject non-positive sizes so avg_size() stays valid.
        if size is None or size <= 0.0:
            return
        self._events.append((time.time(), float(size)))

    def count(self) -> int:
        return len(self._events)

    def avg_size(self) -> float:
        """
        Rolling average trade size over the last _window seconds.

        [FIX-AVG-NONE]  Returns 0.0 (not None) when the deque is empty or
        no events fall within the window.  The caller must check > 0 before
        dividing by this value.
        """
        if not self._events:
            return 0.0

        try:
            cutoff = time.time() - self._window
            sizes  = [sz for ts, sz in self._events if ts >= cutoff and sz is not None]
            if not sizes:
                return 0.0
            total = sum(sizes)
            count = len(sizes)
            # Guard against zero count (logically impossible here, but defensive).
            return float(total) / float(count) if count > 0 else 0.0
        except Exception as exc:
            log.debug("[P15] TradeStats.avg_size() exception: %s", exc)
            return 0.0

    def tps(self, window_secs: float = GLOBAL_TAPE_WINDOW_SECS) -> float:
        """
        Trades per second over the last window_secs.

        [FIX-TPS-NONE]  Returns 0.0 when the deque is empty or window_secs
        is zero/negative.
        """
        if not self._events:
            return 0.0
        if window_secs <= 0.0:
            return 0.0
        try:
            cutoff = time.time() - window_secs
            n      = sum(1 for ts, _ in self._events if ts >= cutoff)
            return float(n) / float(window_secs)
        except Exception as exc:
            log.debug("[P15] TradeStats.tps() exception: %s", exc)
            return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# [P15-1 + P15-3 + P16-1] CoinbaseOracle
# ══════════════════════════════════════════════════════════════════════════════

class CoinbaseOracle:
    """
    Read-only WebSocket observer for Coinbase Advanced Trade.

    [FIX-STATS-INIT]  The _stats dict is pre-populated for every symbol in
    __init__ so _on_trade() never encounters a missing key.

    [FIX-DIV-ZERO]  _on_trade() checks effective_avg > 0 before computing
    ratio.  This is defence-in-depth: even if avg_size() incorrectly returned
    0.0 or if CB_MIN_AVG_SIZE_BTC were set to 0, no ZeroDivisionError occurs.

    ⚠  EXECUTION LOCK ⚠  This class NEVER places orders.
    """

    def __init__(self, symbols: List[str]):
        self.symbols   = [s.upper() for s in symbols]
        self._products = [f"{s}-USD" for s in self.symbols]

        # [FIX-STATS-INIT] Always initialise for every symbol.
        self._stats:   Dict[str, TradeStats]   = {s: TradeStats() for s in self.symbols}
        self._signals: Dict[str, OracleSignal] = {}
        self._running       = False
        self._ws_connected  = False
        self._last_warn:    Dict[str, float] = {}

        self._whale_callback: Optional[WhaleCallback] = None
        self._p16_last_fire:  Dict[str, float]        = {}

        # [P34.1-SYNTH] Track latest Coinbase trade price per symbol for
        # synthetic mid-price calculation: {symbol: (price, timestamp)}
        self._last_trade_px: Dict[str, tuple] = {}

    # ── [P16-1] Callback registration ────────────────────────────────────────

    def register_whale_callback(self, cb: WhaleCallback) -> None:
        self._whale_callback = cb
        log.info(
            "[P16-1] Whale callback registered. trigger_mult=%.1f× cooldown=%.0fs",
            P16_WHALE_TRIGGER_MULT, P16_BRIDGE_COOLDOWN_SECS,
        )

    def _p16_cooldown_ok(self, symbol: str) -> bool:
        return (
            time.time() - self._p16_last_fire.get(symbol, 0.0)
            >= P16_BRIDGE_COOLDOWN_SECS
        )

    def _p16_mark_fired(self, symbol: str) -> None:
        self._p16_last_fire[symbol] = time.time()

    # ── Public interface ──────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        # [FIX-STATS-INIT] Re-seed _stats in case symbols changed after __init__.
        for s in self.symbols:
            if s not in self._stats:
                self._stats[s] = TradeStats()
        log.info(
            "[P15-1] CoinbaseOracle initialised. products=%s auth=%s p16_bridge=%s",
            self._products,
            bool(CB_API_KEY and CB_API_SECRET_PEM),
            self._whale_callback is not None,
        )

    async def stop(self) -> None:
        self._running      = False
        self._ws_connected = False
        log.info("[P15-1] CoinbaseOracle stopped.")

    async def run(self) -> None:
        log.info("[P15-1] Coinbase Oracle WS loop starting.")
        backoff = CB_RECONNECT_DELAY
        while self._running:
            try:
                self._ws_connected = False
                await self._ws_session()
                backoff = CB_RECONNECT_DELAY
            except asyncio.CancelledError:
                log.info("[P15-1] Oracle WS loop cancelled — shutting down.")
                return
            except Exception as exc:
                self._ws_connected = False
                self._warn_once(
                    "ws_loop",
                    f"WS error: {exc} — reconnecting in {backoff:.0f}s",
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)

    def latest_signal(self, symbol: str) -> Optional[OracleSignal]:
        if not self._ws_connected:
            return None
        sig = self._signals.get(symbol.upper())
        return sig if (sig and sig.is_valid()) else None

    def latest_price(self, symbol: str) -> Optional[tuple]:
        """
        [P34.1-SYNTH] Return the latest Coinbase trade price for symbol as
        ``(price: float, timestamp: float)`` or ``None`` if unavailable.
        The timestamp is a Unix epoch float from ``time.time()``.
        """
        return self._last_trade_px.get(symbol.upper())

    def tps(self, symbol: str) -> float:
        stats = self._stats.get(symbol.upper())
        return stats.tps() if stats else 0.0

    def all_symbols_tps(self) -> float:
        return sum(s.tps() for s in self._stats.values())

    # ── WebSocket session ─────────────────────────────────────────────────────

    async def _ws_session(self) -> None:
        jwt_token = build_cb_jwt(CB_API_KEY, CB_API_SECRET_PEM)
        sub_msg   = _json.dumps(self._build_subscribe(jwt_token))

        timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.ws_connect(CB_WS_URL, heartbeat=20.0) as ws:
                self._ws_connected = True
                log.info(
                    "[P15-1] Coinbase WS connected. url=%s auth=%s",
                    CB_WS_URL, bool(jwt_token),
                )
                await ws.send_str(sub_msg)

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            await self._handle(_json.loads(msg.data))
                        except Exception as exc:
                            log.debug("[P15-1] WS parse error: %s", exc)
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        log.warning("[P15-1] WS error frame: %s", ws.exception())
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        log.info("[P15-1] WS closed by server.")
                        break

        self._ws_connected = False

    def _build_subscribe(self, jwt_token: str) -> dict:
        msg: dict = {
            "type":        "subscribe",
            "product_ids": self._products,
            "channel":     "market_trades",
        }
        if jwt_token:
            msg["jwt"] = jwt_token
        return msg

    # ── Message routing ───────────────────────────────────────────────────────

    async def _handle(self, msg: dict) -> None:
        ch = msg.get("channel") or msg.get("type") or ""

        if ch == "subscriptions":
            log.info(
                "[P15-1] Coinbase subscriptions confirmed: %s",
                msg.get("channels"),
            )
            return

        if ch == "market_trades":
            for ev in msg.get("events") or []:
                for trade in ev.get("trades") or []:
                    await self._on_trade(trade)
            return

        if ch == "error":
            log.warning("[P15-1] Coinbase error msg: %s", msg.get("message"))

    # ── Whale detection ───────────────────────────────────────────────────────

    async def _on_trade(self, trade: dict) -> None:
        """
        Evaluate one Coinbase public trade for whale-level size.

        [FIX-AVG-NONE]  avg_size() is checked > 0 before being used as the
        denominator for any ratio calculation.

        [FIX-DIV-ZERO]  effective_avg is checked > 0 immediately before the
        ratio = size / effective_avg line.  Even if CB_MIN_AVG_SIZE_BTC is
        set to 0 in the environment, this guard prevents ZeroDivisionError.

        [FIX-STATS-INIT]  sym is looked up in self._stats; if it is somehow
        absent (hot-added symbol), it is initialised on the fly.
        """
        product_id = trade.get("product_id", "")
        sym        = product_id.replace("-USD", "").upper()

        # [FIX-STATS-INIT] Gracefully handle symbols added after __init__.
        if sym not in self._stats:
            if sym in self.symbols:
                self._stats[sym] = TradeStats()
            else:
                return

        try:
            raw_size = trade.get("size", 0)
            raw_side = trade.get("side", "")
            size = float(raw_size) if raw_size is not None else 0.0
            side = str(raw_side).upper() if raw_side is not None else ""
        except (ValueError, TypeError):
            return

        if size <= 0.0 or side not in ("BUY", "SELL"):
            return

        # [P34.1-SYNTH] Track last trade price for synthetic mid-price calculation.
        try:
            raw_px = trade.get("price")
            if raw_px is not None:
                px = float(raw_px)
                if px > 0.0:
                    self._last_trade_px[sym] = (px, time.time())
        except (ValueError, TypeError):
            pass

        stats = self._stats[sym]

        # Snapshot the average BEFORE adding the new trade (prevents self-inflation).
        avg = stats.avg_size()  # always returns float >= 0.0

        # Add the new trade using local wall-clock time.
        stats.add(size)

        # Need a warm baseline before the average is trustworthy.
        if stats.count() < CB_MIN_WARM_TRADES:
            return

        # [FIX-AVG-NONE]  avg must be a positive number for any comparison
        # to be meaningful.  avg_size() returns 0.0 on empty / no-window data.
        if avg <= 0.0:
            return

        # [P15] Apply minimum avg_size floor to suppress low-liquidity noise.
        # CB_MIN_AVG_SIZE_BTC is read from env; guard against it being <= 0.
        floor = CB_MIN_AVG_SIZE_BTC if CB_MIN_AVG_SIZE_BTC > 0.0 else 1e-8
        effective_avg = max(avg, floor)

        # [FIX-DIV-ZERO]  Final explicit guard before division.
        if effective_avg <= 0.0:
            log.debug(
                "[P15] _on_trade: effective_avg <= 0 for %s after floor — "
                "skipping (avg=%.8f floor=%.8f)",
                sym, avg, floor,
            )
            return

        ratio = size / effective_avg

        if ratio < CB_WHALE_MULTIPLIER:
            return

        # ── Whale confirmed ───────────────────────────────────────────────────
        sig_type        = ORACLE_WHALE_BUY if side == "BUY" else ORACLE_WHALE_SELL
        cancel_buys_flg = (sig_type == ORACLE_WHALE_SELL)

        sig = OracleSignal(
            symbol           = sym,
            signal_type      = sig_type,
            trade_size       = size,
            avg_size         = effective_avg,
            multiplier       = round(ratio, 2),
            ts               = time.time(),
            ttl              = CB_SIGNAL_TTL_SECS,
            cancel_buys_flag = cancel_buys_flg,
        )
        self._signals[sym] = sig

        log.warning(
            "[P15] Oracle: Coinbase Whale detected (+50%% Conviction) "
            "| %s %s size=%.4f avg=%.4f mult=%.1f×",
            sym, sig_type, size, effective_avg, ratio,
        )

        # ── [P16-1] Express Lane ─────────────────────────────────────────────
        if (
            ratio >= P16_WHALE_TRIGGER_MULT
            and self._whale_callback is not None
            and self._p16_cooldown_ok(sym)
        ):
            self._p16_mark_fired(sym)
            direction = "long" if side == "BUY" else "short"
            log.warning(
                "[P16-1] Express Lane TRIGGERED: %s direction=%s mult=%.1f× "
                "— scheduling atomic callback",
                sym, direction, ratio,
            )
            asyncio.ensure_future(self._invoke_callback(sig))

    async def _invoke_callback(self, sig: OracleSignal) -> None:
        try:
            await self._whale_callback(sig)
        except Exception as exc:
            log.error(
                "[P16-1] Whale callback raised an exception for %s: %s",
                sig.symbol, exc, exc_info=True,
            )

    def _warn_once(self, key: str, msg: str, cooldown: float = 30.0) -> None:
        now  = time.time()
        last = self._last_warn.get(key, 0.0)
        if now - last >= cooldown:
            log.warning("[P15-1] CoinbaseOracle: %s", msg)
            self._last_warn[key] = now


# ══════════════════════════════════════════════════════════════════════════════
# [P15-4] GlobalTapeAggregator
# ══════════════════════════════════════════════════════════════════════════════

class GlobalTapeAggregator:
    """
    Combines Coinbase + Binance trade velocities to determine High Volatility Mode.

    ⚠  CRITICAL USAGE CONTRACT ⚠
    hv_stop_extra_pct() returns a read-only per-cycle modifier value.
    Callers MUST NOT write it back to pos.trail_multiplier.
    """

    def __init__(self, oracle: CoinbaseOracle, arbitrator: "Arbitrator"):
        self._oracle = oracle
        self._arb    = arbitrator

    def coinbase_tps(self) -> float:
        try:
            return float(self._oracle.all_symbols_tps())
        except Exception:
            return 0.0

    def binance_tps(self) -> float:
        return 0.0

    def combined_tps(self) -> float:
        return self.coinbase_tps() + self.binance_tps()

    def is_high_volatility_mode(self) -> bool:
        hv = self.combined_tps() >= GLOBAL_TAPE_HV_TPS
        if hv:
            log.debug(
                "[P15-4] High Volatility Mode ACTIVE: combined_tps=%.1f >= %.0f",
                self.combined_tps(), GLOBAL_TAPE_HV_TPS,
            )
        return hv

    def hv_stop_extra_pct(self) -> float:
        return HV_MODE_STOP_WIDE_PCT if self.is_high_volatility_mode() else 0.0

    def status_snapshot(self) -> dict:
        tps = self.combined_tps()
        hv  = tps >= GLOBAL_TAPE_HV_TPS
        return {
            "coinbase_tps":         round(self.coinbase_tps(), 2),
            "binance_tps":          round(self.binance_tps(),  2),
            "combined_tps":         round(tps, 2),
            "hv_threshold_tps":     GLOBAL_TAPE_HV_TPS,
            "high_volatility_mode": hv,
            "hv_stop_extra_pct":    HV_MODE_STOP_WIDE_PCT if hv else 0.0,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Phase 6: Arbitrator
# ══════════════════════════════════════════════════════════════════════════════

class Arbitrator:
    """
    Cross-exchange price monitor using ccxt.async_support.

    Phase 15: self.oracle (CoinbaseOracle) injected by main after construction.

    Phase 40: self._bridge (BridgeClient) injected by main.set_bridge() so
    the Arbitrator reads the "Healed" equity stream from the Rust bridge for
    position sizing rather than raw WS readings that may carry ghost-state $0.
    """

    def __init__(self, symbols: List[str]):
        self.symbols  = [s.upper() for s in symbols]
        self._exs:    Dict[str, any]                      = {}
        self._prices: Dict[str, Dict[str, ExchangePrice]] = {}
        self._lock    = asyncio.Lock()
        self._running = False
        self._ex_last_warn: Dict[str, float] = {}

        self.oracle: Optional[CoinbaseOracle] = None

        # ── [P40] Bridge reference ────────────────────────────────────────────
        # Set by main.py via set_bridge(bridge) after BridgeClient is created.
        # Gives the Arbitrator access to the Rust-validated "Binary Truth" equity
        # so position sizing is always based on the healed equity, never on a
        # ghost-state $0 reading from the raw OKX WebSocket.
        self._bridge: Optional["BridgeClient"] = None
        # ── [/P40] ───────────────────────────────────────────────────────────

    # ── [P40] Bridge integration ──────────────────────────────────────────────

    def set_bridge(self, bridge: Optional["BridgeClient"]) -> None:
        """
        [P40] Inject the shared BridgeClient so the Arbitrator can read the
        Rust-healed equity stream for position sizing.

        Called once by main.py immediately after bridge creation:
            arb.set_bridge(bridge)

        Thread-safe: Python's GIL protects the simple attribute assignment.
        """
        self._bridge = bridge
        if bridge is not None:
            log.info(
                "[P40] Arbitrator: BridgeClient registered — "
                "position sizing will use Rust-healed equity (Binary Truth)."
            )

    @property
    def healed_equity(self) -> float:
        """
        [P40] Return the Rust bridge's last verified good equity value.

        This is the "Binary Truth" — the Rust bridge reconciles OKX WebSocket
        ghost reads ($0.00 account_update) against the REST API and emits a
        healed equity that is always reliable.

        Falls back to 0.0 when the bridge is not available so callers can
        check ``if arb.healed_equity > 0`` before using it.

        Usage in position sizing:
            eq = arb.healed_equity or executor._equity  # prefer healed
        """
        if self._bridge is None:
            return 0.0
        return self._bridge.equity  # bridge.equity is always the last healed value

    @property
    def bridge_connected(self) -> bool:
        """[P40] True when the Rust bridge IPC connection is alive."""
        return self._bridge is not None and self._bridge.connected

    async def start(self) -> None:
        try:
            import ccxt.async_support as ccxt  # type: ignore
        except ImportError:
            log.error(
                "ccxt not installed. Run: pip install ccxt>=4.3\n"
                "Arbitrator will be disabled for this session."
            )
            return

        is_demo = os.environ.get("OKX_DEMO_MODE", "0") == "1"

        exchange_cfgs = {
            "okx": {
                "cls":     ccxt.okx,
                "enabled": not is_demo,
                "config": {
                    "apiKey":   _OKX_KEY,
                    "secret":   _OKX_SECRET,
                    "password": _OKX_PASS,
                    "options":  {"defaultType": "spot"},
                    **({"headers": {"x-simulated-trading": "1"}} if _OKX_DEMO else {}),
                },
            },
            "binance": {
                "cls":     ccxt.binance,
                "enabled": bool(_BINANCE_KEY and "your_key" not in _BINANCE_KEY),
                "config": {
                    "apiKey": _BINANCE_KEY,
                    "secret": _BINANCE_SECRET,
                    "options": {"defaultType": "spot"},
                },
            },
            "bybit": {
                "cls":     ccxt.bybit,
                "enabled": bool(_BYBIT_KEY and "your_key" not in _BYBIT_KEY),
                "config": {
                    "apiKey": _BYBIT_KEY,
                    "secret": _BYBIT_SECRET,
                    "options": {"defaultType": "spot"},
                },
            },
        }

        for name, cfg in exchange_cfgs.items():
            if not cfg["enabled"]:
                log.info("Arbitrator: skipping '%s' (disabled or no keys).", name)
                continue
            try:
                ex = cfg["cls"](cfg["config"])
                ex.timeout = int(ARB_FETCH_TIMEOUT * 1000)
                self._exs[name] = ex
                log.info("Arbitrator: initialised exchange '%s'.", name)
            except Exception as exc:
                log.warning("Arbitrator: could not init '%s': %s", name, exc)

        for sym in self.symbols:
            self._prices[sym] = {}

        self._running = True
        log.info(
            "Arbitrator started. symbols=%s exchanges=%s",
            self.symbols, list(self._exs.keys()),
        )

    async def close(self) -> None:
        self._running = False
        for ex in self._exs.values():
            try:
                await ex.close()
            except Exception:
                pass
        log.info("Arbitrator closed.")

    @staticmethod
    def _ccxt_sym(symbol: str) -> str:
        return f"{symbol}/USDT"

    async def _fetch_one(self, ex_name: str, symbol: str) -> Optional[ExchangePrice]:
        ex = self._exs.get(ex_name)
        if ex is None:
            return None
        try:
            ticker = await asyncio.wait_for(
                ex.fetch_ticker(self._ccxt_sym(symbol)),
                timeout=ARB_FETCH_TIMEOUT,
            )
            bid = float(ticker.get("bid") or 0)
            ask = float(ticker.get("ask") or 0)
            if bid <= 0 or ask <= 0:
                return None
            return ExchangePrice(exchange=ex_name, bid=bid, ask=ask)
        except asyncio.TimeoutError:
            self._warn_once(ex_name, f"timeout after {ARB_FETCH_TIMEOUT}s for {symbol}")
        except Exception as exc:
            err = str(exc)
            if any(kw in err.lower() for kw in
                   ("connection", "timeout", "ssl", "unreachable",
                    "forbidden", "451", "access denied", "unavailable")):
                self._warn_once(ex_name, f"connectivity error {symbol}: {err[:120]}")
            else:
                self._warn_once(ex_name, f"{symbol}: {err[:200]}")
        return None

    def _warn_once(self, ex_name: str, msg: str, cooldown: float = 60.0) -> None:
        now  = time.time()
        last = self._ex_last_warn.get(ex_name, 0.0)
        if now - last >= cooldown:
            log.warning("Arbitrator [%s]: %s", ex_name, msg)
            self._ex_last_warn[ex_name] = now

    async def _poll_once(self) -> None:
        if not self._exs:
            return
        tasks: Dict[Tuple[str, str], asyncio.Task] = {}
        for sym in self.symbols:
            for ex_name in self._exs:
                key = (sym, ex_name)
                tasks[key] = asyncio.create_task(
                    self._fetch_one(ex_name, sym),
                    name=f"arb_{ex_name}_{sym}",
                )
        keys    = list(tasks.keys())
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        async with self._lock:
            for (sym, ex_name), result in zip(keys, results):
                if isinstance(result, ExchangePrice):
                    self._prices[sym][ex_name] = result

    async def poll_loop(self) -> None:
        log.info(
            "Arbitrator poll loop started (interval=%.1fs).", ARB_POLL_INTERVAL
        )
        while self._running:
            try:
                await self._poll_once()
            except Exception as exc:
                log.error("Arbitrator poll_loop error: %s", exc)
            await asyncio.sleep(ARB_POLL_INTERVAL)

    async def get_arbitrage_opportunity(
        self, symbol: str
    ) -> Optional[ArbOpportunity]:
        sym = symbol.upper()
        now = time.time()

        async with self._lock:
            raw = dict(self._prices.get(sym, {}))

        fresh = {
            ex: p for ex, p in raw.items()
            if now - p.ts <= ARB_MAX_PRICE_AGE and p.bid > 0 and p.ask > 0
        }
        if len(fresh) < 2:
            return None

        buy_ex,  buy_obj  = min(fresh.items(), key=lambda kv: kv[1].ask)
        sell_ex, sell_obj = max(fresh.items(), key=lambda kv: kv[1].bid)

        if buy_ex == sell_ex:
            return None

        buy_px     = buy_obj.ask
        sell_px    = sell_obj.bid
        spread_pct = (sell_px - buy_px) / buy_px * 100 if buy_px > 0 else 0.0

        if spread_pct < ARB_SPREAD_MIN_PCT:
            return None

        opp = ArbOpportunity(
            symbol        = sym,
            buy_exchange  = buy_ex,
            sell_exchange = sell_ex,
            buy_price     = buy_px,
            sell_price    = sell_px,
            spread_pct    = round(spread_pct, 5),
            prices        = fresh,
        )
        log.info(
            "ArbOpportunity %s: buy@%s=%.6f sell@%s=%.6f spread=%.4f%%",
            sym, buy_ex, buy_px, sell_ex, sell_px, spread_pct,
        )
        return opp

    async def get_all_prices(self, symbol: str) -> Dict[str, ExchangePrice]:
        async with self._lock:
            return dict(self._prices.get(symbol.upper(), {}))

    def reachable_exchanges(self) -> List[str]:
        return list(self._exs.keys())
