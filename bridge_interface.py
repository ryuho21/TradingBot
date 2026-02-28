"""
bridge_interface.py — Phase 40: Python ↔ Rust Bridge IPC Client

Replaces the direct okx_gateway.py WebSocket and REST calls with a
lightweight TCP IPC client that communicates with the compiled
``okx_bridge`` Rust binary over a JSON-newline protocol.

Architecture
────────────
  main.py / executor.py
       │
       │  (asyncio TCP 127.0.0.1:7878)
       ▼
  bridge_interface.BridgeClient   ←──── this file
       │
       │  JSON-newline messages
       ▼
  okx_bridge (Rust binary)
       │
       ├── OKX Private WebSocket (account, orders, fills)
       └── OKX REST  (standard + express-lane pools)

Wire protocol
─────────────
  Client → Bridge : newline-terminated JSON objects
      { "method": "place_order",  "id": "<uuid>",  "params": {...} }
      { "method": "cancel_order", "id": "<uuid>",  "instId": "...", "ordId": "..." }
      { "method": "get_balance",  "id": "<uuid>",  "ccy": "USDT" }
      { "method": "shutdown" }

  Bridge → Client : newline-terminated JSON objects
      { "event": "account_update",   "eq": 3043.5, ... }
      { "event": "order_fill",       "ordId": "...", ... }
      { "event": "order_ack",        "request_id": "...", "ordId": "...", "latency_us": 312 }
      { "event": "order_reject",     "request_id": "...", "reason": "..." }
      { "event": "ghost_healed",     "raw_ws_eq": 0.0, "healed_eq": 3043.5, ... }
      { "event": "equity_breach",    "confirmed_eq": 0.0, "message": "..." }
      { "event": "balance_response", "request_id": "...", "eq": 3043.5, ... }
      { "event": "bridge_ready",     "version": "40.0.0", "demo_mode": true }
      { "event": "bridge_error",     "code": "...", "message": "..." }

WHALE_SNIPER express lane
─────────────────────────
  Pass ``priority="WHALE_SNIPER"`` to place_order() to activate the
  pre-heated HTTP connection pool in the Rust binary, targeting <500µs
  order submission latency.

Usage
─────
  client = BridgeClient()
  await client.connect()

  # Subscribe to events
  client.on("account_update", my_account_handler)
  client.on("order_fill",     my_fill_handler)
  client.on("ghost_healed",   my_ghost_handler)
  client.on("equity_breach",  my_breach_handler)

  # Place a standard order
  ack = await client.place_order(
      inst_id="BTC-USDT-SWAP",
      td_mode="cross",
      side="buy",
      pos_side="long",    # Hedge Mode — set to None for one-way/spot
      ord_type="market",
      sz="0.01",
  )

  # WHALE_SNIPER express lane
  ack = await client.place_order(
      inst_id="BTC-USDT-SWAP",
      td_mode="cross",
      side="buy",
      pos_side="long",
      ord_type="market",
      sz="0.1",
      priority="WHALE_SNIPER",
  )
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, List, Optional

log = logging.getLogger("bridge_interface")



# ── Safe env parsing ──────────────────────────────────────────────────────────
def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key, "")
    if raw is None or raw == "":
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        log.warning("Invalid int for %s=%r; using default=%s", key, raw, default)
        return int(default)

# ── Config ─────────────────────────────────────────────────────────────────────

BRIDGE_HOST            = "127.0.0.1"
BRIDGE_PORT = _env_int("BRIDGE_PORT", 7878)
BRIDGE_BINARY_NAME     = "okx_bridge"
BRIDGE_RECONNECT_DELAY = 2.0      # seconds between reconnect attempts
BRIDGE_CONNECT_TIMEOUT = 10.0     # seconds to wait for connection
BRIDGE_REQUEST_TIMEOUT = 8.0      # seconds to wait for a request/response
BRIDGE_MAX_RECONNECTS  = 20

# ── Types ──────────────────────────────────────────────────────────────────────

EventHandler = Callable[[Dict[str, Any]], Coroutine]


# ── BridgeClient ───────────────────────────────────────────────────────────────

class BridgeClient:
    """
    Async TCP IPC client for the Phase 40 Rust Logic Execution Bridge.

    This replaces direct okx_gateway.OKXRestClient / OKXWebSocket calls.
    All OKX WebSocket connection management, Ghost-State healing, and
    order signing is handled inside the Rust binary.

    The Python brain interacts exclusively through this client.
    """

    def __init__(
        self,
        host:             str = BRIDGE_HOST,
        port:             int = BRIDGE_PORT,
        auto_start_bridge: bool = True,
    ):
        self._host              = host
        self._port              = port
        self._auto_start        = auto_start_bridge

        self._reader: Optional[asyncio.StreamReader]  = None
        self._writer: Optional[asyncio.StreamWriter]  = None
        self._running           = False
        self._connected         = False
        self._reconnect_count   = 0

        # Pending request futures keyed by request id
        self._pending: Dict[str, asyncio.Future] = {}

        # Event handler registry:  event_name → list of async callbacks
        self._handlers: Dict[str, List[EventHandler]] = defaultdict(list)

        # Shared-state lock (equity cache + pending map).  Required because
        # handlers may read cached state concurrently with the read loop.
        self._state_lock = asyncio.Lock()

        # Cached equity state — updated on every account_update / ghost_healed event
        self._equity: float            = 0.0
        self._equity_is_ghost: bool    = False
        self._ghost_count: int         = 0

        # Bridge subprocess handle (when auto_start=True)
        self._bridge_proc: Optional[subprocess.Popen] = None

        # [P40.1-TEARDOWN] Track the background connect task so close() can
        # cancel/drain it before loop shutdown.
        self._connect_task: Optional[asyncio.Task] = None

    # ── Connection management ──────────────────────────────────────────────────
    async def connect(self) -> None:
        """Connect to the Rust bridge.  Starts the binary if auto_start=True."""
        if self._auto_start:
            self._ensure_bridge_running()

        # [P40.1-TEARDOWN] Remember the task driving the reconnect loop.
        try:
            self._connect_task = asyncio.current_task()
        except Exception:
            self._connect_task = None

        log.info("[BRIDGE] Connecting to okx_bridge on %s:%d", self._host, self._port)
        self._running = True
        await self._connect_loop()


    async def _connect_loop(self) -> None:
        """Maintain a persistent connection with automatic reconnect."""
        while self._running and self._reconnect_count < BRIDGE_MAX_RECONNECTS:
            try:
                # Close any previous writer before attempting a new connection.
                if self._writer is not None:
                    try:
                        self._writer.close()
                        await self._writer.wait_closed()
                    except Exception:
                        pass
                    self._writer = None
                    self._reader = None

                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self._host, self._port),
                    timeout=BRIDGE_CONNECT_TIMEOUT,
                )
                self._reader    = reader
                self._writer    = writer
                self._connected = True
                self._reconnect_count = 0
                log.info("[BRIDGE] Connected to Rust bridge")

                # Dispatch inbound events until connection drops
                await self._read_loop()

            except asyncio.CancelledError:
                self._connected = False
                raise
            except (ConnectionRefusedError, asyncio.TimeoutError, OSError) as e:
                self._connected = False
                self._reconnect_count += 1
                log.warning(
                    "[BRIDGE] Connection failed (attempt %d/%d): %s — retrying in %.1fs",
                    self._reconnect_count, BRIDGE_MAX_RECONNECTS, e, BRIDGE_RECONNECT_DELAY,
                )
                await asyncio.sleep(BRIDGE_RECONNECT_DELAY)

        if self._reconnect_count >= BRIDGE_MAX_RECONNECTS:
            log.critical("[BRIDGE] Max reconnect attempts reached — giving up")
            self._running = False

    async def _read_loop(self) -> None:
        """Read and dispatch newline-delimited JSON events from the bridge."""
        assert self._reader is not None
        try:
            while self._running:
                line = await self._reader.readline()
                if not line:
                    log.warning("[BRIDGE] Connection closed by bridge (EOF)")
                    self._connected = False
                    break
                raw = line.decode("utf-8", errors="replace").strip()
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                    await self._dispatch(msg)
                except json.JSONDecodeError as e:
                    log.warning("[BRIDGE] Malformed JSON from bridge: %s | raw=%r", e, raw[:200])
        except asyncio.CancelledError:
            self._connected = False
            raise
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError) as e:
            log.warning("[BRIDGE] Read error: %s", e)
            self._connected = False

async def _dispatch(self, msg: Dict[str, Any]) -> None:
    """Route an inbound bridge event to registered handlers and update cache."""
    event = msg.get("event", "")

    # ── Built-in cache updates + pending-map resolution (locked) ───────────
    async with self._state_lock:
        try:
            if event == "account_update":
                try:
                    self._equity = float(msg.get("eq", self._equity))
                except Exception as exc:
                    log.warning("[BRIDGE] account_update eq parse error: %s", exc)
                try:
                    self._equity_is_ghost = bool(msg.get("is_ghost", False))
                except Exception:
                    self._equity_is_ghost = False
                try:
                    self._ghost_count = int(msg.get("ghost_count", 0))
                except Exception:
                    self._ghost_count = 0

            elif event == "ghost_healed":
                try:
                    raw = float(msg.get("raw_ws_eq", 0.0))
                except Exception:
                    raw = 0.0
                try:
                    healed = float(msg.get("healed_eq", 0.0))
                except Exception:
                    healed = 0.0
                source = msg.get("source", "unknown")
                log.warning(
                    "[GHOST-HEALED] raw_ws_eq=%.4f healed_eq=%.4f source=%s ghost_count=%d",
                    raw, healed, source, msg.get("ghost_count", 0),
                )
                self._equity = healed
                self._equity_is_ghost = False
                self._ghost_count = 0

            elif event == "equity_breach":
                log.critical(
                    "[EQUITY-BREACH] Bridge confirmed $0.00 equity: %s",
                    msg.get("message", ""),
                )

            elif event == "bridge_ready":
                log.info(
                    "[BRIDGE] Bridge ready — version=%s demo_mode=%s ws_url=%s",
                    msg.get("version"), msg.get("demo_mode"), msg.get("ws_url"),
                )

            elif event == "bridge_error":
                log.error(
                    "[BRIDGE-ERROR] code=%s message=%s",
                    msg.get("code"), msg.get("message"),
                )
        except Exception as exc:
            log.warning("[BRIDGE] _dispatch cache update error: %s", exc)

        # ── Resolve pending request/response futures ──────────────────────
        req_id = msg.get("request_id")
        if req_id and req_id in self._pending:
            fut = self._pending.pop(req_id)
            if not fut.done():
                if event in ("order_reject", "bridge_error"):
                    fut.set_exception(
                        BridgeOrderError(msg.get("reason") or msg.get("message") or "unknown")
                    )
                else:
                    fut.set_result(msg)

    # ── Registered handlers (outside lock) ────────────────────────────────
    for handler in self._handlers.get(event, []):
        try:
            if asyncio.iscoroutinefunction(handler):
                await handler(msg)
            else:
                handler(msg)
        except Exception as e:
            log.exception("[BRIDGE] Handler error for event '%s': %s", event, e)

    # ── Event subscription ─────────────────────────────────────────────────────

    def on(self, event: str, handler: EventHandler) -> None:
        """Register an async (or sync) callback for a bridge event type."""
        self._handlers[event].append(handler)
        log.debug("[BRIDGE] Registered handler for event '%s'", event)

    def off(self, event: str, handler: EventHandler) -> None:
        """Deregister a previously registered event handler."""
        self._handlers[event] = [h for h in self._handlers[event] if h is not handler]

    # ── Send helpers ───────────────────────────────────────────────────────────

    async def _send(self, msg: Dict[str, Any]) -> None:
        """Send a JSON message to the bridge, ensuring a trailing newline."""
        if not self._connected or self._writer is None:
            raise BridgeNotConnectedError("Bridge is not connected")
        payload = json.dumps(msg, separators=(",", ":")) + "\n"
        self._writer.write(payload.encode("utf-8"))
        await self._writer.drain()

    async def _request(self, msg: Dict[str, Any], timeout: float = BRIDGE_REQUEST_TIMEOUT) -> Dict[str, Any]:
        """
        Send a request and await its paired response future.
        The message must carry a unique "id" field.
        """
        req_id = msg.get("id", str(uuid.uuid4()))
        msg["id"] = req_id
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut
        try:
            await self._send(msg)
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            raise BridgeTimeoutError(f"Bridge request timed out after {timeout}s: {msg.get('method')}")
        except Exception:
            self._pending.pop(req_id, None)
            raise

    # ── Public API ─────────────────────────────────────────────────────────────

    async def place_order(
        self,
        inst_id:     str,
        td_mode:     str,
        side:        str,
        ord_type:    str,
        sz:          str,
        pos_side:    Optional[str]  = None,   # "long"/"short"/"net" — Hedge Mode
        px:          Optional[str]  = None,   # limit price
        reduce_only: Optional[bool] = None,
        cl_ord_id:   Optional[str]  = None,
        priority:    Optional[str]  = None,   # "WHALE_SNIPER" → express lane
    ) -> Dict[str, Any]:
        """
        Submit an order through the Rust bridge.

        Parameters
        ──────────
        inst_id     OKX instrument ID, e.g. "BTC-USDT-SWAP"
        td_mode     "cross" | "isolated" | "cash"  (margin mode)
        side        "buy" | "sell"
        ord_type    "market" | "limit" | "post_only" | "ioc" | "fok"
        sz          Order size as string, e.g. "0.01"
        pos_side    "long" | "short" | "net"  (Hedge Mode; None → "net")
        px          Limit price as string (required for limit/post_only)
        reduce_only True to close-only in one-way mode
        cl_ord_id   Client order ID for deduplication
        priority    "WHALE_SNIPER" → bypass standard queue, use express lane

        Returns
        ───────
        The order_ack message dict from the bridge:
            { "event": "order_ack", "ord_id": "...", "latency_us": 312 }

        Raises
        ──────
        BridgeOrderError        — OKX rejected the order
        BridgeNotConnectedError — bridge is not connected
        BridgeTimeoutError      — no ack within BRIDGE_REQUEST_TIMEOUT seconds
        """
        params: Dict[str, Any] = {
            "instId":  inst_id,
            "tdMode":  td_mode,
            "side":    side,
            "ordType": ord_type,
            "sz":      sz,
        }
        if pos_side is not None:
            params["posSide"] = pos_side
        if px is not None:
            params["px"] = px
        if reduce_only is not None:
            params["reduceOnly"] = reduce_only
        if cl_ord_id is not None:
            params["clOrdId"] = cl_ord_id

        msg: Dict[str, Any] = {
            "method": "place_order",
            "id":     cl_ord_id or str(uuid.uuid4()),
            "params": params,
        }
        if priority:
            msg["priority"] = priority

        t0 = time.perf_counter()
        result = await self._request(msg)
        elapsed_us = int((time.perf_counter() - t0) * 1_000_000)

        if priority == "WHALE_SNIPER":
            log.info(
                "[EXPRESS-LANE] WHALE_SNIPER order submitted — "
                "inst_id=%s side=%s sz=%s latency_us=%d",
                inst_id, side, sz, elapsed_us,
            )
        
        # [P40.1-LAT] Ensure latency_us is always present for executor failsafes.
        # The bridge may include its own latency_us; if absent or zero, we attach
        # the measured wall-clock latency for this request.
        try:
            measured = int(elapsed_us)
        except Exception:
            measured = 0
        try:
            if int(result.get("latency_us", 0) or 0) <= 0 and measured > 0:
                result["latency_us"] = measured
        except Exception:
            if measured > 0:
                result["latency_us"] = measured
        if measured > 0:
            result["_measured_latency_us"] = measured
        return result

    async def cancel_order(
        self,
        inst_id:   str,
        ord_id:    Optional[str] = None,
        cl_ord_id: Optional[str] = None,
    ) -> None:
        """Cancel an open order by ordId or clOrdId."""
        if not ord_id and not cl_ord_id:
            raise ValueError("cancel_order requires either ord_id or cl_ord_id")
        msg: Dict[str, Any] = {
            "method":  "cancel_order",
            "id":      str(uuid.uuid4()),
            "instId":  inst_id,
        }
        if ord_id:
            msg["ordId"] = ord_id
        if cl_ord_id:
            msg["clOrdId"] = cl_ord_id
        await self._send(msg)

    async def get_balance(self, ccy: str = "USDT") -> Dict[str, Any]:
        """
        Request the current balance from the bridge.

        The bridge returns a balance_response event with fields:
            eq, cash_bal, avail_eq — all float

        This call uses the standard REST pool (not express lane) and applies
        the Ghost-State verification logic inside the Rust bridge.
        """
        return await self._request({
            "method": "get_balance",
            "id":     str(uuid.uuid4()),
            "ccy":    ccy,
        })

    async def close(self) -> None:
        """Gracefully shut down the bridge connection (and the Rust binary if launched here)."""
        self._running = False

        # [P40.1-TEARDOWN] Cancel the background connect task if this instance is
        # being closed from a different task.
        try:
            ct = self._connect_task
            cur = asyncio.current_task()
            if ct is not None and ct is not cur and not ct.done():
                ct.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(ct), timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        except Exception:
            pass

        # Fail all pending request futures so callers do not hang.
        try:
            for _rid, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(BridgeNotConnectedError("Bridge closed"))
            self._pending.clear()
        except Exception:
            pass

        try:
            await self._send({"method": "shutdown"})
        except Exception:
            pass

        if self._writer:
            try:
                self._writer.close()
                await asyncio.wait_for(asyncio.shield(self._writer.wait_closed()), timeout=2.0)
            except Exception:
                pass
            finally:
                self._writer = None
                self._reader = None
                self._connected = False

        if self._bridge_proc and self._bridge_proc.poll() is None:
            log.info("[BRIDGE] Terminating Rust bridge process PID=%d", self._bridge_proc.pid)
            try:
                self._bridge_proc.terminate()
            except Exception:
                pass


    # ── Properties (cached state) ──────────────────────────────────────────────

    @property
    def equity(self) -> float:
        """Last known good equity value (ghost-healed by the Rust bridge)."""
        return self._equity

    @property
    def equity_is_ghost(self) -> bool:
        """True if the most recent equity reading was a ghost state (WS=0 + REST unverified)."""
        return self._equity_is_ghost

    @property
    def consecutive_ghost_reads(self) -> int:
        """Number of consecutive ghost reads since last verified-good equity."""
        return self._ghost_count

    @property
    def connected(self) -> bool:
        return self._connected

    # ── Auto-start Rust binary ─────────────────────────────────────────────────

    def _ensure_bridge_running(self) -> None:
        """
        Launch the Rust bridge binary if it is not already running.

        The binary is expected to be in the same directory as this file or
        on PATH.  The BRIDGE_BINARY env var can override the path.
        """
        binary = os.environ.get("BRIDGE_BINARY", "")
        if not binary:
            script_dir = Path(__file__).parent
            # Check common build locations
            candidates = [
                script_dir / "okx_bridge",
                script_dir / "target" / "release" / "okx_bridge",
                script_dir / "target" / "release" / "okx_bridge.exe",
                Path("okx_bridge"),
            ]
            for c in candidates:
                if c.is_file():
                    binary = str(c)
                    break

        if not binary:
            log.error(
                "[BRIDGE] Rust binary 'okx_bridge' not found. "
                "Build with: cargo build --release  (in the okx_bridge/ directory). "
                "Set BRIDGE_BINARY=/path/to/okx_bridge to specify the path."
            )
            return

        log.info("[BRIDGE] Auto-launching Rust bridge: %s", binary)
        try:
            self._bridge_proc = subprocess.Popen(
                [binary],
                stdout=sys.stdout,
                stderr=sys.stderr,
                env={**os.environ},
            )
            log.info("[BRIDGE] Rust bridge PID=%d started", self._bridge_proc.pid)
            # Give the binary a moment to bind the port
            import time as _time
            _time.sleep(0.5)
        except (FileNotFoundError, PermissionError) as e:
            log.error("[BRIDGE] Failed to start Rust bridge: %s", e)


# ── Sentinel integration helper ────────────────────────────────────────────────

class BridgeSentinel:
    """
    Thin wrapper that exposes the same interface as the legacy executor
    ghost-state flags so main.py's Sentinel can be adapted without changes.

    Drop-in replacement for reading executor._equity, executor._equity_is_ghost,
    executor._consecutive_ghost_reads from the sentinel's perspective.
    """

    def __init__(self, client: BridgeClient):
        self._client = client

    @property
    def _equity(self) -> float:
        return self._client.equity

    @property
    def _equity_is_ghost(self) -> bool:
        return self._client.equity_is_ghost

    @property
    def _consecutive_ghost_reads(self) -> int:
        return self._client.consecutive_ghost_reads


# ── Exceptions ────────────────────────────────────────────────────────────────

class BridgeError(Exception):
    """Base exception for bridge IPC errors."""

class BridgeNotConnectedError(BridgeError):
    """Raised when a request is made while the bridge is disconnected."""

class BridgeTimeoutError(BridgeError):
    """Raised when a bridge request does not receive a response in time."""

class BridgeOrderError(BridgeError):
    """Raised when OKX rejects an order submitted via the bridge."""


# ── Standalone demo / smoke-test ──────────────────────────────────────────────

async def _demo() -> None:
    """
    Quick smoke-test: connect to the bridge, print the first account_update,
    then exit.  Run with:  python bridge_interface.py
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    received = asyncio.Event()

    client = BridgeClient(auto_start_bridge=True)

    async def on_account(msg: dict) -> None:
        print(f"\n[DEMO] account_update → eq={msg.get('eq'):.4f}  "
              f"is_ghost={msg.get('is_ghost')}  ghost_count={msg.get('ghost_count')}")
        received.set()

    async def on_ghost_healed(msg: dict) -> None:
        print(f"\n[DEMO] GHOST HEALED → raw={msg.get('raw_ws_eq')}  "
              f"healed={msg.get('healed_eq')}  source={msg.get('source')}")

    async def on_equity_breach(msg: dict) -> None:
        print(f"\n[DEMO] ⚠️  EQUITY BREACH → {msg.get('message')}")

    client.on("account_update",  on_account)
    client.on("ghost_healed",    on_ghost_healed)
    client.on("equity_breach",   on_equity_breach)

    connect_task = asyncio.create_task(client.connect())

    try:
        await asyncio.wait_for(received.wait(), timeout=30.0)
        print(f"\n[DEMO] Current equity: ${client.equity:.2f}  "
              f"is_ghost={client.equity_is_ghost}")
    except asyncio.TimeoutError:
        print("[DEMO] No account_update received within 30s — check bridge is running")
    finally:
        await client.close()
        connect_task.cancel()


if __name__ == "__main__":
    asyncio.run(_demo())