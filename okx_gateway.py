"""
okx_gateway.py  —  OKX V5 authentication, REST client, WebSocket base.

Responsibilities:
  - HMAC-SHA256 request signing (REST + WS login frame)
  - Demo-mode header/endpoint injection
  - Reconnecting WebSocket with exponential backoff + resubscription
  - Credential validation at startup

FIXES:
  [FIX-1] _auth_headers() now explicitly always injects demo header so
          every authenticated REST call carries x-simulated-trading:1 in demo.
  [FIX-2] OKXRestClient.get() passes full query string in signature path
          when params are present — OKX requires signing the full path+query.
  [FIX-3] Added _build_query_string() helper to canonicalise GET params.
  [FIX-4] Added structured debug logging for outgoing headers (secrets redacted).
  [FIX-5] Retry logic (3 attempts, exponential back-off) added to REST get/post.
  [FIX-6] 401 handler now clearly distinguishes live-vs-demo mismatch.
  [FIX-7] WS login ack waits up to 10 s with timeout instead of blocking forever.
  [FIX-8] _utc_iso() uses integer milliseconds to avoid floating-point drift.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac as _hmac_mod
import json
import logging
import os
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

log = logging.getLogger("okx_gateway")

# ── Credentials ────────────────────────────────────────────────────────────────
OKX_API_KEY    = os.environ.get("OKX_API_KEY",    "").strip()
OKX_SECRET     = os.environ.get("OKX_SECRET",     "").strip()
OKX_PASSPHRASE = os.environ.get("OKX_PASSPHRASE", "").strip()
OKX_DEMO_MODE  = os.environ.get("OKX_DEMO_MODE",  "0").strip() == "1"

# ── Endpoints ──────────────────────────────────────────────────────────────────
# [NOTE] OKX demo REST still uses the same base URL as live.
#        The only difference is the x-simulated-trading: 1 header.
OKX_REST_BASE   = "https://www.okx.com"

OKX_WS_PUBLIC   = "wss://ws.okx.com:8443/ws/v5/public"
OKX_WS_PRIVATE  = "wss://ws.okx.com:8443/ws/v5/private"
OKX_WS_BIZ      = "wss://ws.okx.com:8443/ws/v5/business"

OKX_DEMO_WS_PUB  = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
OKX_DEMO_WS_PRIV = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
OKX_DEMO_WS_BIZ  = "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999"

_WS_BACKOFF_BASE  = 1.0
_WS_BACKOFF_MAX   = 60.0
_WS_PING_EVERY    = 20
_REST_MAX_RETRIES = 3
_REST_RETRY_SLEEP = 1.5   # seconds; doubled each attempt


# ── Credential validation ──────────────────────────────────────────────────────
def validate_credentials() -> bool:
    ok = True
    for name, val in [
        ("OKX_API_KEY",    OKX_API_KEY),
        ("OKX_SECRET",     OKX_SECRET),
        ("OKX_PASSPHRASE", OKX_PASSPHRASE),
    ]:
        if not val:
            log.error("Missing credential: %s is not set in .env", name)
            ok = False
    if ok:
        mode = "DEMO/paper-trading" if OKX_DEMO_MODE else "LIVE"
        log.info("OKX credentials loaded. Mode: %s", mode)
        # [FIX-4] Log key prefix only — never log secret or passphrase
        log.debug("API key prefix: %s...", OKX_API_KEY[:8] if len(OKX_API_KEY) >= 8 else "???")
    return ok


# ── Signing helpers ────────────────────────────────────────────────────────────
def _utc_iso() -> str:
    """
    [FIX-8] Use integer milliseconds truncated to 3 decimal places.
    OKX spec: ISO-8601 with exactly 3 ms digits, e.g. 2024-01-01T12:00:00.123Z
    Floating-point strftime can produce 6 digits on some platforms.
    """
    now_ms = int(time.time() * 1000)
    secs   = now_ms // 1000
    ms     = now_ms % 1000
    dt     = datetime.utcfromtimestamp(secs)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"


def _sign(secret: str, ts: str, method: str, path: str, body: str = "") -> str:
    """
    OKX V5 signing: base64( HMAC-SHA256( ts + METHOD + path_with_query + body ) )
    path must include query string for GET requests, e.g. /api/v5/foo?bar=1
    """
    prehash = f"{ts}{method.upper()}{path}{body}"
    raw_sig = _hmac_mod.new(
        secret.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(raw_sig).decode("utf-8")


def _build_query_string(params: Optional[Dict[str, str]]) -> str:
    """
    [FIX-3] Canonicalise GET query params into a query string for signing.
    urllib.parse.urlencode preserves insertion order (Python 3.7+).
    """
    if not params:
        return ""
    return "?" + urllib.parse.urlencode(params)


def _demo_headers() -> Dict[str, str]:
    # [FIX-1] Always return the demo header dict when in demo mode
    return {"x-simulated-trading": "1"} if OKX_DEMO_MODE else {}


def _auth_headers(method: str, path: str, body: str = "") -> Dict[str, str]:
    """
    Build the four OKX authentication headers for a REST request.
    path must already include query string (for GET signing).
    [FIX-1] Demo header is unconditionally merged here.
    """
    ts  = _utc_iso()
    sig = _sign(OKX_SECRET, ts, method, path, body)
    h: Dict[str, str] = {
        "OK-ACCESS-KEY":        OKX_API_KEY,
        "OK-ACCESS-SIGN":       sig,
        "OK-ACCESS-TIMESTAMP":  ts,
        "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE,
        "Content-Type":         "application/json",
    }
    # [FIX-1] Merge demo header AFTER building auth headers (never overrides auth)
    h.update(_demo_headers())

    # [FIX-4] Debug log: show all header keys + safe values (no secrets)
    if log.isEnabledFor(logging.DEBUG):
        safe = {k: (v[:4] + "***" if k in ("OK-ACCESS-SIGN", "OK-ACCESS-PASSPHRASE",
                                             "OK-ACCESS-KEY") else v)
                for k, v in h.items()}
        log.debug("REST headers for %s %s: %s", method, path, safe)

    return h


def build_ws_login_frame() -> str:
    """Build the JSON login frame for authenticating a private WebSocket."""
    ts  = str(int(time.time()))
    sig = _sign(OKX_SECRET, ts, "GET", "/users/self/verify")
    return json.dumps({
        "op": "login",
        "args": [{
            "apiKey":     OKX_API_KEY,
            "passphrase": OKX_PASSPHRASE,
            "timestamp":  ts,
            "sign":       sig,
        }],
    })


# ── REST Client ────────────────────────────────────────────────────────────────
class OKXRestClient:
    """
    Async OKX V5 REST client.
    [FIX-2] GET params are included in the signed path.
    [FIX-5] Retry logic with exponential back-off on transient errors.
    [FIX-6] 401 error produces actionable guidance distinguishing demo vs live.
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=15, connect=5)
            self._session = aiohttp.ClientSession(
                base_url=OKX_REST_BASE, timeout=timeout
            )
        return self._session

    def _log_401(self, path: str, method: str = "GET"):
        """[FIX-6] Actionable 401 guidance."""
        log.error(
            "OKX REST 401 Unauthorized — %s %s\n"
            "  Checklist:\n"
            "  1. OKX_API_KEY / OKX_SECRET / OKX_PASSPHRASE correct in .env?\n"
            "  2. Passphrase contains special chars ($, !, etc.)? "
            "Wrap value in double quotes in .env.\n"
            "  3. OKX_DEMO_MODE=%s — API key must be a DEMO key for demo mode.\n"
            "     Demo keys are created at: OKX → Demo Trading → API Management\n"
            "  4. System clock skew? OKX rejects requests >30s off UTC.\n"
            "     Current UTC: %s",
            method, path, OKX_DEMO_MODE, _utc_iso(),
        )

    async def get(self, path: str,
                  params: Optional[Dict[str, str]] = None,
                  auth: bool = True) -> dict:
        """
        [FIX-2] Query string is appended to path BEFORE signing.
        [FIX-5] Up to _REST_MAX_RETRIES attempts on network errors.
        """
        qs       = _build_query_string(params)
        sign_path = path + qs   # full path including query for HMAC
        h        = _auth_headers("GET", sign_path) if auth else {}

        last_exc: Optional[Exception] = None
        for attempt in range(_REST_MAX_RETRIES):
            try:
                async with self._get_session().get(
                    path, headers=h, params=params
                ) as r:
                    if r.status == 401:
                        self._log_401(path, "GET")
                        r.raise_for_status()
                    if r.status >= 500:
                        body = await r.text()
                        log.warning("OKX REST %d on GET %s (attempt %d): %s",
                                    r.status, path, attempt + 1, body[:200])
                        last_exc = aiohttp.ClientResponseError(
                            r.request_info, r.history, status=r.status)
                        await asyncio.sleep(_REST_RETRY_SLEEP * (2 ** attempt))
                        continue
                    r.raise_for_status()
                    return await r.json()
            except aiohttp.ClientResponseError:
                raise   # 4xx — don't retry, re-raise immediately
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                last_exc = e
                log.warning("OKX REST GET %s transient error (attempt %d): %s",
                            path, attempt + 1, e)
                await asyncio.sleep(_REST_RETRY_SLEEP * (2 ** attempt))

        raise last_exc or RuntimeError(f"GET {path} failed after {_REST_MAX_RETRIES} attempts")

    async def post(self, path: str, body: dict) -> dict:
        """[FIX-5] Retry on transient network errors."""
        b  = json.dumps(body)
        h  = _auth_headers("POST", path, b)

        last_exc: Optional[Exception] = None
        for attempt in range(_REST_MAX_RETRIES):
            try:
                async with self._get_session().post(path, headers=h, data=b) as r:
                    if r.status == 401:
                        self._log_401(path, "POST")
                        r.raise_for_status()
                    if r.status >= 500:
                        log.warning("OKX REST %d on POST %s (attempt %d)",
                                    r.status, path, attempt + 1)
                        last_exc = aiohttp.ClientResponseError(
                            r.request_info, r.history, status=r.status)
                        await asyncio.sleep(_REST_RETRY_SLEEP * (2 ** attempt))
                        continue
                    return await r.json()
            except aiohttp.ClientResponseError:
                raise
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                last_exc = e
                log.warning("OKX REST POST %s transient error (attempt %d): %s",
                            path, attempt + 1, e)
                await asyncio.sleep(_REST_RETRY_SLEEP * (2 ** attempt))

        raise last_exc or RuntimeError(f"POST {path} failed after {_REST_MAX_RETRIES} attempts")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


# ── WebSocket Base ─────────────────────────────────────────────────────────────
class OKXWebSocket:
    """
    Reconnecting OKX WebSocket connection.

    [FIX-7] _authenticate() uses asyncio.wait_for() with a 10-second timeout
            so a missing login ack doesn't block the coroutine forever.
    [FIX-4] Incoming WS errors are logged with full detail.
    """

    def __init__(self, url: str, on_message: Callable, private: bool = False):
        self.url        = url
        self.on_message = on_message
        self.private    = private

        self._subs: List[dict] = []
        self._sub_keys: set    = set()
        self._running          = False
        self._ws               = None
        self._attempt          = 0

    def subscribe(self, channels: List[dict]):
        for ch in channels:
            key = json.dumps(ch, sort_keys=True)
            if key not in self._sub_keys:
                self._sub_keys.add(key)
                self._subs.append(ch)

    def stop(self):
        self._running = False

    def _connect_kwargs(self) -> dict:
        extra = _demo_headers()
        try:
            import websockets.version as _wv
            major      = int(_wv.version.split(".")[0])
            header_key = "additional_headers" if major >= 11 else "extra_headers"
        except Exception:
            header_key = "additional_headers"
        return {header_key: extra, "ping_interval": None}

    def _backoff(self) -> float:
        delay = min(_WS_BACKOFF_BASE * (2 ** self._attempt), _WS_BACKOFF_MAX)
        self._attempt += 1
        return delay

    async def _authenticate(self):
        """
        [FIX-7] Wrap login ack wait in asyncio.wait_for(10s) to prevent hang.
        """
        await self._ws.send(build_ws_login_frame())

        async def _wait_login():
            async for raw in self._ws:
                msg   = json.loads(raw)
                event = msg.get("event")
                if event == "login":
                    if msg.get("code") == "0":
                        log.info("OKX WS auth OK: %s", self.url)
                        return
                    raise RuntimeError(
                        f"OKX WS login failed (code={msg.get('code')}): "
                        f"{msg.get('msg')}. "
                        "Ensure OKX_API_KEY/SECRET/PASSPHRASE are correct "
                        "and the key type matches demo/live mode."
                    )
                await self.on_message(msg)

        try:
            await asyncio.wait_for(_wait_login(), timeout=10.0)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"OKX WS login ack timed out (10s) on {self.url}. "
                "Check credentials and network."
            )

    async def _subscribe_all(self):
        if not self._subs:
            return
        frame = json.dumps({"op": "subscribe", "args": self._subs})
        await self._ws.send(frame)

    async def _ping_loop(self):
        while self._running and self._ws:
            await asyncio.sleep(_WS_PING_EVERY)
            try:
                if self._ws:
                    await self._ws.send("ping")
            except Exception:
                break

    async def run(self):
        self._running = True
        while self._running:
            try:
                async with websockets.connect(self.url,
                                              **self._connect_kwargs()) as ws:
                    self._ws      = ws
                    self._attempt = 0
                    log.info("OKX WS connected: %s (private=%s demo=%s)",
                             self.url, self.private, OKX_DEMO_MODE)

                    if self.private:
                        await self._authenticate()

                    await self._subscribe_all()

                    ping_task = asyncio.create_task(self._ping_loop())
                    try:
                        async for raw in ws:
                            if raw == "pong":
                                continue
                            try:
                                msg = json.loads(raw)
                                # [FIX-4] Log WS-level errors clearly
                                if msg.get("event") == "error":
                                    log.error(
                                        "OKX WS server error code=%s msg=%s url=%s",
                                        msg.get("code"), msg.get("msg"), self.url
                                    )
                                await self.on_message(msg)
                            except json.JSONDecodeError:
                                pass
                            except Exception as e:
                                log.warning("OKX WS message handler error: %s", e)
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except (ConnectionClosedOK, ConnectionClosedError) as e:
                log.warning("OKX WS closed: %s — %s", self.url, e)
            except RuntimeError as e:
                # Auth failures etc — log and still reconnect with backoff
                log.error("OKX WS runtime error: %s — %s", self.url, e)
            except Exception as e:
                log.error("OKX WS unexpected error: %s — %s", self.url, e)
            finally:
                self._ws = None

            if not self._running:
                break

            delay = self._backoff()
            log.info("OKX WS reconnecting in %.1fs (attempt %d): %s",
                     delay, self._attempt, self.url)
            await asyncio.sleep(delay)