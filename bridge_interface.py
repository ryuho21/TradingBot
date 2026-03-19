"""
bridge_interface.py  —  Phase 50 Python ↔ Rust Bridge IPC Client
                        (Hybrid C++/Rust Bridge Core + Native Hot-Path)

TCP IPC client for the Rust Logic Execution Bridge (okx_bridge).
All OKX WebSocket management, Ghost-State healing, and order signing are
handled inside the Rust binary.  Python interacts exclusively through BridgeClient.

── Wire Protocol ──────────────────────────────────────────────────────────────
[P40.3] NEWLINE mode (default, backward-compatible):
    JSON messages terminated with \\n over 127.0.0.1:7878.

[P49]   LP mode  (P49_FRAME_MODE=lp):
    4-byte big-endian uint32 length prefix + UTF-8 JSON payload.
    Eliminates readline() buffering latency — required for P50 sub-500µs path.
    Set P49_FRAME_MODE=lp once the native binary supports LP framing.

── Events ─────────────────────────────────────────────────────────────────────
account_update, order_fill, order_ack, order_reject, ghost_healed,
equity_breach, balance_response, bridge_ready, bridge_error,
microstructure_update  [P47],
floating_update        [P48],
bridge_caps            [P49]  — capability handshake response from Rust.
execution_decision     [P50]  — Rust-computed limit_px for Python's hot-path.

[TYPE-VALIDATED-IPC] All inbound numeric fields coerced before firing handlers.

── P47 microstructure_update payload ──────────────────────────────────────────
  { "event": "microstructure_update",
    "symbol": "BTC-USDT",
    "toxicity": 0.72,           # VPIN CDF ToxicityScore ∈ [0.0, 1.0]
    "ofi": -0.35,               # Order Flow Imbalance ∈ [-1.0, +1.0]
    "ofi_bid_pull": false,
    "ofi_ask_pull": false,
    "computed_at_us": 1712345678123456
  }

── P49 bridge_caps payload (Rust → Python) ────────────────────────────────────
  { "event":         "bridge_caps",
    "accepted_caps": ["lp_framing", "intent_v2", "brain_state", "hot_path"],
    "bridge_version": "50.0"
  }

── P49 brain_state_update payload (Python → Rust) ────────────────────────────
  { "method":      "brain_state_update",
    "equity":      12345.67,
    "positions":   3,
    "symbols":     ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
    "regime_map":  {"BTC-USDT-SWAP": "bull", "ETH-USDT-SWAP": "neutral"},
    "is_congested": false,
    "cycle_ts":    1712345678.123456
  }
  Rust uses this to make autonomous decisions (P50 native brain migration).

── P50 execution_request payload (Python → Rust) ─────────────────────────────
  { "method":      "execution_request",
    "request_id":  "a3f1b2c4d5e6f7a8",
    "symbol":      "BTC-USDT-SWAP",
    "side":        "buy",
    "signal_conf": 0.82,
    "kelly_f":     0.12,
    "equity":      12500.00,
    "regime":      "bull",               # optional
    "toxicity":    0.31,                 # optional [P47]
    "obi_score":   0.55,                 # optional [P47]
    "ts_us":       1712345678123456
  }

── P50 execution_decision payload (Rust → Python) ────────────────────────────
  { "event":        "execution_decision",
    "request_id":   "a3f1b2c4d5e6f7a8",
    "symbol":       "BTC-USDT-SWAP",
    "side":         "buy",
    "limit_px":     67482.50,            # tick-rounded, OFI-adjusted
    "post_only":    true,
    "strategy_tag": "ofi_penny",         # "bbo"|"penny"|"ofi_adjusted"|"ofi_penny"
    "ofi_adj_bps":  2.3,
    "latency_us":   187,
    "ts_us":        1712345678123643
  }
  Shadow mode: Python uses its own price; logs agreement. (P50_SHADOW_MODE=1)
  Native mode: Python uses limit_px from this payload.   (P50_SHADOW_MODE=0)

── P51 twap_request payload (Python → Rust) ───────────────────────────────
  { "method":        "twap_request",
    "request_id":    "a3f1b2c4d5e6f7a8",
    "symbol":        "BTC-USDT-SWAP",
    "side":          "buy",
    "total_qty":     0.003,             # base-currency quantity
    "algo":          "twap",            # "twap" | "vwap"
    "duration_secs": 300.0,
    "slices":        10,
    "price_floor":   67000.0,           # Rust MUST NOT execute below this
    "price_ceil":    67500.0,           # Rust MUST NOT execute above this
    "equity":        12500.00,
    "regime":        "bull",            # optional
    "toxicity":      0.31,              # optional [P47]
    "ts_us":         1712345678123456
  }

── P51 iceberg_request payload (Python → Rust) ────────────────────────────
  { "method":       "iceberg_request",
    "request_id":   "a3f1b2c4d5e6f7a8",
    "symbol":       "BTC-USDT-SWAP",
    "side":         "buy",
    "total_qty":    0.10,
    "display_qty":  0.01,               # visible qty per child order
    "price_floor":  67000.0,
    "price_ceil":   67500.0,
    "ttl_secs":     120.0,
    "ts_us":        1712345678123456
  }

── P51 cancel_algo payload (Python → Rust) ────────────────────────────────
  { "method":      "cancel_algo",
    "request_id":  "a3f1b2c4d5e6f7a8",
    "reason":      "signal_reversal",   # optional human label
    "ts_us":       1712345678123456
  }

── P51 twap_slice_fill event (Rust → Python) ──────────────────────────────
  { "event":        "twap_slice_fill",
    "request_id":   "a3f1b2c4d5e6f7a8",
    "symbol":       "BTC-USDT-SWAP",
    "slice_n":      3,
    "total_slices": 10,
    "fill_qty":     0.0003,
    "fill_px":      67142.50,
    "remaining_qty":0.0021,
    "algo":         "twap",
    "ts_us":        1712345678123456
  }

── P51 algo_complete event (Rust → Python) ────────────────────────────────
  { "event":        "algo_complete",
    "request_id":   "a3f1b2c4d5e6f7a8",
    "symbol":       "BTC-USDT-SWAP",
    "algo":         "twap",
    "total_filled": 0.003,
    "avg_px":       67200.00,
    "slippage_bps": 3.2,
    "reason":       "filled",           # "filled"|"cancelled"|"aborted"|"ttl_expired"
    "ts_us":        1712345678123456
  }
  Native mode: Python awaits this event to build the synthetic fill dict.
  Shadow mode: P32/P9 engine executes; Rust result logged for comparison.

── P54 Wire Protocol — Kernel-Bypass Telemetry ────────────────────────────────
[P54] DPDK / Solarflare OpenOnload / zero-copy pipeline.
  Python is the SUPERVISOR.  DPDK/OpenOnload live entirely in the Rust binary.
  Python receives p54_stats at ~1 Hz, caches the telemetry, fires threshold
  alerts, and publishes to trader_status.json → render_p54_kernel_bypass().

── p54_stats event (Rust → Python, ~1 Hz when P54 compiled in) ────────────────
  { "event":          "p54_stats",
    "mode":           "dpdk"|"onload"|"standard"|"unknown",
                      # dpdk     = DPDK PMD ring I/O active
                      # onload   = Solarflare OpenOnload kernel-bypass active
                      # standard = normal kernel TCP socket (no bypass)
                      # unknown  = bridge compiled without P54 support
    "rx_drops":       u64,     # LIFETIME cumulative receive-side ring drops
    "tx_drops":       u64,     # LIFETIME cumulative transmit-side ring drops
    "ring_util_pct":  f32,     # current ring buffer utilisation (0.0-100.0)
    "lat_p50_us":     f32,     # packet-to-signal median latency (us)
    "lat_p95_us":     f32,     # 95th-percentile latency (us)
    "lat_p99_us":     f32,     # 99th-percentile tail latency (us)
    "zero_copy":      bool,    # true when zero-copy memcpy path is active
    "ts_us":          u64      # Rust monotonic clock (us) at emission time
  }

── p54_ptp_offset event (Rust → Python, future — P54_PTP_ENABLE=1) ────────────
  { "event":        "p54_ptp_offset",
    "offset_ns":    i64,       # PTP-measured clock offset (nanoseconds, signed)
    "stratum":      u8,        # PTP grandmaster stratum
    "ts_us":        u64        # Rust monotonic us timestamp
  }
  Python calls mesh_gate.clock_sync.ptp_apply_offset(offset_ns).
  Requires P54_PTP_ENABLE=1 in .env AND Rust recompile with PTP handler.

── Python threshold alerts (log-only, no outbound wire message) ───────────────
  TIER-1 (WARNING):  total_drops > P54_DROP_ALERT_THRESH
                     ring_util_pct > P54_RING_ALERT_PCT
                     lat_p99_us > P54_LAT_P99_ALERT_US
  Each condition fires log.warning() on every p54_stats event where it holds.

── Sentinel degradation escalation (sentinel.py _tick) ─────────────────────────
  TIER-1 (recoverable — soft-pause + Telegram WARNING):
    ring_util > P54_SENTINEL_RING_WARN_PCT  OR  drop_delta > P54_SENTINEL_DROP_WARN
    Action: executor._p54_soft_pause = True  (no restart)
  TIER-2 (critical — optional sys.exit(2)):
    ring_util > P54_SENTINEL_RING_CRITICAL_PCT for P54_SENTINEL_RING_CONSEC ticks
    OR  drop_delta > P54_SENTINEL_DROP_TRIP
    Action: Telegram CRITICAL; if P54_SENTINEL_EXIT_ON_CRITICAL=1 -> sys.exit(2)
  Fail-closed: any p54_snapshot() exception -> skip guard for this tick.

── Watchdog P54 telemetry check (bot_watchdog.py) ──────────────────────────────
  Reads trader_status.json["p54_kernel_bypass"] each monitoring cycle.
  HALT-1: total_drops delta > P54_WD_DROP_HALT_THRESH -> CRITICAL + child SIGTERM
  HALT-2: lat_p99_us > P54_WD_LAT_HALT_US for P54_WD_LAT_CONSEC consecutive reads
          -> CRITICAL + child SIGTERM (ring overrun / bridge instability)
  Always fail-open: JSON read/parse errors skip the P54 watchdog check.

── Rust compile flags for P54 support ──────────────────────────────────────────
  DPDK:     cargo build --features dpdk_bypass
  Onload:   cargo build --features solarflare_onload
  PTP:      cargo build --features ptp_clock
  Standard: cargo build  (no features — p54_stats emitted with mode=standard)
  Python observability layer works with all modes including standard sockets.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Coroutine, Dict, List, Optional

log = logging.getLogger("bridge_interface")
logging.getLogger("bridge_interface").addHandler(logging.NullHandler())
from pt_utils import _env_float, _env_int, atomic_write_json  # [P0-UTIL]
# [P0-FIX] Phase-namespaced env helpers unified under pt_utils._env_float.
# Each alias is a zero-overhead passthrough; call sites are left unchanged.
_env_float_bridge = _env_float  # was undefined — caused NameError on import
_env_float_p48    = _env_float  # local def removed — was identical to _env_float
_env_float_p51    = _env_float  # local def removed — was identical to _env_float
_p54_float        = _env_float  # local def removed — was identical to _env_float

# [P0-FIX-11] _env_int → pt_utils

# ── Config ─────────────────────────────────────────────────────────────────────

BRIDGE_HOST            = "127.0.0.1"
BRIDGE_PORT = _env_int("BRIDGE_PORT", 7878)
BRIDGE_BINARY_NAME     = "okx_bridge"
BRIDGE_RECONNECT_DELAY = 2.0      # seconds between reconnect attempts
BRIDGE_CONNECT_TIMEOUT = 10.0     # seconds to wait for connection
BRIDGE_REQUEST_TIMEOUT = 8.0      # seconds to wait for a request/response
BRIDGE_MAX_RECONNECTS  = 20

# ── [P40.5] High-Resolution Bridge Heartbeat / RTT Monitor ────────────────────
# Sends periodic ping→pong round-trips over the IPC socket to measure latency.
# RTT > BRIDGE_RTT_WARN_MS  (10 ms default) → IPC congestion WARNING + alert.
# RTT > BRIDGE_RTT_CRITICAL_MS (50 ms default) → CRITICAL log + stand-down flag.
# BRIDGE_RTT_STANDDOWN_MISSES consecutive timeouts → stand-down flag set.
BRIDGE_HEARTBEAT_INTERVAL_SECS  = _env_float_bridge("BRIDGE_HEARTBEAT_INTERVAL_SECS", 1.0)
BRIDGE_RTT_WARN_MS               = _env_float_bridge("BRIDGE_RTT_WARN_MS",            10.0)
BRIDGE_RTT_CRITICAL_MS           = _env_float_bridge("BRIDGE_RTT_CRITICAL_MS",         50.0)
BRIDGE_HEARTBEAT_TIMEOUT_SECS    = _env_float_bridge("BRIDGE_HEARTBEAT_TIMEOUT_SECS",   0.10)  # 100 ms
BRIDGE_RTT_WINDOW                = _env_int("BRIDGE_RTT_WINDOW", 60)
BRIDGE_RTT_STANDDOWN_MISSES      = _env_int("BRIDGE_RTT_STANDDOWN_MISSES", 3)

# ── [P40.5-COMPAT] Heartbeat mode
# The Rust bridge may not implement {"method":"ping"} (it will log "Unknown method — ignoring").
# To avoid false stand-down loops, default to RX-based liveness (no RPC) and only use ping when
# explicitly enabled and supported by the bridge build.
#   BRIDGE_HEARTBEAT_MODE="rx"   (default): considers bridge alive if ANY message is received recently
#   BRIDGE_HEARTBEAT_MODE="ping": uses ping→pong requests (requires Rust support)
BRIDGE_HEARTBEAT_MODE            = os.environ.get("BRIDGE_HEARTBEAT_MODE", "rx").strip().lower()
BRIDGE_HEARTBEAT_RX_STALE_SECS   = _env_float_bridge("BRIDGE_HEARTBEAT_RX_STALE_SECS", 6.0)  # [FIX] was 3.0 → 6.0: OKX pushes every 1.6–3.3s; 3s fired false timeouts
# ── [/P40.5-COMPAT] ──────────────────────────────────────────────────────────
# ── [/P40.5] ────────────────────────────────────────────────────────────────── ──────────────────────────────────────────────────────────────────

# ── [P47] Rust Microstructure Offload ─────────────────────────────────────────
# When the Rust bridge computes VPIN ToxicityScore + OFI and pushes
# "microstructure_update" events, BridgeClient caches them here.
# DataHub.get_flow_toxicity() / get_ofi_score() prefer bridge values when fresh.
P47_ENABLE              = os.environ.get("P47_ENABLE",              "1").strip() == "1"
P47_STALENESS_SECS      = _env_float_bridge("P47_STALENESS_SECS",   0.5)
P47_LOG_SOURCE          = os.environ.get("P47_LOG_SOURCE",          "0").strip() == "1"
P47_SUBSCRIBE_ON_CONNECT= os.environ.get("P47_SUBSCRIBE_ON_CONNECT","1").strip() == "1"
# ── [/P47] ────────────────────────────────────────────────────────────────────

# ── [P48] Autonomous Floating ─────────────────────────────────────────────────
# Python sends strategic price bounds to Rust; Rust executes sub-1ms tactical
# pennying within those bounds without a Python round-trip on each tick.
# Set P48_ENABLE=0 to disable and fall back to Python-side pennying.
P48_ENABLE = os.environ.get("P48_ENABLE", "1").strip().strip("'\"") == "1"

# P48_FLOOR_BPS   : default envelope width BELOW target_px for buy orders (bps).
#                   Rust may NOT float below price_floor = target * (1 - floor_bps/10000).
# P48_CEIL_BPS    : default envelope width ABOVE target_px for sell orders (bps).
# P48_SLACK_BPS   : tiny directional slack (1 tick equivalent) for opposite side.
# P48_TTL_SECS    : default intent TTL — Rust auto-expires floating after this.
# P48_LOG_UPDATES : log every floating_update event from Rust (noisy, debug only).

P48_FLOOR_BPS    = _env_float_p48("P48_FLOOR_BPS",   5.0)
P48_CEIL_BPS     = _env_float_p48("P48_CEIL_BPS",    5.0)
P48_SLACK_BPS    = _env_float_p48("P48_SLACK_BPS",   1.0)
P48_TTL_SECS     = _env_float_p48("P48_TTL_SECS",   30.0)
P48_LOG_UPDATES  = os.environ.get("P48_LOG_UPDATES", "0").strip() == "1"
# ── [/P48] ────────────────────────────────────────────────────────────────────

# ── [P49] Hybrid C++/Rust Bridge Core ─────────────────────────────────────────
#
# Three pillars:
#
#  1. LP FRAMING  (P49_FRAME_MODE="lp")
#       Replaces JSON-newline with 4-byte big-endian uint32 length-prefix +
#       UTF-8 payload.  Eliminates asyncio readline() buffering latency, which
#       adds ~40–120µs of unnecessary overhead per inbound frame on Python 3.11+.
#       Set P49_FRAME_MODE=lp once the native binary supports LP frames.
#       Default: "newline" for full v48-bridge backward compatibility.
#
#  2. CAPABILITY HANDSHAKE  (P49_CAPABILITY_HANDSHAKE=1)
#       On every connect, Python sends {"method":"bridge_hello","caps":[...]} to
#       the Rust bridge.  If the bridge is a v49+ build it replies with
#       {"event":"bridge_caps","accepted_caps":[...]}.  Python gates every P49
#       feature on the presence of the corresponding cap so a v48 binary is
#       100% compatible (hello ignored, no caps granted, system runs as P48).
#
#  3. BRAIN STATE PUBLISHER  (P49_BRAIN_STATE_FORCE=1 or cap "brain_state")
#       Python periodically publishes a compact brain snapshot to the bridge:
#       equity, position count, per-symbol regime, congestion flag, cycle_ts.
#       This is the IPC contract that enables P50's native brain — Rust can
#       read Python's strategic state and begin shadowing decisions autonomously.
#
#  INTENT V2  (P49_INTENT_V2=1, requires cap "intent_v2")
#       send_strategic_intent() attaches regime, toxicity, obi_score when the
#       bridge accepts the intent_v2 capability.  Rust uses this context to
#       apply smarter envelope widening (e.g. tighter ceiling in a bull regime).
#
# Environment variables
# ─────────────────────
# P49_ENABLE                    master switch (1=on)
# P49_FRAME_MODE                "newline" (default) | "lp"
# P49_CAPABILITY_HANDSHAKE      send bridge_hello on connect (1=on)
# P49_HANDSHAKE_TIMEOUT_SECS    max wait for bridge_caps response (default 0.5s)
# P49_BRAIN_STATE_INTERVAL_SECS min seconds between brain_state_update publishes
# P49_BRAIN_STATE_FORCE         publish even without "brain_state" cap (1=on)
# P49_INTENT_V2                 enrich set_strategic_intent when cap present (1=on)

P49_ENABLE                   = os.environ.get("P49_ENABLE",                   "1").strip().strip("'\"") == "1"
P49_FRAME_MODE               = os.environ.get("P49_FRAME_MODE",               "newline").strip().lower()
P49_CAPABILITY_HANDSHAKE     = os.environ.get("P49_CAPABILITY_HANDSHAKE",     "1").strip().strip("'\"") == "1"
P49_HANDSHAKE_TIMEOUT_SECS   = _env_float_bridge("P49_HANDSHAKE_TIMEOUT_SECS",  0.5)
P49_BRAIN_STATE_INTERVAL_SECS = _env_float_bridge("P49_BRAIN_STATE_INTERVAL_SECS", 0.5)
P49_BRAIN_STATE_FORCE        = os.environ.get("P49_BRAIN_STATE_FORCE",        "0").strip().strip("'\"") == "1"
P49_INTENT_V2                = os.environ.get("P49_INTENT_V2",                "1").strip().strip("'\"") == "1"
P49_VERSION                  = "49.0"
# Maximum single LP frame size (bytes) — 4 MB safety cap.
_P49_MAX_FRAME_BYTES         = 4 * 1024 * 1024
# ── [/P49] ────────────────────────────────────────────────────────────────────

# ── [P50] Native Hot-Path Port ────────────────────────────────────────────────
#
# Phase 50 moves compute_limit_price + OFI-adjusted order flow logic from
# Python into the Rust binary, targeting end-to-end signal-to-order latency
# of < 500 µs.
#
# Architecture
# ────────────
#  Python           → Rust:  execution_request
#     { method, symbol, side, signal_conf, kelly_f, equity, regime,
#       toxicity, obi_score, order_book_depth, ts_us }
#
#  Rust             → Python: execution_decision
#     { event, symbol, side, limit_px, post_only, strategy_tag,
#       ofi_adj_bps, latency_us, ts_us, request_id }
#
#  Python validates execution_decision (sanity bounds vs current BBO), then
#  passes limit_px directly to the order builder — bypassing all of
#  compute_limit_price() / adjusted_limit_price() / _penny_price().
#
# Shadow Mode (P50_SHADOW_MODE=1, default on first deployment)
# ────────────────────────────────────────────────────────────
#  Both Python and Rust compute independently.  Python uses its own price
#  but records Rust's decision for comparison.  Shadow agreement rate,
#  per-symbol latency ladder, and fallback rate are streamed to the dashboard.
#  Operator promotes to Native Mode (P50_SHADOW_MODE=0) once agreement ≥ 95%.
#
# Capability gate
# ───────────────
#  Enabled only when "hot_path" ∈ bridge._p49_caps AND P50_ENABLE=1.
#  Non-blocking: if Rust doesn't respond within P50_TIMEOUT_US µs, Python
#  falls back to its own price silently.  Fallback increments _p50_fallback_count.
#
# Environment variables
# ─────────────────────
# P50_ENABLE          master switch (1=on, default 1)
# P50_SHADOW_MODE     0=native (use Rust price) 1=shadow (log only, default 1)
# P50_TIMEOUT_US      µs to wait for execution_decision before fallback (default 400)
# P50_LOG_DECISIONS   log every execution_decision (noisy, default 0)
# P50_AGREE_BPS       bps tolerance to count a shadow round as "agreed" (default 2.0)
# P50_FORCE           bypass capability gate — act even if "hot_path" cap absent (0)

P50_ENABLE        = os.environ.get("P50_ENABLE",        "1").strip().strip("'\"") == "1"
P50_SHADOW_MODE   = os.environ.get("P50_SHADOW_MODE",   "1").strip().strip("'\"") == "1"
P50_TIMEOUT_US    = max(50, _env_int("P50_TIMEOUT_US", 400))
P50_LOG_DECISIONS = os.environ.get("P50_LOG_DECISIONS", "0").strip() == "1"
P50_AGREE_BPS     = _env_float("P50_AGREE_BPS", 2.0)
P50_FORCE         = os.environ.get("P50_FORCE",         "0").strip().strip("'\"") == "1"
P50_VERSION       = "50.0"
# ── [/P50] ────────────────────────────────────────────────────────────────────

# ── [P51] Institutional Execution Suite ───────────────────────────────────────
#
# Phase 51 moves native TWAP/VWAP micro-slicing and Iceberg/Hidden order logic
# into the Rust bridge.  Python remains the policy layer — it sets quantity,
# algo type, time window, and price envelope.  Rust owns ALL sub-ms scheduling,
# child-order sizing, and hidden-qty replenishment.  Python retains absolute
# kill-switch authority via cancel_algo(request_id).
#
# Architecture
# ────────────
#  Python   → Rust:  twap_request    (total_qty, algo, duration, price envelope)
#  Python   → Rust:  iceberg_request (total_qty, display_qty, price envelope)
#  Python   → Rust:  cancel_algo     (request_id, optional reason)
#
#  Rust     → Python: twap_slice_fill (per-slice fill notification)
#  Rust     → Python: algo_complete   (terminal — filled/cancelled/aborted)
#
# The Python executor awaits algo_complete with a P51_WAIT_TIMEOUT_SECS deadline.
# Timeout → cancel_algo sent → fall-through to P32/P9 Python-side engine.
#
# Shadow Mode (P51_SHADOW_MODE=1, default on first deployment)
# ────────────────────────────────────────────────────────────
#  Python fires the request fire-and-forget, then immediately falls through to
#  P32/P9 engine for actual execution.  Rust's algo_complete is cached and
#  compared against Python's result for slippage/fill-rate benchmarking.
#  Operator promotes to Native Mode (P51_SHADOW_MODE=0) once Rust algos are
#  validated in production.
#
# Capability gate
# ───────────────
#  Active only when "twap_vwap" ∈ bridge._p49_caps OR P51_FORCE=1.
#  Backward-compat: v50 and older bridges ignore twap_request/iceberg_request.
#  P32/P9 Python engines remain fully intact as fallbacks at all times.
#
# Determinism & Audit Trail
# ─────────────────────────
#  Every child order carries parent_id = request_id for full lineage tracing.
#  Slicing is deterministic and cancel-safe: duplicate request_id is idempotent.
#  All fills are streamed to Python via twap_slice_fill for real-time monitoring.
#
# Environment variables
# ─────────────────────
# P51_ENABLE                  master switch (1=on, default 1)
# P51_SHADOW_MODE             0=native 1=shadow/compare (default 1)
# P51_FORCE                   bypass cap gate (default 0)
# P51_WAIT_TIMEOUT_SECS       max wait for algo_complete (default 600 = 10 min)
# P51_TWAP_DEFAULT_DURATION   default TWAP window in seconds (default 300)
# P51_TWAP_DEFAULT_SLICES     default slice count (default 10)
# P51_ICEBERG_DEFAULT_TTL     default iceberg TTL in seconds (default 120)
# P51_LOG_FILLS               log every twap_slice_fill event (default 0)

P51_ENABLE        = os.environ.get("P51_ENABLE",        "1").strip().strip("'\"") == "1"
P51_SHADOW_MODE   = os.environ.get("P51_SHADOW_MODE",   "1").strip().strip("'\"") == "1"
P51_FORCE         = os.environ.get("P51_FORCE",         "0").strip().strip("'\"") == "1"
P51_VERSION       = "51.0"


P51_WAIT_TIMEOUT_SECS          = _env_float_p51("P51_WAIT_TIMEOUT_SECS",          600.0)
P51_TWAP_DEFAULT_DURATION_SECS = _env_float_p51("P51_TWAP_DEFAULT_DURATION_SECS", 300.0)
P51_TWAP_DEFAULT_SLICES        = max(2, _env_int("P51_TWAP_DEFAULT_SLICES", 10))
P51_ICEBERG_DEFAULT_TTL_SECS   = _env_float_p51("P51_ICEBERG_DEFAULT_TTL_SECS",   120.0)
P51_LOG_FILLS                  = os.environ.get("P51_LOG_FILLS",                  "0").strip() == "1"
# ── [/P51] ────────────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════════════════
# [P54] Kernel-Bypass / DPDK / OpenOnload — Python observability config layer
# ═══════════════════════════════════════════════════════════════════════════════
#
# Python is the SUPERVISOR layer.  DPDK/OpenOnload live entirely in the Rust
# bridge binary.  Python's job:
#   1. Receive p54_stats events from bridge and cache telemetry
#   2. Alert on drop/ring/latency thresholds
#   3. Expose p54_snapshot() to engine_supervisor → trader_status.json
#   4. Drive the dashboard render_p54_kernel_bypass() panel
#
# P54_ENABLE            1 = consume p54_stats events; 0 = ignore (passthrough)
# P54_DROP_ALERT_THRESH ring-drop count per event that triggers a WARNING log
# P54_RING_ALERT_PCT    ring utilization % that triggers a WARNING log
# P54_LAT_P99_ALERT_US  p99 latency (µs) that triggers a WARNING log
# P54_HISTORY_LEN       how many p54_stats events to retain in the rolling buffer


def _p54_int(key: str, default: int) -> int:
    """[P0-UTIL] Thin alias → pt_utils._env_int."""
    return _env_int(key, default)

P54_ENABLE          : bool  = os.environ.get("P54_ENABLE",      "1").strip() == "1"
P54_DROP_ALERT_THRESH: int  = _p54_int  ("P54_DROP_ALERT_THRESH",   10)
P54_RING_ALERT_PCT  : float = _p54_float("P54_RING_ALERT_PCT",      85.0)
# [FIX-P54] 1000µs (1ms) is only realistic for DPDK kernel-bypass.
# Standard TCP on Windows/Linux is 5–50ms — use 50000µs (50ms) as the default.
# Set P54_LAT_P99_ALERT_US=1000 in .env after DPDK activation.
P54_LAT_P99_ALERT_US: float = _p54_float("P54_LAT_P99_ALERT_US", 50_000.0)
P54_HISTORY_LEN     : int   = _p54_int  ("P54_HISTORY_LEN",         120)
P54_VERSION         : str   = "54.0"
# ── [/P54-config] ─────────────────────────────────────────────────────────────

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

        # Cached equity state — updated on every account_update / ghost_healed event
        self._equity: float            = 0.0
        self._equity_is_ghost: bool    = False
        self._ghost_count: int         = 0

        # [P40.1-STALE] Timestamp of the last valid (non-ghost, non-zero) account_update.
        # Exposed via .last_equity_ts for watchdog stale-data detection.
        self._last_equity_ts: float    = 0.0

        # Bridge subprocess handle (when auto_start=True)
        self._bridge_proc: Optional[subprocess.Popen] = None

        # [P40.1-TEARDOWN] Track the background connect task so close() can
        # cancel/drain it before loop shutdown.
        self._connect_task: Optional[asyncio.Task] = None

        # [P40.5] High-Resolution RTT / Heartbeat state
        # _rtt_history: rolling deque of (timestamp, rtt_ms) samples.
        # _last_rtt_ms: most recently measured round-trip time.
        # _rtt_consecutive_timeouts: pings that timed out without a pong.
        # _ipc_congested: True when RTT > BRIDGE_RTT_WARN_MS or enough timeouts.
        # _heartbeat_task: background asyncio.Task driving the ping loop.
        from collections import deque as _deque
        self._rtt_history: "collections.deque[tuple[float,float]]" = _deque(maxlen=BRIDGE_RTT_WINDOW)
        self._last_rtt_ms:              float                 = 0.0
        self._rtt_consecutive_timeouts: int                   = 0
        self._ipc_congested:            bool                  = False
        self._heartbeat_task:           Optional[asyncio.Task] = None

        # [P40.5] RX-liveness timestamp (monotonic seconds) updated on ANY inbound frame.
        # Used when BRIDGE_HEARTBEAT_MODE="rx" to avoid relying on unsupported ping RPCs.
        self._last_rx_monotonic: float = 0.0
        self._last_rx_wall:      float = 0.0
        # ── [/P40.5] ────────────────────────────────────────────────────────── ──────────────────────────────────────────────────────────

        # ── [P47] Rust Microstructure Cache ───────────────────────────────────
        # Keyed by normalised instrument ID (e.g. "BTC-USDT").
        # Each entry: {"toxicity": float, "ofi": float,
        #              "ofi_bid_pull": bool, "ofi_ask_pull": bool,
        #              "ts": float,         # time.time() when Python received it
        #              "computed_at_us": int}  # Unix µs from Rust
        self._p47_ms_cache: Dict[str, Dict[str, Any]] = {}
        self._p47_update_count: int   = 0   # lifetime event counter
        self._p47_enabled:      bool  = P47_ENABLE
        # ── [/P47] ────────────────────────────────────────────────────────────

        # ── [P48] Autonomous Floating State ───────────────────────────────────
        # _p48_active_intents : per-symbol last-sent strategic intent snapshot.
        #   Keyed by normalised instrument ID (e.g. "BTC-USDT-SWAP").
        #   Each entry: {symbol, side, price_floor, price_ceil, target_px,
        #                max_spread_bps, ttl_secs, sent_ts}
        # _p48_floating_cache : per-symbol latest floating_update event pushed
        #   by Rust.  Each entry: {symbol, current_px, spread_bps, tick_count, ts}
        # _p48_intent_count   : lifetime intents emitted to Rust.
        # _p48_update_count   : lifetime floating_update events received.
        self._p48_active_intents: Dict[str, Dict[str, Any]] = {}
        self._p48_floating_cache: Dict[str, Dict[str, Any]] = {}
        self._p48_intent_count:   int = 0
        self._p48_update_count:   int = 0
        self._p48_enabled:        bool = P48_ENABLE
        # ── [/P48] ────────────────────────────────────────────────────────────

        # ── [P49] Hybrid Bridge Core state ────────────────────────────────────
        # _p49_caps               : frozenset of capability strings accepted by
        #                           the bridge during the last handshake.
        #                           Empty frozenset = v48 bridge (no handshake).
        # _p49_handshake_done     : True after bridge_caps received or timeout.
        # _p49_brain_state_count  : lifetime brain_state_update messages sent.
        # _p49_brain_state_ts     : time.time() of last publish (rate limiter).
        # _p49_last_brain_state   : last published brain state dict (for snapshot).
        self._p49_enabled:            bool      = P49_ENABLE
        self._p49_caps:               frozenset = frozenset()
        self._p49_handshake_done:     bool      = False
        self._p49_brain_state_count:  int       = 0
        self._p49_brain_state_ts:     float     = 0.0
        self._p49_last_brain_state:   dict      = {}
        # ── [/P49] ────────────────────────────────────────────────────────────

        # ── [P50] Native Hot-Path Port state ──────────────────────────────────
        # _p50_enabled          : P50_ENABLE flag.
        # _p50_shadow_mode      : True = log-only; False = use Rust price.
        # _p50_pending          : {request_id → asyncio.Future[dict]} for
        #                         coroutines awaiting execution_decision.
        # _p50_request_count    : lifetime execution_request messages sent.
        # _p50_decision_count   : lifetime execution_decision messages received.
        # _p50_fallback_count   : times Python fell back due to timeout/error.
        # _p50_agree_count      : shadow rounds where Rust and Python agreed.
        # _p50_disagree_count   : shadow rounds where prices diverged > P50_AGREE_BPS.
        # _p50_latency_us_history : deque[(ts, latency_us)] for p50_snapshot().
        # _p50_last_decision    : most recent execution_decision dict (any symbol).
        from collections import deque as _dq
        self._p50_enabled:           bool  = P50_ENABLE
        self._p50_shadow_mode:       bool  = P50_SHADOW_MODE
        self._p50_pending:           Dict[str, asyncio.Future] = {}
        self._p50_request_count:     int   = 0
        self._p50_decision_count:    int   = 0
        self._p50_fallback_count:    int   = 0
        self._p50_agree_count:       int   = 0
        self._p50_disagree_count:    int   = 0
        self._p50_latency_us_history: "collections.deque[tuple[float,int]]" = _dq(maxlen=200)
        self._p50_last_decision:     dict  = {}
        # ── [/P50] ────────────────────────────────────────────────────────────P49] ────────────────────────────────────────────────────────────

        # ── [P51] Institutional Execution Suite state ──────────────────────────
        # _p51_active_algos : {request_id → {symbol, algo, side, total_qty,
        #                     fill_qty, slices_done, price_floor, price_ceil,
        #                     sent_ts, shadow_mode}}
        # _p51_pending      : {request_id → asyncio.Future[dict]}
        #                     Futures awaited by executor for algo_complete.
        # _p51_slice_history: deque of recent twap_slice_fill events (cap 200).
        # _p51_completed    : deque of recent algo_complete snapshots (cap 50).
        # _p51_shadow_results: deque of {request_id, python_avg_px, rust_avg_px,
        #                      agree, slippage_bps_diff} for shadow comparison.
        # _p51_request_count : lifetime algo requests sent.
        # _p51_fill_count    : lifetime twap_slice_fill events received.
        # _p51_complete_count: lifetime algo_complete events received.
        # _p51_fallback_count: times Python fell back (cap absent / timeout).
        # _p51_agree_count   : shadow rounds where Rust and Python results agreed.
        # _p51_disagree_count: shadow rounds with divergent results.
        from collections import deque as _dq51
        self._p51_active_algos:    Dict[str, Dict[str, Any]] = {}
        self._p51_pending:         Dict[str, asyncio.Future]  = {}
        self._p51_slice_history:   "collections.deque[dict]"  = _dq51(maxlen=200)
        self._p51_completed:       "collections.deque[dict]"   = _dq51(maxlen=50)
        self._p51_shadow_results:  "collections.deque[dict]"   = _dq51(maxlen=100)
        self._p51_request_count:   int   = 0
        self._p51_fill_count:      int   = 0
        self._p51_complete_count:  int   = 0
        self._p51_fallback_count:  int   = 0
        self._p51_agree_count:     int   = 0
        self._p51_disagree_count:  int   = 0
        # ── [/P51] ────────────────────────────────────────────────────────────

        # [P54] Kernel-bypass telemetry cache (fed by p54_stats bridge events)
        # _p54_mode       : active bypass mode reported by Rust
        # _p54_history    : deque of raw p54_stats dicts (capped at P54_HISTORY_LEN)
        # _p54_rx_drops   : lifetime receive-drop counter
        # _p54_tx_drops   : lifetime transmit-drop counter
        # _p54_ring_util  : last ring buffer utilization (0-100 %)
        # _p54_lat_*_us   : last tail-latency percentiles (µs)
        # _p54_zero_copy  : zero-copy mode active flag
        # _p54_last_ts    : wall-clock of the most recent p54_stats event
        # _p54_event_count: total p54_stats events received
        from collections import deque as _dq54
        self._p54_mode:        str   = "unknown"
        self._p54_history:     "collections.deque[dict]" = _dq54(maxlen=max(P54_HISTORY_LEN, 10))
        self._p54_rx_drops:    int   = 0
        self._p54_tx_drops:    int   = 0
        self._p54_ring_util:   float = 0.0
        self._p54_lat_warn_ts: float = 0.0  # [FIX-P54] throttle
        self._p54_lat_p50_us:  float = 0.0
        self._p54_lat_p95_us:  float = 0.0
        self._p54_lat_p99_us:  float = 0.0
        self._p54_zero_copy:   bool  = False
        self._p54_last_ts:     float = 0.0
        self._p54_event_count: int   = 0
        # ── [/P54-init] ───────────────────────────────────────────────────────

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
                    except Exception as _exc:
                        log.debug("[BRIDGE] cleanup: %s", _exc)
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

                # [P40.5] Start the heartbeat/RTT monitor for this connection.
                self._rtt_consecutive_timeouts = 0
                self._ipc_congested            = False
                self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())

                # [P48] Clear stale intent/floating caches on every new connection.
                # Rust starts fresh after (re)connect — Python must match that state.
                self._p48_active_intents.clear()
                self._p48_floating_cache.clear()
                log.debug("[P48] Intent and floating caches cleared on (re)connect.")

                # [P49] Reset capability state — each new connection starts fresh.
                # Fire the handshake as a concurrent task; the read loop below
                # dispatches the bridge_caps response and signals completion.
                self._p49_caps            = frozenset()
                self._p49_handshake_done  = False
                if P49_ENABLE and P49_CAPABILITY_HANDSHAKE:
                    asyncio.ensure_future(self._do_capability_handshake())
                    log.debug("[P49] Capability handshake task launched.")

                # [P51] Cancel all pending algo futures and clear active algo state.
                # Rust discards all in-flight TWAP/Iceberg on disconnect; Python
                # must resolve their futures with a cancellation sentinel so callers
                # do not hang indefinitely.
                _p51_pending_snap = dict(self._p51_pending)
                for _r_id, _fut in _p51_pending_snap.items():
                    if not _fut.done():
                        _fut.set_exception(
                            BridgeNotConnectedError("bridge reconnected — algo cancelled")
                        )
                self._p51_pending.clear()
                self._p51_active_algos.clear()
                log.debug("[P51] Pending algo futures cancelled on (re)connect.")

                # [P47] Subscribe to microstructure pushes for all tracked symbols.
                # Symbols are populated by main.py injecting hub.symbols after connect.
                # [P47-V48-COMPAT] With v48 Rust bridge, subscribe_microstructure is
                # not yet implemented → Rust logs "Unknown method" WARN once per connect.
                # This is expected and non-fatal; P47 data degrades to Python-computed
                # VPIN estimates (bridge_interface._ipc_congested proxy).
                # The "Unknown method" noise cannot be suppressed from Python; it only
                # fires ONCE per connection — not per cycle — so it is acceptable.
                if P47_ENABLE and P47_SUBSCRIBE_ON_CONNECT:
                    _p47_syms = getattr(self, "_p47_tracked_symbols", [])
                    if _p47_syms:
                        try:
                            await self.subscribe_microstructure(_p47_syms)
                        except Exception as _p47_sub_exc:
                            log.debug("[P47] Auto-subscribe error: %s", _p47_sub_exc)
                    else:
                        log.debug("[P47] No tracked symbols yet — subscribe deferred")

                # Dispatch inbound events until connection drops
                await self._read_loop()

                # [P40.5] Connection dropped — cancel heartbeat before reconnect.
                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(self._heartbeat_task), timeout=0.5
                        )
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass
                self._heartbeat_task = None

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
        """[P49] Frame-mode dispatcher — delegates to the appropriate read loop.

        P49_FRAME_MODE="newline" (default) → _read_loop_newline() — backward compat.
        P49_FRAME_MODE="lp"                → _read_loop_lp()      — length-prefixed.
        """
        if P49_ENABLE and P49_FRAME_MODE == "lp":
            log.debug("[P49] Using LP (length-prefixed) read loop.")
            await self._read_loop_lp()
        else:
            await self._read_loop_newline()

    async def _read_loop_newline(self) -> None:
        """Read and dispatch newline-delimited JSON events from the bridge.

        This is the original Phase 40.3 read loop, preserved intact for
        backward compatibility with all v40–v48 bridge builds.
        """
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

    async def _read_loop_lp(self) -> None:
        """[P49] 4-byte big-endian length-prefixed frame reader.

        Wire format (same as used by the Rust bridge when P49_FRAME_MODE=lp):
            [uint32 big-endian frame_len][frame_len bytes of UTF-8 JSON]

        Advantages over JSON-newline:
          • No readline() buffering — asyncio reads exact bytes in one call.
          • Zero scan for \\n delimiter — ~40–120µs latency reduction per frame.
          • Handles arbitrarily large payloads safely (cap: _P49_MAX_FRAME_BYTES).
          • Foundation for P50 binary framing (MessagePack / FlatBuffers).

        Falls back gracefully: if the bridge sends a zero-length frame or exceeds
        the safety cap, the connection is closed and the reconnect loop retries.
        """
        import struct as _struct
        assert self._reader is not None
        try:
            while self._running:
                # ── Read 4-byte length header ─────────────────────────────────
                header = await self._reader.readexactly(4)
                # Stamp liveness immediately on header receipt (P40.5-RX).
                # _dispatch() stamps again, but the header stamp fires earlier
                # which is important for the congestion ladder.
                self._last_rx_monotonic = time.monotonic()
                self._last_rx_wall      = time.time()

                frame_len = _struct.unpack(">I", header)[0]
                if frame_len == 0:
                    log.warning("[P49-LP] Zero-length frame received — skipping.")
                    continue
                if frame_len > _P49_MAX_FRAME_BYTES:
                    log.error(
                        "[P49-LP] Frame too large: %d bytes (cap=%d) — "
                        "disconnecting to prevent OOM.", frame_len, _P49_MAX_FRAME_BYTES,
                    )
                    self._connected = False
                    break

                # ── Read exact payload ────────────────────────────────────────
                payload = await self._reader.readexactly(frame_len)
                raw = payload.decode("utf-8", errors="replace").strip()
                if not raw:
                    continue
                try:
                    msg = json.loads(raw)
                    await self._dispatch(msg)
                except json.JSONDecodeError as e:
                    log.warning(
                        "[P49-LP] Malformed JSON: %s | raw=%r", e, raw[:200]
                    )

        except asyncio.CancelledError:
            self._connected = False
            raise
        except asyncio.IncompleteReadError:
            log.warning("[P49-LP] Incomplete read — bridge closed connection mid-frame.")
            self._connected = False
        except (ConnectionResetError, OSError) as e:
            log.warning("[P49-LP] Read error: %s", e)
            self._connected = False

    # ── [P40.5] High-Resolution IPC Heartbeat ─────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """
        [P40.5] Background coroutine: sends ping→pong round-trips every
        BRIDGE_HEARTBEAT_INTERVAL_SECS and tracks IPC latency at millisecond
        precision using time.perf_counter().

        Congestion ladder
        -----------------
        RTT < BRIDGE_RTT_WARN_MS (10 ms default)
            Healthy. Clears _ipc_congested flag on first recovery.

        BRIDGE_RTT_WARN_MS ≤ RTT < BRIDGE_RTT_CRITICAL_MS (50 ms default)
            WARNING logged. _ipc_congested = True.
            Fires registered "ipc_congestion" event handlers (severity="warning").

        RTT ≥ BRIDGE_RTT_CRITICAL_MS
            CRITICAL logged. _ipc_congested = True.
            Fires "ipc_congestion" handlers (severity="critical").

        Timeout (no pong within BRIDGE_HEARTBEAT_TIMEOUT_SECS = 100 ms)
            Increments _rtt_consecutive_timeouts.
            After BRIDGE_RTT_STANDDOWN_MISSES consecutive timeouts:
              _ipc_congested = True + fires "ipc_standdown" handlers.
              Sentinel can use is_ipc_congested to halt new entries.
        """
        _hb_log = logging.getLogger("bridge_interface.heartbeat")
        _hb_log.debug(
            "[P40.5] Heartbeat loop started  interval=%.2fs  warn=%gms  crit=%gms  timeout=%gms",
            BRIDGE_HEARTBEAT_INTERVAL_SECS,
            BRIDGE_RTT_WARN_MS,
            BRIDGE_RTT_CRITICAL_MS,
            BRIDGE_HEARTBEAT_TIMEOUT_SECS * 1_000,
        )
        try:
            while self._running and self._connected:
                await asyncio.sleep(BRIDGE_HEARTBEAT_INTERVAL_SECS)
                if not self._connected:
                    break

                # ── Measure health / RTT ───────────────────────────────────────
                rtt_ms: Optional[float] = None

                if BRIDGE_HEARTBEAT_MODE == "ping":
                    ping_id  = str(uuid.uuid4())
                    t0       = time.perf_counter()
                    try:
                        await self._request(
                            {"method": "ping", "id": ping_id},
                            timeout=BRIDGE_HEARTBEAT_TIMEOUT_SECS,
                        )
                        rtt_ms = (time.perf_counter() - t0) * 1_000.0
                    except BridgeTimeoutError:
                        rtt_ms = None
                    except (BridgeNotConnectedError, asyncio.CancelledError):
                        break
                    except Exception as exc:
                        _hb_log.debug("[P40.5] Ping error: %s", exc)
                        continue
                else:
                    # Default ("rx") mode: consider the bridge alive if ANY inbound
                    # frame was observed recently. This avoids false stand-down loops
                    # when the Rust bridge build does not implement {"method":"ping"}.
                    try:
                        last_rx = float(self._last_rx_monotonic or 0.0)
                        age = time.monotonic() - last_rx if last_rx > 0 else 1e9
                    except Exception:
                        age = 1e9

                    if age <= max(BRIDGE_HEARTBEAT_RX_STALE_SECS, BRIDGE_HEARTBEAT_INTERVAL_SECS + BRIDGE_HEARTBEAT_TIMEOUT_SECS):
                        # Treat as healthy. Use a small synthetic RTT for dashboards.
                        rtt_ms = 0.0
                    else:
                        rtt_ms = None

                now = time.time()

                # ── Timeout handling ──────────────────────────────────────────
                if rtt_ms is None:
                    self._rtt_consecutive_timeouts += 1
                    _hb_log.warning(
                        "[P40.5] Ping TIMEOUT (no pong in %.0f ms) | "
                        "consecutive=%d/%d",
                        BRIDGE_HEARTBEAT_TIMEOUT_SECS * 1_000,
                        self._rtt_consecutive_timeouts,
                        BRIDGE_RTT_STANDDOWN_MISSES,
                    )
                    if self._rtt_consecutive_timeouts >= BRIDGE_RTT_STANDDOWN_MISSES:
                        if not self._ipc_congested:
                            _hb_log.critical(
                                "[P40.5] IPC STAND-DOWN: %d consecutive ping timeouts "
                                "— bridge unresponsive. Halting new orders recommended.",
                                self._rtt_consecutive_timeouts,
                            )
                        self._ipc_congested = True
                        await self._fire_synthetic_handlers("ipc_standdown", {
                            "consecutive_timeouts": self._rtt_consecutive_timeouts,
                            "standdown_threshold":  BRIDGE_RTT_STANDDOWN_MISSES,
                            "timestamp":            now,
                        })
                    continue

                # ── Successful pong: record sample ────────────────────────────
                self._rtt_consecutive_timeouts = 0
                self._last_rtt_ms              = rtt_ms
                self._rtt_history.append((now, rtt_ms))

                # ── Classify and alert ────────────────────────────────────────
                if rtt_ms >= BRIDGE_RTT_CRITICAL_MS:
                    _hb_log.critical(
                        "[P40.5] IPC RTT CRITICAL  %.3f ms >= %.0f ms "
                        "— severe congestion, standdown advised.",
                        rtt_ms, BRIDGE_RTT_CRITICAL_MS,
                    )
                    self._ipc_congested = True
                    await self._fire_synthetic_handlers("ipc_congestion", {
                        "rtt_ms":    rtt_ms,
                        "severity":  "critical",
                        "threshold": BRIDGE_RTT_CRITICAL_MS,
                        "timestamp": now,
                    })

                elif rtt_ms >= BRIDGE_RTT_WARN_MS:
                    _hb_log.warning(
                        "[P40.5] IPC RTT WARNING  %.3f ms >= %.0f ms "
                        "— IPC congestion detected.",
                        rtt_ms, BRIDGE_RTT_WARN_MS,
                    )
                    self._ipc_congested = True
                    await self._fire_synthetic_handlers("ipc_congestion", {
                        "rtt_ms":    rtt_ms,
                        "severity":  "warning",
                        "threshold": BRIDGE_RTT_WARN_MS,
                        "timestamp": now,
                    })

                else:
                    # RTT healthy: clear flag with hysteresis (log only on transition)
                    if self._ipc_congested:
                        _hb_log.info(
                            "[P40.5] IPC RTT recovered  %.3f ms  (< %.0f ms warn)",
                            rtt_ms, BRIDGE_RTT_WARN_MS,
                        )
                        self._ipc_congested = False
                        await self._fire_synthetic_handlers("ipc_recovered", {
                            "rtt_ms":    rtt_ms,
                            "timestamp": now,
                        })
                    else:
                        _hb_log.debug("[P40.5] RTT ok: %.3f ms", rtt_ms)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("[P40.5] Heartbeat loop crashed: %s", exc, exc_info=True)
        finally:
            logging.getLogger("bridge_interface.heartbeat").debug(
                "[P40.5] Heartbeat loop exited."
            )

    async def _fire_synthetic_handlers(self, event: str, payload: Dict[str, Any]) -> None:
        """
        Fire registered handlers for a synthetic (Python-generated, not bridge-sent)
        event such as "ipc_congestion", "ipc_recovered", or "ipc_standdown".

        Callers subscribe with:
            client.on("ipc_congestion", my_async_handler)
        """
        for handler in self._handlers.get(event, []):
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(payload)
                else:
                    handler(payload)
            except Exception as exc:
                log.exception(
                    "[BRIDGE] Handler error for synthetic event '%s': %s", event, exc
                )

    async def ping(self) -> float:
        """
        [P40.5] Send a single manual ping and return the measured RTT in milliseconds.

        Uses BRIDGE_HEARTBEAT_TIMEOUT_SECS (default 100 ms) as the timeout.
        Raises BridgeTimeoutError if no pong is received in time.

        Typical use — spot-check IPC health::

            try:
                rtt = await client.ping()
                if rtt > 10.0:
                    log.warning("IPC congestion: %.1f ms", rtt)
            except BridgeTimeoutError:
                log.critical("Bridge unresponsive!")
        """
        t0 = time.perf_counter()
        await self._request(
            {"method": "ping", "id": str(uuid.uuid4())},
            timeout=BRIDGE_HEARTBEAT_TIMEOUT_SECS,
        )
        return (time.perf_counter() - t0) * 1_000.0

    # ── [P40.5] RTT Properties ─────────────────────────────────────────────────

    @property
    def last_rtt_ms(self) -> float:
        """[P40.5] Most recently measured IPC round-trip time in milliseconds."""
        return self._last_rtt_ms

    @property
    def is_ipc_congested(self) -> bool:
        """
        [P40.5] True when the heartbeat loop has detected IPC congestion
        (RTT > BRIDGE_RTT_WARN_MS) or consecutive ping timeouts have exceeded
        BRIDGE_RTT_STANDDOWN_MISSES.  Cleared automatically on recovery.
        """
        return self._ipc_congested

    @property
    def rtt_history_ms(self) -> List[tuple]:
        """
        [P40.5] Rolling list of (timestamp, rtt_ms) tuples — last BRIDGE_RTT_WINDOW
        samples.  Use for dashboard sparklines or P99 calculations.
        """
        return list(self._rtt_history)

    @property
    def rtt_p99_ms(self) -> float:
        """
        [P40.5] 99th-percentile RTT over the rolling window.  Returns 0.0 if
        fewer than 2 samples are available.
        """
        samples = [rtt for _, rtt in self._rtt_history]
        if len(samples) < 2:
            return 0.0
        samples_sorted = sorted(samples)
        idx = max(0, int(len(samples_sorted) * 0.99) - 1)
        return samples_sorted[idx]

    # ── [/P40.5] ──────────────────────────────────────────────────────────────

    async def _dispatch(self, msg: Dict[str, Any]) -> None:
        """
        Route an inbound bridge event to registered handlers and update cache.

        [P40.1-TYPE] All numeric fields are validated before updating cached
        state. Malformed values (NaN, Inf, negative equity) are rejected with
        a WARNING log. Handlers receive the original msg dict but cached state
        is only updated after passing validation.
        """
        import math as _math

        # [P40.5-RX] Stamp liveness timestamp on EVERY inbound frame.
        # This is the heartbeat proof-of-life used when BRIDGE_HEARTBEAT_MODE="rx"
        # (default). Without this stamp the age check in _heartbeat_loop() sees
        # age = time.monotonic() - 0.0 = ~boot_uptime and always fires stand-down.
        self._last_rx_monotonic = time.monotonic()
        self._last_rx_wall      = time.time()

        event = msg.get("event", "")

        # ── Built-in cache updates (type-validated) ───────────────────────────
        if event == "account_update":
            # [P40.1-TYPE] Validate eq: must be a finite positive float.
            raw_eq = msg.get("eq", None)
            try:
                eq_val = float(raw_eq)
                if not _math.isfinite(eq_val):
                    raise ValueError(f"non-finite eq={eq_val!r}")
                if eq_val < 0:
                    raise ValueError(f"negative eq={eq_val!r}")
            except (TypeError, ValueError) as _tv:
                log.warning(
                    "[BRIDGE][P40.1-TYPE] account_update: eq field rejected — %s "
                    "(raw=%r). Cached equity unchanged.",
                    _tv, raw_eq,
                )
                eq_val = None  # skip equity update

            # [P40.1-TYPE] Validate is_ghost: coerce to bool safely.
            try:
                is_ghost_val = bool(msg.get("is_ghost", False))
            except Exception:
                is_ghost_val = False

            # [P40.1-TYPE] Validate ghost_count: must be a non-negative int.
            try:
                gc_raw = msg.get("ghost_count", 0)
                ghost_count_val = max(0, int(gc_raw))
            except (TypeError, ValueError):
                ghost_count_val = 0

            if eq_val is not None:
                self._equity          = eq_val
                self._equity_is_ghost = is_ghost_val
                self._ghost_count     = ghost_count_val
                # [P40.1-STALE] Record timestamp of last validated equity push.
                if eq_val > 0 and not is_ghost_val:
                    self._last_equity_ts = time.time()

        elif event == "ghost_healed":
            # [P40.1-TYPE] Validate healed_eq: must be finite and positive.
            raw_healed = msg.get("healed_eq", 0.0)
            try:
                healed_val = float(raw_healed)
                if not _math.isfinite(healed_val) or healed_val <= 0:
                    raise ValueError(f"invalid healed_eq={healed_val!r}")
            except (TypeError, ValueError) as _tv:
                log.warning(
                    "[BRIDGE][P40.1-TYPE] ghost_healed: healed_eq rejected — %s "
                    "(raw=%r). Ignoring ghost_healed event.",
                    _tv, raw_healed,
                )
                healed_val = None

            raw_raw_ws = msg.get("raw_ws_eq", 0.0)
            try:
                raw_ws_val = float(raw_raw_ws)
            except (TypeError, ValueError):
                raw_ws_val = 0.0

            source = str(msg.get("source", "unknown"))
            log.warning(
                "[GHOST-HEALED] raw_ws_eq=%.4f healed_eq=%s source=%s ghost_count=%d",
                raw_ws_val,
                f"{healed_val:.4f}" if healed_val is not None else "REJECTED",
                source, msg.get("ghost_count", 0),
            )
            if healed_val is not None:
                self._equity          = healed_val
                self._equity_is_ghost = False
                self._ghost_count     = 0
                self._last_equity_ts  = time.time()

        elif event == "equity_breach":
            log.critical(
                "[EQUITY-BREACH] Bridge confirmed $0.00 equity: %s",
                msg.get("message", ""),
            )

        elif event == "order_fill":
            # [P40.1-TYPE] Validate fill_px and sz.
            try:
                fill_px = float(msg.get("fill_px", 0.0))
                if not _math.isfinite(fill_px) or fill_px <= 0:
                    log.warning(
                        "[BRIDGE][P40.1-TYPE] order_fill: invalid fill_px=%r — "
                        "event forwarded to handlers unchanged.", msg.get("fill_px")
                    )
            except (TypeError, ValueError):
                log.warning(
                    "[BRIDGE][P40.1-TYPE] order_fill: fill_px parse error — "
                    "event forwarded to handlers unchanged."
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

        elif event == "balance_response":
            # [DEFECT3-BRIDGE-SAFE] Validate all numeric fields in balance_response
            # to guard against ghost/zero anomalies and non-finite values from the bridge.
            for _field in ("eq", "cash_bal", "avail_eq"):
                _raw_val = msg.get(_field)
                if _raw_val is None:
                    continue
                try:
                    _fv = float(_raw_val)
                    if not _math.isfinite(_fv):
                        log.warning(
                            "[BRIDGE][DEFECT3] balance_response: field %s is non-finite "
                            "(value=%r) — field retained in msg for request resolution "
                            "but callers must validate before use.",
                            _field, _raw_val,
                        )
                    elif _fv < 0.0:
                        log.warning(
                            "[BRIDGE][DEFECT3] balance_response: field %s is negative "
                            "(value=%.6f) — unexpected; callers should treat as invalid.",
                            _field, _fv,
                        )
                except (TypeError, ValueError) as _bv_exc:
                    log.warning(
                        "[BRIDGE][DEFECT3] balance_response: field %s parse error — %s "
                        "(raw=%r). Callers must validate before use.",
                        _field, _bv_exc, _raw_val,
                    )

        elif event == "microstructure_update":
            # ── [P47] Rust Microstructure Offload ─────────────────────────────
            # Rust pushes pre-computed VPIN ToxicityScore + OFI per symbol.
            # Validate all numeric fields; reject malformed events without
            # corrupting the cache.
            if not self._p47_enabled:
                pass  # disabled — still fire registered handlers below
            else:
                _sym_raw = msg.get("symbol", "")
                try:
                    _sym47 = str(_sym_raw).upper().strip()
                    if not _sym47:
                        raise ValueError("empty symbol")

                    # toxicity: must be finite float in [0.0, 1.0]
                    _tox_raw = msg.get("toxicity", None)
                    _tox47 = float(_tox_raw)
                    if not _math.isfinite(_tox47):
                        raise ValueError(f"non-finite toxicity={_tox47!r}")
                    _tox47 = max(0.0, min(1.0, _tox47))

                    # ofi: must be finite float in [-1.0, 1.0]
                    _ofi_raw = msg.get("ofi", None)
                    _ofi47 = float(_ofi_raw)
                    if not _math.isfinite(_ofi47):
                        raise ValueError(f"non-finite ofi={_ofi47!r}")
                    _ofi47 = max(-1.0, min(1.0, _ofi47))

                    _bid_pull = bool(msg.get("ofi_bid_pull", False))
                    _ask_pull = bool(msg.get("ofi_ask_pull", False))
                    _comp_us  = int(msg.get("computed_at_us", 0))

                    self._p47_ms_cache[_sym47] = {
                        "toxicity":      _tox47,
                        "ofi":           _ofi47,
                        "ofi_bid_pull":  _bid_pull,
                        "ofi_ask_pull":  _ask_pull,
                        "ts":            time.time(),
                        "computed_at_us": _comp_us,
                    }
                    self._p47_update_count += 1

                    if P47_LOG_SOURCE:
                        log.debug(
                            "[P47] microstructure_update %s: tox=%.4f ofi=%.4f "
                            "bid_pull=%s ask_pull=%s comp_us=%d",
                            _sym47, _tox47, _ofi47, _bid_pull, _ask_pull, _comp_us,
                        )

                except (TypeError, ValueError) as _p47_err:
                    log.warning(
                        "[P47][TYPE] microstructure_update validation failed "
                        "(symbol=%r): %s — cache NOT updated.",
                        _sym_raw, _p47_err,
                    )
            # ── [/P47] ────────────────────────────────────────────────────────

        elif event == "floating_update":
            # ── [P48] Rust Autonomous Floating — live tick feedback ────────────
            # Rust emits {event:"floating_update", symbol, current_px,
            #             spread_bps, tick_count, ts_us} after each sub-1ms
            # order adjustment within the active strategic intent envelope.
            # Python caches this for dashboard / p48_snapshot() — no order
            # logic runs here; Rust owns the hot-path.
            if self._p48_enabled:
                _sym_raw48 = msg.get("symbol", "")
                try:
                    _sym48 = str(_sym_raw48).upper().strip()
                    if not _sym48:
                        raise ValueError("empty symbol")
                    _cur_px = float(msg.get("current_px", 0.0))
                    _spread = float(msg.get("spread_bps", 0.0))
                    _ticks  = int(msg.get("tick_count", 0))
                    if not (_math.isfinite(_cur_px) and _cur_px > 0):
                        raise ValueError(f"invalid current_px={_cur_px!r}")
                    self._p48_floating_cache[_sym48] = {
                        "symbol":     _sym48,
                        "current_px": round(_cur_px, 8),
                        "spread_bps": round(_spread, 4),
                        "tick_count": _ticks,
                        "ts":         time.time(),
                        "ts_us":      int(msg.get("ts_us", 0)),
                    }
                    self._p48_update_count += 1
                    if P48_LOG_UPDATES:
                        log.debug(
                            "[P48] floating_update %s: px=%.8f spread=%.2fbps ticks=%d",
                            _sym48, _cur_px, _spread, _ticks,
                        )
                except (TypeError, ValueError) as _p48_err:
                    log.debug(
                        "[P48][TYPE] floating_update validation failed "
                        "(symbol=%r): %s — cache NOT updated.",
                        _sym_raw48, _p48_err,
                    )
            # ── [/P48] ────────────────────────────────────────────────────────

        elif event == "bridge_caps":
            # ── [P49] Capability Handshake — Rust confirms which P49 features ─
            # the native binary supports.  Python gates every P49 feature on
            # the presence of the corresponding cap string in _p49_caps.
            # Missing/empty response = v48 bridge — all features degrade cleanly.
            if self._p49_enabled:
                _raw_caps = msg.get("accepted_caps", [])
                try:
                    self._p49_caps        = frozenset(
                        str(c).strip().lower() for c in _raw_caps if c
                    )
                    self._p49_handshake_done = True
                    _bridge_ver = msg.get("bridge_version", "unknown")
                    log.info(
                        "[P49] Capability handshake complete — bridge_version=%s "
                        "accepted_caps=%s",
                        _bridge_ver, sorted(self._p49_caps),
                    )
                    # Signal the waiting _do_capability_handshake() coroutine.
                    _hsk_ev = getattr(self, "_p49_handshake_event", None)
                    if _hsk_ev is not None:
                        _hsk_ev.set()
                except Exception as _caps_exc:
                    log.warning("[P49] bridge_caps parse error: %s", _caps_exc)
            # ── [/P49] ────────────────────────────────────────────────────────

        elif event == "execution_decision":
            # ── [P50] Native Hot-Path — Rust computed limit price ─────────────
            # Rust responds to an execution_request with its own independently
            # computed limit_px (OFI-adjusted, penny-optimised, tick-rounded).
            #
            # In shadow mode Python uses its own price but records agreement.
            # In native mode Python uses Rust's limit_px directly.
            #
            # Wire format (Rust → Python):
            #   { "event": "execution_decision",
            #     "request_id": str,          # echoed from execution_request
            #     "symbol": str,
            #     "side": "buy"|"sell",
            #     "limit_px": float,          # Rust-computed order price
            #     "post_only": bool,
            #     "strategy_tag": str,        # e.g. "ofi_adjusted"|"penny"|"bbo"
            #     "ofi_adj_bps": float,       # OFI float applied (bps)
            #     "latency_us": int,          # µs from Rust's perspective
            #     "ts_us": int                # Unix µs when Rust computed
            #   }
            if self._p50_enabled:
                import math as _math50
                _sym_raw50 = msg.get("symbol", "")
                try:
                    _sym50     = str(_sym_raw50).upper().strip()
                    _lim_px_raw = msg.get("limit_px", None)
                    _lim_px50   = float(_lim_px_raw)
                    if not _math50.isfinite(_lim_px50) or _lim_px50 <= 0:
                        raise ValueError(f"invalid limit_px={_lim_px50!r}")

                    _lat_us50   = int(msg.get("latency_us",  0))
                    _ofi_adj50  = float(msg.get("ofi_adj_bps", 0.0))
                    _post50     = bool(msg.get("post_only",   False))
                    _tag50      = str(msg.get("strategy_tag", "unknown"))
                    _req_id50   = msg.get("request_id", "")
                    _ts_us50    = int(msg.get("ts_us", 0))

                    self._p50_decision_count += 1
                    self._p50_latency_us_history.append((time.time(), _lat_us50))
                    self._p50_last_decision = {
                        "symbol":       _sym50,
                        "side":         str(msg.get("side", "?")).lower(),
                        "limit_px":     round(_lim_px50, 8),
                        "post_only":    _post50,
                        "strategy_tag": _tag50,
                        "ofi_adj_bps":  round(_ofi_adj50, 4),
                        "latency_us":   _lat_us50,
                        "ts_us":        _ts_us50,
                        "ts":           time.time(),
                    }

                    # Resolve the waiting request_execution() future
                    _fut50 = self._p50_pending.pop(_req_id50, None)
                    if _fut50 is not None and not _fut50.done():
                        _fut50.set_result(self._p50_last_decision)

                    if P50_LOG_DECISIONS:
                        log.debug(
                            "[P50] execution_decision %s %s: px=%.8f post=%s "
                            "tag=%s ofi=%.2fbps lat=%dµs",
                            _sym50, msg.get("side"), _lim_px50, _post50,
                            _tag50, _ofi_adj50, _lat_us50,
                        )

                except (TypeError, ValueError) as _p50_err:
                    log.warning(
                        "[P50][TYPE] execution_decision validation failed "
                        "(symbol=%r): %s — decision ignored.",
                        _sym_raw50, _p50_err,
                    )
                    # Resolve with error so request_execution() falls back promptly
                    _req_id_err = msg.get("request_id", "")
                    _fut_err = self._p50_pending.pop(_req_id_err, None)
                    if _fut_err is not None and not _fut_err.done():
                        _fut_err.set_exception(
                            ValueError(f"[P50] invalid execution_decision: {_p50_err}")
                        )
            # ── [/P50] ────────────────────────────────────────────────────────

        elif event == "twap_slice_fill":
            # ── [P51] TWAP/VWAP Slice Fill notification ───────────────────────
            # Rust emits this for every child-order fill within a TWAP or VWAP
            # algo execution.  Python records the fill, updates the active algo
            # state, and fires registered handlers for dashboard/telemetry.
            #
            # Wire format: { "event": "twap_slice_fill",
            #   "request_id", "symbol", "slice_n", "total_slices",
            #   "fill_qty", "fill_px", "remaining_qty", "algo", "ts_us" }
            if P51_ENABLE:
                import math as _math51s
                _req_id51s  = str(msg.get("request_id", ""))
                _sym51s     = str(msg.get("symbol",     "")).upper().strip()
                try:
                    _fill_qty51 = float(msg.get("fill_qty",      0))
                    _fill_px51  = float(msg.get("fill_px",       0))
                    _rem51      = float(msg.get("remaining_qty", 0))
                    _slice_n51  = int  (msg.get("slice_n",       0))
                    _tot_sl51   = int  (msg.get("total_slices",  0))
                    _algo51s    = str  (msg.get("algo",    "twap")).lower()
                    _ts_us51s   = int  (msg.get("ts_us",         0))

                    if not (_math51s.isfinite(_fill_qty51) and _fill_qty51 > 0
                            and _math51s.isfinite(_fill_px51) and _fill_px51 > 0):
                        raise ValueError(
                            f"invalid fill_qty={_fill_qty51!r} or fill_px={_fill_px51!r}"
                        )

                    self._p51_fill_count += 1

                    # Update active algo running totals
                    _algo_entry51 = self._p51_active_algos.get(_req_id51s)
                    if _algo_entry51 is not None:
                        _algo_entry51["fill_qty"]   = (
                            _algo_entry51.get("fill_qty", 0.0) + _fill_qty51
                        )
                        _algo_entry51["slices_done"] = _slice_n51
                        _algo_entry51["last_fill_px"] = _fill_px51
                        _algo_entry51["last_fill_ts"] = time.time()

                    # Append to ring buffer
                    _fill_snap51 = {
                        "request_id":   _req_id51s,
                        "symbol":       _sym51s,
                        "slice_n":      _slice_n51,
                        "total_slices": _tot_sl51,
                        "fill_qty":     round(_fill_qty51, 8),
                        "fill_px":      round(_fill_px51,  8),
                        "remaining_qty":round(_rem51,      8),
                        "algo":         _algo51s,
                        "ts_us":        _ts_us51s,
                        "ts":           time.time(),
                    }
                    self._p51_slice_history.append(_fill_snap51)

                    if P51_LOG_FILLS:
                        log.debug(
                            "[P51] twap_slice_fill %s %s: slice=%d/%d qty=%.8f "
                            "px=%.8f rem=%.8f",
                            _algo51s.upper(), _sym51s, _slice_n51, _tot_sl51,
                            _fill_qty51, _fill_px51, _rem51,
                        )

                except (TypeError, ValueError) as _p51s_err:
                    log.warning(
                        "[P51][TYPE] twap_slice_fill validation failed (sym=%r): %s",
                        _sym51s, _p51s_err,
                    )
            # ── [/P51-slice_fill] ─────────────────────────────────────────────

        elif event == "algo_complete":
            # ── [P51] Algo Complete — terminal event for TWAP/VWAP/Iceberg ────
            # Rust sends this once when the algo finishes for ANY reason:
            # "filled", "cancelled", "aborted", "ttl_expired".
            # Python resolves the pending future so executor.await_algo_complete()
            # can build the synthetic fill dict and return to the call stack.
            #
            # Wire format: { "event": "algo_complete",
            #   "request_id", "symbol", "algo", "total_filled",
            #   "avg_px", "slippage_bps", "reason", "ts_us" }
            if P51_ENABLE:
                import math as _math51c
                _req_id51c = str(msg.get("request_id", ""))
                _sym51c    = str(msg.get("symbol",     "")).upper().strip()
                try:
                    _filled51   = float(msg.get("total_filled",  0))
                    _avg_px51   = float(msg.get("avg_px",        0))
                    _slip51     = float(msg.get("slippage_bps",  0))
                    _reason51   = str  (msg.get("reason",    "unknown")).lower()
                    _algo51c    = str  (msg.get("algo",      "twap")).lower()
                    _ts_us51c   = int  (msg.get("ts_us",         0))

                    self._p51_complete_count += 1
                    self._p51_active_algos.pop(_req_id51c, None)

                    _complete_snap = {
                        "request_id":   _req_id51c,
                        "symbol":       _sym51c,
                        "algo":         _algo51c,
                        "total_filled": round(_filled51, 8),
                        "avg_px":       round(_avg_px51, 8),
                        "slippage_bps": round(_slip51,   4),
                        "reason":       _reason51,
                        "ts_us":        _ts_us51c,
                        "ts":           time.time(),
                    }
                    self._p51_completed.append(_complete_snap)

                    # Resolve the pending future so executor can unblock
                    _fut51c = self._p51_pending.pop(_req_id51c, None)
                    if _fut51c is not None and not _fut51c.done():
                        _fut51c.set_result(_complete_snap)

                    log.info(
                        "[P51] algo_complete %s %s: filled=%.8f avg_px=%.8f "
                        "slip=%.2fbps reason=%s",
                        _algo51c.upper(), _sym51c,
                        _filled51, _avg_px51, _slip51, _reason51,
                    )

                except (TypeError, ValueError) as _p51c_err:
                    log.warning(
                        "[P51][TYPE] algo_complete validation failed (req=%r): %s",
                        _req_id51c, _p51c_err,
                    )
                    # Resolve with error so callers don't hang
                    _fut51c_err = self._p51_pending.pop(_req_id51c, None)
                    if _fut51c_err is not None and not _fut51c_err.done():
                        _fut51c_err.set_exception(
                            ValueError(f"[P51] invalid algo_complete: {_p51c_err}")
                        )
            # ── [/P51-algo_complete] ──────────────────────────────────────────

        elif event == "p54_stats":
            # ── [P54] Kernel-bypass telemetry event from Rust bridge ──────────
            # Wire format (Rust → Python):
            #   { "event": "p54_stats",
            #     "mode":           "dpdk"|"onload"|"standard"|"unknown",
            #     "rx_drops":       u64,   # lifetime receive packet drops
            #     "tx_drops":       u64,   # lifetime transmit drops
            #     "ring_util_pct":  f32,   # ring buffer utilization 0-100
            #     "lat_p50_us":     f32,   # median latency µs
            #     "lat_p95_us":     f32,   # 95th percentile latency µs
            #     "lat_p99_us":     f32,   # 99th percentile latency µs
            #     "zero_copy":      bool,  # zero-copy path active
            #     "ts_us":          u64    # Rust monotonic µs timestamp
            #   }
            # Python updates its cache and fires threshold WARNINGs.
            # The Rust binary emits this at ~1 Hz when P54 is compiled in.
            if P54_ENABLE:
                try:
                    _m54    = str  (msg.get("mode",          "unknown"))
                    _rxd54  = int  (msg.get("rx_drops",           0))
                    _txd54  = int  (msg.get("tx_drops",           0))
                    _ru54   = float(msg.get("ring_util_pct",     0.0))
                    _lp50   = float(msg.get("lat_p50_us",        0.0))
                    _lp95   = float(msg.get("lat_p95_us",        0.0))
                    _lp99   = float(msg.get("lat_p99_us",        0.0))
                    _zc54   = bool (msg.get("zero_copy",        False))
                    _ts54   = time.time()

                    self._p54_mode        = _m54
                    self._p54_rx_drops    = _rxd54
                    self._p54_tx_drops    = _txd54
                    self._p54_ring_util   = _ru54
                    self._p54_lat_p50_us  = _lp50
                    self._p54_lat_p95_us  = _lp95
                    self._p54_lat_p99_us  = _lp99
                    self._p54_zero_copy   = _zc54
                    self._p54_last_ts     = _ts54
                    self._p54_event_count += 1

                    _snap54 = {
                        "mode": _m54, "rx_drops": _rxd54, "tx_drops": _txd54,
                        "ring_util_pct": _ru54,
                        "lat_p50_us": _lp50, "lat_p95_us": _lp95, "lat_p99_us": _lp99,
                        "zero_copy": _zc54, "ts": _ts54,
                    }
                    self._p54_history.append(_snap54)

                    # Threshold alerts
                    _total_drops = _rxd54 + _txd54
                    if _total_drops > P54_DROP_ALERT_THRESH:
                        log.warning(
                            "[P54] RING DROPS: rx=%d tx=%d total=%d (thresh=%d) mode=%s",
                            _rxd54, _txd54, _total_drops, P54_DROP_ALERT_THRESH, _m54,
                        )
                    if _ru54 > P54_RING_ALERT_PCT:
                        log.warning(
                            "[P54] RING SATURATION: util=%.1f%% > thresh=%.1f%% mode=%s",
                            _ru54, P54_RING_ALERT_PCT, _m54,
                        )
                    if _lp99 > P54_LAT_P99_ALERT_US:
                        # [FIX-P54] Throttle: fires at most once per 60s so a
                        # single stale p99 window can't spam 1 WARNING/Hz forever.
                        _t54now = time.monotonic()
                        if _t54now - self._p54_lat_warn_ts >= 60.0:
                            self._p54_lat_warn_ts = _t54now
                            log.warning(
                                "[P54] LATENCY SPIKE: p99=%.1fµs > thresh=%.1fµs mode=%s"
                                " [throttled: 1/60s — set P54_LAT_P99_ALERT_US=50000 for std TCP]",
                                _lp99, P54_LAT_P99_ALERT_US, _m54,
                            )
                    log.debug(
                        "[P54] stats: mode=%s drops(rx=%d tx=%d) ring=%.1f%% "
                        "lat(p50=%.1f p95=%.1f p99=%.1f)µs zc=%s",
                        _m54, _rxd54, _txd54, _ru54, _lp50, _lp95, _lp99, _zc54,
                    )
                except Exception as _p54_exc:
                    log.debug("[P54] p54_stats parse error: %s", _p54_exc)
            # ── [/P54-p54_stats] ─────────────────────────────────────────────

            # ── Resolve pending request/response futures ──────────────────────────
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

        # ── Registered handlers ───────────────────────────────────────────────
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
        """Send a JSON message using the configured frame mode.

        [P49] When P49_FRAME_MODE="lp", writes a 4-byte big-endian length
        prefix followed by the UTF-8 JSON payload (no trailing newline).
        Default ("newline") appends \\n — backward compatible with all v48 bridges.
        """
        if not self._connected or self._writer is None:
            raise BridgeNotConnectedError("Bridge is not connected")
        payload = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        if P49_ENABLE and P49_FRAME_MODE == "lp":
            import struct as _struct
            self._writer.write(_struct.pack(">I", len(payload)) + payload)
        else:
            self._writer.write(payload + b"\n")
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

        # [P40.5] Stop heartbeat loop before tearing down the connection.
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._heartbeat_task), timeout=0.5
                )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        self._heartbeat_task = None

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
        except Exception as _exc:
            log.warning("[BRIDGE] suppressed: %s", _exc)

        # Fail all pending request futures so callers do not hang.
        try:
            for _rid, fut in list(self._pending.items()):
                if not fut.done():
                    fut.set_exception(BridgeNotConnectedError("Bridge closed"))
            self._pending.clear()
        except Exception as _exc:
            log.warning("[BRIDGE] suppressed: %s", _exc)

        try:
            await self._send({"method": "shutdown"})
        except Exception as _exc:
            log.warning("[BRIDGE] suppressed: %s", _exc)

        # ── [P48] Purge intent/floating caches on close ───────────────────────
        # Rust process is stopping.  Any remaining entries in the Python-side
        # caches are stale — clearing them prevents the dashboard from showing
        # ghost FLOATING badges after a bridge restart.
        self._p48_active_intents.clear()
        self._p48_floating_cache.clear()
        log.debug("[P48] Intent and floating caches cleared on bridge close.")
        # ── [/P48] ────────────────────────────────────────────────────────────

        # ── [P49] Reset capability state on close ─────────────────────────────
        self._p49_caps           = frozenset()
        self._p49_handshake_done = False
        _hsk_ev = getattr(self, "_p49_handshake_event", None)
        if _hsk_ev is not None:
            _hsk_ev.set()   # unblock any waiting handshake coroutine
        log.debug("[P49] Capability state reset on bridge close.")
        # ── [/P49] ────────────────────────────────────────────────────────────

        # ── [P51] Cancel all pending algo futures and clear active state ───────
        # Rust is shutting down — any awaited algo_complete will never arrive.
        # Resolve all pending futures with an exception so callers fall back to
        # P32/P9 Python engines immediately without blocking on timeout.
        try:
            for _r51_id, _f51 in list(self._p51_pending.items()):
                if not _f51.done():
                    _f51.set_exception(BridgeNotConnectedError("Bridge closed"))
            self._p51_pending.clear()
            self._p51_active_algos.clear()
            log.debug("[P51] Pending algo futures resolved + active algos cleared on close.")
        except Exception as _exc:
            log.warning("[BRIDGE] suppressed: %s", _exc)
        # ── [/P51] ────────────────────────────────────────────────────────────

        if self._writer:
            try:
                self._writer.close()
                await asyncio.wait_for(asyncio.shield(self._writer.wait_closed()), timeout=2.0)
            except Exception as _exc:
                log.debug("[BRIDGE] cleanup: %s", _exc)
            finally:
                self._writer = None
                self._reader = None
                self._connected = False

        if self._bridge_proc and self._bridge_proc.poll() is None:
            log.info("[BRIDGE] Terminating Rust bridge process PID=%d", self._bridge_proc.pid)
            try:
                self._bridge_proc.terminate()
            except Exception as _exc:
                log.warning("[BRIDGE] suppressed: %s", _exc)

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

    @property
    def last_equity_ts(self) -> float:
        """
        [P40.1-STALE] Wall-clock timestamp (time.time()) of the last validated,
        non-ghost, positive equity push from the bridge.  Returns 0.0 if no
        valid equity has been received yet.

        Used by bot_watchdog.py to detect bridge freeze:
            age = time.time() - client.last_equity_ts
            if age > WATCHDOG_BRIDGE_STALE_TIMEOUT_SECS: restart()
        """
        return self._last_equity_ts

    def heartbeat_snapshot(self) -> dict:
        """
        [P40.5] Return a serialisable snapshot of the current IPC RTT state.
        Suitable for inclusion in trader_status.json / dashboard API.

        Keys
        ----
        last_rtt_ms         : float  — most recent RTT in milliseconds.
        rtt_p99_ms          : float  — 99th-percentile over rolling window.
        is_ipc_congested    : bool   — True if RTT > warn threshold or timeouts.
        consecutive_timeouts: int    — consecutive ping timeouts since last pong.
        rtt_warn_threshold  : float  — configured warn threshold (ms).
        rtt_critical_threshold: float — configured critical threshold (ms).
        rtt_window_size     : int    — max samples in the rolling deque.
        heartbeat_interval  : float  — seconds between pings.
        """
        return {
            "last_rtt_ms":              round(self._last_rtt_ms, 4),
            "rtt_p99_ms":               round(self.rtt_p99_ms, 4),
            "is_ipc_congested":         self._ipc_congested,
            "consecutive_timeouts":     self._rtt_consecutive_timeouts,
            "rtt_warn_threshold_ms":    BRIDGE_RTT_WARN_MS,
            "rtt_critical_threshold_ms": BRIDGE_RTT_CRITICAL_MS,
            "rtt_window_size":          BRIDGE_RTT_WINDOW,
            "heartbeat_interval_secs":  BRIDGE_HEARTBEAT_INTERVAL_SECS,
            "standdown_miss_threshold": BRIDGE_RTT_STANDDOWN_MISSES,
            # [P40.5-RX] Liveness mode diagnostics for dashboard
            "heartbeat_mode":           BRIDGE_HEARTBEAT_MODE,
            "last_rx_monotonic":        round(self._last_rx_monotonic, 3),
            "last_equity_ts_ago_secs":  round(time.time() - self._last_equity_ts, 2)
                                        if self._last_equity_ts > 0 else None,
        }

    # ── [P47] Rust Microstructure Offload — public API ─────────────────────────

    def get_p47_microstructure(
        self, symbol: str
    ) -> tuple:
        """
        [P47] Return the latest Rust-computed microstructure values for *symbol*.

        Returns a 5-tuple:
            (toxicity, ofi, ofi_bid_pull, ofi_ask_pull, is_fresh)

        toxicity      float  ∈ [0.0, 1.0]  — VPIN CDF ToxicityScore
        ofi           float  ∈ [-1.0, 1.0] — Order Flow Imbalance
        ofi_bid_pull  bool                 — passive bid wall pulled last tick
        ofi_ask_pull  bool                 — passive ask wall pulled last tick
        is_fresh      bool                 — True if data is ≤ P47_STALENESS_SECS old

        Returns (0.0, 0.0, False, False, False) when:
          - P47_ENABLE is 0
          - No data received for this symbol yet
          - Data is stale (older than P47_STALENESS_SECS)

        Callers should fall back to Python-computed values when is_fresh=False.
        """
        if not self._p47_enabled:
            return 0.0, 0.0, False, False, False
        try:
            _key = str(symbol).upper().strip()
            entry = self._p47_ms_cache.get(_key)
            if entry is None:
                return 0.0, 0.0, False, False, False
            age = time.time() - entry["ts"]
            is_fresh = age <= P47_STALENESS_SECS
            return (
                entry["toxicity"],
                entry["ofi"],
                entry["ofi_bid_pull"],
                entry["ofi_ask_pull"],
                is_fresh,
            )
        except Exception as exc:
            log.debug("[P47] get_p47_microstructure error %s: %s", symbol, exc)
            return 0.0, 0.0, False, False, False

    async def subscribe_microstructure(self, symbols: list) -> None:
        """
        [P47] Send a subscribe_microstructure request to the Rust bridge,
        instructing it to start pushing microstructure_update events for
        the given symbol list.

        This is a fire-and-forget send — no response is expected.  Safe
        to call before the bridge is connected (the request is dropped
        with a debug log if not connected).

        The Rust bridge must implement the "subscribe_microstructure" method.
        When P47_SUBSCRIBE_ON_CONNECT=1, this is called automatically after
        each successful connection in _connect_loop().
        """
        if not self._p47_enabled:
            return
        if not self._connected or self._writer is None:
            log.debug("[P47] subscribe_microstructure: not connected — skipped")
            return
        try:
            _syms = [str(s).upper().strip() for s in symbols if s]
            msg = {
                "method":  "subscribe_microstructure",
                "symbols": _syms,
                "staleness_ms": int(P47_STALENESS_SECS * 1000),
            }
            await self._send(msg)
            log.info(
                "[P47] subscribe_microstructure sent: symbols=%s staleness_ms=%d",
                _syms, int(P47_STALENESS_SECS * 1000),
            )
        except Exception as exc:
            log.warning("[P47] subscribe_microstructure send error: %s", exc)

    def p47_snapshot(self) -> dict:
        """
        [P47] Return a serialisable snapshot of all cached microstructure data.
        Suitable for inclusion in trader_status.json under "p47_microstructure".

        Keys
        ----
        enabled        : bool  — P47_ENABLE flag
        update_count   : int   — lifetime microstructure_update events received
        staleness_secs : float — P47_STALENESS_SECS threshold
        symbols        : dict  — per-symbol snapshot with freshness info
        """
        now = time.time()
        symbols_out: Dict[str, Any] = {}
        for sym, entry in self._p47_ms_cache.items():
            age = now - entry["ts"]
            symbols_out[sym] = {
                "toxicity":       round(entry["toxicity"],  4),
                "ofi":            round(entry["ofi"],        4),
                "ofi_bid_pull":   entry["ofi_bid_pull"],
                "ofi_ask_pull":   entry["ofi_ask_pull"],
                "age_ms":         round(age * 1000,          1),
                "is_fresh":       age <= P47_STALENESS_SECS,
                "computed_at_us": entry.get("computed_at_us", 0),
            }
        return {
            "enabled":        self._p47_enabled,
            "update_count":   self._p47_update_count,
            "staleness_secs": P47_STALENESS_SECS,
            "symbols":        symbols_out,
        }

    # ── [/P47] ────────────────────────────────────────────────────────────────

    # ── [P48] Autonomous Floating — Strategic Intent API ──────────────────────
    #
    # send_strategic_intent() and cancel_strategic_intent() are defined in the
    # P49 block above (alongside publish_brain_state / _do_capability_handshake)
    # because P49 added regime/toxicity/obi_score enrichment params.
    # The P48 cache update logic is inside those methods.
    # ─────────────────────────────────────────────────────────────────────────

    async def cancel_strategic_intent(self, symbol: str) -> None:
        """[P48] KILL SWITCH: cancel all autonomous floating for *symbol*.

        Python retains absolute authority over Rust.  Rust MUST immediately
        halt all floating for this symbol regardless of any active TTL.

        Safe to call when disconnected — logs a warning rather than raising.
        """
        try:
            await self._send({
                "method": "cancel_strategic_intent",
                "symbol": str(symbol).upper().strip(),
                "ts":     round(time.time(), 6),
            })
            # ── [P48-CACHE] Evict local intent and floating state ──────────────
            _sym_norm = str(symbol).upper().strip()
            self._p48_active_intents.pop(_sym_norm, None)
            self._p48_floating_cache.pop(_sym_norm, None)
            log.warning("[P48] KILL-SWITCH fired: cancel_strategic_intent(%s)", symbol)
        except BridgeNotConnectedError:
            log.warning(
                "[P48] KILL-SWITCH: bridge not connected for %s — "
                "Python-side pennying should already be active as fallback.", symbol
            )
        except Exception as exc:
            log.error("[P48] cancel_strategic_intent error: %s", exc)

    def p48_snapshot(self) -> dict:
        """[P48] Status snapshot for trader_status.json / dashboard.

        Returns
        -------
        dict with keys:
            enabled          : bool  — P48_ENABLE flag
            bridge_connected : bool  — IPC link live
            intent_count     : int   — lifetime intents sent to Rust
            update_count     : int   — lifetime floating_update events received
            ttl_secs         : float — default intent TTL
            active_intents   : dict  — per-symbol active intent (expires_at, bounds)
            floating_state   : dict  — per-symbol latest floating_update from Rust
        """
        now = time.time()
        # ── [P48-PRUNE] Evict expired intents to prevent unbounded dict growth ──
        # TTL expiry is the only way an intent becomes stale when cancel was not
        # explicitly called (e.g. the order filled naturally and Rust auto-expired).
        # Safe: dict.pop() during iteration over a snapshot of keys.
        _expired = [s for s, e in self._p48_active_intents.items()
                    if e.get("expires_at", 0) < now]
        for _s in _expired:
            self._p48_active_intents.pop(_s, None)
        # ── [/P48-PRUNE] ──────────────────────────────────────────────────────
        intents_out: Dict[str, Any] = {}
        for sym, entry in self._p48_active_intents.items():
            age       = now - entry.get("sent_ts", now)
            remaining = max(0.0, entry.get("expires_at", now) - now)
            intents_out[sym] = {
                "side":           entry.get("side", "?"),
                "price_floor":    entry.get("price_floor",    0.0),
                "price_ceil":     entry.get("price_ceil",     0.0),
                "target_px":      entry.get("target_px",      0.0),
                "max_spread_bps": entry.get("max_spread_bps", 0.0),
                "ttl_secs":       entry.get("ttl_secs",       P48_TTL_SECS),
                "ttl_remaining":  round(remaining, 1),
                "age_ms":         round(age * 1000, 1),
                "is_active":      remaining > 0,
            }
        floating_out: Dict[str, Any] = {}
        for sym, fentry in self._p48_floating_cache.items():
            f_age = now - fentry.get("ts", now)
            floating_out[sym] = {
                "current_px":  fentry.get("current_px", 0.0),
                "spread_bps":  fentry.get("spread_bps", 0.0),
                "tick_count":  fentry.get("tick_count", 0),
                "age_ms":      round(f_age * 1000, 1),
                "is_fresh":    f_age < 2.0,   # stale if Rust hasn't ticked in 2s
                "ts_us":       fentry.get("ts_us", 0),
            }
        return {
            "enabled":          self._p48_enabled,
            "bridge_connected": self._connected,
            "intent_count":     self._p48_intent_count,
            "update_count":     self._p48_update_count,
            "ttl_secs":         P48_TTL_SECS,
            "floor_bps":        P48_FLOOR_BPS,
            "ceil_bps":         P48_CEIL_BPS,
            "active_intents":   intents_out,
            "floating_state":   floating_out,
            "description":      "Python price bounds → Rust tactical pennying",
        }

    # ── [/P48] ────────────────────────────────────────────────────────────────

    # ── [P49] Hybrid Bridge Core — capability handshake & brain state ─────────

    async def _do_capability_handshake(self) -> None:
        """[P49] Send bridge_hello and wait for bridge_caps response.

        Called as a concurrent asyncio task immediately after each new TCP
        connection is established in _connect_loop().  The read loop (_read_loop)
        dispatches the bridge_caps response and sets _p49_handshake_event.

        If no response arrives within P49_HANDSHAKE_TIMEOUT_SECS (v48 bridge
        doesn't know this method), the timeout is silently absorbed and
        _p49_caps remains an empty frozenset — all P49 features degrade to their
        v48 fallback paths.  Zero impact on existing functionality.
        """
        if not self._p49_enabled or not P49_CAPABILITY_HANDSHAKE:
            return
        # Lazy-create the event (safe: called from async context only).
        if not hasattr(self, "_p49_handshake_event"):
            self._p49_handshake_event = asyncio.Event()
        self._p49_handshake_event.clear()

        hello = {
            "method":     "bridge_hello",
            "version":    P49_VERSION,
            "caps":       ["lp_framing", "intent_v2", "brain_state", "hot_path", "twap_vwap"],  # [BUG-2-FIX] hot_path + [P51] twap_vwap
            "frame_mode": P49_FRAME_MODE,
            "ts":         round(time.time(), 6),
        }
        try:
            await self._send(hello)
            log.debug("[P49] bridge_hello sent — awaiting bridge_caps (%.1fs timeout).",
                      P49_HANDSHAKE_TIMEOUT_SECS)
            await asyncio.wait_for(
                self._p49_handshake_event.wait(),
                timeout=P49_HANDSHAKE_TIMEOUT_SECS,
            )
        except asyncio.TimeoutError:
            # v48 bridge — no response expected.  Full backward compat.
            log.info(
                "[P49] Capability handshake timeout (%.1fs) — v48 bridge assumed. "
                "LP framing, intent_v2, and brain_state operating in fallback mode.",
                P49_HANDSHAKE_TIMEOUT_SECS,
            )
            self._p49_handshake_done = False
            self._p49_caps           = frozenset()
        except BridgeNotConnectedError:
            pass
        except Exception as exc:
            log.debug("[P49] Handshake error (non-fatal): %s", exc)

    async def publish_brain_state(
        self,
        equity:       float,
        positions:    int,
        symbols:      List[str],
        regime_map:   Dict[str, str],
        cycle_ts:     float,
        is_congested: bool = False,
    ) -> None:
        """[P49] Publish Python's brain state to the Rust bridge.

        This is the IPC contract that enables the P50 native brain migration.
        Rust receives a compact strategic snapshot every P49_BRAIN_STATE_INTERVAL_SECS
        and can shadow Python's decisions, eventually replacing the Python brain
        entirely (Phase 52).

        Parameters
        ----------
        equity       : Current verified equity (Binary Truth anchor).
        positions    : Number of open positions.
        symbols      : Active trading symbols (normalised).
        regime_map   : Per-symbol regime — "bull" | "bear" | "neutral".
        cycle_ts     : Wall-clock timestamp of the current brain cycle.
        is_congested : True if IPC RTT > warn threshold (P40.5).

        Publish conditions
        ------------------
        - P49_ENABLE must be True.
        - "brain_state" must be in _p49_caps OR P49_BRAIN_STATE_FORCE=1.
        - Rate-limited to once per P49_BRAIN_STATE_INTERVAL_SECS (default 0.5s).
        - Silently no-ops when bridge is disconnected.
        """
        if not self._p49_enabled:
            return
        if not ("brain_state" in self._p49_caps or P49_BRAIN_STATE_FORCE):
            return
        now = time.time()
        if now - self._p49_brain_state_ts < P49_BRAIN_STATE_INTERVAL_SECS:
            return  # rate-limit: bridge doesn't need sub-500ms brain updates

        import math as _math_p49
        _eq = float(equity) if _math_p49.isfinite(float(equity)) else 0.0
        msg: Dict[str, Any] = {
            "method":      "brain_state_update",
            "equity":      round(_eq, 2),
            "positions":   max(0, int(positions)),
            "symbols":     [str(s).upper().strip() for s in (symbols or []) if s],
            "regime_map":  {
                str(k).upper().strip(): str(v).lower().strip()
                for k, v in (regime_map or {}).items()
                if k and v
            },
            "is_congested": bool(is_congested),
            "cycle_ts":    round(float(cycle_ts), 6),
            "ts":          round(now, 6),
        }
        try:
            await self._send(msg)
            self._p49_brain_state_count += 1
            self._p49_brain_state_ts    = now
            self._p49_last_brain_state  = msg
            log.debug(
                "[P49] brain_state published #%d: eq=%.2f pos=%d syms=%d congested=%s",
                self._p49_brain_state_count, _eq, positions,
                len(symbols or []), is_congested,
            )
        except BridgeNotConnectedError:
            pass   # bridge down — no-op, next cycle will retry
        except Exception as exc:
            log.debug("[P49] publish_brain_state error (non-fatal): %s", exc)

    async def send_strategic_intent(
        self,
        symbol:         str,
        side:           str,
        price_floor:    float,
        price_ceil:     float,
        target_px:      float,
        max_spread_bps: float = 5.0,
        ttl_secs:       float = 30.0,
        regime:         Optional[str]   = None,   # [P49] "bull"|"bear"|"neutral"
        toxicity:       Optional[float] = None,   # [P49] VPIN toxicity ∈ [0,1]
        obi_score:      Optional[float] = None,   # [P49] OBI ∈ [-1,1]
    ) -> None:
        """[P48/P49] Send price bounds to Rust for autonomous limit floating.

        Python (Strategic Supervisor) computes the safe price envelope;
        Rust (Tactical Predator) handles sub-1ms order placement within it.

        Parameters
        ----------
        symbol          : OKX instrument ID, e.g. "BTC-USDT-SWAP"
        side            : "buy" or "sell"
        price_floor     : Python lower bound — Rust MUST NOT float below this
        price_ceil      : Python upper bound — Rust MUST NOT float above this
        target_px       : current ideal limit price (penny target at send time)
        max_spread_bps  : max tolerable bid-ask spread (default 5 bps)
        ttl_secs        : intent TTL — Rust halts floating after expiry (30 s)
        regime          : [P49] market regime for smarter Rust envelope widening
        toxicity        : [P49] VPIN ToxicityScore ∈ [0, 1] — informs Rust caution
        obi_score       : [P49] Order Flow Imbalance ∈ [-1, 1] — directional bias

        Raises BridgeNotConnectedError when disconnected.
        Caller should catch and fall back to Python-side pennying.
        """
        import math as _math
        # [P48-VALIDATE] Python validates bounds before sending — kill-switch authority
        for _v, _name in ((price_floor, "price_floor"), (price_ceil, "price_ceil"),
                          (target_px,   "target_px")):
            if not isinstance(_v, (int, float)) or not _math.isfinite(_v) or _v <= 0:
                raise ValueError(f"[P48] invalid {_name}={_v!r}")
        if price_floor >= price_ceil:
            raise ValueError(f"[P48] price_floor={price_floor} >= price_ceil={price_ceil}")
        if not (price_floor <= target_px <= price_ceil):
            raise ValueError(
                f"[P48] target_px={target_px} outside [{price_floor}, {price_ceil}]"
            )

        _sym_norm = str(symbol).upper().strip()
        msg: Dict[str, Any] = {
            "method":         "set_strategic_intent",
            "symbol":         _sym_norm,
            "side":           str(side).lower().strip(),
            "price_floor":    round(float(price_floor),    8),
            "price_ceil":     round(float(price_ceil),     8),
            "target_px":      round(float(target_px),      8),
            "max_spread_bps": round(float(max_spread_bps), 4),
            "ttl_secs":       round(float(ttl_secs),       2),
            "ts":             round(time.time(),            6),
        }
        # ── [P49] Intent V2 enrichment ────────────────────────────────────────
        # When the v49+ bridge has accepted the "intent_v2" capability, attach
        # regime, toxicity, and OBI context.  Rust uses this to apply smarter
        # envelope widening (e.g. tighter ceiling in a bull regime with low
        # toxicity).  Extra fields are safely ignored by v48 bridges.
        if P49_ENABLE and P49_INTENT_V2 and "intent_v2" in self._p49_caps:
            if regime is not None:
                msg["regime"] = str(regime).lower().strip()
            if toxicity is not None and _math.isfinite(float(toxicity)):
                msg["toxicity"]   = round(max(0.0, min(1.0, float(toxicity))), 4)
            if obi_score is not None and _math.isfinite(float(obi_score)):
                msg["obi_score"]  = round(max(-1.0, min(1.0, float(obi_score))), 4)
            msg["intent_version"] = 2
        # ── [/P49] ────────────────────────────────────────────────────────────

        await self._send(msg)

        # ── [P48-CACHE] Record locally for p48_snapshot() / dashboard ─────────
        now = time.time()
        self._p48_active_intents[_sym_norm] = {
            "symbol":         _sym_norm,
            "side":           str(side).lower().strip(),
            "price_floor":    round(float(price_floor),    8),
            "price_ceil":     round(float(price_ceil),     8),
            "target_px":      round(float(target_px),      8),
            "max_spread_bps": round(float(max_spread_bps), 4),
            "ttl_secs":       round(float(ttl_secs),       2),
            "sent_ts":        now,
            "expires_at":     now + float(ttl_secs),
            # [P49] cache enrichment fields for dashboard display
            "regime":         regime,
            "toxicity":       round(float(toxicity), 4) if toxicity is not None and _math.isfinite(float(toxicity)) else None,
            "obi_score":      round(float(obi_score), 4) if obi_score is not None and _math.isfinite(float(obi_score)) else None,
        }
        self._p48_intent_count += 1
        log.debug(
            "[P48] intent: %s %s floor=%.6f ceil=%.6f target=%.6f ttl=%.0fs "
            "[P49 v2=%s regime=%s tox=%s obi=%s]",
            symbol, side, price_floor, price_ceil, target_px, ttl_secs,
            "intent_v2" in self._p49_caps, regime, toxicity, obi_score,
        )

    def p49_snapshot(self) -> dict:
        """[P49] Status snapshot for trader_status.json / dashboard.

        Returns a dict suitable for injection under "p49_protocol" in trader_status.json.

        Keys
        ----
        enabled                : bool   — P49_ENABLE flag
        frame_mode             : str    — "newline" | "lp"
        capability_handshake   : bool   — P49_CAPABILITY_HANDSHAKE flag
        handshake_done         : bool   — True if bridge responded with bridge_caps
        accepted_caps          : list   — sorted list of cap strings from bridge
        intent_v2_active       : bool   — intent_v2 cap granted AND P49_INTENT_V2=1
        brain_state_active     : bool   — brain_state cap granted OR FORCE=1
        brain_state_count      : int    — lifetime brain_state_update publishes
        brain_state_interval_s : float  — publish rate limit (seconds)
        last_brain_state_age_s : float|None — seconds since last publish
        bridge_connected       : bool   — IPC link live
        version                : str    — "49.0"
        """
        now = time.time()
        _bs_age = round(now - self._p49_brain_state_ts, 2) if self._p49_brain_state_ts > 0 else None
        return {
            "enabled":               self._p49_enabled,
            "frame_mode":            P49_FRAME_MODE,
            "capability_handshake":  P49_CAPABILITY_HANDSHAKE,
            "handshake_done":        self._p49_handshake_done,
            "accepted_caps":         sorted(self._p49_caps),
            "intent_v2_active":      ("intent_v2" in self._p49_caps and P49_INTENT_V2),
            "brain_state_active":    ("brain_state" in self._p49_caps or P49_BRAIN_STATE_FORCE),
            "brain_state_count":     self._p49_brain_state_count,
            "brain_state_interval_s": P49_BRAIN_STATE_INTERVAL_SECS,
            "last_brain_state_age_s": _bs_age,
            "bridge_connected":       self._connected,
            "version":                P49_VERSION,
        }

    # ── [/P49] ────────────────────────────────────────────────────────────────

    # ── [P50] Native Hot-Path Port ────────────────────────────────────────────

    async def request_execution(
        self,
        symbol:       str,
        side:         str,
        signal_conf:  float,
        kelly_f:      float,
        equity:       float,
        regime:       Optional[str]   = None,
        toxicity:     Optional[float] = None,
        obi_score:    Optional[float] = None,
        book_depth:   Optional[int]   = None,
    ) -> Optional[dict]:
        """[P50] Ask Rust to compute the limit price for this execution.

        Sends an execution_request to the Rust bridge and waits up to
        P50_TIMEOUT_US microseconds for an execution_decision response.

        This is the primary Python ↔ Rust handoff for Phase 50's native
        hot-path.  The response contains Rust's independently computed
        limit_px (OFI-adjusted, penny-optimised, tick-rounded), which
        Python either uses directly (native mode) or logs for agreement
        tracking (shadow mode).

        Parameters
        ----------
        symbol       : Normalised OKX instrument ID (e.g. "BTC-USDT-SWAP")
        side         : "buy" or "sell"
        signal_conf  : Brain signal confidence ∈ [0, 1]
        kelly_f      : Kelly fraction for this execution ∈ [0, 1]
        equity       : Current verified equity (Binary Truth anchor)
        regime       : Market regime ("bull"|"bear"|"neutral"|None)
        toxicity     : VPIN ToxicityScore ∈ [0, 1] from P47 (or None)
        obi_score    : OFI score ∈ [-1, 1] from P47 (or None)
        book_depth   : Levels of order book depth to include (default=None→Rust uses its own)

        Returns
        -------
        dict  : execution_decision from Rust, or None on timeout/fallback.

        The caller MUST handle None gracefully (fall back to Python pricing).
        In shadow mode this always returns None — the decision is cached
        internally for agreement tracking but Python is never blocked.

        Raises
        ------
        Never raises.  All exceptions are caught and logged at debug level;
        the caller receives None and falls back to Python pricing.
        """
        # Capability gate: only active when "hot_path" ∈ accepted caps
        if not self._p50_enabled:
            return None
        if not P50_FORCE and "hot_path" not in self._p49_caps:
            return None
        if not self._connected:
            return None

        import math as _math50
        import uuid  as _uuid50

        req_id = _uuid50.uuid4().hex[:16]

        # Build request payload
        msg: Dict[str, Any] = {
            "method":       "execution_request",
            "request_id":   req_id,
            "symbol":       str(symbol).upper().strip(),
            "side":         str(side).lower().strip(),
            "signal_conf":  round(max(0.0, min(1.0, float(signal_conf))), 4),
            "kelly_f":      round(max(0.0, min(1.0, float(kelly_f))),     4),
            "equity":       round(float(equity) if _math50.isfinite(float(equity)) else 0.0, 2),
            "ts_us":        int(time.time() * 1_000_000),
        }
        # Optional enrichment fields (present only when valid)
        if regime is not None:
            msg["regime"] = str(regime).lower().strip()
        if toxicity is not None and _math50.isfinite(float(toxicity)):
            msg["toxicity"]  = round(max(0.0, min(1.0, float(toxicity))), 4)
        if obi_score is not None and _math50.isfinite(float(obi_score)):
            msg["obi_score"] = round(max(-1.0, min(1.0, float(obi_score))), 4)
        if book_depth is not None:
            msg["book_depth"] = max(1, int(book_depth))

        # Shadow mode: fire-and-forget — don't block Python's hot-path
        if P50_SHADOW_MODE:
            try:
                _fut_shadow: asyncio.Future = asyncio.get_event_loop().create_future()
                self._p50_pending[req_id] = _fut_shadow
                await self._send(msg)
                self._p50_request_count += 1
                # Background task resolves the future when execution_decision arrives
                # Python continues with its own pricing immediately
            except Exception as _s_exc:
                log.debug("[P50-SHADOW] request_execution error (non-fatal): %s", _s_exc)
                self._p50_pending.pop(req_id, None)
            return None  # shadow mode always returns None

        # Native mode: wait for Rust decision within timeout
        timeout_s = P50_TIMEOUT_US / 1_000_000.0
        try:
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._p50_pending[req_id] = fut
            await self._send(msg)
            self._p50_request_count += 1

            decision = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout_s)
            return decision

        except asyncio.TimeoutError:
            self._p50_pending.pop(req_id, None)
            self._p50_fallback_count += 1
            log.debug(
                "[P50] execution_decision timeout (%dµs) for %s %s — Python fallback.",
                P50_TIMEOUT_US, symbol, side,
            )
            return None
        except BridgeNotConnectedError:
            self._p50_pending.pop(req_id, None)
            self._p50_fallback_count += 1
            return None
        except Exception as _exc:
            self._p50_pending.pop(req_id, None)
            self._p50_fallback_count += 1
            log.debug("[P50] request_execution error (non-fatal): %s", _exc)
            return None

    def record_shadow_agreement(
        self,
        python_px:  float,
        rust_px:    float,
        symbol:     str = "",
    ) -> bool:
        """[P50] Record whether Python and Rust agreed on this execution price.

        Called by the executor when a shadow execution_decision is received
        after the order was already placed using Python's price.

        Parameters
        ----------
        python_px : Price Python actually submitted.
        rust_px   : Price Rust independently computed.
        symbol    : For logging only.

        Returns
        -------
        bool : True if the two prices agreed within P50_AGREE_BPS.
        """
        import math as _m
        if not (_m.isfinite(python_px) and python_px > 0 and
                _m.isfinite(rust_px)   and rust_px   > 0):
            return False

        diff_bps = abs(python_px - rust_px) / python_px * 10_000.0
        agreed   = diff_bps <= P50_AGREE_BPS

        if agreed:
            self._p50_agree_count += 1
        else:
            self._p50_disagree_count += 1
            log.debug(
                "[P50-SHADOW] price divergence: py=%.8f rust=%.8f diff=%.2fbps sym=%s",
                python_px, rust_px, diff_bps, symbol,
            )
        return agreed

    def p50_snapshot(self) -> dict:
        """[P50] Status snapshot for trader_status.json / dashboard.

        Keys
        ----
        enabled              : bool   — P50_ENABLE
        shadow_mode          : bool   — P50_SHADOW_MODE
        bridge_connected     : bool   — IPC link live
        hot_path_cap_granted : bool   — "hot_path" ∈ _p49_caps
        request_count        : int    — lifetime execution_requests sent
        decision_count       : int    — lifetime execution_decisions received
        fallback_count       : int    — times Python fell back (timeout/error)
        agree_count          : int    — shadow rounds within P50_AGREE_BPS
        disagree_count       : int    — shadow rounds that diverged
        agreement_rate       : float  — agree / (agree+disagree), None if no data
        avg_latency_us       : float  — rolling avg Rust compute latency (µs)
        p99_latency_us       : float  — p99 latency, None if < 10 samples
        last_decision        : dict   — most recent execution_decision snapshot
        timeout_us           : int    — P50_TIMEOUT_US
        agree_bps            : float  — P50_AGREE_BPS
        version              : str    — "50.0"
        """
        import statistics as _stat

        total_shadow = self._p50_agree_count + self._p50_disagree_count
        agreement_rate = (
            self._p50_agree_count / total_shadow if total_shadow > 0 else None
        )

        lats = [lat for _, lat in self._p50_latency_us_history]
        avg_lat = round(sum(lats) / len(lats), 1) if lats else None
        p99_lat = None
        if len(lats) >= 10:
            lats_sorted = sorted(lats)
            p99_lat = lats_sorted[int(len(lats_sorted) * 0.99)]

        return {
            "enabled":              self._p50_enabled,
            "shadow_mode":          self._p50_shadow_mode,
            "bridge_connected":     self._connected,
            "hot_path_cap_granted": ("hot_path" in self._p49_caps),
            "force_mode":           P50_FORCE,
            "request_count":        self._p50_request_count,
            "decision_count":       self._p50_decision_count,
            "fallback_count":       self._p50_fallback_count,
            "agree_count":          self._p50_agree_count,
            "disagree_count":       self._p50_disagree_count,
            "agreement_rate":       round(agreement_rate, 4) if agreement_rate is not None else None,
            "avg_latency_us":       avg_lat,
            "p99_latency_us":       p99_lat,
            "last_decision":        dict(self._p50_last_decision),
            "timeout_us":           P50_TIMEOUT_US,
            "agree_bps":            P50_AGREE_BPS,
            "version":              P50_VERSION,
            "description":          "Python→Rust hot-path: signal-to-order < 500µs",
        }

    # ── [/P50] ────────────────────────────────────────────────────────────────

    # ── [P51] Institutional Execution Suite — TWAP/VWAP/Iceberg/Cancel API ────

    async def request_twap(
        self,
        symbol:         str,
        side:           str,
        total_qty:      float,
        algo:           str   = "twap",
        duration_secs:  float = None,
        slices:         int   = None,
        price_floor:    float = 0.0,
        price_ceil:     float = 1e18,
        equity:         float = 0.0,
        regime:         Optional[str]   = None,
        toxicity:       Optional[float] = None,
    ) -> Optional[str]:
        """[P51] Delegate a TWAP/VWAP execution to the Rust native engine.

        Sends a twap_request to the Rust bridge and registers a pending
        asyncio.Future keyed by request_id.  The caller must then call
        wait_algo_complete(request_id) to await the result.

        Parameters
        ----------
        symbol        : Normalised OKX instrument ID (e.g. "BTC-USDT-SWAP")
        side          : "buy" or "sell"
        total_qty     : Total base-currency quantity to execute
        algo          : "twap" (default) or "vwap"
        duration_secs : TWAP window in seconds (default P51_TWAP_DEFAULT_DURATION_SECS)
        slices        : Target slice count (default P51_TWAP_DEFAULT_SLICES)
        price_floor   : Hard lower bound — Rust MUST NOT execute below this
        price_ceil    : Hard upper bound — Rust MUST NOT execute above this
        equity        : Current verified equity (for Rust risk gates)
        regime        : Market regime hint ("bull"|"bear"|"neutral")
        toxicity      : VPIN ToxicityScore [0,1] from P47

        Returns
        -------
        str  : request_id to pass to wait_algo_complete(), or
        None : when bridge not available, capability absent, or not enabled.

        Never raises — all exceptions are caught; caller receives None.
        """
        # Capability gate
        if not P51_ENABLE:
            return None
        if not P51_FORCE and "twap_vwap" not in self._p49_caps:
            return None
        if not self._connected:
            return None

        import math as _m51r
        if not (_m51r.isfinite(total_qty) and total_qty > 0):
            log.warning("[P51] request_twap: invalid total_qty=%.8f", total_qty)
            return None

        _dur  = float(duration_secs) if duration_secs is not None else P51_TWAP_DEFAULT_DURATION_SECS
        _slc  = int(slices) if slices is not None else P51_TWAP_DEFAULT_SLICES
        req_id = uuid.uuid4().hex[:16]

        msg: Dict[str, Any] = {
            "method":        "twap_request",
            "request_id":    req_id,
            "symbol":        str(symbol).upper().strip(),
            "side":          str(side).lower().strip(),
            "total_qty":     round(float(total_qty), 8),
            "algo":          str(algo).lower().strip(),
            "duration_secs": round(_dur, 2),
            "slices":        max(2, _slc),
            "price_floor":   round(max(0.0, float(price_floor)), 8),
            "price_ceil":    round(max(0.0, float(price_ceil)),  8),
            "equity":        round(float(equity) if _m51r.isfinite(float(equity)) else 0.0, 2),
            "ts_us":         int(time.time() * 1_000_000),
        }
        if regime is not None:
            msg["regime"]   = str(regime).lower().strip()
        if toxicity is not None and _m51r.isfinite(float(toxicity)):
            msg["toxicity"] = round(max(0.0, min(1.0, float(toxicity))), 4)

        try:
            _fut51 = asyncio.get_event_loop().create_future()
            self._p51_pending[req_id] = _fut51
            self._p51_active_algos[req_id] = {
                "symbol":      msg["symbol"],
                "algo":        msg["algo"],
                "side":        msg["side"],
                "total_qty":   float(total_qty),
                "fill_qty":    0.0,
                "slices_done": 0,
                "price_floor": float(price_floor),
                "price_ceil":  float(price_ceil),
                "sent_ts":     time.time(),
                "shadow_mode": P51_SHADOW_MODE,
            }
            await self._send(msg)
            self._p51_request_count += 1
            log.info(
                "[P51] twap_request sent: %s %s %s qty=%.8f dur=%.0fs slices=%d",
                msg["algo"].upper(), msg["side"].upper(), msg["symbol"],
                float(total_qty), _dur, _slc,
            )
            return req_id

        except BridgeNotConnectedError:
            self._p51_pending.pop(req_id, None)
            self._p51_active_algos.pop(req_id, None)
            self._p51_fallback_count += 1
            return None
        except Exception as exc:
            self._p51_pending.pop(req_id, None)
            self._p51_active_algos.pop(req_id, None)
            self._p51_fallback_count += 1
            log.debug("[P51] request_twap error: %s", exc)
            return None

    async def request_iceberg(
        self,
        symbol:       str,
        side:         str,
        total_qty:    float,
        display_qty:  float,
        price_floor:  float = 0.0,
        price_ceil:   float = 1e18,
        ttl_secs:     float = None,
    ) -> Optional[str]:
        """[P51] Delegate an Iceberg order execution to the Rust native engine.

        Sends an iceberg_request to the Rust bridge.  Rust manages all child-order
        placement, display-qty replenishment, and price re-centering within the
        [price_floor, price_ceil] envelope.  Python retains kill-switch authority
        via cancel_algo(request_id).

        Parameters
        ----------
        symbol       : Normalised OKX instrument ID
        side         : "buy" or "sell"
        total_qty    : Total base-currency quantity to execute
        display_qty  : Visible quantity per child order (should be < total_qty)
        price_floor  : Hard lower bound (Rust MUST NOT execute below)
        price_ceil   : Hard upper bound (Rust MUST NOT execute above)
        ttl_secs     : Max lifetime for this iceberg (default P51_ICEBERG_DEFAULT_TTL_SECS)

        Returns
        -------
        str  : request_id to pass to wait_algo_complete(), or None on failure.
        """
        if not P51_ENABLE:
            return None
        if not P51_FORCE and "twap_vwap" not in self._p49_caps:
            return None
        if not self._connected:
            return None

        import math as _m51i
        if not (_m51i.isfinite(total_qty) and total_qty > 0):
            return None
        if not (_m51i.isfinite(display_qty) and display_qty > 0):
            return None
        if display_qty >= total_qty:
            log.warning(
                "[P51] request_iceberg: display_qty=%.8f >= total_qty=%.8f — "
                "using display_qty = total_qty / 10",
                display_qty, total_qty,
            )
            display_qty = total_qty / 10.0

        _ttl = float(ttl_secs) if ttl_secs is not None else P51_ICEBERG_DEFAULT_TTL_SECS
        req_id = uuid.uuid4().hex[:16]

        msg: Dict[str, Any] = {
            "method":      "iceberg_request",
            "request_id":  req_id,
            "symbol":      str(symbol).upper().strip(),
            "side":        str(side).lower().strip(),
            "total_qty":   round(float(total_qty),   8),
            "display_qty": round(float(display_qty), 8),
            "price_floor": round(max(0.0, float(price_floor)), 8),
            "price_ceil":  round(max(0.0, float(price_ceil)),  8),
            "ttl_secs":    round(_ttl, 2),
            "ts_us":       int(time.time() * 1_000_000),
        }
        try:
            _fut51i = asyncio.get_event_loop().create_future()
            self._p51_pending[req_id] = _fut51i
            self._p51_active_algos[req_id] = {
                "symbol":      msg["symbol"],
                "algo":        "iceberg",
                "side":        msg["side"],
                "total_qty":   float(total_qty),
                "fill_qty":    0.0,
                "slices_done": 0,
                "price_floor": float(price_floor),
                "price_ceil":  float(price_ceil),
                "sent_ts":     time.time(),
                "shadow_mode": P51_SHADOW_MODE,
            }
            await self._send(msg)
            self._p51_request_count += 1
            log.info(
                "[P51] iceberg_request sent: %s %s qty=%.8f display=%.8f ttl=%.0fs",
                msg["side"].upper(), msg["symbol"],
                float(total_qty), float(display_qty), _ttl,
            )
            return req_id

        except BridgeNotConnectedError:
            self._p51_pending.pop(req_id, None)
            self._p51_active_algos.pop(req_id, None)
            self._p51_fallback_count += 1
            return None
        except Exception as exc:
            self._p51_pending.pop(req_id, None)
            self._p51_active_algos.pop(req_id, None)
            self._p51_fallback_count += 1
            log.debug("[P51] request_iceberg error: %s", exc)
            return None

    async def wait_algo_complete(
        self,
        request_id: str,
        timeout_secs: float = None,
    ) -> Optional[dict]:
        """[P51] Await the algo_complete event for a previously sent algo request.

        Must be called after request_twap() or request_iceberg().
        Blocks until Rust fires algo_complete or the timeout expires.

        Parameters
        ----------
        request_id   : The hex ID returned by request_twap / request_iceberg.
        timeout_secs : Max wait (default P51_WAIT_TIMEOUT_SECS = 600s).

        Returns
        -------
        dict : algo_complete snapshot (total_filled, avg_px, slippage_bps, reason…)
        None : on timeout, bridge disconnect, or any error.

        Never raises.  On failure, callers MUST fall back to P32/P9 engines.
        """
        _timeout = float(timeout_secs) if timeout_secs is not None else P51_WAIT_TIMEOUT_SECS
        fut51 = self._p51_pending.get(request_id)
        if fut51 is None:
            log.warning("[P51] wait_algo_complete: no pending future for %r", request_id)
            return None
        try:
            result = await asyncio.wait_for(asyncio.shield(fut51), timeout=_timeout)
            return result
        except asyncio.TimeoutError:
            self._p51_pending.pop(request_id, None)
            self._p51_active_algos.pop(request_id, None)
            self._p51_fallback_count += 1
            log.warning(
                "[P51] wait_algo_complete timeout (%.0fs) for req=%r — Python fallback.",
                _timeout, request_id,
            )
            # Best-effort cancel: tell Rust to abort the in-flight algo
            asyncio.ensure_future(self.cancel_algo(request_id, "python_timeout"))
            return None
        except BridgeNotConnectedError:
            self._p51_pending.pop(request_id, None)
            self._p51_active_algos.pop(request_id, None)
            self._p51_fallback_count += 1
            return None
        except Exception as exc:
            self._p51_pending.pop(request_id, None)
            self._p51_active_algos.pop(request_id, None)
            self._p51_fallback_count += 1
            log.debug("[P51] wait_algo_complete error: %s", exc)
            return None

    async def cancel_algo(
        self,
        request_id: str,
        reason:     str = "",
    ) -> None:
        """[P51] KILL SWITCH: Cancel an in-flight TWAP/VWAP/Iceberg algo.

        Python retains absolute authority over all Rust algo execution.
        Rust MUST immediately halt all child-order placement for this
        request_id, cancel any open child orders, and fire algo_complete
        with reason="cancelled".

        Safe to call:
          - When already completed (Rust ignores unknown request_ids).
          - When bridge is disconnected (logs warning, local state cleared).
          - Multiple times (idempotent by request_id).

        Parameters
        ----------
        request_id : The hex ID returned by request_twap / request_iceberg.
        reason     : Optional label for audit trail (e.g. "signal_reversal").
        """
        try:
            await self._send({
                "method":     "cancel_algo",
                "request_id": str(request_id),
                "reason":     str(reason) if reason else "operator_cancel",
                "ts_us":      int(time.time() * 1_000_000),
            })
            log.warning("[P51] KILL-SWITCH: cancel_algo(%r) reason=%r", request_id, reason)
        except BridgeNotConnectedError:
            log.warning(
                "[P51] KILL-SWITCH: bridge not connected for req=%r — "
                "local state cleared; Rust TTL will expire naturally.", request_id,
            )
        except Exception as exc:
            log.error("[P51] cancel_algo error: %s", exc)
        finally:
            # Always clear local state regardless of wire success
            self._p51_active_algos.pop(request_id, None)
            # Resolve the pending future with cancellation error so callers unblock
            _f51_cancel = self._p51_pending.pop(request_id, None)
            if _f51_cancel is not None and not _f51_cancel.done():
                _f51_cancel.set_exception(
                    asyncio.CancelledError(f"cancel_algo({request_id!r})")
                )

    def record_p51_shadow_agreement(
        self,
        request_id:   str,
        python_avg_px: float,
        rust_avg_px:   float,
        symbol:        str = "",
    ) -> bool:
        """[P51] Compare Python P32/P9 result against Rust algo_complete.

        Called in shadow mode after the Python engine finishes AND the
        Rust algo_complete has been received (both paths executed independently).

        Returns True if avg_px values agreed within P50_AGREE_BPS tolerance
        (reuses the P50 agreement threshold for consistency).
        """
        import math as _ms
        if not (_ms.isfinite(python_avg_px) and python_avg_px > 0 and
                _ms.isfinite(rust_avg_px)   and rust_avg_px   > 0):
            return False
        diff_bps = abs(python_avg_px - rust_avg_px) / python_avg_px * 10_000.0
        agreed   = diff_bps <= P50_AGREE_BPS     # reuse P50 threshold

        if agreed:
            self._p51_agree_count += 1
        else:
            self._p51_disagree_count += 1
            log.debug(
                "[P51-SHADOW] avg_px divergence: py=%.8f rust=%.8f diff=%.2fbps sym=%s",
                python_avg_px, rust_avg_px, diff_bps, symbol,
            )
        _shadow_snap = {
            "request_id":    request_id,
            "symbol":        symbol,
            "python_avg_px": round(python_avg_px, 8),
            "rust_avg_px":   round(rust_avg_px,   8),
            "diff_bps":      round(diff_bps, 4),
            "agree":         agreed,
            "ts":            time.time(),
        }
        self._p51_shadow_results.append(_shadow_snap)
        return agreed

    def p51_snapshot(self) -> dict:
        """[P51] Status snapshot for trader_status.json / dashboard.

        Keys
        ----
        enabled           : bool   — P51_ENABLE flag
        shadow_mode       : bool   — P51_SHADOW_MODE
        bridge_connected  : bool   — IPC link live
        twap_vwap_cap     : bool   — "twap_vwap" ∈ _p49_caps
        force_mode        : bool   — P51_FORCE
        active_algo_count : int    — currently running algo count
        active_algos      : dict   — {req_id → {symbol, algo, side, fill_qty/total_qty…}}
        request_count     : int    — lifetime algo requests sent
        fill_count        : int    — lifetime twap_slice_fill events received
        complete_count    : int    — lifetime algo_complete events received
        fallback_count    : int    — times Python fell back (timeout/cap absent)
        agree_count       : int    — shadow rounds that agreed
        disagree_count    : int    — shadow rounds that diverged
        agreement_rate    : float  — agree / (agree+disagree), None if no data
        recent_completed  : list   — last up-to-5 algo_complete snapshots
        recent_fills      : list   — last up-to-10 twap_slice_fill snapshots
        version           : str    — "51.0"
        """
        now = time.time()
        total_shadow51 = self._p51_agree_count + self._p51_disagree_count
        agr_rate51     = (
            self._p51_agree_count / total_shadow51 if total_shadow51 > 0 else None
        )

        # Build active algo summary (progress %)
        active_out: Dict[str, Any] = {}
        for rid, entry in self._p51_active_algos.items():
            total_q  = entry.get("total_qty",  0) or 1
            fill_q   = entry.get("fill_qty",   0)
            age_s    = now - entry.get("sent_ts", now)
            active_out[rid] = {
                "symbol":      entry.get("symbol",  "?"),
                "algo":        entry.get("algo",    "?"),
                "side":        entry.get("side",    "?"),
                "fill_pct":    round(fill_q / total_q * 100, 1),
                "fill_qty":    round(fill_q,  8),
                "total_qty":   round(total_q, 8),
                "slices_done": entry.get("slices_done", 0),
                "age_secs":    round(age_s, 1),
                "shadow":      entry.get("shadow_mode", False),
            }

        return {
            "enabled":           P51_ENABLE,
            "shadow_mode":       P51_SHADOW_MODE,
            "bridge_connected":  self._connected,
            "twap_vwap_cap":     ("twap_vwap" in self._p49_caps),
            "force_mode":        P51_FORCE,
            "active_algo_count": len(self._p51_active_algos),
            "active_algos":      active_out,
            "request_count":     self._p51_request_count,
            "fill_count":        self._p51_fill_count,
            "complete_count":    self._p51_complete_count,
            "fallback_count":    self._p51_fallback_count,
            "agree_count":       self._p51_agree_count,
            "disagree_count":    self._p51_disagree_count,
            "agreement_rate":    round(agr_rate51, 4) if agr_rate51 is not None else None,
            "recent_completed":  list(self._p51_completed)[-5:],
            "recent_fills":      list(self._p51_slice_history)[-10:],
            "version":           P51_VERSION,
            "description":       "Python policy → Rust native TWAP/VWAP/Iceberg engine",
        }

    # ── [/P51] ────────────────────────────────────────────────────────────────

    # ═══════════════════════════════════════════════════════════════════════════
    # [P54] Kernel-Bypass Telemetry Snapshot
    # ═══════════════════════════════════════════════════════════════════════════

    def p54_snapshot(self) -> dict:
        """[P54] Current kernel-bypass telemetry for dashboard / status.json.

        Keys
        ────
        enabled          bool   — P54_ENABLE flag
        mode             str    — "dpdk" | "onload" | "standard" | "unknown"
        rx_drops         int    — lifetime receive packet drops
        tx_drops         int    — lifetime transmit drops
        total_drops      int    — rx_drops + tx_drops
        ring_util_pct    float  — ring buffer utilization (0–100 %)
        lat_p50_us       float  — median latency (µs)
        lat_p95_us       float  — 95th percentile latency (µs)
        lat_p99_us       float  — 99th percentile latency (µs)
        zero_copy        bool   — zero-copy path active
        last_ts          float  — wall-clock of last p54_stats event
        event_count      int    — total p54_stats events received
        age_secs         float  — seconds since last p54_stats event
        recent_history   list   — last 10 snapshots (newest first)
        alerts           dict   — {drops: bool, ring: bool, lat_p99: bool}
        version          str    — "54.0"
        """
        if not P54_ENABLE:
            return {"enabled": False, "version": P54_VERSION}

        _now  = time.time()
        _age  = round(_now - self._p54_last_ts, 2) if self._p54_last_ts > 0 else None
        _total_drops = self._p54_rx_drops + self._p54_tx_drops

        return {
            "enabled":       True,
            "mode":          self._p54_mode,
            "rx_drops":      self._p54_rx_drops,
            "tx_drops":      self._p54_tx_drops,
            "total_drops":   _total_drops,
            "ring_util_pct": round(self._p54_ring_util,  2),
            "lat_p50_us":    round(self._p54_lat_p50_us, 2),
            "lat_p95_us":    round(self._p54_lat_p95_us, 2),
            "lat_p99_us":    round(self._p54_lat_p99_us, 2),
            "zero_copy":     self._p54_zero_copy,
            "last_ts":       round(self._p54_last_ts, 3),
            "event_count":   self._p54_event_count,
            "age_secs":      _age,
            "recent_history": list(reversed(list(self._p54_history)))[:10],
            "alerts": {
                "drops":   _total_drops > P54_DROP_ALERT_THRESH,
                "ring":    self._p54_ring_util   > P54_RING_ALERT_PCT,
                "lat_p99": self._p54_lat_p99_us  > P54_LAT_P99_ALERT_US,
            },
            "version": P54_VERSION,
        }

    # ── [/P54] ────────────────────────────────────────────────────────────────

    def _ensure_bridge_running(self) -> None:
        """
        Launch the Rust bridge binary if it is not already running.

        Robust one-command behavior:
          - Resolve an absolute executable path (env override supported)
          - Spawn with a correct working directory
          - Fail fast if the executable is missing
          - Health-check that 127.0.0.1:PORT is listening before returning
        """
        # Allow explicit disable
        auto = os.environ.get("OKX_BRIDGE_AUTO_START", os.environ.get("OKX_BRIDGE_AUTO_LAUNCH", "")).strip()
        if auto in ("0", "false", "False", "no", "NO"):
            log.info("[BRIDGE] Auto-launch disabled via OKX_BRIDGE_AUTO_START=0")
            return

        # If we already spawned and it's still alive, don't respawn.
        try:
            if getattr(self, "_bridge_proc", None) is not None and self._bridge_proc.poll() is None:
                return
        except Exception as _exc:
            log.debug("[BRIDGE] data skip: %s", _exc)

        project_dir = Path(__file__).resolve().parent

        # Prefer explicit env overrides (keep compatibility with earlier names).
        raw = (
            os.environ.get("OKX_BRIDGE_BINARY_PATH", "")
            or os.environ.get("BRIDGE_BINARY", "")
            or os.environ.get("OKX_BRIDGE_BINARY", "")
        ).strip()

        def _resolve_binary(candidate: str) -> Path | None:
            if not candidate:
                return None
            # Expand relative paths against project directory.
            c = Path(candidate).expanduser()
            if not c.is_absolute():
                c = (project_dir / c).resolve()

            # If a directory is given, assume cargo layout inside it.
            if c.exists() and c.is_dir():
                # okx_bridge/target/release/okx_bridge(.exe)
                for rel in (
                    Path("target/release/okx_bridge.exe"),
                    Path("target/release/okx_bridge"),
                ):
                    pth = (c / rel)
                    if pth.is_file():
                        return pth
                return None

            # If a file is given without .exe on Windows, also try .exe.
            if not c.exists():
                if os.name == "nt" and c.suffix.lower() != ".exe":
                    c_exe = Path(str(c) + ".exe")
                    if c_exe.is_file():
                        return c_exe
                return None

            return c if c.is_file() else None

        binary_path = _resolve_binary(raw)

        # If no env override, probe common locations relative to project dir.
        if binary_path is None:
            candidates = [
                # If project contains okx_bridge/ (cargo project)
                project_dir / "okx_bridge" / "target" / "release" / "okx_bridge.exe",
                project_dir / "okx_bridge" / "target" / "release" / "okx_bridge",
                # If running from inside okx_bridge/ itself (less common)
                project_dir / "target" / "release" / "okx_bridge.exe",
                project_dir / "target" / "release" / "okx_bridge",
                # A plain okx_bridge.exe next to this file
                project_dir / "okx_bridge.exe",
                project_dir / "okx_bridge",
            ]
            for c in candidates:
                if c.is_file():
                    binary_path = c
                    break

        if binary_path is None:
            log.error(
                "[BRIDGE] Rust binary not found. Set OKX_BRIDGE_BINARY_PATH to "
                "an absolute path to okx_bridge.exe (Windows) or okx_bridge (Linux/macOS). "
                "Expected build output: okx_bridge/target/release/okx_bridge(.exe)"
            )
            return

        binary_path = binary_path.resolve()
        # Prefer running with cwd at the cargo project dir if possible.
        cwd = binary_path.parent
        try:
            # .../okx_bridge/target/release/okx_bridge.exe -> cwd .../okx_bridge
            if binary_path.parent.name.lower() == "release" and binary_path.parent.parent.name.lower() == "target":
                cwd = binary_path.parent.parent.parent
        except Exception as _exc:
            log.warning("[BRIDGE] suppressed: %s", _exc)

        if not binary_path.is_file():
            log.error("[BRIDGE] Bridge executable not found at resolved path: %s", str(binary_path))
            return

        log.info("[BRIDGE] Auto-launching Rust bridge: %s (cwd=%s)", str(binary_path), str(cwd))

        try:
            self._bridge_proc = subprocess.Popen(
                [str(binary_path)],
                cwd=str(cwd),
                stdout=sys.stdout,
                stderr=sys.stderr,
                env={**os.environ},
            )
        except (FileNotFoundError, PermissionError, OSError) as e:
            log.error("[BRIDGE] Failed to start Rust bridge (%s): %s", str(binary_path), e)
            return

        pid = getattr(self._bridge_proc, "pid", None)
        if pid:
            log.info("[BRIDGE] Rust bridge PID=%d started", pid)

        # Health-check: wait for listener. Fail fast if the process exits.
        deadline = time.time() + 20.0
        delay = 0.25
        while time.time() < deadline:
            try:
                if self._bridge_proc.poll() is not None:
                    log.error("[BRIDGE] Rust bridge exited early (code=%s).", self._bridge_proc.returncode)
                    return
            except Exception as _exc:
                log.warning("[BRIDGE] suppressed: %s", _exc)

            try:
                with socket.create_connection((self._host, int(self._port)), timeout=0.5):
                    log.info("[BRIDGE] Rust bridge is listening on %s:%s", self._host, self._port)
                    return
            except OSError:
                time.sleep(delay)
                delay = min(delay * 1.5, 2.0)

        log.error("[BRIDGE] Rust bridge did not start listening on %s:%s within timeout.", self._host, self._port)
        try:
            if self._bridge_proc and self._bridge_proc.poll() is None:
                self._bridge_proc.terminate()
        except Exception as _exc:
            log.warning("[BRIDGE] suppressed: %s", _exc)

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