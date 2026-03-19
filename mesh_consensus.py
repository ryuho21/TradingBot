"""
mesh_consensus.py  —  Phase 53 PowerTrader AI  ·  Multi-Node Consensus Engine
══════════════════════════════════════════════════════════════════════════════════

Phase 53 ARCHITECTURAL ROLE  ("Sentinel Web")
──────────────────────────────────────────────
This module implements the PYTHON-SIDE mesh consensus layer.  It is the
single authoritative source for cross-exchange signal aggregation and
2-of-3 consensus entry gating.

Architecture
────────────
  ┌─────────────────────────────────────────────────────────────┐
  │  OKX Bridge (Rust)     →  OKXNode    ─┐                     │
  │  Binance Tape WS       →  BinanceNode─┼─► MeshConsensusGate │
  │  Coinbase Oracle WS    →  CoinbaseNode┘       2/3 vote       │
  └─────────────────────────────────────────────────────────────┘
       ↓ approved ConsensusResult ↓
    OrchestratorGate pre-trade check (engine_supervisor.py)

Message Signing (Anti-Replay / Anti-Spoofing)
──────────────────────────────────────────────
Each node vote is wrapped in a MeshEnvelope:
  HMAC-SHA256(P53_MESH_SECRET, "<version>|<node_id>|<seq>|<clock_ts_ms>|<payload_hash>")

Clock Synchronisation
─────────────────────
MeshClockSync tracks a per-node NTP offset (ms).  Python-side NTP measurement
is lightweight (stdlib socket UDP).  PTP-ready hooks are exposed for Phase 54.

Consensus Rule
──────────────
  APPROVED  ← at least 2 of 3 nodes have:
                • same direction  (long | short | neutral)
                • vpin_toxicity  < P53_VPIN_TOXICITY_THRESHOLD (default 0.6)
                • vote_age       < P53_CONSENSUS_WINDOW_SECS   (default 10 s)
                • seq is valid (anti-replay window check)

  BLOCKED   ← fewer than 2 qualifying votes; reason written to ConsensusResult

Zero-Drift Preserved
──────────────────────
  Module is PURE Python (stdlib only + hmac/hashlib).
  No new pip dependencies.
  Backward-safe: if P53_MESH_ENABLED=0 the gate returns approved=True on every call.
  Status key p53_mesh is ALWAYS written (fail-closed: {"enabled": False}).

Public API  (consumed by engine_supervisor.py)
──────────────────────────────────────────────
  MeshConsensusGate(symbols, enabled)
    .submit_vote(exchange, symbol, direction, vpin_toxicity, confidence, bridge_ts)
    .check_consensus(symbol, proposed_direction) → ConsensusResult
    .status_snapshot()                          → dict  (written to p53_mesh)
    async .start()
    async .stop()
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import socket
import struct
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

log = logging.getLogger("mesh_consensus_p53")
logging.getLogger("mesh_consensus_p53").addHandler(logging.NullHandler())
from pt_utils import _env_float, _env_int, atomic_write_json  # [P0-UTIL]

# ── Version + schema ──────────────────────────────────────────────────────────
P53_SCHEMA_VERSION = "53.0"

# ── Node identifiers ─────────────────────────────────────────────────────────
NODE_OKX      = "okx_primary"
NODE_BINANCE  = "binance_primary"
NODE_COINBASE = "coinbase_oracle"

# Canonical exchange names (for display + serialisation)
EXCHANGE_NAMES: Dict[str, str] = {
    NODE_OKX:      "OKX",
    NODE_BINANCE:  "BINANCE",
    NODE_COINBASE: "COINBASE",
}

# Regional assignment (latency routing metadata — Phase 54+)
EXCHANGE_REGIONS: Dict[str, str] = {
    NODE_OKX:      "AP",   # Asia-Pacific primary
    NODE_BINANCE:  "AP",   # Asia-Pacific secondary
    NODE_COINBASE: "US",   # US primary
}

ALL_NODES = [NODE_OKX, NODE_BINANCE, NODE_COINBASE]

# [P0-FIX-11] env helpers → pt_utils



def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key, "").strip().lower()
    if raw == "":
        return default
    return raw in ("1", "true", "yes", "on")


# ── Configuration constants ───────────────────────────────────────────────────
# Gate enabled flag — set P53_MESH_ENABLED=0 to bypass (backward-compatible).
P53_MESH_ENABLED: bool = _env_bool("P53_MESH_ENABLED", True)

# Minimum agreements for APPROVED (2-of-3 consensus).
P53_QUORUM: int = _env_int("P53_CONSENSUS_QUORUM", 2)

# Maximum vote age — votes older than this are expired before each check.
P53_CONSENSUS_WINDOW_SECS: float = _env_float("P53_CONSENSUS_WINDOW_SECS", 10.0)

# VPIN toxicity ceiling — votes from nodes with toxicity >= threshold are excluded.
P53_VPIN_TOXICITY_THRESHOLD: float = _env_float("P53_VPIN_TOXICITY_THRESHOLD", 0.60)

# Minimum confidence for a vote to count.
P53_MIN_VOTE_CONFIDENCE: float = _env_float("P53_MIN_VOTE_CONFIDENCE", 0.50)

# Anti-replay seq window — reject seqs more than this far behind the latest seen.
P53_ANTIREPLAY_WINDOW: int = _env_int("P53_ANTIREPLAY_WINDOW", 100)

# NTP sync interval (seconds).  Set 0 to disable background sync.
P53_NTP_SYNC_INTERVAL_SECS: float = _env_float("P53_NTP_SYNC_INTERVAL_SECS", 300.0)

# NTP server pool for clock offset measurement.
P53_NTP_HOST: str = os.environ.get("P53_NTP_HOST", "pool.ntp.org").strip()
P53_NTP_PORT: int = _env_int("P53_NTP_PORT", 123)

# ── [P54] PTP clock override for MeshClockSync ────────────────────────────────
# P54_PTP_ENABLE=1  : MeshClockSync.ptp_apply_offset() becomes live (not no-op)
#                     The Rust bridge sends PTP-measured offsets via a future
#                     ptp_offset_update wire event; Python applies them here.
# P54_PTP_LOG_THRESH_US : log an INFO when the PTP offset shift exceeds this
P54_PTP_ENABLE:          bool  = _env_bool("P54_PTP_ENABLE",         False)
P54_PTP_LOG_THRESH_US:   float = _env_float("P54_PTP_LOG_THRESH_US", 100.0)
# ── [/P54-ptp-config] ─────────────────────────────────────────────────────────

# HMAC signing key.  If absent, signing is disabled (warn but don't crash).
_RAW_SECRET: str = os.environ.get("P53_MESH_SECRET", "").strip()
P53_MESH_SECRET: Optional[bytes] = _RAW_SECRET.encode("utf-8") if _RAW_SECRET else None

# Status snapshot history depth.
_CONSENSUS_HISTORY_MAXLEN: int = 50


# ══════════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class MeshEnvelope:
    """
    [P53-SCHEMA] Versioned, signed, seq-numbered vote envelope.

    canonical_body := f"{schema_version}|{node_id}|{seq}|{clock_ts_ms}|{payload_hash}"
    hmac_sha256    := HMAC-SHA256(P53_MESH_SECRET, canonical_body)

    If P53_MESH_SECRET is absent the envelope is produced without signing
    (hmac_sig = None) and validation skips HMAC checking.
    """
    schema_version: str
    node_id:        str
    exchange:       str
    region:         str
    seq:            int
    clock_ts:       float          # Python wall clock + NTP offset (epoch seconds)
    ntp_offset_ms:  float          # Latest measured NTP offset for this node
    payload_type:   str            # "vote" | "vpin_update" | "heartbeat"
    payload:        dict
    hmac_sig:       Optional[str]  # hex-encoded HMAC-SHA256 or None

    # ── Serialisation ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "node_id":        self.node_id,
            "exchange":       self.exchange,
            "region":         self.region,
            "seq":            self.seq,
            "clock_ts":       round(self.clock_ts, 6),
            "ntp_offset_ms":  round(self.ntp_offset_ms, 3),
            "payload_type":   self.payload_type,
            "payload":        self.payload,
            "hmac_sig":       self.hmac_sig,
        }

    # ── Canonical body for HMAC ───────────────────────────────────────────────

    @staticmethod
    def _payload_hash(payload: dict) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def compute_canonical_body(
        schema_version: str,
        node_id:        str,
        seq:            int,
        clock_ts:       float,
        payload:        dict,
    ) -> str:
        clock_ts_ms = int(round(clock_ts * 1000))
        ph          = MeshEnvelope._payload_hash(payload)
        return f"{schema_version}|{node_id}|{seq}|{clock_ts_ms}|{ph}"

    # ── Sign ─────────────────────────────────────────────────────────────────

    @staticmethod
    def sign(body: str, secret: bytes) -> str:
        return hmac.new(secret, body.encode("utf-8"), hashlib.sha256).hexdigest()

    # ── Verify ───────────────────────────────────────────────────────────────

    def verify(self, secret: Optional[bytes]) -> bool:
        """
        Returns True if:
          • secret is None → signing disabled; always valid.
          • self.hmac_sig matches expected HMAC given secret.
        """
        if secret is None:
            return True
        if self.hmac_sig is None:
            return False
        body     = self.compute_canonical_body(
            self.schema_version, self.node_id, self.seq, self.clock_ts, self.payload
        )
        expected = self.sign(body, secret)
        return hmac.compare_digest(expected, self.hmac_sig)


@dataclass
class ConsensusVote:
    """
    [P53] A single node's opinion on a symbol at a point in time.
    Produced by MeshNode.make_vote() and stored in MeshConsensusGate._votes.
    """
    node_id:        str
    exchange:       str
    symbol:         str
    direction:      str    # "long" | "short" | "neutral"
    vpin_toxicity:  float  # 0.0–1.0; lower = cleaner flow
    confidence:     float  # 0.0–1.0
    vote_ts:        float  # wall clock + NTP offset (epoch seconds)
    seq:            int    # monotonic per-node counter
    hmac_sig:       Optional[str]

    @property
    def age_secs(self) -> float:
        return time.time() - self.vote_ts

    @property
    def is_fresh(self) -> bool:
        return self.age_secs <= P53_CONSENSUS_WINDOW_SECS

    @property
    def is_clean(self) -> bool:
        return (
            self.vpin_toxicity < P53_VPIN_TOXICITY_THRESHOLD
            and self.confidence  >= P53_MIN_VOTE_CONFIDENCE
        )


@dataclass
class ConsensusResult:
    """
    [P53] Result of a 2/3 consensus check.
    Consumed by OrchestratorGate in engine_supervisor._gui_bridge.
    """
    approved:         bool
    symbol:           str
    proposed_dir:     str
    agreeing_nodes:   List[str]          # node_ids that agreed
    dissenting_nodes: List[str]          # node_ids that dissented or were stale
    blocking_reason:  Optional[str]      # set when approved=False
    confidence:       float              # mean confidence of agreeing nodes
    min_toxicity:     float              # lowest toxicity among agreeing nodes
    max_toxicity:     float              # highest toxicity among agreeing nodes
    latency_us:       float              # micros from gate entry to result
    check_ts:         float              # epoch of this check


# ══════════════════════════════════════════════════════════════════════════════
# CLOCK SYNC — NTP OFFSET MEASUREMENT
# ══════════════════════════════════════════════════════════════════════════════

class MeshClockSync:
    """
    [P53-CLOCK] Lightweight NTP offset tracker.

    Sends a single RFC-4330 NTP v4 request to P53_NTP_HOST and measures
    the round-trip to compute an approximate clock offset.

    PTP hooks (Phase 54+) are exposed as override points.

    Thread-safe for asyncio: all mutations happen inside run_in_executor calls.
    """

    # NTP epoch offset: 1900-01-01 to 1970-01-01 in seconds.
    _NTP_EPOCH_DELTA: int = 2_208_988_800

    def __init__(
        self,
        host:          str   = P53_NTP_HOST,
        port:          int   = P53_NTP_PORT,
        timeout_secs:  float = 2.0,
    ) -> None:
        self._host         = host
        self._port         = port
        self._timeout      = timeout_secs
        self._offset_ms:       float = 0.0   # NTP clock offset (ms)
        # [P54] PTP tracking fields
        self._ptp_offset_ns:   int   = 0     # last PTP nanosecond offset applied
        self._ptp_apply_count: int   = 0     # total ptp_apply_offset() calls
        self._last_sync_ts: float = 0.0
        self._sync_count:   int   = 0
        self._error_count:  int   = 0
        self._last_error:   str   = ""

    @property
    def offset_ms(self) -> float:
        """Current clock offset in milliseconds (positive = local clock is ahead)."""
        return self._offset_ms

    @property
    def adjusted_time(self) -> float:
        """Wall clock corrected for NTP offset."""
        return time.time() - (self._offset_ms / 1000.0)

    def _measure_offset_sync(self) -> float:
        """
        Synchronous NTP measurement (runs in executor).
        Returns offset_ms.  Raises on failure.
        """
        # RFC-4330 48-byte NTP v4 client request
        packet = b"\x1b" + b"\x00" * 47

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(self._timeout)
        try:
            t1 = time.time()
            sock.sendto(packet, (self._host, self._port))
            data, _ = sock.recvfrom(1024)
            t4 = time.time()
        finally:
            sock.close()

        if len(data) < 48:
            raise ValueError(f"NTP response too short: {len(data)} bytes")

        # Transmit Timestamp (bytes 40-47): seconds since NTP epoch
        tx_sec, tx_frac = struct.unpack("!II", data[40:48])
        t3 = (tx_sec - self._NTP_EPOCH_DELTA) + (tx_frac / 2**32)

        # NTP clock offset approximation: ((t2-t1) + (t3-t4)) / 2
        # We don't have t2 from a single packet; approximate as t3 ≈ mid-point.
        rtt_half    = (t4 - t1) / 2.0
        offset_secs = t3 - t1 - rtt_half
        return offset_secs * 1000.0   # → ms

    async def sync(self) -> None:
        """Asynchronous NTP sync — runs measurement in a thread executor."""
        loop = asyncio.get_event_loop()
        try:
            offset_ms = await asyncio.wait_for(
                loop.run_in_executor(None, self._measure_offset_sync),
                timeout=self._timeout + 1.0,
            )
            self._offset_ms    = offset_ms
            self._last_sync_ts = time.time()
            self._sync_count  += 1
            log.debug(
                "[P53-CLOCK] NTP sync #%d: offset=%.3f ms (host=%s)",
                self._sync_count, offset_ms, self._host,
            )
        except Exception as exc:
            self._error_count += 1
            self._last_error   = str(exc)
            log.debug("[P53-CLOCK] NTP sync error (non-fatal): %s", exc)

    def status_snapshot(self) -> dict:
        return {
            "offset_ms":        round(self._offset_ms, 3),
            "last_sync_ts":     round(self._last_sync_ts, 3),
            "sync_count":       self._sync_count,
            "error_count":      self._error_count,
            "last_error":       self._last_error,
            "ntp_host":         self._host,
            # [P54] PTP fields — zero when PTP not yet applied
            "ptp_enable":       P54_PTP_ENABLE,
            "ptp_offset_ns":    self._ptp_offset_ns,
            "ptp_apply_count":  self._ptp_apply_count,
        }

    # ── Phase 54+ PTP override hooks (no-op stubs) ────────────────────────────

    def ptp_apply_offset(self, offset_ns: int) -> None:
        """[P54] Apply a PTP-measured nanosecond clock offset.

        When P54_PTP_ENABLE=1 this method is live: it overrides the NTP-derived
        _offset_ms with the higher-precision PTP measurement from the Rust bridge.
        When P54_PTP_ENABLE=0 (default) the call is accepted but logged at DEBUG
        so callers always succeed without raising.

        Parameters
        ----------
        offset_ns   PTP clock offset in nanoseconds (may be negative)
        """
        _new_ms = offset_ns / 1_000_000.0
        if P54_PTP_ENABLE:
            _prev_ms   = self._offset_ms
            _delta_us  = abs(_new_ms - _prev_ms) * 1_000.0
            self._offset_ms       = _new_ms
            self._ptp_offset_ns   = offset_ns
            self._ptp_apply_count += 1
            import logging as _log54
            _log = _log54.getLogger(__name__)
            if _delta_us >= P54_PTP_LOG_THRESH_US:
                _log.info(
                    "[P54-PTP] Clock offset updated: %.3fms → %.3fms "
                    "(delta=%.1fµs apply#%d)",
                    _prev_ms, _new_ms, _delta_us, self._ptp_apply_count,
                )
            else:
                _log.debug(
                    "[P54-PTP] ptp_apply_offset: %.3fms (delta=%.1fµs)",
                    _new_ms, _delta_us,
                )
        else:
            import logging as _log54p
            _log54p.getLogger(__name__).debug(
                "[P54-PTP] ptp_apply_offset ignored (P54_PTP_ENABLE=0): %.3fms",
                _new_ms,
            )


# ══════════════════════════════════════════════════════════════════════════════
# ANTI-REPLAY GUARD
# ══════════════════════════════════════════════════════════════════════════════

class AntiReplayGuard:
    """
    [P53-REPLAY] Per-node sliding-window sequence number validator.

    Maintains the highest seq seen per node and a set of recent accepted seqs
    within P53_ANTIREPLAY_WINDOW.  A seq is rejected if:
      • It is <= (max_seen - P53_ANTIREPLAY_WINDOW)  — too old, likely replay
      • It has been seen before within the window     — exact duplicate

    Thread-safe for single-threaded asyncio event loop.
    """

    def __init__(self, window: int = P53_ANTIREPLAY_WINDOW) -> None:
        self._window:   int            = window
        self._max_seen: Dict[str, int] = defaultdict(lambda: -1)
        self._seen_set: Dict[str, set] = defaultdict(set)
        self._rejected: int            = 0
        self._accepted: int            = 0

    def validate(self, node_id: str, seq: int) -> Tuple[bool, str]:
        """
        Returns (ok, reason).  ok=True means the seq is valid and has been
        recorded.  ok=False means it should be rejected.
        """
        max_seen = self._max_seen[node_id]
        floor    = max_seen - self._window

        if seq <= floor:
            self._rejected += 1
            return False, f"seq {seq} too old (floor={floor}, max={max_seen})"

        if seq in self._seen_set[node_id]:
            self._rejected += 1
            return False, f"seq {seq} already seen (duplicate/replay)"

        # Accept
        self._seen_set[node_id].add(seq)
        if seq > max_seen:
            self._max_seen[node_id] = seq
            # Prune old seqs from the set
            stale = {s for s in self._seen_set[node_id] if s <= max_seen - self._window}
            self._seen_set[node_id] -= stale

        self._accepted += 1
        return True, "ok"

    def status_snapshot(self) -> dict:
        return {
            "accepted":  self._accepted,
            "rejected":  self._rejected,
            "max_seen":  dict(self._max_seen),
        }


# ══════════════════════════════════════════════════════════════════════════════
# MESH NODE  —  per-exchange node state tracker
# ══════════════════════════════════════════════════════════════════════════════

class MeshNode:
    """
    [P53] Represents one exchange's contribution to the consensus mesh.

    Owns:
      • A per-symbol vote buffer (latest vote per symbol).
      • A monotonic seq counter for envelope signing.
      • Liveness tracking (last seen ts, heartbeat age).

    make_vote() produces a ConsensusVote with HMAC-signed MeshEnvelope.
    """

    def __init__(
        self,
        node_id:    str,
        clock_sync: MeshClockSync,
        replay_guard: AntiReplayGuard,
    ) -> None:
        self._node_id      = node_id
        self._exchange     = EXCHANGE_NAMES.get(node_id, node_id)
        self._region       = EXCHANGE_REGIONS.get(node_id, "??")
        self._clock        = clock_sync
        self._replay       = replay_guard
        self._seq:         int   = 0
        self._last_seen:   float = 0.0
        self._votes: Dict[str, ConsensusVote] = {}   # symbol → latest vote

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def exchange(self) -> str:
        return self._exchange

    @property
    def is_alive(self) -> bool:
        """Node is alive if we received a vote within 2× consensus window."""
        return (time.time() - self._last_seen) < (P53_CONSENSUS_WINDOW_SECS * 2)

    @property
    def age_secs(self) -> float:
        return time.time() - self._last_seen if self._last_seen > 0 else float("inf")

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _make_envelope(
        self,
        payload_type: str,
        payload:      dict,
    ) -> MeshEnvelope:
        seq      = self._next_seq()
        clock_ts = self._clock.adjusted_time
        body     = MeshEnvelope.compute_canonical_body(
            P53_SCHEMA_VERSION, self._node_id, seq, clock_ts, payload
        )
        sig      = MeshEnvelope.sign(body, P53_MESH_SECRET) if P53_MESH_SECRET else None
        return MeshEnvelope(
            schema_version = P53_SCHEMA_VERSION,
            node_id        = self._node_id,
            exchange       = self._exchange,
            region         = self._region,
            seq            = seq,
            clock_ts       = clock_ts,
            ntp_offset_ms  = self._clock.offset_ms,
            payload_type   = payload_type,
            payload        = payload,
            hmac_sig       = sig,
        )

    def submit(
        self,
        symbol:        str,
        direction:     str,
        vpin_toxicity: float,
        confidence:    float,
    ) -> Optional[ConsensusVote]:
        """
        Record a new vote from this node.  Builds and validates a MeshEnvelope
        for anti-replay + signature.  Returns None if rejected.
        """
        payload = {
            "symbol":        symbol,
            "direction":     direction,
            "vpin_toxicity": round(float(vpin_toxicity), 6),
            "confidence":    round(float(confidence),    6),
        }
        env = self._make_envelope("vote", payload)

        # Anti-replay check
        ok, reason = self._replay.validate(self._node_id, env.seq)
        if not ok:
            log.warning("[P53-NODE] %s seq rejected: %s", self._node_id, reason)
            return None

        # Signature verification
        if not env.verify(P53_MESH_SECRET):
            log.warning("[P53-NODE] %s envelope signature FAILED — dropping vote.", self._node_id)
            return None

        vote = ConsensusVote(
            node_id       = self._node_id,
            exchange      = self._exchange,
            symbol        = symbol,
            direction     = direction,
            vpin_toxicity = float(vpin_toxicity),
            confidence    = float(confidence),
            vote_ts       = env.clock_ts,
            seq           = env.seq,
            hmac_sig      = env.hmac_sig,
        )
        self._votes[symbol] = vote
        self._last_seen     = time.time()
        return vote

    def latest_vote(self, symbol: str) -> Optional[ConsensusVote]:
        return self._votes.get(symbol)

    def status_snapshot(self) -> dict:
        return {
            "node_id":    self._node_id,
            "exchange":   self._exchange,
            "region":     self._region,
            "alive":      self.is_alive,
            "age_secs":   round(self.age_secs, 1),
            "seq":        self._seq,
            "vote_count": len(self._votes),
        }


# ══════════════════════════════════════════════════════════════════════════════
# MESH CONSENSUS GATE
# ══════════════════════════════════════════════════════════════════════════════

class MeshConsensusGate:
    """
    [P53] Central 2-of-3 consensus gate.

    Maintains one MeshNode per exchange (OKX, Binance, Coinbase).
    Receives external signal feeds via submit_vote() and answers
    check_consensus() queries from OrchestratorGate before each trade.

    Design invariants:
      • If P53_MESH_ENABLED=False  → every check_consensus returns approved=True.
      • If fewer than P53_QUORUM nodes have fresh+clean votes → approved=False.
      • Fail-closed: any exception inside check_consensus → approved=False.
      • status_snapshot() never raises; always returns a safe dict.
    """

    def __init__(self, symbols: List[str], enabled: bool = P53_MESH_ENABLED) -> None:
        self._symbols  = list(symbols)
        self._enabled  = bool(enabled)
        self._clock    = MeshClockSync()
        self._replay   = AntiReplayGuard()

        # One node per exchange
        self._nodes: Dict[str, MeshNode] = {
            nid: MeshNode(nid, self._clock, self._replay)
            for nid in ALL_NODES
        }

        # Per-symbol consensus result history (for dashboard)
        self._history: Dict[str, Deque[dict]] = defaultdict(
            lambda: deque(maxlen=_CONSENSUS_HISTORY_MAXLEN)
        )

        # Aggregate stats
        self._total_checks:   int = 0
        self._approved_count: int = 0
        self._blocked_count:  int = 0
        self._boot_ts:        float = time.time()

        # Background NTP sync task
        self._ntp_task: Optional[asyncio.Task] = None
        self._running:  bool = False

        if not self._enabled:
            log.info("[P53] MeshConsensusGate DISABLED (P53_MESH_ENABLED=0) — pass-through mode.")
        else:
            _secret_status = "✓ signing active" if P53_MESH_SECRET else "⚠ no secret (P53_MESH_SECRET unset)"
            log.info(
                "[P53] MeshConsensusGate ACTIVE — quorum=%d  window=%.0fs  "
                "vpin_threshold=%.2f  %s",
                P53_QUORUM, P53_CONSENSUS_WINDOW_SECS,
                P53_VPIN_TOXICITY_THRESHOLD, _secret_status,
            )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Immediate first sync
        if self._enabled and P53_NTP_SYNC_INTERVAL_SECS > 0:
            await self._clock.sync()
            self._ntp_task = asyncio.create_task(
                self._ntp_sync_loop(), name="p53_ntp_sync"
            )
        log.info("[P53] MeshConsensusGate started.")

    async def stop(self) -> None:
        self._running = False
        if self._ntp_task and not self._ntp_task.done():
            self._ntp_task.cancel()
            try:
                await self._ntp_task
            except asyncio.CancelledError:
                pass
        log.info("[P53] MeshConsensusGate stopped.")

    async def _ntp_sync_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(P53_NTP_SYNC_INTERVAL_SECS)
                await self._clock.sync()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.debug("[P53-CLOCK] NTP sync loop error: %s", exc)

    # ── Vote ingestion ────────────────────────────────────────────────────────

    def submit_vote(
        self,
        exchange:      str,
        symbol:        str,
        direction:     str,
        vpin_toxicity: float,
        confidence:    float,
        bridge_ts:     Optional[float] = None,
    ) -> Optional[ConsensusVote]:
        """
        [P53] Receive a signal from a specific exchange and record it as a vote.

        exchange      : NODE_OKX | NODE_BINANCE | NODE_COINBASE  (or exchange name)
        symbol        : bare ticker e.g. "BTC"
        direction     : "long" | "short" | "neutral"
        vpin_toxicity : 0.0–1.0  (from bridge p47 for OKX; estimated for others)
        confidence    : 0.0–1.0
        bridge_ts     : optional bridge-side timestamp (for latency telemetry)
        """
        if not self._enabled:
            return None

        # Normalise exchange to node_id
        node_id = _exchange_to_node(exchange)
        node    = self._nodes.get(node_id)
        if node is None:
            log.debug("[P53] Unknown exchange/node_id=%r — ignoring vote.", exchange)
            return None

        direction = _normalise_direction(direction)
        symbol    = symbol.upper().replace("-USDT", "").replace("-SWAP", "")

        vote = node.submit(symbol, direction, vpin_toxicity, confidence)
        if vote is not None:
            log.debug(
                "[P53-VOTE] %s %s %s dir=%s vpin=%.3f conf=%.3f seq=%d",
                node.exchange, symbol, direction, direction,
                vpin_toxicity, confidence, vote.seq,
            )
        return vote

    # ── Consensus evaluation ─────────────────────────────────────────────────

    def check_consensus(
        self,
        symbol:           str,
        proposed_direction: str,
    ) -> ConsensusResult:
        """
        [P53] Evaluate whether 2/3 nodes agree on proposed_direction for symbol.

        This is the HOT PATH call — must be very fast (pure in-memory, no I/O).
        Exceptions are caught and converted to a fail-closed blocked result.
        """
        t0     = time.monotonic()
        symbol = symbol.upper().replace("-USDT", "").replace("-SWAP", "")
        pdir   = _normalise_direction(proposed_direction)
        now    = time.time()

        self._total_checks += 1

        # Pass-through when disabled
        if not self._enabled:
            return ConsensusResult(
                approved         = True,
                symbol           = symbol,
                proposed_dir     = pdir,
                agreeing_nodes   = [NODE_OKX, NODE_BINANCE, NODE_COINBASE],
                dissenting_nodes = [],
                blocking_reason  = None,
                confidence       = 1.0,
                min_toxicity     = 0.0,
                max_toxicity     = 0.0,
                latency_us       = (time.monotonic() - t0) * 1e6,
                check_ts         = now,
            )

        try:
            return self._evaluate(symbol, pdir, now, t0)
        except Exception as exc:
            log.warning("[P53] check_consensus exception (fail-closed): %s", exc)
            self._blocked_count += 1
            return ConsensusResult(
                approved         = False,
                symbol           = symbol,
                proposed_dir     = pdir,
                agreeing_nodes   = [],
                dissenting_nodes = list(ALL_NODES),
                blocking_reason  = f"gate_exception: {exc}",
                confidence       = 0.0,
                min_toxicity     = 1.0,
                max_toxicity     = 1.0,
                latency_us       = (time.monotonic() - t0) * 1e6,
                check_ts         = now,
            )

    def _evaluate(
        self, symbol: str, pdir: str, now: float, t0: float
    ) -> ConsensusResult:
        """[P53-CORE] Inner evaluation — called only by check_consensus()."""
        agreeing: List[ConsensusVote]   = []
        dissenting: List[str]           = []

        for node_id, node in self._nodes.items():
            vote = node.latest_vote(symbol)

            if vote is None:
                dissenting.append(node_id)
                log.debug("[P53] %s: no vote for %s", node_id, symbol)
                continue

            if not vote.is_fresh:
                dissenting.append(node_id)
                log.debug("[P53] %s: vote stale (%.1fs) for %s", node_id, vote.age_secs, symbol)
                continue

            if not vote.is_clean:
                dissenting.append(node_id)
                log.debug(
                    "[P53] %s: vote unclean (vpin=%.3f conf=%.3f) for %s",
                    node_id, vote.vpin_toxicity, vote.confidence, symbol,
                )
                continue

            # Direction matching
            # "neutral" votes never block — they count for any direction.
            if vote.direction == "neutral" or vote.direction == pdir:
                agreeing.append(vote)
            else:
                dissenting.append(node_id)
                log.debug(
                    "[P53] %s: direction mismatch (vote=%s proposed=%s) for %s",
                    node_id, vote.direction, pdir, symbol,
                )

        approved = len(agreeing) >= P53_QUORUM

        if agreeing:
            conf_vals = [v.confidence    for v in agreeing]
            tox_vals  = [v.vpin_toxicity for v in agreeing]
            mean_conf = sum(conf_vals) / len(conf_vals)
            min_tox   = min(tox_vals)
            max_tox   = max(tox_vals)
        else:
            mean_conf = 0.0
            min_tox   = 1.0
            max_tox   = 1.0

        if approved:
            self._approved_count += 1
            blocking_reason = None
        else:
            self._blocked_count += 1
            n_fresh  = sum(1 for n in self._nodes.values() if n.latest_vote(symbol) and n.latest_vote(symbol).is_fresh)
            blocking_reason = (
                f"quorum_not_met (need={P53_QUORUM} got={len(agreeing)} "
                f"fresh_nodes={n_fresh} dissenting={dissenting})"
            )
            log.debug(
                "[P53] BLOCKED %s %s — %s",
                symbol, pdir, blocking_reason,
            )

        lat_us = (time.monotonic() - t0) * 1e6
        result = ConsensusResult(
            approved         = approved,
            symbol           = symbol,
            proposed_dir     = pdir,
            agreeing_nodes   = [v.node_id for v in agreeing],
            dissenting_nodes = dissenting,
            blocking_reason  = blocking_reason,
            confidence       = round(mean_conf, 4),
            min_toxicity     = round(min_tox,  4),
            max_toxicity     = round(max_tox,  4),
            latency_us       = round(lat_us,   2),
            check_ts         = now,
        )

        # Record in history for dashboard
        self._history[symbol].append({
            "ts":             round(now, 3),
            "approved":       approved,
            "proposed_dir":   pdir,
            "agreeing":       result.agreeing_nodes,
            "dissenting":     result.dissenting_nodes,
            "blocking_reason": blocking_reason,
            "confidence":     result.confidence,
            "latency_us":     result.latency_us,
        })

        return result

    # ── Status snapshot ───────────────────────────────────────────────────────

    def status_snapshot(self) -> dict:
        """
        [P53] Serialisable status dict written to trader_status.json["p53_mesh"]
        every _gui_bridge cycle.  Never raises.
        """
        try:
            uptime = time.time() - self._boot_ts
            approval_rate = (
                self._approved_count / self._total_checks * 100
                if self._total_checks > 0 else 0.0
            )
            nodes_snap = {
                nid: node.status_snapshot()
                for nid, node in self._nodes.items()
            }
            # Per-symbol latest results
            latest_results: dict = {}
            for sym in self._symbols:
                hist = self._history.get(sym)
                if hist:
                    latest_results[sym] = hist[-1]

            return {
                "enabled":            self._enabled,
                "quorum":             P53_QUORUM,
                "window_secs":        P53_CONSENSUS_WINDOW_SECS,
                "vpin_threshold":     P53_VPIN_TOXICITY_THRESHOLD,
                "signing_active":     P53_MESH_SECRET is not None,
                "ntp_offset_ms":      round(self._clock.offset_ms, 3),
                "uptime_secs":        round(uptime, 1),
                "total_checks":       self._total_checks,
                "approved_count":     self._approved_count,
                "blocked_count":      self._blocked_count,
                "approval_rate_pct":  round(approval_rate, 2),
                "nodes":              nodes_snap,
                "clock_sync":         self._clock.status_snapshot(),
                "anti_replay":        self._replay.status_snapshot(),
                "latest_results":     latest_results,
            }
        except Exception as exc:
            log.debug("[P53] status_snapshot error (non-fatal): %s", exc)
            return {"enabled": False, "error": str(exc)}

    # ── Live node update helpers (called by engine_supervisor) ────────────────

    def feed_okx_vpin(self, symbol: str, vpin_toxicity: float, direction: str,
                      confidence: float = 0.8) -> None:
        """[P53] Feed OKX bridge P47 VPIN data directly as an OKX node vote."""
        self.submit_vote(
            exchange      = NODE_OKX,
            symbol        = symbol,
            direction     = direction,
            vpin_toxicity = vpin_toxicity,
            confidence    = confidence,
        )

    def feed_binance_signal(self, symbol: str, direction: str,
                            vpin_estimate: float = 0.3,
                            confidence: float = 0.65) -> None:
        """[P53] Feed Binance tape-derived signal as a Binance node vote."""
        self.submit_vote(
            exchange      = NODE_BINANCE,
            symbol        = symbol,
            direction     = direction,
            vpin_toxicity = vpin_estimate,
            confidence    = confidence,
        )

    def feed_coinbase_signal(self, symbol: str, direction: str,
                             vpin_estimate: float = 0.3,
                             confidence: float = 0.70) -> None:
        """[P53] Feed Coinbase Oracle signal as a Coinbase node vote."""
        self.submit_vote(
            exchange      = NODE_COINBASE,
            symbol        = symbol,
            direction     = direction,
            vpin_toxicity = vpin_estimate,
            confidence    = confidence,
        )


# ══════════════════════════════════════════════════════════════════════════════
# HELPER UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def _exchange_to_node(exchange: str) -> str:
    """Normalise an exchange name or node_id to a canonical node_id string."""
    _map = {
        "okx":            NODE_OKX,
        "okx_primary":    NODE_OKX,
        "binance":        NODE_BINANCE,
        "binance_primary": NODE_BINANCE,
        "coinbase":       NODE_COINBASE,
        "coinbase_oracle": NODE_COINBASE,
        "cbdax":          NODE_COINBASE,
        "coinbasepro":    NODE_COINBASE,
    }
    return _map.get(exchange.lower(), exchange.lower())


def _normalise_direction(d: str) -> str:
    """Map any direction string to one of: long | short | neutral."""
    d = str(d).lower().strip()
    if d in ("long", "buy", "bullish", "entry_long", "up"):
        return "long"
    if d in ("short", "sell", "bearish", "entry_short", "down"):
        return "short"
    return "neutral"


# ══════════════════════════════════════════════════════════════════════════════
# MODULE SELF-TEST  (python mesh_consensus.py)
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    async def _selftest() -> None:
        print("\n=== Phase 53 Mesh Consensus Self-Test ===\n")

        gate = MeshConsensusGate(symbols=["BTC", "ETH", "XRP"], enabled=True)
        await gate.start()

        # 1. Disabled pass-through
        gate2 = MeshConsensusGate(symbols=["BTC"], enabled=False)
        r = gate2.check_consensus("BTC", "long")
        assert r.approved, "Disabled gate must pass all"
        print("✅ Disabled pass-through")

        # 2. No votes → blocked
        r = gate.check_consensus("BTC", "long")
        assert not r.approved, "No votes → must block"
        print("✅ No votes → blocked")

        # 3. 1 vote → still blocked
        gate.feed_okx_vpin("BTC", 0.2, "long", 0.9)
        r = gate.check_consensus("BTC", "long")
        assert not r.approved, "1 vote < quorum=2 → must block"
        print("✅ 1 vote → blocked (< quorum)")

        # 4. 2 agreeing votes → approved
        gate.feed_binance_signal("BTC", "long", 0.25, 0.7)
        r = gate.check_consensus("BTC", "long")
        assert r.approved, f"2 agreeing votes → must approve; got: {r}"
        assert "okx_primary" in r.agreeing_nodes
        assert "binance_primary" in r.agreeing_nodes
        print(f"✅ 2/3 consensus APPROVED  conf={r.confidence:.3f}  latency={r.latency_us:.1f}µs")

        # 5. High toxicity blocks even with 2 votes
        gate.feed_okx_vpin("ETH", 0.95, "long", 0.9)   # toxic
        gate.feed_binance_signal("ETH", "long", 0.9, 0.9)  # also toxic
        r = gate.check_consensus("ETH", "long")
        assert not r.approved, "High VPIN toxicity → must block"
        print("✅ High VPIN toxicity → blocked")

        # 6. Direction mismatch blocks
        gate.feed_okx_vpin("XRP", 0.1, "long",  0.9)
        gate.feed_binance_signal("XRP", "short", 0.1, 0.9)
        r = gate.check_consensus("XRP", "long")
        assert not r.approved, "Direction mismatch → must block"
        print("✅ Direction mismatch → blocked")

        # 7. Neutral vote counts for any direction
        gate2 = MeshConsensusGate(symbols=["BTC"], enabled=True)
        await gate2.start()
        gate2.feed_okx_vpin("BTC", 0.1, "long", 0.85)
        gate2.feed_binance_signal("BTC", "neutral", 0.15, 0.60)
        r2 = gate2.check_consensus("BTC", "long")
        assert r2.approved, "Neutral vote counts for long → must approve"
        print("✅ Neutral vote accepted for direction")
        await gate2.stop()

        # 8. Anti-replay: duplicate seq
        node = gate._nodes[NODE_OKX]
        node._seq -= 1  # force seq collision on next submit
        old_seq = node._seq + 1
        gate._replay._seen_set[NODE_OKX].add(old_seq)
        gate._replay._max_seen[NODE_OKX] = old_seq
        # This submit will produce seq == old_seq (replay)
        v = node.submit("BTC", "long", 0.1, 0.9)
        # seq is incremented inside submit, so it will equal old_seq+1 — clean.
        # Force a real replay scenario:
        guard2 = AntiReplayGuard(window=10)
        ok, reason = guard2.validate("x", 5)
        assert ok
        ok2, reason2 = guard2.validate("x", 5)  # same seq again
        assert not ok2, "Duplicate seq must be rejected"
        print(f"✅ Anti-replay guard: '{reason2}'")

        # 9. Envelope HMAC signing (if secret set)
        secret = b"test_secret_key"
        payload = {"symbol": "BTC", "direction": "long", "vpin_toxicity": 0.1, "confidence": 0.9}
        body    = MeshEnvelope.compute_canonical_body("53.0", "okx_primary", 1, 1000.0, payload)
        sig     = MeshEnvelope.sign(body, secret)
        env = MeshEnvelope(
            schema_version="53.0", node_id="okx_primary", exchange="OKX",
            region="AP", seq=1, clock_ts=1000.0, ntp_offset_ms=0.0,
            payload_type="vote", payload=payload, hmac_sig=sig,
        )
        assert env.verify(secret), "HMAC verify must pass with correct secret"
        assert not env.verify(b"wrong_key"), "HMAC verify must fail with wrong secret"
        print("✅ HMAC signing + verification")

        # 10. Status snapshot
        snap = gate.status_snapshot()
        assert snap.get("enabled") is True
        assert "nodes" in snap
        assert "latest_results" in snap
        print(f"✅ status_snapshot OK  total_checks={snap['total_checks']}"
              f"  approved={snap['approved_count']}  blocked={snap['blocked_count']}")

        await gate.stop()
        print("\n✅ All Phase 53 mesh consensus tests PASSED\n")

    asyncio.run(_selftest())
    sys.exit(0)
