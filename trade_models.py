"""
trade_models.py — Canonical Trade Lifecycle Models
====================================================

[PHASE1] Minimal canonical models required for Phase 1 acceptance tests.
[PHASE2] AdmissionResult — typed go/no-go result from _admission_gate().

Provides:
  • AdmissionResult   — [PHASE2] typed return from the Trade Admission Gate
  • ExitReason        — structured exit-reason enum (replaces free-form tag strings)
  • ExitReasonCategory — coarse grouping of ExitReason values
  • exit_reason_category() — maps ExitReason → ExitReasonCategory
  • StrategicIntent   — placeholder dataclass (not yet enforced as a hard gate;
                         enforcement planned for a future phase)
  • TradeRecord       — typed representation of one trades-table row;
                         is_close_leg=True marks the canonical finalized close

Design rules:
  • Zero import-time side effects (no makedirs, no print, no FileHandler).
  • Zero dependencies on executor / brain / data_hub / intelligence_layer.
  • All enums inherit (str, Enum) so .name and str() are consistent.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ── AdmissionResult ───────────────────────────────────────────────────────────

@dataclass
class AdmissionResult:
    """
    [PHASE2] Typed go/no-go result from _admission_gate().

    Returned by every call to Executor._admission_gate() regardless of
    whether the trade is admitted or blocked.  Callers check .passed and
    log .gate_name / .reason for audit purposes.

    Fields
    ------
    passed    — True if the trade may proceed to execution; False blocks it.
    reason    — Free-form human-readable string describing the outcome or the
                specific rejection cause (e.g. "metadata_missing", "ok").
    gate_name — Short canonical identifier of the gate that produced this
                result (e.g. "PASS", "METADATA", "SPREAD", "BRIDGE_DISCONNECTED",
                "BRIDGE_GHOST_EQUITY", "BRIDGE_STALE", "IN_FLIGHT", "EDGE",
                "CONF", "EARLY_FLUSH", "SYMBOL_BLOCKED", "REGIME_BLOCKED").
    """
    passed:    bool
    reason:    str
    gate_name: str


# ── ExitReasonCategory ────────────────────────────────────────────────────────

class ExitReasonCategory(str, Enum):
    """Coarse category for an ExitReason — used for risk analytics bucketing."""
    STRATEGY       = "STRATEGY"       # signal-driven, planned exits
    RISK           = "RISK"           # drawdown / circuit-breaker driven
    ANOMALY        = "ANOMALY"        # emergency exits (toxicity, catastrophe)
    MANUAL         = "MANUAL"         # operator-initiated
    RECONCILIATION = "RECONCILIATION" # housekeeping / orphan-position close


# ── ExitReason ────────────────────────────────────────────────────────────────

class ExitReason(str, Enum):
    """
    Structured exit-reason type.

    Replaces the free-form tag strings used by _close_position() in Phase 0.
    Each existing string tag maps to exactly one ExitReason; the legacy tag
    string is preserved alongside the typed value in the DB for backward
    compatibility (Phase 1 constraint: additive-only schema changes).
    """

    # ── Strategy exits ───────────────────────────────────────────────────────
    STRATEGY_TRAIL          = "STRATEGY_TRAIL"          # trailing-stop hit
    STRATEGY_STOP           = "STRATEGY_STOP"           # generic stop-loss
    STRATEGY_DEAD_ALPHA     = "STRATEGY_DEAD_ALPHA"     # P13 stale thesis
    STRATEGY_FRONTRUN_WALL  = "STRATEGY_FRONTRUN_WALL"  # P5 wall front-run
    STRATEGY_HEDGE_CLOSE    = "STRATEGY_HEDGE_CLOSE"    # P35 hedge leg close

    # ── Risk-management exits ────────────────────────────────────────────────
    RISK_CIRCUIT_BREAKER    = "RISK_CIRCUIT_BREAKER"    # P7 circuit breaker
    RISK_DRAWDOWN_ZOMBIE    = "RISK_DRAWDOWN_ZOMBIE"    # P20 zombie drawdown
    RISK_GLOBAL_DRAWDOWN    = "RISK_GLOBAL_DRAWDOWN"    # global equity drawdown

    # ── Anomaly / emergency exits ────────────────────────────────────────────
    ANOMALY_CATASTROPHE_VETO = "ANOMALY_CATASTROPHE_VETO"  # P17/P18 narrative veto
    ANOMALY_VPIN_TOXIC       = "ANOMALY_VPIN_TOXIC"         # P37 flow toxicity
    ANOMALY_FUNDING_BRAKE    = "ANOMALY_FUNDING_BRAKE"      # high funding rate

    # ── Manual / operator exits ──────────────────────────────────────────────
    MANUAL_CLOSE            = "MANUAL_CLOSE"

    # ── Reconciliation / housekeeping ────────────────────────────────────────
    RECONCILIATION_CLOSE    = "RECONCILIATION_CLOSE"

    # ── Position-add events (not closes; is_close_leg=0) ─────────────────────
    # [PHASE4] DCA fills that increase an existing position.  Stored in the
    # trades table so round-trip attribution is complete, but excluded from
    # win-rate / PnL KPIs because they are not closed trades.
    STRATEGY_DCA_ADD        = "STRATEGY_DCA_ADD"        # P3-2 DCA add-to-position


# ── Category mapping ──────────────────────────────────────────────────────────

_CATEGORY_MAP: dict[ExitReason, ExitReasonCategory] = {
    ExitReason.STRATEGY_TRAIL:           ExitReasonCategory.STRATEGY,
    ExitReason.STRATEGY_STOP:            ExitReasonCategory.STRATEGY,
    ExitReason.STRATEGY_DEAD_ALPHA:      ExitReasonCategory.STRATEGY,
    ExitReason.STRATEGY_FRONTRUN_WALL:   ExitReasonCategory.STRATEGY,
    ExitReason.STRATEGY_HEDGE_CLOSE:     ExitReasonCategory.STRATEGY,
    ExitReason.RISK_CIRCUIT_BREAKER:     ExitReasonCategory.RISK,
    ExitReason.RISK_DRAWDOWN_ZOMBIE:     ExitReasonCategory.RISK,
    ExitReason.RISK_GLOBAL_DRAWDOWN:     ExitReasonCategory.RISK,
    ExitReason.ANOMALY_CATASTROPHE_VETO: ExitReasonCategory.ANOMALY,
    ExitReason.ANOMALY_VPIN_TOXIC:       ExitReasonCategory.ANOMALY,
    ExitReason.ANOMALY_FUNDING_BRAKE:    ExitReasonCategory.ANOMALY,
    ExitReason.MANUAL_CLOSE:             ExitReasonCategory.MANUAL,
    ExitReason.RECONCILIATION_CLOSE:     ExitReasonCategory.RECONCILIATION,
    ExitReason.STRATEGY_DCA_ADD:         ExitReasonCategory.STRATEGY,
}


def exit_reason_category(reason: ExitReason) -> ExitReasonCategory:
    """Return the ExitReasonCategory for the given ExitReason.

    Falls back to MANUAL for any unrecognised value (should never occur
    for well-formed enum members, but guards against dynamic construction).
    """
    return _CATEGORY_MAP.get(reason, ExitReasonCategory.MANUAL)


# ── Tag-string → ExitReason mapping ──────────────────────────────────────────
#
# Allows executor._close_position callers that pass a legacy tag string
# to resolve to an ExitReason without changing every call site at once.
# Unknown tags fall back to MANUAL_CLOSE.

_TAG_TO_EXIT_REASON: dict[str, ExitReason] = {
    # Trailing stop
    "TRAIL_CLOSE":                        ExitReason.STRATEGY_TRAIL,
    # Dead alpha
    "BAYESIAN_EXIT_DEAD_ALPHA":           ExitReason.STRATEGY_DEAD_ALPHA,
    "DEAD_ALPHA":                         ExitReason.STRATEGY_DEAD_ALPHA,
    # Front-run wall
    "FRONTRUN_WALL_EXIT":                 ExitReason.STRATEGY_FRONTRUN_WALL,
    # Hedge close
    "HEDGE_FULL_CLOSE":                   ExitReason.STRATEGY_HEDGE_CLOSE,
    "HEDGE_FULL_CLOSE_QUANT_FAIL":        ExitReason.STRATEGY_HEDGE_CLOSE,
    # Circuit breaker / drawdown
    "CIRCUIT_BREAKER":                    ExitReason.RISK_CIRCUIT_BREAKER,
    "P7_CIRCUIT_BREAKER":                 ExitReason.RISK_CIRCUIT_BREAKER,
    "GLOBAL_DRAWDOWN":                    ExitReason.RISK_GLOBAL_DRAWDOWN,
    "P7_CB":                              ExitReason.RISK_CIRCUIT_BREAKER,
    # Zombie mode
    "P20_ZOMBIE_DRAWDOWN":                ExitReason.RISK_DRAWDOWN_ZOMBIE,
    "ZOMBIE":                             ExitReason.RISK_DRAWDOWN_ZOMBIE,
    # Catastrophe veto
    "P18_CATASTROPHE_ENTRY":              ExitReason.ANOMALY_CATASTROPHE_VETO,
    "P18_CATASTROPHE":                    ExitReason.ANOMALY_CATASTROPHE_VETO,
    "P18_EXPRESS_CATASTROPHE":            ExitReason.ANOMALY_CATASTROPHE_VETO,
    "CATASTROPHE_VETO":                   ExitReason.ANOMALY_CATASTROPHE_VETO,
    # VPIN / toxic flow
    "P37_TOXIC_FLUSH_IOC":                ExitReason.ANOMALY_VPIN_TOXIC,
    "P37_TOXIC_FLUSH_FALLBACK":           ExitReason.ANOMALY_VPIN_TOXIC,
    "VPIN_EMERGENCY":                     ExitReason.ANOMALY_VPIN_TOXIC,
    # Funding brake
    "FUNDING_BRAKE_EXIT":                 ExitReason.ANOMALY_FUNDING_BRAKE,
    # Manual / reconciliation
    "CLOSE":                              ExitReason.MANUAL_CLOSE,
    "MANUAL":                             ExitReason.MANUAL_CLOSE,
    "RECONCILIATION":                     ExitReason.RECONCILIATION_CLOSE,
}


def tag_to_exit_reason(tag: str) -> ExitReason:
    """
    Map a legacy free-form close tag to a typed ExitReason.

    Matching is case-insensitive and falls back to MANUAL_CLOSE for
    any tag not in the table (e.g. dynamically constructed reason strings
    from _flatten_all).
    """
    if not tag:
        return ExitReason.MANUAL_CLOSE
    # Exact match first
    result = _TAG_TO_EXIT_REASON.get(tag)
    if result is not None:
        return result
    # Case-insensitive fallback
    upper = tag.upper()
    result = _TAG_TO_EXIT_REASON.get(upper)
    if result is not None:
        return result
    # Substring heuristics for dynamically constructed tags
    if "CIRCUIT" in upper or "CB" in upper:
        return ExitReason.RISK_CIRCUIT_BREAKER
    if "ZOMBIE" in upper or "P20" in upper:
        return ExitReason.RISK_DRAWDOWN_ZOMBIE
    if "DRAWDOWN" in upper:
        return ExitReason.RISK_GLOBAL_DRAWDOWN
    if "CATASTROPHE" in upper or "P18" in upper:
        return ExitReason.ANOMALY_CATASTROPHE_VETO
    if "VPIN" in upper or "TOXIC" in upper or "P37" in upper:
        return ExitReason.ANOMALY_VPIN_TOXIC
    if "FUNDING" in upper:
        return ExitReason.ANOMALY_FUNDING_BRAKE
    if "TRAIL" in upper:
        return ExitReason.STRATEGY_TRAIL
    if "DEAD" in upper or "ALPHA" in upper:
        return ExitReason.STRATEGY_DEAD_ALPHA
    if "HEDGE" in upper:
        return ExitReason.STRATEGY_HEDGE_CLOSE
    if "FRONTRUN" in upper or "WALL" in upper:
        return ExitReason.STRATEGY_FRONTRUN_WALL
    if "RECONCIL" in upper:
        return ExitReason.RECONCILIATION_CLOSE
    if "DCA" in upper:
        return ExitReason.STRATEGY_DCA_ADD
    return ExitReason.MANUAL_CLOSE


# ── StrategicIntent placeholder ───────────────────────────────────────────────

@dataclass
class StrategicIntent:
    """
    [PHASE1] Placeholder canonical intent object.

    Captures the *decision* to enter a trade before any order is placed.
    In a future phase this may become the required argument to the Admission
    Gate; _maybe_enter() would create a StrategicIntent and pass it through
    the admission check before any exchange call is made.

    CURRENT STATUS: defined-only.  This class is NOT imported or referenced
    by executor.py and is NOT enforced as a hard execution gate in any live
    path.  The Phase 2 Admission Gate (AdmissionResult / _admission_gate)
    uses raw signal parameters directly rather than a StrategicIntent wrapper.
    The dataclass is intentionally minimal so no retrofitting is required when
    it is eventually wired in as a post-roadmap improvement.
    """
    symbol:           str
    direction:        str      # "long" | "short"
    usd_amount:       float
    confidence:       float
    regime:           str
    source_signal_ts: float
    intent_id:        str   = field(default_factory=lambda: str(uuid.uuid4()))
    created_at:       float = field(default_factory=time.time)


# ── TradeRecord ───────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    """
    [PHASE1] Typed representation of one row in the trades SQLite table.

    Semantics
    ---------
    trade_id        — links the open row and the close row for one round-trip.
                      Generated at position creation; written to both legs.
    is_close_leg    — True on the row written by _record_close().
                      False (default) on open-leg / WS fill rows.
    exit_reason     — ExitReason enum value; populated only on close rows.
    exit_reason_category — coarse bucket; populated only on close rows.

    Backward compatibility
    ----------------------
    The legacy `tag` column is preserved unchanged; exit_reason_type and
    trade_id are additive NULL-able columns (Phase 1 schema migration).
    Old rows (NULL trade_id / NULL exit_reason_type) remain valid.

    [POST-ROADMAP-DT] Entry-context signal fields (additive; NULL on historical rows):
    entry_regime    — Signal.regime at position entry (e.g. "bull", "bear", "chop").
    entry_direction — Signal.direction at position entry ("long" | "short").
    entry_z_score   — Signal.z_score at position entry (raw float).
    """
    trade_id:             str
    symbol:               str
    side:                 str
    qty:                  float
    price:                float
    cost_basis:           float
    pnl_pct:              float
    realized_usd:         float
    tag:                  str
    order_id:             str
    inst_type:            str
    ts:                   int
    is_close_leg:         int                        = 0
    exit_reason:          Optional[ExitReason]         = None
    exit_reason_category: Optional[ExitReasonCategory] = None
    # [POST-ROADMAP-DT] Entry-context signal fields — None on pre-DT rows.
    entry_regime:         Optional[str]               = None
    entry_direction:      Optional[str]               = None
    entry_z_score:        Optional[float]              = None
