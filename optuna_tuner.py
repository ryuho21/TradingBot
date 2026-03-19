"""
optuna_tuner.py  —  PowerTrader AI  ·  Track 08 Offline Heuristic Tuner
========================================================================

WHAT THIS TOOL IS
-----------------
A standalone offline CLI that reads completed trade history from the SQLite
DB and runs a per-symbol Optuna study over two parameters:

    conf_floor  — minimum entry_confidence to admit a trade
    alloc_cap   — maximum effective allocation fraction per trade

The study is a HEURISTIC OFFLINE SCORING MODEL.  It applies threshold values
to historical closed-trade rows and scores the result.  It does NOT simulate
signal generation, does NOT replay the admission path, does NOT reconstruct
trades that were rejected at admission, and makes NO causal claims about live
performance.  Its output is a scored ranking of threshold combinations over a
fixed historical window — useful as operator guidance, not as ground truth.

WHAT IT PRODUCES
----------------
A JSON file (default: hub_data/optuna_results.json) containing per-symbol
best-found (conf_floor, alloc_cap) values and their heuristic scores vs a
fixed baseline.  The operator reads this file and may optionally apply any
suggestion via the existing Track 07 manual override controls in the
dashboard.  Nothing is applied automatically.

WHAT IT DOES NOT DO
--------------------
- Does not touch the live bot or any shared runtime state
- Does not write to the SQLite DB (read-only connection)
- Does not change any .env variable
- Does not fire any control-queue event
- Does not add new control dimensions beyond the accepted Track 07 surface
  (conf_floor and alloc_cap per symbol only)

BASELINE CONSTANTS
------------------
The offline baseline uses fixed constants that match the verified code-defined
fallback defaults in the accepted source files:

    BASELINE_CONF_FLOOR = 0.45
        Matches: MIN_SIGNAL_CONF = _env_float("MIN_SIGNAL_CONFIDENCE", 0.45)
        Source:  executor.py line 89

    BASELINE_ALLOC_CAP = 0.20
        Matches: MAX_ALLOC_PCT = _env_float("MAX_ALLOC_PCT", 0.2)
        Source:  brain.py line 262

These are fixed offline constants, not runtime reads.  The baseline score
reflects what the heuristic model would have returned under those thresholds
with no env override in effect.

USAGE
-----
    python optuna_tuner.py [--db PATH] [--out PATH] [--trials N]
                           [--min-symbol-trades N] [--min-admitted N]
                           [--verbose]

    --db PATH               Path to powertrader.db
                            (default: hub_data/powertrader.db beside this script)
    --out PATH              Path for output JSON
                            (default: hub_data/optuna_results.json beside this script)
    --trials N              Optuna trials per symbol  (default: 150)
    --min-symbol-trades N   Min rows with non-null entry_confidence per symbol
                            before a study is run  (default: 20)
    --min-admitted N        Min admitted rows per trial before penalty triggers
                            (default: 15)
    --verbose               Enable per-trial Optuna logging (suppressed by default)

EXIT CODES
----------
    0   Success — results written
    1   Unexpected runtime error (DB inaccessible, write failure, missing DB file,
        no usable rows returned from any query tier)
    2   Required dependency not installed (optuna or pandas)
    3   Data-quality failure — DB is readable but contains zero rows with
        non-null entry_confidence (predates Track 01 schema migration)
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

# ── [TRACK08] Logging: NullHandler on import; basicConfig only under __main__.
log = logging.getLogger("optuna_tuner")
log.addHandler(logging.NullHandler())

# ── [TRACK08] Shared resilience utilities (same import pattern as trainer.py).
from pt_utils import atomic_write_json

# ── [TRACK08] Optuna import guard ────────────────────────────────────────────
# Import is deferred to the guard block so that the module can be imported
# without optuna present (e.g. for unit testing the helper functions).
# The actual study execution requires optuna and will exit early if absent.
try:
    import optuna
    _OPTUNA_AVAILABLE = True
except ImportError:
    _OPTUNA_AVAILABLE = False

# ── [TRACK08] pandas ─────────────────────────────────────────────────────────
try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════════════════
# OFFLINE BASELINE CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
# Fixed offline constants matching the verified code-defined fallback defaults
# in the accepted source files.  These are NOT runtime reads and are NOT
# environment-dependent.  The comparison is always against these same values
# so that output is reproducible across machines and .env configurations.
#
# BASELINE_CONF_FLOOR = 0.45
#   Matches: MIN_SIGNAL_CONF = _env_float("MIN_SIGNAL_CONFIDENCE", 0.45)
#   Source:  executor.py line 89
#
# BASELINE_ALLOC_CAP = 0.20
#   Matches: MAX_ALLOC_PCT = _env_float("MAX_ALLOC_PCT", 0.2)
#   Source:  brain.py line 262
BASELINE_CONF_FLOOR: float = 0.45
BASELINE_ALLOC_CAP:  float = 0.20

# ── Search bounds (verified from accepted source files) ──────────────────────
# CONF_FLOOR search range: [MIN_SIGNAL_CONF, MAX_CONF_OVERRIDE]
#   MIN_SIGNAL_CONF  = _env_float("MIN_SIGNAL_CONFIDENCE", 0.45)  executor.py:89
#   MAX_CONF_OVERRIDE = _env_float("MAX_CONF_OVERRIDE",    0.90)  executor.py:136
CONF_FLOOR_MIN: float = 0.45
CONF_FLOOR_MAX: float = 0.90

# ALLOC_CAP search range: [MIN_ALLOC_FLOOR, MAX_ALLOC_PCT]
#   MIN_ALLOC_FLOOR = _env_float("MIN_ALLOC_FLOOR", 0.005)  executor.py:135
#   MAX_ALLOC_PCT   = _env_float("MAX_ALLOC_PCT",   0.2)    brain.py:262
ALLOC_CAP_MIN: float = 0.005
ALLOC_CAP_MAX: float = 0.20

# ── Output schema ─────────────────────────────────────────────────────────────
OUTPUT_VERSION:     int = 1
SCHEMA_NOTE: str = (
    "heuristic offline scoring model — not a simulator, "
    "not a replay engine, not causal backtest truth"
)


# ══════════════════════════════════════════════════════════════════════════════
# DB READ
# ══════════════════════════════════════════════════════════════════════════════

# SQL query tiers — tried in order; first non-empty result wins.
# Two distinct tiers covering the two meaningful schema states:
#
#   Tier 1 (_SQL_MAIN): DB has is_close_leg column (Phase 4+).
#       Fetches only the columns used by the scoring model.
#       pnl_pct and hold_time_secs are intentionally omitted — they are
#       not used in heuristic_score and their presence would be misleading.
#
#   Tier 2 (_SQL_FALLBACK): DB predates is_close_leg (pre-Phase-4).
#       Falls back to the pnl_pct != 0 heuristic to identify close rows.
#       pnl_pct is retained here only as a SQL-side filter predicate.
#
# No writes.  All SELECT only.

_SQL_MAIN = """
    SELECT symbol, realized_usd,
           entry_confidence, entry_kelly_f
    FROM   trades
    WHERE  is_close_leg = 1
    ORDER  BY ts DESC
    LIMIT  5000
"""

_SQL_FALLBACK = """
    SELECT symbol, realized_usd,
           entry_confidence, entry_kelly_f
    FROM   trades
    WHERE  pnl_pct IS NOT NULL AND pnl_pct != 0
    ORDER  BY ts DESC
    LIMIT  5000
"""


def _load_trades(db_path: str) -> "pd.DataFrame":
    """
    Open the DB in read-only mode and load close-leg rows.

    Read-only mode is enforced via the SQLite URI: file:...?mode=ro
    No write SQL paths exist in this function or anywhere in this tool.
    Normal execution produces no DB modifications.

    Returns a DataFrame with at minimum: symbol, realized_usd, entry_confidence.
    entry_kelly_f will be present but may contain nulls on pre-Phase-6 rows.

    Raises RuntimeError on connection failure.
    """
    uri = f"file:{db_path}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            f"Cannot open DB in read-only mode at {db_path!r}: {exc}"
        ) from exc

    df = pd.DataFrame()
    for sql in (_SQL_MAIN, _SQL_FALLBACK):
        try:
            df = pd.read_sql_query(sql.strip(), conn)
            if not df.empty:
                break
        except Exception as exc:
            log.debug("_load_trades: query attempt failed: %s", exc)
            df = pd.DataFrame()

    try:
        conn.close()
    except Exception:
        pass

    if df.empty:
        return df

    # Coerce numeric columns; silently fill missing columns with NaN.
    # Only columns used by the scoring model are coerced.
    for col in ("realized_usd", "entry_confidence", "entry_kelly_f"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = float("nan")

    return df


# ══════════════════════════════════════════════════════════════════════════════
# HEURISTIC SCORING
# ══════════════════════════════════════════════════════════════════════════════

_LARGE_PENALTY: float = -1_000_000.0


def _heuristic_score(
    sym_df: "pd.DataFrame",
    conf_floor: float,
    alloc_cap: float,
    min_admitted: int,
) -> tuple[float, int]:
    """
    Apply (conf_floor, alloc_cap) to sym_df and return (score, admitted_count).

    HEURISTIC MODEL — not a simulator.
    ├── conf_floor : hard filter — rows with entry_confidence < conf_floor
    │   are excluded, approximating what would have been blocked at admission.
    └── alloc_cap  : soft proportional rescaling heuristic — where
        entry_kelly_f > alloc_cap and entry_kelly_f > 0, realized_usd is
        scaled by (alloc_cap / entry_kelly_f) to approximate a reduced
        position size.  This is NOT causal; it assumes linear PnL scaling,
        which is a simplification.  Rows with null/zero entry_kelly_f are
        left unscaled (conservative: no rescaling applied).

    Returns (_LARGE_PENALTY, 0) when fewer than min_admitted rows survive
    the conf_floor filter — prevents spurious high scores on tiny samples.
    """
    # Step 1: conf_floor hard filter
    has_conf = sym_df["entry_confidence"].notna()
    admitted = sym_df[has_conf & (sym_df["entry_confidence"] >= conf_floor)].copy()

    if len(admitted) < min_admitted:
        return _LARGE_PENALTY, 0

    # Step 2: alloc_cap proportional rescaling heuristic
    valid_kf = admitted["entry_kelly_f"].notna() & (admitted["entry_kelly_f"] > 0)
    cap_mask  = valid_kf & (admitted["entry_kelly_f"] > alloc_cap)
    if cap_mask.any():
        admitted.loc[cap_mask, "realized_usd"] = (
            admitted.loc[cap_mask, "realized_usd"]
            * (alloc_cap / admitted.loc[cap_mask, "entry_kelly_f"])
        )

    # Step 3: objective — mean realized_usd per admitted trade
    valid_pnl = admitted["realized_usd"].notna()
    if valid_pnl.sum() < min_admitted:
        return _LARGE_PENALTY, 0

    score = float(admitted.loc[valid_pnl, "realized_usd"].mean())
    return score, int(len(admitted))


# ══════════════════════════════════════════════════════════════════════════════
# PER-SYMBOL OPTUNA STUDY
# ══════════════════════════════════════════════════════════════════════════════

def _run_symbol_study(
    sym: str,
    sym_df: "pd.DataFrame",
    n_trials: int,
    min_admitted: int,
    verbose: bool,
) -> Dict[str, Any]:
    """
    Run an Optuna study for a single symbol.

    Returns a dict suitable for embedding in the output JSON under
    results["symbols"][sym].  All fields are always present; skip_reason
    is None for a completed study.
    """
    # Baseline score at fixed offline constants (no env reads).
    baseline_score, baseline_admitted = _heuristic_score(
        sym_df, BASELINE_CONF_FLOOR, BASELINE_ALLOC_CAP, min_admitted
    )

    def objective(trial: "optuna.Trial") -> float:
        cf = trial.suggest_float(
            "conf_floor", CONF_FLOOR_MIN, CONF_FLOOR_MAX, step=0.01
        )
        ac = trial.suggest_float(
            "alloc_cap", ALLOC_CAP_MIN, ALLOC_CAP_MAX, step=0.005
        )
        score, _ = _heuristic_score(sym_df, cf, ac, min_admitted)
        return score

    sampler = optuna.samplers.TPESampler(seed=42)
    study   = optuna.create_study(direction="maximize", sampler=sampler)

    verbosity = (
        optuna.logging.DEBUG if verbose else optuna.logging.WARNING
    )
    optuna.logging.set_verbosity(verbosity)

    log.info("[TRACK08] %s: running %d trials …", sym, n_trials)

    callbacks = []
    if not verbose:
        # Print lightweight progress every 50 trials.
        def _progress_cb(_study: "optuna.Study", trial: "optuna.FrozenTrial") -> None:
            if trial.number > 0 and trial.number % 50 == 0:
                best = _study.best_value if _study.best_value > _LARGE_PENALTY else None
                print(
                    f"  [{sym}] trial {trial.number}/{n_trials}"
                    + (f"  best={best:.4f}" if best is not None else "  (no valid trial yet)"),
                    flush=True,
                )
        callbacks.append(_progress_cb)

    study.optimize(objective, n_trials=n_trials, callbacks=callbacks)

    n_complete = len([t for t in study.trials
                      if t.state == optuna.trial.TrialState.COMPLETE])

    # Best trial — guard against all-penalty studies.
    best_value: Optional[float] = None
    best_admitted: int = 0
    best_conf_floor: Optional[float] = None
    best_alloc_cap: Optional[float]  = None

    if study.best_value > _LARGE_PENALTY:
        best_value = study.best_value
        best_conf_floor = study.best_params["conf_floor"]
        best_alloc_cap  = study.best_params["alloc_cap"]
        _, best_admitted = _heuristic_score(
            sym_df, best_conf_floor, best_alloc_cap, min_admitted
        )

    improvement_pct: Optional[float] = None
    if (
        best_value is not None
        and baseline_score is not None
        and baseline_score > _LARGE_PENALTY
        and baseline_score != 0.0
    ):
        improvement_pct = round(
            (best_value - baseline_score) / abs(baseline_score) * 100.0, 2
        )

    return {
        "rows_available":   len(sym_df),
        "n_trials_run":     n_complete,
        "baseline_score":   round(baseline_score, 6) if baseline_score > _LARGE_PENALTY else None,
        "baseline_admitted": baseline_admitted if baseline_score > _LARGE_PENALTY else None,
        "best_score":       round(best_value, 6) if best_value is not None else None,
        "best_admitted":    best_admitted if best_value is not None else None,
        "improvement_pct":  improvement_pct,
        "conf_floor":       round(best_conf_floor, 4) if best_conf_floor is not None else None,
        "alloc_cap":        round(best_alloc_cap,  4) if best_alloc_cap  is not None else None,
        "skip_reason":      None,
    }


def _skipped_entry(sym_df: "pd.DataFrame", reason: str) -> Dict[str, Any]:
    return {
        "rows_available":    len(sym_df),
        "n_trials_run":      0,
        "baseline_score":    None,
        "baseline_admitted": None,
        "best_score":        None,
        "best_admitted":     None,
        "improvement_pct":   None,
        "conf_floor":        None,
        "alloc_cap":         None,
        "skip_reason":       reason,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="optuna_tuner",
        description=(
            "PowerTrader AI Track 08 — offline heuristic tuner for "
            "conf_floor and alloc_cap."
        ),
    )
    _base = Path(__file__).resolve().parent / "hub_data"
    p.add_argument(
        "--db",
        default=str(_base / "powertrader.db"),
        help="Path to powertrader.db  (default: hub_data/powertrader.db)",
    )
    p.add_argument(
        "--out",
        default=str(_base / "optuna_results.json"),
        help="Output JSON path  (default: hub_data/optuna_results.json)",
    )
    p.add_argument(
        "--trials",
        type=int,
        default=150,
        metavar="N",
        help="Optuna trials per symbol  (default: 150)",
    )
    p.add_argument(
        "--min-symbol-trades",
        type=int,
        default=20,
        dest="min_symbol_trades",
        metavar="N",
        help="Min rows with non-null entry_confidence per symbol  (default: 20)",
    )
    p.add_argument(
        "--min-admitted",
        type=int,
        default=15,
        dest="min_admitted",
        metavar="N",
        help="Min admitted rows per trial before penalty  (default: 15)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Enable per-trial Optuna logging",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # ── Dependency checks ─────────────────────────────────────────────────────
    if not _OPTUNA_AVAILABLE:
        print(
            "ERROR: optuna is not installed.\n"
            "Install it with:  pip install optuna\n"
            "Then re-run this script.",
            file=sys.stderr,
        )
        return 2

    if not _PANDAS_AVAILABLE:
        print(
            "ERROR: pandas is not installed.\n"
            "Install it with:  pip install pandas\n"
            "Then re-run this script.",
            file=sys.stderr,
        )
        return 2

    # ── DB existence check ────────────────────────────────────────────────────
    if not Path(args.db).exists():
        print(
            f"ERROR: DB not found at {args.db!r}.\n"
            "Confirm the path with --db or run the bot first to create the DB.",
            file=sys.stderr,
        )
        return 1

    # ── Load trades ───────────────────────────────────────────────────────────
    print(f"[optuna_tuner] Loading trades from {args.db!r} …", flush=True)
    try:
        df = _load_trades(args.db)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if df.empty:
        print(
            "ERROR: No usable trade rows returned from the DB.\n"
            "All query tiers failed or the trades table is empty.\n"
            "Confirm the DB path and run the bot to accumulate closed trades.",
            file=sys.stderr,
        )
        return 1

    # Must have at least some rows with non-null entry_confidence to proceed.
    n_with_conf = int(df["entry_confidence"].notna().sum())
    if n_with_conf == 0:
        print(
            "ERROR: No rows with non-null entry_confidence found.\n"
            "The DB may predate the Track 01 entry-context schema migration.\n"
            "Run the bot with the current executor to accumulate qualifying rows.",
            file=sys.stderr,
        )
        return 3

    total_rows = len(df)
    print(
        f"[optuna_tuner] Loaded {total_rows} rows  "
        f"({n_with_conf} with non-null entry_confidence).",
        flush=True,
    )

    # ── Per-symbol studies ────────────────────────────────────────────────────
    symbols_results: Dict[str, Dict[str, Any]] = {}
    run_ts = time.time()

    all_symbols = sorted(df["symbol"].dropna().unique().tolist())
    print(
        f"[optuna_tuner] Found {len(all_symbols)} symbol(s): "
        f"{', '.join(all_symbols)}",
        flush=True,
    )
    print(
        f"[optuna_tuner] Baseline: conf_floor={BASELINE_CONF_FLOOR}  "
        f"alloc_cap={BASELINE_ALLOC_CAP}  "
        f"(fixed offline constants — see module header for source tracing)",
        flush=True,
    )
    print(
        f"[optuna_tuner] Trials per symbol: {args.trials}  "
        f"min_symbol_trades: {args.min_symbol_trades}  "
        f"min_admitted: {args.min_admitted}",
        flush=True,
    )
    print("", flush=True)

    for sym in all_symbols:
        sym_df = df[df["symbol"] == sym].copy().reset_index(drop=True)

        # Count qualifying rows (non-null entry_confidence).
        qualifying = int(sym_df["entry_confidence"].notna().sum())

        if qualifying < args.min_symbol_trades:
            reason = (
                f"insufficient_data "
                f"({qualifying} rows with non-null entry_confidence "
                f"< {args.min_symbol_trades} required)"
            )
            print(f"  [{sym}] SKIPPED — {reason}", flush=True)
            symbols_results[sym] = _skipped_entry(sym_df, reason)
            continue

        print(
            f"  [{sym}] Running study "
            f"({qualifying} qualifying rows, {args.trials} trials) …",
            flush=True,
        )
        result = _run_symbol_study(
            sym=sym,
            sym_df=sym_df[sym_df["entry_confidence"].notna()].copy().reset_index(drop=True),
            n_trials=args.trials,
            min_admitted=args.min_admitted,
            verbose=args.verbose,
        )
        symbols_results[sym] = result

        if result["best_score"] is not None:
            print(
                f"  [{sym}] Done — "
                f"conf_floor={result['conf_floor']}  "
                f"alloc_cap={result['alloc_cap']}  "
                f"best_score={result['best_score']:.4f}  "
                f"baseline_score={result['baseline_score']:.4f}  "
                f"improvement={result['improvement_pct']:+.1f}%  "
                f"admitted={result['best_admitted']}",
                flush=True,
            )
        else:
            print(
                f"  [{sym}] Done — no valid trial found "
                f"(all trials fell below min_admitted={args.min_admitted})",
                flush=True,
            )

    print("", flush=True)

    # ── Build output ──────────────────────────────────────────────────────────
    output: Dict[str, Any] = {
        "version":              OUTPUT_VERSION,
        "schema_note":          SCHEMA_NOTE,
        "run_ts":               run_ts,
        "n_trials_per_symbol":  args.trials,
        "min_symbol_trades":    args.min_symbol_trades,
        "min_admitted_trades":  args.min_admitted,
        "total_rows_loaded":    total_rows,
        "baseline_conf_floor":  BASELINE_CONF_FLOOR,
        "baseline_alloc_cap":   BASELINE_ALLOC_CAP,
        "symbols":              symbols_results,
    }

    # ── Write output atomically ───────────────────────────────────────────────
    out_path = args.out
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    ok = atomic_write_json(out_path, output)
    if not ok:
        print(
            f"ERROR: Failed to write results to {out_path!r}.\n"
            "Check filesystem permissions and disk space.",
            file=sys.stderr,
        )
        return 1

    print(f"[optuna_tuner] Results written to {out_path!r}", flush=True)

    # ── Summary table ─────────────────────────────────────────────────────────
    completed = {s: r for s, r in symbols_results.items() if r["skip_reason"] is None}
    skipped   = {s: r for s, r in symbols_results.items() if r["skip_reason"] is not None}

    if completed:
        col_w = max(len(s) for s in completed) + 2
        header = (
            f"\n{'SYMBOL':<{col_w}} {'CONF_FLOOR':>10} {'ALLOC_CAP':>10} "
            f"{'BEST_SCORE':>12} {'BASELINE':>12} {'IMPROVEMENT':>12} {'ADMITTED':>9}"
        )
        sep = "-" * len(header)
        print(header)
        print(sep)
        for sym, r in sorted(completed.items()):
            imp = f"{r['improvement_pct']:+.1f}%" if r["improvement_pct"] is not None else "n/a"
            bs  = f"{r['best_score']:.4f}"        if r["best_score"]       is not None else "n/a"
            base= f"{r['baseline_score']:.4f}"    if r["baseline_score"]   is not None else "n/a"
            cf  = f"{r['conf_floor']:.2f}"         if r["conf_floor"]       is not None else "n/a"
            ac  = f"{r['alloc_cap']:.3f}"          if r["alloc_cap"]        is not None else "n/a"
            adm = str(r["best_admitted"])          if r["best_admitted"]    is not None else "n/a"
            print(
                f"{sym:<{col_w}} {cf:>10} {ac:>10} "
                f"{bs:>12} {base:>12} {imp:>12} {adm:>9}"
            )
        print(sep)

    if skipped:
        print(f"\nSkipped symbols ({len(skipped)}):")
        for sym, r in sorted(skipped.items()):
            print(f"  {sym}: {r['skip_reason']}")

    print(
        "\nNOTE: These are heuristic offline scoring suggestions only.\n"
        "Apply any desired overrides via the Track 07 manual controls\n"
        "in the dashboard.  Review diagnostics panels before acting.",
        flush=True,
    )

    return 0


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sys.exit(main())
