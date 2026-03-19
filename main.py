"""
main.py  —  Phase 52 PowerTrader AI  ·  CLI Launcher & Mode Dispatcher
═══════════════════════════════════════════════════════════════════════════

Phase 52 ARCHITECTURAL ROLE
────────────────────────────
main.py is now a THIN LAUNCHER.  Its only responsibilities are:

  1. Logging initialisation (one-time, before any import that logs)
  2. Argument parsing (all trading flags + new --mode dispatcher)
  3. Mode dispatch:
       --mode trade     (default)  →  engine_supervisor.async_main(args)
       --mode monitor              →  read-only status monitor (no trading)
       --mode backtest             →  analytics / backtest stub (Phase 53+)
  4. The if __name__ == "__main__" entry point

The trading engine lives in engine_supervisor.py.
The dashboard lives in dashboard.py (run via `streamlit run dashboard.py`).
The process supervisor lives in bot_watchdog.py.

Phase 52 decoupling lets:
  • Python restart as analytics/backtest WITHOUT restarting the bridge.
  • Multiple Python processes connect to the same already-running bridge.
  • bot_watchdog.py supervise engine_supervisor independently of the UI.

Exit codes:
  0  — Clean, intentional shutdown
  1  — Fatal startup error
  2  — Unrecoverable runtime error / forced shutdown after timeout

ABSOLUTE RULES (preserved from pre-P52):
  FAIL-CLOSED: missing credentials → sys.exit(1) before any trading loop.
  BINARY TRUTH: equity/positions/VPIN come from bridge stream; never fabricated.
  ZERO DRIFT: all status schema keys and dashboard layout are unchanged.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv(override=True)

# ── Logging (must be configured before any engine_supervisor import) ───────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("powertrader_p52.log", encoding="utf-8"),
    ],
)
for _lib in ("aiohttp", "websockets", "urllib3", "hpack", "ccxt"):
    logging.getLogger(_lib).setLevel(logging.WARNING)

log = logging.getLogger("main_p52")
from pt_utils import _env_float, _env_int, atomic_write_json  # [P0-UTIL]

# ── [P52] Lazy import — engine_supervisor is only imported for trade mode ─────
# This keeps main.py startup fast for --mode monitor / --mode backtest.


# ─────────────────────────────────────────────────────────────────────────────
# Argument parser  (all existing flags + Phase 52 --mode flag)
# ─────────────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "PowerTrader Phase 52 — Engine Supervisor + Monitoring/Analytics Launcher\n"
            "Use --mode trade (default) to start the trading engine.\n"
            "Use --mode monitor for read-only status monitoring.\n"
            "Use --mode backtest for offline analytics / backtest (Phase 53+)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # ── [P52] Mode dispatcher ─────────────────────────────────────────────────
    parser.add_argument(
        "--mode",
        choices=["trade", "monitor", "backtest"],
        default="trade",
        help=(
            "[P52] Execution mode: "
            "'trade' starts the full trading engine (default); "
            "'monitor' starts a read-only status watcher; "
            "'backtest' runs the offline analytics engine (Phase 53+ stub)."
        ),
    )

    # ── Trading flags (forwarded to engine_supervisor.async_main) ─────────────
    parser.add_argument("--coins",          nargs="+", type=str, default=None)
    parser.add_argument("--train-first",    action="store_true")
    parser.add_argument("--drawdown-limit", type=float, default=None)
    parser.add_argument("--no-arb",         action="store_true")
    parser.add_argument("--no-news",        action="store_true")
    parser.add_argument("--no-governor",    action="store_true")
    parser.add_argument("--no-sentinel",    action="store_true")
    parser.add_argument("--no-p16-bridge",  action="store_true")
    parser.add_argument("--no-p17",         action="store_true")
    parser.add_argument("--no-p18",         action="store_true")
    parser.add_argument("--no-p20-zombie",  action="store_true")
    parser.add_argument("--no-p20-shadow",  action="store_true")
    parser.add_argument("--no-p21",         action="store_true")
    parser.add_argument(
        "--no-p22-panic",
        action="store_true",
        help="Disable Systemic Panic Lock (P22-4)",
    )
    parser.add_argument(
        "--no-p22-entropy",
        action="store_true",
        help="Disable Entropy Shield (P22-2)",
    )
    parser.add_argument(
        "--no-bridge",
        action="store_true",
        help="[P40] Disable Rust IPC bridge; run legacy REST/WS path (Safe Mode)",
    )
    parser.add_argument("--brain-state", type=str, default=None)

    return parser


# ─────────────────────────────────────────────────────────────────────────────
# Mode: monitor  (read-only status watcher)
# ─────────────────────────────────────────────────────────────────────────────

async def _monitor_mode(args: argparse.Namespace) -> int:
    """
    [P52] Read-only monitoring mode.
    Connects to the bridge (read-only) and streams status to console.
    Does NOT execute any trades. Safe to run alongside the trade engine.
    """
    import json
    import os

    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    status_path = os.path.join(_SCRIPT_DIR, "hub_data", "trader_status.json")

    log.info("[P52-MONITOR] Starting read-only monitor. Status file: %s", status_path)
    log.info("[P52-MONITOR] Press Ctrl-C to stop.")

    _stop = asyncio.Event()

    def _sig(*_):
        _stop.set()

    import signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_event_loop().add_signal_handler(sig, _sig)
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, _sig)

    _last_ts: float = 0.0
    _poll = 2.0

    while not _stop.is_set():
        try:
            if os.path.exists(status_path):
                mtime = os.path.getmtime(status_path)
                if mtime != _last_ts:
                    _last_ts = mtime
                    with open(status_path, "r", encoding="utf-8") as _f:
                        doc = json.load(_f)
                    ts      = doc.get("timestamp", 0.0)
                    status  = doc.get("status", "unknown")
                    equity  = doc.get("account", {}).get("total_equity", 0.0)
                    zombie  = doc.get("p20_zombie_mode", False)
                    demo    = doc.get("demo_mode", False)
                    age     = round(time.time() - ts, 1)
                    log.info(
                        "[MONITOR] status=%-12s  equity=%10.2f  zombie=%s  demo=%s  age=%.1fs",
                        status, equity, zombie, demo, age,
                    )
        except Exception as exc:
            log.debug("[MONITOR] status read error: %s", exc)

        try:
            await asyncio.wait_for(_stop.wait(), timeout=_poll)
        except asyncio.TimeoutError:
            pass

    log.info("[P52-MONITOR] Monitor stopped.")
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Mode: backtest  (Phase 53+ stub)
# ─────────────────────────────────────────────────────────────────────────────

async def _backtest_mode(args: argparse.Namespace) -> int:
    """
    [P52] Offline backtest / analytics mode.
    Full backtest engine arrives in Phase 53.  Currently a safe stub that
    reports the stub state and exits cleanly (code 0).
    """
    log.info(
        "[P52-BACKTEST] Backtest/analytics mode selected.\n"
        "  Phase 53 will deliver the full offline backtest engine.\n"
        "  For now: exiting cleanly (code 0).\n"
        "  To run the dashboard, use:  streamlit run dashboard.py"
    )
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> int:
    parser = _build_parser()
    args   = parser.parse_args()

    log.info(
        "[P52] PowerTrader main.py — mode=%s  bridge=%s  demo=%s",
        args.mode,
        "disabled" if getattr(args, "no_bridge", False) else "enabled",
        os.environ.get("OKX_DEMO_MODE", "0") == "1",
    )

    if args.mode == "trade":
        # [P52] Import engine_supervisor lazily so --mode monitor / backtest
        # never pay the cost of loading the full trading stack.
        try:
            import engine_supervisor
        except ImportError as exc:
            log.critical(
                "[P52] Cannot import engine_supervisor: %s\n"
                "Ensure engine_supervisor.py is in the same directory as main.py.",
                exc,
            )
            return 1

        return await engine_supervisor.async_main(args)

    elif args.mode == "monitor":
        return await _monitor_mode(args)

    elif args.mode == "backtest":
        return await _backtest_mode(args)

    else:
        log.critical("[P52] Unknown mode: %r — this should not happen.", args.mode)
        return 1


# ─────────────────────────────────────────────────────────────────────────────
# if __name__ == "__main__"
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    code: int = 2
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        try:
            code = loop.run_until_complete(main())
        except SystemExit as _se:
            code = int(getattr(_se, "code", 0) or 0)
        except Exception as exc:
            log.critical(
                "[P52] Unhandled exception in main loop: %s", exc, exc_info=True
            )
            code = 2
    finally:
        try:
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for t in pending:
                t.cancel()
            if pending:
                timeout = _env_float("SHUTDOWN_TIMEOUT_SECS", 20.0)
                loop.run_until_complete(
                    asyncio.wait_for(
                        asyncio.shield(asyncio.gather(*pending, return_exceptions=True)),
                        timeout=timeout,
                    )
                )
        except Exception as _exc:
            log.warning("[MAIN] suppressed: %s", _exc)
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception as _exc:
            log.warning("[MAIN] suppressed: %s", _exc)
        try:
            asyncio.set_event_loop(None)
        except Exception as _exc:
            log.warning("[MAIN] suppressed: %s", _exc)
        loop.close()
    sys.exit(code)
