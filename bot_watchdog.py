"""
bot_watchdog.py  —  Phase 30.5 Titan Upgrade: Stale-Data Integrity Monitor

[P30.5-WD] Monitors main.py and automatically restarts it if it exits with
        a non-zero return code or crashes unexpectedly.

[P30.5-STALE] Stale-Data Integrity Check — if trader_status.json has not
        been modified in STALE_DATA_TIMEOUT_SECS (default 120 s), the watchdog
        sends SIGTERM to the child and forces a restart.  This catches cases
        where the executor loop deadlocks without crashing (no non-zero exit),
        such as a stuck asyncio task or a hanging OKX WebSocket reconnect.

Behaviour
---------
- On clean exit (code 0):   watchdog stops — assumes intentional shutdown.
- On crash (code != 0):     waits RESTART_DELAY_SECS, then restarts.
- On stale status file:     SIGTERM child after STALE_DATA_TIMEOUT_SECS.
- On KeyboardInterrupt:     sends SIGTERM to the child and exits cleanly.
- Max consecutive failures: after WATCHDOG_MAX_CONSECUTIVE_FAILURES restarts
  without a successful run of at least WATCHDOG_MIN_UPTIME_SECS, the watchdog
  stops to prevent runaway restart loops (e.g. broken config or bad creds).
- All events written to watchdog.log and stdout via the standard logging module.
- CLI arguments are forwarded transparently to main.py (e.g. --no-p17).

Environment variables
---------------------
  WATCHDOG_RESTART_DELAY_SECS            default 5.0
  WATCHDOG_MAX_CONSECUTIVE_FAILURES      default 10
  WATCHDOG_MIN_UPTIME_SECS              default 30.0
  WATCHDOG_LOG_PATH                      default watchdog.log
  WATCHDOG_STALE_DATA_TIMEOUT_SECS      default 120.0   [P30.5-STALE]
  WATCHDOG_STATUS_PATH                  default hub_data/trader_status.json

Usage
-----
  python bot_watchdog.py [args forwarded to main.py]
  python bot_watchdog.py --no-p18 --coins BTC ETH
"""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from typing import List, Optional

def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        return float(raw)
    except Exception:
        logging.getLogger("watchdog").warning("Invalid %s=%r; using %s", name, raw, default)
        return float(default)

def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        return int(raw)
    except Exception:
        logging.getLogger("watchdog").warning("Invalid %s=%r; using %s", name, raw, default)
        return int(default)

# ── Config ─────────────────────────────────────────────────────────────────────
RESTART_DELAY_SECS           = _env_float("WATCHDOG_RESTART_DELAY_SECS", 5.0)
MAX_CONSECUTIVE_FAILURES     = _env_int("WATCHDOG_MAX_CONSECUTIVE_FAILURES", 10)
MIN_UPTIME_SECS              = _env_float("WATCHDOG_MIN_UPTIME_SECS", 30.0)
WATCHDOG_LOG_PATH            = os.environ.get("WATCHDOG_LOG_PATH", "watchdog.log")
# [P30.5-STALE] Stale data detection — how long without a status file update
# before the watchdog considers the child deadlocked and forces SIGTERM.
STALE_DATA_TIMEOUT_SECS      = _env_float("STALE_DATA_TIMEOUT_SECS", _env_float("WATCHDOG_STALE_DATA_TIMEOUT_SECS", 120.0))
_SCRIPT_DIR                  = os.path.dirname(os.path.abspath(__file__))
TRADER_STATUS_PATH           = os.environ.get(
    "WATCHDOG_STATUS_PATH",
    os.path.join(_SCRIPT_DIR, "hub_data", "trader_status.json"),
)

# Path to the target script — sits in the same directory as this file.
MAIN_SCRIPT  = os.path.join(_SCRIPT_DIR, "main.py")

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [watchdog] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(WATCHDOG_LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger("watchdog")


def _build_command(forwarded_args: List[str]) -> List[str]:
    """Build the subprocess command list."""
    return [sys.executable, MAIN_SCRIPT] + forwarded_args


def _status_file_age_secs() -> Optional[float]:
    """
    [P30.5-STALE] Return the age in seconds of trader_status.json.
    Returns None if the file does not exist yet (child may still be starting up).
    """
    try:
        if not os.path.exists(TRADER_STATUS_PATH):
            return None
        mtime = os.path.getmtime(TRADER_STATUS_PATH)
        return time.time() - mtime
    except OSError:
        return None


def _run_once(cmd: List[str]) -> subprocess.Popen:
    """Launch the child process and return the Popen handle."""
    log.info("Launching: %s", " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=sys.stdout,       # stream child output to our stdout
        stderr=sys.stderr,       # stream child errors to our stderr
        text=True,
        bufsize=1,               # line-buffered so logs appear in real time
    )
    log.info("[P30.5-WD] Child PID=%d started.", proc.pid)
    return proc


def _terminate_child(proc: subprocess.Popen) -> None:
    """Send SIGTERM to the child; escalate to SIGKILL if it doesn't exit."""
    if proc.poll() is not None:
        return   # already dead
    log.info("[P30.5-WD] Sending SIGTERM to PID=%d…", proc.pid)
    try:
        proc.terminate()
    except OSError:
        return
    # Give it 10 seconds to exit gracefully.
    for _ in range(20):
        if proc.poll() is not None:
            log.info("[P30.5-WD] PID=%d exited after SIGTERM.", proc.pid)
            return
        time.sleep(0.5)
    # Escalate
    log.warning("[P30.5-WD] PID=%d did not respond to SIGTERM — sending SIGKILL.", proc.pid)
    try:
        proc.kill()
    except OSError:
        pass


def main() -> None:
    # Forward all CLI arguments (except the watchdog script itself) to main_p19.py
    forwarded_args: List[str] = sys.argv[1:]
    cmd = _build_command(forwarded_args)

    log.info(
        "=== PowerTrader Watchdog [P30.5-STALE] started ===\n"
        "  Target script     : %s\n"
        "  Forwarded args    : %s\n"
        "  Restart delay     : %.1f s\n"
        "  Max failures      : %d\n"
        "  Min uptime/success: %.1f s\n"
        "  Stale data timeout: %.1f s  [P30.5-STALE]\n"
        "  Status file path  : %s\n"
        "  Log file          : %s",
        MAIN_SCRIPT,
        forwarded_args or "(none)",
        RESTART_DELAY_SECS,
        MAX_CONSECUTIVE_FAILURES,
        MIN_UPTIME_SECS,
        STALE_DATA_TIMEOUT_SECS,
        TRADER_STATUS_PATH,
        WATCHDOG_LOG_PATH,
    )

    # Verify the target script exists before entering the loop.
    if not os.path.isfile(MAIN_SCRIPT):
        log.critical("[P30.5-WD] Target script not found: %s — aborting.", MAIN_SCRIPT)
        sys.exit(1)

    active_proc: Optional[subprocess.Popen] = None

    # ── Graceful shutdown on Ctrl-C or SIGTERM ─────────────────────────────────
    _shutdown_requested = False

    def _on_signal(signum, frame):
        nonlocal _shutdown_requested
        _shutdown_requested = True
        sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        log.info("[P30.5-WD] Watchdog received %s — requesting shutdown.", sig_name)
        if active_proc is not None:
            _terminate_child(active_proc)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _on_signal)
        except (OSError, ValueError):
            pass  # Windows SIGTERM not always available

    # ── Main restart loop ──────────────────────────────────────────────────────
    consecutive_failures  = 0
    total_restarts        = 0

    while not _shutdown_requested:
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            log.critical(
                "[P30.5-WD] Reached %d consecutive failures (each under %.0f s uptime). "
                "Watchdog is stopping to prevent a restart loop. "
                "Fix the underlying error and relaunch the watchdog manually.",
                consecutive_failures, MIN_UPTIME_SECS,
            )
            break

        start_ts    = time.time()
        active_proc = _run_once(cmd)
        # [P30.5-STALE] When did the child start? Give it a reasonable boot
        # grace period before we start checking the status file age.
        _boot_grace_until = time.time() + max(MIN_UPTIME_SECS, 60.0)

        try:
            # Poll every 5 s so we can detect stale files without blocking on wait().
            while True:
                ret = active_proc.poll()
                if ret is not None:
                    return_code = ret
                    break

                # [P30.5-STALE] Check whether trader_status.json has gone stale.
                if time.time() > _boot_grace_until:
                    _age = _status_file_age_secs()
                    if _age is not None and _age > STALE_DATA_TIMEOUT_SECS:
                        log.error(
                            "[P30.5-STALE] trader_status.json has not been updated "
                            "in %.0f s (threshold=%.0f s) — child appears deadlocked. "
                            "Sending SIGTERM to PID=%d to force restart.",
                            _age, STALE_DATA_TIMEOUT_SECS, active_proc.pid,
                        )
                        _terminate_child(active_proc)
                        # Wait briefly for termination then break loop
                        for _ in range(20):
                            if active_proc.poll() is not None:
                                break
                            time.sleep(0.5)
                        return_code = active_proc.poll() or -9
                        break

                if _shutdown_requested:
                    break
                time.sleep(5.0)
            else:
                return_code = active_proc.wait()
        except KeyboardInterrupt:
            # Handle rare case where Ctrl-C reaches here before signal handler
            _shutdown_requested = True
            _terminate_child(active_proc)
            break

        uptime_secs = time.time() - start_ts
        active_proc = None   # child is gone

        if _shutdown_requested:
            log.info("[P30.5-WD] Shutdown requested — not restarting.")
            break

        if return_code == 0:
            log.info(
                "[P30.5-WD] Child exited cleanly (code=0, uptime=%.1f s). "
                "Watchdog stopping — assuming intentional shutdown.",
                uptime_secs,
            )
            break

        # Non-zero exit — count as failure or success depending on uptime
        if uptime_secs >= MIN_UPTIME_SECS:
            consecutive_failures = 0
            log.warning(
                "[P30.5-WD] Child exited with code=%d after %.1f s (uptime OK). "
                "Consecutive failure counter reset. "
                "Restarting in %.1f s… (total_restarts=%d)",
                return_code, uptime_secs, RESTART_DELAY_SECS, total_restarts + 1,
            )
        else:
            consecutive_failures += 1
            log.error(
                "[P30.5-WD] Child exited with code=%d after only %.1f s "
                "(< %.0f s min_uptime). "
                "Consecutive failures: %d/%d. "
                "Restarting in %.1f s… (total_restarts=%d)",
                return_code, uptime_secs, MIN_UPTIME_SECS,
                consecutive_failures, MAX_CONSECUTIVE_FAILURES,
                RESTART_DELAY_SECS, total_restarts + 1,
            )

        total_restarts += 1

        # Interruptible sleep so a SIGTERM during the delay exits promptly
        sleep_deadline = time.time() + RESTART_DELAY_SECS
        while time.time() < sleep_deadline and not _shutdown_requested:
            time.sleep(0.25)

    log.info(
        "=== PowerTrader Watchdog stopped. total_restarts=%d ===",
        total_restarts,
    )


if __name__ == "__main__":
    main()
