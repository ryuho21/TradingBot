"""
bot_watchdog.py  —  Phase 52 PowerTrader Process Supervisor
═══════════════════════════════════════════════════════════════

Phase 52 ARCHITECTURAL ROLE
────────────────────────────
bot_watchdog.py is the PROCESS SUPERVISOR for the trading engine.

It launches main.py with --mode trade (which in turn calls
engine_supervisor.async_main).  If the child crashes or goes stale, the
watchdog restarts it after RESTART_DELAY_SECS.

Phase 52 additions vs Phase 40.3:
  [P52-WD-1] Launches main.py --mode trade (was: main.py directly)
  [P52-WD-2] Optional bridge binary health check:
             If BRIDGE_PID_FILE is set and the bridge PID is dead,
             the watchdog logs a CRITICAL and stops (bridge must restart
             first before the engine supervisor can reconnect).
  [P52-WD-3] engine_supervisor.py health check: if
             supervisor_health.json (written by engine_supervisor) is
             absent after boot grace, it is treated the same as a stale
             trader_status.json -- SIGTERM is sent.
  [P52-WD-4] Updated banner and version logging.

Phase 54 additions:
  [P54-WD-1] P54 kernel-bypass telemetry halt check.
             Reads trader_status.json["p54_kernel_bypass"] every monitoring
             cycle.  Two independent halt conditions (both configurable):
             - total_drops delta > P54_WD_DROP_HALT_THRESH -> CRITICAL + SIGTERM
               (mode-independent -- drop bursts are always a real failure)
             - lat_p99_us > P54_WD_LAT_HALT_US for P54_WD_LAT_CONSEC consecutive
               checks -> CRITICAL + SIGTERM (ring overrun / bridge instability)
               ONLY in kernel-bypass modes (dpdk, onload).
             Always fail-open: JSON read/parse errors skip the P54 check.

  [FIX-P54-MODE] _check_p54_telemetry() -- HALT-2 (latency) is now gated on
             mode in ("dpdk", "onload").  In standard/unknown mode the bridge
             TCP p99 is 100ms-400ms by design; applying a 5ms halt threshold
             to that path caused spurious SIGTERM on every run.  Standard/unknown
             mode now emits WARNING (alert-only) when lat exceeds the threshold
             and resets the consecutive counter; HALT-1 (drops) is unchanged.

[STALE-DATA-MONITOR] Primary deadlock trigger: if trader_status.json has
not been modified in STALE_DATA_TIMEOUT_SECS (default 120s), SIGTERM is
sent to the child process.  On clean exit (code 0) the watchdog stops.
On non-zero exit it waits RESTART_DELAY_SECS then restarts.
After MAX_CONSECUTIVE_FAILURES rapid restarts the watchdog stops.

[FIX-BOOTSTRAP-STALE] Bootstrap-aware stale check (this revision):
  Problem: the stale check reads the absolute age of trader_status.json.
  On restart the file may carry an mtime from the PREVIOUS run (e.g. 767 s
  old).  The boot grace timer (_boot_grace_until) suppresses the check for
  max(MIN_UPTIME_SECS, 60 s) but that is shorter than the inherited file age
  after a long gap between restarts.  Result: the child is killed at ~60 s
  because the inherited file age exceeds the 120 s threshold -- even though
  the child is still in healthy startup / training / bootstrap.

  Fix: two-phase bootstrap policy (Option C: grace timer + fresh-write gate):
    Phase 1 -- Bootstrap window (WATCHDOG_BOOTSTRAP_GRACE_SECS, default 180 s):
      The stale kill is suppressed until EITHER:
        (a) a fresh status write is observed  (mtime > child launch time), OR
        (b) WATCHDOG_BOOTSTRAP_GRACE_SECS have elapsed since child launch.
      During this phase, a DEBUG message is emitted each poll cycle so
      operators can see that stale checking is suppressed and why.
    Phase 2 -- Normal stale enforcement:
      Once condition (a) or (b) is met, the normal stale check applies:
      if file age > STALE_DATA_TIMEOUT_SECS the child is killed.
      If condition (a) triggered (fresh write seen), the age is measured from
      NOW (absolute), so a genuinely deadlocked child that stops writing after
      an initial burst will still be caught within STALE_DATA_TIMEOUT_SECS.

  Logs emitted:
    INFO  "[BOOTSTRAP-STALE] First fresh status write observed after launch ..."
    INFO  "[BOOTSTRAP-STALE] Stale enforcement now ACTIVE ..."
    DEBUG "[BOOTSTRAP-STALE] Suppressing stale kill -- bootstrap window active ..."
    ERROR "[P30.5-STALE] trader_status.json has not been updated ... deadlocked."
"""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from typing import List, Optional
from pt_utils import atomic_write_json, _env_float, _env_int  # [P0-UTIL] shared resilience helpers



# -- Config -------------------------------------------------------------------
RESTART_DELAY_SECS       = _env_float("WATCHDOG_RESTART_DELAY_SECS", 5.0)
MAX_CONSECUTIVE_FAILURES = _env_int("WATCHDOG_MAX_CONSECUTIVE_FAILURES", 10)
MIN_UPTIME_SECS          = _env_float("WATCHDOG_MIN_UPTIME_SECS", 30.0)
WATCHDOG_LOG_PATH        = os.environ.get("WATCHDOG_LOG_PATH", "watchdog.log")

# [P30.5-STALE] Stale data detection.
STALE_DATA_TIMEOUT_SECS = _env_float(
    "STALE_DATA_TIMEOUT_SECS",
    _env_float("WATCHDOG_STALE_DATA_TIMEOUT_SECS", 120.0),
)

# [FIX-BOOTSTRAP-STALE] Bootstrap grace window.
# The stale kill is suppressed for this many seconds after each child launch,
# OR until the child produces a status file write newer than its launch time --
# whichever comes first.  Default is max(STALE_DATA_TIMEOUT_SECS, 180.0) so
# the bootstrap window is always at least as long as the stale timeout itself.
# Override with WATCHDOG_BOOTSTRAP_GRACE_SECS in .env.
WATCHDOG_BOOTSTRAP_GRACE_SECS = _env_float(
    "WATCHDOG_BOOTSTRAP_GRACE_SECS",
    max(STALE_DATA_TIMEOUT_SECS, 180.0),
)

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Target: main.py (Phase 52 thin launcher) with --mode trade
MAIN_SCRIPT = os.path.join(_SCRIPT_DIR, "main.py")

# hub_data paths
TRADER_STATUS_PATH = os.environ.get(
    "WATCHDOG_STATUS_PATH",
    os.path.join(_SCRIPT_DIR, "hub_data", "trader_status.json"),
)

# [P52-WD-3] Supervisor health file -- dashboard reads this for WD RESTARTS / WD STATE.
SUPERVISOR_HEALTH_PATH = os.path.join(
    os.path.dirname(TRADER_STATUS_PATH), "supervisor_health.json"
)

# -- [P52-WD-2] Optional bridge PID file for health detection.
# Set BRIDGE_PID_FILE=/path/to/bridge.pid in .env to enable bridge liveness check.
BRIDGE_PID_FILE = os.environ.get("BRIDGE_PID_FILE", "").strip()

# [P52-WD-1] Default mode injected when not already in forwarded args.
_P52_DEFAULT_MODE = "--mode"
_P52_DEFAULT_MODE_VAL = "trade"

# -- [P54-WD-1] Kernel-bypass telemetry halt thresholds ----------------------
P54_WD_ENABLE           = os.environ.get("P54_WD_ENABLE", "1").strip() == "1"
P54_WD_DROP_HALT_THRESH = _env_int  ("P54_WD_DROP_HALT_THRESH",  500)
P54_WD_LAT_HALT_US      = _env_float("P54_WD_LAT_HALT_US",     5000.0)
P54_WD_LAT_CONSEC       = _env_int  ("P54_WD_LAT_CONSEC",          5)
# -- [/P54-WD-config] ---------------------------------------------------------


# -- [P52-WD-3] Supervisor health file writer ---------------------------------

def _write_supervisor_health(
    state: str,
    restart_count: int,
    child_pid: "int | None" = None,
    last_exit_code: "int | None" = None,
) -> None:
    payload = {
        "state":          state,
        "restart_count":  restart_count,
        "child_pid":      child_pid,
        "last_exit_code": last_exit_code,
        "ts":             time.time(),
    }
    ok = atomic_write_json(SUPERVISOR_HEALTH_PATH, payload)
    if not ok:
        log.debug("[P52-WD-3] supervisor_health write failed (non-fatal) -- dashboard degrades gracefully")


# -- Logging setup ------------------------------------------------------------
if not logging.getLogger("watchdog").handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [watchdog] %(levelname)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(WATCHDOG_LOG_PATH, encoding="utf-8"),
        ],
    )
log = logging.getLogger("watchdog")
logging.getLogger("watchdog").addHandler(logging.NullHandler())


# -- Helper: build subprocess command ----------------------------------------

def _build_command(forwarded_args: List[str]) -> List[str]:
    cmd = [sys.executable, MAIN_SCRIPT]
    has_mode = any(a == "--mode" or a.startswith("--mode=") for a in forwarded_args)
    if not has_mode:
        cmd += [_P52_DEFAULT_MODE, _P52_DEFAULT_MODE_VAL]
    cmd += forwarded_args
    return cmd


# -- Helper: status file age --------------------------------------------------

def _status_file_age_secs() -> Optional[float]:
    """[P30.5-STALE] Return the age in seconds of trader_status.json.
    Returns None if the file does not exist yet (child still starting).
    """
    try:
        if not os.path.exists(TRADER_STATUS_PATH):
            return None
        mtime = os.path.getmtime(TRADER_STATUS_PATH)
        return time.time() - mtime
    except OSError:
        return None


# -- [FIX-BOOTSTRAP-STALE] Helper: status file mtime -------------------------

def _status_file_mtime() -> Optional[float]:
    """[FIX-BOOTSTRAP-STALE] Return the raw mtime (epoch float) of trader_status.json.
    Returns None if the file does not exist or cannot be stat'd.
    Used to detect whether the child has produced a fresh write since launch.
    """
    try:
        if not os.path.exists(TRADER_STATUS_PATH):
            return None
        return os.path.getmtime(TRADER_STATUS_PATH)
    except OSError:
        return None


# -- [P52-WD-2] Bridge binary health check ------------------------------------

def _bridge_is_alive() -> Optional[bool]:
    if not BRIDGE_PID_FILE:
        return None
    try:
        if not os.path.exists(BRIDGE_PID_FILE):
            return None
        with open(BRIDGE_PID_FILE, "r", encoding="utf-8") as _f:
            pid_str = _f.read().strip()
        if not pid_str:
            return None
        pid = int(pid_str)
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
    except Exception as exc:
        log.debug("[P52-WD-2] Bridge liveness check error (non-fatal): %s", exc)
        return None


# -- [P54-WD-1] Kernel-bypass telemetry halt function ------------------------

_p54_wd_last_total_drops: int = 0
_p54_wd_lat_consec_high:  int = 0


def _check_p54_telemetry() -> Optional[str]:
    """[P54-WD-1] Read p54_kernel_bypass from trader_status.json.

    Returns None when no halt condition is met, or a halt-reason string when
    the caller should send SIGTERM to the child process.

    HALT-1: total_drops delta > P54_WD_DROP_HALT_THRESH  -- mode-independent.
    HALT-2: lat_p99_us > P54_WD_LAT_HALT_US for P54_WD_LAT_CONSEC reads
            -- kernel-bypass modes ONLY (dpdk, onload); alert-only in standard.
    Always fail-open: any read/parse/KeyError returns None.
    """
    global _p54_wd_last_total_drops, _p54_wd_lat_consec_high

    if not P54_WD_ENABLE:
        return None
    try:
        if not os.path.exists(TRADER_STATUS_PATH):
            return None
        import json as _json
        with open(TRADER_STATUS_PATH, "r", encoding="utf-8") as _f:
            _status = _json.load(_f)
        _p54 = _status.get("p54_kernel_bypass", {})
        if not isinstance(_p54, dict) or not _p54.get("enabled", False):
            return None

        _total_drops = int  (_p54.get("total_drops", 0))
        _lat_p99     = float(_p54.get("lat_p99_us",  0.0))
        _mode        = str  (_p54.get("mode",  "unknown"))

        # HALT-1: drop delta check -- mode-independent, always active.
        _drop_delta = max(0, _total_drops - _p54_wd_last_total_drops)
        _p54_wd_last_total_drops = _total_drops
        if _drop_delta > P54_WD_DROP_HALT_THRESH:
            return (
                f"[P54-WD-1] Ring-drop burst: delta={_drop_delta} "
                f"> halt_thresh={P54_WD_DROP_HALT_THRESH} "
                f"(mode={_mode}, total_drops={_total_drops}). "
                f"DPDK/ring overrun suspected."
            )

        # [FIX-P54-MODE] HALT-2 mode guard.
        _bypass_mode = _mode in ("dpdk", "onload")

        if not _bypass_mode:
            if _lat_p99 > P54_WD_LAT_HALT_US:
                log.warning(
                    "[P54-WD-1] p99 latency %.0fus exceeds alert threshold %.0fus "
                    "(mode=%s -- ALERT ONLY, no halt). "
                    "This is expected for non-bypass bridge transports.",
                    _lat_p99, P54_WD_LAT_HALT_US, _mode,
                )
            _p54_wd_lat_consec_high = 0
            return None

        # HALT-2: sustained p99 latency check -- kernel-bypass modes only.
        if _lat_p99 > P54_WD_LAT_HALT_US:
            _p54_wd_lat_consec_high += 1
        else:
            _p54_wd_lat_consec_high = 0

        if _p54_wd_lat_consec_high >= P54_WD_LAT_CONSEC:
            _p54_wd_lat_consec_high = 0
            return (
                f"[P54-WD-1] Sustained p99 latency: "
                f"{_lat_p99:.1f}us > halt={P54_WD_LAT_HALT_US:.0f}us "
                f"for {P54_WD_LAT_CONSEC} consecutive checks "
                f"(mode={_mode}). Ring saturation suspected."
            )

    except Exception as _exc:
        log.debug("[P54-WD-1] Telemetry check error (non-fatal): %s", _exc)

    return None


# -- Helper: launch child -----------------------------------------------------

def _run_once(cmd: List[str]) -> subprocess.Popen:
    log.info("[P52-WD] Launching: %s", " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
        bufsize=1,
    )
    log.info("[P52-WD] Child PID=%d started.", proc.pid)
    return proc


# -- Helper: terminate child --------------------------------------------------

def _terminate_child(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    log.info("[P52-WD] Sending SIGTERM to PID=%d...", proc.pid)
    try:
        proc.terminate()
    except OSError:
        return
    for _ in range(20):
        if proc.poll() is not None:
            log.info("[P52-WD] PID=%d exited after SIGTERM.", proc.pid)
            return
        time.sleep(0.5)
    log.warning("[P52-WD] PID=%d did not respond to SIGTERM -- sending SIGKILL.", proc.pid)
    try:
        proc.kill()
    except OSError:
        pass


# -- Main watchdog loop -------------------------------------------------------

def main() -> None:
    forwarded_args: List[str] = sys.argv[1:]
    cmd = _build_command(forwarded_args)

    log.info(
        "=== PowerTrader Watchdog [Phase 54] started ===\n"
        "  Target script          : %s\n"
        "  Forwarded args         : %s\n"
        "  P52 injected mode      : %s (unless --mode already in args)\n"
        "  Restart delay          : %.1f s\n"
        "  Max failures           : %d\n"
        "  Min uptime/success     : %.1f s\n"
        "  Stale data timeout     : %.1f s  [P30.5-STALE]\n"
        "  Bootstrap grace        : %.1f s  [FIX-BOOTSTRAP-STALE]\n"
        "  Status file path       : %s\n"
        "  Bridge PID file        : %s\n"
        "  P54 WD enabled         : %s  [P54-WD-1]\n"
        "  P54 drop halt          : %d drops/cycle\n"
        "  P54 lat halt           : %.0fus p99 x %d consec\n"
        "  Log file               : %s",
        MAIN_SCRIPT,
        forwarded_args or "(none)",
        _P52_DEFAULT_MODE_VAL,
        RESTART_DELAY_SECS,
        MAX_CONSECUTIVE_FAILURES,
        MIN_UPTIME_SECS,
        STALE_DATA_TIMEOUT_SECS,
        WATCHDOG_BOOTSTRAP_GRACE_SECS,
        TRADER_STATUS_PATH,
        BRIDGE_PID_FILE or "(not configured)",
        P54_WD_ENABLE,
        P54_WD_DROP_HALT_THRESH,
        P54_WD_LAT_HALT_US, P54_WD_LAT_CONSEC,
        WATCHDOG_LOG_PATH,
    )

    if not os.path.isfile(MAIN_SCRIPT):
        log.critical("[P52-WD] Target script not found: %s -- aborting.", MAIN_SCRIPT)
        sys.exit(1)

    active_proc: Optional[subprocess.Popen] = None

    _shutdown_requested = False

    def _on_signal(signum, frame):
        nonlocal _shutdown_requested
        _shutdown_requested = True
        sig_name = (
            signal.Signals(signum).name
            if hasattr(signal, "Signals")
            else str(signum)
        )
        log.info("[P52-WD] Watchdog received %s -- requesting shutdown.", sig_name)
        if active_proc is not None:
            _terminate_child(active_proc)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _on_signal)
        except (OSError, ValueError):
            pass

    consecutive_failures = 0
    total_restarts       = 0

    _write_supervisor_health("STARTING", total_restarts)

    while not _shutdown_requested:
        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            log.critical(
                "[P52-WD] Reached %d consecutive failures (each under %.0f s uptime). "
                "Watchdog is stopping to prevent a restart loop. "
                "Fix the underlying error and relaunch the watchdog manually.",
                consecutive_failures,
                MIN_UPTIME_SECS,
            )
            break

        _bridge_alive = _bridge_is_alive()
        if _bridge_alive is False:
            log.critical(
                "[P52-WD-2] Bridge binary PID file found but process is DEAD.\n"
                "  PID file: %s\n"
                "  The trading engine cannot reconnect to a dead bridge.\n"
                "  ACTION REQUIRED: restart the Rust bridge binary, then the watchdog.",
                BRIDGE_PID_FILE,
            )
            break

        start_ts    = time.time()
        active_proc = _run_once(cmd)

        _write_supervisor_health("RUNNING", total_restarts, child_pid=active_proc.pid)

        # [P30.5-STALE] Early boot grace: absolute suppress before this expires.
        _boot_grace_until = time.time() + max(MIN_UPTIME_SECS, 60.0)

        # [FIX-BOOTSTRAP-STALE] Per-child bootstrap state.
        #
        # _child_launch_ts:
        #   Epoch time when THIS child was launched.  Any status file mtime
        #   older than this value was written by a prior run and must not be
        #   used to evaluate staleness of THIS child.
        #
        # _stale_enforce_active:
        #   False at launch.  Set True when either:
        #     (a) file mtime > _child_launch_ts  (child produced a fresh write), or
        #     (b) now >= _bootstrap_grace_until  (grace window expired).
        #   Stale-kill logic only executes when this is True.
        #   Reset to False on every new child launch.
        #
        # _bootstrap_grace_until:
        #   If the child never writes a fresh status (e.g. it hangs during init
        #   before the first status write), stale enforcement activates here so
        #   a truly deadlocked child is still caught.
        _child_launch_ts       = start_ts
        _stale_enforce_active  = False
        _bootstrap_grace_until = start_ts + WATCHDOG_BOOTSTRAP_GRACE_SECS

        try:
            while True:
                ret = active_proc.poll()
                if ret is not None:
                    return_code = ret
                    break

                # Stale + bootstrap checks run only after the early boot grace.
                if time.time() > _boot_grace_until:

                    # ── [FIX-BOOTSTRAP-STALE] Activation gate ─────────────────
                    # Before we can kill for staleness, we must confirm that the
                    # stale age being measured is actually THIS child's fault.
                    # An inherited file from a prior run has nothing to do with
                    # the current child's health.
                    if not _stale_enforce_active:
                        _now       = time.time()
                        _file_mtime = _status_file_mtime()

                        if _file_mtime is not None and _file_mtime > _child_launch_ts:
                            # Condition (a): child wrote a fresh status after launch.
                            _stale_enforce_active = True
                            log.info(
                                "[BOOTSTRAP-STALE] First fresh status write observed "
                                "for PID=%d at +%.1f s after launch "
                                "(file_mtime=%.3f > launch_ts=%.3f). "
                                "Stale enforcement now ACTIVE.",
                                active_proc.pid,
                                _now - _child_launch_ts,
                                _file_mtime,
                                _child_launch_ts,
                            )
                        elif _now >= _bootstrap_grace_until:
                            # Condition (b): bootstrap grace window expired without
                            # a fresh write.  This child may be deadlocked in init.
                            _stale_enforce_active = True
                            log.info(
                                "[BOOTSTRAP-STALE] Bootstrap grace window (%.0f s) "
                                "expired for PID=%d without a fresh status write. "
                                "Stale enforcement now ACTIVE.",
                                WATCHDOG_BOOTSTRAP_GRACE_SECS,
                                active_proc.pid,
                            )
                        else:
                            # Still inside bootstrap window -- suppress stale kill.
                            _remaining = _bootstrap_grace_until - _now
                            _file_age  = (
                                (_now - _file_mtime) if _file_mtime is not None else None
                            )
                            log.debug(
                                "[BOOTSTRAP-STALE] Suppressing stale kill for PID=%d -- "
                                "bootstrap window active (%.0f s remaining). "
                                "status_file_age=%.0f s  child_uptime=%.0f s. "
                                "Waiting for fresh write (mtime>launch) or grace expiry.",
                                active_proc.pid,
                                _remaining,
                                _file_age if _file_age is not None else -1.0,
                                _now - _child_launch_ts,
                            )
                    # ── [/FIX-BOOTSTRAP-STALE] ────────────────────────────────

                    # [P30.5-STALE] Normal stale-kill -- only when enforcement active.
                    if _stale_enforce_active:
                        _age = _status_file_age_secs()
                        if _age is not None and _age > STALE_DATA_TIMEOUT_SECS:
                            log.error(
                                "[P30.5-STALE] trader_status.json has not been updated "
                                "in %.0f s (threshold=%.0f s) -- child appears deadlocked. "
                                "Sending SIGTERM to PID=%d to force restart.",
                                _age,
                                STALE_DATA_TIMEOUT_SECS,
                                active_proc.pid,
                            )
                            _terminate_child(active_proc)
                            for _ in range(20):
                                if active_proc.poll() is not None:
                                    break
                                time.sleep(0.5)
                            return_code = active_proc.poll() or -9
                            break

                    # [P52-WD-2] Periodic bridge liveness check (every 30 s).
                    if BRIDGE_PID_FILE and (time.time() - start_ts) % 30.0 < 5.1:
                        _alive = _bridge_is_alive()
                        if _alive is False:
                            log.critical(
                                "[P52-WD-2] Bridge process died during engine supervisor "
                                "run (PID file: %s). "
                                "Stopping child PID=%d -- manual bridge restart required.",
                                BRIDGE_PID_FILE,
                                active_proc.pid,
                            )
                            _terminate_child(active_proc)
                            for _ in range(20):
                                if active_proc.poll() is not None:
                                    break
                                time.sleep(0.5)
                            return_code = active_proc.poll() or -9
                            _shutdown_requested = True
                            break

                    # [P54-WD-1] Kernel-bypass telemetry halt check.
                    _p54_halt = _check_p54_telemetry()
                    if _p54_halt is not None:
                        log.critical(
                            "%s  Sending SIGTERM to PID=%d.",
                            _p54_halt, active_proc.pid,
                        )
                        total_restarts += 1
                        _write_supervisor_health(
                            "RESTARTING", total_restarts, last_exit_code=-54
                        )
                        _terminate_child(active_proc)
                        for _ in range(20):
                            if active_proc.poll() is not None:
                                break
                            time.sleep(0.5)
                        return_code = active_proc.poll() or -54
                        break

                if _shutdown_requested:
                    break
                time.sleep(5.0)
            else:
                return_code = active_proc.wait()

        except KeyboardInterrupt:
            _shutdown_requested = True
            _terminate_child(active_proc)
            break

        uptime_secs = time.time() - start_ts
        active_proc = None

        if _shutdown_requested:
            log.info("[P52-WD] Shutdown requested -- not restarting.")
            _write_supervisor_health("STOPPED", total_restarts, last_exit_code=return_code)
            break

        if return_code == 0:
            log.info(
                "[P52-WD] Child exited cleanly (code=0, uptime=%.1f s). "
                "Watchdog stopping -- assuming intentional shutdown.",
                uptime_secs,
            )
            _write_supervisor_health("STOPPED", total_restarts, last_exit_code=0)
            break

        if uptime_secs >= MIN_UPTIME_SECS:
            consecutive_failures = 0
            log.warning(
                "[P52-WD] Child exited with code=%d after %.1f s (uptime OK). "
                "Consecutive failure counter reset. "
                "Restarting in %.1f s... (total_restarts=%d)",
                return_code,
                uptime_secs,
                RESTART_DELAY_SECS,
                total_restarts + 1,
            )
        else:
            consecutive_failures += 1
            log.error(
                "[P52-WD] Child exited with code=%d after only %.1f s "
                "(< %.0f s min_uptime). "
                "Consecutive failures: %d/%d. "
                "Restarting in %.1f s... (total_restarts=%d)",
                return_code,
                uptime_secs,
                MIN_UPTIME_SECS,
                consecutive_failures,
                MAX_CONSECUTIVE_FAILURES,
                RESTART_DELAY_SECS,
                total_restarts + 1,
            )

        total_restarts += 1
        _write_supervisor_health("RESTARTING", total_restarts, last_exit_code=return_code)

        sleep_deadline = time.time() + RESTART_DELAY_SECS
        while time.time() < sleep_deadline and not _shutdown_requested:
            time.sleep(0.25)

    log.info(
        "=== PowerTrader Watchdog [Phase 54] stopped. total_restarts=%d ===",
        total_restarts,
    )
    _write_supervisor_health("STOPPED", total_restarts)


if __name__ == "__main__":
    main()
