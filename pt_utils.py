"""
pt_utils.py — PowerTrader Shared Resilience Utilities
======================================================
[P0-UTIL] Single canonical source of truth for:

  1. _env_float / _env_int    — safe environment variable parsing; never raises
  2. atomic_write_json         — NamedTemporaryFile + fsync + os.replace (3 retries)
                                 + Windows-safe direct-write fallback on PermissionError
  3. atomic_write_json_async   — async-safe wrapper via run_in_executor
  4. _atomic_write_sync        — backward-compat alias (engine_supervisor shim callers)

Resilience contract enforced here:
  • Parse failure → returns typed default + logs WARNING. Never raises, never crashes.
  • Same-directory NamedTemporaryFile → os.replace is always same-filesystem (atomic on POSIX).
  • fsync before rename → survives power-loss mid-write.
  • 3-attempt retry loop → handles transient Windows antivirus / FS contention.
  • Temp cleanup failure → logged at DEBUG, never raises.
  • [WIN-FALLBACK] If ALL temp-rename retries fail with PermissionError / WinError 5
    (Windows handle contention from dashboard readers or AV scanners), a direct
    open-truncate-write fallback is attempted on the target path.  The full
    serialised payload is always written — no silent data loss.

Operator-verifiable log outcomes (all three states are explicitly logged):
  1. PRIMARY SUCCESS  — DEBUG on attempt 1; WARNING on retry attempts 2-N so
                        that any preceding failure warnings are followed by an
                        unambiguous confirmation that the file was written.
  2. FALLBACK SUCCESS — WARNING: "[P0-ATOMIC] fallback direct-write succeeded
                        for <path> after replace contention"
  3. TOTAL FAILURE    — ERROR: "[P0-ATOMIC] fallback direct-write also FAILED
                        for <path>" (returned False; caller must handle)

ALL modules must import from here. Local duplicate helpers are PROHIBITED.
"""
from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any

_log = logging.getLogger("pt_utils")
_log.addHandler(logging.NullHandler())


# ── [P0-UTIL-1] Safe env parsing ─────────────────────────────────────────────

def _env_float(key: str, default: float) -> float:
    """Return float from env var *key*; warn and return *default* on bad/missing values."""
    raw = os.environ.get(key, "")
    if not raw.strip():
        return float(default)
    try:
        return float(raw.strip().strip("'\""))
    except (ValueError, TypeError):
        _log.warning("[P0-ENV] %s=%r is not a valid float — using default %.6g", key, raw, default)
        return float(default)


def _env_int(key: str, default: int) -> int:
    """Return int from env var *key*; warn and return *default* on bad/missing values."""
    raw = os.environ.get(key, "")
    if not raw.strip():
        return int(default)
    try:
        return int(float(raw.strip().strip("'\"")) )
    except (ValueError, TypeError):
        _log.warning("[P0-ENV] %s=%r is not a valid int — using default %d", key, raw, default)
        return int(default)


# ── [P0-UTIL-2] Atomic JSON write (synchronous) ──────────────────────────────

def atomic_write_json(path: str, obj: Any, *, retries: int = 3) -> bool:
    """
    Atomically persist *obj* as JSON at *path*.

    PRIMARY PATH (preferred, all platforms):
      1. Serialise to string first — aborts cleanly on TypeError/ValueError.
      2. Write to NamedTemporaryFile in the same directory (same-FS rename).
      3. fsync the temp file — durable before rename.
      4. os.replace(tmp, path) — atomic on POSIX; best-effort on Windows.
      5. Retry up to *retries* times on PermissionError / OSError.

    SUCCESS LOGGING — operator-verifiable, three explicit outcomes:
      • Attempt 1 succeeds  → DEBUG  "[P0-ATOMIC] atomic replace OK attempt 1/N for <path>"
      • Retry N succeeds    → WARNING "[P0-ATOMIC] atomic replace OK on retry attempt N/N
                              for <path> after earlier PermissionError(s)"
        (This warning immediately follows any preceding failure warnings so the
        operator can see in one glance: failures then recovery. Without this log
        two PermissionError warnings followed by silence is indistinguishable
        from a third failure.)
      • Fallback succeeds   → WARNING "[P0-ATOMIC] fallback direct-write succeeded
                              for <path> after replace contention"
      • Total failure       → ERROR  "[P0-ATOMIC] fallback direct-write also FAILED
                              for <path>"

    [WIN-FALLBACK] Windows-safe direct-write fallback:
      If ALL rename attempts fail with PermissionError / WinError 5 (a reader
      such as the Streamlit dashboard or Windows AV holds a transient handle on
      the target file, preventing os.replace), fall back to a direct
      open-truncate-write on the target path.  Writing directly to the target
      works even while readers hold shared (read-only) handles because Windows
      only exclusive-locks on rename/replace, not on a plain write-open.

      Risk: a reader may briefly see a partially-written file if it opens exactly
      during our write.  All readers in this codebase (dashboard, watchdog)
      wrap reads in try/except JSONDecodeError and fall back to a last-known-good
      cached value on parse failure, so this is acceptable.

    Returns True on success (either path), False only when both paths fail.
    Never raises.
    """
    try:
        payload = json.dumps(obj, indent=2, default=str)
    except (TypeError, ValueError) as exc:
        _log.error("[P0-ATOMIC] JSON serialise failed for %s: %s", path, exc)
        return False

    directory = os.path.dirname(os.path.abspath(path)) or "."
    tmp_path: str | None = None

    # Track whether every retry failure was specifically PermissionError.
    # If non-permission errors occur, the direct-write fallback is unlikely to
    # help (e.g. disk full, bad path) but we still attempt it — fail-safe.
    _had_any_perm_error: bool = False

    for attempt in range(1, retries + 1):
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", suffix=".tmp",
                dir=directory, delete=False,
            ) as tmp_f:
                tmp_path = tmp_f.name
                tmp_f.write(payload)
                tmp_f.flush()
                os.fsync(tmp_f.fileno())
            # os.replace raises PermissionError on Windows when the target
            # file is open for reading by another process (WinError 5 / 32).
            os.replace(tmp_path, path)
            tmp_path = None  # rename consumed the temp file; nothing to clean up

            # ── PRIMARY SUCCESS — explicit log so this attempt is operator-visible ──
            # On attempt 1 (normal hot path) log at DEBUG to avoid log noise.
            # On retries (attempt 2+) log at WARNING so this confirmation is visible
            # immediately after any preceding PermissionError failure warnings —
            # eliminating the ambiguity of "two failures then silence."
            if attempt == 1:
                _log.debug(
                    "[P0-ATOMIC] atomic replace OK attempt %d/%d for %s",
                    attempt, retries, path,
                )
            else:
                _log.warning(
                    "[P0-ATOMIC] atomic replace OK on retry attempt %d/%d for %s "
                    "(recovered after earlier PermissionError — file IS updated).",
                    attempt, retries, path,
                )
            return True

        except PermissionError as exc:
            # WinError 5 (Access is denied) or WinError 32 (sharing violation).
            # Transient — the handle will often be released between retries.
            _had_any_perm_error = True
            _log.warning(
                "[P0-ATOMIC] attempt %d/%d failed (PermissionError) for %s: %s",
                attempt, retries, path, exc,
            )
            _cleanup_tmp(tmp_path)
            tmp_path = None

        except OSError as exc:
            # Any other OS error (disk full, path missing, etc.).
            _log.warning(
                "[P0-ATOMIC] attempt %d/%d failed for %s: %s",
                attempt, retries, path, exc,
            )
            _cleanup_tmp(tmp_path)
            tmp_path = None

        except Exception as exc:
            _log.error("[P0-ATOMIC] unexpected error writing %s: %s", path, exc)
            _cleanup_tmp(tmp_path)
            return False

    # ── [WIN-FALLBACK] All rename retries exhausted ────────────────────────────
    # Skip the rename step entirely.  Open the target directly for writing;
    # this bypasses the Windows restriction that blocks os.replace when a
    # reader holds a shared handle on the *destination* file.
    _log.warning(
        "[P0-ATOMIC] all %d rename attempt(s) exhausted for %s "
        "(had_perm_error=%s) — attempting direct-write fallback.",
        retries, path, _had_any_perm_error,
    )
    try:
        os.makedirs(directory, exist_ok=True)
        with open(path, "w", encoding="utf-8") as _fb_f:
            _fb_f.write(payload)
            _fb_f.flush()
            try:
                os.fsync(_fb_f.fileno())
            except OSError:
                pass  # fsync unavailable on some Windows configurations — tolerated

        # ── FALLBACK SUCCESS — mandated unmistakable log ───────────────────────
        _log.warning(
            "[P0-ATOMIC] fallback direct-write succeeded for %s "
            "after replace contention (file IS updated, non-atomic write).",
            path,
        )
        return True

    except Exception as _fb_exc:
        # ── TOTAL FAILURE — both primary rename and direct-write fallback failed ─
        _log.error(
            "[P0-ATOMIC] fallback direct-write also FAILED for %s: %s — "
            "file NOT updated.",
            path, _fb_exc,
        )
        return False


# Backward-compat alias used by engine_supervisor internal shim
_atomic_write_sync = atomic_write_json


def _cleanup_tmp(tmp_path: str | None) -> None:
    """Best-effort temp removal. Logs DEBUG on failure — never raises."""
    if tmp_path and os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except Exception as exc:
            _log.debug("[P0-UTIL] temp cleanup failed %s: %s", tmp_path, exc)


# ── [P0-UTIL-3] Async variant ─────────────────────────────────────────────────

async def atomic_write_json_async(path: str, obj: Any, *, retries: int = 3) -> bool:
    """Offloads blocking I/O to the default executor — never stalls the event loop."""
    import asyncio
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: atomic_write_json(path, obj, retries=retries)
    )
