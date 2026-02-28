"""
dashboard.py  —  PowerTrader AI · Institutional Elite v26
          [LOGIC EXECUTION BRIDGE · ALADDIN AESTHETIC · GHOST HEAL INDICATOR]
═══════════════════════════════════════════════════════════════════════════════
Architecture:
  ★ [v26-INTEGRITY]    System Integrity Layer — Heartbeat, Bridge Latency (µs),
                        Phase 40.1 display, and Logic Bridge Connectivity pulse.
  ★ [v26-NAV]          NAV Command Center — Total Equity, 24h Delta, Risk Interlock
                        (SECURED / TRIPPED), Drawdown progress bar vs. 99% threshold.
  ★ [v26-ALPHA]        Execution Alpha — Whale Sniper intercept table (last 5 sweeps),
                        Execution Multiplier, Flow Toxicity gauge (macro_score + entropy).
  ★ [v26-TAPE]         The Tape — monospaced fast-scroll action log (Orders, Heals, Vetoes).
  ★ [v26-GHOST]        Ghost Heal Indicator — animated pulse when equity_is_ghost + healed.
  ★ [v26-SHADOW]       P27-style shadow buffer: UI never blanks on mid-write races.
  ★ [v26-FRAGMENT]     Top-level @st.fragment boundaries — no nested fragments.
  ★ [v26-TAB]          P25-style tab-aware adaptive refresh (2 s active / 5 min hidden).
  ★ [v26-NO-SPINNER]   Streamlit status widget hidden; shadow state covers any gap.

Run:
    streamlit run dashboard.py

pip install:
    streamlit>=1.39 plotly pandas numpy
    streamlit-autorefresh          # optional — graceful fallback if absent
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

log = logging.getLogger("dashboard_v26")

# ─── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PowerTrader AI · Institutional Elite v26",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR              = Path(__file__).parent
DB_PATH               = BASE_DIR / "powertrader.db"
HUB_DIR               = BASE_DIR / "hub_data"
TRADER_STATUS         = HUB_DIR  / "trader_status.json"
TIF_PENDING_PATH      = HUB_DIR  / "tif_pending.json"
VETO_AUDIT_PATH       = HUB_DIR  / "veto_audit.json"
TACTICAL_CONFIG_PATH  = HUB_DIR  / "tactical_config.json"
CONTROL_EVENT_PATH    = HUB_DIR  / "control_event.json"
EXECUTOR_LOG_PATH     = Path(os.environ.get("EXECUTOR_LOG_PATH", str(BASE_DIR / "executor.log")))

# ─── Session state keys ────────────────────────────────────────────────────────
_SS_SHADOW_STATUS     = "v26_shadow_status"
_SS_TAB_PREV          = "v26_tab_prev"
_SS_SYNC_AT           = "v26_sync_at"
_SS_TAPE_LINES        = "v26_tape_lines"
_TAB_COOLDOWN_SECS    = 30.0

# ─── Timing ───────────────────────────────────────────────────────────────────
STATUS_STALE_SECS     = 30
HEARTBEAT_LIVE_SECS   = 5
HEARTBEAT_STALE_SECS  = 15
PHASE_LABEL           = "40.1"

# ─── Tactical config keys ─────────────────────────────────────────────────────
TACT_KEY_RISK_OFF = "risk_off_mode"
TACT_KEY_SNIPER   = "sniper_only"
TACT_KEY_HEDGE    = "hedge_mode"
TACT_DEFAULTS     = {TACT_KEY_RISK_OFF: False, TACT_KEY_SNIPER: False, TACT_KEY_HEDGE: False}

# ─── Zombie (drawdown) threshold ──────────────────────────────────────────────
ZOMBIE_DRAWDOWN_THRESHOLD_PCT = 5.0   # 99th-percentile Zombie trip threshold


# ═══════════════════════════════════════════════════════════════════════════════
# COLOUR PALETTE  — "Aladdin" Charcoal Theme
# ═══════════════════════════════════════════════════════════════════════════════
C = {
    # Core surfaces
    "bg":        "#0D0E11",
    "surface":   "#12141A",
    "surface2":  "#181B22",
    "border":    "#1E222C",
    "border2":   "#252B38",
    # Brand accents
    "aqua":      "#30D5C8",   # growth / positive
    "red":       "#FF4B2B",   # risk / critical
    # Neutrals
    "slate":     "#94A3B8",   # labels
    "text":      "#CBD5E1",
    "text2":     "#64748B",
    "muted":     "#334155",
    # Supporting palette
    "green":     "#22D3A5",
    "yellow":    "#F59E0B",
    "blue":      "#3B82F6",
    "purple":    "#A855F7",
    "orange":    "#F97316",
    # Glows
    "glow_a":    "rgba(48,213,200,0.18)",
    "glow_r":    "rgba(255,75,43,0.22)",
    "glow_g":    "rgba(34,211,165,0.18)",
    "glow_y":    "rgba(245,158,11,0.18)",
    "glow_b":    "rgba(59,130,246,0.18)",
    "glow_p":    "rgba(168,85,247,0.18)",
    # Functional
    "panic_bg":  "rgba(255,75,43,0.15)",
    "stale_bg":  "rgba(245,158,11,0.12)",
    # Font stacks
    "mono":      "'JetBrains Mono', 'Cascadia Code', 'Courier New', monospace",
    "sans":      "'Inter', 'SF Pro Display', sans-serif",
}

REGIME_META = {
    "bull":     {"label": "BULL TREND",    "icon": "▲", "col": C["aqua"],   "glow": C["glow_a"]},
    "bear":     {"label": "BEAR TREND",    "icon": "▼", "col": C["red"],    "glow": C["glow_r"]},
    "chop":     {"label": "CHOP / RANGE",  "icon": "◆", "col": C["yellow"], "glow": C["glow_y"]},
    "hvol":     {"label": "HIGH VOL",      "icon": "⚡", "col": C["orange"], "glow": "rgba(249,115,22,0.20)"},
    "neutral":  {"label": "NEUTRAL",       "icon": "─", "col": C["slate"],  "glow": "rgba(148,163,184,0.12)"},
    "deep":     {"label": "DEEP BEAR",     "icon": "☠", "col": "#CC0033",   "glow": "rgba(204,0,51,0.25)"},
}


# ═══════════════════════════════════════════════════════════════════════════════
# CSS INJECTION
# ═══════════════════════════════════════════════════════════════════════════════

def _inject_css(panic_active: bool = False, stale: bool = False) -> None:
    panic_css = ""
    if panic_active:
        panic_css = f"""
      .integrity-bar {{
        border-color: {C['red']} !important;
        animation: panic-flash 0.8s ease-in-out infinite;
      }}"""

    stale_css = ""
    if stale:
        stale_css = f"""
      .integrity-bar {{
        border-bottom-color: {C['yellow']} !important;
        animation: stale-pulse 2s ease-in-out infinite;
      }}"""

    st.markdown(f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

      /* ── Global reset ── */
      div[data-testid='stStatusWidget'] {{ visibility: hidden !important; }}
      html, body, .stApp {{
        background: {C['bg']} !important;
        color: {C['text']};
        font-family: {C['sans']};
      }}
      .block-container {{ padding: 0.5rem 1.0rem 1rem 1.0rem !important; }}
      section[data-testid="stSidebar"] {{ background: {C['surface']}; border-right: 1px solid {C['border']}; }}
      #MainMenu, footer {{ visibility: hidden; }}
      header {{ background: transparent !important; }}
      [data-testid="collapsedControl"] {{
        color: {C['text']} !important;
        background: {C['surface']} !important;
        border-radius: 0 4px 4px 0;
      }}
      div[data-testid="stHorizontalBlock"] {{ gap: 6px !important; }}
      .element-container {{ margin-bottom: 0 !important; }}

      /* ── Integrity bar (top header strip) ── */
      .integrity-bar {{
        display: flex; justify-content: space-between; align-items: center;
        padding: 6px 0 10px 0;
        border-bottom: 1px solid {C['border2']};
        margin-bottom: 8px;
      }}
      {panic_css}
      {stale_css}

      /* ── KPI tiles ── */
      .v26-tile {{
        background: {C['surface']};
        border: 1px solid {C['border']};
        border-radius: 6px;
        padding: 14px 16px 12px 16px;
        margin-bottom: 6px;
        position: relative;
        overflow: hidden;
      }}
      .v26-tile::before {{
        content: '';
        position: absolute; top: 0; left: 0; right: 0; height: 2px;
        background: linear-gradient(90deg, {C['aqua']}60, transparent);
        border-radius: 6px 6px 0 0;
      }}
      .v26-tile-red::before {{
        background: linear-gradient(90deg, {C['red']}60, transparent);
      }}
      .v26-tile-yellow::before {{
        background: linear-gradient(90deg, {C['yellow']}60, transparent);
      }}
      .v26-tile-purple::before {{
        background: linear-gradient(90deg, {C['purple']}60, transparent);
      }}
      .v26-label {{
        font-family: {C['mono']};
        font-size: 0.60rem; font-weight: 600;
        letter-spacing: 0.14em; text-transform: uppercase;
        color: {C['slate']};
        margin-bottom: 4px;
      }}
      .v26-val {{
        font-family: {C['mono']};
        font-size: 1.45rem; font-weight: 700;
        line-height: 1.1;
      }}
      .v26-sub {{
        font-family: {C['mono']};
        font-size: 0.62rem;
        color: {C['text2']};
        margin-top: 4px;
      }}

      /* ── Section headers ── */
      .v26-section {{
        display: flex; align-items: center; gap: 8px;
        font-family: {C['mono']};
        font-size: 0.62rem; font-weight: 700;
        letter-spacing: 0.18em; text-transform: uppercase;
        color: {C['slate']};
        padding: 8px 0 6px 0;
        border-bottom: 1px solid {C['border']};
        margin-bottom: 8px;
      }}
      .v26-section-accent {{ color: {C['aqua']}; }}

      /* ── Progress bar ── */
      .v26-progress-bg {{
        background: {C['surface2']};
        border: 1px solid {C['border']};
        border-radius: 3px;
        height: 8px; width: 100%;
        overflow: hidden;
        margin-top: 6px;
      }}
      .v26-progress-fill {{
        height: 100%; border-radius: 3px;
        transition: width 0.5s ease;
      }}

      /* ── Heartbeat pulse dot ── */
      .hb-live {{
        display: inline-block;
        width: 8px; height: 8px; border-radius: 50%;
        background: {C['aqua']};
        box-shadow: 0 0 8px 2px {C['aqua']};
        animation: hb-beat 1.4s ease-in-out infinite;
        vertical-align: middle;
        margin-right: 5px;
      }}
      .hb-stale {{
        display: inline-block;
        width: 8px; height: 8px; border-radius: 50%;
        background: {C['yellow']};
        box-shadow: 0 0 6px 2px {C['yellow']};
        animation: hb-beat 2.5s ease-in-out infinite;
        vertical-align: middle;
        margin-right: 5px;
      }}
      .hb-dead {{
        display: inline-block;
        width: 8px; height: 8px; border-radius: 50%;
        background: {C['red']};
        vertical-align: middle;
        margin-right: 5px;
      }}
      @keyframes hb-beat {{
        0%, 100% {{ opacity: 1; transform: scale(1); }}
        50%       {{ opacity: 0.5; transform: scale(0.75); }}
      }}
      @keyframes panic-flash {{
        0%, 100% {{ background: {C['panic_bg']}; }}
        50%       {{ background: rgba(255,75,43,0.30); }}
      }}
      @keyframes stale-pulse {{
        0%, 100% {{ opacity: 1; }} 50% {{ opacity: 0.6; }}
      }}

      /* ── Ghost Heal indicator ── */
      .ghost-heal-active {{
        display: inline-flex; align-items: center; gap: 6px;
        padding: 3px 10px; border-radius: 4px;
        background: rgba(168,85,247,0.14);
        border: 1px solid {C['purple']}60;
        font-family: {C['mono']};
        font-size: 0.65rem; font-weight: 700;
        color: {C['purple']};
        animation: ghost-shimmer 1.8s ease-in-out infinite;
      }}
      .ghost-dot {{
        width: 7px; height: 7px; border-radius: 50%;
        background: {C['purple']};
        box-shadow: 0 0 8px 2px {C['purple']};
        animation: ghost-pulse 1.8s ease-in-out infinite;
      }}
      @keyframes ghost-shimmer {{
        0%, 100% {{ opacity: 1; border-color: {C['purple']}60; }}
        50%       {{ opacity: 0.7; border-color: {C['purple']}; }}
      }}
      @keyframes ghost-pulse {{
        0%, 100% {{ transform: scale(1); opacity: 1; }}
        50%       {{ transform: scale(1.4); opacity: 0.6; }}
      }}

      /* ── Risk Interlock badge ── */
      .risk-secured {{
        display: inline-flex; align-items: center; gap: 5px;
        padding: 4px 12px; border-radius: 4px;
        background: rgba(34,211,165,0.12);
        border: 1px solid {C['green']}60;
        font-family: {C['mono']};
        font-size: 0.75rem; font-weight: 800;
        color: {C['green']};
        letter-spacing: 0.10em;
      }}
      .risk-tripped {{
        display: inline-flex; align-items: center; gap: 5px;
        padding: 4px 12px; border-radius: 4px;
        background: {C['panic_bg']};
        border: 1px solid {C['red']}80;
        font-family: {C['mono']};
        font-size: 0.75rem; font-weight: 800;
        color: {C['red']};
        letter-spacing: 0.10em;
        animation: panic-flash 0.9s ease-in-out infinite;
      }}

      /* ── Whale sweep rows ── */
      .whale-row {{
        display: grid;
        grid-template-columns: 55px 70px 120px 80px 70px;
        align-items: center;
        gap: 0;
        padding: 5px 12px;
        border-bottom: 1px solid {C['border']}60;
        font-family: {C['mono']};
        font-size: 0.70rem;
      }}
      .whale-row:last-child {{ border-bottom: none; }}
      .whale-header {{
        background: {C['surface2']};
        border-bottom: 1px solid {C['border2']};
        font-size: 0.58rem;
        letter-spacing: 0.12em;
        color: {C['slate']};
        padding: 5px 12px;
        text-transform: uppercase;
      }}
      .whale-buy  {{ color: {C['aqua']}; font-weight: 700; }}
      .whale-sell {{ color: {C['red']};  font-weight: 700; }}
      .whale-mult {{ color: {C['yellow']}; font-weight: 700; }}

      /* ── Flow Toxicity / VPIN gauge ── */
      .vpin-container {{
        background: {C['surface']};
        border: 1px solid {C['border']};
        border-radius: 6px;
        padding: 14px 16px;
        margin-bottom: 6px;
      }}

      /* ── The Tape ── */
      .tape-container {{
        background: {C['surface']};
        border: 1px solid {C['border']};
        border-radius: 6px;
        overflow: hidden;
        margin-bottom: 6px;
      }}
      .tape-header {{
        background: {C['surface2']};
        border-bottom: 1px solid {C['border2']};
        padding: 6px 14px;
        display: flex; align-items: center; justify-content: space-between;
      }}
      .tape-scroll {{
        max-height: 220px;
        overflow-y: auto;
        scrollbar-width: thin;
        scrollbar-color: {C['border2']} {C['surface']};
      }}
      .tape-line {{
        display: grid;
        grid-template-columns: 80px 55px 1fr;
        gap: 0;
        padding: 3px 14px;
        border-bottom: 1px solid {C['border']}30;
        font-family: {C['mono']};
        font-size: 0.67rem;
        align-items: center;
      }}
      .tape-line:hover {{ background: {C['surface2']}; }}
      .tape-order {{ color: {C['aqua']};   }}
      .tape-heal  {{ color: {C['purple']}; }}
      .tape-veto  {{ color: {C['yellow']}; }}
      .tape-error {{ color: {C['red']};    }}
      .tape-info  {{ color: {C['slate']};  }}
      .tape-time  {{ color: {C['muted']};  }}

      /* ── Connectivity pill ── */
      .conn-pill {{
        display: inline-flex; align-items: center; gap: 4px;
        padding: 2px 8px; border-radius: 10px;
        font-family: {C['mono']};
        font-size: 0.60rem; font-weight: 600;
        letter-spacing: 0.08em;
      }}
      .conn-online  {{ background: rgba(48,213,200,0.12); color: {C['aqua']};   border: 1px solid {C['aqua']}50; }}
      .conn-offline {{ background: rgba(255,75,43,0.12);  color: {C['red']};    border: 1px solid {C['red']}50; }}
      .conn-partial {{ background: rgba(245,158,11,0.12); color: {C['yellow']}; border: 1px solid {C['yellow']}50; }}

      /* ── Tactical sidebar ── */
      .tact-override-on {{
        background: rgba(168,85,247,0.10);
        border: 1px solid {C['purple']}60;
        border-radius: 4px; padding: 6px 10px;
        margin-bottom: 8px;
        font-family: {C['mono']};
        font-size: 0.72rem; font-weight: 800;
        color: {C['purple']}; letter-spacing: 0.06em;
      }}
      .override-pulse {{
        display: inline-block;
        width: 7px; height: 7px; border-radius: 50%;
        background: {C['purple']};
        box-shadow: 0 0 8px 2px {C['purple']};
        animation: ghost-pulse 1.4s ease-in-out infinite;
        margin-right: 5px;
        vertical-align: middle;
      }}

      /* ── Selectbox / dataframe theming ── */
      .stSelectbox > div > div {{
        background: {C['surface']} !important;
        border: 1px solid {C['border']} !important;
        color: {C['text']} !important;
        font-family: {C['mono']} !important;
        font-size: 0.78rem !important;
        border-radius: 4px !important;
      }}
      .stDataFrame {{ border: 1px solid {C['border']}; border-radius: 4px; overflow: hidden; }}
    </style>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB VISIBILITY + AUTOREFRESH  (P25-style)
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_tab_visibility() -> str:
    import streamlit.components.v1 as _components
    _js = """<script>(function(){
        function _getParam(){try{var u=new URL(window.parent.location.href);return u.searchParams.get("tab_state")||"active";}catch(e){return"active";}}
        function _setState(s){try{var u=new URL(window.parent.location.href);if(u.searchParams.get("tab_state")===s)return;u.searchParams.set("tab_state",s);window.parent.history.replaceState(null,"",u.toString());}catch(e){}}
        function _resolve(){var self=document.visibilityState,par="active";try{par=window.parent.document.visibilityState||"active";}catch(e){}return(self==="hidden"||par==="hidden")?"hidden":"active";}
        _setState(_resolve());
        document.addEventListener("visibilitychange",function(){_setState(_resolve());});
        try{window.parent.document.addEventListener("visibilitychange",function(){_setState(_resolve());});}catch(e){}
    })();</script>"""
    _components.html(_js, height=0, scrolling=False)
    _raw = st.query_params.get("tab_state", "active")
    return "active" if _raw not in ("active", "hidden") else _raw


def _install_autorefresh(tab_state: str = "active") -> None:
    interval_ms = 2_000 if tab_state == "active" else 300_000
    try:
        from streamlit_autorefresh import st_autorefresh  # type: ignore
        st_autorefresh(interval=interval_ms, key="v26_autorefresh")
    except ImportError:
        pass


def _handle_tab_transition(current_state: str) -> None:
    prev = st.session_state.get(_SS_TAB_PREV)
    if prev == "hidden" and current_state == "active":
        last = float(st.session_state.get(_SS_SYNC_AT, 0.0))
        if time.time() - last >= _TAB_COOLDOWN_SECS:
            _trigger_gateway_sync()
            st.session_state[_SS_SYNC_AT] = time.time()
    st.session_state[_SS_TAB_PREV] = current_state


def _trigger_gateway_sync() -> None:
    try:
        CONTROL_EVENT_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"event": "reset_gateway", "force_truth": True,
                              "ts": time.time(), "source": "tab_focus_regain"})
        tmp = str(CONTROL_EVENT_PATH) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp, str(CONTROL_EVENT_PATH))
    except Exception as exc:
        log.warning("[v26] trigger_gateway_sync failed: %s", exc)


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════════════════════

def _load_status() -> tuple[dict, bool]:
    REQUIRED = {"p22"}
    if TRADER_STATUS.exists():
        for attempt in range(3):
            try:
                raw = TRADER_STATUS.read_bytes()
                if raw.strip():
                    parsed = json.loads(raw.decode("utf-8"))
                    if isinstance(parsed, dict) and REQUIRED.issubset(parsed.keys()):
                        st.session_state["_v26_lkg_status"] = parsed
                        st.session_state["_v26_lkg_ts"]     = time.time()
                        return parsed, True
            except json.JSONDecodeError:
                if attempt < 2:
                    time.sleep(0.005)
            except Exception:
                break
    cached = st.session_state.get("_v26_lkg_status")
    if cached:
        return cached, False
    return {}, False


def _status_age_secs() -> float:
    try:
        return time.time() - TRADER_STATUS.stat().st_mtime
    except Exception:
        return 9999.0


def _load_veto_logs() -> list[dict]:
    lkg_key = "_v26_lkg_veto"
    def _try():
        if not VETO_AUDIT_PATH.exists():
            return []
        for attempt in range(3):
            try:
                raw = VETO_AUDIT_PATH.read_bytes()
                if not raw.strip():
                    return []
                parsed = json.loads(raw.decode("utf-8"))
                if isinstance(parsed, list):
                    return parsed
                if isinstance(parsed, dict):
                    return parsed.get("vetoes", [])
                return []
            except json.JSONDecodeError:
                if attempt < 2: time.sleep(0.04)
            except Exception:
                break
        return []
    result = _try()
    if result:
        st.session_state[lkg_key] = result
        return result
    return st.session_state.get(lkg_key, [])


@st.cache_resource
def _get_db_conn():
    if not DB_PATH.exists():
        return None
    try:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=9000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn
    except Exception:
        return None


def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = _get_db_conn()
    if conn is None:
        return pd.DataFrame()
    for attempt in range(3):
        try:
            return pd.read_sql_query(sql, conn, params=params)
        except Exception as e:
            if "locked" in str(e).lower() and attempt < 2:
                time.sleep(0.12)
                continue
            break
    return pd.DataFrame()


@st.cache_data(ttl=2)
def _load_trades_raw() -> pd.DataFrame:
    df = _query("""SELECT ts, symbol, side, qty, price, cost_basis,
                          pnl_pct, realized_usd, tag, inst_type
                   FROM trades ORDER BY ts DESC LIMIT 500""")
    if df.empty:
        return df
    df["dt"]           = pd.to_datetime(df["ts"], unit="s")
    df["pnl_pct"]      = pd.to_numeric(df["pnl_pct"],      errors="coerce")
    df["realized_usd"] = pd.to_numeric(df["realized_usd"], errors="coerce")
    df["price"]        = pd.to_numeric(df["price"],        errors="coerce")
    return df


def _load_trades() -> pd.DataFrame:
    lkg = "_v26_lkg_trades"
    try:
        df = _load_trades_raw()
    except Exception:
        df = pd.DataFrame()
    if not df.empty:
        st.session_state[lkg] = df
        return df
    return st.session_state.get(lkg, pd.DataFrame())


@st.cache_data(ttl=5)
def _load_equity_raw() -> pd.DataFrame:
    df = _query("SELECT ts, equity, avail FROM snapshots ORDER BY ts DESC LIMIT 500")
    if not df.empty:
        df = df.sort_values("ts").reset_index(drop=True)
        df["dt"] = pd.to_datetime(df["ts"], unit="s")
        return df
    df = _query("SELECT ts, total_equity AS equity, buying_power AS avail "
                "FROM account_snapshots ORDER BY ts DESC LIMIT 500")
    if df.empty:
        return df
    df = df.sort_values("ts").reset_index(drop=True)
    df["dt"] = pd.to_datetime(df["ts"], unit="s")
    return df


def _load_equity() -> pd.DataFrame:
    lkg = "_v26_lkg_equity"
    try:
        df = _load_equity_raw()
    except Exception:
        df = pd.DataFrame()
    if not df.empty:
        st.session_state[lkg] = df
        return df
    return st.session_state.get(lkg, pd.DataFrame())


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _fig_base(fig: go.Figure, height: int = 240) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=6, r=6, t=24, b=6),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=C["text"], family="JetBrains Mono, monospace", size=9),
        legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=C["border"],
                    font=dict(size=9, family="JetBrains Mono")),
        xaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor=C["border"],
                   showgrid=True, tickfont=dict(size=9, family="JetBrains Mono"),
                   linecolor=C["border"]),
        yaxis=dict(gridcolor="rgba(255,255,255,0.04)", zerolinecolor=C["border"],
                   showgrid=True, tickfont=dict(size=9, family="JetBrains Mono"),
                   linecolor=C["border"]),
    )
    return fig


def _section(label: str, accent: str = "", icon: str = "") -> None:
    accent_span = f'<span class="v26-section-accent">{accent}</span> ' if accent else ""
    icon_span   = f'<span style="font-size:0.80rem;">{icon}</span> ' if icon else ""
    st.markdown(
        f'<div class="v26-section">{icon_span}{accent_span}{label}</div>',
        unsafe_allow_html=True,
    )


def _tile(label: str, value: str, sub: str = "", colour: str = "",
          accent_class: str = "", bar_pct: Optional[float] = None,
          bar_col: str = "") -> None:
    colour  = colour   or C["text"]
    bar_col = bar_col  or C["aqua"]
    bar_html = ""
    if bar_pct is not None:
        safe = max(0.0, min(100.0, float(bar_pct)))
        trip_col = C["red"] if safe > 80 else C["yellow"] if safe > 50 else bar_col
        bar_html = (
            f'<div class="v26-progress-bg">'
            f'<div class="v26-progress-fill" '
            f'style="width:{safe:.1f}%;background:{trip_col};"></div>'
            f'</div>'
            f'<div style="font-family:{C["mono"]};font-size:0.58rem;'
            f'color:{C["text2"]};margin-top:3px;">{safe:.1f}% of threshold</div>'
        )
    st.markdown(f"""
    <div class="v26-tile {accent_class}">
      <div class="v26-label">{label}</div>
      <div class="v26-val" style="color:{colour};">{value}</div>
      <div class="v26-sub">{sub}</div>
      {bar_html}
    </div>""", unsafe_allow_html=True)


def _fmt_usd(v: float, decimals: int = 2) -> str:
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.3f}M"
    if abs(v) >= 1_000:
        return f"${v:,.{decimals}f}"
    return f"${v:.{decimals}f}"


def _latency_colour(lat_us: float) -> str:
    if lat_us < 500:   return C["aqua"]
    if lat_us < 2000:  return C["yellow"]
    return C["red"]


# ═══════════════════════════════════════════════════════════════════════════════
# TACTICAL SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

def _load_tactical_config() -> None:
    SS_KEY = "_v26_tact_loaded"
    if st.session_state.get(SS_KEY):
        return
    cfg = dict(TACT_DEFAULTS)
    try:
        if TACTICAL_CONFIG_PATH.exists():
            raw = TACTICAL_CONFIG_PATH.read_bytes()
            if raw.strip():
                parsed = json.loads(raw.decode("utf-8"))
                if isinstance(parsed, dict):
                    for k in TACT_DEFAULTS:
                        if k in parsed:
                            cfg[k] = bool(parsed[k])
    except Exception:
        pass
    for k, v in cfg.items():
        st.session_state[k] = v
    st.session_state[SS_KEY] = True


def _flush_tactical() -> None:
    cfg = {k: bool(st.session_state.get(k, False)) for k in TACT_DEFAULTS}
    cfg["_written_at"] = datetime.utcnow().isoformat() + "Z"
    try:
        TACTICAL_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(TACTICAL_CONFIG_PATH) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
        os.replace(tmp, str(TACTICAL_CONFIG_PATH))
    except Exception:
        pass


def _any_override() -> bool:
    return any(st.session_state.get(k, False) for k in TACT_DEFAULTS)


def render_tactical_sidebar() -> None:
    _load_tactical_config()
    override_on = _any_override()

    if override_on:
        st.sidebar.markdown(
            f'<div class="tact-override-on">'
            f'<span class="override-pulse"></span>MANUAL OVERRIDE ACTIVE</div>',
            unsafe_allow_html=True,
        )
    else:
        st.sidebar.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.60rem;'
            f'color:{C["muted"]};letter-spacing:0.10em;margin-bottom:8px;">'
            f'ALL OVERRIDES INACTIVE</div>', unsafe_allow_html=True,
        )

    st.sidebar.markdown("### ⚙️ TACTICAL CONTROLS")
    st.sidebar.caption("Changes are written to `tactical_config.json` and read on next bot cycle.")

    prev_ro = st.session_state.get(TACT_KEY_RISK_OFF, False)
    new_ro  = st.sidebar.toggle("🛡 RISK-OFF MODE", value=prev_ro,
                                 key="tact_toggle_risk_off",
                                 help="Reduces ALL position sizing by 50% immediately.")
    if new_ro != prev_ro:
        st.session_state[TACT_KEY_RISK_OFF] = new_ro
        _flush_tactical()
    if new_ro:
        st.sidebar.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.60rem;'
            f'color:{C["yellow"]};margin-top:-4px;margin-bottom:6px;">'
            f'⚠ SIZE × 0.50 — All new entries halved</div>', unsafe_allow_html=True)

    prev_sn = st.session_state.get(TACT_KEY_SNIPER, False)
    new_sn  = st.sidebar.toggle("🎯 SNIPER ONLY", value=prev_sn,
                                  key="tact_toggle_sniper",
                                  help="Whale-sweep signals only. TA entries blocked.")
    if new_sn != prev_sn:
        st.session_state[TACT_KEY_SNIPER] = new_sn
        _flush_tactical()
    if new_sn:
        st.sidebar.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.60rem;'
            f'color:{C["aqua"]};margin-top:-4px;margin-bottom:6px;">'
            f'🐳 WHALE SIGNALS ONLY — TA entries blocked</div>', unsafe_allow_html=True)

    prev_hg = st.session_state.get(TACT_KEY_HEDGE, False)
    new_hg  = st.sidebar.toggle("⚖ HEDGE MODE", value=prev_hg,
                                  key="tact_toggle_hedge",
                                  help="Delta-neutral hedging of open positions.")
    if new_hg != prev_hg:
        st.session_state[TACT_KEY_HEDGE] = new_hg
        _flush_tactical()
    if new_hg:
        st.sidebar.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.60rem;'
            f'color:{C["purple"]};margin-top:-4px;margin-bottom:6px;">'
            f'Δ=0 DELTA-NEUTRAL — Hedging active</div>', unsafe_allow_html=True)

    st.sidebar.markdown("---")
    active_modes = [k.replace("_", "-").upper()
                    for k, v in {TACT_KEY_RISK_OFF: new_ro, TACT_KEY_SNIPER: new_sn,
                                 TACT_KEY_HEDGE: new_hg}.items() if v]
    if active_modes:
        st.sidebar.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.68rem;font-weight:700;'
            f'color:{C["purple"]};letter-spacing:0.08em;">ACTIVE: {" · ".join(active_modes)}</div>',
            unsafe_allow_html=True)
    else:
        st.sidebar.caption("No overrides active — running default algo")
    st.sidebar.caption(f"cfg: `{TACTICAL_CONFIG_PATH.name}`")


# ═══════════════════════════════════════════════════════════════════════════════
# SYSTEM INTEGRITY HEADER
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=2)
def render_integrity_header(status: dict) -> None:
    """
    [v26-INTEGRITY] System Integrity Layer — top bar with:
      · SYSTEM INTEGRITY pulse (replaces LIVE badge)
      · Bridge Latency (µs)
      · Heartbeat Status
      · Current Phase
      · Logic Bridge Connectivity
      · Ghost Heal Indicator (if equity_is_ghost)
    """
    now_ts     = time.time()
    age_secs   = _status_age_secs()
    acct       = status.get("account", {})
    lat_bar    = status.get("p22_latency_bar", {})
    is_ghost   = bool(acct.get("equity_is_ghost", False))
    ghost_cnt  = int(acct.get("consecutive_ghost_reads", 0))
    demo_mode  = bool(status.get("demo_mode", False))

    # Heartbeat classification
    if age_secs < HEARTBEAT_LIVE_SECS:
        hb_dot   = '<span class="hb-live"></span>'
        hb_label = "LIVE"
        hb_col   = C["aqua"]
    elif age_secs < HEARTBEAT_STALE_SECS:
        hb_dot   = '<span class="hb-stale"></span>'
        hb_label = "STALE"
        hb_col   = C["yellow"]
    else:
        hb_dot   = '<span class="hb-dead"></span>'
        hb_label = "DEAD"
        hb_col   = C["red"]

    # Bridge latency — prefer exchange_latency_ms, convert ms → µs
    exc_lat_ms = lat_bar.get("exchange_latency_ms")
    ai_lat_ms  = lat_bar.get("ai_latency_ms")
    if exc_lat_ms is not None:
        lat_us      = float(exc_lat_ms) * 1000
        lat_str     = f"{lat_us:,.0f} µs"
        lat_col     = _latency_colour(lat_us)
        lat_source  = "BRIDGE"
    elif ai_lat_ms is not None:
        lat_us      = float(ai_lat_ms) * 1000
        lat_str     = f"{lat_us:,.0f} µs"
        lat_col     = _latency_colour(lat_us)
        lat_source  = "AI HUB"
    else:
        lat_str    = "— µs"
        lat_col    = C["muted"]
        lat_source = "BRIDGE"

    # Logic Bridge connectivity — check reachable exchanges
    reachable  = status.get("agents", {}).get("arbitrator", {}).get("reachable_exchanges", [])
    if len(reachable) >= 2:
        conn_class = "conn-online"
        conn_label = "MULTI-LEX ONLINE"
    elif len(reachable) == 1:
        conn_class = "conn-partial"
        conn_label = f"{reachable[0].upper()} ONLY"
    else:
        conn_class = "conn-offline"
        conn_label = "BRIDGE OFFLINE"

    # Strategy mode
    strategy  = str(status.get("strategy_mode", "—")).replace("🛡️ ", "").replace(" ", " ")
    demo_html = (
        f'<span style="font-family:{C["mono"]};font-size:0.60rem;font-weight:700;'
        f'color:{C["yellow"]};background:rgba(245,158,11,0.15);padding:2px 6px;'
        f'border-radius:3px;margin-left:6px;">DEMO</span>'
        if demo_mode else ""
    )

    # Ghost heal indicator
    ghost_html = ""
    if is_ghost:
        ghost_html = (
            f'<div class="ghost-heal-active">'
            f'<span class="ghost-dot"></span>'
            f'GHOST STATE · {ghost_cnt} reads · RUST BRIDGE HEALING'
            f'</div>'
        )

    # Render
    ts_str   = datetime.fromtimestamp(now_ts, tz=timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
    up_since = datetime.fromtimestamp(
        float(status.get("timestamp", now_ts)), tz=timezone.utc
    ).strftime("%H:%M:%S")

    st.markdown(f"""
    <div class="integrity-bar">
      <!-- LEFT: identity -->
      <div style="display:flex;align-items:center;gap:14px;">
        <span style="font-family:{C['mono']};font-size:0.72rem;font-weight:800;
                     color:{C['aqua']};letter-spacing:0.18em;">
          ⚡ POWERTRADER AI
        </span>
        <span style="font-family:{C['mono']};font-size:0.62rem;color:{C['muted']};
                     letter-spacing:0.10em;">
          INSTITUTIONAL ELITE · PHASE {PHASE_LABEL}
        </span>
        {demo_html}
      </div>

      <!-- CENTRE: system integrity pulse + ghost -->
      <div style="display:flex;align-items:center;gap:12px;">
        {ghost_html}
        <div style="text-align:center;">
          <div style="font-family:{C['mono']};font-size:0.58rem;font-weight:700;
                      letter-spacing:0.18em;color:{C['slate']};text-transform:uppercase;">
            SYSTEM INTEGRITY
          </div>
          <div style="display:flex;align-items:center;justify-content:center;gap:5px;margin-top:2px;">
            {hb_dot}
            <span style="font-family:{C['mono']};font-size:0.70rem;font-weight:800;
                         color:{hb_col};letter-spacing:0.12em;">{hb_label}</span>
          </div>
        </div>
      </div>

      <!-- RIGHT: latency · heartbeat · connectivity -->
      <div style="display:flex;align-items:center;gap:16px;">
        <div style="text-align:right;">
          <div style="font-family:{C['mono']};font-size:0.56rem;color:{C['muted']};
                      text-transform:uppercase;letter-spacing:0.12em;">{lat_source} LATENCY</div>
          <div style="font-family:{C['mono']};font-size:0.78rem;font-weight:700;
                      color:{lat_col};">{lat_str}</div>
        </div>
        <div style="text-align:right;">
          <div style="font-family:{C['mono']};font-size:0.56rem;color:{C['muted']};
                      text-transform:uppercase;letter-spacing:0.12em;">LAST SNAPSHOT</div>
          <div style="font-family:{C['mono']};font-size:0.68rem;color:{C['slate']};">
            {up_since}
          </div>
        </div>
        <div>
          <span class="conn-pill {conn_class}">
            ● {conn_label}
          </span>
        </div>
        <div style="text-align:right;">
          <div style="font-family:{C['mono']};font-size:0.62rem;color:{C['text2']};">
            {ts_str}
          </div>
          <div style="font-family:{C['mono']};font-size:0.60rem;color:{C['muted']};
                      text-transform:uppercase;letter-spacing:0.10em;">
            {strategy}
          </div>
        </div>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# NAV COMMAND CENTER
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=2)
def render_nav_command_center(status: dict) -> None:
    """
    [v26-NAV] Top-row KPI command center:
      · Total Equity (NAV) — large monospace typography
      · 24h Delta % (equity diff from day-open snapshot in DB)
      · Risk Interlock — SECURED (green) / TRIPPED (red)
      · Drawdown — horizontal progress bar vs 99% Zombie threshold
      · Buying Power / Deployed Capital
    """
    _section("NAV COMMAND CENTER", icon="📊")

    acct     = status.get("account", {})
    dd       = status.get("p7_cb_drawdown", {})
    p22      = status.get("p22", {})
    zombie   = bool(status.get("p20_zombie_mode_active", False))
    cb_trip  = bool(status.get("p7_cb_tripped", False))
    pl       = p22.get("panic_lock", {})
    panic    = bool(pl.get("locked", False))

    total_eq     = float(acct.get("total_equity",   0.0))
    buying_power = float(acct.get("buying_power",   0.0))
    deployed     = float(acct.get("deployed_capital", 0.0))
    avail        = float(acct.get("avail_balance",  0.0))
    dd_pct       = float(dd.get("drawdown_pct",     0.0))
    peak_eq      = float(dd.get("peak_equity",      0.0))

    # 24h delta — try to derive from equity history in DB
    eq_df    = _load_equity()
    delta_24 = None
    if not eq_df.empty and "equity" in eq_df.columns:
        cutoff_ts = time.time() - 86400
        old = eq_df[eq_df["ts"] <= cutoff_ts]
        if not old.empty:
            old_val  = float(old.iloc[-1]["equity"])
            if old_val > 0 and total_eq > 0:
                delta_24 = (total_eq - old_val) / old_val * 100.0

    # Risk Interlock: tripped if zombie AND drawdown > 0, or CB tripped, or panic lock
    risk_tripped = cb_trip or panic or (zombie and dd_pct > 0.5)
    interlock_html = (
        f'<span class="risk-tripped">⚠ RISK INTERLOCK · TRIPPED</span>'
        if risk_tripped else
        f'<span class="risk-secured">✓ RISK INTERLOCK · SECURED</span>'
    )

    # Equity colour
    eq_col = C["aqua"] if total_eq > 1.0 else C["yellow"]
    if total_eq <= 0.01:
        eq_col  = C["muted"]
        eq_disp = "GHOST"
    else:
        eq_disp = _fmt_usd(total_eq)

    # 24h delta display
    if delta_24 is not None:
        d_col  = C["aqua"] if delta_24 >= 0 else C["red"]
        d_sign = "+" if delta_24 >= 0 else ""
        d_html = (
            f'<span style="font-family:{C["mono"]};font-size:0.80rem;'
            f'font-weight:700;color:{d_col};">{d_sign}{delta_24:.2f}% 24h</span>'
        )
    else:
        d_html = (
            f'<span style="font-family:{C["mono"]};font-size:0.72rem;'
            f'color:{C["muted"]};">24h delta: N/A</span>'
        )

    # Drawdown bar percentage relative to zombie threshold
    dd_bar_pct = (dd_pct / ZOMBIE_DRAWDOWN_THRESHOLD_PCT * 100.0) if ZOMBIE_DRAWDOWN_THRESHOLD_PCT > 0 else 0.0
    dd_bar_pct = max(0.0, min(100.0, dd_bar_pct))

    c1, c2, c3, c4 = st.columns([2, 1.4, 1.4, 1.4])

    with c1:
        st.markdown(f"""
        <div class="v26-tile" style="border-color:{C['aqua']}40;">
          <div class="v26-label">TOTAL EQUITY (NAV)</div>
          <div class="v26-val" style="color:{eq_col};font-size:2.0rem;">{eq_disp}</div>
          <div style="margin-top:6px;">{d_html}</div>
          <div style="margin-top:6px;">{interlock_html}</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        _tile(
            "DRAWDOWN VS 99% THRESHOLD",
            f"{dd_pct:.3f}%",
            sub=f"Zombie trip @ {ZOMBIE_DRAWDOWN_THRESHOLD_PCT:.1f}% | Peak {_fmt_usd(peak_eq)}",
            colour=C["red"] if dd_pct > ZOMBIE_DRAWDOWN_THRESHOLD_PCT * 0.7 else C["yellow"] if dd_pct > 0.1 else C["aqua"],
            accent_class="v26-tile-red" if dd_pct > ZOMBIE_DRAWDOWN_THRESHOLD_PCT * 0.7 else "",
            bar_pct=dd_bar_pct,
            bar_col=C["aqua"],
        )

    with c3:
        bp_col = C["aqua"] if buying_power > 100 else C["yellow"]
        _tile("BUYING POWER", _fmt_usd(buying_power),
              sub=f"Avail: {_fmt_usd(avail)}", colour=bp_col)

    with c4:
        dep_pct = float(acct.get("pct_deployed", 0.0))
        # If pct_deployed is absurd (ghost state), show deployed capital directly
        if abs(dep_pct) > 500:
            dep_str = f"{_fmt_usd(deployed)} deployed"
        else:
            dep_str = f"{dep_pct:.1f}%"
        dep_col = C["aqua"] if deployed < buying_power * 0.8 else C["yellow"]
        _tile("DEPLOYED CAPITAL", dep_str,
              sub=f"Raw: {_fmt_usd(deployed)} | Lev ×{acct.get('leverage',1)}",
              colour=dep_col)


# ═══════════════════════════════════════════════════════════════════════════════
# EXECUTION ALPHA — WHALE SNIPER
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=2)
def render_execution_alpha(status: dict) -> None:
    """
    [v26-ALPHA] Execution Alpha section:
      · Whale Sniper — last 5 intercepted sweeps table with multiplier
      · Flow Toxicity (VPIN) — macro_score gauge + per-coin entropy sparkline
    """
    _section("EXECUTION ALPHA", accent="🐳 WHALE SNIPER  ·  📡 FLOW TOXICITY", icon="⚡")

    col_whale, col_vpin = st.columns([1.4, 1])

    # ── WHALE SNIPER INTERCEPT TABLE ──────────────────────────────────────────
    with col_whale:
        st.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.60rem;font-weight:700;'
            f'letter-spacing:0.14em;color:{C["aqua"]};text-transform:uppercase;'
            f'margin-bottom:6px;">🐳 WHALE SWEEP INTERCEPTS</div>',
            unsafe_allow_html=True,
        )

        whale_tape: list[dict] = status.get("p22_whale_tape", [])
        last5 = whale_tape[-5:] if whale_tape else []

        # Sweep active symbols
        sweep_syms  = status.get("p12_sweep_active_symbols", [])
        sweep_badge = ""
        if sweep_syms:
            sweep_badge = (
                f'<span style="font-family:{C["mono"]};font-size:0.60rem;font-weight:700;'
                f'color:{C["red"]};background:rgba(255,75,43,0.12);padding:2px 6px;'
                f'border-radius:3px;animation:panic-flash 1s infinite;">🔴 SWEEP ACTIVE: '
                f'{", ".join(sweep_syms)}</span>'
            )

        if not last5:
            st.markdown(
                f'<div style="background:{C["surface"]};border:1px solid {C["border"]};'
                f'border-radius:6px;padding:24px 16px;text-align:center;'
                f'font-family:{C["mono"]};font-size:0.72rem;color:{C["muted"]};">'
                f'No whale sweeps detected in current session</div>',
                unsafe_allow_html=True,
            )
        else:
            rows_html = ""
            for w in reversed(last5):
                sig     = str(w.get("signal", "")).replace("WHALE_SWEEP_", "")
                is_buy  = "BUY" in sig.upper()
                sym     = str(w.get("symbol", "?"))
                size    = float(w.get("size_usd", 0.0))
                mult    = float(w.get("multiplier", 1.0))
                valid   = bool(w.get("valid", True))
                cancel  = bool(w.get("cancel_buys", False))

                dir_cls = "whale-buy" if is_buy else "whale-sell"
                dir_lbl = "▲ BUY" if is_buy else "▼ SELL"
                v_html  = (
                    f'<span style="color:{C["aqua"]};">✓ VALID</span>'
                    if valid else
                    f'<span style="color:{C["muted"]};">✗ INVALID</span>'
                )
                cancel_html = (
                    f' <span style="color:{C["red"]};">⛔ NO-BUY</span>'
                    if cancel else ""
                )

                rows_html += f"""
                <div class="whale-row">
                  <span class="{dir_cls}">{dir_lbl}</span>
                  <span style="color:{C['text']};font-weight:700;">{sym}</span>
                  <span style="color:{C['slate']};">{_fmt_usd(size, 0)}</span>
                  <span class="whale-mult">×{mult:.2f}</span>
                  <span>{v_html}{cancel_html}</span>
                </div>"""

            st.markdown(f"""
            <div style="background:{C['surface']};border:1px solid {C['border']};border-radius:6px;overflow:hidden;">
              <div class="whale-row whale-header">
                <span>DIR</span><span>SYM</span><span>SIZE USD</span>
                <span>MULT</span><span>STATUS</span>
              </div>
              {rows_html}
            </div>
            """, unsafe_allow_html=True)

        if sweep_badge:
            st.markdown(sweep_badge, unsafe_allow_html=True)

        # RL Brain multipliers per regime
        rl = status.get("agents", {}).get("rl_brain", {})
        if rl:
            regime_gov = status.get("p10_governor", {}).get("regime", {})
            cur_regime = regime_gov.get("personality", "neutral")
            rl_data    = rl.get(cur_regime, {})
            if rl_data:
                rl_mult = float(rl_data.get("multiplier", 1.0))
                rl_wr   = float(rl_data.get("win_rate", 0.0))
                rl_pnl  = float(rl_data.get("total_pnl", 0.0))
                rl_col  = C["aqua"] if rl_mult >= 1.0 else C["red"]
                st.markdown(
                    f'<div style="margin-top:8px;background:{C["surface2"]};'
                    f'border:1px solid {C["border"]};border-radius:4px;padding:8px 12px;'
                    f'display:flex;gap:20px;font-family:{C["mono"]};font-size:0.68rem;">'
                    f'<span style="color:{C["slate"]};">RL BRAIN [{cur_regime.upper()}]</span>'
                    f'<span style="color:{rl_col};font-weight:700;">MULT ×{rl_mult:.2f}</span>'
                    f'<span style="color:{C["slate"]};">WR {rl_wr:.1%}</span>'
                    f'<span style="color:{C["aqua"] if rl_pnl >= 0 else C["red"]};">'
                    f'PnL {_fmt_usd(rl_pnl)}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── FLOW TOXICITY (VPIN) ──────────────────────────────────────────────────
    with col_vpin:
        st.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.60rem;font-weight:700;'
            f'letter-spacing:0.14em;color:{C["aqua"]};text-transform:uppercase;'
            f'margin-bottom:6px;">📡 FLOW TOXICITY (VPIN)</div>',
            unsafe_allow_html=True,
        )

        agents   = status.get("agents", {})
        news     = agents.get("news_wire", {})
        macro_sc = float(news.get("macro_score", 0.0))
        nw_label = str(news.get("label", "—"))
        nw_age   = float(news.get("age_secs", 0.0))
        llm_ok   = bool(news.get("llm_enriched", False))

        # Colour by macro score
        if macro_sc >= 0.15:
            ms_col = C["aqua"]
            ms_lbl = "BULLISH FLOW"
        elif macro_sc <= -0.15:
            ms_col = C["red"]
            ms_lbl = "BEARISH FLOW"
        else:
            ms_col = C["yellow"]
            ms_lbl = "NEUTRAL FLOW"

        # Build Plotly gauge for macro score
        gauge_fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=macro_sc,
            number={"font": {"color": ms_col, "size": 22, "family": "JetBrains Mono"},
                    "suffix": "", "valueformat": ".4f"},
            gauge={
                "axis": {"range": [-1, 1], "tickfont": {"size": 9, "color": C["slate"]},
                         "tickcolor": C["border2"]},
                "bar":  {"color": ms_col, "thickness": 0.28},
                "bgcolor": C["surface2"],
                "bordercolor": C["border"],
                "steps": [
                    {"range": [-1, -0.15], "color": "rgba(255,75,43,0.18)"},
                    {"range": [-0.15, 0.15], "color": "rgba(245,158,11,0.10)"},
                    {"range": [0.15, 1],  "color": "rgba(48,213,200,0.18)"},
                ],
                "threshold": {
                    "line": {"color": C["aqua"], "width": 2},
                    "thickness": 0.75,
                    "value": macro_sc,
                },
            },
            title={"text": f"<span style='font-size:9px;color:{C['slate']};font-family:JetBrains Mono;'>"
                           f"MACRO SCORE · {ms_lbl}</span>",
                   "font": {"size": 9}},
        ))
        gauge_fig.update_layout(
            height=190,
            margin=dict(l=8, r=8, t=30, b=6),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color=C["text"], family="JetBrains Mono"),
        )
        st.plotly_chart(gauge_fig, width="stretch",
                        config={"displayModeBar": False}, key="vpin_gauge")

        # Entropy per coin sparkline
        entropy_map: dict = status.get("p22", {}).get("entropy_per_coin", {})
        positions          = status.get("positions", {})
        entropy_rows       = []
        for sym, edata in entropy_map.items():
            if sym not in positions:
                continue
            ent     = float(edata.get("entropy", 0.0))
            thresh  = float(status.get("p22", {}).get("entropy_threshold", 3.5))
            norm    = min(ent / thresh, 1.0) if thresh > 0 else 0.0
            e_col   = C["aqua"] if norm < 0.40 else C["yellow"] if norm < 0.70 else C["red"]
            bar_w   = int(norm * 80)
            entropy_rows.append(
                f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">'
                f'<span style="font-family:{C["mono"]};font-size:0.62rem;font-weight:700;'
                f'color:{C["text"]};width:32px;">{sym}</span>'
                f'<div style="flex:1;background:{C["surface2"]};border:1px solid {C["border"]};'
                f'border-radius:2px;height:6px;">'
                f'<div style="width:{bar_w}px;height:100%;background:{e_col};'
                f'border-radius:2px;max-width:100%;"></div></div>'
                f'<span style="font-family:{C["mono"]};font-size:0.62rem;'
                f'color:{e_col};width:36px;text-align:right;">{ent:.3f}</span>'
                f'</div>'
            )

        if entropy_rows:
            st.markdown(
                f'<div style="font-family:{C["mono"]};font-size:0.58rem;font-weight:700;'
                f'color:{C["slate"]};text-transform:uppercase;letter-spacing:0.12em;'
                f'margin-bottom:5px;">ENTROPY PER COIN</div>'
                + "".join(entropy_rows),
                unsafe_allow_html=True,
            )

        # News meta
        llm_badge = (
            f'<span style="color:{C["aqua"]};">LLM</span>'
            if llm_ok else
            f'<span style="color:{C["muted"]};">(VADER)</span>'
        )
        st.markdown(
            f'<div style="font-family:{C["mono"]};font-size:0.60rem;color:{C["muted"]};'
            f'margin-top:6px;">News: {nw_label} · Age {nw_age:.0f}s · {llm_badge}</div>',
            unsafe_allow_html=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# POSITION GRID
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=2)
def render_position_grid(status: dict) -> None:
    """Per-symbol execution status: OBI, Tape Velocity, Trust, Signal, Regime."""
    _section("SYMBOL EXECUTION MATRIX", icon="🔬")

    positions = status.get("positions", {})
    if not positions:
        st.caption("No position data available.")
        return

    cols = st.columns(len(positions))
    for col, (sym, pos) in zip(cols, positions.items()):
        with col:
            regime    = str(pos.get("regime", "neutral")).lower()
            r_meta    = REGIME_META.get(regime, REGIME_META["neutral"])
            sig_dir   = str(pos.get("signal_dir", "neutral")).upper()
            sig_conf  = float(pos.get("signal_conf", 0.0))
            sig_prob  = float(pos.get("signal_prob", 0.0))
            obi       = float(pos.get("p12_obi", 0.5))
            vel       = float(pos.get("p12_tape_velocity", 0.0))
            trust     = float(pos.get("p13_agg_trust", 0.5))
            sniper    = bool(pos.get("sniper_ready", False))
            conviction= bool(pos.get("p14_high_conviction", False))
            funding   = float(pos.get("funding_rate", 0.0)) * 100  # to %
            bid       = float(pos.get("current_bid", 0.0))
            ask       = float(pos.get("current_ask", 0.0))
            zombie_sym= bool(pos.get("p20_zombie_mode", False))

            spread_bps = ((ask - bid) / bid * 10000) if bid > 0 else 0.0

            sniper_badge = (
                f'<span style="color:{C["aqua"]};font-size:0.58rem;font-weight:700;">🎯 SNIPER</span>'
                if sniper else ""
            )
            conv_badge = (
                f'<span style="color:{C["yellow"]};font-size:0.58rem;font-weight:700;">⭐ CONVICTION</span>'
                if conviction else ""
            )
            zombie_badge = (
                f'<span style="color:{C["red"]};font-size:0.58rem;">☠ ZOMBIE</span>'
                if zombie_sym else ""
            )

            sig_col = C["aqua"] if "LONG" in sig_dir else C["red"] if "SHORT" in sig_dir else C["slate"]
            obi_col = C["aqua"] if obi >= 0.6 else C["red"] if obi <= 0.4 else C["yellow"]

            st.markdown(f"""
            <div class="v26-tile" style="border-color:{r_meta['col']}40;">
              <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
                <span style="font-family:{C['mono']};font-size:1.0rem;font-weight:800;
                             color:{C['text']};">{sym}</span>
                <span style="font-family:{C['mono']};font-size:0.65rem;font-weight:700;
                             color:{r_meta['col']};">{r_meta['icon']} {r_meta['label']}</span>
              </div>
              <div style="font-family:{C['mono']};font-size:0.68rem;font-weight:700;
                           color:{sig_col};margin-bottom:4px;">
                {sig_dir} · {sig_conf:.0%} · p={sig_prob:.3f}
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;
                          font-family:{C['mono']};font-size:0.63rem;margin-bottom:4px;">
                <span style="color:{C['slate']};">OBI</span>
                <span style="color:{obi_col};font-weight:700;">{obi:.4f}</span>
                <span style="color:{C['slate']};">TAPE VEL</span>
                <span style="color:{C['text']};">{vel:.1f}</span>
                <span style="color:{C['slate']};">TRUST</span>
                <span style="color:{C['aqua']};">{trust:.2f}</span>
                <span style="color:{C['slate']};">FUNDING</span>
                <span style="color:{C['yellow'] if abs(funding)>0.03 else C['text']};">
                  {funding:+.4f}%</span>
                <span style="color:{C['slate']};">SPREAD</span>
                <span style="color:{C['text']};">{spread_bps:.1f} bps</span>
                <span style="color:{C['slate']};">BID</span>
                <span style="color:{C['text']};">{bid:,.4f}</span>
              </div>
              <div style="display:flex;gap:6px;flex-wrap:wrap;">
                {sniper_badge}{conv_badge}{zombie_badge}
              </div>
            </div>
            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# EQUITY CURVE
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=5)
def render_equity_chart() -> None:
    """Equity curve sparkline from DB snapshots."""
    _section("EQUITY CURVE", icon="📈")
    eq_df = _load_equity()
    if eq_df.empty or "equity" not in eq_df.columns:
        st.caption("No equity history in DB — chart will appear once snapshots are recorded.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=eq_df["dt"], y=eq_df["equity"],
        mode="lines",
        line=dict(color=C["aqua"], width=2),
        fill="tozeroy",
        fillcolor=C["glow_a"],
        name="Equity",
        hovertemplate="<b>%{x}</b><br>$%{y:,.2f}<extra></extra>",
    ))
    if "avail" in eq_df.columns:
        fig.add_trace(go.Scatter(
            x=eq_df["dt"], y=eq_df["avail"],
            mode="lines",
            line=dict(color=C["slate"], width=1, dash="dot"),
            name="Avail",
            hovertemplate="<b>%{x}</b><br>$%{y:,.2f}<extra></extra>",
        ))
    _fig_base(fig, height=200)
    st.plotly_chart(fig, width="stretch",
                    config={"displayModeBar": False}, key="equity_curve")


# ═══════════════════════════════════════════════════════════════════════════════
# THE TAPE
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_tape_line(line: str) -> str:
    """Return CSS class for a tape log line based on keywords."""
    low = line.lower()
    if any(k in low for k in ("fill", "order", "buy", "sell", "exec", "trade")):
        return "tape-order"
    if any(k in low for k in ("heal", "ghost", "bridge", "rust")):
        return "tape-heal"
    if any(k in low for k in ("veto", "blocked", "rejected", "entropy", "spread_guard")):
        return "tape-veto"
    if any(k in low for k in ("error", "exception", "failed", "critical", "panic")):
        return "tape-error"
    return "tape-info"


def _parse_tape_from_log(max_lines: int = 60) -> list[tuple[str, str, str]]:
    """
    Read last `max_lines` from executor.log and return list of
    (timestamp, level_label, message) tuples.
    """
    result: list[tuple[str, str, str]] = []
    if not EXECUTOR_LOG_PATH.exists():
        return result
    try:
        with open(EXECUTOR_LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            raw_lines = f.readlines()[-max_lines:]
        for line in raw_lines:
            line = line.strip()
            if not line:
                continue
            # Typical format: "2024-01-01 12:00:00,123 INFO executor - message"
            parts   = line.split(None, 3)
            ts_str  = f"{parts[0]} {parts[1]}" if len(parts) >= 2 else "—"
            ts_disp = ts_str[11:19] if len(ts_str) >= 19 else ts_str
            if len(parts) >= 3:
                lvl = parts[2].strip().rstrip(":")
            else:
                lvl = "INFO"
            msg = parts[3] if len(parts) >= 4 else line
            result.append((ts_disp, lvl, msg))
    except Exception:
        pass
    return result


def _tape_lines_from_status(status: dict) -> list[tuple[str, str, str]]:
    """
    Build synthetic tape lines from status fields when log file unavailable.
    Returns list of (time_str, type, message).
    """
    now    = datetime.now().strftime("%H:%M:%S")
    lines  = []

    # Whale tape events
    for w in reversed(status.get("p22_whale_tape", [])[-5:]):
        sym  = w.get("symbol", "?")
        sig  = str(w.get("signal", "")).replace("WHALE_SWEEP_", "")
        size = float(w.get("size_usd", 0))
        mult = float(w.get("multiplier", 1))
        lines.append((now, "ORDER", f"WHALE {sig} {sym} ${size:,.0f} ×{mult:.2f}"))

    # Ghost state
    acct = status.get("account", {})
    if acct.get("equity_is_ghost"):
        cnt = acct.get("consecutive_ghost_reads", 0)
        lines.append((now, "HEAL", f"GHOST STATE DETECTED · {cnt} reads · Rust Bridge healing…"))

    # Panic lock
    pl = status.get("p22", {}).get("panic_lock", {})
    if pl.get("locked"):
        rem = pl.get("remaining_secs", 0)
        lines.append((now, "VETO", f"PANIC LOCK ACTIVE — {rem:.0f}s remaining"))

    # CB
    if status.get("p7_cb_tripped"):
        lines.append((now, "VETO", "CIRCUIT BREAKER TRIPPED — All entries suspended"))

    # Zombie
    if status.get("p20_zombie_mode_active"):
        lines.append((now, "VETO", "ZOMBIE MODE ACTIVE — Drawdown guard engaged"))

    # Flash crash
    sentinel = status.get("p11_sentinel", {})
    if sentinel.get("flash_crash_fired"):
        sym = sentinel.get("flash_crash_symbol", "?")
        lines.append((now, "ERROR", f"FLASH CRASH DETECTED: {sym}"))

    # Recent trades hint
    p16 = status.get("p16_last_express_by_sym", {})
    for sym, ago in p16.items():
        lines.append((now, "ORDER", f"EXPRESS FILL {sym} · {ago:.0f}s ago"))

    # HFT brakes
    for sym in status.get("p12_hft_brake_symbols", []):
        lines.append((now, "VETO", f"HFT BRAKE ACTIVE: {sym}"))

    return lines


TYPE_CSS = {
    "ORDER": "tape-order",
    "HEAL":  "tape-heal",
    "VETO":  "tape-veto",
    "ERROR": "tape-error",
    "INFO":  "tape-info",
    "DEBUG": "tape-info",
    "WARNING": "tape-veto",
    "CRITICAL": "tape-error",
}


@st.fragment(run_every=1)
def render_the_tape(status: dict) -> None:
    """
    [v26-TAPE] The Tape — fast-scrolling monospaced execution log.
    Reads from executor.log; falls back to synthesising lines from status JSON.
    """
    _section("THE TAPE — EXECUTION LOG", icon="📟")

    # Prefer real log file; fallback to status synthesis
    log_lines = _parse_tape_from_log(max_lines=80)
    use_synth = not log_lines
    if use_synth:
        synth     = _tape_lines_from_status(status)
        log_lines = synth if synth else [
            (datetime.now().strftime("%H:%M:%S"), "INFO",
             f"No executor.log found at {EXECUTOR_LOG_PATH} — awaiting first cycle")
        ]

    # Build HTML rows (newest first)
    rows_html = ""
    for ts_str, level, msg in reversed(log_lines[-60:]):
        css   = TYPE_CSS.get(level.upper(), "tape-info")
        lvl_s = level[:5].upper().ljust(5)
        # Truncate very long lines
        msg_s = msg[:160] + "…" if len(msg) > 160 else msg
        rows_html += (
            f'<div class="tape-line">'
            f'<span class="tape-time">{ts_str}</span>'
            f'<span class="{css}" style="font-size:0.60rem;">[{lvl_s}]</span>'
            f'<span style="color:{C["text"]};font-size:0.67rem;">{msg_s}</span>'
            f'</div>'
        )

    src_badge = (
        f'<span style="font-family:{C["mono"]};font-size:0.58rem;color:{C["muted"]};">'
        f'{"executor.log" if not use_synth else "status.json (synthetic)"}</span>'
    )
    auto_scroll_js = """<script>
    (function(){var s=document.getElementById('tape-scroll-v26');
    if(s){s.scrollTop=s.scrollHeight;}})();
    </script>"""

    st.markdown(f"""
    <div class="tape-container">
      <div class="tape-header">
        <span style="font-family:{C['mono']};font-size:0.62rem;font-weight:700;
                     color:{C['aqua']};letter-spacing:0.12em;">
          ◉ LIVE FEED
        </span>
        {src_badge}
      </div>
      <div class="tape-scroll" id="tape-scroll-v26">
        {rows_html}
      </div>
    </div>
    {auto_scroll_js}
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# REGIME + MACRO PULSE
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=5)
def render_regime_pulse(status: dict) -> None:
    """Regime personality, vol/trend scores, and per-symbol regime overview."""
    _section("REGIME INTELLIGENCE", icon="🌡")

    governor = status.get("p10_governor", {})
    regime   = governor.get("regime", {})
    pers     = str(regime.get("personality", "neutral")).lower()
    vol_sc   = float(regime.get("vol_score",   0.0))
    trend_sc = float(regime.get("trend_score", 0.0))
    n_syms   = int(regime.get("n_symbols", 0))
    r_meta   = REGIME_META.get(pers, REGIME_META["neutral"])

    allocs   = governor.get("allocations", {})
    positions = status.get("positions", {})

    col_regime, col_alloc = st.columns([1, 1.6])

    with col_regime:
        st.markdown(f"""
        <div class="v26-tile" style="border-color:{r_meta['col']}50;text-align:center;
             background:linear-gradient(135deg,{C['surface']},{C['surface2']});">
          <div style="font-size:2.5rem;line-height:1;">{r_meta['icon']}</div>
          <div style="font-family:{C['mono']};font-size:0.90rem;font-weight:800;
                      color:{r_meta['col']};letter-spacing:0.12em;margin:6px 0 2px;">
            {r_meta['label']}
          </div>
          <div style="font-family:{C['mono']};font-size:0.62rem;color:{C['slate']};
                      margin-bottom:10px;">{n_syms} symbols tracked</div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;text-align:left;">
            <div>
              <div class="v26-label">VOL SCORE</div>
              <div style="font-family:{C['mono']};font-size:0.80rem;font-weight:700;
                          color:{C['yellow']};">{vol_sc:.4f}</div>
            </div>
            <div>
              <div class="v26-label">TREND SCORE</div>
              <div style="font-family:{C['mono']};font-size:0.80rem;font-weight:700;
                          color:{C['aqua']};">{trend_sc:.4f}</div>
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

    with col_alloc:
        alloc_rows = ""
        for sym, adata in allocs.items():
            if sym not in positions:
                continue
            pos     = positions[sym]
            tw      = float(adata.get("target_weight", 0.0))
            mx      = float(adata.get("max_usd", 0.0))
            rl_c    = float(adata.get("rl_confidence", 0.5))
            cp      = float(adata.get("corr_penalty", 0.0))
            sym_reg = str(pos.get("regime", "neutral")).lower()
            sr_meta = REGIME_META.get(sym_reg, REGIME_META["neutral"])
            sniper_r= bool(pos.get("sniper_ready", False))
            snip_lbl= "🎯" if sniper_r else " "

            alloc_rows += f"""
            <div style="display:grid;grid-template-columns:50px 70px 90px 70px 70px 24px;
                        align-items:center;gap:0;padding:5px 10px;
                        border-bottom:1px solid {C['border']}40;
                        font-family:{C['mono']};font-size:0.68rem;">
              <span style="font-weight:700;color:{C['text']};">{sym}</span>
              <span style="color:{sr_meta['col']};">{sr_meta['icon']} {sym_reg.upper()}</span>
              <span style="color:{C['aqua']};">{tw:.1%} weight</span>
              <span style="color:{C['slate']};">Max {_fmt_usd(float(pos.get('p10_gov_max_usd', 0)), 0)}</span>
              <span style="color:{C['yellow']};">RL {rl_c:.2f}</span>
              <span>{snip_lbl}</span>
            </div>"""

        st.markdown(f"""
        <div style="background:{C['surface']};border:1px solid {C['border']};
             border-radius:6px;overflow:hidden;">
          <div style="background:{C['surface2']};border-bottom:1px solid {C['border2']};
               padding:5px 10px;display:grid;
               grid-template-columns:50px 70px 90px 70px 70px 24px;gap:0;
               font-family:{C['mono']};font-size:0.58rem;font-weight:700;
               letter-spacing:0.12em;color:{C['slate']};text-transform:uppercase;">
            <span>SYM</span><span>REGIME</span><span>WEIGHT</span>
            <span>MAX USD</span><span>RL CONF</span><span></span>
          </div>
          {alloc_rows if alloc_rows else
           f'<div style="padding:16px;font-family:{C["mono"]};font-size:0.72rem;color:{C["muted"]};">No allocation data</div>'}
        </div>
        """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MTF SIGNAL PANEL
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=3)
def render_mtf_signals(status: dict) -> None:
    """Multi-timeframe signal table: direction, prob, z-score, confidence per sym/tf."""
    _section("MULTI-TIMEFRAME SIGNAL ARRAY", icon="📡")

    mtf_data: dict = status.get("p22_mtf_signals", {})
    if not mtf_data:
        # Synthesize from positions signal
        positions = status.get("positions", {})
        for sym, pos in positions.items():
            mtf_data[sym] = {}

    if not mtf_data:
        st.caption("No MTF signal data available.")
        return

    timeframes = ["1hour", "4hour", "1day"]
    header = (
        f'<div style="display:grid;grid-template-columns:60px {"100px " * len(timeframes)};'
        f'gap:0;background:{C["surface2"]};border-bottom:1px solid {C["border2"]};'
        f'padding:5px 12px;font-family:{C["mono"]};font-size:0.58rem;'
        f'font-weight:700;letter-spacing:0.12em;color:{C["slate"]};text-transform:uppercase;">'
        f'<span>SYM</span>'
        + "".join(f'<span>{tf}</span>' for tf in timeframes)
        + '</div>'
    )

    rows_html = ""
    for sym, tf_map in mtf_data.items():
        if not isinstance(tf_map, dict):
            continue
        cells = f'<span style="font-family:{C["mono"]};font-size:0.72rem;font-weight:800;color:{C["text"]};">{sym}</span>'
        for tf in timeframes:
            td = tf_map.get(tf, {})
            if not td:
                cells += f'<span style="font-family:{C["mono"]};font-size:0.60rem;color:{C["muted"]};">—</span>'
                continue
            direction = str(td.get("direction", "neutral")).upper()
            prob      = float(td.get("prob", 0.5))
            regime    = str(td.get("regime", "")).upper()
            z         = float(td.get("z_score", 0.0))
            conf      = float(td.get("confidence", 0.0))
            dir_col   = C["aqua"] if direction == "LONG" else C["red"] if direction == "SHORT" else C["slate"]
            cells += (
                f'<div style="font-family:{C["mono"]};font-size:0.62rem;">'
                f'<div style="color:{dir_col};font-weight:700;">{direction}</div>'
                f'<div style="color:{C["text2"]};">p={prob:.3f}</div>'
                f'<div style="color:{C["muted"]};">z={z:+.2f} c={conf:.2f}</div>'
                f'</div>'
            )
        rows_html += (
            f'<div style="display:grid;grid-template-columns:60px {"100px " * len(timeframes)};'
            f'gap:0;padding:6px 12px;border-bottom:1px solid {C["border"]}40;'
            f'align-items:start;">{cells}</div>'
        )

    st.markdown(
        f'<div style="background:{C["surface"]};border:1px solid {C["border"]};'
        f'border-radius:6px;overflow:hidden;">{header}{rows_html}</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# VETO AUDIT
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=3)
def render_veto_audit() -> None:
    """[v26-TAPE] Recent veto audit entries from veto_audit.json."""
    _section("VETO AUDIT TRAIL", icon="🛡")

    vetoes = _load_veto_logs()
    if not vetoes:
        st.caption("No veto records found — veto_audit.json empty or not yet written.")
        return

    last10 = vetoes[-10:]
    rows_html = ""
    for v in reversed(last10):
        ts    = v.get("ts", v.get("timestamp", None))
        # ── [P40.1-PARSER] Robust dual-format timestamp parsing ─────────────────
        # Requirement:
        #   • If ts is a string containing "T" or "Z" → datetime.fromisoformat()
        #   • If ts is numeric (string or float)     → datetime.fromtimestamp()
        # Any invalid value falls back to the "—" sentinel.
        if ts is None or ts == "" or ts == 0:
            ts_s = "—"
        else:
            try:
                if isinstance(ts, str) and ("T" in ts or "Z" in ts):
                    _iso = ts
                    # fromisoformat() doesn't accept a bare trailing "Z" on some
                    # Python versions — convert to an explicit UTC offset.
                    if _iso.endswith("Z"):
                        _iso = _iso[:-1] + "+00:00"
                    dt = datetime.fromisoformat(_iso)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    ts_s = dt.astimezone(timezone.utc).strftime("%H:%M:%S")
                else:
                    ts_s = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%H:%M:%S")
            except Exception:
                ts_s = "—"
        # ── [/P40.1-PARSER] ──────────────────────────────────────────────────
        reason= str(v.get("reason", v.get("guard", "—")))
        sym   = str(v.get("symbol", "?"))
        side  = str(v.get("side", "?")).upper()
        msg   = str(v.get("message", ""))[:80]
        rows_html += (
            f'<div style="display:grid;grid-template-columns:68px 45px 60px 1fr;'
            f'gap:0;padding:4px 12px;border-bottom:1px solid {C["border"]}30;'
            f'font-family:{C["mono"]};font-size:0.67rem;align-items:center;">'
            f'<span style="color:{C["muted"]};">{ts_s}</span>'
            f'<span style="color:{C["text"]};font-weight:700;">{sym}</span>'
            f'<span style="color:{C["yellow"]};">{side}</span>'
            f'<span style="color:{C["text2"]};">{reason} {msg}</span>'
            f'</div>'
        )

    st.markdown(
        f'<div style="background:{C["surface"]};border:1px solid {C["border"]};'
        f'border-radius:6px;overflow:hidden;">'
        f'<div style="background:{C["surface2"]};border-bottom:1px solid {C["border2"]};'
        f'padding:5px 12px;display:grid;grid-template-columns:68px 45px 60px 1fr;gap:0;'
        f'font-family:{C["mono"]};font-size:0.58rem;font-weight:700;'
        f'letter-spacing:0.12em;color:{C["slate"]};text-transform:uppercase;">'
        f'<span>TIME</span><span>SYM</span><span>SIDE</span><span>REASON</span></div>'
        f'{rows_html}</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# NEWS HEADLINES
# ═══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=10)
def render_news_wire(status: dict) -> None:
    """News Wire agent headlines."""
    _section("NEWS WIRE", icon="📰")
    news = status.get("agents", {}).get("news_wire", {})
    headlines: list[str] = news.get("top_headlines", [])
    bearish   = bool(news.get("bearish_block", False))
    macro_sc  = float(news.get("macro_score", 0.0))

    if not headlines:
        st.caption("No headlines available.")
        return

    bear_banner = ""
    if bearish:
        bear_banner = (
            f'<div style="background:rgba(255,75,43,0.12);border:1px solid {C["red"]}60;'
            f'border-radius:4px;padding:6px 12px;margin-bottom:8px;'
            f'font-family:{C["mono"]};font-size:0.68rem;font-weight:700;color:{C["red"]};">'
            f'⛔ BEARISH BLOCK ACTIVE — Macro score {macro_sc:.4f} · Entries suppressed'
            f'</div>'
        )

    hl_rows = "".join(
        f'<div style="padding:5px 0;border-bottom:1px solid {C["border"]}30;'
        f'font-family:{C["mono"]};font-size:0.68rem;color:{C["text2"]};">'
        f'<span style="color:{C["muted"]};margin-right:6px;">▸</span>{h}</div>'
        for h in headlines
    )

    st.markdown(
        f'{bear_banner}'
        f'<div style="background:{C["surface"]};border:1px solid {C["border"]};'
        f'border-radius:6px;padding:8px 14px;">{hl_rows}</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════════════

def render_footer(data_source: str, tab_state: str) -> None:
    st.markdown(
        f'<p style="font-family:{C["mono"]};font-size:0.58rem;color:{C["muted"]};'
        f'text-align:right;margin-top:10px;padding-top:6px;'
        f'border-top:1px solid {C["border"]};">'
        f'⚡ POWERTRADER AI · INSTITUTIONAL ELITE v26 · LOGIC EXECUTION BRIDGE · '
        f'ALADDIN AESTHETIC · GHOST HEAL · SHADOW BUFFER · FRAGMENT ISOLATION · '
        f'TAB-AWARE REFRESH · ZERO-FLICKER — '
        f'{datetime.now().strftime("%H:%M:%S")} · tab={tab_state} · src={data_source}</p>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # ── Phase 1: Tab visibility ────────────────────────────────────────────────
    _tab_state = _detect_tab_visibility()

    # ── Phase 2: Adaptive autorefresh ─────────────────────────────────────────
    _install_autorefresh(tab_state=_tab_state)

    # ── Phase 3: Tab transition sync ──────────────────────────────────────────
    _handle_tab_transition(_tab_state)

    # ── Phase 4: Shadow-buffer status load ────────────────────────────────────
    _raw_status, _is_live = _load_status()

    if _raw_status:
        # ── [v26-SHADOW][v26-GHOST] Stabilize Ghost/Zombie integrity flags ────
        # During bridge reconnects the JSON writer can momentarily omit or
        # transiently flip flags for a single fragment tick. We hard-bind UI
        # indicators to executor truth fields:
        #   • account.equity_is_ghost (Rust bridge ghost-state)
        #   • p20_zombie_mode_active (executor._p20_zombie_mode)
        # To eliminate millisecond flicker, we also apply a short latch.
        try:
            _shadow_prev = st.session_state.get(_SS_SHADOW_STATUS, {}) or {}
            now = time.time()

            # Extract raw flags (may be missing).
            acct = _raw_status.get("account", {}) if isinstance(_raw_status, dict) else {}
            prev_acct = _shadow_prev.get("account", {}) if isinstance(_shadow_prev, dict) else {}
            raw_is_ghost = acct.get("equity_is_ghost", prev_acct.get("equity_is_ghost", False))
            raw_ghost_cnt = acct.get("consecutive_ghost_reads", prev_acct.get("consecutive_ghost_reads", 0))

            raw_zombie = _raw_status.get("p20_zombie_mode_active", _shadow_prev.get("p20_zombie_mode_active", False))

            # Ghost latch (2s) — hold last True briefly to suppress flicker.
            ghost_until = float(st.session_state.get("v26_ghost_latch_until", 0.0) or 0.0)
            if bool(raw_is_ghost):
                ghost_until = now + 2.0
            is_ghost = bool(raw_is_ghost) or (now < ghost_until)
            st.session_state["v26_ghost_latch_until"] = ghost_until

            # Zombie latch (2s) — hold True briefly to suppress flicker.
            zombie_until = float(st.session_state.get("v26_zombie_latch_until", 0.0) or 0.0)
            if bool(raw_zombie):
                zombie_until = now + 2.0
            zombie_active = bool(raw_zombie) or (now < zombie_until)
            st.session_state["v26_zombie_latch_until"] = zombie_until

            # Apply stabilized flags back into status blob.
            if isinstance(_raw_status, dict):
                _raw_status.setdefault("account", {})
                if isinstance(_raw_status["account"], dict):
                    _raw_status["account"]["equity_is_ghost"] = is_ghost
                    _raw_status["account"]["consecutive_ghost_reads"] = int(raw_ghost_cnt or 0)
                _raw_status["p20_zombie_mode_active"] = zombie_active
        except Exception:
            pass

        st.session_state[_SS_SHADOW_STATUS] = _raw_status
        status      = _raw_status
        data_source = "live" if _is_live else "retained"
    else:
        _shadow = st.session_state.get(_SS_SHADOW_STATUS)
        if _shadow:
            status      = _shadow
            data_source = "retained"
        else:
            # Boot screen — no data yet
            _inject_css()
            st.markdown(
                "<style>div[data-testid='stStatusWidget']{visibility:hidden}</style>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div style="text-align:center;padding:80px 20px;">'
                f'<div style="font-family:{C["mono"]};font-size:1.4rem;font-weight:800;'
                f'color:{C["aqua"]};letter-spacing:0.18em;">⚡ POWERTRADER AI</div>'
                f'<div style="font-family:{C["mono"]};font-size:0.72rem;color:{C["slate"]};'
                f'margin-top:8px;letter-spacing:0.12em;">INSTITUTIONAL ELITE v26 · LOGIC EXECUTION BRIDGE</div>'
                f'<div style="font-family:{C["mono"]};font-size:0.70rem;color:{C["muted"]};'
                f'margin-top:20px;">⏳ Awaiting first status snapshot…</div>'
                f'<div style="font-family:{C["mono"]};font-size:0.62rem;color:{C["muted"]};'
                f'margin-top:6px;">Expected: <code>{TRADER_STATUS}</code></div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            return

    # ── CSS ───────────────────────────────────────────────────────────────────
    p22          = status.get("p22", {})
    pl           = p22.get("panic_lock", {})
    panic_active = bool(pl.get("locked", False))
    stale        = _status_age_secs() > STATUS_STALE_SECS
    _inject_css(panic_active=panic_active, stale=stale)

    # ── Sidebar ───────────────────────────────────────────────────────────────
    render_tactical_sidebar()

    # ═══════════════════════════════════════════════════════════════════════════
    # RENDER PIPELINE
    # ═══════════════════════════════════════════════════════════════════════════

    # ── [v26-INTEGRITY] System Integrity Header ───────────────────────────────
    render_integrity_header(status)

    # ── [v26-NAV] NAV Command Center ──────────────────────────────────────────
    render_nav_command_center(status)

    st.markdown(
        f'<hr style="border:none;border-top:1px solid {C["border"]};margin:6px 0;">',
        unsafe_allow_html=True,
    )

    # ── [v26-ALPHA] Execution Alpha ───────────────────────────────────────────
    render_execution_alpha(status)

    st.markdown(
        f'<hr style="border:none;border-top:1px solid {C["border"]};margin:6px 0;">',
        unsafe_allow_html=True,
    )

    # ── Position Grid ─────────────────────────────────────────────────────────
    render_position_grid(status)

    st.markdown(
        f'<hr style="border:none;border-top:1px solid {C["border"]};margin:6px 0;">',
        unsafe_allow_html=True,
    )

    # ── Regime + MTF (side by side) ───────────────────────────────────────────
    col_reg, col_mtf = st.columns([1, 1.6])
    with col_reg:
        render_regime_pulse(status)
    with col_mtf:
        render_mtf_signals(status)

    st.markdown(
        f'<hr style="border:none;border-top:1px solid {C["border"]};margin:6px 0;">',
        unsafe_allow_html=True,
    )

    # ── Equity curve ──────────────────────────────────────────────────────────
    render_equity_chart()

    st.markdown(
        f'<hr style="border:none;border-top:1px solid {C["border"]};margin:6px 0;">',
        unsafe_allow_html=True,
    )

    # ── Veto Audit + News Wire ────────────────────────────────────────────────
    col_veto, col_news = st.columns([1.2, 1])
    with col_veto:
        render_veto_audit()
    with col_news:
        render_news_wire(status)

    st.markdown(
        f'<hr style="border:none;border-top:1px solid {C["border"]};margin:6px 0;">',
        unsafe_allow_html=True,
    )

    # ── [v26-TAPE] The Tape ───────────────────────────────────────────────────
    render_the_tape(status)

    # ── Footer ────────────────────────────────────────────────────────────────
    render_footer(data_source=data_source, tab_state=_tab_state)


if __name__ == "__main__":
    main()
