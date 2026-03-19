"""
dashboard.py  —  Phase 53.0 PowerTrader AI  ·  Obsidian Terminal
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CHANGELOG — POST-ROADMAP TRACK 32: REPLAY ANALYTICS — DIRECTION FILTER + NEAR-MISS ENRICHMENT
═══════════════════════════════════════════════════════════════════════════════════════════════
[TRACK32-RA1] _load_near_miss_blocked(): new optional `direction` parameter (default "").
               When non-empty, appends AND direction = ? to the WHERE clause so the lookup
               returns only blocked/shadow decisions that match the trade's entry direction.
               When empty or absent, the query is identical to the pre-Track-32 baseline —
               fully backward-compatible with older rows that have no direction recorded.

               _nm_rows builder in render_trade_replay() now stores
               (symbol, entry_ts, direction) triples instead of (symbol, entry_ts) pairs.
               direction is taken from entry_direction in _display, normalised to uppercase,
               and is "" when the column is absent or null (preserving existing behavior).

               _load_near_miss_blocked() is called with direction=_nm_dir_filter so the
               near-miss table shows only decisions evaluated for the same direction as the
               winning admission.

[TRACK32-RA2] _load_near_miss_blocked(): SELECT enriched with five additional columns that
               are present in the original decision_trace CREATE TABLE (no migration needed):
                 regime, kelly_f, sizing_usd, risk_off, conviction_mult.
               Existing columns unchanged: ts, outcome, gate_name, reason, confidence,
               z_score, llm_verdict, p32_p_success, direction.

               Near-miss table render updated:
                 - Two new columns after DIR: REGIME (colour-coded bull/bear/chop)
                   and KELLY (entry kelly_f, mono font).
                 - One new column after Z: SIZE (sizing_usd as "$N", muted when 0).
                 - risk_off and conviction_mult as compact inline badges inside GATE cell:
                   [R-OFF] (orange) when risk_off truthy; [Cx N.Nx] (cyan) when
                   conviction_mult > 1.0.
               Header: TIME|OUTCOME|GATE|DIR|REGIME|CONF|Z|KELLY|SIZE|LLM|P32|REASON
               Table already uses overflow-x:auto — added width handled gracefully.
               All new fields degrade to "—" / hidden when absent or null.

               TRACK11-NM expander identity and key "rt_nm_trade" unchanged.
               TRACK26-LC3 expander entirely untouched.

CHANGELOG — POST-ROADMAP TRACK 31: REGIME / DIRECTION WRITEBACK REFINEMENT
═══════════════════════════════════════════════════════════════════════════════
[TRACK31-RR2] render_policy_state() Section 7 RL Regime Weight Controls sub-
               section appended immediately after the existing RL regime weight
               table display (inside the same try/except block).  Operator can
               select one of (bull / bear / chop) and click "Reset Selected
               Regime" to fire a "reset_regime_weight" event into the P24
               control_queue.jsonl channel, or click "Reset All Regimes" to
               fire "reset_all_regime_weights".  Executor handles both events
               via the [TRACK31-RR1] handlers and dispatches to the existing
               IntelligenceEngine.reset_regime_weight() /
               reset_all_weights() methods.  On the next status refresh the
               RL REGIME WEIGHT TABLE above reflects the reset.

[TRACK31-RR3] Write-safety mirrors Track 07 Section 8 exactly:
               _wb_writes_ok = _bot_is_live() and not _is_demo_mode().
               Both buttons carry disabled=not _wb_writes_ok.  Write-gate
               warning tiles (BOT OFFLINE / DEMO MODE) shown when writes are
               blocked.  _write_control_event() used — no direct filesystem
               writes from the dashboard.  _audit() called on each button
               press.  Entire new block wrapped in try/except — a failure in
               the reset controls must never break the existing RL table render.

CHANGELOG — POST-ROADMAP TRACK 27: ADVANCED PORTFOLIO / EXPOSURE REFINEMENT
═══════════════════════════════════════════════════════════════════════════════
[TRACK27-PSC2] render_policy_state(): one new entry appended to _GATE_DEFS:
               ("PER_SYMBOL_CAP", "max_per_symbol_usd", ...) so the new gate
               appears in the same gate enable/disable summary table as all
               other operator-configurable gates.  No other change to
               render_policy_state().
[TRACK27-PSC3] render_positions_panel(): Gate-configuration footnote under
               Σ NOTIONAL tile extended with a PER_SYMBOL CAP badge.  Reads
               admission_policy["max_per_symbol_usd"] (gate threshold) and
               admission_policy["current_per_symbol_usd"] (dict of symbol →
               current usd_cost).  Badge hidden (shows "OFF") when gate is
               disabled (0.0).  When enabled, shows threshold and the highest
               per-symbol usage found across open positions, colour-coded:
               yellow when any symbol is ≥90% of threshold, green otherwise.
               Existing CONCURRENT CAP and DEPLOYED CAP badges are unchanged.

CHANGELOG — POST-ROADMAP TRACK 26: RICHER LIFECYCLE REVIEW
═══════════════════════════════════════════════════════════════════════════════
[TRACK26-LC1] Two new module-level lookup-window constants placed immediately
               after the existing _CORR18_* constants:
                 _LC26_ENTRY_LOOKBACK_SECS: int = 120
                   How far before trades.entry_ts to search for the ADMITTED
                   decision_trace row (admission fires before OKX fill).
                 _LC26_CLOSE_SEARCH_SECS: int = 60
                   Half-window around trades.close_ts for the CLOSED
                   decision_trace row (close trace fires near the fill ts).

[TRACK26-LC2] New @st.cache_data(ttl=CACHE_FAST) loader
               _load_trade_lifecycle(symbol, direction, entry_ts, close_ts)
               placed immediately before render_trade_replay().
               Anchors the lookup from the trade side, not the decision side.
               Never raises.  Returns:
                 {"entry_found": bool,
                  "entry_ctx":   dict | None,
                  "close_found": bool,
                  "close_ctx":   dict | None}
               Returns {"entry_found": False, "entry_ctx": None,
                        "close_found": False, "close_ctx": None}
               on all failure paths (missing table, bad params, any exception).

               ADMITTED lookup:
                 window: [entry_ts − _LC26_ENTRY_LOOKBACK_SECS,
                          entry_ts + 10.0]
                 match:  symbol + direction + outcome='ADMITTED'
                 order:  ts DESC LIMIT 1  (nearest prior admission)
                 Two-tier query fallback:
                   Tier 1: with source_path column (Track-16+ DB)
                   Tier 2: NULL AS source_path (Track-10 DB)
                 entry_ctx fields:
                   ts, confidence, z_score, kelly_f, gate_name, source_path,
                   sizing_usd, llm_verdict, risk_off, conviction_mult, regime

               CLOSED lookup:
                 window: [close_ts − _LC26_CLOSE_SEARCH_SECS,
                          close_ts + _LC26_CLOSE_SEARCH_SECS]
                 match:  symbol, source_path='close' (Tier 1),
                         or outcome='CLOSED' (Tier 2 fallback)
                 order:  ABS(ts − close_ts) ASC LIMIT 1
                 Two-tier query fallback:
                   Tier 1: source_path='close' + pnl_pct + hold_time_secs
                           (Track-17+ DB)
                   Tier 2: outcome='CLOSED', pnl/hold as NULL
                           (Track-10/pre-17 DB)
                 close_ctx fields:
                   ts, gate_name, pnl_pct, hold_time_secs

               Both lookups are independent: a missing ADMITTED row does not
               prevent the CLOSED lookup from running.
               No fuzzy heuristics, no confidence/regime matching.
               No DB writes.  No schema changes.  No filesystem writes.

[TRACK26-LC3] New collapsed expander "📋 Trade lifecycle — entry & exit
               decision context" added at the end of render_trade_replay(),
               strictly after the existing [TRACK11-NM] near-miss expander.
               The [TRACK11-NM] expander is byte-stable and untouched.

               Render guard: shown only when _nm_options is non-empty AND
               decision_trace is in _get_known_tables().

               Trade selector: st.selectbox(key="rt_lc_trade") — distinct
               from near-miss key "rt_nm_trade".  Same label list as near-
               miss but built from rows that also have a valid close_ts.
               Rows without a usable close_ts are excluded from this selector.

               On trade selection: calls _load_trade_lifecycle() for the
               selected (symbol, direction, entry_ts, close_ts).

               Renders two st.columns:
               Left — ENTRY CONTEXT (from ADMITTED row):
                 - ts offset: "±Xs vs entry" (signed seconds)
                 - source badge: ENT / EXP / ? (unknown/absent)
                 - confidence, z_score, kelly_f
                 - sizing_usd
                 - gate label + colour (via _DT_GATE_LABEL / _DT_GATE_COLOUR)
                 - llm_verdict when non-empty and not PASS
                 - risk_off flag when True
                 - conviction_mult when > 1.0
               Right — EXIT CONTEXT (from CLOSED row):
                 - ts offset: "±Xs vs close" (signed seconds)
                 - exit gate label + colour (= exit reason enum value)
                 - pnl_pct green/red (or "—" on pre-Track-17 DB)
                 - hold_time_secs via _fmt_hold helper

               Each column shows a muted "not found in window" message when
               the respective lookup returns no row.

               Fully wrapped in standalone try/except Exception; any failure
               logs at ERROR and silently skips without breaking
               render_trade_replay().

CHANGELOG — POST-ROADMAP TRACK 25: PER-DIRECTION BASELINE COMPARISON TABLES
═══════════════════════════════════════════════════════════════════════════════
[TRACK25-DIR1] render_operator_report(): new collapsed expander "🧭 Gate cohort
               vs. per-direction baseline" inserted immediately after the
               [TRACK24-SYM1] expander, before the height:4px spacer before
               Section 4 (Signal Quality).
               No new loader.  No new DB queries.  No new constants.
               Gate list and gate-level colours sourced from
               _load_gate_realized_pnl() (Track 21, @st.cache_data —
               cache hit on same-args call).
               Per-direction cohort stats sourced from
               _load_gate_realized_pnl_drilldown() (Track 22, @st.cache_data
               — cache hit on same-args call).  Only the "by_direction"
               branch is used; "by_symbol" is not read.
               Per-direction baseline computed inline from df (the round-trips
               DataFrame already in scope, windowed to the same days_back +
               sym_filter):
                 direction values normalised once via
                   .astype(str).str.upper()  →  "LONG" / "SHORT"
                 _dir_bl dict keyed by upper-direction string:
                   For each unique direction d in df["entry_direction"].upper():
                     df_d       = df where entry_direction.upper() == d
                     d_n        = len(df_d)
                     d_win_rate = (df_d["realized_usd"] > 0).sum() / d_n
                                  — None when realized_usd absent/all-null/d_n=0
                     d_pnl_pct  = df_d["pnl_pct"].dropna().mean()
                                  — None when pnl_pct absent/all-null
               by_direction rows from Track 22 already store direction as
               .upper() ("LONG" / "SHORT"), so _dir_bl key lookup requires
               no further normalisation.
               Render guard — expander shown only when ALL hold:
                 _load_gate_realized_pnl() returns non-empty list
                 _load_gate_realized_pnl_drilldown()["by_direction"] non-empty
                 dir_matched == True  (entry_direction was used by Track 22)
                 df not empty
                 "entry_direction" in df.columns with at least one non-null
               When dir_matched is False the entire expander is suppressed
               (both cohort and baseline are unavailable together).
               Gate selector: st.selectbox(key="rpt_dir_cmp_gate"), populated
               in same order as _load_gate_realized_pnl() output.  Default
               "— select gate —" renders no table.
               When a gate is selected, displays a compact per-direction HTML
               comparison table with columns:
                 DIR / BLOCKS / TRADES AFTER / COHORT WIN% / BASE WIN% /
                 Δ WIN% / COHORT PnL% / BASE PnL% / Δ PnL%
               Rows with n_blocked == 0 suppressed.
               DIR label colour: T["green"] for LONG, T["red"] for SHORT,
               T["text2"] for any other value.
               COHORT WIN% and BASE WIN% colour: ≥60% green, ≤35% red,
               else T["text2"].
               Δ WIN% colour: green when > +5pp, red when < −5pp, else
               T["text2"].  (Identical thresholds to Tracks 23 and 24.)
               Δ PnL% colour: green when positive, red when negative.
               Baseline columns show "—" when direction absent from df or
               when realized_usd / pnl_pct unavailable.
               Delta columns show "—" when either side is None.
               Empty visible-row set shows a muted placeholder.
               GATE_VS_DIR_BASELINE section appended to csv_rows (one row
               per visible gate × direction pair when table is rendered).
               Fully wrapped in standalone try/except; any failure logs at
               ERROR and silently skips render without breaking
               render_operator_report().
               Existing [TRACK21-XPL2], [TRACK22-DRL1], [TRACK23-CMP1],
               [TRACK24-SYM1] expanders and all other report sections are
               untouched.

CHANGELOG — POST-ROADMAP TRACK 24: PER-SYMBOL BASELINE COMPARISON TABLES
═══════════════════════════════════════════════════════════════════════════════
[TRACK24-SYM1] render_operator_report(): new collapsed expander "📊 Gate cohort
               vs. per-symbol baseline" inserted immediately after the
               [TRACK23-CMP1] expander, before the height:4px spacer before
               Section 4 (Signal Quality).
               No new loader.  No new DB queries.  No new constants.
               Gate list and gate-level colours sourced from
               _load_gate_realized_pnl() (Track 21, @st.cache_data —
               cache hit on same-args call).
               Per-symbol cohort stats sourced from
               _load_gate_realized_pnl_drilldown() (Track 22, @st.cache_data
               — cache hit on same-args call).  Only the "by_symbol" branch
               is used; "by_direction" is not read.
               Per-symbol baseline computed inline from df (the round-trips
               DataFrame already in scope, windowed to the same days_back +
               sym_filter):
                 For each symbol s in df["symbol"].dropna().unique():
                   df_s       = df[df["symbol"] == s]
                   bl_n       = len(df_s)
                   bl_win_rate= (df_s["realized_usd"] > 0).sum() / bl_n
                                — None when realized_usd absent/all-null/bl_n=0
                   bl_pnl_pct = df_s["pnl_pct"].dropna().mean()
                                — None when pnl_pct absent/all-null
               Result stored in _sym_bl: dict[symbol → {n, win_rate, pnl_pct}].
               Pre-computed once before the gate selector widget; no repeated
               per-row DataFrame filtering.
               Expander rendered only when _load_gate_realized_pnl() returns
               non-empty AND _load_gate_realized_pnl_drilldown() returns a
               non-empty "by_symbol" dict AND df is not empty AND "symbol" is
               in df.columns.
               Gate selector: st.selectbox(key="rpt_sym_cmp_gate"), populated
               in same order as _load_gate_realized_pnl() output.  Default
               "— select gate —" renders no table.
               When a gate is selected, displays a compact per-symbol HTML
               comparison table with columns:
                 SYM / BLOCKS / TRADES AFTER / COHORT WIN% / BASE WIN% /
                 Δ WIN% / COHORT PnL% / BASE PnL% / Δ PnL%
               Rows with n_blocked == 0 suppressed.
               Baseline columns show "—" when symbol absent from df or when
               the relevant column (realized_usd / pnl_pct) is unavailable.
               Delta columns show "—" when either side is None.
               Δ WIN% colour: green when > +5pp (gate mainly caught losers),
               red when < −5pp (gate may have blocked winners), else T["text2"].
               Δ PnL% colour: green when positive, red when negative.
               COHORT WIN% colour: ≥60% green, ≤35% red, else T["text2"].
               BASE WIN% colour: same thresholds as COHORT WIN%.
               GATE_VS_SYM_BASELINE section appended to csv_rows (one row per
               visible gate × symbol pair when table is rendered).
               Fully wrapped in standalone try/except; any failure logs at
               ERROR and silently skips render without breaking
               render_operator_report().
               Existing [TRACK21-XPL2], [TRACK22-DRL1], [TRACK23-CMP1]
               expanders and all other report sections are untouched.

CHANGELOG — POST-ROADMAP TRACK 23: BASELINE-VS-GATE COMPARISON TABLES
═══════════════════════════════════════════════════════════════════════════════
[TRACK23-CMP1] render_operator_report(): new collapsed expander "⚖ Gate cohort
               vs. overall baseline" inserted immediately after the
               [TRACK22-DRL1] expander, before the height:4px spacer before
               Section 4 (Signal Quality).
               No new loader.  No new DB queries.  No new constants.
               Gate cohort stats sourced from _load_gate_realized_pnl()
               (Track 21, already @st.cache_data — third call is a cache hit).
               Baseline stats computed inline from df (the round-trips
               DataFrame already in scope in render_operator_report(),
               windowed to the same days_back + sym_filter as all other
               sections):
                 _bl_n    : len(df)
                 _bl_wr   : (df["realized_usd"] > 0).sum() / len(df)
                            — None when realized_usd column absent or df empty
                 _bl_pnl  : df["pnl_pct"].dropna().mean()
                            — None when pnl_pct column absent or no rows
               Expander rendered only when _load_gate_realized_pnl() returns a
               non-empty list AND _bl_n > 0.
               Displays a per-gate HTML comparison table with columns:
                 GATE / COHORT WIN% / BASELINE WIN% / Δ WIN% /
                 COHORT PnL% / BASELINE PnL% / Δ PnL%
               Δ WIN% colour: green when > +5pp (gate caught losers),
               red when < −5pp (gate blocked winners), else T["text2"].
               Δ PnL% colour: green when positive, red when negative.
               All delta cells show "—" when either side is None.
               Caption note explains: baseline = all closed trades in the
               report window; cohort = trades within _CORR21_TRADE_WINDOW_SECS
               after a block.
               GATE_VS_BASELINE section appended to csv_rows (one row per
               gate when expander is shown).
               Fully wrapped in standalone try/except; any failure logs at
               ERROR and silently skips render.
               Existing [TRACK22-DRL1] expander and all other report sections
               are untouched.

CHANGELOG — POST-ROADMAP TRACK 22: PER-SYMBOL/DIRECTION REALIZED-PNL DRILL-DOWN
═══════════════════════════════════════════════════════════════════════════════
[TRACK22-DRL0] _load_gate_realized_pnl_drilldown(days_back, symbol_filter): new
               @st.cache_data(ttl=CACHE_FAST) read-only loader placed
               immediately after _load_gate_realized_pnl().
               Uses identical blocked-query (two-tier source_path fallback) and
               trades-query (four-tier entry_ts fallback) as Track 21.  Same
               strict join rule: same symbol, same direction when available,
               nearest forward trade within _CORR21_TRADE_WINDOW_SECS.
               Accumulates into two independent stat dicts instead of the
               per-gate dict used by Track 21:
                 _sym_stats  — per (gate_key, symbol)
                 _dir_stats  — per (gate_key, direction)
               Returns a dict with three keys:
                 "by_symbol"    dict[gate_key → list[dict]] — per-symbol rows:
                                symbol, n_blocked, n_with_trade, win_count,
                                loss_count, win_rate, mean_pnl_pct,
                                mean_realized_usd; sorted by n_blocked DESC
                 "by_direction" dict[gate_key → list[dict]] — per-direction rows:
                                direction, n_blocked, n_with_trade, win_count,
                                loss_count, win_rate, mean_pnl_pct,
                                mean_realized_usd; sorted by n_blocked DESC
                 "dir_matched"  bool — True if entry_direction was used
               Returns {} on missing tables, empty result, or any exception.
               All queries read-only.  Parameterized SQL only.  No DB writes.
               No schema changes.  No filesystem writes.  Never raises.
               Degrades cleanly when entry_direction is absent: by_direction
               renders a muted "direction column absent" note instead of a table.
               Degrades cleanly when realized_usd is absent: AVG USD shows "—".
[TRACK22-DRL1] render_operator_report(): new collapsed expander "🔍 Gate
               realized-PnL drill-down — by symbol & direction" inserted
               immediately after the [TRACK21-XPL2] expander, before the
               height:4px spacer before Section 4 (Signal Quality).
               Rendered only when _load_gate_realized_pnl() returns non-empty
               AND _load_gate_realized_pnl_drilldown() returns non-empty.
               Contents:
                 — Gate selector: st.selectbox (key="rpt_gate_xpl_drill"),
                   populated from gate_keys in n_blocked order.  Default
                   "— select gate —" shows no breakdown.
                 — By-symbol table (left column): SYM / BLOCKS / TRADES / WIN% /
                   AVG PnL% / AVG USD.  Rows with n_blocked == 0 suppressed.
                   Rendered only when selected gate has ≥1 symbol row.
                 — By-direction table (right column): DIR / BLOCKS / TRADES /
                   WIN% / AVG PnL% / AVG USD.  Rows with n_blocked == 0
                   suppressed.  When entry_direction was unavailable, renders a
                   muted "direction column absent" note instead of a table.
               Both tables use same WIN% and AVG PnL% colour logic as Track 21
               (WIN% ≥60% green / ≤35% red; PnL ≥0 green / <0 red).
               GATE_REALIZED_PNL_BY_SYMBOL section appended to csv_rows.
               GATE_REALIZED_PNL_BY_DIRECTION section appended to csv_rows.
               Both CSV sections only when a gate is selected and data present.
               Fully wrapped in standalone try/except; any failure logs at
               ERROR and silently skips render.
               Existing [TRACK21-XPL2] expander and all other report sections
               are untouched.

CHANGELOG — POST-ROADMAP TRACK 21: CROSS-TABLE BLOCKED-VS-REALIZED PROFITABILITY
═══════════════════════════════════════════════════════════════════════════════
[TRACK21-XPL1] _load_gate_realized_pnl(days_back, symbol_filter): new
               @st.cache_data(ttl=CACHE_FAST) read-only loader placed
               immediately after _load_gate_breakdown(), before the
               FRAGMENT: OPERATOR REVIEW REPORT section.
               Joins BLOCKED/SHADOW entry+express rows from decision_trace
               to canonical close-leg rows from the trades table using a
               time-based bisect-based forward match, independently of the
               DT-local pnl chain used in Tracks 18–20.
               Join rule (strict, no fuzzy heuristics):
                 For each BLOCKED row: find the nearest trades row where
                   trades.symbol = dt.symbol
                   AND trades.entry_direction = dt.direction (when column
                     is available; degrades to symbol-only if absent)
                   AND trades.entry_ts ∈ (block_ts,
                     block_ts + _CORR21_TRADE_WINDOW_SECS)
                 "trades.entry_ts" sourced via three-tier fallback:
                   Tier-1: entry_ts from open-leg self-JOIN on trade_id
                     and is_close_leg=0 (full schema)
                   Tier-2: ts − hold_time_secs proxy (Phase-6+ schema,
                     no JOIN)
                   Tier-3: ts (close fill ts used as last-resort proxy)
               Accumulates per gate_key: n_blocked, n_with_trade,
               trade_rate, win_count, loss_count, win_rate,
               mean_pnl_pct, mean_realized_usd (None when column absent),
               mean_slippage_bps (None when column absent).
               Returns list[dict] sorted by n_blocked DESC.
               Returns [] on missing tables, empty result, or any
               exception.  Two-tier blocked-query fallback identical to
               Track 19 (with / without source_path).  All queries
               read-only, parameterized.  No DB writes.  No schema
               changes.  No filesystem writes.  Never raises into caller.
               New constant: _CORR21_TRADE_WINDOW_SECS = 600.
[TRACK21-XPL2] render_operator_report(): new collapsed expander "💰 Gate-
               level realized trade profile — cross-table view" inserted
               between the existing [TRACK19-AGG2 / TRACK20-BRK2] block
               and the height:4px spacer before Section 4 (Signal
               Quality).  Rendered only when _load_gate_realized_pnl()
               returns a non-empty list.  Displays a compact per-gate
               HTML table:
                 GATE / BLOCKS / TRADES AFTER / WIN% / AVG PnL% /
                 AVG USD / AVG SLIP
               WIN% colour-coded: ≥60% → green (gate mostly caught
               losers), ≤35% → red (gate may have blocked winners),
               else T["text2"].  AVG PnL% colour: green if positive,
               red if negative.  AVG USD and AVG SLIP degrade to "—"
               when columns absent.  Direction-filter note shown when
               entry_direction was not available.
               New GATE_REALIZED_PNL section appended to csv_rows.
               Fully wrapped in standalone try/except; any failure logs
               at ERROR and silently skips render.
               Existing [TRACK19-AGG2 / TRACK20-BRK2] expander and all
               other report sections are untouched.

CHANGELOG — POST-ROADMAP TRACK 20: PER-SYMBOL / DIRECTION GATE ANALYTICS
═══════════════════════════════════════════════════════════════════════════════
[TRACK20-BRK1] _load_gate_breakdown(days_back, symbol_filter): new
               @st.cache_data(ttl=CACHE_FAST) read-only loader placed
               immediately after _load_gate_aggregate().  Uses identical
               three-query structure, identical two-tier source_path fallback,
               identical bisect-based earliest-match semantics, and the same
               _CORR19_ADMITTED_WINDOW_SECS / _CORR18_CLOSE_WINDOW_SECS
               constants as Track 19.  The single accumulation loop is extended
               to bucket each blocked row into three independent stat dicts:
                 _gate_stats    — per gate_key (same as Track 19 output)
                 _sym_stats     — per (gate_key, symbol)
                 _dir_stats     — per (gate_key, direction)
               Returns a dict with three keys:
                 "by_gate"      list[dict] — identical structure to
                                _load_gate_aggregate() result (n_blocked,
                                n_later_adm, adm_rate, n_with_close,
                                mean_pnl_pct, mean_hold_secs, gate_key,
                                label, colour), sorted by n_blocked DESC
                 "by_symbol"    dict[gate_key → list[dict]] — per-symbol
                                breakdown: symbol, n_blocked, n_later_adm,
                                adm_rate, n_with_close, mean_pnl_pct,
                                mean_hold_secs; sorted by n_blocked DESC
                 "by_direction" dict[gate_key → dict[str → dict]] —
                                direction-keyed (LONG / SHORT / other);
                                same stat fields as by_symbol cells
               Returns {} on missing table, empty result, or any exception.
               Two-tier query fallback identical to Track 19.
               All queries read-only.  Parameterized SQL only.  No DB
               writes.  No schema changes.  No filesystem writes.
               Never raises into caller.
[TRACK20-BRK2] render_operator_report(): additive drill-down sub-section
               inserted inside the existing [TRACK19-AGG2] expander,
               immediately below the existing aggregate table, within the
               same outer try/except.
               New elements:
                 — Gate selector: st.selectbox (key="rpt_gate_drill"),
                   populated from by_gate keys in label order.  Default
                   "— select gate —" shows no breakdown.
                 — Per-symbol breakdown table: compact two-column layout
                   (left), columns SYM / BLOCKS / LATER ADM / RATE / AVG
                   PnL / AVG HOLD.  Rendered only when the selected gate
                   has ≥1 symbol entry.  Rows with n_blocked == 0
                   suppressed.
                 — Per-direction breakdown table: compact two-column layout
                   (right), columns DIR / BLOCKS / LATER ADM / RATE / AVG
                   PnL / AVG HOLD.  LONG and SHORT rows; rows with
                   n_blocked == 0 suppressed.
                 — Both tables use identical rate colour-coding to the
                   Track 19 aggregate table (≥50%→yellow, <10%→green,
                   else T["text2"]).
               Drill-down section is skipped silently when no gate is
               selected or breakdown data is absent.
               GATE_BREAKDOWN_BY_SYMBOL and GATE_BREAKDOWN_BY_DIR
               sections appended to csv_rows when data is available.
               The existing Track 19 aggregate table is untouched: same
               columns, same sort, same colour logic, same interpretation.
               _load_gate_aggregate() unchanged and still present.
               The [TRACK19-AGG2] expander call is updated from
               _load_gate_aggregate() to _load_gate_breakdown(), using
               result["by_gate"] for the existing table.

CHANGELOG — POST-ROADMAP TRACK 19: AGGREGATE GATE-OUTCOME ANALYTICS
═══════════════════════════════════════════════════════════════════════════════
[TRACK19-AGG1] _load_gate_aggregate(days_back, symbol_filter): new read-only
               loader placed after _load_report_dt_summary().  For the given
               lookback window, loads all BLOCKED/SHADOW entry+express rows and
               all ADMITTED/CLOSED entry+express rows into two DataFrames, then
               computes per-gate aggregate statistics entirely in Python:
                 — n_blocked    : total BLOCKED rows for this gate
                 — n_later_adm  : blocks where the same symbol+direction was
                                  ADMITTED within _CORR19_ADMITTED_WINDOW_SECS
                                  after the block ts (earliest-match, matching
                                  Track 18 semantics exactly)
                 — adm_rate     : n_later_adm / n_blocked (0.0–1.0)
                 — n_with_close : admitted blocks where a CLOSED row was also
                                  found within _CORR18_CLOSE_WINDOW_SECS
                 — mean_pnl_pct : mean pnl_pct across those closes (None when
                                  absent on pre-Track-17 DB)
                 — mean_hold_secs: mean hold_time_secs (None when absent)
               Returns list[dict] sorted by n_blocked descending.
               Returns [] on missing table, empty result, or any exception.
               Two-tier query fallback:
                 Tier-1: with source_path IN ('entry','express') + pnl_pct +
                         hold_time_secs (Track-17+ DB).
                 Tier-2: without source_path filter, NULL pnl/hold
                         (pre-Track-16 / pre-Track-17 DB).
               All queries read-only and parameterized.  No DB writes.
               No schema changes.  No filesystem writes.  Never raises.

[TRACK19-AGG2] render_operator_report(): new collapsed expander "📊 Gate
               admission rates & outcome context" inserted inside the Admission
               Funnel section (Section 3), immediately after the existing
               top-blocking-gates count table, before the height:4px spacer.
               Expander shown only when _load_gate_aggregate() returns a
               non-empty list.  Displays a compact per-gate table with columns:
                 GATE / BLOCKS / LATER ADM / RATE / AVG PnL / AVG HOLD
               RATE column colour-coded: ≥50% → yellow (may be too strict),
               <10% → T["green"] (gate is doing its job), else T["text2"].
               AVG PnL column: green if positive, red if negative, "—" on
               pre-Track-17 DBs.
               New GATE_AGGREGATE section appended to csv_rows.
               No changes to existing top-blocking-gates table, admission
               funnel counts, or any other render_operator_report() section.

CHANGELOG — POST-ROADMAP TRACK 18: BLOCKED-VS-LATER-ADMITTED CORRELATION
═══════════════════════════════════════════════════════════════════════════════
[TRACK18-CORR1] _load_later_admitted_and_outcome(symbol, direction, blocked_ts,
               window_secs, max_hold_secs): new read-only forward-looking
               correlation loader.  Given a BLOCKED or SHADOW decision row,
               finds the earliest later ADMITTED row (same symbol + direction,
               ts within window_secs) and the earliest subsequent CLOSED row
               (same symbol, ts within max_hold_secs of admission).
               Returns a plain dict — never raises.
               Three-tier query fallback for ADMITTED lookup:
                 (1) with source_path IN ('entry','express') filter (Track-16+ DB)
                 (2) without source_path filter (Track-10/pre-16 DB)
               Two-tier fallback for CLOSE lookup:
                 (1) source_path='close' with pnl_pct + hold_time_secs (Track-17+ DB)
                 (2) outcome='CLOSED' with NULL pnl/hold (Track-10/pre-17 DB)
               All queries read-only and parameterized.  No DB writes.
               No schema changes.  No filesystem writes.

[TRACK18-CORR2] render_decision_trace_history(): expander added after the
               record table, before st.caption().  Shown only when the
               loaded DataFrame contains at least one BLOCKED or SHADOW row.
               User selects a BLOCKED/SHADOW row from a selectbox.  Dashboard
               calls _load_later_admitted_and_outcome() and displays:
                 — if admitted later: latency, source, gate, PnL, hold time
                 — if not admitted within window: not-found note
               Degrades cleanly when source_path column is absent
               (pre-Track-16 DB: all BLOCKED/SHADOW rows are selectable),
               when pnl_pct / hold_time_secs are absent (pre-Track-17 DB:
               those fields show "—"), and when the correlation loader
               returns no result.  No new panel family.  No new @st.fragment.
               No changes to existing filter controls, table, or summary tile.

CHANGELOG — POST-ROADMAP TRACK 17: DCA / EXIT-PATH DECISION-TRACE EXPANSION
═══════════════════════════════════════════════════════════════════════════════
[TRACK17-DT1]  _DT_GATE_LABEL extended with all ExitReason enum values:
               STRATEGY_TRAIL, STRATEGY_STOP, STRATEGY_DEAD_ALPHA,
               STRATEGY_FRONTRUN_WALL, STRATEGY_HEDGE_CLOSE,
               RISK_CIRCUIT_BREAKER, RISK_DRAWDOWN_ZOMBIE, RISK_GLOBAL_DRAWDOWN,
               ANOMALY_CATASTROPHE_VETO, ANOMALY_VPIN_TOXIC, ANOMALY_FUNDING_BRAKE,
               MANUAL_CLOSE, RECONCILIATION_CLOSE, STRATEGY_DCA_ADD.
               Also adds the DCA stage gate label pattern "DCA_N".
               Dashboard-only constants; no runtime coupling.

[TRACK17-DT2]  _DT_GATE_COLOUR extended for the same ExitReason values.
               STRATEGY exits → green.  RISK exits → orange.
               ANOMALY exits → red.  MANUAL/RECONCILIATION → muted.
               DCA_* → blue (pattern fallback).

[TRACK17-DT3]  _load_decision_trace_history(): pnl_pct and hold_time_secs
               added to SELECT with three-query fallback chain:
               (1) with source_path + pnl_pct + hold_time_secs  (Track-17 DB)
               (2) with source_path only, without pnl/hold       (Track-16 DB)
               (3) without source_path, pnl, or hold             (Track-10 DB)
               Pre-Track-17 DBs degrade cleanly: columns absent from DataFrame.
               source_filter updated to accept "CLOSE" and "DCA" in addition
               to "ENTRY" and "EXPRESS" (all lowercased for matching).

[TRACK17-DT4]  render_decision_trace_history():
               Outcome filter selectbox gains "CLOSED" and "DCA" options.
               Source filter selectbox gains "CLOSE" and "DCA" options.
               SRC badge handles source_path "close" → "CLO" (yellow) and
               "dca" → "DCA" (blue).
               Summary tile gains CLOSED and DCA outcome counts.
               Record table gains a PNL column: shows pnl_pct for close rows,
               "—" for all others.  Column absent on pre-Track-17 DBs: shows "—"
               for all rows.  Table header updated to include PNL column.
               All existing filter controls and layout unchanged.
               No new panel.  No DB writes.  No new @st.fragment.

CHANGELOG — POST-ROADMAP TRACK 16: DECISION-TRACE EXPANSION (EXPRESS PATH)
═══════════════════════════════════════════════════════════════════════════════
[TRACK16-DT1]  _DT_GATE_LABEL extended with new express-path gate names:
               ZOMBIE, DRAWDOWN_KILLED, CIRCUIT_BREAKER, VOL_GUARD,
               SYMBOL_NOT_TRACKED, MACRO_FLOOR, EXPRESS_DEDUPE, ALREADY_OPEN,
               CONCURRENT_CAP, DEPLOYED_CAP.
               Dashboard-only constants; no runtime coupling.

[TRACK16-DT2]  _DT_GATE_COLOUR extended with colours for the same gates.

[TRACK16-DT3]  _load_decision_trace_history() gains optional source_filter
               parameter (default "ALL").  When non-ALL, a Python-side filter
               is applied on the returned DataFrame.  source_path column is
               added to the SELECT via a try/except fallback pair — if the
               column does not yet exist (pre-Track-16 DB) the query falls
               back to the original SELECT without source_path.  Backward-
               compatible: callers that omit source_filter get "ALL" behaviour.

[TRACK16-DT4]  render_decision_trace_history() gains a "Source" selectbox
               (ALL / ENTRY / EXPRESS) wired to the source_filter parameter.
               The selectbox is shown only when the source_path column is
               present in the returned DataFrame.  When absent, the filter
               silently acts as ALL.  A SRC column is added to the record
               table to show "entry" vs "express" per row.  All existing
               filter controls and layout are unchanged.

               No other panels changed.  No schema changes.  No DB writes.
               No new @st.fragment or @st.cache_data decorators.

CHANGELOG — POST-ROADMAP TRACK 15: OPERATOR-FACING CONTROL REFINEMENT
═══════════════════════════════════════════════════════════════════════════
[TRACK15-CR1] render_policy_state() Section 8 Score Feedback Controls:
               writes_ok guard added — consistent with render_tactical_sidebar().
               Resolves _wb_bot_live = _bot_is_live() and _wb_writes_ok =
               _wb_bot_live and not _is_demo_mode() at the top of the section.
               Both Apply and Reset buttons gain disabled=not _wb_writes_ok.
               Warning tiles ("BOT OFFLINE" / "DEMO MODE") shown when writes
               are blocked, matching the tactical sidebar visual pattern.
               Belt-and-braces guard inside each handler ensures
               _write_control_event() is never reached when not _wb_writes_ok.
               _audit(), success/warning toasts, and call signatures unchanged.

[TRACK15-CR2] render_policy_state() Section 1 _GATE_DEFS extended with two
               Track 14 rows: CONCURRENT_CAP ("max_open_positions") and
               DEPLOYED_CAP ("max_deployed_pct").  Both show "disabled" /
               threshold in the same gate-enable-status table as all other
               operator-configurable gates.  No other change to the rendering
               loop or the always-on gate list.

[TRACK15-CR3] render_policy_state() Section 6 autotune active-overrides table:
               SOURCE column added.  Reads snap.get("source") from the
               autotune_state status payload (populated by executor Track 15
               addition).  Renders MANUAL (yellow) or AUTO (green) per symbol.
               Falls back to "—" when source is absent (pre-Track15 executor).
               Table header and footer text updated accordingly.

               No other panels changed.  No schema changes.  No DB writes.
               No new library imports.  No new fragment or cache decorators.

CHANGELOG — POST-ROADMAP TRACK 14: BETTER ALLOCATION / EXPOSURE CONTROL
═══════════════════════════════════════════════════════════════════════════
[TRACK14-DASH] render_positions_panel() enriched with three additive
               read-only features (all inside the existing `if pos_active:`
               branch — zero change to idle-symbols display):

               (a) Per-position USD COST and % EQ columns added to the
                   active-positions table.  USD COST = p["usd_cost"] from
                   positions_snap (cost-basis notional, NOT mark-to-market).
                   % EQ = usd_cost / equity × 100.  Both show "—" when
                   usd_cost is zero or absent (external/pre-reconcile rows).
                   % EQ is colour-coded: green <15%, yellow <30%, orange
                   <50%, red ≥50%.

               (b) Σ NOTIONAL tile added below the positions table.  Shows
                   the sum of all open position usd_costs as a % of equity
                   with a coloured progress bar (same colour thresholds as
                   the existing PORTFOLIO HEAT tile).  Explicitly labelled
                   as cost-basis sum, not mark-to-market.

               (c) Gate-visibility footnote embedded inside the Σ NOTIONAL
                   tile.  Reads admission_policy["max_open_positions"],
                   admission_policy["max_deployed_pct"],
                   admission_policy["current_open_positions"], and
                   admission_policy["current_deployed_pct"] from the live
                   status payload (populated by executor Track 14 additions).
                   Shows "OFF" when each gate is at its default-disabled
                   value.  Colour-coded amber when within 1 slot or 90%
                   of the threshold.  Read-only — no writes.

               No other panels changed.  No schema changes.  No DB writes.
               No new library imports.

CHANGELOG — POST-ROADMAP TRACK 13: OPERATOR REVIEW REPORT
═══════════════════════════════════════════════════════════════════════
[TRACK13-RPT1]  _load_report_dt_summary(days_back, symbol_filter): new
               @st.cache_data(ttl=CACHE_FAST) read-only function.  Queries
               decision_trace for aggregate admission-funnel metrics over the
               requested window.  Returns a dict:
                 total, n_admitted, n_blocked, n_shadow,
                 top_gates: list of (gate_key, label, colour, count) up to 5.
               Guards on _get_known_tables().  Returns {} on missing table,
               empty result, or any error.  Parameterized SQL only.
               Never writes.
[TRACK13-RPT2]  render_operator_report(): new @st.fragment(run_every=FRAG_SLOW).
               Five-section compact read-only review panel:
                 1. Performance headline — total PnL, win rate, trade count,
                    avg hold, avg close slippage; per-symbol sub-table when
                    multiple symbols present.  Sourced from _load_round_trips(),
                    windowed by close_ts >= cutoff.
                 2. Exit attribution — STRATEGY / RISK / ANOMALY / OTHER rows
                    with PnL, win rate, count.
                 3. Admission funnel — total evaluated, ADMITTED / BLOCKED /
                    SHADOW counts + rates, top-3 blocking gates by frequency.
                    Sourced from _load_report_dt_summary().
                 4. Signal quality — avg entry confidence, winner vs loser
                    confidence split, avg entry z-score.  Degrades gracefully
                    on pre-Track-01 schema.
                 5. Top / bottom setups — top-3 and bottom-3 (symbol × regime
                    × direction) cells by avg PnL, min 3 trades per cell.
                    Degrades gracefully on insufficient data.
               Lookback selectbox (1d / 7d / 30d, key="rpt_days") and optional
               symbol filter selectbox (key="rpt_sym").
               One st.download_button exports a flat CSV of all five sections
               in-memory (no server-side file write, no hub_data write).
               Degrades gracefully on empty windows and old schema (pre-Track-01
               entry_regime / entry_confidence / entry_z_score columns absent).
               Read-only.  No DB writes.  No schema changes.
[TRACK13-LAY]  render_operator_report() wired into main() col_left after
               render_trade_replay() and before render_policy_state().
               Single st.container(border=True) and 6px spacer, consistent
               with all prior track wiring.

CHANGELOG — POST-ROADMAP TRACK 11: REPLAY / SHADOW REVIEW EXPANSION
═══════════════════════════════════════════════════════════════════════
[TRACK11-RP1]  render_trade_replay() enriched with two new signal-quality
               columns in the round-trip table:
                 Z     — entry_z_score (2 dp; colour-coded ≥1.5 cyan, ≥0.8
                         blue, else muted); shows "—" when absent or NULL.
                 KELLY — entry_kelly_f (3 dp; muted monospace).
               Both values were already fetched by _load_round_trips() in
               Track 02 but were not rendered.  No query changes.
[TRACK11-RP2]  Direction filter added to render_trade_replay().  A new
               "Dir" st.selectbox (key="rt_dir") with options
               ALL / LONG / SHORT is applied as an in-memory filter on
               entry_direction.  Disabled (greyed) when entry_direction
               column is absent from the data.  Existing four filter
               controls are unchanged.
[TRACK11-RP3]  Sort mode control added to render_trade_replay().  A new
               "Sort" st.selectbox (key="rt_sort") with options
               "Recent" (close_ts DESC, existing behaviour) and "Best PnL"
               (realized_usd DESC) is applied as an in-memory sort on fdf
               after all filters.  No SQL change.
[TRACK11-RP4]  Display cap raised from 50 to 100 rows.  Caption updated.
               _load_round_trips() is unchanged (still fetches up to 1000).
[TRACK11-NM]   _load_near_miss_blocked(symbol, entry_ts, window_secs):
               new @st.cache_data(ttl=CACHE_FAST) read-only query.  Fetches
               BLOCKED and SHADOW decision_trace records for the given symbol
               within [entry_ts − window_secs, entry_ts + 30s].  Guards on
               "decision_trace" in _get_known_tables().  Returns empty
               DataFrame on missing table, no rows, or any error.  Uses
               parameterized SQL only.  Never writes.
[TRACK11-LAY]  Near-miss expander added at the bottom of render_trade_replay()
               below the main table.  Operator selects a trade from a compact
               selectbox (populated from the displayed rows: symbol + entry
               time label).  Calls _load_near_miss_blocked() for that trade
               and renders a compact styled table of BLOCKED/SHADOW decisions
               in the ±window.  Degrades to a muted placeholder when
               decision_trace table absent or no near-miss rows exist.
               Skipped entirely when the replay DataFrame is empty.
               render_decision_trace() and render_decision_trace_history()
               are untouched.

CHANGELOG — POST-ROADMAP TRACK 10: DB-BACKED HISTORICAL DECISION-TRACE
═══════════════════════════════════════════════════════════════════════
[TRACK10-DT1]  _load_decision_trace_history(): new @st.cache_data(ttl=CACHE_FAST)
               read-only query function.  Reads from the decision_trace table
               in powertrader.db.  Accepts days_back (int), symbol_filter (str),
               and outcome_filter (str) parameters for server-side filtering.
               Guards on "decision_trace" in _get_known_tables() — returns empty
               DataFrame on missing table, empty table, or any query error.
               No writes.  No schema side effects.  No joins with trades.
[TRACK10-DT2]  render_decision_trace_history(): new @st.fragment(run_every=FRAG_MED)
               panel.  Two-part layout:
               (a) Compact aggregate summary — total decisions, outcome breakdown
                   (ADMITTED / BLOCKED / SHADOW counts and percentages), top-5
                   gate_name breakdown by count over the selected window.
               (b) Filterable recent-record table — symbol, days_back, and
                   outcome filters via st.selectbox/st.slider; shows up to 200
                   rows from the DB query.  Columns: TIME, SYM, DIR, REGIME,
                   CONF, OUTCOME/GATE, SIZE, REASON.  Same badge/colour logic
                   as render_decision_trace().
               Degrades to _slot_placeholder when decision_trace table is absent
               or returns no rows for the selected filter.  No DB writes.
               No new @st.cache_data beyond _load_decision_trace_history().
[TRACK10-LAY]  render_decision_trace_history() wired into main() immediately
               after render_decision_trace() (Track 09 live panel) inside
               Band 3 (col_right).  No other layout changes.

CHANGELOG — POST-ROADMAP TRACK 09: DECISION-TRACE / EXPLANATION LOGGING
═════════════════════════════════════════════════════════════════════════
[TRACK09-DT1]  DECISION_TRACE_PATH constant added (hub_data/decision_trace.json).
               Mirrors the VETO_AUDIT_PATH constant pattern.
[TRACK09-DT2]  load_decision_trace(): new reader with LKG pattern.
               Returns list[dict] from decision_trace.json.  Degrades to []
               on missing file, empty file, or JSON error.
[TRACK09-DT3]  _DT_GATE_LABEL and _DT_GATE_COLOUR module-level dicts map all
               gate_name values emitted by executor.py to display labels and
               theme colours.  Dashboard-only constants, no runtime coupling.
[TRACK09-DT4]  render_decision_trace(): new @st.fragment(run_every=FRAG_MED)
               panel.  Reads decision_trace.json; shows most recent 20 decisions
               (ADMITTED / BLOCKED / SHADOW) with columns: TIME, SYM, DIR,
               REGIME, CONF, OUTCOME/GATE, SIZE, REASON.  Compact badge per
               gate_name.  LLM verdict shown inline when non-PASS.  Degrades
               to muted placeholder when artifact is absent or empty.  No DB
               queries.  No schema dependencies.  No writeback of any kind.
[TRACK09-LAY]  render_decision_trace() wired into main() immediately after
               render_veto_audit() inside the intelligence band (Band 3).
               No other layout changes.  No new @st.cache_data.

CHANGELOG — POST-ROADMAP TRACK 07: SCORE FEEDBACK WRITEBACK (WB1–WB4)
══════════════════════════════════════════════════════════════════════════
[TRACK07-WB1]  Section 8 — Score Feedback Controls subsection appended to
               render_policy_state().  Operator can select a tracked symbol
               from the live autotune_state symbol list, set a conf_floor
               and alloc_cap, and click Apply.  The dashboard fires a
               "set_autotune_override" event via _write_control_event()
               into the existing P24 control queue.  Executor picks it up
               within one trade cycle and calls _perf.set_override(sym,
               conf_floor, alloc_cap), raising the confidence floor and
               capping allocation for that symbol immediately.  The change
               is reflected in the existing Track 04 autotune display on
               the next status refresh — no new display code required.
               A Reset button fires "reset_autotune_override" which clears
               both fields back to defaults via _perf.reset_override(sym).
               Controls are operator-explicit only — no automated writeback.
               No new DB queries.  No schema change.  No new @st.fragment.
               No new @st.cache_data.  No new containers in main().
[TRACK07-WB2]  SF6 panel footer note updated to reference the Score Feedback
               Controls section for operator discoverability.
[TRACK07-LAY]  Section 8 appended inside render_policy_state() only, after
               existing Track 04 sections (SF4 autotune + SF5 RL weights).
               No layout redesign.  No new containers in main().

CHANGELOG — POST-ROADMAP TRACK 06: SCORE FEEDBACK SYNTHESIS (SF6)
══════════════════════════════════════════════════════════════════════════
[TRACK06-SF6]  Panel 15 — Setup evidence summary: ranked table of
               (symbol, entry_regime, entry_direction) cells with per-cell
               trade count, win rate, total PnL, avg PnL, and an evidence
               label (STRONG EDGE ≥60% WR + ≥10 trades / MARGINAL 40–59%
               WR + ≥10 trades / WEAK EDGE <40% WR + ≥10 trades / TOO FEW
               TRADES <10 trades).  Sorted by avg PnL descending so the
               strongest setups appear first.  Capped at 40 rows.  Guard:
               has_entry_regime AND realized_usd present AND ≥3 matching
               rows.  Degrades to muted placeholder on empty DB,
               pre-Track-01 schemas, or mixed rows with no DT truth.
               No new DB query.  No new cache.  No schema change.
               Dashboard-only.  Read-only.  No writeback of any kind.
[TRACK06-LAY]  Panel appended inside render_profitability_diagnostics()
               only, before the existing sample-count note.  No new
               @st.fragment.  No new @st.cache_data.  No new containers
               in main().  All prior panels unchanged.

CHANGELOG — POST-ROADMAP TRACK 05: SCORE FEEDBACK EXPANSION (SF4 + SF5)
══════════════════════════════════════════════════════════════════════════
[TRACK05-SF4]  Panel 13 — PnL / win-rate by symbol × entry regime WITH
               friction per cell.  Extends the Track-03 SF3 symbol×regime
               table by adding avg close_slippage_bps and avg
               spread_at_close_bps columns per (symbol, regime) cell.
               Answers: is slippage eating the edge on specific
               symbol+regime combinations?  Guard: has_p10_friction AND
               has_entry_regime AND ≥1 matching row with both fields.
               Shows "—" for cells with no friction data.  Capped at 30
               rows.  No new DB query.  No new cache.  No schema change.
               Dashboard-only.  No writeback.
[TRACK05-SF5]  Panel 14 — PnL / win-rate by z_score tier × direction.
               Crosses entry_z_score tier (STRONG ≥1.5 / MID 0.8–1.5 /
               WEAK <0.8, using absolute value) with entry_direction
               (long / short).  Up to 6 cells (3 tiers × 2 directions).
               Answers: does z-score strength predict outcomes differently
               for longs vs shorts?  Guard: _has_z_score AND has_entry_
               regime (direction present) AND ≥3 matching rows.  No new
               DB query.  No new cache.  No schema change.  Dashboard-only.
               No writeback.
[TRACK05-LAY]  Both panels appended inside render_profitability_
               diagnostics() only, before the existing sample-count note.
               No new @st.fragment.  No new @st.cache_data.  No new
               containers in main().  SF6 (advisory labels) deferred.

CHANGELOG — POST-ROADMAP TRACK 04: LIVE ADAPTIVE-STATE VISIBILITY
══════════════════════════════════════════════════════════════════════
[TRACK04-SF4]  Section 6 — Per-symbol autotune state sub-section appended to
               render_policy_state().  Reads status["autotune_state"] published
               by executor.py each cycle.  Shows active CoinPerformanceTracker
               overrides per symbol: conf floor, alloc cap, rolling win rate,
               and trade count.  Advisory tile when key absent (pre-Track-04
               executor) or when no overrides are currently active.
               Dashboard-only.  No DB query.  No schema change.
[TRACK04-SF5]  Section 7 — RL regime weight table sub-section appended to
               render_policy_state().  Reads status["rl_regime_weights"] and
               status["rl_config"] published by executor.py each cycle.
               Shows per-regime multiplier (colour-coded above/below 1.0),
               warm/cold state vs RL_MIN_TRADES, win/loss counts, win rate,
               and total PnL.  PnL colour logic defined locally — does not
               reference _pnl_col from render_profitability_diagnostics().
               Advisory tile when key absent.  Dashboard-only.  No DB query.
               No schema change.
[TRACK04-LAY]  Both sections appended inside render_policy_state() only.
               No new @st.fragment.  No new @st.cache_data.  No new containers
               in main().  render_profitability_diagnostics() untouched.

CHANGELOG — POST-ROADMAP TRACK 03: SCORE FEEDBACK
════════════════════════════════════════════════════
[TRACK03-SF1]  Panel 10 — PnL / win-rate by entry confidence tier.
               Buckets entry_confidence into HIGH (≥0.75) / MID (0.50–0.75) /
               LOW (<0.50).  Answers: does the confidence scorer predict outcomes?
               Appended to render_profitability_diagnostics() using the existing
               df from _load_close_diagnostics().  Requires has_p6_quality + ≥5
               rows with non-null entry_confidence.  Degrades gracefully otherwise.
               No new DB query.  No new cache function.  No schema change.
[TRACK03-SF2]  Panel 11 — PnL / win-rate by entry z_score tier.
               Buckets entry_z_score into STRONG (≥1.5) / MID (0.8–1.5) /
               WEAK (<0.8).  entry_z_score was persisted in Track 01 but never
               grouped or displayed until this track.  Requires ≥5 rows with
               non-null entry_z_score.  Degrades gracefully on pre-Track-01 DB.
               No new DB query.  No new cache function.  No schema change.
[TRACK03-SF3]  Panel 12 — PnL / win-rate by symbol × entry regime.
               Cross-tab grouping by (symbol, entry_regime).  Sorted symbol-
               ascending, regime ordered bull/bear/chop within each symbol.
               Capped at 30 rows to prevent table overflow on many-symbol
               deployments.  Requires has_entry_regime + ≥1 matching rows.
               Degrades gracefully on pre-Track-01 DB.  No new DB query.
[TRACK03-LAY]  All three panels appended inside render_profitability_diagnostics()
               before the existing sample-count note.  No new containers in
               main().  No new @st.fragment.  No new @st.cache_data.

CHANGELOG — POST-ROADMAP TRACK 02: REPLAY / SHADOW REVIEW
═══════════════════════════════════════════════════════════
[TRACK02-DQ]  _load_round_trips(): new read-only query.  Self-JOIN on the
              trades table (c=close leg, o=open leg) linked by trade_id.
              Returns one DataFrame row per completed round-trip with full
              entry + exit context.  Six-tier fallback: full JOIN → close-only
              → each progressively narrower schema.  No DB writes.  No schema
              changes.  @st.cache_data(ttl=CACHE_FAST).
[TRACK02-UI]  render_trade_replay(): new @st.fragment(run_every=FRAG_SLOW)
              panel.  Operator filter controls: symbol, exit category, entry
              regime, days-back window.  4-KPI filtered summary (count, win
              rate, avg PnL, avg slippage).  Scrollable round-trip table with
              per-row colour coding.  Degrades gracefully on empty DB,
              pre-Phase-1 (no trade_id), pre-Track-01 (no entry context), and
              mixed old/new rows.
[TRACK02-LAY] Wired into main() left column after render_profitability_
              diagnostics(), before render_policy_state(), inside
              st.container(border=True) with 6px spacer.

CHANGELOG — PHASE 5: PROFITABILITY DIAGNOSTICS
════════════════════════════════════════════════
[PHASE5-G1]  _load_trades_raw / _FULL_SQL now fetches exit_reason_type,
             exit_reason_category, is_close_leg — Phase 4 canonical KPI
             filter (is_close_leg == 1) now active in normal operation;
             pnl_pct != 0 heuristic no longer fires on Phase-4+ schemas.
[PHASE5-G8]  is_close_leg coerced to int after query so == 1 filter is
             type-safe regardless of query path (FULL_SQL / WILD_SQL).
[PHASE5-DQ]  _load_close_diagnostics(): dedicated query for grouped
             profitability analysis — is_close_leg=1 filter in SQL,
             2 000-row sample, 3-tier fallback for pre-Phase-4 schemas.
[PHASE5-UI]  render_profitability_diagnostics(): new @st.fragment panel:
             (1) emergency-exit dominance summary (STRATEGY vs RISK+ANOMALY),
             (2) grouped PnL / win-rate by exit reason category,
             (3) grouped PnL / win-rate by symbol (sorted worst-first).
             Wired into main() layout after render_trade_analytics().
             All metrics derived from canonical close-leg rows only.
             Degrades gracefully on empty or pre-Phase-4 data.

CHANGELOG — PHASE 11: EVIDENCE-BASED POLICY TIGHTENING
════════════════════════════════════════════════════════
[PHASE11-UI] render_policy_state(): new @st.fragment(run_every=FRAG_MED)
             panel. Reads admission_policy block from status dict (published
             by executor.py Phase 11 changes). Surfaces:
               (1) gate enable/disable status for all 9 admission gates
               (2) per-gate rejection counts since last restart
               (3) flush-tracker snapshot (per-symbol early-flush history)
               (4) P32 LIMIT_ONLY symbols currently active
               (5) VPIN hardening threshold summary
             All guidance is strictly advisory — no auto-setting behavior.
             No new CSS classes; no new DB queries.
[PHASE11-LAY] Wired into main() left column after render_profitability_
             diagnostics(), inside st.container(border=True) with 6px spacer.

CHANGELOG v53.0 — P53 MESH + UX BLOOMBERG REDESIGN
═══════════════════════════════════════════════════════
[P53-DASH]  render_p53_mesh: 3-exchange node health matrix, 2/3 quorum KPIs
[P54-FIX]   P54 PENDING state: enabled=True + event_count=0 → ⏳ PENDING (not ⚫ UNKNOWN)
[UX-FIX-1]  BAND 8.5 split into 3 logical Bloomberg rows: Infrastructure / Bridge-Exec / Protocol
[UX-FIX-2]  Minimum font sizes lifted (0.56rem→0.68rem) + panel card min-height added
[UX-FIX-3]  ot-row-label, ot-stat-grid CSS classes for consistent LABEL · VALUE pairs
[UX-FIX-4]  render_p44_supervisor compact mode for narrow columns
[FLICKER]   FRAG_FAST reduced to 1.5s; FRAG_MED to 3s; FRAG_SLOW to 6s (reduces LKG churn)
[PNL-FIX]   PnL = $0.00 is a bridge_fill limitation — noted in panel; executor.py fixed separately
[ATOMIC]    Windows [WinError 2] fixed in engine_supervisor._atomic_write (see that file)

CHANGELOG v52.0 — P52 ENGINE SUPERVISOR HEALTH PANEL
═══════════════════════════════════════════════════════
[P52-DASH]  render_p52_supervisor: role/version badge, LIVE/DEMO mode, uptime clock
[P52-DASH]  Engine cycle counter (loop_iter) — confirms status writes are live
[P52-DASH]  Active symbol list display (up to 6 shown inline, +N overflow)
[P52-DASH]  Bridge link state + IPC framing mode row
[P52-DASH]  P44 auto-restart indicator
[P52-DASH]  15-second staleness guard — shows ⚫ OFFLINE when engine silent
[CSS]       New classes: .ot-p52-live, .ot-p52-demo, .ot-p52-idle, .ot-p52-row, .ot-p52-val, .ot-p52-band
[LAYOUT]    Band 8.5 expanded to 8 columns: P40.5 · P44 · P46 · P48 · P49 · P50 · P51 · P52
[ARCH]      page_title bumped to v52.0

CHANGELOG v46.1 — P44/P46/P42 FULL DASHBOARD CLOSURE
═══════════════════════════════════════════════════════
[P44-DASH]  render_p44_supervisor: win-rate gauge, DCB freeze, Kelly override history
[P44-DASH]  SUPERVISOR_OVERRIDES_PATH constant + load_supervisor_overrides() loader
[P44-DASH]  HUD bar shows DCB freeze countdown when supervisor is paused
[P46-DASH]  render_p46_telemetry: latency histogram, slippage KPIs, P39/P41 activation rates
[P46-DASH]  render_shadow_audit upgraded: dynamic columns, P46 fields, latency_ms, slippage_bps
[P42-DASH]  render_macro_pulse upgraded: SPY/BTC Pearson-r gauge, DECOUPLED badge, stand-down state
[P42-DASH]  render_correlation upgraded: spy_btc_corr KPI from p42_shadow status key
[ARCH]      _inject_lkg now preserves p42_shadow key on stale status
[ARCH]      New fig cache keys: _FIG_KEY_P44_WINRATE, _FIG_KEY_P46_LAT, _FIG_KEY_SPY_BTC
[ARCH]      New BAND 8.5 in layout: P44 Supervisor + P46 Execution Telemetry
[CSS]       New classes: .ot-corr-decouple, .ot-kelly-bar, .ot-p46-badge, .ot-p44-heat
[LAYOUT]    Band ordering: 0=HUD 1=KPIs 2=Ticker 3=Main 4=P22 5=Council 6=Macro 7=Analytics 8=ShadowAudit 8.5=P44+P46 9=WarRoom

CHANGELOG v40.5 — FULL INSTITUTIONAL HARDENING
═══════════════════════════════════════════════
[FIX-CRIT-01]  _get_known_tables() implemented; @st.cache_data(ttl=CACHE_SLOW)
[FIX-CRIT-02]  Cache-bust clears BOTH @st.cache_data AND session-state LKG key
[FIX-CRIT-03]  Monotonic guard: dead _prev variable removed; clean _mono_key flow
[FIX-CRIT-04]  hydrate_institutional_state: explicit isinstance(acct,dict) before latch
[FIX-CRIT-05]  Equity latch writes ONLY in hydrate_institutional_state() and
               _get_binary_truth_equity(); display fragments are strictly read-only
[FIX-CRIT-06]  Leverage stored as float everywhere; no silent int() truncation
[FIX-CRIT-07]  Candlestick overlay: _norm_sym() on both sides of comparison
[FIX-CRIT-08]  _calc_snr: explicit None on short series; badge renders cleanly
[FIX-ARCH-01]  All fragments read via _get_status(); zero stale parameter capture
[FIX-ARCH-02]  _SS_FRAME_TS tracks snapshot write time; HUD age reflects frame not file
[FIX-ARCH-04]  _lkg_load: LKG_MAX_AGE_SECS cap (300 s) with visible stale warning
[FIX-ARCH-05]  No fragment accepts status as a parameter (eliminates stale-capture)
[FIX-QA-01]   _SS_UI_SHADOW: single module-level definition only
[FIX-QA-02]   STATUS_STALE: single module-level definition only
[FIX-QA-03]   T, VERDICT_STYLE, REGIME_META in types.MappingProxyType (immutable)
[FIX-QA-04]   hydrate_institutional_state: plain if/else, no lambdas
[FIX-QA-05]   DEFAULT_TRUST = 0.50 named constant throughout
[FIX-QA-06]   _slot_placeholder() shared helper; SLOTS pattern DRY across all panels
[FIX-QA-07]   LKG figure cache keys: module-level constants, not inline string literals
[FIX-QA-08]   _fig_base(subplot=True) skips global yaxis.side override
[FIX-OPS-02]  load_tactical_config re-reads disk every TACT_RELOAD_SECS (10 s)
[FIX-OPS-04]  render_war_room_log: multi-encoding decode (utf-8 → utf-8-sig → latin-1)
[FIX-OPS-06]  st.tabs inside @st.fragment uses stable key= — no tab-reset on rerender
[FIX-PERF-01] render_flash_feed promoted to @st.fragment(run_every=FRAG_MED)
[FIX-PERF-02] render_systemic_defense_sidebar caches sidebar chart in session_state
[FIX-PERF-04] render_shadow_audit promoted to @st.fragment(run_every=FRAG_SLOW)
[FIX-SEC-01]  _esc() applied to ALL external string data embedded in HTML
[FIX-INST-02] _write_control_event includes monotonic sequence number
[FIX-P13-KEY] render_p13_trust reads p13_trust_scores (live key) with p13_trust_map fallback
[FIX-FLICKER-EQ]  render_equity_curve rebuilds Plotly figure only when underlying data changes
[FIX-FLICKER-P17] render_p17_intelligence rebuilds gauge only when score/verdict/symbol changes
[FIX-TRADES]  _load_trades_raw: resilient multi-schema query; handles missing optional columns

Run:  streamlit run dashboard.py
Deps: streamlit>=1.39  plotly  pandas  numpy  (streamlit-autorefresh optional)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
import hashlib as _hashlib
import threading as _threading
import tempfile as _tempfile
import types
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

log = logging.getLogger("dashboard_p40_5")

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PowerTrader AI · Terminal v53.0",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR             = Path(__file__).resolve().parent
HUB_DIR              = BASE_DIR / "hub_data"

# Canonical DB location (self-contained ops artifacts)
DB_PATH              = HUB_DIR / "powertrader.db"

TRADER_STATUS        = HUB_DIR  / "trader_status.json"
TIF_PENDING_PATH     = HUB_DIR  / "tif_pending.json"
PANIC_LOCK_PATH      = HUB_DIR  / "panic_lock.json"
VETO_AUDIT_PATH      = HUB_DIR  / "veto_audit.json"
DECISION_TRACE_PATH  = HUB_DIR  / "decision_trace.json"   # [TRACK09-DT]
TACTICAL_CONFIG_PATH = HUB_DIR  / "tactical_config.json"
CONTROL_EVENT_PATH   = HUB_DIR  / "control_event.json"
CONTROL_QUEUE_PATH         = HUB_DIR  / "control_queue.jsonl"
SUPERVISOR_OVERRIDES_PATH  = HUB_DIR  / "supervisor_overrides.json"   # [P44-DASH]
SUPERVISOR_HEALTH_PATH     = HUB_DIR  / "supervisor_health.json"       # [P52-DASH] watchdog writes this

# Logs remain in BASE_DIR (operator-local artifacts)
OPERATOR_AUDIT_PATH  = BASE_DIR / "operator_audit.log"
SHADOW_AUDIT_PATH    = Path(os.environ.get(
    "SHADOW_AUDIT_PATH", str(BASE_DIR / "shadow_audit.csv")))
EXECUTOR_LOG_PATH    = Path(os.environ.get(
    "EXECUTOR_LOG_PATH", str(BASE_DIR / "executor.log")))

# ─────────────────────────────────────────────────────────────
# REQUIRED GLOBAL CONSTANTS / KEYS (must exist before use)
# ─────────────────────────────────────────────────────────────

CACHE_FAST = 3
CACHE_SLOW = 30
CACHE_CORR = 30

FRAG_FAST    = 2   # [UX-FIX: was 1s — reduced to 2s to prevent simultaneous LKG churn]
FRAG_MED     = 4   # [UX-FIX: was 2s — reduced to 4s; P49/50/51/52/53/54 panels]
FRAG_SLOW    = 8   # [UX-FIX: was 5s — reduced to 8s; shadow audit, chart panels]
FRAG_WARROOM = 2

_MAX_STATUS_BYTES       = 2_000_000
_MAX_LOG_BYTES          = 512_000
STATUS_STALE            = 10
LKG_MAX_AGE_SECS        = 300
_BOT_HEARTBEAT_STALE_SECS = 10
_BOT_DEAD_WARN_THROTTLE = 15
_TAB_COOLDOWN           = 1.5
TACT_RELOAD_SECS        = 10

DEFAULT_TRUST = 0.50

T = {
    "bg":      "#070A0F",
    "panic_bg": "rgba(244,63,94,0.10)",
    "surface": "rgba(255,255,255,0.04)",
    "surface2": "rgba(255,255,255,0.06)",
    "border":  "rgba(255,255,255,0.08)",
    "border2": "rgba(255,255,255,0.12)",

    "text":  "rgba(255,255,255,0.92)",
    "text2": "rgba(255,255,255,0.70)",
    "muted": "rgba(255,255,255,0.55)",

    "blue":   "#4DA3FF",
    "cyan":   "#2EE6D6",
    "teal":   "#00C2A8",
    "green":  "#22C55E",
    "yellow": "#FACC15",
    "orange": "#FB923C",
    "red":    "#F43F5E",
    "purple": "#A78BFA",

    "glow_b": "rgba(77,163,255,0.18)",
    "glow_g": "rgba(34,197,94,0.16)",
    "glow_r": "rgba(244,63,94,0.16)",

    "mono": "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "
            "'Liberation Mono', 'Courier New', monospace",
}

_FONT_SANS_CSS    = "Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif"
_FONT_SANS_PLOTLY = "Inter, Arial, sans-serif"
_FONT_MONO_PLOTLY = "SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace"

# Updated for full compatibility with legacy and institutional panels
# Updated for full compatibility with legacy and institutional panels
VERDICT_STYLE = {
    "GREEN":   {"icon": "🟢", "badge": "🟢", "color": "#00FF41", "col": "#00FF41", "glow": "rgba(0,255,65,0.2)"},
    "AMBER":   {"icon": "🟠", "badge": "🟠", "color": "#FFB800", "col": "#FFB800", "glow": "rgba(255,184,0,0.2)"},
    "RED":     {"icon": "🔴", "badge": "🔴", "color": "#FF003C", "col": "#FF003C", "glow": "rgba(255,0,60,0.2)"},
    "NEUTRAL": {"icon": "⚪", "badge": "⚪", "color": "#888888", "col": "#888888", "glow": "rgba(255,255,255,0.1)"},
    "N/A":     {"icon": "⚪", "badge": "⚪", "color": "#888888", "col": "#888888", "glow": "rgba(255,255,255,0.1)"},
}

REGIME_META = {
    "trend":       {"badge": "📈", "icon": "📈", "label": "TREND", "col": "#00FF41", "color": "#00FF41", "glow": "rgba(0,255,65,0.2)"},
    "mean_revert": {"badge": "🔁", "icon": "🔁", "label": "MEAN-REVERT", "col": "#00CFEB", "color": "#00CFEB", "glow": "rgba(0,207,235,0.2)"},
    "risk_off":    {"badge": "⚠️", "icon": "⚠️", "label": "RISK-OFF", "col": "#FF003C", "color": "#FF003C", "glow": "rgba(255,0,60,0.2)"},
    "neutral":     {"badge": "➖", "icon": "➖", "label": "NEUTRAL", "col": "#888888", "color": "#888888", "glow": "rgba(255,255,255,0.1)"},
    "unknown":     {"badge": "❔", "icon": "❔", "label": "UNKNOWN", "col": "#888888", "color": "#888888", "glow": "rgba(255,255,255,0.1)"},
}
VETO_REASON_MAP = {
    "risk_off": "Risk-Off",
    "dead_bot": "Bot Offline",
    "demo":     "Demo Mode",
    "limit":    "Risk Limit",
    "hv":       "High Volatility",
}

TACT_RISK_OFF = "tact_risk_off"
TACT_SNIPER   = "tact_sniper_only"
TACT_HEDGE    = "tact_hedge_mode"

TACT_DEFAULTS = {
    TACT_RISK_OFF: False,
    TACT_SNIPER:   False,
    TACT_HEDGE:    False,
}

_SS_UI_SHADOW      = "ui_shadow_lkg"
_SS_TAB_PREV       = "_tab_prev"
_SS_ACCOUNT        = "ss_account"
_SS_POSITIONS      = "ss_positions"
_SS_EQUITY         = "ss_equity"
_SS_P21            = "ss_p21_execution"
_SS_P17            = "ss_p17_intelligence"
_SS_P22            = "ss_p22"
_SS_ORACLE         = "ss_p15_oracle_signals"
_SS_ANALYTICS      = "ss_analytics"
_SS_MARKET_DATA    = "ss_market_data"
_SS_WHALE_TAPE     = "ss_p22_whale_tape"
_SS_P52            = "ss_p52_supervisor"   # [P52-LKG] prevents ON/OFF flicker
_SS_P53            = "ss_p53_mesh"         # [P53-LKG] prevents mesh panel flicker
_SS_FRAME_SNAPSHOT = "ss_frame_snapshot"
_SS_FRAME_TS       = "ss_frame_ts"
_SS_CTRL_SEQ       = "ss_ctrl_seq"
_SS_SYNC_AT        = "ss_sync_at"
_SS_HAS_TRADES     = "ss_has_trades"
_SS_TACT_LOADED_AT = "ss_tact_loaded_at"

_FIG_KEY_EQUITY    = "fig_lkg_equity"
_FIG_KEY_MB_PIE    = "fig_lkg_microburst_pie"
_FIG_KEY_P17_GAUGE = "fig_lkg_p17_gauge"
_FIG_KEY_ENT_BAR   = "fig_lkg_entropy_bar"
_FIG_KEY_OR_LAT    = "fig_lkg_oracle_latency"
_FIG_KEY_LIQ       = "fig_lkg_liquidation"
_FIG_KEY_CORR      = "fig_lkg_correlation"
_FIG_KEY_MACRO     = "fig_lkg_macro"
# [P44-DASH] [P46-DASH] [P42-DASH] new figure cache keys
_FIG_KEY_P44_WINRATE = "fig_lkg_p44_winrate"
_FIG_KEY_P405_RTT    = "fig_lkg_p40_5_rtt"   # [P40.5-DASH] module-scope so LKG cache is stable across re-renders
_FIG_KEY_P46_LAT     = "fig_lkg_p46_latency"
_FIG_KEY_SPY_BTC     = "fig_lkg_spy_btc_corr"

_WARROOM_TICKERS = []

# ── INST-04: Operator action audit logger ─────────────────────────────────────
_op_log = logging.getLogger("operator_audit")


def _setup_operator_audit_log() -> None:
    """[INST-04] Initialise a dedicated rotating file handler for operator actions."""
    if _op_log.handlers:
        return
    try:
        from logging.handlers import RotatingFileHandler as _RFH
        _h = _RFH(
            str(OPERATOR_AUDIT_PATH),
            maxBytes=5_242_880,
            backupCount=5,
            encoding="utf-8",
        )
        _h.setFormatter(logging.Formatter("%(message)s"))
        _op_log.addHandler(_h)
        _op_log.setLevel(logging.INFO)
        _op_log.propagate = False
    except Exception:
        log.warning("_setup_operator_audit_log: could not initialise operator audit log", exc_info=True)


def _audit(action: str, **details) -> None:
    """[INST-04] Emit a structured JSONL audit record for an operator action."""
    try:
        record = json.dumps({
            "ts":     datetime.utcnow().isoformat() + "Z",
            "action": action,
            **details,
        }, default=str)
        _op_log.info(record)
    except Exception:
        pass


def _atomic_write(path: Path, content: str) -> None:
    """[SEC-04] Cross-platform atomic file write."""
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    try:
        fd, tmp_path = _tempfile.mkstemp(dir=str(parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, str(path))
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception:
        log.error("_atomic_write: failed path=%s", path, exc_info=True)
        raise


def save_tactical_config(cfg: dict) -> bool:
    """[SEC-04] Atomic cross-platform write. [INST-04] Operator action recorded."""
    try:
        cfg["_written_at"] = datetime.utcnow().isoformat() + "Z"
        _atomic_write(TACTICAL_CONFIG_PATH, json.dumps(cfg, indent=2))
        st.session_state[_SS_TACT_LOADED_AT] = 0.0
        _audit("tactical_config_save", config=cfg)
        return True
    except Exception:
        log.error("save_tactical_config: write failed", exc_info=True)
        return False


def _write_control_event(event: str, **kwargs) -> bool:
    """[FIX-INST-02-RESIDUAL] Append-only queue — rapid calls never drop events."""
    try:
        seq = st.session_state.get(_SS_CTRL_SEQ, 0) + 1
        st.session_state[_SS_CTRL_SEQ] = seq

        CONTROL_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)

        record = json.dumps(
            {"event": event, "ts": time.time(), "seq": seq, **kwargs},
            separators=(",", ":"),
        )

        with CONTROL_QUEUE_PATH.open("a", encoding="utf-8") as fq:
            fq.write(record + "\n")

        try:
            _audit(f"control_event event={event} seq={seq}")
        except Exception:
            pass

        return True

    except Exception:
        log.error("_write_control_event: failed event=%r", event, exc_info=True)
        return False


def _drain_control_queue() -> list[dict]:
    """Read all pending events from the queue and clear the file."""
    if not CONTROL_QUEUE_PATH.exists():
        return []
    events: list[dict] = []
    try:
        raw = CONTROL_QUEUE_PATH.read_text(encoding="utf-8", errors="replace")
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                log.warning("_drain_control_queue: bad JSON line skipped: %r", line[:80])
        CONTROL_QUEUE_PATH.write_text("", encoding="utf-8")
    except Exception:
        log.error("_drain_control_queue: failed", exc_info=True)
    return events


def _esc(s) -> str:
    """[FIX-SEC-01] HTML-escape ALL external string data before HTML embedding."""
    return (str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;"))


def _norm_sym(s: str) -> str:
    """[FIX-CRIT-07] Normalise any exchange symbol to bare ticker."""
    return str(s).upper().replace("-USDT", "").replace("-SWAP", "").replace("-PERP", "")


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v or default)
    except (TypeError, ValueError):
        return default


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(v or default)
    except (TypeError, ValueError):
        return default


# ══════════════════════════════════════════════════════════════════════════════
# CSS — OBSIDIAN TERMINAL THEME
# ══════════════════════════════════════════════════════════════════════════════

def _inject_css(panic: bool = False, stale: bool = False) -> None:
    extra = ""
    if panic:
        extra += f"""
      .ot-terminal-bar {{
        background:{T['panic_bg']} !important;
        border-bottom:2px solid {T['red']} !important;
        animation:panic-flash 0.8s ease-in-out infinite;
      }}
      @keyframes panic-flash {{
        0%,100%{{background:{T['panic_bg']};}}
        50%{{background:rgba(255,0,60,0.35);}}
      }}"""
    if stale:
        extra += f"""
      .ot-terminal-bar {{
        border-bottom:2px solid {T['yellow']} !important;
        animation:stale-pulse 2s ease-in-out infinite;
      }}
      @keyframes stale-pulse {{
        0%,100%{{opacity:1;}} 50%{{opacity:0.65;}}
      }}"""

    st.markdown(f"""
<style>
  div[data-testid='stStatusWidget']{{visibility:hidden !important;}}
  #MainMenu,footer{{visibility:hidden;}}
  header{{background:transparent !important;}}
  html,body,.stApp{{
    background:{T['bg']} !important;
    color:{T['text']};
    font-family:{_FONT_SANS_CSS};
  }}
  .block-container{{padding:0.4rem 0.8rem 0.8rem 0.8rem !important;}}
  section[data-testid="stSidebar"]{{background:{T['surface']};border-right:1px solid {T['border']};}}
  [data-testid="collapsedControl"]{{
    color:{T['text']} !important;
    background:{T['surface']} !important;
    border-radius:0 4px 4px 0;
  }}
  .ot-terminal-bar{{
    background:{T['surface2']};
    border-bottom:1px solid {T['border']};
    padding:4px 14px;
    display:flex; gap:20px; align-items:center;
    font-family:{T['mono']};
    font-size:0.66rem;
    color:{T['text2']};
    letter-spacing:0.04em;
    overflow-x:auto; white-space:nowrap;
    margin-bottom:6px;
  }}
  .ot-ok   {{color:{T['green']};font-weight:600;}}
  .ot-warn {{color:{T['yellow']};font-weight:600;}}
  .ot-err  {{color:{T['red']};font-weight:700;}}
  .ot-tile{{
    background:{T['surface']};
    border:1px solid {T['border']};
    border-radius:3px;
    padding:10px 12px;
    margin-bottom:6px;
    position:relative;
    overflow:hidden;
  }}
  .ot-tile::after{{
    content:'';
    position:absolute;top:0;left:0;right:0;height:1px;
    background:linear-gradient(90deg,transparent,rgba(0,255,65,0.08),transparent);
  }}
  .ot-kpi-label{{
    font-size:0.58rem;
    color:{T['text2']};
    letter-spacing:0.12em;
    text-transform:uppercase;
    font-family:{_FONT_SANS_CSS};
    margin-bottom:3px;
  }}
  .ot-kpi-value{{
    font-family:{T['mono']};
    font-size:1.30rem;
    font-weight:700;
    color:{T['green']};
    line-height:1.1;
  }}
  .ot-kpi-sub{{
    font-family:{T['mono']};
    font-size:0.65rem;
    color:{T['text2']};
    margin-top:2px;
  }}
  .ot-section{{
    font-family:{_FONT_SANS_CSS};
    font-size:0.62rem;
    letter-spacing:0.14em;
    text-transform:uppercase;
    color:{T['text2']};
    border-bottom:1px solid {T['border']};
    padding-bottom:4px;
    margin:6px 0 8px 0;
    display:flex;align-items:center;gap:6px;
  }}
  .ot-alert-red{{
    background:rgba(255,0,60,0.12);
    border:1px solid {T['red']}44;
    border-radius:3px;
    padding:6px 10px;
    font-family:{T['mono']};
    font-size:0.72rem;
    color:{T['red']};
    margin-bottom:6px;
  }}
  .ot-alert-yellow{{
    background:rgba(255,214,0,0.10);
    border:1px solid {T['yellow']}44;
    border-radius:3px;
    padding:6px 10px;
    font-family:{T['mono']};
    font-size:0.72rem;
    color:{T['yellow']};
    margin-bottom:6px;
  }}
  .ot-alert-green{{
    background:rgba(0,255,65,0.08);
    border:1px solid {T['green']}44;
    border-radius:3px;
    padding:6px 10px;
    font-family:{T['mono']};
    font-size:0.72rem;
    color:{T['green']};
    margin-bottom:6px;
  }}
  .ot-heat-track{{
    background:{T['border2']};
    border-radius:2px;
    height:6px;
    overflow:hidden;
    margin-top:4px;
  }}
  .ot-heat-fill{{
    height:100%;
    border-radius:2px;
    transition:width 0.4s ease;
  }}
  .ot-minibar-bg{{height:3px;background:{T['border2']};border-radius:2px;margin-top:2px;}}
  .ot-minibar-fill{{height:100%;border-radius:2px;}}
  .ot-table{{width:100%;border-collapse:collapse;font-family:{T['mono']};font-size:0.68rem;}}
  .ot-table th{{
    padding:4px 8px;text-align:left;
    font-size:0.56rem;letter-spacing:0.12em;text-transform:uppercase;
    color:{T['muted']};border-bottom:1px solid {T['border']};
    background:{T['surface2']};
  }}
  .ot-table td{{padding:3px 8px;border-bottom:1px solid {T['border']}22;}}
  .ot-table tr:hover td{{background:{T['surface2']};}}
  .ot-scroll{{max-height:240px;overflow:auto;}}
  .ot-scroll-tall{{max-height:320px;overflow:auto;}}
  .ot-badge{{
    display:inline-block;
    padding:1px 6px;
    border-radius:2px;
    font-family:{T['mono']};
    font-size:0.60rem;
    font-weight:700;
    letter-spacing:0.06em;
  }}
  .ot-council-row{{
    display:grid;
    grid-template-columns:90px 55px 60px 1fr;
    gap:4px;
    align-items:center;
    padding:3px 6px;
    border-bottom:1px solid {T['border']}44;
    font-family:{T['mono']};
    font-size:0.66rem;
  }}
  .ot-whale-row{{
    display:flex;
    justify-content:space-between;
    align-items:center;
    padding:3px 6px;
    border-bottom:1px solid {T['border']}33;
    font-family:{T['mono']};
    font-size:0.68rem;
  }}
  .snr-accurate{{color:{T['green']};font-weight:700;}}
  .snr-moderate{{color:{T['yellow']};font-weight:600;}}
  .snr-noisy{{color:{T['red']};font-weight:600;}}
  .snr-none{{color:{T['muted']};}}
  .trust-high{{color:{T['green']};}}
  .trust-mid{{color:{T['yellow']};}}
  .trust-low{{color:{T['red']};}}
  /* [P44/P46/P42-DASH] New institutional classes */
  .ot-corr-decouple{{
    background:rgba(251,146,60,0.14);
    border:1px solid {T['orange']}55;
    border-radius:3px;
    padding:4px 10px;
    font-family:{T['mono']};
    font-size:0.70rem;
    color:{T['orange']};
    margin-bottom:5px;
    display:inline-flex;align-items:center;gap:6px;
  }}
  .ot-kelly-bar-bg{{
    background:{T['border2']};
    border-radius:3px;
    height:8px;
    overflow:hidden;
    margin:4px 0;
  }}
  .ot-kelly-bar-fill{{
    height:100%;
    border-radius:3px;
    transition:width 0.6s ease;
  }}
  .ot-p46-badge{{
    display:inline-block;
    padding:1px 5px;
    border-radius:2px;
    font-family:{T['mono']};
    font-size:0.58rem;
    font-weight:700;
    letter-spacing:0.04em;
  }}
  .ot-p46-active{{color:{T['green']};background:rgba(34,197,94,0.14);border:1px solid {T['green']}44;}}
  .ot-p46-inactive{{color:{T['muted']};background:{T['surface']};border:1px solid {T['border']};}}
  .ot-p44-heat{{
    padding:6px 10px;border-radius:3px;
    font-family:{T['mono']};font-size:0.70rem;margin-bottom:5px;
  }}
  .ot-p44-warn{{background:rgba(250,204,21,0.10);border:1px solid {T['yellow']}44;color:{T['yellow']};}}
  .ot-p44-pause{{background:rgba(244,63,94,0.12);border:1px solid {T['red']}44;color:{T['red']};}}
  .ot-p44-ok{{background:rgba(34,197,94,0.08);border:1px solid {T['green']}44;color:{T['green']};}}
  .ot-p49-active{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;letter-spacing:0.04em;
    color:{T['green']};background:rgba(34,197,94,0.14);border:1px solid {T['green']}44;
  }}
  .ot-p49-partial{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['yellow']};background:rgba(250,204,21,0.10);border:1px solid {T['yellow']}44;
  }}
  .ot-p49-idle{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['muted']};background:{T['surface']};border:1px solid {T['border']};
  }}
  .ot-p49-cap{{
    display:inline-block;padding:0px 5px;border-radius:2px;margin:1px;
    font-family:{T['mono']};font-size:0.55rem;
    color:{T['green']};background:rgba(34,197,94,0.10);border:1px solid {T['green']}33;
  }}
  .ot-p49-row{{
    display:flex;justify-content:space-between;align-items:center;
    font-family:{T['mono']};font-size:0.62rem;color:{T['muted']};
    padding:2px 0;border-bottom:1px solid {T['border']}44;
  }}
  .ot-p49-band{{
    background:rgba(168,85,247,0.06);border:1px solid rgba(168,85,247,0.20);
    border-radius:4px;padding:3px 8px;margin:3px 0;
    font-family:{T['mono']};font-size:0.60rem;color:{T['muted']};
  }}
  .ot-p50-native{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;letter-spacing:0.04em;
    color:{T['green']};background:rgba(34,197,94,0.18);border:1px solid {T['green']}55;
  }}
  .ot-p50-shadow{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['cyan']};background:rgba(6,182,212,0.12);border:1px solid {T['cyan']}44;
  }}
  .ot-p50-idle{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['muted']};background:{T['surface']};border:1px solid {T['border']};
  }}
  .ot-p50-agree{{color:{T['green']};font-weight:700;}}
  .ot-p50-disagree{{color:{T['red']};font-weight:700;}}
  .ot-p50-row{{
    display:flex;justify-content:space-between;align-items:center;
    font-family:{T['mono']};font-size:0.62rem;color:{T['muted']};
    padding:2px 0;border-bottom:1px solid {T['border']}44;
  }}
  .ot-p50-band{{
    background:rgba(6,182,212,0.05);border:1px solid rgba(6,182,212,0.18);
    border-radius:4px;padding:3px 8px;margin:3px 0;
    font-family:{T['mono']};font-size:0.60rem;color:{T['muted']};
  }}
  .ot-lat-fast{{color:{T['green']};font-weight:700;}}
  .ot-lat-warn{{color:{T['yellow']};font-weight:600;}}
  .ot-lat-slow{{color:{T['red']};font-weight:700;}}
  /* ── [P51] Institutional Execution Suite ──────────────────────────────── */
  .ot-p51-native{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;letter-spacing:0.04em;
    color:{T['green']};background:rgba(34,197,94,0.18);border:1px solid {T['green']}55;
  }}
  .ot-p51-shadow{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['yellow']};background:rgba(234,179,8,0.12);border:1px solid {T['yellow']}44;
  }}
  .ot-p51-idle{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['muted']};background:{T['surface']};border:1px solid {T['border']};
  }}
  .ot-p51-algo-row{{
    display:flex;justify-content:space-between;align-items:center;
    font-family:{T['mono']};font-size:0.58rem;color:{T['text2']};
    padding:2px 0;border-bottom:1px solid {T['border']}44;
  }}
  .ot-p51-band{{
    background:rgba(234,179,8,0.05);border:1px solid rgba(234,179,8,0.18);
    border-radius:4px;padding:3px 8px;margin:3px 0;
    font-family:{T['mono']};font-size:0.60rem;color:{T['muted']};
  }}
  .ot-p51-fill{{color:{T['green']};font-weight:700;}}
  .ot-p51-pct{{color:{T['cyan']};font-weight:700;}}
  /* ── [P52] Engine Supervisor Health ───────────────────────────────────────── */
  .ot-p52-active{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;letter-spacing:0.04em;
    color:{T['green']};background:rgba(34,197,94,0.18);border:1px solid {T['green']}55;
  }}
  .ot-p52-idle{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['muted']};background:{T['surface']};border:1px solid {T['border']};
  }}
  .ot-p52-warn{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['yellow']};background:rgba(234,179,8,0.12);border:1px solid {T['yellow']}44;
  }}
  .ot-p52-band{{
    background:rgba(34,197,94,0.04);border:1px solid rgba(34,197,94,0.18);
    border-radius:4px;padding:3px 8px;margin:3px 0;
    font-family:{T['mono']};font-size:0.60rem;color:{T['muted']};
  }}
  .ot-p52-row{{
    display:flex;justify-content:space-between;align-items:center;
    font-family:{T['mono']};font-size:0.57rem;color:{T['text2']};
    padding:2px 0;border-bottom:1px solid {T['border']}44;
  }}
  .ot-p52-val{{color:{T['text']};font-weight:700;}}
  .ot-p52-ok{{color:{T['green']};font-weight:700;}}
  .ot-p52-bad{{color:{T['red']};font-weight:700;}}
  /* ── [P53] Mesh Consensus Gate ─────────────────────────────────────────────── */
  .ot-p53-active{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;letter-spacing:0.04em;
    color:{T['cyan']};background:rgba(0,194,168,0.15);border:1px solid {T['cyan']}55;
  }}
  .ot-p53-idle{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['muted']};background:{T['surface']};border:1px solid {T['border']};
  }}
  .ot-p53-warn{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']}  /* ── [P54] Kernel-Bypass Panel ─────────────────────────────────────────────── */
  .ot-p54-dpdk{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;letter-spacing:0.04em;
    color:{T['green']};background:rgba(34,197,94,0.18);border:1px solid {T['green']}55;
  }}
  .ot-p54-onload{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['blue']};background:rgba(96,165,250,0.15);border:1px solid {T['blue']}44;
  }}
  .ot-p54-standard{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['yellow']};background:rgba(234,179,8,0.10);border:1px solid {T['yellow']}33;
  }}
  .ot-p54-off{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['muted']};background:{T['surface']};border:1px solid {T['border']};
  }}
  .ot-p54-alert{{color:{T['red']};font-weight:700;}}
  .ot-p54-pending{{
    display:inline-block;padding:1px 6px;border-radius:2px;
    font-family:{T['mono']};font-size:0.60rem;font-weight:700;
    color:{T['yellow']};background:rgba(250,204,21,0.12);border:1px solid {T['yellow']}44;
  }}
  .ot-p53-band{{
    background:rgba(0,194,168,0.04);border:1px solid rgba(0,194,168,0.18);
    border-radius:4px;padding:3px 8px;margin:3px 0;
    font-family:{T['mono']};font-size:0.58rem;color:{T['muted']};
  }}
  .ot-p53-row{{
    display:flex;justify-content:space-between;align-items:center;
    font-family:{T['mono']};font-size:0.57rem;color:{T['text2']};
    padding:2px 0;border-bottom:1px solid {T['border']}44;
  }}
  .ot-p53-val{{color:{T['text']};font-weight:700;}}
  .ot-p53-ok{{color:{T['cyan']};font-weight:700;}}
  .ot-p53-blocked{{color:{T['red']};font-weight:700;}}
  .ot-p53-node-alive{{
    display:inline-block;padding:0 4px;border-radius:2px;
    font-family:{T['mono']};font-size:0.54rem;font-weight:700;
    color:{T['cyan']};background:rgba(0,194,168,0.14);
  }}
  .ot-p53-node-dead{{
    display:inline-block;padding:0 4px;border-radius:2px;
    font-family:{T['mono']};font-size:0.54rem;font-weight:700;
    color:{T['red']};background:rgba(244,63,94,0.14);
  }}
  .ot-p53-sign-on{{color:{T['green']};font-size:0.55rem;font-weight:700;}}
  .ot-p53-sign-off{{color:{T['yellow']};font-size:0.55rem;font-weight:700;}}

  /* ── Bloomberg UX: Band row headers ─────────────────────────────────────── */
  .ot-bband-header{{
    display:flex;align-items:center;gap:8px;
    font-family:{_FONT_SANS_CSS};font-size:0.58rem;font-weight:700;
    letter-spacing:0.16em;text-transform:uppercase;
    color:{T['text2']};border-bottom:1px solid {T['border']};
    padding:4px 0 4px 2px;margin:4px 0 6px 0;
    background:linear-gradient(90deg,rgba(0,255,65,0.04),transparent);
  }}
  .ot-bband-sep{{
    color:{T['border']};font-size:0.55rem;letter-spacing:0.04em;
    padding:0 4px;
  }}
  /* ── Stat grid: 2-col label · value pairs ──────────────────────────────── */
  .ot-stat-grid{{
    display:grid;grid-template-columns:1fr 1fr;gap:0;
  }}
  .ot-stat-cell{{
    display:flex;flex-direction:column;
    padding:4px 6px;
    border-bottom:1px solid {T['border']}22;
  }}
  .ot-stat-label{{
    font-family:{_FONT_SANS_CSS};font-size:0.60rem;
    color:{T['muted']};letter-spacing:0.08em;text-transform:uppercase;
    margin-bottom:2px;
  }}
  .ot-stat-value{{
    font-family:{T['mono']};font-size:0.82rem;font-weight:700;
    color:{T['text']};line-height:1.1;
  }}
  /* ── Panel min-height guards (no collapsing panels) ─────────────────────── */
  .ot-panel-guard{{min-height:180px;}}
  .ot-panel-guard-sm{{min-height:120px;}}
  /* ── Row label · value compact rows ─────────────────────────────────────── */
  .ot-kv-row{{
    display:flex;justify-content:space-between;align-items:center;
    font-family:{T['mono']};font-size:0.68rem;
    padding:2px 0;border-bottom:1px solid {T['border']}18;
  }}
  .ot-kv-label{{color:{T['muted']};}}
  .ot-kv-value{{color:{T['text']};font-weight:700;}}
  /* ── Improved table readability ──────────────────────────────────────────── */
  .ot-table{{width:100%;border-collapse:collapse;font-family:{T['mono']};font-size:0.72rem;}}
  .ot-table th{{
    padding:5px 8px;text-align:left;
    font-size:0.62rem;letter-spacing:0.10em;text-transform:uppercase;
    color:{T['muted']};border-bottom:1px solid {T['border']};
    background:{T['surface2']};position:sticky;top:0;z-index:1;
  }}
  .ot-table td{{padding:4px 8px;border-bottom:1px solid {T['border']}22;}}
  .ot-table tr:hover td{{background:{T['surface2']};}}

  .ot-ticker-wrap{{
    overflow:hidden; white-space:nowrap;
    background:{T['surface2']};
    border-top:1px solid {T['border']};
    border-bottom:1px solid {T['border']};
    padding:4px 0; margin-bottom:6px;
  }}
  .ot-ticker-inner{{
    display:inline-flex; gap:0;
    animation:ot-scroll-ticker 60s linear infinite;
  }}
  .ot-ticker-inner:hover{{animation-play-state:paused;}}
  @keyframes ot-scroll-ticker{{
    0%{{transform:translateX(0);}} 100%{{transform:translateX(-50%);}}
  }}
  .ot-tick-item{{
    display:inline-flex; gap:10px; align-items:center;
    font-family:{T['mono']}; font-size:0.67rem;
    padding:0 18px; border-right:1px solid {T['border']};
    color:{T['text2']};
  }}
  .ot-tick-green{{color:{T['green']};}}
  .ot-tick-red{{color:{T['red']};}}
  .ot-tick-yellow{{color:{T['yellow']};}}
  .ot-tick-gray{{color:{T['muted']};}}
  {extra}
</style>
""", unsafe_allow_html=True)


# ── Session-state key for auth ────────────────────────────────────────────────
_SS_AUTHED = "_dashboard_authenticated"

_AUTH_DEV_BYPASS_MSG = (
    "⚠ No dashboard password configured — "
    "set [dashboard] password_hash in .streamlit/secrets.toml before live deployment."
)


def _check_password(candidate: str) -> bool:
    """Constant-time comparison against stored SHA-256 hash."""
    try:
        stored_hash = st.secrets["dashboard"]["password_hash"]
    except (KeyError, AttributeError, Exception):
        return True

    candidate_hash = _hashlib.sha256(candidate.encode("utf-8")).hexdigest()
    return _hashlib.compare_digest(candidate_hash, stored_hash)


def _auth_gate() -> bool:
    """[INST-01] Password gate — must be called as the FIRST statement in main()."""
    _has_secrets = False
    try:
        _ = st.secrets["dashboard"]["password_hash"]
        _has_secrets = True
    except Exception:
        pass

    if not _has_secrets:
        st.markdown(
            f'<div class="ot-alert-yellow" style="margin:8px 0;">'
            f'{_AUTH_DEV_BYPASS_MSG}</div>',
            unsafe_allow_html=True,
        )
        return True

    if st.session_state.get(_SS_AUTHED):
        return True

    st.markdown(
        f"""
<style>
  .auth-wrap {{
    max-width:360px;margin:80px auto;padding:32px 28px;
    background:{T['surface']};border:1px solid {T['border']};border-radius:6px;
  }}
  .auth-title {{
    font-family:{T['mono']};font-size:1.1rem;font-weight:700;
    color:{T['green']};letter-spacing:0.08em;margin-bottom:20px;
    text-align:center;
  }}
</style>
<div class="auth-wrap">
  <div class="auth-title">⚡ POWERTRADER AI · TERMINAL</div>
</div>""",
        unsafe_allow_html=True,
    )

    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        st.markdown(
            f'<p style="font-family:{T["mono"]};font-size:0.72rem;'
            f'color:{T["text2"]};text-align:center;margin-bottom:8px;">'
            f'OPERATOR AUTHENTICATION</p>',
            unsafe_allow_html=True,
        )
        pwd = st.text_input(
            "Password",
            type="password",
            placeholder="Enter dashboard password",
            key="_auth_pwd_input",
            label_visibility="collapsed",
        )
        if st.button("AUTHENTICATE", key="_auth_submit", use_container_width=True):
            if _check_password(pwd):
                st.session_state[_SS_AUTHED] = True
                _audit("operator_login", result="success")
                st.rerun()
            else:
                _audit("operator_login", result="failed")
                st.error("❌ Invalid password.")

    return False


# ══════════════════════════════════════════════════════════════════════════════
# TIER 1 — DATA LAYER  (STASH-GATE)
# ══════════════════════════════════════════════════════════════════════════════

def hydrate_institutional_state(status: dict) -> None:
    """[FIX-QA-04] Plain if/else — no lambda pattern.
    [FIX-CRIT-04] Explicit isinstance(acct, dict) guard before equity latch.
    [FIX-CRIT-05] Only this function and _get_binary_truth_equity() write equity latches.
    """
    def _pd(k: str, slot: str) -> None:
        v = status.get(k)
        if isinstance(v, dict) and v:
            st.session_state[slot] = v

    def _pl(k: str, slot: str) -> None:
        v = status.get(k)
        if isinstance(v, list) and v:
            st.session_state[slot] = v

    _pd("account",            _SS_ACCOUNT)
    _pd("positions",          _SS_POSITIONS)
    _pd("p17_intelligence",   _SS_P17)
    _pd("p22",                _SS_P22)
    _pd("p21_execution",      _SS_P21)
    _pd("p15_oracle_signals", _SS_ORACLE)
    _pd("analytics",          _SS_ANALYTICS)
    _pd("market_data",        _SS_MARKET_DATA)
    _pd("p52_supervisor",     _SS_P52)         # [P52-LKG] preserve supervisor state
    _pd("p53_mesh",           _SS_P53)         # [P53-LKG] preserve mesh consensus state
    _pd("p54_kernel_bypass",  "ss_p54_bypass") # [P54-LKG] preserve kernel-bypass telemetry
    _pl("p22_whale_tape",     _SS_WHALE_TAPE)

    acct = status.get("account")
    if not isinstance(acct, dict) or not acct:
        return
    for _k in ("retained_equity", "total_equity", "equity", "last_valid_equity"):
        try:
            _v = float(acct.get(_k) or 0)
            if _v > 0:
                st.session_state[_SS_EQUITY]         = _v
                st.session_state["last_valid_equity"] = _v
                st.session_state["_bt_equity_lkg"]    = _v
                break
        except (TypeError, ValueError):
            pass


def _sg(slot: str, default=None):
    return st.session_state.get(slot, default)


def _inject_lkg(status: dict) -> dict:
    """Back-fill volatile nulls from shadow slots without mutating the original.
    [P42-DASH]  Preserves p42_shadow so macro panel never goes blank on stale frame.
    [P40.5-LKG] Preserves p40_5_heartbeat so IPC panel never flickers to
                 "not yet available" on a single _gui_bridge exception cycle.
    [P47-LKG]   Preserves p47_microstructure across stale frames.
    """
    result = dict(status)
    for key, slot, typ in (
        ("account",            _SS_ACCOUNT,     dict),
        ("positions",          _SS_POSITIONS,   dict),
        ("p17_intelligence",   _SS_P17,         dict),
        ("p22",                _SS_P22,         dict),
        ("p21_execution",      _SS_P21,         dict),
        ("p15_oracle_signals", _SS_ORACLE,      dict),
        ("analytics",          _SS_ANALYTICS,   dict),
        ("market_data",        _SS_MARKET_DATA, dict),
        ("p52_supervisor",     _SS_P52,         dict),  # [P52-LKG] prevents ⚫ OFF flicker
        ("p53_mesh",           _SS_P53,         dict),  # [P53-LKG] prevents mesh panel flicker
        ("p54_kernel_bypass",  "ss_p54_bypass", dict),  # [P54-LKG] prevents bypass panel flicker
        ("p22_whale_tape",     _SS_WHALE_TAPE,  list),
    ):
        v = result.get(key)
        if not isinstance(v, typ) or not v:
            shadow = st.session_state.get(slot)
            if isinstance(shadow, typ) and shadow:
                result[key] = shadow
            elif typ is dict:
                result.setdefault(key, {})
            else:
                result.setdefault(key, [])

    # [P42-DASH] Preserve p42_shadow + DCB keys on stale frames
    for _dict_key in ("p42_shadow", "p44_dcb_frozen", "p44_dcb_freeze_remaining"):
        if _dict_key not in result or not result[_dict_key]:
            _lkg_val = st.session_state.get(f"_lkg_{_dict_key}")
            if _lkg_val is not None:
                result[_dict_key] = _lkg_val

    # [P40.5-LKG] Heartbeat snapshot: write to LKG when valid, restore when missing.
    # Root cause of "data sometimes disappears": any exception in main.py _gui_bridge()
    # causes status["p40_5_heartbeat"] = {"enabled": False} — missing heartbeat_interval_secs
    # → panel guard fires → "not yet available" placeholder for that render cycle.
    _hb = result.get("p40_5_heartbeat")
    if isinstance(_hb, dict) and _hb.get("heartbeat_interval_secs"):
        # Valid frame — write to LKG
        st.session_state["_lkg_p40_5_heartbeat"] = _hb
    else:
        # Invalid / missing — restore from LKG
        _hb_lkg = st.session_state.get("_lkg_p40_5_heartbeat")
        if isinstance(_hb_lkg, dict) and _hb_lkg.get("heartbeat_interval_secs"):
            result["p40_5_heartbeat"] = _hb_lkg

    # [P47-LKG] Microstructure snapshot: same LKG pattern
    _ms = result.get("p47_microstructure")
    if isinstance(_ms, dict) and _ms.get("enabled") is not None:
        st.session_state["_lkg_p47_microstructure"] = _ms
    else:
        _ms_lkg = st.session_state.get("_lkg_p47_microstructure")
        if isinstance(_ms_lkg, dict):
            result["p47_microstructure"] = _ms_lkg

    # [P48-LKG] Autonomous Floating snapshot: same LKG pattern
    _p48 = result.get("p48_floating")
    if isinstance(_p48, dict) and _p48.get("enabled") is not None:
        st.session_state["_lkg_p48_floating"] = _p48
    else:
        _p48_lkg = st.session_state.get("_lkg_p48_floating")
        if isinstance(_p48_lkg, dict):
            result["p48_floating"] = _p48_lkg

    # [P49-LKG] Hybrid Bridge Core snapshot: same LKG pattern
    _p49 = result.get("p49_protocol")
    if isinstance(_p49, dict) and _p49.get("enabled") is not None:
        st.session_state["_lkg_p49_protocol"] = _p49
    else:
        _p49_lkg = st.session_state.get("_lkg_p49_protocol")
        if isinstance(_p49_lkg, dict):
            result["p49_protocol"] = _p49_lkg

    # [P50-LKG] Native Hot-Path snapshot: same LKG pattern
    _p50 = result.get("p50_native")
    if isinstance(_p50, dict) and _p50.get("enabled") is not None:
        st.session_state["_lkg_p50_native"] = _p50
    else:
        _p50_lkg = st.session_state.get("_lkg_p50_native")
        if isinstance(_p50_lkg, dict):
            result["p50_native"] = _p50_lkg

    # [P51-LKG] Institutional Execution Suite snapshot: same LKG pattern
    _p51 = result.get("p51_algo")
    if isinstance(_p51, dict) and _p51.get("enabled") is not None:
        st.session_state["_lkg_p51_algo"] = _p51
    else:
        _p51_lkg = st.session_state.get("_lkg_p51_algo")
        if isinstance(_p51_lkg, dict):
            result["p51_algo"] = _p51_lkg

    return result


def _get_status() -> dict:
    """[FIX-ARCH-01] Central read for ALL fragments — zero stale param capture."""
    raw = (st.session_state.get(_SS_FRAME_SNAPSHOT)
           or st.session_state.get("last_valid_status")
           or {})
    return _inject_lkg(dict(raw))


def _load_status() -> tuple[dict, bool]:
    """
    [SEC-02] Size-guarded read.
    [OPS-05] Zero blocking sleep.
    [FIX-CRIT-03] Monotonic guard: clean _mono_key flow, no dead _prev variable.
    """
    REQUIRED = {"timestamp"}

    if TRADER_STATUS.exists():
        try:
            file_size = TRADER_STATUS.stat().st_size
            if file_size > _MAX_STATUS_BYTES:
                log.error(
                    "_load_status: status file too large (%d bytes > %d limit) — skipping",
                    file_size, _MAX_STATUS_BYTES,
                )
                cached = st.session_state.get("last_valid_status")
                if cached:
                    return cached, False
                return {}, False
            if file_size == 0:
                cached = st.session_state.get("last_valid_status")
                return (cached, False) if cached else ({}, False)
        except OSError as _se:
            log.warning("_load_status: stat failed: %s", _se)

        for attempt in range(2):
            try:
                raw = TRADER_STATUS.read_bytes()
                if not raw.strip():
                    break
                parsed = json.loads(raw.decode("utf-8"))
                if not isinstance(parsed, dict) or not REQUIRED.issubset(parsed.keys()):
                    break

                def _mono_key(d: dict) -> float:
                    for _f in ("frame_id", "timestamp", "heartbeat_ts"):
                        try:
                            _v = d.get(_f)
                            if _v is not None:
                                return float(_v)
                        except (TypeError, ValueError):
                            pass
                    return 0.0

                prev_key = _mono_key(st.session_state.get("last_valid_status") or {})
                new_key  = _mono_key(parsed)
                if prev_key > 0 and new_key > 0 and prev_key > new_key + 0.5:
                    cached = st.session_state.get("last_valid_status")
                    if cached:
                        return cached, False

                if not isinstance(parsed.get("account"),   dict):
                    parsed["account"]   = st.session_state.get(_SS_ACCOUNT)   or {}
                if not isinstance(parsed.get("positions"), dict):
                    parsed["positions"] = st.session_state.get(_SS_POSITIONS) or {}
                if not isinstance(parsed.get("analytics"), dict):
                    parsed["analytics"] = st.session_state.get(_SS_ANALYTICS) or {}

                st.session_state["last_valid_status"]    = parsed
                st.session_state["last_valid_status_ts"] = time.time()
                hydrate_institutional_state(parsed)
                return parsed, True

            except json.JSONDecodeError as e:
                log.warning("_load_status: JSONDecodeError attempt=%d: %s", attempt, e)
            except Exception:
                log.error("_load_status: unexpected error attempt=%d", attempt, exc_info=True)
                break

    cached = st.session_state.get("last_valid_status")
    if cached:
        return cached, False
    return {}, False


def _get_binary_truth_equity(status: dict) -> float:
    """[FIX-CRIT-05] ONLY authoritative equity latch writer alongside hydrate_institutional_state."""
    account = status.get("account") if isinstance(status.get("account"), dict) else {}
    for candidate in (
        account.get("retained_equity"),
        account.get("total_equity"),
        account.get("equity"),
        (status.get("p23_ghost_meter") or {}).get("last_valid_equity"),
        account.get("last_valid_equity"),
    ):
        try:
            v = float(candidate)
            if v > 0.0:
                st.session_state["last_valid_equity"] = v
                st.session_state[_SS_EQUITY]          = v
                st.session_state["_bt_equity_lkg"]    = v
                return v
        except (TypeError, ValueError):
            continue

    for _k in ("last_valid_equity", _SS_EQUITY, "_bt_equity_lkg"):
        try:
            v = float(st.session_state.get(_k) or 0)
            if v > 0.0:
                return v
        except (TypeError, ValueError):
            pass
    return 0.0


def _bot_is_live() -> bool:
    """[INST-03] Returns True only if the bot has written a status file within _BOT_HEARTBEAT_STALE_SECS."""
    return _status_age_secs() <= _BOT_HEARTBEAT_STALE_SECS


def _bot_dead_warning(action_label: str) -> None:
    """Show a throttled warning when a write is blocked due to bot being dead."""
    _k = "_bot_dead_warn_last"
    now = time.time()
    if now - st.session_state.get(_k, 0) > _BOT_DEAD_WARN_THROTTLE:
        st.sidebar.warning(
            f"⚠ BOT OFFLINE — '{action_label}' blocked until heartbeat resumes.",
            icon="🔌",
        )
        st.session_state[_k] = now


def _is_demo_mode() -> bool:
    """[INST-05] Check current status for demo_mode flag."""
    status = _get_status()
    return bool(status.get("demo_mode", True))


# ══════════════════════════════════════════════════════════════════════════════
# DATABASE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_db_local = _threading.local()


def _get_db_conn():
    """[SEC-03] Thread-local SQLite connection pool."""
    if not DB_PATH.exists():
        return None
    conn = getattr(_db_local, "conn", None)
    if conn is not None:
        try:
            conn.execute("SELECT 1")
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            _db_local.conn = None

    try:
        conn = sqlite3.connect(
            str(DB_PATH),
            check_same_thread=False,
            timeout=10,
            isolation_level=None,   # autocommit — each SELECT sees latest committed data
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=9000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-4096;")
        conn.execute("PRAGMA read_uncommitted=0;")
        _db_local.conn = conn
        log.debug("_get_db_conn: new thread-local connection thread=%s", _threading.current_thread().name)
        return conn
    except Exception:
        log.error("_get_db_conn: failed to open %s", DB_PATH, exc_info=True)
        return None


def _query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Thread-safe query; each call uses the caller thread's own connection."""
    conn = _get_db_conn()
    if conn is None:
        return pd.DataFrame()
    for attempt in range(3):
        try:
            return pd.read_sql_query(sql, conn, params=params)
        except Exception as e:
            if "locked" in str(e).lower() and attempt < 2:
                time.sleep(0.05 * (attempt + 1))
                continue
            log.error("_query: attempt=%d sql=%r error=%s", attempt, sql[:80], e)
            break
    return pd.DataFrame()


@st.cache_data(ttl=CACHE_FAST)
def _get_known_tables() -> frozenset:
    """TTL reduced to CACHE_FAST — new tables detected within 3s."""
    conn = _get_db_conn()
    if conn is None:
        return frozenset()
    try:
        df = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table'", conn
        )
        return frozenset(df["name"].tolist())
    except Exception:
        log.error("_get_known_tables: failed", exc_info=True)
        return frozenset()


def _lkg_load(fn_name: str, loader_fn, *args, **kwargs) -> pd.DataFrame:
    """[FIX-ARCH-04] LKG_MAX_AGE_SECS cap with visible stale warning in caller."""
    lkg_key    = f"_lkg_{fn_name}"
    lkg_ts_key = f"_lkg_{fn_name}_ts"
    try:
        df = loader_fn(*args, **kwargs)
    except Exception:
        log.error("_lkg_load: loader_fn=%s failed", fn_name, exc_info=True)
        df = pd.DataFrame()
    if not df.empty:
        st.session_state[lkg_key]    = df
        st.session_state[lkg_ts_key] = time.time()
        return df
    lkg = st.session_state.get(lkg_key, pd.DataFrame())
    return lkg


def _lkg_age(fn_name: str) -> float:
    """Seconds since last successful load for a named LKG slot."""
    ts = st.session_state.get(f"_lkg_{fn_name}_ts")
    return (time.time() - ts) if ts else float("inf")


@st.cache_data(ttl=CACHE_SLOW)
def _load_equity_raw() -> pd.DataFrame:
    tables = _get_known_tables()
    if "snapshots" in tables:
        df = _query("SELECT ts, equity, avail FROM snapshots ORDER BY ts DESC LIMIT 500")
    else:
        df = pd.DataFrame()
    if not df.empty:
        df = df.sort_values("ts").reset_index(drop=True)
        df["dt"] = pd.to_datetime(df["ts"], unit="s")
        return df
    df = _query(
        "SELECT ts, total_equity AS equity, buying_power AS avail "
        "FROM account_snapshots ORDER BY ts DESC LIMIT 500"
    )
    if df.empty:
        return df
    df = df.sort_values("ts").reset_index(drop=True)
    df["dt"] = pd.to_datetime(df["ts"], unit="s")
    return df


def load_equity() -> pd.DataFrame:
    return _lkg_load("equity", _load_equity_raw)


# ── [FIX-TRADES] Resilient multi-schema trade query ──────────────────────────

@st.cache_data(ttl=CACHE_FAST)
def _load_trades_raw() -> pd.DataFrame:
    """[FIX-TRADES] Resilient multi-schema query; handles missing optional columns."""
    conn = _get_db_conn()
    if conn is None:
        return pd.DataFrame()
    # Flush WAL to main DB file so this reader sees all committed bot writes
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
    except Exception:
        pass
    # Direct table check — bypasses _get_known_tables cache for trade freshness
    try:
        chk = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='trades'", conn
        )
        if chk.empty:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    _FULL_SQL = """
        SELECT ts, symbol, side, qty, price,
               cost_basis, pnl_pct, realized_usd, tag, inst_type,
               exit_reason_type, exit_reason_category, is_close_leg
        FROM trades ORDER BY ts DESC LIMIT 500
    """
    _PART_SQL = """
        SELECT ts, symbol, side, qty, price, pnl_pct, realized_usd
        FROM trades ORDER BY ts DESC LIMIT 500
    """
    _WILD_SQL = "SELECT * FROM trades ORDER BY ts DESC LIMIT 500"

    df = pd.DataFrame()
    for sql in (_FULL_SQL, _PART_SQL, _WILD_SQL):
        try:
            df = _query(sql.strip())
            if not df.empty:
                break
        except Exception as _e:
            log.debug("_load_trades_raw: schema attempt failed: %s", _e)
            df = pd.DataFrame()

    if df.empty:
        return df

    # [FIX-RENAME-PURGE] Do NOT rename ts/pnl_pct — all downstream consumers
    # (render_trade_analytics, _check_trade_toasts, render_candlestick) read
    # original DB column names.  The 'pnl'→'PNL' rename was a no-op (column
    # absent from schema).  'ts'→'TS' and 'pnl_pct'→'PNL%' broke 4 sites.
    for col in ("ts", "pnl_pct", "price", "qty"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    # [PHASE5] Coerce is_close_leg so the canonical KPI filter (== 1) works correctly
    # when SQLite returns it as string/float via the wild-card fallback path.
    if "is_close_leg" in df.columns:
        df["is_close_leg"] = pd.to_numeric(df["is_close_leg"], errors="coerce").fillna(0).astype(int)

    df["dt"] = pd.to_datetime(df["ts"], unit="s", errors="coerce")
    return df



@st.cache_data(ttl=CACHE_FAST)
def _load_close_diagnostics() -> pd.DataFrame:
    """[PHASE5] Dedicated query for profitability-diagnostic grouping.

    Fetches only canonical close-leg rows (is_close_leg = 1) with a larger
    sample than the display table so grouped metrics are statistically meaningful.
    Falls back gracefully when the column is absent (pre-Phase-4 schema).

    [PHASE6] Also fetches entry_confidence, entry_kelly_f, hold_time_secs when
    present (pre-Phase-6 databases return NULL / missing columns, handled below).

    [PHASE10] Also fetches close_slippage_bps, spread_at_close_bps when present.
    Pre-Phase-10 databases return NULL for both.  Pre-Phase-15 HEDGE_TRIM rows also
    return NULL (no tick was captured on that path before Phase 15).  Phase 15 added
    tick-based friction capture for HEDGE_TRIM and liquidation exits; post-Phase-15
    rows carry non-NULL friction values when tick data was available at close time.

    [POST-ROADMAP-DT] Also fetches entry_regime, entry_direction, entry_z_score when
    present.  Pre-DT rows return NULL for all three; the column-coercion block below
    ensures the columns always exist in the returned DataFrame.

    Returns a DataFrame with columns:
        symbol, exit_reason_type, exit_reason_category, tag, realized_usd, pnl_pct
        [+ entry_confidence, entry_kelly_f, hold_time_secs when Phase 6 schema present]
        [+ close_slippage_bps, spread_at_close_bps when Phase 10 schema present]
        [+ entry_regime, entry_direction, entry_z_score when DT schema present]
    or an empty DataFrame on any failure.
    """
    conn = _get_db_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
    except Exception:
        pass

    # Preferred: DT schema — includes entry-context signal columns + friction
    _SQL_DT = """
        SELECT symbol, exit_reason_type, exit_reason_category, tag,
               realized_usd, pnl_pct,
               entry_confidence, entry_kelly_f, hold_time_secs,
               close_slippage_bps, spread_at_close_bps,
               entry_regime, entry_direction, entry_z_score
        FROM trades
        WHERE is_close_leg = 1
        ORDER BY ts DESC LIMIT 2000
    """
    # Fallback A: Phase-10 schema — includes friction columns but no entry-context
    _SQL_P10 = """
        SELECT symbol, exit_reason_type, exit_reason_category, tag,
               realized_usd, pnl_pct,
               entry_confidence, entry_kelly_f, hold_time_secs,
               close_slippage_bps, spread_at_close_bps
        FROM trades
        WHERE is_close_leg = 1
        ORDER BY ts DESC LIMIT 2000
    """
    # Fallback B: Phase-6 schema — includes diagnostic columns but no friction
    _SQL_P6 = """
        SELECT symbol, exit_reason_type, exit_reason_category, tag,
               realized_usd, pnl_pct,
               entry_confidence, entry_kelly_f, hold_time_secs
        FROM trades
        WHERE is_close_leg = 1
        ORDER BY ts DESC LIMIT 2000
    """
    # Fallback C: Phase-4 schema — filter directly in SQL, no diagnostic cols
    _SQL_P4 = """
        SELECT symbol, exit_reason_type, exit_reason_category, tag,
               realized_usd, pnl_pct
        FROM trades
        WHERE is_close_leg = 1
        ORDER BY ts DESC LIMIT 2000
    """
    # Fallback D: columns present but no is_close_leg (partial migration)
    _SQL_FALLBACK_A = """
        SELECT symbol, exit_reason_type, exit_reason_category, tag,
               realized_usd, pnl_pct
        FROM trades
        WHERE pnl_pct IS NOT NULL AND pnl_pct != 0
        ORDER BY ts DESC LIMIT 2000
    """
    # Fallback E: minimal schema — just pnl_pct != 0 heuristic
    _SQL_FALLBACK_B = """
        SELECT symbol, tag, realized_usd, pnl_pct
        FROM trades
        WHERE pnl_pct IS NOT NULL AND pnl_pct != 0
        ORDER BY ts DESC LIMIT 2000
    """

    df = pd.DataFrame()
    for sql in (_SQL_DT, _SQL_P10, _SQL_P6, _SQL_P4, _SQL_FALLBACK_A, _SQL_FALLBACK_B):
        try:
            df = _query(sql.strip())
            if not df.empty:
                break
        except Exception as _e:
            log.debug("_load_close_diagnostics: attempt failed: %s", _e)
            df = pd.DataFrame()

    if df.empty:
        return df

    for col in ("realized_usd", "pnl_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Ensure grouping columns exist even if the query was a lower fallback
    for col in ("exit_reason_type", "exit_reason_category"):
        if col not in df.columns:
            df[col] = None

    # [PHASE6] Ensure diagnostic columns exist (None when pre-Phase-6 schema).
    for col in ("entry_confidence", "entry_kelly_f", "hold_time_secs"):
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # [PHASE10] Ensure friction columns exist.  NULL becomes NaN after to_numeric.
    # Sources of NULL: pre-Phase-10 rows; pre-Phase-15 HEDGE_TRIM rows; any row
    # where tick data was unavailable at close time.  Phase 15 added tick-based
    # friction for HEDGE_TRIM and liquidation exits, so post-Phase-15 rows are
    # non-NULL when tick data was available.
    for col in ("close_slippage_bps", "spread_at_close_bps"):
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # [POST-ROADMAP-DT] Ensure entry-context columns exist.
    # entry_regime / entry_direction: string columns; kept as-is (no numeric coercion).
    # entry_z_score: float column; coerced to numeric (NULL → NaN).
    # All three are None/NaN on historical rows (pre-DT schema) — callers guard
    # with notna() before grouping.
    for col in ("entry_regime", "entry_direction"):
        if col not in df.columns:
            df[col] = None
    if "entry_z_score" not in df.columns:
        df["entry_z_score"] = None
    else:
        df["entry_z_score"] = pd.to_numeric(df["entry_z_score"], errors="coerce")

    return df


def _force_bust_trades_cache() -> None:
    """Force-invalidate cache, LKG slots, rendered cache, AND SQLite connection snapshot."""
    try:
        _load_trades_raw.clear()
    except Exception:
        pass
    # Close the thread-local SQLite connection so the next query gets a
    # fresh connection with a new read snapshot — fixes WAL stale-read problem
    try:
        conn = getattr(_db_local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            _db_local.conn = None
    except Exception:
        pass
    for _k in ("_lkg_trades", "_lkg_trades_ts", _SS_HAS_TRADES,
               "_b3_ta_kpi_vals", "_b3_ta_table_html", "_b3_ta_latest_ts"):
        st.session_state.pop(_k, None)


def load_trades() -> pd.DataFrame:
    return _lkg_load("trades", _load_trades_raw)


@st.cache_data(ttl=CACHE_FAST)
def _get_trades_count_raw() -> int:
    """[FIX-CRIT-01] Uses _get_known_tables() which is now properly defined."""
    tables = _get_known_tables()
    if "trades" not in tables:
        return 0
    df = _query("SELECT COUNT(*) AS cnt FROM trades LIMIT 1")
    if df.empty:
        return -1
    try:
        return int(df["cnt"].iloc[0])
    except Exception:
        return -1


def _trades_known_nonempty() -> bool:
    if st.session_state.get(_SS_HAS_TRADES):
        return True
    cnt = _get_trades_count_raw()
    if cnt > 0:
        st.session_state[_SS_HAS_TRADES] = True
        return True
    return False


@st.cache_data(ttl=CACHE_SLOW)
def load_candles(symbol: str, tf: str = "1hour", limit: int = 150) -> pd.DataFrame:
    # [FIX-CANDLE-WAL] Flush WAL frames to main DB file before reading so CI-2
    # REST writes (committed in the bot process) are visible to this reader.
    # Mirrors the same checkpoint pattern used in _load_trades_raw.
    _conn = _get_db_conn()
    if _conn is not None:
        try:
            _conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
        except Exception:
            pass
    inst = f"{_norm_sym(symbol)}-USDT"
    df   = _query(
        "SELECT ts, open, high, low, close, volume FROM candles "
        "WHERE symbol=? AND tf=? ORDER BY ts DESC LIMIT ?",
        (inst, tf, min(limit, 300)),
    )
    if df.empty:
        return df
    df["dt"] = pd.to_datetime(df["ts"], unit="s")
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("dt").reset_index(drop=True)


@st.cache_data(ttl=CACHE_CORR)
def load_correlation_matrix(symbols: tuple) -> Optional[pd.DataFrame]:
    """[OPS-08 / PERF-03] Single batched SQL query replaces N sequential queries."""
    if len(symbols) < 2:
        return None

    norm_syms    = [f"{_norm_sym(s)}-USDT" for s in symbols]
    placeholders = ",".join("?" * len(norm_syms))

    df = _query(
        f"""
        SELECT symbol, ts, close
        FROM   candles
        WHERE  symbol IN ({placeholders})
          AND  tf = '1hour'
          AND  ts >= (
              SELECT MAX(ts) - 604800
              FROM   candles
              WHERE  symbol IN ({placeholders})
                AND  tf = '1hour'
          )
        ORDER  BY ts ASC
        """,
        tuple(norm_syms) * 2,
    )

    if df.empty:
        return None

    df["close"]     = pd.to_numeric(df["close"], errors="coerce")
    df["sym_label"] = df["symbol"].apply(_norm_sym)

    pivot = (
        df.pivot_table(index="ts", columns="sym_label", values="close", aggfunc="last")
        .sort_index()
    )

    pivot = pivot.loc[:, pivot.notna().sum() >= 10]

    if pivot.shape[1] < 2:
        return None

    returns = pivot.pct_change(fill_method=None).dropna(how="all")
    if returns.shape[0] < 5:
        return None

    return returns.corr().round(3)


def _status_age_secs() -> float:
    try:
        return time.time() - TRADER_STATUS.stat().st_mtime
    except FileNotFoundError:
        # Expected when bot has not started yet — not an error
        return 9999.0
    except Exception as _e:
        log.debug("_status_age_secs: stat failed: %s", _e)
        return 9999.0


def load_tactical_config() -> dict:
    """[FIX-OPS-02] Re-reads disk every TACT_RELOAD_SECS — not session-sticky forever."""
    last_load    = st.session_state.get(_SS_TACT_LOADED_AT, 0.0)
    needs_reload = (time.time() - last_load) > TACT_RELOAD_SECS
    if not needs_reload:
        return {k: st.session_state.get(k, v) for k, v in TACT_DEFAULTS.items()}
    try:
        cfg = json.loads(TACTICAL_CONFIG_PATH.read_bytes()) if TACTICAL_CONFIG_PATH.exists() else {}
    except Exception:
        log.error("load_tactical_config: parse error", exc_info=True)
        cfg = {}
    for k, v in TACT_DEFAULTS.items():
        st.session_state[k] = cfg.get(k, v)
    st.session_state[_SS_TACT_LOADED_AT] = time.time()
    return {k: st.session_state.get(k, v) for k, v in TACT_DEFAULTS.items()}


def any_override_active() -> bool:
    return any(st.session_state.get(k, False) for k in TACT_DEFAULTS)


def load_veto_logs() -> list[dict]:
    LKG = "_lkg_veto_logs"
    if not VETO_AUDIT_PATH.exists():
        return st.session_state.get(LKG, [])
    for attempt in range(3):
        try:
            raw = VETO_AUDIT_PATH.read_bytes()
            if not raw.strip():
                return st.session_state.get(LKG, [])
            parsed = json.loads(raw.decode("utf-8"))
            result = parsed if isinstance(parsed, list) else parsed.get("vetoes", [])
            if result:
                st.session_state[LKG] = result
            return result
        except json.JSONDecodeError as e:
            log.warning("load_veto_logs: JSONDecodeError attempt=%d: %s", attempt, e)
            if attempt < 2:
                time.sleep(0.04)
        except Exception:
            log.error("load_veto_logs: unexpected error attempt=%d", attempt, exc_info=True)
            break
    return st.session_state.get(LKG, [])


def load_decision_trace() -> list[dict]:
    """[TRACK09-DT] Read decision_trace.json with LKG pattern.

    Returns a list of decision-trace records (most recent first).
    Degrades to [] on missing file, empty file, or JSON error.
    Uses last-known-good (LKG) caching so stale data is returned
    rather than an empty list on transient read failure.
    """
    LKG = "_lkg_decision_trace"
    if not DECISION_TRACE_PATH.exists():
        return st.session_state.get(LKG, [])
    for attempt in range(3):
        try:
            raw = DECISION_TRACE_PATH.read_bytes()
            if not raw.strip():
                return st.session_state.get(LKG, [])
            parsed = json.loads(raw.decode("utf-8"))
            result = parsed if isinstance(parsed, list) else []
            if result:
                st.session_state[LKG] = result
            return result
        except json.JSONDecodeError as e:
            log.warning("load_decision_trace: JSONDecodeError attempt=%d: %s", attempt, e)
            if attempt < 2:
                time.sleep(0.04)
        except Exception:
            log.error("load_decision_trace: unexpected error attempt=%d", attempt, exc_info=True)
            break
    return st.session_state.get(LKG, [])


@st.cache_data(ttl=CACHE_FAST)
def _load_decision_trace_history(
    days_back: int = 7,
    symbol_filter: str = "ALL",
    outcome_filter: str = "ALL",
    source_filter: str = "ALL",
) -> "pd.DataFrame":
    """[TRACK10-DT1] Read decision_trace table for the historical panel.

    Returns a DataFrame of decision-trace records matching the given filters.
    Degrades gracefully to an empty DataFrame when:
      - the decision_trace table does not yet exist (pre-Track-10 DB)
      - the table exists but the filtered query returns zero rows
      - any query error occurs

    Parameters
    ----------
    days_back : int
        Number of calendar days to include (relative to now).
    symbol_filter : str
        "ALL" to include every symbol, or a specific symbol string.
    outcome_filter : str
        "ALL" | "ADMITTED" | "BLOCKED" | "SHADOW" | "CLOSED" | "DCA"
    source_filter : str
        [TRACK16-DT] "ALL" | "ENTRY" | "EXPRESS".
        [TRACK17-DT] Also accepts "CLOSE" | "DCA".  Applied as a Python-side
        filter on source_path after the query so it degrades cleanly on
        pre-Track-16/17 DBs where the column is absent.

    [TRACK17-DT3] Three-query fallback chain:
      (1) with source_path + pnl_pct + hold_time_secs  (Track-17 DB)
      (2) with source_path only, without pnl/hold       (Track-16 DB)
      (3) without source_path, pnl, or hold             (Track-10 DB)
    Pre-Track-17 DBs: pnl_pct / hold_time_secs absent from DataFrame (→ "—"
    in table).  Pre-Track-16 DBs: source_path also absent.

    No DB writes.  No schema side effects.  No joins with other tables.
    """
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return pd.DataFrame()
    try:
        cutoff_ts = time.time() - days_back * 86400.0
        params: list = [cutoff_ts]
        where_parts = ["ts >= ?"]
        if symbol_filter and symbol_filter != "ALL":
            where_parts.append("symbol = ?")
            params.append(symbol_filter)
        if outcome_filter and outcome_filter != "ALL":
            where_parts.append("outcome = ?")
            params.append(outcome_filter)
        where_clause = " AND ".join(where_parts)

        _base_cols = (
            "SELECT ts, symbol, direction, regime, confidence, z_score,"
            " outcome, gate_name, reason, llm_verdict, p32_p_success,"
            " sizing_usd, risk_off, p39_snipe, conviction_mult"
        )
        _tail = f" FROM decision_trace WHERE {where_clause} ORDER BY ts DESC LIMIT 500"

        # [TRACK17-DT3] Three-query fallback chain — most capable to least.
        _sql_full = (
            _base_cols
            + ", source_path, pnl_pct, hold_time_secs"
            + _tail
        )
        _sql_src_only = (
            _base_cols
            + ", source_path"
            + _tail
        )
        _sql_base = _base_cols + _tail

        df = pd.DataFrame()
        for _sql in (_sql_full, _sql_src_only, _sql_base):
            try:
                _df = _query(_sql, tuple(params))
                if _df is not None:
                    df = _df
                    break
            except Exception as _fb_exc:
                log.debug("[TRACK17-DT3] _load_decision_trace_history fallback: %s", _fb_exc)

        if df is None:
            return pd.DataFrame()

        # [TRACK16-DT / TRACK17-DT] Apply source_filter as Python-side filter.
        # When source_path column is absent (pre-Track-16 DB), this is a no-op.
        if source_filter and source_filter != "ALL" and "source_path" in df.columns:
            df = df[df["source_path"].fillna("entry").str.lower() == source_filter.lower()]

        return df if not df.empty else pd.DataFrame()
    except Exception:
        log.error("[TRACK10-DT1] _load_decision_trace_history: query failed", exc_info=True)
        return pd.DataFrame()


def load_supervisor_overrides() -> dict:
    """[P44-DASH] Read supervisor_overrides.json with LKG pattern.

    Returns the parsed payload dict, or {} on error.
    Fields from sentinel.py PerformanceSupervisor._write_overrides():
      win_rate, n_trades, regime_class, supervisor_paused, pause_until_ts,
      warn_floor, pause_floor, resume_rate, recommendations.
    Fields from apply_overrides() audit trail (override_history list):
      each entry has: ts, old_kelly, new_kelly, win_rate, regime_class.
    """
    _LKG = "_lkg_sup_overrides"
    if not SUPERVISOR_OVERRIDES_PATH.exists():
        return st.session_state.get(_LKG, {})
    for attempt in range(3):
        try:
            raw = SUPERVISOR_OVERRIDES_PATH.read_bytes()
            if not raw.strip():
                return st.session_state.get(_LKG, {})
            parsed = json.loads(raw.decode("utf-8"))
            if isinstance(parsed, dict):
                st.session_state[_LKG] = parsed
                return parsed
        except json.JSONDecodeError as e:
            log.warning("load_supervisor_overrides: JSONDecodeError attempt=%d: %s", attempt, e)
            if attempt < 2:
                time.sleep(0.04)
        except Exception:
            log.error("load_supervisor_overrides: unexpected error", exc_info=True)
            break
    return st.session_state.get(_LKG, {})


def _detect_tab_visibility() -> str:
    import streamlit.components.v1 as _c
    _js = """<script>(function(){
        function _cur(){try{var u=new URL(window.parent.location.href);
          return u.searchParams.get("tab_state")||"active";}catch(e){return"active";}}
        function _set(s){try{var u=new URL(window.parent.location.href);
          if(u.searchParams.get("tab_state")===s)return;
          u.searchParams.set("tab_state",s);
          window.parent.history.replaceState(null,"",u.toString());}catch(e){}}
        function _res(){var s=document.visibilityState,p="active";
          try{p=window.parent.document.visibilityState||"active";}catch(e){}
          return(s==="hidden"||p==="hidden")?"hidden":"active";}
        _set(_res());
        document.addEventListener("visibilitychange",function(){_set(_res());});
        try{window.parent.document.addEventListener("visibilitychange",
          function(){_set(_res());});}catch(e){}
    })();</script>"""
    _c.html(_js, height=0, scrolling=False)
    raw = st.query_params.get("tab_state", "active")
    return "active" if raw not in ("active", "hidden") else raw


def trigger_gateway_sync() -> None:
    _write_control_event("reset_gateway", force_truth=True, source="tab_focus_regain")


def _handle_tab_transition(state: str) -> None:
    prev = st.session_state.get(_SS_TAB_PREV)
    if prev == "hidden" and state == "active":
        last = float(st.session_state.get(_SS_SYNC_AT, 0.0))
        if time.time() - last > _TAB_COOLDOWN:
            trigger_gateway_sync()
            st.session_state[_SS_SYNC_AT] = time.time()
    st.session_state[_SS_TAB_PREV] = state


def _install_autorefresh(tab: str = "active") -> None:
    ms = 2_000 if tab == "active" else 300_000
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=ms, key="ot_autorefresh")
    except ImportError:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _section(label: str, icon: str = "") -> None:
    st.markdown(
        f'<div class="ot-section">'
        f'<span style="color:{T["green"]};font-size:0.75rem;">{icon}</span>'
        f'<span>{_esc(label)}</span></div>',
        unsafe_allow_html=True,
    )


def _kpi(label: str, value: str, sub: str = "", colour: str = "") -> None:
    colour = colour or T["green"]
    st.markdown(
        f'<div class="ot-tile">'
        f'<div class="ot-kpi-label">{_esc(label)}</div>'
        f'<div class="ot-kpi-value" style="color:{colour};">{_esc(value)}</div>'
        + (f'<div class="ot-kpi-sub">{_esc(sub)}</div>' if sub else "")
        + '</div>',
        unsafe_allow_html=True,
    )


def _badge(text: str, colour: str, bg: str = "") -> str:
    bg = bg or colour + "22"
    return (
        f'<span class="ot-badge" style="color:{colour};background:{bg};">'
        f'{_esc(text)}</span>'
    )


def _verdict_badge(verdict: str) -> str:
    vs   = VERDICT_STYLE.get(verdict, VERDICT_STYLE.get("N/A", {}))
    icon = vs.get("icon", "⚪")
    col  = vs.get("col") or vs.get("color") or T["muted"]
    bg   = vs.get("bg") or vs.get("glow") or "rgba(255,255,255,0.08)"
    return _badge(f"{icon} {verdict}", col, bg)


def _fig_base(fig: go.Figure, height: int = 280, subplot: bool = False) -> None:
    """[FIX-QA-08] subplot=True skips global yaxis.side override."""
    layout = dict(
        height=height,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=8, b=0),
        font=dict(color=T["text"], size=9, family=_FONT_MONO_PLOTLY),
        xaxis=dict(gridcolor=T["border"], zeroline=False,
                   tickfont=dict(size=8), showgrid=True),
        legend=dict(font=dict(size=8), bgcolor="rgba(0,0,0,0)",
                    orientation="h", y=1.04, x=0),
    )
    if not subplot:
        layout["yaxis"] = dict(gridcolor=T["border"], zeroline=False,
                               tickfont=dict(size=8), showgrid=True, side="right")
    fig.update_layout(**layout)


def _snr_badge(snr: Optional[float]) -> str:
    if snr is None:
        return '<span class="snr-none">N/A</span>'
    if snr >= 1.5:
        cls, lbl = "snr-accurate", f"SNR {snr:.2f} ✓"
    elif snr >= 0.8:
        cls, lbl = "snr-moderate", f"SNR {snr:.2f} ▲"
    else:
        cls, lbl = "snr-noisy", f"SNR {snr:.2f} ⚠"
    return f'<span class="{cls}" style="font-family:{T["mono"]};font-size:0.72rem;">{lbl}</span>'


def _calc_snr(predicted: list, actual: list) -> Optional[float]:
    """[FIX-CRIT-08] Explicit None on short series."""
    n = min(len(predicted), len(actual), 10)
    if n < 3:
        return None
    try:
        p            = np.array([float(x) for x in predicted[:n]])
        a            = np.array([float(x) for x in actual[:n]])
        signal_power = float(np.mean(np.abs(p)))
        noise        = float(np.mean(np.abs(p - a))) + 1e-9
        return round(signal_power / noise, 4)
    except Exception:
        log.debug("_calc_snr: computation failed", exc_info=True)
        return None


def _trust_class(v: float) -> str:
    if v >= 0.65: return "trust-high"
    if v >= 0.45: return "trust-mid"
    return "trust-low"


def _slot_placeholder(label: str, height: int = 200) -> None:
    """[FIX-QA-06] Shared fixed-height placeholder — prevents layout shift."""
    st.markdown(
        f'<div class="ot-tile" style="min-height:{height}px;opacity:0.55;'
        f'display:flex;align-items:center;justify-content:center;">'
        f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
        f'{_esc(label)}</span></div>',
        unsafe_allow_html=True,
    )


def _paint_fig(
    slot,
    fig_fresh: Optional[go.Figure],
    lkg_key: str,
    height_px: int,
    await_label: str,
    chart_key: str,
) -> None:
    """Unified figure paint: fresh → LKG → skeleton. Exactly one paint per call."""
    if fig_fresh is not None:
        st.session_state[lkg_key] = fig_fresh
    fig = fig_fresh if fig_fresh is not None else st.session_state.get(lkg_key)
    if fig is not None:
        slot.plotly_chart(fig, width="stretch",
                          config={"displayModeBar": False}, key=chart_key)
    else:
        slot.markdown(
            f'<div class="ot-tile" style="height:{height_px}px;display:flex;'
            f'align-items:center;justify-content:center;">'
            f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.72rem;">'
            f'{_esc(await_label)}</span></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TACTICAL SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def render_tactical_sidebar() -> None:
    """[INST-03] All writes blocked when bot heartbeat is stale.
    [INST-05] All writes blocked and visually flagged when demo_mode=True.
    [INST-04] Every toggle/button action emits an operator audit record.
    """
    load_tactical_config()
    bot_live  = _bot_is_live()
    demo_mode = _is_demo_mode()
    writes_ok = bot_live and not demo_mode

    with st.sidebar:
        st.markdown(
            f'<div style="font-family:{T["mono"]};font-size:0.72rem;'
            f'color:{T["green"]};letter-spacing:0.10em;'
            f'padding:8px 0 4px 0;border-bottom:1px solid {T["border"]};">'
            f'⚙ TACTICAL OVERRIDES</div>',
            unsafe_allow_html=True,
        )

        if not bot_live:
            st.markdown(
                f'<div class="ot-alert-red" style="font-size:0.68rem;">'
                f'🔌 BOT OFFLINE — writes disabled</div>',
                unsafe_allow_html=True,
            )
        if demo_mode:
            st.markdown(
                f'<div class="ot-alert-yellow" style="font-size:0.68rem;">'
                f'🧪 DEMO MODE — writes disabled</div>',
                unsafe_allow_html=True,
            )

        def _toggle(key: str, label: str, warn: str = "") -> bool:
            val = st.toggle(
                label,
                value=st.session_state.get(key, False),
                key=f"tact_{key}",
                disabled=not writes_ok,
            )
            if val != st.session_state.get(key, False):
                if not writes_ok:
                    if not bot_live:
                        _bot_dead_warning(label)
                    return st.session_state.get(key, False)
                st.session_state[key] = val
                cfg = {k: st.session_state.get(k, False) for k in TACT_DEFAULTS}
                save_tactical_config(cfg)
                if warn:
                    st.warning(warn)
            return val

        ro = _toggle(TACT_RISK_OFF, "🛡 Risk-Off Mode",  "Risk-off: no new entries.")
        sn = _toggle(TACT_SNIPER,   "🎯 Sniper Only",    "Sniper: high-conviction only.")
        hd = _toggle(TACT_HEDGE,    "↔ Hedge Mode",      "Hedge: delta-neutral only.")

        if any((ro, sn, hd)):
            st.markdown(
                f'<div class="ot-alert-yellow" style="margin-top:8px;">'
                f'⚡ OVERRIDE ACTIVE</div>', unsafe_allow_html=True,
            )

        st.markdown("---")

        _sync_disabled = not writes_ok
        if st.button(
            "⟳ Force Gateway Sync",
            key="force_sync_btn",
            width="stretch",
            disabled=_sync_disabled,
        ):
            if not bot_live:
                _bot_dead_warning("Force Gateway Sync")
            elif demo_mode:
                st.sidebar.info("Demo mode: sync suppressed.")
            else:
                if _write_control_event("reset_gateway", force_truth=True, source="manual"):
                    st.success("✓ Sync event written")
                else:
                    st.error("✗ Write failed")

        st.markdown(
            f'<div style="font-family:{T["mono"]};font-size:0.56rem;'
            f'color:{T["muted"]};margin-top:6px;">'
            f'v40.5 · OBSIDIAN TERMINAL</div>',
            unsafe_allow_html=True,
        )


def render_systemic_defense_sidebar(status: dict) -> None:
    """[FIX-PERF-02] Sidebar chart cached in session_state — not rebuilt every rerun."""
    intel = status.get("intelligence", {})
    if not isinstance(intel, dict): intel = {}
    mkt   = status.get("market_data",  {})
    if not isinstance(mkt,   dict): mkt   = {}
    wt    = mkt.get("whale_tape", {})
    if not isinstance(wt,    dict): wt    = {}

    ent_raw  = _safe_float(intel.get("entropy"))
    ent_norm = _safe_float(intel.get("entropy_normalized"))
    conv_m   = _safe_float(intel.get("conviction_multiplier"), 1.0)
    pk_mult  = _safe_float(wt.get("peak_multiplier"))
    dom_side = str(wt.get("dominant_side", "NEUTRAL"))
    pk_sym   = str(wt.get("peak_symbol",   "—"))
    ent_hist = intel.get("entropy_history", [])

    e_col = T["green"] if ent_raw < 1.5 else T["yellow"] if ent_raw < 2.5 else T["red"]
    s_col = T["green"] if dom_side == "BUY" else T["red"] if dom_side == "SELL" else T["muted"]

    st.sidebar.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.72rem;color:{T["green"]};'
        f'letter-spacing:0.10em;padding:8px 0 4px 0;border-bottom:1px solid {T["border"]};">'
        f'🛡 SYSTEMIC DEFENSE</div>', unsafe_allow_html=True,
    )
    rows = "".join(
        f'<div style="display:flex;justify-content:space-between;padding:2px 0;">'
        f'<span style="color:{T["muted"]};letter-spacing:0.08em;">{_esc(k)}</span>'
        f'<span style="color:{vc};font-weight:700;">{_esc(v)}</span></div>'
        for k, v, vc in [
            ("ENTROPY RAW",  f"{ent_raw:.4f}b",         e_col),
            ("ENTROPY NORM", f"{ent_norm:.4f}",         e_col),
            ("CONV MULT",    f"{conv_m:.2f}×",          T["text"]),
            ("PEAK MULT",    f"{pk_mult:.2f}×",         T["orange"]),
            ("WHALE SIDE",   f"{dom_side} ({pk_sym})",  s_col),
        ]
    )
    st.sidebar.markdown(
        f'<div style="background:{T["surface"]};border:1px solid {T["border"]};'
        f'border-radius:4px;padding:10px 12px;font-family:{T["mono"]};font-size:0.70rem;">'
        f'{rows}</div>', unsafe_allow_html=True,
    )

    _SB_CHART_LEN = "_sb_ent_hist_len"
    hist_len = len(ent_hist) if isinstance(ent_hist, list) else 0
    if hist_len > 1 and hist_len != st.session_state.get(_SB_CHART_LEN):
        max_bits = 3.32
        normed   = [round(min(_safe_float(v) / max_bits, 1.0), 4)
                    for v in ent_hist if v is not None]
        if normed:
            st.sidebar.line_chart(pd.DataFrame({"Entropy (norm)": normed}), height=70)
            st.session_state[_SB_CHART_LEN] = hist_len

    if st.sidebar.button("⚡ FORCE GATEWAY RESET", key="ot_gw_reset_btn", width="stretch"):
        if _write_control_event("reset_gateway", force_truth=True, source="sidebar"):
            st.sidebar.success("✓ Gateway reset queued")
        else:
            st.sidebar.error("✗ Write failed")
    st.sidebar.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: HUD BAR
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_hud_bar() -> None:
    """[FIX-ARCH-01] Reads _get_status() — no stale parameter capture."""
    status = _get_status()

    p17       = status.get("p17_intelligence", {})
    recents   = p17.get("recent_results", [])
    latencies = [r.get("latency_ms") for r in recents if r.get("latency_ms") is not None]
    avg_lat   = sum(latencies) / len(latencies) if latencies else None
    lat_cls   = "ot-ok" if (avg_lat and avg_lat < 60) else "ot-warn" if (avg_lat and avg_lat < 90) else "ot-err"
    lat_str   = f"{avg_lat:.1f}ms" if avg_lat else "PENDING"

    p22    = status.get("p22", {})
    or_model = _esc(str(p22.get("openrouter_model", "—"))[:28])
    or_ok    = p22.get("openrouter_active", False)
    or_cls   = "ot-ok" if or_ok else "ot-warn"

    p16_act  = status.get("p16_bridge_active", False)
    p16_cls  = "ot-ok" if p16_act else "ot-err"
    p16_str  = "BRIDGE:ACTIVE" if p16_act else "BRIDGE:OFFLINE"

    # [P40.5-DASH] IPC RTT pill — reads from session_state written by render_p40_5_heartbeat()
    _p405_rtt      = _safe_float(st.session_state.get("_p405_last_rtt_ms", None))
    _p405_cong     = bool(st.session_state.get("_p405_is_congested", False))
    _p405_timeouts = _safe_int(st.session_state.get("_p405_timeouts", 0))
    if _p405_timeouts >= 3:
        _p405_cls = "ot-err"
        _p405_str = f"IPC:TIMEOUT×{_p405_timeouts}"
    elif _p405_cong:
        _p405_cls = "ot-warn"
        _p405_str = f"RTT:{_p405_rtt:.1f}ms⚠"
    elif _p405_rtt and _p405_rtt > 0:
        _p405_cls = "ot-ok"
        _p405_str = f"RTT:{_p405_rtt:.1f}ms"
    else:
        _p405_cls = "ot-warn"
        _p405_str = "RTT:—"

    cb     = status.get("circuit_breaker", False)
    ep     = status.get("emergency_pause", False)
    sys_cls= "ot-err" if cb or ep else "ot-ok"
    sys_str= "CB:TRIPPED" if cb else "PAUSED" if ep else "SYS:OK"

    p20     = status.get("p20_global_risk", {})
    zombie  = bool(p20.get("zombie_active") or status.get("p20_zombie_mode_active"))
    dd_pct  = _safe_float(p20.get("drawdown_pct"))
    p20_cls = "ot-err" if zombie else "ot-warn" if dd_pct > 5 else "ot-ok"
    p20_str = "☠ ZOMBIE" if zombie else f"DD:{dd_pct:.1f}%"

    pl       = p22.get("panic_lock", {})
    pl_locked= bool(pl.get("locked", False)) if isinstance(pl, dict) else False
    pl_cls   = "ot-err" if pl_locked else "ot-ok"
    rem_secs = _safe_float(pl.get("remaining_secs")) if isinstance(pl, dict) else 0.0
    pl_str   = f"🔒PANIC:{rem_secs:.0f}s" if pl_locked else "PANIC:CLEAR"

    p32      = _esc(str(status.get("p32_aggression_mode", "—")))
    demo     = status.get("demo_mode", True)
    strategy = _esc(str(status.get("strategy_mode", "—"))[:16])

    # [P44-DASH] DCB / Supervisor freeze state in HUD
    p44_frozen   = bool(status.get("p44_dcb_frozen", False))
    p44_rem      = _safe_float(status.get("p44_dcb_freeze_remaining", 0.0))
    p44_cls      = "ot-err" if p44_frozen else "ot-ok"
    p44_str      = f"🧊DCB:{p44_rem:.0f}s" if p44_frozen else "SUP:OK"

    frame_ts  = st.session_state.get(_SS_FRAME_TS, 0.0)
    frame_age = time.time() - frame_ts if frame_ts else 9999.0
    file_age  = _status_age_secs()
    age       = min(frame_age, file_age)
    age_cls   = "ot-ok" if age < 5 else "ot-warn" if age < 15 else "ot-err"
    age_str   = f"⬤ {age:.1f}s" if age < 15 else f"⚠ STALE {age:.0f}s"
    ts_str    = datetime.now().strftime("%H:%M:%S.%f")[:-3]

    demo_badge = (
        f'<span class="ot-badge" style="color:{T["yellow"]};background:rgba(255,214,0,0.12);">DEMO</span>'
        if demo else
        f'<span class="ot-badge" style="color:{T["green"]};background:rgba(0,255,65,0.08);">LIVE</span>'
    )
    override_badge = (
        f'<span class="ot-badge" style="color:{T["purple"]};background:rgba(191,95,255,0.12);">OVERRIDE</span>'
        if any_override_active() else ""
    )

    st.markdown(
        f'<div class="ot-terminal-bar">'
        f'<span><span class="{age_cls}">{age_str}</span></span>'
        f'<span style="color:{T["muted"]};">{ts_str}</span>'
        f'{demo_badge}{override_badge}'
        f'<span>MODE: <span style="color:{T["cyan"]};">{strategy}</span></span>'
        f'<span>LLM: <span class="{lat_cls}">{lat_str}</span></span>'
        f'<span>OR: <span class="{or_cls}">{or_model}</span></span>'
        f'<span class="{p16_cls}">{p16_str}</span>'
        f'<span class="{_p405_cls}">{_esc(_p405_str)}</span>'
        f'<span class="{sys_cls}">{sys_str}</span>'
        f'<span class="{p20_cls}">{p20_str}</span>'
        f'<span class="{pl_cls}">{pl_str}</span>'
        f'<span class="{p44_cls}">{p44_str}</span>'
        f'<span style="color:{T["blue"]};">AGG:{p32}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: HEADER KPIs
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_header_kpis() -> None:
    """[FIX-ARCH-05] No status parameter — reads _get_status() internally."""
    status   = _get_status()
    equity   = _get_binary_truth_equity(status)
    account  = status.get("account", {})
    avail    = _safe_float(account.get("avail", account.get("buying_power", 0)))
    leverage = max(_safe_float(account.get("leverage", 1.0), 1.0), 1.0)
    deployed = _safe_float(account.get("pct_deployed"))
    deployed_usd = _safe_float(account.get("deployed_capital"))
    ghost    = bool(account.get("equity_is_ghost", False))
    ghost_ct = _safe_int(account.get("consecutive_ghost_reads"))

    p20     = status.get("p20_global_risk", {})
    peak_eq = _safe_float(p20.get("peak_equity"))
    dd_pct  = _safe_float(p20.get("drawdown_pct"))
    zombie  = bool(p20.get("zombie_active") or status.get("p20_zombie_mode_active"))

    positions = status.get("positions", {})
    if not isinstance(positions, dict): positions = {}
    open_pos  = sum(1 for p in positions.values() if p.get("direction", "none") != "none")

    p17      = status.get("p17_intelligence", {})
    counters = p17.get("counters", {})
    recents  = p17.get("recent_results", [])
    last_score = recents[0].get("score") if recents else None

    p23  = status.get("p23_ghost_meter", {})
    lve  = _safe_float((p23 or {}).get("last_valid_equity"))

    eq_col  = T["red"] if ghost else T["green"]
    dd_col  = T["red"] if dd_pct > 10 else T["yellow"] if dd_pct > 5 else T["green"]
    dep_col = T["yellow"] if deployed > 70 else T["green"]

    if ghost:
        st.markdown(
            f'<div class="ot-alert-red" style="margin-bottom:4px;">'
            f'☠ GHOST EQUITY ({ghost_ct} reads) · LVE: ${lve:,.2f}</div>',
            unsafe_allow_html=True,
        )
    if zombie:
        st.markdown(
            f'<div class="ot-alert-red" style="margin-bottom:4px;">'
            f'☠ ZOMBIE MODE ACTIVE — trading halted</div>',
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    with c1:
        _kpi("EQUITY (BT)", f"${equity:,.2f}", f"LVE ${lve:,.2f}", eq_col)
    with c2:
        _kpi("AVAILABLE", f"${avail:,.2f}", f"{100-deployed:.1f}% free", T["blue"])
    with c3:
        _kpi("DEPLOYED", f"{deployed:.1f}%", f"${deployed_usd:,.2f}", dep_col)
    with c4:
        _kpi("PEAK EQUITY", f"${peak_eq:,.2f}", f"DD: {dd_pct:.2f}%", dd_col)
    with c5:
        _kpi("OPEN POS", str(open_pos), f"{leverage:.1f}× lev",
             T["green"] if open_pos > 0 else T["text2"])
    with c6:
        _kpi("AI VETOES", str(counters.get("veto", 0)),
             f"Boosts: {counters.get('boost', 0)}",
             T["red"] if counters.get("veto", 0) > 0 else T["text2"])
    with c7:
        if last_score is not None:
            sc        = _safe_float(last_score)
            score_col = T["green"] if sc > 0.7 else T["yellow"] if sc > 0.4 else T["red"]
            _kpi("LAST AI SCORE", f"{sc:.4f}", "", score_col)
        else:
            _kpi("LAST AI SCORE", "—", "", T["muted"])


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: EQUITY CURVE
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_equity_curve() -> None:
    """[FIX-CRIT-05] Read-only display fragment — no equity latch writes.
    [FIX-FLICKER-EQ] Only rebuilds Plotly figure when underlying data changes.
    """
    status = _get_status()
    _section("REAL-TIME EQUITY CURVE", "📈")

    _KPI_KEY   = "_lkg_equity_kpis"
    _EQ_TS_KEY = "_eq_last_data_ts"

    df = pd.DataFrame()
    try:
        df = load_equity()
    except Exception:
        log.error("render_equity_curve: load_equity failed", exc_info=True)

    # ── Detect whether the underlying series actually changed ─────────────────
    _eq_data_changed = True
    if not df.empty and "ts" in df.columns:
        try:
            _cur_ts = (int(df["ts"].iloc[-1]), len(df))
        except Exception:
            _cur_ts = (0, len(df))
        _eq_data_changed = st.session_state.get(_EQ_TS_KEY) != _cur_ts
        if _eq_data_changed:
            st.session_state[_EQ_TS_KEY] = _cur_ts

    # ── Build figure only when data changed ───────────────────────────────────
    fig_fresh = None
    if not df.empty and _eq_data_changed:
        lkg_eq  = _sg(_SS_EQUITY) or _sg("last_valid_equity")
        running = _safe_float(lkg_eq) or None
        eq_vals: list = []
        av_vals: list = []

        for _, row in df.iterrows():
            ev = _safe_float(row.get("equity"))
            if ev > 0.0:
                running = ev
                eq_vals.append(ev)
            else:
                eq_vals.append(running if running else ev)
            av = _safe_float(row.get("avail"))
            av_vals.append(av if av > 0 else None)

        df = df.copy()
        df["equity_clamped"] = eq_vals
        df["avail_clamped"]  = av_vals

        eq0    = _safe_float(df["equity_clamped"].iloc[0]) if len(df) else 0.0
        eq_now = _safe_float(df["equity_clamped"].iloc[-1]) if len(df) else 0.0
        peak   = _safe_float(df["equity_clamped"].max()) if len(df) else 0.0
        dd_pct = (peak - eq_now) / peak * 100 if peak > 0 else 0.0
        ret    = (eq_now - eq0) / eq0 * 100 if eq0 > 0 else 0.0

        with st.container():
            s1, s2, s3, s4 = st.columns(4)
            with s1: _kpi("CURRENT EQUITY", f"${eq_now:,.2f}", "db series")
            with s2: _kpi("TOTAL RETURN", f"{ret:+.2f}%", "",
                          T["green"] if ret >= 0 else T["red"])
            with s3: _kpi("PEAK EQUITY", f"${peak:,.2f}", "session")
            with s4: _kpi("DRAWDOWN", f"{dd_pct:.2f}%", "",
                          T["red"] if dd_pct > 5 else T["green"])

        fig = go.Figure()
        if df["avail_clamped"].notna().any():
            fig.add_trace(go.Scattergl(
                x=df["dt"], y=df["avail_clamped"], name="Avail",
                mode="lines", line=dict(color=T["blue"], width=1, dash="dot"),
            ))
        fig.add_trace(go.Scattergl(
            x=df["dt"], y=df["equity_clamped"], name="Equity",
            mode="lines", line=dict(color=T["green"], width=2),
            fill="tozeroy", fillcolor=T["glow_g"],
        ))
        _fig_base(fig, height=280)
        fig_fresh = fig
        st.session_state[_KPI_KEY] = (eq_now, ret, peak, dd_pct)

    elif not df.empty and not _eq_data_changed:
        # Data unchanged — repaint KPIs only, reuse LKG chart (no flicker)
        lkg_kpis = st.session_state.get(_KPI_KEY)
        if lkg_kpis:
            eq_now, ret, peak, dd_pct = lkg_kpis
            with st.container():
                s1, s2, s3, s4 = st.columns(4)
                with s1: _kpi("CURRENT EQUITY", f"${eq_now:,.2f}", "db series")
                with s2: _kpi("TOTAL RETURN", f"{ret:+.2f}%", "",
                              T["green"] if ret >= 0 else T["red"])
                with s3: _kpi("PEAK EQUITY", f"${peak:,.2f}", "session")
                with s4: _kpi("DRAWDOWN", f"{dd_pct:.2f}%", "",
                              T["red"] if dd_pct > 5 else T["green"])
        # Reuse cached figure at end via single-slot paint (no double render)

    else:
        # No live or cached dataframe
        lkg_kpis = st.session_state.get(_KPI_KEY)
        if lkg_kpis:
            eq_now, ret, peak, dd_pct = lkg_kpis
            age_s = _lkg_age("equity")
            sub   = f"lkg ({age_s:.0f}s)" if age_s < LKG_MAX_AGE_SECS else "⚠ LKG STALE"
            with st.container():
                s1, s2, s3, s4 = st.columns(4)
                with s1: _kpi("CURRENT EQUITY", f"${eq_now:,.2f}", sub)
                with s2: _kpi("TOTAL RETURN", f"{ret:+.2f}%", "",
                              T["green"] if ret >= 0 else T["red"])
                with s3: _kpi("PEAK EQUITY", f"${peak:,.2f}", sub)
                with s4: _kpi("DRAWDOWN", f"{dd_pct:.2f}%", "",
                              T["red"] if dd_pct > 5 else T["green"])
        else:
            fallback = _get_binary_truth_equity(status)
            with st.container():
                s1, s2, s3, s4 = st.columns(4)
                with s1:
                    _kpi("CURRENT EQUITY",
                         f"${fallback:,.2f}" if fallback > 0 else "—",
                         "bt latch" if fallback > 0 else "",
                         T["green"] if fallback > 0 else T["muted"])
                with s2: _kpi("TOTAL RETURN", "—", "", T["muted"])
                with s3: _kpi("PEAK EQUITY",  "—", "", T["muted"])
                with s4: _kpi("DRAWDOWN",     "—", "", T["muted"])

    chart_slot = st.empty()
    fig_to_show = fig_fresh if fig_fresh is not None else st.session_state.get(_FIG_KEY_EQUITY)
    if fig_to_show is not None:
        if fig_fresh is not None:
            st.session_state[_FIG_KEY_EQUITY] = fig_fresh
        chart_slot.plotly_chart(
            fig_to_show,
            width="stretch",
            config={"displayModeBar": False},
            key="ot_equity_chart",
        )
    else:
        chart_slot.markdown(
            f'<div class="ot-tile" style="height:280px;display:flex;'
            f'align-items:center;justify-content:center;">'
            f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.72rem;">'
            f'AWAITING EQUITY DATA</span></div>',
            unsafe_allow_html=True,
        )



# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P21 EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_p21_execution() -> None:
    status = _get_status()
    _section("P21 INSTITUTIONAL EXECUTION — SLIPPAGE · TIF · MICROBURST", "🏛")

    p21 = status.get("p21_execution")
    if not isinstance(p21, dict) or not p21:
        p21 = _sg(_SS_P21, {})
    p21 = p21 if isinstance(p21, dict) else {}

    if not p21:
        bot_run = _safe_int(status.get("cycle_count")) > 0 or bool(status.get("positions"))
        if bot_run:
            p21 = {"p21_enabled": True, "_synthesised": True}
        else:
            _slot_placeholder("P21 DISABLED — awaiting bot start", 120)
            return

    if p21.get("_synthesised"):
        st.markdown(
            f'<div class="ot-alert-yellow">ℹ P21 status lag — refreshing…</div>',
            unsafe_allow_html=True,
        )

    sg  = p21.get("p21_slippage_guard", {})
    mb  = p21.get("p21_microburst",     {})
    tif = p21.get("p21_tif_watcher",    {})

    throttle_pct = _safe_float(p21.get("p21_spread_throttle_pct"), 0.05)
    mb_legs      = _safe_int(p21.get("p21_microburst_legs"), 3)
    mb_delay     = _safe_float(p21.get("p21_microburst_delay_secs"), 0.8)
    tif_timeout  = _safe_float(p21.get("p21_tif_timeout_secs"), 10.0)

    col_sg, col_mb, col_tif = st.columns([1.4, 1.0, 1.6])

    with col_sg:
        st.markdown(
            f'<div class="ot-kpi-label">[P21-1] SLIPPAGE GUARD — LIVE SPREADS</div>',
            unsafe_allow_html=True,
        )
        spreads   = sg.get("latest_spreads",  {}) or {}
        throttled = sg.get("throttled_now",   {}) or {}
        thr_cnt   = _safe_int(sg.get("throttle_count"))
        pass_cnt  = _safe_int(sg.get("pass_count"))
        total     = thr_cnt + pass_cnt
        thr_rate  = thr_cnt / total * 100 if total else 0

        if spreads:
            rows = ""
            for sym in sorted(spreads.keys()):
                sp   = _safe_float(spreads[sym])
                is_t = bool(throttled.get(sym, False))
                r    = sp / throttle_pct if throttle_pct > 0 else 0
                bpct = min(r * 100, 100)
                bcol = T["red"] if is_t else T["yellow"] if r > 0.7 else T["green"]
                tbg  = (f'<span class="ot-badge" style="color:{T["red"]};">THRT</span>'
                        if is_t else "")
                rows += (
                    f'<tr>'
                    f'<td style="color:{T["text"]};font-weight:700;">{_esc(sym)}</td>'
                    f'<td>{sp:.5f}% {tbg}'
                    f'<div class="ot-minibar-bg"><div class="ot-minibar-fill"'
                    f' style="width:{bpct:.1f}%;background:{bcol};height:3px;"></div></div></td>'
                    f'<td style="color:{bcol};">{r:.1f}×</td>'
                    f'</tr>'
                )
            hdr = "".join(
                f'<th>{h}</th>'
                for h in ["SYM", f"SPREAD (thr={throttle_pct:.4f}%)", "RATIO"]
            )
            st.markdown(
                f'<div class="ot-tile ot-scroll" style="padding:0;">'
                f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
                f'<tbody>{rows}</tbody></table></div>',
                unsafe_allow_html=True,
            )
        else:
            _slot_placeholder("AWAITING SPREAD DATA", 80)

        c1, c2 = st.columns(2)
        with c1:
            _kpi("THROTTLED", str(thr_cnt), f"{thr_rate:.1f}% of chks",
                 T["red"] if thr_cnt > 0 else T["muted"])
        with c2:
            _kpi("PASSED", str(pass_cnt), f"thr={throttle_pct:.4f}%", T["green"])

    with col_mb:
        st.markdown(f'<div class="ot-kpi-label">[P21-2] MICROBURST EXECUTOR</div>',
                    unsafe_allow_html=True)
        burst_ct  = _safe_int(mb.get("burst_count"))
        single_ct = _safe_int(mb.get("single_count"))
        leg_fills = _safe_int(mb.get("leg_fills_total"))
        fracs     = mb.get("leg_fractions", []) or []
        total_ord = burst_ct + single_ct

        chart_slot = st.empty()
        fig_fresh  = None
        if total_ord > 0:
            try:
                fig = go.Figure(go.Pie(
                    labels=["MicroBurst", "Single"],
                    values=[burst_ct, single_ct],
                    hole=0.60,
                    marker=dict(colors=[T["teal"], T["blue"]],
                                line=dict(color=T["bg"], width=2)),
                    textinfo="label+percent",
                    textfont=dict(size=8, color=T["text"], family=_FONT_MONO_PLOTLY),
                    hoverinfo="label+value",
                ))
                fig.update_layout(
                    height=170, margin=dict(l=4, r=4, t=8, b=4),
                    paper_bgcolor="rgba(0,0,0,0)", showlegend=False,
                    annotations=[dict(
                        text=f'<span style="font-weight:700;color:{T["teal"]};font-size:16px;">{burst_ct}</span>',
                        x=0.5, y=0.5, showarrow=False, xref="paper", yref="paper",
                    )],
                )
                fig_fresh = fig
            except Exception:
                log.debug("render_p21_execution: pie build failed", exc_info=True)

        _paint_fig(chart_slot, fig_fresh, _FIG_KEY_MB_PIE, 170,
                   "NO EXECUTIONS YET", "ot_mb_pie")

        if fracs:
            frac_html = "".join(
                f'<div style="display:flex;justify-content:space-between;'
                f'padding:1px 0;font-family:{T["mono"]};font-size:0.64rem;">'
                f'<span style="color:{T["text2"]};">LEG {i+1}</span>'
                f'<span style="color:{T["teal"]};">{_safe_float(fv)*100:.1f}%</span></div>'
                for i, fv in enumerate(fracs)
            )
            st.markdown(
                f'<div class="ot-tile">'
                f'<div class="ot-kpi-label">TAPERED LEGS ({mb_legs}×{mb_delay:.2f}s)</div>'
                f'{frac_html}'
                f'<div style="border-top:1px solid {T["border"]};margin-top:4px;'
                f'padding-top:4px;display:flex;justify-content:space-between;'
                f'font-family:{T["mono"]};font-size:0.64rem;">'
                f'<span style="color:{T["text2"]};">LEG FILLS</span>'
                f'<span style="color:{T["green"]};">{leg_fills}</span></div></div>',
                unsafe_allow_html=True,
            )

    with col_tif:
        st.markdown(
            f'<div class="ot-kpi-label">[P21-3] TIF WATCHER (timeout={tif_timeout:.0f}s)</div>',
            unsafe_allow_html=True,
        )
        pending_ords = tif.get("pending_orders", []) or []
        cancel_ct    = _safe_int(tif.get("cancel_count"))
        fill_ct      = _safe_int(tif.get("fill_count"))
        timeout_ct   = _safe_int(tif.get("timeout_count"))
        pending_ct   = _safe_int(tif.get("pending_count"))

        c1, c2, c3 = st.columns(3)
        with c1:
            _kpi("CANCELLED", str(cancel_ct), "TIF fired",
                 T["red"] if cancel_ct > 0 else T["muted"])
        with c2:
            _kpi("FILLED", str(fill_ct), "before TO", T["green"])
        with c3:
            _kpi("TIMEDOUT", str(timeout_ct), "total",
                 T["yellow"] if timeout_ct > 0 else T["muted"])

        if pending_ords:
            rows = ""
            for o in pending_ords[:8]:
                age  = _safe_float(o.get("age_secs"))
                pct  = _safe_float(o.get("pct_timeout"))
                bcol = T["red"] if pct > 80 else T["yellow"] if pct > 50 else T["green"]
                sym  = _esc(str(o.get("symbol", "?")))
                side = _esc(str(o.get("side", "?")).upper())
                usd  = _safe_float(o.get("usd"))
                oid  = _esc(str(o.get("ord_id", ""))[-10:])
                scol = T["green"] if side in ("BUY", "LONG") else T["red"]
                rows += (
                    f'<tr>'
                    f'<td style="color:{T["text2"]};font-size:0.60rem;">{oid}</td>'
                    f'<td style="font-weight:700;">{sym}</td>'
                    f'<td style="color:{scol};">{side}</td>'
                    f'<td style="color:{T["text2"]};">${usd:.2f}</td>'
                    f'<td style="color:{bcol};">{age:.1f}s'
                    f'<div class="ot-minibar-bg"><div class="ot-minibar-fill"'
                    f' style="width:{min(pct,100):.1f}%;background:{bcol};height:3px;"></div></div></td>'
                    f'</tr>'
                )
            hdr = "".join(f'<th>{h}</th>'
                          for h in ["ID", "SYM", "SIDE", "USD", f"AGE/{tif_timeout:.0f}s"])
            st.markdown(
                f'<div class="ot-tile ot-scroll" style="padding:0;">'
                f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
                f'<tbody>{rows}</tbody></table></div>',
                unsafe_allow_html=True,
            )
        elif pending_ct == 0:
            st.markdown(
                f'<div class="ot-alert-green">✓ TIF watcher IDLE</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="ot-alert-yellow">⚠ {pending_ct} pending…</div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: POSITIONS PANEL
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_positions_panel() -> None:
    status = _get_status()
    _section("POSITION MONITOR — HEAT · SIGNAL · TRUST · SNR", "🌡")

    positions = status.get("positions", {})
    if not isinstance(positions, dict): positions = {}
    analytics = status.get("analytics", {})
    if not isinstance(analytics, dict):
        analytics = _sg(_SS_ANALYTICS, {})

    account = status.get("account", {})
    equity  = _get_binary_truth_equity(status)
    avail   = _safe_float(account.get("avail", account.get("buying_power", 0)))
    lev     = max(_safe_float(account.get("leverage", 1.0), 1.0), 1.0)

    deployed_margin = 0.0
    for p in positions.values():
        if p.get("direction", "none") == "none":
            continue
        mu = _safe_float(p.get("margin_used"))
        if mu > 0:
            deployed_margin += mu
        else:
            notional = _safe_float(p.get("notional",
                                   p.get("position_usd", p.get("cost_basis", 0))))
            plev = max(_safe_float(p.get("leverage", lev), lev), 1.0)
            deployed_margin += notional / plev

    pos_cnt  = sum(1 for p in positions.values() if p.get("direction", "none") != "none")
    heat_pct = (deployed_margin / equity * 100) if equity > 0 and pos_cnt > 0 else 0.0
    h_col    = (T["green"] if heat_pct < 30 else T["yellow"] if heat_pct < 60
                else T["orange"] if heat_pct < 80 else T["red"])
    h_lbl    = ("LOW" if heat_pct < 30 else "MODERATE" if heat_pct < 60
                else "ELEVATED" if heat_pct < 80 else "CRITICAL")

    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
    with c1:
        st.markdown(
            f'<div class="ot-tile">'
            f'<div class="ot-kpi-label">PORTFOLIO HEAT — MARGIN vs EQUITY</div>'
            f'<div style="display:flex;justify-content:space-between;margin-bottom:4px;">'
            f'<span style="font-family:{T["mono"]};font-size:0.82rem;color:{h_col};font-weight:700;">'
            f'{heat_pct:.1f}% · {h_lbl}</span>'
            f'<span style="font-family:{T["mono"]};font-size:0.68rem;color:{T["text2"]};">'
            f'${deployed_margin:,.2f} / ${equity:,.2f}</span></div>'
            f'<div class="ot-heat-track"><div class="ot-heat-fill"'
            f' style="width:{min(heat_pct,100):.1f}%;'
            f'background:linear-gradient(90deg,{T["green"]},{h_col});'
            f'{"box-shadow:0 0 6px 1px "+T["red"]+";" if heat_pct>80 else ""}"></div></div></div>',
            unsafe_allow_html=True,
        )
    with c2: _kpi("HEAT %",   f"{heat_pct:.1f}%",         "", h_col)
    with c3: _kpi("MARGIN $", f"${deployed_margin:,.0f}", "")
    with c4:
        ratio = deployed_margin / avail if avail > 0 and pos_cnt > 0 else 0
        _kpi("RISK RATIO", f"{ratio:.2f}×", "",
             T["red"] if ratio > 2 else T["yellow"] if ratio > 1 else T["green"])

    pos_active = {s: p for s, p in positions.items() if p.get("direction", "none") != "none"}
    all_syms   = list(positions.keys())

    if pos_active:
        rows = ""
        for sym, p in pos_active.items():
            drx   = _esc(p.get("direction", "—").upper())
            dcol  = T["green"] if drx == "LONG" else T["red"]
            qty   = _safe_float(p.get("qty"))
            bid   = _safe_float(p.get("current_bid"))
            pnl   = p.get("pnl_pct")
            # [FIX-FSTR-001] Python 3.11: no nested f-strings/backslash in f-expr
            _pnl_f     = _safe_float(pnl)
            _pnl_sign  = "+" if _pnl_f >= 0 else ""
            _pnl_str   = f"{_pnl_sign}{_pnl_f:.2f}%"
            _pnl_col   = "#00FF41" if _pnl_f >= 0 else "#FF003C"
            pnl_html = (
                f'<span style="color:{_pnl_col};">'
                f'{_pnl_str}</span>'
                if pnl is not None else "<span>—</span>"
            )
            current_regime = p.get("regime", "unknown")
            meta  = REGIME_META.get(current_regime, REGIME_META.get("unknown", {}))
            icon  = meta.get("icon", "❓")
            color = meta.get("col", T["muted"])
            label = meta.get("label", "UNKNOWN")
            reg_badge = f'<span style="color:{color};">{icon} {label}</span>'
            sig_dir   = _esc(p.get("signal_dir", "—").upper())
            sig_prob  = _safe_float(p.get("signal_prob"))
            trust     = _safe_float(p.get("p13_agg_trust", DEFAULT_TRUST), DEFAULT_TRUST)
            conv      = _safe_float(p.get("p18_conviction_multiplier", 1.0), 1.0)
            hc        = bool(p.get("p14_high_conviction"))
            hc_badge  = (f'<span class="ot-badge" style="color:{T["purple"]};">HC</span>'
                         if hc else "")
            obi       = _safe_float(p.get("p12_obi"))
            obi_col   = T["green"] if obi > 0.3 else T["red"] if obi < -0.3 else T["text2"]
            fr        = _safe_float(p.get("funding_rate"))
            fb        = bool(p.get("funding_brake_active"))
            fr_col    = T["red"] if abs(fr) > 0.005 else T["text2"]

            # [TRACK14-DASH] Per-position USD cost and % of equity
            _usd_cost = _safe_float(p.get("usd_cost"))
            if _usd_cost > 0 and equity > 0:
                _pct_eq = _usd_cost / equity * 100.0
                _usd_str = f'<span style="color:{T["text2"]};font-family:{T["mono"]};font-size:0.63rem;">${_usd_cost:,.0f}</span>'
                _pct_eq_col = (T["green"] if _pct_eq < 15 else
                               T["yellow"] if _pct_eq < 30 else
                               T["orange"] if _pct_eq < 50 else T["red"])
                _pct_str = f'<span style="color:{_pct_eq_col};font-family:{T["mono"]};font-size:0.63rem;">{_pct_eq:.1f}%</span>'
            else:
                _usd_str = f'<span style="color:{T["muted"]};">—</span>'
                _pct_str = f'<span style="color:{T["muted"]};">—</span>'

            sym_anl = analytics.get(sym, {}) if isinstance(analytics, dict) else {}
            pred    = sym_anl.get("predicted_move", []) if isinstance(sym_anl, dict) else []
            act     = sym_anl.get("actual_realized_move", []) if isinstance(sym_anl, dict) else []
            if not isinstance(pred, list): pred = [pred] if pred else []
            if not isinstance(act,  list): act  = [act]  if act  else []
            snr = _calc_snr(pred, act)

            rows += (
                f'<tr>'
                f'<td style="font-weight:700;color:{T["text"]};font-size:0.78rem;">{_esc(sym)}</td>'
                f'<td style="color:{dcol};font-weight:700;">{drx}</td>'
                f'<td>{pnl_html}</td>'
                f'<td style="color:{T["text2"]};">{qty:.4f}</td>'
                f'<td style="color:{T["text2"]};">${bid:,.4f}</td>'
                f'<td style="padding:3px 6px;">{_usd_str}</td>'
                f'<td style="padding:3px 6px;">{_pct_str}</td>'
                f'<td>{reg_badge}</td>'
                f'<td style="color:{T["blue"]};">{sig_dir} {sig_prob:.2f}</td>'
                f'<td class="{_trust_class(trust)}">{trust:.2f}</td>'
                f'<td style="color:{T["purple"]};">{conv:.3f}× {hc_badge}</td>'
                f'<td style="color:{obi_col};">{obi:+.3f}</td>'
                f'<td style="color:{fr_col};">{fr*100:.4f}% {"🛑" if fb else ""}</td>'
                f'<td>{_snr_badge(snr)}</td>'
                f'</tr>'
            )
        hdr = "".join(f'<th>{h}</th>'
                      for h in ["SYMBOL", "DIR", "PNL%", "QTY", "BID",
                                 "USD COST", "% EQ",
                                 "REGIME", "SIGNAL", "TRUST", "CONV×", "OBI", "FUND%", "SNR"])
        st.markdown(
            f'<div class="ot-tile" style="padding:0;overflow:auto;">'
            f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
            f'<tbody>{rows}</tbody></table></div>',
            unsafe_allow_html=True,
        )

        # [TRACK14-DASH] Total notional tile — Σ usd_cost across all open positions
        _total_usd_cost = sum(
            _safe_float(p.get("usd_cost"))
            for p in pos_active.values()
        )
        _total_pct_eq = (_total_usd_cost / equity * 100.0) if equity > 0 else 0.0
        _tot_col = (T["green"] if _total_pct_eq < 40 else
                    T["yellow"] if _total_pct_eq < 70 else
                    T["orange"] if _total_pct_eq < 90 else T["red"])
        _tot_lbl = ("LOW" if _total_pct_eq < 40 else
                    "MODERATE" if _total_pct_eq < 70 else
                    "ELEVATED" if _total_pct_eq < 90 else "CRITICAL")

        # [TRACK14-DASH] Gate-configuration footnote from admission_policy
        _ap = _get_status().get("admission_policy", {})
        if not isinstance(_ap, dict):
            _ap = {}
        _gate_max_pos   = _safe_int(_ap.get("max_open_positions", 0))
        _gate_dep_pct   = _safe_float(_ap.get("max_deployed_pct", 0.0))
        _cur_open       = _safe_int(_ap.get("current_open_positions", len(pos_active)))
        _cur_dep_usd    = _safe_float(_ap.get("current_deployed_usd", _total_usd_cost))
        _cur_dep_pct    = _safe_float(_ap.get("current_deployed_pct", _total_pct_eq / 100.0))
        # [TRACK27-PSC3] Per-symbol cap visibility
        _gate_psc_usd   = _safe_float(_ap.get("max_per_symbol_usd", 0.0))
        _cur_psc_dict   = _ap.get("current_per_symbol_usd", {})
        if not isinstance(_cur_psc_dict, dict):
            _cur_psc_dict = {}
        _cur_psc_max    = max(_cur_psc_dict.values()) if _cur_psc_dict else 0.0

        _conc_gate_str  = (f"MAX {_gate_max_pos} ({_cur_open}/{_gate_max_pos})"
                           if _gate_max_pos > 0 else "OFF")
        _dep_gate_str   = (f"{_gate_dep_pct*100:.0f}% ({_cur_dep_pct*100:.1f}% now)"
                           if _gate_dep_pct > 0.0 else "OFF")
        _psc_gate_str   = (f"${_gate_psc_usd:,.0f} (max now ${_cur_psc_max:,.0f})"
                           if _gate_psc_usd > 0.0 else "OFF")
        _conc_gate_col  = (T["yellow"] if _gate_max_pos > 0 and _cur_open >= _gate_max_pos - 1
                           else T["green"] if _gate_max_pos > 0 else T["muted"])
        _dep_gate_col   = (T["yellow"] if _gate_dep_pct > 0.0 and _cur_dep_pct >= _gate_dep_pct * 0.9
                           else T["green"] if _gate_dep_pct > 0.0 else T["muted"])
        _psc_gate_col   = (T["yellow"] if _gate_psc_usd > 0.0 and _cur_psc_max >= _gate_psc_usd * 0.9
                           else T["green"] if _gate_psc_usd > 0.0 else T["muted"])

        _nt1, _nt2 = st.columns([3, 1], gap="small")
        with _nt1:
            st.markdown(
                f'<div class="ot-tile" style="padding:10px;">'
                f'<div class="ot-kpi-label">Σ NOTIONAL — OPEN COST BASIS vs EQUITY</div>'
                f'<div style="display:flex;justify-content:space-between;margin-bottom:4px;">'
                f'<span style="font-family:{T["mono"]};font-size:0.82rem;'
                f'color:{_tot_col};font-weight:700;">'
                f'{_total_pct_eq:.1f}% · {_tot_lbl}</span>'
                f'<span style="font-family:{T["mono"]};font-size:0.68rem;color:{T["text2"]};">'
                f'${_total_usd_cost:,.2f} / ${equity:,.2f}</span></div>'
                f'<div class="ot-heat-track"><div class="ot-heat-fill"'
                f' style="width:{min(_total_pct_eq, 100):.1f}%;'
                f'background:linear-gradient(90deg,{T["green"]},{_tot_col});'
                f'{"box-shadow:0 0 6px 1px "+T["red"]+";" if _total_pct_eq > 90 else ""}'
                f'"></div></div>'
                f'<div style="font-family:{T["mono"]};font-size:0.57rem;color:{T["muted"]};'
                f'margin-top:5px;">'
                f'Cost-basis sum · not mark-to-market · '
                f'<span style="color:{_conc_gate_col};">CONCURRENT CAP: {_esc(_conc_gate_str)}</span>'
                f' &nbsp;·&nbsp; '
                f'<span style="color:{_dep_gate_col};">DEPLOYED CAP: {_esc(_dep_gate_str)}</span>'
                f' &nbsp;·&nbsp; '
                f'<span style="color:{_psc_gate_col};">PER SYMBOL CAP: {_esc(_psc_gate_str)}</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )
        with _nt2:
            _kpi("Σ NOTIONAL", f"{_total_pct_eq:.1f}%",
                 f"${_total_usd_cost:,.0f}", _tot_col)

    else:
        rows = ""
        for sym in all_syms:
            p        = positions.get(sym, {})
            regime   = p.get("regime", "neutral")
            rm       = REGIME_META.get(regime, REGIME_META["neutral"])
            sig_prob = _safe_float(p.get("signal_prob"))
            sig_dir  = _esc(p.get("signal_dir", "neutral").upper())
            trust    = _safe_float(p.get("p13_agg_trust", DEFAULT_TRUST), DEFAULT_TRUST)
            obi      = _safe_float(p.get("p12_obi"))
            obi_col  = T["green"] if obi > 0.3 else T["red"] if obi < -0.3 else T["text2"]
            fr       = _safe_float(p.get("funding_rate"))
            sniper   = bool(p.get("sniper_ready"))
            sn_badge = (f'<span class="ot-badge" style="color:{T["cyan"]};">SNIPER</span>'
                        if sniper else "")
            rows += (
                f'<tr>'
                f'<td style="font-weight:700;color:{T["text2"]};">{_esc(sym)}</td>'
                f'<td style="color:{T["muted"]};">IDLE</td>'
                f'<td style="color:{rm["col"]};">{rm["icon"]} {rm["label"]}</td>'
                f'<td style="color:{T["blue"]};">{sig_dir} {sig_prob:.3f}</td>'
                f'<td class="{_trust_class(trust)}">{trust:.2f}</td>'
                f'<td style="color:{obi_col};">{obi:+.3f}</td>'
                f'<td style="color:{T["text2"]};">{fr*100:.4f}%</td>'
                f'<td>{sn_badge}</td>'
                f'</tr>'
            )
        hdr = "".join(f'<th>{h}</th>'
                      for h in ["SYMBOL", "STATUS", "REGIME", "SIGNAL", "TRUST", "OBI", "FUND%", "FLAGS"])
        st.markdown(
            f'<div class="ot-tile" style="padding:0;">'
            f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
            f'<tbody>{rows}</tbody></table></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: TRADE ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_trade_analytics() -> None:
    _section("TRADE HISTORY — RECENT FILLS", "💹")

    _LKG_KPIS  = "_b3_ta_kpi_vals"
    _LKG_TABLE = "_b3_ta_table_html"
    _LKG_TS    = "_b3_ta_latest_ts"

    _skel_kpis = (
        f'<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;">'
        + (f'<div class="ot-tile"><div class="ot-kpi-label">LOADING</div>'
           f'<div class="ot-kpi-value" style="color:{T["muted"]};">—</div></div>') * 4
        + '</div>'
    )
    _skel_table = (
        f'<div class="ot-tile ot-scroll" style="padding:12px;min-height:180px;'
        f'display:flex;align-items:center;justify-content:center;">'
        f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.72rem;">'
        f'LOADING TRADE HISTORY…</span></div>'
    )


    cached_kpis  = st.session_state.get(_LKG_KPIS)
    cached_table = st.session_state.get(_LKG_TABLE)

    new_kpis  = None
    new_table = None
    try:
        # Force bust on every render cycle — guarantees fresh OKX fills appear
        _force_bust_trades_cache()
        df = load_trades()

        if df.empty:
            # [FIX-CRIT: table_slot NameError + NO-TRADES path]
            # 'table_slot' was never defined in this scope — NameError silently
            # caught every cycle, trapping the panel in permanent LOADING state.
            # Fix: assign new_table so the LKG display logic below renders it
            # correctly instead of falling back to _skel_table.
            if cached_table is None and not _trades_known_nonempty():
                new_table = (
                    f'<div class="ot-tile ot-scroll" style="padding:12px;min-height:180px;'
                    f'display:flex;align-items:center;justify-content:center;">'
                    f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.72rem;">'
                    f'NO TRADES RECORDED YET</span></div>'
                )
        else:
            st.session_state[_SS_HAS_TRADES] = True
            # [PHASE4] KPI metrics (win rate, total PnL, avg PnL, trade count) must
            # be computed over canonical close-leg rows only (is_close_leg = 1).
            # Open-leg rows, DCA add rows, and noise fill rows (is_close_leg = 0 or
            # NULL / absent on old rows) are excluded so win rate and total PnL
            # reflect only finalized round-trip results.
            # Fall back to pnl_pct != 0 as a best-effort heuristic on schemas that
            # pre-date the Phase 4 migration (is_close_leg column absent).
            if "is_close_leg" in df.columns:
                kpi_df = df[df["is_close_leg"] == 1]
            else:
                kpi_df = df[df["pnl_pct"].fillna(0.0) != 0.0]
            total_pnl = _safe_float(kpi_df["realized_usd"].sum()) if "realized_usd" in kpi_df.columns else 0.0
            wins      = int((kpi_df["realized_usd"] > 0).sum()) if "realized_usd" in kpi_df.columns else 0
            total     = len(kpi_df)
            wr        = wins / total * 100 if total > 0 else 0.0
            avg_pnl   = (_safe_float(kpi_df["realized_usd"].mean())
                         if "realized_usd" in kpi_df.columns and total > 0 else 0.0)
            new_kpis  = (total, wr, wins, total_pnl, avg_pnl)

            latest_ts = None
            if "ts" in df.columns and total:
                try:
                    latest_ts = int(df["ts"].iloc[0])
                except Exception:
                    pass

            prev_ts = st.session_state.get(_LKG_TS)
            # Rebuild table whenever latest trade ts changes OR no cached table exists
            if latest_ts is not None and prev_ts == latest_ts and cached_table is not None:
                    new_table = cached_table  # no new trades — reuse rendered HTML
            else:
                rows = ""
                for _, row in df.head(10).iterrows():
                    side    = _esc(str(row.get("side", "?")).upper())
                    scol    = T["green"] if side in ("BUY", "LONG", "ENTRY_LONG") else T["red"]
                    sym     = _esc(_norm_sym(str(row.get("symbol", "?"))))
                    px      = _safe_float(row.get("price"))
                    pnl     = row.get("realized_usd")
                    pnl_str = f"${_safe_float(pnl):+,.2f}" if pd.notna(pnl) else "—"
                    pnl_col = (T["green"] if _safe_float(pnl) > 0
                               else T["red"] if _safe_float(pnl) < 0 else T["muted"])
                    pnl_pct = _safe_float(row.get("pnl_pct"))
                    tag     = _esc(str(row.get("tag", ""))[:16])
                    ts_val  = row.get("dt", "")
                    ts_str  = (ts_val.strftime("%m-%d %H:%M")
                               if hasattr(ts_val, "strftime") else "—")
                    rows += (
                        f'<tr>'
                        f'<td style="color:{T["text2"]};font-size:0.60rem;">{ts_str}</td>'
                        f'<td style="font-weight:700;color:{T["text"]};">{sym}</td>'
                        f'<td style="color:{scol};font-weight:700;">{side}</td>'
                        f'<td style="color:{T["text2"]};font-size:0.70rem;">{px:,.4f}</td>'
                        f'<td style="color:{pnl_col};font-weight:700;">{pnl_str}</td>'
                        f'<td style="color:{T["muted"]};font-size:0.60rem;">{pnl_pct:+.2f}%</td>'
                        f'<td style="color:{T["muted"]};font-size:0.58rem;">{tag}</td>'
                        f'</tr>'
                    )
                new_table = (
                    f'<div class="ot-tile ot-scroll" style="padding:10px;min-height:180px;">'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>TIME</th><th>SYMBOL</th><th>SIDE</th>'
                    f'<th>PRICE</th><th>PNL</th><th>PNL%</th><th>TAG</th>'
                    f'</tr></thead><tbody>{rows}</tbody></table></div>'
                )
                if latest_ts is not None:
                    st.session_state[_LKG_TS] = latest_ts

            st.session_state[_LKG_KPIS]  = new_kpis
            st.session_state[_LKG_TABLE] = new_table

    except Exception as e:
        log.debug("[B3][trade_analytics] data error (keeping LKG): %s", e)

    # Single consolidated render — fresh data takes priority, falls back to cache
    display_kpis  = new_kpis  if new_kpis  is not None else cached_kpis
    display_table = new_table if new_table is not None else cached_table

    if display_kpis is not None:
        _tot, _wr, _wins, _tpnl, _apnl = display_kpis
        _sub = "all time" if new_kpis is not None else f"lkg ({_lkg_age('trades'):.0f}s)"
        c1, c2, c3, c4 = st.columns(4)
        with c1: _kpi("TOTAL TRADES",  str(_tot), _sub)
        with c2: _kpi("WIN RATE",      f"{_wr:.1f}%", f"{_wins}W / {_tot-_wins}L",
                      T["green"] if _wr > 55 else T["yellow"] if _wr > 40 else T["red"])
        with c3: _kpi("REALIZED PnL",  f"${_tpnl:+,.2f}", _sub,
                      T["green"] if _tpnl >= 0 else T["red"])
        with c4: _kpi("AVG TRADE PnL", f"${_apnl:+,.2f}", _sub,
                      T["green"] if _apnl >= 0 else T["red"])
    else:
        st.markdown(_skel_kpis, unsafe_allow_html=True)

    if display_table is not None:
        st.markdown(display_table, unsafe_allow_html=True)
    else:
        st.markdown(_skel_table, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: PROFITABILITY DIAGNOSTICS  [PHASE5]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_profitability_diagnostics() -> None:
    """[PHASE5/6] Grouped profitability truth from canonical close-leg rows.

    Renders:
      1. Emergency-exit dominance summary (STRATEGY vs RISK+ANOMALY)
      2. PnL / win-rate breakdown by exit reason category
      3. PnL / win-rate breakdown by symbol
      [PHASE6]
      4. PnL / win-rate breakdown by exit reason type (mechanism detail)
      5. Entry quality: winner vs loser confidence/kelly comparison
      6. Hold time by exit category
      [PHASE10]
      7. Close-leg slippage by symbol (avg and median bps, worst first)
      8. Execution friction by exit category (slippage + spread comparison)
      [POST-ROADMAP-DT]
      9. PnL / win-rate by entry regime × direction
      [TRACK03]
      10. PnL / win-rate by entry confidence tier
      11. PnL / win-rate by entry z_score tier
      12. PnL / win-rate by symbol × entry regime
      [TRACK05]
      13. PnL / win-rate by symbol × entry regime WITH friction per cell
      14. PnL / win-rate by z_score tier × direction
      [TRACK06]
      15. Setup evidence summary: symbol × regime × direction ranked table

    All metrics sourced exclusively from is_close_leg = 1 rows via
    _load_close_diagnostics().  Degrades gracefully on empty or
    pre-Phase-4/6/10/DT/Track-01/Track-05/Track-06 schemas.
    """
    _section("PROFITABILITY DIAGNOSTICS — EXIT ATTRIBUTION", "🔬")

    try:
        df = _load_close_diagnostics()
    except Exception as _exc:
        log.debug("[P5-DIAG] _load_close_diagnostics error: %s", _exc)
        df = pd.DataFrame()

    if df.empty:
        st.markdown(
            f'<div class="ot-tile" style="padding:12px;text-align:center;">'
            f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.72rem;">'
            f'NO CLOSED TRADES RECORDED YET</span></div>',
            unsafe_allow_html=True,
        )
        return

    total_rows = len(df)
    has_categories = (
        "exit_reason_category" in df.columns
        and df["exit_reason_category"].notna().any()
    )
    has_reason_types = (
        "exit_reason_type" in df.columns
        and df["exit_reason_type"].notna().any()
    )
    # [PHASE6] Phase 6 diagnostic columns available?
    has_p6_quality = (
        "entry_confidence" in df.columns
        and df["entry_confidence"].notna().any()
    )
    has_p6_hold = (
        "hold_time_secs" in df.columns
        and df["hold_time_secs"].notna().any()
    )
    # [PHASE10] Phase 10 friction columns available?
    has_p10_friction = (
        "close_slippage_bps" in df.columns
        and df["close_slippage_bps"].notna().any()
    )
    # [POST-ROADMAP-DT] Entry-context columns available?
    has_entry_regime = (
        "entry_regime" in df.columns
        and df["entry_regime"].notna().any()
        and "entry_direction" in df.columns
        and df["entry_direction"].notna().any()
    )

    # ── Helper: compute group stats ──────────────────────────────────────────
    def _group_stats(sub: pd.DataFrame) -> tuple:
        """Return (count, win_count, total_pnl_usd, avg_pnl_usd)."""
        n     = len(sub)
        wins  = int((sub["realized_usd"] > 0).sum()) if "realized_usd" in sub else 0
        total = float(sub["realized_usd"].sum())     if "realized_usd" in sub else 0.0
        avg   = total / n if n > 0 else 0.0
        return n, wins, total, avg

    def _pnl_col(v: float) -> str:
        return T["green"] if v > 0 else T["red"] if v < 0 else T["muted"]

    def _wr_col(wins: int, n: int) -> str:
        if n == 0:
            return T["muted"]
        wr = wins / n
        return T["green"] if wr > 0.55 else T["yellow"] if wr > 0.40 else T["red"]

    # ── 1. Emergency-exit dominance summary ─────────────────────────────────
    if has_categories:
        strategy_mask = df["exit_reason_category"] == "STRATEGY"
        emergency_mask = df["exit_reason_category"].isin(["RISK", "ANOMALY"])

        strat_n, strat_w, strat_pnl, _ = _group_stats(df[strategy_mask])
        emerg_n, emerg_w, emerg_pnl, _ = _group_stats(df[emergency_mask])
        other_n = total_rows - strat_n - emerg_n

        emerg_pct = emerg_n / total_rows * 100 if total_rows > 0 else 0.0
        emerg_col = T["red"] if emerg_pct > 30 else T["yellow"] if emerg_pct > 15 else T["green"]

        emerg_pnl_str  = f"${emerg_pnl:+,.2f}"
        strat_pnl_str  = f"${strat_pnl:+,.2f}"

        dominance_html = (
            f'<div class="ot-tile" style="margin-bottom:6px;padding:8px 12px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
            f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
            f'EMERGENCY EXIT DOMINANCE (RISK + ANOMALY)</div>'
            f'<div style="display:flex;gap:16px;flex-wrap:wrap;">'
            f'<div><span style="color:{T["muted"]};font-size:0.65rem;font-family:{T["mono"]};">STRATEGY&nbsp;</span>'
            f'<span style="color:{_pnl_col(strat_pnl)};font-weight:700;font-family:{T["mono"]};">'
            f'{strat_n}× {strat_pnl_str}</span></div>'
            f'<div><span style="color:{T["muted"]};font-size:0.65rem;font-family:{T["mono"]};">EMERGENCY&nbsp;</span>'
            f'<span style="color:{emerg_col};font-weight:700;font-family:{T["mono"]};">'
            f'{emerg_n}× {emerg_pnl_str} ({emerg_pct:.0f}%)</span></div>'
            + (f'<div><span style="color:{T["muted"]};font-size:0.65rem;font-family:{T["mono"]};">OTHER&nbsp;</span>'
               f'<span style="color:{T["muted"]};font-family:{T["mono"]};">{other_n}×</span></div>'
               if other_n > 0 else "")
            + f'</div></div>'
        )
        st.markdown(dominance_html, unsafe_allow_html=True)

        if emerg_pct > 30:
            st.markdown(
                f'<div class="ot-alert-red">⚠ Emergency exits represent {emerg_pct:.0f}% of closed trades '
                f'({emerg_n}/{total_rows}). Risk or anomaly logic may be dominating outcomes.</div>',
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            f'<div class="ot-alert-yellow">Exit categories not yet attributed — '
            f'dominance summary requires Phase 4 trade data.</div>',
            unsafe_allow_html=True,
        )

    # ── 2. Category breakdown table ──────────────────────────────────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    _CAT_ORDER = ["STRATEGY", "RISK", "ANOMALY", "MANUAL", "RECONCILIATION"]
    _CAT_COLORS = {
        "STRATEGY":       T["green"],
        "RISK":           T["yellow"],
        "ANOMALY":        T["red"],
        "MANUAL":         T["blue"],
        "RECONCILIATION": T["muted"],
    }

    if has_categories:
        cat_rows_html = ""
        # Build stats per category
        cats_present = []
        for cat in _CAT_ORDER:
            sub = df[df["exit_reason_category"] == cat]
            if len(sub) == 0:
                continue
            n, wins, total, avg = _group_stats(sub)
            wr_pct = wins / n * 100 if n > 0 else 0.0
            cats_present.append((cat, n, wins, wr_pct, total, avg))

        # NULL / unattributed rows
        null_mask = df["exit_reason_category"].isna()
        if null_mask.any():
            sub_null = df[null_mask]
            n, wins, total, avg = _group_stats(sub_null)
            wr_pct = wins / n * 100 if n > 0 else 0.0
            cats_present.append(("UNATTRIBUTED", n, wins, wr_pct, total, avg))

        for cat, n, wins, wr_pct, total, avg in cats_present:
            col = _CAT_COLORS.get(cat, T["muted"])
            losses = n - wins
            cat_rows_html += (
                f'<tr>'
                f'<td style="color:{col};font-weight:700;">{_esc(cat)}</td>'
                f'<td style="color:{T["text2"]};">{n}</td>'
                f'<td style="color:{_wr_col(wins, n)};font-weight:700;">{wr_pct:.0f}%'
                f'<span style="color:{T["muted"]};font-weight:400;font-size:0.60rem;"> '
                f'{wins}W/{losses}L</span></td>'
                f'<td style="color:{_pnl_col(total)};font-weight:700;">${total:+,.2f}</td>'
                f'<td style="color:{_pnl_col(avg)};">${avg:+,.2f}</td>'
                f'</tr>'
            )

        cat_table_html = (
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
            f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">BY EXIT CATEGORY</div>'
            f'<table class="ot-table"><thead><tr>'
            f'<th>CATEGORY</th><th>TRADES</th><th>WIN RATE</th>'
            f'<th>TOTAL PnL</th><th>AVG PnL</th>'
            f'</tr></thead><tbody>{cat_rows_html}</tbody></table></div>'
        )
        st.markdown(cat_table_html, unsafe_allow_html=True)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
            f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">BY EXIT CATEGORY</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;padding:6px 0;">'
            f'Not yet available — requires Phase 4 trade attribution data.</div></div>',
            unsafe_allow_html=True,
        )

    # ── 3. Per-symbol breakdown table ────────────────────────────────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    if "symbol" in df.columns and df["symbol"].notna().any():
        try:
            sym_rows_html = ""
            sym_groups = df.groupby("symbol", sort=False)
            sym_stats = []
            for sym, sub in sym_groups:
                n, wins, total, avg = _group_stats(sub)
                sym_stats.append((sym, n, wins, total, avg))
            # Sort by absolute total PnL descending so worst losers appear first
            sym_stats.sort(key=lambda x: x[3])

            for sym, n, wins, total, avg in sym_stats[:15]:  # cap at 15 symbols
                losses = n - wins
                wr_pct = wins / n * 100 if n > 0 else 0.0
                sym_rows_html += (
                    f'<tr>'
                    f'<td style="font-weight:700;color:{T["text"]};">'
                    f'{_esc(_norm_sym(str(sym)))}</td>'
                    f'<td style="color:{T["text2"]};">{n}</td>'
                    f'<td style="color:{_wr_col(wins, n)};font-weight:700;">{wr_pct:.0f}%'
                    f'<span style="color:{T["muted"]};font-weight:400;font-size:0.60rem;"> '
                    f'{wins}W/{losses}L</span></td>'
                    f'<td style="color:{_pnl_col(total)};font-weight:700;">${total:+,.2f}</td>'
                    f'<td style="color:{_pnl_col(avg)};">${avg:+,.2f}</td>'
                    f'</tr>'
                )

            sym_table_html = (
                f'<div class="ot-tile ot-scroll" style="padding:10px;">'
                f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
                f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
                f'BY SYMBOL (sorted by total PnL, worst first)</div>'
                f'<table class="ot-table"><thead><tr>'
                f'<th>SYMBOL</th><th>TRADES</th><th>WIN RATE</th>'
                f'<th>TOTAL PnL</th><th>AVG PnL</th>'
                f'</tr></thead><tbody>{sym_rows_html}</tbody></table></div>'
            )
            st.markdown(sym_table_html, unsafe_allow_html=True)
        except Exception as _exc:
            log.debug("[P5-DIAG] symbol groupby failed: %s", _exc)

    # ── 4. [PHASE6] Per-exit-reason-type breakdown ───────────────────────────
    # Answers: which specific mechanism is causing emergency losses?
    # e.g. ANOMALY_VPIN_TOXIC vs RISK_CIRCUIT_BREAKER vs RISK_DRAWDOWN_ZOMBIE
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    if has_reason_types:
        try:
            # Highlight known high-priority emergency reason types
            _EMERG_TYPES = {
                "ANOMALY_VPIN_TOXIC", "ANOMALY_CATASTROPHE_VETO",
                "ANOMALY_FUNDING_BRAKE", "RISK_CIRCUIT_BREAKER",
                "RISK_DRAWDOWN_ZOMBIE", "RISK_GLOBAL_DRAWDOWN",
                "RISK_DAILY_LOSS_HALT",
            }
            type_stats = []
            for rtype, sub in df.groupby("exit_reason_type", sort=False):
                if pd.isna(rtype):
                    continue
                n, wins, total, avg = _group_stats(sub)
                wr_pct = wins / n * 100 if n > 0 else 0.0
                type_stats.append((str(rtype), n, wins, wr_pct, total, avg))
            # Sort by absolute total PnL ascending so worst first
            type_stats.sort(key=lambda x: x[4])

            # Handle NULL exit_reason_type rows
            null_rt_mask = df["exit_reason_type"].isna()
            if null_rt_mask.any():
                sub_null = df[null_rt_mask]
                n, wins, total, avg = _group_stats(sub_null)
                wr_pct = wins / n * 100 if n > 0 else 0.0
                type_stats.append(("UNATTRIBUTED", n, wins, wr_pct, total, avg))

            if type_stats:
                type_rows_html = ""
                for rtype, n, wins, wr_pct, total, avg in type_stats:
                    losses = n - wins
                    is_emerg = rtype in _EMERG_TYPES
                    label_col = T["red"] if is_emerg else T["text"]
                    type_rows_html += (
                        f'<tr>'
                        f'<td style="color:{label_col};font-weight:{"700" if is_emerg else "400"};'
                        f'font-size:0.62rem;">{_esc(rtype)}</td>'
                        f'<td style="color:{T["text2"]};">{n}</td>'
                        f'<td style="color:{_wr_col(wins, n)};font-weight:700;">{wr_pct:.0f}%'
                        f'<span style="color:{T["muted"]};font-weight:400;font-size:0.60rem;"> '
                        f'{wins}W/{losses}L</span></td>'
                        f'<td style="color:{_pnl_col(total)};font-weight:700;">${total:+,.2f}</td>'
                        f'<td style="color:{_pnl_col(avg)};">${avg:+,.2f}</td>'
                        f'</tr>'
                    )
                type_table_html = (
                    f'<div class="ot-tile ot-scroll" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
                    f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
                    f'BY EXIT REASON TYPE — mechanism detail (worst first)</div>'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>REASON TYPE</th><th>TRADES</th><th>WIN RATE</th>'
                    f'<th>TOTAL PnL</th><th>AVG PnL</th>'
                    f'</tr></thead><tbody>{type_rows_html}</tbody></table></div>'
                )
                st.markdown(type_table_html, unsafe_allow_html=True)
        except Exception as _exc:
            log.debug("[P6-DIAG] reason-type groupby failed: %s", _exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
            f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
            f'BY EXIT REASON TYPE</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;padding:6px 0;">'
            f'Not yet available — requires Phase 4 trade attribution data.</div></div>',
            unsafe_allow_html=True,
        )

    # ── 5. [PHASE6] Entry quality: winner vs loser comparison ────────────────
    # Answers: are losing trades entered on weaker signals than winning trades?
    # If loser avg confidence ≈ winner avg confidence → exit quality problem.
    # If loser avg confidence << winner avg confidence → overtrading / weak entry.
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    if has_p6_quality and "realized_usd" in df.columns:
        try:
            q_df = df[df["entry_confidence"].notna()].copy()
            if len(q_df) >= 4:
                win_mask  = q_df["realized_usd"] > 0
                lose_mask = q_df["realized_usd"] <= 0

                def _avg(series: "pd.Series") -> Optional[float]:
                    v = series.dropna()
                    return float(v.mean()) if len(v) > 0 else None

                w_conf = _avg(q_df.loc[win_mask,  "entry_confidence"])
                l_conf = _avg(q_df.loc[lose_mask, "entry_confidence"])
                w_kf   = _avg(q_df.loc[win_mask,  "entry_kelly_f"])
                l_kf   = _avg(q_df.loc[lose_mask, "entry_kelly_f"])
                w_n    = int(win_mask.sum())
                l_n    = int(lose_mask.sum())

                def _fmt_val(v: Optional[float], fmt: str = ".3f") -> str:
                    return f"{v:{fmt}}" if v is not None else "—"

                def _delta_col(w: Optional[float], l: Optional[float]) -> str:
                    """Colour the loser value: red if meaningfully below winner."""
                    if w is None or l is None:
                        return T["muted"]
                    return T["red"] if (w - l) > 0.05 * max(abs(w), 1e-9) else T["green"]

                quality_html = (
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
                    f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
                    f'ENTRY QUALITY — WINNERS vs LOSERS'
                    f'<span style="color:{T["muted"]};font-weight:400;"> '
                    f'({len(q_df)} Phase-6 rows)</span></div>'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>METRIC</th>'
                    f'<th style="color:{T["green"]};">WINNERS ({w_n})</th>'
                    f'<th style="color:{T["red"]};">LOSERS ({l_n})</th>'
                    f'</tr></thead><tbody>'
                    f'<tr><td style="color:{T["muted"]};">AVG CONFIDENCE</td>'
                    f'<td style="color:{T["green"]};font-weight:700;">{_fmt_val(w_conf)}</td>'
                    f'<td style="color:{_delta_col(w_conf, l_conf)};font-weight:700;">'
                    f'{_fmt_val(l_conf)}</td></tr>'
                    f'<tr><td style="color:{T["muted"]};">AVG KELLY F</td>'
                    f'<td style="color:{T["green"]};font-weight:700;">{_fmt_val(w_kf)}</td>'
                    f'<td style="color:{_delta_col(w_kf, l_kf)};font-weight:700;">'
                    f'{_fmt_val(l_kf)}</td></tr>'
                    f'</tbody></table>'
                )
                # Diagnostic interpretation hint
                if w_conf is not None and l_conf is not None:
                    gap = w_conf - l_conf
                    if gap > 0.10:
                        quality_html += (
                            f'<div style="font-family:{T["mono"]};font-size:0.62rem;'
                            f'color:{T["red"]};margin-top:4px;">'
                            f'⚠ Winner confidence {gap:.3f} higher than loser — '
                            f'weak-entry overtrading likely.</div>'
                        )
                    elif gap < 0.02:
                        quality_html += (
                            f'<div style="font-family:{T["mono"]};font-size:0.62rem;'
                            f'color:{T["yellow"]};margin-top:4px;">'
                            f'Entry quality similar — losses more likely driven by '
                            f'exit quality or external conditions.</div>'
                        )
                quality_html += f'</div>'
                st.markdown(quality_html, unsafe_allow_html=True)
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
                    f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
                    f'ENTRY QUALITY — WINNERS vs LOSERS</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'Insufficient Phase-6 data ({len(q_df)} rows — need ≥4).</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _exc:
            log.debug("[P6-DIAG] entry quality panel failed: %s", _exc)
    else:
        _p6_note = (
            "No Phase-6 data yet — entry_confidence/entry_kelly_f not yet present in DB."
            if not has_p6_quality else
            "realized_usd column missing — cannot split winners/losers."
        )
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
            f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
            f'ENTRY QUALITY — WINNERS vs LOSERS</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'{_esc(_p6_note)}</div></div>',
            unsafe_allow_html=True,
        )

    # ── 6. [PHASE6] Hold time by exit category ───────────────────────────────
    # Answers: are emergency exits cutting trades very short (< 60s)?
    # Short ANOMALY hold = emergency flush / friction-dominated loss.
    # Long STRATEGY hold with loss = thesis failure / adverse drift.
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    if has_p6_hold and has_categories:
        try:
            h_df = df[df["hold_time_secs"].notna() & df["exit_reason_category"].notna()].copy()
            if len(h_df) >= 2:
                hold_rows_html = ""
                for cat in _CAT_ORDER:
                    sub = h_df[h_df["exit_reason_category"] == cat]
                    if len(sub) == 0:
                        continue
                    hold_vals = sub["hold_time_secs"].dropna()
                    if len(hold_vals) == 0:
                        continue
                    med_s = float(hold_vals.median())
                    avg_s = float(hold_vals.mean())
                    n_sub = len(hold_vals)
                    # Format: show seconds and hours
                    med_str = (
                        f"{med_s:.0f}s" if med_s < 120
                        else f"{med_s/3600:.2f}h"
                    )
                    avg_str = (
                        f"{avg_s:.0f}s" if avg_s < 120
                        else f"{avg_s/3600:.2f}h"
                    )
                    # Flag very short emergency holds
                    is_short_emerg = (
                        cat in ("RISK", "ANOMALY") and med_s < 120
                    )
                    med_col = T["red"] if is_short_emerg else T["text"]
                    label_col = _CAT_COLORS.get(cat, T["muted"])
                    hold_rows_html += (
                        f'<tr>'
                        f'<td style="color:{label_col};font-weight:700;">{_esc(cat)}</td>'
                        f'<td style="color:{T["text2"]};">{n_sub}</td>'
                        f'<td style="color:{med_col};font-weight:700;">{_esc(med_str)}</td>'
                        f'<td style="color:{T["muted"]};">{_esc(avg_str)}</td>'
                        f'</tr>'
                    )
                if hold_rows_html:
                    p6_row_count = len(h_df)
                    hold_table_html = (
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
                        f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
                        f'HOLD TIME BY EXIT CATEGORY'
                        f'<span style="color:{T["muted"]};font-weight:400;"> '
                        f'({p6_row_count} Phase-6 rows)</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>CATEGORY</th><th>TRADES</th>'
                        f'<th>MEDIAN HOLD</th><th>AVG HOLD</th>'
                        f'</tr></thead><tbody>{hold_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'Red median = emergency exit with very short hold (≤ 120s).</div>'
                        f'</div>'
                    )
                    st.markdown(hold_table_html, unsafe_allow_html=True)
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'HOLD TIME — No matching rows.</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
                    f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
                    f'HOLD TIME BY EXIT CATEGORY</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'Insufficient Phase-6 data ({len(h_df)} rows — need ≥2).</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _exc:
            log.debug("[P6-DIAG] hold-time panel failed: %s", _exc)
    else:
        _hold_note = (
            "No Phase-6 data yet — hold_time_secs not yet present in DB."
            if not has_p6_hold else
            "Exit categories not yet attributed — requires Phase 4 trade data."
        )
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
            f'text-transform:uppercase;color:{T["text2"]};margin-bottom:6px;">'
            f'HOLD TIME BY EXIT CATEGORY</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'{_esc(_hold_note)}</div></div>',
            unsafe_allow_html=True,
        )

    # ── 7. [PHASE10] Close-leg slippage by symbol ────────────────────────────
    if has_p10_friction:
        try:
            slip_df = df[df["close_slippage_bps"].notna()].copy()
            # Require at least 3 Phase-10 rows per symbol for a meaningful stat.
            sym_counts = slip_df.groupby("symbol")["close_slippage_bps"].count()
            syms_ok = sym_counts[sym_counts >= 3].index.tolist()
            slip_df_ok = slip_df[slip_df["symbol"].isin(syms_ok)]
            if len(slip_df_ok) >= 1:
                slip_rows_html = ""
                sym_stats = (
                    slip_df_ok.groupby("symbol")["close_slippage_bps"]
                    .agg(n="count", avg="mean", med="median")
                    .reset_index()
                    .sort_values("avg", ascending=False)   # worst first
                )
                for _, row in sym_stats.iterrows():
                    sym_val  = str(row["symbol"])
                    avg_val  = float(row["avg"])
                    med_val  = float(row["med"])
                    n_val    = int(row["n"])
                    # Flag symbols where average slippage exceeds 5 bps (adverse).
                    is_warn  = avg_val > 5.0
                    avg_col  = T["red"] if avg_val > 5.0 else T["yellow"] if avg_val > 2.0 else T["green"]
                    med_col  = T["red"] if med_val > 5.0 else T["yellow"] if med_val > 2.0 else T["green"]
                    flag_str = " ⚠" if is_warn else ""
                    slip_rows_html += (
                        f'<tr>'
                        f'<td style="color:{T["text"]};font-weight:700;">'
                        f'{_esc(sym_val)}{_esc(flag_str)}</td>'
                        f'<td style="color:{T["text2"]};">{n_val}</td>'
                        f'<td style="color:{avg_col};font-weight:700;">'
                        f'{avg_val:+.2f}</td>'
                        f'<td style="color:{med_col};">{med_val:+.2f}</td>'
                        f'</tr>'
                    )
                p10_row_count = len(slip_df)
                slip_table_html = (
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'CLOSE-LEG SLIPPAGE BY SYMBOL'
                    f'<span style="color:{T["muted"]};font-weight:400;"> '
                    f'({p10_row_count} Phase-10 rows)</span></div>'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>SYMBOL</th><th>TRADES</th>'
                    f'<th>AVG SLIP (bps)</th><th>MEDIAN SLIP (bps)</th>'
                    f'</tr></thead><tbody>{slip_rows_html}</tbody></table>'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'color:{T["muted"]};margin-top:4px;">'
                    f'Positive = adverse (fill worse than expected bid/ask). '
                    f'⚠ = avg &gt; 5 bps. '
                    f'Positive = adverse (fill worse than expected bid/ask). ⚠ = avg &gt; 5 bps.'
                    f'</div></div>'
                )
                st.markdown(slip_table_html, unsafe_allow_html=True)
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'CLOSE-LEG SLIPPAGE BY SYMBOL</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
                    f'font-size:0.70rem;">'
                    f'Insufficient Phase-10 data — need ≥3 rows per symbol.</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        except Exception as _p10s_exc:
            log.debug("[PHASE10] slippage-by-symbol panel failed: %s", _p10s_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'CLOSE-LEG SLIPPAGE BY SYMBOL</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
            f'font-size:0.70rem;">'
            f'No Phase-10 friction data yet — close_slippage_bps not yet '
            f'present in DB.</div></div>',
            unsafe_allow_html=True,
        )

    # ── 8. [PHASE10] Execution friction by exit category ─────────────────────
    if has_p10_friction and has_categories:
        try:
            fric_df = df[
                df["close_slippage_bps"].notna() &
                df["exit_reason_category"].notna()
            ].copy()
            if len(fric_df) >= 1:
                fric_rows_html = ""
                for cat in _CAT_ORDER:
                    sub = fric_df[fric_df["exit_reason_category"] == cat]
                    if len(sub) == 0:
                        continue
                    slip_vals   = sub["close_slippage_bps"].dropna()
                    spread_vals = sub["spread_at_close_bps"].dropna() \
                                  if "spread_at_close_bps" in sub.columns else pd.Series([], dtype=float)
                    if len(slip_vals) == 0:
                        continue
                    avg_slip    = float(slip_vals.mean())
                    avg_spread  = float(spread_vals.mean()) if len(spread_vals) > 0 else float("nan")
                    n_sub       = len(slip_vals)
                    slip_col    = T["red"] if avg_slip > 5.0 else T["yellow"] if avg_slip > 2.0 else T["green"]
                    label_col   = _CAT_COLORS.get(cat, T["muted"])
                    spread_str  = f"{avg_spread:.2f}" if not __import__('math').isnan(avg_spread) else "—"
                    fric_rows_html += (
                        f'<tr>'
                        f'<td style="color:{label_col};font-weight:700;">{_esc(cat)}</td>'
                        f'<td style="color:{T["text2"]};">{n_sub}</td>'
                        f'<td style="color:{slip_col};font-weight:700;">{avg_slip:+.2f}</td>'
                        f'<td style="color:{T["muted"]};">{_esc(spread_str)}</td>'
                        f'</tr>'
                    )
                if fric_rows_html:
                    p10_fric_count = len(fric_df)
                    fric_table_html = (
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'FRICTION BY EXIT CATEGORY'
                        f'<span style="color:{T["muted"]};font-weight:400;"> '
                        f'({p10_fric_count} Phase-10 rows)</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>CATEGORY</th><th>TRADES</th>'
                        f'<th>AVG SLIP (bps)</th><th>AVG SPREAD (bps)</th>'
                        f'</tr></thead><tbody>{fric_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'Positive slip = adverse fill. '
                        f'ANOMALY/RISK exits typically use market orders — '
                        f'expect higher friction than STRATEGY exits.'
                        f'</div></div>'
                    )
                    st.markdown(fric_table_html, unsafe_allow_html=True)
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'FRICTION BY EXIT CATEGORY — No matching rows.</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'FRICTION BY EXIT CATEGORY</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
                    f'font-size:0.70rem;">'
                    f'Insufficient Phase-10 data ({len(fric_df)} rows — need ≥1).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _p10f_exc:
            log.debug("[PHASE10] friction-by-category panel failed: %s", _p10f_exc)
    else:
        _fric_note = (
            "No Phase-10 friction data yet — close_slippage_bps not yet present in DB."
            if not has_p10_friction else
            "Exit categories not yet attributed — requires Phase 4 trade data."
        )
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'FRICTION BY EXIT CATEGORY</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
            f'font-size:0.70rem;">'
            f'{_esc(_fric_note)}</div></div>',
            unsafe_allow_html=True,
        )

    # ── 9. [POST-ROADMAP-DT] PnL / win-rate by entry regime × direction ────────
    if has_entry_regime:
        try:
            rd_df = df[
                df["entry_regime"].notna() & df["entry_direction"].notna()
            ].copy()
            if len(rd_df) >= 1:
                # Build one row per (regime, direction) combination.
                rd_rows_html = ""
                # Sort groups: bull before bear before chop, long before short.
                _regime_order    = ["bull", "bear", "chop"]
                _direction_order = ["long", "short"]
                seen_groups = set()
                ordered_groups = []
                for r in _regime_order:
                    for d in _direction_order:
                        ordered_groups.append((r, d))
                        seen_groups.add((r, d))
                # Append any observed combinations not in the canonical order.
                for r, d in rd_df.groupby(
                    ["entry_regime", "entry_direction"], sort=True
                ).groups.keys():
                    if (r, d) not in seen_groups:
                        ordered_groups.append((r, d))

                for regime, direction in ordered_groups:
                    sub = rd_df[
                        (rd_df["entry_regime"] == regime) &
                        (rd_df["entry_direction"] == direction)
                    ]
                    if len(sub) == 0:
                        continue
                    n, wins, total_pnl, avg_pnl = _group_stats(sub)
                    wr_pct = wins / n * 100 if n > 0 else 0.0
                    regime_col = (
                        T["green"] if regime == "bull" else
                        T["red"]   if regime == "bear" else
                        T["muted"]
                    )
                    dir_col = T["cyan"] if direction == "long" else T["yellow"]
                    pnl_col = _pnl_col(total_pnl)
                    wr_col  = _wr_col(wins, n)
                    rd_rows_html += (
                        f'<tr>'
                        f'<td style="color:{regime_col};font-weight:700;">'
                        f'{_esc(regime.upper())}</td>'
                        f'<td style="color:{dir_col};">{_esc(direction.upper())}</td>'
                        f'<td style="color:{T["text2"]};">{n}</td>'
                        f'<td style="color:{wr_col};font-weight:700;">'
                        f'{wr_pct:.0f}%</td>'
                        f'<td style="color:{pnl_col};font-weight:700;">'
                        f'${total_pnl:+,.2f}</td>'
                        f'<td style="color:{_pnl_col(avg_pnl)};">'
                        f'${avg_pnl:+,.2f}</td>'
                        f'</tr>'
                    )
                if rd_rows_html:
                    rd_count = len(rd_df)
                    rd_table_html = (
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'PnL BY ENTRY REGIME × DIRECTION'
                        f'<span style="color:{T["muted"]};font-weight:400;"> '
                        f'({rd_count} DT rows)</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>REGIME</th><th>DIRECTION</th><th>TRADES</th>'
                        f'<th>WIN RATE</th><th>TOTAL PnL</th><th>AVG PnL</th>'
                        f'</tr></thead><tbody>{rd_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'Regime and direction as recorded at position entry. '
                        f'Historical rows (pre-DT schema) carry NULL and are excluded.'
                        f'</div></div>'
                    )
                    st.markdown(rd_table_html, unsafe_allow_html=True)
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'ENTRY REGIME × DIRECTION — No matching rows.</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'PnL BY ENTRY REGIME × DIRECTION</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
                    f'font-size:0.70rem;">'
                    f'Insufficient DT data ({len(rd_df)} rows — need ≥1).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _dt_rd_exc:
            log.debug("[POST-ROADMAP-DT] regime×direction panel failed: %s", _dt_rd_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'PnL BY ENTRY REGIME × DIRECTION</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
            f'font-size:0.70rem;">'
            f'No entry-context data yet — entry_regime / entry_direction not yet '
            f'present in DB, or no trades closed since migration.</div></div>',
            unsafe_allow_html=True,
        )

    # ── 10. [TRACK03-SF1] PnL / win-rate by entry confidence tier ──────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    _has_z_score = (
        "entry_z_score" in df.columns
        and df["entry_z_score"].notna().any()
    )

    if has_p6_quality and "realized_usd" in df.columns:
        try:
            cf_df = df[df["entry_confidence"].notna()].copy()
            if len(cf_df) >= 5:
                # Bucket definitions: HIGH / MID / LOW
                _CONF_TIERS = [
                    ("HIGH",  0.75, 1.01, T["green"]),
                    ("MID",   0.50, 0.75, T["yellow"]),
                    ("LOW",   0.00, 0.50, T["red"]),
                ]
                cf_rows_html = ""
                for tier_label, lo, hi, tier_col in _CONF_TIERS:
                    sub = cf_df[
                        (cf_df["entry_confidence"] >= lo) &
                        (cf_df["entry_confidence"] <  hi)
                    ]
                    if len(sub) == 0:
                        continue
                    n, wins, total_pnl, avg_pnl = _group_stats(sub)
                    wr_pct = wins / n * 100 if n > 0 else 0.0
                    cf_rows_html += (
                        f'<tr>'
                        f'<td style="color:{tier_col};font-weight:700;font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{_esc(tier_label)}</td>'
                        f'<td style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.63rem;">'
                        f'{lo:.2f}–{min(hi, 1.0):.2f}</td>'
                        f'<td style="color:{T["text2"]};">{n}</td>'
                        f'<td style="color:{_wr_col(wins, n)};font-weight:700;">{wr_pct:.0f}%</td>'
                        f'<td style="color:{_pnl_col(total_pnl)};font-weight:700;">'
                        f'${total_pnl:+,.2f}</td>'
                        f'<td style="color:{_pnl_col(avg_pnl)};">${avg_pnl:+,.2f}</td>'
                        f'</tr>'
                    )
                if cf_rows_html:
                    n_cf = len(cf_df)
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'PnL BY ENTRY CONFIDENCE TIER'
                        f'<span style="color:{T["muted"]};font-weight:400;"> ({n_cf} rows)</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>TIER</th><th>RANGE</th><th>TRADES</th>'
                        f'<th>WIN RATE</th><th>TOTAL PnL</th><th>AVG PnL</th>'
                        f'</tr></thead><tbody>{cf_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'HIGH ≥0.75 · MID 0.50–0.75 · LOW <0.50. '
                        f'Confidence as recorded at position entry.</div></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'CONFIDENCE TIER — No rows matched tier buckets.</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'PnL BY ENTRY CONFIDENCE TIER</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'Insufficient data ({len(cf_df)} rows with confidence — need ≥5).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _cf_exc:
            log.debug("[TRACK03-SF1] confidence tier panel failed: %s", _cf_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'PnL BY ENTRY CONFIDENCE TIER</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'No entry-confidence data yet — requires Phase 6 schema or later.'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── 11. [TRACK03-SF2] PnL / win-rate by entry z_score tier ─────────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    if _has_z_score and "realized_usd" in df.columns:
        try:
            zs_df = df[df["entry_z_score"].notna()].copy()
            if len(zs_df) >= 5:
                # Bucket definitions: STRONG / MID / WEAK
                _Z_TIERS = [
                    ("STRONG", 1.5,  999.0, T["green"]),
                    ("MID",    0.8,  1.5,   T["yellow"]),
                    ("WEAK",   0.0,  0.8,   T["red"]),
                ]
                zs_rows_html = ""
                for tier_label, lo, hi, tier_col in _Z_TIERS:
                    sub = zs_df[
                        (zs_df["entry_z_score"].abs() >= lo) &
                        (zs_df["entry_z_score"].abs() <  hi)
                    ]
                    if len(sub) == 0:
                        continue
                    n, wins, total_pnl, avg_pnl = _group_stats(sub)
                    wr_pct = wins / n * 100 if n > 0 else 0.0
                    hi_disp = "∞" if hi >= 999 else f"{hi:.1f}"
                    zs_rows_html += (
                        f'<tr>'
                        f'<td style="color:{tier_col};font-weight:700;font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{_esc(tier_label)}</td>'
                        f'<td style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.63rem;">'
                        f'|z|≥{lo:.1f}–{hi_disp}</td>'
                        f'<td style="color:{T["text2"]};">{n}</td>'
                        f'<td style="color:{_wr_col(wins, n)};font-weight:700;">{wr_pct:.0f}%</td>'
                        f'<td style="color:{_pnl_col(total_pnl)};font-weight:700;">'
                        f'${total_pnl:+,.2f}</td>'
                        f'<td style="color:{_pnl_col(avg_pnl)};">${avg_pnl:+,.2f}</td>'
                        f'</tr>'
                    )
                if zs_rows_html:
                    n_zs = len(zs_df)
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'PnL BY ENTRY Z-SCORE TIER'
                        f'<span style="color:{T["muted"]};font-weight:400;"> ({n_zs} rows)</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>TIER</th><th>|Z| RANGE</th><th>TRADES</th>'
                        f'<th>WIN RATE</th><th>TOTAL PnL</th><th>AVG PnL</th>'
                        f'</tr></thead><tbody>{zs_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'STRONG |z|≥1.5 · MID 0.8–1.5 · WEAK |z|<0.8. '
                        f'Uses absolute z_score; sign reflects direction at entry.</div></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'Z-SCORE TIER — No rows matched tier buckets.</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'PnL BY ENTRY Z-SCORE TIER</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'Insufficient data ({len(zs_df)} rows with z_score — need ≥5).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _zs_exc:
            log.debug("[TRACK03-SF2] z_score tier panel failed: %s", _zs_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'PnL BY ENTRY Z-SCORE TIER</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'No z_score data yet — requires Track 01 entry-context schema or later.'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── 12. [TRACK03-SF3] PnL / win-rate by symbol × entry regime ───────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    if has_entry_regime and "realized_usd" in df.columns:
        try:
            sr_df = df[
                df["entry_regime"].notna() & df["symbol"].notna()
            ].copy()
            if len(sr_df) >= 1:
                _regime_order = {"bull": 0, "bear": 1, "chop": 2}
                sr_df["_regime_ord"] = (
                    sr_df["entry_regime"].str.lower()
                    .map(_regime_order)
                    .fillna(9)
                    .astype(int)
                )
                sr_df["_sym_norm"] = sr_df["symbol"].apply(
                    lambda s: _norm_sym(str(s)) if pd.notna(s) else "?"
                )
                sr_groups = (
                    sr_df.sort_values(["_sym_norm", "_regime_ord"])
                    .groupby(["_sym_norm", "entry_regime"], sort=False)
                )
                sr_rows_html = ""
                row_count = 0
                _MAX_SR_ROWS = 30
                for (sym, regime), sub in sr_groups:
                    if row_count >= _MAX_SR_ROWS:
                        break
                    n, wins, total_pnl, avg_pnl = _group_stats(sub)
                    wr_pct    = wins / n * 100 if n > 0 else 0.0
                    regime_u  = str(regime).upper()
                    regime_col = (
                        T["green"] if regime_u == "BULL" else
                        T["red"]   if regime_u == "BEAR" else
                        T["muted"]
                    )
                    sr_rows_html += (
                        f'<tr>'
                        f'<td style="color:{T["text"]};font-weight:700;">{_esc(sym)}</td>'
                        f'<td style="color:{regime_col};font-weight:700;font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{_esc(regime_u)}</td>'
                        f'<td style="color:{T["text2"]};">{n}</td>'
                        f'<td style="color:{_wr_col(wins, n)};font-weight:700;">{wr_pct:.0f}%</td>'
                        f'<td style="color:{_pnl_col(total_pnl)};font-weight:700;">'
                        f'${total_pnl:+,.2f}</td>'
                        f'<td style="color:{_pnl_col(avg_pnl)};">${avg_pnl:+,.2f}</td>'
                        f'</tr>'
                    )
                    row_count += 1
                if sr_rows_html:
                    _cap_note = (
                        f" (first {_MAX_SR_ROWS} rows shown)"
                        if row_count >= _MAX_SR_ROWS else ""
                    )
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'PnL BY SYMBOL × ENTRY REGIME'
                        f'<span style="color:{T["muted"]};font-weight:400;">{_esc(_cap_note)}</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>SYMBOL</th><th>REGIME</th><th>TRADES</th>'
                        f'<th>WIN RATE</th><th>TOTAL PnL</th><th>AVG PnL</th>'
                        f'</tr></thead><tbody>{sr_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'Entry regime as recorded at position open. '
                        f'Historical rows (pre-Track-01 schema) carry NULL and are excluded.</div></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'SYMBOL × REGIME — No matching rows.</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'PnL BY SYMBOL × ENTRY REGIME</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'Insufficient data ({len(sr_df)} rows — need ≥1 with regime and symbol).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _sr_exc:
            log.debug("[TRACK03-SF3] symbol×regime panel failed: %s", _sr_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'PnL BY SYMBOL × ENTRY REGIME</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'No entry-regime data yet — requires Track 01 entry-context schema or later.'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── 13. [TRACK05-SF4] PnL / win-rate by symbol × entry regime + friction ──
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    # Guard: need both entry_regime and at least one friction column with data.
    _has_sf4_friction = (
        has_entry_regime
        and has_p10_friction
        and "realized_usd" in df.columns
    )

    if _has_sf4_friction:
        try:
            sf4_df = df[
                df["entry_regime"].notna() & df["symbol"].notna()
            ].copy()
            if len(sf4_df) >= 1:
                _regime_order_sf4 = {"bull": 0, "bear": 1, "chop": 2}
                sf4_df["_regime_ord"] = (
                    sf4_df["entry_regime"].str.lower()
                    .map(_regime_order_sf4)
                    .fillna(9)
                    .astype(int)
                )
                sf4_df["_sym_norm"] = sf4_df["symbol"].apply(
                    lambda s: _norm_sym(str(s)) if pd.notna(s) else "?"
                )
                sf4_groups = (
                    sf4_df.sort_values(["_sym_norm", "_regime_ord"])
                    .groupby(["_sym_norm", "entry_regime"], sort=False)
                )
                sf4_rows_html = ""
                sf4_row_count = 0
                _MAX_SF4_ROWS = 30
                for (sym, regime), sub in sf4_groups:
                    if sf4_row_count >= _MAX_SF4_ROWS:
                        break
                    n, wins, total_pnl, avg_pnl = _group_stats(sub)
                    wr_pct = wins / n * 100 if n > 0 else 0.0
                    regime_u   = str(regime).upper()
                    regime_col = (
                        T["green"] if regime_u == "BULL" else
                        T["red"]   if regime_u == "BEAR" else
                        T["muted"]
                    )
                    # Friction: mean of non-null values for this cell only.
                    slip_vals = (
                        sub["close_slippage_bps"].dropna()
                        if "close_slippage_bps" in sub.columns else pd.Series([], dtype=float)
                    )
                    sprd_vals = (
                        sub["spread_at_close_bps"].dropna()
                        if "spread_at_close_bps" in sub.columns else pd.Series([], dtype=float)
                    )
                    slip_str = (
                        f"{slip_vals.mean():+.1f}" if len(slip_vals) > 0 else "—"
                    )
                    sprd_str = (
                        f"{sprd_vals.mean():.1f}" if len(sprd_vals) > 0 else "—"
                    )
                    # Colour slip: positive bps = adverse = red; near-zero = muted.
                    slip_col = (
                        T["red"]   if len(slip_vals) > 0 and slip_vals.mean() > 2.0 else
                        T["yellow"] if len(slip_vals) > 0 and slip_vals.mean() > 0.5 else
                        T["muted"]
                    )
                    sf4_rows_html += (
                        f'<tr>'
                        f'<td style="color:{T["text"]};font-weight:700;">{_esc(sym)}</td>'
                        f'<td style="color:{regime_col};font-weight:700;font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{_esc(regime_u)}</td>'
                        f'<td style="color:{T["text2"]};">{n}</td>'
                        f'<td style="color:{_wr_col(wins, n)};font-weight:700;">{wr_pct:.0f}%</td>'
                        f'<td style="color:{_pnl_col(total_pnl)};font-weight:700;">'
                        f'${total_pnl:+,.2f}</td>'
                        f'<td style="color:{_pnl_col(avg_pnl)};">${avg_pnl:+,.2f}</td>'
                        f'<td style="color:{slip_col};font-family:{T["mono"]};font-size:0.63rem;">'
                        f'{_esc(slip_str)}</td>'
                        f'<td style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.63rem;">'
                        f'{_esc(sprd_str)}</td>'
                        f'</tr>'
                    )
                    sf4_row_count += 1
                if sf4_rows_html:
                    _cap_note_sf4 = (
                        f" (first {_MAX_SF4_ROWS} rows shown)"
                        if sf4_row_count >= _MAX_SF4_ROWS else ""
                    )
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'PnL BY SYMBOL × ENTRY REGIME + FRICTION'
                        f'<span style="color:{T["muted"]};font-weight:400;">'
                        f'{_esc(_cap_note_sf4)}</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>SYMBOL</th><th>REGIME</th><th>TRADES</th>'
                        f'<th>WIN RATE</th><th>TOTAL PnL</th><th>AVG PnL</th>'
                        f'<th>AVG SLIP bps</th><th>AVG SPRD bps</th>'
                        f'</tr></thead><tbody>{sf4_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'Slip = avg close_slippage_bps (+ = adverse). '
                        f'Sprd = avg spread_at_close_bps. '
                        f'— = no friction data for that cell (pre-Phase-10 or HEDGE_TRIM rows). '
                        f'Entry regime as recorded at position open.</div></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'SYMBOL × REGIME + FRICTION — No matching rows.</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'PnL BY SYMBOL × ENTRY REGIME + FRICTION</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'Insufficient data ({len(sf4_df)} rows — need ≥1 with regime and symbol).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _sf4_exc:
            log.debug("[TRACK05-SF4] symbol×regime+friction panel failed: %s", _sf4_exc)
    else:
        # Explain whichever guard is not yet satisfied.
        if not has_entry_regime:
            _sf4_reason = (
                "No entry-regime data yet — requires Track 01 entry-context schema or later."
            )
        elif not has_p10_friction:
            _sf4_reason = (
                "No friction data yet — requires Phase 10 schema. "
                "close_slippage_bps / spread_at_close_bps not yet present in DB."
            )
        else:
            _sf4_reason = "Insufficient data for symbol × regime + friction panel."
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'PnL BY SYMBOL × ENTRY REGIME + FRICTION</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'{_esc(_sf4_reason)}</div></div>',
            unsafe_allow_html=True,
        )

    # ── 14. [TRACK05-SF5] PnL / win-rate by z_score tier × direction ──────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    # Guard: need both z_score and direction columns with data.
    _has_sf5 = (
        _has_z_score
        and has_entry_regime   # entry_direction is present whenever entry_regime is
        and "entry_direction" in df.columns
        and df["entry_direction"].notna().any()
        and "realized_usd" in df.columns
    )

    if _has_sf5:
        try:
            sf5_df = df[
                df["entry_z_score"].notna() & df["entry_direction"].notna()
            ].copy()
            if len(sf5_df) >= 3:
                # Tier definitions (same thresholds as SF2, using |z| absolute value).
                _Z5_TIERS = [
                    ("STRONG", 1.5,  999.0, T["green"]),
                    ("MID",    0.8,  1.5,   T["yellow"]),
                    ("WEAK",   0.0,  0.8,   T["red"]),
                ]
                _DIR_ORDER = ["long", "short"]
                sf5_rows_html = ""
                for tier_label, lo, hi, tier_col in _Z5_TIERS:
                    tier_mask = (
                        (sf5_df["entry_z_score"].abs() >= lo) &
                        (sf5_df["entry_z_score"].abs() <  hi)
                    )
                    tier_sub = sf5_df[tier_mask]
                    if len(tier_sub) == 0:
                        continue
                    hi_disp = "∞" if hi >= 999 else f"{hi:.1f}"
                    for direction in _DIR_ORDER:
                        dir_sub = tier_sub[
                            tier_sub["entry_direction"].str.lower() == direction
                        ]
                        if len(dir_sub) == 0:
                            continue
                        n, wins, total_pnl, avg_pnl = _group_stats(dir_sub)
                        wr_pct  = wins / n * 100 if n > 0 else 0.0
                        dir_col = T["cyan"] if direction == "long" else T["yellow"]
                        sf5_rows_html += (
                            f'<tr>'
                            f'<td style="color:{tier_col};font-weight:700;'
                            f'font-family:{T["mono"]};font-size:0.65rem;">'
                            f'{_esc(tier_label)}</td>'
                            f'<td style="color:{T["muted"]};font-family:{T["mono"]};'
                            f'font-size:0.63rem;">|z|≥{lo:.1f}–{hi_disp}</td>'
                            f'<td style="color:{dir_col};">{_esc(direction.upper())}</td>'
                            f'<td style="color:{T["text2"]};">{n}</td>'
                            f'<td style="color:{_wr_col(wins, n)};font-weight:700;">'
                            f'{wr_pct:.0f}%</td>'
                            f'<td style="color:{_pnl_col(total_pnl)};font-weight:700;">'
                            f'${total_pnl:+,.2f}</td>'
                            f'<td style="color:{_pnl_col(avg_pnl)};">'
                            f'${avg_pnl:+,.2f}</td>'
                            f'</tr>'
                        )
                if sf5_rows_html:
                    n_sf5 = len(sf5_df)
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'PnL BY Z-SCORE TIER × DIRECTION'
                        f'<span style="color:{T["muted"]};font-weight:400;"> '
                        f'({n_sf5} rows)</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>TIER</th><th>|Z| RANGE</th><th>DIRECTION</th>'
                        f'<th>TRADES</th><th>WIN RATE</th>'
                        f'<th>TOTAL PnL</th><th>AVG PnL</th>'
                        f'</tr></thead><tbody>{sf5_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'STRONG |z|≥1.5 · MID 0.8–1.5 · WEAK |z|<0.8. '
                        f'Uses absolute z_score; sign reflects direction at entry. '
                        f'Direction as recorded at position open.</div></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                        f'font-family:{T["mono"]};font-size:0.70rem;">'
                        f'Z-SCORE TIER × DIRECTION — No rows matched tier/direction buckets.'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'PnL BY Z-SCORE TIER × DIRECTION</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'Insufficient data ({len(sf5_df)} rows with z_score + direction — need ≥3).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _sf5_exc:
            log.debug("[TRACK05-SF5] z_score tier × direction panel failed: %s", _sf5_exc)
    else:
        if not _has_z_score:
            _sf5_reason = (
                "No z_score data yet — requires Track 01 entry-context schema or later."
            )
        elif not (
            "entry_direction" in df.columns and df["entry_direction"].notna().any()
        ):
            _sf5_reason = (
                "No entry_direction data yet — requires Track 01 entry-context schema or later."
            )
        else:
            _sf5_reason = "Insufficient data for z_score tier × direction panel."
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'PnL BY Z-SCORE TIER × DIRECTION</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'{_esc(_sf5_reason)}</div></div>',
            unsafe_allow_html=True,
        )

    # ── 15. [TRACK06-SF6] Setup evidence summary ─────────────────────────────
    # Ranked table of (symbol, entry_regime, entry_direction) cells with
    # per-cell trade count, win rate, total PnL, avg PnL, and an evidence
    # label.  Sorted by avg PnL descending.  Read-only; no writeback.
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    # Label thresholds — purely descriptive evidence tiers.
    _SF6_MIN_TRADES_LABEL  = 10    # cells with fewer trades get TOO FEW TRADES
    _SF6_STRONG_WR         = 0.60  # win rate threshold for STRONG EDGE
    _SF6_WEAK_WR           = 0.40  # win rate threshold for WEAK EDGE (below = weak)
    _SF6_MAX_ROWS          = 40
    _SF6_MIN_ROWS_RENDER   = 3     # minimum rows in filtered df before panel renders

    # Guard: need entry_regime + entry_direction (has_entry_regime covers both)
    # and a realized PnL column.
    _has_sf6 = (
        has_entry_regime
        and "realized_usd" in df.columns
    )

    if _has_sf6:
        try:
            sf6_df = df[
                df["entry_regime"].notna()
                & df["entry_direction"].notna()
                & df["symbol"].notna()
            ].copy()

            if len(sf6_df) >= _SF6_MIN_ROWS_RENDER:
                # Normalise symbol for consistent grouping.
                sf6_df["_sym_norm"] = sf6_df["symbol"].apply(
                    lambda s: _norm_sym(str(s)) if pd.notna(s) else "?"
                )
                sf6_df["_regime_lower"] = sf6_df["entry_regime"].str.lower()
                sf6_df["_dir_lower"]    = sf6_df["entry_direction"].str.lower()

                # Build one row per (symbol, regime, direction) combination.
                # Use groupby on the three normalised keys; iterate in avg-PnL
                # descending order so strongest setups appear first.
                sf6_groups = (
                    sf6_df
                    .groupby(["_sym_norm", "_regime_lower", "_dir_lower"], sort=False)
                )

                # Collect all cells with their stats.
                _sf6_cells = []
                for (sym, regime, direction), sub in sf6_groups:
                    n, wins, total_pnl, avg_pnl = _group_stats(sub)
                    wr = wins / n if n > 0 else 0.0
                    _sf6_cells.append((sym, regime, direction, n, wins, wr,
                                       total_pnl, avg_pnl))

                # Sort by avg_pnl descending.
                _sf6_cells.sort(key=lambda x: x[7], reverse=True)

                sf6_rows_html = ""
                rendered_rows = 0
                for (sym, regime, direction, n, wins, wr,
                     total_pnl, avg_pnl) in _sf6_cells:
                    if rendered_rows >= _SF6_MAX_ROWS:
                        break

                    wr_pct = wr * 100

                    # Evidence label + colour.
                    if n < _SF6_MIN_TRADES_LABEL:
                        ev_label = "TOO FEW TRADES"
                        ev_col   = T["muted"]
                    elif wr >= _SF6_STRONG_WR:
                        ev_label = "STRONG EDGE"
                        ev_col   = T["green"]
                    elif wr < _SF6_WEAK_WR:
                        ev_label = "WEAK EDGE"
                        ev_col   = T["red"]
                    else:
                        ev_label = "MARGINAL"
                        ev_col   = T["yellow"]

                    regime_u   = regime.upper()
                    regime_col = (
                        T["green"] if regime_u == "BULL" else
                        T["red"]   if regime_u == "BEAR" else
                        T["muted"]
                    )
                    dir_col = T["cyan"] if direction == "long" else T["yellow"]

                    sf6_rows_html += (
                        f'<tr>'
                        f'<td style="color:{T["text"]};font-weight:700;">'
                        f'{_esc(sym)}</td>'
                        f'<td style="color:{regime_col};font-weight:700;'
                        f'font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_esc(regime_u)}</td>'
                        f'<td style="color:{dir_col};">'
                        f'{_esc(direction.upper())}</td>'
                        f'<td style="color:{T["text2"]};">{n}</td>'
                        f'<td style="color:{_wr_col(wins, n)};font-weight:700;">'
                        f'{wr_pct:.0f}%</td>'
                        f'<td style="color:{_pnl_col(total_pnl)};font-weight:700;">'
                        f'${total_pnl:+,.2f}</td>'
                        f'<td style="color:{_pnl_col(avg_pnl)};">'
                        f'${avg_pnl:+,.2f}</td>'
                        f'<td style="color:{ev_col};font-weight:700;'
                        f'font-family:{T["mono"]};font-size:0.62rem;">'
                        f'{_esc(ev_label)}</td>'
                        f'</tr>'
                    )
                    rendered_rows += 1

                if sf6_rows_html:
                    _cap_note_sf6 = (
                        f" (first {_SF6_MAX_ROWS} shown)"
                        if rendered_rows >= _SF6_MAX_ROWS else ""
                    )
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:6px;">'
                        f'SETUP EVIDENCE SUMMARY — SYMBOL × REGIME × DIRECTION'
                        f'<span style="color:{T["muted"]};font-weight:400;">'
                        f'{_esc(_cap_note_sf6)}</span></div>'
                        f'<table class="ot-table"><thead><tr>'
                        f'<th>SYMBOL</th><th>REGIME</th><th>DIRECTION</th>'
                        f'<th>TRADES</th><th>WIN RATE</th>'
                        f'<th>TOTAL PnL</th><th>AVG PnL</th>'
                        f'<th>EVIDENCE</th>'
                        f'</tr></thead><tbody>{sf6_rows_html}</tbody></table>'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'color:{T["muted"]};margin-top:4px;">'
                        f'Sorted by avg PnL ↓. '
                        f'STRONG EDGE: ≥{_SF6_MIN_TRADES_LABEL} trades + '
                        f'WR ≥{_SF6_STRONG_WR:.0%}. '
                        f'WEAK EDGE: ≥{_SF6_MIN_TRADES_LABEL} trades + '
                        f'WR &lt;{_SF6_WEAK_WR:.0%}. '
                        f'MARGINAL: ≥{_SF6_MIN_TRADES_LABEL} trades + '
                        f'WR {_SF6_WEAK_WR:.0%}–{_SF6_STRONG_WR:.0%}. '
                        f'TOO FEW TRADES: &lt;{_SF6_MIN_TRADES_LABEL} trades. '
                        f'Historical evidence only — no writeback. '
                        f'To act on this evidence, use Score Feedback Controls '
                        f'in the Policy State panel.</div></div>',
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;'
                        f'color:{T["muted"]};font-family:{T["mono"]};'
                        f'font-size:0.70rem;">'
                        f'SETUP EVIDENCE SUMMARY — No rows matched grouping.'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'SETUP EVIDENCE SUMMARY</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
                    f'font-size:0.70rem;">'
                    f'Insufficient data ({len(sf6_df)} rows with regime + direction'
                    f' — need ≥{_SF6_MIN_ROWS_RENDER}).'
                    f'</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _sf6_exc:
            log.debug("[TRACK06-SF6] setup evidence summary panel failed: %s", _sf6_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'SETUP EVIDENCE SUMMARY</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};'
            f'font-size:0.70rem;">'
            f'No entry-context data yet — requires Track 01 entry-context schema '
            f'(entry_regime / entry_direction).  Historical rows carry NULL and are '
            f'excluded.</div></div>',
            unsafe_allow_html=True,
        )

    # ── Sample note ──────────────────────────────────────────────────────────
    st.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;color:{T["muted"]};'
        f'margin-top:4px;">Based on {total_rows} canonical close-leg rows '
        f'(is_close_leg = 1, up to 2 000 most recent).</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# TRACK 02 — REPLAY / SHADOW REVIEW  [POST-ROADMAP-TRACK02]
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=CACHE_FAST)
def _load_round_trips() -> pd.DataFrame:
    """[TRACK02-DQ] Read-only self-JOIN query for round-trip review.

    Links each canonical close-leg row (is_close_leg=1) to its matching
    open-leg row (is_close_leg=0) via trade_id.  Returns one row per
    completed round-trip with full entry + exit context.

    Column set (subject to schema vintage):
      trade_id, symbol, inst_type,
      entry_ts, entry_price,                          # from open leg
      close_ts, close_price, pnl_pct, realized_usd,  # from close leg
      tag, exit_reason_type, exit_reason_category,
      hold_time_secs,
      entry_regime, entry_direction, entry_z_score,   # Track 01+
      entry_confidence, entry_kelly_f,                # Phase 6+
      close_slippage_bps, spread_at_close_bps         # Phase 10+

    Fallback tiers (tried in order, first non-empty wins):
      1. Full JOIN (requires trade_id + all diagnostic columns)
      2. Full JOIN without Phase-10 friction columns
      3. Full JOIN without Phase-6/10/DT columns (core only)
      4. Close-leg only (no open-leg price) with diagnostic columns
      5. Close-leg only, core columns only
      6. Minimal (pnl_pct != 0 heuristic, no is_close_leg column)

    Returns an empty DataFrame on any failure — never raises.
    No DB writes.  No schema changes.
    """
    conn = _get_db_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE);")
    except Exception:
        pass

    # Tier 1: Full JOIN — trade_id + all diagnostic columns (Track 01 schema)
    _SQL_FULL = """
        SELECT
            c.trade_id,
            c.symbol,
            c.inst_type,
            o.ts                   AS entry_ts,
            o.price                AS entry_price,
            c.ts                   AS close_ts,
            c.price                AS close_price,
            c.pnl_pct,
            c.realized_usd,
            c.tag,
            c.exit_reason_type,
            c.exit_reason_category,
            c.hold_time_secs,
            c.entry_regime,
            c.entry_direction,
            c.entry_z_score,
            c.entry_confidence,
            c.entry_kelly_f,
            c.close_slippage_bps,
            c.spread_at_close_bps
        FROM trades c
        JOIN trades o
          ON c.trade_id = o.trade_id
         AND o.is_close_leg = 0
        WHERE c.is_close_leg = 1
          AND c.trade_id IS NOT NULL
        ORDER BY c.ts DESC
        LIMIT 1000
    """
    # Tier 2: Full JOIN — no Phase-10 friction columns
    _SQL_JOIN_P6 = """
        SELECT
            c.trade_id,
            c.symbol,
            c.inst_type,
            o.ts                   AS entry_ts,
            o.price                AS entry_price,
            c.ts                   AS close_ts,
            c.price                AS close_price,
            c.pnl_pct,
            c.realized_usd,
            c.tag,
            c.exit_reason_type,
            c.exit_reason_category,
            c.hold_time_secs,
            c.entry_confidence,
            c.entry_kelly_f
        FROM trades c
        JOIN trades o
          ON c.trade_id = o.trade_id
         AND o.is_close_leg = 0
        WHERE c.is_close_leg = 1
          AND c.trade_id IS NOT NULL
        ORDER BY c.ts DESC
        LIMIT 1000
    """
    # Tier 3: Full JOIN — core columns only (Phase 1 schema)
    _SQL_JOIN_CORE = """
        SELECT
            c.trade_id,
            c.symbol,
            o.ts                   AS entry_ts,
            o.price                AS entry_price,
            c.ts                   AS close_ts,
            c.price                AS close_price,
            c.pnl_pct,
            c.realized_usd,
            c.tag,
            c.exit_reason_type,
            c.exit_reason_category
        FROM trades c
        JOIN trades o
          ON c.trade_id = o.trade_id
         AND o.is_close_leg = 0
        WHERE c.is_close_leg = 1
          AND c.trade_id IS NOT NULL
        ORDER BY c.ts DESC
        LIMIT 1000
    """
    # Tier 4: Close-leg only — all diagnostic columns (no entry_price from JOIN)
    _SQL_CLOSE_DIAG = """
        SELECT
            trade_id,
            symbol,
            inst_type,
            ts                  AS close_ts,
            price               AS close_price,
            pnl_pct,
            realized_usd,
            tag,
            exit_reason_type,
            exit_reason_category,
            hold_time_secs,
            entry_regime,
            entry_direction,
            entry_z_score,
            entry_confidence,
            entry_kelly_f,
            close_slippage_bps,
            spread_at_close_bps
        FROM trades
        WHERE is_close_leg = 1
        ORDER BY ts DESC
        LIMIT 1000
    """
    # Tier 5: Close-leg only — core columns only
    _SQL_CLOSE_CORE = """
        SELECT
            trade_id,
            symbol,
            ts      AS close_ts,
            price   AS close_price,
            pnl_pct,
            realized_usd,
            tag,
            exit_reason_type,
            exit_reason_category
        FROM trades
        WHERE is_close_leg = 1
        ORDER BY ts DESC
        LIMIT 1000
    """
    # Tier 6: Minimal — pre-Phase-1 schema, pnl_pct heuristic
    _SQL_MINIMAL = """
        SELECT
            symbol,
            ts      AS close_ts,
            price   AS close_price,
            pnl_pct,
            realized_usd,
            tag
        FROM trades
        WHERE pnl_pct IS NOT NULL AND pnl_pct != 0
        ORDER BY ts DESC
        LIMIT 1000
    """

    df = pd.DataFrame()
    for sql in (_SQL_FULL, _SQL_JOIN_P6, _SQL_JOIN_CORE,
                _SQL_CLOSE_DIAG, _SQL_CLOSE_CORE, _SQL_MINIMAL):
        try:
            df = _query(sql.strip())
            if not df.empty:
                break
        except Exception as _e:
            log.debug("_load_round_trips: tier attempt failed: %s", _e)
            df = pd.DataFrame()

    if df.empty:
        return df

    # Coerce numeric columns — graceful when column absent (older tier)
    for col in ("entry_ts", "close_ts", "entry_price", "close_price",
                "pnl_pct", "realized_usd", "hold_time_secs",
                "entry_z_score", "entry_confidence", "entry_kelly_f",
                "close_slippage_bps", "spread_at_close_bps"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None

    # Ensure string columns exist for filter logic
    for col in ("symbol", "tag", "exit_reason_type", "exit_reason_category",
                "entry_regime", "entry_direction", "inst_type"):
        if col not in df.columns:
            df[col] = None

    # Derive datetime columns for display and filtering
    if "entry_ts" in df.columns and df["entry_ts"].notna().any():
        df["entry_dt"] = pd.to_datetime(df["entry_ts"], unit="s", errors="coerce")
    else:
        df["entry_dt"] = pd.NaT
    if "close_ts" in df.columns and df["close_ts"].notna().any():
        df["close_dt"] = pd.to_datetime(df["close_ts"], unit="s", errors="coerce")
    else:
        df["close_dt"] = pd.NaT

    return df


@st.cache_data(ttl=CACHE_FAST)
def _load_near_miss_blocked(
    symbol: str,
    entry_ts: float,
    window_secs: int = 300,
    direction: str = "",
) -> "pd.DataFrame":
    """[TRACK11-NM] Read-only near-miss blocked-decision lookup.

    Fetches BLOCKED and SHADOW records from the decision_trace table for the
    given symbol within the time window [entry_ts − window_secs, entry_ts + 30s].

    This is an APPROXIMATE lookup — decision_trace.ts is the admission
    decision timestamp, which precedes the OKX fill timestamp stored in
    trades by signal-to-fill latency (typically a few seconds to tens of
    seconds).  The default ±5 min window is intentionally generous to avoid
    false-negatives while remaining meaningful for forensics.

    Only one admission can succeed per symbol at a time (executor enforces
    _entry_in_flight), so the lookup will not spuriously surface unrelated
    decisions during non-overlapping trade periods for the same symbol.

    Parameters
    ----------
    symbol      : exact symbol string from the trades row (e.g. "BTC")
    entry_ts    : Unix timestamp (s) from the open-leg ts column
    window_secs : seconds before entry_ts to include (default 300 = 5 min)
    direction   : [TRACK32-RA1] optional entry direction ("LONG" / "SHORT").
                  When non-empty, restricts results to rows whose direction
                  column matches.  When "" (default), no direction filter is
                  applied — identical to pre-Track-32 behaviour.

    Returns
    -------
    DataFrame with columns: ts, outcome, gate_name, reason, confidence,
    z_score, llm_verdict, p32_p_success, direction,
    [TRACK32-RA2] regime, kelly_f, sizing_usd, risk_off, conviction_mult.
    Empty DataFrame on missing table, no rows, or any error.

    Read-only.  No DB writes.  No schema changes.  Parameterized queries only.
    """
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return pd.DataFrame()
    if not symbol or not entry_ts:
        return pd.DataFrame()
    try:
        lo = float(entry_ts) - float(window_secs)
        hi = float(entry_ts) + 30.0

        # [TRACK32-RA1] Direction filter — applied only when direction is provided.
        _dir = str(direction or "").strip().upper()
        if _dir:
            sql = (
                "SELECT ts, outcome, gate_name, reason, confidence, z_score,"
                " llm_verdict, p32_p_success, direction,"
                " regime, kelly_f, sizing_usd, risk_off, conviction_mult"
                " FROM decision_trace"
                " WHERE symbol = ?"
                "   AND ts BETWEEN ? AND ?"
                "   AND outcome IN ('BLOCKED', 'SHADOW')"
                "   AND direction = ?"
                " ORDER BY ts DESC"
                " LIMIT 30"
            )
            params = (symbol, lo, hi, _dir)
        else:
            sql = (
                "SELECT ts, outcome, gate_name, reason, confidence, z_score,"
                " llm_verdict, p32_p_success, direction,"
                " regime, kelly_f, sizing_usd, risk_off, conviction_mult"
                " FROM decision_trace"
                " WHERE symbol = ?"
                "   AND ts BETWEEN ? AND ?"
                "   AND outcome IN ('BLOCKED', 'SHADOW')"
                " ORDER BY ts DESC"
                " LIMIT 30"
            )
            params = (symbol, lo, hi)

        df = _query(sql, params)
        return df if df is not None else pd.DataFrame()
    except Exception:
        log.error("[TRACK11-NM] _load_near_miss_blocked: query failed", exc_info=True)
        return pd.DataFrame()


# [TRACK18-CORR] Forward-look windows — how far ahead to search for later
# admission, and how long after admission to look for the close row.
_CORR18_ADMITTED_WINDOW_SECS: int = 600    # 10 min: look this far forward for admission
_CORR18_CLOSE_WINDOW_SECS:    int = 86400  # 24 hr: look this far after admission for close

# [TRACK26-LC1] Trade-lifecycle lookup windows — anchored from the trade side.
_LC26_ENTRY_LOOKBACK_SECS: int = 120  # search this far before entry_ts for ADMITTED row
_LC26_CLOSE_SEARCH_SECS:   int = 60   # half-window around close_ts for CLOSED row


def _load_later_admitted_and_outcome(
    symbol:        str,
    direction:     str,
    blocked_ts:    float,
    window_secs:   int = _CORR18_ADMITTED_WINDOW_SECS,
    max_hold_secs: int = _CORR18_CLOSE_WINDOW_SECS,
) -> dict:
    """[TRACK18-CORR1] Forward-looking blocked/shadow → admitted → closed correlation.

    Given a BLOCKED or SHADOW decision (symbol, direction, blocked_ts), finds:

    Step 1 — earliest ADMITTED row for the same symbol + direction with
             ts ∈ (blocked_ts, blocked_ts + window_secs).
             Two-tier query fallback:
               (1) With source_path IN ('entry','express') — Track-16+ DBs.
               (2) Without source_path filter — Track-10/pre-16 DBs.
             If neither returns a row the look-up stops; admitted_found=False.

    Step 2 — if Step 1 found a row, finds the earliest CLOSED row for the
             same symbol with ts ∈ (admitted_ts, admitted_ts + max_hold_secs).
             Two-tier query fallback:
               (1) source_path='close', reads pnl_pct + hold_time_secs — Track-17+.
               (2) outcome='CLOSED', pnl/hold returned as NULL — Track-10/pre-17.

    Returns
    -------
    dict with keys:
        admitted_found    : bool          True if a later admission was found
        latency_secs      : float | None  seconds from block ts to admission ts
        admitted_ts       : float | None
        admitted_gate     : str   | None  gate_name of the admitted row
        admitted_source   : str   | None  source_path of the admitted row
        close_found       : bool          True if a close row was found
        pnl_pct           : float | None  from close row; None on pre-Track-17 DB
        hold_time_secs    : float | None  from close row; None on pre-Track-17 DB
        close_gate        : str   | None  gate_name (exit reason) of close row

    All failure paths return {"admitted_found": False, "close_found": False}.
    Read-only.  Parameterized queries only.  No DB writes.  No schema changes.
    No filesystem writes.  Never raises into caller.
    """
    _MISS: dict = {"admitted_found": False, "close_found": False}
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return _MISS
    if not symbol or not direction or not blocked_ts:
        return _MISS
    try:
        _lo = float(blocked_ts)
        _hi = _lo + float(window_secs)

        # ── Step 1: earliest later ADMITTED row ───────────────────────────────
        _adm_series = None
        # Tier 1: Track-16+ DB with source_path column
        _sql_adm_src = (
            "SELECT ts, gate_name, source_path"
            " FROM decision_trace"
            " WHERE symbol = ? AND direction = ? AND outcome = 'ADMITTED'"
            "   AND source_path IN ('entry', 'express')"
            "   AND ts > ? AND ts < ?"
            " ORDER BY ts ASC LIMIT 1"
        )
        # Tier 2: pre-Track-16 DB — no source_path column
        _sql_adm_nosrc = (
            "SELECT ts, gate_name, NULL AS source_path"
            " FROM decision_trace"
            " WHERE symbol = ? AND direction = ? AND outcome = 'ADMITTED'"
            "   AND ts > ? AND ts < ?"
            " ORDER BY ts ASC LIMIT 1"
        )
        _adm_params = (symbol, direction, _lo, _hi)
        for _adm_sql in (_sql_adm_src, _sql_adm_nosrc):
            try:
                _adf = _query(_adm_sql, _adm_params)
                if _adf is not None and not _adf.empty:
                    _adm_series = _adf.iloc[0]
                    break
                if _adf is not None:
                    break   # query ran OK, no rows — no later admission in window
            except Exception as _ae:
                log.debug("[TRACK18-CORR1] admitted query fallback: %s", _ae)
                continue

        if _adm_series is None:
            return _MISS

        try:
            admitted_ts     = float(_adm_series["ts"])
            admitted_gate   = str(_adm_series.get("gate_name") or "")
            admitted_source = str(_adm_series.get("source_path") or "")
            latency_secs    = round(admitted_ts - _lo, 1)
        except Exception:
            return _MISS

        result: dict = {
            "admitted_found":  True,
            "latency_secs":    latency_secs,
            "admitted_ts":     admitted_ts,
            "admitted_gate":   admitted_gate,
            "admitted_source": admitted_source,
            "close_found":     False,
            "pnl_pct":         None,
            "hold_time_secs":  None,
            "close_gate":      None,
        }

        # ── Step 2: earliest CLOSED row after admission ───────────────────────
        _clo_hi  = admitted_ts + float(max_hold_secs)
        _clo_params = (symbol, admitted_ts, _clo_hi)
        # Tier 1: Track-17+ DB — source_path='close' with pnl_pct + hold_time_secs
        _sql_clo_full = (
            "SELECT ts, gate_name, pnl_pct, hold_time_secs"
            " FROM decision_trace"
            " WHERE symbol = ? AND source_path = 'close'"
            "   AND ts > ? AND ts < ?"
            " ORDER BY ts ASC LIMIT 1"
        )
        # Tier 2: Track-10/pre-17 DB — match on outcome='CLOSED', NULL pnl/hold
        _sql_clo_fallback = (
            "SELECT ts, gate_name, NULL AS pnl_pct, NULL AS hold_time_secs"
            " FROM decision_trace"
            " WHERE symbol = ? AND outcome = 'CLOSED'"
            "   AND ts > ? AND ts < ?"
            " ORDER BY ts ASC LIMIT 1"
        )
        for _clo_sql in (_sql_clo_full, _sql_clo_fallback):
            try:
                _cdf = _query(_clo_sql, _clo_params)
                if _cdf is not None and not _cdf.empty:
                    _cr = _cdf.iloc[0]
                    result["close_found"] = True
                    result["close_gate"]  = str(_cr.get("gate_name") or "")
                    _pnl = _cr.get("pnl_pct")
                    _hld = _cr.get("hold_time_secs")
                    result["pnl_pct"]        = float(_pnl) if _pnl is not None else None
                    result["hold_time_secs"] = float(_hld) if _hld is not None else None
                    break
                if _cdf is not None:
                    break   # query ran OK, no close row found in window
            except Exception as _ce:
                log.debug("[TRACK18-CORR1] close query fallback: %s", _ce)
                continue

        return result

    except Exception:
        log.error(
            "[TRACK18-CORR1] _load_later_admitted_and_outcome: unexpected error",
            exc_info=True,
        )
        return _MISS


@st.cache_data(ttl=CACHE_FAST)
def _load_trade_lifecycle(
    symbol:    str,
    direction: str,
    entry_ts:  float,
    close_ts:  float,
) -> dict:
    """[TRACK26-LC2] Trade-anchored lifecycle lookup.

    Given a completed trade (symbol, direction, entry_ts, close_ts),
    locates the decision_trace rows for its ADMITTED admission decision
    and its CLOSED exit record.

    Anchors from the trade side, not the decision side.  No fuzzy
    heuristics — time-window + exact symbol + direction only.

    Returns
    -------
    dict:
      entry_found : bool
      entry_ctx   : dict | None
      close_found : bool
      close_ctx   : dict | None

    Never raises.  Read-only.  No DB writes.  No schema changes.
    """
    _MISS: dict = {
        "entry_found": False, "entry_ctx": None,
        "close_found": False, "close_ctx": None,
    }
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return _MISS
    if not symbol or not entry_ts or not close_ts:
        return _MISS
    try:
        _elo = float(entry_ts) - float(_LC26_ENTRY_LOOKBACK_SECS)
        _ehi = float(entry_ts) + 10.0
        _clo = float(close_ts) - float(_LC26_CLOSE_SEARCH_SECS)
        _chi = float(close_ts) + float(_LC26_CLOSE_SEARCH_SECS)
        _dir = str(direction or "").strip()
        _cts = float(close_ts)
    except (TypeError, ValueError):
        return _MISS

    result: dict = {
        "entry_found": False, "entry_ctx": None,
        "close_found": False, "close_ctx": None,
    }

    # ── ADMITTED lookup ───────────────────────────────────────────────────────
    # Tier 1: with source_path column (Track-16+ DB)
    _sql_e1 = (
        "SELECT ts, confidence, z_score, kelly_f, gate_name, source_path,"
        " sizing_usd, llm_verdict, risk_off, conviction_mult, regime"
        " FROM decision_trace"
        " WHERE symbol = ? AND direction = ? AND outcome = 'ADMITTED'"
        "   AND ts >= ? AND ts <= ?"
        " ORDER BY ts DESC LIMIT 1"
    )
    # Tier 2: no source_path column (Track-10 DB)
    _sql_e2 = (
        "SELECT ts, confidence, z_score, kelly_f, gate_name,"
        " NULL AS source_path,"
        " sizing_usd, llm_verdict, risk_off, conviction_mult, regime"
        " FROM decision_trace"
        " WHERE symbol = ? AND direction = ? AND outcome = 'ADMITTED'"
        "   AND ts >= ? AND ts <= ?"
        " ORDER BY ts DESC LIMIT 1"
    )
    _ep = (symbol, _dir, _elo, _ehi)
    for _esql in (_sql_e1, _sql_e2):
        try:
            _edf = _query(_esql, _ep)
            if _edf is not None and not _edf.empty:
                _er = _edf.iloc[0]
                result["entry_found"] = True
                result["entry_ctx"] = {
                    "ts":             float(_er.get("ts") or 0.0),
                    "confidence":     _er.get("confidence"),
                    "z_score":        _er.get("z_score"),
                    "kelly_f":        _er.get("kelly_f"),
                    "gate_name":      str(_er.get("gate_name") or ""),
                    "source_path":    str(_er.get("source_path") or ""),
                    "sizing_usd":     _er.get("sizing_usd"),
                    "llm_verdict":    str(_er.get("llm_verdict") or ""),
                    "risk_off":       bool(_er.get("risk_off") or False),
                    "conviction_mult": _er.get("conviction_mult"),
                    "regime":         str(_er.get("regime") or ""),
                }
                break
            if _edf is not None:
                break   # query OK, no rows in window
        except Exception as _ef:
            log.debug("[TRACK26-LC2] admitted lookup fallback: %s", _ef)
            continue

    # ── CLOSED lookup ─────────────────────────────────────────────────────────
    # Tier 1: source_path='close' + pnl_pct + hold_time_secs (Track-17+)
    _sql_c1 = (
        "SELECT ts, gate_name, pnl_pct, hold_time_secs"
        " FROM decision_trace"
        " WHERE symbol = ? AND source_path = 'close'"
        "   AND ts >= ? AND ts <= ?"
        " ORDER BY ABS(ts - ?) ASC LIMIT 1"
    )
    # Tier 2: outcome='CLOSED', NULL pnl/hold (Track-10/pre-17)
    _sql_c2 = (
        "SELECT ts, gate_name, NULL AS pnl_pct, NULL AS hold_time_secs"
        " FROM decision_trace"
        " WHERE symbol = ? AND outcome = 'CLOSED'"
        "   AND ts >= ? AND ts <= ?"
        " ORDER BY ABS(ts - ?) ASC LIMIT 1"
    )
    _cp = (symbol, _clo, _chi, _cts)
    for _csql in (_sql_c1, _sql_c2):
        try:
            _cdf = _query(_csql, _cp)
            if _cdf is not None and not _cdf.empty:
                _cr = _cdf.iloc[0]
                _cpnl = _cr.get("pnl_pct")
                _chld = _cr.get("hold_time_secs")
                result["close_found"] = True
                result["close_ctx"] = {
                    "ts":             float(_cr.get("ts") or 0.0),
                    "gate_name":      str(_cr.get("gate_name") or ""),
                    "pnl_pct":        float(_cpnl) if _cpnl is not None else None,
                    "hold_time_secs": float(_chld) if _chld is not None else None,
                }
                break
            if _cdf is not None:
                break   # query OK, no rows in window
        except Exception as _cf:
            log.debug("[TRACK26-LC2] closed lookup fallback: %s", _cf)
            continue

    return result


@st.fragment(run_every=FRAG_SLOW)
def render_trade_replay() -> None:
    """[TRACK02-UI / TRACK11] Round-trip trade replay / shadow review panel.

    Renders a filterable, scrollable table of completed round-trips with
    full entry and exit context side by side.  Operator controls:
      - Symbol selector (all + each distinct symbol in the data)
      - Exit category filter (ALL / STRATEGY / RISK / ANOMALY)
      - Entry regime filter (ALL / bull / bear / chop)
      - Days-back window (7 / 30 / 90 / ALL)
      [TRACK11]
      - Direction filter (ALL / LONG / SHORT)
      - Sort mode (Recent = close_ts DESC | Best PnL = realized_usd DESC)

    Table columns:
      ENTRY, EXIT, SYMBOL, DIR, REGIME, ENTRY PX, CONF, Z, KELLY,
      EXIT TAG, CAT, HOLD, PNL%, PNL $, SLIP bps
      [TRACK11: Z and KELLY added; cap raised from 50 to 100]

    KPI summary row reflects the filtered subset.

    Near-miss expander [TRACK11]:
      Operator selects a visible trade; shows BLOCKED/SHADOW decisions
      from decision_trace within ±5 min of that entry.  Approximate
      time-window lookup — not an exact join.  Degrades gracefully when
      decision_trace table absent or no rows match.

    Degrades gracefully for:
      - Empty DB
      - Pre-Phase-1 DB (no trade_id — falls back to close-leg only)
      - Pre-Track-01 DB (no entry_regime / entry_direction)
      - Mixed old/new rows (filters skip NULL columns gracefully)

    Read-only.  No DB writes.  No schema changes.
    """
    _section("TRADE REPLAY — ROUND-TRIP REVIEW", "🔁")

    try:
        df = _load_round_trips()
    except Exception as _exc:
        log.debug("[TRACK02] _load_round_trips error: %s", _exc)
        df = pd.DataFrame()

    if df.empty:
        st.markdown(
            f'<div class="ot-tile" style="padding:12px;text-align:center;">'
            f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.72rem;">'
            f'NO COMPLETED ROUND-TRIPS RECORDED YET</span></div>',
            unsafe_allow_html=True,
        )
        return

    # ── Availability flags ───────────────────────────────────────────────────
    has_regime  = "entry_regime"    in df.columns and df["entry_regime"].notna().any()
    has_cat     = "exit_reason_category" in df.columns and df["exit_reason_category"].notna().any()
    has_slip    = "close_slippage_bps" in df.columns and df["close_slippage_bps"].notna().any()
    has_entry_p = "entry_price" in df.columns and df["entry_price"].notna().any()
    has_conf    = "entry_confidence" in df.columns and df["entry_confidence"].notna().any()
    has_hold    = "hold_time_secs" in df.columns and df["hold_time_secs"].notna().any()
    # [TRACK11] New availability flags
    has_z       = "entry_z_score" in df.columns and df["entry_z_score"].notna().any()
    has_kelly   = "entry_kelly_f"  in df.columns and df["entry_kelly_f"].notna().any()
    has_dir     = "entry_direction" in df.columns and df["entry_direction"].notna().any()

    # ── Filter controls — row 1: original four ───────────────────────────────
    raw_syms = df["symbol"].dropna().unique().tolist() if "symbol" in df.columns else []
    sym_opts = ["ALL"] + sorted(set(_norm_sym(str(s)) for s in raw_syms))

    cat_opts    = ["ALL", "STRATEGY", "RISK", "ANOMALY"]
    regime_opts = ["ALL", "bull", "bear", "chop"]
    days_opts   = ["7d", "30d", "90d", "ALL"]

    fc1, fc2, fc3, fc4 = st.columns([1, 1, 1, 1], gap="small")
    with fc1:
        sel_sym = st.selectbox(
            "Symbol", options=sym_opts, index=0,
            key="rt_sym", label_visibility="collapsed"
        )
    with fc2:
        sel_cat = st.selectbox(
            "Exit Category", options=cat_opts, index=0,
            key="rt_cat", label_visibility="collapsed"
        )
    with fc3:
        sel_regime = st.selectbox(
            "Entry Regime", options=regime_opts, index=0,
            key="rt_regime", label_visibility="collapsed",
            disabled=(not has_regime),
        )
    with fc4:
        sel_days = st.selectbox(
            "Window", options=days_opts, index=1,
            key="rt_days", label_visibility="collapsed"
        )

    # ── Filter controls — row 2: direction + sort [TRACK11] ─────────────────
    fd1, fd2, fd_pad = st.columns([1, 1, 2], gap="small")
    with fd1:
        sel_dir = st.selectbox(
            "Dir", options=["ALL", "LONG", "SHORT"], index=0,
            key="rt_dir", label_visibility="collapsed",
            disabled=(not has_dir),
        )
    with fd2:
        sel_sort = st.selectbox(
            "Sort", options=["Recent", "Best PnL"], index=0,
            key="rt_sort", label_visibility="collapsed",
        )

    # ── Apply filters ────────────────────────────────────────────────────────
    fdf = df.copy()

    if sel_sym != "ALL" and "symbol" in fdf.columns:
        fdf = fdf[fdf["symbol"].apply(
            lambda s: _norm_sym(str(s)) == sel_sym if pd.notna(s) else False
        )]

    if sel_cat != "ALL" and has_cat:
        fdf = fdf[fdf["exit_reason_category"].astype(str).str.upper() == sel_cat]

    if sel_regime != "ALL" and has_regime:
        fdf = fdf[fdf["entry_regime"].astype(str).str.lower() == sel_regime.lower()]

    if sel_days != "ALL":
        _days_map = {"7d": 7, "30d": 30, "90d": 90}
        _n_days   = _days_map.get(sel_days, 30)
        import time as _t
        _cutoff   = _t.time() - _n_days * 86400
        if "close_ts" in fdf.columns and fdf["close_ts"].notna().any():
            fdf = fdf[fdf["close_ts"].fillna(0) >= _cutoff]
        elif "entry_ts" in fdf.columns and fdf["entry_ts"].notna().any():
            fdf = fdf[fdf["entry_ts"].fillna(0) >= _cutoff]

    # [TRACK11] Direction filter
    if sel_dir != "ALL" and has_dir:
        fdf = fdf[fdf["entry_direction"].astype(str).str.upper() == sel_dir]

    # [TRACK11] Sort mode
    if sel_sort == "Best PnL" and "realized_usd" in fdf.columns:
        fdf = fdf.sort_values("realized_usd", ascending=False)
    # "Recent" keeps the existing close_ts DESC order from the SQL query

    # ── Filtered KPI summary ─────────────────────────────────────────────────
    n_rt     = len(fdf)
    if n_rt == 0:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;text-align:center;margin-bottom:6px;">'
            f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'NO ROUND-TRIPS MATCH CURRENT FILTERS</span></div>',
            unsafe_allow_html=True,
        )
        return

    _rusd    = fdf["realized_usd"].fillna(0.0) if "realized_usd" in fdf.columns else pd.Series([0.0] * n_rt)
    wins     = int((_rusd > 0).sum())
    wr_pct   = wins / n_rt * 100 if n_rt > 0 else 0.0
    avg_pnl  = float(_rusd.mean()) if n_rt > 0 else 0.0
    avg_slip = (
        float(fdf["close_slippage_bps"].dropna().mean())
        if has_slip and "close_slippage_bps" in fdf.columns
        else None
    )

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        _kpi("ROUND-TRIPS", str(n_rt),
             f"{wins}W / {n_rt - wins}L")
    with k2:
        _kpi("WIN RATE", f"{wr_pct:.1f}%",
             f"filtered subset",
             T["green"] if wr_pct > 55 else T["yellow"] if wr_pct > 40 else T["red"])
    with k3:
        _kpi("AVG PnL", f"${avg_pnl:+,.2f}",
             "per round-trip",
             T["green"] if avg_pnl >= 0 else T["red"])
    with k4:
        if avg_slip is not None:
            _slip_col = T["green"] if avg_slip < 3 else T["yellow"] if avg_slip < 8 else T["red"]
            _kpi("AVG CLOSE SLIP", f"{avg_slip:+.1f}bps",
                 "positive = adverse", _slip_col)
        else:
            _kpi("AVG CLOSE SLIP", "—", "no friction data", T["muted"])

    # ── Round-trip table [TRACK11: cap 100, Z + KELLY columns] ──────────────
    _display = fdf.head(100)  # [TRACK11] raised from 50 to 100

    def _fmt_hold(secs) -> str:
        """Format hold_time_secs to a human-readable string."""
        try:
            s = float(secs)
            if s < 0 or s != s:  # negative or NaN
                return "—"
            if s < 90:
                return f"{s:.0f}s"
            if s < 5400:
                return f"{s / 60:.1f}m"
            return f"{s / 3600:.1f}h"
        except (TypeError, ValueError):
            return "—"

    def _fmt_dt(dt_val) -> str:
        try:
            if pd.isna(dt_val):
                return "—"
            return dt_val.strftime("%m-%d %H:%M")
        except Exception:
            return "—"

    rows = ""
    for _, row in _display.iterrows():
        # PnL colour
        _pnl_usd = _safe_float(row.get("realized_usd"))
        _pnl_pct = _safe_float(row.get("pnl_pct"))
        _pnl_col  = T["green"] if _pnl_usd > 0 else T["red"] if _pnl_usd < 0 else T["muted"]

        # Entry dt: prefer entry_dt, fall back to close_dt label
        _ent_dt   = _fmt_dt(row.get("entry_dt"))
        _cls_dt   = _fmt_dt(row.get("close_dt"))

        # Symbol
        _sym = _esc(_norm_sym(str(row.get("symbol", "?"))))

        # Direction
        _dir = str(row.get("entry_direction") or "").upper()
        _dir_col = T["green"] if _dir == "LONG" else T["red"] if _dir == "SHORT" else T["muted"]
        _dir_str = _esc(_dir) if _dir else "—"

        # Entry regime
        _reg = str(row.get("entry_regime") or "")
        _reg_upper = _reg.upper()
        _reg_col = (T["green"] if _reg_upper == "BULL" else
                    T["red"] if _reg_upper == "BEAR" else
                    T["yellow"] if _reg_upper == "CHOP" else T["muted"])
        _reg_str = _esc(_reg_upper) if _reg else "—"

        # Entry price
        _ent_px = _safe_float(row.get("entry_price"))
        _ent_px_str = f"{_ent_px:,.4f}" if _ent_px > 0 else "—"

        # Entry confidence
        _conf = row.get("entry_confidence")
        _conf_str = f"{float(_conf):.3f}" if pd.notna(_conf) else "—"

        # [TRACK11] Entry z_score
        _z = row.get("entry_z_score")
        if pd.notna(_z):
            _zf = float(_z)
            _zc = (T["cyan"] if abs(_zf) >= 1.5 else
                   T["blue"] if abs(_zf) >= 0.8 else T["muted"])
            _z_str = f'<span style="color:{_zc};font-family:{T["mono"]};">{_zf:+.2f}</span>'
        else:
            _z_str = f'<span style="color:{T["muted"]};">—</span>'

        # [TRACK11] Entry kelly_f
        _kf = row.get("entry_kelly_f")
        _kf_str = (
            f'<span style="color:{T["text2"]};font-family:{T["mono"]};font-size:0.61rem;">'
            f'{float(_kf):.3f}</span>'
            if pd.notna(_kf)
            else f'<span style="color:{T["muted"]};">—</span>'
        )

        # Exit tag
        _tag = _esc(str(row.get("tag") or "")[:18])

        # Exit category
        _cat = str(row.get("exit_reason_category") or "")
        _cat_upper = _cat.upper()
        _cat_col = (T["green"] if _cat_upper == "STRATEGY" else
                    T["red"]   if _cat_upper in ("RISK", "ANOMALY") else T["muted"])
        _cat_str = _esc(_cat_upper[:3]) if _cat else "—"

        # Hold time
        _hold_str = _fmt_hold(row.get("hold_time_secs"))

        # Slippage
        _slip = row.get("close_slippage_bps")
        if pd.notna(_slip):
            _slip_f = float(_slip)
            _sc = T["green"] if _slip_f < 3 else T["yellow"] if _slip_f < 8 else T["red"]
            _slip_str = f'<span style="color:{_sc};font-family:{T["mono"]};">{_slip_f:+.1f}</span>'
        else:
            _slip_str = f'<span style="color:{T["muted"]};">—</span>'

        # Row background: subtle tint by pnl direction
        _row_bg = ("rgba(34,197,94,0.03)" if _pnl_usd > 0
                   else "rgba(244,63,94,0.04)" if _pnl_usd < 0
                   else "transparent")

        rows += (
            f'<tr style="border-bottom:1px solid {T["border"]}22;background:{_row_bg};">'
            f'<td style="padding:3px 6px;color:{T["text2"]};font-size:0.59rem;">{_ent_dt}</td>'
            f'<td style="padding:3px 6px;color:{T["text2"]};font-size:0.59rem;">{_cls_dt}</td>'
            f'<td style="padding:3px 6px;font-weight:700;color:{T["text"]};">{_sym}</td>'
            f'<td style="padding:3px 6px;color:{_dir_col};font-weight:700;">{_dir_str}</td>'
            f'<td style="padding:3px 6px;color:{_reg_col};font-family:{T["mono"]};">{_reg_str}</td>'
            f'<td style="padding:3px 6px;color:{T["text2"]};font-family:{T["mono"]};font-size:0.65rem;">{_ent_px_str}</td>'
            f'<td style="padding:3px 6px;color:{T["muted"]};font-family:{T["mono"]};font-size:0.63rem;">{_conf_str}</td>'
            f'<td style="padding:3px 6px;font-size:0.63rem;">{_z_str}</td>'
            f'<td style="padding:3px 6px;font-size:0.61rem;">{_kf_str}</td>'
            f'<td style="padding:3px 6px;color:{T["muted"]};font-size:0.63rem;">{_tag}</td>'
            f'<td style="padding:3px 6px;color:{_cat_col};font-weight:700;font-size:0.63rem;">{_cat_str}</td>'
            f'<td style="padding:3px 6px;color:{T["muted"]};font-family:{T["mono"]};font-size:0.63rem;">{_hold_str}</td>'
            f'<td style="padding:3px 6px;color:{_pnl_col};font-family:{T["mono"]};font-weight:700;">{_pnl_pct:+.2f}%</td>'
            f'<td style="padding:3px 6px;color:{_pnl_col};font-family:{T["mono"]};font-weight:700;">${_pnl_usd:+,.2f}</td>'
            f'<td style="padding:3px 6px;font-family:{T["mono"]};font-size:0.63rem;">{_slip_str}</td>'
            f'</tr>'
        )

    # [TRACK11] Z and KELLY added to header
    hdr_labels = [
        "ENTRY", "EXIT", "SYMBOL", "DIR", "REGIME",
        "ENTRY PX", "CONF", "Z", "KELLY", "EXIT TAG", "CAT", "HOLD",
        "PNL%", "PNL $", "SLIP bps",
    ]
    hdr = "".join(
        f'<th style="padding:4px 6px;font-size:0.54rem;letter-spacing:0.10em;'
        f'color:{T["muted"]};text-transform:uppercase;white-space:nowrap;'
        f'border-bottom:1px solid {T["border"]};">{lbl}</th>'
        for lbl in hdr_labels
    )

    # Data-availability footnotes
    _notes = []
    if not has_entry_p:
        _notes.append("entry price unavailable (pre-Phase-1 schema)")
    if not has_regime:
        _notes.append("regime/direction unavailable (pre-Track-01 schema)")
    if not has_slip:
        _notes.append("slippage unavailable (pre-Phase-10 schema or no tick data)")
    # [TRACK11] Note Z/KELLY absence on old schema
    if not has_z:
        _notes.append("Z unavailable (pre-Track-01 schema)")
    _footnote = (
        f'<div style="font-family:{T["mono"]};font-size:0.57rem;color:{T["muted"]};margin-top:4px;">'
        + (" · ".join(_notes) if _notes else
           "Positive slip = adverse. CONF = entry_confidence. Z = entry_z_score. "
           "KELLY = entry_kelly_f. CAT = exit category (3-letter).")
        + "</div>"
    )

    total_in_filter = len(fdf)
    _cap = (
        f'<div style="font-family:{T["mono"]};font-size:0.57rem;color:{T["muted"]};'
        f'margin-top:2px;">Showing {len(_display)} of {total_in_filter} filtered '
        f'round-trips (max 100 displayed · {len(df)} total in DB)</div>'  # [TRACK11] cap 100
    )

    st.markdown(
        f'<div class="ot-tile ot-scroll" style="padding:10px;overflow-x:auto;">'
        f'<table style="width:100%;border-collapse:collapse;font-size:0.68rem;'
        f'font-family:{T["mono"]};">'
        f'<thead><tr>{hdr}</tr></thead>'
        f'<tbody>{rows}</tbody>'
        f'</table>'
        f'{_footnote}{_cap}</div>',
        unsafe_allow_html=True,
    )

    # ── [TRACK11-NM] Near-miss blocked-decisions expander ────────────────────
    # Build the trade selector from displayed rows.  Only shown when there is
    # at least one visible row with a non-null entry_ts.
    _has_entry_ts = (
        "entry_ts" in _display.columns and _display["entry_ts"].notna().any()
    )
    if not _has_entry_ts:
        return   # nothing to select — pre-JOIN schema, skip silently

    # Build label → (symbol, entry_ts, direction) map for the selectbox.
    # Labels are "{SYM} @ {entry_dt} [{PnL sign}]" — human-readable and unique
    # enough for a display context.
    # [TRACK32-RA1] direction stored as third element (uppercase string, "" when absent).
    _nm_options = []   # list of str labels, same order as _nm_rows
    _nm_rows    = []   # list of (symbol, entry_ts, direction) tuples
    for _, _r in _display.iterrows():
        _r_sym    = str(_r.get("symbol") or "").strip()
        _r_ets    = _r.get("entry_ts")
        if not _r_sym or not pd.notna(_r_ets):
            continue
        try:
            _r_ets_f = float(_r_ets)
        except (TypeError, ValueError):
            continue
        _r_edt = _r.get("entry_dt")
        try:
            _r_edt_s = _r_edt.strftime("%m-%d %H:%M") if pd.notna(_r_edt) else "?"
        except Exception:
            _r_edt_s = "?"
        _r_pnl = _safe_float(_r.get("realized_usd"))
        _r_sign = "+" if _r_pnl > 0 else ("−" if _r_pnl < 0 else "·")
        # [TRACK32-RA1] capture direction; "" when column absent or null
        _r_dir_raw = _r.get("entry_direction") if "entry_direction" in _display.columns else None
        _r_dir_str = str(_r_dir_raw).strip().upper() if pd.notna(_r_dir_raw) and _r_dir_raw else ""
        _nm_options.append(f"{_norm_sym(_r_sym)} @ {_r_edt_s} [{_r_sign}]")
        _nm_rows.append((_r_sym, _r_ets_f, _r_dir_str))

    if not _nm_options:
        return   # no selectable trades

    with st.expander("🔍 Near-miss blocked decisions — select a trade", expanded=False):
        st.markdown(
            f'<div style="font-family:{T["mono"]};font-size:0.60rem;color:{T["muted"]};'
            f'margin-bottom:6px;">Shows BLOCKED and SHADOW admission decisions recorded '
            f'in the decision_trace table within 5 min before the selected entry. '
            f'Approximate time-window lookup — not an exact join. '
            f'[TRACK32-RA1] Filtered to matching direction when available.</div>',
            unsafe_allow_html=True,
        )

        _nm_sel_label = st.selectbox(
            "Trade", options=_nm_options, index=0,
            key="rt_nm_trade", label_visibility="collapsed",
        )

        # Resolve selection back to (symbol, entry_ts, direction)
        try:
            _nm_idx = _nm_options.index(_nm_sel_label)
            _nm_sym, _nm_ets, _nm_dir_filter = _nm_rows[_nm_idx]
        except (ValueError, IndexError):
            _nm_sym, _nm_ets, _nm_dir_filter = "", 0.0, ""

        if not _nm_sym or not _nm_ets:
            st.markdown(
                f'<div class="ot-tile" style="padding:8px;text-align:center;">'
                f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.65rem;">'
                f'SELECT A TRADE ABOVE</span></div>',
                unsafe_allow_html=True,
            )
            return

        # Check table presence before calling the loader
        if "decision_trace" not in _get_known_tables():
            _slot_placeholder(
                "✓ DECISION TRACE TABLE NOT YET CREATED — start the bot to populate", 50
            )
            return

        nm_df = _load_near_miss_blocked(_nm_sym, _nm_ets, window_secs=300,
                                        direction=_nm_dir_filter)  # [TRACK32-RA1]

        if nm_df.empty:
            st.markdown(
                f'<div class="ot-tile" style="padding:8px;text-align:center;">'
                f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.65rem;">'
                f'NO BLOCKED / SHADOW DECISIONS IN THE ±5 MIN WINDOW FOR '
                f'{_esc(_norm_sym(_nm_sym))}</span></div>',
                unsafe_allow_html=True,
            )
            return

        # Render near-miss table
        nm_rows_html = ""
        for _, nm_r in nm_df.iterrows():
            _nm_ts_raw = nm_r.get("ts") or 0.0
            _nm_ts_str = "—"
            try:
                _nm_ts_str = datetime.fromtimestamp(float(_nm_ts_raw)).strftime("%m-%d %H:%M:%S")
            except (ValueError, TypeError, OSError):
                pass

            _nm_out   = str(nm_r.get("outcome") or "").upper()
            _nm_gate  = str(nm_r.get("gate_name") or "")
            _nm_dir   = str(nm_r.get("direction") or "").upper()
            _nm_conf  = nm_r.get("confidence")
            _nm_z     = nm_r.get("z_score")
            _nm_llm   = str(nm_r.get("llm_verdict") or "")
            _nm_p32   = nm_r.get("p32_p_success")
            _nm_reason = _esc(str(nm_r.get("reason") or "")[:70])

            # [TRACK32-RA2] enrichment fields
            _nm_regime  = str(nm_r.get("regime") or "") if "regime" in nm_df.columns else ""
            _nm_kf      = nm_r.get("kelly_f")      if "kelly_f"      in nm_df.columns else None
            _nm_sz      = nm_r.get("sizing_usd")   if "sizing_usd"   in nm_df.columns else None
            _nm_roff    = nm_r.get("risk_off")      if "risk_off"     in nm_df.columns else None
            _nm_cm      = nm_r.get("conviction_mult") if "conviction_mult" in nm_df.columns else None

            _nm_out_col  = T["red"] if _nm_out == "BLOCKED" else T["blue"]
            _nm_gate_col = _DT_GATE_COLOUR.get(_nm_gate, T["text2"])
            _nm_gate_lbl = _DT_GATE_LABEL.get(_nm_gate, _nm_gate[:12] or "—")
            _nm_dir_col  = T["green"] if _nm_dir == "LONG" else T["red"] if _nm_dir == "SHORT" else T["muted"]
            _nm_conf_s   = f"{float(_nm_conf):.3f}" if pd.notna(_nm_conf) else "—"
            _nm_z_s      = f"{float(_nm_z):+.2f}"  if pd.notna(_nm_z)    else "—"
            _nm_p32_s    = f"{float(_nm_p32):.3f}" if pd.notna(_nm_p32)  else "—"
            _nm_llm_s    = _esc(_nm_llm[:14]) if _nm_llm and _nm_llm != "PASS" else "—"
            _nm_llm_col  = (T["red"] if "VETO" in _nm_llm or "CATASTROPHE" in _nm_llm
                            else T["yellow"] if _nm_llm_s != "—" else T["muted"])

            # [TRACK32-RA2] Regime colour
            _nm_reg_u   = _nm_regime.upper()
            _nm_reg_col = (T["green"]  if _nm_reg_u == "BULL" else
                           T["red"]    if _nm_reg_u == "BEAR" else
                           T["yellow"] if _nm_reg_u == "CHOP" else T["muted"])
            _nm_reg_s   = _esc(_nm_reg_u) if _nm_reg_u else "—"

            # [TRACK32-RA2] Kelly
            _nm_kf_s = f"{float(_nm_kf):.3f}" if _nm_kf is not None and pd.notna(_nm_kf) else "—"

            # [TRACK32-RA2] Size
            try:
                _nm_sz_f = float(_nm_sz) if _nm_sz is not None and pd.notna(_nm_sz) else None
            except (TypeError, ValueError):
                _nm_sz_f = None
            _nm_sz_s  = f"${_nm_sz_f:.0f}" if _nm_sz_f is not None and _nm_sz_f > 0 else "—"
            _nm_sz_col = T["text2"] if _nm_sz_f and _nm_sz_f > 0 else T["muted"]

            # [TRACK32-RA2] Inline badges for gate cell: risk_off + conviction_mult
            _nm_gate_badges = ""
            try:
                if _nm_roff is not None and pd.notna(_nm_roff) and int(_nm_roff):
                    _nm_gate_badges += (
                        f'<span class="ot-badge" style="color:{T["orange"]};'
                        f'background:{T["orange"]}18;border:1px solid {T["orange"]}40;'
                        f'font-size:0.50rem;margin-left:3px;">R-OFF</span>'
                    )
            except (TypeError, ValueError):
                pass
            try:
                _nm_cm_f = float(_nm_cm) if _nm_cm is not None and pd.notna(_nm_cm) else None
                if _nm_cm_f is not None and _nm_cm_f > 1.0:
                    _nm_gate_badges += (
                        f'<span class="ot-badge" style="color:{T["cyan"]};'
                        f'background:{T["cyan"]}18;border:1px solid {T["cyan"]}40;'
                        f'font-size:0.50rem;margin-left:3px;">Cx&nbsp;{_nm_cm_f:.1f}x</span>'
                    )
            except (TypeError, ValueError):
                pass

            nm_rows_html += (
                f'<tr style="border-bottom:1px solid {T["border"]}22;">'
                f'<td style="padding:3px 6px;color:{T["text2"]};font-size:0.58rem;white-space:nowrap;">{_nm_ts_str}</td>'
                f'<td style="padding:3px 6px;color:{_nm_out_col};font-weight:700;font-size:0.62rem;">{_esc(_nm_out[:8])}</td>'
                f'<td style="padding:3px 6px;white-space:nowrap;">'
                f'<span class="ot-badge" style="color:{_nm_gate_col};background:{_nm_gate_col}18;'
                f'border:1px solid {_nm_gate_col}40;">{_esc(_nm_gate_lbl)}</span>'
                f'{_nm_gate_badges}</td>'
                f'<td style="padding:3px 6px;color:{_nm_dir_col};font-weight:700;font-size:0.62rem;">{_esc(_nm_dir[:5] or "—")}</td>'
                f'<td style="padding:3px 6px;color:{_nm_reg_col};font-weight:700;font-size:0.60rem;">{_nm_reg_s}</td>'
                f'<td style="padding:3px 6px;color:{T["text2"]};font-family:{T["mono"]};font-size:0.62rem;">{_nm_conf_s}</td>'
                f'<td style="padding:3px 6px;color:{T["text2"]};font-family:{T["mono"]};font-size:0.62rem;">{_nm_z_s}</td>'
                f'<td style="padding:3px 6px;color:{T["text2"]};font-family:{T["mono"]};font-size:0.60rem;">{_nm_kf_s}</td>'
                f'<td style="padding:3px 6px;color:{_nm_sz_col};font-family:{T["mono"]};font-size:0.60rem;">{_nm_sz_s}</td>'
                f'<td style="padding:3px 6px;color:{_nm_llm_col};font-size:0.60rem;">{_nm_llm_s}</td>'
                f'<td style="padding:3px 6px;color:{T["text2"]};font-family:{T["mono"]};font-size:0.60rem;">{_nm_p32_s}</td>'
                f'<td style="padding:3px 6px;color:{T["text2"]};font-size:0.58rem;max-width:180px;'
                f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_nm_reason}</td>'
                f'</tr>'
            )

        nm_hdr_labels = ["TIME", "OUTCOME", "GATE", "DIR", "REGIME", "CONF", "Z", "KELLY", "SIZE", "LLM", "P32", "REASON"]
        nm_hdr = "".join(
            f'<th style="padding:4px 6px;font-size:0.53rem;letter-spacing:0.09em;'
            f'color:{T["muted"]};text-transform:uppercase;white-space:nowrap;'
            f'border-bottom:1px solid {T["border"]};">{lbl}</th>'
            for lbl in nm_hdr_labels
        )
        st.markdown(
            f'<div class="ot-tile ot-scroll" style="padding:8px;overflow-x:auto;">'
            f'<table style="width:100%;border-collapse:collapse;font-size:0.66rem;'
            f'font-family:{T["mono"]};">'
            f'<thead><tr>{nm_hdr}</tr></thead>'
            f'<tbody>{nm_rows_html}</tbody></table></div>',
            unsafe_allow_html=True,
        )
        st.caption(
            f"{len(nm_df)} near-miss decision(s) for {_norm_sym(_nm_sym)} "
            f"in 5 min before entry · approximate window · decision_trace table"
        )

    # ── [TRACK26-LC3] Lifecycle expander — entry & exit decision context ────
    # Placed strictly after the Track11 near-miss expander.  Uses its own
    # trade selector (key="rt_lc_trade") so no widget-key collision occurs.
    # Shown only when decision_trace exists and there are selectable trades
    # with a usable close_ts.  Fully wrapped in standalone try/except so
    # any failure cannot break render_trade_replay().
    try:
        if "decision_trace" in _get_known_tables() and _nm_options:
            # Build lifecycle selector — same label format as near-miss but
            # only includes rows that also have a valid close_ts and direction.
            _lc_opts: list = []   # display labels
            _lc_rows: list = []   # (symbol, direction, entry_ts, close_ts)
            _has_close_ts = (
                "close_ts" in _display.columns
                and _display["close_ts"].notna().any()
            )
            _has_lc_dir = (
                "entry_direction" in _display.columns
            )
            if _has_close_ts:
                for _, _lr in _display.iterrows():
                    _lr_sym = str(_lr.get("symbol") or "").strip()
                    _lr_ets = _lr.get("entry_ts")
                    _lr_cts = _lr.get("close_ts")
                    if not _lr_sym:
                        continue
                    if not pd.notna(_lr_ets) or not pd.notna(_lr_cts):
                        continue
                    try:
                        _lr_ets_f = float(_lr_ets)
                        _lr_cts_f = float(_lr_cts)
                    except (TypeError, ValueError):
                        continue
                    _lr_dir = str(_lr.get("entry_direction") or "").upper() if _has_lc_dir else ""
                    _lr_edt = _lr.get("entry_dt")
                    try:
                        _lr_edt_s = _lr_edt.strftime("%m-%d %H:%M") if pd.notna(_lr_edt) else "?"
                    except Exception:
                        _lr_edt_s = "?"
                    _lr_pnl  = _safe_float(_lr.get("realized_usd"))
                    _lr_sign = "+" if _lr_pnl > 0 else ("−" if _lr_pnl < 0 else "·")
                    _lc_opts.append(
                        f"{_norm_sym(_lr_sym)} @ {_lr_edt_s} [{_lr_sign}]"
                    )
                    _lc_rows.append((_lr_sym, _lr_dir, _lr_ets_f, _lr_cts_f))

            if _lc_opts:
                with st.expander(
                    "📋 Trade lifecycle — entry & exit decision context",
                    expanded=False,
                ):
                    st.markdown(
                        f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                        f'color:{T["muted"]};margin-bottom:6px;">'
                        f'For the selected trade, searches decision_trace for the '
                        f'ADMITTED row within {_LC26_ENTRY_LOOKBACK_SECS}s before entry '
                        f'and the CLOSED row within \u00b1{_LC26_CLOSE_SEARCH_SECS}s of close. '
                        f'Time-window match only \u2014 no fuzzy heuristics. '
                        f'Read-only.</div>',
                        unsafe_allow_html=True,
                    )

                    _lc_sel = st.selectbox(
                        "Trade", options=_lc_opts, index=0,
                        key="rt_lc_trade", label_visibility="collapsed",
                    )

                    try:
                        _lc_idx = _lc_opts.index(_lc_sel)
                        _lc_sym, _lc_dir, _lc_ets, _lc_cts = _lc_rows[_lc_idx]
                    except (ValueError, IndexError):
                        _lc_sym, _lc_dir, _lc_ets, _lc_cts = "", "", 0.0, 0.0

                    if not _lc_sym or not _lc_ets or not _lc_cts:
                        st.markdown(
                            f'<div class="ot-tile" style="padding:8px;text-align:center;">'
                            f'<span style="color:{T["muted"]};font-family:{T["mono"]};'
                            f'font-size:0.65rem;">SELECT A TRADE ABOVE</span></div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        _lc = _load_trade_lifecycle(
                            symbol=_lc_sym,
                            direction=_lc_dir,
                            entry_ts=_lc_ets,
                            close_ts=_lc_cts,
                        )

                        # ── Shared row-render helpers (hoisted — used by both columns) ─
                        def _vfmt(v, fmt="{:.3f}") -> str:
                            try:
                                return fmt.format(float(v)) if v is not None else "\u2014"
                            except (TypeError, ValueError):
                                return "\u2014"

                        def _lc_row(lbl, val_html) -> str:
                            return (
                                f'<tr>'
                                f'<td style="color:{T["muted"]};padding:1px 6px 1px 0;'
                                f'white-space:nowrap;font-size:0.57rem;">'
                                f'{_esc(lbl)}</td>'
                                f'<td style="padding:1px 0;">{val_html}</td>'
                                f'</tr>'
                            )

                        _col_ent, _col_cls = st.columns(2, gap="small")

                        # ── Left: ENTRY CONTEXT ──────────────────────────────
                        with _col_ent:
                            st.markdown(
                                f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                                f'color:{T["muted"]};letter-spacing:0.08em;margin-bottom:4px;">'
                                f'ENTRY CONTEXT</div>',
                                unsafe_allow_html=True,
                            )
                            if _lc.get("entry_found") and _lc.get("entry_ctx"):
                                _ec = _lc["entry_ctx"]

                                # ts offset vs entry_ts
                                _ec_offset = _ec["ts"] - _lc_ets
                                _ec_off_s  = f"{_ec_offset:+.0f}s vs entry"

                                # source badge
                                _ec_src = _ec.get("source_path", "")
                                if _ec_src == "entry":
                                    _ec_src_lbl, _ec_src_col = "ENT", T["muted"]
                                elif _ec_src == "express":
                                    _ec_src_lbl, _ec_src_col = "EXP", T["cyan"]
                                else:
                                    _ec_src_lbl, _ec_src_col = "?", T["muted"]

                                # gate
                                _ec_gn  = _ec.get("gate_name", "")
                                _ec_glbl = _DT_GATE_LABEL.get(_ec_gn, _ec_gn[:14] or "PASS")
                                _ec_gcol = _DT_GATE_COLOUR.get(_ec_gn, T["green"])

                                # numeric fields
                                _ec_conf = _ec.get("confidence")
                                _ec_z    = _ec.get("z_score")
                                _ec_kf   = _ec.get("kelly_f")
                                _ec_sz   = _ec.get("sizing_usd")
                                _ec_cm   = _ec.get("conviction_mult")
                                _ec_ro   = _ec.get("risk_off", False)
                                _ec_llm  = _ec.get("llm_verdict", "")

                                _ec_html = (
                                    f'<div class="ot-tile" style="padding:8px 10px;">'
                                    f'<div style="margin-bottom:4px;">'
                                    f'<span style="color:{T["text2"]};font-size:0.59rem;">'
                                    f'{_esc(_ec_off_s)}</span>'
                                    f'&nbsp;<span class="ot-badge" style="color:{_ec_src_col};'
                                    f'background:{_ec_src_col}18;border:1px solid {_ec_src_col}40;">'
                                    f'{_esc(_ec_src_lbl)}</span>'
                                    f'</div>'
                                    f'<table style="width:100%;border-collapse:collapse;'
                                    f'font-size:0.62rem;font-family:{T["mono"]};line-height:1.55;">'
                                )

                                _ec_html += _lc_row(
                                    "CONF",
                                    f'<span style="color:{T["text2"]};">{_vfmt(_ec_conf)}</span>',
                                )
                                _ec_html += _lc_row(
                                    "Z",
                                    f'<span style="color:{T["text2"]};">{_vfmt(_ec_z, "{:+.2f}")}</span>',
                                )
                                _ec_html += _lc_row(
                                    "KELLY",
                                    f'<span style="color:{T["text2"]};">{_vfmt(_ec_kf)}</span>',
                                )
                                if _ec_sz is not None:
                                    try:
                                        _sz_s = f"${float(_ec_sz):.0f}"
                                    except (TypeError, ValueError):
                                        _sz_s = "\u2014"
                                    _ec_html += _lc_row(
                                        "SIZE",
                                        f'<span style="color:{T["text2"]};">{_esc(_sz_s)}</span>',
                                    )
                                _ec_html += _lc_row(
                                    "GATE",
                                    f'<span class="ot-badge" style="color:{_ec_gcol};'
                                    f'background:{_ec_gcol}18;border:1px solid {_ec_gcol}40;">'
                                    f'{_esc(_ec_glbl)}</span>',
                                )
                                if _ec_llm and _ec_llm not in ("", "PASS"):
                                    _llm_c = (
                                        T["red"] if ("VETO" in _ec_llm or "CATASTROPHE" in _ec_llm)
                                        else T["yellow"]
                                    )
                                    _ec_html += _lc_row(
                                        "LLM",
                                        f'<span style="color:{_llm_c};">{_esc(_ec_llm[:18])}</span>',
                                    )
                                if _ec_ro:
                                    _ec_html += _lc_row(
                                        "RISK OFF",
                                        f'<span style="color:{T["yellow"]};font-weight:700;">YES</span>',
                                    )
                                if _ec_cm is not None:
                                    try:
                                        _cm_f = float(_ec_cm)
                                        if _cm_f > 1.0:
                                            _ec_html += _lc_row(
                                                "CONVICTION",
                                                f'<span style="color:{T["cyan"]};">{_cm_f:.2f}x</span>',
                                            )
                                    except (TypeError, ValueError):
                                        pass

                                _ec_html += "</table></div>"
                                st.markdown(_ec_html, unsafe_allow_html=True)
                            else:
                                st.markdown(
                                    f'<div class="ot-tile" style="padding:8px 10px;">'
                                    f'<span style="color:{T["muted"]};font-family:{T["mono"]};'
                                    f'font-size:0.62rem;">— ADMITTED row not found in '
                                    f'{_LC26_ENTRY_LOOKBACK_SECS}s window</span></div>',
                                    unsafe_allow_html=True,
                                )

                        # ── Right: EXIT CONTEXT ──────────────────────────────
                        with _col_cls:
                            st.markdown(
                                f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                                f'color:{T["muted"]};letter-spacing:0.08em;margin-bottom:4px;">'
                                f'EXIT CONTEXT</div>',
                                unsafe_allow_html=True,
                            )
                            if _lc.get("close_found") and _lc.get("close_ctx"):
                                _cc = _lc["close_ctx"]

                                _cc_offset = _cc["ts"] - _lc_cts
                                _cc_off_s  = f"{_cc_offset:+.0f}s vs close"

                                _cc_gn   = _cc.get("gate_name", "")
                                _cc_glbl = _DT_GATE_LABEL.get(_cc_gn, _cc_gn[:18] or "\u2014")
                                _cc_gcol = _DT_GATE_COLOUR.get(_cc_gn, T["yellow"])

                                _cc_pnl  = _cc.get("pnl_pct")
                                _cc_hld  = _cc.get("hold_time_secs")

                                if _cc_pnl is not None:
                                    _pnl_col = T["green"] if _cc_pnl >= 0 else T["red"]
                                    _pnl_s   = f'<span style="color:{_pnl_col};font-weight:700;">{_cc_pnl:+.2f}%</span>'
                                else:
                                    _pnl_s   = f'<span style="color:{T["muted"]};">\u2014</span>'

                                _hld_s = (
                                    _fmt_hold(_cc_hld)
                                    if _cc_hld is not None
                                    else "\u2014"
                                )

                                _cc_html = (
                                    f'<div class="ot-tile" style="padding:8px 10px;">'
                                    f'<div style="margin-bottom:4px;">'
                                    f'<span style="color:{T["text2"]};font-size:0.59rem;">'
                                    f'{_esc(_cc_off_s)}</span>'
                                    f'</div>'
                                    f'<table style="width:100%;border-collapse:collapse;'
                                    f'font-size:0.62rem;font-family:{T["mono"]};line-height:1.55;">'
                                    + _lc_row(
                                        "EXIT GATE",
                                        f'<span class="ot-badge" style="color:{_cc_gcol};'
                                        f'background:{_cc_gcol}18;border:1px solid {_cc_gcol}40;">'
                                        f'{_esc(_cc_glbl)}</span>',
                                    )
                                    + _lc_row("PnL", _pnl_s)
                                    + _lc_row(
                                        "HOLD",
                                        f'<span style="color:{T["text2"]};">{_esc(_hld_s)}</span>',
                                    )
                                    + "</table></div>"
                                )
                                st.markdown(_cc_html, unsafe_allow_html=True)
                            else:
                                st.markdown(
                                    f'<div class="ot-tile" style="padding:8px 10px;">'
                                    f'<span style="color:{T["muted"]};font-family:{T["mono"]};'
                                    f'font-size:0.62rem;">— CLOSED row not found in '
                                    f'\u00b1{_LC26_CLOSE_SEARCH_SECS}s window</span></div>',
                                    unsafe_allow_html=True,
                                )

    except Exception:
        log.error("[TRACK26-LC3] lifecycle expander error", exc_info=True)
    # ── [/TRACK26-LC3] ───────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════════════════
# LOADER: OPERATOR REPORT — DECISION-TRACE SUMMARY  [TRACK13]
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=CACHE_FAST)
def _load_report_dt_summary(
    days_back: int = 7,
    symbol_filter: str = "ALL",
) -> dict:
    """[TRACK13-RPT1] Aggregate admission-funnel summary for the operator report.

    Queries the decision_trace table for the given lookback window and returns
    a compact summary dict.  Designed exclusively for render_operator_report();
    callers must not depend on dict keys beyond what is documented here.

    Parameters
    ----------
    days_back : int
        Number of calendar days to include (relative to now).
    symbol_filter : str
        "ALL" to include every symbol, or a specific symbol string.

    Returns
    -------
    dict with keys:
        total       : int  — total rows in window
        n_admitted  : int  — rows where outcome == 'ADMITTED'
        n_blocked   : int  — rows where outcome == 'BLOCKED'
        n_shadow    : int  — rows where outcome == 'SHADOW'
        top_gates   : list of (gate_key: str, label: str, colour: str, count: int)
                      up to 5 entries, BLOCKED rows only, descending by count
    {} on missing table, empty result, or any query error.
    Read-only.  No DB writes.  No schema changes.  Parameterized SQL only.
    """
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return {}
    try:
        cutoff_ts = time.time() - days_back * 86400.0
        params: list = [cutoff_ts]
        where_parts = ["ts >= ?"]
        if symbol_filter and symbol_filter != "ALL":
            where_parts.append("symbol = ?")
            params.append(symbol_filter)
        where_clause = " AND ".join(where_parts)
        sql = (
            "SELECT outcome, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            " ORDER BY ts DESC LIMIT 2000"
        )
        df = _query(sql, tuple(params))
        if df is None or df.empty:
            return {}

        total      = len(df)
        n_admitted = int((df["outcome"] == "ADMITTED").sum())
        n_blocked  = int((df["outcome"] == "BLOCKED").sum())
        n_shadow   = int((df["outcome"] == "SHADOW").sum())

        gate_counts: dict = {}
        blocked_mask = df["outcome"] == "BLOCKED"
        for gn in df.loc[blocked_mask, "gate_name"].dropna():
            gate_counts[gn] = gate_counts.get(gn, 0) + 1
        top_gates = []
        for gk, cnt in sorted(gate_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            lbl = _DT_GATE_LABEL.get(gk, gk[:12] or "—")
            col = _DT_GATE_COLOUR.get(gk, T["text2"])
            top_gates.append((gk, lbl, col, cnt))

        return {
            "total":      total,
            "n_admitted": n_admitted,
            "n_blocked":  n_blocked,
            "n_shadow":   n_shadow,
            "top_gates":  top_gates,
        }
    except Exception:
        log.error("[TRACK13-RPT1] _load_report_dt_summary: query failed", exc_info=True)
        return {}


# [TRACK19-AGG] Forward-look window for aggregate admission matching.
# Kept separate from _CORR18_ADMITTED_WINDOW_SECS so Track 18 and Track 19
# can be adjusted independently.  Uses the same 10-minute default.
_CORR19_ADMITTED_WINDOW_SECS: int = 600   # seconds to search forward for a later ADMITTED row


def _load_gate_aggregate(
    days_back:     int = 7,
    symbol_filter: str = "ALL",
) -> "list[dict]":
    """[TRACK19-AGG1] Per-gate aggregate admission-rate and outcome statistics.

    For the given lookback window, loads BLOCKED/SHADOW entry+express rows and
    ADMITTED/CLOSED entry+express rows from decision_trace, then computes
    per-gate aggregate statistics entirely in Python (no GROUP BY SQL — avoids
    N serial sub-queries while keeping the forward-match logic consistent with
    Track 18's exact earliest-match semantics).

    Matching rule (mirrors Track 18 exactly):
      For each BLOCKED row, the earliest ADMITTED row for the same
      (symbol, direction) with ts ∈ (block_ts, block_ts +
      _CORR19_ADMITTED_WINDOW_SECS) is the match.  If found, the earliest
      CLOSED row for the same symbol with ts ∈ (admitted_ts,
      admitted_ts + _CORR18_CLOSE_WINDOW_SECS) contributes pnl_pct and
      hold_time_secs to the per-gate mean.

    Two-tier query fallback:
      Tier-1: source_path IN ('entry','express') filter + pnl_pct +
              hold_time_secs columns (Track-16/17+ DBs).
      Tier-2: no source_path filter, NULL AS pnl_pct / hold_time_secs
              (pre-Track-16 / pre-Track-17 DBs).

    Returns
    -------
    list of dict, sorted by n_blocked descending.  Each dict has:
        gate_key      : str            — raw gate_name value
        label         : str            — human label from _DT_GATE_LABEL
        colour        : str            — hex colour from _DT_GATE_COLOUR
        n_blocked     : int            — BLOCKED rows for this gate
        n_later_adm   : int            — of those, how many had a later ADMITTED
        adm_rate      : float          — n_later_adm / n_blocked
        n_with_close  : int            — of the admitted subset, how many had close
        mean_pnl_pct  : float | None   — mean pnl_pct across closes; None on
                                         pre-Track-17 DB or no closes found
        mean_hold_secs: float | None   — mean hold_time_secs; None when absent

    Returns [] on missing table, empty result, or any exception.
    Read-only.  Parameterized SQL only.  No DB writes.  No schema changes.
    No filesystem writes.  Never raises into caller.
    """
    _EMPTY: list = []
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return _EMPTY
    try:
        cutoff_ts = time.time() - days_back * 86400.0
        params_base: list = [cutoff_ts]
        where_parts = ["ts >= ?"]
        if symbol_filter and symbol_filter != "ALL":
            where_parts.append("symbol = ?")
            params_base.append(symbol_filter)
        where_clause = " AND ".join(where_parts)

        # ── Query A: BLOCKED + SHADOW rows (blocked candidates) ───────────────
        # Tier-1: Track-16+ DB — filter to entry/express source_path
        _sql_blk_src = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            "   AND source_path IN ('entry', 'express')"
            " ORDER BY ts ASC"
        )
        # Tier-2: pre-Track-16 DB — no source_path column
        _sql_blk_nosrc = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            " ORDER BY ts ASC"
        )

        _blk_df: "pd.DataFrame | None" = None
        _has_src: bool = False
        for _blk_sql in (_sql_blk_src, _sql_blk_nosrc):
            try:
                _bdf = _query(_blk_sql, tuple(params_base))
                if _bdf is not None:
                    _blk_df = _bdf
                    _has_src = (_blk_sql is _sql_blk_src)
                    break
            except Exception as _be:
                log.debug("[TRACK19-AGG1] blocked query fallback: %s", _be)
                continue

        if _blk_df is None or _blk_df.empty:
            return _EMPTY

        # ── Query B: ADMITTED rows (match targets) ─────────────────────────────
        # Fetch all ADMITTED rows in the same window +  _CORR19_ADMITTED_WINDOW_SECS
        # buffer so that blocks near the window edge can still find matches.
        _adm_hi = time.time()  # upper bound: now (no future rows)
        _adm_params_base: list = [cutoff_ts, _adm_hi]
        _adm_where = "ts >= ? AND ts <= ? AND outcome = 'ADMITTED'"
        if symbol_filter and symbol_filter != "ALL":
            _adm_where += " AND symbol = ?"
            _adm_params_base.append(symbol_filter)

        _sql_adm_src = (
            "SELECT ts, symbol, direction"
            f" FROM decision_trace WHERE {_adm_where}"
            "   AND source_path IN ('entry', 'express')"
            " ORDER BY ts ASC"
        )
        _sql_adm_nosrc = (
            "SELECT ts, symbol, direction"
            f" FROM decision_trace WHERE {_adm_where}"
            " ORDER BY ts ASC"
        )

        _adm_df: "pd.DataFrame | None" = None
        _adm_sqls = (_sql_adm_src, _sql_adm_nosrc) if _has_src else (_sql_adm_nosrc,)
        for _adm_sql in _adm_sqls:
            try:
                _adf = _query(_adm_sql, tuple(_adm_params_base))
                if _adf is not None:
                    _adm_df = _adf
                    break
            except Exception as _ae:
                log.debug("[TRACK19-AGG1] admitted query fallback: %s", _ae)
                continue

        if _adm_df is None:
            _adm_df = pd.DataFrame(columns=["ts", "symbol", "direction"])

        # ── Query C: CLOSED rows (outcome context) ─────────────────────────────
        # Build the close WHERE clause directly (same time bounds, different outcome filter).
        _clo_where_parts = ["ts >= ?", "ts <= ?"]
        _clo_params_list: list = [cutoff_ts, _adm_hi]
        if symbol_filter and symbol_filter != "ALL":
            _clo_where_parts.append("symbol = ?")
            _clo_params_list.append(symbol_filter)
        _clo_where = " AND ".join(_clo_where_parts)

        # Tier-1: Track-17+ DB with pnl_pct + hold_time_secs via source_path='close'
        _sql_clo_full = (
            "SELECT ts, symbol, pnl_pct, hold_time_secs"
            " FROM decision_trace WHERE " + _clo_where +
            "   AND source_path = 'close'"
            " ORDER BY ts ASC"
        )
        # Tier-2: pre-Track-17 DB — outcome='CLOSED', NULL pnl/hold
        _sql_clo_fallback = (
            "SELECT ts, symbol, NULL AS pnl_pct, NULL AS hold_time_secs"
            " FROM decision_trace WHERE " + _clo_where +
            "   AND outcome = 'CLOSED'"
            " ORDER BY ts ASC"
        )

        _clo_params = tuple(_clo_params_list)
        _clo_df: "pd.DataFrame | None" = None
        for _clo_sql in (_sql_clo_full, _sql_clo_fallback):
            try:
                _cdf = _query(_clo_sql, _clo_params)
                if _cdf is not None:
                    _clo_df = _cdf
                    break
            except Exception as _ce:
                log.debug("[TRACK19-AGG1] close query fallback: %s", _ce)
                continue

        if _clo_df is None:
            _clo_df = pd.DataFrame(columns=["ts", "symbol", "pnl_pct", "hold_time_secs"])

        # ── Coerce timestamp columns to float ─────────────────────────────────
        for _col in ("ts",):
            if _col in _blk_df.columns:
                _blk_df[_col] = pd.to_numeric(_blk_df[_col], errors="coerce")
            if _col in _adm_df.columns:
                _adm_df[_col] = pd.to_numeric(_adm_df[_col], errors="coerce")
            if _col in _clo_df.columns:
                _clo_df[_col] = pd.to_numeric(_clo_df[_col], errors="coerce")
        if "pnl_pct" in _clo_df.columns:
            _clo_df["pnl_pct"] = pd.to_numeric(_clo_df["pnl_pct"], errors="coerce")
        if "hold_time_secs" in _clo_df.columns:
            _clo_df["hold_time_secs"] = pd.to_numeric(_clo_df["hold_time_secs"], errors="coerce")

        _blk_df = _blk_df.dropna(subset=["ts", "symbol", "direction"])
        _adm_df = _adm_df.dropna(subset=["ts", "symbol", "direction"])
        _clo_df = _clo_df.dropna(subset=["ts", "symbol"])

        # ── Per-gate aggregation ───────────────────────────────────────────────
        # Build admit-match lookup: for each (symbol, direction), sorted ADMITTED ts list
        _adm_lookup: dict = {}
        for _ar in _adm_df.itertuples(index=False):
            _key = (str(_ar.symbol), str(_ar.direction))
            _adm_lookup.setdefault(_key, []).append(float(_ar.ts))
        # (lists are already sorted ASC because query ordered ASC)

        # Build close lookup: for each symbol, sorted CLOSED ts list with pnl/hold
        _clo_lookup: dict = {}
        for _cr in _clo_df.itertuples(index=False):
            _s = str(_cr.symbol)
            _clo_lookup.setdefault(_s, []).append((
                float(_cr.ts),
                getattr(_cr, "pnl_pct", None),
                getattr(_cr, "hold_time_secs", None),
            ))

        # Accumulate per-gate stats
        import bisect as _bisect
        _gate_stats: dict = {}  # gate_key → {n_blk, n_adm, pnl_list, hold_list, n_close}

        for _br in _blk_df.itertuples(index=False):
            _gk  = str(getattr(_br, "gate_name", "") or "")
            _sym = str(_br.symbol)
            _dir = str(_br.direction)
            _bts = float(_br.ts)

            if not _gk:
                continue

            _gs = _gate_stats.setdefault(_gk, {
                "n_blk": 0, "n_adm": 0,
                "n_close": 0, "pnl_list": [], "hold_list": [],
            })
            _gs["n_blk"] += 1

            # Find earliest ADMITTED ts > _bts within window
            _adm_ts_list = _adm_lookup.get((_sym, _dir), [])
            _lo_idx = _bisect.bisect_right(_adm_ts_list, _bts)
            _hi_bound = _bts + _CORR19_ADMITTED_WINDOW_SECS
            _adm_ts = None
            if _lo_idx < len(_adm_ts_list) and _adm_ts_list[_lo_idx] <= _hi_bound:
                _adm_ts = _adm_ts_list[_lo_idx]
                _gs["n_adm"] += 1

            if _adm_ts is None:
                continue

            # Find earliest CLOSED ts > _adm_ts within close window
            _clo_ts_list = _clo_lookup.get(_sym, [])   # list of (ts, pnl, hold)
            _clo_sorted_ts = [_ct[0] for _ct in _clo_ts_list]
            _clo_lo = _bisect.bisect_right(_clo_sorted_ts, _adm_ts)
            _clo_hi = _adm_ts + _CORR18_CLOSE_WINDOW_SECS
            if _clo_lo < len(_clo_ts_list) and _clo_ts_list[_clo_lo][0] <= _clo_hi:
                _pnl_v, _hld_v = _clo_ts_list[_clo_lo][1], _clo_ts_list[_clo_lo][2]
                _gs["n_close"] += 1
                if _pnl_v is not None and _pnl_v == _pnl_v:  # NaN guard
                    _gs["pnl_list"].append(float(_pnl_v))
                if _hld_v is not None and _hld_v == _hld_v:
                    _gs["hold_list"].append(float(_hld_v))

        if not _gate_stats:
            return _EMPTY

        # Build result list
        result: list = []
        for _gk, _gs in _gate_stats.items():
            _nb  = _gs["n_blk"]
            _na  = _gs["n_adm"]
            _nc  = _gs["n_close"]
            _pls = _gs["pnl_list"]
            _hls = _gs["hold_list"]
            result.append({
                "gate_key":       _gk,
                "label":          _DT_GATE_LABEL.get(_gk, _gk[:12] or "—"),
                "colour":         _DT_GATE_COLOUR.get(_gk, T["text2"]),
                "n_blocked":      _nb,
                "n_later_adm":    _na,
                "adm_rate":       _na / _nb if _nb > 0 else 0.0,
                "n_with_close":   _nc,
                "mean_pnl_pct":   (sum(_pls) / len(_pls)) if _pls else None,
                "mean_hold_secs": (sum(_hls) / len(_hls)) if _hls else None,
            })

        result.sort(key=lambda x: x["n_blocked"], reverse=True)
        return result

    except Exception:
        log.error("[TRACK19-AGG1] _load_gate_aggregate: unexpected error", exc_info=True)
        return _EMPTY


# ── [TRACK20-BRK1] Per-symbol / direction gate breakdown loader ───────────────

@st.cache_data(ttl=CACHE_FAST)
def _load_gate_breakdown(
    days_back:     int = 7,
    symbol_filter: str = "ALL",
) -> dict:
    """[TRACK20-BRK1] Per-gate breakdown by symbol and direction.

    Extends Track 19's aggregate matching logic to additionally bucket each
    blocked row by (gate_key, symbol) and (gate_key, direction), producing
    per-symbol and per-direction admission-rate + outcome tables.

    Matching rule: identical to Track 19 (_load_gate_aggregate) exactly —
      same symbol + direction match, same earliest-later-ADMITTED bisect,
      same earliest-later-CLOSED-after-admission bisect, same windows.

    Two-tier query fallback: identical to Track 19.
      Tier-1: source_path IN ('entry','express') + pnl_pct + hold_time_secs
              (Track-16/17+ DBs).
      Tier-2: no source_path filter, NULL pnl/hold (pre-Track-16 DBs).

    Parameters
    ----------
    days_back     : int  — calendar days to include (relative to now)
    symbol_filter : str  — "ALL" or exact symbol string

    Returns
    -------
    dict with keys:
        "by_gate"      : list[dict] — same structure as _load_gate_aggregate()
                         output; one dict per gate_key, sorted by n_blocked DESC.
                         Fields: gate_key, label, colour, n_blocked, n_later_adm,
                         adm_rate, n_with_close, mean_pnl_pct, mean_hold_secs.
        "by_symbol"    : dict[gate_key → list[dict]] — per-symbol breakdown.
                         Each inner dict: symbol, n_blocked, n_later_adm,
                         adm_rate, n_with_close, mean_pnl_pct, mean_hold_secs.
                         Sorted by n_blocked DESC within each gate.
        "by_direction" : dict[gate_key → dict[direction → dict]] — direction
                         breakdown (LONG / SHORT / other normalised to upper).
                         Each leaf dict: n_blocked, n_later_adm, adm_rate,
                         n_with_close, mean_pnl_pct, mean_hold_secs.
    {} on missing table, empty result, or any exception.
    Read-only.  Parameterized SQL only.  No DB writes.  No schema changes.
    No filesystem writes.  Never raises into caller.
    """
    _EMPTY: dict = {}
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return _EMPTY
    try:
        import bisect as _bisect

        cutoff_ts = time.time() - days_back * 86400.0
        params_base: list = [cutoff_ts]
        where_parts = ["ts >= ?"]
        if symbol_filter and symbol_filter != "ALL":
            where_parts.append("symbol = ?")
            params_base.append(symbol_filter)
        where_clause = " AND ".join(where_parts)

        # ── Query A: BLOCKED + SHADOW rows ────────────────────────────────────
        _sql_blk_src = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            "   AND source_path IN ('entry', 'express')"
            " ORDER BY ts ASC"
        )
        _sql_blk_nosrc = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            " ORDER BY ts ASC"
        )

        _blk_df: "pd.DataFrame | None" = None
        _has_src: bool = False
        for _blk_sql in (_sql_blk_src, _sql_blk_nosrc):
            try:
                _bdf = _query(_blk_sql, tuple(params_base))
                if _bdf is not None:
                    _blk_df = _bdf
                    _has_src = (_blk_sql is _sql_blk_src)
                    break
            except Exception as _be:
                log.debug("[TRACK20-BRK1] blocked query fallback: %s", _be)
                continue

        if _blk_df is None or _blk_df.empty:
            return _EMPTY

        # ── Query B: ADMITTED rows ────────────────────────────────────────────
        _adm_hi = time.time()
        _adm_params_base: list = [cutoff_ts, _adm_hi]
        _adm_where = "ts >= ? AND ts <= ? AND outcome = 'ADMITTED'"
        if symbol_filter and symbol_filter != "ALL":
            _adm_where += " AND symbol = ?"
            _adm_params_base.append(symbol_filter)

        _sql_adm_src = (
            "SELECT ts, symbol, direction"
            f" FROM decision_trace WHERE {_adm_where}"
            "   AND source_path IN ('entry', 'express')"
            " ORDER BY ts ASC"
        )
        _sql_adm_nosrc = (
            "SELECT ts, symbol, direction"
            f" FROM decision_trace WHERE {_adm_where}"
            " ORDER BY ts ASC"
        )

        _adm_df: "pd.DataFrame | None" = None
        _adm_sqls = (_sql_adm_src, _sql_adm_nosrc) if _has_src else (_sql_adm_nosrc,)
        for _adm_sql in _adm_sqls:
            try:
                _adf = _query(_adm_sql, tuple(_adm_params_base))
                if _adf is not None:
                    _adm_df = _adf
                    break
            except Exception as _ae:
                log.debug("[TRACK20-BRK1] admitted query fallback: %s", _ae)
                continue

        if _adm_df is None:
            _adm_df = pd.DataFrame(columns=["ts", "symbol", "direction"])

        # ── Query C: CLOSED rows ──────────────────────────────────────────────
        _clo_where_parts = ["ts >= ?", "ts <= ?"]
        _clo_params_list: list = [cutoff_ts, _adm_hi]
        if symbol_filter and symbol_filter != "ALL":
            _clo_where_parts.append("symbol = ?")
            _clo_params_list.append(symbol_filter)
        _clo_where = " AND ".join(_clo_where_parts)

        _sql_clo_full = (
            "SELECT ts, symbol, pnl_pct, hold_time_secs"
            " FROM decision_trace WHERE " + _clo_where +
            "   AND source_path = 'close'"
            " ORDER BY ts ASC"
        )
        _sql_clo_fallback = (
            "SELECT ts, symbol, NULL AS pnl_pct, NULL AS hold_time_secs"
            " FROM decision_trace WHERE " + _clo_where +
            "   AND outcome = 'CLOSED'"
            " ORDER BY ts ASC"
        )

        _clo_params = tuple(_clo_params_list)
        _clo_df: "pd.DataFrame | None" = None
        for _clo_sql in (_sql_clo_full, _sql_clo_fallback):
            try:
                _cdf = _query(_clo_sql, _clo_params)
                if _cdf is not None:
                    _clo_df = _cdf
                    break
            except Exception as _ce:
                log.debug("[TRACK20-BRK1] close query fallback: %s", _ce)
                continue

        if _clo_df is None:
            _clo_df = pd.DataFrame(columns=["ts", "symbol", "pnl_pct", "hold_time_secs"])

        # ── Coerce numerics ───────────────────────────────────────────────────
        for _nc_df, _nc_col in (
            (_blk_df, "ts"), (_adm_df, "ts"), (_clo_df, "ts"),
        ):
            if _nc_col in _nc_df.columns:
                _nc_df[_nc_col] = pd.to_numeric(_nc_df[_nc_col], errors="coerce")
        for _nc_col2 in ("pnl_pct", "hold_time_secs"):
            if _nc_col2 in _clo_df.columns:
                _clo_df[_nc_col2] = pd.to_numeric(_clo_df[_nc_col2], errors="coerce")

        _blk_df = _blk_df.dropna(subset=["ts", "symbol", "direction"])
        _adm_df = _adm_df.dropna(subset=["ts", "symbol", "direction"])
        _clo_df = _clo_df.dropna(subset=["ts", "symbol"])

        # ── Build admit + close lookups (same as Track 19) ────────────────────
        _adm_lookup: dict = {}
        for _ar in _adm_df.itertuples(index=False):
            _key = (str(_ar.symbol), str(_ar.direction))
            _adm_lookup.setdefault(_key, []).append(float(_ar.ts))

        _clo_lookup: dict = {}
        for _cr in _clo_df.itertuples(index=False):
            _s = str(_cr.symbol)
            _clo_lookup.setdefault(_s, []).append((
                float(_cr.ts),
                getattr(_cr, "pnl_pct", None),
                getattr(_cr, "hold_time_secs", None),
            ))

        # ── Helper: empty stats bucket ────────────────────────────────────────
        def _empty_bucket() -> dict:
            return {"n_blk": 0, "n_adm": 0, "n_close": 0,
                    "pnl_list": [], "hold_list": []}

        # ── Three accumulation dicts ──────────────────────────────────────────
        _gate_stats: dict = {}            # gate_key           → bucket
        _sym_stats:  dict = {}            # (gate_key, symbol) → bucket
        _dir_stats:  dict = {}            # (gate_key, dir)    → bucket

        for _br in _blk_df.itertuples(index=False):
            _gk  = str(getattr(_br, "gate_name", "") or "")
            _sym = str(_br.symbol)
            _dir = str(_br.direction).upper()
            _bts = float(_br.ts)

            if not _gk:
                continue

            _gs  = _gate_stats.setdefault(_gk,        _empty_bucket())
            _gss = _sym_stats .setdefault((_gk, _sym), _empty_bucket())
            _gsd = _dir_stats .setdefault((_gk, _dir), _empty_bucket())

            _gs["n_blk"]  += 1
            _gss["n_blk"] += 1
            _gsd["n_blk"] += 1

            # Earliest ADMITTED (same symbol + direction, within window)
            _adm_ts_list = _adm_lookup.get((_sym, _dir), [])
            _lo_idx  = _bisect.bisect_right(_adm_ts_list, _bts)
            _hi_bound = _bts + _CORR19_ADMITTED_WINDOW_SECS
            _adm_ts  = None
            if _lo_idx < len(_adm_ts_list) and _adm_ts_list[_lo_idx] <= _hi_bound:
                _adm_ts = _adm_ts_list[_lo_idx]
                _gs["n_adm"]  += 1
                _gss["n_adm"] += 1
                _gsd["n_adm"] += 1

            if _adm_ts is None:
                continue

            # Earliest CLOSED after admission (same symbol, within window)
            _clo_ts_list   = _clo_lookup.get(_sym, [])
            _clo_sorted_ts = [_ct[0] for _ct in _clo_ts_list]
            _clo_lo = _bisect.bisect_right(_clo_sorted_ts, _adm_ts)
            _clo_hi = _adm_ts + _CORR18_CLOSE_WINDOW_SECS
            if _clo_lo < len(_clo_ts_list) and _clo_ts_list[_clo_lo][0] <= _clo_hi:
                _pnl_v = _clo_ts_list[_clo_lo][1]
                _hld_v = _clo_ts_list[_clo_lo][2]
                for _bkt in (_gs, _gss, _gsd):
                    _bkt["n_close"] += 1
                    if _pnl_v is not None and _pnl_v == _pnl_v:
                        _bkt["pnl_list"].append(float(_pnl_v))
                    if _hld_v is not None and _hld_v == _hld_v:
                        _bkt["hold_list"].append(float(_hld_v))

        if not _gate_stats:
            return _EMPTY

        # ── Helper: bucket → stats dict ───────────────────────────────────────
        def _bkt_to_stats(bkt: dict) -> dict:
            _nb  = bkt["n_blk"]
            _na  = bkt["n_adm"]
            _nc  = bkt["n_close"]
            _pls = bkt["pnl_list"]
            _hls = bkt["hold_list"]
            return {
                "n_blocked":       _nb,
                "n_later_adm":     _na,
                "adm_rate":        _na / _nb if _nb > 0 else 0.0,
                "n_with_close":    _nc,
                "mean_pnl_pct":    (sum(_pls) / len(_pls)) if _pls else None,
                "mean_hold_secs":  (sum(_hls) / len(_hls)) if _hls else None,
            }

        # ── Build by_gate (identical to _load_gate_aggregate output) ─────────
        by_gate: list = []
        for _gk, _bkt in _gate_stats.items():
            _s = _bkt_to_stats(_bkt)
            by_gate.append({
                "gate_key": _gk,
                "label":    _DT_GATE_LABEL.get(_gk, _gk[:12] or "—"),
                "colour":   _DT_GATE_COLOUR.get(_gk, T["text2"]),
                **_s,
            })
        by_gate.sort(key=lambda x: x["n_blocked"], reverse=True)

        # ── Build by_symbol ───────────────────────────────────────────────────
        by_symbol: dict = {}
        for (_gk, _sym), _bkt in _sym_stats.items():
            _s = _bkt_to_stats(_bkt)
            by_symbol.setdefault(_gk, []).append({"symbol": _sym, **_s})
        for _gk in by_symbol:
            by_symbol[_gk].sort(key=lambda x: x["n_blocked"], reverse=True)

        # ── Build by_direction ────────────────────────────────────────────────
        by_direction: dict = {}
        for (_gk, _dir), _bkt in _dir_stats.items():
            _s = _bkt_to_stats(_bkt)
            by_direction.setdefault(_gk, {})[_dir] = _s

        return {
            "by_gate":      by_gate,
            "by_symbol":    by_symbol,
            "by_direction": by_direction,
        }

    except Exception:
        log.error("[TRACK20-BRK1] _load_gate_breakdown: unexpected error", exc_info=True)
        return _EMPTY


# ── [TRACK21-XPL1] Cross-table gate realized-profitability loader ─────────────

# Forward-look window for cross-table trade proximity matching.
# Kept separate from _CORR19_ADMITTED_WINDOW_SECS so Track 21 can be tuned
# independently.  Same 10-minute default for consistency with Tracks 18–20.
_CORR21_TRADE_WINDOW_SECS: int = 600


@st.cache_data(ttl=CACHE_FAST)
def _load_gate_realized_pnl(
    days_back:     int = 7,
    symbol_filter: str = "ALL",
) -> "list[dict]":
    """[TRACK21-XPL1] Per-gate cross-table realized-profitability analytics.

    For each BLOCKED/SHADOW entry+express row in decision_trace, finds the
    nearest later trade (from the trades table) for the same symbol and
    direction within _CORR21_TRADE_WINDOW_SECS seconds.  Accumulates per
    gate_key: how many blocks had a trade soon after, and what the realized
    profitability of those trades looked like.

    This gives the operator ledger-quality realized PnL context (realized_usd,
    close_slippage_bps, exit categorization) rather than the DT-local pnl_pct
    available only on Track-17+ databases.

    Join rule (strict — no fuzzy heuristics):
      For each BLOCKED row (gate_key, symbol, direction, ts):
        Find the earliest trades row where
          trades.symbol = dt.symbol
          AND trades.entry_direction = dt.direction  (when available)
          AND trades.entry_ts ∈ (block_ts, block_ts + _CORR21_TRADE_WINDOW_SECS)
        If found → bucket into gate_stats with pnl_pct, realized_usd,
          close_slippage_bps from that trade row.

    trades.entry_ts sourced via three-tier fallback:
      Tier-1: entry_ts from open-leg self-JOIN (full schema, trade_id present)
      Tier-2: ts − hold_time_secs (Phase-6+ schema, no open-leg join)
      Tier-3: ts (close-fill timestamp as last-resort proxy)

    Blocked-query two-tier fallback identical to Track 19:
      Tier-1: source_path IN ('entry','express') filter
      Tier-2: no source_path filter (pre-Track-16 DBs)

    Returns
    -------
    list of dict, sorted by n_blocked descending.  Each dict:
        gate_key         : str
        label            : str          — human label from _DT_GATE_LABEL
        colour           : str          — hex from _DT_GATE_COLOUR
        n_blocked        : int
        n_with_trade     : int          — blocks where a forward trade was found
        trade_rate       : float        — n_with_trade / n_blocked
        win_count        : int          — trades with pnl_pct > 0
        loss_count       : int
        win_rate         : float | None — win_count / n_with_trade; None if 0
        mean_pnl_pct     : float | None
        mean_realized_usd: float | None — None if column absent
        mean_slippage_bps: float | None — None if column absent
        dir_matched      : bool         — True if entry_direction was used for matching

    Returns [] on missing tables, empty result, or any exception.
    Read-only.  Parameterized SQL only.  No DB writes.  No schema changes.
    No filesystem writes.  Never raises into caller.
    """
    _EMPTY: list = []
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return _EMPTY
    conn = _get_db_conn()
    if conn is None:
        return _EMPTY
    try:
        import bisect as _bisect

        cutoff_ts = time.time() - days_back * 86400.0
        _now_ts   = time.time()

        # ── Build WHERE clause for blocked query ──────────────────────────────
        params_base: list = [cutoff_ts]
        where_parts = ["ts >= ?"]
        if symbol_filter and symbol_filter != "ALL":
            where_parts.append("symbol = ?")
            params_base.append(symbol_filter)
        where_clause = " AND ".join(where_parts)

        # ── Query A: BLOCKED + SHADOW rows from decision_trace ────────────────
        # Two-tier fallback identical to Track 19
        _sql_blk_src = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            "   AND source_path IN ('entry', 'express')"
            " ORDER BY ts ASC"
        )
        _sql_blk_nosrc = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            " ORDER BY ts ASC"
        )

        _blk_df: "pd.DataFrame | None" = None
        _has_src: bool = False
        for _blk_sql in (_sql_blk_src, _sql_blk_nosrc):
            try:
                _bdf = _query(_blk_sql, tuple(params_base))
                if _bdf is not None:
                    _blk_df = _bdf
                    _has_src = (_blk_sql is _sql_blk_src)
                    break
            except Exception as _be:
                log.debug("[TRACK21-XPL1] blocked query fallback: %s", _be)
                continue

        if _blk_df is None or _blk_df.empty:
            return _EMPTY

        # ── Build WHERE clause for trades query ───────────────────────────────
        _trd_params_base: list = [cutoff_ts, _now_ts]
        _trd_where_parts = ["t_entry >= ?", "t_entry <= ?"]
        if symbol_filter and symbol_filter != "ALL":
            _trd_where_parts.append("symbol = ?")
            _trd_params_base.append(symbol_filter)
        _trd_where = " AND ".join(_trd_where_parts)

        # ── Query B: trades rows — three-tier entry_ts fallback ───────────────
        # Tier-1: Full schema — entry_ts from open-leg JOIN on trade_id
        # Selects: entry_direction, pnl_pct, realized_usd, close_slippage_bps
        _sql_trd_full = f"""
            SELECT
                c.symbol,
                c.entry_direction,
                o.ts          AS t_entry,
                c.pnl_pct,
                c.realized_usd,
                c.close_slippage_bps
            FROM trades c
            JOIN trades o
              ON c.trade_id = o.trade_id
             AND o.is_close_leg = 0
            WHERE c.is_close_leg = 1
              AND c.trade_id IS NOT NULL
              AND o.ts >= ?
              AND o.ts <= ?
            {('AND c.symbol = ?' if (symbol_filter and symbol_filter != 'ALL') else '')}
            ORDER BY o.ts ASC
        """
        # Tier-2: Phase-6+ schema — use ts − hold_time_secs as entry_ts proxy
        _sql_trd_p6 = f"""
            SELECT
                symbol,
                entry_direction,
                (ts - COALESCE(hold_time_secs, 0)) AS t_entry,
                pnl_pct,
                realized_usd,
                close_slippage_bps
            FROM trades
            WHERE is_close_leg = 1
              AND (ts - COALESCE(hold_time_secs, 0)) >= ?
              AND (ts - COALESCE(hold_time_secs, 0)) <= ?
            {('AND symbol = ?' if (symbol_filter and symbol_filter != 'ALL') else '')}
            ORDER BY (ts - COALESCE(hold_time_secs, 0)) ASC
        """
        # Tier-3: Minimal schema — use close-fill ts as last-resort proxy
        _sql_trd_min = f"""
            SELECT
                symbol,
                NULL AS entry_direction,
                ts   AS t_entry,
                pnl_pct,
                realized_usd,
                NULL AS close_slippage_bps
            FROM trades
            WHERE is_close_leg = 1
              AND ts >= ?
              AND ts <= ?
            {('AND symbol = ?' if (symbol_filter and symbol_filter != 'ALL') else '')}
            ORDER BY ts ASC
        """
        # Tier-4: Pre-Phase-4 (no is_close_leg) — pnl_pct heuristic
        _sql_trd_legacy = f"""
            SELECT
                symbol,
                NULL AS entry_direction,
                ts   AS t_entry,
                pnl_pct,
                realized_usd,
                NULL AS close_slippage_bps
            FROM trades
            WHERE pnl_pct IS NOT NULL
              AND pnl_pct != 0
              AND ts >= ?
              AND ts <= ?
            {('AND symbol = ?' if (symbol_filter and symbol_filter != 'ALL') else '')}
            ORDER BY ts ASC
        """

        _trd_df: "pd.DataFrame | None" = None
        _dir_matched: bool = False
        _trd_params = tuple(_trd_params_base)

        # Tier-1 uses the JOIN; params are the same base list
        _trd_tiers = [
            (_sql_trd_full,   _trd_params),
            (_sql_trd_p6,     _trd_params),
            (_sql_trd_min,    _trd_params),
            (_sql_trd_legacy, _trd_params),
        ]
        for _tsql, _tpar in _trd_tiers:
            try:
                _tdf = _query(_tsql, _tpar)
                if _tdf is not None and not _tdf.empty:
                    _trd_df = _tdf
                    break
                if _tdf is not None:
                    # empty result — still a valid query, stop trying
                    _trd_df = _tdf
                    break
            except Exception as _te:
                log.debug("[TRACK21-XPL1] trades query tier fallback: %s", _te)
                continue

        if _trd_df is None:
            _trd_df = pd.DataFrame(
                columns=["symbol", "entry_direction", "t_entry",
                         "pnl_pct", "realized_usd", "close_slippage_bps"]
            )

        # ── Coerce numeric columns ────────────────────────────────────────────
        for _nc_df, _nc_col in (
            (_blk_df, "ts"),
            (_trd_df, "t_entry"),
            (_trd_df, "pnl_pct"),
            (_trd_df, "realized_usd"),
            (_trd_df, "close_slippage_bps"),
        ):
            if _nc_col in _nc_df.columns:
                _nc_df[_nc_col] = pd.to_numeric(_nc_df[_nc_col], errors="coerce")

        _blk_df = _blk_df.dropna(subset=["ts", "symbol", "direction"])
        _trd_df = _trd_df.dropna(subset=["t_entry", "symbol"])

        if _trd_df.empty:
            return _EMPTY

        # ── Determine whether direction matching is available ─────────────────
        _has_dir = (
            "entry_direction" in _trd_df.columns
            and _trd_df["entry_direction"].notna().any()
        )
        _dir_matched = _has_dir

        # ── Build trade lookup: (symbol, direction) → sorted list of
        #    (t_entry, pnl_pct, realized_usd, slippage_bps) ──────────────────
        # If direction not available, key by symbol only ("" as direction stub)
        _trd_lookup: dict = {}
        for _tr in _trd_df.itertuples(index=False):
            _t_sym = str(_tr.symbol)
            _t_dir = (
                str(_tr.entry_direction).lower()
                if (_has_dir and getattr(_tr, "entry_direction", None) is not None)
                else ""
            )
            _t_ts  = float(_tr.t_entry)
            _t_pnl = getattr(_tr, "pnl_pct",            None)
            _t_usd = getattr(_tr, "realized_usd",        None)
            _t_slp = getattr(_tr, "close_slippage_bps",  None)
            # NaN guard
            if _t_pnl is not None and _t_pnl != _t_pnl:
                _t_pnl = None
            if _t_usd is not None and _t_usd != _t_usd:
                _t_usd = None
            if _t_slp is not None and _t_slp != _t_slp:
                _t_slp = None
            _trd_lookup.setdefault((_t_sym, _t_dir), []).append(
                (_t_ts, _t_pnl, _t_usd, _t_slp)
            )
        # Sort each list by t_entry ASC (query already ordered, but be safe)
        for _k in _trd_lookup:
            _trd_lookup[_k].sort(key=lambda x: x[0])

        # ── Per-gate accumulation ─────────────────────────────────────────────
        _gate_stats: dict = {}  # gate_key → bucket dict

        def _empty_bkt() -> dict:
            return {
                "n_blk": 0, "n_hit": 0,
                "win": 0, "loss": 0,
                "pnl_list": [], "usd_list": [], "slp_list": [],
            }

        for _br in _blk_df.itertuples(index=False):
            _gk  = str(getattr(_br, "gate_name", "") or "")
            _sym = str(_br.symbol)
            _dir = str(_br.direction).lower()
            _bts = float(_br.ts)
            if not _gk:
                continue

            _gs = _gate_stats.setdefault(_gk, _empty_bkt())
            _gs["n_blk"] += 1

            # Find earliest trade after block_ts within window
            # Try direction-matched key first; fall back to symbol-only if no match
            _hi = _bts + _CORR21_TRADE_WINDOW_SECS
            _match = None

            if _has_dir:
                _cand_list = _trd_lookup.get((_sym, _dir), [])
                _c_ts_list = [_c[0] for _c in _cand_list]
                _lo = _bisect.bisect_right(_c_ts_list, _bts)
                if _lo < len(_cand_list) and _cand_list[_lo][0] <= _hi:
                    _match = _cand_list[_lo]
            else:
                # No direction column — use symbol-only key ("")
                _cand_list = _trd_lookup.get((_sym, ""), [])
                _c_ts_list = [_c[0] for _c in _cand_list]
                _lo = _bisect.bisect_right(_c_ts_list, _bts)
                if _lo < len(_cand_list) and _cand_list[_lo][0] <= _hi:
                    _match = _cand_list[_lo]

            if _match is None:
                continue

            _m_pnl, _m_usd, _m_slp = _match[1], _match[2], _match[3]
            _gs["n_hit"] += 1
            if _m_pnl is not None:
                _gs["pnl_list"].append(float(_m_pnl))
                if float(_m_pnl) > 0:
                    _gs["win"] += 1
                else:
                    _gs["loss"] += 1
            if _m_usd is not None:
                _gs["usd_list"].append(float(_m_usd))
            if _m_slp is not None:
                _gs["slp_list"].append(float(_m_slp))

        if not _gate_stats:
            return _EMPTY

        # ── Build result list ─────────────────────────────────────────────────
        result: list = []
        for _gk, _gs in _gate_stats.items():
            _nb  = _gs["n_blk"]
            _nh  = _gs["n_hit"]
            _win = _gs["win"]
            _pls = _gs["pnl_list"]
            _uls = _gs["usd_list"]
            _sls = _gs["slp_list"]
            result.append({
                "gate_key":          _gk,
                "label":             _DT_GATE_LABEL.get(_gk, _gk[:12] or "—"),
                "colour":            _DT_GATE_COLOUR.get(_gk, T["text2"]),
                "n_blocked":         _nb,
                "n_with_trade":      _nh,
                "trade_rate":        _nh / _nb if _nb > 0 else 0.0,
                "win_count":         _win,
                "loss_count":        _gs["loss"],
                "win_rate":          _win / _nh if _nh > 0 else None,
                "mean_pnl_pct":      (sum(_pls) / len(_pls)) if _pls else None,
                "mean_realized_usd": (sum(_uls) / len(_uls)) if _uls else None,
                "mean_slippage_bps": (sum(_sls) / len(_sls)) if _sls else None,
                "dir_matched":       _dir_matched,
            })

        result.sort(key=lambda x: x["n_blocked"], reverse=True)
        return result

    except Exception:
        log.error("[TRACK21-XPL1] _load_gate_realized_pnl: unexpected error", exc_info=True)
        return _EMPTY


# ── [TRACK22-DRL0] Per-symbol/direction realized-PnL drill-down loader ────────

@st.cache_data(ttl=CACHE_FAST)
def _load_gate_realized_pnl_drilldown(
    days_back:     int = 7,
    symbol_filter: str = "ALL",
) -> "dict":
    """[TRACK22-DRL0] Per-gate realized-PnL drill-down by symbol and direction.

    Uses the same strict join rule as _load_gate_realized_pnl() (Track 21):
      - same symbol
      - same direction when entry_direction is available
      - nearest forward trade entry within _CORR21_TRADE_WINDOW_SECS

    Instead of accumulating per-gate aggregates, accumulates per
    (gate_key, symbol) and per (gate_key, direction) in a single pass.

    blocked-query: identical two-tier source_path fallback to Track 21.
    trades-query:  identical four-tier entry_ts fallback to Track 21.
                   close_slippage_bps is intentionally omitted here; slippage
                   is already shown in the Track 21 gate-level table.

    Returns
    -------
    dict with keys:
        "by_symbol"    : dict[gate_key → list[dict]]
                         Each row: symbol, n_blocked, n_with_trade,
                         win_count, loss_count, win_rate, mean_pnl_pct,
                         mean_realized_usd — sorted by n_blocked DESC
        "by_direction" : dict[gate_key → list[dict]]
                         Each row: direction, n_blocked, n_with_trade,
                         win_count, loss_count, win_rate, mean_pnl_pct,
                         mean_realized_usd — sorted by n_blocked DESC
        "dir_matched"  : bool — True when entry_direction was present and used

    Returns {} on missing tables, empty result, or any exception.
    Read-only.  Parameterized SQL only.  No DB writes.  No schema changes.
    No filesystem writes.  Never raises into caller.
    """
    _EMPTY: dict = {}
    tables = _get_known_tables()
    if "decision_trace" not in tables:
        return _EMPTY
    conn = _get_db_conn()
    if conn is None:
        return _EMPTY
    try:
        import bisect as _bisect

        cutoff_ts = time.time() - days_back * 86400.0
        _now_ts   = time.time()

        # ── Build WHERE clause for blocked query ──────────────────────────────
        params_base: list = [cutoff_ts]
        where_parts = ["ts >= ?"]
        if symbol_filter and symbol_filter != "ALL":
            where_parts.append("symbol = ?")
            params_base.append(symbol_filter)
        where_clause = " AND ".join(where_parts)

        # ── Query A: BLOCKED + SHADOW rows — two-tier fallback ────────────────
        _sql_blk_src = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            "   AND source_path IN ('entry', 'express')"
            " ORDER BY ts ASC"
        )
        _sql_blk_nosrc = (
            "SELECT ts, symbol, direction, gate_name"
            f" FROM decision_trace WHERE {where_clause}"
            "   AND outcome IN ('BLOCKED', 'SHADOW')"
            " ORDER BY ts ASC"
        )

        _blk_df: "pd.DataFrame | None" = None
        for _blk_sql in (_sql_blk_src, _sql_blk_nosrc):
            try:
                _bdf = _query(_blk_sql, tuple(params_base))
                if _bdf is not None:
                    _blk_df = _bdf
                    break
            except Exception as _be:
                log.debug("[TRACK22-DRL0] blocked query fallback: %s", _be)
                continue

        if _blk_df is None or _blk_df.empty:
            return _EMPTY

        # ── Build WHERE clause for trades query ───────────────────────────────
        _trd_params = tuple([cutoff_ts, _now_ts]
                            + ([symbol_filter] if (symbol_filter and symbol_filter != "ALL") else []))
        _sym_flt_c  = ("AND c.symbol = ?" if (symbol_filter and symbol_filter != "ALL") else "")
        _sym_flt    = ("AND symbol = ?"   if (symbol_filter and symbol_filter != "ALL") else "")

        # ── Query B: trades rows — identical four-tier fallback to Track 21 ───
        # (close_slippage_bps omitted; already shown in Track 21 gate-level table)
        _sql_trd_full = f"""
            SELECT
                c.symbol,
                c.entry_direction,
                o.ts          AS t_entry,
                c.pnl_pct,
                c.realized_usd
            FROM trades c
            JOIN trades o
              ON c.trade_id = o.trade_id
             AND o.is_close_leg = 0
            WHERE c.is_close_leg = 1
              AND c.trade_id IS NOT NULL
              AND o.ts >= ?
              AND o.ts <= ?
            {_sym_flt_c}
            ORDER BY o.ts ASC
        """
        _sql_trd_p6 = f"""
            SELECT
                symbol,
                entry_direction,
                (ts - COALESCE(hold_time_secs, 0)) AS t_entry,
                pnl_pct,
                realized_usd
            FROM trades
            WHERE is_close_leg = 1
              AND (ts - COALESCE(hold_time_secs, 0)) >= ?
              AND (ts - COALESCE(hold_time_secs, 0)) <= ?
            {_sym_flt}
            ORDER BY (ts - COALESCE(hold_time_secs, 0)) ASC
        """
        _sql_trd_min = f"""
            SELECT
                symbol,
                NULL AS entry_direction,
                ts   AS t_entry,
                pnl_pct,
                realized_usd
            FROM trades
            WHERE is_close_leg = 1
              AND ts >= ?
              AND ts <= ?
            {_sym_flt}
            ORDER BY ts ASC
        """
        _sql_trd_legacy = f"""
            SELECT
                symbol,
                NULL AS entry_direction,
                ts   AS t_entry,
                pnl_pct,
                realized_usd
            FROM trades
            WHERE pnl_pct IS NOT NULL
              AND pnl_pct != 0
              AND ts >= ?
              AND ts <= ?
            {_sym_flt}
            ORDER BY ts ASC
        """

        _trd_df: "pd.DataFrame | None" = None
        for _tsql in (_sql_trd_full, _sql_trd_p6, _sql_trd_min, _sql_trd_legacy):
            try:
                _tdf = _query(_tsql, _trd_params)
                if _tdf is not None:
                    _trd_df = _tdf
                    break
            except Exception as _te:
                log.debug("[TRACK22-DRL0] trades query tier fallback: %s", _te)
                continue

        if _trd_df is None:
            _trd_df = pd.DataFrame(
                columns=["symbol", "entry_direction", "t_entry",
                         "pnl_pct", "realized_usd"]
            )

        # ── Coerce numeric columns ────────────────────────────────────────────
        for _nc_df, _nc_col in (
            (_blk_df, "ts"),
            (_trd_df, "t_entry"),
            (_trd_df, "pnl_pct"),
            (_trd_df, "realized_usd"),
        ):
            if _nc_col in _nc_df.columns:
                _nc_df[_nc_col] = pd.to_numeric(_nc_df[_nc_col], errors="coerce")

        _blk_df = _blk_df.dropna(subset=["ts", "symbol", "direction"])
        _trd_df = _trd_df.dropna(subset=["t_entry", "symbol"])

        if _trd_df.empty:
            return _EMPTY

        # ── Direction-match availability ──────────────────────────────────────
        _has_dir = (
            "entry_direction" in _trd_df.columns
            and _trd_df["entry_direction"].notna().any()
        )

        # ── Build trade lookup (identical to Track 21) ────────────────────────
        _trd_lookup: dict = {}
        for _tr in _trd_df.itertuples(index=False):
            _t_sym = str(_tr.symbol)
            _t_dir = (
                str(_tr.entry_direction).lower()
                if (_has_dir and getattr(_tr, "entry_direction", None) is not None)
                else ""
            )
            _t_ts  = float(_tr.t_entry)
            _t_pnl = getattr(_tr, "pnl_pct",     None)
            _t_usd = getattr(_tr, "realized_usd", None)
            if _t_pnl is not None and _t_pnl != _t_pnl:
                _t_pnl = None
            if _t_usd is not None and _t_usd != _t_usd:
                _t_usd = None
            _trd_lookup.setdefault((_t_sym, _t_dir), []).append(
                (_t_ts, _t_pnl, _t_usd)
            )
        for _k in _trd_lookup:
            _trd_lookup[_k].sort(key=lambda x: x[0])

        # ── Accumulation into sym_stats and dir_stats ─────────────────────────
        def _empty_bkt() -> dict:
            return {"n_blk": 0, "n_hit": 0, "win": 0, "loss": 0,
                    "pnl_list": [], "usd_list": []}

        _sym_stats: dict = {}   # (gate_key, symbol)    → bkt
        _dir_stats: dict = {}   # (gate_key, direction) → bkt

        for _br in _blk_df.itertuples(index=False):
            _gk  = str(getattr(_br, "gate_name", "") or "")
            _sym = str(_br.symbol)
            _dir = str(_br.direction).lower()
            _bts = float(_br.ts)
            if not _gk:
                continue

            _ss = _sym_stats.setdefault((_gk, _sym), _empty_bkt())
            _ds = _dir_stats.setdefault((_gk, _dir), _empty_bkt())
            _ss["n_blk"] += 1
            _ds["n_blk"] += 1

            _hi = _bts + _CORR21_TRADE_WINDOW_SECS
            _match = None

            if _has_dir:
                _cand = _trd_lookup.get((_sym, _dir), [])
                _cts  = [_c[0] for _c in _cand]
                _lo   = _bisect.bisect_right(_cts, _bts)
                if _lo < len(_cand) and _cand[_lo][0] <= _hi:
                    _match = _cand[_lo]
            else:
                _cand = _trd_lookup.get((_sym, ""), [])
                _cts  = [_c[0] for _c in _cand]
                _lo   = _bisect.bisect_right(_cts, _bts)
                if _lo < len(_cand) and _cand[_lo][0] <= _hi:
                    _match = _cand[_lo]

            if _match is None:
                continue

            _m_pnl, _m_usd = _match[1], _match[2]
            for _bkt in (_ss, _ds):
                _bkt["n_hit"] += 1
                if _m_pnl is not None:
                    _bkt["pnl_list"].append(float(_m_pnl))
                    if float(_m_pnl) > 0:
                        _bkt["win"] += 1
                    else:
                        _bkt["loss"] += 1
                if _m_usd is not None:
                    _bkt["usd_list"].append(float(_m_usd))

        if not _sym_stats and not _dir_stats:
            return _EMPTY

        # ── Build by_symbol ───────────────────────────────────────────────────
        _by_sym: dict = {}
        for (_gk, _sym), _bkt in _sym_stats.items():
            _nb = _bkt["n_blk"]
            _nh = _bkt["n_hit"]
            _w  = _bkt["win"]
            _p  = _bkt["pnl_list"]
            _u  = _bkt["usd_list"]
            _by_sym.setdefault(_gk, []).append({
                "symbol":            _sym,
                "n_blocked":         _nb,
                "n_with_trade":      _nh,
                "win_count":         _w,
                "loss_count":        _bkt["loss"],
                "win_rate":          _w / _nh if _nh > 0 else None,
                "mean_pnl_pct":      (sum(_p) / len(_p)) if _p else None,
                "mean_realized_usd": (sum(_u) / len(_u)) if _u else None,
            })
        for _gk in _by_sym:
            _by_sym[_gk].sort(key=lambda x: x["n_blocked"], reverse=True)

        # ── Build by_direction ────────────────────────────────────────────────
        _by_dir: dict = {}
        for (_gk, _dir), _bkt in _dir_stats.items():
            _nb = _bkt["n_blk"]
            _nh = _bkt["n_hit"]
            _w  = _bkt["win"]
            _p  = _bkt["pnl_list"]
            _u  = _bkt["usd_list"]
            _by_dir.setdefault(_gk, []).append({
                "direction":         _dir.upper() if _dir else "",
                "n_blocked":         _nb,
                "n_with_trade":      _nh,
                "win_count":         _w,
                "loss_count":        _bkt["loss"],
                "win_rate":          _w / _nh if _nh > 0 else None,
                "mean_pnl_pct":      (sum(_p) / len(_p)) if _p else None,
                "mean_realized_usd": (sum(_u) / len(_u)) if _u else None,
            })
        for _gk in _by_dir:
            _by_dir[_gk].sort(key=lambda x: x["n_blocked"], reverse=True)

        return {
            "by_symbol":    _by_sym,
            "by_direction": _by_dir,
            "dir_matched":  _has_dir,
        }

    except Exception:
        log.error(
            "[TRACK22-DRL0] _load_gate_realized_pnl_drilldown: unexpected error",
            exc_info=True,
        )
        return _EMPTY


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: OPERATOR REVIEW REPORT  [TRACK13]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_operator_report() -> None:
    """[TRACK13-RPT2] Operator review report panel.

    Synthesises trade history, exit attribution, admission-funnel data,
    signal quality, and top/bottom setups into a single compact read-only
    review panel for the selected lookback window.

    Data sources:
      - _load_round_trips()          → trade-side sections (has close_ts for
                                       windowing, entry-context columns, friction)
      - _load_report_dt_summary()    → admission funnel only

    Degrades gracefully when:
      - no trades in selected window (placeholder, no error)
      - decision_trace table absent (admission funnel shows placeholder)
      - entry_regime / entry_direction / entry_z_score / entry_confidence
        absent (pre-Track-01 schema): signal quality and setup sections degrade

    Read-only.  No DB writes.  No schema changes.  No hub_data file writes.
    """
    import io as _io

    _section("OPERATOR REVIEW REPORT", "📋")

    # ── Controls row ──────────────────────────────────────────────────────────
    _hc1, _hc2, _hc3 = st.columns([2, 2, 4])
    with _hc1:
        days_label = st.selectbox(
            "Window", ["1d", "7d", "30d"], index=1,
            key="rpt_days", label_visibility="collapsed",
        )
    days_back = int(days_label.rstrip("d"))
    cutoff    = time.time() - days_back * 86400.0

    # ── Load and window trade data (round-trips carry close_ts) ──────────────
    rt_all = _load_round_trips()

    sym_opts = ["ALL"]
    if not rt_all.empty and "symbol" in rt_all.columns:
        sym_opts += sorted(rt_all["symbol"].dropna().unique().tolist())
    with _hc2:
        sym_filter = st.selectbox(
            "Symbol", sym_opts, key="rpt_sym", label_visibility="collapsed",
        )

    df = rt_all.copy()
    if "close_ts" in df.columns and not df.empty:
        df["close_ts"] = pd.to_numeric(df["close_ts"], errors="coerce")
        df = df[df["close_ts"] >= cutoff]
    if sym_filter != "ALL" and "symbol" in df.columns:
        df = df[df["symbol"] == sym_filter]
    df = df.reset_index(drop=True)

    # Coerce numeric columns
    for _nc in ("realized_usd", "pnl_pct", "hold_time_secs",
                "entry_confidence", "entry_z_score", "close_slippage_bps"):
        if _nc in df.columns:
            df[_nc] = pd.to_numeric(df[_nc], errors="coerce")

    # csv_rows accumulates one dict per metric for the CSV export
    csv_rows: list = []

    def _pnl_col(v: float) -> str:
        return T["green"] if v > 0 else T["red"] if v < 0 else T["muted"]

    def _wr_col(wins: int, n: int) -> str:
        if n == 0:
            return T["muted"]
        wr = wins / n
        return T["green"] if wr > 0.55 else T["yellow"] if wr > 0.40 else T["red"]

    def _fmt_hold_secs(s) -> str:
        try:
            s = float(s)
            if s != s:           # NaN check
                return "—"
        except (TypeError, ValueError):
            return "—"
        if s >= 3600:
            return f"{s / 3600:.1f}h"
        if s >= 60:
            return f"{s / 60:.0f}m"
        return f"{s:.0f}s"

    # ── Section 1 — Performance Headline ─────────────────────────────────────
    st.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{T["text2"]};margin:8px 0 4px 0;">'
        f'PERFORMANCE — LAST {days_label}</div>',
        unsafe_allow_html=True,
    )

    if df.empty:
        _slot_placeholder(f"NO CLOSED TRADES IN LAST {days_label}", 60)
        csv_rows.append({
            "section": "PERFORMANCE", "metric": "trades",
            "value": "0", "sub": days_label,
        })
    else:
        n_trades   = len(df)
        total_pnl  = float(df["realized_usd"].sum()) if "realized_usd" in df.columns else 0.0
        n_wins     = int((df["realized_usd"] > 0).sum()) if "realized_usd" in df.columns else 0
        win_rate   = n_wins / n_trades if n_trades > 0 else 0.0
        avg_hold_v = df["hold_time_secs"].dropna().mean() if "hold_time_secs" in df.columns else None
        avg_hold_str = _fmt_hold_secs(avg_hold_v)
        avg_slip_v = df["close_slippage_bps"].dropna().mean() if "close_slippage_bps" in df.columns else None
        avg_slip_str = (
            f"{float(avg_slip_v):.1f}bps"
            if (avg_slip_v is not None and avg_slip_v == avg_slip_v)
            else "—"
        )

        pc1, pc2, pc3, pc4 = st.columns(4)
        with pc1:
            _kpi("TOTAL PnL", f"${total_pnl:+,.2f}", days_label, _pnl_col(total_pnl))
        with pc2:
            _kpi("WIN RATE", f"{win_rate * 100:.1f}%",
                 f"{n_wins}/{n_trades} trades", _wr_col(n_wins, n_trades))
        with pc3:
            _kpi("AVG HOLD", avg_hold_str, "per trade", T["text2"])
        with pc4:
            _kpi("AVG SLIP", avg_slip_str, "close leg", T["text2"])

        csv_rows += [
            {"section": "PERFORMANCE", "metric": "total_pnl_usd",
             "value": f"{total_pnl:+.2f}", "sub": days_label},
            {"section": "PERFORMANCE", "metric": "win_rate_pct",
             "value": f"{win_rate * 100:.1f}", "sub": f"{n_wins}/{n_trades}"},
            {"section": "PERFORMANCE", "metric": "trade_count",
             "value": str(n_trades), "sub": days_label},
            {"section": "PERFORMANCE", "metric": "avg_hold",
             "value": avg_hold_str, "sub": ""},
            {"section": "PERFORMANCE", "metric": "avg_slip_bps",
             "value": avg_slip_str, "sub": ""},
        ]

        # Per-symbol sub-table when multiple symbols are present
        if "symbol" in df.columns and df["symbol"].nunique() > 1:
            sym_grp = (
                df.groupby("symbol")
                  .agg(
                      n=("realized_usd", "count"),
                      pnl=("realized_usd", "sum"),
                      wins=("realized_usd", lambda s: (s > 0).sum()),
                  )
                  .reset_index()
                  .sort_values("pnl", ascending=False)
            )
            sym_rows_html = ""
            for _, _sr in sym_grp.iterrows():
                _sn   = _esc(str(_sr["symbol"]))
                _sp   = float(_sr["pnl"])
                _sn2  = int(_sr["n"])
                _sw   = int(_sr["wins"])
                _swr  = _sw / _sn2 if _sn2 > 0 else 0.0
                sym_rows_html += (
                    f'<tr>'
                    f'<td style="color:{T["text"]};font-weight:700;font-size:0.65rem;">{_sn}</td>'
                    f'<td style="color:{_pnl_col(_sp)};font-weight:700;font-family:{T["mono"]};'
                    f'font-size:0.65rem;">${_sp:+,.2f}</td>'
                    f'<td style="color:{_wr_col(_sw, _sn2)};font-family:{T["mono"]};'
                    f'font-size:0.65rem;">{_swr * 100:.0f}%</td>'
                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                    f'font-size:0.62rem;">{_sn2}×</td>'
                    f'</tr>'
                )
                csv_rows.append({
                    "section": "PERFORMANCE_BY_SYMBOL",
                    "metric": str(_sr["symbol"]),
                    "value": f"{_sp:+.2f}",
                    "sub": f"WR={_swr * 100:.0f}% n={_sn2}",
                })
            st.markdown(
                f'<div class="ot-tile ot-scroll" style="padding:0;margin-top:4px;">'
                f'<table class="ot-table"><thead><tr>'
                f'<th>SYM</th><th>PnL</th><th>WR</th><th>COUNT</th>'
                f'</tr></thead><tbody>{sym_rows_html}</tbody></table></div>',
                unsafe_allow_html=True,
            )

    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    # ── Section 2 — Exit Attribution ──────────────────────────────────────────
    st.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{T["text2"]};margin:8px 0 4px 0;">'
        f'EXIT ATTRIBUTION</div>',
        unsafe_allow_html=True,
    )
    has_exit_cat = (
        not df.empty
        and "exit_reason_category" in df.columns
        and df["exit_reason_category"].notna().any()
    )
    if not has_exit_cat:
        _slot_placeholder(
            "EXIT CATEGORY DATA UNAVAILABLE (pre-Phase-4 schema or empty window)", 50
        )
        csv_rows.append({
            "section": "EXIT_ATTRIBUTION", "metric": "unavailable",
            "value": "—", "sub": "",
        })
    else:
        _CAT_ORDER = ["STRATEGY", "RISK", "ANOMALY"]
        _CAT_COLS  = {
            "STRATEGY": T["green"],
            "RISK":     T["orange"],
            "ANOMALY":  T["red"],
        }
        cat_rows_html = ""
        for _cat in _CAT_ORDER:
            _sub = df[df["exit_reason_category"] == _cat]
            _n   = len(_sub)
            if _n == 0:
                continue
            _p   = float(_sub["realized_usd"].sum()) if "realized_usd" in _sub.columns else 0.0
            _w   = int((_sub["realized_usd"] > 0).sum()) if "realized_usd" in _sub.columns else 0
            _wr  = _w / _n if _n > 0 else 0.0
            _cc  = _CAT_COLS.get(_cat, T["text2"])
            cat_rows_html += (
                f'<tr>'
                f'<td style="color:{_cc};font-weight:700;font-family:{T["mono"]};'
                f'font-size:0.65rem;">{_esc(_cat)}</td>'
                f'<td style="color:{_pnl_col(_p)};font-weight:700;font-family:{T["mono"]};'
                f'font-size:0.65rem;">${_p:+,.2f}</td>'
                f'<td style="color:{_wr_col(_w, _n)};font-family:{T["mono"]};'
                f'font-size:0.65rem;">{_wr * 100:.0f}%</td>'
                f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                f'font-size:0.62rem;">{_n}×</td>'
                f'</tr>'
            )
            csv_rows.append({
                "section": "EXIT_ATTRIBUTION", "metric": _cat,
                "value": f"{_p:+.2f}",
                "sub": f"WR={_wr * 100:.0f}% n={_n}",
            })

        # OTHER — any category not in the three canonical buckets
        other_mask = (
            ~df["exit_reason_category"].isin(_CAT_ORDER)
            & df["exit_reason_category"].notna()
        )
        if other_mask.any():
            _sub_o = df[other_mask]
            _n_o   = len(_sub_o)
            _p_o   = float(_sub_o["realized_usd"].sum()) if "realized_usd" in _sub_o.columns else 0.0
            _w_o   = int((_sub_o["realized_usd"] > 0).sum()) if "realized_usd" in _sub_o.columns else 0
            _wr_o  = _w_o / _n_o if _n_o > 0 else 0.0
            cat_rows_html += (
                f'<tr>'
                f'<td style="color:{T["muted"]};font-weight:700;font-family:{T["mono"]};'
                f'font-size:0.65rem;">OTHER</td>'
                f'<td style="color:{_pnl_col(_p_o)};font-weight:700;font-family:{T["mono"]};'
                f'font-size:0.65rem;">${_p_o:+,.2f}</td>'
                f'<td style="color:{_wr_col(_w_o, _n_o)};font-family:{T["mono"]};'
                f'font-size:0.65rem;">{_wr_o * 100:.0f}%</td>'
                f'<td style="color:{T["muted"]};font-family:{T["mono"]};'
                f'font-size:0.62rem;">{_n_o}×</td>'
                f'</tr>'
            )
            csv_rows.append({
                "section": "EXIT_ATTRIBUTION", "metric": "OTHER",
                "value": f"{_p_o:+.2f}",
                "sub": f"WR={_wr_o * 100:.0f}% n={_n_o}",
            })

        if cat_rows_html:
            st.markdown(
                f'<div class="ot-tile ot-scroll" style="padding:0;">'
                f'<table class="ot-table"><thead><tr>'
                f'<th>CATEGORY</th><th>PnL</th><th>WIN RATE</th><th>COUNT</th>'
                f'</tr></thead><tbody>{cat_rows_html}</tbody></table></div>',
                unsafe_allow_html=True,
            )
        else:
            _slot_placeholder("NO CATEGORISED EXITS IN WINDOW", 50)

    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    # ── Section 3 — Admission Funnel ──────────────────────────────────────────
    st.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{T["text2"]};margin:8px 0 4px 0;">'
        f'ADMISSION FUNNEL — LAST {days_label}</div>',
        unsafe_allow_html=True,
    )
    dt_summary = _load_report_dt_summary(days_back, sym_filter)

    if not dt_summary:
        _slot_placeholder(
            "DECISION TRACE TABLE ABSENT OR EMPTY — start bot to populate", 50
        )
        csv_rows.append({
            "section": "ADMISSION_FUNNEL", "metric": "unavailable",
            "value": "—", "sub": "",
        })
    else:
        _dt_total = dt_summary.get("total",      0)
        _dt_adm   = dt_summary.get("n_admitted", 0)
        _dt_blk   = dt_summary.get("n_blocked",  0)
        _dt_shd   = dt_summary.get("n_shadow",   0)

        def _pct_str(n: int) -> str:
            return f"{n / _dt_total * 100:.1f}%" if _dt_total > 0 else "—"

        st.markdown(
            f'<div class="ot-tile" style="padding:8px 12px;">'
            f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.60rem;">'
            f'{_dt_total} SIGNALS EVALUATED &nbsp;|&nbsp;</span>'
            f'<span style="color:{T["green"]};font-family:{T["mono"]};'
            f'font-size:0.62rem;font-weight:700;">'
            f'✓ ADMITTED {_dt_adm} ({_pct_str(_dt_adm)}) &nbsp;</span>'
            f'<span style="color:{T["red"]};font-family:{T["mono"]};'
            f'font-size:0.62rem;font-weight:700;">'
            f'✗ BLOCKED {_dt_blk} ({_pct_str(_dt_blk)}) &nbsp;</span>'
            f'<span style="color:{T["blue"]};font-family:{T["mono"]};'
            f'font-size:0.62rem;font-weight:700;">'
            f'◈ SHADOW {_dt_shd} ({_pct_str(_dt_shd)})'
            f'</span></div>',
            unsafe_allow_html=True,
        )
        csv_rows += [
            {"section": "ADMISSION_FUNNEL", "metric": "total_evaluated",
             "value": str(_dt_total), "sub": days_label},
            {"section": "ADMISSION_FUNNEL", "metric": "admitted",
             "value": str(_dt_adm), "sub": _pct_str(_dt_adm)},
            {"section": "ADMISSION_FUNNEL", "metric": "blocked",
             "value": str(_dt_blk), "sub": _pct_str(_dt_blk)},
            {"section": "ADMISSION_FUNNEL", "metric": "shadow",
             "value": str(_dt_shd), "sub": _pct_str(_dt_shd)},
        ]

        top_gates = dt_summary.get("top_gates", [])
        if top_gates:
            gate_rows_html = ""
            for _rank, (_gk, _gl, _gc, _cnt) in enumerate(top_gates[:3], 1):
                _blk_pct = f"{_cnt / _dt_blk * 100:.0f}%" if _dt_blk > 0 else "—"
                gate_rows_html += (
                    f'<tr>'
                    f'<td style="color:{T["muted"]};font-family:{T["mono"]};'
                    f'font-size:0.60rem;">{_rank}</td>'
                    f'<td><span class="ot-badge" style="color:{_gc};background:{_gc}18;'
                    f'border:1px solid {_gc}40;">{_esc(_gl)}</span></td>'
                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                    f'font-size:0.65rem;font-weight:700;">{_cnt}</td>'
                    f'<td style="color:{T["muted"]};font-family:{T["mono"]};'
                    f'font-size:0.62rem;">{_blk_pct} of blocked</td>'
                    f'</tr>'
                )
                csv_rows.append({
                    "section": "ADMISSION_FUNNEL_TOP_GATES",
                    "metric": _gk,
                    "value": str(_cnt),
                    "sub": f"{_blk_pct} of blocked",
                })
            st.markdown(
                f'<div class="ot-tile ot-scroll" style="padding:0;margin-top:4px;">'
                f'<table class="ot-table"><thead><tr>'
                f'<th>#</th><th>TOP BLOCKING GATE</th><th>COUNT</th><th>SHARE</th>'
                f'</tr></thead><tbody>{gate_rows_html}</tbody></table></div>',
                unsafe_allow_html=True,
            )

    # ── [TRACK19-AGG2 / TRACK20-BRK2] Gate analytics expander ────────────────
    try:
        _gbd = _load_gate_breakdown(days_back, sym_filter)
        _ga_rows = _gbd.get("by_gate", []) if _gbd else []
        if _ga_rows:
            with st.expander(
                "📊 Gate admission rates & outcome context — aggregate view",
                expanded=False,
            ):
                st.markdown(
                    f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                    f'color:{T["muted"]};margin-bottom:6px;">'
                    f'For each blocking gate: blocks in last {days_label}, how many were '
                    f'followed by an ADMITTED decision on the same symbol + direction '
                    f'within {_CORR19_ADMITTED_WINDOW_SECS}s, and aggregate outcome where '
                    f'a close was found within {_CORR18_CLOSE_WINDOW_SECS // 3600}h. '
                    f'Rate ≥50% (yellow) may indicate excessive conservatism. '
                    f'Read-only.</div>',
                    unsafe_allow_html=True,
                )
                _ga_html_rows = ""
                for _gr in _ga_rows:
                    _gk2    = _gr["gate_key"]
                    _glbl   = _esc(_gr["label"])
                    _gcol   = _gr["colour"]
                    _nb2    = _gr["n_blocked"]
                    _na2    = _gr["n_later_adm"]
                    _rate   = _gr["adm_rate"]
                    _mpnl   = _gr["mean_pnl_pct"]
                    _mhld   = _gr["mean_hold_secs"]

                    # Admission-rate colour: ≥50% → yellow (may be too strict),
                    # <10% → green (gate effective), else muted
                    if _rate >= 0.50:
                        _rate_col = T["yellow"]
                    elif _rate < 0.10:
                        _rate_col = T["green"]
                    else:
                        _rate_col = T["text2"]
                    _rate_str = f"{_rate * 100:.0f}%"

                    # PnL colour
                    if _mpnl is not None:
                        _pnl_col2 = T["green"] if _mpnl >= 0 else T["red"]
                        _pnl_str  = f'<span style="color:{_pnl_col2};font-weight:700;">{_mpnl:+.2f}%</span>'
                    else:
                        _pnl_str  = f'<span style="color:{T["muted"]};">—</span>'

                    # Hold-time formatting
                    if _mhld is not None:
                        _hm, _hs = divmod(int(_mhld), 60)
                        _hld_str = f"{_hm}m{_hs:02d}s" if _hm else f"{_hs}s"
                    else:
                        _hld_str = "—"

                    _ga_html_rows += (
                        f'<tr>'
                        f'<td><span class="ot-badge" style="color:{_gcol};'
                        f'background:{_gcol}18;border:1px solid {_gcol}40;">'
                        f'{_glbl}</span></td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.65rem;font-weight:700;">{_nb2}</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{_na2}</td>'
                        f'<td style="color:{_rate_col};font-family:{T["mono"]};'
                        f'font-size:0.65rem;font-weight:700;">{_rate_str}</td>'
                        f'<td style="font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_pnl_str}</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.62rem;">{_esc(_hld_str)}</td>'
                        f'</tr>'
                    )
                    # CSV rows for each gate
                    csv_rows.append({
                        "section": "GATE_AGGREGATE",
                        "metric":  _gk2,
                        "value":   f"{_rate * 100:.1f}% admitted",
                        "sub":     (
                            f"blocks={_nb2} later_adm={_na2}"
                            + (f" mean_pnl={_mpnl:+.2f}%" if _mpnl is not None else "")
                            + (f" mean_hold={_hld_str}" if _mhld is not None else "")
                        ),
                    })

                st.markdown(
                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>GATE</th><th>BLOCKS</th><th>LATER ADM</th>'
                    f'<th>RATE</th><th>AVG PnL</th><th>AVG HOLD</th>'
                    f'</tr></thead><tbody>{_ga_html_rows}</tbody></table></div>',
                    unsafe_allow_html=True,
                )

                # ── [TRACK20-BRK2] Per-symbol / direction drill-down ──────────
                _by_sym = _gbd.get("by_symbol", {}) if _gbd else {}
                _by_dir = _gbd.get("by_direction", {}) if _gbd else {}

                # Gate selector — labels built from by_gate order
                _drill_opts = ["— select gate —"] + [
                    f"{_DT_GATE_LABEL.get(_gr['gate_key'], _gr['gate_key'][:12])} ({_gr['gate_key']})"
                    for _gr in _ga_rows
                ]
                # Map label → gate_key for lookup
                _drill_label_to_key = {
                    f"{_DT_GATE_LABEL.get(_gr['gate_key'], _gr['gate_key'][:12])} ({_gr['gate_key']})": _gr["gate_key"]
                    for _gr in _ga_rows
                }

                st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
                _drill_sel = st.selectbox(
                    "Drill into gate:",
                    options=_drill_opts,
                    index=0,
                    key="rpt_gate_drill",
                    label_visibility="visible",
                )

                _drill_gk = _drill_label_to_key.get(_drill_sel)
                if _drill_gk:
                    _d_sym_rows = _by_sym.get(_drill_gk, [])
                    _d_dir_map  = _by_dir.get(_drill_gk, {})

                    st.markdown(
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.10em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin:6px 0 4px 0;">'
                        f'{_esc(_DT_GATE_LABEL.get(_drill_gk, _drill_gk))} — '
                        f'per-symbol &amp; per-direction breakdown</div>',
                        unsafe_allow_html=True,
                    )

                    _dcol_left, _dcol_right = st.columns(2)

                    # ── Per-symbol table ──────────────────────────────────────
                    def _rate_col_fn(rate: float) -> str:
                        if rate >= 0.50:
                            return T["yellow"]
                        if rate < 0.10:
                            return T["green"]
                        return T["text2"]

                    def _pnl_str_fn(mpnl) -> str:
                        if mpnl is not None:
                            _c = T["green"] if mpnl >= 0 else T["red"]
                            return (
                                f'<span style="color:{_c};font-weight:700;">'
                                f'{mpnl:+.2f}%</span>'
                            )
                        return f'<span style="color:{T["muted"]};">—</span>'

                    def _hold_str_fn(mhld) -> str:
                        if mhld is not None:
                            _hm2, _hs2 = divmod(int(mhld), 60)
                            return f"{_hm2}m{_hs2:02d}s" if _hm2 else f"{_hs2}s"
                        return "—"

                    with _dcol_left:
                        st.markdown(
                            f'<div style="font-family:{T["mono"]};font-size:0.56rem;'
                            f'color:{T["muted"]};margin-bottom:3px;">BY SYMBOL</div>',
                            unsafe_allow_html=True,
                        )
                        if _d_sym_rows:
                            _sym_tbl_rows = ""
                            for _sr in _d_sym_rows:
                                if _sr["n_blocked"] == 0:
                                    continue
                                _s_sym  = _esc(str(_sr["symbol"]))
                                _s_nb   = _sr["n_blocked"]
                                _s_na   = _sr["n_later_adm"]
                                _s_rate = _sr["adm_rate"]
                                _s_rc   = _rate_col_fn(_s_rate)
                                _s_rs   = f"{_s_rate * 100:.0f}%"
                                _s_ps   = _pnl_str_fn(_sr["mean_pnl_pct"])
                                _s_hs   = _hold_str_fn(_sr["mean_hold_secs"])
                                _sym_tbl_rows += (
                                    f'<tr>'
                                    f'<td style="color:{T["text"]};font-weight:700;'
                                    f'font-size:0.65rem;">{_s_sym}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;font-weight:700;">{_s_nb}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;">{_s_na}</td>'
                                    f'<td style="color:{_s_rc};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;font-weight:700;">{_s_rs}</td>'
                                    f'<td style="font-family:{T["mono"]};'
                                    f'font-size:0.65rem;">{_s_ps}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.62rem;">{_esc(_s_hs)}</td>'
                                    f'</tr>'
                                )
                                csv_rows.append({
                                    "section": "GATE_BREAKDOWN_BY_SYMBOL",
                                    "metric":  f"{_drill_gk}:{_sr['symbol']}",
                                    "value":   f"{_s_rate * 100:.1f}% admitted",
                                    "sub": (
                                        f"blocks={_s_nb} later_adm={_s_na}"
                                        + (f" mean_pnl={_sr['mean_pnl_pct']:+.2f}%"
                                           if _sr["mean_pnl_pct"] is not None else "")
                                    ),
                                })
                            if _sym_tbl_rows:
                                st.markdown(
                                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                                    f'<table class="ot-table"><thead><tr>'
                                    f'<th>SYM</th><th>BLOCKS</th><th>LATER ADM</th>'
                                    f'<th>RATE</th><th>AVG PnL</th><th>AVG HOLD</th>'
                                    f'</tr></thead><tbody>{_sym_tbl_rows}</tbody>'
                                    f'</table></div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.markdown(
                                    f'<span style="color:{T["muted"]};'
                                    f'font-family:{T["mono"]};font-size:0.62rem;">'
                                    f'no symbol data</span>',
                                    unsafe_allow_html=True,
                                )
                        else:
                            st.markdown(
                                f'<span style="color:{T["muted"]};'
                                f'font-family:{T["mono"]};font-size:0.62rem;">'
                                f'no symbol data</span>',
                                unsafe_allow_html=True,
                            )

                    # ── Per-direction table ───────────────────────────────────
                    with _dcol_right:
                        st.markdown(
                            f'<div style="font-family:{T["mono"]};font-size:0.56rem;'
                            f'color:{T["muted"]};margin-bottom:3px;">BY DIRECTION</div>',
                            unsafe_allow_html=True,
                        )
                        if _d_dir_map:
                            _dir_tbl_rows = ""
                            for _dkey in ("LONG", "SHORT"):
                                _dd = _d_dir_map.get(_dkey)
                                if _dd is None or _dd.get("n_blocked", 0) == 0:
                                    continue
                                _d_nb   = _dd["n_blocked"]
                                _d_na   = _dd["n_later_adm"]
                                _d_rate = _dd["adm_rate"]
                                _d_rc   = _rate_col_fn(_d_rate)
                                _d_rs   = f"{_d_rate * 100:.0f}%"
                                _d_dc   = T["green"] if _dkey == "LONG" else T["red"]
                                _d_ps   = _pnl_str_fn(_dd["mean_pnl_pct"])
                                _d_hs   = _hold_str_fn(_dd["mean_hold_secs"])
                                _dir_tbl_rows += (
                                    f'<tr>'
                                    f'<td style="color:{_d_dc};font-weight:700;'
                                    f'font-family:{T["mono"]};font-size:0.65rem;">'
                                    f'{_esc(_dkey)}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;font-weight:700;">{_d_nb}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;">{_d_na}</td>'
                                    f'<td style="color:{_d_rc};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;font-weight:700;">{_d_rs}</td>'
                                    f'<td style="font-family:{T["mono"]};'
                                    f'font-size:0.65rem;">{_d_ps}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.62rem;">{_esc(_d_hs)}</td>'
                                    f'</tr>'
                                )
                                csv_rows.append({
                                    "section": "GATE_BREAKDOWN_BY_DIR",
                                    "metric":  f"{_drill_gk}:{_dkey}",
                                    "value":   f"{_d_rate * 100:.1f}% admitted",
                                    "sub": (
                                        f"blocks={_d_nb} later_adm={_d_na}"
                                        + (f" mean_pnl={_dd['mean_pnl_pct']:+.2f}%"
                                           if _dd["mean_pnl_pct"] is not None else "")
                                    ),
                                })
                            # Also include any direction values that are neither LONG nor SHORT
                            for _dkey2, _dd2 in _d_dir_map.items():
                                if _dkey2 in ("LONG", "SHORT"):
                                    continue
                                if _dd2.get("n_blocked", 0) == 0:
                                    continue
                                _d_nb2   = _dd2["n_blocked"]
                                _d_na2   = _dd2["n_later_adm"]
                                _d_rate2 = _dd2["adm_rate"]
                                _d_rc2   = _rate_col_fn(_d_rate2)
                                _d_rs2   = f"{_d_rate2 * 100:.0f}%"
                                _d_ps2   = _pnl_str_fn(_dd2["mean_pnl_pct"])
                                _d_hs2   = _hold_str_fn(_dd2["mean_hold_secs"])
                                _dir_tbl_rows += (
                                    f'<tr>'
                                    f'<td style="color:{T["text2"]};font-weight:700;'
                                    f'font-family:{T["mono"]};font-size:0.65rem;">'
                                    f'{_esc(_dkey2[:8])}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;font-weight:700;">{_d_nb2}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;">{_d_na2}</td>'
                                    f'<td style="color:{_d_rc2};font-family:{T["mono"]};'
                                    f'font-size:0.65rem;font-weight:700;">{_d_rs2}</td>'
                                    f'<td style="font-family:{T["mono"]};'
                                    f'font-size:0.65rem;">{_d_ps2}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.62rem;">{_esc(_d_hs2)}</td>'
                                    f'</tr>'
                                )
                            if _dir_tbl_rows:
                                st.markdown(
                                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                                    f'<table class="ot-table"><thead><tr>'
                                    f'<th>DIR</th><th>BLOCKS</th><th>LATER ADM</th>'
                                    f'<th>RATE</th><th>AVG PnL</th><th>AVG HOLD</th>'
                                    f'</tr></thead><tbody>{_dir_tbl_rows}</tbody>'
                                    f'</table></div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.markdown(
                                    f'<span style="color:{T["muted"]};'
                                    f'font-family:{T["mono"]};font-size:0.62rem;">'
                                    f'no direction data</span>',
                                    unsafe_allow_html=True,
                                )
                        else:
                            st.markdown(
                                f'<span style="color:{T["muted"]};'
                                f'font-family:{T["mono"]};font-size:0.62rem;">'
                                f'no direction data</span>',
                                unsafe_allow_html=True,
                            )
                # ── [/TRACK20-BRK2] ──────────────────────────────────────────

    except Exception:
        log.error("[TRACK19-AGG2] gate_aggregate expander failed", exc_info=True)
    # ── [/TRACK19-AGG2] ───────────────────────────────────────────────────────

    # ── [TRACK21-XPL2] Gate realized-PnL cross-table expander ────────────────
    try:
        _xpl_rows = _load_gate_realized_pnl(days_back, sym_filter)
        if _xpl_rows:
            with st.expander(
                "💰 Gate-level realized trade profile — cross-table view",
                expanded=False,
            ):
                _xpl_dir_matched = _xpl_rows[0].get("dir_matched", False)
                _xpl_note = (
                    f'For each blocking gate: of the trades that opened within '
                    f'{_CORR21_TRADE_WINDOW_SECS}s after a block on the same symbol'
                    + (' + direction' if _xpl_dir_matched else
                       ' (direction column absent — symbol-only match)')
                    + ', realized profitability from the trades ledger.  '
                    f'WIN% ≥60% (green) → gate mainly caught losers.  '
                    f'WIN% ≤35% (red) → gate may have blocked winners.  '
                    f'Cross-table join; read-only.'
                )
                st.markdown(
                    f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                    f'color:{T["muted"]};margin-bottom:6px;">{_esc(_xpl_note)}</div>',
                    unsafe_allow_html=True,
                )

                _xpl_html_rows = ""
                for _xr in _xpl_rows:
                    _xgk   = _xr["gate_key"]
                    _xgl   = _esc(_xr["label"])
                    _xgc   = _xr["colour"]
                    _xnb   = _xr["n_blocked"]
                    _xnh   = _xr["n_with_trade"]
                    _xwr   = _xr["win_rate"]
                    _xpnl  = _xr["mean_pnl_pct"]
                    _xusd  = _xr["mean_realized_usd"]
                    _xslp  = _xr["mean_slippage_bps"]

                    # WIN% colour: ≥60% green, ≤35% red, else muted
                    if _xwr is not None:
                        if _xwr >= 0.60:
                            _xwr_col = T["green"]
                        elif _xwr <= 0.35:
                            _xwr_col = T["red"]
                        else:
                            _xwr_col = T["text2"]
                        _xwr_str = f"{_xwr * 100:.0f}%"
                    else:
                        _xwr_col = T["muted"]
                        _xwr_str = "—"

                    # AVG PnL% colour
                    if _xpnl is not None:
                        _xpnl_col = T["green"] if _xpnl >= 0 else T["red"]
                        _xpnl_str = (
                            f'<span style="color:{_xpnl_col};font-weight:700;">'
                            f'{_xpnl:+.2f}%</span>'
                        )
                    else:
                        _xpnl_str = f'<span style="color:{T["muted"]};">—</span>'

                    # AVG USD
                    if _xusd is not None:
                        _xusd_col = T["green"] if _xusd >= 0 else T["red"]
                        _xusd_str = (
                            f'<span style="color:{_xusd_col};font-weight:700;">'
                            f'${_xusd:+,.2f}</span>'
                        )
                    else:
                        _xusd_str = f'<span style="color:{T["muted"]};">—</span>'

                    # AVG SLIP
                    _xslp_str = (
                        f"{_xslp:.1f}bps"
                        if _xslp is not None
                        else "—"
                    )

                    _xpl_html_rows += (
                        f'<tr>'
                        f'<td><span class="ot-badge" style="color:{_xgc};'
                        f'background:{_xgc}18;border:1px solid {_xgc}40;">'
                        f'{_xgl}</span></td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.65rem;font-weight:700;">{_xnb}</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{_xnh}</td>'
                        f'<td style="color:{_xwr_col};font-family:{T["mono"]};'
                        f'font-size:0.65rem;font-weight:700;">{_esc(_xwr_str)}</td>'
                        f'<td style="font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_xpnl_str}</td>'
                        f'<td style="font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_xusd_str}</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.62rem;">{_esc(_xslp_str)}</td>'
                        f'</tr>'
                    )
                    csv_rows.append({
                        "section": "GATE_REALIZED_PNL",
                        "metric":  _xgk,
                        "value":   (
                            f"win_rate={f'{_xwr*100:.1f}%' if _xwr is not None else '—'}"
                        ),
                        "sub": (
                            f"blocks={_xnb} trades_after={_xnh}"
                            + (f" mean_pnl={_xpnl:+.2f}%"
                               if _xpnl is not None else "")
                            + (f" mean_usd={_xusd:+.2f}"
                               if _xusd is not None else "")
                            + (f" mean_slip={_xslp:.1f}bps"
                               if _xslp is not None else "")
                        ),
                    })

                st.markdown(
                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>GATE</th><th>BLOCKS</th><th>TRADES AFTER</th>'
                    f'<th>WIN%</th><th>AVG PnL%</th>'
                    f'<th>AVG USD</th><th>AVG SLIP</th>'
                    f'</tr></thead><tbody>{_xpl_html_rows}</tbody></table></div>',
                    unsafe_allow_html=True,
                )
    except Exception:
        log.error("[TRACK21-XPL2] gate_realized_pnl expander failed", exc_info=True)
    # ── [/TRACK21-XPL2] ──────────────────────────────────────────────────────

    # ── [TRACK22-DRL1] Gate realized-PnL drill-down — by symbol & direction ──
    try:
        _drl_xpl_rows = _load_gate_realized_pnl(days_back, sym_filter)
        _drl_data     = _load_gate_realized_pnl_drilldown(days_back, sym_filter)
        if _drl_xpl_rows and _drl_data:
            with st.expander(
                "🔍 Gate realized-PnL drill-down — by symbol & direction",
                expanded=False,
            ):
                # ── Gate selector ─────────────────────────────────────────────
                _drl_gate_opts   = [r["gate_key"] for r in _drl_xpl_rows]
                _drl_gate_labels = {r["gate_key"]: r["label"] for r in _drl_xpl_rows}
                _drl_options     = ["— select gate —"] + _drl_gate_opts
                _drl_sel = st.selectbox(
                    "Gate",
                    options=_drl_options,
                    format_func=lambda k: (
                        k if k == "— select gate —"
                        else _drl_gate_labels.get(k, k)
                    ),
                    key="rpt_gate_xpl_drill",
                    label_visibility="collapsed",
                )

                if _drl_sel and _drl_sel != "— select gate —":
                    _drl_sym_rows = _drl_data.get("by_symbol",    {}).get(_drl_sel, [])
                    _drl_dir_rows = _drl_data.get("by_direction", {}).get(_drl_sel, [])
                    _drl_dir_ok   = _drl_data.get("dir_matched", False)

                    _dc_sym, _dc_dir = st.columns(2)

                    # ── By-symbol table ───────────────────────────────────────
                    with _dc_sym:
                        st.markdown(
                            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                            f'letter-spacing:0.10em;text-transform:uppercase;'
                            f'color:{T["text2"]};margin-bottom:4px;">BY SYMBOL</div>',
                            unsafe_allow_html=True,
                        )
                        _drl_sym_visible = [
                            r for r in _drl_sym_rows if r["n_blocked"] > 0
                        ]
                        if _drl_sym_visible:
                            _drl_sym_html = ""
                            for _sr in _drl_sym_visible:
                                _swr  = _sr["win_rate"]
                                _spnl = _sr["mean_pnl_pct"]
                                _susd = _sr["mean_realized_usd"]
                                # WIN% colour: ≥60% green, ≤35% red, else text2
                                if _swr is not None:
                                    _swr_c = (
                                        T["green"] if _swr >= 0.60 else
                                        T["red"]   if _swr <= 0.35 else
                                        T["text2"]
                                    )
                                    _swr_s = f"{_swr * 100:.0f}%"
                                else:
                                    _swr_c = T["muted"]
                                    _swr_s = "—"
                                # AVG PnL% colour
                                if _spnl is not None:
                                    _spnl_c = T["green"] if _spnl >= 0 else T["red"]
                                    _spnl_s = (
                                        f'<span style="color:{_spnl_c};font-weight:700;">'
                                        f'{_spnl:+.2f}%</span>'
                                    )
                                else:
                                    _spnl_s = (
                                        f'<span style="color:{T["muted"]};">—</span>'
                                    )
                                # AVG USD colour
                                if _susd is not None:
                                    _susd_c = T["green"] if _susd >= 0 else T["red"]
                                    _susd_s = (
                                        f'<span style="color:{_susd_c};">'
                                        f'${_susd:+,.2f}</span>'
                                    )
                                else:
                                    _susd_s = (
                                        f'<span style="color:{T["muted"]};">—</span>'
                                    )
                                _drl_sym_html += (
                                    f'<tr>'
                                    f'<td style="color:{T["text1"]};font-family:{T["mono"]};'
                                    f'font-size:0.63rem;">{_esc(_sr["symbol"])}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.63rem;font-weight:700;">'
                                    f'{_sr["n_blocked"]}</td>'
                                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                    f'font-size:0.63rem;">{_sr["n_with_trade"]}</td>'
                                    f'<td style="color:{_swr_c};font-family:{T["mono"]};'
                                    f'font-size:0.63rem;font-weight:700;">'
                                    f'{_esc(_swr_s)}</td>'
                                    f'<td style="font-family:{T["mono"]};'
                                    f'font-size:0.63rem;">{_spnl_s}</td>'
                                    f'<td style="font-family:{T["mono"]};'
                                    f'font-size:0.63rem;">{_susd_s}</td>'
                                    f'</tr>'
                                )
                                csv_rows.append({
                                    "section": "GATE_REALIZED_PNL_BY_SYMBOL",
                                    "metric":  f"{_drl_sel}|{_sr['symbol']}",
                                    "value":   (
                                        f"win_rate="
                                        + (f"{_swr * 100:.1f}%"
                                           if _swr is not None else "—")
                                    ),
                                    "sub": (
                                        f"blocks={_sr['n_blocked']}"
                                        f" trades_after={_sr['n_with_trade']}"
                                        + (f" mean_pnl={_spnl:+.2f}%"
                                           if _spnl is not None else "")
                                        + (f" mean_usd={_susd:+.2f}"
                                           if _susd is not None else "")
                                    ),
                                })
                            st.markdown(
                                f'<div class="ot-tile ot-scroll" style="padding:0;">'
                                f'<table class="ot-table"><thead><tr>'
                                f'<th>SYM</th><th>BLOCKS</th><th>TRADES</th>'
                                f'<th>WIN%</th><th>AVG PnL%</th><th>AVG USD</th>'
                                f'</tr></thead><tbody>'
                                f'{_drl_sym_html}'
                                f'</tbody></table></div>',
                                unsafe_allow_html=True,
                            )
                        else:
                            st.markdown(
                                f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                                f'color:{T["muted"]};">'
                                f'No symbol data for this gate.</div>',
                                unsafe_allow_html=True,
                            )

                    # ── By-direction table ────────────────────────────────────
                    with _dc_dir:
                        st.markdown(
                            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                            f'letter-spacing:0.10em;text-transform:uppercase;'
                            f'color:{T["text2"]};margin-bottom:4px;">BY DIRECTION</div>',
                            unsafe_allow_html=True,
                        )
                        if not _drl_dir_ok:
                            # Graceful degrade: entry_direction column absent
                            st.markdown(
                                f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                                f'color:{T["muted"]};">'
                                f'Direction column absent — symbol-only match used.</div>',
                                unsafe_allow_html=True,
                            )
                        else:
                            _drl_dir_visible = [
                                r for r in _drl_dir_rows if r["n_blocked"] > 0
                            ]
                            if _drl_dir_visible:
                                _drl_dir_html = ""
                                for _dr in _drl_dir_visible:
                                    _dwr  = _dr["win_rate"]
                                    _dpnl = _dr["mean_pnl_pct"]
                                    _dusd = _dr["mean_realized_usd"]
                                    # WIN% colour
                                    if _dwr is not None:
                                        _dwr_c = (
                                            T["green"] if _dwr >= 0.60 else
                                            T["red"]   if _dwr <= 0.35 else
                                            T["text2"]
                                        )
                                        _dwr_s = f"{_dwr * 100:.0f}%"
                                    else:
                                        _dwr_c = T["muted"]
                                        _dwr_s = "—"
                                    # AVG PnL% colour
                                    if _dpnl is not None:
                                        _dpnl_c = T["green"] if _dpnl >= 0 else T["red"]
                                        _dpnl_s = (
                                            f'<span style="color:{_dpnl_c};font-weight:700;">'
                                            f'{_dpnl:+.2f}%</span>'
                                        )
                                    else:
                                        _dpnl_s = (
                                            f'<span style="color:{T["muted"]};">—</span>'
                                        )
                                    # AVG USD colour
                                    if _dusd is not None:
                                        _dusd_c = T["green"] if _dusd >= 0 else T["red"]
                                        _dusd_s = (
                                            f'<span style="color:{_dusd_c};">'
                                            f'${_dusd:+,.2f}</span>'
                                        )
                                    else:
                                        _dusd_s = (
                                            f'<span style="color:{T["muted"]};">—</span>'
                                        )
                                    _drl_dir_html += (
                                        f'<tr>'
                                        f'<td style="color:{T["text1"]};font-family:{T["mono"]};'
                                        f'font-size:0.63rem;">'
                                        f'{_esc(_dr["direction"] or "—")}</td>'
                                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                        f'font-size:0.63rem;font-weight:700;">'
                                        f'{_dr["n_blocked"]}</td>'
                                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                                        f'font-size:0.63rem;">{_dr["n_with_trade"]}</td>'
                                        f'<td style="color:{_dwr_c};font-family:{T["mono"]};'
                                        f'font-size:0.63rem;font-weight:700;">'
                                        f'{_esc(_dwr_s)}</td>'
                                        f'<td style="font-family:{T["mono"]};'
                                        f'font-size:0.63rem;">{_dpnl_s}</td>'
                                        f'<td style="font-family:{T["mono"]};'
                                        f'font-size:0.63rem;">{_dusd_s}</td>'
                                        f'</tr>'
                                    )
                                    csv_rows.append({
                                        "section": "GATE_REALIZED_PNL_BY_DIRECTION",
                                        "metric":  f"{_drl_sel}|{_dr['direction']}",
                                        "value":   (
                                            f"win_rate="
                                            + (f"{_dwr * 100:.1f}%"
                                               if _dwr is not None else "—")
                                        ),
                                        "sub": (
                                            f"blocks={_dr['n_blocked']}"
                                            f" trades_after={_dr['n_with_trade']}"
                                            + (f" mean_pnl={_dpnl:+.2f}%"
                                               if _dpnl is not None else "")
                                            + (f" mean_usd={_dusd:+.2f}"
                                               if _dusd is not None else "")
                                        ),
                                    })
                                st.markdown(
                                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                                    f'<table class="ot-table"><thead><tr>'
                                    f'<th>DIR</th><th>BLOCKS</th><th>TRADES</th>'
                                    f'<th>WIN%</th><th>AVG PnL%</th><th>AVG USD</th>'
                                    f'</tr></thead><tbody>'
                                    f'{_drl_dir_html}'
                                    f'</tbody></table></div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.markdown(
                                    f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                                    f'color:{T["muted"]};">'
                                    f'No direction data for this gate.</div>',
                                    unsafe_allow_html=True,
                                )
    except Exception:
        log.error("[TRACK22-DRL1] gate_realized_pnl drilldown failed", exc_info=True)
    # ── [/TRACK22-DRL1] ──────────────────────────────────────────────────────

    # ── [TRACK23-CMP1] Gate cohort vs. overall baseline comparison ────────────
    try:
        _cmp_rows = _load_gate_realized_pnl(days_back, sym_filter)

        # Baseline stats from df (round-trips already in scope, same window).
        _bl_n   = len(df) if not df.empty else 0
        _bl_wr: "float | None" = None
        _bl_pnl: "float | None" = None
        if _bl_n > 0:
            if "realized_usd" in df.columns and df["realized_usd"].notna().any():
                _bl_wins = int((df["realized_usd"] > 0).sum())
                _bl_wr   = _bl_wins / _bl_n
            if "pnl_pct" in df.columns and df["pnl_pct"].notna().any():
                _bl_pnl = float(df["pnl_pct"].dropna().mean())

        if _cmp_rows and _bl_n > 0:
            with st.expander(
                "⚖ Gate cohort vs. overall baseline",
                expanded=False,
            ):
                _cmp_dir_matched = _cmp_rows[0].get("dir_matched", False)
                _cmp_note = (
                    f"Baseline = all {_bl_n} closed trades in the last {days_label} "
                    f"window{' for ' + sym_filter if sym_filter != 'ALL' else ''}. "
                    f"Cohort = trades that opened within {_CORR21_TRADE_WINDOW_SECS}s "
                    f"after a block on the same symbol"
                    + (" + direction" if _cmp_dir_matched else
                       " (direction column absent — symbol-only match)")
                    + ". "
                    "Δ WIN% > +5pp (green) → gate mainly caught losers. "
                    "Δ WIN% < −5pp (red) → gate may have blocked winners. "
                    "Read-only."
                )
                st.markdown(
                    f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                    f'color:{T["muted"]};margin-bottom:6px;">{_esc(_cmp_note)}</div>',
                    unsafe_allow_html=True,
                )

                _cmp_html_rows = ""
                for _cr2 in _cmp_rows:
                    _cgk   = _cr2["gate_key"]
                    _cgl   = _esc(_cr2["label"])
                    _cgc   = _cr2["colour"]
                    _cwr   = _cr2.get("win_rate")        # float | None
                    _cpnl  = _cr2.get("mean_pnl_pct")    # float | None

                    # ── Cohort WIN% ──────────────────────────────────────────
                    if _cwr is not None:
                        if _cwr >= 0.60:
                            _cwr_col = T["green"]
                        elif _cwr <= 0.35:
                            _cwr_col = T["red"]
                        else:
                            _cwr_col = T["text2"]
                        _cwr_str = f"{_cwr * 100:.0f}%"
                    else:
                        _cwr_col = T["muted"]
                        _cwr_str = "—"

                    # ── Baseline WIN% ────────────────────────────────────────
                    if _bl_wr is not None:
                        if _bl_wr >= 0.60:
                            _bwr_col = T["green"]
                        elif _bl_wr <= 0.35:
                            _bwr_col = T["red"]
                        else:
                            _bwr_col = T["text2"]
                        _bwr_str = f"{_bl_wr * 100:.0f}%"
                    else:
                        _bwr_col = T["muted"]
                        _bwr_str = "—"

                    # ── Δ WIN% ───────────────────────────────────────────────
                    if _cwr is not None and _bl_wr is not None:
                        _dwr      = _cwr - _bl_wr
                        _dwr_str  = f"{_dwr * 100:+.0f}pp"
                        if _dwr > 0.05:
                            _dwr_col = T["green"]
                        elif _dwr < -0.05:
                            _dwr_col = T["red"]
                        else:
                            _dwr_col = T["text2"]
                    else:
                        _dwr_str = "—"
                        _dwr_col = T["muted"]

                    # ── Cohort PnL% ──────────────────────────────────────────
                    if _cpnl is not None:
                        _cpnl_col = T["green"] if _cpnl >= 0 else T["red"]
                        _cpnl_str = (
                            f'<span style="color:{_cpnl_col};font-weight:700;">'
                            f'{_cpnl:+.2f}%</span>'
                        )
                    else:
                        _cpnl_str = f'<span style="color:{T["muted"]};">—</span>'

                    # ── Baseline PnL% ────────────────────────────────────────
                    if _bl_pnl is not None:
                        _bpnl_col = T["green"] if _bl_pnl >= 0 else T["red"]
                        _bpnl_str = (
                            f'<span style="color:{_bpnl_col};font-weight:700;">'
                            f'{_bl_pnl:+.2f}%</span>'
                        )
                    else:
                        _bpnl_str = f'<span style="color:{T["muted"]};">—</span>'

                    # ── Δ PnL% ───────────────────────────────────────────────
                    if _cpnl is not None and _bl_pnl is not None:
                        _dpnl     = _cpnl - _bl_pnl
                        _dpnl_col = T["green"] if _dpnl >= 0 else T["red"]
                        _dpnl_str = (
                            f'<span style="color:{_dpnl_col};font-weight:700;">'
                            f'{_dpnl:+.2f}pp</span>'
                        )
                    else:
                        _dpnl_str = f'<span style="color:{T["muted"]};">—</span>'

                    _cmp_html_rows += (
                        f'<tr>'
                        f'<td><span class="ot-badge" style="color:{_cgc};'
                        f'background:{_cgc}18;border:1px solid {_cgc}40;">'
                        f'{_cgl}</span></td>'
                        f'<td style="color:{_cwr_col};font-family:{T["mono"]};'
                        f'font-size:0.65rem;font-weight:700;">{_esc(_cwr_str)}</td>'
                        f'<td style="color:{_bwr_col};font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{_esc(_bwr_str)}</td>'
                        f'<td style="color:{_dwr_col};font-family:{T["mono"]};'
                        f'font-size:0.65rem;font-weight:700;">{_esc(_dwr_str)}</td>'
                        f'<td style="font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_cpnl_str}</td>'
                        f'<td style="font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_bpnl_str}</td>'
                        f'<td style="font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_dpnl_str}</td>'
                        f'</tr>'
                    )

                    # CSV row per gate
                    csv_rows.append({
                        "section": "GATE_VS_BASELINE",
                        "metric":  _cgk,
                        "value": (
                            f"cohort_wr={_cwr_str} baseline_wr={_bwr_str} "
                            f"delta_wr={_dwr_str}"
                        ),
                        "sub": (
                            f"cohort_pnl="
                            + (f"{_cpnl:+.2f}%" if _cpnl is not None else "—")
                            + f" baseline_pnl="
                            + (f"{_bl_pnl:+.2f}%" if _bl_pnl is not None else "—")
                        ),
                    })

                st.markdown(
                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>GATE</th>'
                    f'<th>COHORT WIN%</th><th>BASE WIN%</th><th>Δ WIN%</th>'
                    f'<th>COHORT PnL%</th><th>BASE PnL%</th><th>Δ PnL%</th>'
                    f'</tr></thead><tbody>{_cmp_html_rows}</tbody></table></div>',
                    unsafe_allow_html=True,
                )
    except Exception:
        log.error("[TRACK23-CMP1] gate_vs_baseline expander failed", exc_info=True)
    # ── [/TRACK23-CMP1] ──────────────────────────────────────────────────────

    # ── [TRACK24-SYM1] Gate cohort vs. per-symbol baseline comparison ─────────
    try:
        _s24_xpl_rows = _load_gate_realized_pnl(days_back, sym_filter)
        _s24_drl_data = _load_gate_realized_pnl_drilldown(days_back, sym_filter)
        _s24_by_sym   = _s24_drl_data.get("by_symbol", {}) if _s24_drl_data else {}
        _s24_df_ok    = (
            not df.empty
            and "symbol" in df.columns
            and df["symbol"].notna().any()
        )

        if _s24_xpl_rows and _s24_by_sym and _s24_df_ok:

            # Pre-compute per-symbol baseline from df (single pass, no DB call).
            # Keyed by symbol string; all per-row accesses are O(1) dict lookups.
            _sym_bl: "dict[str, dict]" = {}
            _s24_has_usd = (
                "realized_usd" in df.columns
                and df["realized_usd"].notna().any()
            )
            _s24_has_pnl = (
                "pnl_pct" in df.columns
                and df["pnl_pct"].notna().any()
            )
            for _s24_sym in df["symbol"].dropna().unique():
                _s24_dfs = df[df["symbol"] == _s24_sym]
                _s24_n   = len(_s24_dfs)
                _s24_bwr: "float | None" = None
                _s24_bpl: "float | None" = None
                if _s24_n > 0:
                    if _s24_has_usd and _s24_dfs["realized_usd"].notna().any():
                        _s24_bwins = int((_s24_dfs["realized_usd"] > 0).sum())
                        _s24_bwr   = _s24_bwins / _s24_n
                    if _s24_has_pnl and _s24_dfs["pnl_pct"].notna().any():
                        _s24_bpl = float(_s24_dfs["pnl_pct"].dropna().mean())
                _sym_bl[str(_s24_sym)] = {
                    "n":        _s24_n,
                    "win_rate": _s24_bwr,
                    "pnl_pct":  _s24_bpl,
                }

            with st.expander(
                "📊 Gate cohort vs. per-symbol baseline",
                expanded=False,
            ):
                _s24_dir_ok = _s24_drl_data.get("dir_matched", False)
                _s24_note = (
                    f"Baseline per row = all closed trades for that symbol in "
                    f"the last {days_label} window. "
                    f"Cohort = trades that opened within "
                    f"{_CORR21_TRADE_WINDOW_SECS}s after a block on the same symbol"
                    + (" + direction" if _s24_dir_ok else
                       " (direction column absent \u2014 symbol-only match)")
                    + ". "
                    "Δ WIN% > +5pp (green) \u2192 gate mainly caught losers for "
                    "that symbol. "
                    "Δ WIN% < \u22125pp (red) \u2192 gate may have blocked winners. "
                    "Read-only."
                )
                st.markdown(
                    f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                    f'color:{T["muted"]};margin-bottom:6px;">'
                    f'{_esc(_s24_note)}</div>',
                    unsafe_allow_html=True,
                )

                # Gate selector
                _s24_gate_opts    = [r["gate_key"] for r in _s24_xpl_rows]
                _s24_gate_labels  = {r["gate_key"]: r["label"]  for r in _s24_xpl_rows}
                _s24_gate_colours = {r["gate_key"]: r["colour"] for r in _s24_xpl_rows}
                _s24_options      = ["— select gate —"] + _s24_gate_opts
                _s24_sel = st.selectbox(
                    "Gate",
                    options=_s24_options,
                    format_func=lambda k: (
                        k if k == "— select gate —"
                        else _s24_gate_labels.get(k, k)
                    ),
                    key="rpt_sym_cmp_gate",
                    label_visibility="collapsed",
                )

                if _s24_sel and _s24_sel != "— select gate —":
                    _s24_sym_rows = _s24_by_sym.get(_s24_sel, [])
                    _s24_visible  = [
                        r for r in _s24_sym_rows if r["n_blocked"] > 0
                    ]

                    if _s24_visible:
                        _s24_html = ""

                        for _s24r in _s24_visible:
                            _s24_rsym = str(_s24r["symbol"])
                            _s24_cwr  = _s24r.get("win_rate")      # cohort win rate
                            _s24_cpnl = _s24r.get("mean_pnl_pct")  # cohort pnl%
                            _s24_nb   = _s24r["n_blocked"]
                            _s24_nh   = _s24r["n_with_trade"]

                            # Per-symbol baseline
                            _s24_bsl  = _sym_bl.get(_s24_rsym, {})
                            _s24_bwr  = _s24_bsl.get("win_rate")   # baseline
                            _s24_bpnl = _s24_bsl.get("pnl_pct")    # baseline
                            _s24_bn   = _s24_bsl.get("n", 0)

                            # COHORT WIN%
                            if _s24_cwr is not None:
                                _s24_cwr_col = (
                                    T["green"] if _s24_cwr >= 0.60 else
                                    T["red"]   if _s24_cwr <= 0.35 else
                                    T["text2"]
                                )
                                _s24_cwr_s = f"{_s24_cwr * 100:.0f}%"
                            else:
                                _s24_cwr_col = T["muted"]
                                _s24_cwr_s   = "—"

                            # BASE WIN%
                            if _s24_bwr is not None:
                                _s24_bwr_col = (
                                    T["green"] if _s24_bwr >= 0.60 else
                                    T["red"]   if _s24_bwr <= 0.35 else
                                    T["text2"]
                                )
                                _s24_bwr_s = f"{_s24_bwr * 100:.0f}%"
                            else:
                                _s24_bwr_col = T["muted"]
                                _s24_bwr_s   = "—"

                            # Δ WIN%
                            if _s24_cwr is not None and _s24_bwr is not None:
                                _s24_dwr     = _s24_cwr - _s24_bwr
                                _s24_dwr_s   = f"{_s24_dwr * 100:+.0f}pp"
                                _s24_dwr_col = (
                                    T["green"] if _s24_dwr >  0.05 else
                                    T["red"]   if _s24_dwr < -0.05 else
                                    T["text2"]
                                )
                            else:
                                _s24_dwr_s   = "—"
                                _s24_dwr_col = T["muted"]

                            # COHORT PnL%
                            if _s24_cpnl is not None:
                                _s24_cpnl_col = (
                                    T["green"] if _s24_cpnl >= 0 else T["red"]
                                )
                                _s24_cpnl_s = (
                                    f'<span style="color:{_s24_cpnl_col};'
                                    f'font-weight:700;">'
                                    f'{_s24_cpnl:+.2f}%</span>'
                                )
                            else:
                                _s24_cpnl_s = (
                                    f'<span style="color:{T["muted"]};">—</span>'
                                )

                            # BASE PnL%
                            if _s24_bpnl is not None:
                                _s24_bpnl_col = (
                                    T["green"] if _s24_bpnl >= 0 else T["red"]
                                )
                                _s24_bpnl_s = (
                                    f'<span style="color:{_s24_bpnl_col};'
                                    f'font-weight:700;">'
                                    f'{_s24_bpnl:+.2f}%</span>'
                                )
                            else:
                                _s24_bpnl_s = (
                                    f'<span style="color:{T["muted"]};">—</span>'
                                )

                            # Δ PnL%
                            if _s24_cpnl is not None and _s24_bpnl is not None:
                                _s24_dpnl     = _s24_cpnl - _s24_bpnl
                                _s24_dpnl_col = (
                                    T["green"] if _s24_dpnl >= 0 else T["red"]
                                )
                                _s24_dpnl_s = (
                                    f'<span style="color:{_s24_dpnl_col};'
                                    f'font-weight:700;">'
                                    f'{_s24_dpnl:+.2f}pp</span>'
                                )
                            else:
                                _s24_dpnl_s = (
                                    f'<span style="color:{T["muted"]};">—</span>'
                                )

                            _s24_html += (
                                f'<tr>'
                                f'<td style="color:{T["text1"]};'
                                f'font-family:{T["mono"]};font-size:0.63rem;">'
                                f'{_esc(_s24_rsym)}</td>'
                                f'<td style="color:{T["text2"]};'
                                f'font-family:{T["mono"]};font-size:0.63rem;'
                                f'font-weight:700;">{_s24_nb}</td>'
                                f'<td style="color:{T["text2"]};'
                                f'font-family:{T["mono"]};font-size:0.63rem;">'
                                f'{_s24_nh}</td>'
                                f'<td style="color:{_s24_cwr_col};'
                                f'font-family:{T["mono"]};font-size:0.63rem;'
                                f'font-weight:700;">{_esc(_s24_cwr_s)}</td>'
                                f'<td style="color:{_s24_bwr_col};'
                                f'font-family:{T["mono"]};font-size:0.63rem;">'
                                f'{_esc(_s24_bwr_s)}</td>'
                                f'<td style="color:{_s24_dwr_col};'
                                f'font-family:{T["mono"]};font-size:0.63rem;'
                                f'font-weight:700;">{_esc(_s24_dwr_s)}</td>'
                                f'<td style="font-family:{T["mono"]};'
                                f'font-size:0.63rem;">{_s24_cpnl_s}</td>'
                                f'<td style="font-family:{T["mono"]};'
                                f'font-size:0.63rem;">{_s24_bpnl_s}</td>'
                                f'<td style="font-family:{T["mono"]};'
                                f'font-size:0.63rem;">{_s24_dpnl_s}</td>'
                                f'</tr>'
                            )

                            csv_rows.append({
                                "section": "GATE_VS_SYM_BASELINE",
                                "metric":  f"{_s24_sel}|{_s24_rsym}",
                                "value": (
                                    f"cohort_wr={_s24_cwr_s}"
                                    f" base_wr={_s24_bwr_s}"
                                    f" delta_wr={_s24_dwr_s}"
                                ),
                                "sub": (
                                    f"blocks={_s24_nb}"
                                    f" trades_after={_s24_nh}"
                                    f" base_n={_s24_bn}"
                                    + (f" cohort_pnl={_s24_cpnl:+.2f}%"
                                       if _s24_cpnl is not None else "")
                                    + (f" base_pnl={_s24_bpnl:+.2f}%"
                                       if _s24_bpnl is not None else "")
                                ),
                            })

                        st.markdown(
                            f'<div class="ot-tile ot-scroll" style="padding:0;">'
                            f'<table class="ot-table"><thead><tr>'
                            f'<th>SYM</th>'
                            f'<th>BLOCKS</th><th>TRADES AFTER</th>'
                            f'<th>COHORT WIN%</th><th>BASE WIN%</th>'
                            f'<th>\u0394 WIN%</th>'
                            f'<th>COHORT PnL%</th><th>BASE PnL%</th>'
                            f'<th>\u0394 PnL%</th>'
                            f'</tr></thead><tbody>{_s24_html}</tbody>'
                            f'</table></div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f'<div style="font-family:{T["mono"]};'
                            f'font-size:0.62rem;color:{T["muted"]};">'
                            f'No symbol rows with blocks for this gate.</div>',
                            unsafe_allow_html=True,
                        )

    except Exception:
        log.error("[TRACK24-SYM1] gate_vs_sym_baseline expander failed", exc_info=True)
    # ── [/TRACK24-SYM1] ──────────────────────────────────────────────────────

    # ── [TRACK25-DIR1] Gate cohort vs. per-direction baseline comparison ──────
    try:
        _s25_xpl_rows = _load_gate_realized_pnl(days_back, sym_filter)
        _s25_drl_data = _load_gate_realized_pnl_drilldown(days_back, sym_filter)
        _s25_by_dir   = _s25_drl_data.get("by_direction", {}) if _s25_drl_data else {}
        _s25_dir_ok   = bool(_s25_drl_data.get("dir_matched", False)) if _s25_drl_data else False
        _s25_df_ok    = (
            not df.empty
            and "entry_direction" in df.columns
            and df["entry_direction"].notna().any()
        )

        if _s25_xpl_rows and _s25_by_dir and _s25_dir_ok and _s25_df_ok:

            # Pre-compute per-direction baseline from df (single pass, no DB
            # call).  Normalise to upper-case once so dict keys align with the
            # .upper() values already stored in by_direction rows.
            _dir_bl: "dict[str, dict]" = {}
            _s25_has_usd = (
                "realized_usd" in df.columns
                and df["realized_usd"].notna().any()
            )
            _s25_has_pnl = (
                "pnl_pct" in df.columns
                and df["pnl_pct"].notna().any()
            )
            _s25_dir_upper = df["entry_direction"].astype(str).str.upper()
            for _s25_d in _s25_dir_upper.dropna().unique():
                _s25_dfd = df[_s25_dir_upper == _s25_d]
                _s25_dn  = len(_s25_dfd)
                _s25_dwr: "float | None" = None
                _s25_dpl: "float | None" = None
                if _s25_dn > 0:
                    if _s25_has_usd and _s25_dfd["realized_usd"].notna().any():
                        _s25_dwins = int((_s25_dfd["realized_usd"] > 0).sum())
                        _s25_dwr   = _s25_dwins / _s25_dn
                    if _s25_has_pnl and _s25_dfd["pnl_pct"].notna().any():
                        _s25_dpl = float(_s25_dfd["pnl_pct"].dropna().mean())
                _dir_bl[str(_s25_d)] = {
                    "n":        _s25_dn,
                    "win_rate": _s25_dwr,
                    "pnl_pct":  _s25_dpl,
                }

            with st.expander(
                "🧭 Gate cohort vs. per-direction baseline",
                expanded=False,
            ):
                _s25_note = (
                    f"Baseline per row = all closed trades for that direction "
                    f"in the last {days_label} window. "
                    f"Cohort = trades that opened within "
                    f"{_CORR21_TRADE_WINDOW_SECS}s after a block on the same "
                    f"symbol + direction. "
                    "Δ WIN% > +5pp (green) \u2192 gate mainly caught losers "
                    "for that direction. "
                    "Δ WIN% < \u22125pp (red) \u2192 gate may have blocked "
                    "winners. Read-only."
                )
                st.markdown(
                    f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                    f'color:{T["muted"]};margin-bottom:6px;">'
                    f'{_esc(_s25_note)}</div>',
                    unsafe_allow_html=True,
                )

                # Gate selector
                _s25_gate_opts   = [r["gate_key"] for r in _s25_xpl_rows]
                _s25_gate_labels = {r["gate_key"]: r["label"] for r in _s25_xpl_rows}
                _s25_options     = ["— select gate —"] + _s25_gate_opts
                _s25_sel = st.selectbox(
                    "Gate",
                    options=_s25_options,
                    format_func=lambda k: (
                        k if k == "— select gate —"
                        else _s25_gate_labels.get(k, k)
                    ),
                    key="rpt_dir_cmp_gate",
                    label_visibility="collapsed",
                )

                if _s25_sel and _s25_sel != "— select gate —":
                    _s25_dir_rows = _s25_by_dir.get(_s25_sel, [])
                    _s25_visible  = [
                        r for r in _s25_dir_rows if r["n_blocked"] > 0
                    ]

                    if _s25_visible:
                        _s25_html = ""

                        for _s25r in _s25_visible:
                            _s25_rdir = str(_s25r.get("direction") or "")
                            _s25_cwr  = _s25r.get("win_rate")      # cohort
                            _s25_cpnl = _s25r.get("mean_pnl_pct")  # cohort
                            _s25_nb   = _s25r["n_blocked"]
                            _s25_nh   = _s25r["n_with_trade"]

                            # Per-direction baseline
                            _s25_bsl  = _dir_bl.get(_s25_rdir, {})
                            _s25_bwr  = _s25_bsl.get("win_rate")
                            _s25_bpnl = _s25_bsl.get("pnl_pct")
                            _s25_bn   = _s25_bsl.get("n", 0)

                            # DIR label colour
                            _s25_dir_col = (
                                T["green"] if _s25_rdir == "LONG"  else
                                T["red"]   if _s25_rdir == "SHORT" else
                                T["text2"]
                            )

                            # COHORT WIN%
                            if _s25_cwr is not None:
                                _s25_cwr_col = (
                                    T["green"] if _s25_cwr >= 0.60 else
                                    T["red"]   if _s25_cwr <= 0.35 else
                                    T["text2"]
                                )
                                _s25_cwr_s = f"{_s25_cwr * 100:.0f}%"
                            else:
                                _s25_cwr_col = T["muted"]
                                _s25_cwr_s   = "—"

                            # BASE WIN%
                            if _s25_bwr is not None:
                                _s25_bwr_col = (
                                    T["green"] if _s25_bwr >= 0.60 else
                                    T["red"]   if _s25_bwr <= 0.35 else
                                    T["text2"]
                                )
                                _s25_bwr_s = f"{_s25_bwr * 100:.0f}%"
                            else:
                                _s25_bwr_col = T["muted"]
                                _s25_bwr_s   = "—"

                            # Δ WIN%
                            if _s25_cwr is not None and _s25_bwr is not None:
                                _s25_dwr     = _s25_cwr - _s25_bwr
                                _s25_dwr_s   = f"{_s25_dwr * 100:+.0f}pp"
                                _s25_dwr_col = (
                                    T["green"] if _s25_dwr >  0.05 else
                                    T["red"]   if _s25_dwr < -0.05 else
                                    T["text2"]
                                )
                            else:
                                _s25_dwr_s   = "—"
                                _s25_dwr_col = T["muted"]

                            # COHORT PnL%
                            if _s25_cpnl is not None:
                                _s25_cpnl_col = (
                                    T["green"] if _s25_cpnl >= 0 else T["red"]
                                )
                                _s25_cpnl_s = (
                                    f'<span style="color:{_s25_cpnl_col};'
                                    f'font-weight:700;">'
                                    f'{_s25_cpnl:+.2f}%</span>'
                                )
                            else:
                                _s25_cpnl_s = (
                                    f'<span style="color:{T["muted"]};">—</span>'
                                )

                            # BASE PnL%
                            if _s25_bpnl is not None:
                                _s25_bpnl_col = (
                                    T["green"] if _s25_bpnl >= 0 else T["red"]
                                )
                                _s25_bpnl_s = (
                                    f'<span style="color:{_s25_bpnl_col};'
                                    f'font-weight:700;">'
                                    f'{_s25_bpnl:+.2f}%</span>'
                                )
                            else:
                                _s25_bpnl_s = (
                                    f'<span style="color:{T["muted"]};">—</span>'
                                )

                            # Δ PnL%
                            if _s25_cpnl is not None and _s25_bpnl is not None:
                                _s25_dpnl     = _s25_cpnl - _s25_bpnl
                                _s25_dpnl_col = (
                                    T["green"] if _s25_dpnl >= 0 else T["red"]
                                )
                                _s25_dpnl_s = (
                                    f'<span style="color:{_s25_dpnl_col};'
                                    f'font-weight:700;">'
                                    f'{_s25_dpnl:+.2f}pp</span>'
                                )
                            else:
                                _s25_dpnl_s = (
                                    f'<span style="color:{T["muted"]};">—</span>'
                                )

                            _s25_html += (
                                f'<tr>'
                                f'<td style="color:{_s25_dir_col};'
                                f'font-family:{T["mono"]};font-size:0.63rem;'
                                f'font-weight:700;">'
                                f'{_esc(_s25_rdir or "—")}</td>'
                                f'<td style="color:{T["text2"]};'
                                f'font-family:{T["mono"]};font-size:0.63rem;'
                                f'font-weight:700;">{_s25_nb}</td>'
                                f'<td style="color:{T["text2"]};'
                                f'font-family:{T["mono"]};font-size:0.63rem;">'
                                f'{_s25_nh}</td>'
                                f'<td style="color:{_s25_cwr_col};'
                                f'font-family:{T["mono"]};font-size:0.63rem;'
                                f'font-weight:700;">{_esc(_s25_cwr_s)}</td>'
                                f'<td style="color:{_s25_bwr_col};'
                                f'font-family:{T["mono"]};font-size:0.63rem;">'
                                f'{_esc(_s25_bwr_s)}</td>'
                                f'<td style="color:{_s25_dwr_col};'
                                f'font-family:{T["mono"]};font-size:0.63rem;'
                                f'font-weight:700;">{_esc(_s25_dwr_s)}</td>'
                                f'<td style="font-family:{T["mono"]};'
                                f'font-size:0.63rem;">{_s25_cpnl_s}</td>'
                                f'<td style="font-family:{T["mono"]};'
                                f'font-size:0.63rem;">{_s25_bpnl_s}</td>'
                                f'<td style="font-family:{T["mono"]};'
                                f'font-size:0.63rem;">{_s25_dpnl_s}</td>'
                                f'</tr>'
                            )

                            csv_rows.append({
                                "section": "GATE_VS_DIR_BASELINE",
                                "metric":  f"{_s25_sel}|{_s25_rdir}",
                                "value": (
                                    f"cohort_wr={_s25_cwr_s}"
                                    f" base_wr={_s25_bwr_s}"
                                    f" delta_wr={_s25_dwr_s}"
                                ),
                                "sub": (
                                    f"blocks={_s25_nb}"
                                    f" trades_after={_s25_nh}"
                                    f" base_n={_s25_bn}"
                                    + (f" cohort_pnl={_s25_cpnl:+.2f}%"
                                       if _s25_cpnl is not None else "")
                                    + (f" base_pnl={_s25_bpnl:+.2f}%"
                                       if _s25_bpnl is not None else "")
                                ),
                            })

                        st.markdown(
                            f'<div class="ot-tile ot-scroll" style="padding:0;">'
                            f'<table class="ot-table"><thead><tr>'
                            f'<th>DIR</th>'
                            f'<th>BLOCKS</th><th>TRADES AFTER</th>'
                            f'<th>COHORT WIN%</th><th>BASE WIN%</th>'
                            f'<th>\u0394 WIN%</th>'
                            f'<th>COHORT PnL%</th><th>BASE PnL%</th>'
                            f'<th>\u0394 PnL%</th>'
                            f'</tr></thead><tbody>{_s25_html}</tbody>'
                            f'</table></div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            f'<div style="font-family:{T["mono"]};'
                            f'font-size:0.62rem;color:{T["muted"]};">'
                            f'No direction rows with blocks for this gate.</div>',
                            unsafe_allow_html=True,
                        )

    except Exception:
        log.error("[TRACK25-DIR1] gate_vs_dir_baseline expander failed", exc_info=True)
    # ── [/TRACK25-DIR1] ──────────────────────────────────────────────────────

    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    # ── Section 4 — Signal Quality ────────────────────────────────────────────
    st.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{T["text2"]};margin:8px 0 4px 0;">'
        f'SIGNAL QUALITY</div>',
        unsafe_allow_html=True,
    )
    has_conf = (
        not df.empty
        and "entry_confidence" in df.columns
        and df["entry_confidence"].notna().any()
    )
    has_z = (
        not df.empty
        and "entry_z_score" in df.columns
        and df["entry_z_score"].notna().any()
    )
    if not has_conf and not has_z:
        _slot_placeholder(
            "SIGNAL QUALITY COLUMNS ABSENT (pre-Track-01 schema or empty window)", 50
        )
        csv_rows.append({
            "section": "SIGNAL_QUALITY", "metric": "unavailable",
            "value": "—", "sub": "",
        })
    else:
        sq_cols = st.columns(4)
        _sq_idx = 0

        if has_conf and "realized_usd" in df.columns:
            avg_conf    = float(df["entry_confidence"].dropna().mean())
            winners     = df[df["realized_usd"] > 0]
            losers      = df[df["realized_usd"] <= 0]
            avg_conf_w  = (
                float(winners["entry_confidence"].dropna().mean())
                if not winners.empty and winners["entry_confidence"].notna().any()
                else None
            )
            avg_conf_l  = (
                float(losers["entry_confidence"].dropna().mean())
                if not losers.empty and losers["entry_confidence"].notna().any()
                else None
            )
            win_conf_str = f"{avg_conf_w:.3f}" if avg_conf_w is not None else "—"
            los_conf_str = f"{avg_conf_l:.3f}" if avg_conf_l is not None else "—"

            with sq_cols[0]:
                _kpi("AVG CONF", f"{avg_conf:.3f}", "all trades", T["text2"])
            with sq_cols[1]:
                _kpi("WINNER CONF", win_conf_str, "avg confidence", T["green"])
            with sq_cols[2]:
                _kpi("LOSER CONF",  los_conf_str, "avg confidence", T["red"])
            _sq_idx = 3

            csv_rows += [
                {"section": "SIGNAL_QUALITY", "metric": "avg_confidence_all",
                 "value": f"{avg_conf:.4f}", "sub": ""},
                {"section": "SIGNAL_QUALITY", "metric": "avg_confidence_winners",
                 "value": win_conf_str, "sub": ""},
                {"section": "SIGNAL_QUALITY", "metric": "avg_confidence_losers",
                 "value": los_conf_str, "sub": ""},
            ]

        if has_z:
            avg_z = float(df["entry_z_score"].dropna().mean())
            z_col = T["cyan"] if avg_z >= 1.5 else T["blue"] if avg_z >= 0.8 else T["muted"]
            with sq_cols[_sq_idx]:
                _kpi("AVG Z-SCORE", f"{avg_z:.2f}", "entry signal", z_col)
            csv_rows.append({
                "section": "SIGNAL_QUALITY", "metric": "avg_z_score",
                "value": f"{avg_z:.4f}", "sub": "",
            })

    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)

    # ── Section 5 — Top / Bottom Setups ───────────────────────────────────────
    st.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;letter-spacing:0.12em;'
        f'text-transform:uppercase;color:{T["text2"]};margin:8px 0 4px 0;">'
        f'TOP / BOTTOM SETUPS (sym × regime × direction · min 3 trades)</div>',
        unsafe_allow_html=True,
    )
    has_setup = (
        not df.empty
        and "entry_regime" in df.columns
        and df["entry_regime"].notna().any()
        and "entry_direction" in df.columns
        and df["entry_direction"].notna().any()
        and "symbol" in df.columns
        and "realized_usd" in df.columns
    )
    if not has_setup:
        _slot_placeholder(
            "SETUP BREAKDOWN UNAVAILABLE (pre-Track-01 schema or empty window)", 50
        )
        csv_rows.append({
            "section": "SETUPS", "metric": "unavailable",
            "value": "—", "sub": "",
        })
    else:
        setup_grp = (
            df.dropna(subset=["entry_regime", "entry_direction"])
              .groupby(["symbol", "entry_regime", "entry_direction"])
              .agg(
                  n=("realized_usd", "count"),
                  avg_pnl=("realized_usd", "mean"),
                  total_pnl=("realized_usd", "sum"),
                  wins=("realized_usd", lambda s: (s > 0).sum()),
              )
              .reset_index()
        )
        setup_grp = setup_grp[setup_grp["n"] >= 3].sort_values(
            "avg_pnl", ascending=False
        ).reset_index(drop=True)

        if setup_grp.empty:
            _slot_placeholder(
                "INSUFFICIENT DATA — need ≥3 trades per (sym × regime × dir) cell", 50
            )
            csv_rows.append({
                "section": "SETUPS", "metric": "insufficient_data",
                "value": "—", "sub": "need ≥3 per cell",
            })
        else:
            top3    = setup_grp.head(3)
            bottom3 = setup_grp.tail(3).iloc[::-1].reset_index(drop=True)

            def _setup_rows_html(grp_df: "pd.DataFrame", tag: str) -> str:
                rows = ""
                for _i, (_, _sr) in enumerate(grp_df.iterrows(), 1):
                    _sym  = _esc(str(_sr["symbol"]))
                    _reg  = _esc(str(_sr["entry_regime"]).lower()[:8])
                    _dir  = str(_sr["entry_direction"]).upper()
                    _dc   = T["green"] if _dir == "LONG" else T["red"] if _dir == "SHORT" else T["text2"]
                    _ap   = float(_sr["avg_pnl"])
                    _n2   = int(_sr["n"])
                    _w    = int(_sr["wins"])
                    _wr   = _w / _n2 if _n2 > 0 else 0.0
                    rows += (
                        f'<tr>'
                        f'<td style="color:{T["muted"]};font-family:{T["mono"]};'
                        f'font-size:0.60rem;">{_i}</td>'
                        f'<td style="color:{T["text"]};font-weight:700;'
                        f'font-size:0.65rem;">{_sym}</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.62rem;">{_reg}</td>'
                        f'<td style="color:{_dc};font-weight:700;font-family:{T["mono"]};'
                        f'font-size:0.62rem;">{_esc(_dir[:5])}</td>'
                        f'<td style="color:{_pnl_col(_ap)};font-weight:700;'
                        f'font-family:{T["mono"]};font-size:0.65rem;">${_ap:+.2f}</td>'
                        f'<td style="color:{_wr_col(_w, _n2)};font-family:{T["mono"]};'
                        f'font-size:0.62rem;">{_wr * 100:.0f}%</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.62rem;">{_n2}×</td>'
                        f'</tr>'
                    )
                    csv_rows.append({
                        "section": f"SETUPS_{tag}",
                        "metric":
                            f"{_sr['symbol']}×{_sr['entry_regime']}×{_dir}",
                        "value": f"{_ap:+.2f}",
                        "sub": f"WR={_wr * 100:.0f}% n={_n2}",
                    })
                return rows

            _tbl_hdr = (
                f'<thead><tr>'
                f'<th>#</th><th>SYM</th><th>REGIME</th><th>DIR</th>'
                f'<th>AVG PnL</th><th>WR</th><th>N</th>'
                f'</tr></thead>'
            )
            sb1, sb2 = st.columns(2)
            with sb1:
                st.markdown(
                    f'<div style="color:{T["green"]};font-family:{T["mono"]};'
                    f'font-size:0.58rem;letter-spacing:0.10em;margin-bottom:4px;">'
                    f'▲ TOP 3</div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                    f'<table class="ot-table">{_tbl_hdr}'
                    f'<tbody>{_setup_rows_html(top3, "TOP")}</tbody></table></div>',
                    unsafe_allow_html=True,
                )
            with sb2:
                st.markdown(
                    f'<div style="color:{T["red"]};font-family:{T["mono"]};'
                    f'font-size:0.58rem;letter-spacing:0.10em;margin-bottom:4px;">'
                    f'▼ BOTTOM 3</div>',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                    f'<table class="ot-table">{_tbl_hdr}'
                    f'<tbody>{_setup_rows_html(bottom3, "BOTTOM")}</tbody></table></div>',
                    unsafe_allow_html=True,
                )

    st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

    # ── CSV Download ──────────────────────────────────────────────────────────
    if csv_rows:
        try:
            buf = _io.StringIO()
            pd.DataFrame(csv_rows).to_csv(buf, index=False)
            csv_bytes = buf.getvalue().encode("utf-8")
            report_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename  = f"powertrader_report_{days_label}_{report_ts}.csv"
            with _hc3:
                st.download_button(
                    label=f"⬇ Export report as CSV ({days_label})",
                    data=csv_bytes,
                    file_name=filename,
                    mime="text/csv",
                    key="rpt_csv_dl",
                )
        except Exception:
            log.debug(
                "[TRACK13] render_operator_report: CSV export failed", exc_info=True
            )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: POLICY STATE  [PHASE11]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_policy_state() -> None:
    """[PHASE11] Admission policy state panel.

    Reads admission_policy from the status dict published by executor.py.
    Shows:
      1. Gate enable/disable summary (which gates are active vs disabled)
      2. Per-gate rejection counts since last restart
      3. Flush-tracker snapshot per symbol
      4. P32 LIMIT_ONLY symbols currently enforced
      5. VPIN hardening thresholds
      [TRACK04]
      6. Per-symbol autotune state (CoinPerformanceTracker live overrides)
      7. RL regime weight table (RegimeWeightTable live multipliers)
      [TRACK07]
      8. Score Feedback Controls — operator-explicit per-symbol autotune override
    All guidance is strictly advisory — no automatic threshold-setting.
    No new DB queries; no new CSS classes.
    """
    _section("POLICY STATE — ADMISSION GATE VISIBILITY", "🔏")
    status = _get_status()
    ap = status.get("admission_policy", {})
    if not isinstance(ap, dict):
        ap = {}

    # ── 1. Gate enable/disable summary ──────────────────────────────────────
    # A gate is ACTIVE when its controlling threshold is non-zero / non-empty.
    # [TRACK15] CONCURRENT_CAP and DEPLOYED_CAP (Track 14) added to _GATE_DEFS so
    # they appear in the same table as all other operator-configurable gates.
    _GATE_DEFS = [
        # (display_name, status_key, is_active_fn, threshold_fmt_fn)
        ("CONF",           "min_confidence",      lambda v: v > 0.0,        lambda v: f"{v:.4f}"),
        ("EDGE",           "min_edge_pct",        lambda v: v > 0.0,        lambda v: f"{v:.4f}"),
        ("EARLY_FLUSH",    "flush_cooldown_secs", lambda v: v > 0.0,        lambda v: f"{v:.0f}s"),
        ("SYMBOL_BLOCKED", "suppress_symbols",    lambda v: len(v) > 0,     lambda v: ", ".join(v) if v else "—"),
        ("REGIME_BLOCKED", "suppress_regimes",    lambda v: len(v) > 0,     lambda v: ", ".join(v) if v else "—"),
        ("BRIDGE_STALE",   "bridge_stale_secs",   lambda v: v > 0.0,        lambda v: f"{v:.0f}s"),
        ("VPIN_CONSEC",    "vpin_emerg_consecutive", lambda v: v > 1,       lambda v: f"≥{v} ticks"),
        ("VPIN_HOLD",      "vpin_emerg_min_hold", lambda v: v > 0.0,        lambda v: f"{v:.0f}s"),
        # [TRACK15] Track 14 exposure gates — env-var configured, disabled by default.
        ("CONCURRENT_CAP", "max_open_positions",  lambda v: int(v) > 0,     lambda v: f"≤{int(v)} positions"),
        ("DEPLOYED_CAP",   "max_deployed_pct",    lambda v: v > 0.0,        lambda v: f"≤{v * 100:.0f}% equity"),
        # [TRACK27] Per-symbol cost-basis cap — env-var configured, disabled by default.
        ("PER_SYMBOL_CAP", "max_per_symbol_usd",  lambda v: v > 0.0,        lambda v: f"≤${v:,.0f}/symbol"),
    ]
    # Gates always-on (not operator-controlled):
    _ALWAYS_ON = ["METADATA", "SPREAD", "BRIDGE_DISCONNECTED", "BRIDGE_GHOST_EQUITY",
                  "IN_FLIGHT"]

    gate_rows_html = ""
    for gate_name, key, is_active_fn, fmt_fn in _GATE_DEFS:
        val = ap.get(key)
        try:
            active = is_active_fn(val) if val is not None else False
            val_str = fmt_fn(val) if val is not None else "—"
        except Exception:
            active = False
            val_str = "—"
        state_col   = T["green"]  if active else T["muted"]
        state_label = "ACTIVE"    if active else "disabled"
        gate_rows_html += (
            f'<tr>'
            f'<td style="color:{T["text"]};font-weight:700;font-family:{T["mono"]};'
            f'font-size:0.65rem;">{_esc(gate_name)}</td>'
            f'<td style="color:{state_col};font-weight:700;font-family:{T["mono"]};'
            f'font-size:0.65rem;">{state_label}</td>'
            f'<td style="color:{T["text2"]};font-family:{T["mono"]};font-size:0.65rem;">'
            f'{_esc(val_str)}</td>'
            f'</tr>'
        )
    always_on_str = " · ".join(_ALWAYS_ON)
    gate_table_html = (
        f'<div class="ot-tile" style="padding:10px;">'
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
        f'letter-spacing:0.12em;text-transform:uppercase;'
        f'color:{T["text2"]};margin-bottom:6px;">GATE ENABLE STATUS</div>'
        f'<table class="ot-table"><thead><tr>'
        f'<th>GATE</th><th>STATE</th><th>THRESHOLD</th>'
        f'</tr></thead><tbody>{gate_rows_html}</tbody></table>'
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
        f'color:{T["muted"]};margin-top:4px;">'
        f'Always-on: {_esc(always_on_str)}</div>'
        f'</div>'
    )
    st.markdown(gate_table_html, unsafe_allow_html=True)

    # ── 2. Per-gate rejection counts ────────────────────────────────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
    gate_rejections = ap.get("gate_rejections", {})
    if isinstance(gate_rejections, dict) and gate_rejections:
        try:
            sorted_rej = sorted(gate_rejections.items(), key=lambda x: x[1], reverse=True)
            rej_rows_html = ""
            for gname, cnt in sorted_rej:
                if cnt == 0:
                    continue
                # Advisory: highlight gates with unexpectedly high counts
                # SPREAD / METADATA high = market quality / connectivity issue
                # CONF / EDGE high = signal quality issue
                # EARLY_FLUSH high = repeated flush pattern
                cnt_col = (
                    T["red"]    if cnt >= 50 else
                    T["yellow"] if cnt >= 10 else
                    T["text"]
                )
                rej_rows_html += (
                    f'<tr>'
                    f'<td style="color:{T["text"]};font-weight:700;font-family:{T["mono"]};'
                    f'font-size:0.65rem;">{_esc(str(gname))}</td>'
                    f'<td style="color:{cnt_col};font-weight:700;font-family:{T["mono"]};'
                    f'font-size:0.65rem;">{cnt:,}</td>'
                    f'</tr>'
                )
            if rej_rows_html:
                rej_table_html = (
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'GATE REJECTIONS — SINCE LAST RESTART</div>'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>GATE</th><th>REJECTIONS</th>'
                    f'</tr></thead><tbody>{rej_rows_html}</tbody></table>'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'color:{T["muted"]};margin-top:4px;">'
                    f'Counts reset on restart. High SPREAD/METADATA = connectivity. '
                    f'High CONF/EDGE = signal selectivity. High EARLY_FLUSH = repeated flush pattern.'
                    f'</div></div>'
                )
                st.markdown(rej_table_html, unsafe_allow_html=True)
            else:
                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">GATE REJECTIONS</div>'
                    f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
                    f'No rejections recorded since last restart.</div></div>',
                    unsafe_allow_html=True,
                )
        except Exception as _rej_exc:
            log.debug("[PHASE11] gate rejection panel error: %s", _rej_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">GATE REJECTIONS</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'No rejections recorded yet — or admission_policy not yet in status.</div></div>',
            unsafe_allow_html=True,
        )

    # ── 3. Flush tracker snapshot ────────────────────────────────────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
    flush_tracker = ap.get("flush_tracker", {})
    flush_lookback = _safe_int(ap.get("flush_lookback", 3))
    flush_cooldown = _safe_float(ap.get("flush_cooldown_secs", 0.0))
    if isinstance(flush_tracker, dict) and flush_tracker:
        try:
            flush_rows_html = ""
            for sym, snap in flush_tracker.items():
                entries           = _safe_int(snap.get("entries", 0))
                last_ago          = snap.get("last_flush_ago_secs")
                last_ago_str      = f"{last_ago:.0f}s ago" if last_ago is not None else "—"
                at_threshold      = entries >= flush_lookback
                entries_col       = T["red"] if at_threshold else T["yellow"]
                flush_rows_html += (
                    f'<tr>'
                    f'<td style="color:{T["text"]};font-weight:700;font-family:{T["mono"]};'
                    f'font-size:0.65rem;">{_esc(str(sym))}</td>'
                    f'<td style="color:{entries_col};font-weight:700;font-family:{T["mono"]};'
                    f'font-size:0.65rem;">{entries}/{flush_lookback}</td>'
                    f'<td style="color:{T["text2"]};font-family:{T["mono"]};font-size:0.65rem;">'
                    f'{_esc(last_ago_str)}</td>'
                    f'</tr>'
                )
            gate_state_str = "ACTIVE" if flush_cooldown > 0.0 else f"disabled (ADMISSION_FLUSH_COOLDOWN_SECS=0)"
            flush_table_html = (
                f'<div class="ot-tile" style="padding:10px;">'
                f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                f'letter-spacing:0.12em;text-transform:uppercase;'
                f'color:{T["text2"]};margin-bottom:6px;">'
                f'EARLY-FLUSH TRACKER'
                f'<span style="color:{T["muted"]};font-weight:400;"> '
                f'(EARLY_FLUSH gate: {_esc(gate_state_str)})</span></div>'
                f'<table class="ot-table"><thead><tr>'
                f'<th>SYMBOL</th><th>FLUSHES / LOOKBACK</th><th>LAST FLUSH</th>'
                f'</tr></thead><tbody>{flush_rows_html}</tbody></table>'
                f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                f'color:{T["muted"]};margin-top:4px;">'
                f'Resets on restart. Red = at or above lookback threshold. '
                f'Suggestion: if a symbol reaches threshold repeatedly, consider enabling '
                f'ADMISSION_FLUSH_COOLDOWN_SECS or reviewing its exit quality.'
                f'</div></div>'
            )
            st.markdown(flush_table_html, unsafe_allow_html=True)
        except Exception as _ft_exc:
            log.debug("[PHASE11] flush tracker panel error: %s", _ft_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">EARLY-FLUSH TRACKER</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'No early-flush events recorded since last restart.</div></div>',
            unsafe_allow_html=True,
        )

    # ── 4. P32 LIMIT_ONLY symbols ────────────────────────────────────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
    p32_limit_only = status.get("p32_limit_only_symbols", [])
    if isinstance(p32_limit_only, list) and p32_limit_only:
        p32_syms_str = "  ·  ".join(_esc(str(s)) for s in p32_limit_only)
        p32_html = (
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">P32 LIMIT-ONLY SYMBOLS</div>'
            f'<div style="color:{T["yellow"]};font-weight:700;font-family:{T["mono"]};'
            f'font-size:0.72rem;">{p32_syms_str}</div>'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'color:{T["muted"]};margin-top:4px;">'
            f'These symbols are in LIMIT_ONLY mode due to excessive close-side slippage '
            f'(P32 adaptive slippage). Market-order fallbacks are suppressed. '
            f'Clears automatically on expiry or restart.'
            f'</div></div>'
        )
    else:
        p32_html = (
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">P32 LIMIT-ONLY SYMBOLS</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'None — no symbols currently in LIMIT_ONLY mode.</div></div>'
        )
    st.markdown(p32_html, unsafe_allow_html=True)

    # ── 5. VPIN hardening thresholds ─────────────────────────────────────────
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
    vpin_tox    = _safe_float(ap.get("vpin_emerg_toxicity",    0.95))
    vpin_consec = _safe_int(ap.get("vpin_emerg_consecutive",  1))
    vpin_hold   = _safe_float(ap.get("vpin_emerg_min_hold",    0.0))
    consec_col  = T["green"] if vpin_consec > 1 else T["muted"]
    hold_col    = T["green"] if vpin_hold > 0.0  else T["muted"]
    vpin_html = (
        f'<div class="ot-tile" style="padding:10px;">'
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
        f'letter-spacing:0.12em;text-transform:uppercase;'
        f'color:{T["text2"]};margin-bottom:6px;">VPIN EMERGENCY-EXIT HARDENING</div>'
        f'<table class="ot-table"><thead><tr>'
        f'<th>PARAMETER</th><th>VALUE</th><th>NOTE</th>'
        f'</tr></thead><tbody>'
        f'<tr>'
        f'<td style="color:{T["text"]};font-family:{T["mono"]};font-size:0.65rem;">'
        f'Toxicity threshold</td>'
        f'<td style="color:{T["text"]};font-weight:700;font-family:{T["mono"]};'
        f'font-size:0.65rem;">{vpin_tox:.2f}</td>'
        f'<td style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.65rem;">'
        f'P37_EMERGENCY_EXIT_TOXICITY</td>'
        f'</tr>'
        f'<tr>'
        f'<td style="color:{T["text"]};font-family:{T["mono"]};font-size:0.65rem;">'
        f'Consecutive required</td>'
        f'<td style="color:{consec_col};font-weight:700;font-family:{T["mono"]};'
        f'font-size:0.65rem;">{vpin_consec}</td>'
        f'<td style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.65rem;">'
        f'{"HARDENED" if vpin_consec > 1 else "default (1 tick)"}</td>'
        f'</tr>'
        f'<tr>'
        f'<td style="color:{T["text"]};font-family:{T["mono"]};font-size:0.65rem;">'
        f'Min hold before exit</td>'
        f'<td style="color:{hold_col};font-weight:700;font-family:{T["mono"]};'
        f'font-size:0.65rem;">{vpin_hold:.0f}s</td>'
        f'<td style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.65rem;">'
        f'{"HARDENED" if vpin_hold > 0.0 else "default (no hold)"}</td>'
        f'</tr>'
        f'</tbody></table>'
        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
        f'color:{T["muted"]};margin-top:4px;">'
        f'Suggestion: if emergency exits are frequent with short hold times, '
        f'consider raising P37_EMERGENCY_EXIT_CONSECUTIVE_REQUIRED (≥2) '
        f'or P37_EMERGENCY_EXIT_MIN_HOLD_SECS to reduce single-tick panic exits.'
        f'</div></div>'
    )
    st.markdown(vpin_html, unsafe_allow_html=True)

    # ── 6. [TRACK04-SF4] Per-symbol autotune state ───────────────────────────
    # Reads status["autotune_state"] published by executor._cycle() each trade
    # cycle.  Each entry reflects the live CoinPerformanceTracker state for one
    # tracked symbol: whether a conf/alloc override is currently active, the
    # effective floor/cap values, and rolling win-rate + trade count for the
    # current autotune window.  All reads are sync and at most one cycle stale.
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
    _at_state = status.get("autotune_state", {})
    if isinstance(_at_state, dict) and _at_state:
        try:
            _active_syms = {
                sym: snap for sym, snap in _at_state.items()
                if isinstance(snap, dict) and snap.get("active")
            }
            if _active_syms:
                at_rows_html = ""
                for sym, snap in sorted(_active_syms.items()):
                    cf     = _safe_float(snap.get("conf_floor", 0.0))
                    ac     = _safe_float(snap.get("alloc_cap",  0.0))
                    wr     = snap.get("win_rate")
                    tc     = _safe_int(snap.get("trade_count", 0))
                    wr_str = f"{wr * 100:.0f}%" if wr is not None else "—"
                    wr_col = (
                        T["green"]  if wr is not None and wr >= 0.50 else
                        T["yellow"] if wr is not None and wr >= 0.35 else
                        T["red"]    if wr is not None else
                        T["muted"]
                    )
                    # [TRACK15] SOURCE column: "manual" = operator Score Feedback Controls,
                    # "auto" = autotune engine, None = unknown (pre-Track15 executor).
                    _src     = snap.get("source")
                    _src_lbl = "MANUAL" if _src == "manual" else "AUTO" if _src == "auto" else "—"
                    _src_col = T["yellow"] if _src == "manual" else T["green"] if _src == "auto" else T["muted"]
                    at_rows_html += (
                        f'<tr>'
                        f'<td style="color:{T["text"]};font-weight:700;'
                        f'font-family:{T["mono"]};font-size:0.65rem;">'
                        f'{_esc(str(sym))}</td>'
                        f'<td style="color:{T["yellow"]};font-weight:700;'
                        f'font-family:{T["mono"]};font-size:0.65rem;">ACTIVE</td>'
                        f'<td style="color:{_src_col};font-weight:700;'
                        f'font-family:{T["mono"]};font-size:0.65rem;">{_src_lbl}</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{cf:.4f}</td>'
                        f'<td style="color:{T["text2"]};font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{ac:.4f}</td>'
                        f'<td style="color:{wr_col};font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{wr_str}</td>'
                        f'<td style="color:{T["muted"]};font-family:{T["mono"]};'
                        f'font-size:0.65rem;">{tc}</td>'
                        f'</tr>'
                    )
                at_html = (
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'AUTOTUNE OVERRIDES — ACTIVE SYMBOLS'
                    f'<span style="color:{T["muted"]};font-weight:400;">'
                    f' ({len(_active_syms)} symbol{"s" if len(_active_syms) != 1 else ""})'
                    f'</span></div>'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>SYMBOL</th><th>STATUS</th>'
                    f'<th>SOURCE</th>'
                    f'<th>CONF FLOOR</th><th>ALLOC CAP</th>'
                    f'<th>WIN RATE</th><th>TRADES</th>'
                    f'</tr></thead><tbody>{at_rows_html}</tbody></table>'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'color:{T["muted"]};margin-top:4px;">'
                    f'CoinPerformanceTracker live state. SOURCE: MANUAL = operator Score Feedback, '
                    f'AUTO = autotune engine. '
                    f'Win rate and trade count reflect the rolling autotune window only.'
                    f'</div></div>'
                )
                st.markdown(at_html, unsafe_allow_html=True)
            else:
                _all_syms  = list(_at_state.keys())
                _sym_list  = ", ".join(_esc(str(s)) for s in sorted(_all_syms)[:8])
                _more      = f" +{len(_all_syms) - 8} more" if len(_all_syms) > 8 else ""
                at_html = (
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">AUTOTUNE OVERRIDES</div>'
                    f'<div style="color:{T["green"]};font-weight:700;'
                    f'font-family:{T["mono"]};font-size:0.70rem;">'
                    f'NO ACTIVE AUTOTUNE OVERRIDES</div>'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'color:{T["muted"]};margin-top:4px;">'
                    f'All {len(_all_syms)} tracked symbol{"s" if len(_all_syms) != 1 else ""} '
                    f'({_sym_list}{_esc(_more)}) running at default conf/alloc thresholds. '
                    f'Overrides activate when a symbol\'s recent win rate falls below '
                    f'AUTO_TUNE_WIN_RATE_FLOOR.'
                    f'</div></div>'
                )
                st.markdown(at_html, unsafe_allow_html=True)
        except Exception as _at_exc:
            log.debug("[TRACK04-SF4] autotune state panel failed: %s", _at_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">AUTOTUNE OVERRIDES</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'Autotune state not yet in status — requires Track 04 executor or later.'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── 7. [TRACK04-SF5] RL regime weight table ──────────────────────────────
    # Reads status["rl_regime_weights"] (RegimeWeightTable.get_state() output) and
    # status["rl_config"] (RL constant dict) published by executor._cycle().
    # PnL colour is determined by a locally-defined lambda — this function does
    # NOT reference _pnl_col from render_profitability_diagnostics() which is
    # out of scope here.
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
    _rl_weights = status.get("rl_regime_weights", {})
    _rl_stats   = _rl_weights.get("stats", {}) if isinstance(_rl_weights, dict) else {}
    _rl_cfg     = status.get("rl_config", {})
    if isinstance(_rl_stats, dict) and _rl_stats:
        try:
            # Local PnL colour helper — defined here to avoid any scope dependency
            # on _pnl_col from render_profitability_diagnostics().
            def _rl_pnl_col(v: float) -> str:
                return T["green"] if v >= 0 else T["red"]

            _min_trades = int(_rl_cfg.get("min_trades", 3))  if isinstance(_rl_cfg, dict) else 3
            _lr         = _safe_float(_rl_cfg.get("learn_rate", 0.05)) if isinstance(_rl_cfg, dict) else 0.05
            _max_m      = _safe_float(_rl_cfg.get("max_mult",   2.0))  if isinstance(_rl_cfg, dict) else 2.0
            _min_m      = _safe_float(_rl_cfg.get("min_mult",   0.3))  if isinstance(_rl_cfg, dict) else 0.3
            _RL_ORDER   = ["bull", "bear", "chop"]
            rl_rows_html = ""
            for regime in _RL_ORDER:
                rs = _rl_stats.get(regime, {})
                if not isinstance(rs, dict):
                    continue
                mult      = _safe_float(rs.get("multiplier", 1.0))
                wins      = _safe_int(rs.get("wins",         0))
                losses    = _safe_int(rs.get("losses",       0))
                total_pnl = _safe_float(rs.get("total_pnl",  0.0))
                total_t   = wins + losses
                wr_pct    = wins / total_t * 100.0 if total_t > 0 else 0.0
                warm      = total_t >= _min_trades
                mult_col  = (
                    T["green"]  if warm and mult > 1.0 else
                    T["red"]    if warm and mult < 1.0 else
                    T["muted"]
                )
                warm_label = "warm" if warm else f"cold ({total_t}/{_min_trades})"
                warm_col   = T["text2"] if warm else T["yellow"]
                wr_col     = (
                    T["green"]  if total_t > 0 and wr_pct >= 50 else
                    T["yellow"] if total_t > 0 and wr_pct >= 35 else
                    T["red"]    if total_t > 0 else
                    T["muted"]
                )
                rl_rows_html += (
                    f'<tr>'
                    f'<td style="color:{T["text"]};font-weight:700;'
                    f'font-family:{T["mono"]};font-size:0.65rem;">'
                    f'{_esc(regime.upper())}</td>'
                    f'<td style="color:{mult_col};font-weight:700;'
                    f'font-family:{T["mono"]};font-size:0.65rem;">'
                    f'{mult:.4f}×</td>'
                    f'<td style="color:{warm_col};font-family:{T["mono"]};'
                    f'font-size:0.65rem;">{_esc(warm_label)}</td>'
                    f'<td style="color:{T["text2"]};">{wins}</td>'
                    f'<td style="color:{T["text2"]};">{losses}</td>'
                    f'<td style="color:{wr_col};font-weight:700;">'
                    f'{"—" if total_t == 0 else f"{wr_pct:.0f}%"}</td>'
                    f'<td style="color:{_rl_pnl_col(total_pnl)};">'
                    f'{"—" if total_t == 0 else f"${total_pnl:+,.2f}"}</td>'
                    f'</tr>'
                )
            if rl_rows_html:
                rl_html = (
                    f'<div class="ot-tile" style="padding:10px;">'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'letter-spacing:0.12em;text-transform:uppercase;'
                    f'color:{T["text2"]};margin-bottom:6px;">'
                    f'RL REGIME WEIGHT TABLE</div>'
                    f'<table class="ot-table"><thead><tr>'
                    f'<th>REGIME</th><th>MULTIPLIER</th><th>STATE</th>'
                    f'<th>WINS</th><th>LOSSES</th><th>WIN RATE</th><th>TOTAL PnL</th>'
                    f'</tr></thead><tbody>{rl_rows_html}</tbody></table>'
                    f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                    f'color:{T["muted"]};margin-top:4px;">'
                    f'Multiplier applied to signal.confidence and signal.kelly_f at '
                    f'every analyze() call for this regime. '
                    f'Cold = below RL_MIN_TRADES ({_min_trades}) — multiplier locked at 1.0. '
                    f'Config: learn_rate={_lr:.3f} · range [{_min_m:.2f}–{_max_m:.2f}].'
                    f'</div></div>'
                )
                st.markdown(rl_html, unsafe_allow_html=True)
                # ── [TRACK31-RR2] RL regime weight reset controls ─────────────
                # Appended inside the same try/except that wraps the RL regime
                # weight table render above.  A failure here must not break the
                # table; the outer except handles it at log.debug level.
                # Write-gate, _write_control_event(), _audit(), and disabled=
                # button pattern are identical to Section 8 (Track 07 writeback).
                try:
                    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
                    _rl_bot_live  = _bot_is_live()
                    _rl_demo_mode = _is_demo_mode()
                    _rl_writes_ok = _rl_bot_live and not _rl_demo_mode

                    if not _rl_bot_live:
                        st.markdown(
                            f'<div class="ot-alert-red" style="font-size:0.68rem;margin-bottom:6px;">'
                            f'🔌 BOT OFFLINE — writes disabled</div>',
                            unsafe_allow_html=True,
                        )
                    elif _rl_demo_mode:
                        st.markdown(
                            f'<div class="ot-alert-yellow" style="font-size:0.68rem;margin-bottom:6px;">'
                            f'🧪 DEMO MODE — writes disabled</div>',
                            unsafe_allow_html=True,
                        )

                    st.markdown(
                        f'<div class="ot-tile" style="padding:10px;">'
                        f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                        f'letter-spacing:0.12em;text-transform:uppercase;'
                        f'color:{T["text2"]};margin-bottom:8px;">'
                        f'RL REGIME WEIGHT CONTROLS — MANUAL RESET</div>'
                        f'<div style="font-family:{T["mono"]};font-size:0.62rem;'
                        f'color:{T["muted"]};margin-bottom:8px;">'
                        f'Resets the selected regime\'s multiplier to 1.0 and clears its '
                        f'trade history in-memory. Use after a bad market phase has '
                        f'distorted learning. The RL table resumes learning from the next '
                        f'trade. Operator-explicit only — not automated.</div></div>',
                        unsafe_allow_html=True,
                    )

                    _rl_rc1, _rl_rc2, _rl_rc3 = st.columns([1, 1, 3])
                    with _rl_rc1:
                        _rl_sel_regime = st.selectbox(
                            "Regime",
                            options=["bull", "bear", "chop"],
                            index=0,
                            key="rl_reset_regime_select",
                            label_visibility="visible",
                        )
                    with _rl_rc2:
                        if st.button(
                            "↺ Reset Selected",
                            key="rl_reset_regime_btn",
                            use_container_width=True,
                            disabled=not _rl_writes_ok,
                        ):
                            if _rl_writes_ok:
                                _ok = _write_control_event(
                                    "reset_regime_weight",
                                    regime=_rl_sel_regime,
                                    source="rl_regime_reset",
                                )
                                if _ok:
                                    st.success(
                                        f"Regime reset queued for '{_rl_sel_regime}'. "
                                        f"Executor picks up within one trade cycle.",
                                        icon="✅",
                                    )
                                    _audit(
                                        f"rl_regime_reset regime={_rl_sel_regime}"
                                    )
                                else:
                                    st.warning(
                                        "Control event write failed — check dashboard logs.",
                                        icon="⚠️",
                                    )
                    with _rl_rc3:
                        if st.button(
                            "↺ Reset All Regimes",
                            key="rl_reset_all_btn",
                            use_container_width=True,
                            disabled=not _rl_writes_ok,
                        ):
                            if _rl_writes_ok:
                                _ok = _write_control_event(
                                    "reset_all_regime_weights",
                                    source="rl_regime_reset",
                                )
                                if _ok:
                                    st.success(
                                        "All regime weights reset queued. "
                                        "Executor picks up within one trade cycle.",
                                        icon="✅",
                                    )
                                    _audit("rl_regime_reset_all")
                                else:
                                    st.warning(
                                        "Control event write failed — check dashboard logs.",
                                        icon="⚠️",
                                    )
                except Exception as _rl_ctrl_exc:
                    log.debug("[TRACK31-RR2] RL regime reset controls failed: %s", _rl_ctrl_exc)
                # ── [/TRACK31-RR2] ────────────────────────────────────────────

                st.markdown(
                    f'<div class="ot-tile" style="padding:10px;color:{T["muted"]};'
                    f'font-family:{T["mono"]};font-size:0.70rem;">'
                    f'RL REGIME WEIGHTS — No regime stats found in snapshot.</div>',
                    unsafe_allow_html=True,
                )
        except Exception as _rl_exc:
            log.debug("[TRACK04-SF5] RL regime weight panel failed: %s", _rl_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">RL REGIME WEIGHT TABLE</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'RL regime weights not yet in status — requires Track 04 executor or later.'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── 8. [TRACK07-WB1] Score Feedback Controls ─────────────────────────────
    # Operator-explicit per-symbol autotune override.  Fires a P24 control
    # event into the existing control_queue.jsonl channel.  Executor picks
    # up "set_autotune_override" / "reset_autotune_override" within one cycle
    # and mutates CoinPerformanceTracker accordingly.  The Track 04 autotune
    # display above reflects the change on the next status refresh.
    # No automated writeback.  Operator action is the only trigger.
    # [TRACK15] writes_ok guard added — consistent with render_tactical_sidebar().
    # Apply and Reset buttons are disabled and _write_control_event() is never
    # reached when the bot is offline or demo mode is active.
    st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)
    _at_ctrl_state  = status.get("autotune_state", {})
    _at_ctrl_syms   = sorted(_at_ctrl_state.keys()) if isinstance(_at_ctrl_state, dict) else []
    _wb_bot_live    = _bot_is_live()
    _wb_demo_mode   = _is_demo_mode()
    _wb_writes_ok   = _wb_bot_live and not _wb_demo_mode

    if _at_ctrl_syms:
        try:
            # [TRACK15] Write-gate warning tiles — matching render_tactical_sidebar() style.
            if not _wb_bot_live:
                st.markdown(
                    f'<div class="ot-alert-red" style="font-size:0.68rem;margin-bottom:6px;">'
                    f'🔌 BOT OFFLINE — writes disabled</div>',
                    unsafe_allow_html=True,
                )
            elif _wb_demo_mode:
                st.markdown(
                    f'<div class="ot-alert-yellow" style="font-size:0.68rem;margin-bottom:6px;">'
                    f'🧪 DEMO MODE — writes disabled</div>',
                    unsafe_allow_html=True,
                )

            st.markdown(
                f'<div class="ot-tile" style="padding:10px;">'
                f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
                f'letter-spacing:0.12em;text-transform:uppercase;'
                f'color:{T["text2"]};margin-bottom:8px;">'
                f'SCORE FEEDBACK CONTROLS — AUTOTUNE OVERRIDE</div>'
                f'<div style="font-family:{T["mono"]};font-size:0.62rem;'
                f'color:{T["muted"]};margin-bottom:8px;">'
                f'Applies a manual confidence floor and allocation cap to a symbol\'s '
                f'admission path.  Based on setup evidence from the diagnostics panel.  '
                f'Operator-explicit only — not automated.  Override persists until Reset '
                f'is pressed or the executor restarts.</div></div>',
                unsafe_allow_html=True,
            )

            _cc1, _cc2, _cc3 = st.columns([2, 1, 1])
            with _cc1:
                _wb_sel_sym = st.selectbox(
                    "Symbol",
                    options=_at_ctrl_syms,
                    index=0,
                    key="wb_sym_select",
                    label_visibility="visible",
                )
            with _cc2:
                _wb_conf = st.number_input(
                    "Conf Floor",
                    min_value=0.00,
                    max_value=0.99,
                    value=0.55,
                    step=0.05,
                    format="%.2f",
                    key="wb_conf_floor",
                    label_visibility="visible",
                    help="Minimum signal confidence required for entry. "
                         "Clamped to [MIN_SIGNAL_CONF, MAX_CONF_OVERRIDE] by executor.",
                )
            with _cc3:
                _wb_alloc = st.number_input(
                    "Alloc Cap",
                    min_value=0.001,
                    max_value=0.200,
                    value=0.100,
                    step=0.010,
                    format="%.3f",
                    key="wb_alloc_cap",
                    label_visibility="visible",
                    help="Maximum allocation fraction for entry sizing. "
                         "Clamped to [MIN_ALLOC_FLOOR, MAX_ALLOC_PCT] by executor.",
                )

            _wb_col_apply, _wb_col_reset, _wb_col_pad = st.columns([1, 1, 3])
            with _wb_col_apply:
                # [TRACK15] disabled= kwarg prevents interaction when writes are blocked.
                if st.button("▶ Apply Override", key="wb_apply_btn",
                             use_container_width=True,
                             disabled=not _wb_writes_ok):
                    if not _wb_writes_ok:  # defensive: belt-and-braces guard
                        pass
                    else:
                        _ok = _write_control_event(
                            "set_autotune_override",
                            symbol=_wb_sel_sym,
                            conf_floor=float(_wb_conf),
                            alloc_cap=float(_wb_alloc),
                            source="score_feedback",
                        )
                        if _ok:
                            st.success(
                                f"Override queued for {_wb_sel_sym}: "
                                f"conf_floor={_wb_conf:.2f}  alloc_cap={_wb_alloc:.3f}. "
                                f"Executor picks up within one trade cycle.",
                                icon="✅",
                            )
                            _audit(
                                f"score_feedback_override symbol={_wb_sel_sym} "
                                f"conf_floor={_wb_conf:.4f} alloc_cap={_wb_alloc:.4f}"
                            )
                        else:
                            st.warning(
                                "Control event write failed — check dashboard logs.",
                                icon="⚠️",
                            )

            with _wb_col_reset:
                # [TRACK15] disabled= kwarg prevents interaction when writes are blocked.
                if st.button("✕ Reset Override", key="wb_reset_btn",
                             use_container_width=True,
                             disabled=not _wb_writes_ok):
                    if not _wb_writes_ok:  # defensive: belt-and-braces guard
                        pass
                    else:
                        _ok = _write_control_event(
                            "reset_autotune_override",
                            symbol=_wb_sel_sym,
                            source="score_feedback",
                        )
                        if _ok:
                            st.success(
                                f"Reset queued for {_wb_sel_sym}. "
                                f"Executor picks up within one trade cycle.",
                                icon="✅",
                            )
                            _audit(f"score_feedback_reset symbol={_wb_sel_sym}")
                        else:
                            st.warning(
                                "Control event write failed — check dashboard logs.",
                                icon="⚠️",
                            )

        except Exception as _wb_exc:
            log.debug("[TRACK07-WB1] score feedback controls failed: %s", _wb_exc)
    else:
        st.markdown(
            f'<div class="ot-tile" style="padding:10px;">'
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'letter-spacing:0.12em;text-transform:uppercase;'
            f'color:{T["text2"]};margin-bottom:6px;">'
            f'SCORE FEEDBACK CONTROLS</div>'
            f'<div style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.70rem;">'
            f'No tracked symbols in autotune_state — requires Track 04 executor or later. '
            f'Controls become available once at least one symbol is tracked by '
            f'CoinPerformanceTracker.</div></div>',
            unsafe_allow_html=True,
        )
    # ── [/TRACK07-WB1] ───────────────────────────────────────────────────────




# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P13 TRUST SCORES
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p13_trust() -> None:
    status = _get_status()
    _section("P13 BAYESIAN TRUST SCORE — MTF CONFIDENCE FILTER", "🧬")

    # [FIX-P13-KEY] Bot writes "p13_trust_scores"; "p13_trust_map" is legacy fallback
    trust_map = status.get("p13_trust_scores", status.get("p13_trust_map", {}))
    if not isinstance(trust_map, dict):
        trust_map = {}
    positions = status.get("positions", {})
    SLOTS     = 4
    cols      = st.columns(SLOTS)

    by_sym: dict[str, dict] = {}
    if isinstance(trust_map, dict) and trust_map:
        for key, v in trust_map.items():
            if not isinstance(v, dict):
                continue
            parts = str(key).split(":")
            if len(parts) != 2:
                continue
            sym, tf = parts
            by_sym.setdefault(sym, {})[tf] = v

    sym_items = sorted(by_sym.items())[:SLOTS]

    for i in range(SLOTS):
        if i >= len(sym_items):
            with cols[i]:
                _slot_placeholder("P13 SLOT EMPTY", 260)
            continue

        sym, tf_data = sym_items[i]
        pos      = positions.get(sym, {}) if isinstance(positions, dict) else {}
        agg_tr   = _safe_float(pos.get("p13_agg_trust", DEFAULT_TRUST), DEFAULT_TRUST)
        censored = bool(pos.get("p13_censored", False))
        dead_a   = _safe_int(status.get("p13_dead_alpha_exits"))
        tr_col   = T["green"] if agg_tr >= 0.65 else T["yellow"] if agg_tr >= 0.45 else T["red"]

        tf_rows = ""
        for tf in ("1hour", "4hour", "1day"):
            d    = tf_data.get(tf, {}) if isinstance(tf_data, dict) else {}
            if not isinstance(d, dict): continue
            a    = _safe_float(d.get("alpha",  2.0), 2.0)
            b    = _safe_float(d.get("beta",   2.0), 2.0)
            tf_v = _safe_float(d.get("trust_factor", DEFAULT_TRUST), DEFAULT_TRUST)
            lbl  = _esc(str(d.get("label", "Neutral")))
            n    = _safe_int(d.get("samples"))
            bpct = max(0.0, min(tf_v * 100.0, 100.0))
            bcol = T["green"] if tf_v >= 0.65 else T["yellow"] if tf_v >= 0.45 else T["red"]
            tf_rows += (
                f'<div style="margin-bottom:6px;">'
                f'<div style="display:flex;justify-content:space-between;'
                f'font-family:{T["mono"]};font-size:0.66rem;margin-bottom:2px;">'
                f'<span style="color:{T["text2"]};">{tf}</span>'
                f'<span style="color:{bcol};font-weight:700;">{tf_v:.4f}</span></div>'
                f'<div class="ot-heat-track"><div class="ot-heat-fill"'
                f' style="width:{bpct:.1f}%;background:{bcol};"></div></div>'
                f'<div style="display:flex;justify-content:space-between;'
                f'font-family:{T["mono"]};font-size:0.60rem;margin-top:2px;">'
                f'<span style="color:{T["muted"]};">{lbl}</span>'
                f'<span style="color:{T["muted"]};">{n} samp · α={a:.1f} β={b:.1f}</span>'
                f'</div></div>'
            )

        censor_badge = (
            f'<span class="ot-badge" style="color:{T["red"]};background:{T["glow_r"]};">CENSORED</span>'
            if censored else
            f'<span class="ot-badge" style="color:{T["green"]};background:{T["glow_g"]};">CLEAR</span>'
        )

        with cols[i]:
            st.markdown(
                f'<div class="ot-tile" style="min-height:260px;border-color:{tr_col}44;">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<span style="font-family:{T["mono"]};font-size:0.92rem;font-weight:700;">{_esc(sym)}</span>'
                f'{censor_badge}</div>'
                f'<div style="font-family:{T["mono"]};font-size:1.12rem;font-weight:800;'
                f'color:{tr_col};margin-top:4px;">{agg_tr:.4f}</div>'
                f'<div class="ot-kpi-sub">aggregate trust</div>'
                f'{tf_rows}'
                f'<div style="border-top:1px solid {T["border"]};margin-top:6px;'
                f'padding-top:6px;display:flex;justify-content:space-between;'
                f'font-family:{T["mono"]};font-size:0.62rem;color:{T["text2"]};">'
                f'<span>DEAD-α EXITS</span>'
                f'<span style="color:{T["yellow"]};font-weight:700;">{dead_a}</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P15 ORACLE
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_p15_oracle() -> None:
    status = _get_status()
    _section("P15 INSTITUTIONAL ORACLE — COINBASE · VOL SURGE · NUCLEAR CANCEL", "🛰")

    p15    = status.get("p15_oracle_signals", {})
    oracle = p15 if isinstance(p15, dict) else {}
    _LKG_KEY = "_p15_lkg_oracle_cards"

    def _norm_sig(sym: str, sig: dict) -> dict:
        return {
            "sym":    sym,
            "signal": str(sig.get("signal", "—")).upper(),
            "mult":   _safe_float(sig.get("mult", 1.0), 1.0),
            "valid":  bool(sig.get("valid", False)),
            "size":   _safe_float(sig.get("size")),
        }

    items = [_norm_sig(str(sym), sig)
             for sym, sig in oracle.items() if isinstance(sig, dict)]

    def _sort_key(d: dict):
        return (0 if d["valid"] else 1, -d["mult"], -d["size"], d["sym"])

    ordered = sorted(items, key=_sort_key) if items else []
    if ordered:
        st.session_state[_LKG_KEY] = ordered
    else:
        ordered = st.session_state.get(_LKG_KEY, []) or []

    SLOTS = 4
    cols  = st.columns(SLOTS)

    for i in range(SLOTS):
        with cols[i]:
            if i >= len(ordered):
                _slot_placeholder("ORACLE SLOT EMPTY", 210)
                continue
            d        = ordered[i]
            sym      = _esc(d["sym"])
            signal   = _esc(d["signal"])
            mult     = d["mult"]
            valid    = d["valid"]
            size_usd = d["size"]
            is_buy   = "BUY" in d["signal"] or "BULL" in d["signal"]
            is_sell  = "SELL" in d["signal"] or "BEAR" in d["signal"]
            sig_col  = T["green"] if is_buy else T["red"] if is_sell else T["text2"]
            sig_icon = "▲" if is_buy else "▼" if is_sell else "━"
            v_badge  = (
                f'<span class="ot-badge" style="color:{T["green"]};background:{T["glow_g"]};">✓ VALID</span>'
                if valid else
                f'<span class="ot-badge" style="color:{T["muted"]};">EXPIRED</span>'
            )
            mult_col = T["red"] if mult >= 6 else T["yellow"] if mult >= 3 else T["green"]
            st.markdown(
                f'<div class="ot-tile" style="min-height:210px;border-color:{sig_col}44;">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;">'
                f'<span style="font-family:{T["mono"]};font-size:0.92rem;font-weight:700;">{sym}</span>'
                f'{v_badge}</div>'
                f'<div style="font-family:{T["mono"]};font-size:1.15rem;font-weight:900;'
                f'color:{sig_col};margin-top:6px;">{sig_icon} {signal}</div>'
                f'<div style="display:flex;justify-content:space-between;font-family:{T["mono"]};'
                f'font-size:0.70rem;margin-top:8px;">'
                f'<span style="color:{T["text2"]};">MULT</span>'
                f'<span style="color:{mult_col};font-weight:800;">{mult:.2f}×</span></div>'
                f'<div style="display:flex;justify-content:space-between;font-family:{T["mono"]};'
                f'font-size:0.70rem;margin-top:4px;">'
                f'<span style="color:{T["text2"]};">SIZE</span>'
                f'<span style="color:{T["text"]};font-weight:700;">${size_usd:,.0f}</span></div>'
                f'<div class="ot-kpi-sub" style="margin-top:10px;">oracle directive</div></div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P17 INTELLIGENCE FUSION
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_p17_intelligence() -> None:
    status = _get_status()
    _section("P17 INTELLIGENCE FUSION — NARRATIVE · COUNCIL · SCORES", "🧠")

    p17 = status.get("p17_intelligence")
    if not isinstance(p17, dict) or not p17:
        p17 = _sg(_SS_P17, {})
    p17 = p17 if isinstance(p17, dict) else {}

    counters  = p17.get("counters", {})
    recents   = p17.get("recent_results", [])
    veto_thr  = _safe_float(p17.get("veto_threshold",        0.30), 0.30)
    boost_thr = _safe_float(p17.get("boost_threshold",       0.80), 0.80)
    cat_thr   = _safe_float(p17.get("catastrophe_threshold", 0.10), 0.10)
    heat_lim  = _safe_float(p17.get("max_portfolio_heat",    0.25), 0.25)
    obi_veto  = _safe_float(p17.get("p20_obi_veto_threshold", -0.5), -0.5)
    wall_rat  = _safe_float(p17.get("p20_wall_ratio",         4.0),  4.0)
    council_on= bool(p17.get("council_enabled", True))

    veto_ct  = _safe_int(counters.get("veto"))
    boost_ct = _safe_int(counters.get("boost"))
    cat_ct   = _safe_int(counters.get("catastrophe"))
    to_ct    = _safe_int(counters.get("timeout"))

    last_r       = recents[0] if recents else {}
    last_score   = last_r.get("score")
    last_verdict = last_r.get("verdict", "N/A")
    last_sym     = _esc(str(last_r.get("symbol", "—")))
    last_lat     = last_r.get("latency_ms")
    last_llm     = last_r.get("llm_used", False)
    last_err     = str(last_r.get("llm_error", ""))
    last_conv    = _safe_float(last_r.get("conviction_multiplier", 1.0), 1.0)
    last_age     = last_r.get("age_secs")
    last_entropy = _safe_float(last_r.get("entropy_normalized"))
    vs           = VERDICT_STYLE.get(last_verdict, VERDICT_STYLE.get("N/A", {}))
    vs_bg        = vs.get("bg") or vs.get("glow") or T["surface2"]

    # [FIX-FLICKER-P17] Rebuild gauge ONLY when score/verdict/symbol changes
    _P17_HASH_KEY = "_p17_gauge_data_hash"
    _p17_hash_now = hash((
        str(last_score), str(last_verdict), str(last_sym), str(last_err)
    ))
    _p17_changed  = st.session_state.get(_P17_HASH_KEY) != _p17_hash_now
    if _p17_changed:
        st.session_state[_P17_HASH_KEY] = _p17_hash_now

    
    fig_fresh  = None
    if last_score is not None and _p17_changed:
        g_col = T["yellow"] if last_err else vs["col"]
        try:
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number", value=round(_safe_float(last_score), 4),
                number={"font": {"size": 36, "color": g_col, "family": _FONT_MONO_PLOTLY},
                        "valueformat": ".4f"},
                gauge={
                    "axis": {"range": [0, 1],
                             "tickvals": [0, cat_thr, veto_thr, 0.5, boost_thr, 1],
                             "ticktext": ["0", f"☣{cat_thr:.2f}", f"✕{veto_thr:.2f}",
                                          "0.5", f"▲{boost_thr:.2f}", "1"],
                             "tickfont": {"size": 7, "color": T["muted"], "family": _FONT_MONO_PLOTLY},
                             "tickwidth": 1, "ticklen": 3},
                    "bar": {"color": g_col, "thickness": 0.22},
                    "bgcolor": "rgba(0,0,0,0)", "borderwidth": 0,
                    "steps": [
                        {"range": [0, cat_thr],         "color": "rgba(255,0,0,0.28)"},
                        {"range": [cat_thr, veto_thr],  "color": "rgba(255,0,60,0.14)"},
                        {"range": [veto_thr, boost_thr],"color": "rgba(100,100,100,0.08)"},
                        {"range": [boost_thr, 1.0],     "color": "rgba(0,255,65,0.18)"},
                    ],
                    "threshold": {"line": {"color": T["yellow"], "width": 2},
                                  "value": 0.5, "thickness": 0.80},
                },
                title={"text": f"NARRATIVE SCORE [{last_sym}]",
                       "font": {"color": T["text2"], "size": 9, "family": _FONT_SANS_PLOTLY}},
            ))
            fig_gauge.update_layout(
                height=220, margin=dict(l=16, r=16, t=20, b=0),
                paper_bgcolor="rgba(0,0,0,0)", font=dict(color=T["text"]),
            )
            fig_fresh = fig_gauge
        except Exception:
            log.debug("render_p17_intelligence: gauge build failed", exc_info=True)

    elif last_score is not None and not _p17_changed:
        # Gauge unchanged — reuse LKG via single-slot paint below
        pass
    else:
        pass  # no score yet — _paint_fig will render the skeleton below

    gauge_slot = st.empty()

    if fig_fresh is not None:
        st.session_state[_FIG_KEY_P17_GAUGE] = fig_fresh

    fig_to_show = (
        fig_fresh
        if fig_fresh is not None
        else (st.session_state.get(_FIG_KEY_P17_GAUGE) if last_score is not None else None)
    )

    if fig_to_show is not None:
        gauge_slot.plotly_chart(
            fig_to_show,
            width="stretch",
            config={"displayModeBar": False},
            key="ot_p17_score_gauge",
        )
    else:
        gauge_slot.markdown(
            f'<div class="ot-tile" style="height:220px;display:flex;'
            f'align-items:center;justify-content:center;">'
            f'<span style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.72rem;">'
            f'AWAITING SCORE</span></div>',
            unsafe_allow_html=True,
        )

    mode_badge = (
        f'<span class="ot-badge" style="color:{T["green"]};">LLM</span>'
        if last_llm else
        f'<span class="ot-badge" style="color:{T["orange"]};">FALLBACK</span>'
    )
    lat_str  = f"{_safe_float(last_lat):.1f}ms" if last_lat else "—"
    age_str  = f"{_safe_float(last_age):.0f}s ago" if last_age else ""
    conv_col = T["green"] if last_conv >= 1.2 else T["yellow"] if last_conv >= 0.8 else T["red"]

    st.markdown(
        f'<div class="ot-tile" style="background:{vs_bg};border-color:{vs["col"]}44;">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;">'
        f'{_verdict_badge(last_verdict)} {mode_badge}'
        f'<span style="font-family:{T["mono"]};font-size:0.66rem;color:{T["muted"]};">'
        f'{lat_str} · {age_str}</span></div>'
        f'<div style="display:flex;justify-content:space-between;margin-top:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.72rem;color:{T["text2"]};">CONV MULT</span>'
        f'<span style="font-family:{T["mono"]};font-size:0.72rem;color:{conv_col};'
        f'font-weight:700;">{last_conv:.4f}×</span></div>'
        f'<div style="display:flex;justify-content:space-between;">'
        f'<span style="font-family:{T["mono"]};font-size:0.66rem;color:{T["text2"]};">ENTROPY NORM</span>'
        f'<span style="font-family:{T["mono"]};font-size:0.66rem;color:{T["blue"]};">{last_entropy:.4f}</span>'
        f'</div></div>',
        unsafe_allow_html=True,
    )
    if last_err:
        st.markdown(
            f'<div class="ot-alert-yellow" style="font-size:0.65rem;">'
            f'⚠ LLM ERR: {_esc(last_err[:80])}</div>',
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4 = st.columns(4)
    with c1: _kpi("BOOSTS",   str(boost_ct), "", T["green"]  if boost_ct > 0 else T["muted"])
    with c2: _kpi("VETOES",   str(veto_ct),  "", T["red"]    if veto_ct  > 0 else T["muted"])
    with c3: _kpi("☣ CAT",   str(cat_ct),   "", "#FF0000"    if cat_ct   > 0 else T["muted"])
    with c4: _kpi("TIMEOUTS", str(to_ct),    "", T["yellow"] if to_ct    > 0 else T["muted"])

    st.markdown(
        f'<div class="ot-tile" style="padding:8px 12px;">'
        f'<div class="ot-kpi-label" style="margin-bottom:6px;">P17 CONFIGURATION</div>'
        + "".join(
            f'<div style="display:flex;justify-content:space-between;padding:2px 0;'
            f'font-family:{T["mono"]};font-size:0.66rem;">'
            f'<span style="color:{T["text2"]};">{k}</span>'
            f'<span style="color:{vc};">{v}</span></div>'
            for k, v, vc in [
                ("VETO THRESH",  f"{veto_thr:.2f}",          T["red"]),
                ("BOOST THRESH", f"{boost_thr:.2f}",         T["green"]),
                ("CAT THRESH",   f"{cat_thr:.2f}",           "#FF0000"),
                ("HEAT LIMIT",   f"{heat_lim*100:.0f}%",     T["cyan"]),
                ("OBI VETO",     f"{obi_veto:.2f}",          T["yellow"]),
                ("WALL RATIO",   f"{wall_rat:.1f}×",         T["orange"]),
                ("COUNCIL",      "ON" if council_on else "OFF",
                                  T["green"] if council_on else T["red"]),
            ]
        )
        + '</div>',
        unsafe_allow_html=True,
    )

    if recents:
        _section("RECENT COUNCIL RESULTS", "")
        rows = ""
        for r in recents[:8]:
            sym2  = _esc(str(r.get("symbol", "?")))
            drx2  = _esc(str(r.get("direction", "?")).upper())
            v2    = r.get("verdict", "NEUTRAL")
            sc2   = r.get("score")
            lat2  = r.get("latency_ms")
            age2  = r.get("age_secs")
            vs2   = VERDICT_STYLE.get(v2, VERDICT_STYLE["N/A"])
            dc2   = T["green"] if drx2 == "LONG" else T["red"] if drx2 == "SHORT" else T["text2"]
            lat2_c= (T["green"] if (lat2 and _safe_float(lat2) < 60)
                     else T["yellow"] if (lat2 and _safe_float(lat2) < 90) else T["red"])
            rows += (
                f'<tr>'
                f'<td style="color:{T["text"]};font-weight:700;">{sym2}</td>'
                f'<td style="color:{dc2};">{drx2}</td>'
                f'<td>{_verdict_badge(v2)}</td>'
                # [FIX-FSTR-002] Python 3.11: pre-compute nested f-strings
                f'<td style="color:{vs2["col"]};">{(f"{_safe_float(sc2):.4f}") if sc2 is not None else chr(8212)}</td>'
                f'<td style="color:{lat2_c};">{(f"{_safe_float(lat2):.1f}ms") if lat2 else chr(8212)}</td>'
                f'<td style="color:{T["muted"]};">{(f"{_safe_float(age2):.0f}s") if age2 else chr(8212)}</td>'
                f'</tr>'
            )
        hdr = "".join(f'<th>{h}</th>' for h in ["SYM", "DIR", "VERDICT", "SCORE", "LAT", "AGE"])
        st.markdown(
            f'<div class="ot-tile ot-scroll-tall" style="padding:0;">'
            f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
            f'<tbody>{rows}</tbody></table></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P20 COUNCIL
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p20_council() -> None:
    status = _get_status()
    _section("P20 ZOMBIE COUNCIL — AGENT VOTE DETAIL PER SYMBOL", "🗳")

    positions   = status.get("positions", {})
    if not isinstance(positions, dict): positions = {}
    p20_risk    = status.get("p20_global_risk", {})
    zombie_mode = bool(p20_risk.get("zombie_active") or status.get("p20_zombie_mode_active"))
    dd_pct      = _safe_float(p20_risk.get("drawdown_pct"))
    peak_eq     = _safe_float(p20_risk.get("peak_equity"))
    curr_eq     = _safe_float(p20_risk.get("current_equity"))
    zombie_pct  = _safe_float(status.get("p20_drawdown_zombie_pct"), 99.0)
    grace       = bool(p20_risk.get("in_grace_period", False))
    grace_rem   = _safe_float(p20_risk.get("grace_remaining_secs"))

    z_col = T["red"] if zombie_mode else T["yellow"] if dd_pct > 5 else T["green"]
    c1, c2, c3 = st.columns(3)
    with c1: _kpi("DRAWDOWN",         f"{dd_pct:.2f}%", f"peak ${peak_eq:,.2f}", z_col)
    with c2: _kpi("ZOMBIE TRIPPED AT", f"{zombie_pct:.0f}%", "dd threshold", T["text2"])
    with c3: _kpi("CURRENT EQUITY",   f"${curr_eq:,.2f}",
                  "🛡 GRACE ACTIVE" if grace else "running",
                  T["yellow"] if grace else T["text2"])

    if zombie_mode:
        st.markdown(f'<div class="ot-alert-red">☠ ZOMBIE MODE — all new entries halted</div>',
                    unsafe_allow_html=True)
    if grace:
        st.markdown(f'<div class="ot-alert-yellow">🛡 GRACE PERIOD — {grace_rem:.0f}s remaining</div>',
                    unsafe_allow_html=True)

    if not positions:
        return
    for sym, pos in sorted(positions.items()):
        council = pos.get("p20_council_detail", [])
        if not isinstance(council, list) or not council:
            continue
        zm_pos = bool(pos.get("p20_zombie_mode", False))
        cb_pos = bool(pos.get("p7_cb_tripped",   False))
        dir_s  = _esc(pos.get("direction", "none").upper())
        dir_c  = T["green"] if dir_s == "LONG" else T["red"] if dir_s == "SHORT" else T["muted"]

        header = (
            f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
            f'<span style="font-family:{T["mono"]};font-size:0.88rem;font-weight:700;">{_esc(sym)}</span>'
            f'<span style="color:{dir_c};font-family:{T["mono"]};font-size:0.72rem;">{dir_s}</span>'
            + (f'<span class="ot-badge" style="color:{T["red"]};">ZOMBIE</span>' if zm_pos else "")
            + (f'<span class="ot-badge" style="color:{T["red"]};">CB TRIPPED</span>' if cb_pos else "")
            + '</div>'
        )
        rows = ""
        for ag in council:
            if not isinstance(ag, dict): continue
            agent  = _esc(str(ag.get("agent", "—")).upper())
            vote   = _esc(str(ag.get("vote", "—")).upper())
            score  = ag.get("score")
            reason = _esc(str(ag.get("reason", "—"))[:72])
            v_col  = T["green"] if vote == "ALLOW" else T["red"] if vote == "BLOCK" else T["text2"]
            rows += (
                f'<div class="ot-council-row">'
                f'<span style="color:{T["text"]};font-weight:700;">{agent}</span>'
                f'<span style="color:{v_col};font-weight:700;">{vote}</span>'
                # [FIX-FSTR-003] Python 3.11: inline nested f-string
                f'<span style="color:{T["blue"]};">{(f"{_safe_float(score):.4f}") if score is not None else chr(8212)}</span>'
                f'<span style="color:{T["text2"]};font-size:0.62rem;">{reason}</span>'
                f'</div>'
            )
        st.markdown(
            f'<div class="ot-tile" style="margin-bottom:6px;">{header}{rows}</div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P22 WHALE TAPE
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_whale_tape() -> None:
    status = _get_status()
    _section("P22 WHALE SWEEP TAPE — ORACLE SIGNALS", "🐋")

    raw_tape = status.get("p22_whale_tape")
    if not isinstance(raw_tape, list) or not raw_tape:
        raw_tape = _sg(_SS_WHALE_TAPE, [])
    tape = raw_tape if isinstance(raw_tape, list) else []

    mkt = status.get("market_data", {})
    wt  = mkt.get("whale_tape", {}) if isinstance(mkt, dict) else {}
    if isinstance(wt, dict) and wt:
        pk_mult = _safe_float(wt.get("peak_multiplier"))
        dom_sid = _esc(str(wt.get("dominant_side", "NEUTRAL")))
        pk_sym  = _esc(str(wt.get("peak_symbol", "—")))
        dom_col = T["green"] if dom_sid == "BUY" else T["red"] if dom_sid == "SELL" else T["muted"]
        st.markdown(
            f'<div class="ot-tile" style="padding:8px 12px;margin-bottom:4px;'
            f'border-color:{dom_col}44;">'
            f'<div style="display:flex;justify-content:space-between;align-items:center;">'
            f'<span style="font-family:{T["mono"]};font-size:0.66rem;color:{T["text2"]};">PEAK WHALE SWEEP</span>'
            f'<span style="font-family:{T["mono"]};font-size:0.88rem;font-weight:700;color:{dom_col};">'
            f'{dom_sid} {pk_sym}</span>'
            f'<span style="font-family:{T["mono"]};font-size:0.82rem;font-weight:700;color:{T["orange"]};">'
            f'{pk_mult:.2f}×</span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    if not tape:
        _slot_placeholder("NO ORACLE SWEEPS — WATCHING…", 80)
        return

    rows = ""
    for row in tape:
        if not isinstance(row, dict): continue
        sym    = _esc(str(row.get("symbol", "—")))
        sig    = _esc(str(row.get("signal", "—")).upper())
        size   = _safe_float(row.get("size_usd"))
        mult   = _safe_float(row.get("multiplier"), 1.0)
        valid  = bool(row.get("valid", False))
        cancel = bool(row.get("cancel_buys", False))
        if "BUY" in sig or "BULL" in sig:
            sig_col, arrow = T["green"], "▲"
        elif "SELL" in sig or "BEAR" in sig:
            sig_col, arrow = T["red"], "▼"
        else:
            sig_col, arrow = T["text2"], "━"
        sz_col  = T["red"] if size >= 500_000 else T["yellow"] if size >= 100_000 else T["green"]
        vbadge  = (
            f'<span style="color:{T["red"]};font-size:0.60rem;">⛔</span>' if cancel else
            (f'<span style="color:{T["green"]};font-size:0.60rem;">✔</span>' if valid else
             f'<span style="color:{T["muted"]};font-size:0.60rem;">⏱</span>')
        )
        rows += (
            f'<div class="ot-whale-row">'
            f'<span style="color:{T["text"]};font-weight:700;font-family:{T["mono"]};">{sym}</span>'
            f'<span style="color:{sig_col};font-weight:700;">{arrow}</span>'
            f'<span style="color:{sz_col};font-family:{T["mono"]};">${size:>10,.0f}</span>'
            f'<span style="color:{T["text2"]};font-family:{T["mono"]};">{mult:.3f}×</span>'
            f'{vbadge}</div>'
        )
    st.markdown(
        f'<div class="ot-tile ot-scroll" style="padding:4px 0;">{rows}</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P22 UNIFIED (Entropy / Panic Lock / Regime / OpenRouter)
# ══════════════════════════════════════════════════════════════════════════════

def _entropy_colour(norm: float) -> str:
    if norm < 0.45: return T["green"]
    if norm < 0.75: return T["yellow"]
    return T["red"]


@st.fragment(run_every=FRAG_MED)
def render_p22_unified() -> None:
    status = _get_status()
    _section("P22 ENTROPY SHIELD · PANIC LOCK · REGIME TRAIL", "🛡")

    p22 = status.get("p22")
    if not isinstance(p22, dict) or not p22:
        p22 = _sg(_SS_P22, {})
    p22 = p22 if isinstance(p22, dict) else {}

    pl = p22.get("panic_lock", {})
    if isinstance(pl, dict) and pl.get("locked"):
        rem = _safe_float(pl.get("remaining_secs"))
        st.markdown(
            f'<div class="ot-alert-red" style="font-size:0.90rem;font-weight:700;padding:10px 14px;">'
            f'🚨 PANIC LOCK ACTIVE — {rem:.0f}s — BUY ENTRIES BLOCKED</div>',
            unsafe_allow_html=True,
        )

    tab_ent, tab_pl, tab_reg, tab_or = st.tabs(
        ["📊 Entropy Shield", "🚨 Panic Lock", "📈 Regime Trail", "🧠 OpenRouter"],
    )

    # ── Entropy Shield ────────────────────────────────────────────────────────
    with tab_ent:
        ent_thr   = _safe_float(p22.get("entropy_threshold",        3.5), 3.5)
        ent_boost = _safe_float(p22.get("entropy_confidence_boost", 0.20), 0.20)
        epc       = p22.get("entropy_per_coin", {}) or {}
        max_bits  = 3.32

        if epc and isinstance(epc, dict):
            syms  = sorted(epc.keys())
            ents  = [_safe_float(epc[s].get("entropy")) for s in syms]
            norms = [e / max_bits for e in ents]
            cols_e= [_entropy_colour(n) for n in norms]
            fig   = go.Figure(go.Bar(
                x=syms, y=ents,
                marker_color=cols_e, marker_line_width=0,
                text=[f"{e:.3f}b" for e in ents],
                textposition="outside",
                textfont=dict(size=9, family=_FONT_MONO_PLOTLY, color=T["text"]),
            ))
            fig.add_hline(y=ent_thr, line_dash="dot", line_color=T["orange"], line_width=1.5,
                          annotation_text=f"boost @ {ent_thr:.1f}b",
                          annotation_font_color=T["orange"], annotation_font_size=8)
            _fig_base(fig, height=200)
            fig.update_layout(title=dict(text="Shannon Entropy per Coin (bits)",
                                         font=dict(size=9, color=T["text2"])),
                              showlegend=False,
                              yaxis=dict(title="bits", side="right"))
            with st.container(height=240, border=False):
                _paint_fig(st.empty(), fig, _FIG_KEY_ENT_BAR, 200,
                           "AWAITING ENTROPY", "ot_p22_entropy_bar")
            st.markdown(
                f'<div class="ot-tile" style="padding:8px 12px;">'
                f'<div style="display:flex;gap:20px;font-family:{T["mono"]};font-size:0.68rem;">'
                f'<span style="color:{T["text2"]};">THRESHOLD: <span style="color:{T["orange"]};">'
                f'{ent_thr:.2f}b</span></span>'
                f'<span style="color:{T["text2"]};">CONF BOOST: <span style="color:{T["green"]};">'
                f'+{ent_boost*100:.0f}%</span></span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )
        else:
            _slot_placeholder("ENTROPY BUFFER WARMING…", 200)

    # ── Panic Lock ────────────────────────────────────────────────────────────
    with tab_pl:
        if not pl or not isinstance(pl, dict):
            _slot_placeholder("PANIC LOCK DISABLED (--no-p22-panic)", 120)
        else:
            locked      = bool(pl.get("locked", False))
            rem_secs    = _safe_float(pl.get("remaining_secs"))
            trigger_ct  = _safe_int(pl.get("trigger_count"))
            drop_thr    = _safe_float(pl.get("drop_threshold"), 1.5)
            quorum_pct  = _safe_float(pl.get("quorum_pct"), 0.66)
            window_secs = _safe_float(pl.get("window_secs"), 60.0)
            block_secs  = _safe_float(pl.get("block_secs"), 300.0)
            per_coin    = pl.get("per_coin", {}) or {}

            lk_col = T["red"] if locked else T["green"]
            c1, c2, c3, c4 = st.columns(4)
            with c1: _kpi("STATUS", "🔒 LOCKED" if locked else "✓ CLEAR",
                          f"{rem_secs:.0f}s" if locked else "entries ok", lk_col)
            with c2: _kpi("TRIGGERS", str(trigger_ct), "session",
                          T["red"] if trigger_ct > 0 else T["muted"])
            with c3: _kpi("DROP THRESH", f"{drop_thr:.2f}%", f"quorum {quorum_pct*100:.0f}%",
                          T["orange"])
            with c4: _kpi("BLOCK", f"{block_secs:.0f}s", f"window {window_secs:.0f}s", T["blue"])

            if per_coin and isinstance(per_coin, dict):
                rows = ""
                for sym, d in sorted(per_coin.items()):
                    if not isinstance(d, dict): continue
                    drop    = _safe_float(d.get("drop_pct"))
                    paniced = bool(d.get("panicked", False))
                    old_p   = _safe_float(d.get("oldest_price"))
                    lat_p   = _safe_float(d.get("latest_price"))
                    obs     = _safe_int(d.get("observations"))
                    pbar    = min(abs(drop) / drop_thr * 100, 100) if drop_thr > 0 else 0
                    bc      = T["red"] if paniced else T["yellow"] if abs(drop) > drop_thr * 0.5 else T["green"]
                    pb_html = (f'<span class="ot-badge" style="color:{T["red"]};">⚠ PANIC</span>'
                               if paniced else "")
                    rows += (
                        f'<tr>'
                        f'<td style="font-weight:700;">{_esc(sym)}</td>'
                        f'<td style="color:{T["text2"]};">${old_p:.4f}</td>'
                        f'<td style="color:{T["text"]};">${lat_p:.4f}</td>'
                        f'<td style="color:{bc};font-weight:700;">{drop:+.4f}%{pb_html}'
                        f'<div class="ot-minibar-bg"><div class="ot-minibar-fill"'
                        f' style="width:{pbar:.1f}%;background:{bc};height:3px;"></div></div></td>'
                        f'<td style="color:{T["muted"]};">{obs}</td>'
                        f'</tr>'
                    )
                hdr = "".join(f'<th>{h}</th>'
                              for h in ["SYM", "OLDEST", "LATEST",
                                        f"DROP% (thr {drop_thr:.1f}%)", "OBS"])
                st.markdown(
                    f'<div class="ot-tile ot-scroll" style="padding:0;">'
                    f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
                    f'<tbody>{rows}</tbody></table></div>',
                    unsafe_allow_html=True,
                )

    # ── Regime Trail ──────────────────────────────────────────────────────────
    with tab_reg:
        bull_w = _safe_float(p22.get("bull_trail_widen"),   0.005)
        chop_t = _safe_float(p22.get("chop_trail_tighten"), 0.002)
        st.markdown(
            f'<div class="ot-tile" style="padding:8px 12px;margin-bottom:8px;">'
            f'<div style="display:flex;gap:28px;font-family:{T["mono"]};font-size:0.72rem;">'
            f'<span><span style="color:{T["text2"]};">BULL </span>'
            f'<span style="color:{T["green"]};font-weight:700;">+{bull_w*100:.2f}% wider</span></span>'
            f'<span><span style="color:{T["text2"]};">CHOP </span>'
            f'<span style="color:{T["yellow"]};font-weight:700;">−{chop_t*100:.2f}% tighter</span></span>'
            f'</div></div>',
            unsafe_allow_html=True,
        )
        p17  = status.get("p17_intelligence", {})
        recs = p17.get("recent_results", []) if isinstance(p17, dict) else []
        pos  = status.get("positions", {})
        seen: set  = set()
        items: list = []
        for r in recs:
            sym2 = r.get("symbol", "?")
            if sym2 in seen: continue
            seen.add(sym2)
            ta = r.get("trailing_gap_adjustment")
            if ta is None: continue
            reg = (pos.get(sym2, {}).get("regime") or "chop").lower() if isinstance(pos, dict) else "chop"
            rm  = REGIME_META.get(reg, REGIME_META["neutral"])
            try:
                items.append((sym2, rm, float(ta)))
            except Exception:
                continue

        SLOTS = 4
        cols  = st.columns(SLOTS)
        items = items[:SLOTS]
        for idx in range(SLOTS):
            if idx >= len(items):
                with cols[idx]: _slot_placeholder("TRAIL SLOT EMPTY", 150)
                continue
            s2, rm, ta = items[idx]
            ta_s = f"+{ta*100:.3f}%" if ta >= 0 else f"{ta*100:.3f}%"
            ta_c = T["green"] if ta > 0 else T["yellow"] if ta < 0 else T["muted"]
            with cols[idx]:
                st.markdown(
                    f'<div class="ot-tile" style="min-height:150px;box-shadow:0 0 10px {rm["glow"]};">'
                    f'<div class="ot-kpi-label">{_esc(s2)}</div>'
                    f'<span style="color:{rm["col"]};font-size:0.72rem;font-weight:700;">'
                    f'{rm["icon"]} {rm["label"]}</span>'
                    f'<div style="font-family:{T["mono"]};font-size:1.1rem;font-weight:700;'
                    f'color:{ta_c};margin-top:4px;">{ta_s}</div>'
                    f'<div class="ot-kpi-sub">trailing gap adj</div></div>',
                    unsafe_allow_html=True,
                )

    # ── OpenRouter Hub ────────────────────────────────────────────────────────
    with tab_or:
        or_model  = _esc(str(p22.get("openrouter_model", "—")))
        or_active = bool(p22.get("openrouter_active", False))
        p17_inner = status.get("p17_intelligence", {})
        recs2     = p17_inner.get("recent_results", []) if isinstance(p17_inner, dict) else []
        lat_list  = [_safe_float(r["latency_ms"]) for r in recs2 if r.get("latency_ms") is not None]
        avg_lat   = sum(lat_list) / len(lat_list) if lat_list else None
        vader_mode= bool(recs2 and not any(r.get("llm_used", False) for r in recs2[:5]))

        or_col   = T["green"] if or_active else T["yellow"]
        lat_col2 = (T["green"] if (avg_lat and avg_lat < 60)
                    else T["yellow"] if (avg_lat and avg_lat < 90) else T["red"])

        c1, c2, c3 = st.columns(3)
        with c1: _kpi("OR STATUS", "ACTIVE" if or_active else "FALLBACK",
                      "API key set" if or_active else "VADER/keyword", or_col)
        with c2: _kpi("MODEL", or_model[:22], "", T["purple"])
        with c3: _kpi("AVG LAT", f"{avg_lat:.1f}ms" if avg_lat else "—",
                      "VADER FALLBACK" if vader_mode else "LLM active", lat_col2)

        if vader_mode:
            st.markdown(
                f'<div class="ot-alert-yellow">⚠ VADER/KEYWORD FALLBACK — LLM unreachable</div>',
                unsafe_allow_html=True,
            )
        if len(lat_list) >= 2:
            ema_prev = None
            ema_vals: list = []
            for lv in lat_list[-30:]:
                ema_prev = 0.3 * lv + 0.7 * ema_prev if ema_prev else lv
                ema_vals.append(round(ema_prev, 2))
            fig = go.Figure()
            fig.add_trace(go.Scattergl(
                y=ema_vals, mode="lines+markers",
                line=dict(color=T["blue"], width=1.5),
                marker=dict(size=3, color=T["cyan"]),
                fill="tozeroy", fillcolor=T["glow_b"], name="EMA lat",
            ))
            fig.add_hline(y=60, line_dash="dot", line_color=T["yellow"], line_width=1,
                          annotation_text="60ms", annotation_font_color=T["yellow"],
                          annotation_font_size=8)
            _fig_base(fig, height=160)
            fig.update_layout(title=dict(text="Council Latency EMA α=0.30 (ms)",
                                         font=dict(size=9, color=T["text2"])))
            with st.container(height=200, border=False):
                _paint_fig(st.empty(), fig, _FIG_KEY_OR_LAT, 160,
                           "AWAITING LATENCY", "ot_or_lat_spark")


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: VETO AUDIT TRAIL
# ══════════════════════════════════════════════════════════════════════════════

GUARD_COLOUR: dict[str, str] = {
    "Entropy Shield":  T["blue"],
    "Spread Guard":    T["yellow"],
    "Regime Veto":     T["orange"],
    "Volatility Lock": T["red"],
    "Circuit Breaker": "#FF0000",
    "Ghost Equity":    T["purple"],
}


def _map_veto_reason(raw: str) -> str:
    raw_l = raw.lower()
    for k, v in VETO_REASON_MAP.items():
        if k in raw_l: return v
    if "ghost"   in raw_l: return "Ghost Equity"
    if "cb"      in raw_l or "circuit" in raw_l: return "Circuit Breaker"
    return raw[:28] if raw else "Unknown"


@st.fragment(run_every=FRAG_SLOW)
def render_veto_audit() -> None:
    _section("P24 VETO AUDIT TRAIL — NEARLY-TRADED REJECTIONS", "🚫")
    logs = load_veto_logs()
    if not logs:
        _slot_placeholder("✓ NO VETOES — all signals admitted", 60)
        return
    rows = ""
    for entry in list(reversed(logs))[:12]:
        if not isinstance(entry, dict): continue
        raw_ts = entry.get("ts") or entry.get("timestamp") or ""
        sym    = _esc(_norm_sym(str(entry.get("symbol") or "—")))
        sig    = _esc(str(entry.get("signal") or entry.get("direction") or "—").upper())
        sig    = "LONG" if sig in ("LONG", "BUY", "1") else "SHORT" if sig in ("SHORT", "SELL", "-1") else sig
        raw_r  = str(entry.get("reason") or entry.get("veto_reason") or "")
        guard  = _map_veto_reason(raw_r)
        gc     = GUARD_COLOUR.get(guard, T["text2"])
        sc     = T["green"] if sig == "LONG" else T["red"] if sig == "SHORT" else T["text2"]
        ts_str = "—"
        if raw_ts:
            try:
                ts_str = datetime.fromtimestamp(float(raw_ts)).strftime("%H:%M:%S")
            except (ValueError, TypeError, OSError):
                ts_str = str(raw_ts)[:19]
        rows += (
            f'<tr>'
            f'<td style="color:{T["text2"]};font-size:0.62rem;">{ts_str}</td>'
            f'<td style="font-weight:700;color:{T["text"]};">{sym}</td>'
            f'<td style="color:{sc};font-weight:700;">{sig}</td>'
            f'<td><span class="ot-badge" style="color:{gc};background:{gc}18;'
            f'border:1px solid {gc}40;">{_esc(guard)}</span></td>'
            f'</tr>'
        )
    hdr = "".join(f'<th>{h}</th>' for h in ["TIME", "SYM", "SIGNAL", "VETO REASON"])
    st.markdown(
        f'<div class="ot-tile ot-scroll" style="padding:0;">'
        f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
        f'<tbody>{rows}</tbody></table></div>',
        unsafe_allow_html=True,
    )
    st.caption(f"Showing last {min(len(logs),12)} of {len(logs)} · {VETO_AUDIT_PATH.name}")


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: DECISION TRACE  [TRACK09-DT]
# ══════════════════════════════════════════════════════════════════════════════

# Gate-name → display label and colour for the decision-trace panel.
# Covers all gate_name values emitted by _append_decision_trace in executor.py.
_DT_GATE_LABEL: dict[str, str] = {
    "PASS":             "ADMITTED",
    "SHADOW":           "SHADOW",
    "IN_FLIGHT":        "IN-FLIGHT",
    "SYMBOL_BLOCKED":   "SYM-BLOCK",
    "REGIME_BLOCKED":   "REGIME",
    "METADATA":         "METADATA",
    "SPREAD":           "SPREAD",
    "BRIDGE_DISCONNECTED": "BRIDGE",
    "BRIDGE_GHOST_EQUITY": "GHOST-EQ",
    "BRIDGE_STALE":     "STALE-EQ",
    "CONF":             "CONF-HARD",
    "EARLY_FLUSH":      "FLUSH-CD",
    "EDGE":             "EDGE",
    "GATED_ENTER":      "GATED",
    "OBI":              "OBI",
    "LLM_CATASTROPHE":  "CATASTRO",
    "LLM_VETO":         "LLM-VETO",
    "SNIPER_ONLY":      "SNIPER",
    "VETO_ARB":         "VETO-ARB",
    "CONF_FLOOR":       "CONF-FLOOR",
    "CORR_FREEZE":      "CORR",
    "CHOP_SNIPER_ONLY": "CHOP-SNP",
    "KELLY_ZERO":       "KELLY-0",
    "AVAIL_CAP":        "AVAIL-CAP",
    "CHOP_SHORT":       "CHOP-SHT",
    # [TRACK16-DT] Express-path gate names
    "ZOMBIE":              "ZOMBIE",
    "DRAWDOWN_KILLED":     "DD-KILL",
    "CIRCUIT_BREAKER":     "CIRC-BRK",
    "VOL_GUARD":           "VOL-GUARD",
    "SYMBOL_NOT_TRACKED":  "SYM-TRACK",
    "MACRO_FLOOR":         "MACRO-FLR",
    "EXPRESS_DEDUPE":      "DEDUPE",
    "ALREADY_OPEN":        "OPEN-POS",
    # [TRACK16-DT] Admission gate names also used by express path
    "CONCURRENT_CAP":  "CONC-CAP",
    "DEPLOYED_CAP":    "DEPL-CAP",
    # [TRACK17-DT1] ExitReason values — gate_name for close trace records
    "STRATEGY_TRAIL":           "TRAIL-STP",
    "STRATEGY_STOP":            "STOP",
    "STRATEGY_DEAD_ALPHA":      "DEAD-α",
    "STRATEGY_FRONTRUN_WALL":   "WALL-EXIT",
    "STRATEGY_HEDGE_CLOSE":     "HEDGE-CLS",
    "RISK_CIRCUIT_BREAKER":     "CB-EXIT",
    "RISK_DRAWDOWN_ZOMBIE":     "ZOMBIE-DD",
    "RISK_GLOBAL_DRAWDOWN":     "GLOB-DD",
    "ANOMALY_CATASTROPHE_VETO": "CATASTRO",
    "ANOMALY_VPIN_TOXIC":       "VPIN-FLSH",
    "ANOMALY_FUNDING_BRAKE":    "FUND-BRK",
    "MANUAL_CLOSE":             "MANUAL",
    "RECONCILIATION_CLOSE":     "RECON",
    "STRATEGY_DCA_ADD":         "DCA-ADD",
}

_DT_GATE_COLOUR: dict[str, str] = {
    "PASS":             T["green"],
    "SHADOW":           T["blue"],
    "CONF_FLOOR":       T["yellow"],
    "CONF":             T["yellow"],
    "LLM_VETO":         T["orange"],
    "LLM_CATASTROPHE":  T["red"],
    "VETO_ARB":         T["purple"],
    "SNIPER_ONLY":      T["cyan"],
    "OBI":              T["orange"],
    "SPREAD":           T["yellow"],
    "CORR_FREEZE":      T["blue"],
    "CHOP_SNIPER_ONLY": T["muted"],
    "CHOP_SHORT":       T["muted"],
    "KELLY_ZERO":       T["muted"],
    "AVAIL_CAP":        T["orange"],
    "REGIME_BLOCKED":   T["orange"],
    "SYMBOL_BLOCKED":   T["red"],
    "BRIDGE_DISCONNECTED": T["red"],
    "BRIDGE_GHOST_EQUITY": T["red"],
    "BRIDGE_STALE":     T["yellow"],
    "METADATA":         T["yellow"],
    "EARLY_FLUSH":      T["orange"],
    "GATED_ENTER":      T["muted"],
    "IN_FLIGHT":        T["muted"],
    "EDGE":             T["muted"],
    # [TRACK16-DT] Express-path gate colours
    "ZOMBIE":              T["red"],
    "DRAWDOWN_KILLED":     T["red"],
    "CIRCUIT_BREAKER":     T["red"],
    "VOL_GUARD":           T["orange"],
    "SYMBOL_NOT_TRACKED":  T["muted"],
    "MACRO_FLOOR":         T["orange"],
    "EXPRESS_DEDUPE":      T["muted"],
    "ALREADY_OPEN":        T["muted"],
    "CONCURRENT_CAP":      T["orange"],
    "DEPLOYED_CAP":        T["orange"],
    # [TRACK17-DT2] ExitReason colours — STRATEGY=green, RISK=orange,
    # ANOMALY=red, MANUAL/RECONCILIATION=muted
    "STRATEGY_TRAIL":           T["green"],
    "STRATEGY_STOP":            T["green"],
    "STRATEGY_DEAD_ALPHA":      T["green"],
    "STRATEGY_FRONTRUN_WALL":   T["green"],
    "STRATEGY_HEDGE_CLOSE":     T["green"],
    "RISK_CIRCUIT_BREAKER":     T["orange"],
    "RISK_DRAWDOWN_ZOMBIE":     T["orange"],
    "RISK_GLOBAL_DRAWDOWN":     T["orange"],
    "ANOMALY_CATASTROPHE_VETO": T["red"],
    "ANOMALY_VPIN_TOXIC":       T["red"],
    "ANOMALY_FUNDING_BRAKE":    T["red"],
    "MANUAL_CLOSE":             T["muted"],
    "RECONCILIATION_CLOSE":     T["muted"],
    "STRATEGY_DCA_ADD":         T["blue"],
}


@st.fragment(run_every=FRAG_MED)
def render_decision_trace() -> None:
    """[TRACK09-DT] Decision-trace panel — all admission outcomes from _maybe_enter.

    Shows the most recent decisions (ADMITTED / BLOCKED / SHADOW) from the
    decision_trace.json artifact written by executor.py.  Complements the
    existing veto_audit panel (P24) by covering ALL signal-evaluation outcomes,
    not just selected vetoed paths.

    Reads decision_trace.json with last-known-good (LKG) pattern.
    Degrades to a placeholder tile on missing file or empty artifact.
    No DB queries.  No schema dependencies.  No live state writes.
    """
    _section("DECISION TRACE — ALL ADMISSION OUTCOMES", "🔍")
    traces = load_decision_trace()
    if not traces:
        _slot_placeholder("✓ NO DECISIONS YET — waiting for first signal evaluation", 60)
        return

    # Show most recent entries first, capped at 20 rows.
    recent = list(reversed(traces))[:20]
    rows = ""
    for entry in recent:
        if not isinstance(entry, dict):
            continue
        ts_raw    = entry.get("ts") or 0.0
        sym       = _esc(_norm_sym(str(entry.get("symbol") or "—")))
        direction = str(entry.get("direction") or "").upper()
        regime    = _esc(str(entry.get("regime") or "—").lower())
        conf      = entry.get("confidence")
        outcome   = str(entry.get("outcome") or "").upper()
        gate_raw  = str(entry.get("gate_name") or "")
        reason    = _esc(str(entry.get("reason") or "")[:60])
        llm_v     = str(entry.get("llm_verdict") or "")
        sizing    = entry.get("sizing_usd")

        # Timestamp
        ts_str = "—"
        try:
            ts_str = datetime.fromtimestamp(float(ts_raw)).strftime("%H:%M:%S")
        except (ValueError, TypeError, OSError):
            pass

        # Direction colour
        dir_col = T["green"] if direction == "LONG" else T["red"] if direction == "SHORT" else T["text2"]

        # Outcome + gate badge
        gate_label  = _DT_GATE_LABEL.get(gate_raw, gate_raw[:10] or "—")
        gate_colour = _DT_GATE_COLOUR.get(gate_raw, T["text2"])
        if outcome == "ADMITTED":
            out_col = T["green"]
        elif outcome == "SHADOW":
            out_col = T["blue"]
        else:
            out_col = T["red"]

        # Confidence display
        conf_str = f"{conf:.3f}" if isinstance(conf, (int, float)) else "—"

        # Sizing display (admitted only)
        size_str = f"${sizing:.0f}" if isinstance(sizing, (int, float)) and sizing else "—"

        # LLM verdict indicator (compact)
        llm_str = ""
        if llm_v and llm_v not in ("PASS", ""):
            lc = T["red"] if "VETO" in llm_v or "CATASTROPHE" in llm_v else T["yellow"]
            llm_str = (
                f'<span style="color:{lc};font-size:0.58rem;margin-left:3px;">'
                f'{_esc(llm_v[:12])}</span>'
            )

        rows += (
            f'<tr>'
            f'<td style="color:{T["text2"]};font-size:0.60rem;">{ts_str}</td>'
            f'<td style="font-weight:700;color:{T["text"]};">{sym}</td>'
            f'<td style="color:{dir_col};font-weight:700;font-size:0.65rem;">'
            f'{_esc(direction[:5])}</td>'
            f'<td style="color:{T["text2"]};font-size:0.62rem;">{regime}</td>'
            f'<td style="color:{T["text2"]};font-size:0.62rem;">{conf_str}</td>'
            f'<td>'
            f'<span style="color:{out_col};font-weight:700;font-size:0.62rem;">'
            f'{_esc(outcome[:8])}</span>'
            f'<span class="ot-badge" style="color:{gate_colour};background:{gate_colour}18;'
            f'border:1px solid {gate_colour}40;margin-left:4px;">'
            f'{_esc(gate_label)}</span>'
            f'{llm_str}</td>'
            f'<td style="color:{T["muted"]};font-size:0.58rem;">{size_str}</td>'
            f'<td style="color:{T["text2"]};font-size:0.58rem;max-width:160px;'
            f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{reason}</td>'
            f'</tr>'
        )

    hdr = "".join(
        f'<th>{h}</th>'
        for h in ["TIME", "SYM", "DIR", "REGIME", "CONF", "OUTCOME / GATE", "SIZE", "REASON"]
    )
    st.markdown(
        f'<div class="ot-tile ot-scroll" style="padding:0;">'
        f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
        f'<tbody>{rows}</tbody></table></div>',
        unsafe_allow_html=True,
    )
    st.caption(
        f"Showing last {min(len(traces), 20)} of {len(traces)} decisions · "
        f"{DECISION_TRACE_PATH.name}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: HISTORICAL DECISION-TRACE (DB-BACKED)  [TRACK10-DT]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_decision_trace_history() -> None:
    """[TRACK10-DT2] Historical decision-trace panel — DB-backed cross-session view.

    Reads from the decision_trace SQLite table written by Track 10.
    Provides an aggregate funnel summary and a filterable record table.
    Additive to the Track 09 live JSON panel — does not replace it.

    [TRACK16-DT4] Source filter selectbox (ALL / ENTRY / EXPRESS) added.
    [TRACK17-DT4] Outcome filter gains CLOSED / DCA.
                  Source filter gains CLOSE / DCA.
                  Summary tile shows CLOSED and DCA counts.
                  SRC badge handles "close" → CLO (yellow) and "dca" → DCA (blue).
                  PNL column added: pnl_pct for close rows, "—" otherwise.
                  Degrades cleanly on pre-Track-17 DBs (pnl column absent → "—").
    [TRACK18-CORR2] Blocked/shadow → later-admitted → closed correlation expander
                  added after the record table.  Shown only when the filtered
                  DataFrame contains at least one BLOCKED or SHADOW row.
                  Calls _load_later_admitted_and_outcome() for the selected row.
                  Degrades cleanly on pre-Track-16/17 DBs.

    Degrades to a placeholder when:
      - the decision_trace table does not exist (pre-Track-10 DB or cold start)
      - no rows match the current filters
    No DB writes.  No schema side effects.  No joins with other tables.
    """
    _section("DECISION TRACE — HISTORICAL (DB)", "🗄️")

    tables = _get_known_tables()
    if "decision_trace" not in tables:
        _slot_placeholder("✓ DECISION TRACE TABLE NOT YET CREATED — start the bot to populate", 60)
        return

    # ── Filter controls ───────────────────────────────────────────────────────
    # [TRACK16-DT4] Four columns: symbol / outcome / source / days-back.
    _c1, _c2, _c3, _c4 = st.columns([2, 2, 1, 1])
    with _c1:
        try:
            sym_df = _query(
                "SELECT DISTINCT symbol FROM decision_trace ORDER BY symbol LIMIT 50"
            )
            sym_opts = ["ALL"] + (sym_df["symbol"].tolist() if not sym_df.empty else [])
        except Exception:
            sym_opts = ["ALL"]
        symbol_filter = st.selectbox(
            "Symbol", sym_opts, key="_dth_sym", label_visibility="collapsed"
        )
    with _c2:
        # [TRACK17-DT4] Outcome filter includes CLOSED and DCA.
        outcome_filter = st.selectbox(
            "Outcome", ["ALL", "ADMITTED", "BLOCKED", "SHADOW", "CLOSED", "DCA"],
            key="_dth_outcome", label_visibility="collapsed"
        )
    with _c3:
        # [TRACK17-DT4] Source filter includes CLOSE and DCA.
        source_filter = st.selectbox(
            "Source", ["ALL", "ENTRY", "EXPRESS", "CLOSE", "DCA"],
            key="_dth_src", label_visibility="collapsed"
        )
    with _c4:
        days_back = st.slider(
            "Days", min_value=1, max_value=30, value=7,
            key="_dth_days", label_visibility="collapsed"
        )

    df = _load_decision_trace_history(
        days_back=days_back,
        symbol_filter=symbol_filter,
        outcome_filter=outcome_filter,
        source_filter=source_filter,
    )

    if df.empty:
        _slot_placeholder("✓ NO DECISION TRACE DATA — adjust filters or wait for signals", 60)
        return

    total = len(df)
    # [TRACK16-DT4] Detect whether source_path column is present.
    _has_src = "source_path" in df.columns
    # [TRACK17-DT4] Detect whether pnl_pct column is present.
    _has_pnl = "pnl_pct" in df.columns

    # ── Aggregate summary ─────────────────────────────────────────────────────
    n_admitted = int((df["outcome"] == "ADMITTED").sum())
    n_blocked  = int((df["outcome"] == "BLOCKED").sum())
    n_shadow   = int((df["outcome"] == "SHADOW").sum())
    # [TRACK17-DT4] Lifecycle outcome counts.
    n_closed   = int((df["outcome"] == "CLOSED").sum())
    n_dca      = int((df["outcome"] == "DCA").sum())

    def _pct(n: int) -> str:
        return f"{n / total * 100:.1f}%" if total > 0 else "—"

    gate_counts: dict = {}
    for gn in df["gate_name"].dropna():
        gate_counts[gn] = gate_counts.get(gn, 0) + 1
    top_gates = sorted(gate_counts.items(), key=lambda x: x[1], reverse=True)[:5]

    summary_html = (
        f'<div class="ot-tile" style="padding:8px 12px;margin-bottom:6px;">'
        f'<span style="color:{T["text2"]};font-size:0.62rem;">LAST {days_back}d · {total} decisions &nbsp;|&nbsp; </span>'
        f'<span style="color:{T["green"]};font-size:0.62rem;font-weight:700;">'
        f'✓ ADMITTED {n_admitted} ({_pct(n_admitted)}) &nbsp;</span>'
        f'<span style="color:{T["red"]};font-size:0.62rem;font-weight:700;">'
        f'✗ BLOCKED {n_blocked} ({_pct(n_blocked)}) &nbsp;</span>'
        f'<span style="color:{T["blue"]};font-size:0.62rem;font-weight:700;">'
        f'◈ SHADOW {n_shadow} ({_pct(n_shadow)})'
    )
    # [TRACK17-DT4] Append CLOSED and DCA counts when non-zero.
    if n_closed:
        summary_html += (
            f' &nbsp;</span>'
            f'<span style="color:{T["yellow"]};font-size:0.62rem;font-weight:700;">'
            f'⊠ CLOSED {n_closed} ({_pct(n_closed)})'
        )
    if n_dca:
        summary_html += (
            f' &nbsp;</span>'
            f'<span style="color:{T["blue"]};font-size:0.62rem;">'
            f'↑ DCA {n_dca} ({_pct(n_dca)})'
        )
    summary_html += f'</span></div>'

    if top_gates:
        gate_parts = []
        for gn, cnt in top_gates:
            lbl = _DT_GATE_LABEL.get(gn, gn[:12] or "—")
            col = _DT_GATE_COLOUR.get(gn, T["text2"])
            gate_parts.append(
                f'<span style="color:{col};font-size:0.60rem;">'
                f'{_esc(lbl)}&thinsp;<span style="color:{T["muted"]};">{cnt}</span></span>'
            )
        summary_html += (
            f'<div class="ot-tile" style="padding:4px 12px;margin-bottom:6px;">'
            f'<span style="color:{T["muted"]};font-size:0.58rem;">TOP GATES: </span>'
            + ' &nbsp;·&nbsp; '.join(gate_parts)
            + '</div>'
        )
    st.markdown(summary_html, unsafe_allow_html=True)

    # ── Record table ──────────────────────────────────────────────────────────
    display_df = df.head(200)
    rows = ""
    for _, row in display_df.iterrows():
        ts_raw    = row.get("ts") or 0.0
        sym       = _esc(_norm_sym(str(row.get("symbol") or "—")))
        direction = str(row.get("direction") or "").upper()
        regime    = _esc(str(row.get("regime") or "—").lower())
        conf      = row.get("confidence")
        outcome   = str(row.get("outcome") or "").upper()
        gate_raw  = str(row.get("gate_name") or "")
        reason    = _esc(str(row.get("reason") or "")[:60])
        llm_v     = str(row.get("llm_verdict") or "")
        sizing    = row.get("sizing_usd")
        # [TRACK16-DT4] source_path — "—" when column absent (pre-Track-16 DB).
        src_raw   = str(row.get("source_path") or "") if _has_src else ""
        # [TRACK17-DT4] pnl_pct — None when column absent (pre-Track-17 DB).
        pnl_val   = row.get("pnl_pct") if _has_pnl else None

        ts_str = "—"
        try:
            ts_str = datetime.fromtimestamp(float(ts_raw)).strftime("%m-%d %H:%M")
        except (ValueError, TypeError, OSError):
            pass

        dir_col     = T["green"] if direction == "LONG" else T["red"] if direction == "SHORT" else T["text2"]
        gate_label  = _DT_GATE_LABEL.get(gate_raw, gate_raw[:10] or "—")
        gate_colour = _DT_GATE_COLOUR.get(gate_raw, T["text2"])
        if outcome == "ADMITTED":
            out_col = T["green"]
        elif outcome == "SHADOW":
            out_col = T["blue"]
        elif outcome == "CLOSED":
            out_col = T["yellow"]
        elif outcome == "DCA":
            out_col = T["blue"]
        else:
            out_col = T["red"]

        conf_str = f"{conf:.3f}" if isinstance(conf, (int, float)) else "—"
        size_str = f"${sizing:.0f}" if isinstance(sizing, (int, float)) and sizing else "—"

        llm_str = ""
        if llm_v and llm_v not in ("PASS", ""):
            lc = T["red"] if "VETO" in llm_v or "CATASTROPHE" in llm_v else T["yellow"]
            llm_str = (
                f'<span style="color:{lc};font-size:0.58rem;margin-left:3px;">'
                f'{_esc(llm_v[:12])}</span>'
            )

        # [TRACK17-DT4] SRC badge — CLO (yellow) for close, DCA (blue) for dca,
        # EXP (cyan) for express, ENT (muted) for entry, — when absent.
        if src_raw == "close":
            src_col = T["yellow"]
            src_lbl = "CLO"
        elif src_raw == "dca":
            src_col = T["blue"]
            src_lbl = "DCA"
        elif src_raw == "express":
            src_col = T["cyan"]
            src_lbl = "EXP"
        elif src_raw == "entry":
            src_col = T["muted"]
            src_lbl = "ENT"
        else:
            src_col = T["muted"]
            src_lbl = "—"

        # [TRACK17-DT4] PNL cell — only populated for close rows.
        if pnl_val is not None and isinstance(pnl_val, (int, float)):
            _pnl_col = T["green"] if pnl_val >= 0 else T["red"]
            pnl_str  = f'<span style="color:{_pnl_col};font-weight:700;">{pnl_val:+.2f}%</span>'
        else:
            pnl_str  = f'<span style="color:{T["muted"]};">—</span>'

        rows += (
            f'<tr>'
            f'<td style="color:{T["text2"]};font-size:0.58rem;white-space:nowrap;">{ts_str}</td>'
            f'<td style="font-weight:700;color:{T["text"]};">{sym}</td>'
            f'<td style="color:{dir_col};font-weight:700;font-size:0.65rem;">{_esc(direction[:5])}</td>'
            f'<td style="color:{T["text2"]};font-size:0.62rem;">{regime}</td>'
            f'<td style="color:{T["text2"]};font-size:0.62rem;">{conf_str}</td>'
            f'<td>'
            f'<span style="color:{out_col};font-weight:700;font-size:0.62rem;">{_esc(outcome[:8])}</span>'
            f'<span class="ot-badge" style="color:{gate_colour};background:{gate_colour}18;'
            f'border:1px solid {gate_colour}40;margin-left:4px;">{_esc(gate_label)}</span>'
            f'{llm_str}</td>'
            f'<td style="color:{T["muted"]};font-size:0.58rem;">{size_str}</td>'
            f'<td style="color:{src_col};font-size:0.58rem;font-weight:600;">{_esc(src_lbl)}</td>'
            f'<td style="font-size:0.58rem;">{pnl_str}</td>'
            f'<td style="color:{T["text2"]};font-size:0.58rem;max-width:140px;'
            f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{reason}</td>'
            f'</tr>'
        )

    # [TRACK17-DT4] PNL column added to header.
    hdr = "".join(
        f'<th>{h}</th>'
        for h in ["TIME", "SYM", "DIR", "REGIME", "CONF", "OUTCOME / GATE", "SIZE", "SRC", "PNL", "REASON"]
    )
    st.markdown(
        f'<div class="ot-tile ot-scroll" style="padding:0;">'
        f'<table class="ot-table"><thead><tr>{hdr}</tr></thead>'
        f'<tbody>{rows}</tbody></table></div>',
        unsafe_allow_html=True,
    )
    st.caption(
        f"Showing {len(display_df)} of {total} matching · last {days_back}d · powertrader.db"
    )

    # ── [TRACK18-CORR2] Blocked/shadow → later-admitted → closed expander ────
    # Build the pool of selectable BLOCKED/SHADOW rows from the FULL loaded df
    # (not just display_df) so the selector is not limited by the 200-row cap.
    # When source_path is absent (pre-Track-16 DB), all BLOCKED/SHADOW rows are
    # included; when present, filter to entry/express source paths only so that
    # structural CLOSED/DCA rows are never confused for blocked admission attempts.
    _corr_pool = df[df["outcome"].isin(["BLOCKED", "SHADOW"])].copy()
    if _has_src and "source_path" in _corr_pool.columns:
        _corr_pool = _corr_pool[
            _corr_pool["source_path"].fillna("entry").isin(["entry", "express"])
        ]

    if not _corr_pool.empty:
        with st.expander(
            "🔗 Blocked/shadow → later admitted → closed correlation — select a row",
            expanded=False,
        ):
            st.markdown(
                f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
                f'color:{T["muted"]};margin-bottom:6px;">'
                f'For the selected BLOCKED or SHADOW decision, searches forward '
                f'{_CORR18_ADMITTED_WINDOW_SECS}s for a later ADMITTED decision '
                f'on the same symbol and direction, then up to '
                f'{_CORR18_CLOSE_WINDOW_SECS // 3600}h for the resulting close. '
                f'Exact symbol + direction + time-window match — no fuzzy heuristics. '
                f'Read-only.</div>',
                unsafe_allow_html=True,
            )

            # Build label → (symbol, direction, ts, gate) map.
            _corr_opts:  list[str]   = []
            _corr_meta:  list[tuple] = []   # (symbol, direction, ts, gate_raw)
            for _, _cr in _corr_pool.head(100).iterrows():
                _cr_sym = str(_cr.get("symbol") or "").strip()
                _cr_dir = str(_cr.get("direction") or "").strip()
                _cr_ts  = _cr.get("ts")
                _cr_out = str(_cr.get("outcome") or "BLOCKED").upper()
                _cr_gn  = str(_cr.get("gate_name") or "")
                if not _cr_sym or not _cr_ts:
                    continue
                try:
                    _cr_ts_f = float(_cr_ts)
                except (TypeError, ValueError):
                    continue
                try:
                    _cr_dt = datetime.fromtimestamp(_cr_ts_f).strftime("%m-%d %H:%M")
                except (ValueError, OSError):
                    _cr_dt = "?"
                _lbl = (
                    f"{_cr_out[:6]} {_norm_sym(_cr_sym)} {_cr_dir[:5]} "
                    f"@ {_cr_dt} [{_DT_GATE_LABEL.get(_cr_gn, _cr_gn[:10] or '—')}]"
                )
                _corr_opts.append(_lbl)
                _corr_meta.append((_cr_sym, _cr_dir, _cr_ts_f, _cr_gn))

            if not _corr_opts:
                st.markdown(
                    f'<div class="ot-tile" style="padding:8px;text-align:center;">'
                    f'<span style="color:{T["muted"]};font-family:{T["mono"]};'
                    f'font-size:0.65rem;">No selectable rows in current filter window.</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            else:
                _corr_sel = st.selectbox(
                    "Blocked/shadow row",
                    options=_corr_opts,
                    index=0,
                    key="_dt_corr_sel",
                    label_visibility="collapsed",
                )
                try:
                    _ci = _corr_opts.index(_corr_sel)
                    _c_sym, _c_dir, _c_ts, _c_gn = _corr_meta[_ci]
                except (ValueError, IndexError):
                    _c_sym, _c_dir, _c_ts, _c_gn = "", "", 0.0, ""

                if _c_sym and _c_ts:
                    _corr = _load_later_admitted_and_outcome(
                        symbol=_c_sym,
                        direction=_c_dir,
                        blocked_ts=_c_ts,
                    )
                    if _corr.get("admitted_found"):
                        # ── Admitted: show latency, source, gate, PnL, hold ──
                        _c_lat   = _corr.get("latency_secs")
                        _c_asrc  = str(_corr.get("admitted_source") or "")
                        _c_agn   = str(_corr.get("admitted_gate") or "")
                        _c_pnl   = _corr.get("pnl_pct")
                        _c_hld   = _corr.get("hold_time_secs")
                        _c_cgn   = str(_corr.get("close_gate") or "")

                        _lat_s   = f"{_c_lat:.0f}s" if _c_lat is not None else "?"
                        _src_lbl = {"entry": "ENT", "express": "EXP"}.get(_c_asrc, _c_asrc[:4] or "?")
                        _agn_lbl = _DT_GATE_LABEL.get(_c_agn, _c_agn[:12] or "PASS")
                        _agn_col = _DT_GATE_COLOUR.get(_c_agn, T["green"])

                        if _c_pnl is not None:
                            _pnl_col = T["green"] if _c_pnl >= 0 else T["red"]
                            _pnl_s   = f'<span style="color:{_pnl_col};font-weight:700;">{_c_pnl:+.2f}%</span>'
                        else:
                            _pnl_s   = f'<span style="color:{T["muted"]};">—</span>'

                        if _c_hld is not None:
                            _m, _s = divmod(int(_c_hld), 60)
                            _hld_s = f"{_m}m{_s:02d}s" if _m else f"{_s}s"
                        else:
                            _hld_s = "—"

                        if _corr.get("close_found"):
                            _cgn_lbl = _DT_GATE_LABEL.get(_c_cgn, _c_cgn[:14] or "—")
                            _close_s = (
                                f'<span style="color:{T["yellow"]};font-size:0.60rem;">'
                                f'⊠ CLOSED via {_esc(_cgn_lbl)} · '
                                f'PnL {_pnl_s} · hold {_esc(_hld_s)}'
                                f'</span>'
                            )
                        else:
                            _close_s = (
                                f'<span style="color:{T["muted"]};font-size:0.60rem;">'
                                f'close not found in {_CORR18_CLOSE_WINDOW_SECS // 3600}h window'
                                f'</span>'
                            )

                        _result_html = (
                            f'<div class="ot-tile" style="padding:8px 12px;">'
                            f'<span style="color:{T["green"]};font-size:0.62rem;font-weight:700;">'
                            f'✓ ADMITTED +{_lat_s}</span>'
                            f'<span style="color:{T["muted"]};font-size:0.58rem;"> · src={_esc(_src_lbl)}</span>'
                            f'<span class="ot-badge" style="color:{_agn_col};'
                            f'background:{_agn_col}18;border:1px solid {_agn_col}40;'
                            f'margin-left:4px;">{_esc(_agn_lbl)}</span>'
                            f'<br><span style="margin-left:2px;">{_close_s}</span>'
                            f'</div>'
                        )
                        st.markdown(_result_html, unsafe_allow_html=True)

                    else:
                        # ── Not admitted within look-ahead window ─────────────
                        st.markdown(
                            f'<div class="ot-tile" style="padding:8px 12px;">'
                            f'<span style="color:{T["muted"]};font-family:{T["mono"]};'
                            f'font-size:0.62rem;">'
                            f'— No admission found within {_CORR18_ADMITTED_WINDOW_SECS}s '
                            f'for {_esc(_norm_sym(_c_sym))} {_esc(_c_dir)}'
                            f'</span></div>',
                            unsafe_allow_html=True,
                        )
    # ── [/TRACK18-CORR2] ─────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_flash_feed() -> None:
    """[FIX-PERF-01] Now a @st.fragment — no longer causes full-page rerun cost."""
    status   = _get_status()
    p17      = status.get("p17_intelligence", {})
    recents  = p17.get("recent_results", []) if isinstance(p17, dict) else []
    nw       = (status.get("agents") or {}).get("news_wire", {})
    headlines= nw.get("top_headlines", []) if isinstance(nw, dict) else []
    p22      = status.get("p22", {})
    pl       = p22.get("panic_lock", {}) if isinstance(p22, dict) else {}
    p21      = status.get("p21_execution", {})
    sg_d     = p21.get("p21_slippage_guard", {}) if isinstance(p21, dict) else {}
    throttled= sg_d.get("throttled_now", {}) if isinstance(sg_d, dict) else {}
    p32      = _esc(str(status.get("p32_aggression_mode", "—")))

    ticker_items: list[str] = []

    if isinstance(pl, dict) and pl.get("locked"):
        rem = _safe_float(pl.get("remaining_secs"))
        ticker_items.append(
            f'<div class="ot-tick-item ot-tick-red">'
            f'<span>🚨 PANIC LOCK ACTIVE</span><span>BUY ENTRIES BLOCKED</span>'
            f'<span style="color:{T["muted"]};">{rem:.0f}s</span></div>'
        )

    if isinstance(throttled, dict):
        for sym, thr in throttled.items():
            if thr:
                sp = _safe_float(sg_d.get("latest_spreads", {}).get(sym))
                ticker_items.append(
                    f'<div class="ot-tick-item ot-tick-red">'
                    f'<span>⚡ [{_esc(sym)}]</span><span>SPREAD THROTTLED</span>'
                    f'<span style="color:{T["muted"]};">{sp:.5f}%</span></div>'
                )

    if p32 not in ("—", "NORMAL"):
        ticker_items.append(
            f'<div class="ot-tick-item ot-tick-yellow">'
            f'<span>🔱 P32 AGG</span><span>{p32}</span></div>'
        )

    for r in recents[:8]:
        if not isinstance(r, dict): continue
        sym2    = _esc(str(r.get("symbol", "?")))
        verdict = r.get("verdict", "NEUTRAL")
        score   = r.get("score")
        sc_s    = f"{_safe_float(score):.3f}" if score is not None else "—"
        cls     = ("ot-tick-green" if verdict == "BOOST"
                   else "ot-tick-red" if "VETO" in verdict else "ot-tick-gray")
        icon    = VERDICT_STYLE.get(verdict, VERDICT_STYLE["N/A"])["icon"]
        ticker_items.append(
            f'<div class="ot-tick-item {cls}">'
            f'<span>{icon} [{sym2}]</span><span>{_esc(verdict)}</span>'
            f'<span style="color:{T["muted"]};">{sc_s}</span></div>'
        )

    for hl in headlines[:4]:
        if not isinstance(hl, str): continue
        hl_l = hl.lower()
        if any(k in hl_l for k in ("rally", "surge", "bullish", "approval", "breakout")):
            cls, icon = "ot-tick-green", "▲"
        elif any(k in hl_l for k in ("ban", "hack", "crash", "fear", "lawsuit", "bearish")):
            cls, icon = "ot-tick-red", "▼"
        else:
            cls, icon = "ot-tick-gray", "─"
        safe_hl = _esc(hl[:80]) + ("…" if len(hl) > 80 else "")
        ticker_items.append(
            f'<div class="ot-tick-item {cls}"><span>{icon}</span><span>{safe_hl}</span></div>'
        )

    if not ticker_items:
        ticker_items = [
            f'<div class="ot-tick-item ot-tick-gray"><span>── Awaiting intelligence data ──</span></div>'
        ]

    items_html = "".join(ticker_items)
    st.markdown(
        f'<div class="ot-ticker-wrap">'
        f'<div class="ot-ticker-inner">{items_html}{items_html}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: LIQUIDATION HEAT-MAP
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_liq_heatmap() -> None:
    status = _get_status()
    _section("LIQUIDATION CLUSTER HEAT-MAP — STOP-RUN ZONES", "💥")

    mkt      = status.get("market_data", {})
    liq_raw  = mkt.get("liquidations", {}) if isinstance(mkt, dict) else {}
    spot     = _safe_float((liq_raw.get("spot_price") if isinstance(liq_raw, dict) else None))
    clusters = liq_raw.get("clusters", []) if isinstance(liq_raw, dict) else []

    if not clusters or not isinstance(clusters, list):
        _slot_placeholder("NO LIQUIDATION CLUSTER DATA", 300)
        return

    parsed: list[dict] = []
    for c in clusters:
        try:
            p = _safe_float(c.get("price"))
            v = _safe_float(c.get("size_usd") or c.get("volume") or
                            c.get("notional") or c.get("size"))
            s = str(c.get("side", "long")).lower()
            s = "short" if any(k in s for k in ("short", "sell", "ask")) else "long"
            if p > 0 and v > 0:
                parsed.append({"price": p, "size_usd": v, "side": s})
        except (TypeError, ValueError):
            continue

    if not parsed:
        _slot_placeholder("NO PARSED CLUSTER DATA", 300)
        return

    parsed.sort(key=lambda x: x["price"])
    if spot <= 0.0:
        spot = float(np.median([c["price"] for c in parsed]))

    max_v = max(c["size_usd"] for c in parsed) or 1.0
    fig   = go.Figure()

    for s_type, label, rgb in (
        ("long",  "Long Liq (↓ stop-run)",  "255,23,68"),
        ("short", "Short Liq (↑ stop-run)", "68,138,255"),
    ):
        sub = [c for c in parsed if c["side"] == s_type]
        if sub:
            colors = [f"rgba({rgb},{0.35+0.65*c['size_usd']/max_v:.2f})" for c in sub]
            fig.add_trace(go.Bar(
                y=[c["price"]    for c in sub],
                x=[c["size_usd"] for c in sub],
                orientation="h", name=label,
                marker=dict(color=colors, line=dict(width=0)),
                hovertemplate="$%{y:,.2f}<br>$%{x:,.0f}<extra></extra>",
            ))

    if spot > 0:
        fig.add_hline(y=spot, line_color=T["yellow"], line_width=1.5, line_dash="dot",
                      annotation_text=f"  SPOT ${spot:,.2f}",
                      annotation_position="right",
                      annotation_font=dict(family=_FONT_MONO_PLOTLY, size=9, color=T["yellow"]))

    n_bars = len(parsed)
    fig.update_layout(
        height=max(300, n_bars * 24), margin=dict(l=8, r=80, t=24, b=24),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        barmode="overlay", showlegend=True,
        font=dict(color=T["text"], family=_FONT_MONO_PLOTLY, size=9),
        xaxis=dict(gridcolor=T["border"], tickformat="$,.0f",
                   tickfont=dict(color=T["text2"])),
        yaxis=dict(gridcolor=T["border"], tickformat="$,.2f", autorange="reversed",
                   tickfont=dict(color=T["text2"])),
        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center",
                    font=dict(size=9)),
    )

    total_long  = sum(c["size_usd"] for c in parsed if c["side"] == "long")
    total_short = sum(c["size_usd"] for c in parsed if c["side"] == "short")
    max_c       = max(parsed, key=lambda x: x["size_usd"])

    col_chart, col_stat = st.columns([3, 1])
    with col_chart:
        _paint_fig(st.empty(), fig, _FIG_KEY_LIQ,
                   max(300, n_bars * 24), "AWAITING LIQUIDATIONS", "ot_liq_map")
    with col_stat:
        st.markdown(
            f'<div class="ot-tile">'
            f'<div class="ot-kpi-label">LONG LIQ</div>'
            f'<div class="ot-kpi-value" style="color:{T["red"]};">${total_long:,.0f}</div>'
            f'<div class="ot-kpi-label" style="margin-top:10px;">SHORT LIQ</div>'
            f'<div class="ot-kpi-value" style="color:{T["blue"]};">${total_short:,.0f}</div>'
            f'<hr style="border-color:{T["border"]};margin:8px 0;">'
            f'<div class="ot-kpi-label">LARGEST</div>'
            f'<div style="font-family:{T["mono"]};font-size:0.88rem;font-weight:700;'
            f'color:{T["yellow"]};">${max_c["size_usd"]:,.0f}</div>'
            f'<div style="font-family:{T["mono"]};font-size:0.65rem;color:{T["text2"]};">'
            f'@ ${max_c["price"]:,.2f}</div></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: CORRELATION MATRIX
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_correlation(symbols: list) -> None:
    status  = _get_status()
    _section("CROSS-ASSET CORRELATION MATRIX — 7d HOURLY", "🔗")
    corr_df = load_correlation_matrix(tuple(sorted(symbols)))
    frozen  = bool(status.get("correlation_frozen", False))

    col_heat, col_stat = st.columns([3, 1])
    with col_heat:
        fig_fresh = None
        if corr_df is not None:
            try:
                fig = go.Figure(go.Heatmap(
                    z=corr_df.values,
                    x=corr_df.columns.tolist(),
                    y=corr_df.index.tolist(),
                    colorscale=[[0, T["red"]], [0.5, "rgba(14,17,23,0.9)"], [1, T["green"]]],
                    zmin=-1, zmax=1,
                    text=corr_df.round(3).values,
                    texttemplate="%{text:.3f}",
                    textfont={"size": 12, "color": T["text"], "family": _FONT_MONO_PLOTLY},
                    xgap=2, ygap=2,
                ))
                _fig_base(fig, height=250)
                fig_fresh = fig
            except Exception:
                log.debug("render_correlation: heatmap build failed", exc_info=True)
        _paint_fig(st.empty(), fig_fresh, _FIG_KEY_CORR, 250,
                   "AWAITING CANDLE HISTORY", "ot_corr_heatmap")

    with col_stat:
        ep = bool(status.get("emergency_pause", False))
        _kpi("VOL GUARD", "⚠ PAUSED" if ep else "✓ ACTIVE", "",
             T["red"] if ep else T["green"])
        if corr_df is not None:
            mask  = corr_df.copy()
            np.fill_diagonal(mask.values, np.nan)
            avg_c = float(np.nanmean(mask.values))
            mx_c  = float(np.nanmax(np.abs(mask.fillna(0).values)))
            _kpi("AVG PAIRWISE", f"{avg_c:+.3f}", "",
                 T["red"] if abs(avg_c) > 0.7 else T["yellow"])
            _kpi("MAX |CORR|", f"{mx_c:.3f}", "",
                 T["red"] if mx_c > 0.95 else T["green"])
            if frozen:
                st.markdown(f'<div class="ot-alert-red">🧊 CORRELATION GUARD FROZEN</div>',
                            unsafe_allow_html=True)
            elif mx_c > 0.99:
                st.markdown(f'<div class="ot-alert-red">⚠ EXTREME CORRELATION {mx_c:.4f}</div>',
                            unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: REGIME PANEL
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_regime_panel() -> None:
    status = _get_status()
    _section("HMM REGIME IDENTIFICATION + RL WEIGHTS + P22 TRAIL ADJ", "🔬")

    positions   = status.get("positions", {})
    p17_rec_map = {r.get("symbol"): r for r in (
        status.get("p17_intelligence", {}).get("recent_results", [])
        if isinstance(status.get("p17_intelligence"), dict) else []
    )}
    coins = list(positions.keys()) if isinstance(positions, dict) else []

    SLOTS = 4
    cols  = st.columns(SLOTS)
    coins = coins[:SLOTS]

    for i in range(SLOTS):
        if i >= len(coins):
            with cols[i]: _slot_placeholder("REGIME SLOT EMPTY", 250)
            continue

        sym    = coins[i]
        pos    = positions.get(sym, {}) if isinstance(positions, dict) else {}
        regime = (pos.get("regime") or "chop").lower()
        rm     = REGIME_META.get(regime, REGIME_META["neutral"])
        drx    = _esc(str(pos.get("direction", "none")).upper())
        drx_c  = T["green"] if drx == "LONG" else T["red"] if drx == "SHORT" else T["muted"]
        conf   = _safe_float(pos.get("signal_conf"))
        mtf    = bool(pos.get("mtf_aligned", True))
        p17_r  = p17_rec_map.get(sym, {}) or {}
        trail_adj = p17_r.get("trailing_gap_adjustment")
        ta_s   = (
            f"+{float(trail_adj)*100:.3f}%" if trail_adj is not None and float(trail_adj) >= 0
            else (f"{float(trail_adj)*100:.3f}%" if trail_adj is not None else "—")
        )
        ta_c     = T["green"] if (trail_adj or 0) > 0 else T["yellow"] if (trail_adj or 0) < 0 else T["muted"]
        p18_conv = _safe_float(pos.get("p18_conviction_multiplier", 1.0), 1.0)
        p18_c    = T["green"] if p18_conv >= 1.2 else T["yellow"] if p18_conv >= 0.8 else T["red"]
        p13_trust= _safe_float(pos.get("p13_agg_trust", DEFAULT_TRUST), DEFAULT_TRUST)
        obi      = _safe_float(pos.get("p12_obi"))
        pnl      = pos.get("pnl_pct")
        # [FIX-FSTR-004] Python 3.11: no nested f-strings in f-expr
        _pnl2_f   = _safe_float(pnl)
        _pnl2_sgn = "+" if _pnl2_f >= 0 else ""
        _pnl2_str = f"{_pnl2_sgn}{_pnl2_f:.2f}%"
        _pnl2_col = "#00FF41" if _pnl2_f >= 0 else "#FF003C"
        pnl_html = (
            f'<span style="color:{_pnl2_col};'
            f'font-family:{T["mono"]};font-weight:700;">'
            f'{_pnl2_str}</span>'
            if pnl is not None else f'<span style="color:{T["muted"]};">—</span>'
        )

        row_data = [
            ("DIRECTION",   f'<span style="color:{drx_c};font-weight:700;">{drx}</span>'),
            ("LIVE PnL",    pnl_html),
            ("SIGNAL CONF", f'<span style="font-family:{T["mono"]};">{conf:.4f}</span>'),
            ("MTF",         f'<span style="color:{T["green"]};">✓ ALIGNED</span>'
                            if mtf else f'<span style="color:{T["red"]};">✗ CONFLICT</span>'),
            ("P18 CONV",    f'<span style="color:{p18_c};font-family:{T["mono"]};">{p18_conv:.4f}×</span>'),
            ("P13 TRUST",   f'<span class="{_trust_class(p13_trust)};font-family:{T["mono"]};">{p13_trust:.4f}</span>'),
            ("OBI",         f'<span style="color:{"#00FF41" if obi>0.3 else "#FF003C" if obi<-0.3 else T["text2"]};font-family:{T["mono"]};">{obi:+.4f}</span>'),
            ("P22 TRAIL",   f'<span style="color:{ta_c};font-family:{T["mono"]};">{ta_s}</span>'),
        ]
        rows_html = "".join(
            f'<div style="display:flex;justify-content:space-between;'
            f'border-bottom:1px solid {T["border"]};padding:3px 0;font-size:0.68rem;">'
            f'<span style="color:{T["muted"]};font-size:0.60rem;letter-spacing:0.08em;">{k}</span>'
            f'<span>{v}</span></div>'
            for k, v in row_data
        )
        with cols[i]:
            st.markdown(
                f'<div class="ot-tile" style="min-height:250px;box-shadow:0 0 14px {rm["glow"]};">'
                f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">'
                f'<span style="font-family:{T["mono"]};font-size:0.96rem;font-weight:700;">{_esc(sym)}</span>'
                f'<span class="ot-badge" style="color:{rm["col"]};background:{rm["col"]}18;'
                f'border:1px solid {rm["col"]}44;">{rm["icon"]} {rm["label"]}</span></div>'
                f'{rows_html}</div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: CANDLESTICK
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_candlestick(symbols: list) -> None:
    _section("PRICE ACTION — OHLCV + TRADE ENTRIES/EXITS", "🕯")
    c1, c2, _ = st.columns([1, 1, 4])
    with c1:
        sym = st.selectbox("Coin", options=symbols,
                           key="ot_cs_sym", label_visibility="collapsed")
    with c2:
        tf  = st.selectbox("TF", options=["1hour", "4hour", "1day", "15min"],
                           index=0, key="ot_cs_tf", label_visibility="collapsed")

    _LKG_KEY = "_lkg_candle_fig"
    df         = pd.DataFrame()
    fig_fresh  = None

    try:
        df = load_candles(sym, tf, limit=150)
    except Exception:
        log.error("render_candlestick: load_candles failed sym=%s tf=%s", sym, tf, exc_info=True)


    if not df.empty:
        # Latest trade timestamp must participate in rebuild signature so entry/exit
        # markers update immediately without waiting for a new candle.
        inst = f"{_norm_sym(sym)}-USDT"
        latest_trade_ts = 0
        try:
            _mx = _query("SELECT MAX(ts) AS mx FROM trades WHERE symbol = ?", (inst,))
            if not _mx.empty:
                try:
                    latest_trade_ts = int(float(_mx["mx"].iloc[0] or 0))
                except Exception:
                    latest_trade_ts = 0
        except Exception:
            latest_trade_ts = 0

        try:
            candle_ts = int(float(df["ts"].max() or 0))
        except Exception:
            candle_ts = 0

        sig = (inst, str(tf), int(candle_ts), int(latest_trade_ts))
        prev_sig = st.session_state.get("_cs_fig_sig")
        if prev_sig == sig and st.session_state.get(_LKG_KEY) is not None:
            candle_slot = st.empty()
            candle_slot.plotly_chart(
                st.session_state.get(_LKG_KEY),
                width="stretch",
                config={"displayModeBar": False},
                key="ot_candle_chart",
            )
            return
        st.session_state["_cs_fig_sig"] = sig

        trades_df = load_trades()
        # If the cached trades DF is stale vs DB MAX(ts), force one bounded refresh.
        try:
            if latest_trade_ts > 0 and not trades_df.empty:
                _df_max = 0
                # [FIX-RENAME-PURGE] ts column is always lowercase after the
                # _load_trades_raw rename removal — 'TS' branch is dead code removed.
                if "ts" in trades_df.columns:
                    _df_max = int(float(pd.to_numeric(trades_df["ts"], errors="coerce").max() or 0))
                if _df_max < latest_trade_ts:
                    _force_bust_trades_cache()
                    trades_df = load_trades()
        except Exception:
            pass
        ot        = pd.DataFrame()
        try:
            if not trades_df.empty and "symbol" in trades_df.columns:
                sym_clean = _norm_sym(sym)
                ot = trades_df[
                    trades_df["symbol"].astype(str).apply(_norm_sym) == sym_clean
                ].copy()
                # Ensure dt column is valid
                if "dt" not in ot.columns or ot["dt"].isna().all():
                    if "ts" in ot.columns:
                        ot["dt"] = pd.to_datetime(ot["ts"], unit="s", errors="coerce")
                ot = ot.dropna(subset=["dt"])
                # Restrict to visible candle window so markers land on chart
                if not ot.empty:
                    t_min = df["dt"].min()
                    t_max = df["dt"].max()
                    ot = ot[(ot["dt"] >= t_min) & (ot["dt"] <= t_max)]
        except Exception:
            log.debug("render_candlestick: trade overlay filter failed", exc_info=True)

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            row_heights=[0.76, 0.24], vertical_spacing=0.02)

        fig.add_trace(go.Candlestick(
            x=df["dt"], open=df["open"], high=df["high"],
            low=df["low"],  close=df["close"],
            increasing=dict(fillcolor=T["green"], line=dict(color=T["green"], width=1)),
            decreasing=dict(fillcolor=T["red"],   line=dict(color=T["red"],   width=1)),
            name=sym, showlegend=False,
        ), row=1, col=1)

        if len(df) >= 20:
            _df2 = df.copy()
            _df2["ma20"] = _df2["close"].rolling(20).mean()
            fig.add_trace(go.Scattergl(
                x=_df2["dt"], y=_df2["ma20"], name="MA20", mode="lines",
                line=dict(color=T["blue"], width=1.2, dash="dot"),
            ), row=1, col=1)
        if len(df) >= 50:
            _df2 = df.copy()
            _df2["ma50"] = _df2["close"].rolling(50).mean()
            fig.add_trace(go.Scattergl(
                x=_df2["dt"], y=_df2["ma50"], name="MA50", mode="lines",
                line=dict(color=T["purple"], width=1, dash="dash"),
            ), row=1, col=1)

        if not ot.empty and all(c in ot.columns for c in ("side", "price", "dt")):
            try:
                side_lower = ot["side"].astype(str).str.lower()
                # Covers OKX spot (buy/sell), perp (long/short), tags (entry_long/exit_short/close)
                buy_mask  = (
                    side_lower.str.contains("buy")   |
                    side_lower.str.contains("long")  |
                    side_lower.str.contains("entry")
                )
                sell_mask = (
                    side_lower.str.contains("sell")  |
                    side_lower.str.contains("short") |
                    side_lower.str.contains("exit")  |
                    side_lower.str.contains("close")
                )
                sell_mask = sell_mask & ~buy_mask  # entry takes priority over exit for mixed tags

                buys  = ot[buy_mask].copy()
                sells = ot[sell_mask].copy()

                # Drop zero/NaN prices
                for _s in (buys, sells):
                    _s["price"] = pd.to_numeric(_s["price"], errors="coerce")

                buys  = buys.dropna(subset=["price"])
                buys  = buys[buys["price"] > 0]
                sells = sells.dropna(subset=["price"])
                sells = sells[sells["price"] > 0]

                if not buys.empty:
                    fig.add_trace(go.Scattergl(
                        x=buys["dt"], y=buys["price"], mode="markers",
                        marker=dict(size=12, symbol="triangle-up",
                                    color=T["green"], line=dict(color=T["bg"], width=1.5)),
                        name="Buys",
                        hovertemplate="BUY $%{y:.4f}<extra></extra>",
                        showlegend=True,
                    ), row=1, col=1)
                if not sells.empty:
                    fig.add_trace(go.Scattergl(
                        x=sells["dt"], y=sells["price"], mode="markers",
                        marker=dict(size=12, symbol="triangle-down",
                                    color=T["red"], line=dict(color=T["bg"], width=1.5)),
                        name="Sells",
                        hovertemplate="SELL $%{y:.4f}<extra></extra>",
                        showlegend=True,
                    ), row=1, col=1)
                log.debug(
                    "render_candlestick: %s overlay buys=%d sells=%d window=[%s,%s]",
                    sym, len(buys), len(sells),
                    df["dt"].min() if not df.empty else "?",
                    df["dt"].max() if not df.empty else "?",
                )
            except Exception:
                log.debug("render_candlestick: overlay markers failed", exc_info=True)
        else:
            # [P52-FIX] No trade records yet — overlay open-position cost_basis
            # as a dashed horizontal reference line so the operator always sees
            # where the bot entered even when the trades table is empty.
            try:
                _pos_status = status.get("positions", {})
                _sym_clean  = _norm_sym(sym)
                for _psym, _pdata in _pos_status.items():
                    if _norm_sym(_psym) != _sym_clean:
                        continue
                    _drx = str(_pdata.get("direction", "none")).lower()
                    if _drx in ("none", ""):
                        continue
                    _cb = float(_pdata.get("cost_basis", 0) or 0)
                    if _cb <= 0:
                        continue
                    _line_col = T["green"] if "long" in _drx or "buy" in _drx else T["red"]
                    _line_lbl = f"{'LONG' if 'long' in _drx or 'buy' in _drx else 'SHORT'} entry"
                    fig.add_hline(
                        y=_cb, line_dash="dot", line_color=_line_col,
                        line_width=1.5, row=1, col=1,
                        annotation_text=f"{_line_lbl} ${_cb:.4f}",
                        annotation_position="top right",
                        annotation_font=dict(size=9, color=_line_col, family=_FONT_MONO_PLOTLY),
                    )
                    log.debug(
                        "render_candlestick: %s no-trades fallback line %s @ %.4f",
                        sym, _line_lbl, _cb,
                    )
            except Exception:
                log.debug("render_candlestick: position reference line failed", exc_info=True)

        vol_col = df.get("volume", pd.Series([0] * len(df)))
        fig.add_trace(go.Bar(
            x=df["dt"], y=vol_col,
            marker_color=T["border2"], name="Vol", showlegend=False,
        ), row=2, col=1)

        # [FIX-QA-08] subplot=True — no global yaxis.side override
        _fig_base(fig, height=420, subplot=True)
        fig.update_layout(
            height=420,
            xaxis=dict(gridcolor=T["border"], zeroline=False, showgrid=True),
            yaxis=dict(gridcolor=T["border"],  zeroline=False, showgrid=True, side="right"),
            yaxis2=dict(gridcolor=T["border"], zeroline=False, showgrid=True, side="right"),
            showlegend=True,
            legend=dict(
                orientation="h", yanchor="bottom", y=1.01, xanchor="left", x=0,
                font=dict(size=9, family=_FONT_MONO_PLOTLY, color=T["text2"]),
                bgcolor="rgba(0,0,0,0)", bordercolor="rgba(0,0,0,0)",
            ),
        )
        fig_fresh = fig
        st.session_state[_LKG_KEY] = fig_fresh

    candle_slot = st.empty()
    fig_to_paint = fig_fresh if fig_fresh is not None else st.session_state.get(_LKG_KEY)
    if fig_to_paint is not None:
        candle_slot.plotly_chart(
            fig_to_paint,
            width="stretch",
            config={"displayModeBar": False},
            key="ot_candle_chart",
        )
    else:
        candle_slot.markdown(
            f'<div class="ot-tile" style="height:420px;display:flex;align-items:center;'
            f'justify-content:center;"><span style="color:{T["muted"]};font-size:0.72rem;'
            f'font-family:{T["mono"]};">NO CANDLES {_esc(sym)}/{_esc(tf)}</span></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: MACRO PULSE
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_macro_pulse() -> None:
    status = _get_status()
    _section("MACRO SENTIMENT + P42 SHADOW CORRELATION ENGINE", "📡")

    nw        = (status.get("agents") or {}).get("news_wire", {})
    if not isinstance(nw, dict): nw = {}
    score     = _safe_float(nw.get("macro_score"))
    label     = _esc(str(nw.get("label", "Waiting…")))
    source    = _esc(str(nw.get("source", "—")))
    age       = _safe_float(nw.get("age_secs"))
    block     = bool(nw.get("bearish_block", False))
    headlines = nw.get("top_headlines", []) or []

    # [P42-DASH] Read P42 shadow correlation gate state
    p42       = status.get("p42_shadow", {}) or {}
    spy_btc_r = _safe_float(p42.get("spy_btc_corr",    1.0), default=1.0)
    decoupled = bool(p42.get("spy_gate_decoupled", False))
    corr_thr  = _safe_float(p42.get("corr_threshold",  0.3), default=0.3)
    btc_samp  = _safe_int(p42.get("btc_samples",        0))
    # Also try pulling from raw global_market_status path
    if spy_btc_r == 1.0 and not decoupled:
        gms = status.get("global_market_status", {}) or {}
        spy_btc_r = _safe_float(gms.get("spy_btc_corr", spy_btc_r), default=spy_btc_r)

    # Cache p42_shadow for LKG
    if p42:
        st.session_state["_lkg_p42_shadow"] = p42

    # ── Layout: VADER gauge | SPY-BTC gauge | Info
    c_vader, c_corr, c_info = st.columns([1.2, 1.0, 1.4])

    with c_vader:
        gc  = T["green"] if score > 0.1 else T["red"] if score < -0.1 else T["yellow"]
        fig = go.Figure(go.Indicator(
            mode="gauge+number", value=round(score, 4),
            number={"font": {"size": 32, "color": gc, "family": _FONT_MONO_PLOTLY},
                    "valueformat": "+.4f"},
            gauge={
                "axis": {"range": [-1, 1],
                         "tickvals": [-1, -0.5, -0.1, 0, 0.1, 0.5, 1],
                         "ticktext": ["−1", "−0.5", "−0.1", "0", "+0.1", "+0.5", "+1"],
                         "tickfont": {"size": 7, "color": T["muted"], "family": _FONT_MONO_PLOTLY},
                         "tickwidth": 1, "tickcolor": T["muted"]},
                "bar":   {"color": gc, "thickness": 0.20},
                "bgcolor": "rgba(0,0,0,0)", "borderwidth": 0,
                "steps": [
                    {"range": [-1,   -0.50], "color": "rgba(255,0,60,0.30)"},
                    {"range": [-0.50,-0.10], "color": "rgba(255,0,60,0.12)"},
                    {"range": [-0.10, 0.10], "color": "rgba(100,100,100,0.10)"},
                    {"range": [0.10,  0.50], "color": "rgba(0,255,65,0.12)"},
                    {"range": [0.50,  1.00], "color": "rgba(0,255,65,0.30)"},
                ],
                "threshold": {"line": {"color": T["red"], "width": 2},
                              "thickness": 0.80, "value": -0.50},
            },
            title={"text": "VADER MACRO SCORE",
                   "font": {"color": T["text2"], "size": 9, "family": _FONT_SANS_PLOTLY}},
        ))
        fig.update_layout(height=210, margin=dict(l=20, r=20, t=20, b=0),
                          paper_bgcolor="rgba(0,0,0,0)", font=dict(color=T["text"]))
        with st.container(height=240, border=False):
            _paint_fig(st.empty(), fig, _FIG_KEY_MACRO, 210,
                       "AWAITING MACRO", "ot_macro_pulse_gauge")

    with c_corr:
        # [P42-DASH] SPY/BTC Pearson-r gauge
        _corr_col = (T["orange"] if decoupled else
                     T["green"] if spy_btc_r > 0.5 else
                     T["yellow"] if spy_btc_r > 0.3 else T["orange"])
        fig_corr = None
        try:
            fig_corr = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(spy_btc_r, 3),
                number={"font": {"size": 28, "color": _corr_col, "family": _FONT_MONO_PLOTLY},
                        "valueformat": ".3f"},
                gauge={
                    "axis": {"range": [-1, 1],
                             "tickvals": [-1, -0.3, 0, 0.3, 0.6, 1],
                             "ticktext": ["−1", "−0.3", "0", "0.3", "0.6", "1"],
                             "tickfont": {"size": 7, "color": T["muted"], "family": _FONT_MONO_PLOTLY}},
                    "bar":   {"color": _corr_col, "thickness": 0.18},
                    "bgcolor": "rgba(0,0,0,0)", "borderwidth": 0,
                    "steps": [
                        {"range": [-1,   corr_thr], "color": "rgba(251,146,60,0.25)"},
                        {"range": [corr_thr, 0.6],  "color": "rgba(250,204,21,0.12)"},
                        {"range": [0.6,  1.0],      "color": "rgba(0,255,65,0.15)"},
                    ],
                    "threshold": {"line": {"color": T["orange"], "width": 2},
                                  "thickness": 0.85, "value": corr_thr},
                },
                title={"text": f"SPY/BTC PEARSON-r (thr={corr_thr:.1f})",
                       "font": {"color": T["text2"], "size": 8, "family": _FONT_SANS_PLOTLY}},
            ))
            fig_corr.update_layout(height=210, margin=dict(l=20, r=20, t=20, b=0),
                                   paper_bgcolor="rgba(0,0,0,0)", font=dict(color=T["text"]))
        except Exception:
            log.debug("render_macro_pulse: spy_btc corr gauge failed", exc_info=True)

        with st.container(height=240, border=False):
            _paint_fig(st.empty(), fig_corr, _FIG_KEY_SPY_BTC, 210,
                       "AWAITING CORR DATA", "ot_spy_btc_gauge")

        # Decouple badge
        if decoupled:
            st.markdown(
                f'<div class="ot-corr-decouple">⚡ SPY-GATE STANDING DOWN<br>'
                f'<span style="font-size:0.60rem;opacity:0.8;">'
                f'r={spy_btc_r:.3f} &lt; {corr_thr:.1f} — crypto decoupled</span></div>',
                unsafe_allow_html=True,
            )
        else:
            spy_gate_col = T["green"] if spy_btc_r > corr_thr else T["orange"]
            st.markdown(
                f'<div class="ot-p44-ok" style="font-size:0.66rem;">'
                f'✓ SPY-gate ACTIVE · r={spy_btc_r:.3f} · {btc_samp} samples</div>',
                unsafe_allow_html=True,
            )

    with c_info:
        if block:
            st.markdown(f'<div class="ot-alert-red">⛔ LONG ENTRIES BLOCKED — macro &lt; −0.50</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="ot-alert-green">✓ Long entries permitted by NewsWire</div>',
                        unsafe_allow_html=True)
        st.markdown(
            f'<div class="ot-tile">'
            f'<div class="ot-kpi-sub">Source: <span style="color:{T["text"]};font-family:{T["mono"]};">'
            f'{source}</span></div>'
            f'<div class="ot-kpi-sub">Label: <span style="color:{T["text"]};">{label}</span>'
            f' · Age: <span style="font-family:{T["mono"]};">{age:.0f}s</span></div></div>',
            unsafe_allow_html=True,
        )
        if headlines and isinstance(headlines, list):
            items = "".join(
                f'<div style="font-size:0.66rem;color:{T["text2"]};padding:3px 0;'
                f'border-bottom:1px solid {T["border"]};">'
                f'▸ {_esc(str(h)[:88])}{"…" if len(str(h))>88 else ""}</div>'
                for h in headlines[:3] if isinstance(h, str)
            )
            st.markdown(
                f'<div class="ot-tile" style="margin-top:6px;">'
                f'<div class="ot-kpi-label">LIVE HEADLINES</div>{items}</div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: SHADOW AUDIT  [FIX-PERF-04]
# ══════════════════════════════════════════════════════════════════════════════

_MAX_AUDIT_TAIL_BYTES = 65_536


def _csv_tail_lines(path: Path, n_lines: int, max_bytes: int = _MAX_AUDIT_TAIL_BYTES) -> list[str]:
    """[OPS-03] Efficient OS-level tail read — never loads the full file."""
    with path.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        f.seek(max(0, size - max_bytes))
        raw = f.read()
    for enc in ("utf-8", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if size > max_bytes and lines:
        lines = lines[1:]
    return lines[-n_lines:] if len(lines) > n_lines else lines


@st.fragment(run_every=FRAG_SLOW)
def render_shadow_audit(n_rows: int = 8) -> None:
    """[OPS-03] Efficient tail read. [OPS-04] Multi-encoding decode.
    [FIX-PERF-04] @st.fragment — not re-run on every main() rerun.
    [P46-DASH] Dynamic column detection — shows signal_ts, theoretical_px,
               p39_active, p41_active; computes latency_ms and slippage_bps inline.
    """
    status     = _get_status()
    audit_path = Path(str(status.get("p19_shadow_audit_path") or SHADOW_AUDIT_PATH))

    with st.expander("📋 SHADOW AUDIT — DECISION LOG · P46 EXECUTION TELEMETRY", expanded=False):
        if not audit_path.exists():
            st.info("Shadow audit log not yet available.")
            return
        try:
            all_lines = _csv_tail_lines(audit_path, n_lines=n_rows + 1)
            if not all_lines:
                st.info("Shadow audit is empty.")
                return

            import io as _io
            with audit_path.open("rb") as _hf:
                header_raw = _hf.readline()
            for enc in ("utf-8", "latin-1"):
                try:
                    header_line = header_raw.decode(enc).strip()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                header_line = header_raw.decode("utf-8", errors="replace").strip()

            data_lines = [l for l in all_lines if l.strip() and l.strip() != header_line]
            csv_str    = header_line + "\n" + "\n".join(data_lines[-n_rows:])
            df_tail    = pd.read_csv(_io.StringIO(csv_str)).iloc[::-1].reset_index(drop=True)

            if df_tail.empty:
                st.info("Shadow audit is empty.")
                return

            # Format timestamp
            if "ts" in df_tail.columns:
                df_tail["ts"] = (
                    pd.to_datetime(df_tail["ts"], unit="s", errors="coerce")
                    .dt.strftime("%H:%M:%S")
                )

            # [P46-DASH] Compute latency_ms and slippage_bps from P46 fields
            if "signal_ts" in df_tail.columns:
                # latency_ms = (fill_ts - signal_ts) * 1000
                # ts column is now formatted as string; use raw ts from CSV via index trick
                # Reparse ts for arithmetic
                try:
                    _raw_ts = pd.read_csv(_io.StringIO(csv_str))["ts"].iloc[::-1].reset_index(drop=True)
                    _sig_ts = pd.to_numeric(df_tail["signal_ts"], errors="coerce")
                    _fill_ts = pd.to_numeric(_raw_ts, errors="coerce")
                    df_tail["latency_ms"] = ((_fill_ts - _sig_ts) * 1000).round(1)
                except Exception:
                    df_tail["latency_ms"] = pd.NA

            if "theoretical_px" in df_tail.columns and "fill_px" in df_tail.columns:
                try:
                    _th = pd.to_numeric(df_tail["theoretical_px"], errors="coerce")
                    _fp = pd.to_numeric(df_tail["fill_px"],        errors="coerce")
                    df_tail["slippage_bps"] = ((_fp - _th) / _th.replace(0, pd.NA) * 10000).round(2)
                except Exception:
                    df_tail["slippage_bps"] = pd.NA

            # [P46-DASH] Dynamic column display — show everything present, P46 cols prioritised
            _core_cols    = ["ts", "symbol", "direction", "tag",
                             "narrative_verdict", "conviction_multiplier", "regime", "real_usd"]
            _p46_cols     = ["latency_ms", "slippage_bps", "theoretical_px",
                             "p39_active", "p41_active", "signal_ts"]
            _desired_order = _core_cols + _p46_cols
            disp = [c for c in _desired_order if c in df_tail.columns]
            # Also include any extra columns not in our lists
            for c in df_tail.columns:
                if c not in disp:
                    disp.append(c)

            # ── P46 KPI summary row ──────────────────────────────────────────
            if any(c in df_tail.columns for c in ("latency_ms", "p39_active", "p41_active", "slippage_bps")):
                k1, k2, k3, k4 = st.columns(4)
                if "latency_ms" in df_tail.columns:
                    _lats = pd.to_numeric(df_tail["latency_ms"], errors="coerce").dropna()
                    _avg_lat = _lats.mean() if len(_lats) else 0.0
                    _lat_col = T["green"] if _avg_lat < 100 else T["yellow"] if _avg_lat < 300 else T["red"]
                    with k1:
                        _kpi("AVG LATENCY", f"{_avg_lat:.0f}ms",
                             f"median {_lats.median():.0f}ms" if len(_lats) else "—", _lat_col)
                if "slippage_bps" in df_tail.columns:
                    _slips = pd.to_numeric(df_tail["slippage_bps"], errors="coerce").dropna()
                    _avg_slip = _slips.mean() if len(_slips) else 0.0
                    _slip_col = T["green"] if abs(_avg_slip) < 5 else T["yellow"] if abs(_avg_slip) < 15 else T["red"]
                    with k2:
                        _kpi("AVG SLIPPAGE", f"{_avg_slip:+.1f}bps",
                             f"std {_slips.std():.1f}bps" if len(_slips) > 1 else "—", _slip_col)
                if "p39_active" in df_tail.columns:
                    _p39 = pd.to_numeric(df_tail["p39_active"], errors="coerce").fillna(0)
                    _p39_rate = _p39.sum() / max(len(_p39), 1) * 100
                    with k3:
                        _kpi("P39 SNIPE RATE", f"{_p39_rate:.0f}%",
                             f"{int(_p39.sum())}/{len(_p39)} trades",
                             T["cyan"] if _p39_rate > 10 else T["muted"])
                if "p41_active" in df_tail.columns:
                    _p41 = pd.to_numeric(df_tail["p41_active"], errors="coerce").fillna(0)
                    _p41_rate = _p41.sum() / max(len(_p41), 1) * 100
                    with k4:
                        _kpi("P41 BAIT HIT%", f"{_p41_rate:.0f}%",
                             f"{int(_p41.sum())}/{len(_p41)} trades",
                             T["purple"] if _p41_rate > 10 else T["muted"])

            # ── Table ────────────────────────────────────────────────────────
            rows = ""
            for _, row in df_tail.iterrows():
                cells = ""
                for col in disp:
                    val = row.get(col, "—")
                    # Custom renderers
                    if col == "narrative_verdict":
                        vc2  = (T["green"] if "PASS" in str(val).upper()
                                else T["red"] if "VETO" in str(val).upper() else T["muted"])
                        cell = f'<span style="color:{vc2};font-weight:700;">{_esc(str(val))}</span>'
                    elif col == "direction":
                        dc3  = (T["green"] if str(val).upper() == "LONG"
                                else T["red"] if str(val).upper() == "SHORT" else T["muted"])
                        cell = f'<span style="color:{dc3};font-weight:700;">{_esc(str(val))}</span>'
                    elif col in ("conviction_multiplier", "narrative_score"):
                        try:
                            cell = f'<span style="color:{T["cyan"]};font-family:{T["mono"]};">{float(val):.4f}</span>'
                        except (TypeError, ValueError):
                            cell = f'<span style="color:{T["muted"]};">—</span>'
                    elif col == "latency_ms":
                        try:
                            _lv = float(val)
                            _lc = "ot-lat-fast" if _lv < 100 else "ot-lat-warn" if _lv < 300 else "ot-lat-slow"
                            cell = f'<span class="{_lc}">{_lv:.0f}ms</span>'
                        except (TypeError, ValueError):
                            cell = f'<span style="color:{T["muted"]};">—</span>'
                    elif col == "slippage_bps":
                        try:
                            _sv  = float(val)
                            _sc2 = T["green"] if _sv < 0 else T["yellow"] if _sv < 10 else T["red"]
                            cell = f'<span style="color:{_sc2};font-family:{T["mono"]};">{_sv:+.1f}bps</span>'
                        except (TypeError, ValueError):
                            cell = f'<span style="color:{T["muted"]};">—</span>'
                    elif col in ("p39_active", "p41_active"):
                        try:
                            _av = int(float(val))
                            _acls = "ot-p46-active" if _av else "ot-p46-inactive"
                            _albl = ("P39✓" if col == "p39_active" else "P41✓") if _av else "—"
                            cell = f'<span class="ot-p46-badge {_acls}">{_albl}</span>'
                        except (TypeError, ValueError):
                            cell = f'<span style="color:{T["muted"]};">—</span>'
                    elif col == "theoretical_px":
                        try:
                            cell = f'<span style="color:{T["text2"]};font-family:{T["mono"]};font-size:0.66rem;">{float(val):.4f}</span>'
                        except (TypeError, ValueError):
                            cell = f'<span style="color:{T["muted"]};">—</span>'
                    else:
                        cell = f'<span style="font-family:{T["mono"]};color:{T["text2"]};">{_esc(str(val))}</span>'
                    cells += f'<td style="padding:4px 6px;">{cell}</td>'
                rows += f'<tr style="border-bottom:1px solid {T["border"]}22;">{cells}</tr>'

            hdr = "".join(
                f'<th style="padding:4px 6px;font-size:0.55rem;letter-spacing:0.10em;'
                f'color:{T["muted"]};text-transform:uppercase;white-space:nowrap;">'
                f'{_esc(c.upper().replace("_"," "))}</th>'
                for c in disp
            )
            st.markdown(
                f'<div style="overflow-x:auto;">'
                f'<table style="width:100%;border-collapse:collapse;font-size:0.68rem;'
                f'font-family:{T["mono"]};">'
                f'<thead><tr style="border-bottom:1px solid {T["border"]};">{hdr}</tr></thead>'
                f'<tbody>{rows}</tbody></table></div>',
                unsafe_allow_html=True,
            )
            try:
                total_approx = audit_path.stat().st_size // max(len(csv_str) // max(len(df_tail), 1), 1)
            except Exception:
                total_approx = "?"
            st.caption(
                f"Last {len(df_tail)} rows (tail-read, ~{total_approx} total) · "
                f"{audit_path.name} · cols: {', '.join(disp)}"
            )
        except Exception:
            log.error("render_shadow_audit: error", exc_info=True)
            st.warning("Shadow audit temporarily unavailable.")


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P44 PERFORMANCE SUPERVISOR  [P44-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p44_supervisor() -> None:
    """[P44-DASH] Live Supervisor state: win-rate gauge, Kelly override history,
    DCB freeze status, thresholds. Reads supervisor_overrides.json directly.
    """
    _section("P44 PERFORMANCE SUPERVISOR — WIN-RATE · DCB · KELLY AUTO-OVERRIDE", "🛡")

    status = _get_status()
    sup    = load_supervisor_overrides()

    # [P44-GUARD] Early-return BEFORE any data extraction.
    # If the guard were placed after the .get() calls, sup={} would produce
    # win_rate=0.5, n_trades=0, etc. via defaults — masking the "no data" state
    # and rendering a misleading "HEALTHY" banner with zero trades.
    if not sup:
        _slot_placeholder("P44 SUPERVISOR — awaiting first supervisor cycle", 100)
        return

    # DCB state from executor status
    p44_frozen = bool(status.get("p44_dcb_frozen", False))
    p44_rem    = _safe_float(status.get("p44_dcb_freeze_remaining", 0.0))
    p44_until  = _safe_float(status.get("p44_dcb_frozen_until",    0.0))

    # Supervisor data from overrides file
    win_rate    = _safe_float(sup.get("win_rate",           0.5), 0.5)
    n_trades    = _safe_int(sup.get("n_trades",              0))
    regime_cls  = _esc(str(sup.get("regime_class",          "—")))
    sup_paused  = bool(sup.get("supervisor_paused",          False))
    pause_until = _safe_float(sup.get("pause_until_ts",     0.0))
    warn_floor  = _safe_float(sup.get("warn_floor",          0.45), 0.45)
    pause_floor = _safe_float(sup.get("pause_floor",         0.35), 0.35)
    resume_rate = _safe_float(sup.get("resume_rate",         0.55), 0.55)
    hist        = sup.get("override_history", []) or []
    recs        = sup.get("recommendations",  {}) or {}

    # ── Status banner ────────────────────────────────────────────────────────
    if p44_frozen or sup_paused:
        rem_secs = p44_rem if p44_frozen else max(0.0, pause_until - time.time())
        rem_min  = rem_secs / 60
        st.markdown(
            f'<div class="ot-p44-pause">🧊 SUPERVISOR PAUSED · '
            f'win_rate={win_rate:.1%} · unfreeze in {rem_min:.1f}min '
            f'· trades watched: {n_trades}</div>',
            unsafe_allow_html=True,
        )
    elif win_rate < warn_floor:
        st.markdown(
            f'<div class="ot-p44-warn">⚠ ADVISORY: win_rate={win_rate:.1%} '
            f'&lt; warn_floor={warn_floor:.0%} · watching…</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="ot-p44-ok">✓ SUPERVISOR HEALTHY · '
            f'win_rate={win_rate:.1%} · regime={regime_cls} · n={n_trades}</div>',
            unsafe_allow_html=True,
        )

    # ── [LAYOUT-FIX] Full-width gauge on top, thresholds+kelly in 2-col below ──
    # Eliminates gauge clipping inside narrow outer columns at any screen width.

    # ── Win-rate gauge — full column width ──────────────────────────────────
    wr_col = (T["red"] if win_rate < pause_floor else
              T["yellow"] if win_rate < warn_floor else T["green"])
    fig_wr = None
    try:
        fig_wr = go.Figure(go.Indicator(
            mode="gauge+number",
            value=round(win_rate * 100, 2),
            number={"font": {"size": 30, "color": wr_col, "family": _FONT_MONO_PLOTLY},
                    "valueformat": ".1f", "suffix": "%"},
            gauge={
                "axis": {"range": [0, 100],
                         "tickvals": [0, 35, 45, 55, 100],
                         "ticktext": ["0", "PAUSE", "WARN", "RESUME", "100"],
                         "tickfont": {"size": 7, "color": T["muted"]}},
                "bar":  {"color": wr_col, "thickness": 0.22},
                "bgcolor": "rgba(0,0,0,0)", "borderwidth": 0,
                "steps": [
                    {"range": [0,          pause_floor*100], "color": "rgba(244,63,94,0.28)"},
                    {"range": [pause_floor*100, warn_floor*100], "color": "rgba(250,204,21,0.20)"},
                    {"range": [warn_floor*100,  resume_rate*100], "color": "rgba(100,100,100,0.10)"},
                    {"range": [resume_rate*100, 100],             "color": "rgba(34,197,94,0.18)"},
                ],
                "threshold": {"line": {"color": T["red"], "width": 2},
                              "thickness": 0.80, "value": pause_floor * 100},
            },
            title={"text": f"WIN RATE (n={n_trades})",
                   "font": {"color": T["text2"], "size": 9, "family": _FONT_SANS_PLOTLY}},
        ))
        # [LAYOUT-FIX] height/margin consistent with _paint_fig reservation.
        # l=4,r=4 gives arc full width in narrow column; t=24 fits title.
        fig_wr.update_layout(height=170, margin=dict(l=4, r=4, t=24, b=0),
                             paper_bgcolor="rgba(0,0,0,0)", font=dict(color=T["text"]))
    except Exception:
        log.debug("render_p44_supervisor: gauge build failed", exc_info=True)

    _paint_fig(st.empty(), fig_wr, _FIG_KEY_P44_WINRATE, 170,
               "AWAITING SUPERVISOR DATA", "ot_p44_wr_gauge")

    # ── 2-col bottom row: Thresholds left | Kelly right ─────────────────────
    _col_thresh, _col_kelly = st.columns([1, 1.4], gap="small")

    # ── Thresholds ───────────────────────────────────────────────────────────
    with _col_thresh:
        st.markdown(f'<div class="ot-kpi-label">SUPERVISOR THRESHOLDS</div>',
                    unsafe_allow_html=True)
        rows_t = (
            ("WARN FLOOR",   f"{warn_floor:.0%}",  T["yellow"]),
            ("PAUSE FLOOR",  f"{pause_floor:.0%}",  T["red"]),
            ("RESUME RATE",  f"{resume_rate:.0%}",  T["green"]),
            ("REGIME",       regime_cls,            T["cyan"]),
        )
        rows_html = "".join(
            f'<div style="display:flex;justify-content:space-between;'
            f'padding:4px 0;border-bottom:1px solid {T["border"]}33;'
            f'font-family:{T["mono"]};font-size:0.69rem;">'
            f'<span style="color:{T["text2"]};white-space:nowrap;">{_esc(lbl)}</span>'
            f'<span style="color:{col};font-weight:700;margin-left:4px;">{_esc(val)}</span></div>'
            for lbl, val, col in rows_t
        )
        st.markdown(
            f'<div class="ot-tile" style="margin-top:4px;">{rows_html}</div>',
            unsafe_allow_html=True,
        )
        if recs:
            rec_html = "".join(
                f'<div style="font-size:0.62rem;color:{T["text2"]};padding:2px 0;">'
                f'▸ <span style="color:{T["text"]};">{_esc(k)}</span>: {_esc(str(v))}</div>'
                for k, v in recs.items()
            )
            st.markdown(
                f'<div class="ot-tile" style="margin-top:4px;">'
                f'<div class="ot-kpi-label">RECOMMENDATIONS</div>'
                f'{rec_html}</div>',
                unsafe_allow_html=True,
            )

    # ── Kelly override history ───────────────────────────────────────────────
    with _col_kelly:
        st.markdown(f'<div class="ot-kpi-label">KELLY AUTO-OVERRIDE HISTORY (last {min(len(hist),8)})</div>',
                    unsafe_allow_html=True)
        if hist:
            hist_show = list(reversed(hist))[:8]
            rows_k = ""
            for h in hist_show:
                _ts    = h.get("ts", 0)
                _ts_s  = datetime.fromtimestamp(float(_ts)).strftime("%m-%d %H:%M") if _ts else "—"
                _old_k = _safe_float(h.get("old_kelly", 0)) * 100
                _new_k = _safe_float(h.get("new_kelly", 0)) * 100
                _wr_h  = _safe_float(h.get("win_rate",  0)) * 100
                _reg   = _esc(str(h.get("regime_class", "—"))[:10])
                _dir   = "▼" if _new_k < _old_k else "▲"
                _dc    = T["red"] if _new_k < _old_k else T["green"]
                _bar_w = min(max(_new_k, 0), 100)
                _bar_c = T["red"] if _new_k < 8 else T["yellow"] if _new_k < 12 else T["green"]
                rows_k += (
                    f'<tr>'
                    f'<td style="color:{T["text2"]};font-size:0.60rem;">{_ts_s}</td>'
                    f'<td style="color:{T["muted"]};">{_old_k:.1f}%→'
                    f'<span style="color:{_dc};font-weight:700;">{_new_k:.1f}%</span> {_dir}</td>'
                    f'<td style="color:{T["yellow"]};">{_wr_h:.0f}%</td>'
                    f'<td style="color:{T["cyan"]};font-size:0.62rem;">{_reg}</td>'
                    f'</tr>'
                )
            st.markdown(
                f'<div class="ot-tile ot-scroll" style="padding:0;">'
                f'<table class="ot-table">'
                f'<thead><tr>'
                f'<th>TIME</th><th>KELLY CHANGE</th><th>WIN%</th><th>REGIME</th>'
                f'</tr></thead>'
                f'<tbody>{rows_k}</tbody></table></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="ot-alert-green" style="margin-top:4px;">'
                f'✓ No auto-overrides fired — Kelly at nominal value</div>',
                unsafe_allow_html=True,
            )
            _cur_kelly = _safe_float(status.get("kelly_fraction",
                         (status.get("p20_global_risk") or {}).get("kelly_fraction", 0)))
            if _cur_kelly > 0:
                _kf_w = min(_cur_kelly * 100 / 25 * 100, 100)
                _kf_c = T["green"] if _cur_kelly > 0.12 else T["yellow"] if _cur_kelly > 0.08 else T["red"]
                st.markdown(
                    f'<div class="ot-tile" style="margin-top:4px;">'
                    f'<div class="ot-kpi-label">CURRENT KELLY FRACTION</div>'
                    f'<div class="ot-kpi-value" style="color:{_kf_c};">{_cur_kelly:.2%}</div>'
                    f'<div class="ot-kelly-bar-bg"><div class="ot-kelly-bar-fill"'
                    f' style="width:{_kf_w:.1f}%;background:{_kf_c};"></div></div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )



# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P40.5 IPC BRIDGE HEARTBEAT / RTT MONITOR  [P40.5-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_FAST)
def render_p40_5_heartbeat() -> None:
    """[P40.5-DASH] Live IPC bridge latency: RTT gauge, P99, congestion state,
    consecutive timeout counter. Reads p40_5_heartbeat from trader_status.json.
    """
    _section("P40.5 IPC BRIDGE HEARTBEAT — RTT · CONGESTION · STANDDOWN", "🔗")

    status = _get_status()
    hb     = status.get("p40_5_heartbeat") or {}

    if not hb or not hb.get("heartbeat_interval_secs"):
        _slot_placeholder("P40.5 — bridge heartbeat data not yet available", 80)
        return

    last_rtt      = _safe_float(hb.get("last_rtt_ms",              0.0))
    p99_rtt       = _safe_float(hb.get("rtt_p99_ms",               0.0))
    congested     = bool(hb.get("is_ipc_congested",                False))
    timeouts      = _safe_int(hb.get("consecutive_timeouts",        0))
    warn_thresh   = _safe_float(hb.get("rtt_warn_threshold_ms",    10.0), 10.0)
    crit_thresh   = _safe_float(hb.get("rtt_critical_threshold_ms",50.0), 50.0)
    miss_thresh   = _safe_int(hb.get("standdown_miss_threshold",    3))
    interval      = _safe_float(hb.get("heartbeat_interval_secs",   1.0), 1.0)
    window        = _safe_int(hb.get("rtt_window_size",            60))
    # [P40.5-RX] RX-mode liveness: bridge uses equity-update proof-of-life
    # instead of ping/pong when BRIDGE_HEARTBEAT_MODE="rx" (default).
    rx_mode       = bool(hb.get("heartbeat_mode") == "rx" or last_rtt == 0.0)
    last_eq_ago   = _safe_float(hb.get("last_equity_ts_ago_secs",   0.0))

    # ── Status banner ────────────────────────────────────────────────────────
    # In RX mode with timeouts ≥ miss_thresh BUT last_rtt == 0.0 (synthetic healthy),
    # the bridge is alive via equity-update liveness — don't show false stand-down.
    in_standdown = congested and timeouts >= miss_thresh and last_rtt > 0.0
    if in_standdown:
        st.markdown(
            f'<div class="ot-alert-red">🛑 IPC STAND-DOWN — '
            f'{timeouts} consecutive ping timeouts (threshold: {miss_thresh}) · '
            f'Bridge may be deadlocked</div>',
            unsafe_allow_html=True,
        )
    elif congested and last_rtt >= crit_thresh:
        st.markdown(
            f'<div class="ot-alert-red">🔴 IPC CRITICAL — '
            f'RTT {last_rtt:.2f} ms ≥ {crit_thresh:.0f} ms · severe congestion</div>',
            unsafe_allow_html=True,
        )
    elif congested:
        st.markdown(
            f'<div class="ot-p44-warn">🟡 IPC CONGESTION — '
            f'RTT {last_rtt:.2f} ms ≥ {warn_thresh:.0f} ms warn threshold</div>',
            unsafe_allow_html=True,
        )
    elif rx_mode:
        st.markdown(
            f'<div class="ot-p44-ok">✓ IPC HEALTHY (RX-liveness mode) — '
            f'equity-update proof-of-life · last update {last_eq_ago:.1f}s ago</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="ot-p44-ok">✓ IPC HEALTHY — '
            f'RTT {last_rtt:.3f} ms · P99 {p99_rtt:.3f} ms</div>',
            unsafe_allow_html=True,
        )

    # ── [LAYOUT-FIX] Full-width stacked layout ─────────────────────────────────
    # Gauge takes full column width (no inner split) so the arc is never clipped.
    # Stats table renders in a compact 2-column grid directly below the gauge.

    # ── RTT gauge — full column width ────────────────────────────────────────
    rtt_col = (T["red"]    if last_rtt >= crit_thresh else
               T["yellow"] if last_rtt >= warn_thresh else T["green"])
    fig_rtt = None
    try:
        import plotly.graph_objects as _go
        _gauge_max = max(crit_thresh * 1.5, last_rtt * 1.2, 100.0)
        fig_rtt = _go.Figure(_go.Indicator(
            mode="gauge+number",
            value=round(last_rtt, 3),
            number={"font": {"size": 20, "color": rtt_col, "family": _FONT_MONO_PLOTLY},
                    "valueformat": ".3f", "suffix": " ms"},
            gauge={
                "axis": {
                    "range": [0, _gauge_max],
                    "tickvals": [0, warn_thresh, crit_thresh, _gauge_max],
                    "ticktext": ["0", f"{warn_thresh:.0f}", f"{crit_thresh:.0f}", f"{_gauge_max:.0f}ms"],
                    "tickfont": {"size": 7, "color": T["muted"]},
                },
                "bar":     {"color": rtt_col, "thickness": 0.28},
                "bgcolor": "rgba(0,0,0,0)", "borderwidth": 0,
                "steps": [
                    {"range": [0,           warn_thresh], "color": "rgba(34,197,94,0.18)"},
                    {"range": [warn_thresh,  crit_thresh], "color": "rgba(250,204,21,0.20)"},
                    {"range": [crit_thresh,  _gauge_max],  "color": "rgba(244,63,94,0.28)"},
                ],
                "threshold": {"line": {"color": T["yellow"], "width": 2},
                              "thickness": 0.80, "value": warn_thresh},
            },
            title={"text": "LAST RTT (ms)",
                   "font": {"color": T["text2"], "size": 9, "family": _FONT_SANS_PLOTLY}},
        ))
        # height=160: short panel — just the arc + number, no wasted whitespace
        fig_rtt.update_layout(
            height=160, margin=dict(l=4, r=4, t=18, b=0),
            paper_bgcolor="rgba(0,0,0,0)", font=dict(color=T["text"]),
        )
    except Exception:
        log.debug("render_p40_5_heartbeat: gauge build failed", exc_info=True)

    _paint_fig(st.empty(), fig_rtt, _FIG_KEY_P405_RTT, 160,
               "AWAITING RTT DATA", "ot_p405_rtt_gauge")

    # ── Stats table — compact 2-col grid below gauge ──────────────────────────
    timeout_col = T["red"] if timeouts >= miss_thresh else (
                  T["yellow"] if timeouts > 0 else T["green"])
    cong_col = T["red"] if congested else T["green"]
    cong_str = "🔴 CONGESTED" if congested else "✅ CLEAR"

    rows = (
        ("LAST RTT",       f"{last_rtt:.3f} ms",         rtt_col),
        ("P99 RTT",        f"{p99_rtt:.3f} ms",          T["cyan"]),
        ("WARN THRESHOLD", f"{warn_thresh:.0f} ms",      T["yellow"]),
        ("CRIT THRESHOLD", f"{crit_thresh:.0f} ms",      T["red"]),
        ("CONGESTED",      cong_str,                      cong_col),
        ("TIMEOUTS",       f"{timeouts} / {miss_thresh}", timeout_col),
        ("PING INTERVAL",  f"{interval:.1f} s",           T["muted"]),
        ("RTT WINDOW",     f"{window} samples",           T["muted"]),
    )
    rows_html = "".join(
        f'<div style="display:flex;justify-content:space-between;'
        f'padding:3px 0;border-bottom:1px solid {T["border"]}33;'
        f'font-family:{T["mono"]};font-size:0.67rem;">'
        f'<span style="color:{T["text2"]};white-space:nowrap;">{_esc(lbl)}</span>'
        f'<span style="color:{col};font-weight:700;white-space:nowrap;margin-left:6px;">{_esc(val)}</span></div>'
        for lbl, val, col in rows
    )
    st.markdown(
        f'<div class="ot-tile" style="margin-top:4px;padding:4px 8px;">{rows_html}</div>',
        unsafe_allow_html=True,
    )

    # ── HUD pill for Bridge RTT ───────────────────────────────────────────────
    # Expose congestion state into session_state so render_hud_bar() can use it
    # without adding a dependency on the fragment cycle.
    st.session_state["_p405_last_rtt_ms"]   = last_rtt
    st.session_state["_p405_is_congested"]  = congested
    st.session_state["_p405_timeouts"]      = timeouts


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P46 EXECUTION TELEMETRY  [P46-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_SLOW)
def render_p46_telemetry() -> None:
    """[P46-DASH] Execution quality telemetry: latency histogram, slippage
    distribution, P39/P41 activation rates.

    Data source priority (root cause fix):
      1. execution_telemetry.csv  — authoritative P46 file with pre-computed
         total_latency_ms, slippage_bps, p39/p41 flags, 3-stage latency chain.
         Written by executor._p46_record_entry().  Path resolved via
         p46_telem_path key in trader_status.json; falls back to BASE_DIR.
      2. shadow_audit.csv (P46-capable) — if signal_ts column present, latency
         and slippage can be derived on the fly.
      3. shadow_audit.csv (pre-P46) — show a clear info banner; no dummy error.

    Previously: dashboard ONLY read shadow_audit.csv and showed "columns absent"
    error because executor writes P46 data to a SEPARATE execution_telemetry.csv.
    """
    _section("P46 EXECUTION TELEMETRY — LATENCY · SLIPPAGE · SNIPE RATES", "⚡")

    status = _get_status()
    import io as _io

    # ── helpers ───────────────────────────────────────────────────────────────
    def _read_csv_tail(path: Path, n: int = 200):
        """Tail-read last n data rows; return DataFrame or None."""
        try:
            all_lines = _csv_tail_lines(path, n_lines=n + 1)
            if not all_lines:
                return None
            with path.open("rb") as _hf:
                hdr_raw = _hf.readline()
            for enc in ("utf-8", "latin-1"):
                try:
                    hdr = hdr_raw.decode(enc).strip()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                hdr = hdr_raw.decode("utf-8", errors="replace").strip()
            data = [l for l in all_lines if l.strip() and l.strip() != hdr]
            if not data:
                return None
            return pd.read_csv(_io.StringIO(hdr + "\n" + "\n".join(data[-n:])))
        except Exception:
            return None

    # ── 1. Resolve paths ──────────────────────────────────────────────────────
    # execution_telemetry.csv path: prefer trader_status key, fall back to BASE_DIR
    telem_path = Path(str(status.get("p46_telem_path") or
                          BASE_DIR / "execution_telemetry.csv"))
    audit_path = Path(str(status.get("p19_shadow_audit_path") or SHADOW_AUDIT_PATH))

    # ── 2. Find best data source ──────────────────────────────────────────────
    df          = None
    data_source = None
    lat_col     = "total_latency_ms"   # column name in execution_telemetry.csv
    p41_col     = "p41_confirmed"      # column name in execution_telemetry.csv

    # Priority 1: execution_telemetry.csv (full P46 data — latency chain + slippage)
    if telem_path.exists():
        _tdf = _read_csv_tail(telem_path, 200)
        if _tdf is not None and "total_latency_ms" in _tdf.columns:
            df          = _tdf
            data_source = f"execution_telemetry.csv ({len(df)} rows)"

    # Priority 2: shadow_audit.csv with P46 signal_ts column
    if df is None and audit_path.exists():
        _adf = _read_csv_tail(audit_path, 200)
        if _adf is not None and "signal_ts" in _adf.columns and "ts" in _adf.columns:
            _raw_ts = pd.to_numeric(_adf["ts"],        errors="coerce")
            _sig_ts = pd.to_numeric(_adf["signal_ts"], errors="coerce")
            _adf["total_latency_ms"] = ((_raw_ts - _sig_ts) * 1000).clip(lower=0)
            if "theoretical_px" in _adf.columns and "fill_px" in _adf.columns:
                _th = pd.to_numeric(_adf["theoretical_px"], errors="coerce")
                _fp = pd.to_numeric(_adf["fill_px"],        errors="coerce")
                _adf["slippage_bps"] = ((_fp - _th) / _th.replace(0, pd.NA) * 10000)
            # shadow_audit uses p41_active not p41_confirmed
            p41_col     = "p41_active"
            df          = _adf
            data_source = f"shadow_audit.csv/P46 ({len(df)} rows)"

        # Priority 3: pre-P46 shadow_audit — clear info banner, no dummy error
        elif _adf is not None and not _adf.empty:
            _cols = ", ".join(_adf.columns[:8])
            st.markdown(
                f'<div class="ot-alert-yellow">ℹ P46 telemetry: '
                f'<code>execution_telemetry.csv</code> not found yet and '
                f'<code>shadow_audit.csv</code> predates Phase 46 '
                f'(no <code>signal_ts</code> column). '
                f'Columns present: <code>{_cols}</code>. '
                f'Full telemetry will appear after the next entry fill.</div>',
                unsafe_allow_html=True,
            )
            return

    if df is None or df.empty:
        _slot_placeholder("P46 — no telemetry data yet (first fill will populate)", 120)
        return

    try:
        # ── 3. Numeric coercions ──────────────────────────────────────────────
        for _c in (lat_col, "slippage_bps", "p39_active", p41_col):
            if _c and _c in df.columns:
                df[_c] = pd.to_numeric(df[_c], errors="coerce")

        n_total = len(df)
        lats    = df[lat_col].dropna()         if lat_col   in df.columns else pd.Series(dtype=float)
        slips   = df["slippage_bps"].dropna()  if "slippage_bps" in df.columns else pd.Series(dtype=float)
        p39_ser = df["p39_active"].fillna(0)   if "p39_active" in df.columns else pd.Series(dtype=float)
        p41_ser = df[p41_col].fillna(0)        if p41_col and p41_col in df.columns else pd.Series(dtype=float)

        # source badge
        st.markdown(
            f'<div style="font-size:10px;color:{T["muted"]};margin-bottom:6px;">'
            f'📄 Source: <code style="color:{T["cyan"]};">{data_source}</code></div>',
            unsafe_allow_html=True,
        )

        # ── 4. KPI row ────────────────────────────────────────────────────────
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        with k1:
            _avg_l = lats.mean() if len(lats) else 0.0
            _lc    = T["green"] if _avg_l < 100 else T["yellow"] if _avg_l < 300 else T["red"]
            _kpi("AVG LATENCY", f"{_avg_l:.0f}ms",
                 f"p95: {lats.quantile(0.95):.0f}ms" if len(lats) > 4 else "—", _lc)
        with k2:
            _med_l = lats.median() if len(lats) else 0.0
            _kpi("MED LATENCY", f"{_med_l:.0f}ms",
                 f"std: {lats.std():.0f}ms" if len(lats) > 1 else "—", T["blue"])
        with k3:
            _avg_s = slips.mean() if len(slips) else 0.0
            _sc    = T["green"] if abs(_avg_s) < 5 else T["yellow"] if abs(_avg_s) < 15 else T["red"]
            _kpi("AVG SLIPPAGE", f"{_avg_s:+.1f}bps",
                 f"std: {slips.std():.1f}" if len(slips) > 1 else "—", _sc)
        with k4:
            _p39r   = int(p39_ser.sum()) if len(p39_ser) else 0
            _p39pct = _p39r / max(n_total, 1) * 100
            _kpi("P39 SNIPES", f"{_p39pct:.0f}%", f"{_p39r}/{n_total}",
                 T["cyan"] if _p39pct > 5 else T["muted"])
        with k5:
            _p41r   = int(p41_ser.sum()) if len(p41_ser) else 0
            _p41pct = _p41r / max(n_total, 1) * 100
            _kpi("P41 BAIT", f"{_p41pct:.0f}%", f"{_p41r}/{n_total}",
                 T["purple"] if _p41pct > 5 else T["muted"])
        with k6:
            # Show 3-stage latency breakdown if available (execution_telemetry.csv only)
            _s2o = df["signal_to_order_ms"].dropna().mean() \
                   if "signal_to_order_ms" in df.columns else None
            _o2f = df["order_to_fill_ms"].dropna().mean() \
                   if "order_to_fill_ms"   in df.columns else None
            if _s2o is not None and _o2f is not None:
                _kpi("SIG→ORD→FILL",
                     f"{_s2o:.0f}ms",
                     f"ord→fill {_o2f:.0f}ms", T["text2"])
            else:
                _kpi("SAMPLE", str(n_total), "rows", T["text2"])

        # ── 5. Charts: latency histogram | slippage distribution ──────────────
        if len(lats) >= 5 or len(slips) >= 5:
            c_lat, c_slip = st.columns(2)

            with c_lat:
                fig_lat = None
                if len(lats) >= 5:
                    try:
                        _clip_q = min(lats.quantile(0.98), lats.max())
                        _bins   = np.linspace(lats.min(), _clip_q, 20)
                        _hist, _edges = np.histogram(lats.clip(upper=_clip_q), bins=_bins)
                        _mid    = (_edges[:-1] + _edges[1:]) / 2
                        _bcols  = [T["green"] if b < 100 else T["yellow"] if b < 300 else T["red"]
                                   for b in _mid]
                        fig_lat = go.Figure(go.Bar(
                            x=_mid, y=_hist, marker_color=_bcols,
                            marker_line_width=0, name="Latency",
                        ))
                        fig_lat.add_vline(x=lats.mean(), line_color=T["cyan"],
                                          line_dash="dash", line_width=1,
                                          annotation_text=f"avg {lats.mean():.0f}ms",
                                          annotation_font_size=8,
                                          annotation_font_color=T["cyan"])
                        _fig_base(fig_lat, height=180)
                        fig_lat.update_layout(
                            title=dict(text="SIGNAL→FILL LATENCY (ms)",
                                       font=dict(size=9, color=T["text2"])),
                            xaxis_title="ms", yaxis_title="fills", showlegend=False,
                        )
                    except Exception:
                        log.debug("render_p46_telemetry: lat hist failed", exc_info=True)
                _paint_fig(st.empty(), fig_lat, _FIG_KEY_P46_LAT, 180,
                           "AWAITING LATENCY DATA", "ot_p46_lat_hist")

            with c_slip:
                fig_slip = None
                if len(slips) >= 5:
                    try:
                        _sc2    = slips.clip(lower=slips.quantile(0.02), upper=slips.quantile(0.98))
                        _bins2  = np.linspace(_sc2.min(), _sc2.max(), 20)
                        _hist2, _edges2 = np.histogram(_sc2, bins=_bins2)
                        _mid2   = (_edges2[:-1] + _edges2[1:]) / 2
                        _cols2  = [T["red"] if b > 10 else T["yellow"] if b > 3 else T["green"]
                                   for b in _mid2]
                        fig_slip = go.Figure(go.Bar(
                            x=_mid2, y=_hist2, marker_color=_cols2,
                            marker_line_width=0, name="Slippage",
                        ))
                        fig_slip.add_vline(x=0, line_color=T["border"],
                                           line_dash="solid", line_width=1)
                        fig_slip.add_vline(x=slips.mean(), line_color=T["orange"],
                                           line_dash="dash", line_width=1,
                                           annotation_text=f"avg {slips.mean():+.1f}bps",
                                           annotation_font_size=8,
                                           annotation_font_color=T["orange"])
                        _fig_base(fig_slip, height=180)
                        fig_slip.update_layout(
                            title=dict(text="SLIPPAGE DISTRIBUTION (bps)",
                                       font=dict(size=9, color=T["text2"])),
                            xaxis_title="bps", yaxis_title="fills", showlegend=False,
                        )
                    except Exception:
                        log.debug("render_p46_telemetry: slip hist failed", exc_info=True)
                _paint_fig(st.empty(), fig_slip, "fig_lkg_p46_slip", 180,
                           "AWAITING SLIPPAGE DATA", "ot_p46_slip_hist")

    except Exception:
        log.error("render_p46_telemetry: error", exc_info=True)
        st.warning("P46 telemetry temporarily unavailable.")




# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P48 AUTONOMOUS FLOATING  [P48-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p48_floating() -> None:
    """[P48] Autonomous Floating — Strategic Intent Panel (Band 8.5).

    Displays:
      • P48 enable state + bridge connection
      • Per-symbol active intents (floor / target / ceil band, TTL remaining)
      • Per-symbol latest Rust floating_update (current_px, spread_bps, tick_count)
      • Lifetime counters (intents sent, floating_update events received)
    """
    status = _get_status()
    p48    = status.get("p48_floating", {})

    enabled   = bool(p48.get("enabled", False))
    connected = bool(p48.get("bridge_connected", False))
    n_intents = int(p48.get("intent_count",  0))
    n_updates = int(p48.get("update_count",  0))
    ttl_dflt  = float(p48.get("ttl_secs",    30.0))
    floor_bps = float(p48.get("floor_bps",   5.0))

    active_intents = p48.get("active_intents", {})
    floating_state = p48.get("floating_state", {})

    _status_cls = "ot-p48-active" if (enabled and connected) else (
                  "ot-p48-stale" if enabled else "ot-p48-idle")
    _status_lbl = ("🟢 LIVE" if (enabled and connected) else
                   ("🟡 DISC" if enabled else "⚫ OFF"))

    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;margin-bottom:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.70rem;'
        f'font-weight:700;color:{T["text"]};letter-spacing:0.06em;">'
        f'P48 · FLOATING</span>'
        f'<span class="{_status_cls}">{_status_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="ot-p48-band">'
        f'INTENTS&nbsp;<b style="color:{T["text"]};">{n_intents}</b>'
        f'&nbsp;·&nbsp;TICKS&nbsp;<b style="color:{T["text"]};">{n_updates}</b>'
        f'&nbsp;·&nbsp;TTL&nbsp;<b style="color:{T["text"]};">{ttl_dflt:.0f}s</b>'
        f'&nbsp;·&nbsp;ENV&nbsp;<b style="color:{T["text"]};">±{floor_bps:.1f}bp</b>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if not enabled:
        st.caption("P48_ENABLE=0")
        return
    if not connected:
        st.caption("Bridge disconnected.")
        return

    all_syms = sorted(set(list(active_intents.keys()) + list(floating_state.keys())))
    if not all_syms:
        st.caption("No active intents.")
        return

    for sym in all_syms:
        intent  = active_intents.get(sym, {})
        fstate  = floating_state.get(sym, {})
        is_active = bool(intent.get("is_active", False))
        side      = str(intent.get("side", "?")).upper()
        cur_px    = float(fstate.get("current_px", 0.0))
        spread    = float(fstate.get("spread_bps", 0.0))
        is_fresh  = bool(fstate.get("is_fresh",   False))
        _sym_base = sym.split("-")[0]
        if is_active and is_fresh:
            _badge = f'<span class="ot-p48-active">FLOAT</span>'
        elif is_active:
            _badge = f'<span class="ot-p48-stale">INTENT</span>'
        else:
            _badge = f'<span class="ot-p48-idle">EXP</span>'
        _cur = f'{cur_px:.4f}' if cur_px > 0 else '—'
        _sp  = f'{spread:.1f}bp' if spread > 0 else '—'
        st.markdown(
            f'<div class="ot-p48-row">'
            f'<b style="color:{T["text"]};">{_sym_base}</b>'
            f'<span style="color:{T["muted"]};">{side}</span>'
            f'<span style="color:{T["muted"]};">{_cur}</span>'
            f'<span style="color:{T["muted"]};">{_sp}</span>'
            f'{_badge}'
            f'</div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P49 HYBRID BRIDGE CORE  [P49-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p49_protocol() -> None:
    """[P49] Hybrid C++/Rust Bridge Core — capability & framing panel (Band 8.5).

    Displays:
      • Frame mode (newline vs LP) and P49 enable state
      • Capability handshake status + accepted cap badges
      • Intent V2 and Brain State publisher active flags
      • Brain state publish count and last publish age
    """
    status = _get_status()
    p49    = status.get("p49_protocol", {})

    enabled       = bool(p49.get("enabled", False))
    connected     = bool(p49.get("bridge_connected", False))
    frame_mode    = str(p49.get("frame_mode", "newline")).upper()
    hsk_done      = bool(p49.get("handshake_done", False))
    accepted_caps = list(p49.get("accepted_caps", []))
    iv2_active    = bool(p49.get("intent_v2_active", False))
    bs_active     = bool(p49.get("brain_state_active", False))
    bs_count      = int(p49.get("brain_state_count",  0))
    bs_age        = p49.get("last_brain_state_age_s")

    # ── Header ────────────────────────────────────────────────────────────────
    if enabled and connected and hsk_done:
        _hdr_cls = "ot-p49-active"
        _hdr_lbl = "🟢 v49 LIVE"
    elif enabled and connected:
        _hdr_cls = "ot-p49-partial"
        _hdr_lbl = "🟡 v48 COMPAT"
    elif enabled:
        _hdr_cls = "ot-p49-partial"
        _hdr_lbl = "🟡 DISC"
    else:
        _hdr_cls = "ot-p49-idle"
        _hdr_lbl = "⚫ OFF"

    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;margin-bottom:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.70rem;'
        f'font-weight:700;color:{T["text"]};letter-spacing:0.06em;">'
        f'P49 · BRIDGE CORE</span>'
        f'<span class="{_hdr_cls}">{_hdr_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Frame mode row ────────────────────────────────────────────────────────
    _fm_color = T["green"] if frame_mode == "LP" else T["muted"]
    st.markdown(
        f'<div class="ot-p49-band">'
        f'FRAME&nbsp;<b style="color:{_fm_color};">{frame_mode}</b>'
        f'&nbsp;·&nbsp;CAPS&nbsp;<b style="color:{T["text"]};">{len(accepted_caps)}</b>'
        f'&nbsp;·&nbsp;HSK&nbsp;<b style="color:{T["text"] if hsk_done else T["muted"]};">'
        f'{"✓" if hsk_done else "—"}</b>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if not enabled:
        st.caption("P49_ENABLE=0")
        return

    # ── Feature rows ──────────────────────────────────────────────────────────
    _bs_age_str = f"{bs_age:.1f}s" if bs_age is not None else "—"
    for _label, _val, _active in (
        ("INTENT V2",    "enriched intents",             iv2_active),
        ("BRAIN STATE",  f"#{bs_count} age {_bs_age_str}", bs_active),
    ):
        _cls = "ot-p49-active" if _active else "ot-p49-idle"
        st.markdown(
            f'<div class="ot-p49-row">'
            f'<span>{_label}</span>'
            f'<span class="{_cls}">{"ON" if _active else "OFF"}</span>'
            f'<span style="color:{T["muted"]};font-size:0.58rem;">{_val}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Accepted capability badges ────────────────────────────────────────────
    if accepted_caps:
        _cap_html = "".join(
            f'<span class="ot-p49-cap">{c}</span>'
            for c in sorted(accepted_caps)
        )
        st.markdown(
            f'<div style="margin-top:4px;">{_cap_html}</div>',
            unsafe_allow_html=True,
        )
    elif hsk_done:
        st.caption("No caps accepted (v48 bridge).")
    else:
        st.caption("Awaiting handshake…")


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P50 NATIVE HOT-PATH  [P50-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p50_native() -> None:
    """[P50] Native Hot-Path Port — shadow agreement, latency ladder, fallback rate.

    Displays:
      • P50 enable state + mode (SHADOW vs NATIVE) + "hot_path" cap badge
      • Shadow agreement rate gauge (target ≥ 95% before promoting to native)
      • Avg / p99 Rust compute latency (µs) — target < 400µs for < 500µs e2e
      • Lifetime counters: requests, decisions, fallbacks, agrees, disagrees
      • Last execution_decision snapshot (symbol, tag, limit_px, latency_us)
    """
    status = _get_status()
    p50    = status.get("p50_native", {})

    enabled     = bool(p50.get("enabled", False))
    shadow_mode = bool(p50.get("shadow_mode", True))
    connected   = bool(p50.get("bridge_connected", False))
    hp_cap      = bool(p50.get("hot_path_cap_granted", False))
    force_mode  = bool(p50.get("force_mode", False))

    req_cnt  = int(p50.get("request_count",   0))
    dec_cnt  = int(p50.get("decision_count",  0))
    fb_cnt   = int(p50.get("fallback_count",  0))
    ag_cnt   = int(p50.get("agree_count",     0))
    dis_cnt  = int(p50.get("disagree_count",  0))
    agr_rate = p50.get("agreement_rate")          # float|None
    avg_lat  = p50.get("avg_latency_us")          # float|None
    p99_lat  = p50.get("p99_latency_us")          # int|None
    last_dec = p50.get("last_decision", {}) or {}
    timeout_us = int(p50.get("timeout_us",  400))
    agree_bps  = float(p50.get("agree_bps", 2.0))

    # ── Header badge ──────────────────────────────────────────────────────────
    if not enabled:
        _hdr_cls = "ot-p50-idle"
        _hdr_lbl = "⚫ OFF"
    elif not connected:
        _hdr_cls = "ot-p50-idle"
        _hdr_lbl = "🟡 DISC"
    elif shadow_mode:
        _hdr_cls = "ot-p50-shadow"
        _hdr_lbl = "🔵 SHADOW"
    else:
        _hdr_cls = "ot-p50-native"
        _hdr_lbl = "🟢 NATIVE"

    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;margin-bottom:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.70rem;'
        f'font-weight:700;color:{T["text"]};letter-spacing:0.06em;">'
        f'P50 · HOT-PATH</span>'
        f'<span class="{_hdr_cls}">{_hdr_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Mode + cap band ───────────────────────────────────────────────────────
    _cap_color  = T["green"] if hp_cap  else T["muted"]
    _cap_lbl    = "✓ hot_path" if hp_cap else "— no cap"
    _mode_color = T["cyan"]  if shadow_mode else T["green"]
    _mode_lbl   = "SHADOW" if shadow_mode else "NATIVE"
    _force_str  = " FORCED" if force_mode else ""
    st.markdown(
        f'<div class="ot-p50-band">'
        f'MODE&nbsp;<b style="color:{_mode_color};">{_mode_lbl}{_force_str}</b>'
        f'&nbsp;·&nbsp;CAP&nbsp;<b style="color:{_cap_color};">{_cap_lbl}</b>'
        f'&nbsp;·&nbsp;TO&nbsp;<b style="color:{T["text"]};">{timeout_us}µs</b>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if not enabled:
        st.caption("P50_ENABLE=0 — set P50_ENABLE=1 to activate.")
        return

    # ── Shadow agreement gauge ────────────────────────────────────────────────
    total_shadow = ag_cnt + dis_cnt
    if total_shadow > 0 and agr_rate is not None:
        _pct     = round(agr_rate * 100, 1)
        _bar_col = (T["green"] if _pct >= 95 else
                    T["yellow"] if _pct >= 80 else T["red"])
        _agree_cls = "ot-p50-agree" if _pct >= 95 else "ot-p50-disagree"
        st.markdown(
            f'<div style="margin:4px 0;">'
            f'<div style="display:flex;justify-content:space-between;'
            f'font-family:{T["mono"]};font-size:0.60rem;margin-bottom:2px;">'
            f'<span style="color:{T["muted"]};">SHADOW AGREE</span>'
            f'<span class="{_agree_cls}">{_pct:.1f}%</span>'
            f'</div>'
            f'<div class="ot-kelly-bar-bg">'
            f'<div class="ot-kelly-bar-fill" style="width:{min(_pct,100):.1f}%;'
            f'background:{_bar_col};"></div>'
            f'</div>'
            f'<div style="font-family:{T["mono"]};font-size:0.55rem;'
            f'color:{T["muted"]};text-align:right;">'
            f'≥95% to promote native · tol={agree_bps:.1f}bps'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.caption(f"No shadow data yet. ({total_shadow} rounds)")

    # ── Latency metrics ───────────────────────────────────────────────────────
    if avg_lat is not None:
        _lat_cls  = ("ot-lat-fast" if avg_lat < 250 else
                     "ot-lat-warn" if avg_lat < 400 else "ot-lat-slow")
        _p99_str  = f"{p99_lat}µs" if p99_lat is not None else "—"
        st.markdown(
            f'<div class="ot-p50-band">'
            f'RUST LAT&nbsp;<b class="{_lat_cls}">{avg_lat:.0f}µs</b>'
            f'&nbsp;·&nbsp;P99&nbsp;<b style="color:{T["text"]};">{_p99_str}</b>'
            f'&nbsp;·&nbsp;TARGET&nbsp;<b style="color:{T["muted"]};">≤400µs</b>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Counters table ────────────────────────────────────────────────────────
    _fb_pct = round(fb_cnt / max(req_cnt, 1) * 100, 1) if req_cnt else 0.0
    _fb_col = T["green"] if _fb_pct < 5 else T["yellow"] if _fb_pct < 20 else T["red"]
    rows = [
        ("REQUESTS",  str(req_cnt),            T["text2"]),
        ("DECISIONS", str(dec_cnt),            T["text2"]),
        ("FALLBACKS", f"{fb_cnt} ({_fb_pct:.0f}%)", _fb_col),
        ("AGREES",    str(ag_cnt),             T["green"]),
        ("DISAGREES", str(dis_cnt),            T["red"] if dis_cnt > 0 else T["muted"]),
    ]
    rows_html = "".join(
        f'<div class="ot-p50-row">'
        f'<span>{_esc(lbl)}</span>'
        f'<span style="color:{col};font-weight:700;">{_esc(val)}</span>'
        f'</div>'
        for lbl, val, col in rows
    )
    st.markdown(
        f'<div class="ot-tile" style="margin-top:3px;">{rows_html}</div>',
        unsafe_allow_html=True,
    )

    # ── Last decision snapshot ────────────────────────────────────────────────
    if last_dec and last_dec.get("limit_px"):
        _ld_sym  = _esc(str(last_dec.get("symbol", "?")))
        _ld_side = _esc(str(last_dec.get("side",   "?")).upper())
        _ld_px   = float(last_dec.get("limit_px", 0.0))
        _ld_tag  = _esc(str(last_dec.get("strategy_tag", "?")))
        _ld_lat  = int(last_dec.get("latency_us", 0))
        _ld_lat_cls = ("ot-lat-fast" if _ld_lat < 250 else
                       "ot-lat-warn" if _ld_lat < 400 else "ot-lat-slow")
        st.markdown(
            f'<div style="font-family:{T["mono"]};font-size:0.58rem;'
            f'color:{T["muted"]};margin-top:4px;padding:3px 6px;'
            f'background:{T["surface2"]};border-radius:3px;">'
            f'<b style="color:{T["text2"]};">{_ld_sym} {_ld_side}</b>'
            f'&nbsp;px=<b style="color:{T["text"]};">{_ld_px:.4f}</b>'
            f'&nbsp;tag={_ld_tag}'
            f'&nbsp;lat=<b class="{_ld_lat_cls}">{_ld_lat}µs</b>'
            f'</div>',
            unsafe_allow_html=True,
        )




# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P51 INSTITUTIONAL EXECUTION SUITE  [P51-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p51_algo() -> None:
    """[P51] Institutional Execution Suite — TWAP/VWAP/Iceberg bridge dashboard.

    Displays:
      • P51 enable state + mode (SHADOW vs NATIVE) + "twap_vwap" cap badge
      • Active algo count with per-algo fill progress bar
      • Lifetime counters: requests, fills, completions, fallbacks
      • Shadow agreement rate (Python P32/P9 vs Rust algo engine)
      • Recent algo_complete log (last 5 entries)
    """
    status = _get_status()
    p51    = status.get("p51_algo", {})

    enabled        = bool(p51.get("enabled",           False))
    shadow_mode    = bool(p51.get("shadow_mode",        True))
    connected      = bool(p51.get("bridge_connected",   False))
    tv_cap         = bool(p51.get("twap_vwap_cap",      False))
    force_mode     = bool(p51.get("force_mode",         False))
    active_count   = int (p51.get("active_algo_count",  0))
    active_algos   = p51.get("active_algos",   {}) or {}
    req_cnt        = int (p51.get("request_count",      0))
    fill_cnt       = int (p51.get("fill_count",         0))
    complete_cnt   = int (p51.get("complete_count",     0))
    fallback_cnt   = int (p51.get("fallback_count",     0))
    agree_cnt      = int (p51.get("agree_count",        0))
    disagree_cnt   = int (p51.get("disagree_count",     0))
    agr_rate       = p51.get("agreement_rate")          # float|None
    recent_done    = p51.get("recent_completed", []) or []

    # ── Header badge ──────────────────────────────────────────────────────────
    if not enabled:
        _hdr_cls = "ot-p51-idle"
        _hdr_lbl = "⚫ OFF"
    elif not connected:
        _hdr_cls = "ot-p51-idle"
        _hdr_lbl = "🟡 DISC"
    elif active_count > 0:
        _hdr_cls = "ot-p51-native" if not shadow_mode else "ot-p51-shadow"
        _hdr_lbl = f"🟢 {active_count} ACTIVE"
    elif shadow_mode:
        _hdr_cls = "ot-p51-shadow"
        _hdr_lbl = "🔵 SHADOW"
    else:
        _hdr_cls = "ot-p51-native"
        _hdr_lbl = "🟢 NATIVE"

    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;margin-bottom:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.70rem;'
        f'font-weight:700;color:{T["text"]};letter-spacing:0.06em;">'
        f'P51 · ALGO</span>'
        f'<span class="{_hdr_cls}">{_hdr_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Mode + cap band ───────────────────────────────────────────────────────
    _cap_color = T["green"] if tv_cap   else T["muted"]
    _cap_lbl   = "✓ twap_vwap" if tv_cap else "— no cap"
    _mode_color= T["yellow"] if shadow_mode else T["green"]
    _mode_lbl  = "SHADOW" if shadow_mode else "NATIVE"
    _force_str = " FORCED" if force_mode else ""
    st.markdown(
        f'<div class="ot-p51-band">'
        f'MODE&nbsp;<b style="color:{_mode_color};">{_mode_lbl}{_force_str}</b>'
        f'&nbsp;·&nbsp;CAP&nbsp;<b style="color:{_cap_color};">{_cap_lbl}</b>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if not enabled:
        st.caption("P51_ENABLE=0 — set P51_ENABLE=1 to activate.")
        return

    # ── Active algo progress bars ─────────────────────────────────────────────
    if active_algos:
        for rid, entry in list(active_algos.items())[:3]:  # show max 3
            _sym51  = _norm_sym(str(entry.get("symbol",  "?")))
            _algo51 = str(entry.get("algo",     "?")).upper()
            _side51 = str(entry.get("side",     "?")).upper()
            _pct51  = float(entry.get("fill_pct", 0))
            _age51  = float(entry.get("age_secs", 0))
            _shd51  = bool(entry.get("shadow",  False))
            _bar_col= T["yellow"] if _shd51 else T["green"]
            _mode51 = "SHD" if _shd51 else "LVE"
            st.markdown(
                f'<div style="margin:3px 0;">'
                f'<div style="display:flex;justify-content:space-between;'
                f'font-family:{T["mono"]};font-size:0.58rem;margin-bottom:1px;">'
                f'<span style="color:{T["text2"]};">'
                f'{_algo51} {_side51} {_sym51} [{_mode51}]</span>'
                f'<span class="ot-p51-pct">{_pct51:.0f}% · {_age51:.0f}s</span>'
                f'</div>'
                f'<div class="ot-kelly-bar-bg">'
                f'<div class="ot-kelly-bar-fill" style="width:{min(_pct51,100):.0f}%;'
                f'background:{_bar_col};"></div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    else:
        st.caption("No active algos.")

    # ── Shadow agreement gauge ────────────────────────────────────────────────
    total_shadow51 = agree_cnt + disagree_cnt
    if total_shadow51 > 0 and agr_rate is not None:
        _pct_agr = round(agr_rate * 100, 1)
        _agr_col = T["green"] if _pct_agr >= 90 else T["yellow"] if _pct_agr >= 70 else T["red"]
        _agr_cls = "ot-p51-fill" if _pct_agr >= 90 else "ot-p50-disagree"
        st.markdown(
            f'<div style="margin:3px 0;">'
            f'<div style="display:flex;justify-content:space-between;'
            f'font-family:{T["mono"]};font-size:0.58rem;margin-bottom:1px;">'
            f'<span style="color:{T["muted"]};">SHADOW AGREE</span>'
            f'<span class="{_agr_cls}">{_pct_agr:.1f}%</span>'
            f'</div>'
            f'<div class="ot-kelly-bar-bg">'
            f'<div class="ot-kelly-bar-fill" style="width:{min(_pct_agr,100):.0f}%;'
            f'background:{_agr_col};"></div>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Counters row ──────────────────────────────────────────────────────────
    _fb_pct = round(fallback_cnt / max(req_cnt, 1) * 100, 1) if req_cnt else 0.0
    _fb_col = T["green"] if _fb_pct < 5 else T["yellow"] if _fb_pct < 20 else T["red"]
    st.markdown(
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:2px;margin-top:4px;">'
        f'<div class="ot-p51-algo-row">'
        f'<span>REQS</span><span class="ot-p51-fill">{req_cnt}</span></div>'
        f'<div class="ot-p51-algo-row">'
        f'<span>FILLS</span><span style="color:{T["cyan"]};">{fill_cnt}</span></div>'
        f'<div class="ot-p51-algo-row">'
        f'<span>DONE</span><span style="color:{T["text"]};">{complete_cnt}</span></div>'
        f'<div class="ot-p51-algo-row">'
        f'<span>FALLBK</span>'
        f'<span style="color:{_fb_col};">{fallback_cnt}({_fb_pct:.0f}%)</span>'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Recent completions ────────────────────────────────────────────────────
    if recent_done:
        _last = recent_done[-1]
        _r_sym  = _norm_sym(str(_last.get("symbol",  "?")))
        _r_algo = str(_last.get("algo",   "?")).upper()
        _r_rsn  = str(_last.get("reason", "?"))
        _r_px   = float(_last.get("avg_px",       0))
        _r_slip = float(_last.get("slippage_bps",  0))
        _slip_col = T["green"] if abs(_r_slip) < 5 else T["yellow"] if abs(_r_slip) < 20 else T["red"]
        st.markdown(
            f'<div style="margin-top:4px;font-family:{T["mono"]};font-size:0.57rem;'
            f'color:{T["muted"]};padding:2px 5px;background:{T["surface2"]};'
            f'border-radius:3px;">'
            f'<b style="color:{T["text2"]};">{_r_algo} {_r_sym}</b>'
            f'&nbsp;px=<b style="color:{T["text"]};">{_r_px:.4f}</b>'
            f'&nbsp;slip=<b style="color:{_slip_col};">{_r_slip:+.1f}bps</b>'
            f'&nbsp;<i>{_r_rsn}</i>'
            f'</div>',
            unsafe_allow_html=True,
        )




# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P52 ENGINE SUPERVISOR HEALTH  [P52-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p52_supervisor() -> None:
    """[P52] Engine Supervisor Health — role, PID, uptime, loop rate, bridge link.

    Reads:
      • status["p52_supervisor"]  (written by engine_supervisor._gui_bridge every cycle)
      • hub_data/supervisor_health.json  (written by bot_watchdog — optional)

    Displays:
      • Supervisor role badge (TRADING ENGINE / OFF)
      • Version + PID + uptime
      • Loop cycle counter + approx Hz
      • Bridge health indicator
      • Watchdog restart count (if supervisor_health.json present)
    """
    status = _get_status()
    p52    = status.get("p52_supervisor", {})

    # [P52-LKG] Fall back to last-known-good session state when the status key
    # is absent (stale read, pre-seed frame, or brief _gui_bridge exception cycle).
    # Without this the panel flickers ⚫ OFF on every stale frame.
    if not isinstance(p52, dict) or not p52:
        p52 = _sg(_SS_P52, {})

    enabled        = bool(p52.get("enabled",        False))
    role           = str (p52.get("role",           "—"))
    version        = str (p52.get("version",        "—"))
    pid            = p52.get("pid")
    uptime_secs    = float(p52.get("uptime_secs",   0.0))
    loop_cycles    = int  (p52.get("loop_cycles",   0))
    last_cycle_ts  = float(p52.get("last_cycle_ts", 0.0))
    bridge_healthy = bool (p52.get("bridge_healthy", False))
    demo_mode      = bool (p52.get("demo_mode",      False))
    active_symbols = p52.get("active_symbols", [])

    # ── Derive Hz from uptime + cycles (avoid div-0 on fresh boot) ────────────
    _hz = round(loop_cycles / max(uptime_secs, 1.0), 2)

    # ── Age of last cycle (staleness guard) ───────────────────────────────────
    _cycle_age = time.time() - last_cycle_ts if last_cycle_ts > 0 else None
    _stale     = (_cycle_age is not None and _cycle_age > 10.0)

    # ── Optional watchdog sidecar data ────────────────────────────────────────
    _wd_restarts: Optional[int] = None
    _wd_state:    str            = "—"
    try:
        if SUPERVISOR_HEALTH_PATH.exists():
            _wd_raw = json.loads(SUPERVISOR_HEALTH_PATH.read_text(encoding="utf-8"))
            _wd_restarts = int(_wd_raw.get("restart_count", 0))
            _wd_state    = str(_wd_raw.get("state", "—")).upper()
    except Exception:
        pass

    # ── Header badge ──────────────────────────────────────────────────────────
    if not enabled:
        _hdr_cls = "ot-p52-idle"
        _hdr_lbl = "⚫ OFF"
    elif _stale:
        _hdr_cls = "ot-p52-warn"
        _hdr_lbl = "🟡 STALE"
    else:
        _hdr_cls = "ot-p52-active"
        _hdr_lbl = "🟢 LIVE"

    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;margin-bottom:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.70rem;'
        f'font-weight:700;color:{T["text"]};letter-spacing:0.06em;">'
        f'P52 · SUPERVISOR</span>'
        f'<span class="{_hdr_cls}">{_hdr_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Role + version band ───────────────────────────────────────────────────
    _role_disp = role.upper().replace("_", " ")
    st.markdown(
        f'<div class="ot-p52-band">'
        f'<b style="color:{T["text2"]};">{_role_disp}</b>'
        f'&nbsp;v{_esc(version)}'
        f'</div>',
        unsafe_allow_html=True,
    )

    if not enabled:
        st.caption("p52_supervisor key absent — engine_supervisor not running.")
        return

    # ── Demo / Live mode badge + active symbols ────────────────────────────────
    _mode_col = T["yellow"] if demo_mode else T["green"]
    _mode_lbl = "🟡 DEMO" if demo_mode else "🟢 LIVE"
    _sym_str  = ", ".join(str(s) for s in active_symbols) if active_symbols else "—"
    _sym_cnt  = len(active_symbols)
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;align-items:center;'
        f'margin-top:3px;margin-bottom:1px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.64rem;color:{T["muted"]};">MODE</span>'
        f'<span style="font-family:{T["mono"]};font-size:0.68rem;font-weight:700;'
        f'color:{_mode_col};">{_mode_lbl}</span>'
        f'</div>'
        f'<div style="display:flex;justify-content:space-between;align-items:center;'
        f'margin-bottom:3px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.64rem;color:{T["muted"]};">'
        f'SYMBOLS ({_sym_cnt})</span>'
        f'<span style="font-family:{T["mono"]};font-size:0.62rem;color:{T["text2"]};">'
        f'{_esc(_sym_str)}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── PID + uptime row ──────────────────────────────────────────────────────
    _up_h  = int(uptime_secs // 3600)
    _up_m  = int((uptime_secs % 3600) // 60)
    _up_s  = int(uptime_secs % 60)
    _up_str = f"{_up_h:02d}:{_up_m:02d}:{_up_s:02d}" if _up_h else f"{_up_m:02d}:{_up_s:02d}"
    _pid_str = str(pid) if pid is not None else "—"

    st.markdown(
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:1px;margin-top:3px;">'
        f'<div class="ot-p52-row"><span>PID</span>'
        f'<span class="ot-p52-val">{_esc(_pid_str)}</span></div>'
        f'<div class="ot-p52-row"><span>UP</span>'
        f'<span class="ot-p52-val">{_esc(_up_str)}</span></div>'
        f'<div class="ot-p52-row"><span>CYCLES</span>'
        f'<span class="ot-p52-val">{loop_cycles:,}</span></div>'
        f'<div class="ot-p52-row"><span>RATE</span>'
        f'<span class="ot-p52-val">{_hz:.2f}Hz</span></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Bridge health indicator ───────────────────────────────────────────────
    _br_cls = "ot-p52-ok" if bridge_healthy else "ot-p52-bad"
    _br_lbl = "✓ BRIDGE OK" if bridge_healthy else "✗ BRIDGE DOWN"
    st.markdown(
        f'<div class="ot-p52-row" style="margin-top:3px;">'
        f'<span>BRIDGE</span>'
        f'<span class="{_br_cls}">{_br_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Cycle staleness ───────────────────────────────────────────────────────
    if _cycle_age is not None:
        _age_col = T["green"] if _cycle_age < 5 else T["yellow"] if _cycle_age < 15 else T["red"]
        st.markdown(
            f'<div class="ot-p52-row">'
            f'<span>LAST CYCLE</span>'
            f'<span style="color:{_age_col};font-weight:700;">{_cycle_age:.1f}s ago</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Watchdog sidecar (optional) ───────────────────────────────────────────
    if _wd_restarts is not None:
        _wd_col = T["green"] if _wd_restarts == 0 else T["yellow"] if _wd_restarts < 3 else T["red"]
        st.markdown(
            f'<div class="ot-p52-row">'
            f'<span>WD RESTARTS</span>'
            f'<span style="color:{_wd_col};font-weight:700;">{_wd_restarts}</span>'
            f'</div>'
            f'<div class="ot-p52-row">'
            f'<span>WD STATE</span>'
            f'<span class="ot-p52-val">{_esc(_wd_state)}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )




# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P53 MESH CONSENSUS GATE                                [P53-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p53_mesh() -> None:
    """[P53] Multi-Node Mesh Consensus — 3-exchange signal agreement panel.

    Reads:
      • status["p53_mesh"]  (written by engine_supervisor._gui_bridge every cycle)

    Displays:
      • Header badge: ACTIVE (🔵) / BOOT (🟡) / OFF (⚫)
      • Config band: quorum/3, window, VPIN threshold, HMAC signing status
      • 3-node health matrix: OKX · BINANCE · COINBASE (alive, region, vote#, age)
      • Aggregate stats: total checks, approval rate%, approved count, blocked count
      • Per-symbol latest result table (sym, dir, status, agreeing nodes, confidence)
      • Clock sync row: NTP offset ms, sync count / error count
      • Anti-replay row: accepted / rejected seq counts
    """
    status = _get_status()
    p53    = status.get("p53_mesh", {})

    # [P53-LKG] Fall back to last-known-good session state on stale/missing frames.
    if not isinstance(p53, dict) or not p53:
        p53 = _sg(_SS_P53, {})
    if not isinstance(p53, dict):
        p53 = {}

    enabled    = bool(p53.get("enabled",           False))
    quorum     = _safe_int(p53.get("quorum",        2))
    window_s   = _safe_float(p53.get("window_secs", 10.0))
    vpin_thr   = _safe_float(p53.get("vpin_threshold", 0.60))
    signing    = bool(p53.get("signing_active",     False))
    ntp_off    = _safe_float(p53.get("ntp_offset_ms", 0.0))
    uptime_s   = _safe_float(p53.get("uptime_secs",   0.0))
    total_chk  = _safe_int(p53.get("total_checks",    0))
    appr_cnt   = _safe_int(p53.get("approved_count",  0))
    blk_cnt    = _safe_int(p53.get("blocked_count",   0))
    appr_rate  = _safe_float(p53.get("approval_rate_pct", 0.0))
    nodes      = p53.get("nodes",          {}) or {}
    latest_res = p53.get("latest_results", {}) or {}
    clock_snap = p53.get("clock_sync",     {}) or {}
    replay_snap= p53.get("anti_replay",    {}) or {}

    # ── Header badge ──────────────────────────────────────────────────────────
    _stale = enabled and uptime_s == 0.0
    if not enabled:
        _hdr_cls = "ot-p53-idle"
        _hdr_lbl = "⚫ OFF"
    elif _stale:
        _hdr_cls = "ot-p53-warn"
        _hdr_lbl = "🟡 BOOT"
    else:
        _hdr_cls = "ot-p53-active"
        _hdr_lbl = "🔵 ACTIVE"

    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;margin-bottom:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.70rem;'
        f'font-weight:700;color:{T["text"]};letter-spacing:0.06em;">'
        f'P53 · MESH</span>'
        f'<span class="{_hdr_cls}">{_hdr_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Config band ───────────────────────────────────────────────────────────
    _sign_cls = "ot-p53-sign-on"  if signing else "ot-p53-sign-off"
    _sign_lbl = "✓ HMAC"          if signing else "⚠ UNSIGNED"
    st.markdown(
        f'<div class="ot-p53-band">'
        f'QUORUM {quorum}/3 &nbsp;·&nbsp; WIN {window_s:.0f}s'
        f' &nbsp;·&nbsp; VPIN&lt;{vpin_thr:.2f}'
        f'&nbsp;&nbsp;<span class="{_sign_cls}">{_sign_lbl}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if not enabled:
        st.caption("p53_mesh absent — set P53_MESH_ENABLED=1 and restart.")
        return

    # ── 3-Node health matrix ──────────────────────────────────────────────────
    _NODE_DISPLAY = [
        ("okx_primary",     "OKX",      "AP"),
        ("binance_primary", "BINANCE",  "AP"),
        ("coinbase_oracle", "COINBASE", "US"),
    ]
    node_rows = ""
    for nid, exch_lbl, region_lbl in _NODE_DISPLAY:
        nd        = nodes.get(nid, {})
        alive     = bool(nd.get("alive", False))
        age_s     = _safe_float(nd.get("age_secs", float("inf")))
        votes     = _safe_int(nd.get("vote_count", 0))
        alive_cls = "ot-p53-node-alive" if alive else "ot-p53-node-dead"
        alive_lbl = "●" if alive else "○"
        age_str   = f"{age_s:.0f}s" if age_s < 9999 else "—"
        node_rows += (
            f'<tr>'
            f'<td><span class="{alive_cls}">{alive_lbl}</span></td>'
            f'<td style="color:{T["text"]};font-weight:700;">{_esc(exch_lbl)}</td>'
            f'<td style="color:{T["muted"]};">{_esc(region_lbl)}</td>'
            f'<td style="color:{T["text2"]};">{votes}</td>'
            f'<td style="color:{T["muted"]};">{age_str}</td>'
            f'</tr>'
        )
    _hdr_n = "".join(f'<th>{h}</th>' for h in ["", "NODE", "RGN", "V#", "AGE"])
    st.markdown(
        f'<div class="ot-tile" style="padding:0;margin-bottom:4px;">'
        f'<table class="ot-table"><thead><tr>{_hdr_n}</tr></thead>'
        f'<tbody>{node_rows}</tbody></table></div>',
        unsafe_allow_html=True,
    )

    # ── Aggregate consensus KPIs ──────────────────────────────────────────────
    _rate_col = (
        T["green"]  if appr_rate >= 60 else
        T["yellow"] if appr_rate >= 30 else
        T["red"]
    )
    st.markdown(
        f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:1px;margin:3px 0;">'
        f'<div class="ot-p53-row"><span>CHECKS</span>'
        f'<span class="ot-p53-val">{total_chk:,}</span></div>'
        f'<div class="ot-p53-row"><span>APR%</span>'
        f'<span style="color:{_rate_col};font-weight:700;">{appr_rate:.1f}%</span></div>'
        f'<div class="ot-p53-row"><span>✓APPR</span>'
        f'<span class="ot-p53-ok">{appr_cnt:,}</span></div>'
        f'<div class="ot-p53-row"><span>✗BLKD</span>'
        f'<span class="ot-p53-blocked">{blk_cnt:,}</span></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Per-symbol latest consensus results ───────────────────────────────────
    if latest_res:
        _EXCH_SHORT = {
            "okx_primary":     "OKX",
            "binance_primary": "BNC",
            "coinbase_oracle": "CB",
        }
        sym_rows = ""
        for sym, res in list(latest_res.items())[:6]:
            _appr    = bool(res.get("approved",     False))
            _pdir    = str(res.get("proposed_dir",  "—")).upper()
            _agree   = res.get("agreeing",           []) or []
            _conf    = _safe_float(res.get("confidence", 0.0))
            _lat_us  = _safe_float(res.get("latency_us", 0.0))
            _agree_s = "+".join(_EXCH_SHORT.get(n, n[:3].upper()) for n in _agree) or "—"
            _st_cls  = "ot-p53-ok"      if _appr else "ot-p53-blocked"
            _st_lbl  = "✓"              if _appr else "✗"
            _dir_col = (T["green"] if "LONG"  in _pdir else
                        T["red"]   if "SHORT" in _pdir else T["muted"])
            sym_rows += (
                f'<tr>'
                f'<td style="color:{T["text"]};font-weight:700;">{_esc(sym)}</td>'
                f'<td style="color:{_dir_col};">{_esc(_pdir)}</td>'
                f'<td><span class="{_st_cls}">{_st_lbl}</span></td>'
                f'<td style="color:{T["text2"]};font-size:0.53rem;">{_esc(_agree_s)}</td>'
                f'<td style="color:{T["muted"]};">{_conf:.2f}</td>'
                f'</tr>'
            )
        _hdr_s = "".join(f'<th>{h}</th>' for h in ["SYM", "DIR", "ST", "AGREE", "CONF"])
        st.markdown(
            f'<div class="ot-tile ot-scroll" style="padding:0;margin-bottom:4px;">'
            f'<table class="ot-table"><thead><tr>{_hdr_s}</tr></thead>'
            f'<tbody>{sym_rows}</tbody></table></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div style="font-family:{T["mono"]};font-size:0.62rem;'
            f'color:{T["muted"]};padding:3px 0;">Awaiting first consensus check…</div>',
            unsafe_allow_html=True,
        )

    # ── Clock sync + anti-replay footer rows ──────────────────────────────────
    _sync_cnt = _safe_int(clock_snap.get("sync_count",  0))
    _sync_err = _safe_int(clock_snap.get("error_count", 0))
    _ar_ok    = _safe_int(replay_snap.get("accepted",   0))
    _ar_rej   = _safe_int(replay_snap.get("rejected",   0))
    _ntp_col  = (T["green"]  if abs(ntp_off) < 50 else
                 T["yellow"] if abs(ntp_off) < 200 else T["red"])
    _rej_col  = T["red"] if _ar_rej > 0 else T["muted"]

    st.markdown(
        f'<div class="ot-p53-row">'
        f'<span>NTP OFFSET</span>'
        f'<span style="color:{_ntp_col};font-weight:700;">{ntp_off:+.1f}ms</span>'
        f'</div>'
        f'<div class="ot-p53-row">'
        f'<span>NTP SYNCS</span>'
        f'<span class="ot-p53-val">{_sync_cnt}'
        # [FIX-FSTR-005] Python 3.11: pre-compute conditional f-expr
        f'{"" if _sync_err == 0 else (" / <span style=color:" + T["red"] + ">" + str(_sync_err) + "err</span>")}'
        f'</span></div>'
        f'<div class="ot-p53-row">'
        f'<span>REPLAY ✓/✗</span>'
        f'<span class="ot-p53-val">{_ar_ok} / '
        f'<span style="color:{_rej_col};">{_ar_rej}</span>'
        f'</span></div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: P54 KERNEL-BYPASS TELEMETRY                             [P54-DASH]
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_MED)
def render_p54_kernel_bypass() -> None:
    """[P54] Kernel-Bypass / DPDK / OpenOnload telemetry panel.

    Reads  status["p54_kernel_bypass"]  written by engine_supervisor every cycle.
    Shows: bypass mode badge · ring drops · ring utilisation · latency histogram
           (p50/p95/p99 µs) · zero-copy flag · event rate · PTP clock state.
    Alert colours: ring/drop/lat thresholds trigger red badges.
    """
    status = _get_status()
    p54    = status.get("p54_kernel_bypass", {})

    enabled   = bool (p54.get("enabled",       False))
    mode      = str  (p54.get("mode",          "unknown")).lower()
    rx_drops  = int  (p54.get("rx_drops",      0))
    tx_drops  = int  (p54.get("tx_drops",      0))
    tot_drops = int  (p54.get("total_drops",   0))
    ring_util = float(p54.get("ring_util_pct", 0.0))
    lat_p50   = float(p54.get("lat_p50_us",    0.0))
    lat_p95   = float(p54.get("lat_p95_us",    0.0))
    lat_p99   = float(p54.get("lat_p99_us",    0.0))
    zero_copy = bool (p54.get("zero_copy",     False))
    ev_count  = int  (p54.get("event_count",   0))
    age_secs  = p54.get("age_secs")
    alerts    = dict (p54.get("alerts",        {}))
    version   = str  (p54.get("version",       "—"))

    # ── Header badge ──────────────────────────────────────────────────────────
    if not enabled:
        _cls, _lbl = "ot-p54-off",      "⚫ OFF"
    elif mode == "dpdk":
        _cls, _lbl = "ot-p54-dpdk",     "🟢 DPDK"
    elif mode == "onload":
        _cls, _lbl = "ot-p54-onload",   "🔵 ONLOAD"
    elif mode == "standard":
        _cls, _lbl = "ot-p54-standard", "🟡 STD"
    elif ev_count == 0:
        # [P54-FIX] Bridge enabled but no p54_stats events yet — waiting for
        # the Rust binary to emit telemetry. Show PENDING instead of UNKNOWN.
        _cls, _lbl = "ot-p54-pending",  "⏳ PENDING"
    else:
        _cls, _lbl = "ot-p54-off",      "⚫ UNKNOWN"

    _any_alert = any(alerts.values())
    _hdr_extra = (
        ' <span class="ot-p54-alert">⚠</span>' if _any_alert else ""
    )

    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'align-items:center;margin-bottom:4px;">'
        f'<span style="font-family:{T["mono"]};font-size:0.70rem;'
        f'font-weight:700;color:{T["text"]};letter-spacing:0.06em;">'
        f'P54 · BYPASS</span>'
        f'<span class="{_cls}">{_lbl}{_hdr_extra}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if not enabled:
        st.caption("p54_kernel_bypass absent — P54_ENABLE=0 or bridge not compiled.")
        return

    # [P54-FIX] Awaiting first p54_stats event from Rust bridge
    if ev_count == 0:
        st.markdown(
            f'<div class="ot-alert-yellow" style="font-size:0.65rem;margin:4px 0;">'
            f'⏳ Awaiting first p54_stats event from bridge.<br>'
            f'<span style="color:{T["muted"]};font-size:0.60rem;">'
            f'Bridge must be compiled with P54 support (standard/dpdk/onload). '
            f'Events emit at ~1 Hz once connected.</span></div>',
            unsafe_allow_html=True,
        )
        # Show zero-baseline stats so panel isn't empty
        st.markdown(
            f'<div class="ot-kv-row"><span class="ot-kv-label">RX/TX DROP</span>'
            f'<span class="ot-kv-value" style="color:{T["muted"]};">—</span></div>'
            f'<div class="ot-kv-row"><span class="ot-kv-label">RING UTIL</span>'
            f'<span class="ot-kv-value" style="color:{T["muted"]};">—</span></div>'
            f'<div class="ot-kv-row"><span class="ot-kv-label">LATENCY p99</span>'
            f'<span class="ot-kv-value" style="color:{T["muted"]};">—</span></div>'
            f'<div class="ot-kv-row"><span class="ot-kv-label">EVENTS</span>'
            f'<span class="ot-kv-value" style="color:{T["muted"]};">0</span></div>',
            unsafe_allow_html=True,
        )
        return

    # ── Zero-copy + version row ───────────────────────────────────────────────
    _zc_col = T["green"] if zero_copy else T["muted"]
    _zc_lbl = "ZC ✓" if zero_copy else "ZC ✗"
    _age_str = f"{age_secs:.0f}s" if isinstance(age_secs, float) else "—"
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'font-family:{T["mono"]};font-size:0.60rem;color:{T["muted"]};'
        f'margin-bottom:3px;">'
        f'<span style="color:{_zc_col};font-weight:700;">{_zc_lbl}</span>'
        f'<span>v{_esc(version)}</span>'
        f'<span>age {_age_str}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Ring drops ────────────────────────────────────────────────────────────
    _drop_col = T["red"] if alerts.get("drops") else T["text"]
    st.markdown(
        f'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:1px;">'
        f'<div class="ot-p52-row"><span>RX DROP</span>'
        f'<span style="color:{_drop_col};font-weight:700;">{rx_drops:,}</span></div>'
        f'<div class="ot-p52-row"><span>TX DROP</span>'
        f'<span style="color:{_drop_col};font-weight:700;">{tx_drops:,}</span></div>'
        f'<div class="ot-p52-row"><span>TOTAL</span>'
        f'<span style="color:{_drop_col};font-weight:700;">{tot_drops:,}</span></div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Ring utilisation bar ──────────────────────────────────────────────────
    _ru_col   = (T["red"]    if ring_util > 85 else
                 T["yellow"] if ring_util > 60 else T["green"])
    _ru_bar_w = min(100, ring_util)
    st.markdown(
        f'<div style="margin:4px 0 2px;font-family:{T["mono"]};'
        f'font-size:0.60rem;color:{T["muted"]};">RING UTIL</div>'
        f'<div style="background:{T["surface"]};border-radius:2px;height:6px;overflow:hidden;">'
        f'<div style="background:{_ru_col};width:{_ru_bar_w:.1f}%;height:100%;'
        f'transition:width 0.4s;"></div></div>'
        f'<div style="text-align:right;font-family:{T["mono"]};'
        f'font-size:0.60rem;color:{_ru_col};font-weight:700;">{ring_util:.1f}%</div>',
        unsafe_allow_html=True,
    )

    # ── Latency histogram (p50 / p95 / p99) ──────────────────────────────────
    _lat99_col = T["red"]    if alerts.get("lat_p99") else T["text"]
    _lat_max   = max(lat_p99, 1.0)
    st.markdown(
        f'<div style="font-family:{T["mono"]};font-size:0.60rem;'
        f'color:{T["muted"]};margin-top:4px;">LATENCY (µs)</div>',
        unsafe_allow_html=True,
    )
    for _pct_lbl, _lat_val, _is_alert in (
        ("p50", lat_p50, False),
        ("p95", lat_p95, False),
        ("p99", lat_p99, alerts.get("lat_p99", False)),
    ):
        _bar_w   = min(100, _lat_val / _lat_max * 100)
        _val_col = T["red"] if _is_alert else T["text"]
        _bar_col = T["red"] if _is_alert else T["blue"]
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:4px;margin:1px 0;">'
            f'<span style="font-family:{T["mono"]};font-size:0.58rem;'
            f'color:{T["muted"]};width:22px;">{_pct_lbl}</span>'
            f'<div style="flex:1;background:{T["surface"]};border-radius:2px;height:5px;">'
            f'<div style="background:{_bar_col};width:{_bar_w:.1f}%;height:100%;"></div></div>'
            f'<span style="font-family:{T["mono"]};font-size:0.58rem;'
            f'color:{_val_col};font-weight:700;width:44px;text-align:right;">'
            f'{_lat_val:.1f}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Event counter ─────────────────────────────────────────────────────────
    st.markdown(
        f'<div style="display:flex;justify-content:space-between;'
        f'font-family:{T["mono"]};font-size:0.58rem;color:{T["muted"]};margin-top:3px;">'
        f'<span>EVENTS</span>'
        f'<span style="color:{T["text2"]};font-weight:700;">{ev_count:,}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# FRAGMENT: WAR ROOM LOG
# ══════════════════════════════════════════════════════════════════════════════

@st.fragment(run_every=FRAG_WARROOM)
def render_war_room_log() -> None:
    import re as _re

    _LOG_RE = _re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[,\.]\d+)?"
        r"(?:\s+\[executor\])?\s*(CRITICAL|ERROR|WARNING|INFO|DEBUG)\s*:?\s*(.*)",
        _re.IGNORECASE,
    )

    def _hi_tickers(msg: str) -> str:
        for tk in _WARROOM_TICKERS:
            msg = _re.sub(rf"\b({tk})\b", r"<b>\1</b>", msg)
        return msg

    st.markdown(
        f'<div style="display:flex;align-items:center;gap:8px;padding:6px 12px;'
        f'background:{T["surface"]};border:1px solid {T["border"]};'
        f'border-radius:4px 4px 0 0;margin-top:8px;">'
        f'<span style="font-size:1.0rem;">🖥</span>'
        f'<span style="font-family:{T["mono"]};font-size:0.76rem;font-weight:600;'
        f'color:{T["cyan"]};letter-spacing:0.08em;">WAR ROOM · executor.log (last 20 lines)</span>'
        f'<span style="font-family:{T["mono"]};font-size:0.65rem;color:{T["muted"]};'
        f'margin-left:auto;">⟳ {FRAG_WARROOM}s</span></div>',
        unsafe_allow_html=True,
    )

    try:
        if not EXECUTOR_LOG_PATH.exists():
            st.info("⏳ executor.log not yet written — bot starting up.", icon="📄")
            return

        try:
            with EXECUTOR_LOG_PATH.open("rb") as lf:
                lf.seek(0, 2)
                log_size = lf.tell()
                lf.seek(max(0, log_size - _MAX_LOG_BYTES))
                raw_bytes = lf.read()

            # [FIX-OPS-04] Multi-encoding decode — handles latin-1 from external libs
            for _enc in ("utf-8", "utf-8-sig", "latin-1"):
                try:
                    _decoded = raw_bytes.decode(_enc)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                _decoded = raw_bytes.decode("utf-8", errors="replace")
            all_lines = _decoded.splitlines()
            if log_size > _MAX_LOG_BYTES and all_lines:
                all_lines = all_lines[1:]

        except Exception:
            log.error("render_war_room_log: bounded read failed", exc_info=True)
            st.info("War room log temporarily unavailable.", icon="📄")
            return

        tail = all_lines[-20:] if len(all_lines) > 20 else all_lines
        if not tail:
            st.info("executor.log is empty.", icon="📄")
            return

        rows_html: list[str] = []
        for raw_line in tail:
            m = _LOG_RE.match(raw_line.strip())
            if m:
                level   = m.group(1).upper()
                message = m.group(2).strip()
            else:
                import re as _re2
                level   = "INFO"
                message = _re2.sub(
                    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[,\.]\d+)?"
                    r"\s*(?:\[executor\])?\s*", "", raw_line.strip()
                ) or raw_line.strip()

            if level in ("CRITICAL", "ERROR"):
                lv_html = f'<span style="color:{T["red"]};font-weight:700;">🔴 {level}</span>'
                msg_col, msg_w = T["red"], "700"
            elif level == "WARNING":
                lv_html = f'<span style="color:{T["yellow"]};font-weight:700;">🟡 {level}</span>'
                msg_col, msg_w = T["yellow"], "600"
            elif level == "INFO":
                lv_html = f'<span style="color:{T["green"]};">🟢 {level}</span>'
                msg_col, msg_w = T["text"], "400"
            else:
                lv_html = f'<span style="color:{T["muted"]};">⚪ {level}</span>'
                msg_col, msg_w = T["text2"], "400"

            safe_msg = _hi_tickers(_esc(message))
            rows_html.append(
                f"<tr>"
                f"<td style='padding:3px 10px;white-space:nowrap;vertical-align:top;"
                f"border-bottom:1px solid {T['border']};'>{lv_html}</td>"
                f"<td style='padding:3px 10px;vertical-align:top;"
                f"border-bottom:1px solid {T['border']};word-break:break-word;'>"
                f"<span style='color:{msg_col};font-weight:{msg_w};'>{safe_msg}</span></td>"
                f"</tr>"
            )

        st.markdown(
            f"<div style='background:{T['bg']};border:1px solid {T['border']};"
            f"border-top:none;border-radius:0 0 4px 4px;"
            f"font-family:{T['mono']};font-size:0.70rem;line-height:1.55;"
            f"max-height:320px;overflow-y:auto;overflow-x:auto;'>"
            f"<table style='width:100%;border-collapse:collapse;'>"
            f"<thead><tr style='border-bottom:2px solid {T['border2']};'>"
            f"<th style='padding:4px 10px;text-align:left;color:{T['muted']};font-size:0.65rem;'>LEVEL</th>"
            f"<th style='padding:4px 10px;text-align:left;color:{T['muted']};font-size:0.65rem;'>MESSAGE</th>"
            f"</tr></thead>"
            f"<tbody>{''.join(rows_html)}</tbody>"
            f"</table></div>",
            unsafe_allow_html=True,
        )

    except PermissionError:
        st.warning("⚠ War Room log access denied.", icon="🔒")
    except Exception:
        log.error("render_war_room_log: unexpected error", exc_info=True)
        st.caption("⚠ War Room log temporarily unavailable.")


# ══════════════════════════════════════════════════════════════════════════════
# TRADE TOAST CHECKER
# ══════════════════════════════════════════════════════════════════════════════

def _check_trade_toasts() -> None:
    prev = st.session_state.get("_toast_last_trade_ts", 0.0)
    df   = st.session_state.get("_lkg_trades", pd.DataFrame())
    if df.empty or "ts" not in df.columns:
        return
    latest = df["ts"].max()
    if latest and float(latest) > float(prev) + 1.0:
        row   = df.loc[df["ts"] == latest].iloc[0]
        sym   = _norm_sym(str(row.get("symbol", "?")))
        sd    = str(row.get("side", "?")).upper()
        pnl   = row.get("realized_usd")
        pnl_s = f" · PnL ${_safe_float(pnl):+,.2f}" if pd.notna(pnl) and pnl else ""
        icon  = "✅" if _safe_float(pnl) >= 0 else "🔴"
        st.toast(f"{icon} TRADE: {sym} {sd}{pnl_s}", icon="💹")
        st.session_state["_toast_last_trade_ts"] = float(latest)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    _setup_operator_audit_log()
    if not _auth_gate():
        return

    if _SS_UI_SHADOW not in st.session_state:
        st.session_state[_SS_UI_SHADOW] = {
            "account": {},
            "positions": [],
            "p17_intelligence": {},
            "p22": {},
            "p21_execution": {},
            "oracle_signals": {}
        }
    _tab_state = _detect_tab_visibility()
    _install_autorefresh(tab=_tab_state)
    _handle_tab_transition(_tab_state)

    _raw_status, _is_live = _load_status()

    if _raw_status:
        st.session_state[_SS_UI_SHADOW]      = _raw_status
        st.session_state[_SS_FRAME_SNAPSHOT] = _raw_status
        st.session_state[_SS_FRAME_TS]       = time.time()
        status      = _raw_status
        data_source = "live" if _is_live else "retained"
    else:
        _shadow = st.session_state.get(_SS_UI_SHADOW)
        if _shadow:
            st.session_state[_SS_FRAME_SNAPSHOT] = _shadow
            status      = _shadow
            data_source = "retained"
        else:
            _inject_css()
            st.warning("⏳ Waiting for bot data — trader_status.json not yet written…")
            st.caption("Dashboard will auto-refresh when data is available.")
            return

    status = _inject_lkg(status)
    st.session_state[_SS_FRAME_SNAPSHOT] = status

    p22          = status.get("p22", {})
    pl           = p22.get("panic_lock", {}) if isinstance(p22, dict) else {}
    panic_active = bool(pl.get("locked", False)) if isinstance(pl, dict) else False
    stale        = _status_age_secs() > STATUS_STALE

    _inject_css(panic=panic_active, stale=stale)
    _check_trade_toasts()

    pos_syms = list(status.get("positions", {}).keys()) if isinstance(status.get("positions"), dict) else []
    symbols  = pos_syms if pos_syms else ["BTC", "ETH", "XRP"]

    # ── SIDEBARS ──────────────────────────────────────────────────────────────
    render_tactical_sidebar()
    render_systemic_defense_sidebar(status)

    # ── [BAND 0] HUD BAR ──────────────────────────────────────────────────────
    render_hud_bar()

    # ── [BAND 1] HEADER KPIs ──────────────────────────────────────────────────
    with st.container(border=True):
        render_header_kpis()

    # ── [BAND 2] FLASH SENTIMENT TICKER ───────────────────────────────────────
    render_flash_feed()

    st.markdown(
        f"<div style='height:4px;background:linear-gradient(90deg,"
        f"{T['border']},transparent);margin:4px 0 8px 0;'></div>",
        unsafe_allow_html=True,
    )

    # ── [BAND 3] MAIN GRID — LEFT 65% | RIGHT 35% ────────────────────────────
    col_left, col_right = st.columns([13, 7], gap="small")

    # ─── STAGE-LEFT ───────────────────────────────────────────────────────────
    with col_left:

        with st.container(border=True):
            render_equity_curve()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_p21_execution()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_positions_panel()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_trade_analytics()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_profitability_diagnostics()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        # [TRACK02] Trade replay / shadow review panel
        with st.container(border=True):
            render_trade_replay()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        # [TRACK13] Operator review report
        with st.container(border=True):
            render_operator_report()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_policy_state()

    # ─── STAGE-RIGHT ──────────────────────────────────────────────────────────
    with col_right:

        with st.container(border=True):
            render_p17_intelligence()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_whale_tape()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_p15_oracle()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_veto_audit()

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_decision_trace()    # [TRACK09-DT]

        st.markdown("<div style='height:6px;'></div>", unsafe_allow_html=True)

        with st.container(border=True):
            render_decision_trace_history()    # [TRACK10-DT]

    # ── [BAND 4] P22 UNIFIED ─────────────────────────────────────────────────
    st.markdown(
        f"<hr style='border-color:{T['border']};margin:10px 0;'>",
        unsafe_allow_html=True,
    )
    with st.container(border=True):
        render_p22_unified()

    # ── [BAND 5] P20 COUNCIL + P13 TRUST SCORES ──────────────────────────────
    st.markdown(
        f"<hr style='border-color:{T['border']};margin:8px 0;'>",
        unsafe_allow_html=True,
    )
    c_council, c_trust = st.columns([3, 2], gap="small")
    with c_council:
        with st.container(border=True):
            render_p20_council()
    with c_trust:
        with st.container(border=True):
            render_p13_trust()

    # ── [BAND 6] MACRO PULSE + CORRELATION MATRIX ────────────────────────────
    st.markdown(
        f"<hr style='border-color:{T['border']};margin:8px 0;'>",
        unsafe_allow_html=True,
    )
    c_pulse, c_corr = st.columns([1, 1], gap="small")
    with c_pulse:
        with st.container(border=True):
            render_macro_pulse()
    with c_corr:
        with st.container(border=True):
            render_correlation(symbols)

    # ── [BAND 7] BOTTOM ANALYTICS BLOCK ───────────────────────────────────────
    st.markdown(
        f"<hr style='border-color:{T['border']};margin:8px 0;'>",
        unsafe_allow_html=True,
    )

    with st.container(border=True):
        render_regime_panel()

    st.markdown(
        f"<hr style='border-color:{T['border']};margin:6px 0;'>",
        unsafe_allow_html=True,
    )

    with st.container(border=True):
        render_liq_heatmap()


    st.markdown(
        f"<hr style='border-color:{T['border']};margin:6px 0;'>",
        unsafe_allow_html=True,
    )

    # [7-C] Candlestick — OHLCV + Trade Overlays
    # [FIX-CRIT-07] _norm_sym() applied on both sides of symbol comparison
    # [FIX-QA-08]   _fig_base(subplot=True) — no global yaxis.side override
    with st.container(border=True):
        render_candlestick(symbols)

    st.markdown(
        f"<hr style='border-color:{T['border']};margin:6px 0;'>",
        unsafe_allow_html=True,
    )

    # [7-D] Shadow Audit Decision Log
    # [FIX-PERF-04] Now a @st.fragment — not re-run on every main() rerun
    with st.container(border=False):
        render_shadow_audit(n_rows=8)

    # ── [BAND 8.5] Bloomberg-style 3-row panel grid ──────────────────────────
    # ROW A — Infrastructure & Policy:  P40.5 · P44 · P46 · P48
    # ROW B — Bridge Execution Layer:   P49 · P50 · P51 · P52
    # ROW C — Protocol & Bypass:        P53 · P54
    # Each row has its own header label, consistent internal column widths,
    # and st.container(border=True) panels for a Bloomberg terminal aesthetic.
    # ─────────────────────────────────────────────────────────────────────────
    st.markdown(
        f"<hr style='border-color:{T['border']};margin:8px 0;'>",
        unsafe_allow_html=True,
    )

    # ── ROW A: Infrastructure & Policy ────────────────────────────────────────
    st.markdown(
        f'<div class="ot-bband-header">'
        f'⬡ INFRASTRUCTURE & POLICY'
        f'<span class="ot-bband-sep">·</span>'
        f'<span style="font-weight:400;color:{T["muted"]};">IPC · SUPERVISOR · EXECUTION TELEMETRY · FLOATING</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    # P40.5 and P44 are content-heavy; P46 is medium; P48 is compact
    c_p405, c_p44, c_p46, c_p48 = st.columns([1.30, 1.45, 1.10, 0.80], gap="small")
    with c_p405:
        with st.container(border=True):
            render_p40_5_heartbeat()
    with c_p44:
        with st.container(border=True):
            render_p44_supervisor()
    with c_p46:
        with st.container(border=True):
            render_p46_telemetry()
    with c_p48:
        with st.container(border=True):
            render_p48_floating()

    # ── ROW B: Bridge Execution Layer ─────────────────────────────────────────
    st.markdown(
        f'<div class="ot-bband-header" style="margin-top:10px;">'
        f'⬡ BRIDGE EXECUTION LAYER'
        f'<span class="ot-bband-sep">·</span>'
        f'<span style="font-weight:400;color:{T["muted"]};">P49 PROTOCOL · P50 HOT-PATH · P51 ALGO · P52 SUPERVISOR</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    c_p49, c_p50, c_p51, c_p52 = st.columns([1.0, 1.0, 1.0, 1.0], gap="small")
    with c_p49:
        with st.container(border=True):
            render_p49_protocol()
    with c_p50:
        with st.container(border=True):
            render_p50_native()
    with c_p51:
        with st.container(border=True):
            render_p51_algo()
    with c_p52:
        with st.container(border=True):
            render_p52_supervisor()

    # ── ROW C: Protocol & Bypass ──────────────────────────────────────────────
    st.markdown(
        f'<div class="ot-bband-header" style="margin-top:10px;">'
        f'⬡ PROTOCOL & BYPASS'
        f'<span class="ot-bband-sep">·</span>'
        f'<span style="font-weight:400;color:{T["muted"]};">P53 MESH CONSENSUS · P54 KERNEL BYPASS</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    c_p53, c_p54 = st.columns([1.6, 1.0], gap="small")
    with c_p53:
        with st.container(border=True):
            render_p53_mesh()
    with c_p54:
        with st.container(border=True):
            render_p54_kernel_bypass()

    # ── [BAND 8] WAR ROOM — isolated @st.fragment, no scroll-reset ───────────
    # Must be called at top-level scope (not nested inside another fragment
    # or st.container) to satisfy Streamlit ≥ 1.37 fragment isolation rules.
    # main() is invoked directly from __main__ so this IS top-level at runtime.
    render_war_room_log()

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown(
        f'<p style="color:{T["muted"]};font-family:{T["mono"]};font-size:0.58rem;'
        f'text-align:right;margin-top:10px;letter-spacing:0.04em;">'
        f'⟳ v53.0 · OBSIDIAN TERMINAL · STASH-GATE · ANTI-ZIGZAG-LOCK · '
        f'EQUITY-CHART-LATCH · PANEL-STABILITY · NULL-KEY-PROTECTION · '
        f'SHADOW-BUFFER · ZERO-FLICKER · P40-INSTITUTIONAL-REBUILD · '
        f'P40.5-BRIDGE-RTT · P44-SUPERVISOR-DASHBOARD · P46-EXEC-TELEMETRY · P42-CORR-GATE · '
        f'P47-RUST-MICROSTRUCTURE · P48-AUTONOMOUS-FLOATING · '
        f'P49-HYBRID-BRIDGE-CORE · P49-LP-FRAMING · P49-CAP-HANDSHAKE · P49-BRAIN-STATE · '
        f'P50-NATIVE-HOT-PATH · P50-SHADOW-AGREE · P50-LATENCY-LADDER · '
        f'P51-INST-EXEC-SUITE · P51-TWAP-VWAP · P51-ICEBERG · P51-CANCEL-ALGO · P51-SHADOW-COMPARE · '
        f'P52-ENGINE-SUPERVISOR · P52-UPTIME-PANEL · P52-CYCLE-COUNTER · P52-SYMBOL-LIST · P52-BRIDGE-LINK · '
        f'P53-MESH-CONSENSUS · P53-2OF3-QUORUM · P53-VPIN-GATE · P53-HMAC-SIGNING · P53-ANTIREPLAY · P53-NTP-SYNC · '
        f'P54-KERNEL-BYPASS · P54-DPDK · P54-ONLOAD · P54-RING-TELEMETRY · P54-LAT-HISTOGRAM · P54-PTP-HOOK · P54-PENDING-STATE · '
        f'FIX:[CRIT-01..08|ARCH-01..05|QA-01..08|OPS-01..06|PERF-01..04|SEC-01|INST-02|P44-DASH|P46-DASH|P42-DASH|P40.5-DASH|P48-DASH|P49-DASH|P50-DASH|P51-DASH|P52-DASH|P53-DASH|P54-DASH|FSTR-001..005|WIN-ATOMIC|PNL-FILL|P54-PENDING|UX-BLOOMBERG|FLICKER-FRAG] · '
        f'TRACK22-DRL-SYM-DIR · '
        f'{datetime.now().strftime("%H:%M:%S")} · '
        f'tab={_esc(_tab_state)} · src={_esc(data_source)}</p>',
        unsafe_allow_html=True,
    )

if __name__ == "__main__":
    main()
