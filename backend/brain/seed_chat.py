"""brain/seed_chat.py — one-shot ABSORPTION of everything this project/chat validated into the
brain's memory (findings.json). Idempotent: registry.register() merges by id, so re-running only
updates. This is the 'brain learns from everything in the chat' step: every edge, booster/gate,
law, null and score-discriminator we proved is written as a queryable finding the decision-spine
and its agents read. Stats come from the project's crystallized memory (per-year path-sim, DSR/PBO).

Tiers: core = robust (≈5-6/6 positive years, worst-year survivable, DSR/ablation-checked);
       watch = real but modest / era-tilted / specific;  null = proven no-edge (never resurrect).
Run:  .venv/bin/python -m brain.seed_chat
"""
from __future__ import annotations
from . import registry

R = registry.record

# ── EDGES (base detectors edge_replay can fire; stats = full-history path-sim) ──────────────
EDGES = [
    # id, title, col, tier, direction, definition, stats, source
    ("l43triple", "L43-TRIPLE", "E_l43triple", "core", "long",
     "L43 VSA-body + reversal-T + gap-sweet; most-validated (MC P>0=100%, ablation, plateau)",
     {"median": 2.13, "pf": 1.65, "pos_years": "5-6/6", "dsr": 0.02}, "project_l43_triple"),
    ("z11t11", "Z11-T11", "E_z11t11", "core", "long",
     "Z11 oversold anchor → T11 confirm; DSR selection-proof",
     {"median": 8.01, "mean": 7.04, "win": 75, "pos_years": "5/5", "dsr": 0.97}, "project_edge_overfit_dsr"),
    ("g3_gap", "G3 gap reclaim", "E_g3", "core", "long",
     "Large gap-up on oversold (RSI<45) + T + non-VB, ANY L; L5 sharpest",
     {"median": 2.15, "win": 59, "pos_years": "6/6"}, "project_g3_gap_reclaim"),
    ("atomic", "Atomic bull", "E_atomic", "core", "long",
     "bull-T close=open + gap + vol=B; real & time-stable; $21-89 quality",
     {"pos_years": "5-6/6"}, "project_atomic_edge_validated"),
    ("g3abs", "⚡G3-Abs", "E_g3abs", "core", "long",
     "same-bar G3 ∧ Atomic-absorption",
     {"median": 4.24, "pf": 1.83, "pos_years": "5-6/6"}, "project_atomic_edge_validated"),
    ("engulfabs", "Engulf-Absorption", "E_engulfabs", "core", "long",
     "bull-T RANGE-engulfs prior 2 bars ≥$21 + RSI<45 swallowing a fresh Edge signal; RANGE≫body",
     {"median": 3.44, "mean": 5.16, "pf": 2.03, "pos_years": "5-6/6", "y2022": 2.19}, "project_engulf_absorption"),
    ("engulfL46", "Engulf-L46 (GEM2)", "E_engulfL46", "core", "long",
     "engulf-absorption on L46 volume-line",
     {"pf": 2.23}, "project_capitulation_bounce"),
    ("washout", "Washout reversal", "E_washout", "core", "long",
     "quality (beta 0.6-1.5, NOT high-beta) oversold in VIX-spike panic; 2022-survivor",
     {"median": 1.73, "win": 57, "pos_years": "6/6"}, "project_washout_reversal"),
    ("h1bottom", "1H-confirmed bottom", "E_h1bottom", "core", "long",
     "1D deep-oversold + 1H VX-climax → R2X reclaim; flips the knife",
     {"median": 1.38, "pos_years": "6/6"}, "project_h1_confirmed_bottom"),
    ("rtb_base", "RTB-Base", "E_rtb_base", "core", "long",
     "RTB phase A/B build/turn + RSI<35 (RTB total/phase ranks BACKWARDS — only this slice works)",
     {"median": 1.77, "pos_years": "5-6/6"}, "project_rtb_score"),
    ("highbase15", "HighBase-15m-Dip", "E_highbase15", "core", "long",
     "held high base: e200↑ · RSI1d 40-60 · ≤15% off 20d-high · green · min 15m-RSI≤28",
     {"median": 0.34, "mean": 1.86, "pf": 1.31, "pos_years": "5-6/6", "sigma": 6.0}, "project_highbase_15m_dip"),
    ("g3g3rl", "G3→G3→RL chain", "E_g3g3rl", "core", "long",
     "two G3 gaps then reclaim-line; strongest gap-chain, plateau-proven",
     {"pf": 3.07, "pos_years": "6/6"}, "project_g3_gapchain"),
    ("g3rl", "G3+RL", "E_g3rl", "core", "long", "G3 gap + reclaim-line",
     {"pf": 2.03}, "project_g3_gapchain"),
    ("zoneretest_e", "Zone-Retest + EDGE-absorb", "E_zoneretest_E", "core", "long",
     "2nd+ touch of 25-bar support that HOLDS (not first knife) WITH an edge-absorption bar",
     {"median": 1.73, "pf": 1.29}, "project_zone_retest"),
    # ── watch (real but modest / era-tilted / specific) ──
    ("spring", "Wyckoff Spring", "E_spring", "watch", "long",
     "buy the SHAKEOUT not breakout: w2_spring + RSI35-45 + T + non-VB (base median-neg; 🔑/🌀SC/🏆RS variants elite)",
     {"median": 1.06, "win": 54, "pos_years": "6/6", "base_median": -1.1}, "project_wyckoff_spring"),
    ("p55", "P55 refined long", "E_p55", "watch", "long",
     "1D+1H P55 exact + absorption-T + non-VB + Z1G/T5 prelude + P→D→P + shallow shakeout",
     {"clip25": 1.0, "pos_years": "5/6"}, "project_p55_setup_refined"),
    ("parabola", "P parabola ride", "E_parabola", "watch", "long",
     "any-P + accumulation fingerprint + 25% TRAILING stop; trend-follow, edge-in-tail",
     {"median": 0.0, "mean": 5.5}, "project_p_parabola_ride"),
    ("zoneretest", "Zone-Retest (base)", "E_zoneretest", "watch", "long",
     "buy 2nd+ touch of 25-bar support that holds; modest median, edge excludes the knife",
     {"median": -0.06}, "project_zone_retest"),
    ("t6_sc_oversold", "T6 @ Wyckoff SC-floor", None, "watch", "long",
     "T6 at Wyckoff SC floor (±5% support) + RSI<40 — the ONE tradeable T4/T6 edge (T4=2025 artifact)",
     {"median": 1.33, "mean": 1.92, "pf": 1.36, "pos_years": "5-6/6"}, "project_t6_sc_oversold"),
]

# ── GATES / BOOSTERS (modify selection & sizing; not standalone signals) ─────────────────────
GATES = [
    ("gate_rs", "🏆 RS gate — worst-year rescuer", "boost",
     "rs=close/benchmark > EMA200(rs) = quality-dip vs structural-knife. UNIVERSAL worst-year rescuer: "
     "makes Cluster/QZ-Capit/G3-Abs/Washout/D+L1/Spring 2022-POSITIVE. Sector-ETF sharper than SPY.",
     "project_rs_gate"),
    ("gate_ob", "🧱 OB gate — amplifier", "boost",
     "order-block retest amplifies momentum edges: G3→G3→RL🧱OB +8.70→+21.4; RTB-Base/D+L1/Cluster/QZ-Capit gain.",
     "project_edge_gate_system"),
    ("gate_key", "🔑 Key-level gate", "boost",
     "support tested ≥2× = real level not knife; sharpens LOCATION-reversal edges (Spring/QZ-Capit/D+L1). "
     "Deep-wick HURTS. Only helps location edges, not momentum.",
     "project_key_level_gate"),
    ("gate_tls", "🎋 TLS (trendline-support) gate", "boost",
     "edge-specific: QZ-Capit🎋TLS +3.22/PF1.57 built; G3-Abs🎋TLS era-tilted=watch.",
     "project_entry_timing"),
    ("gate_charged", "⚡ CHARGED energy state", "context",
     "pre-spike prep (AUC 0.63/6yr/3TF); energy NOT direction; boosts 9/10 setups (Z11 +14.6). Feeder, not timer.",
     "project_charged_energy_state"),
    ("gate_mtf_confirm", "MTF confirmation layer", "boost",
     "strongest VETO: 1D signal with ZERO 4H/1H/15M echo = negative (0/6yr); ≥1 confirm flips positive (5-6/6yr). "
     "4H-trigger early entry +0.84pp. Flags the BASE not breakouts.",
     "project_mtf_confirmation"),
    ("gate_pullback_entry", "Pullback entry timing", "context",
     "UNIVERSAL winner: enter on dip-and-reclaim not strength-chase (+0.7..+1.9pp/6yr matched). Chasing strength LOSES.",
     "project_entry_timing"),
    ("gate_cluster_ladder", "🎯 Confluence-cluster ladder", "boost",
     "≥N distinct edge-FAMILIES in 10-bar window = real bottom; ×3 +3.80 → ×6+ +12.93/med+9.58/win72. "
     "NOT an RSI proxy (4 STATE axes); survives 2022. Buy LOW of the box.",
     "project_confluence_cluster_bottom"),
]

# ── LAWS (priors the agents reason with) ─────────────────────────────────────────────────────
LAWS = [
    ("law_cross_tf_echo", "Real edges echo across timeframes (fractal criterion)",
     "A genuine edge repeats on other TFs; G3 4/5 TFs, Z11 3/4. GEM1 & Z-Absorb are strictly 1D-native "
     "(that's a property, not a flaw). Use echo as a reality-check on new edges.", "project_edge_echo_crosstf"),
    ("law_absorbed_effort", "Absorbed effort = buy; unconfirmed confirmation = noise",
     "Confluence law 1: high effort (volume) absorbed with no result → accumulation → buy. Law 3: paying for "
     "confirmation (breakout/BO/BX/BE arrows) costs edge — the move already happened.", "project_confluence_laws"),
    ("law_conf_over_legacy", "CONF beats every legacy momentum score",
     "All-vs-all confluence score: conditional Spearman +0.13 (~5× ultra_v3), orthogonal to legacy (|ρ|<0.14). "
     "Legacy momentum scores are anti-predictive; use CONF/edges to rank, legacy only as context.", "project_confluence_laws"),
    ("law_fib_price_zone", "Win% rises with price, catastrophe falls",
     "Quality $21-89 win 59-62%/cat~1%; cheap <$8 = lottery (high MEAN illusion, median negative). "
     "Read win%/median/catastrophe BY PRICE bucket, never pooled mean.", "project_fib_price_zones"),
    ("law_survivorship", "Single-name brilliance = survivorship, not edge",
     "RKLB PB/BO, RSI-50 cross, P55 big-growth all looked great on one name, ≈coin-flip universe-wide. "
     "Auto-detection over the whole universe is the only honest test.", "project_trend_pb_bo_survivorship"),
    ("law_season", "Dec-Mar suppresses ALL setups",
     "Dec-Mar kills all 14 setups (median −2..−8, even bull years); Apr-Jun + Sep-Nov 14/14 positive. "
     "Only Z11 & GEM1 tolerate the bad season.", "project_season_gate"),
]

# ── NULLS (proven no-edge — never resurrect) ─────────────────────────────────────────────────
NULLS = [
    ("null_fractal_matching", "Cross-TF price-shape analog matching = no edge",
     "IC≈0 / hit 50% OOS. SHAPE is noise; only purest STATE>SHAPE confirmation survives.", "project_fractal_matching_no_edge"),
    ("null_edge_seq_confluence", "EDGE ∩ robust-sequence = no edge (OOS)",
     "Cross-confirming edge+seq good in-sample, FAILED walk-forward (circular). Don't build.", "project_edge_seq_confluence_no_edge"),
    ("null_lcluster_move", "L-cluster → big-move = no edge",
     "L34/L22/L43/L64 clustering ≠ volatility/direction; coil MUTES moves (MFE 3.5 vs 6.3%).", "project_lcluster_no_edge"),
    ("null_p_signal", "P50/P55/P66 EMA-cross alone = no long edge",
     "Multi-TF confluence ≈0pp; VB volume = fade-trap (MFE-proxy illusion, neg 6/6yr).", "project_p_signal_no_edge"),
    ("null_engulf_goga", "Counting swallowed candles = no edge",
     "No edge any variant; RSI subsumes it. Hand-picked zones = selection bias, auto-detection exposed it.", "project_engulf_goga_no_edge"),
]

# ── METHODOLOGY DISCRIMINATORS (how the brain must measure) ───────────────────────────────────
METHODS = [
    ("method_pathsim_not_proxy", "discriminator", "Path-sim, never MFE-proxy",
     "True bar-by-bar path sim (stop-first), never MFE≥target proxy — proxy inflates hugely "
     "(EMA-breakout proxy +3.4 → path-sim −2.4). Every stat in this brain is path-sim.", "feedback-pathsim-not-mfe-proxy"),
    ("method_price_bucket", "discriminator", "Always segment by price bucket",
     "Test every edge in <8 / 8-21 / 21-89 / 89+ buckets; pooling inflates via the <$8 lottery. "
     "Real edge lives $21-89.", "feedback-price-bucket-always"),
    ("score_rsi_dominates", "discriminator", "BUY score: RSI dominates, legacy anti-predictive",
     "Screener Score = RSI+volB backbone, 2-sided veto (RSI≥60 🔴EXT / <28 🔪KNIFE). Legacy momentum "
     "scores are anti-predictive; LOW momentum predicts better, HIGH = survivorship trap.", "project_buy_score"),
    ("score_ultra_v3", "discriminator", "ULTRA Score v3 = reweighted ranker",
     "old ultra_score anti-predictive (Spearman −0.004); v3 drops dead breakouts + adds oversold/price-zone/"
     "RS/cluster/TLS → Spearman +0.078, Q5−Q1 med +6.74pp/6yr. Modest triage, use as SORT.", "project_ultra_score_v3"),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════
# WEEK OF 2026-07-22 → 07-28 — everything validated since the registry's last entry (07-25).
# Kept as separate lists so the provenance of this batch stays visible; run() merges them.
# ═══════════════════════════════════════════════════════════════════════════════════════════

NEW_EDGES = [
    ("l34cont", "🏆 L34→L34 continuity", "E_l34cont", "core", "long",
     "the SAME L34 VSA volume-line on BOTH the Z-absorption bar and the next-bar T1 demand bar. "
     "The CONTINUITY is the edge — L34 on the T1 bar alone is null (+0.59/med−0.91/4-6yr). "
     "Not RSI-subsumed (2021 flips −2.4→+2.6). L46/L25 never persist across Z→T1; L3→L3 fails.",
     {"median": 0.53, "mean": 2.60, "pos_years": "5-6/6", "y2021": 2.65, "y2022": 2.52,
      "worst_year": -0.5, "dsr": 0.84}, "project_l34_continuity"),
    ("l34cont_rs", "🏆 L34→L34 + RS", "E_l34cont_rs", "core", "long",
     "L34→L34 continuity gated on RS-intact — the flagship variant",
     {"median": 1.90, "pos_years": "5/5", "worst_year": 2.60, "dsr": 0.93}, "project_l34_continuity"),
    ("stopvol_confirm", "🎬 StopVol-Confirm", "E_stopvol_confirm", "core", "long",
     "climax/stopping volume followed by a CONFIRMATION bar. The only pattern of 13 VSA-video "
     "patterns built raw with no priors that survived — 11/13 were coin-flips and the shorts lost. "
     "Confirmation + confluence is what survives, never the raw pattern.",
     {"expR": 0.08, "expR_deep": 0.16, "dsr": 0.997, "novel_vs_capit": 0.74}, "project_vsa_video_edges"),
    ("zrt_l46", "🔁 Zone-Retest × GREEN-L46", "E_zrt_l46", "core", "long",
     "Zone-Retest gated on the L46 volume-line. ZRT was our weakest big edge (4/6yr, worst −1.7) and "
     "RS is a TRAP on it; green-L46 is the right gate. +dwell tier is stronger still.",
     {"median": 1.03, "pf": 1.38, "pos_years": "6/6", "worst_year": 0.6,
      "dwell_median": 1.32, "dwell_pf": 1.48, "dwell_worst": 0.9, "dsr": 1.00, "family_pbo": 0.243},
     "project_zone_retest"),
    ("g3abs_mid", "⚡ G3-Abs × 🕯️mid-close", "E_g3abs_mid", "core", "long",
     "G3-Abs whose close sits in the MIDDLE 38-62% of the bar's own range. The only G3-Abs gate that "
     "is 6/6yr with a positive worst year (🎋TLS 5/6 −1.9 · 🏆RS 4/5 −0.2) and it fires 3× as often as RS. "
     "Survives both price buckets, so uncapped.",
     {"median": 3.43, "win": 59.6, "pf": 2.32, "pos_years": "6/6", "worst_year": 1.5, "dsr": 1.000},
     "project_midclose_gate"),
    ("l43triple_mid", "🧩 L43-TRIPLE × 🕯️mid-close", "E_l43triple_mid", "core", "long",
     "L43-TRIPLE with a mid-range close, $21-89 only ($89-377 is 4/6yr worst −2.0). "
     "Best worst-year on the whole Replay board.",
     {"median": 6.18, "win": 64.0, "pf": 2.85, "pos_years": "6/6", "worst_year": 3.2, "dsr": 0.999},
     "project_midclose_gate"),
    ("washout_vs", "🌊 Washout × 💥 volume event", "E_washout_vs", "core", "long",
     "Washout requiring a real intraday volume event — separates a genuine climax from a slow drift down. "
     "The most dramatic single application of the intraday-volume gate.",
     {"median": 0.42, "win": 50.9, "pf": 1.35, "worst_year": -0.0, "base_worst": -2.2},
     "project_volume_magnitude"),
]

NEW_GATES = [
    ("gate_intraday_vspike", "💥 Intraday volume event (15m max/avg ≥4×)", "boost",
     "THE most UNIVERSAL filter found. Unlike RS/dissonance (edge-specific) it improves EVERY edge tested, "
     "and its absence is a loss cell everywhere. Monotone ladder on raw L46: <2.5× med −5.85/pf0.90 → "
     "2.5-4× −1.31 → 4-6× −0.10 → 6×+ +0.36. Reads: was there a REAL volume event inside the day?",
     "project_volume_magnitude"),
    ("gate_no_vol_event", "⛔ No-volume-event veto (<2.5× intraday)", "disqualify",
     "The severe cell (~3% of days): a flat session with no intraday volume event at all. "
     "L34 with it → pf 0.81, 3/6yr. Treat as a veto, not a weak signal.",
     "project_volume_magnitude"),
    ("gate_dwell", "🔵 Dwell (price hugging the level)", "boost",
     "price within 3% of the 20-bar low on ≥5 of the last 10 bars = a genuinely REPEATED retest. "
     "IDENTICAL to the 'many prior touches' axis (91.3% overlap) — neither pays alone, only the "
     "intersection does, so they are one phenomenon measured two ways. Do not stack them.",
     "project_zone_retest"),
    ("gate_green_l46", "🟢 Green-L46 gate", "boost",
     "a GREEN L46 and a RED L46 are two different signals. Green-L46 rescued Zone-Retest from 4/6yr "
     "worst −1.7 to 6/6yr worst +0.6. Suppressors on green-L46: RSI>55, vol=VB, above-EMA200 — "
     "buying strength kills it, buying the beaten-down base works.",
     "project_zone_retest"),
    ("gate_intraday_l34", "🔎 Intraday demand-line confirmation", "boost",
     "no 15m L34 printed inside the session = validated loss cell on washout (1/6yr, med −5.28, pf 0.72), "
     "ZRT, atomic and D+L1 — but NOT on qzcapit (its no-L34 cell is BETTER) or coilfloor. Edge-specific.",
     "project_zone_retest"),
    ("gate_midclose", "🕯️ Mid-close gate (38-62% of range)", "boost",
     "INVERTED-U: a STRONG close HURTS 6/8 edges (D+L1 −0.90, G3-Abs −0.48, Washout −0.40); the MIDDLE "
     "band pays. Wide plateau — every cut from 30-70 through 45-55 works. Reads as 'demand showed up but "
     "is not yet exhausted'. Edge-specific: G3-Abs/L43 carry it alone; on Washout/ZRT mid-close ALONE is a "
     "TRAP and only pays together with the volume event (those combined variants are DSR 0.54 = watch only).",
     "project_midclose_gate"),
    ("gate_score_hits", "🎲 Score-hits (ranker agreement)", "boost",
     "how many of 6 rankers sit in THEIR OWN measured good zone — NOT 'high is good': ULTRA and BUY are "
     "inverted-U and high prebreak_v2 is the single worst cell in the system. Each component alone is "
     "near-worthless; the AGREEMENT carries. hits 0→5: −1.06/−0.67/+0.00/+1.05/+2.12/+3.79 monotone; "
     "≥4 = 6/6yr with BOTH bear years positive. Zones picked on 2021-23 and the ladder HELD on 2024-26.",
     "project_score_ensemble"),
]

NEW_LAWS = [
    ("law_volume_context", "Volume's sign is CONTEXT-DEPENDENT",
     "On ABSORPTION/reversal bars (L46/L34) 1D volume is an inverted-U: 2-3× avg best, 3×+ worse (huge "
     "volume = climax = exhaustion). On BREAKOUT bars it INVERTS: 1.5-3× is the worst band and only 3×+ is "
     "positive (huge volume = real institutional participation). NEVER carry a volume threshold across bar "
     "contexts — re-measure it per setup type.", "project_zanger_no_edge"),
    ("law_line_colour", "A green L-line and a red L-line are different signals",
     "Colour must be read with the VSA line, not averaged over it. Red-L34 is the absorption partner "
     "(Z-Absorb, L34→L34); GREEN-L46 is the Zone-Retest rescuer; green L34 on a REV flag is a trap. "
     "Whenever an L-line looks null, split it by colour before concluding.", "project_l34_red_triple"),
    ("law_retest_direction", "Retest is NOT direction-agnostic",
     "The IDENTICAL formula pays on support-after-a-drop (+0.46/pf1.23; built tier 6/6yr) and LOSES on "
     "broken-resistance-after-a-breakout (−0.89/pf1.04; the '2nd entry' cell every breakout infographic "
     "pushes is −6.18/win40.6/pf0.97/3-6yr). So the edge was never 'retest' as geometry — it is BUYING "
     "ABSORBED WEAKNESS. An uptrend retest of broken resistance is strength-chasing in retest clothing.",
     "project_zone_retest"),
    ("law_edge_decay", "Best edges COMPRESS, they don't die — decay lives in COMBINATIONS",
     "Excess-slope is now a standard metric. Raw signal alphabet is flat (all 29 TZ/L codes); the strongest "
     "edges compress over time (GEM1 +11.4→+4.7) rather than dying, and only Spring is rising (📈 emerging "
     "tier). The SHORT side is closed: 0/29 codes positive, Z-codes worst.", "project_edge_decay_and_shorts"),
    ("law_atr_time", "Price↔time is governed by VOLATILITY, not by pattern geometry",
     "days to ±X% ≈ k·(X/ATR%)^p, OOS-calibrated (GBM diffusion would predict p=2; empirics are far lower). "
     "Measure MAGNITUDE in ATR-units, not raw %. This is a TIMING tool, never a directional edge — and ATR%% "
     "is a per-NAME property, so it is useless as a per-BAR signal.", "project_atr_time_forecast"),
]

NEW_NULLS = [
    ("null_harmonic_patterns", "Harmonic XABCD (Gartley/Bat/Alt-Bat) = no edge",
     "Tested causally (D = the bar price reaches the projected zone, NOT a pivot — so no ZigZag lookahead). "
     "All patterns ≤ baseline; no plateau. THE CONTROL is decisive: same skeleton and same D-zone with the "
     "Fibonacci ratios REMOVED does BETTER (Gartley −0.57 vs −2.09 with ratios) — the ratios SUBTRACT. "
     "Price law inverted ($21-89 is the WORST bucket) = a reliable noise signature. NB Fibonacci works for "
     "us only as absolute DOLLAR zones, never as retracement ratios.", "project_harmonic_patterns_no_edge"),
    ("null_zanger_breakout", "Dan Zanger flag + volume breakout = no edge",
     "Decomposed rather than tested whole: the tight-consolidation precondition SUBTRACTS (breakout without "
     "a coil beats the coiled one); 'wait for the breakout' is the worst of 3 entries (coil −0.38 · pullback "
     "−0.36 · breakout −0.70); his >50%-above-average volume is the WORST band; his <7% stop is the worst "
     "exit tested (pf 0.93/win 31%). Only 'biggest movers, avoid laggards' survives — and that is our RS gate. "
     "$10k→$42M is n=1 in the 1998-2000 tape.", "project_zanger_no_edge"),
    ("null_breakout_closepos", "'Strong breakout' close-position rule = no edge (raw)",
     "Every 20d/50d breakout × close-position cell is negative and the 'STRONG' bucket (close ≥62% past the "
     "level) is WORSE than baseline. The colour rule runs BACKWARDS: green breakout −0.88 vs red −0.68, and "
     "the picture's best-rated cell (green + holds the level) is the worst at −1.04. The axis was still worth "
     "keeping — as a GATE it became gate_midclose.", "project_midclose_gate"),
    ("null_cup_geometry", "AM-GM / cup geometry (symmetry, measured move) = no edge",
     "Symmetry ❌; measured-move projection ❌ (depth → med −9 trap, recovery time ≈ constant not geometric). "
     "ONLY rounded-vs-V survived, and that is dwell/absorption at the bottom = a coil-floor STATE, not a shape.",
     "project_cup_amgm_geometry"),
    ("null_l2_l4", "L2 / L4 quiet-bar markers = no edge",
     "Rare (L2 ≈1% of L12) but pf<1 and 64% carry vol=L; nothing rescues them. L2 is the daily analogue of "
     "the no-volume-event veto → context, never a signal.", "project_l2_l4_no_edge"),
]

NEW_METHODS = [
    ("method_risk_reward", "discriminator", "Measure risk-reward, not fixed horizons",
     "1d/5d/20d forward returns are the wrong success metric. Every trade now carries exp_r (mean ÷ planned "
     "risk), payoff (avg win ÷ avg loss), sortino, med_mae (heat taken) and med_mfe from the same path-sim.",
     "feedback-pathsim-not-mfe-proxy"),
    ("method_dsr_bar", "discriminator", "DSR ≥0.6 is the trust bar",
     "Deflate every candidate against the FULL family of variants tried, including ones tried-and-discarded. "
     "Worked example: mid-close on G3-Abs 1.000 and L43 0.999 → BUILT; the same gate on Washout/ZRT came in at "
     "0.541/0.537 → NOT built, despite a real-looking worst-year rescue. Legacy pre-path-sim numbers "
     "(e.g. 'VX-PS-R2X +7.87%/97.7%') are MFE-proxy artifacts — under path-sim it is −0.32 and the ordering "
     "against its supposed worst cell even INVERTS.", "project_edge_overfit_dsr"),
    ("method_overlap_check", "discriminator", "Measure overlap BEFORE building",
     "A new-looking gate is usually an old one from another angle. Touch-count vs dwell: 91.3% overlap and "
     "NEITHER pays alone → one phenomenon, and tightening would have made the served edge WORSE (worst-year "
     "+0.7 → +0.2). Same check turned 'green L46 + dwell' into a ZRT gate instead of a new setup (94% overlap).",
     "feedback-analysis-standard"),
    ("method_confluence_first", "discriminator", "A single-axis null is not a combo null",
     "Test the CONFLUENCE before declaring no-edge: MTF-EMA stacks, intraday echo and mid-close were all null "
     "standalone yet real as gates. Conversely test the components before believing a combo: decomposing "
     "Zanger/harmonics showed the distinctive part SUBTRACTS while the surviving part was already ours.",
     "feedback-analysis-standard"),
    ("method_frequency_first", "discriminator", "Count occurrences before believing a 'formation'",
     "A visually distinctive chart formation that occurs 100k+ times is not a formation. Eyes sample by "
     "OUTCOME — you notice the shape on charts where a move followed. Count first, then measure.",
     "feedback-analysis-standard"),
    ("method_trajectory", "discriminator", "Rank by trajectory, not only by level",
     "Markets adapt, so a rare edge that still repeats is worth more than a common one fading. Excess-slope "
     "(edge minus baseline, per year) is now measured alongside median/PF so compression and emergence are "
     "visible rather than hidden inside a 6-year average.", "project_edge_decay_and_shorts"),
]


# broad / low-precision / booster edges: the brain KNOWS them (memory + revalidate) but the spine
# does NOT fire them standalone — they inform confluence, not "buy this now". Keeps decisions selective.
CONTEXT_ONLY = {"zoneretest", "zoneretest_e", "parabola", "p55", "atomic", "t6_sc_oversold", "spring"}


def run():
    n = 0
    for id, title, col, tier, direction, definition, stats, source in EDGES + NEW_EDGES:
        act = "context" if id in CONTEXT_ONLY else "signal"
        R(id, "edge", title, definition=definition, layer=3, direction=direction,
          action=act, tier=tier, status="live", col=col, stats=stats, source=source); n += 1
    for id, title, action, definition, source in GATES + NEW_GATES:
        R(id, "gate", title, definition=definition, layer=4, direction="long",
          action=action, tier="core", status="live", source=source); n += 1
    for id, title, definition, source in LAWS + NEW_LAWS:
        R(id, "law", title, definition=definition, direction="both",
          action="context", status="live", source=source); n += 1
    for id, title, definition, source in NULLS + NEW_NULLS:
        R(id, "null", title, definition=definition, direction="none",
          action="disqualify", tier="null", status="live", source=source); n += 1
    for id, typ, title, definition, source in METHODS + NEW_METHODS:
        R(id, typ, title, definition=definition, direction="none",
          action="context", status="live", source=source); n += 1
    return n


if __name__ == "__main__":
    added = run()
    s = registry.summary()
    print(f"seeded/updated {added} findings")
    print("registry now:", s)
