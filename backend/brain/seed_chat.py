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


# broad / low-precision / booster edges: the brain KNOWS them (memory + revalidate) but the spine
# does NOT fire them standalone — they inform confluence, not "buy this now". Keeps decisions selective.
CONTEXT_ONLY = {"zoneretest", "zoneretest_e", "parabola", "p55", "atomic", "t6_sc_oversold", "spring"}


def run():
    n = 0
    for id, title, col, tier, direction, definition, stats, source in EDGES:
        act = "context" if id in CONTEXT_ONLY else "signal"
        R(id, "edge", title, definition=definition, layer=3, direction=direction,
          action=act, tier=tier, status="live", col=col, stats=stats, source=source); n += 1
    for id, title, action, definition, source in GATES:
        R(id, "gate", title, definition=definition, layer=4, direction="long",
          action=action, tier="core", status="live", source=source); n += 1
    for id, title, definition, source in LAWS:
        R(id, "law", title, definition=definition, direction="both",
          action="context", status="live", source=source); n += 1
    for id, title, definition, source in NULLS:
        R(id, "null", title, definition=definition, direction="none",
          action="disqualify", tier="null", status="live", source=source); n += 1
    for id, typ, title, definition, source in METHODS:
        R(id, typ, title, definition=definition, direction="none",
          action="context", status="live", source=source); n += 1
    return n


if __name__ == "__main__":
    added = run()
    s = registry.summary()
    print(f"seeded/updated {added} findings")
    print("registry now:", s)
