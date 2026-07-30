"""brain/seed_contrarian.py — the ADVERSARIAL layer: how to not be the crowd.

seed_chat.py taught the brain WHAT works. This teaches it WHY the obvious things do not,
and how to position on the other side of them. Everything here is derived from measured
results in this project — no doctrine without a number behind it.

The single organising claim, and it is empirical rather than philosophical:
  **published geometry carries no forward information; measured STATE does.**
Every pattern that appears in trading infographics (flags, harmonics, cup-and-handle,
measured moves, breakout retests, EMA crosses) came back at or below baseline when tested
honestly. Every edge that survived is a state you cannot screenshot — absorption,
compression, relative-strength integrity, path roughness.

Run:  .venv/bin/python -m brain.seed_contrarian
"""
from __future__ import annotations
from . import registry

R = registry.record

# ── LAWS — the adversarial doctrine ────────────────────────────────────────────────────
LAWS = [
    ("law_smooth_trend_is_the_trap",
     "The smoother a trend LOOKS, the worse it pays — buy roughness",
     "Hurst exponent of the trailing 60-bar path, measured 2026-07-30 on 2.7M bars, is MONOTONE "
     "and points the opposite way to intuition: H<0.35 +0.14 / 0.35-0.45 −0.31 / 0.45-0.55 −0.74 "
     "/ 0.55-0.65 −0.93 / H>0.65 −2.37 (baseline −0.71, win 50.4%→44.8%, pf 1.24→0.90). "
     "Replicates on a 40-bar window. This is NOT volatility in disguise: corr(H, ATR%) = −0.008, "
     "and the H<0.45 vs H>0.55 spread holds INSIDE every ATR%% tercile (lo +0.92/+0.67, "
     "mid +0.47/−0.49, hi −5.51/−8.65). A persistent, low-noise, visually clean advance is where "
     "positioning is already crowded; a rough choppy path is where it is not. Practical rule: "
     "the chart you would screenshot as 'a beautiful trend' is the one to skip.",
     "hurst.py 2026-07-30"),

    ("law_published_geometry_is_empty",
     "If a pattern is in an infographic, assume it has no forward information",
     "Six independent tests, six nulls: cross-TF fractal shape-matching (IC≈0, hit 50% OOS) · "
     "harmonic XABCD — Fib RATIOS actively SUBTRACT vs a control · cup/AM-GM symmetry and "
     "measured-move (depth→median −9) · Dan Zanger flag-breakout (coil subtracts, breakout worst "
     "of 3 entries, 1.5-2× volume the worst band) · the SETUP tokens A/SM/N/MX (0.04pp spread "
     "across four 'different' signals) · the RSI/CCI trendline break (+0.09pp over a FLAT line at "
     "the same pivot). Visual proof: deterministic Koch/Peano/Hilbert curve traversals — zero "
     "market content — produce waveforms indistinguishable from price charts, complete with "
     "double tops, channels and breakouts. If a Koch curve is indistinguishable from AAPL, "
     "'it looks like X' carries no information. STATE > SHAPE, now 9 confirmations.",
     "project_fractal_matching_no_edge + harmonic + cup + zanger + setup tokens"),

    ("law_unmeasured_weights_are_coin_flips",
     "A number nobody measured is as likely to be backwards as forwards",
     "Two independent cases, one week apart. CCI0R carried +5 (HARD_BEAR ×2) and +3 across three "
     "scoring engines: measured empty AND on the wrong side. BB↑ carried +15 in ultra_score — the "
     "LARGEST single bonus in the whole scheme — and measured −1.55 vs a −0.63 baseline, i.e. "
     "actively negative; SVS carried +8 and +5 and measured exactly nil. Both were inherited "
     "Pine heuristics that looked reasonable. Rule: treat every unmeasured weight as unsigned. "
     "Audit them by size — the biggest bonus on the least evidence is the first to check.",
     "project_i_row_signals + a89ef7d"),

    ("law_score_inversion",
     "High momentum scores are a survivorship trap; low scores predict better",
     "Legacy momentum scores rank BACKWARDS (RTB phase/total anti-predictive; ultra_score and "
     "buy_score are inverted-U with their good zone in the MIDDLE; prebreak_v2/v3 anti-predictive). "
     "The 🎲 ensemble only works because each ranker is read in ITS OWN good zone rather than "
     "'higher = better': 0→5 hits moves median −1.06 → +3.79, monotone, and ≥4 hits is 6/6 years. "
     "Whenever a score looks impressive at its top decile, check whether the top decile is just "
     "the names that already ran.",
     "project_score_zones + project_score_ensemble + project_rtb_score"),

    ("law_breakout_family_is_the_wrong_side",
     "The entire breakout alphabet measures at or below zero — systematically",
     "BO/BX/BE = noise (confluence kill-list) · 3G (gap above all 3 fast EMAs) is the WORST "
     "breakout entry at +0.4%/3-6yr while the scoring engine ranked it HIGHEST (+12) · BB↑ "
     "negative (−1.55) · breakout was the worst of three entries in the Zanger decomposition · "
     "the '2nd entry on broken resistance' that every breakout infographic pushes is "
     "−6.18/win40.6/pf0.97 · RSI-50 and all 12 EMA-cross codes (P2/P3/P50/P55/P66/P89 + D mirror) "
     "are indistinguishable from baseline. The same formula that LOSES on broken-resistance PAYS "
     "on support-after-a-drop (+0.46/pf1.23, 6/6yr): the edge was never the geometry, it is "
     "BUYING ABSORBED WEAKNESS. Strength-chasing in retest clothing is still strength-chasing.",
     "project_zone_retest + project_zanger_no_edge + project_p_signal_no_edge"),

    ("law_edge_is_invisible_by_construction",
     "The surviving edges are states you cannot screenshot",
     "Sort the validated book by what it measures and the pattern is total: absorption (L34/L46 "
     "colour-split, engulf-absorption, Z-absorb, D+L1), capitulation (GEM1, washout, QZ-capit), "
     "compression (❄️CONSO, coil-floor, dwell), relative-strength integrity (🏆RS — the universal "
     "worst-year rescuer), cross-timeframe echo, path roughness (H). None of these is a shape; "
     "every one requires measurement over a window. That is a structural reason they survive: "
     "they cannot be eyeballed, therefore they cannot be crowded by eyeballs.",
     "project_what_actually_works"),

    ("law_lower_tf_is_the_information_edge",
     "When only the DAILY shows it, it fails — the daily is the crowded timeframe",
     "The strongest single veto found: a 1D signal with ZERO 4H/1H/15M echo is negative in 0/6 "
     "years; requiring ≥1 lower-TF confirmation flips it positive. The 🕐 1H dual-reclaim gate is "
     "the second universal booster after volume-event: 52 of 63 board setups improve, median "
     "Δ+1.34, and it survives the period-matched control. Mechanism to hold onto: everyone reads "
     "the daily chart, so the daily alone is the picture the crowd already has. Information lives "
     "one timeframe below where the crowd is looking.",
     "project_mtf_confirmation + project_oscillator_divergence_reclaim"),

    ("law_suppressors_beat_signals",
     "Knowing when NOT to trade is worth more than another signal",
     "Measured suppressors, each larger than most edges: Dec-Mar kills ALL 14 setups (even in "
     "bull years; only Z11 and GEM1 tolerate it) · close<EMA200 with e9>e20>e50, the bear-market "
     "rally, costs every setup −1 to −3.3 and is era-independent · no intraday volume EVENT drops "
     "EVERY one of the 29 TZ/L codes by 4-8 points · NOT-CONSO (expansion state) is −3.67/win43.6 "
     "· H>0.65 is −2.37. Compare to a GOOD edge at +2 to +5. The asymmetry means the first "
     "question is never 'what fires?' but 'is this a state where anything works?'",
     "project_season_gate + project_sub200_rally_suppressor + project_i_row_signals"),

    ("law_enter_where_it_hurts",
     "Take the entry the crowd finds uncomfortable — every time it is measured, it wins",
     "Pullback beats strength-chase universally (+0.7 to +1.9pp, 6/6yr; chasing strength LOSES). "
     "Buy the LOW of the confluence box, not the breakout of it. Wyckoff: buy the SHAKEOUT "
     "(spring +1.06/6yr, gap variant +2.87), never the breakout. Zone-retest: buy the 5th+ test "
     "of a support that HOLDS — a level everyone has written off. Capitulation bounce: buy the "
     "SMALL T1 inside a panic, not the big reversal bar. The common shape is that the profitable "
     "entry is the one that looks like it is failing at the moment you take it.",
     "project_entry_timing + project_wyckoff_spring + project_zone_retest + project_capitulation_bounce"),

    ("law_single_name_conviction_is_survivorship",
     "A pattern that is beautiful on one chart is a story, not an edge",
     "RKLB pullback-EMA20 and breakout looked superb on the name and measured ≈0 universe-wide; "
     "dormancy did not rescue it. The RSI-50 cross shows the same asymmetry on single names and "
     "nothing in aggregate. The 'smooth fingerprint' that seemed to select big P55 growth winners "
     "was pure survivor bias. Rule: never accept an idea whose evidence is a chart you were shown. "
     "Re-derive it across the universe, split by price bucket, and read the WORST year.",
     "project_trend_pb_bo_survivorship + project_p55_biggrowth"),
]

# ── GATES built from the two newest measurements ──────────────────────────────────────
GATES = [
    ("gate_hurst_rough", "🌀 Rough-path gate (H<0.45)",
     "Trailing-60-bar Hurst below 0.45 — a choppy, non-persistent path. Improved 9 of 10 base "
     "setups: D+L1 +1.08→+2.19 (worst −0.0) · Washout +0.21→+1.11 · L43-TRIPLE +2.90→+3.59 · "
     "RTB-Base +1.23→+1.91 (6/6, worst +0.6) · Atomic +0.40→+1.02 · G3-Abs +2.05→+2.66. "
     "Its mirror is a WARNING, not a gate: H>0.55 destroys momentum setups — L43-TRIPLE "
     "+2.90→+0.08, D+L1 +1.08→−0.90, G3-Abs +2.05→+0.20. ONE documented exception: Wyckoff "
     "Spring prefers SMOOTH (+0.11→+1.58 with H>0.55; rough HURTS it at −0.57) — a shakeout "
     "inside a controlled range is a different animal. Orthogonal to volatility (corr −0.008).",
     "hurst.py 2026-07-30"),

    ("gate_conso_compression", "❄️ Compression gate (sig_conso)",
     "The combo_engine tight gate: 6-bar range≤3.5% OR ATR%≤3.0 OR |ema9−ema20|/ema20≤2.0. Fires "
     "on 69% of bars, so it is a REGIME and its own median is zero (+0.03) — what carries is its "
     "ABSENCE: NOT-CONSO is −3.67/win 43.6. Helped 8 of 11 base setups (median Δ+0.31) and the "
     "split is mechanical: it helps every absorption/capitulation setup and HURTS the three that "
     "require range EXPANSION by their own definition (Engulf-Abs −1.32, L43-TRIPLE −0.44, "
     "G3-Abs −0.27). Built where it clears 6/6 years with a positive worst year: "
     "Washout🧊 (−0.53→+1.54, worst −2.2→+0.7) and RTB-Base🧊 (+0.72→+1.09, worst −0.1→+0.4).",
     "project_i_row_signals 2026-07-29"),
]

# ── NULLS — never resurrect ────────────────────────────────────────────────────────────
NULLS = [
    ("null_bb_brk", "BB↑ (Bollinger breakout) = NEGATIVE, not merely empty",
     "close>BB_upper(20,2) & volume crossing 1.5× & RSI>55. Path-sim 6yr $21-377: median −1.55 / "
     "win 46.1 / pf 1.05 against a −0.63 baseline — 0.92pp WORSE — and the NOT-BB↑ half is −0.56. "
     "It held the largest single bonus in ultra_score (+15) and a +2 in turbo_engine; both removed "
     "2026-07-29. A textbook strength-chase breakout, which is exactly the sign 'fade strength' "
     "predicts.", "project_i_row_signals"),

    ("null_svs", "SVS (volume expansion on a green bar) = empty",
     "vol/avg20 crossing 1.4× with close>open. −0.69 vs a −0.63 baseline, NOT-SVS −0.53: zero "
     "content. Carried +8 in ultra_score and +5 in prebreak_v3, both removed. It is the BREAKOUT "
     "side of the context-dependent volume law, where volume does not pay.",
     "project_i_row_signals"),

    ("null_um", "UM (established up-move) = the worst of its family, and it POISONS setups",
     "ema9>ema20>ema50 & ROC(40)≥8% & max-vol(40)>1.4×. −1.40 / win 46.5 / 3-6yr, 0.77pp below "
     "baseline while NOT-UM is −0.29. As a gate it destroys validated setups: D+L1 −2.66, "
     "QZ-Capit −2.51. It is the literal definition of 'the trend is already running', and that is "
     "the state to avoid, not to join.", "project_i_row_signals"),

    ("null_setup_tokens", "SETUP tokens A / SM / N / MX = one mask wearing four labels",
     "SM −0.44 · MX −0.46 · A −0.48 · N −0.48 against a −0.63 baseline — a 0.04pp spread across "
     "four supposedly different signals, and every complement lands in the same place (NOT-A "
     "−0.54, NOT-SM −0.55, NOT-N −0.54, NOT-MX −0.55): presence vs absence is worth 0.08pp. "
     "Stacking adds nothing (all four −0.39) and +🏆RS lifts all four to the SAME number "
     "(+0.29/+0.33/+0.31/+0.31). They share preTurnStructure and identical structure gates. "
     "NOTE the meta-lesson: SM is the only absorption-flavoured one and the prior said it would "
     "win — it did not. 'It contains the right idea' does not survive measurement.",
     "setup_tokens.py 2026-07-30"),
]

# ── METHODS — the discipline that keeps the above honest ───────────────────────────────
METHODS = [
    ("method_complement_control", "discriminator", "Compare a filter to its COMPLEMENT, never to the baseline",
     "A filter that keeps 69% of the population can beat the whole population for trivial reasons. "
     "CONSO looked like a weak positive (+0.03 vs −0.63) until it was compared to NOT-CONSO "
     "(−3.67) — which is what made it credible AND reframed it from signal to suppressor. Any "
     "filter retaining more than ~25% of bars must be scored against its own complement.",
     "project_i_row_signals"),

    ("method_orthogonality_check", "discriminator", "Prove a new variable is not an old one renamed",
     "Before believing a new state variable, correlate it with the ones already in the book and "
     "re-test it INSIDE their buckets. H passed: corr(H, ATR%) = −0.008 and the H spread survives "
     "within every ATR%% tercile, so it is genuinely orthogonal to volatility. Had it only paid "
     "ACROSS vol buckets it would have been volatility with a Greek letter.",
     "hurst.py 2026-07-30"),

    ("method_audit_by_bonus_size", "discriminator", "Audit scoring weights largest-first",
     "Both backwards weights found so far were large and unmeasured: CCI0R (+5 doubled in "
     "HARD_BEAR) and BB↑ (+15, the single biggest bonus in ultra_score). Sort every scoring "
     "engine by weight magnitude and measure from the top down; the ranking of unmeasured "
     "confidence is a ranking of risk.", "project_i_row_signals"),

    ("method_frame_column_trap", "discriminator", "A study returning zero fires everywhere is a plumbing bug",
     "edge_replay._pull's OUTER projection is an explicit column list — the SELECT * is only in "
     "the inner CTE. A new DB column that is not added there arrives silently as all-False, and "
     "every variant in the study returns 0 or an identical number. If every arm of an experiment "
     "agrees exactly, suspect the data before the hypothesis.", "i_row_signals.py 2026-07-29"),

    ("method_read_the_worst_year", "discriminator", "Rank by worst year, not by mean",
     "The TIER-1 bar used for every gate built in this project is 5-6 positive years AND a "
     "positive worst year, on base setups only. It is what separated Washout🧊 and RTB-Base🧊 "
     "(both 6/6, worst +0.7/+0.4) from Spring +1.17Δ, QZ-Capit +0.61Δ and Zone-Retest +0.59Δ, "
     "which have larger or comparable lifts but still-negative worst years. A mean is a promise; "
     "a worst year is a survival test.", "project_edge_gate_system"),
]


def run() -> int:
    n = 0
    for id, title, definition, source in LAWS:
        R(id, "law", title, definition=definition, direction="both",
          action="context", status="live", source=source); n += 1
    for id, title, definition, source in GATES:
        R(id, "gate", title, definition=definition, layer=4, direction="long",
          action="boost", tier="core", status="live", source=source); n += 1
    for id, title, definition, source in NULLS:
        R(id, "null", title, definition=definition, direction="none",
          action="disqualify", tier="null", status="live", source=source); n += 1
    for id, typ, title, definition, source in METHODS:
        R(id, typ, title, definition=definition, direction="none",
          action="context", status="live", source=source); n += 1
    return n


if __name__ == "__main__":
    added = run()
    print(f"seeded/updated {added} findings")
    print("registry now:", registry.summary())
