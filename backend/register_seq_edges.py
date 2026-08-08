"""One-shot: push the 2026-08-02..04 session's validated builds into the brain registry.

Seven edges (2 core + 5 user-contract watch) + the prefix-grammar law. Stats are copied
verbatim from the validation comments in edge_replay.py — single source of truth there.
Run once: .venv/bin/python register_seq_edges.py
"""
from brain.registry import record

# ── core tier (passed every gate) ────────────────────────────────────────────────
record("t2gsand_rs", "edge", "🥪 T2G-Sandwich 🏆RS",
       definition=("T2G→T10→T2G ladder (distribution bar swallowed between two gap-up "
                   "closes), RSI>=70, rs_intact, $21-89. First OVERBOUGHT-momentum edge: "
                   "strength buyable only after a tested-and-absorbed washout. "
                   "RS is REQUIRED — without it med −1.74/worst −11.5."),
       layer=3, action="signal", tier="core", status="live", col="E_t2gsand_rs",
       source="project_t2g_sandwich", date="2026-08-02",
       stats={"median": 1.70, "win": 57, "pf": 1.93, "years_pos": "5/5",
              "worst_year": 0.0, "dsr": 0.947})

record("t1gnb_rs", "edge", "🪨 T1G-NB 🏆RS",
       definition=("Gap-up T1 with the NB suffix (no-effort + both wicks): the gap tested "
                   "both ways on no effort and HELD. rs_intact, $21-89. Suffix league's only "
                   "double-REAL; the SAME suffix on gapless T1 is a 6/6 suppressor (gate_t1nb) "
                   "— gap context flips the meaning. RS REQUIRED (without it worst −4.8)."),
       layer=3, action="signal", tier="core", status="live", col="E_t1gnb_rs",
       source="project_t1g_nb_suffix", date="2026-08-03",
       stats={"median": 2.42, "win": 56.9, "pf": 1.91, "years_pos": "5/5",
              "worst_year": 2.3, "dsr": 0.982})

# ── watch tier — the user's explicit trade contract (2026-08-04) ─────────────────
# All five: selection-circular (tops of same-window sweeps), era-tilted (2025-heavy),
# failed the worst>=−2 gate in at least one form. Traded BY USER DECISION with structural
# stops; spine already halves watch-tier size and drops it in robust_only regimes.
# Revisit after ~6 months of live fires.
_WATCH_NOTE = "user-contract watch: era-tilt accepted, structural stop, revisit ~2027-02"

record("z1gt4", "edge", "🌉 Z1G→T4 🟡watch",
       definition=("3 four-bar sequences ending T4, Z1G in prefix: T6→Z1G→Z2G→T4 · "
                   "Z1G→T1G→Z5→T4 · Z1G→Z6→Z2G→T4. $21-377. " + _WATCH_NOTE),
       layer=3, action="signal", tier="watch", status="live", col="E_z1gt4",
       source="project_t1g_nb_suffix", date="2026-08-04",
       stats={"median": 7.74, "win": 64.5, "pf": 5.5, "years_pos": "5/6",
              "worst_year": -3.2, "n": 166, "era_note": "2025 +29.4"})

record("z9hl", "edge", "🧲 Z9-HL 🟡watch",
       definition=("Higher-low grammar: reversal → absorbed Z9 retest → reversal. "
                   "Z3→T4→Z9→T3 · T4→Z9→T3→Z5 (the two biggest-n cells). $21-377. "
                   + _WATCH_NOTE),
       layer=3, action="signal", tier="watch", status="live", col="E_z9hl",
       source="project_t1g_nb_suffix", date="2026-08-04",
       stats={"median": 12.89, "win": 73, "pf": 5.26, "years_pos": "4/6",
              "worst_year": -10.7, "n": 270, "era_note": "worst = 2022"})

record("z1gt36", "edge", "🌉v2 Z1G→T3/T6 🟡watch",
       definition=("Z1G family on new endings: T6→Z1G→Z2G→T3 · T6→Z1G→T5→T6 · "
                   "Z1G→T1→T2G→T6. $21-377. " + _WATCH_NOTE),
       layer=3, action="signal", tier="watch", status="live", col="E_z1gt36",
       source="project_t1g_nb_suffix", date="2026-08-04",
       stats={"median": 15.36, "win": 75.5, "pf": 6.52, "years_pos": "5/6",
              "worst_year": -6.1, "n": 188})

record("seq20", "edge", "🧺 SEQ-20 🟡watch",
       definition=("20 pre-registered remaining top triples of the 8-ending sweep, pooled. "
                   "$21-377. Tamest of the watch builds — one whisker off the worst>=−2 gate. "
                   + _WATCH_NOTE),
       layer=3, action="signal", tier="watch", status="live", col="E_seq20",
       source="project_t1g_nb_suffix", date="2026-08-04",
       stats={"median": 2.92, "win": 56.1, "pf": 1.88, "years_pos": "4/6",
              "worst_year": -2.3, "n": 594})

record("z1gcrown", "edge", "👑 Z1G-CROWN 🟡watch",
       definition=("The big-n family, 9 sequences: double absorbed gap-down → green attempt "
                   "→ SOFT RED entry (Z3/Z4/Z9 endings, no confirmation premium), $8-377, "
                   "PLUS the intraday-anatomy filter: entry-day close must sit in the LOWER "
                   "half of its range (a half-bounced day is the weak subset +3.57 vs +6.66; "
                   "with same-day 1H REV it collapses to +0.98). Same-day 1H REV-turn on a "
                   "compressed close ADDS +2.3pp — reaches the brain via mtf_echo. "
                   + _WATCH_NOTE),
       layer=3, action="signal", tier="watch", status="live", col="E_z1gcrown",
       source="project_t1g_nb_suffix", date="2026-08-04",
       stats={"median": 14.53, "win": 72.9, "pf": 5.83, "years_pos": "3/6",
              "worst_year": -2.5, "n": 853, "era_note": "2025 +26.8; $8-21 slice 5/6yr"})

# ── the law the whole series taught ──────────────────────────────────────────────
record("law_prefix_grammar", "law", "Sequence grammar: the PREFIX carries the information",
       definition=("From the 2026-08-04 prefix-sweep series (~1,200-1,500 cells × 21 endings): "
                   "(1) the prefix decides, the ending only names the entry bar; "
                   "(2) Z1G (absorbed gap-down) is the strongest prefix token — tops 6/9+ "
                   "ending leagues; (3) Z12/Z7 in the prefix = universal poison; "
                   "(4) green chains WITHOUT absorption (e.g. T3→T2) = universal anti-pattern "
                   "(42.8% win, n=5.4k); (5) best entries END on soft red bars (Z3/Z4/Z9) — "
                   "confirmation/gap-up entries pay a premium that kills the edge "
                   "(the T4→T2G family died exactly this way)."),
       layer=3, action="context", status="live",
       source="project_t1g_nb_suffix", date="2026-08-04")

if __name__ == "__main__" or True:
    from brain import registry
    s = registry.summary()
    print("registry after:", s["total"], "findings,", s["live_edges"], "live edges,",
          "by_tier:", s["by_tier"])
    for fid in ("t2gsand_rs", "t1gnb_rs", "z1gt4", "z9hl", "z1gt36", "seq20",
                "z1gcrown", "law_prefix_grammar"):
        r = registry.get(fid)
        print(f"  ✓ {fid:20s} tier={r.get('tier')} col={r.get('col')}")
