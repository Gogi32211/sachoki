"""The Atomic / Capit→Atom journal question, answered by REPLAY over 6 years, per year,
each slice against the EXACT random-basket baseline on its own signal dates (journal_bench).
TRAIN 2021-23 / TEST 2024-26 = the frozen pseudo-OOS."""
import sys; sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
import pandas as pd
from ai_journal.atomic_journal import replay
import journal_bench as JB

for lab, conf in (("⚛️ Atomic (score≥70)", False), ("🔥 Capit→Atom confluence", True)):
    d = replay(months=72, min_score=70, conf_only=conf, limit=1_000_000)
    tr = pd.DataFrame(d["trades"])
    tr["signal_date"] = tr["signal_date"].astype(str).str[:10]
    tr["yr"] = tr["signal_date"].str[:4]
    print("\n" + "=" * 92)
    print(f"{lab} — journal vs a RANDOM basket on its OWN signal dates (same -15%/+100%/20-bar exit)")
    print("=" * 92)
    print(f"{'window':12} {'n':>6} | {'win':>6} {'rand':>6} {'lift':>7} | {'mean':>7} {'rand':>7} {'lift':>7}")
    for w in sorted(tr.yr.unique()) + ["TRAIN 21-23", "TEST 24-26", "ALL 6yr"]:
        if w == "TRAIN 21-23":   sub = tr[tr.yr.isin(["2021","2022","2023"])]
        elif w == "TEST 24-26":  sub = tr[tr.yr.isin(["2024","2025","2026"])]
        elif w == "ALL 6yr":     sub = tr
        else:                    sub = tr[tr.yr == w]
        if len(sub) < 20: continue
        b = JB.baseline(list(sub.signal_date))
        if not b: continue
        jw, jm = (sub.pnl > 0).mean()*100, sub.pnl.mean()
        sep = "  " if w[0].isdigit() else " *"
        print(f"{w:12}{sep}{len(sub):5} | {jw:5.1f}% {b['win']:5.1f}% {jw-b['win']:+6.1f}pp | "
              f"{jm:+6.2f}% {b['mean']:+6.2f}% {jm-b['mean']:+6.2f}pp")
