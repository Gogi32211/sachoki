"""Capit→Atom finds good DAYS, not good stocks (260717_jrnl_6yr_bench). So: keep its
TIMING, swap its SELECTION for ours ($21-89 · 🏆RS · 🎯Cluster) and see if the two halves
compose.

Method — the exit is held CONSTANT at the journal's rule (next-open entry, -15% stop /
+100% target / 20-bar, stop-first, gap-aware) for every arm, so the only thing varying is
WHICH stock is bought. Outcomes come from journal_bench's vectorised per-(ticker,date)
table, so every arm is directly comparable to the same random-basket baseline.

Arms, each measured on the confluence's own firing days AND on all days (the control that
tells us whether the day-timing is doing any work):
  random     — every liquid ticker            (the baseline: what the day itself pays)
  confluence — the tickers the tab actually buys
  $21-89 · RS · Cluster · and their stacks    — our validated selection axes
"""
import sys
import numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
from ai_journal.atomic_journal import replay
import journal_bench as JB
import edge_replay as ER

# ── 1. the confluence's firing days + the tickers it buys ────────────────────────────
cf = pd.DataFrame(replay(months=72, min_score=70, conf_only=True, limit=1_000_000)["trades"])
cf["signal_date"] = cf["signal_date"].astype(str).str[:10]
day_n = cf["signal_date"].value_counts()               # intensity = how many fired that day
CDAYS = set(day_n.index)
CONF = set(zip(cf["ticker"], cf["signal_date"]))
print(f"confluence: {len(cf)} trades over {len(CDAYS)} firing days "
      f"({day_n.median():.0f} median/day, {day_n.max()} max)", flush=True)

# ── 2. our selection axes, from the prepped 6yr edge frame ───────────────────────────
grp, as_of = ER._frame(72, 3_000_000)
print(f"edge frame: {len(grp)} tickers, as_of {as_of}", flush=True)
sel = []
for tk, g in grp.items():
    d = g["date"].astype(str).str[:10]
    sel.append(pd.DataFrame({
        "ticker": tk, "date": d,
        "px2189": g["close"].between(21, 89).to_numpy(),
        "rs": g["rs_intact"].fillna(False).to_numpy().astype(bool),
        "clus": (g["conf_n"] >= 3).to_numpy(),
    }))
S = pd.concat(sel, ignore_index=True)
print(f"selector table: {len(S)} rows", flush=True)

# ── 3. the journal-rule outcome for every (ticker, date) ─────────────────────────────
a = get_analytics_conn()
bars = a.execute(f"""
    WITH r AS (SELECT ticker, date, open, high, low, close, close*volume dv,
        row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
      FROM bars WHERE close > 0 AND date >= DATE '{as_of}' - INTERVAL 2270 DAY)
    SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
a.close()
bars["date"] = bars["date"].astype(str).str[:10]
parts = []
for tk, g in bars.groupby("ticker", sort=False):
    g = g.reset_index(drop=True)
    parts.append(pd.DataFrame({"ticker": tk, "date": g["date"], "dv": g["dv"],
                               "ret": JB._outcomes(g, 0.15, 1.00)}))
O = pd.concat(parts, ignore_index=True)
O = O[O["ret"].notna() & (O["dv"] >= 3_000_000)]
print(f"outcomes: {len(O)} liquid (ticker,date)", flush=True)

M = O.merge(S, on=["ticker", "date"], how="left")
for c in ("px2189", "rs", "clus"):
    M[c] = M[c].fillna(False)
M["conf"] = [(t, d) in CONF for t, d in zip(M.ticker, M.date)]
M["cday"] = M["date"].isin(CDAYS)
M["yr"] = M["date"].str[:4]
print(f"merged: {len(M)}  · on confluence days: {int(M.cday.sum())}", flush=True)

# ── 4. compare ───────────────────────────────────────────────────────────────────────
ARMS = [
    ("random (= the day itself)", lambda m: pd.Series(True, index=m.index)),
    ("confluence tickers",        lambda m: m.conf),
    ("$21-89",                    lambda m: m.px2189),
    ("🏆RS",                      lambda m: m.rs),
    ("🎯Cluster ×3+",             lambda m: m.clus),
    ("$21-89 + RS",               lambda m: m.px2189 & m.rs),
    ("$21-89 + RS + Cluster",     lambda m: m.px2189 & m.rs & m.clus),
]

def line(lab, sub, base_mean, base_win):
    if len(sub) < 30:
        print(f"  {lab:26} n={len(sub):6}  — too few"); return
    w, mn = (sub.ret > 0).mean()*100, sub.ret.mean()
    print(f"  {lab:26} n={len(sub):6} | win {w:5.1f}% ({w-base_win:+5.1f}) | "
          f"mean {mn:+6.2f}% ({mn-base_mean:+6.2f})")

for scope, mask in (("CONFLUENCE DAYS ONLY", M.cday), ("ALL DAYS (control)", pd.Series(True, index=M.index))):
    m = M[mask]
    base = m.ret
    bm, bw = base.mean(), (base > 0).mean()*100
    print("\n" + "=" * 92)
    print(f"{scope} — same exit for every arm; (Δ) = vs the random basket on those same days")
    print(f"  random basket on these days: win {bw:.1f}%  mean {bm:+.2f}%   [n={len(m)}]")
    print("=" * 92)
    for lab, f in ARMS:
        line(lab, m[f(m)], bm, bw)

# ── 5. the composition question, per year ────────────────────────────────────────────
print("\n" + "=" * 92)
print("$21-89 + RS + Cluster — does the confluence's TIMING add on top of OUR selection?")
print("=" * 92)
print(f"{'year':6} | {'on conf-days':>26} | {'on all days':>26} | {'timing edge':>12}")
for y in sorted(M.yr.unique()):
    my = M[M.yr == y]
    pick = my.px2189 & my.rs & my.clus
    on, off = my[pick & my.cday], my[pick & ~my.cday]
    if len(on) < 20 or len(off) < 20:
        print(f"{y:6} | n={len(on)}/{len(off)} too few"); continue
    print(f"{y:6} | n={len(on):5} mean {on.ret.mean():+6.2f}% win {(on.ret>0).mean()*100:4.1f}% "
          f"| n={len(off):5} mean {off.ret.mean():+6.2f}% win {(off.ret>0).mean()*100:4.1f}% "
          f"| {on.ret.mean()-off.ret.mean():+6.2f}pp")
