"""Anatomy of the (T10|T11|Z11) → Z2G → T1G chain: how many, how often up, how high.

The spec is exactly as the user built it: all three tokens inside ONE 14-bar window, strict
order, gaps allowed (other bars may sit between them), entry on the T1G bar — the chain
completes there, so nothing looks forward.

When several valid (head, Z2G) pairs exist before the same T1G, we take the TIGHTEST one:
the latest Z2G that still has a head before it, and the latest head before that Z2G. That
makes the two gaps well-defined per occurrence:

        head ──g1── Z2G ──g2── T1G(entry)          g1 + g2 ≤ 13

Two passes, deliberately separated:

  PASS 1 — DESCRIPTIVE. Every occurrence counted, win rate and MFE landscape per variant.
    3 heads × 3 g1 bands × 3 g2 bands = 27 cells. No verdict is drawn here; picking the
    max of 27 cells is a selection, and the winner's apparent edge is partly the selection.

  PASS 2 — the top cells run through research_kit with day-clustered intervals and a trial
    budget that INCLUDES the 27 enumerated cells, so DSR reflects the real search.

MFE is read off the same path the trade actually took (⚡ATR×12 trail, 60-bar cap), so
"peak" means the peak an open position genuinely saw, not a hindsight high.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from research_kit import EdgeStudy          # noqa: E402

pd.set_option("display.width", 230)

W = 14
HEADS = ("T10", "T11", "Z11")


def token_arr(g) -> np.ndarray:
    t = g["t"].astype(str).to_numpy()
    z = g["z"].astype(str).to_numpy()
    out = np.where((t != "") & (t != "nan") & (t != "None"), t, z)
    return np.where((out == "nan") | (out == "None"), "", out)


def chain_detail(g) -> dict:
    """Per-bar: is this T1G the end of an ordered chain, and with what geometry."""
    tok = token_arr(g)
    n = len(tok)
    is_t1g, is_mid, is_head = tok == "T1G", tok == "Z2G", np.isin(tok, HEADS)
    ch = np.zeros(n, bool)
    hd = np.full(n, "", object)
    g1 = np.full(n, 0, np.int16)
    g2 = np.full(n, 0, np.int16)
    for i in np.where(is_t1g)[0]:
        lo = max(0, i - (W - 1))
        mids = np.where(is_mid[lo:i])[0] + lo
        for j in mids[::-1]:                      # tightest Z2G first
            heads = np.where(is_head[lo:j])[0] + lo
            if len(heads):
                k = heads[-1]                     # tightest head before that Z2G
                ch[i], hd[i], g1[i], g2[i] = True, tok[k], j - k, i - j
                break
    return {"chd": ch, "hd": hd, "g1": g1, "g2": g2}


EXTRA = ("chd", "hd", "g1", "g2")


def band(v):
    return np.where(v <= 2, "≤2", np.where(v <= 5, "3-5", "≥6"))


st = EdgeStudy("chain anatomy — count · how often up · peak · which variant is best",
               n_trials=35, extra_sig_cols=EXTRA)
print("computing chain geometry (prior bars only)...", flush=True)
for tk, g in st.grp.items():
    for k, v in chain_detail(g).items():
        g[k] = v
print("done\n", flush=True)

# ── raw occurrence count, BEFORE the 5-bar spacing rule removes overlaps ──────
raw = sum(int(g["chd"].sum()) for g in st.grp.values())
raw_tk = sum(1 for g in st.grp.values() if g["chd"].any())
t1g_raw = sum(int((token_arr(g) == "T1G").sum()) for g in st.grp.values())
print("=" * 118, flush=True)
print(f"  RAW OCCURRENCES (every completed chain in 5 years, no spacing filter)", flush=True)
print(f"    T1G bars in total          {t1g_raw:>9,}", flush=True)
print(f"    of them ending a chain(14) {raw:>9,}   ({raw / t1g_raw:.1%} of all T1G)",
      flush=True)
print(f"    tickers that ever show one {raw_tk:>9,} of {len(st.grp):,}", flush=True)
print("=" * 118 + "\n", flush=True)

st.universe("chain(14) დამთავრებული ბარები", lambda g: g["chd"].astype(bool))
tr = st.trades.copy()
tr["hd"] = tr["sig_hd"].astype(str)
tr["b1"], tr["b2"] = band(tr["sig_g1"].to_numpy()), band(tr["sig_g2"].to_numpy())
tr["yr"] = pd.to_datetime(tr["date_in"]).dt.year
R = tr["ret"] * 100
F = tr["mfe"] * 100
A = tr["mae"] * 100

# ── PASS 1 · descriptive landscape ───────────────────────────────────────────
def line(lbl, m):
    s, f, a = R[m], F[m], A[m]
    if len(s) < 25:
        return f"    {lbl:22s} n={len(s):>6,}  (too thin to read)"
    return (f"    {lbl:22s} n={len(s):>6,}  ↑{(s > 0).mean():>5.1%}  "
            f"med {s.median():>+6.2f}  |  peak: med {f.median():>5.2f} "
            f"p75 {f.quantile(.75):>5.1f} p90 {f.quantile(.90):>5.1f} "
            f"max {f.max():>6.1f}  |  ≥5% {(f >= 5).mean():>5.1%} "
            f"≥10% {(f >= 10).mean():>5.1%} ≥20% {(f >= 20).mean():>5.1%}  |  "
            f"MAE med {a.median():>6.2f}")


print("─" * 118, flush=True)
print("  PASS 1 · DESCRIPTIVE — ↑ = ended positive · peak = MFE the open position "
      "actually saw", flush=True)
print("─" * 118, flush=True)
print(line("ALL chain(14)", pd.Series(True, index=tr.index)), flush=True)
print(flush=True)
print("  by head token:", flush=True)
for h in HEADS:
    print(line(h, tr.hd == h), flush=True)
print("\n  by gap head→Z2G (g1):", flush=True)
for b in ("≤2", "3-5", "≥6"):
    print(line(f"g1 {b}", tr.b1 == b), flush=True)
print("\n  by gap Z2G→T1G (g2):", flush=True)
for b in ("≤2", "3-5", "≥6"):
    print(line(f"g2 {b}", tr.b2 == b), flush=True)
print("\n  by year:", flush=True)
for y in sorted(tr.yr.unique()):
    print(line(str(y), tr.yr == y), flush=True)

print("\n" + "─" * 118, flush=True)
print("  27-CELL ENUMERATION  (head × g1 × g2) — ranked by median; the top row is a "
      "SELECTION, not a finding", flush=True)
print("─" * 118, flush=True)
rows = []
for h in HEADS:
    for b1 in ("≤2", "3-5", "≥6"):
        for b2 in ("≤2", "3-5", "≥6"):
            m = (tr.hd == h) & (tr.b1 == b1) & (tr.b2 == b2)
            if m.sum() < 25:
                rows.append(dict(cell=f"{h} g1{b1} g2{b2}", n=int(m.sum()), thin=True))
                continue
            s, f = R[m], F[m]
            ym = s.groupby(tr.yr[m]).median()
            rows.append(dict(cell=f"{h} g1{b1} g2{b2}", n=int(m.sum()), thin=False,
                             up=(s > 0).mean(), med=s.median(), mfe=f.median(),
                             p90=f.quantile(.90), yrs=int((ym > 0).sum()), nyr=len(ym),
                             worst=ym.min()))
E = pd.DataFrame(rows)
ok = E[~E.thin].sort_values("med", ascending=False)
print(f"    {'cell':22s} {'n':>6s} {'↑':>6s} {'med':>7s} {'MFEmed':>7s} {'MFEp90':>7s} "
      f"{'yrs':>5s} {'worst':>7s}", flush=True)
for _, r in ok.iterrows():
    print(f"    {r.cell:22s} {r.n:>6,} {r.up:>6.1%} {r.med:>+7.2f} {r.mfe:>7.2f} "
          f"{r.p90:>7.1f} {r.yrs}/{r.nyr:<3d} {r.worst:>+7.2f}", flush=True)
print(f"    ({int(E.thin.sum())} of 27 cells too thin to read, n<25)", flush=True)
print(f"\n    spread across the 27: best {ok.med.max():+.2f} · median "
      f"{ok.med.median():+.2f} · worst {ok.med.min():+.2f}   ← a spread this wide is what "
      f"pure noise across 27 cells looks like", flush=True)

# ── PASS 2 · the finalists, with intervals ───────────────────────────────────
print("\n" + "─" * 118, flush=True)
print("  PASS 2 · the top cells under day-clustered intervals (budget includes the 27)",
      flush=True)
print("─" * 118, flush=True)
top = list(ok.cell.head(3))
print(f"  finalists: {top}\n", flush=True)


def cellmask(spec):
    h, a, b = spec.split()
    return lambda t: ((t["sig_hd"].astype(str) == h)
                      & (pd.Series(band(t["sig_g1"].to_numpy()), index=t.index) == a[2:])
                      & (pd.Series(band(t["sig_g2"].to_numpy()), index=t.index) == b[2:]))


fin = None
for spec in top:
    c = st.cell(spec, cellmask(spec))
    if fin is None:
        fin = c
st.cell("T11 (any geometry)", lambda t: t["sig_hd"].astype(str) == "T11")
st.verdict(fin, mask_fn=cellmask(top[0]), family="seq_chain_t10z2g_t1g")
print("\nDONE", flush=True)
