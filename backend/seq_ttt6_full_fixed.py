"""The whole tok1 → tok2 → T6 analysis, redone on deduplicated, calendar-adjacent bars.

This replaces seq_ttt6_pairs.py, seq_ttt6_slots.py and seq_ttt6_worst.py in one pass. All
three were run on a frame carrying 39.6% duplicate rows, which inflated every sample roughly
2.8× and let sequences be assembled from bars that were not consecutive — the JLHL case the
user caught, labelled T2→T2→T6 when its chart reads Z10→T5→T6.

Four sections, in the order that decides them:

  1. every concrete pair, with the reference for what chance produces at those cell sizes
  2. the same thing mined on 2021-23 and read on 2024-26 without touching it
  3. the slot marginals, since a 196-cell grid usually hides something lower-dimensional
  4. the deepest drawdowns with names and dates, separated from unadjusted corporate actions

Entry is close[i+1] as before. On clean data the two entries agree — that divergence was the
duplicates — but close[i+1] shares no print with the selection, so it stays the primary.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_contract import sequence_mask, verify_sample
from naked_study import NakedStudy

MIN_N, SPLIT = 100, "2024-01-01"

st = NakedStudy("tok1 → tok2 → T6, full analysis (fixed)", n_trials=2,
                columns=("t_sig", "z_sig"), horizons=(5,), min_price=5.0,
                min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str)
Z = d["z_sig"].fillna("").astype(str)
d["tok"] = np.where(T.ne("") & T.ne("nan"), T, Z)
TT = sorted({t for t in d["tok"].unique() if str(t).startswith("T")})

tk = d["ticker"].to_numpy()
c = d["close"].to_numpy(float)
lo = d["low"].to_numpy(float)
dates = d["date"].astype(str).str[:10].to_numpy()
c1 = np.r_[c[1:], np.nan]
c6 = np.r_[c[6:], np.full(6, np.nan)]
lowroll = pd.Series(lo).rolling(5).min().shift(-5).to_numpy()
barmin = pd.Series(np.r_[c[1:] / c[:-1] - 1, np.nan]).rolling(5).min().shift(-5).to_numpy()
s1 = np.r_[tk[:-1] == tk[1:], False]
s6 = np.r_[tk[:-6] == tk[6:], np.zeros(6, bool)]
ent = np.where(s1, c1, np.nan)
ret5 = np.where(s6, c6 / ent - 1, np.nan) * 100
mae5 = (np.where(s6, np.r_[lowroll[1:], np.nan], np.nan) / ent - 1) * 100
wbar = np.r_[barmin[1:], np.nan] * 100

base = sequence_mask(d, [list(TT), list(TT), "T6"]) & np.isfinite(ret5) & np.isfinite(mae5)
tokarr = d["tok"].to_numpy()
p1 = np.r_[[""], tokarr[:-1]]
p2 = np.r_[["", ""], tokarr[:-2]]
fam_r, fam_m = np.median(ret5[base]), np.median(mae5[base])
print(f"\n  usable T→T→T6: {base.sum():,}  ·  family ret5 {fam_r:+.3f}% · "
      f"MAE5 {fam_m:+.2f}% · win {(ret5[base] > 0).mean():.2%}", flush=True)
verify_sample(d, base, n=4, label="T→T→T6 (fixed)")

yr = d["_dt"].to_numpy()
mined, oos = base & (yr < np.datetime64(SPLIT)), base & (yr >= np.datetime64(SPLIT))

# ── 1 · every pair ──────────────────────────────────────────────────────────
rows = []
for a in TT:
    for b in TT:
        m = base & (p2 == a) & (p1 == b)
        k = int(m.sum())
        if k < MIN_N:
            continue
        r, mae = ret5[m], mae5[m]
        mi, oo = mined & (p2 == a) & (p1 == b), oos & (p2 == a) & (p1 == b)
        rows.append(dict(pair=f"{a}→{b}→T6", n=k, ret=np.median(r), win=(r > 0).mean() * 100,
                         mae=np.median(mae), p10=np.percentile(mae, 10),
                         d5=(mae < -5).mean() * 100, d10=(mae < -10).mean() * 100,
                         n_mined=int(mi.sum()),
                         r_mined=np.median(ret5[mi]) if mi.sum() >= 40 else np.nan,
                         r_oos=np.median(ret5[oo]) if oo.sum() >= 40 else np.nan))
D = pd.DataFrame(rows).sort_values("ret", ascending=False)
print("\n" + "=" * 124, flush=True)
print(f"  1 · ALL {len(D)} PAIRS with n ≥ {MIN_N}  (5 bars, entry close[i+1])", flush=True)
print("=" * 124, flush=True)
print(f"  {'pair':16s} {'n':>7s} {'ret5':>8s} {'Δfam':>7s} {'win':>7s} | {'MAE med':>8s} "
      f"{'MAE p10':>8s} {'>5%':>7s} {'>10%':>7s}", flush=True)
for _, r in D.iterrows():
    print(f"  {r.pair:16s} {r.n:>7,} {r.ret:>+8.3f} {r.ret - fam_r:>+7.3f} {r.win:>6.2f}% | "
          f"{r.mae:>+8.2f} {r.p10:>+8.2f} {r.d5:>6.1f}% {r.d10:>6.1f}%", flush=True)

rng = np.random.default_rng(0)
pool, sizes = ret5[base], D.n.to_numpy()
spreads = [np.ptp([np.median(rng.choice(pool, s, replace=False)) for s in sizes])
           for _ in range(400)]
obs = D.ret.max() - D.ret.min()
print(f"\n    observed best−worst spread {obs:+.3f}pp · chance median "
      f"{np.median(spreads):.3f} · p95 {np.percentile(spreads, 95):.3f}  →  "
      f"{'INSIDE chance — a lucky cell' if obs <= np.percentile(spreads, 95) else 'OUTSIDE chance'}",
      flush=True)

# ── 2 · mined → frozen OOS ──────────────────────────────────────────────────
E = D.dropna(subset=["r_mined", "r_oos"]).sort_values("r_mined", ascending=False)
print("\n" + "=" * 124, flush=True)
print(f"  2 · MINED 2021-05→2023-12 → FROZEN OOS 2024-01→2026-07  ({len(E)} cells)",
      flush=True)
print("=" * 124, flush=True)
print(f"  {'pair':16s} {'n mined':>8s} {'mined':>8s} {'OOS':>8s}", flush=True)
for _, r in pd.concat([E.head(6), E.tail(3)]).iterrows():
    print(f"  {r.pair:16s} {r.n_mined:>8,} {r.r_mined:>+8.3f} {r.r_oos:>+8.3f}", flush=True)
top = E.head(max(3, len(E) // 4))
print(f"\n    top quartile mined {top.r_mined.median():+.3f} → OOS {top.r_oos.median():+.3f}"
      f" · rank correlation {E.r_mined.corr(E.r_oos, method='spearman'):+.3f}"
      f" · sign held {int((np.sign(top.r_mined) == np.sign(top.r_oos)).sum())}/{len(top)}",
      flush=True)

# ── 3 · slot marginals ──────────────────────────────────────────────────────
print("\n" + "=" * 124, flush=True)
print("  3 · SLOT MARGINALS — is it the pair, or just which token sits where?", flush=True)
print("=" * 124, flush=True)
for slot, arr in (("slot 1 (two bars back)", p2), ("slot 2 (bar before T6)", p1)):
    print(f"\n  {slot}", flush=True)
    print(f"    {'token':>7s} {'n':>8s} {'ret5':>8s} {'Δfam':>7s} {'win':>7s} | "
          f"{'MAE med':>8s} {'Δfam':>7s} {'>5%':>7s} {'>10%':>7s} | {'mined':>8s} {'OOS':>8s}",
          flush=True)
    R = []
    for t in TT:
        m = base & (arr == t)
        if m.sum() < 150:
            continue
        r, a = ret5[m], mae5[m]
        mi, oo = mined & (arr == t), oos & (arr == t)
        R.append((t, int(m.sum()), np.median(r), (r > 0).mean() * 100, np.median(a),
                  (a < -5).mean() * 100, (a < -10).mean() * 100,
                  np.median(ret5[mi]) if mi.sum() >= 80 else np.nan,
                  np.median(ret5[oo]) if oo.sum() >= 80 else np.nan))
    for t, k, r, w, a, x5, x10, mi, oo in sorted(R, key=lambda x: -x[2]):
        print(f"    {t:>7s} {k:>8,} {r:>+8.3f} {r - fam_r:>+7.3f} {w:>6.2f}% | "
              f"{a:>+8.2f} {a - fam_m:>+7.2f} {x5:>6.1f}% {x10:>6.1f}% | "
              f"{mi:>+8.3f} {oo:>+8.3f}", flush=True)
    M = pd.DataFrame(R, columns=list("tnrwaxym") + ["o"]).dropna()
    if len(M):
        print(f"    mined↔OOS rank correlation {M.m.corr(M.o, method='spearman'):+.3f} · "
              f"drawdown spread {M.a.max() - M.a.min():.2f}pp", flush=True)

# ── 4 · the deepest drawdowns, with names ───────────────────────────────────
i = np.where(base)[0]
W = pd.DataFrame(dict(ticker=tk[i], t6_date=dates[i], entry_date=dates[np.minimum(i + 1,
                                                                                 len(d) - 1)],
                      combo=[f"{a}→{b}→T6" for a, b in zip(p2[i], p1[i])],
                      entry=np.round(ent[i], 2), mae=np.round(mae5[i], 2),
                      ret5=np.round(ret5[i], 2), worst_bar=np.round(wbar[i], 2)))
W["artifact"] = (W.worst_bar < -45) | (W.entry > 900)
W = W.sort_values("mae")
W.to_csv("seq_ttt6_worst_fixed.csv", index=False)
C = W[~W.artifact]
print("\n" + "=" * 124, flush=True)
print("  4 · THE 20 DEEPEST *REAL* DRAWDOWNS  (unadjusted corporate actions removed)",
      flush=True)
print("=" * 124, flush=True)
print(f"  {'ticker':>7s} {'entry date':>12s} {'combo':>16s} {'$in':>9s} {'MAE':>8s} "
      f"{'ret5':>8s} {'worst bar':>10s}", flush=True)
for _, r in C.head(20).iterrows():
    print(f"  {r.ticker:>7s} {r.entry_date:>12s} {r.combo:>16s} {r.entry:>9.2f} "
          f"{r.mae:>+8.2f} {r.ret5:>+8.2f} {r.worst_bar:>+10.2f}", flush=True)
print(f"\n  removed as corporate actions: {int(W.artifact.sum()):,} rows "
      f"({W.artifact.mean():.2%}) · of drawdowns worse than −50%, "
      f"{W[W.mae <= -50].artifact.mean():.0%} are artifacts", flush=True)
print(f"  clean family: median {C.mae.median():+.2f}% · p10 {np.percentile(C.mae, 10):+.2f}% "
      f"· p1 {np.percentile(C.mae, 1):+.2f}% · worst {C.mae.min():+.2f}% "
      f"({C.iloc[0].ticker} {C.iloc[0].entry_date})", flush=True)

print("\n  worst real case per combination:", flush=True)
print(f"  {'combo':>16s} {'n':>7s} {'medMAE':>8s} | {'ticker':>7s} {'date':>12s} {'MAE':>8s}",
      flush=True)
out = []
for combo in D.pair:
    g = C[C.combo == combo]
    if len(g):
        w = g.iloc[0]
        out.append((combo, int((W.combo == combo).sum()),
                    W[W.combo == combo].mae.median(), w.ticker, w.entry_date, w.mae))
for combo, k, m, t, dd, mm in sorted(out, key=lambda x: x[5]):
    print(f"  {combo:>16s} {k:>7,} {m:>+8.2f} | {t:>7s} {dd:>12s} {mm:>+8.2f}", flush=True)
D.to_csv("seq_ttt6_pairs_fixed.csv", index=False)
print("\n  written: seq_ttt6_pairs_fixed.csv · seq_ttt6_worst_fixed.csv", flush=True)
print("\nDONE", flush=True)
