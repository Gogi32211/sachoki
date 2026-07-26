"""Does the ×Nd re-fire count predict anything for the Parabola ride?

Reproduces p_parabola_scan's EXACT gate over 6 years, counts re-fires in the trailing
6 bars (= the board's 7-calendar-day window), and path-sims each tier separately
(trail 25% / -15% initial / 120-bar cap / SLIP 15bps / gap-realistic), per year and
per price bucket. Tiers are separate mask columns so _pathsim's 5-bar cooldown does
not let tier-1 entries eat the tier-2/3 ones.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
import edge_replay as ER

W = 10        # P anchor sits 10 bars before the ride entry (the scanner's _W)
WIN = 6       # trailing bars counted for ×Nd (7 calendar days ≈ 6 trading bars)

a = get_analytics_conn()
as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
print("as_of", as_of, flush=True)
df = a.execute(f"""
    WITH r AS (
      SELECT universe, ticker, date, open, high, low, close, volume,
             coalesce(vol_bucket,'') vb, coalesce(sig_any_p,0) any_p,
             row_number() OVER (PARTITION BY ticker,date
               ORDER BY CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
      FROM bars
      WHERE close > 0 AND date >= DATE '{as_of}' - INTERVAL 2270 DAY
    )
    SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date
""").fetchdf()
a.close()
print("rows", len(df), "tickers", df.ticker.nunique(), flush=True)

out = []
for tk, g in df.groupby("ticker", sort=False):
    g = g.reset_index(drop=True)
    n = len(g)
    if n < W + 2:
        continue
    cl = g["close"].to_numpy(float); vol = g["volume"].to_numpy(float)
    anyp = g["any_p"].to_numpy(int); isvb = (g["vb"].to_numpy(object) == "VB")

    # non-VB across the whole [anchor .. entry] window, vectorised: no VB in the last W+1 bars
    vbcum = np.concatenate([[0], np.cumsum(isvb)])
    novb = np.full(n, False)
    idx = np.arange(W, n)
    novb[idx] = (vbcum[idx + 1] - vbcum[idx - W]) == 0

    adv = np.full(n, np.nan)
    adv[idx] = (cl[idx] / cl[idx - W] - 1.0) * 100.0

    p_anchor = np.full(n, False)
    p_anchor[idx] = anyp[idx - W] == 1

    liq = (cl >= 5) & (cl * vol >= 1_000_000)
    qual = p_anchor & novb & (adv >= 3.0) & liq
    if not qual.any():
        continue

    # ×Nd = qualifying bars in the trailing WIN bars, inclusive. Backward-looking = causal.
    qcum = np.concatenate([[0], np.cumsum(qual)])
    lo_i = np.maximum(np.arange(n) - WIN + 1, 0)
    nfire = qcum[np.arange(n) + 1] - qcum[lo_i]

    g["qual"] = qual
    g["nfire"] = nfire
    g["adv"] = adv
    out.append(g)

d = pd.concat(out, ignore_index=True)
q = d["qual"]
print("total qual bars", int(q.sum()), flush=True)
print("nfire dist on qual bars:", d.loc[q, "nfire"].value_counts().sort_index().to_dict(), flush=True)

# tier masks — separate columns so the 5-bar cooldown works per tier
d["T1"] = q & (d["nfire"] == 1)
d["T2"] = q & (d["nfire"] == 2)
d["T3"] = q & (d["nfire"] >= 3)
d["ALL"] = q

grp = {tk: gg.reset_index(drop=True) for tk, gg in d.groupby("ticker", sort=False)}

def run(col):
    tr = ER._pathsim(grp, col, "trail", 0.15, 0.25, 0.25, 120, slip=0.0015)
    return tr

res = {}
for col in ("ALL", "T1", "T2", "T3"):
    tr = run(col)
    # attach price + adv of the entry bar for bucketing
    key = d.loc[d[col], ["ticker", "date", "close", "nfire", "adv"]].copy()
    key["date"] = key["date"].astype(str)
    # _pathsim's date_in is the NEXT bar; map back by ticker order of trades instead
    res[col] = tr
    print(f"\n=== {col}  n={len(tr)}", flush=True)

def rep(name, tr, px=None):
    if len(tr) == 0:
        print(f"{name:16} n=0"); return
    yrs = tr.groupby("yr")["ret"].mean() * 100
    pos = int((yrs > 0).sum()); tot = len(yrs)
    wins = tr["ret"] > 0
    pfn = tr.loc[wins, "ret"].sum(); pfd = -tr.loc[~wins, "ret"].sum()
    pf = pfn / pfd if pfd > 0 else float("nan")
    print(f"{name:16} n={len(tr):5}  mean{tr.ret.mean()*100:+6.2f}  med{tr.ret.median()*100:+6.2f}  "
          f"win{wins.mean()*100:5.1f}%  PF{pf:5.2f}  yrs{pos}/{tot}  "
          f"tail(+50%){(tr.ret >= 0.5).mean()*100:4.1f}%")
    print(f"{'':16} per-yr: " + " ".join(f"{y}:{v:+.1f}" for y, v in yrs.items()))

print("\n" + "=" * 100)
print("PARABOLA RIDE by ×Nd tier — trail25 / -15% / 120bar / slip15bps / 6yr")
print("=" * 100)
for col, lab in (("ALL", "all"), ("T1", "×1d fresh"), ("T2", "×2d"), ("T3", "×3d+")):
    rep(lab, res[col])

# price buckets — need entry price; re-derive by joining on ticker+date_in's prior bar
print("\n" + "=" * 100)
print("BY PRICE BUCKET")
print("=" * 100)
pxmap = d.set_index(["ticker", d["date"].astype(str)])["close"]
for col, lab in (("T1", "×1d fresh"), ("T2", "×2d"), ("T3", "×3d+")):
    tr = res[col].copy()
    if not len(tr):
        continue
    tr["px"] = [pxmap.get((t, dd), np.nan) for t, dd in zip(tr.ticker, tr.date_in)]
    for lo, hi, bl in ((5, 21, "$5-21"), (21, 89, "$21-89"), (89, 1e9, "$89+")):
        rep(f"{lab} {bl}", tr[(tr.px >= lo) & (tr.px < hi)])
