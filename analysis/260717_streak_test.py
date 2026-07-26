"""Does a CONSECUTIVE-day streak of Parabola ride-entries predict anything?

Different measure from n_fires (which counts qualifying days in the window with gaps
allowed). streak = unbroken run of qualifying trading days ENDING at this bar.
Same honest rig as refire.py: exact scanner gate replayed 6yr, tiers as separate mask
columns (so _pathsim's 5-bar cooldown can't let streak-1 eat streak-2/3), trail 25% /
-15% initial / 120-bar cap / SLIP 15bps / gap-realistic. Per year + per price bucket.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
import edge_replay as ER

W = 10        # P anchor sits 10 bars before the ride entry (the scanner's _W)
WIN = 6       # the board's window: 7 calendar days ≈ 6 trading bars

a = get_analytics_conn()
as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
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
print("as_of", as_of, "rows", len(df), flush=True)

out = []
for tk, g in df.groupby("ticker", sort=False):
    g = g.reset_index(drop=True)
    n = len(g)
    if n < W + 2:
        continue
    cl = g["close"].to_numpy(float); vol = g["volume"].to_numpy(float)
    anyp = g["any_p"].to_numpy(int); isvb = (g["vb"].to_numpy(object) == "VB")

    vbcum = np.concatenate([[0], np.cumsum(isvb)])
    novb = np.full(n, False); idx = np.arange(W, n)
    novb[idx] = (vbcum[idx + 1] - vbcum[idx - W]) == 0
    adv = np.full(n, np.nan); adv[idx] = (cl[idx] / cl[idx - W] - 1.0) * 100.0
    p_anchor = np.full(n, False); p_anchor[idx] = anyp[idx - W] == 1
    qual = p_anchor & novb & (adv >= 3.0) & (cl >= 5) & (cl * vol >= 1_000_000)
    if not qual.any():
        continue

    # n_fires: qualifying bars in the trailing WIN bars (gaps allowed) — what the board shows
    qcum = np.concatenate([[0], np.cumsum(qual)])
    ar = np.arange(n)
    nfire = qcum[ar + 1] - qcum[np.maximum(ar - WIN + 1, 0)]

    # streak: UNBROKEN run of qualifying bars ending here, capped at the window
    streak = np.zeros(n, int)
    run = 0
    for i in range(n):
        run = run + 1 if qual[i] else 0
        streak[i] = min(run, WIN)

    g["qual"] = qual; g["nfire"] = nfire; g["streak"] = streak; g["adv"] = adv
    out.append(g)

d = pd.concat(out, ignore_index=True)
q = d["qual"]
print("qual bars", int(q.sum()))
print("streak dist on qual bars:", d.loc[q, "streak"].value_counts().sort_index().to_dict())
print("nfire vs streak (how often they disagree):",
      f"{(d.loc[q, 'nfire'] != d.loc[q, 'streak']).mean()*100:.1f}% of qual bars", flush=True)

d["S1"] = q & (d["streak"] == 1)
d["S2"] = q & (d["streak"] == 2)
d["S3"] = q & (d["streak"] == 3)
d["S4"] = q & (d["streak"] >= 4)
grp = {tk: gg.reset_index(drop=True) for tk, gg in d.groupby("ticker", sort=False)}

def rep(name, tr):
    if len(tr) == 0:
        print(f"{name:20} n=0"); return
    yrs = tr.groupby("yr")["ret"].mean() * 100
    wins = tr["ret"] > 0
    pfd = -tr.loc[~wins, "ret"].sum()
    pf = tr.loc[wins, "ret"].sum() / pfd if pfd > 0 else float("nan")
    print(f"{name:20} n={len(tr):5}  mean{tr.ret.mean()*100:+6.2f}  med{tr.ret.median()*100:+6.2f}  "
          f"win{wins.mean()*100:5.1f}%  PF{pf:5.2f}  yrs{int((yrs>0).sum())}/{len(yrs)}  "
          f"tail(+50%){(tr.ret>=0.5).mean()*100:4.1f}%")
    print(f"{'':20} per-yr: " + " ".join(f"{y}:{v:+.1f}" for y, v in yrs.items()))

res = {}
for col in ("S1", "S2", "S3", "S4"):
    res[col] = ER._pathsim(grp, col, "trail", 0.15, 0.25, 0.25, 120, slip=0.0015)

print("\n" + "=" * 100)
print("PARABOLA RIDE by CONSECUTIVE-DAY STREAK — trail25 / -15% / 120bar / slip15bps / 6yr")
print("=" * 100)
for col, lab in (("S1", "1 day (fresh)"), ("S2", "2 in a row"),
                 ("S3", "3 in a row"), ("S4", "4+ in a row")):
    rep(lab, res[col])

print("\n" + "=" * 100)
print("BY PRICE BUCKET (the axis that DOES rank)")
print("=" * 100)
pxmap = d.set_index(["ticker", d["date"].astype(str)])["close"]
for col, lab in (("S1", "1 day"), ("S2", "2 row"), ("S3", "3 row"), ("S4", "4+ row")):
    tr = res[col].copy()
    if not len(tr):
        continue
    tr["px"] = [pxmap.get((t, dd), np.nan) for t, dd in zip(tr.ticker, tr.date_in)]
    for lo, hi, bl in ((5, 21, "$5-21"), (21, 89, "$21-89"), (89, 1e9, "$89+")):
        rep(f"{lab} {bl}", tr[(tr.px >= lo) & (tr.px < hi)])

# ---- what the LIVE board would show today
print("\n" + "=" * 100)
print("LIVE TODAY — streaks ending on the freshest qualifying day per ticker")
print("=" * 100)
recent = d[d["date"].astype(str) >= "2026-07-09"]
rows = []
for tk, gg in recent[recent["qual"]].groupby("ticker"):
    last = gg.iloc[-1]
    rows.append((tk, str(last["date"])[:10], int(last["nfire"]), int(last["streak"]),
                 round(float(last["close"]), 2), round(float(last["adv"]), 1)))
r = pd.DataFrame(rows, columns=["ticker", "date", "nfire", "streak", "close", "adv"])
print("streak distribution today:", r["streak"].value_counts().sort_index().to_dict())
print("nfire distribution today: ", r["nfire"].value_counts().sort_index().to_dict())
print("\ntop streaks (still qualifying on 07-16):")
top = r[(r["date"] == "2026-07-16")].sort_values(["streak", "adv"], ascending=False).head(12)
print(top.to_string(index=False))
