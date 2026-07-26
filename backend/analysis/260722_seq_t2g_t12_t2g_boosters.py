"""What strengthens the user's T2G / T12(RSI35-55) / T2G 3-bar sequence (2026-07-22)?

Base: exact-sequence match (query_exact_sequence), 1,102 matches, HH 52.8%/HL 47.2%,
avg gain 18.11%/-5.62%, all-universe. Anchor bar = the FINAL T2G bar (bar 0).

For each candidate "Ultra screener" signal available historically in `bars` on the
ANCHOR bar — the 11 legacy scores (turbo/ultra/ultra_v3/buy_score/gog/beta/rtb/aes/
prebreak/profile/final_bull) + the 19 engine-only signals persisted 2026-07-21
(um_2809, ev_l22/43/64/34, bo/bx/be_dn, buy_here, atr_brk, bb_brk, rtv, svs_raw,
cons_atr, gog1-3, setup/context tokens) + vol_bucket/gap_class/wyc_phase/price/RSI/CCI
— split the 1,102-row matched set into "present" vs "absent" (or terciles for
continuous scores) and compare a REAL path-sim (trail25/-15%/60bar/15bps, next-open
entry — NOT the raw fwd_20d close-to-close) against the baseline. min-n=40 gate;
anything below is reported but flagged untrustworthy (n=1,102 total is already thin).

NOT available historically per-bar (computed live-only at scan time, would need a
separate re-derivation): RS-gate, MTF-echo/score_conf, key-level, rev/brk flags,
seq34/CONF/EDGE masks. Skipped here — see caveat in the printed summary.
"""
import json, os, sys, time
import numpy as np, pandas as pd, duckdb

t0 = time.time()
S_ = 0.0015
HERE = os.path.dirname(os.path.abspath(__file__))
BACKEND = os.path.dirname(HERE)
sys.path.insert(0, BACKEND)

MATCHES_JSON = "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad/t2g_t12_t2g.json"
with open(MATCHES_JSON) as f:
    matched = json.load(f)["rows"]
print(f"anchor matches loaded: {len(matched)}", flush=True)

tickers = sorted({r["ticker"] for r in matched})
anchor_by_tk = {}
for r in matched:
    anchor_by_tk.setdefault(r["ticker"], set()).add(r["date"])

SCORE_COLS = ["turbo_score", "ultra_score", "ultra_score_v3", "buy_score", "gog_score",
              "beta_score", "rtb_total", "aes_score", "prebreak_score", "profile_score",
              "final_bull_score"]
NEWBIN = ["um_2809", "ev_l22", "ev_l43", "ev_l64", "ev_l34", "bo_dn", "bx_dn", "be_dn",
          "buy_here", "atr_brk", "bb_brk", "rtv", "svs_raw", "cons_atr", "gog1", "gog2", "gog3"]

conn = duckdb.connect(os.path.join(os.path.dirname(BACKEND), "data", "studio_analytics.duckdb"), read_only=True)
colsel = ", ".join(f'"{c}"' for c in SCORE_COLS + NEWBIN)
placeholders = ",".join(f"'{t}'" for t in tickers)
df = conn.execute(f"""
    WITH r AS (SELECT ticker, date, open, high, low, close, rsi_14, cci_20,
        coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
        coalesce(wyc_phase,'') wp, coalesce(setup_tokens,'') sut, coalesce(context_tokens,'') cxt,
        {colsel},
        row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
        FROM bars WHERE ticker IN ({placeholders}))
    SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date
""").fetchdf()
conn.close()
print(f"per-ticker frame {len(df):,} rows, {df.ticker.nunique()} tickers ({time.time()-t0:.0f}s)", flush=True)

# ── path-sim, computed only around each anchor date (full per-ticker series so the
# entry = next bar's open and the 60-bar trailing window has real forward data) ──
o = df.open.to_numpy(float); h = df.high.to_numpy(float); lo_ = df.low.to_numpy(float); c = df.close.to_numpy(float)
tk = df.ticker.to_numpy(); dt = df.date.astype(str).to_numpy()
n = len(df)
is_anchor = np.zeros(n, bool)
for i in range(n):
    s = anchor_by_tk.get(tk[i])
    if s and dt[i] in s:
        is_anchor[i] = True
print(f"anchor rows located in frame: {is_anchor.sum()} / {len(matched)}", flush=True)

ps = np.full(n, np.nan)
idx_by_tk = {}
for i, t in enumerate(tk):
    idx_by_tk.setdefault(t, []).append(i)
for t, idxs in idx_by_tk.items():
    lo_i, hi_i = idxs[0], idxs[-1]
    for b in range(lo_i, hi_i):
        if not is_anchor[b]:
            continue
        e = o[b + 1] * (1 + S_)
        if e <= 0:
            continue
        pk = e; hd = e * 0.85; end = min(b + 61, hi_i + 1); r = None
        for q in range(b + 1, end):
            if q > b + 1 and o[q] <= hd: r = o[q] / e - 1 - S_; break
            if lo_[q] <= hd: r = -0.15 - S_; break
            pk = max(pk, h[q]); ts = pk * 0.75
            if q > b + 1 and o[q] <= ts: r = o[q] / e - 1 - S_; break
            if lo_[q] <= ts: r = ts / e - 1 - S_; break
        ps[b] = r if r is not None else c[end - 1] / e - 1 - S_
print(f"path-sim done for anchors ({time.time()-t0:.0f}s)", flush=True)

A = df[is_anchor].copy()
A["ps"] = ps[is_anchor]
A = A[A["ps"].notna()].reset_index(drop=True)
base_ps = A["ps"].mean() * 100
base_med = A["ps"].median() * 100
base_win = (A["ps"] > 0).mean() * 100
print(f"\nBASELINE (n={len(A)}): ps_mean {base_ps:+.2f}% | ps_med {base_med:+.2f}% | win {base_win:.1f}%\n", flush=True)

def report(label, mask, min_n=40):
    nn = int(mask.sum())
    if nn < min_n:
        print(f"  {label:34} n={nn:4}  ⚠ below min-n({min_n}), SKIP")
        return None
    pm = A.loc[mask, "ps"].mean() * 100
    pmed = A.loc[mask, "ps"].median() * 100
    win = (A.loc[mask, "ps"] > 0).mean() * 100
    lift = pm - base_ps
    print(f"  {label:34} n={nn:4}  ps {pm:+6.2f}% (lift {lift:+5.2f})  med {pmed:+6.2f}%  win {win:5.1f}%")
    return (label, nn, pm, lift, pmed, win)

results = []

print("── Legacy scores (median split) ──")
for col in SCORE_COLS:
    if col not in A.columns or A[col].notna().sum() < 80:
        continue
    med = A[col].median()
    r1 = report(f"{col} >= median({med:.1f})", A[col] >= med)
    r2 = report(f"{col} <  median({med:.1f})", A[col] <  med)
    if r1: results.append(r1)
    if r2: results.append(r2)

print("\n── New engine-only binary signals (2026-07-21 persisted) ──")
for col in NEWBIN:
    if col not in A.columns:
        continue
    r = report(f"{col} fired", A[col] == 1)
    if r: results.append(r)

print("\n── Volume bucket / gap class / Wyckoff phase (entry bar) ──")
for val in A["vb"].dropna().unique():
    if val == "": continue
    r = report(f"vol={val}", A["vb"] == val)
    if r: results.append(r)
for val in A["gap"].dropna().unique():
    if val == "": continue
    r = report(f"gap={val}", A["gap"] == val)
    if r: results.append(r)
for val in A["wp"].dropna().unique():
    if val == "": continue
    r = report(f"wyc={val}", A["wp"] == val)
    if r: results.append(r)

print("\n── Price bucket (entry bar close) ──")
r = report("px $5-21",  (A["close"] >= 5) & (A["close"] < 21));  results.append(r) if r else None
r = report("px $21-89", (A["close"] >= 21) & (A["close"] < 89)); results.append(r) if r else None
r = report("px $89+",   A["close"] >= 89);                       results.append(r) if r else None

print("\n── RSI / CCI on entry bar (T2G bar itself — not the T12 bar already gated) ──")
r = report("RSI<40",     A["rsi_14"] < 40);                        results.append(r) if r else None
r = report("RSI40-60",   A["rsi_14"].between(40, 60));              results.append(r) if r else None
r = report("RSI60+",     A["rsi_14"] >= 60);                        results.append(r) if r else None
r = report("CCI<-100",   A["cci_20"] < -100);                       results.append(r) if r else None
r = report("CCI>100",    A["cci_20"] >= 100);                       results.append(r) if r else None

print("\n── Setup / context tokens (entry bar) ──")
for tok in ["A", "SM", "N", "MX"]:
    r = report(f"su={tok}", A["sut"].fillna("").str.contains(fr"(?:^|\s){tok}(?:\s|$)"))
    if r: results.append(r)
for tok in ["LD", "WRC", "SVS", "LRC", "LDS", "SQB", "LDC", "F8C", "BCT", "LDP", "LRP"]:
    r = report(f"cx={tok}", A["cxt"].fillna("").str.contains(fr"(?:^|\s){tok}(?:\s|$)"))
    if r: results.append(r)

print(f"\n═══ TOP 15 boosters by lift (n>=40) ═══")
results = [r for r in results if r]
results.sort(key=lambda r: -r[3])
for label, nn, pm, lift, pmed, win in results[:15]:
    print(f"  {label:34} n={nn:4}  ps {pm:+6.2f}% (lift {lift:+5.2f})  med {pmed:+6.2f}%  win {win:5.1f}%")

print(f"\n═══ WORST 10 (suppressors) ═══")
for label, nn, pm, lift, pmed, win in results[-10:]:
    print(f"  {label:34} n={nn:4}  ps {pm:+6.2f}% (lift {lift:+5.2f})  med {pmed:+6.2f}%  win {win:5.1f}%")

print(f"\ndone ({time.time()-t0:.0f}s)")
