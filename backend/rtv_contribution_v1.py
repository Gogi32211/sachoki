"""RTV_CONTRIBUTION_V1 — does turbo_engine.py:390's unconditional `rtv +3` improve selection?

FROZEN SPEC (user-approved 2026-09-13, k = 1):
  estimand   same fires, same stored score, only RTV's effective CAPPED contribution removed
  harness    SCORE_AUDIT_V1 unchanged — its sealed X sample and its sacred-_pathsim direct_trades
             are REUSED, not rebuilt, so both arms stand on identical rows and identical outcomes
  treatment  authority = bars.rtv (the engine boolean production actually scores on);
             combo_sig is a data-quality cross-check ONLY
  delta      delta_rtv = min(combo,14) - min(combo-3,14), buy_2809 recovered from bars.combo_sig
  primary    paired WITH vs WITHOUT over the same days
  sens       concordant-positive LOWER BOUND: remove the contribution only where bars.rtv = 1 AND
             the token agrees; discordant boolean-only rows KEEP their +3 (their stored score
             really does contain it)
  decision   uplift -> keep · null/mixed -> unchanged · degradation/no contribution -> removal candidate
             primary and sensitivity disagreeing -> DATA_QUALITY_BOUND, no removal
  no weight search (+1/+2/+4). RTV-alone / RTV x RS / liquidity / concentration = diagnostics only.
"""
from __future__ import annotations
import os, sys, json
import numpy as np, pandas as pd, duckdb

sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from breadth_family import GATES                                            # noqa: E402

R = "/Users/sachoki/MASSIVE_DATA/SCORE_AUDIT_V1/runs/"
X_RUN, T_RUN = "SA_20260907T162603Z", "OUT_20260907T163751Z_AMENDMENT_1"
DB = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
MIN_DAY, MIN_CELL, TOP_PCT = 20, 5, 0.8
OUT = os.environ.get("RTV_OUT", "/Users/sachoki/Desktop/sachoki-desktop/research_out")

def line(c="─", n=78): print(c * n)

# ── 1 · the sealed sample and its outcomes, reused verbatim ──────────────────
X = pd.read_parquet(os.path.join(R, X_RUN, "X.parquet"))
T = pd.read_parquet(os.path.join(R, T_RUN, "direct_trades.parquet"))
print(f"X (sealed sample)      {len(X):,} rows")
print(f"direct_trades (sacred) {len(T):,} rows · censored {int(T['censored'].sum()):,}")

# ── 2 · join the combo-family inputs from bars ───────────────────────────────
con = duckdb.connect(DB, read_only=True)
# bars carries one row per (ticker, date, universe) — dedupe exactly as the sealed X build did
B = con.execute("""
    WITH r AS (SELECT ticker, date, universe, rtv, rocket, sig_3g, hilo_buy, atr_brk,
                      sig_cd, sig_ca, sig_cw, sig_seq_bcont, combo_sig,
                      row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
               FROM bars)
    SELECT ticker, date AS session, rtv, rocket, sig_3g, hilo_buy, atr_brk,
           sig_cd, sig_ca, sig_cw, sig_seq_bcont, combo_sig
    FROM r WHERE rn = 1""").fetchdf()
B["session"] = pd.to_datetime(B["session"]).dt.date.astype(str)
X["session"] = pd.to_datetime(X["session"]).dt.date.astype(str)
n0 = len(X); X = X.merge(B, on=["ticker", "session"], how="left")
assert len(X) == n0, "join must not duplicate"
unmatched = X["combo_sig"].isna() & X["rtv"].isna()
print(f"bars join               matched {n0 - int(unmatched.sum()):,} / {n0:,}")

b = lambda c: (pd.to_numeric(X[c], errors="coerce").fillna(0) != 0).to_numpy()
tok = X["combo_sig"].fillna("")
known = tok.ne("").to_numpy()
buy2809 = tok.str.contains("BUY", regex=False).to_numpy()

base = (np.where(b("rocket"), 12, np.where(buy2809, 8, 0))
        + np.where(b("sig_3g"), 4, 0) + np.where(b("hilo_buy"), 4, 0) + np.where(b("atr_brk"), 2, 0)
        + np.where(b("sig_cd"), 5, np.where(b("sig_ca"), 3, np.where(b("sig_cw"), 2, 0)))
        + np.where(b("sig_seq_bcont"), 3, 0))
delta = np.minimum(base + 3, 14) - np.minimum(base, 14)

rtv_bool = b("rtv")
rtv_tok  = tok.str.contains("RTV", regex=False).to_numpy()
concordant = rtv_bool & rtv_tok

# ── 3 · the four mandated counts ─────────────────────────────────────────────
line("═")
print("MANDATED RUN-REPORT COUNTS  (sealed sample)")
print(f"  bars.rtv positive                {rtv_bool.sum():>8,}")
print(f"  RTV token positive               {rtv_tok.sum():>8,}")
print(f"  concordant positive              {concordant.sum():>8,}")
print(f"  discordant boolean-only          {(rtv_bool & ~rtv_tok).sum():>8,}")
print(f"  discordant token-only            {(~rtv_bool & rtv_tok).sum():>8,}")
print(f"  combo_sig absent (buy unknown)   {(~known).sum():>8,}")
line("═")
d_known = delta[rtv_bool & known]
print("delta_rtv on treated rows:", {int(k): int(v) for k, v in
      zip(*np.unique(d_known, return_counts=True))})

# ── 4 · the two arms ─────────────────────────────────────────────────────────
X["score_with"] = X["turbo_score"].astype(float)
X["score_wo"]      = X["score_with"] - np.where(rtv_bool,   delta, 0.0)     # primary
X["score_wo_conc"] = X["score_with"] - np.where(concordant, delta, 0.0)     # sensitivity

def pct(col):
    return X[col].groupby([X["session"], X["rsi_band"], X["px_band"]]).rank(method="average", pct=True).to_numpy(float)

# self-check: our recomputed percentile must reproduce the sealed pct_turbo_score
chk = np.nanmax(np.abs(pct("score_with") - X["pct_turbo_score"].to_numpy(float)))
print(f"\nself-check  max|pct_recomputed - pct_sealed| = {chk:.2e}", "✓" if chk < 1e-9 else "✗ MISMATCH")
assert chk < 1e-9, "percentile reconstruction does not match the sealed column"

for c in ("score_with", "score_wo", "score_wo_conc"):
    X[f"pct_{c}"] = pct(c)

# ── 5 · merge outcomes, build edge exactly as AMENDMENT_1 did ────────────────
df = X.merge(T, on=["ticker", "session"], how="inner")
df = df[df["ret"].notna()].reset_index(drop=True)
dn = df.groupby("date_in")["ret"].transform("size").to_numpy()
dm = df.groupby("date_in")["ret"].transform("median").to_numpy()
df["edge_raw"] = np.where(dn >= MIN_DAY, df["ret"].to_numpy(float) - dm, np.nan)

g = df.groupby(["date_in", "rsi_band", "px_band"])["ret"]
n1, m1 = g.transform("size").to_numpy(), g.transform("median").to_numpy()
g2 = df.groupby(["date_in", "rsi_band"])["ret"]
n2, m2 = g2.transform("size").to_numpy(), g2.transform("median").to_numpy()
r = df["ret"].to_numpy(float)
df["edge"] = np.where(n1 >= MIN_CELL, r - m1, np.where(n2 >= MIN_CELL, r - m2, np.nan))

print(f"\nscored trades {len(df):,} over {df['date_in'].nunique():,} entry days")
line()

# ── 6 · deciding series per arm, paired by day ───────────────────────────────
def series(pcol):
    s = df[(df[pcol].to_numpy(float) >= TOP_PCT) & np.isfinite(df["edge"].to_numpy(float))]
    return s.groupby("date_in")["edge"].median()

def window(ser, lo, hi):
    d = ser[(ser.index >= lo) & (ser.index <= hi)]
    yr = d.groupby(pd.Index(d.index).str.slice(0, 4)).median()
    dpy = d.groupby(pd.Index(d.index).str.slice(0, 4)).size()
    yr = yr[dpy >= GATES["mine"]["min_days_per_year"]]
    return dict(n_days=len(d), median=round(float(d.median()), 4) if len(d) else None,
                day_win=round(float((d > 0).mean() * 100), 1) if len(d) else None,
                pos_years=int((yr > 0).sum()), years=len(yr),
                worst_year=round(float(yr.min()), 3) if len(yr) else None)

arms = {"WITH rtv +3": "pct_score_with", "WITHOUT (primary)": "pct_score_wo",
        "WITHOUT (concordant)": "pct_score_wo_conc"}
S = {k: series(v) for k, v in arms.items()}
print(f"{'arm':22s} {'window':7s} {'days':>5s} {'median':>8s} {'win%':>6s} {'yrs':>6s} {'worst':>7s}")
for k, s in S.items():
    for w in ("MINE", "VERIFY"):
        st = window(s, *OOS[w])
        print(f"{k:22s} {w:7s} {st['n_days']:>5,} {st['median']:>8.4f} {st['day_win']:>6.1f} "
              f"{st['pos_years']:>2d}/{st['years']:<3d} {st['worst_year']:>7.3f}")
line()

# ── 7 · PRIMARY: paired difference, bootstrap by date ────────────────────────
rng = np.random.default_rng(20260913)
def paired(a, b, label):
    j = pd.concat([a.rename("a"), b.rename("b")], axis=1).dropna()
    print(f"\n{label}   (WITHOUT - WITH, per day; positive => removing the +3 HELPS)")
    for w in ("MINE", "VERIFY"):
        k = j[(j.index >= OOS[w][0]) & (j.index <= OOS[w][1])]
        if not len(k): continue
        d = (k["b"] - k["a"]).to_numpy(float)
        nz = d[d != 0]
        bs = np.array([rng.choice(d, len(d), replace=True).mean() for _ in range(4000)])
        lo, hi = np.percentile(bs, [2.5, 97.5])
        print(f"  {w:7s} days {len(d):>4,} · days the selection differs {len(nz):>4,} "
              f"({100*len(nz)/len(d):4.1f} %)")
        print(f"          mean diff {d.mean():+.4f}  [95 % by-date {lo:+.4f}, {hi:+.4f}]  "
              f"median {np.median(d):+.4f}  days>0 {100*(d>0).mean():.1f} %")
        if len(nz):
            print(f"          on differing days only: mean {nz.mean():+.4f}  "
                  f"median {np.median(nz):+.4f}  days>0 {100*(nz>0).mean():.1f} %")

paired(S["WITH rtv +3"], S["WITHOUT (primary)"], "PRIMARY  k = 1")
paired(S["WITH rtv +3"], S["WITHOUT (concordant)"], "SENSITIVITY  concordant-positive lower bound")

json.dump(dict(counts=dict(rtv_bool=int(rtv_bool.sum()), rtv_token=int(rtv_tok.sum()),
                           concordant=int(concordant.sum()),
                           boolean_only=int((rtv_bool & ~rtv_tok).sum()),
                           token_only=int((~rtv_bool & rtv_tok).sum())),
               trades=int(len(df)), days=int(df["date_in"].nunique())),
          open(os.path.join(OUT, "rtv_counts.json"), "w"), indent=1)
