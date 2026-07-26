"""
t1_confluence.py — ANALYSIS ONLY. What STRENGTHENS raw T1 / T1G?
Confluence miner on the T1/T1G candidate population: for every co-occurring
context flag (L-family + ~155 sig_ booleans + wyckoff/prebreak/delta + categoricals),
measure the MARGINAL forward lift on top of the bare signal, with IS/OOS split and
per-year stability. Adversarially filter — keep only confluences that HOLD OOS and
are positive across regimes. Percent units, per-universe (never pooled).
Run: cd backend && uv run python ../analysis/t1_confluence.py
"""
import duckdb, numpy as np, pandas as pd
DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
UNIS = ("sp500", "nasdaq", "russell2k")
OOS_FROM = "2024-09-01"          # time split: IS < cutoff, OOS >=
MIN_N = 60                        # min cell size for a confluence claim
MIN_OOS = 25
OUT = "/Users/sachoki/Desktop/sachoki-desktop/T1_CONFLUENCE_ANALIZI.md"

con = duckdb.connect(DB, read_only=True)

# ---- discover candidate feature columns (boolean-ish) ----
info = con.execute("PRAGMA table_info(bars)").fetchdf()
PREF = ("sig_", "l34", "l88", "wyc_", "d_", "w2_", "wt_", "prebreak_", "pb_",
        "vbo_up", "ad_", "rtb_", "psar")
SKIP = {"sig_name", "l_sig", "z_sig", "t_sig"}
feat_cols = []
for _, r in info.iterrows():
    n = r["name"]; t = str(r["type"]).upper()
    if n in SKIP:
        continue
    if any(n.startswith(p) or n == p for p in PREF) and ("INT" in t or "BOOL" in t or "TINY" in t or "SMALL" in t):
        feat_cols.append(n)
# de-dup, keep order
feat_cols = list(dict.fromkeys(feat_cols))
print(f"discovered {len(feat_cols)} candidate boolean features")

CATS = {"vol_bucket": ["VB", "B", "N", "L", "W"],
        "wyc_phase": ["ACC_TR", "MARKUP", "SOS", "SPRING", "DIST_TR", "MKDN"],
        "rtb_phase": None}   # rtb_phase values discovered dynamically

base_cols = ["universe", "ticker", "date", "t_sig", "fwd_10d", "mfe_10d", "mae_10d",
             "vol_bucket", "wyc_phase", "rtb_phase"]
sel = base_cols + [c for c in feat_cols if c not in base_cols]


def load(sig):
    q = f"SELECT {','.join(sel)} FROM (SELECT *, row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn FROM bars WHERE t_sig='{sig}') WHERE rn=1"
    df = con.execute(q).fetchdf()
    df["fwd_10d"] = pd.to_numeric(df["fwd_10d"], errors="coerce")
    df = df[np.isfinite(df["fwd_10d"]) & df["fwd_10d"].between(-90, 500)].copy()
    df["yr"] = pd.to_datetime(df["date"]).dt.year
    df["oos"] = df["date"].astype(str) >= OOS_FROM
    return df


def med(s):
    a = np.asarray(s, dtype=float)
    a = a[np.isfinite(a)]
    return float(np.median(a)) if len(a) else np.nan


def baseline(uni):
    return con.execute(f"SELECT median(fwd_10d) FROM bars WHERE universe='{uni}' AND fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500").fetchone()[0]


def scan(df, uni, base, sig_med):
    """Return ranked confluence rows for one universe."""
    d = df[df.universe == uni]
    if len(d) < MIN_N:
        return []
    rows = []
    # boolean features
    feats = []
    for c in feat_cols:
        if c not in d.columns:
            continue
        v = (pd.to_numeric(d[c], errors="coerce").fillna(0).to_numpy() == 1)
        if v.sum() >= MIN_N:
            feats.append((c, v))
    # categoricals
    for col, vals in CATS.items():
        if col not in d.columns:
            continue
        vv = vals or [x for x in d[col].dropna().unique()]
        for val in vv:
            v = (d[col].astype(str).to_numpy() == str(val))
            if v.sum() >= MIN_N:
                feats.append((f"{col}={val}", v))
    fwd = d["fwd_10d"].to_numpy(float)
    mfe = pd.to_numeric(d["mfe_10d"], errors="coerce").to_numpy(float)
    oos = d["oos"].to_numpy()
    yrs = d["yr"].to_numpy()
    for name, v in feats:
        fv = fwd[v]
        n = len(fv); n_oos = int((v & oos).sum())
        m = float(np.median(fv))
        lift_base = round(m - base, 3)
        lift_sig = round(m - sig_med, 3)             # marginal over bare signal
        win = round(float((fv > 0).mean() * 100), 1)
        is_med = med(fwd[v & ~oos]); oos_med = med(fwd[v & oos])
        # per-year median sign (count positive years with n>=15)
        pos_yr = tot_yr = 0
        for y in range(2021, 2027):
            fy = fwd[v & (yrs == y)]
            if len(fy) >= 15:
                tot_yr += 1; pos_yr += int(np.median(fy) > base)
        medmfe = round(float(np.nanmedian(mfe[v])), 1)
        rows.append(dict(feat=name, n=n, n_oos=n_oos, med=round(m, 2),
                         lift_base=lift_base, lift_sig=lift_sig, win=win,
                         is_med=round(is_med, 2) if not np.isnan(is_med) else None,
                         oos_med=round(oos_med, 2) if not np.isnan(oos_med) else None,
                         pos_yr=pos_yr, tot_yr=tot_yr, medmfe=medmfe))
    rows.sort(key=lambda r: r["lift_sig"], reverse=True)
    return rows


def robust(r, base):
    return (r["n"] >= MIN_N and r["n_oos"] >= MIN_OOS and r["lift_sig"] > 0
            and r["oos_med"] is not None and r["oos_med"] > base
            and r["tot_yr"] >= 3 and r["pos_yr"] / max(r["tot_yr"], 1) >= 0.6)


md = ["# T1 / T1G — რა აძლიერებს? (confluence-ის ღრმა ანალიზი)", ""]
md.append(f"_T1/T1G კანდიდატ-ბარებზე ყველა თანმხვედრი კონტექსტის მარგინალური LIFT (ბარე სიგნალის ზემოთ). "
          f"IS<{OOS_FROM} / OOS≥. per-universe (არასდროს pooled). n≥{MIN_N}, OOS n≥{MIN_OOS}. "
          f"**lift_sig** = median(T1G&feat) − median(T1G-ალელი) = კონტექსტის წმინდა წვლილი. პროცენტი._\n")

results = {}
for sig in ("T1G", "T1"):
    df = load(sig)
    md.append(f"\n## {sig}\n")
    for uni in UNIS:
        base = baseline(uni)
        d = df[df.universe == uni]
        sig_med = med(d["fwd_10d"])
        rows = scan(df, uni, base, sig_med)
        results[(sig, uni)] = (rows, base, sig_med, len(d))
        rob = [r for r in rows if robust(r, base)]
        md.append(f"\n### {sig} · {uni}  (n={len(d)}, baseline med {round(base,3)}, {sig}-alone med {round(sig_med,2)})\n")
        md.append(f"_robust confluences (OOS holds + ≥60% წლები base-ს ზემოთ): **{len(rob)}**_\n")
        md.append("| feature | n | n_oos | med10 | **lift_sig** | lift_base | win% | IS→OOS | წლები+ | medMFE |")
        md.append("|---|---|---|---|---|---|---|---|---|---|")
        for r in rob[:14]:
            md.append(f"| {r['feat']} | {r['n']} | {r['n_oos']} | {r['med']} | **{r['lift_sig']}** | "
                      f"{r['lift_base']} | {r['win']} | {r['is_med']}→{r['oos_med']} | "
                      f"{r['pos_yr']}/{r['tot_yr']} | {r['medmfe']} |")
        if not rob:
            md.append("| _(არცერთი confluence არ აკმაყოფილებს robust-კრიტერიუმს)_ | | | | | | | | | |")

# headline cross-universe: which features are robust in BOTH nasdaq AND russell2k for T1G
md.append("\n## ჯვარედინი დადასტურება — T1G-ის confluence robust ორივეში (nasdaq & russell2k)\n")
rn = {r["feat"]: r for r in results[("T1G", "nasdaq")][0] if robust(r, results[("T1G", "nasdaq")][1])}
rr = {r["feat"]: r for r in results[("T1G", "russell2k")][0] if robust(r, results[("T1G", "russell2k")][1])}
both = sorted(set(rn) & set(rr), key=lambda f: rn[f]["lift_sig"] + rr[f]["lift_sig"], reverse=True)
md.append("| feature | nasdaq lift_sig (n) | russell2k lift_sig (n) |\n|---|---|---|")
for f in both[:20]:
    md.append(f"| {f} | {rn[f]['lift_sig']} (n{rn[f]['n']}) | {rr[f]['lift_sig']} (n{rr[f]['n']}) |")
if not both:
    md.append("| _(ცარიელი — ვერცერთი confluence ვერ გაიმეორა ორივე უნივერსიში robust-ად)_ | | |")

con.close()
open(OUT, "w").write("\n".join(md) + "\n")
print("wrote", OUT)
print("cross-universe robust (T1G nas&r2k):", both[:15])
