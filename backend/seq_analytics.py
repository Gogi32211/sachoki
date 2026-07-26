"""
seq_analytics.py — fresh 5-YEAR signal/sequence rule-database generator, built RIGHT:
  • covers the FULL signal set (TZ + L + ULTRA flags), not just TZ+L+suffix
  • CLEAN forward returns (the precomputed fwd_* cols are corrupt — recomputed via lead)
  • TIME-ROBUSTNESS is first-class: every rule gets a per-year regime flag
    STABLE (positive in >=60% of years) / 2025-ARTIFACT (<=40%) / MIXED — so the
    2025-melt-up mirages (which are ~75% of the old DB) are flagged out, not chased.

Stage 1 = unified per-signal baseline. Stage 2 (build_sequences) = N-bar sequences on
the robust signals. Mirrors ~/Desktop/_TZ ANALYTICS methodology but extends + de-biases it.
READ-ONLY on bars.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

# the signal vocabulary we score (TZ continuation/reversal + L + the meaningful ULTRA flags)
TZ = ["t1","t1g","t2","t2g","t3","t4","t5","t6","t9","t10","t11","t12",
      "z1","z1g","z2","z2g","z3","z4","z5","z6","z7","z9","z10","z11","z12"]
L  = ["l1","l2","l3","l4","l5","l6"]
ULTRA = ["fbo_up","fbo_dn","eb_up","eb_dn","para_start","para_prep","para_retest",
         "sc","svs","strong","gog_plus","org_up","va","seq_bcont","flp_up",
         "p2","p3","p50","p55","p66","p89","fri34","fri43","fri64","wk_up","wk_dn",
         "vbo_dn","fly_abcd","g6","rh","rl"]
ALL_SIG = TZ + L + ULTRA


# ULTRA priority when a bar carries no Z/T token (folds the ULTRA set into the vocabulary)
_ULTRA_TOK = ["para_start","fbo_up","eb_up","sc","gog_plus","fri43","fri64","p55",
              "p66","svs","strong","va","org_up","flp_up","wk_up","vbo_dn","fbo_dn","eb_dn"]


import os as _os
from studio.paths import db_path as _dbp

_TF_DB = {
    "1d": _dbp("studio_analytics.duckdb"),
    "4h": _dbp("4h"),
    "1h": _dbp("1h"),
}
_TF_DVFLOOR = {"1d": 3_000_000, "4h": 800_000, "1h": 400_000}


def _conn(tf: str = "1d"):
    """Read-only connection to the DB for the given timeframe (1d/4h/1h)."""
    if tf == "1d":
        from ai_journal.db import get_analytics_conn
        return get_analytics_conn()
    import duckdb
    return duckdb.connect(_TF_DB[tf], read_only=True)


def _pull(tf: str = "1d", dv_floor: float | None = None):
    a = _conn(tf)
    if dv_floor is None:
        dv_floor = _TF_DVFLOOR.get(tf, 3_000_000)
    try:
        sigsel = ", ".join(f"coalesce(sig_{s},0) {s}" for s in ALL_SIG)
        df = a.execute(f"""
            WITH r AS (
              SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
              FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>={dv_floor}
            ),
            d AS (SELECT * FROM r WHERE rn=1)
            SELECT universe, ticker, date, close,
                   coalesce(z_sig,'') z_sig, coalesce(t_sig,'') t_sig, coalesce(l_sig,'') l_sig,
                   lead(close,10) OVER w f10, lead(close,20) OVER w f20,
                   max(high) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) fhi,
                   min(low)  OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING) flo,
                   {sigsel}
            FROM d WINDOW w AS (PARTITION BY ticker ORDER BY date)
        """).fetchdf()
    finally:
        a.close()
    df["yr"] = df["date"].astype(str).str[:4]
    df["r10"] = df["f10"]/df["close"] - 1
    df["r20"] = df["f20"]/df["close"] - 1
    df["mfe"] = df["fhi"]/df["close"] - 1
    df["mae"] = df["flo"]/df["close"] - 1
    build_token(df)
    return df


def build_token(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-bar 'tok': Z (priority) else -T else *ULTRA else '-', then + L.
    df must have z_sig/t_sig/l_sig + the ULTRA sig_* flag columns (named bare, e.g. fbo_up)."""
    z = df["z_sig"].fillna(""); t = df["t_sig"].fillna(""); l = df["l_sig"].fillna("")
    ult = pd.Series("", index=df.index)
    for u in reversed(_ULTRA_TOK):           # reversed → earlier in list wins
        if u in df:
            ult = ult.mask(df[u] == 1, u.upper().replace("_", ""))
    prim = z.where(z != "", ("-" + t).where(t != "",
                  ("*" + ult).where(ult != "", "-")))
    df["tok"] = prim + l.where(l != "", "")
    return df


def _regime(pos_yrs: int, n_yrs: int) -> str:
    if n_yrs == 0:
        return "NA"
    frac = pos_yrs / n_yrs
    return "STABLE" if frac >= 0.60 else ("2025-ARTIFACT" if frac <= 0.40 else "MIXED")


def baseline_per_signal(df: pd.DataFrame, min_n: int = 300) -> pd.DataFrame:
    rows = []
    have = df["r20"].notna()
    for s in ALL_SIG:
        m = (df[s] == 1) & have
        sub = df[m]
        if len(sub) < min_n:
            continue
        ym = sub.groupby("yr")["r20"].median()
        pos = int((ym > 0).sum()); nyr = len(ym)
        rows.append({
            "signal": s, "n": len(sub),
            "med10": round(sub["r10"].median()*100, 2),
            "med20": round(sub["r20"].median()*100, 2),
            "win": round((sub["r20"] > 0).mean()*100, 1),
            "fail": round((sub["r10"] <= -0.05).mean()*100, 1),
            "mfe": round(sub["mfe"].median()*100, 1),
            "mae": round(sub["mae"].median()*100, 1),
            "pos_yrs": pos, "n_yrs": nyr,
            "regime": _regime(pos, nyr),
            "y2026": round(sub[sub.yr=="2026"]["r20"].median()*100, 2) if len(sub[sub.yr=="2026"]) >= 20 else None,
        })
    out = pd.DataFrame(rows)
    # Status (mirrors their GOOD/AVERAGE/REJECT but on r20 median)
    def status(r):
        if r.med20 <= -0.1 or r.fail >= 28: return "REJECT"
        if r.med20 >= 0.7 and r.fail <= 20: return "GOOD"
        return "AVERAGE"
    out["status"] = out.apply(status, axis=1)
    return out.sort_values(["regime", "med20"], ascending=[True, False])


def build_sequences(df: pd.DataFrame, depth: int = 3, min_n: int = 150) -> pd.DataFrame:
    """N-bar token sequences ending at bar i (entry after i, fwd from i). Per-year
    regime flag built in. Returns rule DB sorted by robustness then med20."""
    d = df.sort_values(["ticker", "date"]).copy()
    g = d.groupby("ticker")["tok"]
    parts = [d["tok"]]
    for k in range(1, depth):
        parts.insert(0, g.shift(k))
    seq = parts[0].astype(str)
    for p in parts[1:]:
        seq = seq + " " + p.astype(str)
    d["seq"] = seq
    d = d[d["r20"].notna() & ~d["seq"].str.contains("nan", na=True)]
    rows = []
    for s, sub in d.groupby("seq"):
        if len(sub) < min_n:
            continue
        ym = sub.groupby("yr")["r20"].median()
        pos = int((ym > 0).sum()); nyr = len(ym)
        rows.append({
            "seq": s, "n": len(sub),
            "med20": round(sub["r20"].median()*100, 2),
            "win": round((sub["r20"] > 0).mean()*100, 1),
            "fail": round((sub["r10"] <= -0.05).mean()*100, 1),
            "pos_yrs": pos, "n_yrs": nyr,
            "maxyr_share": round(sub["yr"].value_counts(normalize=True).max()*100, 0),
            "regime": _regime(pos, nyr),
            "y2026": round(sub[sub.yr=="2026"]["r20"].median()*100, 2) if len(sub[sub.yr=="2026"]) >= 8 else None,
        })
    out = pd.DataFrame(rows)
    # ROBUST = STABLE regime AND not time-concentrated AND positive median
    out["robust"] = (out.regime == "STABLE") & (out.maxyr_share <= 40) & (out.med20 > 0)
    return out.sort_values(["robust", "med20"], ascending=[False, False])


if __name__ == "__main__":
    import sys
    _tf = sys.argv[1] if len(sys.argv) > 1 else "1d"
    print(f"[{_tf}] pulling bars + clean fwd + full signal set ...")
    df = _pull(tf=_tf)
    print(f"bars: {len(df):,}  signals scored: {len(ALL_SIG)}\n")
    base = baseline_per_signal(df)
    base.to_csv("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad/baseline_5yr_full.csv", index=False)
    print("=== ROBUST signals (STABLE, sorted by med20) — the real ones ===")
    st = base[base.regime == "STABLE"].sort_values("med20", ascending=False)
    print(f"{'signal':>12}{'n':>8}{'med20':>7}{'win':>6}{'fail':>6}{'yrs+':>6}{'2026':>7}{'status':>9}")
    for _, r in st.head(30).iterrows():
        y = f"{r.y2026:+.1f}" if r.y2026 is not None else "·"
        print(f"{r.signal:>12}{r.n:>8}{r.med20:>7.2f}{r.win:>6.0f}{r.fail:>6.0f}{r.pos_yrs:>4}/{r.n_yrs}{y:>7}{r.status:>9}")
    print(f"\nregime counts: {base.regime.value_counts().to_dict()}")
    print(f"GOOD/AVG/REJECT: {base.status.value_counts().to_dict()}")

    SD = "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad/"
    import json, os
    rule_export = {}
    for depth in (2, 3, 4):
        seqs = build_sequences(df, depth=depth, min_n=(150 if depth < 4 else 100))
        rob = seqs[seqs.robust]
        seqs.to_csv(f"{SD}seq_rules_{depth}bar_5yr.csv", index=False)
        for _, r in rob.iterrows():
            rule_export[r.seq] = {"depth": depth, "n": int(r.n), "med20": float(r.med20),
                                  "win": float(r.win), "fail": float(r.fail),
                                  "pos_yrs": int(r.pos_yrs), "n_yrs": int(r.n_yrs),
                                  "y2026": (None if r.y2026 is None or r.y2026 != r.y2026 else float(r.y2026))}
        print(f"\n===== {depth}-BAR sequences: {len(seqs)} (n>=150) · ROBUST(STABLE,no-yr>40%,med>0)={len(rob)} "
              f"· artifacts flagged={len(seqs)-len(rob)} =====")
        print(f"   {'sequence':>30}{'n':>6}{'med20':>7}{'win':>5}{'yrs+':>6}{'max%':>6}{'2026':>7}")
        for _, r in rob.head(18).iterrows():
            y = f"{r.y2026:+.1f}" if r.y2026 is not None else "·"
            print(f"   {r.seq:>30}{r.n:>6}{r.med20:>7.2f}{r.win:>5.0f}{r.pos_yrs:>4}/{r.n_yrs}{r.maxyr_share:>5.0f}%{y:>7}")
    _fn = "seq_rules.json" if _tf == "1d" else f"seq_rules_{_tf}.json"
    _path = os.path.join(os.path.dirname(__file__), _fn)
    with open(_path, "w") as f:
        json.dump(rule_export, f)
    print(f"\n✔ saved {len(rule_export)} robust rules → {_path}")
