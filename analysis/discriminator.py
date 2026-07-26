"""
discriminator.py — ANALYSIS ONLY (does NOT touch the production scoring engine).

Validates a 4-feature pre-breakout discriminator at scale on studio_analytics.duckdb.
Score = sum of 4 causal binary flags evaluated on a trailing 15-bar window ending
AT each candidate bar (a bar with a bullish T-signal). Forward = 10-day from the
candidate bar's close. Positive-skew-aware metrics. Episode de-dup. Per-universe.

Writes DISCRIMINATOR_VALIDATION.md.

Run:  cd backend && uv run python ../analysis/discriminator.py
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
WIN = 15            # trailing window (bars), inclusive of candidate bar
HORIZON = "10d"
DEDUP_BARS = 5     # collapse candidates within this many bars into one episode
CASE_STUDY = {"PAVS", "WNW", "GLOO"}   # design tickers -> held out for OOS check
OUT = "/Users/sachoki/Desktop/sachoki-desktop/DISCRIMINATOR_VALIDATION.md"

FWD, MFE, MAE = "fwd_10d", "mfe_10d", "mae_10d"


def load_universe(con, universe):
    df = con.execute(f"""
        SELECT universe, ticker, date, open, high, low, close,
               wyc_phase, vol_bucket, t_sig, sig_z1g, {FWD}, {MFE}, {MAE}
        FROM bars
        WHERE universe = ?
        ORDER BY ticker, date
    """, [universe]).fetchdf()
    # dedup literal duplicate (ticker,date) rows (DB has some) -> keep first
    df = df.drop_duplicates(subset=["ticker", "date"], keep="first").reset_index(drop=True)
    return df


def compute_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """Per-universe frame -> one row per de-duped candidate episode with the 4
    flags, score, and forward outcomes. Fully causal (no bar after candidate)."""
    out = []
    bull = (df["close"] > df["open"]).to_numpy()
    is_vb = (df["vol_bucket"] == "VB").to_numpy()
    is_demand = bull | is_vb
    is_acc = (df["wyc_phase"] == "ACC_TR").to_numpy()
    is_mkdn = (df["wyc_phase"] == "MKDN").to_numpy()
    z1g = (pd.to_numeric(df["sig_z1g"], errors="coerce").fillna(0).to_numpy() == 1)
    has_t = df["t_sig"].notna().to_numpy() & (df["t_sig"].astype(str).str.strip() != "")
    demand_range = np.where(is_demand, (df["high"] - df["low"]) / df["close"].replace(0, np.nan), 0.0)
    demand_range = np.nan_to_num(demand_range.astype(float), nan=0.0, posinf=0.0)

    df = df.assign(_bull=bull, _vb=is_vb, _acc=is_acc, _mkdn=is_mkdn, _z1g=z1g,
                   _hasT=has_t, _drange=demand_range)
    df["_ret"] = df.groupby("ticker")["close"].pct_change() * 100.0

    for tk, g in df.groupby("ticker", sort=False):
        n = len(g)
        if n < WIN:
            continue
        gi = g.reset_index(drop=True)
        close = gi["close"].to_numpy(float)
        low = gi["low"].to_numpy(float)
        acc = gi["_acc"].to_numpy(); mkdn = gi["_mkdn"].to_numpy()
        vbb = (gi["_vb"].to_numpy() & gi["_bull"].to_numpy())
        vbs = (gi["_vb"].to_numpy() & ~gi["_bull"].to_numpy())
        z1g_a = gi["_z1g"].to_numpy(); ret = gi["_ret"].to_numpy()
        drange = gi["_drange"].to_numpy()
        hasT = gi["_hasT"].to_numpy()

        # ---- rolling primitives (length WIN, ending at index i) ----
        s_acc = pd.Series(acc.astype(float)).rolling(WIN).mean().to_numpy()
        s_mkdn = pd.Series(mkdn.astype(float)).rolling(WIN).mean().to_numpy()
        s_vbb = pd.Series(vbb.astype(float)).rolling(WIN).sum().to_numpy()
        s_vbs = pd.Series(vbs.astype(float)).rolling(WIN).sum().to_numpy()
        s_drange = pd.Series(drange).rolling(WIN).max().to_numpy()

        # flag1 acc_tr : >=50% ACC_TR AND acc dominates markdown
        f1 = (s_acc >= 0.5) & (s_acc > s_mkdn)
        # flag2 bull_vb: >=1 demand-VB AND demand-VB > supply-VB
        f2 = (s_vbb >= 1) & (s_vbb > s_vbs)
        # flag4 range_exp: an explosive-range demand bar in window
        f4 = (s_drange >= 1.5)

        # flag3 spring_reclaim: a window bar makes a NEW window-low via Z1G or
        # <=-8% close, then is RECLAIMED (higher close) within next 3 bars.
        # sr_j computed per bar (reclaim looks j+1..j+3 forward); to keep the
        # candidate evaluation causal we only admit j<=t-3 (reclaim bars <= t).
        roll_low = pd.Series(low).rolling(WIN).min().to_numpy()          # window low ending at j
        is_new_low = low <= roll_low + 1e-12                              # j is the window low
        spring_j = (z1g_a | (ret <= -8.0)) & is_new_low
        # reclaimed within next 3 bars: max(close[j+1..j+3]) > close[j]
        fwd_close_max3 = pd.Series(close[::-1]).rolling(3, min_periods=1).max().shift(1).to_numpy()[::-1]
        reclaimed = fwd_close_max3 > close
        sr = spring_j & reclaimed                                         # per-bar spring+reclaim
        # candidate t flagged if any sr in [t-14 .. t-3]  (shift 3, window WIN-3=12)
        f3 = (pd.Series(sr.astype(float)).shift(3).rolling(WIN - 3).max().to_numpy() >= 1.0)

        score = (f1.astype(int) + f2.astype(int) + f3.astype(int) + f4.astype(int))
        valid = ~np.isnan(s_acc)   # full window available

        cand_idx = np.where(hasT & valid)[0]
        # episode de-dup: keep first, skip any within DEDUP_BARS positions
        kept, last = [], -10**9
        for i in cand_idx:
            if i - last > DEDUP_BARS:
                kept.append(i); last = i
        if not kept:
            continue
        sub = gi.iloc[kept]
        out.append(pd.DataFrame({
            "universe": sub["universe"].values, "ticker": tk, "date": sub["date"].values,
            "f1_acc_tr": f1[kept].astype(int), "f2_bull_vb": f2[kept].astype(int),
            "f3_spring": f3[kept].astype(int), "f4_range_exp": f4[kept].astype(int),
            "score": score[kept],
            FWD: sub[FWD].values, MFE: sub[MFE].values, MAE: sub[MAE].values,
        }))
    if not out:
        return pd.DataFrame()
    res = pd.concat(out, ignore_index=True)
    # clean forwards: keep finite only
    for c in (FWD, MFE, MAE):
        res[c] = pd.to_numeric(res[c], errors="coerce")
    res = res[np.isfinite(res[FWD]) & np.isfinite(res[MFE]) & np.isfinite(res[MAE])].copy()
    return res


# ---------- metrics ----------
def bucket_metrics(d: pd.DataFrame) -> dict:
    if len(d) == 0:
        return dict(n=0)
    fwd = d[FWD].clip(-100, 500)
    mfe = d[MFE]; mae = d[MAE].abs()
    med_mae = np.median(mae) if len(mae) else np.nan
    return dict(
        n=len(d),
        med_fwd=round(float(np.median(fwd)), 2),
        mean_fwd=round(float(np.mean(fwd)), 2),
        p25=round(float((mfe >= 25).mean() * 100), 1),
        p50=round(float((mfe >= 50).mean() * 100), 1),
        p100=round(float((mfe >= 100).mean() * 100), 1),
        rr=round(float(np.median(mfe) / med_mae), 2) if med_mae and med_mae > 0 else np.nan,
        win=round(float((d[FWD] > 0).mean() * 100), 1),
    )


def fmt_row(label, m):
    if m.get("n", 0) == 0:
        return f"| {label} | 0 | — | — | — | — | — | — | — |"
    return (f"| {label} | {m['n']} | {m['med_fwd']} | {m['mean_fwd']} | {m['p25']} | "
            f"{m['p50']} | {m['p100']} | {m['rr']} | {m['win']} |")


HEAD = "| bucket | n | med fwd% | mean fwd% | P(+25%) | P(+50%) | P(+100%) | RR | win% |\n|---|---|---|---|---|---|---|---|---|"


def per_bucket_table(d: pd.DataFrame) -> str:
    lines = [HEAD]
    for s in range(5):
        lines.append(fmt_row(f"score {s}", bucket_metrics(d[d["score"] == s])))
    lines.append("| | | | | | | | | |")
    lines.append(fmt_row("**LOW (<2)**", bucket_metrics(d[d["score"] < 2])))
    lines.append(fmt_row("**HIGH (>=2)**", bucket_metrics(d[d["score"] >= 2])))
    return "\n".join(lines)


def feature_lift_table(d: pd.DataFrame) -> str:
    feats = [("acc_tr", "f1_acc_tr"), ("bull_vb", "f2_bull_vb"),
             ("spring_reclaim", "f3_spring"), ("range_exp", "f4_range_exp")]
    lines = ["| feature | state | n | med fwd% | P(+50%) | P(+100%) | win% |",
             "|---|---|---|---|---|---|---|"]
    for name, col in feats:
        on, off = bucket_metrics(d[d[col] == 1]), bucket_metrics(d[d[col] == 0])
        for state, m in (("ON", on), ("OFF", off)):
            if m.get("n", 0) == 0:
                lines.append(f"| {name} | {state} | 0 | — | — | — | — |")
            else:
                lines.append(f"| {name} | {state} | {m['n']} | {m['med_fwd']} | {m['p50']} | {m['p100']} | {m['win']} |")
        # lift line
        if on.get("n") and off.get("n"):
            lines.append(f"| **{name} Δ** | ON−OFF | | {round(on['med_fwd']-off['med_fwd'],2)} | "
                         f"{round(on['p50']-off['p50'],1)} | {round(on['p100']-off['p100'],1)} | {round(on['win']-off['win'],1)} |")
    return "\n".join(lines)


def monotonic_p50(d: pd.DataFrame):
    vals = []
    for s in range(5):
        m = bucket_metrics(d[d["score"] == s])
        vals.append((s, m.get("n", 0), m.get("p50") if m.get("n") else None))
    return vals


def main():
    con = duckdb.connect(DB, read_only=True)
    frames = []
    for uni in ("russell2k", "nasdaq", "sp500"):
        print(f"loading {uni} ...", flush=True)
        df = load_universe(con, uni)
        print(f"  {len(df):,} bars -> computing candidates", flush=True)
        c = compute_candidates(df)
        print(f"  {len(c):,} episodes", flush=True)
        frames.append(c)
    con.close()
    allc = pd.concat(frames, ignore_index=True)
    allc.to_parquet("/Users/sachoki/Desktop/sachoki-desktop/analysis/_candidates.parquet")

    md = []
    md.append("# Pre-breakout discriminator — scale validation\n")
    md.append(f"_Source: studio_analytics.duckdb · window={WIN} bars (causal) · horizon={HORIZON} · "
              f"episode de-dup={DEDUP_BARS} bars · candidates = bars with a bullish T-signal._\n")
    md.append(f"_Total de-duped episodes (all universes, finite forwards): **{len(allc):,}**_\n")
    md.append("Metrics are positive-skew aware: P(+X%) is on max-10d-high gain (mfe_10d); "
              "RR = median(MFE)/median(|MAE|); win% on close fwd_10d. Median alone understates these lottery setups.\n")

    # per universe
    for uni in ("russell2k", "nasdaq", "sp500"):
        d = allc[allc["universe"] == uni]
        md.append(f"\n## {uni}  (n={len(d):,} episodes)\n")
        md.append("### Per-score bucket + HIGH vs LOW\n")
        md.append(per_bucket_table(d))
        md.append("\n\n### Per-feature individual lift\n")
        md.append(feature_lift_table(d))
        mono = monotonic_p50(d)
        md.append("\n\n**P(+50%) monotonicity 0→4:** " +
                  " → ".join(f"{s}:{(p if p is not None else 'n/a')}(n{n})" for s, n, p in mono) + "\n")

    # OOS: exclude case-study tickers
    oos = allc[~allc["ticker"].isin(CASE_STUDY)]
    md.append(f"\n## OUT-OF-SAMPLE — held out PAVS/WNW/GLOO (n={len(oos):,})\n")
    md.append("Pooled across universes for the held-out check (separation should persist).\n\n")
    md.append(per_bucket_table(oos))
    md.append("\n\n### OOS per-feature lift\n")
    md.append(feature_lift_table(oos))
    md.append("\n\n**OOS P(+50%) monotonicity 0→4:** " +
              " → ".join(f"{s}:{(p if p is not None else 'n/a')}(n{n})" for s, n, p in monotonic_p50(oos)) + "\n")

    # case-study tickers themselves (sanity, in-sample)
    isd = allc[allc["ticker"].isin(CASE_STUDY)]
    md.append(f"\n## Case-study tickers (in-sample, sanity) — PAVS/WNW/GLOO (n={len(isd):,})\n")
    if len(isd):
        md.append("| ticker | univ | date | score | acc | vb | spring | rng | fwd% | mfe% | mae% |")
        md.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for _, r in isd.sort_values(["ticker", "date"]).iterrows():
            md.append(f"| {r['ticker']} | {r['universe']} | {str(r['date'])[:10]} | {r['score']} | "
                      f"{r['f1_acc_tr']} | {r['f2_bull_vb']} | {r['f3_spring']} | {r['f4_range_exp']} | "
                      f"{round(r[FWD],1)} | {round(r[MFE],1)} | {round(r[MAE],1)} |")
    else:
        md.append("_(no episodes — tickers absent from this universe set)_")

    with open(OUT, "w") as f:
        f.write("\n".join(md) + "\n")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
