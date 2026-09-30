"""BOTTOM_CLUSTER_V1 — PART B, the forward confirmation (research_out/BOTTOM_CLUSTER_V1.md).

The rule was SEEN on all history (a NATURE_SHIFT_V1 comparator), so the only clean test is forward.
Frozen 2026-09-28; entries with date_in ≥ 2026-09-29; read ONCE on/after 2027-03-01.

Rules (▽△ anatomy verdict from data/anatomy_signals.parquet — same predicates as the catalog keys
x_anat_rev_rs / x_anat_rev_norm / x_anat_shake in frontend/src/lib/v4ExtraGroups.js):
  bottom(t)  = anat_v == 'rev'  or  anat_v == 'shake'
  B1 CLUSTER = bottom(t) and ≥1 other bottom bar in t-4..t-1                (the frozen rule)
  B2 CL·🔻💪 = B1 and anat_v(t) == 'rev' and anat_rs(t)                      (AMENDMENT_1, declared
               2026-09-28 before any forward data, from the seen-data decomposition)
  B3 🔻💪+T9 = 🔻💪 (anat_v 'rev' and anat_rs) on t AND T/Z state T9 on t      (AMENDMENT_2, 2026-09-28,
  B4 🕐DR+Z2G = 🕐DR edge (edge_replay h1dr_chip) on t AND T/Z state Z2G on t    TZ_X_BOTTOM_V1 — the 2 cells
               that passed MINE→VERIFY; declared before any forward data. Forward k = 4.)
  B3/B4 additionally need Δ ≥ their anchor's forward Δ (🔻💪 alone / 🕐DR alone) — the state must add.
  B5 T5L46ED  / B6 Z2GL46ED / B7 Z9L46NUR = package composites (T/Z state + l_sig + full_suffix) that passed
      TZPKG_REVISIT_V1 Stage 2 (AMENDMENT_3, 2026-09-28, declared before any forward data). Each must also beat its
      anchor = the same T/Z state without the composite (A_T5 / A_Z2G / A_Z9). PASS for B5-B7 = the research claim
      itself: same-day PAIRED difference (composite mean − anchor mean, days with both) > 0 with day-clustered CI lo > 0.
      Forward k = 7.
⚠️ NOTE 2026-09-29 (recorded before any forward data; rules NOT changed): the B3-B7 research contrasts were not
ATR-matched. Within day × ATR% (T9_RS_V1, TZ_BOOSTERS_V1 AMENDMENT_1): B3 T9 adds nothing to 🔻💪 (−1.06 pp 2024-26),
B4 Z2G adds nothing to 🕐DR (−0.86), B5/B6/B7 ≈ 0 (+0.16 / −0.35 / +0.59). Expect B3-B7 to FAIL their anchor tests.
  B8 GX = inside any T/Z state, ⚛ MARKUP (golden cross EMA50>EMA200) vs ⚛ MKDN (death cross), strata = day × ATR%-bin × state × 🏆RS
  B9 GX·RS = inside any T/Z state, (MARKUP ∧ RS) vs (MKDN ∧ ¬RS), strata = day × ATR%-bin × state
      (AMENDMENT_4, 2026-09-30, user OK "samive"; from WYC_AXIS_V1 / GX_RS_V1 — seen-data VERIFY 2024-26: B8 +1.35 [1.10, 1.61],
      B9 +2.07 [1.72, 2.42]; declared before any forward data). Estimand = the research one: PER-BAR book return (entry open[t+1],
      trail clip(12·ATR%, 15, 60), maxh 60, 15 bps, NO cooldown), paired within the strata, day-clustered. ATR% bins use the FROZEN
      MINE-window cut points ATR_CUTS below. PASS = mean day Δ > 0 with 95 % CI lo > 0. Forward k = 9.
Eligible: close ≥ $5, 20-day mean $volume ≥ $5M. Entry open[t+1], book _pathsim ATR×12 trail, maxh 60,
slip .0015, 5-bar cooldown. Control: every eligible bar on the stride-10 control_keys.phase grid.
Only trades whose full 60-session horizon lies inside the data are scored (right-edge censoring).
PASS per rule: same-day Δ vs control > 0 with day-clustered 95 % CI lo > 0 (days with ≥ 20 control trades).

  python bottom_cluster_forward.py                      → the forward read (refuses before 2027-03-01)
  python bottom_cluster_forward.py --check 2024-01-01 2024-12-31
                                                        → the same code on a SEEN window, to verify
                                                          it reproduces the research (no verdict)
"""
from __future__ import annotations
import os, sys
from datetime import date

import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from edge_replay import _pathsim                       # noqa: E402
from control_keys import phase                         # noqa: E402
from studio.paths import db_path, DATA_DIR             # noqa: E402

FORWARD_START = "2026-09-29"
READ_ON_OR_AFTER = date(2027, 3, 1)
HORIZON = 60
# AMENDMENT_4 — ATR%-bin cut points frozen from the research MINE window (2021-23, rowseq ok bars); never re-estimate.
ATR_CUTS = [0.01785, 0.020244, 0.022135, 0.02382, 0.025396, 0.02694, 0.028528, 0.030209, 0.032028, 0.034012, 0.036232,
            0.038752, 0.041687, 0.045093, 0.049218, 0.054449, 0.061351, 0.071419, 0.089267, 0.11816, 0.212641]


def _bar_returns(o, h, l, c, atr, tk, idx):
    """Per-bar book return for signal bars `idx` (entry open[t+1], ATR×12 trail, maxh 60, 15 bps, gap fills at open)."""
    out = np.full(len(idx), np.nan); S = 0.0015; n = len(o)
    for q, t in enumerate(idx):
        end = t + 1 + HORIZON
        if end > n or tk[end - 1] != tk[t] or not (o[t + 1] > 0) or not (atr[t] > 0) or not (c[t] > 0):
            continue
        tr = min(0.60, max(0.15, 12.0 * atr[t] / c[t])); entry = o[t + 1] * (1 + S); pk = entry; r = np.nan
        for j in range(t + 1, end):
            if j > t + 1 and o[j] <= pk * (1 - tr):
                r = o[j] / entry - 1 - S; break
            pk = max(pk, h[j]); ts = pk * (1 - tr)
            if l[j] <= ts:
                r = ts / entry - 1 - S; break
        out[q] = r if r == r else c[end - 1] / entry - 1 - S
    return out


def _paired(frame: pd.DataFrame, rng) -> dict:
    """frame: d, stratum, r, g (bool group). Paired group − rest within stratum, n(group)-weighted per day, day bootstrap."""
    g = frame.groupby(["d", "k", "g"]).r.mean().unstack("g").dropna()
    if g.empty or True not in g or False not in g:
        return {"days": 0, "note": "no paired strata"}
    w = frame[frame.g].groupby(["d", "k"]).size().reindex(g.index)
    z = pd.DataFrame({"x": (g[True] - g[False]).to_numpy() * w.to_numpy(), "w": w.to_numpy(), "d": g.index.get_level_values(0)})
    s_ = z.groupby("d")[["x", "w"]].sum(); x = (s_.x / s_.w * 100).to_numpy()
    if len(x) < 10:
        return {"days": len(x), "note": "too few paired days"}
    lo, hi = np.percentile([x[rng.integers(0, len(x), len(x))].mean() for _ in range(3000)], [2.5, 97.5])
    return {"days": len(x), "n_group": int(frame.g.sum()), "paired_delta_pp": round(float(x.mean()), 2),
            "ci95": [round(lo, 2), round(hi, 2)], "PASS": bool(x.mean() > 0 and lo > 0)}


def run(d0: str, d1: str | None) -> dict:
    load_from = (pd.Timestamp(d0) - pd.Timedelta(days=150)).strftime("%Y-%m-%d")    # ATR / $vol / t-4 warm-up
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    bars = con.execute(f"""WITH r AS (SELECT ticker, date, open, high, low, "close", volume,
            row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
            FROM bars WHERE universe <> 'index' AND date >= '{load_from}')
        SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
    bars["date"] = pd.to_datetime(bars["date"]).dt.strftime("%Y-%m-%d")
    an = duckdb.connect().execute(f"SELECT ticker, CAST(date AS VARCHAR) date, v, rs FROM "
                                  f"read_parquet('{DATA_DIR}/anatomy_signals.parquet') WHERE date >= '{load_from}'").fetchdf()
    an["date"] = an["date"].str[:10]
    df = bars.merge(an.drop_duplicates(["ticker", "date"]), on=["ticker", "date"], how="left").reset_index(drop=True)
    g = df.groupby("ticker", sort=False)
    pc = g["close"].shift(1)
    tr = np.maximum(df.high - df.low, np.maximum((df.high - pc).abs(), (df.low - pc).abs()))
    df["atr_14"] = tr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    dv = (df["close"] * df["volume"]).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    el = ((df["close"] >= 5) & (dv >= 5e6) & df["atr_14"].notna()).to_numpy()
    v = df["v"].fillna("").to_numpy(); rs = df["rs"].fillna(False).astype(bool).to_numpy()
    tzq = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True).execute(f"""WITH r AS (SELECT ticker,
            cast(date as varchar)[:10] date, t_sig, z_sig, l_sig, full_suffix, phys_wyc, row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
            FROM bars WHERE universe <> 'index' AND date >= '{load_from}') SELECT ticker, date, t_sig, z_sig, l_sig, full_suffix, phys_wyc FROM r WHERE rn = 1""").fetchdf()
    df = df.merge(tzq, on=["ticker", "date"], how="left")
    tzs = df["t_sig"].fillna("").astype(str).where(df["t_sig"].fillna("").astype(str) != "", df["z_sig"].fillna("").astype(str)).to_numpy()
    import edge_replay
    grp_e, _ = edge_replay._frame(64, 3_000_000)
    dr_set = set()
    for t_, g_ in grp_e.items():
        if "h1dr_chip" in g_:
            for d_ in g_["date"].astype(str).str[:10].to_numpy()[g_["h1dr_chip"].to_numpy(bool)]:
                if d_ >= load_from: dr_set.add((t_, d_))
    del grp_e
    dr = np.array([k in dr_set for k in zip(df.ticker, df.date)])
    tk = df.ticker.to_numpy(); n = len(df)
    bot = (v == "rev") | (v == "shake")
    prev = np.zeros(n, np.int16)
    for j in range(1, 5):
        s = np.zeros(n, bool); s[j:] = bot[:-j]; s[np.r_[np.zeros(j, bool), tk[j:] != tk[:-j]]] = False; prev += s
    df["B1"] = bot & (prev >= 1) & el
    df["B2"] = df["B1"].to_numpy() & (v == "rev") & rs
    rsb = (v == "rev") & rs & el
    df["A_RS"] = rsb; df["B3"] = rsb & (tzs == "T9")
    df["A_DR"] = dr & el; df["B4"] = dr & el & (tzs == "Z2G")
    comp = pd.Series(tzs).astype(str) + df["l_sig"].fillna("").astype(str).str.strip().to_numpy() + df["full_suffix"].fillna("").astype(str).str.strip().to_numpy()
    comp = comp.to_numpy()
    for rule, anchor, st, cp in (("B5", "A_T5", "T5", "T5L46ED"), ("B6", "A_Z2G", "Z2G", "Z2GL46ED"), ("B7", "A_Z9", "Z9", "Z9L46NUR")):
        df[anchor] = el & (tzs == st) & (comp != cp); df[rule] = el & (comp == cp)
    starts = np.r_[0, np.flatnonzero(tk[1:] != tk[:-1]) + 1]; ends = np.r_[starts[1:], n]
    pos = np.arange(n) - np.repeat(starts, ends - starts)
    ph = pd.Series(tk).map(lambda t: phase(t, 10)).to_numpy()
    df["CTRL"] = el & ((pos - ph) % 10 == 0)
    grp = {t: x.reset_index(drop=True) for t, x in df[["ticker", "date", "open", "high", "low", "close", "atr_14",
                                                        "B1", "B2", "B3", "B4", "A_RS", "A_DR",
                                                        "B5", "B6", "B7", "A_T5", "A_Z2G", "A_Z9", "CTRL"]].groupby("ticker", sort=False)}
    cal = sorted(df.date.unique())
    last_ok = cal[-(HORIZON + 2)] if len(cal) > HORIZON + 2 else cal[0]     # entry must have 60 sessions after it
    hi = min(d1, last_ok) if d1 else last_ok
    sim = lambda c: _pathsim(grp, c, "trail", 0.10, 0.25, 0.25, HORIZON, slip=0.0015, atr_k=12.0)
    keep = lambda t: t[(t.date_in >= d0) & (t.date_in <= hi)]
    ctrl = keep(sim("CTRL"))
    cd = ctrl.groupby("date_in").ret.agg(["mean", "size"])
    rng = np.random.default_rng(20260928)
    out = {"window": [d0, hi], "control_trades": len(ctrl)}
    for rule in ("A_RS", "A_DR", "A_T5", "A_Z2G", "A_Z9", "B1", "B2", "B3", "B4", "B5", "B6", "B7"):
        t = keep(sim(rule))
        dm = t.groupby("date_in").ret.mean().to_frame("s").join(cd, how="inner"); dm = dm[dm["size"] >= 20]
        x = ((dm.s - dm["mean"]) * 100).to_numpy()
        if len(x) < 5:
            out[rule] = {"n": len(t), "days": len(x), "note": "too few scored days"}; continue
        lo, hi_ = np.percentile([x[rng.integers(0, len(x), len(x))].mean() for _ in range(3000)], [2.5, 97.5])
        out[rule] = {"n": len(t), "days": len(x), "mean_ret_pct": round(100 * t.ret.mean(), 2),
                     "same_day_delta_pp": round(float(x.mean()), 2), "ci95": [round(lo, 2), round(hi_, 2)],
                     "PASS": bool(x.mean() > 0 and lo > 0)}
    for rule, anchor in (("B3", "A_RS"), ("B4", "A_DR")):         # the state must add over its anchor
        if isinstance(out.get(rule), dict) and "PASS" in out[rule] and "same_day_delta_pp" in out.get(anchor, {}):
            out[rule]["PASS"] = bool(out[rule]["PASS"] and out[rule]["same_day_delta_pp"] >= out[anchor]["same_day_delta_pp"])
    sims = {}
    for rule, anchor in (("B5", "A_T5"), ("B6", "A_Z2G"), ("B7", "A_Z9")):      # paired same-day composite − own signal
        a_ = keep(sim(rule)).groupby("date_in").ret.mean(); b_ = keep(sim(anchor)).groupby("date_in").ret.mean()
        x = ((a_ - b_).dropna() * 100).to_numpy()
        if isinstance(out.get(rule), dict) and len(x) >= 10:
            lo2, hi2 = np.percentile([x[rng.integers(0, len(x), len(x))].mean() for _ in range(3000)], [2.5, 97.5])
            out[rule]["paired_vs_anchor_pp"] = round(float(x.mean()), 2); out[rule]["paired_ci95"] = [round(lo2, 2), round(hi2, 2)]
            out[rule]["PASS"] = bool(x.mean() > 0 and lo2 > 0)
    # ── AMENDMENT_4: B8 / B9 (golden-cross regime inside T/Z; per-bar returns, paired, ATR-matched) ──
    sx = duckdb.connect().execute(f"SELECT ticker, CAST(date AS VARCHAR) date, rs AS rs_shape FROM "
                                  f"read_parquet('{DATA_DIR}/shapectx_signals.parquet') WHERE date >= '{load_from}'").fetchdf()
    sx["date"] = sx["date"].str[:10]
    rsx = df[["ticker", "date"]].merge(sx.drop_duplicates(["ticker", "date"]), on=["ticker", "date"], how="left")["rs_shape"]
    rs_s = rsx.fillna(False).astype(bool).to_numpy()
    wyc = df["phys_wyc"].fillna("").astype(str).to_numpy()
    gc, dcx = wyc == "MARKUP", wyc == "MKDN"
    dts = df["date"].to_numpy()
    cand = np.flatnonzero(el & (tzs != "") & (gc | dcx) & (dts >= d0) & (dts <= hi))
    O_, H_, L_, C_, A_ = (df[x].to_numpy(float) for x in ("open", "high", "low", "close", "atr_14"))
    rets = _bar_returns(O_, H_, L_, C_, A_, tk, cand)
    okr = ~np.isnan(rets); cand, rets = cand[okr], rets[okr]
    abin = np.digitize(A_[cand] / C_[cand], ATR_CUTS)
    st_code = pd.factorize(tzs[cand])[0]
    base = pd.DataFrame({"d": dts[cand], "r": rets})
    f8 = base.assign(k=(abin + 32 * st_code) * 2 + rs_s[cand], g=gc[cand])
    out["B8"] = _paired(f8, rng); out["B8"]["rule"] = "GC vs DC inside T/Z, RS held equal"
    m9 = (gc[cand] & rs_s[cand]) | (dcx[cand] & ~rs_s[cand])
    f9 = base.assign(k=abin + 32 * st_code, g=gc[cand] & rs_s[cand])[m9]
    out["B9"] = _paired(f9, rng); out["B9"]["rule"] = "GC∧RS vs DC∧¬RS inside T/Z"
    for anchor in ("A_RS", "A_DR", "A_T5", "A_Z2G", "A_Z9"):
        if isinstance(out.get(anchor), dict): out[anchor].pop("PASS", None); out[anchor]["role"] = "anchor (reference, not a test)"
    return out


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--check":
        print("SEEN-WINDOW CHECK (no verdict):", run(sys.argv[2], sys.argv[3]))
    elif len(sys.argv) == 1:
        if date.today() < READ_ON_OR_AFTER:
            sys.exit(f"refusing: the forward read is registered for on/after {READ_ON_OR_AFTER} (single read)")
        print("FORWARD READ (BOTTOM_CLUSTER_V1 PART B):", run(FORWARD_START, None))
    else:
        sys.exit(__doc__)
