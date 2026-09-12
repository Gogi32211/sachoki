"""SIGNAL CO-OCCURRENCE MAP — which marks fire together, and which lead which.

WHAT THIS IS, AND WHAT IT IS NOT
  DESCRIPTIVE ONLY. It reads no forward return, runs no path-sim, and produces no verdict, so it
  spends NO multiplicity budget. It answers one question: across the whole book of display layers
  and edges, what actually co-occurs, and what tends to arrive FIRST?

  It cannot say whether any of it pays. Anything it surfaces becomes a HYPOTHESIS that would need
  its own sealed family with a reserved OOS window ([[feedback-analysis-standard]]). That order —
  map first, then one targeted sealed test — is the point: the shape programme burned k = 21 on
  four families because each question was asked before the map existed.

  Prompted by AMD 2026-03-30..04-02, where the user noticed the display layers turning BEFORE the
  book edges did: 03-30 L46VX + ○○○ at the floor -> 03-31 L34VH + UDN N·D+ flipping to U·U+ + a MID
  shape -> 04-01 P55 fires -> 04-02 HB15 fires, +11 % over three sessions. That is n = 1, chosen
  after it worked. This is the denominator for it.

MEASURES (all on the liquid 1D universe, one row per ticker-session)
  base       P(A) — how often each mark fires at all. A mark on 40 % of bars is not a signal.
  same-bar   lift(A,B) = P(A and B) / (P(A) P(B)). 1.0 = independent. Reported with the raw
             co-occurrence count, because a lift of 8 on 11 events is noise.
  lead-lag   lead(A->B, k) = P(B within the next k bars | A) / P(B within any k bars).
             Computed BOTH directions; the asymmetry is the interesting part. A pair that only
             looks strong in one direction is a candidate ordering, not just an association.
  Jaccard    |A and B| / |A or B| — guards against the lift illusion of two rare marks.

WHY LIFT AND NOT CORRELATION
  These are sparse binary events with very different base rates (from 0.2 % to 40 %). Pearson
  correlation on such columns is dominated by the shared zeros and reads as "everything is
  unrelated". Lift conditions on the event actually happening.

INPUTS — the built display-layer parquets plus the engine frame:
  shapectx · lbal · lvx · ovdmap · vol7 signals parquets, and edge_replay's own frame for the
  E_* edge masks and the T/Z/L tokens, so the map covers exactly what the app shows.

RUN
  backend/.venv/bin/python backend/signal_cooccurrence_map.py            # full map
  backend/.venv/bin/python backend/signal_cooccurrence_map.py --lag 5    # different lead-lag window
"""
from __future__ import annotations
import os
import sys
import json
import time
import argparse

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from studio.paths import DATA_DIR                                        # noqa: E402

OUT_DIR = "/Users/sachoki/MASSIVE_DATA/SIGNAL_COOCCURRENCE"
WINDOW = ("2021-09-07", "2026-09-09")
PX_MIN, DV_FLOOR = 21.0, 3_000_000
MIN_EVENTS = 200          # a mark rarer than this is listed but never ranked
DEFAULT_LAG = 3


# ── the marks, grouped by layer. Each entry: (name, parquet column expression) ────────────────
def _shape_marks():
    return {
        "SHP:MTH": "code = 'MTH'", "SHP:CL4": "code = 'CL4'", "SHP:MID": "code = 'MID'",
        "SHP:EXP": "code = 'EXP'", "SHP:CON": "code = 'CON'", "SHP:LST": "code = 'LST'",
        "SHP:WRP": "code = 'WRP'",
        "SHP:LST_up_VETO": "lstup_veto", "SHP:knife_VETO": "veto",
        "SHP:absorbed": "absorb AND shape", "SHP:dry": "dry AND shape",
        "SHP:floor": "floor AND shape", "SHP:key": "key AND shape", "SHP:rs": "rs AND shape",
        "SHP:diversity": "by_fam", "SHP:density": "by_den",
        "SHP:both_cluster": "by_fam AND by_den",
    }


LAYERS = [
    # (label, parquet, key cols, {mark: sql-bool})
    ("SHAPE", "shapectx_signals.parquet", _shape_marks()),
    # BOTH counting modes are carried. The parquet's primary columns are V (only 15m bars with
    # volume > SMA20 — the setting the user's chart usually shows) and the *_all columns are every
    # labelled bar. They genuinely disagree on individual bars: AMD 2026-03-30 is N·D+ ○○○ in V and
    # D·D+ with no mark in ALL. Reading one silently, as I did when describing that bar to the
    # user, makes a claim ambiguous — so each mode gets its own named marks.
    ("LBAL", "lbal_signals.parquet", {
        "UDNv:U": "udn = 'U'", "UDNv:D": "udn = 'D'", "UDNv:N": "udn = 'N'",
        "UDNv:U+": "udn_c = 'U'", "UDNv:D+": "udn_c = 'D'",
        "UDNv:star": "star", "UDNv:conflict": "conflict",
        "UDNv:half_up": "half_up", "UDNv:half_dn": "half_dn", "UDNv:XXX": "nn",
        "UDNa:U": "udn_all = 'U'", "UDNa:D": "udn_all = 'D'", "UDNa:N": "udn_all = 'N'",
        "UDNa:U+": "udn_c_all = 'U'", "UDNa:D+": "udn_c_all = 'D'",
        "UDNa:star": "star_all", "UDNa:conflict": "conflict_all",
        "UDNa:half_up": "half_up_all", "UDNa:half_dn": "half_dn_all", "UDNa:XXX": "nn_all",
    }),
    ("LVX", "lvx_signals.parquet", {
        "LVX:L34": "fam = 'L34'", "LVX:L46": "fam = 'L46'",
        "LVX:tier>=2": "tier >= 2", "LVX:tier>=3": "tier >= 3", "LVX:VX(tier4)": "tier >= 4",
    }),
    ("OVD", "ovdmap_signals.parquet", {
        "OVD:any": "tokens <> ''",
        "OVD:OB": "tokens LIKE '%OB%'", "OVD:RC": "tokens LIKE '%RC%'",
        "OVD:CD": "tokens LIKE '%CD%'", "OVD:HO": "tokens LIKE '%HO%'",
    }),
    # vol7 stores the jump as a signed level transition (`trans`) and the model disagreement as
    # `cons` ('Σ+' / 'MR+' / '='), not as separate booleans — read them from those columns.
    ("VOL7", "vol7_signals.parquet", {
        "V7:M0": "mr = 0", "V7:M5": "mr = 5", "V7:M6": "mr = 6",
        "V7:jump_up": "trans >= 2", "V7:jump_dn": "trans <= -2",
        "V7:sigma_plus": "cons = 'Σ+'", "V7:mr_plus": "cons = 'MR+'", "V7:VB2": "vb2",
        "V7:SHIFT_up": "shift_up", "V7:SHIFT_dn": "shift_dn",
    }),
]


def _cols(con, path):
    return {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}


def build_frame(log=print) -> pd.DataFrame:
    """One row per (ticker, date) in the liquid window, one boolean column per mark."""
    import duckdb
    import edge_replay as E
    con = duckdb.connect()
    t0 = time.time()

    # ── spine: the engine frame, so the universe is exactly what the app scans ──
    grp, as_of = E._frame(60, float(DV_FLOOR))
    edge_cols = [c for c in next(iter(grp.values())).columns if c.startswith("E_")]
    keep_tok = [c for c in ("t_sig", "z_sig", "l_sig", "vol_bucket", "rsi_14", "close") if
                c in next(iter(grp.values())).columns]
    parts = []
    for tk, g in grp.items():
        p = g[["date"] + keep_tok + edge_cols].copy()
        p["ticker"] = tk
        parts.append(p)
    spine = pd.concat(parts, ignore_index=True)
    spine["date"] = spine["date"].astype(str).str[:10]
    spine = spine[(spine["date"] >= WINDOW[0]) & (spine["date"] <= WINDOW[1])]
    del parts, grp
    log(f"  spine {len(spine):,} bars · {spine['ticker'].nunique():,} tickers · "
        f"{len(edge_cols)} edge masks · as_of {as_of} ({time.time()-t0:.0f}s)")

    marks: dict[str, np.ndarray] = {}
    # book edges (each its own mark) + the T/Z/L tokens the chart shows
    for c in edge_cols:
        marks[f"EDGE:{c[2:]}"] = spine[c].fillna(False).to_numpy(bool)
    if "l_sig" in spine:
        for lab in ("L34", "L46", "L12", "L25"):
            marks[f"L:{lab}"] = (spine["l_sig"].astype(str) == lab).to_numpy()
    if "vol_bucket" in spine:
        for lab in ("B", "VB", "W"):
            marks[f"VOLB:{lab}"] = (spine["vol_bucket"].astype(str) == lab).to_numpy()

    key = spine[["ticker", "date"]].reset_index(drop=True)
    con.register("spine_keys", key)

    for label, fname, exprs in LAYERS:
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            log(f"  ! {label}: {fname} missing — skipped")
            continue
        have = _cols(con, path)
        usable = {m: e for m, e in exprs.items()
                  if all(tok in have for tok in _tokens(e))}
        skipped = sorted(set(exprs) - set(usable))
        if skipped:
            log(f"  ! {label}: no column for {skipped}")
        if not usable:
            continue
        sel = ", ".join(f"({e}) AS \"{m}\"" for m, e in usable.items())
        df = con.execute(f"""
            SELECT k.ticker, k.date, {sel}
            FROM spine_keys k LEFT JOIN read_parquet('{path}') p
              ON p.ticker = k.ticker AND CAST(p.date AS VARCHAR) = k.date
        """).fetchdf()
        for m in usable:
            marks[m] = df[m].fillna(False).to_numpy(bool)
        log(f"  {label}: {len(usable)} marks joined ({time.time()-t0:.0f}s)")

    con.close()
    out = pd.DataFrame(marks)
    out.insert(0, "date", key["date"].to_numpy())
    out.insert(0, "ticker", key["ticker"].to_numpy())
    return out.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)


def _tokens(expr: str):
    """Bare identifiers in a simple boolean expression — used to check the parquet has them.

    STRING LITERALS MUST BE STRIPPED FIRST. Without that, `code = 'MTH'` yields the token MTH,
    the check looks for a column called MTH, finds none, and silently drops the mark. That is
    exactly what happened on the first run: all seven shape codes, every UDN state, both LVX
    families and all four OVD tokens vanished, and the map was built from what was left."""
    import re
    kw = {"AND", "OR", "NOT", "LIKE", "IS", "NULL", "TRUE", "FALSE"}
    bare = re.sub(r"'[^']*'", " ", expr)
    return [t for t in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", bare) if t.upper() not in kw]


def cooccurrence(df: pd.DataFrame, lag: int, log=print) -> tuple[pd.DataFrame, pd.DataFrame]:
    names = [c for c in df.columns if c not in ("ticker", "date")]
    M = df[names].to_numpy(bool)
    n = len(df)
    base = M.mean(axis=0)
    cnt = M.sum(axis=0)
    log(f"  {len(names)} marks over {n:,} ticker-sessions")

    # ── same-bar lift ──
    inter = (M.astype(np.float32).T @ M.astype(np.float32))       # |A ∧ B|
    rows = []
    for i, a in enumerate(names):
        for j, b in enumerate(names):
            if j <= i:
                continue
            both = float(inter[i, j])
            if both == 0 or cnt[i] < MIN_EVENTS or cnt[j] < MIN_EVENTS:
                continue
            exp = base[i] * base[j] * n
            union = cnt[i] + cnt[j] - both
            pba, pab = both / cnt[i], both / cnt[j]
            rows.append(dict(a=a, b=b, n_a=int(cnt[i]), n_b=int(cnt[j]), both=int(both),
                             lift=round(both / exp, 3) if exp else None,
                             jaccard=round(both / union, 4) if union else None,
                             p_b_given_a=round(pba, 4), p_a_given_b=round(pab, 4),
                             # NESTED: one mask is (near) a strict subset of the other — a
                             # DEFINITION, not a discovery. E_l43triple_rs is E_l43triple with the
                             # RS gate bolted on, so it scores lift 8303 and drowns the map.
                             nested=bool(pba > 0.98 or pab > 0.98),
                             layer_a=a.split(":")[0], layer_b=b.split(":")[0],
                             cross_layer=a.split(":")[0] != b.split(":")[0]))
    same = pd.DataFrame(rows).sort_values("lift", ascending=False)

    # ── lead-lag: does A tend to arrive BEFORE B? ──
    # "B within the next `lag` bars" per ticker, computed by shifting the ticker-blocked matrix.
    tk = df["ticker"].to_numpy()
    newblk = np.r_[True, tk[1:] != tk[:-1]]
    blk_id = np.cumsum(newblk) - 1
    fwd = np.zeros_like(M)
    for k in range(1, lag + 1):
        sh = np.zeros_like(M)
        sh[:-k] = M[k:]
        same_blk = np.r_[blk_id[k:] == blk_id[:-k], np.zeros(k, bool)]
        fwd |= sh & same_blk[:, None]
    fwd_base = fwd.mean(axis=0)
    inter_f = (M.astype(np.float32).T @ fwd.astype(np.float32))   # |A now ∧ B within lag|
    rl = []
    for i, a in enumerate(names):
        if cnt[i] < MIN_EVENTS:
            continue
        for j, b in enumerate(names):
            if i == j or cnt[j] < MIN_EVENTS or fwd_base[j] == 0:
                continue
            hit = float(inter_f[i, j])
            if hit == 0:
                continue
            p = hit / cnt[i]
            rl.append(dict(first=a, then=b, n_first=int(cnt[i]), hits=int(hit),
                           p_then_given_first=round(p, 4), p_then_base=round(float(fwd_base[j]), 4),
                           lead_lift=round(p / fwd_base[j], 3)))
    lead = pd.DataFrame(rl)
    if len(lead):
        # the asymmetry IS the finding: A→B strong while B→A is not
        rev = lead.set_index(["first", "then"])["lead_lift"]
        lead["rev_lift"] = [rev.get((b, a), np.nan) for a, b in zip(lead["first"], lead["then"])]
        lead["asymmetry"] = (lead["lead_lift"] - lead["rev_lift"]).round(3)
        lead = lead.sort_values("lead_lift", ascending=False)
    return same, lead, pd.DataFrame(dict(mark=names, n=cnt, base=np.round(base, 5))).sort_values("n", ascending=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lag", type=int, default=DEFAULT_LAG)
    a = ap.parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)
    t0 = time.time()
    print(f"SIGNAL_COOCCURRENCE · window {WINDOW} · lag {a.lag} · DESCRIPTIVE (no outcome, no k)", flush=True)
    df = build_frame()
    same, lead, base = cooccurrence(df, a.lag)
    base.to_csv(f"{OUT_DIR}/base_rates.csv", index=False)
    same.to_csv(f"{OUT_DIR}/same_bar_lift.csv", index=False)
    lead.to_csv(f"{OUT_DIR}/lead_lag_{a.lag}.csv", index=False)
    df.to_parquet(f"{OUT_DIR}/marks_matrix.parquet", index=False)
    json.dump(dict(window=list(WINDOW), lag=a.lag, rows=int(len(df)),
                   marks=int(len(base)), min_events=MIN_EVENTS,
                   built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   note="DESCRIPTIVE ONLY — no forward return is read; spends no multiplicity budget"),
              open(f"{OUT_DIR}/SPEC.json", "w"), indent=1)
    print(f"\nsaved -> {OUT_DIR} ({time.time()-t0:.0f}s)")
    pd.set_option("display.width", 200)
    print(f"\n═══ base rates (top 25 of {len(base)}) ═══")
    print(base.head(25).to_string(index=False))
    real = same[~same["nested"]]
    cross = real[real["cross_layer"]]
    print(f"\n═══ SAME-BAR, CROSS-LAYER (nested masks and same-layer pairs excluded) ═══")
    print(f"    {len(same):,} pairs · {int(same['nested'].sum()):,} nested (definitional, dropped) · "
          f"{len(cross):,} cross-layer")
    cols = ["a", "b", "n_a", "n_b", "both", "lift", "jaccard", "p_b_given_a", "p_a_given_b"]
    print(cross.head(30)[cols].to_string(index=False))
    print(f"\n═══ SAME-BAR, WITHIN a layer (still excluding nested) ═══")
    print(real[~real["cross_layer"]].head(15)[cols].to_string(index=False))
    print(f"\n═══ most NEGATIVE associations (lift << 1: marks that avoid each other) ═══")
    print(cross[cross["both"] >= 200].nsmallest(15, "lift")[cols].to_string(index=False))
    print(f"\n═══ strongest LEAD -> LAG within {a.lag} bars, by asymmetry ═══")
    if len(lead):
        nested_pairs = set(map(tuple, same.loc[same["nested"], ["a", "b"]].to_numpy())) | \
                       set(map(tuple, same.loc[same["nested"], ["b", "a"]].to_numpy()))
        lead["nested"] = [(f, t) in nested_pairs for f, t in zip(lead["first"], lead["then"])]
        lead["cross_layer"] = [f.split(":")[0] != t.split(":")[0]
                               for f, t in zip(lead["first"], lead["then"])]
        L = lead[(~lead["nested"]) & lead["cross_layer"] & (lead["hits"] >= 200)]
        lc = ["first", "then", "n_first", "hits", "p_then_given_first", "p_then_base",
              "lead_lift", "rev_lift", "asymmetry"]
        print(L.sort_values("asymmetry", ascending=False).head(30)[lc].to_string(index=False))
        print(f"\n═══ and the strongest ORDERINGS INTO a book edge (what arrives before an EDGE) ═══")
        into = L[L["then"].str.startswith("EDGE:") & ~L["first"].str.startswith("EDGE:")]
        print(into.sort_values("asymmetry", ascending=False).head(25)[lc].to_string(index=False))
        lead.to_csv(f"{OUT_DIR}/lead_lag_{a.lag}.csv", index=False)


if __name__ == "__main__":
    main()
