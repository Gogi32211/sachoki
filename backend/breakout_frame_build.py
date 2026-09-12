"""Broad-universe research frame for the breakout-systems programme. 2026-09-12.

WHY THIS EXISTS. The 28 `*_1d_signals.csv` files in ~/Downloads are chart exports taken at
different times: 11 different schemas, 326-476 columns, 12,430 stock-days, and `PHYS_*` — the
backbone of every state machine — present in only 7 of them. They are inspection cases, not a
dataset. This builds the equivalent frame for the whole SP500+Nasdaq universe from the same
sources the export path reads, in ONE schema, with volume and an explicit ticker.

SOURCES, and why each is the right one
  studio_analytics.duckdb `bars`   every per-bar engine output, de-duplicated across universe tags
                                   by sp500 > nasdaq > russell2k so a ticker in two indices is one row
  data/lbal_signals.parquet        UDN / L-BAL
  data/lvx_signals.parquet         L-VX grade
  data/vol7_signals.parquet        VOL7 regime
  data/shapectx_signals.parquet    SHAPE + the LST↑ veto
  data/anatomy_signals.parquet     ▽△ (carried for reference, not used by the state machines)

TWO FIELDS ARE NOT STORED and are handled explicitly rather than silently:
  rtb_transition  reconstructed causally from the rtb_phase sequence plus the hard-reset flags
                  the engine itself uses (_HARD_RESET_KEYS = vbo_dn, bo_dn, bx_dn, fbo_bear,
                  be_dn). RESET_SOFT is NOT reconstructed — its inputs are not stored — so a soft
                  reset appears here as an ordinary HOLD/TO transition. Only A_TO_B and phase
                  membership are used downstream, and both are exact.
  mtf_echo        needs a 4H/1H intraday REV-day join and is gated on `rev_buy`. NOT built in this
                  pass. The MTF_ZERO veto is therefore reported as NOT EVALUATED, not as a null.

FORWARD COLUMNS ARE NEVER READ. The label is derived from raw OHLC in a separate pass
(para_target_v1), so this frame contains no outcome of any kind.
"""
from __future__ import annotations

import os
import sys
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DATA = os.path.join(os.path.dirname(HERE), "data")
OUT = os.path.join(DATA, "breakout_frame_v1.parquet")
UNIVERSES = ("sp500", "nasdaq")
DATE_LO, DATE_HI = "2021-05-26", "2026-09-10"

# Per-bar engine columns taken from `bars`. Every one is SAFE: verified by
# backend/tests/test_feature_lookahead.py (133 engine columns x 20 dates x 6 tickers, 0 diffs).
BAR_COLS = [
    # identity + raw
    "ticker", "universe", "open", "high", "low", "close", "volume", "avg_vol_20d",
    "vol_bucket", "atr_14", "rsi_14", "cci_20", "sector",
    # physics
    "phys_k", "phys_k_x", "phys_regime", "phys_m", "phys_s", "phys_r", "phys_e",
    "phys_h", "phys_e_release", "phys_c", "phys_gap_true", "phys_line6",
    # RTB (phase only — rtb_total and the numeric sub-scores are excluded by instruction)
    "rtb_phase",
    # turn ingredients
    "hilo_buy", "rtv", "bo_up", "bx_up", "be_up", "l34", "l43", "l22",
    # participation
    "t_sig", "z_sig", "l_sig", "sig_t2g", "vbo_up", "gog_tier",
    "sig_f1", "sig_f2", "sig_f3", "sig_f4", "sig_f5", "sig_f6", "sig_f7", "sig_f8",
    "sig_f9", "sig_f10", "sig_f11", "sig_any_f",
    "sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad",
    "setup_tokens", "context_tokens",
    # hard-reset inputs for the rtb_transition reconstruction
    "sig_vbo_dn", "bo_dn", "bx_dn", "fbo_bear", "be_dn",
    # context carried for reporting, not for the state machines
    "wyc_phase", "sig_conso", "sq", "load", "svs", "sig_blue",
    "price_gt_20", "price_gt_50", "price_gt_200",
]

_HARD_RESET_COLS = ["sig_vbo_dn", "bo_dn", "bx_dn", "fbo_bear", "be_dn"]


def _bars(log=print) -> pd.DataFrame:
    import duckdb
    from studio.db import tf_db_path
    cols = ", ".join(BAR_COLS)
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    try:
        df = con.execute(f"""
            SELECT substr(CAST(date AS VARCHAR),1,10) AS date, {cols}
            FROM bars
            WHERE universe IN {UNIVERSES} AND date >= ? AND date <= ?
            QUALIFY row_number() OVER (PARTITION BY ticker, date ORDER BY
              CASE universe WHEN 'sp500' THEN 1 ELSE 2 END) = 1
            ORDER BY ticker, date""", [DATE_LO, DATE_HI]).fetchdf()
    finally:
        con.close()
    log(f"  bars {len(df):,} rows · {df['ticker'].nunique():,} tickers "
        f"· {df['date'].min()}..{df['date'].max()}")
    return df


def _rtb_transition(df: pd.DataFrame) -> pd.Series:
    """Reconstruct rtb_engine._transition from the stored phase sequence.

        hard        -> RESET_HARD        (any of vbo_dn/bo_dn/bx_dn/fbo_bear/be_dn — the engine's
                                          own _HARD_RESET_KEYS)
        cur == '0'  -> '0'
        prev=='0' and cur=='A' -> A_START
        prev == cur -> f'{cur}_HOLD'
        else        -> f'{prev}_TO_{cur}'

    RESET_SOFT is not reproducible from stored fields and is therefore absent; such a bar appears
    as its ordinary HOLD/TO transition. Downstream only A_TO_B and phase membership are used.
    """
    ph = df["rtb_phase"].fillna("0").astype(str).replace({"": "0", "nan": "0"})
    prev = ph.groupby(df["ticker"], sort=False).shift(1).fillna("0")
    hard = np.zeros(len(df), bool)
    for c in _HARD_RESET_COLS:
        if c in df.columns:
            hard |= df[c].fillna(0).astype(float).to_numpy() > 0
    p, c = prev.to_numpy(), ph.to_numpy()
    out = np.where(c == "0", "0",
          np.where((p == "0") & (c == "A"), "A_START",
          np.where(p == c, np.char.add(c.astype(str), "_HOLD"),
                   np.char.add(np.char.add(p.astype(str), "_TO_"), c.astype(str)))))
    out = np.where(hard, "RESET_HARD", out)
    return pd.Series(out, index=df.index, dtype=object)


def _join_layers(df: pd.DataFrame, log=print) -> pd.DataFrame:
    specs = [
        ("lbal_signals", {"udn": "LBAL_UDN", "udn_c": "LBAL_UDN_C", "n_pos": "LBAL_NPOS",
                          "n_neg": "LBAL_NNEG", "star": "LBAL_STAR", "colour": "LBAL_COLOUR"}),
        ("lvx_signals", {"tier": "LVX_TIER", "label": "LVX_LABEL", "fam": "LVX_FAM"}),
        ("vol7_signals", {"mr": "VOL7_MR", "sg": "VOL7_SG", "ratio": "VOL7_RATIO",
                          "cons": "VOL7_CONS"}),
        ("shapectx_signals", {"lstup_veto": "SHAPE_LSTUP_VETO", "knife": "SHAPE_KNIFE",
                              "code": "SHAPE_CODE", "touches": "SHAPE_TOUCHES",
                              "key": "SHAPE_KEY", "rs": "SHAPE_RS", "floor": "SHAPE_FLOOR"}),
        ("anatomy_signals", {"v": "ANAT_V", "s": "ANAT_S", "rs": "ANAT_RS"}),
    ]
    for fname, ren in specs:
        p = os.path.join(DATA, f"{fname}.parquet")
        if not os.path.exists(p):
            log(f"  ⚠ missing {fname} — its columns will be absent")
            continue
        t = pd.read_parquet(p, columns=["ticker", "date"] + list(ren))
        t["date"] = t["date"].astype(str).str[:10]
        # lvx carries one row per (ticker, date, fam); keep the strongest tier per session
        if fname == "lvx_signals":
            t = (t.sort_values("tier", ascending=False)
                   .drop_duplicates(subset=["ticker", "date"], keep="first"))
        t = t.drop_duplicates(subset=["ticker", "date"]).rename(columns=ren)
        before = len(df)
        df = df.merge(t, on=["ticker", "date"], how="left")
        assert len(df) == before, f"{fname} join changed the row count"
        cov = df[list(ren.values())[0]].notna().mean() * 100
        log(f"  + {fname:22s} coverage {cov:5.1f}%")
    return df


def audit(df: pd.DataFrame, log=print) -> dict:
    """The six checks §1 of the approval requires, run BEFORE any research."""
    rep = {}
    rep["rows"] = int(len(df))
    rep["tickers"] = int(df["ticker"].nunique())
    rep["date_range"] = [str(df["date"].min()), str(df["date"].max())]

    dup = int(df.duplicated(subset=["ticker", "date"]).sum())
    rep["duplicate_ticker_date"] = dup
    log(f"  1. one schema/version .............. single frame, {df.shape[1]} columns")
    log(f"  2. duplicate (ticker,date) rows .... {dup}  {'OK' if dup == 0 else '*** FAIL ***'}")

    rep["has_ticker"] = "ticker" in df.columns
    rep["has_universe"] = "universe" in df.columns
    rep["universe_counts"] = df["universe"].value_counts().to_dict()
    log(f"  3. ticker & universe explicit ...... {rep['has_ticker']} / {rep['has_universe']}  "
        f"{rep['universe_counts']}")

    vol_ok = df["volume"].notna().mean()
    rep["volume_coverage"] = round(float(vol_ok) * 100, 3)
    log(f"  4. raw volume present .............. {rep['volume_coverage']:.2f}% non-null")

    need = ["phys_k", "phys_regime", "phys_m", "phys_s", "phys_r", "phys_e", "phys_e_release",
            "rtb_phase", "RTB_TRANSITION", "hilo_buy", "rtv", "vbo_up", "gog_tier", "t_sig",
            "sig_f1", "sig_f3", "sig_f6", "sig_f7", "sig_fly_abcd", "rsi_14", "atr_14",
            "LBAL_UDN", "LBAL_NPOS", "LBAL_NNEG", "LVX_TIER", "VOL7_MR", "SHAPE_LSTUP_VETO"]
    missing = [c for c in need if c not in df.columns]
    rep["missing_required"] = missing
    rep["not_evaluated"] = ["mtf_echo / MTF_ZERO veto — needs a 4H/1H intraday REV join"]
    log(f"  5. required SAFE fields ............ "
        f"{'all present' if not missing else '*** MISSING ' + str(missing) + ' ***'}")
    log(f"     not evaluated this pass ......... mtf_echo (MTF_ZERO veto)")

    cov = {c: round(float(df[c].notna().mean()) * 100, 2)
           for c in ("phys_k", "LBAL_UDN", "LVX_TIER", "VOL7_MR", "SHAPE_LSTUP_VETO")}
    rep["layer_coverage_pct"] = cov
    log(f"  6. layer coverage .................. {cov}")
    return rep


def build(log=print) -> pd.DataFrame:
    t0 = time.time()
    log("BREAKOUT RESEARCH FRAME v1")
    df = _bars(log)
    df["RTB_TRANSITION"] = _rtb_transition(df)
    log(f"  rtb_transition reconstructed · "
        f"{df['RTB_TRANSITION'].value_counts().head(6).to_dict()}")
    df = _join_layers(df, log)
    df = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    log("\nDATA AUDIT (§1)")
    rep = audit(df, log)
    tmp = OUT + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, OUT)
    log(f"\n  wrote {OUT} · {len(df):,} rows × {df.shape[1]} cols ({time.time()-t0:.0f}s)")
    import json
    json.dump(rep, open(OUT.replace(".parquet", "_audit.json"), "w"), indent=1, default=str)
    return df


if __name__ == "__main__":
    build()
