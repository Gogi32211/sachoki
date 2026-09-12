"""ROW-OUTCOME CONSERVATION (engineering only; reads counts, never return values).

For each horizon: X_v2 rows -> submitted to _pathsim (mask rows) -> engine drop reasons, taken verbatim from
the engine's own contract (edge_replay._pathsim):
    NO_NEXT_SESSION   i + 1 >= n        (the row is the ticker's last canonical bar)
    NO_ENTRY_OPEN     ep <= 0           (next bar's open not positive)
    COOLDOWN_SKIPPED  i - last < 5      (must be 0 by construction of the five mod-5 masks)
-> pathsim-defined rows. Sub-denominators inside the defined set (NOT drops):
    RIGHT_CENSORED    i + 1 + maxh > n and the exit is the last bar
    ATR_FALLBACK      atr_14 NaN at the signal bar -> engine used the fixed fallback trail (nan_to_num)
Assert: submitted = defined + NO_NEXT_SESSION + NO_ENTRY_OPEN + COOLDOWN_SKIPPED, exact.
"""
from __future__ import annotations
import os, sys, json, glob, hashlib, time
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop                                          # noqa: E402
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def main(run_dir: str):
    seal = json.load(open(os.path.join(FAMILY_DIR, "SEAL_V2_1.json")))
    x_p = os.path.join(FAMILY_DIR, "runs", seal["x_run"], "X_v2.parquet")
    cur = json.load(open(os.path.join(FAMILY_DIR, "canonical_1d", "CURRENT.json")))
    X = pd.read_parquet(x_p, columns=["ticker", "session"]); X["session"] = X["session"].astype(str)
    canon = pd.read_parquet(cur["derived_parquet"], columns=["ticker", "session_date", "open", "atr_14"])
    canon = canon.rename(columns={"session_date": "date"}); canon["date"] = canon["date"].astype(str)
    canon = canon.sort_values(["ticker", "date"]).reset_index(drop=True)
    canon["pos"] = canon.groupby("ticker").cumcount()
    canon["n"] = canon.groupby("ticker")["date"].transform("size")
    canon["next_open"] = canon.groupby("ticker")["open"].shift(-1)
    canon["last_date"] = canon.groupby("ticker")["date"].transform("last")
    m = X.merge(canon, left_on=["ticker", "session"], right_on=["ticker", "date"], how="left", indicator=True)
    not_in_calendar = int((m["_merge"] != "both").sum())
    if not_in_calendar:
        raise HardStop(f"{not_in_calendar} X rows are not on the canonical calendar")
    m["no_next_session"] = m["pos"] + 1 >= m["n"]
    m["no_entry_open"] = ~m["no_next_session"] & ~(m["next_open"] > 0)          # ep <= 0 (NaN counts as not > 0)
    m["atr_fallback"] = m["atr_14"].isna()
    # cooldown: within one mod-5 mask, consecutive submitted rows are >= 5 bars apart by construction
    m["mask"] = m["pos"] % 5
    gaps = m.sort_values(["ticker", "mask", "pos"]).groupby(["ticker", "mask"])["pos"].diff().dropna()
    cooldown_violations = int((gaps < 5).sum())
    rep = dict(run=os.path.basename(run_dir), x_run=seal["x_run"], x_rows=int(len(X)), submitted_to_pathsim=int(len(m)),
               engine_contract=dict(NO_NEXT_SESSION="i + 1 >= n", NO_ENTRY_OPEN="ep <= 0", COOLDOWN_SKIPPED="i - last < 5",
                                    RIGHT_CENSORED="i + 1 + maxh > n and exit at the last bar (inside the DEFINED set)",
                                    ATR_FALLBACK="atr_14 NaN at the signal bar -> fixed fallback trail (inside the DEFINED set)"),
               drops=dict(NO_NEXT_SESSION=int(m["no_next_session"].sum()), NO_ENTRY_OPEN=int(m["no_entry_open"].sum()),
                          COOLDOWN_SKIPPED_by_construction=cooldown_violations),
               no_next_session_tickers=int(m.loc[m["no_next_session"], "ticker"].nunique()),
               tickers_in_x=int(X["ticker"].nunique()),
               tickers_whose_last_x_row_is_not_their_last_canonical_bar=int(X["ticker"].nunique() - m.loc[m["no_next_session"], "ticker"].nunique()),
               atr_fallback_rows_submitted=int(m["atr_fallback"].sum()), horizons={})
    expected_defined = rep["submitted_to_pathsim"] - rep["drops"]["NO_NEXT_SESSION"] - rep["drops"]["NO_ENTRY_OPEN"] - cooldown_violations
    for p in sorted(glob.glob(os.path.join(run_dir, "row_outcomes_h*.parquet"))):
        h = int(os.path.basename(p)[len("row_outcomes_h"):-len(".parquet")])
        o = pd.read_parquet(p, columns=["ticker", "session", "ret", "censored"])
        defined = int(o["ret"].notna().sum()); rows = int(len(o))
        dup = int(o.duplicated(["ticker", "session"]).sum())
        extra = int((~o.set_index(["ticker", "session"]).index.isin(m.set_index(["ticker", "session"]).index)).sum())
        j = m.merge(o[["ticker", "session", "censored"]], on=["ticker", "session"], how="left", indicator="got")
        missing_defined_reasoned = int(((j["got"] == "left_only") & ~(j["no_next_session"] | j["no_entry_open"])).sum())
        censored = int(o["censored"].sum())
        atr_fb_defined = int(j.loc[j["got"] == "both", "atr_fallback"].sum())
        ok = (rows == defined == expected_defined) and dup == 0 and extra == 0 and missing_defined_reasoned == 0
        rep["horizons"][h] = dict(output_rows=rows, pathsim_defined=defined, expected_defined=expected_defined, duplicates=dup,
                                  rows_not_submitted=extra, unexplained_missing=missing_defined_reasoned,
                                  RIGHT_CENSORED_inside_defined=censored, ATR_FALLBACK_inside_defined=atr_fb_defined,
                                  identity="submitted = defined + NO_NEXT_SESSION + NO_ENTRY_OPEN + COOLDOWN_SKIPPED",
                                  conservation_exact=bool(ok), file_sha256_16=_dig(p))
        if not ok:
            raise HardStop(f"row conservation FAILED at h={h}: {rep['horizons'][h]}")
    rep["difference_x_rows_minus_defined"] = rep["x_rows"] - expected_defined
    rep["explanation"] = (f"{rep['drops']['NO_NEXT_SESSION']} rows are their ticker's last canonical bar (no next session to enter) "
                          f"across {rep['no_next_session_tickers']} tickers; {rep['tickers_in_x']} tickers are in X, so "
                          f"{rep['tickers_whose_last_x_row_is_not_their_last_canonical_bar']} tickers' last ELIGIBLE row precedes their last canonical bar "
                          f"(eligibility lapsed before the bar history ended) and therefore loses no row; NO_ENTRY_OPEN = {rep['drops']['NO_ENTRY_OPEN']}; "
                          f"cooldown skips = {cooldown_violations}. Right-censored rows are INSIDE the defined set and are a separate denominator.")
    rep["written_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    out = os.path.join(FAMILY_DIR, f"ROW_CONSERVATION_{os.path.basename(run_dir)}.json")
    json.dump(rep, open(out, "w"), indent=1)
    print(json.dumps({k: v for k, v in rep.items() if k != "engine_contract"}, indent=1))
    print("written", out, _dig(out))


if __name__ == "__main__":
    runs = sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")))
    main(sys.argv[1] if len(sys.argv) > 1 else runs[-1])
