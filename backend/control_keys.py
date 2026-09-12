"""CONTROL_KEYS — the shared day-clustered CONTROL for every research family, built once and versioned.

WHY THIS EXISTS (2026-09-09, user: "gavasworot kontrolis simkvrive").
The book control is "every 40th bar with close >= $21, per ticker" (`edge_replay.edge_replay`, the
`EDGE_CTRL` mask). Its selection is `np.where(close >= 21)[0][::40]` — the grid starts at index 0 of each
ticker's own eligible sequence. Because most tickers enter the frame on the SAME session, their 40-bar
grids ALIGN: control trades bunch onto a few sessions and leave the rest nearly empty. Measured on
RTV_V1's exposed run, that made the per-day control:

    trades/day median  2021  9 · 2022  9 · 2023 11 · 2024 15 · 2025 20 · 2026 25
    days reaching the >= 20 needed for a day-median:  16 % of MINE days vs 64 % of VERIFY days

So the day-clustered series was thin exactly where it decided (117 MINE days, some YEARS resting on 17),
and the two OOS windows sampled days on completely different densities — which makes a MINE-vs-VERIFY
comparison unsafe. This was flagged twice before (BQB, then RTV) and is fixed here.

THE FIX — de-phase, do not densify. Same rule, same 1-in-40 sampling RATE, same estimand; the only change
is that each ticker's grid starts at a DETERMINISTIC per-ticker offset instead of at 0:

    offset(ticker) = sha256(ticker)[:8] % stride        # stable across processes, unlike hash()
    control bar    = eligible bars where pos % stride == offset

Key count is essentially unchanged (54.1k vs 56.3k) but the density becomes flat: median 39-49 trades/day
and >= 20 on 100 % of sessions in EVERY year. Nothing about what a control trade IS has changed — this
removes a sampling artifact, it does not redefine the benchmark.

SCOPE. Prior sealed families used the phase-0 keys; their results are NOT invalidated (a thin control makes
passing HARDER, and those families were NULL anyway) — but their MINE day-series were thin, which is
recorded in each family's notes. Families opened from 2026-09-09 use this artifact.

Usage:
    python control_keys.py build      # writes a versioned parquet + CURRENT.json
    from control_keys import current, load
    man = current(); keys = load()    # DataFrame(ticker, session)
"""
from __future__ import annotations
import os, sys, json, time, hashlib
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DIR = "/Users/sachoki/MASSIVE_DATA/CONTROL_KEYS"
STRIDE = 40                 # the book's sampling rate — unchanged
PX_MIN = 21.0               # the book's control rule: close >= $21
DV_FLOOR = 3_000_000        # the liquid universe every family draws its signals from
MIN_CONTROL = 20            # a day needs this many control trades to yield a day-median
MIN_DAY_COVERAGE = 95.0     # every year must reach MIN_CONTROL on at least this share of sessions
WINDOW = ("2021-09-07", "2026-09-03")


def _dig(p: str, n: int = 16) -> str:
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def phase(ticker: str, stride: int = STRIDE) -> int:
    """Deterministic per-ticker grid offset. NOT python hash() — that is salted per process."""
    return int(hashlib.sha256(str(ticker).encode()).hexdigest()[:8], 16) % stride


def build(stride: int = STRIDE, log=print) -> dict:
    from ovd_build import HardStop, canonical_current
    cur = canonical_current()
    os.makedirs(DIR, exist_ok=True)
    run_id = time.strftime("CTL_%Y%m%dT%H%M%SZ", time.gmtime())
    df = pd.read_parquet(cur["derived_parquet"], columns=["ticker", "session_date", "close", "dollar_vol"])
    df["session"] = df["session_date"].astype(str).str[:10]
    df = df[(df["close"] >= PX_MIN) & (df["dollar_vol"] >= DV_FLOOR)
            & (df["session"] >= WINDOW[0]) & (df["session"] <= WINDOW[1])]
    df = df.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    df["pos"] = df.groupby("ticker").cumcount()
    off = df["ticker"].map(lambda t: phase(t, stride)).to_numpy()
    keys = df.loc[(df["pos"].to_numpy() % stride) == off, ["ticker", "session"]].reset_index(drop=True)
    # coverage: how many CONTROL BARS land on each session (the trade lands the next session, which
    # shifts the calendar by one bar and cannot change the density)
    n = keys.groupby("session").size()
    yr = pd.Index(n.index).str[:4]
    cov = {}
    for y in sorted(set(yr)):
        s = n[yr == y]
        cov[str(y)] = dict(sessions=int(len(s)), median=float(s.median()), p10=float(s.quantile(0.10)),
                           share_ge_min=round(float((s >= MIN_CONTROL).mean() * 100), 1))
    worst = min(v["share_ge_min"] for v in cov.values())
    p = os.path.join(DIR, f"control_{run_id}.parquet")
    keys.to_parquet(p, index=False)
    man = dict(run_id=run_id, version="CONTROL_V2_DEPHASED", built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               rule=dict(stride=stride, px_min=PX_MIN, dv_floor=DV_FLOOR, window=list(WINDOW),
                         offset="sha256(ticker)[:8] % stride", source="canonical 1D derived parquet",
                         note="identical to the book's every-Nth-bar control except the per-ticker grid offset"),
               supersedes=dict(what="phase-0 keys (edge_replay EDGE_CTRL / GATE_QUIET_GEM1_V1/engine_control_keys.parquet)",
                               reason="per-ticker grid alignment clustered control trades onto few sessions",
                               effect="same sampling rate, flat density; prior families' NULL verdicts stand"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               keys=int(len(keys)), tickers=int(keys["ticker"].nunique()), sessions=int(keys["session"].nunique()),
               min_control=MIN_CONTROL, coverage_by_year=cov, worst_year_share_ge_min=worst,
               parquet=p, sha256_16=_dig(p))
    if worst < MIN_DAY_COVERAGE:
        raise HardStop(f"control still thin: worst year reaches >= {MIN_CONTROL} on only {worst}% of sessions")
    tmp = os.path.join(DIR, ".CURRENT.tmp"); json.dump(man, open(tmp, "w"), indent=1, default=str)
    os.replace(tmp, os.path.join(DIR, "CURRENT.json"))
    log(f"{run_id}: {len(keys):,} control keys · {man['tickers']:,} tickers · {man['sessions']:,} sessions")
    for y, v in cov.items():
        log(f"  {y}: sessions {v['sessions']:>3} · median {v['median']:>5.1f}/day · p10 {v['p10']:>5.1f} · >= {MIN_CONTROL} on {v['share_ge_min']:.1f}% of days")
    return man


def current() -> dict:
    """The CURRENT control manifest. HARD STOP if absent or if its parquet was tampered with."""
    from ovd_build import HardStop
    p = os.path.join(DIR, "CURRENT.json")
    if not os.path.exists(p):
        raise HardStop("no CONTROL_KEYS authority — run `python control_keys.py build`")
    man = json.load(open(p))
    if not os.path.exists(man["parquet"]) or _dig(man["parquet"]) != man["sha256_16"]:
        raise HardStop("control parquet missing or digest mismatch")
    return man


def load(man: dict | None = None) -> pd.DataFrame:
    man = man or current()
    return pd.read_parquet(man["parquet"])


def assert_matches_canonical(man: dict, canonical: dict) -> None:
    """A family must run its control on the SAME price authority its trades run on."""
    from ovd_build import HardStop
    if man["canonical_1d"]["derived_sha256_16"] != canonical["derived_sha256_16"]:
        raise HardStop("control keys were built on a different canonical 1D authority — rebuild the control")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "build":
        build()
    elif cmd == "show":
        print(json.dumps(current(), indent=1))
