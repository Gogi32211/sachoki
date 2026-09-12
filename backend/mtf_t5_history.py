"""Per-ticker T5 fire history with its 1H/15m confirmation group and realised outcome.

usage:  mtf_t5_history.py PVH [TICKER ...]

Prints every 1D T5 fire for the ticker over the study window: the date, the close, how
many lower timeframes echoed the SAME T5 that session, and what the trade actually did
under the study's exit rule (trail = clip(12*ATR%,15,60), maxh 60, slip 15bps).

Fires outside the $21-89 price bucket are listed too, marked `--` in the confirm column,
because the study never scored them -- they are shown so the chart history is complete,
not as evidence.
"""
from __future__ import annotations
import os, sys                                                          # noqa: E402

sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
STUDY = "/Users/sachoki/MASSIVE_DATA/MTF_1D_X_LTF_STUDY"
DATA = "/Users/sachoki/Desktop/sachoki-desktop/data"


def main(tickers):
    import pandas as pd, numpy as np, duckdb
    from edge_replay import _pathsim

    m = pd.read_parquet(os.path.join(STUDY, "_mtf_masks.parquet"))
    m["date"] = pd.to_datetime(m.date)
    as_of = m.date.max().date()

    for tk in tickers:
        tk = tk.upper()
        g = m[m.ticker == tk]
        if g.empty:
            print(f"\n{tk}: not in the study universe (needs close>=5, "
                  f"close*volume>=3M, universe<>'index')")
            continue

        # every 1D T5 bar, including ones outside the price bucket
        a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
        raw = a.execute(f"""SELECT DISTINCT CAST(date AS DATE) d, close
                            FROM bars WHERE ticker = '{tk}' AND t_sig = 'T5'
                            ORDER BY d""").df()
        a.close()
        raw["d"] = pd.to_datetime(raw["d"]).dt.date

        gg = g.reset_index(drop=True)
        grp = {tk: gg}
        # _pathsim records date_in = the bar AFTER the signal (entry is the next open).
        # The chart-visible signal bar is therefore the date one position earlier.
        dts = list(pd.to_datetime(gg.date).dt.date)
        pos = {d: i for i, d in enumerate(dts)}
        conf = {}
        for lab, col in (("0", "T5_c0"), ("1", "T5_c1"), ("2", "T5_c2")):
            for d in np.asarray(dts)[gg[col].to_numpy()]:
                conf[d] = lab
        out = {}
        for lab, col in (("0", "T5_c0"), ("1", "T5_c1"), ("2", "T5_c2")):
            tr = _pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
            for _, r in tr.iterrows():
                din = pd.Timestamp(r["date_in"]).date()
                sig = dts[pos[din] - 1] if pos.get(din, 0) > 0 else din
                out[sig] = (lab, r["ret"] * 100, r["hold"],
                            din, pd.Timestamp(r["date_out"]).date())

        print(f"\n{tk} — 1D T5 fires, {raw.d.min()} .. {as_of}   "
              f"({len(raw)} total, {len(out)} inside $21-89 and scored)")
        print("  signal bar    close   confirm    result    held   entry -> exit")
        for _, r in raw.iterrows():
            d = r.d
            c = conf.get(d, "--")
            if d in out:
                lab, ret, hold, din, dout = out[d]
                print(f"  {d}   ${r.close:7.2f}      {c}     {ret:+7.2f}%   {int(hold):>3}b   "
                      f"{din} -> {dout}")
            else:
                why = ("out of $21-89, never scored" if not (21 <= r.close <= 89)
                       else "OPEN - no forward data yet")
                print(f"  {d}   ${r.close:7.2f}      {c}     {why}")
        n2 = sum(1 for v in out.values() if v[0] == "2")
        print(f"  -> {n2} of {len(out)} scored fires had BOTH 1H and 15m T5 echo")


if __name__ == "__main__":
    main(sys.argv[1:] or ["PVH"])
