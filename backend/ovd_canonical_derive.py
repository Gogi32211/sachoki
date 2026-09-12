"""OPENING_VOLUME_DYNAMICS_V1 — A7: recompute EVERY price-derived field from the canonical 1D
store. Nothing is reused from the contaminated Studio store.

Per ticker, on canonical OHLCV (one adjustment basis):
  atr_14        TR.ewm(alpha=1/14, adjust=False).mean()      -- exact enricher formula (studio/enricher.py:117)
  avg_vol_20d   volume.rolling(20, min_periods=1).mean()     -- exact enricher formula (studio/enricher.py:321)
  wt_*          wyckoff_trig_engine.compute_wyckoff_trig(df) -- the same engine the enricher calls (studio/enricher.py:712)
  gap           open / prev_close - 1  (signed)
  dollar_vol    close * volume  (derived, never independent)
Writes canonical_1d/derived_<run>.parquet + DERIVED_<run>.json (digests, formulas, engine digest).
"""
from __future__ import annotations
import os, sys, json, time, hashlib, tempfile                            # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
CANON_DIR = os.path.join(FAMILY_DIR, "canonical_1d")


class HardStop(RuntimeError):
    pass


def derive_one(g):
    import numpy as np, pandas as pd
    from wyckoff_trig_engine import compute_wyckoff_trig
    g = g.sort_values("session_date").reset_index(drop=True)
    h, l, c = g["high"], g["low"], g["close"]
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    g["atr_14"] = tr.ewm(alpha=1.0 / 14, adjust=False).mean()
    g["avg_vol_20d"] = g["volume"].rolling(20, min_periods=1).mean()
    g["gap"] = g["open"] / pc - 1
    g["dollar_vol"] = g["close"] * g["volume"]
    try:
        w = compute_wyckoff_trig(g[["open", "high", "low", "close", "volume"]].copy())
        for col in ("wt_valid_tr", "wt_quality", "wt_support", "wt_resistance"):
            g[col] = w[col].to_numpy() if col in w else np.nan
    except Exception as e:
        g["wt_error"] = str(e)[:80]
        for col in ("wt_valid_tr", "wt_quality", "wt_support", "wt_resistance"):
            g[col] = np.nan
    return g


def main():
    import pandas as pd, inspect
    import wyckoff_trig_engine as W
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    X = pd.read_parquet(cur["canonical_parquet"])
    t0 = time.time()
    parts = []
    for i, (tk, g) in enumerate(X.groupby("ticker", sort=True)):
        parts.append(derive_one(g))
        if i and i % 500 == 0:
            print(f"  {i:,} tickers ({time.time()-t0:.0f}s)", flush=True)
    D = pd.concat(parts, ignore_index=True)
    if D.duplicated(["ticker", "session_date"]).any():
        raise HardStop("duplicates after derive")
    out = os.path.join(CANON_DIR, f"derived_{cur['run_id']}.parquet")
    fd, tmp = tempfile.mkstemp(dir=CANON_DIR, prefix=".tmp_"); os.close(fd)
    D.to_parquet(tmp, index=False); os.replace(tmp, out)
    dig = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]
    man = dict(run_id=cur["run_id"], canonical_sha256_16=cur["canonical_sha256_16"], derived_parquet=out, derived_sha256_16=dig(out),
               rows=int(len(D)), tickers=int(D.ticker.nunique()),
               wt_engine=dict(path=W.__file__, file_sha256_16=dig(W.__file__),
                              compute_sha256_16=hashlib.sha256(inspect.getsource(W.compute_wyckoff_trig).encode()).hexdigest()[:16],
                              SW_WIN=W._SW_WIN, LOOKBACK=W._LOOKBACK),
               formulas=dict(atr_14="TR.ewm(alpha=1/14, adjust=False).mean()  [studio/enricher.py:117]",
                             avg_vol_20d="volume.rolling(20, min_periods=1).mean()  [studio/enricher.py:321]",
                             gap="open/prev_close - 1", dollar_vol="close*volume"),
               wt_errors=int(D["wt_error"].notna().sum()) if "wt_error" in D else 0,
               wt_resistance_nonnull_share=round(float(D["wt_resistance"].notna().mean()), 4),
               elapsed_s=round(time.time() - t0))
    json.dump(man, open(os.path.join(CANON_DIR, f"DERIVED_{cur['run_id']}.json"), "w"), indent=1)
    cur["derived_parquet"] = out; cur["derived_sha256_16"] = man["derived_sha256_16"]
    json.dump(cur, open(os.path.join(CANON_DIR, "CURRENT.json"), "w"), indent=1)
    print(json.dumps({k: man[k] for k in ("run_id", "rows", "tickers", "derived_sha256_16", "wt_errors", "wt_resistance_nonnull_share", "elapsed_s")}, indent=1))


if __name__ == "__main__":
    main()
