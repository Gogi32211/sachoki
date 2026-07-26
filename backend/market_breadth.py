"""
market_breadth.py — causal market-breadth regime, the gate for the atomic capitulation-
reversion edge (validated 2026-07-01: the weak-close gap-up edge works in FEAR / low
breadth and dies in euphoria — see project_atomic_edge_validated / validate_breadth.py).

breadth(date) = fraction of the liquid universe whose close > close `window` bars ago.
Uses only past→present data (causal). risk_off = breadth < threshold (default 0.50; the
edge strengthens monotonically as breadth falls — deep fear <0.40 is 6/6yr positive).

  from market_breadth import breadth_series, breadth_for_date, current_regime
"""
from __future__ import annotations
import os, json
import duckdb, pandas as pd
from studio.paths import ANALYTICS_DB

WINDOW = 20
RISK_OFF_CUT = 0.50          # breadth < this = risk-off (fear) = trade the atomic edge
DEEP_FEAR_CUT = 0.40         # breadth < this = deep fear (strongest, 6/6yr)
DV_FLOOR = 3_000_000
_CACHE = os.path.join(os.path.dirname(__file__), "market_breadth.json")


def breadth_series(window: int = WINDOW, dv_floor: float = DV_FLOOR) -> pd.Series:
    """Date-indexed (str 'YYYY-MM-DD') breadth series over the liquid universe."""
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = con.execute(f"""
            WITH r AS (SELECT ticker, date, close,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>={dv_floor})
            SELECT ticker, date, close FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        con.close()
    df = df.sort_values(["ticker", "date"])
    df["cw"] = df.groupby("ticker")["close"].shift(window)
    df["up"] = (df["close"] > df["cw"]).astype(float)
    br = df.dropna(subset=["cw"]).groupby("date")["up"].mean()
    br.index = br.index.astype(str).str[:10]
    return br


def refresh_cache(window: int = WINDOW) -> dict:
    br = breadth_series(window)
    data = {"window": window, "as_of": br.index[-1] if len(br) else None,
            "series": {d: round(float(v), 4) for d, v in br.items()}}
    with open(_CACHE, "w") as f:
        json.dump(data, f)
    return data


def _load() -> dict:
    if not os.path.exists(_CACHE):
        return refresh_cache()
    with open(_CACHE) as f:
        return json.load(f)


def breadth_for_date(date: str, data: dict | None = None) -> float | None:
    d = (data or _load())["series"]
    return d.get(str(date)[:10])


def regime_label(breadth: float | None) -> str:
    if breadth is None:
        return "UNKNOWN"
    if breadth < DEEP_FEAR_CUT:
        return "DEEP_FEAR"      # strongest edge
    if breadth < RISK_OFF_CUT:
        return "RISK_OFF"       # tradeable
    return "RISK_ON"            # atomic edge dead — stand down


def current_regime(data: dict | None = None) -> dict:
    data = data or _load()
    ao = data.get("as_of")
    br = data["series"].get(ao) if ao else None
    return {"as_of": ao, "breadth": br, "regime": regime_label(br),
            "risk_off": br is not None and br < RISK_OFF_CUT}


if __name__ == "__main__":
    d = refresh_cache()
    print(f"cached {len(d['series'])} days, as_of {d['as_of']}")
    print("current:", current_regime(d))
