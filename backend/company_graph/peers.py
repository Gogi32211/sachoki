"""company_graph/peers.py — the ecosystem's price action, on one comparable scale.

The question this answers is "of everyone connected to this company, who is rising and who
is falling", and it only has a meaningful answer if every row is measured the same way.

PERCENT FROM THE WINDOW START, NOT PRICE
    A $220 stock and a $2 stock plotted as prices are not a comparison, they are two
    unrelated pictures sharing an axis. Every row here is normalised to percent change
    from the first bar's OPEN in the window, so the shapes are directly readable against
    each other and the ranking means something.

WHAT IS MISSING IS RETURNED, NOT DROPPED
    Most of a small company's competitors are private — DJI, T-Motor, Orqa, ModalAI. They
    have no ticker and no price, and quietly omitting them would turn "here is the
    competitive field" into "here is the part of the competitive field that happens to be
    listed", with nothing on screen to mark the difference. `unpriced` carries them out
    with the reason.

THE WEEKLY DATABASE LAGS AND SAYS SO
    studio_1w is written by the nightly job and its last bar is typically one to two weeks
    behind today. A "2 month" window that silently ends a fortnight ago would be read as
    current. `as_of` and `weeks_behind` travel with the data.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import duckdb

from studio.paths import db_path

log = logging.getLogger(__name__)

WEEKLY_DB = "studio_1w.duckdb"
DEFAULT_WEEKS = 9                       # ~2 months of weekly bars


def _conn():
    return duckdb.connect(db_path(WEEKLY_DB), read_only=True)


def weekly_bars(tickers: list[str], weeks: int = DEFAULT_WEEKS) -> dict:
    """Weekly OHLCV for several tickers over the last `weeks` bars, on a shared date axis.

    Rows are deduplicated per (ticker, date). A ticker that belongs to more than one
    universe has one row per universe in `bars`, and this project has already been bitten
    once by a per-universe duplicate silently doubling a series.
    """
    tickers = [t.upper() for t in tickers if t and not t.startswith(("CIK", "NAME:"))]
    if not tickers:
        return {"weeks": [], "series": {}, "as_of": None, "found": [], "not_in_db": []}

    ph = ",".join("?" * len(tickers))
    with _conn() as c:
        as_of = c.execute("SELECT MAX(date) FROM bars").fetchone()[0]
        if as_of is None:
            return {"weeks": [], "series": {}, "as_of": None, "found": [],
                    "not_in_db": tickers}
        # a little slack so a ticker that missed a week still lands inside the window
        since = as_of - timedelta(weeks=weeks + 1)
        df = c.execute(f"""
            SELECT ticker, date, open, high, low, close, volume FROM (
                SELECT ticker, date, open, high, low, close, volume,
                       ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) rn
                FROM bars WHERE ticker IN ({ph}) AND date > ?
            ) WHERE rn = 1
            ORDER BY ticker, date
        """, [*tickers, since]).fetchdf()

    if not len(df):
        return {"weeks": [], "series": {}, "as_of": str(as_of), "found": [],
                "not_in_db": tickers}

    axis = sorted({str(d) for d in df["date"]})[-weeks:]
    axis_set = set(axis)
    series: dict[str, list] = {}
    for tk, g in df.groupby("ticker"):
        bars = [{"date": str(r.date), "o": float(r.open), "h": float(r.high),
                 "l": float(r.low), "c": float(r.close), "v": float(r.volume or 0)}
                for r in g.itertuples() if str(r.date) in axis_set]
        if bars:
            series[tk] = bars

    return {"weeks": axis, "series": series, "as_of": str(as_of),
            "found": sorted(series), "not_in_db": sorted(set(tickers) - set(series))}


def peer_table(ticker: str, edges: list[dict], classify, counterparty,
               weeks: int = DEFAULT_WEEKS) -> dict:
    """One row per company in the ecosystem, ranked by how it moved over the window."""
    ticker = ticker.upper()

    # A company can be reached by several relationships (CoreWeave is both NVIDIA's
    # customer and its investee). The table is one row per COMPANY, so the relationships
    # are collected onto the row rather than producing duplicate rows that would each
    # carry the same price series and triple its apparent weight in the ranking.
    who: dict[str, dict] = {}
    for e in edges:
        if e.get("status") == "MODEL_PRIOR":
            continue
        other = counterparty(e, ticker)
        if not other or other == ticker:
            continue
        slot = who.setdefault(other, {"code": other, "rels": [], "side": set()})
        slot["rels"].append({"rel_type": e["rel_type"], "component": e.get("component", ""),
                             "confidence": e.get("confidence"), "doc_date": e.get("doc_date")})
        slot["side"].add(classify(e, ticker))

    wanted = [ticker] + [k for k in who if not k.startswith(("CIK", "NAME:"))]
    data = weekly_bars(wanted, weeks=weeks)
    axis = data["weeks"]

    def build(code: str, entry: Optional[dict]) -> dict:
        bars = data["series"].get(code) or []
        base = bars[0]["o"] if bars else None
        pct = [round((b["c"] / base - 1) * 100, 2) for b in bars] if base else []
        return {
            "ticker": code,
            "is_target": code == ticker,
            "relations": (entry or {}).get("rels", []),
            "side": sorted((entry or {}).get("side", []) or []),
            "bars": bars,
            "pct": pct,
            "change_pct": pct[-1] if pct else None,
            "last_close": bars[-1]["c"] if bars else None,
            "n_bars": len(bars),
        }

    rows = [build(ticker, None)] + [build(k, v) for k, v in who.items()
                                    if not k.startswith(("CIK", "NAME:"))]
    priced = [r for r in rows if r["change_pct"] is not None]
    priced.sort(key=lambda r: r["change_pct"], reverse=True)

    # everything the table cannot price, with the reason, so the field is not silently
    # narrowed to the listed part of it
    unpriced = []
    for k, v in who.items():
        if k.startswith("NAME:"):
            unpriced.append({"name": k[5:], "why": "private — not registered with the SEC",
                             "relations": v["rels"]})
        elif k.startswith("CIK"):
            unpriced.append({"name": k, "why": "SEC filer with no listed shares",
                             "relations": v["rels"]})
        elif k in data["not_in_db"]:
            unpriced.append({"name": k, "why": "not in the weekly bars database",
                             "relations": v["rels"]})

    weeks_behind = None
    if data["as_of"]:
        try:
            weeks_behind = max(0, (date.today() - date.fromisoformat(data["as_of"])).days // 7)
        except ValueError:
            weeks_behind = None

    return {"ok": True, "ticker": ticker, "weeks": axis, "rows": priced,
            "unpriced": unpriced, "as_of": data["as_of"], "weeks_behind": weeks_behind,
            "n_priced": len(priced), "n_unpriced": len(unpriced),
            "basis": "percent change from the first bar's open in the window — "
                     "prices are not comparable across companies, percentages are"}


__all__ = ["weekly_bars", "peer_table", "DEFAULT_WEEKS"]
