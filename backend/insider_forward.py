"""INSIDER_BUY forward confirmation (research_out/INSIDER_BUY_V1.md, 2026-10-01, user OK "samive gaakete").

Seen history (SEC Insider Transactions Data Sets 2021Q1-2026Q1): officer/director open-market purchases
WORKED in 2021-23 and FAILED in 2024-26Q1 (clusters +3.60 → −1.63, CEO/CFO +2.02 → −2.36 pp vs the same
day's bars at equal ATR%). Sub-$5 names: V1 positive in all 6 years but not significant (+4.17 / +3.31).
The only clean way to learn whether the effect returns is forward data the research never saw.

Frozen 2026-10-01; FILINGS dated 2026-04-01 … 2027-02-28 (the first quarter after the last archive seen);
read ONCE on/after 2027-06-01 (60-session horizon of the last event plus the SEC quarterly publication lag).

Rules (officer/director only — relationship contains Director or Officer; open-market P / acquired A;
Form 4 or 4/A; event bar = first session ≥ FILING date, entry next open):
  F1 CLUSTER  ≥ 2 distinct insiders with filings in the trailing 7 calendar days (event when the count
              first reaches 2), book universe (close ≥ $5, 20-day $volume ≥ $5M)
  F2 CEO/CFO  own day total ≥ $50k (title CEO|Chief Executive|CFO|Chief Financial), book universe
  F3 SUB-$5   any officer/director day total ≥ $10k, universe close $1-5 and 20-day $volume ≥ $1M
Estimand (the research one): PER-BAR book return (bottom_cluster_forward._bar_returns: ATR×12 trail,
maxh 60, 15 bps, no cooldown) of event bars vs ALL bars of the same universe, paired within the same
day × ATR%-bin (frozen cut points below), day-clustered bootstrap. PASS = mean Δ > 0 with 95 % CI lo > 0.
Forward k = 3. Sub-$5 costs are optimistic (spreads wider than 15 bps).

DATA: put the SEC quarterly archives (YYYYqN_form345.zip, 2026q2 … 2027q1) into
MASSIVE_DATA/INSIDER_FORM345/raw — each download needs the user's OK (SEC User-Agent with contact).

  python insider_forward.py                     → the forward read (refuses before 2027-06-01)
  python insider_forward.py --check 2024-01-01 2024-12-31
                                                → the same code on a SEEN window (no verdict)
"""
from __future__ import annotations
import glob, io, os, re, sys, zipfile
from datetime import date

import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import _bar_returns, _paired, ATR_CUTS, HORIZON   # noqa: E402
from studio.paths import db_path                                                 # noqa: E402

RAW = "/Users/sachoki/MASSIVE_DATA/INSIDER_FORM345/raw"
FORWARD_START, FORWARD_END = "2026-04-01", "2027-02-28"
READ_ON_OR_AFTER = date(2027, 6, 1)
# sub-$5 ATR%-bin cut points, frozen from the research MINE window (2021-23) of that universe
ATR_CUTS_SUB5 = [0.038569, 0.044454, 0.048951, 0.053188, 0.056917, 0.060487, 0.06396, 0.067412, 0.070926,
                 0.074543, 0.078349, 0.082379, 0.086642, 0.091494, 0.097401, 0.104513, 0.113943, 0.129796,
                 0.16151, 0.220616, 0.37484]
CEO_RE = r"\bCEO\b|Chief Executive|\bCFO\b|Chief Financial"


def load_purchases() -> pd.DataFrame:
    out = []
    for f in sorted(glob.glob(os.path.join(RAW, "*_form345.zip"))):
        z = zipfile.ZipFile(f)
        rd = lambda n, cols: pd.read_csv(io.BytesIO(z.read(n)), sep="\t", dtype=str, usecols=cols, quoting=3, on_bad_lines="skip")
        sub = rd("SUBMISSION.tsv", ["ACCESSION_NUMBER", "FILING_DATE", "DOCUMENT_TYPE", "ISSUERTRADINGSYMBOL"])
        tr = rd("NONDERIV_TRANS.tsv", ["ACCESSION_NUMBER", "TRANS_DATE", "TRANS_CODE", "TRANS_SHARES", "TRANS_PRICEPERSHARE", "TRANS_ACQUIRED_DISP_CD"])
        ow = rd("REPORTINGOWNER.tsv", ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNER_RELATIONSHIP", "RPTOWNER_TITLE"])
        p = tr[(tr.TRANS_CODE == "P") & (tr.TRANS_ACQUIRED_DISP_CD == "A")]
        p = p.merge(sub, on="ACCESSION_NUMBER").merge(ow.drop_duplicates("ACCESSION_NUMBER"), on="ACCESSION_NUMBER", how="left")
        out.append(p[p.DOCUMENT_TYPE.isin(["4", "4/A"])])
    P = pd.concat(out, ignore_index=True)
    for c_ in ("TRANS_SHARES", "TRANS_PRICEPERSHARE"):
        P[c_] = pd.to_numeric(P[c_], errors="coerce")
    P["value"] = P.TRANS_SHARES * P.TRANS_PRICEPERSHARE
    P["fdt"] = pd.to_datetime(P.FILING_DATE, format="%d-%b-%Y", errors="coerce")
    P["ticker"] = P.ISSUERTRADINGSYMBOL.str.upper().str.strip()
    P = P.drop_duplicates(["ACCESSION_NUMBER", "TRANS_DATE", "TRANS_SHARES", "TRANS_PRICEPERSHARE"])
    P = P[P.RPTOWNER_RELATIONSHIP.fillna("").str.contains("Director|Officer") & (P.value > 0)].copy()
    P["ceo"] = P.RPTOWNER_TITLE.fillna("").str.contains(CEO_RE, flags=re.I)
    P["fd"] = P.fdt.dt.strftime("%Y-%m-%d")
    return P


def events(P: pd.DataFrame, d0: str, d1: str) -> dict:
    P = P[(P.fd >= d0) & (P.fd <= d1)]
    day = P.groupby(["ticker", "fd"]).agg(val=("value", "sum")).reset_index()
    ceo = P[P.ceo].groupby(["ticker", "fd"]).value.sum().reset_index()
    cl = set()
    for t_, g in P.groupby("ticker"):
        prev = 0
        for dd_ in sorted(g.fdt.unique()):
            cnt = g[(g.fdt > dd_ - pd.Timedelta(days=7)) & (g.fdt <= dd_)].RPTOWNERCIK.nunique()
            if cnt >= 2 and prev < 2:
                cl.add((t_, pd.Timestamp(dd_).strftime("%Y-%m-%d")))
            prev = cnt
    return {"F1": cl,
            "F2": set(map(tuple, ceo[ceo.value >= 5e4][["ticker", "fd"]].to_numpy())),
            "F3": set(map(tuple, day[day.val >= 1e4][["ticker", "fd"]].to_numpy()))}


def run(d0: str, d1: str) -> dict:
    P = load_purchases()
    ev = events(P, d0, d1)
    load_from = (pd.Timestamp(d0) - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""WITH r AS (SELECT ticker, cast(date as varchar)[:10] date, open, high, low, "close", volume,
            row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
            FROM bars WHERE universe <> 'index' AND date >= '{load_from}')
        SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
    g = df.groupby("ticker", sort=False)
    pc = g["close"].shift(1)
    tr = np.maximum(df.high - df.low, np.maximum((df.high - pc).abs(), (df.low - pc).abs()))
    df["atr_14"] = tr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    dv = (df["close"] * df["volume"]).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    tk = df.ticker.to_numpy(); dts = df.date.to_numpy(); n = len(df)
    cal = sorted(df.date.unique()); last_ok = cal[-(HORIZON + 2)] if len(cal) > HORIZON + 2 else cal[0]
    hi = min(d1, last_ok)
    O_, H_, L_, C_, A_ = (df[x].to_numpy(float) for x in ("open", "high", "low", "close", "atr_14"))
    book = (C_ >= 5) & (dv.to_numpy() >= 5e6)
    sub5 = (C_ >= 1) & (C_ < 5) & (dv.to_numpy() >= 1e6)
    by = {t_: (g_.date.to_numpy(), g_.index.to_numpy()) for t_, g_ in df.groupby("ticker", sort=False)}
    def mask(pairs):
        m = np.zeros(n, bool)
        for t_, dd_ in pairs:
            if t_ in by:
                ds, ix = by[t_]; k = np.searchsorted(ds, dd_)
                if k < len(ds): m[ix[k]] = True
        return m
    rng = np.random.default_rng(20261001)
    out = {"window": [d0, hi], "purchases_in_window": int(((P.fd >= d0) & (P.fd <= d1)).sum())}
    for rule, uni, cuts in (("F1", book, ATR_CUTS), ("F2", book, ATR_CUTS), ("F3", sub5, ATR_CUTS_SUB5)):
        m = mask(ev[rule])
        cand = np.flatnonzero(uni & np.isfinite(A_) & (A_ > 0) & (dts >= d0) & (dts <= hi))
        r = _bar_returns(O_, H_, L_, C_, A_, tk, cand); ok_ = ~np.isnan(r); cand, r = cand[ok_], r[ok_]
        fr = pd.DataFrame({"d": dts[cand], "r": r, "k": np.digitize(A_[cand] / C_[cand], cuts), "g": m[cand]})
        out[rule] = _paired(fr, rng); out[rule]["n_events"] = int(m[cand].sum())
    return out


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--check":
        print("SEEN-WINDOW CHECK (no verdict):", run(sys.argv[2], sys.argv[3]))
    elif len(sys.argv) == 1:
        if date.today() < READ_ON_OR_AFTER:
            sys.exit(f"refusing: the forward read is registered for on/after {READ_ON_OR_AFTER} (single read)")
        print("FORWARD READ (INSIDER_BUY F1-F3):", run(FORWARD_START, FORWARD_END))
    else:
        sys.exit(__doc__)
