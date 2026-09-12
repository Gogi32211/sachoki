"""OVD DAILY MAP — the "260904_OVD_4_VOLUME_LOGICS_DAILY_MAP" Pine v6 script, ported for DISPLAY.

STATUS OF EVIDENCE — read before trusting a token
  The research family OPENING_VOLUME_DYNAMICS_V1/V2.1 (300 registered cells, sealed, direct sacred
  _pathsim, 2026-09-04) closed 0 BUILD / 0 VETO / 283 NULL / 17 THIN. These tokens are the Pine
  script's VISUAL reproduction of the four volume logics — as the script itself says, L1/L2 are
  visual state reproductions and the sealed A/B sample gating is not reproduced. DESCRIPTIVE ONLY,
  never a ranking input. Nothing here touches the sealed artifacts.

TOKENS (the user's own renaming from the merged chart script, kept so they never collide with the
WLNBB L1..L6 labels):  OB = Logic 1 opening build · RC = Logic 2 prior-HV reclaim ·
  CD = Logic 3 close dominance · HO = Logic 4 close→next-open handoff · NM? = near-miss proxy.

DEFINITIONS (verbatim from the script; per ticker, over the DAILY bar sequence, 15m slots by NY time)
  slots      m1..m4 = 15m volume at 09:30 09:45 10:00 10:15 · cv1..cv4 at 15:00 15:15 15:30 15:45
  windows    o30a = m1+m2 · o30b = m3+m4 · o60 = o30a+o30b · c30a = cv1+cv2 · c30b = cv3+cv4 · c60 = c30a+c30b
             (each only when both of its slots exist); full regular day = all eight slots present
  RVOL       same-slot value / median of the SAME slot over the last 20 FULL regular sessions strictly
             before the day (na until 20 such sessions exist) — the script's f_med / f_push_valid
  OB·30      rvO30A rising 3 sessions in a row AND rvO30B rising 3 sessions in a row (D-2 < D-1 < D)
  OB·60      rvO60 rising 3 sessions in a row
  HV event   a day s with rvO60(s) ≥ 2.0 and close(s-1) < close(s-6)
  RC·30/60   the NEAREST HV event 6..30 sessions back exists and today's o30a/o30b (RC·30: both) or o60
             (RC·60) ≥ that event day's same window
  CD·60      close < close[5] and rvC60 > 1 and c60 / o60 ≥ 1        CD·30: rvC30B > 1 and c30b / o30a ≥ 1
  HO·60      yesterday closed complete with close[5]-decline and rvC60 > 1; today rvO60 > 1 and
             o60 / yesterday's c60 within [0.80, 1.25]              HO·30: the c30b / o30a analogue
  NM?        reclaim60 in [0.50, 0.75) and all four opening 15m slots have RVOL > 1
  Daily close series = the app's 1D store (deduplicated per ticker); a day whose 15m bars are missing
  simply has na slots, exactly as an empty intrabar array does in Pine.

OUTPUT  data/ovdmap_signals.parquet — only days carrying ≥ 1 token; nine booleans, `tokens`, `text`.
Built by lbal_build.py in the same nightly step (atomic replace).
"""
from __future__ import annotations
import os
import sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from studio.paths import DATA_DIR                                      # noqa: E402

OUT = os.path.join(DATA_DIR, "ovdmap_signals.parquet")
RVOL_LEN = 20
HANDOFF_LOW, HANDOFF_HIGH = 0.80, 1.25
EVENT_K = range(6, 31)          # nearest HV event 6..30 sessions back
HV_EVENT_RVOL = 2.0
NM_RECLAIM = (0.50, 0.75)
SLOTS = {"m1": "09:30", "m2": "09:45", "m3": "10:00", "m4": "10:15", "cv1": "15:00", "cv2": "15:15", "cv3": "15:30", "cv4": "15:45"}
TOKENS = [("ob30", "OB·30"), ("ob60", "OB·60"), ("rc30", "RC·30"), ("rc60", "RC·60"),
          ("cd30", "CD·30"), ("cd60", "CD·60"), ("ho30", "HO·30"), ("ho60", "HO·60"), ("nm", "NM?")]
TOKEN_MEANING = {
    "OB·30": "Logic 1 · opening build — both opening 30m windows' same-slot RVOL rose three sessions in a row",
    "OB·60": "Logic 1 · opening build — the opening 60m same-slot RVOL rose three sessions in a row",
    "RC·30": "Logic 2 · prior-HV reclaim — both opening 30m windows ≥ the nearest HV-decline event (6..30 sessions back)",
    "RC·60": "Logic 2 · prior-HV reclaim — the opening 60m ≥ the nearest HV-decline event's opening 60m",
    "CD·30": "Logic 3 · close dominance — 5-day decline, last-30m RVOL > 1 and last 30m ≥ first 30m",
    "CD·60": "Logic 3 · close dominance — 5-day decline, last-60m RVOL > 1 and last 60m ≥ first 60m",
    "HO·30": "Logic 4 · handoff — yesterday's heavy last 30m (in a decline) carried into today's first 30m (0.80..1.25×, RVOL > 1)",
    "HO·60": "Logic 4 · handoff — yesterday's heavy last 60m (in a decline) carried into today's first 60m (0.80..1.25×, RVOL > 1)",
    "NM?":   "near-miss proxy — reclaim60 in [0.50, 0.75) and all four opening 15m slots have RVOL > 1",
}


def slots_sql(src: str) -> str:
    """One row per (ticker, NY session) with the eight slot volumes (na when the 15m bar is absent)."""
    cols = ", ".join(f"max(CASE WHEN hm = '{t}' THEN v END) AS {k}" for k, t in SLOTS.items())
    return f"""
    SELECT ticker, CAST(ny AS DATE) AS session, {cols}
    FROM (SELECT ticker, COALESCE(volume, 0)::DOUBLE AS v,
                 timezone('America/New_York', timezone('UTC', date)) AS ny,
                 strftime(timezone('America/New_York', timezone('UTC', date)), '%H:%M') AS hm
          FROM {src})
    GROUP BY 1, 2"""


def prior_median(values: np.ndarray, full: np.ndarray, n: int = RVOL_LEN) -> np.ndarray:
    """den[i] = median of `values` over the last n FULL sessions strictly before i (nan until n exist).
    Pine: f_push_valid on full regular days only, array capped at n, f_med requires size >= n; pushed
    only AFTER the day's own states are computed, so the day never sees itself."""
    fv = values[full]
    rm = pd.Series(fv).rolling(n).median().to_numpy() if len(fv) else np.array([])
    n_before = np.cumsum(full) - full.astype(int)
    den = np.full(len(values), np.nan)
    ok = n_before >= n
    if ok.any():
        den[ok] = rm[n_before[ok] - 1]
    return den


def _shift(a: np.ndarray, k: int) -> np.ndarray:
    out = np.full(len(a), np.nan)
    if k < len(a):
        out[k:] = a[:len(a) - k]
    return out


def _ratio(num, den):
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.where(np.isfinite(num) & np.isfinite(den) & (den > 0), num / den, np.nan)
    return r


def compute_ticker(d: pd.DataFrame) -> pd.DataFrame:
    """d: one ticker's DAILY bars (sorted): date, close, m1..m4, cv1..cv4. Returns the per-day flags."""
    g = lambda c: d[c].to_numpy(float)
    m1, m2, m3, m4, cv1, cv2, cv3, cv4 = (g(c) for c in SLOTS)
    close = g("close")
    fin = np.isfinite
    oA, oB, cA, cB = fin(m1) & fin(m2), fin(m3) & fin(m4), fin(cv1) & fin(cv2), fin(cv3) & fin(cv4)
    opening, closing = oA & oB, cA & cB
    full = opening & closing
    o30a = np.where(oA, m1 + m2, np.nan); o30b = np.where(oB, m3 + m4, np.nan); o60 = np.where(opening, o30a + o30b, np.nan)
    c30a = np.where(cA, cv1 + cv2, np.nan); c30b = np.where(cB, cv3 + cv4, np.nan); c60 = np.where(closing, c30a + c30b, np.nan)
    rv = {k: _ratio(v, prior_median(v, full)) for k, v in
          dict(m1=m1, m2=m2, m3=m3, m4=m4, o30a=o30a, o30b=o30b, o60=o60, c30a=c30a, c30b=c30b, c60=c60).items()}
    # Logic 1 — three rising same-slot RVOLs (histories are session-aligned: D-1, D-2 are the previous daily bars)
    def rising3(x):
        p1, p2 = _shift(x, 1), _shift(x, 2)
        return fin(p2) & fin(p1) & fin(x) & (p2 < p1) & (p1 < x)
    ob30 = rising3(rv["o30a"]) & rising3(rv["o30b"])
    ob60 = rising3(rv["o60"])
    # Logic 2 — nearest HV-decline event 6..30 sessions back
    c1, c6 = _shift(close, 1), _shift(close, 6)
    flag = fin(rv["o60"]) & (rv["o60"] >= HV_EVENT_RVOL) & fin(c6) & (c1 < c6)
    eA = np.full(len(d), np.nan); eB = eA.copy(); e60 = eA.copy(); found = np.zeros(len(d), bool); ek = np.zeros(len(d), int)
    for k in EVENT_K:
        fk = _shift(flag.astype(float), k) == 1.0
        new = fk & ~found
        eA[new] = _shift(o30a, k)[new]; eB[new] = _shift(o30b, k)[new]; e60[new] = _shift(o60, k)[new]
        ek[new] = k; found |= new
    r30a, r30b, r60 = _ratio(o30a, eA), _ratio(o30b, eB), _ratio(o60, e60)
    rc30 = found & fin(r30a) & fin(r30b) & (r30a >= 1.0) & (r30b >= 1.0)
    rc60 = found & fin(r60) & (r60 >= 1.0)
    # Logic 3 — close dominance under a 5-day decline
    c5 = _shift(close, 5)
    decline = fin(c5) & (close < c5)
    cd60 = decline & fin(rv["c60"]) & (rv["c60"] > 1.0) & fin(c60) & fin(o60) & (o60 > 0) & (_ratio(c60, o60) >= 1.0)
    cd30 = decline & fin(rv["c30b"]) & (rv["c30b"] > 1.0) & fin(c30b) & fin(o30a) & (o30a > 0) & (_ratio(c30b, o30a) >= 1.0)
    # Logic 4 — previous close → current open handoff (yesterday's state only if yesterday closed complete)
    y_closing = _shift(closing.astype(float), 1) == 1.0
    pC30B = np.where(y_closing, _shift(c30b, 1), np.nan); pRvC30B = np.where(y_closing, _shift(rv["c30b"], 1), np.nan)
    pC60 = np.where(y_closing, _shift(c60, 1), np.nan); pRvC60 = np.where(y_closing, _shift(rv["c60"], 1), np.nan)
    pDecline = y_closing & (_shift(decline.astype(float), 1) == 1.0)
    h30, h60 = _ratio(o30a, pC30B), _ratio(o60, pC60)
    ho30 = pDecline & fin(pRvC30B) & (pRvC30B > 1.0) & fin(rv["o30a"]) & (rv["o30a"] > 1.0) & fin(h30) & (h30 >= HANDOFF_LOW) & (h30 <= HANDOFF_HIGH)
    ho60 = pDecline & fin(pRvC60) & (pRvC60 > 1.0) & fin(rv["o60"]) & (rv["o60"] > 1.0) & fin(h60) & (h60 >= HANDOFF_LOW) & (h60 <= HANDOFF_HIGH)
    # near-miss proxy
    allm = fin(rv["m1"]) & fin(rv["m2"]) & fin(rv["m3"]) & fin(rv["m4"])
    breadth1 = allm & (rv["m1"] > 1) & (rv["m2"] > 1) & (rv["m3"] > 1) & (rv["m4"] > 1)
    nm = fin(r60) & (r60 >= NM_RECLAIM[0]) & (r60 < NM_RECLAIM[1]) & breadth1
    out = pd.DataFrame(dict(date=d["date"].to_numpy(), ob30=ob30, ob60=ob60, rc30=rc30, rc60=rc60, cd30=cd30, cd60=cd60,
                            ho30=ho30, ho60=ho60, nm=nm, full=full, rv_o60=rv["o60"], rv_c60=rv["c60"],
                            reclaim60=r60, event_k=ek, handoff60=h60, hv_event=flag))
    return out


def tokens_of(row) -> str:
    return " ".join(sym for key, sym in TOKENS if bool(row[key]))


def text_of(row) -> str:
    def f(x, nd=2):
        return "—" if x is None or (isinstance(x, float) and not np.isfinite(x)) else f"{x:.{nd}f}"
    ev = f"reclaim60 {f(row['reclaim60'])} (event {int(row['event_k'])}d back)" if row["event_k"] else "no HV event 6..30d back"
    return f"{tokens_of(row)} · rvO60 {f(row['rv_o60'])} · rvC60 {f(row['rv_c60'])} · {ev} · handoff60 {f(row['handoff60'])}"


def build(slots: pd.DataFrame, daily: pd.DataFrame, log=print) -> pd.DataFrame:
    """slots: the slots_sql result (ticker, session, m1..cv4); daily: deduplicated 1D bars (ticker, date 'YYYY-MM-DD', close)."""
    slots = slots.copy()
    slots["date"] = pd.to_datetime(slots["session"]).dt.strftime("%Y-%m-%d")
    slots = slots.drop(columns=["session"])
    log(f"  OVD map: {len(slots):,} sessions with 15m slots")
    d = daily[["ticker", "date", "close"]].merge(slots, on=["ticker", "date"], how="left").sort_values(["ticker", "date"])
    parts = []
    for tk, g in d.groupby("ticker", sort=False):
        r = compute_ticker(g.reset_index(drop=True))
        any_tok = r[[k for k, _ in TOKENS]].any(axis=1)
        if any_tok.any():
            r = r[any_tok].copy(); r.insert(0, "ticker", tk); parts.append(r)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    out["tokens"] = [tokens_of(r) for _, r in out.iterrows()]
    out["text"] = [text_of(r) for _, r in out.iterrows()]
    out["event_k"] = out["event_k"].astype("int16")
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    if out.duplicated(["ticker", "date"]).any():
        raise RuntimeError("OVD map: duplicate (ticker, date) grain")
    if (out["tokens"] == "").any():
        raise RuntimeError("OVD map: a stored row without a token")
    return out
