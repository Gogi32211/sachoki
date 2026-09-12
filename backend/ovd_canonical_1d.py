"""OPENING_VOLUME_DYNAMICS_V1 — CANONICAL 1D OHLCV AUTHORITY (option A, user decision).

The Studio 1D store (studio_analytics.duckdb) has vintage-dependent corporate-action seams
(bulk import adjusted as of 2026-05-22; later splits appended unadjusted; universe copies
disagreeing by the split factor). It is NOT patched and NOT used as a price authority here.
It is used for exactly one thing: the TICKER ROSTER of the study's eligible universe.

CONVENTION (one basis for the whole window, proven by probe 2026-09-04):
  source        Massive daily aggregates  GET /v2/aggs/ticker/{T}/range/1/day/{from}/{to}
  adjusted      true  -> split-adjusted AS OF THE FETCH DATE (parameter proven live: NVDA
                pre-split 1209.98 under false vs 121.00 under true, volume x10 inverse)
  dividends     NOT adjusted (COST closes identical under true/false across the 2023-12-27
                $15 special-dividend ex-date) — recorded, not invented
  basis_asof    the fetch timestamp of THIS run; every row carries it
  split events  GET /v3/reference/splits per ticker, persisted raw with digests — used for
                VALIDATION (A4/A5), never to create adjustment factors

Nothing here reads or writes the Studio database. Import-side-effect free.
"""
from __future__ import annotations
import os, sys, json, time, hashlib, tempfile, concurrent.futures as cf      # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
CANON_DIR = os.path.join(FAMILY_DIR, "canonical_1d")
SPLIT_DIR = os.path.join(CANON_DIR, "split_reference")
FROM_DATE = "2021-05-01"
RATE_DELAY = 0.08            # same pacing the repo's Massive client uses
WORKERS = 6


class HardStop(RuntimeError):
    pass


def _env():
    from dotenv import load_dotenv
    load_dotenv(os.path.join(HERE, ".env"))
    base = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
    key = os.environ.get("MASSIVE_API_KEY") or os.environ.get("POLYGON_API_KEY") or ""
    if not key:
        raise HardStop("no vendor key in environment (MASSIVE_API_KEY / POLYGON_API_KEY)")
    return base, key


def _get(base, key, path, params, tries=4):
    import requests
    for a in range(tries):
        try:
            r = requests.get(f"{base}{path}", params={**params, "apiKey": key}, timeout=(10, 45))
        except requests.RequestException:
            time.sleep(min(2 ** a, 16)); continue
        if r.status_code == 429:
            time.sleep(min(2 * (a + 1), 20)); continue
        if r.status_code != 200:
            return r.status_code, None
        return 200, r.json()
    return 599, None


def fetch_daily(base, key, tk, frm, to) -> tuple[list, str]:
    """All daily bars in [frm, to], adjusted=true, following next_url. Returns (rows, status)."""
    rows, url, params = [], f"/v2/aggs/ticker/{tk}/range/1/day/{frm}/{to}", {"adjusted": "true", "sort": "asc", "limit": 50000}
    for _ in range(20):
        sc, js = _get(base, key, url, params)
        if sc != 200 or js is None:
            return rows, f"http_{sc}"
        rows += js.get("results", []) or []
        nxt = js.get("next_url")
        if not nxt:
            return rows, "ok"
        url = nxt.replace(base, ""); params = {}
        time.sleep(RATE_DELAY)
    return rows, "pages_exhausted"


def fetch_splits(base, key, tk) -> tuple[list, int]:
    sc, js = _get(base, key, "/v3/reference/splits", {"ticker": tk, "limit": 100, "order": "asc"})
    return ((js or {}).get("results", []) if sc == 200 else []), sc


def roster(months: int = 72) -> list[str]:
    """Ticker roster ONLY (the Studio store is not a price authority here)."""
    import duckdb
    a = duckdb.connect(os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb"), read_only=True)
    r = [x[0] for x in a.execute(f"""SELECT DISTINCT ticker FROM bars WHERE universe<>'index'
         AND close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
         AND date >= (SELECT max(date) FROM bars) - INTERVAL {months*31+60} DAY ORDER BY 1""").fetchall()]
    a.close()
    return r


def build(months: int = 72, limit: int | None = None) -> dict:
    import pandas as pd, duckdb
    base, key = _env()
    os.makedirs(SPLIT_DIR, exist_ok=True)
    run_id = time.strftime("CANON1D_%Y%m%dT%H%M%SZ", time.gmtime())
    basis_asof = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    to = time.strftime("%Y-%m-%d", time.gmtime())
    tks = roster(months)
    if limit:
        tks = tks[:limit]
    t0 = time.time()
    print(f"{run_id}: {len(tks):,} tickers, window {FROM_DATE}..{to}, adjusted=true, basis_asof {basis_asof}", flush=True)

    def one(tk):
        rows, st = fetch_daily(base, key, tk, FROM_DATE, to)
        time.sleep(RATE_DELAY)
        sp, ssc = fetch_splits(base, key, tk)
        time.sleep(RATE_DELAY)
        return tk, rows, st, sp, ssc

    frames, status, splits_all = [], {}, []
    with cf.ThreadPoolExecutor(WORKERS) as ex:
        for i, (tk, rows, st, sp, ssc) in enumerate(ex.map(one, tks)):
            status[tk] = dict(bars=len(rows), status=st, splits=len(sp), splits_http=ssc)
            for e in sp:
                splits_all.append(dict(ticker=tk, execution_date=e.get("execution_date"),
                                       split_from=e.get("split_from"), split_to=e.get("split_to"), id=e.get("id")))
            if rows:
                df = pd.DataFrame(rows)
                df["ticker"] = tk
                frames.append(df)
            if i and i % 250 == 0:
                print(f"  {i:,}/{len(tks):,}  ({time.time()-t0:.0f}s)", flush=True)
    if not frames:
        raise HardStop("no bars fetched")
    X = pd.concat(frames, ignore_index=True)
    X["session_date"] = pd.to_datetime(X["t"], unit="ms", utc=True).dt.tz_convert("America/New_York").dt.date
    X = X.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "vw": "vwap", "n": "trades"})
    X = X[["ticker", "session_date", "open", "high", "low", "close", "volume", "vwap", "trades"]]
    X["source_id"] = "massive:/v2/aggs/1/day"
    X["adjustment_convention"] = "SPLIT_ADJUSTED_ASOF_FETCH;DIVIDENDS_NOT_ADJUSTED"
    X["basis_asof"] = basis_asof
    X["run_id"] = run_id
    # ── persist RAW first (a 23-minute vendor pull is never discarded by a validator) ──
    raw_out = os.path.join(CANON_DIR, f"raw_{run_id}.parquet")
    fd, tmp = tempfile.mkstemp(dir=CANON_DIR, prefix=".tmp_"); os.close(fd)
    X.to_parquet(tmp, index=False); os.replace(tmp, raw_out)
    # ── validate → QUARANTINE with reasons; HARD STOP only if material ──
    dup = X.duplicated(["ticker", "session_date"], keep=False)
    bad_px = (X[["open", "high", "low", "close"]] <= 0).any(axis=1) | X[["open", "high", "low", "close"]].isna().any(axis=1)
    bad_hl = (X["high"] < X["low"]) | (X["high"] < X[["open", "close"]].max(axis=1)) | (X["low"] > X[["open", "close"]].min(axis=1))
    bad_vol = X["volume"].isna() | (X["volume"] < 0)
    reason = pd.Series("", index=X.index)
    reason[bad_px] = "NONPOSITIVE_OR_NULL_PRICE"; reason[bad_hl & (reason == "")] = "HL_INCONSISTENT"
    reason[bad_vol & (reason == "")] = "BAD_VOLUME"; reason[dup & (reason == "")] = "DUPLICATE_KEY"
    Q = X[reason != ""].assign(quarantine_reason=reason[reason != ""])
    X = X[reason == ""].reset_index(drop=True)
    q_share = len(Q) / max(1, len(Q) + len(X))
    q_out = os.path.join(CANON_DIR, f"quarantine_{run_id}.parquet")
    Q.to_parquet(q_out, index=False)
    print(f"  quarantined {len(Q):,} rows ({100*q_share:.4f}%) on {Q.ticker.nunique() if len(Q) else 0} tickers: "
          f"{Q.quarantine_reason.value_counts().to_dict() if len(Q) else {}}", flush=True)
    if len(Q):
        print("  quarantine sample:", Q[["ticker", "session_date", "open", "high", "low", "close", "volume", "quarantine_reason"]].head(6).to_dict("records"), flush=True)
    if q_share > 0.001:
        raise HardStop(f"quarantine share {100*q_share:.3f}% > 0.1% — vendor response not trustworthy as a whole; raw kept at {raw_out}")
    if X.duplicated(["ticker", "session_date"]).any():
        raise HardStop("duplicates survived quarantine")
    S = pd.DataFrame(splits_all)
    out = os.path.join(CANON_DIR, f"canonical_1d_{run_id}.parquet")
    fd, tmp = tempfile.mkstemp(dir=CANON_DIR, prefix=".tmp_"); os.close(fd)
    X.to_parquet(tmp, index=False); os.replace(tmp, out)
    sp_out = os.path.join(SPLIT_DIR, f"splits_{run_id}.parquet")
    S.to_parquet(sp_out, index=False)
    json.dump(status, open(os.path.join(CANON_DIR, f"fetch_status_{run_id}.json"), "w"), indent=1)
    dig = lambda p: hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]
    man = dict(run_id=run_id, basis_asof=basis_asof, host=base, window=[FROM_DATE, to],
               convention="SPLIT_ADJUSTED_ASOF_FETCH;DIVIDENDS_NOT_ADJUSTED",
               tickers_requested=len(tks), tickers_with_bars=int(X.ticker.nunique()), rows=int(len(X)),
               session_range=[str(X.session_date.min()), str(X.session_date.max())],
               fetch_failures={k: v for k, v in status.items() if v["status"] != "ok"},
               splits_events=int(len(S)), splits_tickers=int(S.ticker.nunique()) if len(S) else 0,
               canonical_parquet=out, canonical_sha256_16=dig(out),
               splits_parquet=sp_out, splits_sha256_16=dig(sp_out),
               raw_parquet=raw_out, raw_sha256_16=dig(raw_out),
               quarantine=dict(rows=int(len(Q)), tickers=int(Q.ticker.nunique()) if len(Q) else 0,
                               share=round(q_share, 6), reasons=(Q.quarantine_reason.value_counts().to_dict() if len(Q) else {}),
                               parquet=q_out, rule="rows failing price/HL/volume/duplicate checks are EXCLUDED from canonical and kept here; HARD STOP if share > 0.1%"),
               elapsed_s=round(time.time() - t0))
    json.dump(man, open(os.path.join(CANON_DIR, f"MANIFEST_{run_id}.json"), "w"), indent=1)
    json.dump(man, open(os.path.join(CANON_DIR, "CURRENT.json"), "w"), indent=1)
    print(json.dumps({k: man[k] for k in ("run_id", "rows", "tickers_with_bars", "session_range", "splits_events", "canonical_sha256_16", "elapsed_s")}, indent=1), flush=True)
    print("fetch failures:", len(man["fetch_failures"]), flush=True)
    return man


def main():
    months = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 72
    limit = None
    for a in sys.argv:
        if a.startswith("--limit="):
            limit = int(a.split("=")[1])
    build(months=months, limit=limit)


if __name__ == "__main__":
    main()
