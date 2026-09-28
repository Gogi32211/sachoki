"""Nightly: TURN·58 count + ⟲ROW per-row turn tiers for EVERY ticker, last KEEP sessions → Ultra filters.

Why a build step: both gauges need several bars of the app's full fired-signal catalog (TURN·58: t-2..t
on a 10-bar low; ⟲ROW: t-4..t per row), and Ultra only holds the latest bar per ticker. So this runs the
research pipeline the two gauges were validated on (research_out/TURN_SET_V1.md, ROWSEQ_V1.md) over a
recent window, and evaluates it with the SAME JS the Superchart runs — the catalog (UltraScanPanel
V4_ALL_GROUPS / v4Fired), lib/turnCount.js and lib/rowSeq.js, bundled by esbuild. No re-typed copies,
so Ultra and the chart cannot drift apart.

Per session row (same sources as the research builder):
  bars (studio_analytics.duckdb, one row per ticker/date — universe order as Ultra) renamed to UI keys
  + the 7 display stores through each store's own _to_ui()  + anatomy + PT5/PT9 strong
  + ▲4H/△1H REV days from studio_4h/studio_1h with the live rule the Superchart and Ultra use
  + EDGE codes with Ultra's semantics (same-day and CODE·Nd for 1..5 calendar days).

Output: data/turn_rowseq_signals.parquet — ticker, date, turn_cand, turn_n, rs_<row> (0/1/2), rs_pair —
plus data/TURN_ROWSEQ_SIGNALS_V1.json (build meta). Read by studio/turn_rowseq_store.py.

DESCRIPTIVE ONLY. Both gauges identify turn zones (replicated out of sample); neither picks a better
trade (same-day Δ ≈ 0) and big-move prediction was NULL. Never a ranking or score input.
"""
from __future__ import annotations
import json, os, subprocess, sys, tempfile, time
from datetime import date as _date, timedelta as _td

import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.chdir(HERE)
from studio.paths import db_path, DATA_DIR                      # noqa: E402
from studio import (lbal_store, lvx_store, ovdmap_store, pv_multi_store, shapectx_store,  # noqa: E402
                    vol7_store, vol_echo_store)

KEEP = 15                   # sessions written per ticker
WINDOW_DAYS = 110           # calendar days of bars loaded (≥ 60 sessions: FLY-fresh needs a 15-bar gap)
CHUNK = 800                 # tickers per node call
ROOT = os.path.dirname(HERE)
TR = os.path.join(HERE, "turn_rowseq")
OUT = os.path.join(DATA_DIR, "turn_rowseq_signals.parquet")
META = os.path.join(DATA_DIR, "TURN_ROWSEQ_SIGNALS_V1.json")
ROWS = ["FLY", "GR", "MTF", "PHYS", "VOL7", "PV", "BREAK", "OVD", "DELTA"]
t0 = time.time()
log = lambda *a: print(f"[{time.time()-t0:6.0f}s]", *a, flush=True)


def _bundle(tmp: str) -> str:
    out = os.path.join(tmp, "bundle.mjs")
    esb = os.path.join(ROOT, "frontend", "node_modules", ".bin", "esbuild")
    subprocess.run([esb, os.path.join(TR, "entry.mjs"), "--bundle", "--format=esm", "--platform=node",
                    f"--outfile={out}", "--log-level=error", '--define:import.meta.env={"VITE_API_URL":""}'],
                   check=True, cwd=ROOT)
    return out


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="turn_rowseq_")
    bundle = _bundle(tmp)
    deps = json.loads(subprocess.run(["node", os.path.join(TR, "runner.mjs"), bundle, "deps"],
                                     check=True, capture_output=True, text=True).stdout)
    srcs = json.load(open(os.path.join(TR, "field_sources.json")))
    need = sorted({f for v in deps.values() for f in v})
    log("catalog keys", len(deps), "fields", len(need))

    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    bars_cols = {r[0] for r in con.execute("DESCRIBE bars").fetchall()}
    ui_from_db = {f: srcs[f][5:] for f in need if str(srcs.get(f, "")).startswith("bars:")}
    for f, c in {"tz_any_t": "sig_t", "tz_any_z": "sig_z", "tz_bull_flip": "sig_tz_flip",
                 "tz_wlnbb_l_signal": "l_sig"}.items():
        if c in bars_cols:
            ui_from_db[f] = c
    for st in ["T1G", "T2G", "T1", "T2", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12"]:
        if f"sig_{st.lower()}" in bars_cols:
            ui_from_db[f"tz_{st.lower()}"] = f"sig_{st.lower()}"
    base = ["ticker", "date", "open", "high", "low", "close", "volume", "atr_14", "rsi_14", "context_tokens",
            "profile_category", "buy_score", "l_sig", "t_sig", "z_sig", "bar_body_wick",
            "sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad"]
    sel = [c for c in sorted(set(base) | set(ui_from_db.values())) if c in bars_cols]
    maxd = con.execute("SELECT max(date) FROM bars").fetchone()[0]
    cutoff = (pd.Timestamp(maxd) - pd.Timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
    tickers = [r[0] for r in con.execute(f"SELECT DISTINCT ticker FROM bars WHERE date >= '{cutoff}' "
                                         "AND universe <> 'index' ORDER BY 1").fetchall()]
    log(f"bars {cutoff} → {maxd}: {len(tickers)} tickers, {len(sel)} columns")

    # EDGE codes — Ultra's semantics: same-day codes plus CODE·Nd for fires 1..5 calendar days back
    import edge_replay
    grp, _ = edge_replay._frame(64, 3_000_000)
    emap, tset = {}, set(tickers)
    for tkr, g in grp.items():
        if tkr not in tset:
            continue
        ds = g["date"].astype(str).str[:10].to_numpy(); per = {}
        for code, col in edge_replay.DISPLAY_SETUPS + edge_replay.MINED_DISPLAY:
            if col in g:
                for d in ds[g[col].to_numpy(bool)]:
                    if d >= cutoff:
                        per.setdefault(d, []).append(code)
        per = {d: edge_replay._collapse_codes(c) for d, c in per.items()}
        dset = set(ds); acc = {d: list(c) for d, c in per.items()}
        for d2 in sorted(per):
            o2 = _date.fromisoformat(d2)
            for age in range(1, 6):
                tg = (o2 + _td(days=age)).isoformat()
                if tg in dset:
                    acc.setdefault(tg, []).extend(f"{c}·{age}d" for c in per[d2])
        for d, v in acc.items():
            emap[(tkr, d)] = v
    del grp
    log("edge map", len(emap))

    stores = {"lbal_signals": (lbal_store, [f for f in need if f.startswith("lbal_")]),
              "lvx_signals": (lvx_store, [f for f in need if f.startswith("lvx_")]),
              "ovdmap_signals": (ovdmap_store, [f for f in need if f.startswith("ovdmap_")]),
              "pv_multi_signals": (pv_multi_store, [f for f in need if f.startswith("pv_")]),
              "shapectx_signals": (shapectx_store, [f for f in need if f.startswith("shape_")]
                                   + ["shape_dir_up", "shape_dir_dn", "shape"]),
              "vol7_signals": (vol7_store, [f for f in need if f.startswith("vol7_")] + ["vol7_sg"]),
              "vol_echo_signals": (vol_echo_store, [f for f in need if f.startswith("ve_")])}
    results = []
    for ci in range(0, len(tickers), CHUNK):
        TL = tickers[ci:ci + CHUNK]
        con.execute("DROP TABLE IF EXISTS tk")
        con.execute("CREATE TEMP TABLE tk AS SELECT * FROM (VALUES " + ",".join(
            "('" + t.replace("'", "''") + "')" for t in TL) + ") v(ticker)")
        df = con.execute(f"""WITH r AS (SELECT {", ".join(f'b."{c}"' for c in sel)},
                row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                FROM bars b JOIN tk USING (ticker) WHERE b.date >= '{cutoff}' AND b.universe <> 'index')
            SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        for ui, dbc in ui_from_db.items():
            if ui != dbc:
                df[ui] = df[dbc]
        df["rsi"] = df["rsi_14"]
        df["tz_sig"] = df["t_sig"].where(df["t_sig"].fillna("") != "", df["z_sig"]).fillna("")
        ctx = df["context_tokens"].fillna("").astype(str)
        for k in ["lds", "ldc", "ldp", "lrc", "lrp", "wrc", "sqb", "bct"]:
            df[f"ctx_{k}"] = ctx.str.contains(rf"\b{k.upper()}\b", regex=True).astype(int)
        # ✦ FLY-fresh — the first FLY after a >15-bar absence (main.py rule, as in the research builder)
        fly = (df[["sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad"]].fillna(0).astype(int).sum(axis=1) > 0).to_numpy()
        ff = np.zeros(len(df), bool); tk = df["ticker"].to_numpy(); last = -99; prev = None; pos = 0
        for i in range(len(df)):
            if tk[i] != prev:
                last, prev, pos = -99, tk[i], 0
            if fly[i] and pos - last > 15:
                ff[i] = True
            if fly[i]:
                last = pos
            pos += 1
        df["fly_fresh"] = ff
        for name, (mod, keep) in stores.items():
            if not keep:
                continue
            recs = con.execute(f"SELECT s.* FROM '{os.path.join(DATA_DIR, name)}.parquet' s JOIN tk USING (ticker) "
                               f"WHERE s.date >= '{cutoff}'").fetchdf()
            if recs.empty:
                continue
            recs["date"] = pd.to_datetime(recs["date"]).dt.strftime("%Y-%m-%d")
            sf = pd.DataFrame([{k: u.get(k) for k in keep} | {"ticker": r["ticker"], "date": r["date"]}
                               for r in recs.to_dict("records") for u in [mod._to_ui(r)]])
            df = df.merge(sf.drop_duplicates(["ticker", "date"]), on=["ticker", "date"], how="left")
        an = con.execute(f"SELECT a.ticker, a.date, a.v anat_v, a.rs anat_rs FROM '{DATA_DIR}/anatomy_signals.parquet' a "
                         f"JOIN tk USING (ticker) WHERE a.date >= '{cutoff}'").fetchdf()
        an["date"] = pd.to_datetime(an["date"]).dt.strftime("%Y-%m-%d")
        df = df.merge(an.drop_duplicates(["ticker", "date"]), on=["ticker", "date"], how="left")
        # ▲4H / △1H REV-trigger days — the SAME rule and source the Superchart (api_bar_signals) and
        # Ultra (_annotate intraday) evaluate live: studio_4h / studio_1h bars, close ≥ 5,
        # min(rsi_14 over the 5 prior bars) < 38, 30 ≤ rsi_14 ≤ 55, close and rsi both up.
        # (data/mtf_rev_signals.parquet is a research reconstruction that is not rebuilt nightly.)
        for tfi, col in (("4h", "h4_rev_today"), ("1h", "h1_rev_today")):
            try:
                ic = duckdb.connect(db_path(f"studio_{tfi}.duckdb"), read_only=True)
                ic.register("tkv", pd.DataFrame({"ticker": TL}))
                rd = ic.execute(f"""WITH r AS (SELECT b.ticker, b.date, b.close, b.rsi_14,
                        MIN(b.rsi_14) OVER (PARTITION BY b.ticker ORDER BY b.date
                            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
                        LAG(b.close) OVER (PARTITION BY b.ticker ORDER BY b.date) cp,
                        LAG(b.rsi_14) OVER (PARTITION BY b.ticker ORDER BY b.date) rp
                      FROM bars b JOIN tkv USING (ticker)
                      WHERE b.close >= 5 AND b.date >= DATE '{cutoff}' - INTERVAL 10 DAY)
                    SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP), '%Y-%m-%d') date FROM r
                    WHERE m5 < 38 AND rsi_14 BETWEEN 30 AND 55 AND close > cp AND rsi_14 > rp""").fetchdf()
                ic.close()
                hit = set(zip(rd.ticker, rd.date))
            except Exception as exc:                       # an unreadable intraday DB → flags stay False
                log(f"  ⚠ {tfi} REV read failed: {exc}")
                hit = set()
            df[col] = [k in hit for k in zip(df.ticker, df.date)]
        for fam in ("pt5", "pt9"):
            pt = con.execute(f"SELECT p.ticker, p.date, p.pt_class FROM '{DATA_DIR}/{fam}_signals.parquet' p "
                             f"JOIN tk USING (ticker) WHERE p.date >= '{cutoff}'").fetchdf()
            pt["date"] = pd.to_datetime(pt["date"]).dt.strftime("%Y-%m-%d")
            pt[f"{fam}_strong"] = pt.pt_class.astype(str).str.endswith("_STRONG") & (pt.date <= "2026-08-20")
            df = df.merge(pt[["ticker", "date", f"{fam}_strong"]].drop_duplicates(["ticker", "date"]),
                          on=["ticker", "date"], how="left")
        df["pt5_strong"] = df["pt5_strong"].fillna(False).astype(bool)
        df["pt9_strong"] = df["pt9_strong"].fillna(False).astype(bool)
        df["pt_any_strong"] = df.pt5_strong | df.pt9_strong
        df["edges"] = [emap.get(k, []) for k in zip(df.ticker, df.date)]
        cols = list(dict.fromkeys(["ticker", "date", "open", "high", "low", "close"] + [f for f in need if f in df.columns]))
        rows_f = os.path.join(tmp, "rows.ndjson")
        df[cols].to_json(rows_f, orient="records", lines=True)
        with open(rows_f) as fh:
            p = subprocess.run(["node", "--max-old-space-size=6144", os.path.join(TR, "runner.mjs"), bundle,
                                "compute", str(KEEP)], stdin=fh, check=True, capture_output=True, text=True)
        for line in p.stdout.splitlines():
            if line:
                results.append(json.loads(line))
        log(f"chunk {ci // CHUNK + 1}: {len(TL)} tickers, {len(df):,} bars → {len(results):,} rows so far")
        del df

    out = pd.DataFrame({"ticker": [r["t"] for r in results], "date": [r["d"] for r in results],
                        "turn_cand": [bool(r["turn_cand"]) for r in results],
                        "turn_n": pd.array([r["turn_n"] for r in results], dtype="Int16")})
    for row in ROWS:
        out[f"rs_{row.lower()}"] = np.array([r["rs"][row] for r in results], dtype=np.int8)
    out["rs_pair"] = [bool(r["rs"]["pair"]) for r in results]
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    tmp_out = OUT + ".tmp"
    out.to_parquet(tmp_out, index=False)
    os.replace(tmp_out, OUT)
    meta = {"built_at": time.strftime("%Y-%m-%d %H:%M:%S"), "max_date": str(maxd)[:10], "cutoff": cutoff,
            "keep_sessions": KEEP, "tickers": int(out.ticker.nunique()), "rows": int(len(out)),
            "latest_date_rows": int((out.date == out.date.max()).sum()),
            "turn_cand_latest": int(out[out.date == out.date.max()].turn_cand.sum()),
            "sources": ["research_out/TURN_SET_V1.md", "research_out/ROWSEQ_V1.md"],
            "note": "DESCRIPTIVE turn-zone gauges; not a buy signal, never a score input."}
    json.dump(meta, open(META, "w"), indent=1)
    log("wrote", OUT, meta)


if __name__ == "__main__":
    main()
