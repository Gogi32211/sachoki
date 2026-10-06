"""V4 history — the Superchart V4 for EVERY ticker and EVERY bar, stored (user: "V4 mtlian DB bazashi", 2026-10-05).

What is stored per (ticker, date): the FIRED catalog keys (space-joined) plus the score with the weights of the
build day. The keys are the durable part — V4_WEIGHTS changes as the user edits it, so a reader should re-sum
weights over `v4_keys` (studio/v4_history_store.py does) instead of trusting the frozen `v4_score_build`.

Evaluation = the app's own code: V4_ALL_GROUPS + v4Fired + V4_WEIGHTS, bundled by esbuild (v4_history/entry.mjs),
over Superchart-shaped rows — the same row shape the Superchart V4 row and the CSV export score.

Row sources (one row per ticker/date, universe order as Ultra; same field map as turn_rowseq_build.py):
  bars (studio_analytics.duckdb) renamed to UI keys via turn_rowseq/field_sources.json + raw l_sig/close/…
  + the 7 display stores through each store's own _to_ui()  + ▽△ anatomy (anatomy_signals.parquet)
  + PT5/PT9 strong (≤ 2026-08-20 cutoff) + ✦ FLY-fresh + ctx_* tokens
  + EDGE codes exactly as the Superchart attaches them: edge_replay (60, 3M) frame, DISPLAY_SETUPS, same day
  + Superchart-only live fields, ported from main.py api_bar_signals with the same rules:
      rev_buy / brk_buy (zones), h4/h1_rev_today, mtf_echo, turn_echo_n (4h/1h/15m REV sets),
      mtf_score_conf (intraday vetoed buy_score ≥ 60), buy_score, div_buy (frame E_rsidiv_rs), l34_grade,
      seq_ctx / seq_ens (buyseq_context lookup over bars i-3..i)
NOT reproducible historically (live-only, never stored per bar) — listed in the meta file:
  rs_strong, rgti_*, mtf_ema_ages (mtf_up…), seq34 (x_seq34_gold).

Output: data/v4_signals.parquet (ticker, date, v4_keys, v4_n, v4_score_build) + data/V4_SIGNALS_V1.json.
  python v4_history_build.py            full history, all tickers (resumable: per-chunk parts in data/_v4_parts/)
  python v4_history_build.py --since 2026-09-01   rebuild only bars on/after a date (merged into the store)
  python v4_history_build.py --since <D> --nightly   the same, from update_all.sh (skips the 02:45-06:00 guard,
                                                     which exists only to keep MANUAL runs off the nightly DB write)
"""
from __future__ import annotations
import glob, hashlib, json, os, subprocess, sys, tempfile, time
from datetime import date as _date

import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
os.chdir(HERE)
from studio.paths import db_path, DATA_DIR                      # noqa: E402
from studio import (lbal_store, lvx_store, ovdmap_store, pv_multi_store, shapectx_store,  # noqa: E402
                    vol7_store, vol_echo_store)

ROOT = os.path.dirname(HERE)
VH = os.path.join(HERE, "v4_history")
TR = os.path.join(HERE, "turn_rowseq")
OUT = os.path.join(DATA_DIR, "v4_signals.parquet")
META = os.path.join(DATA_DIR, "V4_SIGNALS_V1.json")
PARTS = os.path.join(DATA_DIR, "_v4_parts")
CHUNK = 60
WARMUP_DAYS = 120          # extra calendar days loaded before --since so rolling rules see their history
t0 = time.time()
log = lambda *a: print(f"[{time.time()-t0:7.0f}s]", *a, flush=True)
NOT_REPRODUCIBLE = ["rs_strong", "rgti_upup", "rgti_upupup", "rgti_green", "rgti_greencirc",
                    "mtf_up", "mtf_upup", "mtf_upupup", "x_seq34_gold"]


def _bundle(tmp: str) -> str:
    out = os.path.join(tmp, "bundle.mjs")
    esb = os.path.join(ROOT, "frontend", "node_modules", ".bin", "esbuild")
    subprocess.run([esb, os.path.join(VH, "entry.mjs"), "--bundle", "--format=esm", "--platform=node",
                    f"--outfile={out}", "--log-level=error", '--define:import.meta.env={"VITE_API_URL":""}'],
                   check=True, cwd=ROOT)
    return out


# ── Superchart-only fields, ported from main.py api_bar_signals (same rules, per ticker, causal) ──────────
def _rev_sets(tickers: list[str], since: str | None) -> dict:
    """{tf: {ticker: set(date)}} — strict REV-turn days on 4h/1h/15m (main.py ~6043: close>=5 series,
    min of the 5 PRIOR rsi (min_periods 2) < 38, 30<=rsi<=55, close up, rsi up)."""
    out = {}
    lo = (pd.Timestamp(since) - pd.Timedelta(days=10)).strftime("%Y-%m-%d") if since else None
    for tf in ("4h", "1h", "15m"):
        sets: dict = {}
        try:
            c = duckdb.connect(db_path(f"studio_{tf}.duckdb"), read_only=True)
            c.register("tkv", pd.DataFrame({"ticker": tickers}))
            w = f" AND b.date >= '{lo}'" if lo else ""
            df = c.execute(f"SELECT b.ticker, b.date, b.close, b.rsi_14 FROM bars b JOIN tkv USING (ticker) "
                           f"WHERE b.close >= 5{w} ORDER BY b.ticker, b.date").fetchdf()
            c.close()
            g = df.groupby("ticker", sort=False)
            m5 = g["rsi_14"].transform(lambda s: s.rolling(5, min_periods=2).min().shift(1))
            up = df["close"] > g["close"].shift(1)
            ri = df["rsi_14"] > g["rsi_14"].shift(1)
            mm = (m5 < 38) & (df.rsi_14 >= 30) & (df.rsi_14 <= 55) & up & ri & m5.notna() & df.rsi_14.notna()
            hit = df[mm]
            for tk, d in zip(hit.ticker, hit.date.astype(str).str[:10]):
                sets.setdefault(tk, set()).add(d)
        except Exception as exc:
            log(f"  ⚠ {tf} REV read failed: {exc}")
        out[tf] = sets
    return out


def _conf_sets(tickers: list[str], since: str | None) -> dict:
    """{tf: {ticker: set(date)}} — intraday vetoed buy_score >= 60 (main.py ~6096)."""
    from prebreak_v2 import prebreak_v2_score_sql
    BS = ("LEAST(GREATEST((1.5*LEAST(GREATEST(COALESCE({v2},0),0),27)"
          " + 12*(CASE WHEN upper(COALESCE(vol_bucket,''))='B' THEN 1 ELSE 0 END)"
          " + 0.9*GREATEST(0, 55-COALESCE(rsi_14,50)))*1.3, 0), 100)")
    veto = lambda v2: (f"CASE WHEN rsi_14>=60 THEN LEAST({BS.format(v2=v2)},20) "
                       f"WHEN rsi_14<28 THEN LEAST({BS.format(v2=v2)},60) ELSE {BS.format(v2=v2)} END")
    lo = (pd.Timestamp(since) - pd.Timedelta(days=10)).strftime("%Y-%m-%d") if since else None
    out = {}
    for tf, v2c in (("4h", "prebreak_v2"), ("1h", "prebreak_v2"), ("15m", f"({prebreak_v2_score_sql()})")):
        sets: dict = {}
        try:
            c = duckdb.connect(db_path(f"studio_{tf}.duckdb"), read_only=True)
            c.register("tkv", pd.DataFrame({"ticker": tickers}))
            w = f" AND date >= '{lo}'" if lo else ""
            df = c.execute(f"SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM bars "
                           f"WHERE ticker IN (SELECT ticker FROM tkv) AND close >= 5{w} AND ({veto(v2c)}) >= 60").fetchdf()
            c.close()
            for tk, d in zip(df.ticker, df.d):
                sets.setdefault(tk, set()).add(d)
        except Exception as exc:
            log(f"  ⚠ {tf} conf read failed: {exc}")
        out[tf] = sets
    return out


def _superchart_live(df: pd.DataFrame, rev: dict, conf: dict, divs: dict) -> pd.DataFrame:
    """rev_buy/brk_buy, h4/h1_rev_today, mtf_echo, turn_echo_n, mtf_score_conf, div_buy, l34_grade — per ticker,
    in bar order, exactly as api_bar_signals assigns them on the Superchart's full-history fetch."""
    n = len(df)
    rev_buy = np.zeros(n, bool); brk_buy = np.zeros(n, bool)
    h4 = np.zeros(n, bool); h1 = np.zeros(n, bool)
    mtf_echo = np.full(n, None, object); turn_n = np.full(n, None, object); mconf = np.full(n, None, object)
    div_buy = np.zeros(n, bool); l34g = np.full(n, None, object)
    tk = df["ticker"].to_numpy(); ds = df["date"].to_numpy()
    rsi = pd.to_numeric(df["rsi_14"], errors="coerce").fillna(0).to_numpy(float)
    rsi50 = pd.to_numeric(df["rsi_14"], errors="coerce").fillna(50).to_numpy(float)
    cl = pd.to_numeric(df["close"], errors="coerce").fillna(0).to_numpy(float)
    op = pd.to_numeric(df["open"], errors="coerce").fillna(0).to_numpy(float)
    lo = pd.to_numeric(df["low"], errors="coerce").fillna(0).to_numpy(float)
    beta = pd.to_numeric(df.get("beta_score", 0), errors="coerce").fillna(0).to_numpy(float) if "beta_score" in df else np.zeros(n)
    turbo = pd.to_numeric(df.get("turbo_score", 0), errors="coerce").fillna(0).to_numpy(float) if "turbo_score" in df else np.zeros(n)
    bs = pd.to_numeric(df["buy_score"], errors="coerce").to_numpy(float) if "buy_score" in df else np.full(n, np.nan)
    isl = (df["l_sig"].fillna("").astype(str) == "L34").to_numpy()
    starts = np.r_[0, np.flatnonzero(tk[1:] != tk[:-1]) + 1, n]
    for a, b in zip(starts[:-1], starts[1:]):
        t = tk[a]
        r4, r1, r15 = rev["4h"].get(t, set()), rev["1h"].get(t, set()), rev["15m"].get(t, set())
        rev_days = r4 | r1
        c4, c1, c15 = conf["4h"].get(t, set()), conf["1h"].get(t, set()), conf["15m"].get(t, set())
        dv = divs.get(t, set())
        for i in range(a, b):
            j = i - a
            d0 = ds[i]; d1 = ds[i - 1] if j > 0 else ""
            if j >= 1 and cl[i] > 0 and cl[i - 1] > 0:
                m5 = rsi[max(a, i - 4):i + 1].min()
                up = cl[i] > cl[i - 1]
                if m5 < 38 and 30 <= rsi[i] <= 55 and up and beta[i] <= 13:
                    rev_buy[i] = True
                if rsi[i - 1] < 50 <= rsi[i] and up and turbo[i] <= 28:
                    brk_buy[i] = True
            h4[i] = d0 in r4; h1[i] = d0 in r1
            if rev_buy[i] and rev_days:
                mtf_echo[i] = (d0 in rev_days) or (d1 in rev_days)
            if j > 0 and cl[i] > cl[i - 1] and rsi[i] > rsi[i - 1] and rsi[i] < 55:
                k = sum(1 for s in (r4, r1, r15) if (d0 in s) or (d1 in s))
                if k > 0:
                    turn_n[i] = k
            if bs[i] == bs[i] and bs[i] >= 60:
                mconf[i] = sum(1 for s in (c4, c1, c15) if (d0 in s) or (d1 in s))
            div_buy[i] = d0 in dv
            if isl[i] and cl[i] < op[i]:
                g = 0
                lo25 = lo[max(a, i - 25):i]
                lo25 = lo25.min() if len(lo25) else 0
                if lo25 > 0 and cl[i] / lo25 - 1 <= 0.10:
                    g += 1
                if rsi50[i] < 40:
                    g += 1
                for k2 in range(max(a, i - 20), i):
                    if isl[k2] and cl[k2] < op[k2] and cl[k2] > 0 and abs(cl[i] / cl[k2] - 1) <= 0.05:
                        g += 1
                        break
                l34g[i] = g
    df["rev_buy"], df["brk_buy"] = rev_buy, brk_buy
    df["h4_rev_today"], df["h1_rev_today"] = h4, h1
    df["mtf_echo"], df["turn_echo_n"], df["mtf_score_conf"] = mtf_echo, turn_n, mconf
    df["div_buy"], df["l34_grade"] = div_buy, l34g
    return df


def _seq_ctx(df: pd.DataFrame) -> pd.DataFrame:
    """⤴/⤵ preceding-sequence context + ENS, as api_bar_signals (main.py ~6141): layers of bars i-3..i
    (coarse/fine T/Z+L tokens and sx/bw/gr/q5/vb), the bar's signal list, buyseq_context.lookup/_ensemble."""
    from buyseq_context import make_tokens, lookup, lookup_ensemble
    n = len(df)
    tk = df["ticker"].to_numpy()
    zs = df["z_sig"].fillna("").astype(str).to_numpy(); ts = df["t_sig"].fillna("").astype(str).to_numpy()
    ls = df["l_sig"].fillna("").astype(str).to_numpy()
    lyr = {k: df[c].fillna("").astype(str).to_numpy() for k, c in
           (("sx", "close_suffix"), ("bw", "bar_body_wick"), ("gr", "bar_gap_range"), ("q5", "bar_line5"), ("vb", "vol_bucket"))}
    cts, fts = [], []
    for i in range(n):
        c, f = make_tokens(zs[i], ts[i], ls[i]); cts.append(c); fts.append(f)
    rev = df["rev_buy"].to_numpy(); brk = df["brk_buy"].to_numpy(); h4 = df["h4_rev_today"].to_numpy()
    h1 = df["h1_rev_today"].to_numpy(); mconf = df["mtf_score_conf"].to_numpy(); ten = df["turn_echo_n"].to_numpy()
    ed = df["edges"].to_numpy(); ff = df["fly_fresh"].to_numpy()
    fly = (df[["sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad"]].fillna(0).astype(int).sum(axis=1) > 0).to_numpy()
    cl = pd.to_numeric(df["close"], errors="coerce").fillna(0).to_numpy(float)
    op = pd.to_numeric(df["open"], errors="coerce").fillna(0).to_numpy(float)
    ctx = np.full(n, None, object); ens = np.full(n, None, object)
    starts = np.r_[0, np.flatnonzero(tk[1:] != tk[:-1]) + 1, n]
    for a, b in zip(starts[:-1], starts[1:]):
        for i in range(a + 1, b):
            sigs = []
            if rev[i]: sigs.append("rev")
            if brk[i]: sigs.append("brk")
            if h4[i]: sigs.append("h4")
            if ls[i] == "L34" and cl[i] < op[i]: sigs.append("lh")
            if len(ed[i]): sigs.append("ea")
            if fly[i]: sigs.append("fly")
            if ff[i]: sigs.append("flyf")
            if mconf[i] is not None: sigs.append("score")
            if ten[i]: sigs.append("turn")
            if h1[i]: sigs.append("h1")
            sigs.append("anyb")
            lo = max(a, i - 3)
            layers = {"c": cts[lo:i + 1], "f": fts[lo:i + 1]}
            for k in ("sx", "bw", "gr", "q5", "vb"):
                layers[k] = list(lyr[k][lo:i + 1])
            hit = lookup(layers, sigs)
            if hit:
                ctx[i] = hit
            e = lookup_ensemble(layers, sigs)
            if e:
                ens[i] = e
    df["seq_ctx"] = ctx; df["seq_ens"] = ens
    return df


def main() -> None:
    since = None
    if "--since" in sys.argv:
        since = sys.argv[sys.argv.index("--since") + 1]
    os.makedirs(PARTS, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="v4hist_")
    bundle = _bundle(tmp)
    cat = json.loads(subprocess.run(["node", os.path.join(VH, "runner.mjs"), bundle, "catalog"],
                                    check=True, capture_output=True, text=True).stdout)
    deps = json.loads(subprocess.run(["node", os.path.join(TR, "runner.mjs"), bundle, "deps"],
                                     check=True, capture_output=True, text=True).stdout)
    weights = {k: v for k, v in cat["weights"].items()}
    w_hash = hashlib.sha256(json.dumps(weights, sort_keys=True).encode()).hexdigest()[:12]
    srcs = json.load(open(os.path.join(TR, "field_sources.json")))
    need = sorted({f for v in deps.values() for f in v})
    log("catalog", len(cat["keys"]), "keys · weights hash", w_hash, "· fields", len(need))

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
            "profile_category", "buy_score", "l_sig", "t_sig", "z_sig", "bar_body_wick", "beta_score", "turbo_score",
            "close_suffix", "bar_gap_range", "bar_line5", "vol_bucket",
            "sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad"]
    sel = [c for c in sorted(set(base) | set(ui_from_db.values())) if c in bars_cols]
    maxd = con.execute("SELECT max(date) FROM bars").fetchone()[0]
    load_from = (pd.Timestamp(since) - pd.Timedelta(days=WARMUP_DAYS)).strftime("%Y-%m-%d") if since else "1900-01-01"
    tickers = [r[0] for r in con.execute(f"SELECT DISTINCT ticker FROM bars WHERE universe <> 'index' "
                                         f"AND date >= '{since or '1900-01-01'}' ORDER BY 1").fetchall()]
    test = None
    if "--tickers" in sys.argv:                       # parity test: listed tickers only, never touches the store
        test = [t.strip().upper() for t in sys.argv[sys.argv.index("--tickers") + 1].split(",") if t.strip()]
        tickers = [t for t in tickers if t in set(test)]
    log(f"bars → {maxd}: {len(tickers)} tickers, {len(sel)} columns, since={since}")

    # EDGE codes + 📐 div_buy, exactly as the Superchart attaches them: (60, 3M) frame, same-day DISPLAY_SETUPS
    import edge_replay
    grp, _ = edge_replay._frame(60, 3_000_000)
    emap, divs = {}, {}
    for tkr, g in grp.items():
        for d, codes in edge_replay._edges_from_group(g).items():
            emap[(tkr, d)] = codes
        if "E_rsidiv_rs" in g:
            dd = g["date"].astype(str).str[:10].to_numpy()[g["E_rsidiv_rs"].to_numpy(bool)]
            divs[tkr] = set(dd)
    del grp
    edge_replay._CACHE.clear()                     # the frame is only needed for these two maps — free it
    import gc; gc.collect()
    log("edge map", len(emap), "· div tickers", len(divs))

    stores = {"lbal_signals": (lbal_store, [f for f in need if f.startswith("lbal_")]),
              "lvx_signals": (lvx_store, [f for f in need if f.startswith("lvx_")]),
              "ovdmap_signals": (ovdmap_store, [f for f in need if f.startswith("ovdmap_")]),
              "pv_multi_signals": (pv_multi_store, [f for f in need if f.startswith("pv_")]),
              "shapectx_signals": (shapectx_store, [f for f in need if f.startswith("shape_")]
                                   + ["shape_dir_up", "shape_dir_dn", "shape"]),
              "vol7_signals": (vol7_store, [f for f in need if f.startswith("vol7_")] + ["vol7_sg", "vol7_mr"]),
              "vol_echo_signals": (vol_echo_store, [f for f in need if f.startswith("ve_")])}
    tag = ("test_" if test else "") + (since or "full")
    con.close()
    for ci in range(0, len(tickers), CHUNK):
        part = os.path.join(PARTS, f"{tag}_{ci // CHUNK:04d}.parquet")
        if os.path.exists(part):
            continue
        # never hold the DB files during the nightly update (03:00 Tbilisi) — stop cleanly; parts resume
        _tb = pd.Timestamp.now(tz="Asia/Tbilisi")
        if (_tb.hour, _tb.minute) >= (2, 45) and _tb.hour < 6 and not test and "--nightly" not in sys.argv:
            log("⏸ 02:45-06:00 Tbilisi — nightly DB update window; stopping. Re-run to resume from chunk", ci // CHUNK + 1)
            return
        con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
        TL = tickers[ci:ci + CHUNK]
        con.execute("DROP TABLE IF EXISTS tk")
        con.register("tkdf", pd.DataFrame({"ticker": TL}))
        con.execute("CREATE TEMP TABLE tk AS SELECT * FROM tkdf")
        df = con.execute(f"""WITH r AS (SELECT {", ".join(f'b."{c}"' for c in sel)},
                row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                FROM bars b JOIN tk USING (ticker) WHERE b.date >= '{load_from}' AND b.universe <> 'index')
            SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
        if df.empty:
            pd.DataFrame(columns=["ticker", "date", "v4_keys", "v4_n", "v4_score_build"]).to_parquet(part, index=False)
            con.close()
            continue
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        for ui, dbc in ui_from_db.items():
            if ui != dbc:
                df[ui] = df[dbc]
        df["rsi"] = df["rsi_14"]
        df["tz_sig"] = df["t_sig"].where(df["t_sig"].fillna("") != "", df["z_sig"]).fillna("")
        # ctx_* and pt*_strong are NOT attached to Superchart bars (api_bar_signals) — left out on purpose so the
        # stored V4 equals the Superchart's (AAPL parity check 2026-10-05: they fired only in the builder).
        fly = (df[["sig_fly_abcd", "sig_fly_cd", "sig_fly_bd", "sig_fly_ad"]].fillna(0).astype(int).sum(axis=1) > 0).to_numpy()
        ff = np.zeros(len(df), bool); tkr = df["ticker"].to_numpy(); last = -99; prev = None; pos = 0
        for i in range(len(df)):
            if tkr[i] != prev:
                last, prev, pos = -99, tkr[i], 0
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
                               f"WHERE s.date >= '{load_from}'").fetchdf()
            if recs.empty:
                continue
            recs["date"] = pd.to_datetime(recs["date"]).dt.strftime("%Y-%m-%d")
            sf = pd.DataFrame([{k: u.get(k) for k in keep} | {"ticker": r["ticker"], "date": r["date"]}
                               for r in recs.to_dict("records") for u in [mod._to_ui(r)]])
            df = df.merge(sf.drop_duplicates(["ticker", "date"]), on=["ticker", "date"], how="left")
        an = con.execute(f"SELECT a.ticker, a.date, a.v anat_v, a.s anat_s, a.rs anat_rs FROM "
                         f"'{DATA_DIR}/anatomy_signals.parquet' a JOIN tk USING (ticker) WHERE a.date >= '{load_from}'").fetchdf()
        an["date"] = pd.to_datetime(an["date"]).dt.strftime("%Y-%m-%d")
        an["anat_v"] = an["anat_v"].replace("", None)
        df = df.merge(an.drop_duplicates(["ticker", "date"]), on=["ticker", "date"], how="left")
        df["edges"] = [emap.get(k, []) for k in zip(df.ticker, df.date)]
        df = _superchart_live(df, _rev_sets(TL, since and load_from), _conf_sets(TL, since and load_from), divs)
        df = _seq_ctx(df)
        if since:
            df = df[df.date >= since]
        extra = ["rev_buy", "brk_buy", "h4_rev_today", "h1_rev_today", "mtf_echo", "turn_echo_n",
                 "mtf_score_conf", "div_buy", "l34_grade", "buy_score", "anat_s", "seq_ctx", "seq_ens"]
        cols = list(dict.fromkeys(["ticker", "date", "open", "high", "low", "close", "l_sig", "bar_body_wick"]
                                  + [f for f in need if f in df.columns] + [c for c in extra if c in df.columns]))
        rows_f = os.path.join(tmp, "rows.ndjson")
        df[cols].to_json(rows_f, orient="records", lines=True)
        with open(rows_f) as fh:
            p = subprocess.run(["node", "--max-old-space-size=8192", os.path.join(VH, "runner.mjs"), bundle, "fire"],
                               stdin=fh, check=True, capture_output=True, text=True)
        recs = [json.loads(x) for x in p.stdout.splitlines() if x]
        out = pd.DataFrame({"ticker": [r["t"] for r in recs], "date": [r["d"] for r in recs],
                            "v4_keys": [r["k"] for r in recs],
                            "v4_n": np.array([len(r["k"].split()) if r["k"] else 0 for r in recs], dtype=np.int16),
                            "v4_score_build": np.array([r["s"] for r in recs], dtype=np.int32)})
        out.to_parquet(part + ".tmp", index=False); os.replace(part + ".tmp", part)
        log(f"chunk {ci // CHUNK + 1}/{(len(tickers) + CHUNK - 1) // CHUNK}: {len(TL)} tickers, {len(out):,} bars")
        del df
        con.close()

    parts = sorted(glob.glob(os.path.join(PARTS, f"{tag}_*.parquet")))
    new = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    if test:
        tout = os.path.join(DATA_DIR, "_v4_test.parquet")
        new.to_parquet(tout, index=False)
        for p in parts:
            os.remove(p)
        log("TEST wrote", tout, len(new), "rows")
        return
    if since and os.path.exists(OUT):
        old = pd.read_parquet(OUT)
        new = pd.concat([old[old.date < since], new], ignore_index=True)
    new = new.sort_values(["ticker", "date"]).drop_duplicates(["ticker", "date"], keep="last").reset_index(drop=True)
    new.to_parquet(OUT + ".tmp", index=False)
    os.replace(OUT + ".tmp", OUT)
    for p in parts:
        os.remove(p)
    meta = {"built_at": time.strftime("%Y-%m-%d %H:%M:%S"), "max_date": str(maxd)[:10], "since": since,
            "rows": int(len(new)), "tickers": int(new.ticker.nunique()),
            "first_date": str(new.date.min()), "last_date": str(new.date.max()),
            "weights_hash_at_build": w_hash, "catalog_keys": len(cat["keys"]),
            "not_reproducible_keys": NOT_REPRODUCIBLE,
            "row_shape": "Superchart (api_bar_signals) — same as the Superchart V4 row and the CSV export",
            "note": "Re-sum V4_WEIGHTS over v4_keys to get the score for the CURRENT weights; v4_score_build is frozen."}
    json.dump(meta, open(META, "w"), indent=1)
    log("wrote", OUT, meta)


if __name__ == "__main__":
    main()
