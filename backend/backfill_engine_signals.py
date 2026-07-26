"""
backfill_engine_signals.py — one-shot historical backfill of the 19 engine-only
signal columns (2026-07-21, "ertxel da samudamod"): um_2809, ev_l22/l43/l64/l34,
bo_dn/bx_dn/be_dn, buy_here, atr_brk, bb_brk, rtv, svs_raw, cons_atr, gog1-3,
setup_tokens, context_tokens.

Runs on a STAGING COPY of studio_analytics.duckdb (live app keeps serving), then
atomically swaps: bootout backend → replace file → bootstrap. Resume-safe via
/tmp/backfill_engsig_done.txt. ~2s/ticker × ~8.7k tickers ≈ 5-6h.
"""
import os, shutil, subprocess, sys, time, logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("engsig")

BASE = os.path.dirname(os.path.abspath(__file__))
LIVE = os.path.join(BASE, "..", "data", "studio_analytics.duckdb")
STAGE = os.path.join(BASE, "..", "data", "studio_analytics.engsig_staging.duckdb")
DONE = "/tmp/backfill_engsig_done.txt"
COLS = ["um_2809","ev_l22","ev_l43","ev_l64","ev_l34","bo_dn","bx_dn","be_dn","buy_here",
        "atr_brk","bb_brk","rtv","svs_raw","cons_atr","gog1","gog2","gog3",
        "setup_tokens","context_tokens"]

def _wait_out_nightly_window():
    """Never swap 01:00-02:30 Tbilisi — the nightly delta worker owns that window."""
    while True:
        h, m = time.localtime().tm_hour, time.localtime().tm_min
        if not (h == 1 or (h == 2 and m < 35) or (h == 0 and m >= 55)):
            return
        log.info("nightly window — waiting 10 min…")
        time.sleep(600)


def _catch_up_from_live(con):
    """Copy rows the nightly wrote to LIVE after our staging snapshot was taken."""
    con.execute(f"ATTACH '{LIVE}' AS live (READ_ONLY)")
    n = con.execute("""INSERT INTO bars SELECT l.* FROM live.bars l
                       LEFT JOIN bars s ON s.ticker = l.ticker AND s.date = l.date
                       WHERE s.ticker IS NULL""").fetchone()
    log.info("catch-up rows inserted from live: %s", n)
    con.execute("DETACH live")


def main(limit: int = 0):
    import duckdb, pandas as pd
    from main import api_bar_signals
    if not os.path.exists(STAGE):
        log.info("copying live → staging (%.1f GB)…", os.path.getsize(LIVE)/1e9)
        shutil.copy(LIVE, STAGE)
    done = set()
    if os.path.exists(DONE):
        done = set(open(DONE).read().split())
    con = duckdb.connect(STAGE)
    tickers = [r[0] for r in con.execute("SELECT DISTINCT ticker FROM bars ORDER BY ticker").fetchall()]
    if limit:
        tickers = [t for t in tickers if t not in done][:limit]
    log.info("tickers %d (done %d)", len(tickers), len(done))
    t0 = time.time(); processed = 0
    for tk in tickers:
        if tk in done:
            continue
        try:
            bars = api_bar_signals(tk, tf="1d", bars=1650, universe="sp500")
        except Exception as e:
            log.warning("%s fetch failed: %s", tk, e)
            open(DONE, "a").write(tk + "\n")
            continue
        rows = []
        for b in bars:
            _su = b.get("setup"); _cx = b.get("context")
            rows.append({
                "date": str(b.get("date"))[:10],
                "um_2809": int(b.get("raw_um") or 0), "ev_l22": int(b.get("raw_l22") or 0),
                "ev_l43": int(b.get("raw_l43") or 0), "ev_l64": int(b.get("raw_l64") or 0),
                "ev_l34": int(b.get("raw_l34") or 0), "bo_dn": int(b.get("sig_bo_dn") or 0),
                "bx_dn": int(b.get("sig_bx_dn") or 0), "be_dn": int(b.get("sig_be_dn") or 0),
                "buy_here": int(b.get("raw_buy_here") or 0), "atr_brk": int(b.get("raw_atr_brk") or 0),
                "bb_brk": int(b.get("raw_bb_brk") or 0), "rtv": int(b.get("raw_rtv") or 0),
                "svs_raw": int(b.get("raw_svs_raw") or 0), "cons_atr": int(b.get("raw_cons") or 0),
                "gog1": int(b.get("gog1") or 0), "gog2": int(b.get("gog2") or 0),
                "gog3": int(b.get("gog3") or 0),
                "setup_tokens": " ".join(_su) if isinstance(_su, list) else str(_su or ""),
                "context_tokens": " ".join(_cx) if isinstance(_cx, list) else str(_cx or ""),
            })
        if rows:
            df = pd.DataFrame(rows)
            con.register("t_", df)
            sets = ", ".join(f"{c2} = t_.{c2}" for c2 in COLS)
            con.execute(f"""UPDATE bars SET {sets} FROM t_
                            WHERE bars.ticker = ?
                              AND bars.date = CAST(t_.date AS DATE)""".replace("?", "$tk"),
                        {"tk": tk})
            con.unregister("t_")
        open(DONE, "a").write(tk + "\n")
        processed += 1
        if processed % 100 == 0:
            con.commit()
            el = time.time() - t0
            log.info("%d done · %.1f s/ticker · ETA %.1f h",
                     processed, el/processed, (len(tickers)-len(done)-processed)*el/processed/3600)
    con.commit()
    if limit:
        con.close()
        log.info("limit run done — NO swap (staging kept for inspection)")
        return
    log.info("backfill COMPLETE — catch-up + swap…")
    _wait_out_nightly_window()
    _catch_up_from_live(con)
    con.close()
    subprocess.run(f"launchctl bootout gui/{os.getuid()}/com.sachoki.backend", shell=True)
    time.sleep(3)
    bak = LIVE + ".pre_engsig"
    if os.path.exists(bak):
        os.remove(bak)
    os.rename(LIVE, bak)
    os.rename(STAGE, LIVE)
    subprocess.run(f"launchctl bootstrap gui/{os.getuid()} "
                   f"{os.path.expanduser('~/Library/LaunchAgents/com.sachoki.backend.plist')}", shell=True)
    log.info("swap done — old DB kept at %s", bak)

if __name__ == "__main__":
    _lim = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 0
    main(_lim)
