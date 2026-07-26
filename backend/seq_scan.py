"""
seq_scan.py — live scanner for the TIME-ROBUST 5-year sequence rule-database
(seq_rules.json, built by seq_analytics.py). For recent bars it builds the per-bar
token, forms the trailing 2/3/4-bar sequences, and surfaces the ones whose pattern
matches a ROBUST rule (STABLE, ≥5/6yr-ish, not 2025-concentrated, positive 2026 where
known). Entry = next open after the matched (last) bar. READ-ONLY on bars.
"""
from __future__ import annotations
import json
import os
import pandas as pd
from seq_analytics import build_token, _ULTRA_TOK, ALL_SIG

_RULES_PATH = os.path.join(os.path.dirname(__file__), "seq_rules.json")


def _load_rules(tf: str = "1d") -> dict:
    """1d prefers seq_rules_v2.json (frozen-OOS re-validation, 2026-07-06): rules mined
    on 2021-23 ONLY and verified on 2024-26 with stop-first path-sim — only the
    OOS_VERIFIED tier is served (969 rules; 14 also DSR≥0.6 = selection-proof).
    Mapped into the scanner's rule shape (+tier/dsr/ps fields). Falls back to the
    legacy in-sample DB if v2 is missing. Intraday tfs keep their legacy files."""
    base = os.path.dirname(__file__)
    if tf == "1d":
        try:
            with open(os.path.join(base, "seq_rules_v2.json")) as f:
                v2 = json.load(f)
            out = {}
            for seq, r in v2.get("rules", {}).items():
                if r.get("tier") != "OOS_VERIFIED":
                    continue
                ty = r.get("test_yrs", {})
                out[seq] = {
                    "depth": r["depth"],
                    "n": int(r.get("n_train", 0)) + int(r.get("n_test", 0)),
                    "med20": r.get("med20_test"),          # OOS fwd median (display)
                    "ps_med": r.get("ps_med_test"),        # OOS path-sim median (tradeable)
                    "win": r.get("ps_win_test"),           # OOS path-sim win%
                    "fail": None,
                    "pos_yrs": int(sum(1 for v in ty.values() if v > 0)),
                    "n_yrs": len(ty),
                    "y2026": ty.get("2026"),
                    "yrs_detail": ty,
                    "tier": "OOS✓", "dsr": r.get("dsr"),
                    "med20_train": r.get("med20_train"),
                }
            if out:
                return out
        except Exception:
            pass
    fn = "seq_rules.json" if tf == "1d" else f"seq_rules_{tf}.json"
    try:
        with open(os.path.join(base, fn)) as f:
            return json.load(f)
    except Exception:
        return {}


def seq_scan(max_age_days: int = 4, dv_floor: float | None = None, limit: int = 150, tf: str = "1d") -> dict:
    from seq_analytics import _conn, _TF_DVFLOOR
    rules = _load_rules(tf)
    if not rules:
        return {"rows": [], "count": 0, "error": f"seq_rules{'' if tf=='1d' else '_'+tf}.json missing — run seq_analytics.py {tf}"}
    if dv_floor is None:
        dv_floor = _TF_DVFLOOR.get(tf, 3_000_000)
    win_days = 18 if tf == "1d" else (5 if tf == "4h" else 4)
    a = _conn(tf)
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        sigsel = ", ".join(f"coalesce(sig_{s},0) {s}" for s in _ULTRA_TOK if s in set(ALL_SIG) or True)
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>={dv_floor}
                         AND date >= DATE '{as_of}' - INTERVAL {win_days} DAY)
            SELECT universe, ticker, date, close, close*volume dv, rsi_14,
                   coalesce(z_sig,'') z_sig, coalesce(t_sig,'') t_sig, coalesce(l_sig,'') l_sig,
                   CASE WHEN sig_p89=1 THEN 'P89' WHEN sig_p66=1 THEN 'P66' WHEN sig_p55=1 THEN 'P55'
                        WHEN sig_p50=1 THEN 'P50' WHEN sig_p3=1 THEN 'P3' WHEN sig_p2=1 THEN 'P2'
                        ELSE '' END p_which,
                   CASE WHEN sig_d89=1 THEN 'D89' WHEN sig_d66=1 THEN 'D66' WHEN sig_d55=1 THEN 'D55'
                        WHEN sig_d50=1 THEN 'D50' WHEN sig_d3=1 THEN 'D3' WHEN sig_d2=1 THEN 'D2'
                        ELSE '' END d_which,
                   {sigsel}
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    if df.empty:
        return {"rows": [], "count": 0, "as_of": as_of}
    build_token(df)
    g = df.groupby("ticker")
    df["k1"] = g["tok"].shift(1); df["k2"] = g["tok"].shift(2); df["k3"] = g["tok"].shift(3)
    # specific P/D presence on prior window bars (validated booster: +P broad +0.40pp/62%,
    # +D strong on oversold-reversal seqs +5pp). Show the strongest specific type (P55/D66/…).
    for k in (1, 2, 3):
        df[f"p_which{k}"] = g["p_which"].shift(k); df[f"d_which{k}"] = g["d_which"].shift(k)
    # only evaluate recent bars, counting freshness in TRADING days (not calendar):
    # rank distinct trading days latest-first (as_of day = 0), so a weekend/holiday
    # doesn't silently "waste" a freshness step. max_age_days=1 → latest day only,
    # =2 → latest two trading days, etc.
    _day = df["date"].astype(str).str[:10]
    _rank = {d: i for i, d in enumerate(sorted(_day.unique(), reverse=True))}
    df["age"] = _day.map(_rank)                     # 0 = most recent trading day
    recent = df[df["age"] <= max_age_days - 1].copy()

    out = []
    for _, r in recent.iterrows():
        # try 4,3,2-bar (prefer the deepest match for specificity)
        for depth in (4, 3, 2):
            toks = [r["k3"], r["k2"], r["k1"], r["tok"]][4 - depth:]
            if any(pd.isna(x) for x in toks):
                continue
            seq = " ".join(str(x) for x in toks)
            rule = rules.get(seq)
            if not rule:
                continue
            # P/D-in-window booster (validated 2026-07-01): +P broad (+0.40pp/62% of robust
            # seqs), +D strong on oversold-reversal seqs (+5pp). Show the specific type present.
            p_types = [r["p_which"]] + [r.get(f"p_which{k}") for k in range(1, depth)]
            d_types = [r["d_which"]] + [r.get(f"d_which{k}") for k in range(1, depth)]
            p_types = sorted({str(x) for x in p_types if x and str(x) != "nan"}, reverse=True)
            d_types = sorted({str(x) for x in d_types if x and str(x) != "nan"}, reverse=True)
            p_win, d_win = bool(p_types), bool(d_types)
            p_label = p_types[0] if p_types else None
            d_label = d_types[0] if d_types else None
            # +D is biggest on the oversold-reversal family (a Z-gap absorption resolving to a -T2*)
            rev_fam = ("Z" in seq and "L46" in seq) or ("Z2GL" in seq) or ("Z1GL46" in seq)
            # score: v2 rules → OOS path-sim median + win + DSR (selection-proof bonus);
            # legacy rules → the old med20-based blend.
            _psm = rule.get("ps_med")
            if _psm is not None:
                score = int(min(100, 25 + _psm * 3.5 + (rule["win"] - 50) * 1.0
                                + (20 if (rule.get("dsr") or 0) >= 0.6 else
                                   8 if (rule.get("dsr") or 0) >= 0.3 else 0)
                                + (depth - 2) * 3
                                + (6 if p_win else 0)
                                + ((12 if rev_fam else 6) if d_win else 0)))
            else:
                score = int(min(100, 30 + rule["med20"] * 6 + (rule["win"] - 50) * 1.2
                                + (rule["pos_yrs"] / max(rule["n_yrs"], 1)) * 12
                                + (4 if (rule.get("y2026") or 0) > 2 else 0) + (depth - 2) * 3
                                + (6 if p_win else 0)
                                + ((12 if rev_fam else 6) if d_win else 0)))
            out.append({
                "ticker": str(r["ticker"]), "universe": str(r["universe"]),
                "signal_date": str(r["date"])[:10], "seq": seq, "depth": depth,
                "close": round(float(r["close"]), 2),
                "rsi": round(float(r["rsi_14"]), 0) if r["rsi_14"] == r["rsi_14"] else None,
                "med20": rule["med20"], "win": rule["win"], "n_hist": rule["n"],
                "pos_yrs": rule["pos_yrs"], "n_yrs": rule["n_yrs"], "y2026": rule.get("y2026"),
                "tier": rule.get("tier"), "dsr": rule.get("dsr"), "ps_med": rule.get("ps_med"),
                "yrs_detail": rule.get("yrs_detail"),
                "p_ctx": p_win, "d_ctx": d_win, "p_label": p_label, "d_label": d_label,
                "dv_m": round(float(r["dv"]) / 1e6, 1) if r["dv"] else None,
                "age_days": int(r["age"]), "score": max(0, score),
            })
            break  # deepest match only

    # dedup to best (highest score) row per ticker
    best = {}
    for r in out:
        tk = r["ticker"]
        if tk not in best or r["score"] > best[tk]["score"]:
            best[tk] = r
    rows = sorted(best.values(), key=lambda x: (-x["score"], -x["med20"]))[:limit]
    return {
        "as_of": as_of, "count": len(rows), "rows": rows, "n_rules": len(rules),
        "edge_note": ("FROZEN-OOS sequence scanner (v2, 2026-07-06): rules mined on 2021-23 ONLY and "
                      "verified out-of-sample on 2024-26 with stop-first trail25 path-sim (" + str(len(rules)) +
                      " OOS✓ rules of 2,371 mined; 14 also DSR≥0.6 = selection-proof vs all trials). "
                      "med20 = OOS forward median · win/ps = OOS path-sim · entry next-open. "
                      "Deeper sequence = more specific."),
    }


_COARSE_RULES: list = [None]


def _load_rules_coarse() -> dict:
    """seq_rules_v2_coarse.json → the scanner rule shape (OOS_VERIFIED only).
    Coarse token = no L-suffix (Z2G Z11 -T3) — same frozen-OOS pipeline, ~6× denser
    fires (2026-07-20, seq_mine_coarse.py)."""
    if _COARSE_RULES[0] is not None:
        return _COARSE_RULES[0]
    out = {}
    try:
        with open(os.path.join(os.path.dirname(__file__), "seq_rules_v2_coarse.json")) as f:
            v2 = json.load(f)
        for seq, r in v2.get("rules", {}).items():
            if r.get("tier") != "OOS_VERIFIED":
                continue
            out[seq] = {"depth": r["depth"], "tier": "OOS✓",
                        "win": r.get("ps_win_test"), "ps_med": r.get("ps_med_test"),
                        "med20": r.get("med20_test"), "dsr": r.get("dsr")}
    except Exception:
        pass
    _COARSE_RULES[0] = out
    return out


# ── CURATED 🧬 serving base (2026-07-20, user-selected) ──────────────────────────
# The broad OOS✓ tier (2,231 rules) diluted the chip ("ese ar amartlebs"); serving is
# now: every DSR>=0.6 rule (selection-proof, both bases, dynamic) + this explicit
# allowlist of T1/T1G/T9-resolution rules the user picked (all OOS✓, most 3/3 test-yrs+).
_CURATED_EXTRA_EXACT = {
    "-T4L12 Z3L25 -T1L3", "-T2GL12 Z1L46 -T1L12", "Z2GL46 Z11L12 -T1L12",
    "-T3L12 Z1GL46 -T1GL12", "Z9L25 Z2L46 -T1GL12", "Z9L46 Z2GL5 -T1GL3",
    "Z2L25 Z2GL46 -T9L34", "Z2GL46 Z2GL46 Z2L5 -T9L12", "-T2GL12 Z1GL5 -T9L34",
    "Z6L5 Z2GL46 -T9L12", "-T9L12 Z1GL46 -T9L34", "Z1GL5 -T9L34",
}
_CURATED_EXTRA_COARSE = {
    "Z2G -T3 Z4 -T1", "Z3 -T5 Z4 -T1", "Z2G Z11 Z2G -T1", "Z1 -T4 Z3 -T1",
    "Z1G Z6 -T1G", "Z2G -T4 Z3 -T1G", "Z2 -T9 Z1G -T1G",
    "Z1G -T5 Z4 -T9",
}


def _curated(rules: dict, extra: set) -> dict:
    return {k: r for k, r in rules.items()
            if (r.get("dsr") or 0) >= 0.6 or k in extra}


_SEQ_TK_CACHE: dict = {}


def ticker_seq_hits(ticker: str) -> dict:
    """Per-bar frozen-OOS 2-4-bar sequence completions for ONE ticker:
    {'YYYY-MM-DD': {seq, depth, win, ps_med, dsr}} — quality gate = the same one the
    Ultra screener 🧬SEQ chip uses (tier OOS✓ · depth>=2, T-ending rule REMOVED 2026-07-20 — the coarse elite ends on Z, chip shows win%).
    Deepest match per bar. TTL 1h. (2026-07-20, Superchart SEQ row)"""
    import time
    tk = str(ticker).upper()
    hit = _SEQ_TK_CACHE.get(tk)
    if hit and (time.time() - hit[0]) < 3600:
        return hit[1]
    out = {}
    try:
        from seq_analytics import _conn
        rules = _curated(_load_rules("1d"), _CURATED_EXTRA_EXACT)
        a = _conn("1d")
        try:
            sigsel = ", ".join(f"coalesce(sig_{s},0) {s}" for s in _ULTRA_TOK)
            df = a.execute(f"""
                WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                           FROM bars WHERE ticker = ? AND close >= 5)
                SELECT ticker, date, coalesce(z_sig,'') z_sig, coalesce(t_sig,'') t_sig,
                       coalesce(l_sig,'') l_sig, {sigsel}
                FROM r WHERE rn = 1 ORDER BY date
            """, [tk]).fetchdf()
        finally:
            a.close()
        crules = _curated(_load_rules_coarse(), _CURATED_EXTRA_COARSE)
        if len(df) >= 5 and (rules or crules):
            build_token(df)
            tok = df["tok"].tolist()
            # coarse token = prim only (strip the L-suffix by rebuilding from z/t/ULT)
            import pandas as _pd
            _z = df["z_sig"].fillna(""); _t = df["t_sig"].fillna("")
            _u = _pd.Series("", index=df.index)
            from seq_analytics import _ULTRA_TOK as _UT
            for u in reversed(_UT):
                if u in df:
                    _u = _u.mask(df[u] == 1, u.upper().replace("_", ""))
            ctok = _z.where(_z != "", ("-" + _t).where(_t != "", ("*" + _u).where(_u != "", "-"))).tolist()
            ds = df["date"].astype(str).str[:10].tolist()
            for i in range(3, len(df)):
                hit = None
                for depth in (4, 3, 2):
                    if i - depth + 1 < 0:
                        continue
                    seq = " ".join(str(x) for x in tok[i - depth + 1:i + 1])
                    rule = rules.get(seq)
                    if (rule and rule.get("depth", 0) >= 2
                            and str(rule.get("tier") or "").startswith("OOS")):
                        hit = {"seq": seq, "depth": depth, "win": rule["win"],
                               "ps_med": rule["ps_med"], "dsr": rule.get("dsr")}
                        break
                if hit is None:
                    for depth in (4, 3, 2):
                        if i - depth + 1 < 0:
                            continue
                        seq = " ".join(str(x) for x in ctok[i - depth + 1:i + 1])
                        rule = crules.get(seq)
                        if rule:
                            hit = {"seq": seq, "depth": depth, "win": rule["win"],
                                   "ps_med": rule["ps_med"], "dsr": rule.get("dsr"),
                                   "coarse": True}
                            break
                if hit:
                    out[ds[i]] = hit
    except Exception:
        pass
    _SEQ_TK_CACHE[tk] = (time.time(), out)
    while len(_SEQ_TK_CACHE) > 300:
        _SEQ_TK_CACHE.pop(next(iter(_SEQ_TK_CACHE)))
    return out


def today_seq_map() -> dict:
    """Bulk {ticker: fire} for the LATEST trading day — exact rules first, coarse
    fallback (both frozen-OOS, depth>=2, any ending — T-rule removed 2026-07-20). One DB pull for the whole liquid
    universe; feeds the Ultra screener 🧬SEQ chip (2026-07-20)."""
    from seq_analytics import _conn
    rules = _curated(_load_rules("1d"), _CURATED_EXTRA_EXACT)
    crules = _curated(_load_rules_coarse(), _CURATED_EXTRA_COARSE)
    a = _conn("1d")
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        sigsel = ", ".join(f"coalesce(sig_{s},0) {s}" for s in _ULTRA_TOK)
        df = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                         AND date >= DATE '{as_of}' - INTERVAL 18 DAY)
            SELECT ticker, date, coalesce(z_sig,'') z_sig, coalesce(t_sig,'') t_sig,
                   coalesce(l_sig,'') l_sig, {sigsel}
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    if df.empty:
        return {}
    build_token(df)
    _z = df["z_sig"].fillna(""); _t = df["t_sig"].fillna("")
    _u = pd.Series("", index=df.index)
    for u in reversed(_ULTRA_TOK):
        if u in df:
            _u = _u.mask(df[u] == 1, u.upper().replace("_", ""))
    df["ctok"] = _z.where(_z != "", ("-" + _t).where(_t != "", ("*" + _u).where(_u != "", "-")))
    g = df.groupby("ticker")
    for k in (1, 2, 3):
        df[f"k{k}"] = g["tok"].shift(k)
        df[f"c{k}"] = g["ctok"].shift(k)
    last = df[df["date"].astype(str).str[:10] == as_of]
    m = {}
    for _, r in last.iterrows():
        hit = None
        for depth in (4, 3, 2):
            toks = [r.get("k3"), r.get("k2"), r.get("k1"), r["tok"]][4 - depth:]
            if any(pd.isna(x) for x in toks):
                continue
            seq = " ".join(str(x) for x in toks)
            rule = rules.get(seq)
            if (rule and str(rule.get("tier") or "").startswith("OOS")):
                hit = {"seq": seq, "depth": depth, "win": rule["win"],
                       "ps_med": rule["ps_med"], "dsr": rule.get("dsr")}
                break
        if hit is None:
            for depth in (4, 3, 2):
                toks = [r.get("c3"), r.get("c2"), r.get("c1"), r["ctok"]][4 - depth:]
                if any(pd.isna(x) for x in toks):
                    continue
                seq = " ".join(str(x) for x in toks)
                rule = crules.get(seq)
                if rule:
                    hit = {"seq": seq, "depth": depth, "win": rule["win"],
                           "ps_med": rule["ps_med"], "dsr": rule.get("dsr"), "coarse": True}
                    break
        if hit:
            m[str(r["ticker"])] = hit
    return m


_SIG_TOK_COLS = ["t_sig", "z_sig", "l_sig"] + _ULTRA_TOK


def _live_token(hist_df, live_bar) -> str | None:
    """Run the canonical engine on (DB history + today's live forming bar) and return
    the LAST bar's TZ+L+ULTRA token — exactly the same vocabulary as the rule DB."""
    import main as _m
    import pandas as pd
    df = hist_df.copy()
    nb = pd.DataFrame([{**live_bar}], index=[df.index[-1] + pd.Timedelta(days=1)])
    df = pd.concat([df[["open", "high", "low", "close", "volume"]], nb])
    try:
        bars = _m.api_bar_signals("?", tf="1d", bars=len(df), universe="nasdaq",
                                  _df=df, _last_only=True)
    except Exception:
        return None
    if not bars:
        return None
    b = {k.lower(): v for k, v in bars[-1].items()}
    z = str(b.get("z_sig") or ""); t = str(b.get("t_sig") or ""); l = str(b.get("l_sig") or "")
    ult = ""
    for u in reversed(_ULTRA_TOK):
        if b.get(u) or b.get("sig_" + u):
            ult = u.upper().replace("_", "")
    prim = z if z else ("-" + t if t else ("*" + ult if ult else "-"))
    return prim + l


def seq_scan_live(limit: int = 60, max_candidates: int = 90) -> dict:
    """TODAY-0 live scan: find tickers whose trailing completed tokens match a robust
    rule PREFIX, fetch today's still-forming bar, run the engine on it, and surface the
    ones that COMPLETE a robust sequence at the current live price. Falls back to the
    plain DB scan when the regular session is closed."""
    from premarket_cache import get_today_bars
    from ai_journal.db import get_analytics_conn
    rules = _load_rules()
    if not rules:
        return {"rows": [], "count": 0, "error": "seq_rules.json missing"}
    # rule prefix index: prefix-tuple -> [(final_tok, full_seq, rule)]
    pref = {}
    for seq, r in rules.items():
        toks = seq.split(" ")
        pref.setdefault(tuple(toks[:-1]), []).append((toks[-1], seq, r))

    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        sigsel = ", ".join(f"coalesce(sig_{s},0) {s}" for s in _ULTRA_TOK)
        hist = a.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                         AND date >= DATE '{as_of}' - INTERVAL 90 DAY)
            SELECT universe, ticker, date, open, high, low, close, volume, rsi_14,
                   coalesce(z_sig,'') z_sig, coalesce(t_sig,'') t_sig, coalesce(l_sig,'') l_sig, {sigsel}
            FROM r WHERE rn=1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    if hist.empty:
        return {"rows": [], "count": 0, "as_of": as_of}
    build_token(hist)
    hist["date"] = __import__("pandas").to_datetime(hist["date"])
    g = hist.groupby("ticker")
    last = g.tail(1).set_index("ticker")
    # last completed tokens per ticker (newest-last)
    toks_by_tk = {tk: list(sub["tok"]) for tk, sub in g}

    # find near-completion candidates: ticker's trailing (depth-1) tokens == a rule prefix
    cand = {}   # ticker -> list of needed final tokens (+rule)
    for tk, toks in toks_by_tk.items():
        for d in (4, 3, 2):
            if len(toks) < d - 1:
                continue
            key = tuple(toks[-(d - 1):])
            if key in pref:
                cand.setdefault(tk, []).extend(pref[key])
    # prioritise: only live-enrich the candidates whose BEST completable rule is strongest
    # (2-bar prefixes match thousands of tickers; the engine run is the cost → cap hard).
    best_med = {tk: max(r["med20"] for _, _, r in lst) for tk, lst in cand.items()}
    cand_tks = sorted(cand, key=lambda t: -best_med[t])[:max_candidates]
    from premarket_cache import _regular_session_open
    session_open = _regular_session_open()
    try:
        today = get_today_bars(cand_tks) if session_open else {}
    except Exception:
        today = {}
    live_data = bool(today)

    # FALLBACK: session closed / live feed empty → show the regular last-completed-bar scan
    # so the table is never empty. Keep the live flags + an explanatory banner.
    if not (session_open and live_data):
        base = seq_scan(max_age_days=2, dv_floor=dv_floor, limit=limit)
        base.update({"live": False, "session_open": session_open,
                     "n_candidates": len(cand_tks), "candidates_checked": 0, "fallback": True})
        base["edge_note"] = (
            "Session OPEN but live feed empty (MASSIVE transient) — showing last completed bar (≤2d). Hit ↻."
            if session_open else
            "Regular session CLOSED — showing last completed bar (≤2d). Live mode resumes when the market opens.")
        return base

    out = []
    import pandas as pd
    for tk in cand_tks:
        lb = today.get(tk)
        if not lb:
            continue
        sub = hist[hist.ticker == tk].set_index("date")
        ltok = _live_token(sub, lb)
        if not ltok:
            continue
        for final_tok, seq, rule in cand[tk]:
            if ltok != final_tok:
                continue
            depth = rule["depth"]
            _psm = rule.get("ps_med")
            if _psm is not None:   # v2 OOS-verified rule
                score = int(min(100, 27 + _psm * 3.5 + (rule["win"] - 50) * 1.0
                                + (20 if (rule.get("dsr") or 0) >= 0.6 else
                                   8 if (rule.get("dsr") or 0) >= 0.3 else 0)
                                + (depth - 2) * 3))
            else:
                score = int(min(100, 32 + rule["med20"]*6 + (rule["win"]-50)*1.2
                                + (rule["pos_yrs"]/max(rule["n_yrs"],1))*12
                                + (4 if (rule.get("y2026") or 0) > 2 else 0) + (depth-2)*3))
            out.append({
                "ticker": tk, "universe": str(last.loc[tk, "universe"]),
                "seq": seq, "depth": depth, "live_token": ltok,
                "live_price": round(float(lb["close"]), 2),
                "rsi": None, "med20": rule["med20"], "win": rule["win"], "n_hist": rule["n"],
                "pos_yrs": rule["pos_yrs"], "n_yrs": rule["n_yrs"], "y2026": rule.get("y2026"),
                "tier": rule.get("tier"), "dsr": rule.get("dsr"), "ps_med": rule.get("ps_med"),
                "yrs_detail": rule.get("yrs_detail"),
                "score": max(0, score),
            })
    best = {}
    for r in out:
        if r["ticker"] not in best or r["score"] > best[r["ticker"]]["score"]:
            best[r["ticker"]] = r
    rows = sorted(best.values(), key=lambda x: (-x["score"], -x["med20"]))[:limit]
    if session_open and live_data:
        note = ("TODAY-0 LIVE: tickers whose last completed bars match a robust-rule PREFIX, where "
                "today's still-forming bar (current live price → engine signals) COMPLETES the sequence. "
                "Provisional — the token can flip before close.")
    elif session_open and not live_data:
        note = ("Session is OPEN but the live feed returned no forming bars (MASSIVE feed transient/"
                "unavailable). Hit refresh; or use freshness 1d/2d on the last completed bar.")
    else:
        note = "Regular session CLOSED — no forming bar. Use freshness 1d/2d on the last completed bar."
    return {
        "as_of": as_of, "live": live_data, "session_open": session_open,
        "count": len(rows), "rows": rows,
        "n_candidates": len(cand_tks), "candidates_checked": len(today), "n_rules": len(rules),
        "edge_note": note,
    }


if __name__ == "__main__":
    import json as _j
    print(_j.dumps(seq_scan(max_age_days=6), indent=2)[:1500])
    print("\n--- LIVE ---")
    print(_j.dumps(seq_scan_live(), indent=2)[:1500])
