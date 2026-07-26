"""brain/fingerprint.py — captures WHICH raw signals were active at the entry bar of a trade, so
the brain can later learn from REAL outcomes which signals (and pairs) actually pay. This is the
realized-outcome complement to miner.py's backtest: the miner proposes combos from history; the
fingerprint lets closed trades confirm or deny them on money actually risked.

capture(ticker) reads the latest 1d bar's active boolean signal flags (+ the edges that fired) and
returns a compact token list. journal.open_position stores it on the position; when the trade
closes, combo_stats() aggregates outcomes per token and per token-pair. Read-only on the DB.
"""
from __future__ import annotations
from collections import defaultdict

# the boolean signal flags worth fingerprinting (compact, meaningful state — not every column)
_FLAG_COLS = [
    "sig_t1", "sig_t2", "sig_t3", "sig_t4", "sig_t5", "sig_t6", "sig_t9", "sig_t11", "sig_t12",
    "sig_t1g", "sig_t2g", "sig_z1", "sig_z3", "sig_z5", "sig_z9", "sig_z11", "sig_z1g",
    "sig_l1", "sig_l2", "sig_l3", "sig_l4", "sig_l5", "sig_l6",
    "wyc_spring", "wyc_sos", "wyc_sow", "w2_sc", "w2_ar", "w2_spring", "w2_evr", "w2_accum",
    "um_2809", "ev_l22", "ev_l34", "ev_l43", "ev_l64", "bo_dn", "bx_dn", "be_dn",
    "gog1", "gog2", "gog3", "sig_p55", "sig_para_start", "sig_not_ext",
    "sig_vol_5x", "sig_vol_10x", "sig_bias_up", "sig_bias_dn", "sig_buy", "sig_g11", "sig_gog_plus",
]
_TEXT_COLS = ["t_sig", "z_sig", "l_sig"]  # non-empty = active token (value itself is the token)


def capture(ticker: str) -> list:
    """Active-signal fingerprint at the latest bar for `ticker`: e.g. ['T3','Z11','L43','wyc_spring',
    'um_2809','EDGE:g3abs']. Fired edges are added from edge_replay. Returns [] on any failure."""
    tk = (ticker or "").upper().strip()
    if not tk:
        return []
    tokens = []
    try:
        import duckdb
        from studio.db import tf_db_path
        cols = _FLAG_COLS + _TEXT_COLS
        con = duckdb.connect(tf_db_path("1d"), read_only=True)
        have = {r[0] for r in con.execute("DESCRIBE bars").fetchall()}
        sel = [c for c in cols if c in have]
        row = con.execute(
            f"SELECT {', '.join(sel)} FROM bars WHERE ticker=? ORDER BY date DESC LIMIT 1", [tk]
        ).fetchone()
        con.close()
        if row:
            d = dict(zip(sel, row))
            for c in _FLAG_COLS:
                if c in d and bool(d[c]):
                    tokens.append(c.replace("sig_", "").upper() if c.startswith("sig_") else c)
            for c in _TEXT_COLS:
                v = d.get(c)
                if v:
                    tokens.append(str(v))
    except Exception:
        pass
    try:  # add the edges that fired (from the warm frame if present)
        import edge_replay as er
        for code, age in er.latest_edges_map(build=False).get(tk, []):
            tokens.append(f"EDGE:{code}")
    except Exception:
        pass
    # de-dup, stable order
    seen, out = set(), []
    for t in tokens:
        if t not in seen:
            seen.add(t); out.append(t)
    return out


def combo_stats(min_n: int = 3) -> dict:
    """From CLOSED trades: realized outcome per fingerprint TOKEN and per TOKEN-PAIR — 'which active
    signals (and combinations) actually paid'. Complements the backtest miner with real money."""
    from . import journal
    closed = journal.closed_trades()
    single = defaultdict(list)
    pair = defaultdict(list)
    for c in closed:
        fp = c.get("fingerprint") or []
        entry = c.get("entry")
        if not entry:
            continue
        ret = (c.get("exit", entry) / entry - 1) * 100
        for t in fp:
            single[t].append(ret)
        for i in range(len(fp)):
            for j in range(i + 1, len(fp)):
                pair[tuple(sorted((fp[i], fp[j])))].append(ret)

    def _agg(d):
        out = []
        for k, rets in d.items():
            if len(rets) < min_n:
                continue
            s = sorted(rets)
            out.append({"token": k if isinstance(k, str) else " + ".join(k), "n": len(rets),
                        "win": round(sum(1 for r in rets if r > 0) / len(rets), 2),
                        "median": round(s[len(s) // 2], 2), "mean": round(sum(rets) / len(rets), 2)})
        return sorted(out, key=lambda x: x["median"], reverse=True)

    return {"n_closed": len(closed), "by_signal": _agg(single), "by_pair": _agg(pair),
            "note": f"realized outcomes, tokens with ≥{min_n} trades"}
