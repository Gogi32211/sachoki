"""The measurement engine, in its own process — because it cannot live in the backend's.

Two reasons, both hard:

    CLEAN ROOM   NakedStudy's constructor asserts `edge_replay` is not in the process, so its
                 measurements cannot inherit the book's setups, gates or exit law. The backend
                 imports edge_replay at startup; inside it, NakedStudy refuses to construct.
                 Weakening that check would keep the name and lose the guarantee.

    MEMORY       a loaded study is a ~774k-row frame with forward outcomes attached. The
                 resident backend has been OOM-killed before by exactly this kind of freight
                 (see project_delta_worker_oom); a worker can be fat, die, and be restarted
                 without taking the API down with it.

PROTOCOL: JSON lines. Requests on stdin, responses on a dup of fd 1. The dup matters —
NakedStudy narrates to stdout by design, and that narration must go to the log, not into the
middle of a JSON response. So the real stdout is captured first, and `sys.stdout` is pointed at
stderr before anything statistical is imported.

The worker holds ONE study at a time (per universe) and exits after 20 idle minutes so the
memory is not held overnight for nobody.
"""
from __future__ import annotations

import json
import os
import select
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

# claim the protocol channel BEFORE any import that prints
_PROTO = os.fdopen(os.dup(1), "w")
sys.stdout = sys.stderr

import duckdb  # noqa: E402

import measure_expression as MX  # noqa: E402
import sources as srcs  # noqa: E402

IDLE_SECONDS = 20 * 60
HORIZONS = (5, 10, 20, 60)
MIN_CELL = 40

_STUDY = {}          # universe -> NakedStudy; at most one entry


def _loadable_columns() -> tuple:
    """Primitives that actually exist in the table. Metadata read, not a second data door."""
    con = duckdb.connect(os.path.join(srcs.DATA, srcs.DB["1d"]), read_only=True)
    try:
        available = {r[0] for r in con.execute("DESCRIBE bars").fetchall()}
    finally:
        con.close()
    skip = {"ticker", "date", "open", "high", "low", "close", "volume"}
    return tuple(sorted((set(srcs.BAR_PRIMITIVES) & available) - skip))


def _study(universe: str):
    if universe in _STUDY:
        return _STUDY[universe]
    from naked_study import NakedStudy                                # noqa: PLC0415
    st = NakedStudy(
        question=f"measurement console · {universe}",
        # the console's honest accounting is the durable k ledger in the API process;
        # the in-memory trial budget would reset on every worker restart and flatter
        n_trials=10**9,
        columns=_loadable_columns(), horizons=HORIZONS, tf="1d", universe=universe, seed=0)
    st.population(n_boot=300)
    _STUDY.clear()                       # one resident study; a second universe evicts the first
    _STUDY[universe] = st
    return st


def _res_row(N: int, s, b) -> dict:
    r = lambda x: None if x is None or (isinstance(x, float) and not (x == x)) else round(float(x), 3)  # noqa: E731
    return {"N": N, "n": int(s.n), "up": r(s.up * 100), "med": r(s.med),
            "lo": r(s.lo), "hi": r(s.hi),
            "d_med": r(s.med - b.med), "d_up": r((s.up - b.up) * 100),
            "d_mfe": r(s.fmed - b.fmed), "d_mae": r(s.amed - b.amed),
            "n_eff": int(s.n_eff),
            "separate": bool((s.lo > b.hi) or (s.hi < b.lo))}


def handle(req: dict) -> dict:
    expr = req["expr"]
    universe = req.get("universe") or "sp500"
    st = _study(universe)
    mask, info = MX.evaluate(st.df, expr)
    if info["n_matched"] < MIN_CELL:
        return {"error": (f"{info['n_matched']} bars match "
                          f"({info['n_dropped_nonadjacent']} more were excluded because their "
                          f"shift window crosses a calendar gap). Below {MIN_CELL} the matched "
                          f"comparison is noise wearing a table."),
                "info": info}

    on = st.df[info["on_column"]] if info["on_column"] else None
    from naked_study import NakedViolation                            # noqa: PLC0415
    try:
        out = st.signal(info["canonical"][:80], mask, n_boot=500, match=True, on=on)
    except NakedViolation as e:
        return {"error": str(e), "info": info}

    d = st.df[mask.to_numpy()]
    horizons = [_res_row(N, out[N], st.ctl_stat[N]) for N in HORIZONS]
    yearly = {str(int(y)): round(float(v), 2)
              for y, v in out[10].per_year.items()} if 10 in out else {}
    baseline = [{"N": N, "up": round(st.base[N].up * 100, 2),
                 "med": round(st.base_full[N], 3),
                 "lo": round(st.base[N].lo, 3), "hi": round(st.base[N].hi, 3)}
                for N in HORIZONS]
    return {
        "universe": universe,
        "claim_basis": "CLAIM_ABOUT_THE_MARKET — inputs are primitives only",
        "canonical": info["canonical"],
        "columns_used": info["columns"], "max_shift": info["max_shift"],
        "n_matched": info["n_matched"],
        "n_dropped_nonadjacent": info["n_dropped_nonadjacent"],
        "pct_of_bars": round(100.0 * info["n_matched"] / len(st.df), 3),
        "per_day": round(info["n_matched"] / max(1, st.df["dstr"].nunique()), 2),
        "matched_on": info["on_column"] or "price × liquidity × year strata only",
        "outcome": "ret_N = close[i+N]/open[i+1] − 1 · entry next open · no exit rule",
        "window": [str(d["dstr"].min()), str(d["dstr"].max())] if len(d) else ["", ""],
        "baseline": baseline, "horizons": horizons, "yearly_med_r10": yearly,
        "population_bars": int(len(st.df)),
    }


def main() -> None:
    while True:
        ready, _, _ = select.select([sys.stdin], [], [], IDLE_SECONDS)
        if not ready:
            print("measure_worker: idle timeout, exiting to release memory", flush=True)
            return
        line = sys.stdin.readline()
        if not line:
            return
        try:
            req = json.loads(line)
            resp = handle(req)
        except MX.ExpressionError as e:
            resp = {"error": str(e)}
        except Exception as e:                                        # noqa: BLE001
            import traceback                                          # noqa: PLC0415
            traceback.print_exc()
            resp = {"error": f"worker failure: {e}"}
        resp["id"] = (req.get("id") if isinstance(req, dict) else None) or ""
        _PROTO.write(json.dumps(resp) + "\n")
        _PROTO.flush()


if __name__ == "__main__":
    main()
