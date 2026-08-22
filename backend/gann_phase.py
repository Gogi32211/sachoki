"""Phase-world machinery for GANN — recomputes the whole X layer under a lattice shift.

A world gives every ticker ONE phi ~ U[0,1), used on every date and on BOTH families
(GANN_PHASE_NULL_V1). Only the lattice phase moves; anchors, M, B, slopes, spacing, dates
and OHLC are the frozen ones. Under the shift the analytic solve changes by a single term:

    integer i is touched  <=>  x_lo - phi <= i <= x_hi - phi

so touch, onset, direction, confluence consensus and the control direction are all
recomputed per world from X alone.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, os, sys                                              # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import gann_grid as G                                                # noqa: E402

PHASE_NS = "GANN_PHASE_NULL_V1"


def world_phi(root: bytes, world: int, tickers: np.ndarray) -> np.ndarray:
    """One phi per ticker for this world — SHA256-derived, replayable, never hash()."""
    m = hashlib.sha256()
    m.update(PHASE_NS.encode()); m.update(b"|"); m.update(root)
    m.update(b"|"); m.update(str(world).encode())
    seed = int.from_bytes(m.digest()[:8], "big")
    rng = np.random.default_rng(seed)
    return rng.random(len(tickers))


def prepare(Gr: pd.DataFrame):
    """Column views the world loop reuses; sorted by (ticker, di) once, not per world."""
    Gr = Gr.sort_values(["ticker", "di"], kind="stable").reset_index(drop=True)
    tick_code, tick_uniq = pd.factorize(Gr.ticker, sort=True)
    logL = np.log(Gr.L.to_numpy())
    logM = np.log(Gr.M.to_numpy())
    dt = Gr["dt"].to_numpy()
    B = Gr.B.to_numpy()
    st = dict(
        n=len(Gr), tick_code=tick_code, tickers=tick_uniq,
        di=Gr.di.to_numpy(), date=Gr.date.to_numpy(),
        logL=logL, logM=logM, dt=dt, B=B,
        x_lo=(np.log(Gr.low_d.to_numpy()) - logL) / logM,
        x_hi=(np.log(Gr.high_d.to_numpy()) - logL) / logM,
        x_prev=(np.log(Gr.prev_close.to_numpy()) - logL) / logM,
        prev_close=Gr.prev_close.to_numpy(),
        L=Gr.L.to_numpy(), M=Gr.M.to_numpy())
    # first observable bar per ticker, for the onset observability rule
    first = pd.Series(st["di"]).groupby(tick_code).transform("min").to_numpy()
    st["first_di"] = first
    return st


def _family(st, phi_row, sign):
    """Per-row touch flag, direction of the touched line, and control direction."""
    off = sign * st["dt"] / st["B"] + phi_row          # i + off in [x_lo, x_hi]
    i0 = np.ceil(st["x_lo"] - off)
    i1 = np.floor(st["x_hi"] - off)
    cnt = np.maximum(0, i1 - i0 + 1)
    touch = cnt > 0
    off_prev = sign * (st["dt"] - 1.0) / st["B"] + phi_row
    # treated pick: nearest to Close[D-1] among the touched lines, clamped
    pick_t = np.clip(np.round(st["x_prev"] - off), i0, i1)
    # control pick: simply the nearest line to Close[D-1], evaluated at D-1
    pick_c = np.round(st["x_prev"] - off_prev)
    pick = np.where(touch, pick_t, pick_c)
    # the comparison must be BIT-IDENTICAL to the frozen builder, so the line price is
    # rebuilt multiplicatively and compared in PRICE space — comparing in log space moved
    # 11 of 109,229 confluence rows at phi = 0, all of them boundary cases
    line_prev = st["L"] * np.power(st["M"], pick + off_prev)
    pc = st["prev_close"]
    direction = np.where(pc > line_prev, 1, np.where(pc < line_prev, -1, 0))
    return touch, direction, cnt


def world_state(st, phi_by_ticker: np.ndarray):
    """Touch/onset/direction for all three claims in one phase world."""
    phi_row = phi_by_ticker[st["tick_code"]]
    t_asc, d_asc, n_asc = _family(st, phi_row, +1.0)
    t_desc, d_desc, n_desc = _family(st, phi_row, -1.0)
    t_conf = t_asc & t_desc
    d_conf = np.where((d_asc == 0) | (d_desc == 0), 0,
                      np.where(d_asc == d_desc, d_asc, 9))           # 9 = conflict
    out = {}
    for name, touch, direction, cnt in (("ASC", t_asc, d_asc, n_asc),
                                        ("DESC", t_desc, d_desc, n_desc),
                                        ("CONF", t_conf, d_conf, n_asc + n_desc)):
        onset = _onset(st, touch)
        out[name] = dict(touch=touch, onset=onset, direction=direction, n_lines=cnt)
    return out


def _onset(st, touch):
    """5-session onset with the observability rule: missing is not FALSE."""
    idx = np.flatnonzero(touch)
    if len(idx) == 0:
        return np.zeros(st["n"], bool)
    tc, di = st["tick_code"][idx], st["di"][idx]
    new_t = np.empty(len(idx), bool); new_t[0] = True
    new_t[1:] = tc[1:] != tc[:-1]
    prev_di = np.empty(len(idx), float); prev_di[0] = np.nan
    prev_di[1:] = np.where(tc[1:] == tc[:-1], di[:-1], np.nan)
    quiet = np.isnan(prev_di) | ((di - prev_di) > G.ONSET_QUIET_BARS)
    observable = (di - G.ONSET_QUIET_BARS) >= st["first_di"][idx]
    onset = np.zeros(st["n"], bool)
    onset[idx[quiet & observable]] = True
    return onset


def census(st, ws):
    """Per-claim X census for one world — counts only, never an outcome."""
    rows = {}
    for name, d in ws.items():
        on = d["onset"]
        dirn = d["direction"]
        valid = on & np.isin(dirn, (1, -1))
        rows[name] = dict(
            events=int(on.sum()),
            directional_valid=int(valid.sum()),
            long=int((on & (dirn == 1)).sum()),
            short=int((on & (dirn == -1)).sum()),
            invalid_equality=int((on & (dirn == 0)).sum()),
            invalid_conflict=int((on & (dirn == 9)).sum()),
            tickers=int(len(np.unique(st["tick_code"][on]))) if on.any() else 0,
            dates=int(len(np.unique(st["date"][on]))) if on.any() else 0,
            touch_rows=int(d["touch"].sum()))
    return rows
