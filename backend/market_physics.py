"""Market Physics · stage 0 — measurement only, no interpretation and no claim.

Everything here is a MEASUREMENT. Not one function in this file says what a value means or what
follows from it; that separation is the whole point of the exercise and it is enforced by the
file having no outcome column in it at all.

WHY BOTH PARAMETERISATIONS ARE COMPUTED. I argued that the seven named quantities collapse to
about four independent axes, and that a different five would be more honest. That was a
hypothesis about correlation structure, not a finding — so this module computes the declared
seven AND the proposed five side by side, and stage 1 is free to show that I was wrong. A
diagnostic that can only confirm the person who designed it is not a diagnostic.

    DECLARED   R C H M E K S              as specified, including the parts I criticised
    PROPOSED   IMPACT LANDSCAPE DISORDER SCALE DAMPING

THE MEASUREMENT THAT CANNOT BE FIXED BY CARE. Daily OHLCV carries no signed order flow. Every
impact-like quantity here rests on a close-location proxy — where the bar closed inside its own
range, times volume. It is the standard proxy and it is still a proxy: a bar that closes high
after being bought and a bar that closes high because a seller stopped are the same number to
it. This is the weakest joint in the whole construction and no amount of downstream rigour
repairs it. It is recorded in the artifact next to every impact column rather than mentioned
once in a docstring.

ONE DOOR. Everything arrives through `sources.bars()` — deduplicated to one row per
(ticker, date), index rows excluded, primitive allowlist enforced, `prev_ok` attached. This
module does no loading of its own, because two loaders with two contracts is the fault this
project was already caught by once.

EVERY WINDOW IS BACKWARD-LOOKING. Rolling statistics use only bars at or before t, and every
groupby is per ticker. A window that reaches across a ticker boundary or into the future would
make all seven quantities beautiful and worthless.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

import sources as srcs

# ── window constants, declared once so the artifact can record them ─────────
WIN_IMPACT = 60          # bars for the price-impact regression
WIN_PROFILE = 120        # bars in the volume-at-price landscape
WIN_ENTROPY = 40         # bars for permutation entropy
WIN_HURST = 120          # bars for the DFA exponent
WIN_SCALE = 500          # bars for "where is today's volatility in its own history"
WIN_DAMPING = 60         # bars for the AR(1) decay of displacement
PERM_ORDER = 3           # ordinal pattern length; 3! = 6 patterns
PROFILE_BINS = 24        # ATR-scaled bins in the volume profile

MIN_HISTORY = max(WIN_SCALE, WIN_PROFILE, WIN_HURST)

PROXY_NOTE = ("daily OHLCV carries no signed order flow. Every impact column rests on the "
              "close-location proxy — where the bar closed inside its own range, times volume. "
              "A bar that closed high because it was bought and one that closed high because a "
              "seller stopped are the same number to it.")


# ── small rolling helpers, vectorised per ticker ────────────────────────────
def _roll(s: pd.Series, w: int):
    return s.rolling(w, min_periods=w)


def _windows(a: np.ndarray, w: int) -> np.ndarray:
    """Backward-looking sliding windows; rows before the first full window are NaN-padded."""
    if len(a) < w:
        return np.full((len(a), w), np.nan)
    v = np.lib.stride_tricks.sliding_window_view(a, w)
    return np.vstack([np.full((w - 1, w), np.nan), v])


# ── the primitives every axis is built from ─────────────────────────────────
def _base(g: pd.DataFrame) -> pd.DataFrame:
    """Per-bar quantities that several axes share. One ticker's frame, date-ordered."""
    o, h, l, c = (g[k].to_numpy(float) for k in ("open", "high", "low", "close"))
    v = g["volume"].to_numpy(float)
    rng = np.where(h > l, h - l, np.nan)

    out = pd.DataFrame(index=g.index)
    out["ret"] = pd.Series(c, index=g.index).pct_change()
    # true range, then ATR as a percentage of price — the scale every distance is measured in
    prev_c = np.r_[np.nan, c[:-1]]
    tr = np.nanmax(np.vstack([h - l, np.abs(h - prev_c), np.abs(l - prev_c)]), axis=0)
    out["atr"] = _roll(pd.Series(tr, index=g.index), 14).mean()
    out["atr_pct"] = out["atr"] / pd.Series(c, index=g.index)
    # close location value in [-1, 1]: the signed-flow proxy, and the weak joint
    out["clv"] = pd.Series(np.where(np.isfinite(rng), ((c - l) - (h - c)) / rng, np.nan),
                           index=g.index)
    out["flow"] = out["clv"] * v
    out["dollar_vol"] = pd.Series(c * v, index=g.index)
    # bounded displacement efficiency — the same thing bar_body_wick encodes, kept numeric
    out["efficiency"] = pd.Series(np.where(np.isfinite(rng), np.abs(c - o) / rng, np.nan),
                                  index=g.index)
    return out


# ── PROPOSED axes ───────────────────────────────────────────────────────────
def _slope_r2(x: pd.Series, y: pd.Series, w: int):
    """Rolling OLS slope and R² of y on x, backward-looking."""
    xm, ym = _roll(x, w).mean(), _roll(y, w).mean()
    cov = _roll(x * y, w).mean() - xm * ym
    var = _roll(x * x, w).mean() - xm * xm
    vary = _roll(y * y, w).mean() - ym * ym
    slope = cov / var.replace(0, np.nan)
    r2 = (cov ** 2) / (var * vary).replace(0, np.nan)
    return slope, r2


def _impact(g: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Impact as a rolling COEFFICIENT — and split in three after a circularity was found.

    THE ERROR, WORTH KEEPING IN THE FILE. The first version regressed the bar's return on the
    bar's own close-location flow. Both are built from the same close, so the regression is
    partly an identity rather than a measurement: corr(CLV_t, ret_t) = +0.52 on AAPL, while
    corr(CLV_t-1, ret_t) = +0.01. A lambda fitted contemporaneously would have looked strong and
    stable and would have been measuring `close` against itself — price as cause and result at
    once, which is exactly the failure mode this whole exercise is supposed to hunt. Kyle's
    lambda IS contemporaneous when flow comes from trade direction; with a price-derived proxy
    it cannot be.

    So three quantities, each honestly named:

        impact_range_lambda   range per unit volume — the sign is never shared, so no identity
        impact_response       does YESTERDAY's flow move today's price. A response function,
                              not an impact, and near zero is a real answer rather than a bug
        impact_efficiency     bounded displacement per unit range
    """
    out = pd.DataFrame(index=g.index)
    # (1) magnitude impact: range on volume. Range carries no sign, so it shares no term with a
    # signed return and the regression cannot become an identity.
    logv = np.log(g["volume"].to_numpy(float) + 1.0)
    x_mag = pd.Series(logv, index=g.index)
    y_mag = (g["high"] - g["low"]) / g["close"]
    slope, r2 = _slope_r2(x_mag - _roll(x_mag, WIN_IMPACT).mean(), y_mag, WIN_IMPACT)
    out["impact_range_lambda"] = slope
    out["impact_range_r2"] = r2
    # (2) response: lagged flow → return. Causal by construction, and allowed to be zero.
    x_flow = (b["flow"] / b["dollar_vol"].rolling(WIN_IMPACT, min_periods=WIN_IMPACT).mean())
    resp, resp_r2 = _slope_r2(x_flow.shift(1), b["ret"], WIN_IMPACT)
    out["impact_response"] = resp
    out["impact_response_r2"] = resp_r2
    # (3) bounded efficiency — no denominator that can vanish
    out["impact_efficiency"] = _roll(b["efficiency"], 20).mean()
    return out


def _landscape(g: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Volume-at-price as a potential: U(p) = −log(share of volume traded at p).

    NOT Coulomb. In one dimension an inverse-square field is geometrically unmotivated, and
    levels defined by pivots are circular — a pivot exists BECAUSE price reversed there, so
    "price reverses at levels" is partly a tautology. Volume-at-price is defined by trading
    rather than by reversal, which removes the circularity, and it yields the two quantities
    that actually matter: where the mode is, and how high the barrier is on the way out.
    """
    c = g["close"].to_numpy(float)
    v = g["volume"].to_numpy(float)
    atr = b["atr"].to_numpy(float)
    n = len(c)
    dist = np.full(n, np.nan)
    barrier_up = np.full(n, np.nan)
    barrier_dn = np.full(n, np.nan)
    density = np.full(n, np.nan)

    cw = _windows(c, WIN_PROFILE)
    vw = _windows(v, WIN_PROFILE)
    for i in range(WIN_PROFILE - 1, n):
        px, vol, a = cw[i], vw[i], atr[i]
        if not np.isfinite(a) or a <= 0 or not np.isfinite(px).all():
            continue
        lo, hi = px.min(), px.max()
        if hi <= lo:
            continue
        edges = np.linspace(lo, hi, PROFILE_BINS + 1)
        share, _ = np.histogram(px, bins=edges, weights=vol)
        tot = share.sum()
        if tot <= 0:
            continue
        share = share / tot
        centres = (edges[:-1] + edges[1:]) / 2
        # U is the potential: high where little was traded, low where much was
        U = -np.log(np.maximum(share, 1e-9))
        here = int(np.clip(np.digitize(c[i], edges) - 1, 0, PROFILE_BINS - 1))
        mode = int(np.argmax(share))
        dist[i] = (c[i] - centres[mode]) / a
        density[i] = share[here]
        # barrier = the highest potential between here and the edge, in each direction
        barrier_up[i] = U[here:].max() - U[here] if here < PROFILE_BINS - 1 else np.nan
        barrier_dn[i] = U[:here + 1].max() - U[here] if here > 0 else np.nan

    out = pd.DataFrame(index=g.index)
    out["land_dist_mode_atr"] = dist          # the honest "stretch": distance to where trade happened
    out["land_density_here"] = density        # structure under the current price
    out["land_barrier_up"] = barrier_up
    out["land_barrier_dn"] = barrier_dn
    return out


def _perm_entropy(w: np.ndarray, order: int) -> np.ndarray:
    """Bandt–Pompe permutation entropy of each window, normalised to [0, 1].

    Ordinal patterns rather than the sign distribution: the entropy of return SIGNS is close to
    maximal almost always and carries nearly nothing, because it cannot see the order in which
    the signs arrived. Permutation entropy can.
    """
    n, w_len = w.shape
    m = w_len - order + 1
    out = np.full(n, np.nan)
    good = np.isfinite(w).all(axis=1)
    if not good.any():
        return out
    sub = w[good]
    # rank each length-`order` sub-window; encode the permutation as a base-`order` integer
    idx = np.arange(m)[:, None] + np.arange(order)[None, :]
    pats = np.argsort(sub[:, idx], axis=2)
    codes = (pats * (order ** np.arange(order))).sum(axis=2)
    n_pat = int(np.math.factorial(order))
    ent = np.zeros(len(sub))
    for k in range(order ** order):
        p = (codes == k).mean(axis=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            ent -= np.where(p > 0, p * np.log(p), 0.0)
    out[good] = ent / np.log(n_pat)
    return out


def _dfa(w: np.ndarray) -> np.ndarray:
    """Detrended fluctuation analysis exponent per window.

    Measures whether the series is trending (>0.5), mean-reverting (<0.5) or random (≈0.5) AT A
    GIVEN SCALE. This is the honest multiscale quantity — EMA-slope agreement is very nearly a
    deterministic function of one recent trend and carries little information beyond it.
    """
    n, w_len = w.shape
    out = np.full(n, np.nan)
    good = np.isfinite(w).all(axis=1)
    if not good.any():
        return out
    sub = w[good]
    prof = np.cumsum(sub - sub.mean(axis=1, keepdims=True), axis=1)
    scales = [s for s in (8, 16, 32) if s * 4 <= w_len]
    if len(scales) < 2:
        return out
    logF = []
    for s in scales:
        k = w_len // s
        seg = prof[:, :k * s].reshape(len(sub), k, s)
        t = np.arange(s)
        tm, tv = t.mean(), ((t - t.mean()) ** 2).sum()
        ym = seg.mean(axis=2, keepdims=True)
        slope = ((seg - ym) * (t - tm)).sum(axis=2, keepdims=True) / tv
        resid = seg - (ym + slope * (t - tm))
        logF.append(np.log(np.sqrt((resid ** 2).mean(axis=(1, 2))) + 1e-12))
    ls = np.log(np.array(scales, float))
    Y = np.vstack(logF).T
    out[good] = ((Y - Y.mean(axis=1, keepdims=True)) * (ls - ls.mean())).sum(axis=1) \
        / ((ls - ls.mean()) ** 2).sum()
    return out


def _disorder(g: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    r = b["ret"].to_numpy(float)
    out = pd.DataFrame(index=g.index)
    out["dis_perm_entropy"] = _perm_entropy(_windows(r, WIN_ENTROPY), PERM_ORDER)
    out["dis_hurst"] = _dfa(_windows(r, WIN_HURST))
    # switching frequency: how often the sign flips. The quantity the declared H was reaching
    # for when it distinguished "distribution" from "how often it changes".
    flips = (np.sign(r) != np.sign(np.r_[np.nan, r[:-1]])).astype(float)
    out["dis_switch_rate"] = _roll(pd.Series(flips, index=g.index), WIN_ENTROPY).mean()
    return out


def _scale(g: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Where today's volatility sits in its OWN long-run distribution.

    Deliberately not called energy. There is no conservation law making suppressed variance owe
    a release later; the observed regularity is the opposite — volatility clusters, so low
    predicts low. What is true is slow mean reversion of a persistent process, which has a
    completely different timing profile from a spring, and the name should not smuggle one in.
    """
    out = pd.DataFrame(index=g.index)
    out["scale_atr_pct"] = b["atr_pct"]
    out["scale_vol_pctile"] = _roll(b["atr_pct"], WIN_SCALE).rank(pct=True)
    # how long the current compression has lasted — duration, not depth
    low = (out["scale_vol_pctile"] < 0.25).astype(int)
    grp = (low != low.shift()).cumsum()
    out["scale_compression_len"] = low.groupby(grp).cumsum()
    return out


def _damping(g: pd.DataFrame, b: pd.DataFrame, land: pd.DataFrame) -> pd.DataFrame:
    """AR(1) coefficient of displacement — the axis the declared set is missing.

    A spring with no damper oscillates forever. Hooke gives a restoring force; nothing in the
    declared seven says how fast a displacement decays, yet that is exactly the question behind
    "in which regimes does deviation actually revert". φ near 0 → displacement dies at once,
    near 1 → it persists, above 1 → it grows, which is a trend and not a spring at all.
    """
    x = land["land_dist_mode_atr"]
    xl = x.shift(1)
    xm = _roll(x, WIN_DAMPING).mean()
    xlm = _roll(xl, WIN_DAMPING).mean()
    cov = _roll(x * xl, WIN_DAMPING).mean() - xm * xlm
    var = _roll(xl * xl, WIN_DAMPING).mean() - xlm * xlm
    out = pd.DataFrame(index=g.index)
    out["damp_phi"] = cov / var.replace(0, np.nan)
    return out


# ── DECLARED seven, as specified — including what I criticised ──────────────
def _declared(g: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """The seven as the author defined them. Written faithfully, not charitably.

    `decl_R` keeps its unbounded denominator and `decl_E` keeps its "low volatility = stored
    energy" reading, because stage 1 compares parameterisations and a comparison against a
    version I quietly improved would prove nothing.
    """
    c = g["close"].to_numpy(float)
    out = pd.DataFrame(index=g.index)
    disp = np.abs(np.r_[np.nan, np.diff(c)])
    atr = b["atr"].to_numpy(float)

    # R — volume per unit displacement, denominator floored at a tick to stay finite
    out["decl_R"] = np.log1p(g["volume"].to_numpy(float) / np.maximum(disp, 0.01))
    # C — inverse-distance field from rolling extremes, unsigned superposition
    hi = _roll(g["high"], 60).max()
    lo = _roll(g["low"], 60).min()
    dh = (hi - g["close"]).abs() / pd.Series(atr, index=g.index)
    dl = (g["close"] - lo).abs() / pd.Series(atr, index=g.index)
    out["decl_C"] = 1.0 / (1.0 + dh ** 2) + 1.0 / (1.0 + dl ** 2)
    # H — Shannon entropy of the up/down distribution, the version that is nearly always maximal
    up = _roll((b["ret"] > 0).astype(float), WIN_ENTROPY).mean()
    out["decl_H"] = -(up * np.log(up.clip(1e-9)) + (1 - up) * np.log((1 - up).clip(1e-9))) \
        / np.log(2)
    # M — p = m·v with dollar volume as mass, signed
    out["decl_M"] = (b["dollar_vol"] / _roll(b["dollar_vol"], 60).mean()) * b["ret"]
    # E — compression as stored energy: the inverse of current volatility percentile
    out["decl_E"] = 1.0 - _roll(b["atr_pct"], WIN_SCALE).rank(pct=True)
    # K — stretch from an EMA equilibrium, in ATR
    ema = g["close"].ewm(span=20, adjust=False).mean()
    out["decl_K"] = (g["close"] - ema) / pd.Series(atr, index=g.index)
    # S — agreement of EMA slopes across four scales
    slopes = []
    for span in (9, 20, 50, 200):
        e = g["close"].ewm(span=span, adjust=False).mean()
        slopes.append(np.sign(e.diff()))
    sl = pd.concat(slopes, axis=1)
    out["decl_S"] = sl.mean(axis=1).abs()
    return out


# ── assembly ────────────────────────────────────────────────────────────────
AXES_PROPOSED = ["impact_range_lambda", "impact_range_r2", "impact_response",
                 "impact_response_r2", "impact_efficiency",
                 "land_dist_mode_atr", "land_density_here", "land_barrier_up",
                 "land_barrier_dn", "dis_perm_entropy", "dis_hurst", "dis_switch_rate",
                 "scale_atr_pct", "scale_vol_pctile", "scale_compression_len", "damp_phi"]
AXES_DECLARED = ["decl_R", "decl_C", "decl_H", "decl_M", "decl_E", "decl_K", "decl_S"]


def compute(df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """All axes for a bars frame. Per ticker, backward-looking, no outcome touched."""
    parts = []
    n_tk = df["ticker"].nunique()
    for j, (tk, g) in enumerate(df.groupby("ticker", sort=False)):
        if len(g) < MIN_HISTORY:
            continue
        g = g.sort_values("date")
        b = _base(g)
        land = _landscape(g, b)
        parts.append(pd.concat([
            g[["ticker", "date", "close", "volume"]],
            _impact(g, b), land, _disorder(g, b), _scale(g, b),
            _damping(g, b, land), _declared(g, b)], axis=1))
        if verbose and j % 100 == 0:
            print(f"  {j}/{n_tk} tickers", flush=True)
    if not parts:
        raise ValueError("no ticker had enough history for the longest window")
    return pd.concat(parts, ignore_index=True)


def load_and_compute(universe: str = "sp500", start: str | None = None,
                     verbose: bool = True) -> pd.DataFrame:
    df = srcs.bars("1d", universe=universe, start=start, verbose=False)
    if verbose:
        print(f"  {len(df):,} bars · {df.ticker.nunique():,} tickers · "
              f"{df.date.min()} → {df.date.max()}", flush=True)
    return compute(df, verbose=verbose)


def spec() -> dict:
    """What was measured and with which windows — recorded beside every artifact."""
    return {"windows": {"impact": WIN_IMPACT, "profile": WIN_PROFILE, "entropy": WIN_ENTROPY,
                        "hurst": WIN_HURST, "scale": WIN_SCALE, "damping": WIN_DAMPING,
                        "perm_order": PERM_ORDER, "profile_bins": PROFILE_BINS},
            "axes_proposed": AXES_PROPOSED, "axes_declared": AXES_DECLARED,
            "signed_flow_proxy": PROXY_NOTE,
            "circularity_found_and_split": (
                "the first impact draft regressed a bar's return on its own close-location "
                "flow; both are built from the same close (corr +0.52 contemporaneous vs +0.01 "
                "lagged), so it measured an identity. Split into a sign-free magnitude impact "
                "and an explicitly lagged response function."),
            "stage": "0 — MEASUREMENT ONLY. No interpretation, no outcome, no claim."}
