"""NakedStudy — measurement with the book unplugged.

Five years of research live in two places besides my own memory: in the setups and gates
that edge_replay computes, and in the DuckDB columns that store our conclusions about a bar
(ultra_score_v3, buy_score, gog_tier, prebreak_v4, turbo_score, …). A study that loads those
is not testing an idea against the market; it is testing an idea against our previous ideas.

This module refuses that by construction, in three ways:

1. It never imports edge_replay. No SETUPS, no E_* masks, no gates, no ⚡ATR trail, no 5-bar
   spacing, no $5/$3M screen, no $21-89 bucket. The constructor asserts the module is absent
   from the process, so the guarantee is checkable rather than promised.

2. It loads an ALLOWLIST of primitives, not everything minus a denylist. The bars table has
   415 columns and most of them encode a conclusion; enumerating what to ban would never be
   complete, so instead nothing arrives unless it is a raw description of the bar. Anything
   else raises — including our own forward labels (fwd_*, mfe_*, hit_*), which bake in
   horizon and definition choices that this module is meant to re-make from OHLC.

3. It has no exit rule. Positions are never closed early, so no stop, trail, target or cap
   can flatter or spoil the answer. What you get is what the market did:

       ret_N = close[i+N] / open[i+1] − 1        entry on the bar AFTER the signal
       mfe_N = max(high[i+1 … i+N]) / open[i+1] − 1
       mae_N = min(low [i+1 … i+N]) / open[i+1] − 1

Filters that came from research (a price floor, a liquidity floor) are not banned — they are
un-defaulted. Pass them explicitly and they are printed in the header and written to the
ledger, so a reader can see that a choice was made and where it came from.

What this module does NOT fix: the data itself. Daily history starts 2021-05-26 and the
ledger already records ~5,916 scored cells over it, which is a floor. Every result here is
still conditioned on that search whether or not anyone remembers it — which is exactly why
the ledger stays switched on even when memory is switched off. Forgetting the prior work
does not restore innocence; it only hides k. The one genuinely clean window is the one that
has not arrived yet: freeze a spec today, score it on bars dated after today.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
from analysis_kit import GuardError, bootstrap_ci_clustered, effective_n  # noqa: E402

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data",
                  "studio_analytics.duckdb")

# ── the allowlist ────────────────────────────────────────────────────────────
# Raw description of a bar: what it IS, never what we think it is worth.
IDENT = ("ticker", "date")
OHLCV = ("open", "high", "low", "close", "volume")
PRIMITIVE = {
    # mechanical pattern labels — the tokens a bar carries, no scoring attached
    "t_sig", "z_sig", "l_sig", "g_sig", "b_sig", "fly_sig", "vol_sig", "combo_sig",
    "ne_suffix", "wick_suffix", "penetration_suffix", "close_suffix", "full_suffix",
    "bar_body_wick", "bar_gap_range", "bar_gap_class", "bar_range_class",
    "setup_tokens", "context_tokens", "swing_type", "swing_type_3", "swing_type_5",
    # standard public indicators — not ours, and no threshold implied
    "rsi_14", "cci_20", "atr_14", "avg_vol_20d", "change_pct",
    # plain facts about the instrument
    "sector", "universe",
}
# Named so the error message can be specific about WHY something is refused.
_CONCLUSION_HINT = (
    "scores, tiers, zones, book setups, gates and our precomputed forward labels are "
    "conclusions from earlier research, not observations of the bar"
)

_CACHE: dict = {}


class NakedViolation(GuardError):
    pass


def _check_clean_room():
    if "edge_replay" in sys.modules:
        raise NakedViolation(
            "edge_replay is loaded in this process. NakedStudy exists to measure without "
            "the book's setups, gates and exit law; run it in its own process.")


def _load(columns: tuple, start: str | None, end: str | None) -> pd.DataFrame:
    bad = [c for c in columns if c not in PRIMITIVE]
    if bad:
        raise NakedViolation(
            f"refused columns {bad} — not in the primitive allowlist. {_CONCLUSION_HINT}. "
            f"If one of these really is a raw property of the bar, add it to PRIMITIVE "
            f"deliberately, in a commit, with the reason.")
    key = (columns, start, end)
    if key in _CACHE:
        return _CACHE[key]
    cols = ", ".join(IDENT + OHLCV + tuple(columns))
    where = []
    if start:
        where.append(f"date >= DATE '{start}'")
    if end:
        where.append(f"date <= DATE '{end}'")
    q = (f"SELECT {cols} FROM bars"
         + (" WHERE " + " AND ".join(where) if where else "")
         + " ORDER BY ticker, date")
    con = duckdb.connect(DB, read_only=True)
    df = con.execute(q).fetch_df()
    con.close()
    _CACHE[key] = df
    return df


@dataclass
class _Res:
    label: str
    n: int
    up: float
    med: float
    mean: float
    lo: float
    hi: float
    fmed: float
    f90: float
    amed: float
    n_eff: int
    per_year: pd.Series


class NakedStudy:
    """Forward-return measurement over primitives only. No exits, no gates, no priors."""

    SEARCH_THRESHOLD = 50

    def __init__(self, question: str, n_trials: int, columns: tuple = (),
                 horizons: tuple = (5, 10, 20, 60), start: str | None = None,
                 end: str | None = None, min_price: float | None = None,
                 min_dollar_vol: float | None = None, seed: int = 0):
        _check_clean_room()
        self.q, self.n_trials, self.hor, self.seed = question, n_trials, horizons, seed
        self.n_cells = 0
        self.rng = np.random.default_rng(seed)
        df = _load(tuple(columns), start, end).copy()

        # research-derived filters are allowed but never silent
        self.filters = {}
        if min_price is not None:
            df = df[df["close"] >= min_price]
            self.filters["min_price"] = min_price
        if min_dollar_vol is not None:
            dv = df["close"] * df["volume"]
            df = df[dv >= min_dollar_vol]
            self.filters["min_dollar_vol"] = min_dollar_vol
        df = df.sort_values(["ticker", "date"], ignore_index=True)

        self.df = self._forward(df)
        self.pop = self.df  # the population IS the baseline; no sampling, no proxy
        d0, d1 = str(self.df.date.min())[:10], str(self.df.date.max())[:10]
        print("=" * 122, flush=True)
        print(f"NAKED STUDY: {question}", flush=True)
        print(f"  pre-registered trials: {n_trials} · horizons {horizons} · "
              f"no exit rule, no gate, no book column", flush=True)
        print(f"  {len(self.df):,} usable bars · {self.df.ticker.nunique():,} tickers · "
              f"{d0} → {d1}", flush=True)
        print(f"  primitives loaded: {list(columns) or '(OHLCV only)'}", flush=True)
        print(f"  declared filters : {self.filters or 'NONE — nothing inherited'}",
              flush=True)
        print("=" * 122, flush=True)

    # ── forward outcomes, recomputed from OHLC ───────────────────────────────
    def _forward(self, df: pd.DataFrame) -> pd.DataFrame:
        tk = df["ticker"].to_numpy()
        o, h, l, c = (df[k].to_numpy(float) for k in ("open", "high", "low", "close"))
        H = max(self.hor)
        ent = np.r_[o[1:], np.nan]                      # entry = NEXT bar's open
        same1 = np.r_[tk[1:] == tk[:-1], False]
        ent = np.where(same1, ent, np.nan)
        for N in self.hor:
            cc = np.r_[c[N:], np.full(N, np.nan)]
            hi = pd.Series(h).rolling(N).max().shift(-N).to_numpy()
            ll = pd.Series(l).rolling(N).min().shift(-N).to_numpy()
            same = np.r_[tk[N:] == tk[:-N], np.zeros(N, bool)]   # no crossing tickers
            cc, hi, ll = (np.where(same, x, np.nan) for x in (cc, hi, ll))
            df[f"r{N}"] = cc / ent - 1
            df[f"f{N}"] = hi / ent - 1
            df[f"a{N}"] = ll / ent - 1
        df["_dt"] = pd.to_datetime(df["date"])
        df["yr"] = df["_dt"].dt.year
        df["dstr"] = df["_dt"].dt.strftime("%Y-%m-%d")
        keep = np.isfinite(df[f"r{H}"].to_numpy()) & np.isfinite(ent) & (ent > 0)
        df = df[keep].reset_index(drop=True)

        # Strata for the matched comparison. Without these, "all bars" is the wrong
        # control: a pattern that only appears on $40 liquid names would be credited for
        # not being a $2 illiquid one. Cut on the population, once, so every cell is
        # measured against the same ladder.
        df["_sp"] = pd.qcut(df["close"], 5, labels=False, duplicates="drop")
        df["_sv"] = pd.qcut(df["close"] * df["volume"], 5, labels=False, duplicates="drop")
        df["_stratum"] = (df["_sp"].fillna(-1).astype(int) * 1000
                          + df["_sv"].fillna(-1).astype(int) * 10 + (df["yr"] - 2000))
        return df

    # ── measurement ──────────────────────────────────────────────────────────
    def _stat(self, d: pd.DataFrame, N: int, label: str, n_boot: int) -> _Res:
        r, f, a = d[f"r{N}"] * 100, d[f"f{N}"] * 100, d[f"a{N}"] * 100
        lo, hi = bootstrap_ci_clustered(r, d["dstr"], stat="median", n_boot=n_boot,
                                        seed=self.seed)
        eff = effective_n(r.to_numpy(), d["dstr"].to_numpy())
        return _Res(label, len(r), (r > 0).mean(), r.median(), r.mean(), lo, hi,
                    f.median(), f.quantile(.90), a.median(), int(eff["n_eff"]),
                    r.groupby(d["yr"]).median())

    def population(self, n_boot: int = 400, sample: int = 200_000):
        """Every bar, same measurement. This is what a cell must beat."""
        src = self.pop if len(self.pop) <= sample else self.pop.sample(sample,
                                                                      random_state=self.seed)
        self.base = {N: self._stat(src, N, "ALL BARS", n_boot) for N in self.hor}
        self.base_full = {N: (self.pop[f"r{N}"] * 100).median() for N in self.hor}
        print(f"\n  POPULATION BASELINE  (point estimates on all {len(self.pop):,} bars; "
              f"intervals on a seeded {len(src):,} subsample)", flush=True)
        print(f"    {'N':>4s} {'↑':>7s} {'med':>7s} {'CI(days)':>17s} {'mean':>7s} "
              f"{'MFEmed':>7s} {'MFEp90':>7s} {'MAEmed':>7s}", flush=True)
        for N in self.hor:
            b = self.base[N]
            print(f"    {N:>4d} {b.up:>7.2%} {self.base_full[N]:>+7.2f} "
                  f"[{b.lo:>+6.2f},{b.hi:>+6.2f}] {b.mean:>+7.2f} {b.fmed:>7.2f} "
                  f"{b.f90:>7.1f} {b.amed:>7.2f}", flush=True)
        return self.base

    def _spend(self, k: int):
        self.n_cells += k
        if self.n_cells > self.n_trials:
            raise NakedViolation(
                f"trial budget exhausted: {self.n_trials} pre-registered, this run is at "
                f"cell {self.n_cells}. Re-register with the true count (which weakens "
                f"every DSR downstream) or stop.")

    def matched(self, mask, ratio: int = 5) -> pd.DataFrame:
        """Non-signal bars drawn to the cell's own price × liquidity × year mix.

        A pattern picks its own habitat. Comparing it to every bar in the market credits
        it for where it lives as much as for what it says; this draws the control from the
        same neighbourhoods, in the same proportions, so only the pattern is left.
        """
        want = self.df.loc[mask, "_stratum"].value_counts()
        pool = self.df[~mask]
        idx = []
        for st_, k in want.items():
            cand = pool.index[pool["_stratum"].to_numpy() == st_]
            if not len(cand):
                continue
            take = min(len(cand), int(k) * ratio)
            idx.append(self.rng.choice(cand, take, replace=False))
        if not idx:
            raise NakedViolation("no matched control could be drawn — the cell occupies "
                                 "strata that contain nothing else.")
        return self.df.loc[np.concatenate(idx)]

    def signal(self, label: str, mask, n_boot: int = 600, match: bool = True):
        """Score one condition at every horizon against a matched control."""
        if not hasattr(self, "base"):
            raise NakedViolation("call population() first — a cell without its baseline is "
                                 "a number without a meaning.")
        self._spend(1)
        mask = np.asarray(mask, bool) if not isinstance(mask, pd.Series) else mask.to_numpy()
        d = self.df[mask]
        ctl = self.matched(mask) if match else None
        print(f"\n  ▸ {label}   n={len(d):,}  ({len(d) / len(self.df):.2%} of bars, "
              f"≈{len(d) / self.df.dstr.nunique():.1f}/day)", flush=True)
        if match:
            print(f"    control: {len(ctl):,} non-signal bars matched on "
                  f"price×liquidity×year strata", flush=True)
        print(f"    {'N':>4s} {'↑':>7s} {'med':>7s} {'CI(days)':>17s} {'Δmed':>7s} "
              f"{'Δ↑':>7s} {'ΔMFE':>7s} {'ΔMAE':>7s} {'n_eff':>8s}  verdict", flush=True)
        out, self.ctl_stat = {}, {}
        if not hasattr(self, "ctl_all"):
            self.ctl_all = {}
        for N in self.hor:
            s = self._stat(d, N, label, n_boot)
            b = self._stat(ctl, N, "matched", max(200, n_boot // 3)) if match \
                else self.base[N]
            bmed = b.med if match else self.base_full[N]
            sep = (s.lo > b.hi) or (s.hi < b.lo)
            out[N], self.ctl_stat[N] = s, b
            self.ctl_all.setdefault(label, {})[N] = b
            print(f"    {N:>4d} {s.up:>7.2%} {s.med:>+7.2f} [{s.lo:>+6.2f},{s.hi:>+6.2f}] "
                  f"{s.med - bmed:>+7.2f} {(s.up - b.up) * 100:>+7.2f} "
                  f"{s.fmed - b.fmed:>+7.2f} {s.amed - b.amed:>+7.2f} {s.n_eff:>8,}  "
                  f"{'SEPARATE' if sep else 'overlaps control'}", flush=True)
        return out

    # ── enumeration + the honest noise reference ─────────────────────────────
    def enumerate_cells(self, title: str, cells: dict, N: int, min_n: int = 40,
                        n_shuffle: int = 400):
        """Rank many variants AND say how wide a spread pure chance gives at these sizes.

        Ranking N cells and reporting the winner is a selection. The only defence that
        costs nothing is to draw cells of the SAME SIZES at random from the same pool and
        see how far apart they land — if the real spread sits inside that, the winner is
        a lucky cell and nothing more.
        """
        self._spend(len(cells))     # an enumeration is a search; it costs what it costs
        rows = []
        for lbl, m in cells.items():
            d = self.df[m]
            if len(d) < min_n:
                rows.append(dict(cell=lbl, n=len(d), thin=True))
                continue
            r = d[f"r{N}"] * 100
            ym = r.groupby(d["yr"]).median()
            rows.append(dict(cell=lbl, n=len(d), thin=False, up=(r > 0).mean(),
                             med=r.median(), mfe=(d[f"f{N}"] * 100).median(),
                             yrs=int((ym > 0).sum()), nyr=len(ym), worst=ym.min()))
        E = pd.DataFrame(rows)
        ok = E[~E.thin].sort_values("med", ascending=False)
        b = self.base_full[N]
        print(f"\n  {title} — at {N} bars, ranked by median. The top row is a SELECTION.",
              flush=True)
        print(f"    {'cell':26s} {'n':>7s} {'↑':>7s} {'med':>7s} {'Δpop':>7s} "
              f"{'MFEmed':>7s} {'yrs':>6s} {'worst':>7s}", flush=True)
        for _, r in ok.iterrows():
            print(f"    {r.cell:26s} {r.n:>7,} {r.up:>7.2%} {r.med:>+7.2f} "
                  f"{r.med - b:>+7.2f} {r.mfe:>7.2f} {r.yrs}/{r.nyr:<4d} {r.worst:>+7.2f}",
                  flush=True)
        if int(E.thin.sum()):
            print(f"    ({int(E.thin.sum())} of {len(E)} cells below n={min_n}, not read)",
                  flush=True)
        pool = self.df[f"r{N}"].to_numpy() * 100
        sizes = ok.n.to_numpy()
        spreads = [max(m) - min(m) for m in
                   ([np.median(self.rng.choice(pool, s, replace=False)) for s in sizes]
                    for _ in range(n_shuffle))]
        obs, p95 = ok.med.max() - ok.med.min(), float(np.percentile(spreads, 95))
        inside = obs <= p95
        print(f"\n    observed best−worst spread {obs:+.2f}pp", flush=True)
        print(f"    random cells, same sizes:   median {np.median(spreads):.2f} · "
              f"p95 {p95:.2f}", flush=True)
        print(f"    → the winner is {'INSIDE' if inside else 'OUTSIDE'} what chance "
              f"produces across {len(ok)} cells"
              f"{'  — it is a lucky cell, not a finding' if inside else ''}", flush=True)
        return ok, dict(observed=obs, p95=p95, inside=bool(inside))

    # ── verdict ──────────────────────────────────────────────────────────────
    def verdict(self, res: dict, label: str, N: int, family: str = "",
                mined_window: str | None = None, oos_window: str | None = None):
        if self.n_cells > self.SEARCH_THRESHOLD and not (mined_window and oos_window):
            raise NakedViolation(
                f"{self.n_cells} cells scored — this is a SEARCH, not a test. Declare "
                f"mined_window= and oos_window= (and score only the frozen spec on the "
                f"OOS half), or stop.")
        s, b = res[N], self.base[N]
        ym = s.per_year
        npos, nper = int((ym > 0).sum()), len(ym)
        lift = s.med - self.base_full[N]
        sep = (s.lo > b.hi) or (s.hi < b.lo)
        L1 = (npos >= nper - 2) and (ym.min() >= -2)
        L2 = (lift >= 1.0) and sep
        L3 = s.n_eff >= 80
        v = "SIGNAL" if (L1 and L2 and L3) else ("WATCH" if (L1 and L2) else "NULL")
        gate = "L1" if not L1 else ("L2" if not L2 else ("L3" if not L3 else "—"))
        print("\n" + "=" * 122, flush=True)
        print(f"VERDICT: {v}   ({label} @ {N} bars, naked)", flush=True)
        print(f"  L1 periods {npos}/{nper} · worst {ym.min():+.2f} → "
              f"{'PASS' if L1 else 'FAIL'}", flush=True)
        print(f"  L2 lift vs population {lift:+.2f}pp (need ≥1.0) · intervals "
              f"{'SEPARATE' if sep else 'OVERLAP'} → {'PASS' if L2 else 'FAIL'}", flush=True)
        print(f"  L3 n_eff={s.n_eff:,} (raw {s.n:,}) (need ≥80) → "
              f"{'PASS' if L3 else 'FAIL'}", flush=True)
        print(f"  deciding gate: {gate} · cells scored: {self.n_cells} "
              f"(registered {self.n_trials})", flush=True)
        print("=" * 122, flush=True)
        try:                                   # accounting stays ON while priors stay OFF
            from ledger import log_trial      # noqa: PLC0415
            log_trial(self.q, family=family or label, n_cells=self.n_cells, verdict=v,
                      est=float(s.med), n_obs=int(s.n), n_eff=int(s.n_eff),
                      universe="naked/all-bars",
                      params=dict(naked=True, horizon=N, label=label,
                                  filters=self.filters, lift=float(lift),
                                  separate=bool(sep), primitives_only=True),
                      script=os.path.basename(sys.argv[0]))
        except Exception as e:                 # never let bookkeeping eat a result
            print(f"  (ledger not written: {e})", flush=True)
        return v
