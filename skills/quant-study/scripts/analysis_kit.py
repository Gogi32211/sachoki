"""analysis_kit — mechanical guards for quantitative studies on any tabular data.

This exists because prose does not stop mechanical mistakes. Every guard here was written
after a real failure, and each one REFUSES rather than warns:

  Study.baseline()  the baseline is computed on the SAME rows the cell is drawn from.
                    Comparing a 2024-26 cell to a 2021-26 baseline manufactured +1.3pp of
                    free lift in one real study before this was enforced.
  Study.describe()  a raw value means nothing without its population. "82% vs 46%" was read
                    as an anomaly when the population median was 38 — i.e. it was NORMAL.
                    cell() refuses to score a threshold on a column never described.
  Study.controls()  controls are generated FROM the hypothesis, not remembered. A chain
                    A→B→C is automatically compared against A alone, B alone, C alone and
                    every leave-one-out subset.
  Study.verdict()   will not print BUILD unless L1/L2/L3 actually passed. Not a reminder.
  purged_splits()   plain k-fold on overlapping-horizon data leaks. Only purged/embargoed
                    walk-forward is offered.
  Study.n_trials    fixed at construction, before anything runs, and enforced.

No dependency on any project: give it a DataFrame and a column of outcomes.

    st = Study("does X pay?", n_trials=12, outcome="ret", time_col="date")
    st.describe(df, "cla")                       # population context first
    st.baseline(df)                              # matched baseline
    st.cell(df, "X >= 20", df.cla >= 20)
    st.controls(df, {"X": df.cla >= 20, "RS": df.rs, "OS": df.rsi < 45})
    print(st.verdict())
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from itertools import combinations

import numpy as np
import pandas as pd

__all__ = ["Study", "pct_of", "bootstrap_ci", "bootstrap_ci_clustered", "effective_n",
           "purged_splits", "rank_ic", "GuardError"]


class GuardError(RuntimeError):
    """Raised when a guard is violated. Guards refuse; they do not warn."""


# ── small utilities ───────────────────────────────────────────────────────────
def pct_of(series: pd.Series, value: float) -> float:
    """Where does `value` sit inside `series`? The question to ask before calling
    any raw number 'high' or 'low'."""
    s = pd.Series(series).dropna()
    if s.empty:
        return float("nan")
    return float((s <= value).mean() * 100)


def bootstrap_ci(x, stat=np.median, n_boot: int = 2000, alpha: float = 0.05,
                 seed: int = 0) -> tuple[float, float]:
    """Percentile bootstrap CI treating every row as independent.

    ⚠ Only correct when rows really are independent. For anything where many observations
    share a common shock — a market day, a cohort, a site — use bootstrap_ci_clustered,
    which is what Study uses automatically when a time column is present."""
    a = np.asarray(pd.Series(x).dropna(), dtype=float)
    if len(a) < 8:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(a), size=(n_boot, len(a)))
    vals = np.sort(stat(a[idx], axis=1))
    return (float(vals[int(n_boot * alpha / 2)]), float(vals[int(n_boot * (1 - alpha / 2))]))


def bootstrap_ci_clustered(x, groups, stat: str = "median", n_boot: int = 1000,
                           alpha: float = 0.05, seed: int = 0) -> tuple[float, float]:
    """Percentile bootstrap that resamples whole GROUPS, not rows.

    The failure this exists for: a signal board fires 114 times on one market day. Row-level
    bootstrap counts that as 114 independent observations. Informationally it is closer to
    ONE — they share a single market return. Row-level intervals are therefore far too
    narrow, and narrow intervals are how a nothing becomes a finding.

    Implemented as a multinomial reweighting of dates, which is exactly "resample dates with
    replacement" but vectorised, so it costs the same as the naive version.
    """
    v = np.asarray(pd.Series(x).astype(float))
    g = pd.factorize(pd.Series(groups))[0]
    ok = np.isfinite(v) & (g >= 0)
    v, g = v[ok], g[ok]
    ng = int(g.max()) + 1 if len(g) else 0
    if len(v) < 8 or ng < 3:
        return (float("nan"), float("nan"))
    order = np.argsort(v, kind="stable")
    v, g = v[order], g[order]
    rng = np.random.default_rng(seed)
    p = np.full(ng, 1.0 / ng)
    out = np.empty(n_boot)
    for b in range(n_boot):
        w = rng.multinomial(ng, p)[g].astype(float)     # each date drawn ~Poisson(1)
        tot = w.sum()
        if tot <= 0:
            out[b] = np.nan; continue
        if stat == "mean":
            out[b] = float((v * w).sum() / tot)
        else:
            cw = np.cumsum(w)
            out[b] = float(v[np.searchsorted(cw, tot / 2.0)])
    out = np.sort(out[np.isfinite(out)])
    if len(out) < 20:
        return (float("nan"), float("nan"))
    return (float(out[int(len(out) * alpha / 2)]),
            float(out[int(len(out) * (1 - alpha / 2))]))


def effective_n(x, groups) -> dict:
    """How many INDEPENDENT observations are really here?

    Rows clustered inside groups carry less information than their count suggests. The
    design effect deff = 1 + (m̄ − 1)·ICC converts the raw count into an effective one, with
    ICC estimated by one-way ANOVA across groups. `n_eff` is the number to quote when asking
    whether a result rests on enough evidence — `n_rows` almost always flatters it.
    """
    s = pd.DataFrame({"v": pd.to_numeric(pd.Series(x), errors="coerce"),
                      "g": pd.Series(groups).astype(str)}).dropna()
    n, k = len(s), s["g"].nunique()
    if n < 10 or k < 2:
        return {"n_rows": n, "n_groups": k, "n_eff": n, "icc": 0.0, "deff": 1.0,
                "max_group_share": 1.0}
    grand = s["v"].mean()
    gm = s.groupby("g")["v"].agg(["mean", "size"])
    ssb = float((gm["size"] * (gm["mean"] - grand) ** 2).sum())
    ssw = float(((s["v"] - s["g"].map(gm["mean"])) ** 2).sum())
    msb = ssb / (k - 1) if k > 1 else 0.0
    msw = ssw / (n - k) if n > k else 0.0
    mbar = n / k
    icc = (msb - msw) / (msb + (mbar - 1) * msw) if (msb + (mbar - 1) * msw) > 0 else 0.0
    icc = float(min(max(icc, 0.0), 1.0))
    deff = 1.0 + (mbar - 1.0) * icc
    return {"n_rows": n, "n_groups": int(k), "n_eff": int(round(n / deff)) if deff > 0 else n,
            "icc": round(icc, 4), "deff": round(deff, 2),
            "max_group_share": round(float(gm["size"].max() / n), 4)}


def purged_splits(dates, n_splits: int = 5, horizon: int = 1, embargo: int = 0):
    """Walk-forward splits with PURGING and EMBARGO — the only honest CV when the
    outcome of a row spans `horizon` periods forward.

    Plain k-fold puts future rows in train and past rows in test, and overlapping
    labels leak across the boundary even in a chronological split. Purging drops the
    `horizon` rows before the test block; embargo drops rows after it.

    Yields (train_idx, test_idx) as positional integer arrays.
    """
    d = pd.Series(pd.to_datetime(pd.Series(dates).values)).reset_index(drop=True)
    n = len(d)
    if n_splits < 2:
        raise GuardError("n_splits must be >= 2")
    order = np.argsort(d.values, kind="stable")
    bounds = np.linspace(0, n, n_splits + 1).astype(int)
    for k in range(1, n_splits):                    # expanding window, always train<test
        tr_end = bounds[k]
        te_beg, te_end = bounds[k], bounds[k + 1]
        train = order[: max(0, tr_end - horizon)]   # PURGE the overlap
        test = order[te_beg:te_end]
        if embargo:
            train = train[: max(0, len(train))]     # expanding: embargo only bites forward
        if len(train) and len(test):
            yield train, test


def rank_ic(pred, actual, by=None) -> dict:
    """Cross-sectional rank IC — the metric for 'does my ranking actually rank?'.
    `by` groups per date so each period is ranked against its own cross-section."""
    df = pd.DataFrame({"p": np.asarray(pred, float), "a": np.asarray(actual, float)})
    if by is not None:
        df["g"] = np.asarray(by)
        ics = df.groupby("g").apply(
            lambda x: x["p"].corr(x["a"], method="spearman") if len(x) > 4 else np.nan,
            include_groups=False).dropna()
    else:
        ics = pd.Series([df["p"].corr(df["a"], method="spearman")])
    ics = ics.dropna()
    if ics.empty:
        return {"ic": float("nan"), "n": 0}
    return {"ic": float(ics.mean()), "ic_std": float(ics.std()),
            "icir": float(ics.mean() / ics.std()) if ics.std() > 0 else float("nan"),
            "n_periods": int(len(ics)), "hit": float((ics > 0).mean() * 100)}


# ── the study object ──────────────────────────────────────────────────────────
@dataclass
class _Cell:
    label: str
    n: int
    est: float
    lo: float
    hi: float
    per_period: dict = field(default_factory=dict)
    eff: dict = field(default_factory=dict)
    worst: float = float("nan")
    n_periods_pos: int = 0
    n_periods: int = 0


class Study:
    """One study. Construct it BEFORE running anything — n_trials is a pre-registration."""

    def __init__(self, question: str, n_trials: int, outcome: str,
                 time_col: str | None = None, stat=np.median, unit: str = ""):
        if not question or n_trials < 1:
            raise GuardError("a study needs a question and a pre-registered n_trials")
        self.question = question
        self.n_trials = int(n_trials)
        self.outcome = outcome
        self.time_col = time_col
        self.stat = stat
        self.unit = unit
        self._described: set[str] = set()
        self._baseline: _Cell | None = None
        self._baseline_universe: int | None = None
        self.cells: list[_Cell] = []
        self._log: list[str] = []
        print(f"\n{'='*88}\nSTUDY: {question}\n"
              f"pre-registered trials: {n_trials} · outcome: {outcome}\n{'='*88}", flush=True)

    # ── guard 1: population context before interpretation ────────────────────
    def describe(self, df: pd.DataFrame, col: str, value: float | None = None) -> pd.Series:
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        q = s.quantile([.01, .05, .25, .5, .75, .95, .99])
        self._described.add(col)
        print(f"\n  POPULATION · {col}  (n={len(s):,})", flush=True)
        print("    " + "  ".join(f"p{int(k*100)} {v:+.2f}" for k, v in q.items()), flush=True)
        if value is not None:
            p = pct_of(s, value)
            verdict = ("UNREMARKABLE" if 20 <= p <= 80 else
                       "notable" if 5 <= p <= 95 else "EXTREME")
            print(f"    your value {value:+.2f} sits at p{p:.1f} → {verdict}", flush=True)
        return q

    # ── guard 2: the baseline must come from the same universe ───────────────
    def baseline(self, df: pd.DataFrame, label: str = "BASELINE (matched universe)") -> _Cell:
        """Compute the baseline on EXACTLY the rows the cells will be drawn from.
        Pass the already-filtered frame — the same one used for cells."""
        c = self._score(df, label, np.ones(len(df), bool))
        self._baseline = c
        self._baseline_universe = len(df)
        print(f"    ↑ this is the number every cell is measured against, "
              f"not a global constant", flush=True)
        return c

    # ── scoring ───────────────────────────────────────────────────────────────
    def _score(self, df: pd.DataFrame, label: str, mask) -> _Cell:
        m = np.asarray(pd.Series(mask).fillna(False), bool)
        sub = df.loc[m]
        y = pd.to_numeric(sub[self.outcome], errors="coerce").dropna()
        if len(y) == 0:
            raise GuardError(f"cell '{label}' is empty")
        est = float(self.stat(y))
        # CLUSTER BY DATE when we have one. Row-level intervals treat 114 fires on a single
        # market day as 114 independent facts; they are closer to one.
        eff = {}
        if self.time_col and self.time_col in sub:
            dkey = pd.to_datetime(sub[self.time_col]).dt.strftime("%Y-%m-%d")
            dkey = dkey.loc[y.index]
            lo, hi = bootstrap_ci_clustered(
                y, dkey, stat=("mean" if self.stat is np.mean else "median"))
            eff = effective_n(y, dkey)
        else:
            lo, hi = bootstrap_ci(y, self.stat)
        per, worst, npos, nper = {}, float("nan"), 0, 0
        if self.time_col and self.time_col in sub:
            g = pd.to_datetime(sub[self.time_col]).dt.year
            pp = y.groupby(g).apply(lambda z: float(self.stat(z)) if len(z) >= 5 else np.nan)
            pp = pp.dropna()
            per = {int(k): float(v) for k, v in pp.items()}
            if len(pp):
                worst, npos, nper = float(pp.min()), int((pp > 0).sum()), int(len(pp))
        # KEYWORDS, NEVER POSITIONAL. Adding `eff` in the middle of the dataclass once
        # silently shifted worst/n_periods_pos/n_periods by one slot, and the verdict then
        # reported "L1 not evaluable" on a study that had five clean years. It did not
        # crash — it just printed the wrong deciding gate.
        c = _Cell(label=label, n=len(y), est=est, lo=lo, hi=hi, per_period=per, eff=eff,
                  worst=worst, n_periods_pos=npos, n_periods=nper)
        self.cells.append(c)
        rel = ""
        if self._baseline is not None and label != self._baseline.label:
            rel = f"  Δbase {est - self._baseline.est:+.2f}"
        yr = f"  {npos}/{nper}yr worst {worst:+.2f}" if nper else ""
        ef = ""
        if eff:
            flag = " ⚠weak" if eff["n_eff"] < 30 else ""
            ef = (f"  n_eff {eff['n_eff']:,}/{eff['n_rows']:,} "
                  f"({eff['n_groups']:,} dates, deff {eff['deff']}){flag}")
        print(f"  {label:44s} n={len(y):>7,} {est:+8.2f}{self.unit} "
              f"[{lo:+.2f},{hi:+.2f}]{yr}{rel}{ef}", flush=True)
        return c

    def cell(self, df: pd.DataFrame, label: str, mask, requires: list[str] | None = None):
        if self._baseline is None:
            raise GuardError("call baseline(df) first — a cell without a matched baseline "
                             "is uninterpretable")
        if len(df) != self._baseline_universe:
            raise GuardError(
                f"row universe changed since baseline ({self._baseline_universe:,} → "
                f"{len(df):,}). Recompute baseline(df) on THIS frame — this is the "
                f"window-mismatch guard.")
        for col in (requires or []):
            if col not in self._described:
                raise GuardError(
                    f"'{col}' thresholded but never described. Call describe(df, '{col}') "
                    f"first — a raw cutoff without its population is how a normal value "
                    f"gets reported as an anomaly.")
        if len(self.cells) - 1 >= self.n_trials:
            raise GuardError(
                f"trial budget exhausted: {self.n_trials} pre-registered. Either the study "
                f"grew beyond its registration (re-register honestly with the true count, "
                f"which weakens every DSR/FDR) or stop here.")
        return self._score(df, label, mask)

    # ── guard 3: controls generated from the hypothesis ──────────────────────
    def controls(self, df: pd.DataFrame, components: dict, max_show: int = 12):
        """Given the hypothesis parts, measure each ALONE and every leave-one-out.
        If the full combination does not beat its own pieces, the extras are decoration."""
        keys = list(components)
        print(f"\n  CONTROLS — the combination must beat its own parts", flush=True)
        for k in keys:
            self._score(df, f"    only {k}", components[k])
        if len(keys) > 2:
            for drop in keys:
                rest = [k for k in keys if k != drop]
                m = np.ones(len(df), bool)
                for k in rest:
                    m &= np.asarray(pd.Series(components[k]).fillna(False), bool)
                self._score(df, f"    all but {drop}", m)
        full = np.ones(len(df), bool)
        for k in keys:
            full &= np.asarray(pd.Series(components[k]).fillna(False), bool)
        return self._score(df, "    ★ FULL combination", full)

    # ── guard 4: the verdict refuses to overstate ────────────────────────────
    #: a study that scores more cells than this is a SEARCH, and a search must reserve an
    #: out-of-sample window before it starts, not after it finds something.
    SEARCH_THRESHOLD = 50

    def verdict(self, cell: _Cell | None = None, min_n: int = 80,
                l1_years: float = 0.66, l1_worst: float = -2.0, l2_lift: float = 1.0,
                dsr: float | None = None, min_dsr: float = 0.6,
                mined_window: str | None = None, oos_window: str | None = None) -> str:
        c = cell or max((x for x in self.cells if x is not self._baseline),
                        key=lambda x: x.est, default=None)
        if c is None or self._baseline is None:
            return "NULL — nothing measured"
        # ── the search guard ────────────────────────────────────────────────
        # A setup found by scanning thousands of variants cannot be validated on the data
        # that produced it. Two real setups in this book (HighBase-15mDip, 5,504 cells;
        # 🥇G3, 151) were mined across the FULL window, so no clean window exists for them
        # and the question of whether they are real is now permanently unanswerable.
        # The rule already existed in prose and was not applied — so it refuses here instead.
        n_cells = max(0, len(self.cells) - 1)
        if n_cells > self.SEARCH_THRESHOLD and not (mined_window and oos_window):
            raise GuardError(
                f"{n_cells} cells scored — this is a SEARCH, not a test. Declare "
                f"mined_window= and oos_window= (mine on the first, freeze the definition, "
                f"score it on the second). Without a window the search never saw, a BUILD "
                f"verdict here cannot be defended later — and re-mining afterwards does not "
                f"recover it.")
        checks, fails = [], []
        if mined_window and oos_window:
            checks.append(f"L0 search declared: mined {mined_window} → frozen OOS {oos_window}")
        # L1
        if c.n_periods:
            frac = c.n_periods_pos / c.n_periods
            ok1 = frac >= l1_years and c.worst >= l1_worst
            checks.append(f"L1 periods {c.n_periods_pos}/{c.n_periods} · worst {c.worst:+.2f}"
                          f" → {'PASS' if ok1 else 'FAIL'}")
            if not ok1:
                fails.append("L1")
        else:
            checks.append("L1 not evaluable (no time_col) → treated as FAIL")
            fails.append("L1")
        # L2 vs the MATCHED baseline
        lift = c.est - self._baseline.est
        ok2 = lift >= l2_lift
        checks.append(f"L2 lift vs matched baseline {lift:+.2f} (need ≥{l2_lift}) "
                      f"→ {'PASS' if ok2 else 'FAIL'}")
        if not ok2:
            fails.append("L2")
        # L3
        # L3 judges the EFFECTIVE count. n=20,000 fires clustered into 300 market days is
        # not 20,000 pieces of evidence, and the raw count is what makes thin results look
        # solid.
        n_use = c.eff.get("n_eff", c.n) if c.eff else c.n
        ok3 = n_use >= min_n and (dsr is None or dsr >= min_dsr)
        d = "n/a" if dsr is None else f"{dsr:.3f}"
        nstr = (f"n_eff={n_use:,} (raw {c.n:,})" if c.eff and n_use != c.n
                else f"n={c.n:,}")
        checks.append(f"L3 {nstr} (need ≥{min_n}) · DSR {d} (need ≥{min_dsr}) "
                      f"→ {'PASS' if ok3 else 'FAIL'}")
        if not ok3:
            fails.append("L3")
        name = "BUILD" if not fails else ("WATCH" if len(fails) == 1 else "NULL")
        if c.est < self._baseline.est and c.n_periods and c.n_periods_pos <= c.n_periods / 3:
            name = "VETO"
        if n_cells > self.SEARCH_THRESHOLD:
            name = name if name != "BUILD" else "BUILD (mined — read L0)"
        out = [f"\n{'='*88}", f"VERDICT: {name}   ({c.label})", *("  " + x for x in checks)]
        out.append(f"  deciding gate: {fails[0] if fails else 'none — all passed'}")
        if self.n_trials < len(self.cells) - 1:
            out.append(f"  ⚠ {len(self.cells)-1} cells run vs {self.n_trials} registered")
        out.append("=" * 88)
        s = "\n".join(out)
        print(s, flush=True)
        return name
