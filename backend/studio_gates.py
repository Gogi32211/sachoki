"""Integrity gates, an honest null, feasibility — the corrections of 2026-08-10.

Six changes, four of them from an external review of the architecture and two from our own
incidents. Each is here because a specific thing went wrong, not because it reads well.

1 · FATAL vs SOFT. The design said "≥2 destroy lenses agree → kill", which is a category
    error, and our own bug proves it: the 39.6% duplicate rows would have been found by the
    data-contract lens ALONE. The other four would have honestly reported nothing wrong,
    because the arithmetic was flawless. A 1-of-5 vote would have let it through.
    Lookahead is not an argument against a signal — it means the experiment did not happen.
    So integrity failures are FATAL, alone, and produce a sixth verdict: INVALID.

2 · A NULL THAT KEEPS THE MARKET. `chance_band` used to draw cells i.i.d. from the pool of
    returns. That destroys date clustering — and ours is severe, deff 6.3, so 45,146 trades
    are 7,142 facts. An i.i.d. opponent is too weak and the band comes out too narrow.
    Checked the direction on yesterday's pair study: observed spread 1.930 against a p95 of
    2.118, "inside chance". A wider true band makes that MORE inside — so the conclusions
    survived, but only by luck. Had anything come out positive I would have called it
    OUTSIDE chance against an understated threshold.
    The fix is to permute cell labels WITHIN each date, which destroys any cell→return
    relationship while leaving regime, market-wide moves and event clustering exactly as
    they are.

3 · EVIDENCE LEVEL. 2024-26 is not out-of-sample for us in the sense that matters: we have
    looked at it for months and changed the book on it. A script not reading it is not the
    same as a researcher not knowing it. Four rungs, and only the top two are worth much.

4 · FEASIBILITY. Everything above is about whether a number is true. Three times yesterday
    the number was true and the trade was not: the causal dual-reclaim fires on 47-60% of
    the old signals, T4's "+0.9%" lived in the opening print, Z7's win-rate lift vanished
    under a real exit. A verdict that passes every gate and cannot be traded is worth zero.

5 · HUMAN CHECK. Neither of yesterday's defects was found by a test. Both were found by
    looking at bars. verify_sample is therefore a gate, not a helper.

6 · FAMILY BEFORE THE SEARCH. family_of() collapses 119 setups to 65 mechanically, which is
    good — but the collapsing RULE was invented during the audit, looking at results.
    Multiplicity leaks exactly there, so the family and its declared k are registered with
    the spec.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

# ── 3 · evidence levels ──────────────────────────────────────────────────────
BACKTEST = "backtest"            # searched on this data
HIST_OOS = "historical-oos"      # this script did not read it; the researcher already knew it
FROZEN = "frozen-forward"        # spec sealed before the bars existed
LIVE = "live"                    # traded

EVIDENCE_RANK = {BACKTEST: 0, HIST_OOS: 1, FROZEN: 2, LIVE: 3}
EVIDENCE_NOTE = {
    BACKTEST: "searched on this window — the weakest thing we can say",
    HIST_OOS: "the script did not read it, but we have looked at this period for months "
              "and changed the book on it; epistemically contaminated",
    FROZEN: "spec sealed before these bars existed — k = 1",
    LIVE: "traded",
}


# ── 1 · integrity: fatal, alone, produce INVALID ─────────────────────────────
FATAL = {
    "data_contract": "duplicate grain, broken adjacency, index rows, or unadjusted actions "
                     "— the frame describes a different world",
    "lookahead": "a value was used before it was knowable — the experiment did not happen",
    "survivorship": "the sample cannot contain the names that died, so every worst-case "
                    "reads high",
    "oos_contamination": "the 'out-of-sample' window was already used to shape this idea",
    "undeclared_search": "cells were scored beyond the registered budget, so k is unknown",
    "execution_timing": "the entry or exit price was not obtainable at the decision time",
}


@dataclass
class Integrity:
    """Fatal checks. Any single one invalidates the study — no voting."""
    violations: list = field(default_factory=list)

    def fail(self, kind: str, detail: str):
        if kind not in FATAL:
            raise KeyError(f"{kind!r} is not a fatal class — soft evidence belongs in "
                           f"destroy lenses, which are weighed rather than absolute")
        self.violations.append((kind, detail))

    @property
    def invalid(self) -> bool:
        return bool(self.violations)

    def report(self):
        if not self.violations:
            print("  INTEGRITY ✅ no fatal violation", flush=True)
            return
        print("  INTEGRITY 🔴 INVALID — the result cannot be interpreted:", flush=True)
        for k, d in self.violations:
            print(f"      {k}: {d}\n        ({FATAL[k]})", flush=True)


# ── 2 · the corrected null ───────────────────────────────────────────────────
def chance_band(values, dates, cells, *, n_perm: int = 500, min_n: int = 1,
                seed: int = 0, stat="median") -> dict:
    """How far apart do cells of these sizes land when the labels mean nothing?

    Labels are permuted WITHIN each date. A market-wide up day lifts every event on it, and
    that structure survives the shuffle untouched; what does not survive is any relationship
    between which cell an event belongs to and what it returned. That is the null we want.

    The old version drew rows i.i.d. from the pool, which silently assumed every trade was
    an independent fact. At deff 6.3 that made the band far too narrow, and a narrow band
    is a lenient opponent — it declares findings significant that are not.
    """
    v = np.asarray(values, float)
    d = pd.Series(np.asarray(dates)).astype(str).to_numpy()
    c = pd.Series(np.asarray(cells)).astype(str).to_numpy()
    ok = np.isfinite(v)
    v, d, c = v[ok], d[ok], c[ok]
    if not len(v):
        raise ValueError("nothing to permute")

    sizes = pd.Series(c).value_counts()
    keep = sizes[sizes >= min_n].index
    obs = pd.Series(v).groupby(c).agg(stat)[keep]
    observed_spread = float(obs.max() - obs.min())

    rng = np.random.default_rng(seed)
    order = np.argsort(d, kind="stable")
    v_s, c_s, d_s = v[order], c[order], d[order]
    starts = np.r_[0, np.flatnonzero(d_s[1:] != d_s[:-1]) + 1, len(d_s)]

    spreads, tops = [], []
    for _ in range(n_perm):
        cc = c_s.copy()
        for a, b in zip(starts[:-1], starts[1:]):        # shuffle labels inside each date
            if b - a > 1:
                cc[a:b] = rng.permutation(cc[a:b])
        m = pd.Series(v_s).groupby(cc).agg(stat)
        m = m[[k for k in keep if k in m.index]]
        if len(m) > 1:
            spreads.append(float(m.max() - m.min()))
            tops.append(float(m.max()))
    spreads = np.asarray(spreads)
    p95 = float(np.percentile(spreads, 95))
    return dict(cells=len(keep), n=len(v), dates=len(starts) - 1,
                observed_spread=observed_spread, chance_median=float(np.median(spreads)),
                chance_p95=p95, inside=observed_spread <= p95,
                observed_best=float(obs.max()), chance_best_p95=float(np.percentile(tops, 95)),
                best_cell=str(obs.idxmax()))


def chance_band_iid(values, cells, *, n_perm: int = 500, min_n: int = 1, seed: int = 0,
                    stat="median") -> float:
    """The OLD, too-easy null. Kept only so the difference can be shown, never for a verdict."""
    v = np.asarray(values, float)
    v = v[np.isfinite(v)]
    sizes = pd.Series(np.asarray(cells).astype(str)).value_counts()
    sizes = sizes[sizes >= min_n].to_numpy()
    rng = np.random.default_rng(seed)
    f = np.median if stat == "median" else stat
    sp = [np.ptp([f(rng.choice(v, s, replace=False)) for s in sizes]) for _ in range(n_perm)]
    return float(np.percentile(sp, 95))


# ── 4 · feasibility ──────────────────────────────────────────────────────────
COST_LADDER = (0.05, 0.15, 0.30)     # round-trip %, from tight to realistic-for-small-caps


def feasibility(effect_pct: float, *, n: int, span_days: int, dollar_vol: pd.Series | None,
                hold_bars: float, slots: int = 10, verbose: bool = True) -> dict:
    """Is the true number also a tradeable one?

    No spread model is invented here — we have no quotes. Instead the effect is shown
    against a ladder of round-trip cost assumptions, so the reader sees where it breaks
    rather than trusting a number I made up.
    """
    per_day = n / max(span_days, 1)
    capacity = float(dollar_vol.median()) if dollar_vol is not None and len(dollar_vol) else np.nan
    # a slot is occupied for hold_bars; how much of the fire rate can 10 slots absorb?
    absorbable = slots / max(hold_bars, 1)
    out = dict(fires_per_day=per_day, hold_bars=hold_bars, slots=slots,
               absorbable_per_day=absorbable,
               coverage=min(1.0, absorbable / per_day) if per_day else np.nan,
               median_dollar_vol=capacity,
               net={f"{c:.2f}%": effect_pct - c for c in COST_LADDER},
               survives_cost=effect_pct > COST_LADDER[1])
    if verbose:
        print(f"  FEASIBILITY  {per_day:.1f} fires/day · hold {hold_bars:.0f} bars · "
              f"{slots} slots absorb {absorbable:.1f}/day "
              f"→ {out['coverage']:.0%} of them", flush=True)
        if np.isfinite(capacity):
            print(f"    median $-volume at signal ${capacity/1e6:,.1f}M", flush=True)
        print("    effect after costs: "
              + " · ".join(f"{k} → {v:+.2f}" for k, v in out["net"].items())
              + ("   ✅" if out["survives_cost"] else "   🔴 eaten"), flush=True)
        if out["coverage"] < 0.25:
            print(f"    ⚠ only {out['coverage']:.0%} of fires can be taken — the measured "
                  f"median is not what a portfolio would earn", flush=True)
    return out


# ── 6 · family registered before the search ──────────────────────────────────
def register_family(family_id: str, space: dict, *, mined_window: str, oos_window: str,
                    selection_metric: str) -> dict:
    """Declare the search space BEFORE looking. declared_k comes from the space, not the run.

    family_of() collapses setups mechanically, which is right, but that rule was written
    during an audit with the results already on screen. A family decided after the fact
    leaks exactly the multiplicity it is supposed to bound.
    """
    declared_k = 1
    for v in space.values():
        declared_k *= max(1, len(v) if hasattr(v, "__len__") else 1)
    payload = dict(family_id=family_id, space={k: list(v) for k, v in space.items()},
                   declared_k=declared_k, mined_window=mined_window,
                   oos_window=oos_window, selection_metric=selection_metric)
    payload["search_space_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()).hexdigest()[:12]
    return payload


# ── verdict, with INVALID and an evidence level ──────────────────────────────
VERDICTS = ("BUILD", "BOOSTER", "WATCH", "NULL", "VETO", "INVALID")


def verdict(*, label: str, integrity: Integrity, lift: float, ci: tuple,
            periods_positive: int, periods: int, worst: float, n_eff: int,
            evidence: str = BACKTEST, human_checked: bool = False,
            feasible: dict | None = None, soft: dict | None = None,
            build_bar: float = 1.0) -> str:
    """One of six. Integrity is checked first and alone; nothing outranks it."""
    print("\n" + "=" * 118, flush=True)
    integrity.report()
    if integrity.invalid:
        print(f"VERDICT: INVALID   ({label})", flush=True)
        print(f"  evidence level: {evidence} — irrelevant, the experiment did not happen",
              flush=True)
        print("=" * 118, flush=True)
        return "INVALID"

    if not human_checked:
        print("  🔴 REFUSED: no verdict without verify_sample — neither defect of "
              "2026-08-09 was found by a test; both were found by looking at bars",
              flush=True)
        print("=" * 118, flush=True)
        return "INVALID"

    L1 = periods_positive >= periods - 2 and worst >= -2
    L2 = lift >= build_bar and (ci[0] > 0 or ci[1] < 0)
    L3 = n_eff >= 80
    soft_against = [k for k, v in (soft or {}).items() if v]

    v = ("BUILD" if (L1 and L2 and L3) else
         "WATCH" if (L1 and L2) else
         "VETO" if (L1 and lift <= -build_bar) else "NULL")
    if v == "BUILD" and feasible is not None and not feasible.get("survives_cost", True):
        v = "WATCH"
    if v == "BUILD" and len(soft_against) >= 2:
        v = "WATCH"

    print(f"VERDICT: {v}   ({label})", flush=True)
    print(f"  L1 periods {periods_positive}/{periods} · worst {worst:+.2f} → "
          f"{'PASS' if L1 else 'FAIL'}", flush=True)
    print(f"  L2 lift {lift:+.2f} (need ≥{build_bar:.1f}) · CI [{ci[0]:+.2f},{ci[1]:+.2f}] → "
          f"{'PASS' if L2 else 'FAIL'}", flush=True)
    print(f"  L3 n_eff {n_eff:,} → {'PASS' if L3 else 'FAIL'}", flush=True)
    if soft_against:
        print(f"  soft evidence against: {', '.join(soft_against)}", flush=True)
    print(f"  evidence level: {evidence} — {EVIDENCE_NOTE.get(evidence, '')}", flush=True)
    if evidence == BACKTEST:
        print("    → a backtest-only verdict is a CANDIDATE for freeze, never a promotion",
              flush=True)
    print("=" * 118, flush=True)
    return v
