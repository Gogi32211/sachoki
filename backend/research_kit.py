"""research_kit — the Sachoki adapter for the quant-study guards.

The universal core lives in ~/.claude/skills/quant-study/scripts/analysis_kit.py and knows
nothing about this project. This file is the thin layer that teaches it our specifics:
edge_replay's frame, the ⚡ATR×12 path-sim, the $-buckets, and DSR against the 119-setup
family. Nothing is re-implemented here — the guards are imported.

WHY THE API LOOKS LIKE THIS. The expensive mistake in this repo is not a bad model, it is a
cell compared against the wrong baseline. So `universe()` path-sims the filtered population
ONCE and every cell is a mask over those same trades. Baseline and cell cannot drift apart,
because they are literally rows of one simulation. That is the whole design.

    from research_kit import EdgeStudy

    st = EdgeStudy("does the T1G token add anything?", n_trials=8)
    st.universe("RS-intact & oversold", lambda g: g["rs_intact"] & (g["rsi_14"] < 45))
    st.describe("rsi_14")
    st.cell("T1G", lambda t: t["sig_t"] == "T1G")
    st.controls({"T1G": lambda t: t["sig_t"] == "T1G",
                 "any green": lambda t: t["sig_t"].str.startswith("T")})
    st.buckets("T1G", lambda t: t["sig_t"] == "T1G")
    st.verdict()

Signal-bar attributes (`sig_*`) are attached to every trade by a vectorised join on the
ENTRY bar's date mapped back to the SIGNAL bar. Getting that off by one silently shifts every
feature by a bar, so it is done once, here, instead of in each study.
"""
from __future__ import annotations

import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import edge_replay as er              # noqa: E402
import overfit_stats as ofs           # noqa: E402
from ledger import log_trial, family_of, trial_count   # noqa: E402
from analysis_kit import (            # noqa: E402
    GuardError, Study, bootstrap_ci, pct_of, purged_splits, rank_ic,
)

__all__ = ["EdgeStudy", "GuardError", "rank_ic", "purged_splits", "pct_of", "bootstrap_ci"]

# our validated defaults — a study that silently changes these is not comparable to the book
ATR_K, STOP, TARGET, TRAIL, MAXH = 12.0, 0.10, 0.25, 0.25, 60
BUCKETS = [(5, 21), (21, 89), (89, 377)]
_FAMILY_CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".dsr_family.json")


def _family(grp, as_of: str) -> list[float]:
    """Sharpes of every registry setup — the trial universe DSR deflates against.
    119 path-sims is ~4 minutes, so it is cached per frame date."""
    if os.path.exists(_FAMILY_CACHE):
        try:
            blob = json.load(open(_FAMILY_CACHE))
            if blob.get("as_of") == str(as_of) and blob.get("sharpes"):
                return blob["sharpes"]
        except Exception:
            pass
    print("  building DSR family (119 setups, once per frame date)...", flush=True)
    out = []
    for _, col in er.SETUPS:
        tr = er._pathsim(grp, col, "trail", STOP, TARGET, TRAIL, MAXH, atr_k=ATR_K)
        if len(tr) >= 30:
            out.append(ofs.sharpe(tr["ret"].to_numpy()))
    try:
        json.dump({"as_of": str(as_of), "sharpes": out}, open(_FAMILY_CACHE, "w"))
    except Exception:
        pass
    return out


class EdgeStudy:
    """A study over edge_replay trades, with the quant-study guards enforced."""

    #: columns lifted from the SIGNAL bar onto each trade, as sig_<name>
    SIG_COLS = ("t", "z", "l_sig", "full_suffix", "vol_bucket", "close", "rsi_14",
                "cci_20", "atr_14", "conso", "rs_intact", "lead_in_lag", "adx_regime",
                "wyc_phase", "sector", "volume")

    def __init__(self, question: str, n_trials: int, months: int = 60,
                 dv_floor: int = 3_000_000, extra_sig_cols: tuple = ()):
        self.grp, self.as_of = er._frame(months, dv_floor)
        self.fam = _family(self.grp, self.as_of)
        self.sig_cols = tuple(self.SIG_COLS) + tuple(extra_sig_cols)
        self.trades: pd.DataFrame | None = None
        self._universe_label = ""
        self._st = Study(question, n_trials=n_trials, outcome="ret_pct",
                         time_col="date", unit="%")
        print(f"  frame as_of {self.as_of} · {len(self.grp)} tickers · "
              f"DSR family {len(self.fam)} setups", flush=True)

    # ── the matched universe: one simulation, every cell a mask over it ───────
    def universe(self, label: str, mask_fn) -> pd.DataFrame:
        """Path-sim the filtered population ONCE. This becomes the baseline, and every
        cell is a subset of these exact trades — they cannot drift apart."""
        for tk, g in self.grp.items():
            g["_U"] = pd.Series(mask_fn(g), index=g.index).fillna(False).astype(bool)
        n_bars = sum(int(g["_U"].sum()) for g in self.grp.values())
        tr = er._pathsim(self.grp, "_U", "trail", STOP, TARGET, TRAIL, MAXH, atr_k=ATR_K)
        if len(tr) == 0:
            raise GuardError("universe produced no trades")
        # ⚠ path-sim enforces a 5-bar gap between entries, so a firing bar can be skipped
        # because an earlier trade is still open. Say so — n will not match a solo run.
        print(f"\n  UNIVERSE «{label}»: {n_bars:,} qualifying bars → {len(tr):,} trades "
              f"({100*len(tr)/max(n_bars,1):.0f}% — the rest fell inside the 5-bar "
              f"minimum spacing of an open trade)", flush=True)
        self._universe_label = label
        self.trades = self._attach_signal_bar(tr)
        self.trades["ret_pct"] = self.trades["ret"] * 100
        self.trades["date"] = pd.to_datetime(self.trades["date_in"])
        self._st.baseline(self.trades, f"BASELINE — {label}, no further condition")
        return self.trades

    def _attach_signal_bar(self, tr: pd.DataFrame) -> pd.DataFrame:
        """Map each trade back to its SIGNAL bar (entry is the next bar's open) and lift
        the descriptor columns onto it. Vectorised join — a per-bar python loop over the
        whole frame costs a minute and invites an off-by-one."""
        frames = []
        for tk, g in self.grp.items():
            cols = [c for c in self.sig_cols if c in g]
            d = g[cols].copy()
            d.columns = [f"sig_{c}" for c in cols]
            d["ticker"] = tk
            # the bar AFTER the signal is the entry bar — that is what date_in holds
            d["date_in"] = g["date"].astype(str).shift(-1).to_numpy()
            frames.append(d.dropna(subset=["date_in"]))
        look = pd.concat(frames, ignore_index=True)
        out = tr.merge(look, on=["ticker", "date_in"], how="left", validate="m:1")
        miss = out["sig_close"].isna().mean() if "sig_close" in out else 1.0
        if miss > 0.02:
            raise GuardError(f"signal-bar join failed on {miss*100:.1f}% of trades — "
                             f"the date alignment is wrong, not a data gap")
        return out

    # ── pass-throughs that keep the guards in force ──────────────────────────
    def describe(self, col: str, value: float | None = None):
        """Population context. Reads from the FRAME (all bars), not just traded ones."""
        s = pd.concat([pd.to_numeric(g[col], errors="coerce")
                       for g in self.grp.values() if col in g], ignore_index=True)
        return self._st.describe(pd.DataFrame({col: s}), col, value)

    def _need(self):
        if self.trades is None:
            raise GuardError("call universe(...) first — a cell without a matched "
                             "baseline is uninterpretable")

    def cell(self, label: str, mask_fn, requires: list[str] | None = None):
        self._need()
        return self._st.cell(self.trades, label,
                             pd.Series(mask_fn(self.trades)).fillna(False),
                             requires=requires)

    def controls(self, components: dict):
        self._need()
        return self._st.controls(
            self.trades,
            {k: pd.Series(f(self.trades)).fillna(False) for k, f in components.items()})

    # ── project-specific honesty checks ──────────────────────────────────────
    def buckets(self, label: str, mask_fn):
        """$-buckets. Pooling hides a real edge in one band and an artefact in another —
        the book's own finding is that the tradeable band is $21-89."""
        self._need()
        base = pd.Series(mask_fn(self.trades)).fillna(False).to_numpy(bool)
        print(f"\n  PRICE BUCKETS · {label}", flush=True)
        for lo, hi in BUCKETS:
            m = base & self.trades["sig_close"].between(lo, hi).to_numpy(bool)
            if m.sum() < 80:
                print(f"    ${lo}-{hi:<4} n={int(m.sum())} — below the L3 minimum of 80, "
                      f"REPORTED not hidden", flush=True)
                continue
            self._st._score(self.trades, f"    ${lo}-{hi}", m)

    def concentration(self, label: str, mask_fn, top=(1, 3, 5, 10)):
        """Does the result survive deleting the biggest contributors? A handful of names
        carrying an 'edge' is the survivorship trap this book has hit before."""
        self._need()
        m = pd.Series(mask_fn(self.trades)).fillna(False).to_numpy(bool)
        sub = self.trades.loc[m]
        if len(sub) < 40:
            print(f"    concentration: n={len(sub)} too thin", flush=True); return
        vc = sub["ticker"].value_counts()
        base = float(np.median(sub["ret_pct"]))
        print(f"\n  CONCENTRATION · {label}  ({sub['ticker'].nunique()} tickers, "
              f"top-10 = {vc.head(10).sum()/len(sub)*100:.0f}% of fires)", flush=True)
        for k in top:
            keep = sub[~sub["ticker"].isin(vc.head(k).index)]
            if len(keep) >= 30:
                print(f"    drop top-{k:<2d} n={len(keep):>6,} "
                      f"med {np.median(keep['ret_pct']):+.2f}%  (was {base:+.2f}%)",
                      flush=True)

    def plateau(self, label: str, mask_factory, values):
        """Sweep one parameter. A winner whose neighbours disagree is noise — this prints
        the neighbourhood so the peak cannot be quoted alone."""
        self._need()
        print(f"\n  PLATEAU · {label}", flush=True)
        out = []
        for v in values:
            m = pd.Series(mask_factory(self.trades, v)).fillna(False).to_numpy(bool)
            if m.sum() < 40:
                print(f"    {v!r:>10}: n={int(m.sum())} thin", flush=True); out.append(np.nan)
                continue
            med = float(np.median(self.trades.loc[m, "ret_pct"]))
            out.append(med)
            print(f"    {v!r:>10}: n={int(m.sum()):>6,}  med {med:+.2f}%", flush=True)
        arr = np.array(out, dtype=float)
        if np.isfinite(arr).sum() >= 3:
            i = int(np.nanargmax(arr))
            peak = float(arr[i])
            nb = [arr[j] for j in (i - 1, i + 1) if 0 <= j < len(arr) and np.isfinite(arr[j])]
            if nb:
                # report the RATIO, not a boolean. An earlier absolute threshold let a
                # 2.2x spike (5.61 against neighbours 2.25/2.84) pass unflagged by 0.035.
                nbmed = float(np.median(nb))
                ratio = peak / nbmed if nbmed > 0 else float("inf")
                verdict = ("PLATEAU" if ratio < 1.35 else
                           "⚠ PEAK, NOT PLATEAU — treat as noise" if ratio >= 1.8 else
                           "⚠ borderline — neighbours only partly agree")
                print(f"    peak {peak:+.2f}% vs neighbour median {nbmed:+.2f}% "
                      f"= {ratio:.2f}×  →  {verdict}", flush=True)
        return arr

    def dsr(self, mask_fn) -> float:
        self._need()
        m = pd.Series(mask_fn(self.trades)).fillna(False).to_numpy(bool)
        r = self.trades.loc[m, "ret"].to_numpy()
        if len(r) < 30:
            return float("nan")
        return float(ofs.dsr(r, self.fam, n_trials=self._st.n_trials)["dsr"])

    def verdict(self, cell=None, mask_fn=None, family: str = "", **kw):
        d = self.dsr(mask_fn) if mask_fn is not None else kw.pop("dsr", None)
        v = self._st.verdict(cell, dsr=d, **kw)
        # EVERY verdict is logged, automatically. A count that depends on remembering to
        # record it is the count that gets forgotten on the study that mattered.
        try:
            c = cell or max((x for x in self._st.cells if x is not self._st._baseline),
                            key=lambda x: x.est, default=None)
            log_trial(
                self._st.question,
                family=family or (c.label if c else self._st.question),
                n_cells=max(1, len(self._st.cells) - 1),
                verdict=v,
                est=(c.est if c else None),
                n_obs=(c.n if c else None),
                n_eff=((c.eff or {}).get("n_eff") if c else None),
                dsr=d, universe=self._universe_label, as_of=str(self.as_of),
                params={"n_trials_declared": self._st.n_trials})
        except Exception as e:                       # logging must never break a study
            print(f"  ⚠ ledger write failed: {e}", flush=True)
        return v
