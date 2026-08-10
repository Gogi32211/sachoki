"""The forward registry: the only place where k = 1.

Today made the case by accident. An exit grid ranked 48 rules on 2021-23 and the ranking had a
−0.04 correlation with 2024-26. Forty-six sequence combinations produced a "best" cell that sat
inside the chance band and a mined→OOS rank correlation of −0.03. The _TZ packages scored
312,189 cells and their grades replicated at 5/8, which is chance. Every one of those searches
was honest about its arithmetic; what none of them had was a window that had not been looked at.

Five years of daily history exist and all of it has been mined — the ledger counts ~5,916 cells
as a floor. There is no unseen slice left to reserve. The only genuinely clean data is the data
that has not arrived, so this registry does one thing: it freezes a specification with today's
date and a hash, and refuses to score it on any bar dated on or before that day. Nothing else in
the toolchain can make that promise.

    freeze(name, spec, claim)   write the spec, the claim it makes, and the date it was sealed
    score(name)                 evaluate ONLY on bars after the freeze date
    report()                    every spec, what has accrued, and when it becomes readable

A spec is executable rather than prose, so it cannot drift: either a column from the book's own
SETUPS registry, or a token pattern the evaluator here interprets. Both are run through the same
⚡ATR×12 path-sim the book trades on, so a forward number is directly comparable to the claim.

The report's most useful column is the last one. A claim of +4% on a setup that fires twice a
week needs months before its interval is narrower than the effect; printing that up front stops
anyone reading a two-week result as evidence. Nothing here can be back-dated: the freeze date is
written once and the scorer takes its lower bound from the file, not from an argument.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import date, datetime

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))

REG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "forward_registry.jsonl")
ATR_K, MAXH, STOP, TARGET, TRAIL = 12.0, 60, 0.10, 0.25, 0.25


def _hash(spec: dict) -> str:
    return hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()[:12]


def _read() -> list[dict]:
    if not os.path.exists(REG):
        return []
    out = []
    with open(REG) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def freeze(name: str, spec: dict, claim: float, metric: str = "median_ret_pct",
           note: str = "", fires_per_year: float | None = None) -> dict:
    """Seal a specification. Append-only; a name can be frozen once."""
    have = {r["name"] for r in _read()}
    if name in have:
        raise ValueError(f"'{name}' is already frozen — a spec cannot be re-sealed. "
                         f"Freeze a new name if the definition changed.")
    rec = dict(name=name, spec=spec, spec_hash=_hash(spec), claim=float(claim),
               metric=metric, note=note, fires_per_year=fires_per_year,
               frozen_on=date.today().isoformat(),
               frozen_at=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    with open(REG, "a") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return rec


# ── evaluation ───────────────────────────────────────────────────────────────
def _mask(g: pd.DataFrame, spec: dict) -> np.ndarray:
    """Turn a frozen spec into a boolean mask. Only these two kinds exist by design —
    an arbitrary expression could be edited after the fact, which is the thing this file
    is built to prevent."""
    kind = spec["kind"]
    if kind == "setup_col":
        col = spec["col"]
        return g[col].fillna(False).to_numpy(bool) if col in g else np.zeros(len(g), bool)
    if kind == "token":
        t = g["t"].astype(str).to_numpy()
        z = g["z"].astype(str).to_numpy()
        tok = np.where((t != "") & (t != "nan"), t, z)
        m = np.isin(tok, spec["tokens"])
        if spec.get("min_price"):
            m &= g["close"].to_numpy(float) >= spec["min_price"]
        if spec.get("max_price"):
            m &= g["close"].to_numpy(float) <= spec["max_price"]
        if spec.get("rs_intact") and "rs_intact" in g:
            m &= g["rs_intact"].fillna(False).to_numpy(bool)
        if spec.get("l_sig"):
            m &= np.isin(g["l"].astype(str).to_numpy(), spec["l_sig"])
        if spec.get("suffix"):
            m &= np.isin(g["fsfx"].astype(str).to_numpy(), spec["suffix"])
        return m
    raise ValueError(f"unknown spec kind {kind!r}")


def score(name: str, grp=None, as_of: str = "") -> dict:
    """Evaluate a frozen spec on bars strictly AFTER its freeze date."""
    import edge_replay as er                                    # noqa: PLC0415
    rec = next((r for r in _read() if r["name"] == name), None)
    if rec is None:
        raise KeyError(name)
    if grp is None:
        grp, as_of = er._frame(60, 3_000_000)
    cut = np.datetime64(rec["frozen_on"])

    col = f"_fwd_{rec['spec_hash']}"
    for tk, g in grp.items():
        m = _mask(g, rec["spec"])
        after = pd.to_datetime(g["date"]).to_numpy() > cut     # strictly after — no back-dating
        g[col] = m & after
    tr = er._pathsim(grp, col, "trail", STOP, TARGET, TRAIL, MAXH, atr_k=ATR_K)
    n = len(tr)
    out = dict(rec)
    out.update(n=n, as_of=as_of, days=(np.datetime64(as_of[:10]) - cut).astype(int)
               if as_of else None)
    if n >= 20:
        from analysis_kit import bootstrap_ci_clustered          # noqa: PLC0415
        d = pd.to_datetime(tr["date_in"]).dt.strftime("%Y-%m-%d")
        lo, hi = bootstrap_ci_clustered(tr["ret"] * 100, d, stat="median", n_boot=400)
        out.update(est=float(tr["ret"].median() * 100), lo=float(lo), hi=float(hi),
                   win=float((tr["ret"] > 0).mean() * 100))
    return out


def _readable_in(rec: dict, n_now: int) -> str:
    """When will this be worth reading? Rough, and deliberately pessimistic.

    A median's standard error is about 1.25·σ/√n. To distinguish a claim of c from zero the
    interval has to be narrower than c, so n ≈ (2·1.25·σ/c)². With σ≈12% for a 60-bar trade
    that is a real number of fires, and at a few per week it is months.
    """
    c = abs(rec.get("claim") or 0)
    fpy = rec.get("fires_per_year")
    if c < 0.05:
        return "a null — read it when the interval is tight around 0"
    need = int((2 * 1.2533 * 12.0 / c) ** 2)
    if not fpy:
        return f"needs ~{need:,} fires"
    left = max(0, need - n_now)
    return f"needs ~{need:,} fires · {left / fpy * 365:.0f} more days at its historical rate"


def report(grp=None) -> pd.DataFrame:
    import edge_replay as er                                    # noqa: PLC0415
    recs = _read()
    if not recs:
        print("  forward registry is empty", flush=True)
        return pd.DataFrame()
    if grp is None:
        grp, as_of = er._frame(60, 3_000_000)
    else:
        as_of = ""
    rows = []
    print("=" * 126, flush=True)
    print(f"  FORWARD REGISTRY — scored only on bars after each freeze date  "
          f"(frame as_of {as_of})", flush=True)
    print("=" * 126, flush=True)
    print(f"  {'spec':26s} {'frozen':>11s} {'days':>5s} {'n':>6s} {'claim':>7s} "
          f"{'forward':>8s} {'CI':>17s}   status", flush=True)
    for r in recs:
        s = score(r["name"], grp=grp, as_of=as_of)
        rows.append(s)
        if s["n"] < 20:
            status = _readable_in(r, s["n"])
            print(f"  {r['name']:26s} {r['frozen_on']:>11s} {s['days'] or 0:>5d} "
                  f"{s['n']:>6,} {r['claim']:>+7.2f} {'—':>8s} {'—':>17s}   {status}",
                  flush=True)
        else:
            keeps = "✅ holds" if s["lo"] > 0 and r["claim"] > 0 else (
                "✅ stayed null" if abs(r["claim"]) < 0.05 and s["lo"] <= 0 <= s["hi"]
                else "🔴 does not hold")
            print(f"  {r['name']:26s} {r['frozen_on']:>11s} {s['days']:>5d} {s['n']:>6,} "
                  f"{r['claim']:>+7.2f} {s['est']:>+8.2f} [{s['lo']:>+6.2f},{s['hi']:>+6.2f}]"
                  f"   {keeps} ({s['est'] / r['claim'] if r['claim'] else float('nan'):.0%} "
                  f"of claim)", flush=True)
    return pd.DataFrame(rows)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seed":
        # Claims are MEASURED, never placeholders (measure_claims.py). A spec sealed with a
        # made-up 0.0 would be reported as "a null" forever, which is a different statement
        # from "measured at zero" — and two of these genuinely measure at zero.
        SEED = [
            ("🔄DualReclaim🏆RS", dict(kind="setup_col", col="E_dualrec_rs"), 4.000, 540,
             "causal rebuild 2026-08-10; was +4.246 with the ±w lookahead. CI [+2.59,+5.61], 5/5yr"),
            ("🔄DualReclaim deep", dict(kind="setup_col", col="E_dualrec_rs_deep"), 3.944, 139,
             "causal rebuild; 18% of the old +4.804 came from bars that had not printed. "
             "CI [+1.63,+6.86], 5/5yr"),
            ("Z7 raw", dict(kind="token", tokens=["Z7"], min_price=5.0), 0.069, 3622,
             "the +3.76pp win-rate lift vs a matched control does NOT survive the book's own "
             "path-sim: +0.069, CI [-0.22,+0.39], 4/6yr. Frozen to settle it forward"),
            ("Z7·L5·ED", dict(kind="token", tokens=["Z7"], min_price=5.0, l_sig=["L5"],
                              suffix=["ED"]), 0.641, 153,
             "the one _TZ package finding that replicated on win rate; through path-sim "
             "+0.641, CI [-1.16,+2.49], 3/6yr — not yet an edge"),
            ("T6 alone", dict(kind="token", tokens=["T6"], min_price=5.0), 0.299, 8643,
             "NULL at every horizon against a matched control; +0.299 CI [-0.23,+0.98] 4/6yr "
             "through path-sim. Frozen to watch that it stays null"),
        ]
        for name, spec, claim, fpy, note in SEED:
            try:
                r = freeze(name, spec, claim, note=note, fires_per_year=fpy)
                print(f"  frozen  {name:26s} hash {r['spec_hash']}  claim {claim:+.3f}")
            except ValueError as e:
                print(f"  skip    {name:26s} {e}")
        print(f"\n  registry: {REG}")
    else:
        report()
