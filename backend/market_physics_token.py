"""Market Physics · stage 3 — line 10. The landscape token, and only the landscape.

Three stages of screening left one candidate standing:

    stage 1    the axes are independent of each other — more than I predicted
    stage 2    none of them is law-like. The most invariant, decl_R, is a size proxy
               (R² 0.958 on price and share count); the next three collapse into
               volatility, which is real and long known
    stage 2.5  decl_M is re-encoded by the nine lines at OOS R² 0.857, decl_K at 0.586 —
               and the landscape is the one thing the alphabet does not describe

So the token carries the landscape and nothing else. Momentum, stretch, resonance and entropy
are not in it: two are the incumbent under new names, and the others did not survive stage 2.
A token that carried all seven would be mostly a re-encoding wearing a physics label, and its
extra levels would multiply k for nothing.

WHY THE VOLUME PROFILE AND NOT PIVOT LEVELS. A pivot exists BECAUSE price reversed there, so
"price reverses at levels" is partly a tautology. Volume-at-price is defined by where trading
happened; the circularity is gone, and the same object yields both the equilibrium (its mode)
and the barrier on the way out.

NO THRESHOLD IS FITTED, and that is the part worth checking rather than trusting:

    position     ±0.5 and ±2.0 ATR. Round numbers in a unit that is already scale-free
    barrier      the up/down ratio at 2/3 and 3/2. Round numbers again
    density      compared against 1/24 — the share a UNIFORM profile would put in one bin.
                 A principled reference point rather than a median split, which would be a
                 threshold fitted to the sample

Everything above is declared here, hashed into the artifact, and used unchanged. If a cut is
ever moved, the spec hash moves with it and every downstream count is visibly from a different
token.

NOT WRITTEN INTO THE BARS TABLE. The DuckDB `bars` writers are the nightly job and the manual
incremental update, and adding a third would break a single-writer rule that exists for good
reasons. The token materialises to its own parquet keyed by (ticker, date), and consumers join
it.
"""
from __future__ import annotations

import hashlib
import json
import os

import numpy as np
import pandas as pd

import market_physics as MP
import sources as srcs

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_PARQUET = os.path.join(srcs.DATA, "market_physics_line10.parquet")
SPEC_JSON = os.path.join(HERE, "MARKET_PHYSICS_LINE10.json")

TOKEN_VERSION = "line10_landscape_v1"

# ── the cuts. Declared, round, and never fitted ─────────────────────────────
POS_NEAR = 0.5           # ATR from the profile mode: inside this is equilibrium
POS_FAR = 2.0            # ATR: beyond this is far
BARRIER_LOW = 2.0 / 3    # up/down barrier ratio below which escaping UP is easier
BARRIER_HIGH = 3.0 / 2   # ... above which escaping DOWN is easier
UNIFORM_SHARE = 1.0 / MP.PROFILE_BINS    # what a flat profile would put in one bin

POSITION = {"D": f"deeper than {POS_FAR} ATR below the volume mode",
            "B": f"{POS_NEAR}–{POS_FAR} ATR below the mode",
            "E": f"within {POS_NEAR} ATR of the mode — at equilibrium",
            "A": f"{POS_NEAR}–{POS_FAR} ATR above the mode",
            "F": f"further than {POS_FAR} ATR above the mode"}
ESCAPE = {"u": f"barrier up / down < {BARRIER_LOW:.2f} — less structure above",
          "d": f"barrier up / down > {BARRIER_HIGH:.2f} — less structure below",
          "=": "barriers comparable in both directions"}
DENSITY = {"V": f"volume share here below {UNIFORM_SHARE:.3f} — thinner than a flat profile",
           "S": f"volume share here at or above {UNIFORM_SHARE:.3f} — structure under price"}


def spec() -> dict:
    s = {"token_version": TOKEN_VERSION,
         "carries": "the volume-at-price landscape only",
         "why_only_landscape": (
             "stage 2.5 found decl_M re-encoded by the nine lines at OOS R² 0.857 and decl_K at "
             "0.586; stage 2 found no law-like invariant anywhere and decl_R to be a size proxy. "
             "The landscape is the one description the alphabet does not already carry."),
         "cuts": {"pos_near_atr": POS_NEAR, "pos_far_atr": POS_FAR,
                  "barrier_ratio_low": round(BARRIER_LOW, 4),
                  "barrier_ratio_high": round(BARRIER_HIGH, 4),
                  "density_reference": round(UNIFORM_SHARE, 5),
                  "density_reference_is": "the share a UNIFORM profile puts in one bin — a "
                                          "reference point, not a threshold fitted to the sample"},
         "windows": {"profile_bars": MP.WIN_PROFILE, "profile_bins": MP.PROFILE_BINS,
                     "atr": 14},
         "alphabet": {"position": POSITION, "escape": ESCAPE, "density": DENSITY},
         "levels": len(POSITION) * len(ESCAPE) * len(DENSITY),
         "written_to": os.path.basename(OUT_PARQUET),
         "not_written_to": "the DuckDB bars table — single-writer rule",
         "outcome_touched": "none. This is a description of a bar, not a claim about it."}
    s["spec_hash"] = hashlib.sha256(
        json.dumps(s, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
    return s


def tokenise(phys: pd.DataFrame) -> pd.Series:
    """One token per bar from the four landscape columns. NaN where the profile is not yet formed."""
    dist = phys["land_dist_mode_atr"]
    up, dn = phys["land_barrier_up"], phys["land_barrier_dn"]
    dens = phys["land_density_here"]

    pos = pd.Series(np.select(
        [dist <= -POS_FAR, dist <= -POS_NEAR, dist < POS_NEAR, dist < POS_FAR],
        ["D", "B", "E", "A"], default="F"), index=phys.index)

    # ratio of the barrier above to the barrier below; both are in log-potential units and a
    # zero barrier is a real state, so the denominator is floored rather than dropped
    ratio = up / dn.clip(lower=1e-3)
    esc = pd.Series(np.select([ratio < BARRIER_LOW, ratio > BARRIER_HIGH], ["u", "d"],
                              default="="), index=phys.index)
    den = pd.Series(np.where(dens < UNIFORM_SHARE, "V", "S"), index=phys.index)

    tok = pos + esc + den
    bad = ~np.isfinite(dist) | ~np.isfinite(up) | ~np.isfinite(dn) | ~np.isfinite(dens)
    return tok.mask(bad)


def build(universe: str | None = None, verbose: bool = True) -> pd.DataFrame:
    """Compute the landscape for every universe and materialise the token."""
    frames = []
    for uni in ([universe] if universe else ["sp500", "nasdaq", "russell2k"]):
        if verbose:
            print(f"  {uni}…", flush=True)
        phys = MP.load_and_compute(universe=uni, verbose=False)
        phys["land_token"] = tokenise(phys)
        frames.append(phys[["ticker", "date", "land_token", "land_dist_mode_atr",
                            "land_density_here", "land_barrier_up", "land_barrier_dn"]])
    out = pd.concat(frames, ignore_index=True)
    # a name in two indices is one bar; keep one row per (ticker, date)
    out = out.drop_duplicates(subset=["ticker", "date"], keep="first")
    out["date"] = out["date"].astype(str)
    return out


def write(df: pd.DataFrame) -> dict:
    df.to_parquet(OUT_PARQUET, index=False)
    s = spec()
    counts = df["land_token"].value_counts()
    s.update({"rows": int(len(df)), "tokens_present": int(counts.size),
              "coverage_pct": round(100 * float(df["land_token"].notna().mean()), 2),
              "distribution": {k: int(v) for k, v in counts.items()}})
    with open(SPEC_JSON, "w") as f:
        json.dump(s, f, indent=1, sort_keys=True)
    return s


def load() -> pd.DataFrame:
    """The token table, for consumers that want to join it onto bars."""
    return pd.read_parquet(OUT_PARQUET)


if __name__ == "__main__":
    import sys                                                        # noqa: PLC0415
    uni = sys.argv[1] if len(sys.argv) > 1 else None
    df = build(uni)
    s = write(df)
    print("\n" + "=" * 92, flush=True)
    print(f"  LINE 10 · {s['token_version']} · spec {s['spec_hash']}", flush=True)
    print("=" * 92, flush=True)
    print(f"  {s['rows']:,} rows · {s['coverage_pct']}% tokenised · "
          f"{s['tokens_present']} of {s['levels']} levels present", flush=True)
    print(f"\n  {'token':<8}{'n':>10}{'%':>8}   reading", flush=True)
    tot = sum(s["distribution"].values())
    for t, n in sorted(s["distribution"].items(), key=lambda kv: -kv[1]):
        print(f"  {t:<8}{n:>10,}{100 * n / tot:>7.1f}%   "
              f"{POSITION[t[0]].split(' —')[0]} · {ESCAPE[t[1]].split(' —')[0]} · "
              f"{'thin' if t[2] == 'V' else 'structure'}", flush=True)
    print(f"\n  written to {os.path.basename(OUT_PARQUET)} and "
          f"{os.path.basename(SPEC_JSON)}", flush=True)
