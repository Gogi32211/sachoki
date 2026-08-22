"""GANN capability engine — qualifies GATE A. It cannot reach the historical statistic.

WHAT THIS MODULE IS ALLOWED TO DO, and nothing else:

    load the direction-neutral outcome pair
      -> block-nullize it (the pair moves as ONE row, always)
      -> inject the registered additive alternative into the phi = 0 treated component
      -> run the registered 999-permutation max-Z engine
      -> emit detections

WHAT IT MUST NOT BE ABLE TO REACH: the observed phi = 0 statistic on un-nullized Y, any
winner ranking, any survivor selection, any gate-B evaluation. There is no import of a
historical runner here and no code path that computes Z before nullization — that is
asserted by the pre-Y closure, not merely intended.

The synthetic mode exists so the ENGINE can be qualified before a real outcome is touched.

No real Y is read unless load_outcomes() is called with synthetic=False, which the pre-Y
closure never does.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                  # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import gann_rank as RANK                                             # noqa: E402

STATE = os.path.join(os.path.dirname(HERE), "data", "gann_estimand_state.parquet")
CLAIMS = dict(ASC="GANN_ASC_TOUCH_V1", DESC="GANN_DESC_TOUCH_V1",
              CONF="GANN_CONFLUENCE_TOUCH_V1")
N_PERM = 999
DELTAS_PP = (0.5, 1.0, 2.0, 3.0, 4.0, 5.0)
WORLDS = 20
WORLD_NS = "GANN_CAPABILITY_WORLD_V1"
PERM_NS = "GANN_CAPABILITY_PERM_V1"
NULL_NS = "GANN_CAPABILITY_NULL_V1"


def rng_root() -> bytes:
    """Derived from the sealed artifacts — never chosen, never process-dependent."""
    m = hashlib.sha256()
    for part in ("GANN_CAPABILITY_EVIDENCE_V1",
                 ART.file_digest("GANN_X_FREEZE_V1.json"),
                 ART.file_digest("GANN_ESTIMAND_V1.json"),
                 ART.file_digest("GANN_ESTIMAND_STATE_V1.json"),
                 ART.file_digest("GANN_CAPABILITY_PROTOCOL_V1.json"),
                 ART.file_digest("gann_rank.py")):
        m.update(part.encode()); m.update(b"|")
    return m.digest()


def _seed(ns: str, root: bytes, *parts) -> int:
    m = hashlib.sha256()
    m.update(ns.encode()); m.update(b"|"); m.update(root)
    for p in parts:
        m.update(b"|"); m.update(str(p).encode())
    return int.from_bytes(m.digest()[:8], "big")


def seed_manifest(root: bytes) -> dict:
    """Every stochastic decision this engine will make, enumerated up front."""
    man = {}
    for claim in CLAIMS:
        for d in DELTAS_PP:
            for w in range(WORLDS):
                man[f"{claim}|{d:.1f}|{w}|null"] = _seed(NULL_NS, root, claim, d, w)
                man[f"{claim}|{d:.1f}|{w}|p0"] = _seed(PERM_NS, root, claim, d, w, 0)
                man[f"{claim}|{d:.1f}|{w}|p998"] = _seed(PERM_NS, root, claim, d, w, 998)
                man[f"{claim}|{d:.1f}|{w}|world"] = _seed(WORLD_NS, root, claim, d, w)
    return man


def load_state(claim_key: str, limit_rows: int | None = None):
    """The X side only: treated mask, direction, block codes. No outcome."""
    cols = ["ticker", "date", f"{claim_key}_treated", f"{claim_key}_dir",
            f"{claim_key}_block", "liq_half", "vol_half"]
    S = pd.read_parquet(STATE, columns=cols)
    S = S[S[f"{claim_key}_dir"].isin([1, -1])].reset_index(drop=True)
    if limit_rows:
        S = S.head(limit_rows).reset_index(drop=True)
    treated = S[f"{claim_key}_treated"].to_numpy()
    direction = S[f"{claim_key}_dir"].to_numpy()
    block, _ = pd.factorize(S[f"{claim_key}_block"])
    nullblock, _ = pd.factorize(S.date + "|" + S.liq_half + "|" + S.vol_half)
    return dict(n=len(S), treated=treated, direction=direction, block=block,
                nb=int(block.max()) + 1, nullblock=nullblock,
                n_nullblock=int(nullblock.max()) + 1)


def synthetic_outcomes(n: int, seed: int = 0):
    """A stand-in outcome pair for engine qualification. Deliberately NOT market data."""
    rng = np.random.default_rng(seed)
    lo = np.round(np.abs(rng.normal(0.05, 0.04, n)), 6)
    sh = np.round(np.abs(rng.normal(0.05, 0.04, n)), 6)
    return lo, sh


def nullize_pair(long_y, short_y, nullblock, n_nullblock, seed):
    """Permute the PAIR within each nullization block — one row, never two halves.

    The pair index is permuted and BOTH components are gathered with the same index, so a
    row's LONG can never end up beside another row's SHORT.
    """
    rng = np.random.default_rng(seed)
    idx = np.arange(len(long_y))
    out = idx.copy()
    order = np.argsort(nullblock, kind="stable")
    starts = np.searchsorted(nullblock[order], np.arange(n_nullblock))
    ends = np.searchsorted(nullblock[order], np.arange(n_nullblock), side="right")
    for b in range(n_nullblock):
        seg = order[starts[b]:ends[b]]
        if len(seg) > 1:
            out[seg] = seg[rng.permutation(len(seg))]
    return long_y[out], short_y[out], out


def select_component(long_y, short_y, direction):
    """The statistic's Y: LONG rows read the LONG component, SHORT rows the SHORT one."""
    return np.where(direction == 1, long_y, short_y)


def run_world(state, long_y, short_y, delta_pp, root, claim_key, world,
              n_perm=N_PERM, all_claims=None):
    """One capability world: nullize -> inject -> permutation max-Z -> detection."""
    nl, ns, perm_index = nullize_pair(long_y, short_y, state["nullblock"],
                                      state["n_nullblock"],
                                      _seed(NULL_NS, root, claim_key, delta_pp, world))
    inj_l, inj_s = nl.copy(), ns.copy()
    tr = state["treated"]
    d = state["direction"]
    inj_l[tr & (d == 1)] += delta_pp / 100.0
    inj_s[tr & (d == -1)] += delta_pp / 100.0
    y = select_component(inj_l, inj_s, d)
    z_obs, diag = RANK.z_rank(y, tr, state["block"], state["nb"])

    mx = np.empty(n_perm)
    for p in range(n_perm):
        rng = np.random.default_rng(_seed(PERM_NS, root, claim_key, delta_pp, world, p))
        pl, ps, _ = nullize_pair(inj_l, inj_s, state["nullblock"],
                                 state["n_nullblock"], rng.integers(1 << 62))
        yp = select_component(pl, ps, d)
        zs = [RANK.z_rank(yp, tr, state["block"], state["nb"])[0]]
        if all_claims:
            for st2 in all_claims:
                zs.append(RANK.z_rank(select_component(pl, ps, st2["direction"]),
                                      st2["treated"], st2["block"], st2["nb"])[0])
        mx[p] = np.nanmax(zs)
    p95 = float(np.sort(mx)[int(np.ceil(0.95 * n_perm)) - 1])
    return dict(z_obs=float(z_obs), p95=p95, detected=bool(z_obs > p95),
                nullized_long=nl, nullized_short=ns, injected_long=inj_l,
                injected_short=inj_s, maxz=mx, diag=diag, perm_index=perm_index)


if __name__ == "__main__":
    print("gann_capability is a library; the pre-Y closure qualifies it on synthetic data "
          "and the capability run is launched only after that closure passes.")
