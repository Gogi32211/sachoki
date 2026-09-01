"""B — monthly X-ONLY support census over the four frozen forward hubs.

X-only. No Y value, no Y availability, no outcome artifact is ever opened.

TWO DISTINCT LAYERS, deliberately not conflated:

  X-POSITIVE SEMANTICS (Option A, frozen in FORWARD_X_SUPPORT_COUNT_SPEC_V1)
      token_A at its registered position AVAILABLE_VALID and TRUE
      token_B at its registered position AVAILABLE_VALID and TRUE
      the availability of any UNRELATED feature is irrelevant

  EPISODE STRUCTURAL INTEGRITY (an upstream invariant, NOT a predicate filter)
      every admitted (security, session) that exists in Stage-A must carry all four
      opening-hour positions: presence mask == 1111.
      The qualified coarse producer constructs a session's scheduled 15m intervals from
      the XNYS schedule; missing underlying minutes change coverage_state to
      UNOBSERVED_CONTAMINATED but do NOT delete the scheduled interval row. A mask of
      1110 or 1101 is therefore not a normal episode state — it is a pipeline/input
      invariant violation, and it HOLDS rather than being counted, zeroed, or silently
      resolved by picking a universe.

Import-side-effect free.
"""
from __future__ import annotations
import os, json, hashlib                                               # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(HERE, "massive_new_family_runtime")

TOKEN_DICT = os.path.join(RUNTIME, "TOKEN_DICTIONARY_123.json")
MANIFEST_V2 = os.path.join(RUNTIME, "CANONICAL_69_MANIFEST_V2.json")
COHORT_FILE = os.path.join(RUNTIME, "FORWARD_COHORT_476_security_key_v1.txt")
COHORT_DIGEST = "2d1ec872dcf74df4"

ET = ["09:30", "09:45", "10:00", "10:15"]
POSN = ["M1", "M2", "M3", "M4"]
PAIRS = [(0, 1), (1, 2), (2, 3)]
PAIRN = ["M1->M2", "M2->M3", "M3->M4"]
N_TOKENS = 123
MASK_OK = (1, 1, 1, 1)

HUBS = {"H1_VOL_BUCKET": dict(file="HUB_H1_VOL_BUCKET_ids.npy", n=460, required=230,
                              digest="b0d6f9b4d67d2b6d"),
        "H2_GAP_V": dict(file="HUB_H2_GAP_V_ids.npy", n=173, required=87,
                         digest="c814b6ddaf166f3d"),
        "H3_VOL_CONSTRUCT": dict(file="HUB_H3_VOL_CONSTRUCT_ids.npy", n=61, required=31,
                                 digest="b4d902efd7103dfb"),
        "H4_LSIG_M1_STAR": dict(file="HUB_H4_LSIG_M1_STAR_ids.npy", n=48, required=24,
                                digest="938a6f0f5eb6a9b7")}
X_POSITIVE_FLOOR = 100          # frozen in FORWARD_HUB_PRESPEC_V1


class BHold(Exception):
    """Fail-closed. Never downgraded."""


def _sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def load_cohort(path=COHORT_FILE):
    if not os.path.exists(path):
        raise BHold(f"GUARD cohort_missing: {path}")
    blob = open(path, "rb").read()
    keys = [l for l in blob.decode().split("\n") if l]
    d = hashlib.sha256(blob).hexdigest()[:16]
    if d != COHORT_DIGEST:
        raise BHold(f"GUARD cohort_digest: {d} != sealed {COHORT_DIGEST}")
    return sorted(keys)


def load_tokens(path=TOKEN_DICT):
    if not os.path.exists(path):
        raise BHold(f"GUARD token_dictionary_missing: {path}")
    t = json.load(open(path))
    if len(t) != N_TOKENS:
        raise BHold(f"GUARD token_count: {len(t)} != {N_TOKENS}")
    return t


def load_hub(name, runtime=RUNTIME):
    import numpy as np
    meta = HUBS[name]
    p = os.path.join(runtime, meta["file"])
    if not os.path.exists(p):
        raise BHold(f"GUARD hub_membership_missing: {name} at {p}")
    ids = np.load(p)
    d = hashlib.sha256(ids.astype("int64").tobytes()).hexdigest()[:16]
    if d != meta["digest"]:
        raise BHold(f"GUARD hub_membership_digest: {name} {d} != sealed {meta['digest']}")
    if len(ids) != meta["n"]:
        raise BHold(f"GUARD hub_membership_size: {name} {len(ids)} != {meta['n']}")
    return ids


# ── EPISODE STRUCTURAL INTEGRITY ────────────────────────────────────────────
def assert_episode_structure(mask_counts):
    """mask_counts: {(m1,m2,m3,m4): n}. Anything other than 1111 HOLDS.

    This is NOT the X-positive predicate and NOT a four-position universe filter. It is
    an upstream invariant on the input surface. Deciding what to do with a partial
    opening hour AFTER seeing forward X would be a decision taken with the data in view;
    it is settled here instead."""
    bad = {k: v for k, v in mask_counts.items() if tuple(k) != MASK_OK}
    if bad:
        raise BHold(f"UPSTREAM_EPISODE_STRUCTURE_VIOLATION: presence masks other than "
                    f"1111 found {bad}. Do NOT count, do NOT classify as negative, do NOT "
                    f"pick a universe — HOLD for source investigation.")
    return True


# ── X-POSITIVE SEMANTICS (Option A) ─────────────────────────────────────────
def token_hit(feature_type, bval, sval, registered_value):
    """AVAILABLE_VALID is checked by the caller; this is the predicate only."""
    if feature_type == "BOOLEAN":
        return bval == 1
    return (sval if sval is not None else "") == registered_value


# ── PRODUCTION / DRY-RUN MODE ───────────────────────────────────────────────
def require_production_authorities(maturity_authority=None, evidentiary_start=None,
                                   a3_digest=None):
    """Production mode is BLOCKED until the source-maturity authority exists. There is no
    workaround and none will be built."""
    missing = [n for n, v in (("final_source_maturity_authority", maturity_authority),
                              ("forward_evidentiary_start", evidentiary_start),
                              ("A3_authority_digest", a3_digest)) if not v]
    if missing:
        raise BHold(f"PRODUCTION_MODE_BLOCKED: missing {missing}. The settlement study has "
                    f"not established M; no test value may substitute for it.")
    if isinstance(maturity_authority, dict) and maturity_authority.get("non_evidentiary"):
        raise BHold("PRODUCTION_MODE_BLOCKED: a DRY_RUN/NON_EVIDENTIARY maturity value was "
                    "presented to production mode")
    return True


def parse_claim(claim_id):
    """'tok@M1->tok@M2' -> (token_a, posA, token_b, posB). Position is part of identity."""
    try:
        left, right = claim_id.split("->")
        ta, pa = left.rsplit("@", 1)
        tb, pb = right.rsplit("@", 1)
    except ValueError:
        raise BHold(f"GUARD claim_grammar: cannot parse {claim_id!r}")
    if pa not in POSN or pb not in POSN:
        raise BHold(f"GUARD claim_position: {claim_id!r} names {pa}/{pb}")
    if (POSN.index(pa), POSN.index(pb)) not in PAIRS:
        raise BHold(f"GUARD claim_pair: {pa}->{pb} is not a registered adjacent pair")
    return ta, pa, tb, pb


def x_positive_for_claims(frame, claims, tokens):
    """frame: pos4-shaped rows for ONE security — columns session_date, et, feature,
    bval, sval, availability. Returns {claim_id: X_positive_episode_n}.

    Option A throughout: only the two registered positions are consulted. The presence
    mask is asserted separately, upstream — it is not a term in this predicate."""
    import numpy as np
    if frame.duplicated(subset=["session_date", "et", "feature"]).any():
        raise BHold("GUARD duplicate_position_row: repeated (session_date, et, feature)")
    sess = sorted(frame.session_date.unique())
    SI = {s: i for i, s in enumerate(sess)}
    n = len(sess)
    hit = {}
    fr = frame.to_dict("records")
    for r in fr:
        if r["availability"] != "AVAILABLE_VALID":
            continue
        f = r["feature"]
        for tid, meta in tokens.items():
            if meta["source_feature"] != f:
                continue
            if token_hit(meta["feature_type"], r["bval"], r["sval"], meta.get("value")):
                hit.setdefault((r["et"], tid), np.zeros(n, bool))[SI[r["session_date"]]] = True
    out = {}
    for cid in claims:
        ta, pa, tb, pb = parse_claim(cid)
        if ta not in tokens or tb not in tokens:
            raise BHold(f"GUARD token_unknown: {cid}")
        A = hit.get((ET[POSN.index(pa)], ta))
        Bv = hit.get((ET[POSN.index(pb)], tb))
        out[cid] = 0 if (A is None or Bv is None) else int((A & Bv).sum())
    return out


def self_digest():
    return _sha(os.path.abspath(__file__))


if __name__ == "__main__":
    c = load_cohort(); t = load_tokens()
    print(f"cohort {len(c)} · tokens {len(t)}")
    for h in HUBS:
        print(f"  {h:20s} {len(load_hub(h)):>4} members · required {HUBS[h]['required']}")
    print(f"module digest {self_digest()[:16]}")
