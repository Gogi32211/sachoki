"""Deriving the fingerprints of what has ALREADY been exposed, by measuring rather than asserting.

Neither artifact on disk records its own evidence identity. The oracle records a population and
31 θ; the qualification records 289,467 rows and 31 intervals; neither records a content digest,
a coverage window, or the outcome definition — those live in the code that produced them. So a
registry built by reading the two JSON files would be a registry of declarations.

The alternative is to open the data the two runs used and measure it. That is legitimate here in
a way that re-running the estimator is not: a digest of the outcome vector produces no estimate,
no interval and no cell, so nothing about the 31 claims becomes more available than it already
was. Measuring what the data IS is not asking it a question.

    oracle       population {rows: 289467, dates: 1304}   ← declared, 2026-08-12
    qualification outcome_rows_total 289467               ← declared, 2026-08-13
    measured now  rows · dates · digest · coverage        ← this module

The declared numbers become a CHECK rather than an input. If the measurement disagrees, the
parquet moved under two artifacts that claim to describe it, and this module refuses to register
instead of quietly recording the newer shape — which would be the same laundering one level down,
attaching a fresh lineage to old claims.

WHY BOTH EXPOSURES SHARE ONE FINGERPRINT. `v2_core_oracle` computes `f.theta(v_real)` after
`CL.load_base()` and `E.Support(O, dates, g1)`; `v2_real_y_qualification` calls the identical
three. Same data, same population, same estimand, same 31 claims — one evidence item, exposed
twice. The second exposure added intervals to claims already exposed, which is why `k_exposed`
is 31 rather than 62, and the fingerprint says so structurally instead of by argument.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import evidence_fingerprint as FP                                     # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ORACLE = os.path.join(HERE, "V2_CORE_ORACLE.json")
QUALIFICATION = os.path.join(HERE, "V2_REAL_Y_QUALIFICATION.json")
EXPOSURE_LOG = os.path.join(HERE, "EVIDENCE_EXPOSURE_LOG.json")

SNAPSHOT = "opportunities-parquet-2026-08-12"
OUTCOME_DEFINITION = "O['ret'] * 100 within frozen strata"
SUPPORT_POLICY_HASH = "6f825ca4763fea76"
ESTIMAND = "stratified_within_setup_median_difference_pp"


class ExposedEvidenceError(RuntimeError):
    """The data no longer matches what the exposed artifacts say it was."""


def _read(p: str) -> dict:
    with open(p) as f:
        return json.load(f)


def declared_population() -> dict:
    """What the two artifacts say, side by side. Disagreement between them is itself a finding."""
    o, q = _read(ORACLE), _read(QUALIFICATION)
    return {"oracle_rows": int(o["population"]["rows"]), "oracle_dates": int(o["population"]["dates"]),
            "qualification_rows_total": int(q["outcome_rows_total"]),
            "qualification_rows_used": int(q["outcome_rows_used"])}


def measure_lineage():
    """Open the data both runs used and take its identity. No estimator is called."""
    import combo_lab as CL                                            # noqa: PLC0415
    _, v_real, dates = CL.load_base(verbose=False)
    return FP.lineage_from_vector(v_real, dates, SNAPSHOT)


def assert_matches_declarations(lin) -> dict:
    """The declared numbers are a check on the measurement, never a substitute for it."""
    d = declared_population()
    problems = []
    if lin.rows != d["oracle_rows"]:
        problems.append(f"rows measured {lin.rows}, oracle declared {d['oracle_rows']}")
    if lin.dates != d["oracle_dates"]:
        problems.append(f"dates measured {lin.dates}, oracle declared {d['oracle_dates']}")
    if lin.rows != d["qualification_rows_total"]:
        problems.append(f"rows measured {lin.rows}, qualification declared "
                        f"{d['qualification_rows_total']}")
    if problems:
        raise ExposedEvidenceError(
            "the data does not match the artifacts that claim to describe it: "
            + "; ".join(problems)
            + ". Registering the newer shape against the older claims would attach a fresh "
              "lineage to evidence produced from something else, which is the laundering this "
              "registry exists to catch, one level down.")
    return d


def claim_cells() -> list:
    """The 31 OPPORTUNITY_LEVEL cells, taken from the oracle and cross-checked against the
    qualification. Two artifacts agreeing on the claim set is worth more than either alone."""
    o, q = _read(ORACLE), _read(QUALIFICATION)
    a, b = sorted(o["cells"]), sorted(c["cell"] for c in q["cells"])
    if a != b:
        raise ExposedEvidenceError(
            f"the two exposures name different claim sets ({len(a)} vs {len(b)}); they cannot "
            f"share an evidence identity")
    return a


def fingerprint(lineage=None) -> FP.EvidenceFingerprint:
    lin = lineage if lineage is not None else measure_lineage()
    assert_matches_declarations(lin)
    return FP.EvidenceFingerprint(
        data_lineage=lin, outcome_definition=OUTCOME_DEFINITION,
        population=f"support_policy:{SUPPORT_POLICY_HASH}",
        claim_identity=FP.claim_set_identity(claim_cells()), estimand=ESTIMAND)


def exposure_ids() -> list:
    return [e["exposure_id"] for e in _read(EXPOSURE_LOG)["exposures"]]


def exposed_theta() -> dict:
    """The 31 θ, read from the immutable oracle. Nothing is recomputed to obtain them."""
    per = _read(ORACLE)["per_cell"]
    return {c: float.fromhex(r["theta_real_hex"]) for c, r in per.items()}


def exposed_intervals() -> dict:
    """The 31 intervals, read from the immutable qualification report."""
    return {c["cell"]: tuple(c["interval"]) for c in _read(QUALIFICATION)["cells"]}


def register(*, derived_at: str, note: str = "") -> dict:
    """Write both exposures into the fingerprint registry, under one identity."""
    lin = measure_lineage()
    declared = assert_matches_declarations(lin)
    fp = fingerprint(lin)
    log = _read(EXPOSURE_LOG)["exposures"]
    reg = FP.FingerprintRegistry.load()
    entries = []
    for e in log:
        entries.append(reg.append({
            "fingerprint": fp.fingerprint(),
            "exposure_id": e["exposure_id"],
            "evidence_status": e["evidence_status"],
            "components": fp.components(),
            "data_lineage": fp.data_lineage.as_dict(),
            "derived_from": {"artifacts": ["V2_CORE_ORACLE.json", "V2_REAL_Y_QUALIFICATION.json"],
                             "declared_population": declared,
                             "measured_at": derived_at,
                             "measurement": "digest and coverage of the outcome vector; no "
                                            "estimator was called",
                             "lineage_completeness": "COMPLETE"},
            "note": note}))
    reg.save()
    return {"fingerprint": fp.fingerprint(), "exposures": [e["exposure_id"] for e in log],
            "entries": len(reg.entries), "lineage": fp.data_lineage.as_dict(),
            "declared_population": declared}


if __name__ == "__main__":
    at = sys.argv[1] if len(sys.argv) > 1 else "unspecified"
    out = register(derived_at=at,
                   note="both exposures ran CL.load_base + E.Support + f.theta over the same "
                        "data; one evidence item, exposed twice")
    print(json.dumps(out, indent=1, sort_keys=True), flush=True)
    print(f"\n  registry entries {out['entries']} · fingerprint {out['fingerprint']}", flush=True)
    print(f"  sha of registry file "
          f"{hashlib.sha256(open(FP.REGISTRY, 'rb').read()).hexdigest()[:16]}", flush=True)
