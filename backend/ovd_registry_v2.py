"""OPENING_VOLUME_DYNAMICS — PRE-OUTCOME DESIGN AMENDMENT V2 / V2.1 · search registry (pre-outcome).

Provenance: POST_V1_SEAL · PRE_OUTCOME · PRE_PATHSIM · USER_DIRECTED_HYPOTHESIS_EXPANSION (V2);
V2.1 = POST_V2_SEAL · PRE_OUTCOME · PRE_PATHSIM with TWO disclosed change classes:
  HARNESS_CORRECTION (zero-outcome guard, current code-digest binding) and
  SAMPLE_AUTHORITY_BINDING_CORRECTION (Logic-1 / Logic-2 30m states inherit their sealed V1 parents' sample
  authorities exactly). V2 feature definitions, thresholds, causal timestamps and entry rules are unchanged.
V1 (SEAL.json, k=293) and V2 (SEAL_V2.json) are immutable provenance.

CLAIM IDENTITY (V2.1 rule): a claim is identified by AT LEAST
  feature/state · threshold/category (bin) · scientific family · sample eligibility authority ·
  causal as-of timestamp · entry rule.
"same feature on ALL sessions" is therefore NOT the same claim as "same feature on the BASE sample".

SAMPLE BINDINGS (V2.1, recovered from the SEALED V1 registry, never from prose):
  Logic 1  OPEN30_SUSTAINED_BUILD_3D  -> the families of its V1 parent feature OPEN1H_RAMP_3D
  Logic 2  FULL_OPENING_RECLAIM_30    -> the families of its V1 parent feature PRIOR_HV_VOLUME_RECLAIM
  Logic 3/4 (decline-context reversal / handoff) -> NEW family C (all eligible sessions; DECLINE_CONTEXT
           lives inside the claim predicate exactly as sealed)
k is derived by exact enumeration of identities; nothing is forced to 299 — a different k is a HARD STOP for review.
"""
from __future__ import annotations
import os, sys, json, hashlib, time                                     # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
V1_SEAL_SHA256 = "18b52e65a4fead1db34f0df104b0e572ea9bcc27f54a98ddd9b1733ba8885bf0"
V2_SEAL_SHA256 = "f05151387120d8069ea8f76c213f8c33d828385f0e80ba9745367c5135d9d639"
# USER DECISION A (2026-09-04): ACCEPT THE EXACT PARENT BINDING. The V2 amendment expected 299; exact
# enumeration gave 300 because the Logic-1 parent OPEN1H_RAMP_3D is registered on "AB" (two sample
# authorities) -> two distinct cells A and B. Narrowing to A-only or B-only merely to keep 299 would add a
# discretionary research decision; k is not a target, the exact enumeration is the authority.
EXPECTED_K = 300
USER_DECISION = dict(id="A_ACCEPT_EXACT_PARENT_BINDING", date="2026-09-04",
                     statement="OPEN30_SUSTAINED_BUILD_3D inherits the exact V1 parent 'AB' authority as two distinct cells A and B; "
                               "FULL_OPENING_RECLAIM_30 inherits the exact V1 parent 'B'; Logic 3/4 stay in family C; global k = 300 by "
                               "exact enumeration; do NOT narrow Logic-1 to A-only or B-only merely to preserve k = 299")
PROVENANCE_V2_1 = ["POST_V2_SEAL", "PRE_OUTCOME", "PRE_PATHSIM", "HARNESS_CORRECTION", "SAMPLE_AUTHORITY_BINDING_CORRECTION"]
CHANGE_CLASSES_V2_1 = {
    "HARNESS_CORRECTION": "corrected zero-outcome filename guard; seal-chain execution authority; current code-digest binding",
    "SAMPLE_AUTHORITY_BINDING_CORRECTION": "OPEN30_SUSTAINED_BUILD_3D inherits the exact V1 parent 'AB' authority as two distinct cells "
                                           "A and B; FULL_OPENING_RECLAIM_30 inherits the exact V1 parent 'B'"}
WORDING_V2_1 = ("V2 feature definitions, thresholds, causal timestamps and entry rules are unchanged. V2.1 corrects sample-authority "
                "binding for two 30m resolution-extension states to reproduce their sealed V1 parent sample authorities exactly.")
from ovd_build import HardStop                                          # noqa: E402  one HardStop class family-wide

# ── family C: the sample of the four genuinely new decline-context claims ──────────────────
FAMILY_C = dict(
    id="C",
    sample="X.eligible — every eligible ticker-session (close >= 5, avg_vol_20d > 0, close*volume >= 3M), independent of "
           "base/breakout; the registered DECLINE_CONTEXT(D) = close(D) < close(D-5) is part of each claim predicate exactly as sealed",
    same_day_control="all OTHER family-C rows on the same ENTRY day on which the claim is EVALUABLE (flag TRUE or FALSE, "
                     "never NULL) with a non-null _pathsim outcome; self excluded; min 20; else edge UNAVAILABLE",
    statistic="day-clustered median edge vs the same-day control at the registered _pathsim config (V1 A7 identity)")
SAMPLE_SQL = {"A": "base AND NOT breakout", "B": "breakout", "C": "TRUE"}     # row masks on X (V1 predicates)

# ── the six new confirmatory states (frozen; constants are NEW_RESEARCH_SPEC) ─────────────
CLAIMS = [
    dict(name="OPEN30_SUSTAINED_BUILD_3D", logic=1, logic_name="MULTI-DAY OPENING PARTICIPATION BUILD", resolution="30m",
         column="open30_sustained_build_3d", parent="OPEN1H_RAMP_3D",
         predicate="RVOL_OPEN30_A[D-2] < RVOL_OPEN30_A[D-1] < RVOL_OPEN30_A[D] AND RVOL_OPEN30_B[D-2] < RVOL_OPEN30_B[D-1] < RVOL_OPEN30_B[D]",
         availability="all six 30m RVOL observations must exist, else UNAVAILABLE (NULL, never FALSE)",
         feature_available="10:30 ET D", row_anchor="D", entry="D+1 open", entry_offset_from_D=1),
    dict(name="FULL_OPENING_RECLAIM_30", logic=2, logic_name="PRIOR HIGH-VOLUME DECLINE EVENT / OPENING RECLAIM", resolution="30m",
         column="full_opening_reclaim_30", parent="PRIOR_HV_VOLUME_RECLAIM",
         predicate="OPEN30_A(D)/OPEN30_A(prior_event) >= 1.0 AND OPEN30_B(D)/OPEN30_B(prior_event) >= 1.0; prior_event = most recent s in "
                   "D-30..D-6 with RVOL_OPEN60(s) >= 2.0 AND close(s-1) < close(s-6) [2.0 / 30 / 5-session separation = NEW_RESEARCH_SPEC; "
                   "not a proven 'stopping day'; the event day's own price response is a separate X feature, never in the selector]",
         availability="prior event must exist and both 30m windows of D and of the event must be available, else UNAVAILABLE",
         feature_available="10:30 ET D", row_anchor="D", entry="D+1 open", entry_offset_from_D=1),
    dict(name="CLOSE60_DOMINANCE", logic=3, logic_name="CLOSE60_DOMINANCE_REVERSAL", resolution="60m",
         column="close60_dominance", parent=None,
         predicate="DECLINE_CONTEXT(D) AND RVOL_CLOSE60(D) > 1.0 AND CLOSE60(D)/OPEN60(D) >= 1.0; DECLINE_CONTEXT(D) = close(D) < close(D-5) "
                   "[the >= 1.0 term is BROAD (X-only census: median CLOSE60/OPEN60 ~ 2.07) — a pre-outcome property of the registered "
                   "hypothesis; any stricter threshold (1.25 / 1.5 / 2.0) after outcome exposure is a NEW post-exposure hypothesis]",
         availability="DECLINE_CONTEXT, RVOL_CLOSE60, CLOSE60, OPEN60 all available (regular session), else UNAVAILABLE",
         feature_available="16:00 ET D", row_anchor="D", entry="D+1 open", entry_offset_from_D=1,
         forbidden_wording=["we are at the bottom", "bottom confirmed", "institutional accumulation"]),
    dict(name="CLOSE30_DOMINANCE", logic=3, logic_name="CLOSE60_DOMINANCE_REVERSAL", resolution="30m",
         column="close30_dominance", parent=None,
         predicate="DECLINE_CONTEXT(D) AND RVOL_CLOSE30_B(D) > 1.0 AND CLOSE30_B(D)/OPEN30_A(D) >= 1.0  (final 30 minutes vs first 30 minutes)",
         availability="DECLINE_CONTEXT, RVOL_CLOSE30_B, CLOSE30_B, OPEN30_A all available, else UNAVAILABLE",
         feature_available="16:00 ET D", row_anchor="D", entry="D+1 open", entry_offset_from_D=1),
    dict(name="CLOSE60_TO_NEXT_OPEN60_HANDOFF", logic=4, logic_name="CLOSE -> NEXT OPEN PARTICIPATION HANDOFF", resolution="60m",
         column="close60_to_next_open60_handoff", parent=None,
         predicate="DECLINE_CONTEXT(D) AND RVOL_CLOSE60(D) > 1.0 AND RVOL_OPEN60(D+1) > 1.0 AND 0.80 <= OPEN60(D+1)/CLOSE60(D) <= 1.25 "
                   "[band 0.80-1.25 = NEW_RESEARCH_SPEC, never optimised after outcome access]",
         availability="all four inputs available (D regular, D+1 opening hour complete), else UNAVAILABLE — a missing D+1 OPEN60 is NULL, not FALSE",
         feature_available="10:30 ET D+1", row_anchor="D+1 (the handoff-observing session)", entry="D+2 open = the open after the anchor row",
         entry_offset_from_D=2,
         allowed_claim="close-to-next-open participation handoff -> subsequent path behaviour from D+2 open",
         forbidden_claim="predicts the same D+1 intraday reversal",
         report_by="gap bucket of D+1 (the anchor row's own gap_bucket) explicitly"),
    dict(name="CLOSE30_TO_NEXT_OPEN30_HANDOFF", logic=4, logic_name="CLOSE -> NEXT OPEN PARTICIPATION HANDOFF", resolution="30m",
         column="close30_to_next_open30_handoff", parent=None,
         predicate="DECLINE_CONTEXT(D) AND RVOL_CLOSE30_B(D) > 1.0 AND RVOL_OPEN30_A(D+1) > 1.0 AND 0.80 <= OPEN30_A(D+1)/CLOSE30_B(D) <= 1.25",
         availability="all four inputs available, else UNAVAILABLE — a missing D+1 OPEN30_A is NULL, not FALSE",
         feature_available="10:00 ET D+1", row_anchor="D+1", entry="D+2 open = the open after the anchor row", entry_offset_from_D=2,
         allowed_claim="close-to-next-open participation handoff -> subsequent path behaviour from D+2 open",
         forbidden_claim="predicts the same D+1 intraday reversal", report_by="gap bucket of D+1 explicitly"),
]
CLAIM_COLUMNS = {c["name"]: c["column"] for c in CLAIMS}
ENTRY_OFFSET = {c["name"]: c["entry_offset_from_D"] for c in CLAIMS}
CLAIM_BY_NAME = {c["name"]: c for c in CLAIMS}

# ── DESCRIPTIVE_ONLY registry: reported, never promoted / rescued / vetoed, never in k ─────
DESCRIPTIVE_ONLY = {
    "OPEN30_INTERNAL_RATIO": "OPEN30_B / OPEN30_A",
    "OPEN30_RVOL_RATIO": "RVOL_OPEN30_B / RVOL_OPEN30_A",
    "CLOSE30_INTERNAL_RATIO": "CLOSE30_B / CLOSE30_A  (= CLOSE30_INTERNAL_LATE_STRENGTH)",
    "CLOSE30_RVOL_RATIO": "RVOL_CLOSE30_B / RVOL_CLOSE30_A",
    "OPEN30_BREADTH": "count(RVOL_OPEN30_A > 1, RVOL_OPEN30_B > 1) in {0,1,2}",
    "CLOSE30_BREADTH": "count(RVOL_CLOSE30_A > 1, RVOL_CLOSE30_B > 1) in {0,1,2}",
    "OPEN30_BUILD_DECOMPOSITION": "BOTH_BUILD / ONLY_FIRST_HALF_BUILDS / ONLY_SECOND_HALF_BUILDS / NEITHER (Logic 1 decomposition; NOT in k)",
    "CLOSE30_SHAPE": "EARLY_CLOSE_SPIKE (ra>=1.5 AND rb<0.6*ra) / LATE_ACCELERATION (rb>=1.2 AND rb>1.25*ra) / SUSTAINED_CLOSE (ra>=1 AND rb>=1) / "
                     "QUIET_CLOSE (ra<1 AND rb<1) / OTHER, ra=RVOL_CLOSE30_A, rb=RVOL_CLOSE30_B — thresholds NEW_RESEARCH_SPEC, descriptive only",
    "RAW_HANDOFF60": "OPEN60(D+1) / CLOSE60(D) distribution", "NORM_HANDOFF60": "RVOL_OPEN60(D+1) / RVOL_CLOSE60(D) distribution",
    "RAW_HANDOFF30": "OPEN30_A(D+1) / CLOSE30_B(D) distribution", "NORM_HANDOFF30": "RVOL_OPEN30_A(D+1) / RVOL_CLOSE30_B(D) distribution",
    "CLOSE60_OVER_OPEN60": "CLOSE60 / OPEN60 distribution", "CLOSE30B_OVER_OPEN30A": "CLOSE30_B / OPEN30_A distribution",
    "NEXT_OPEN30_PERSISTENCE": "RVOL_OPEN30_A(D+1) > 1 AND RVOL_OPEN30_B(D+1) > 1 (auction-only handoff vs participation elevated through 10:30)",
    "NEXT_OPEN30_INTERNAL_RATIO": "OPEN30_B(D+1) / OPEN30_A(D+1)",
    "BASE_STATE": "BASE / BREAKOUT / OTHER stratum of the anchor row (V1 predicates, unchanged)",
    "RVOL_OPEN60_VS_V1_OPEN1H_RVOL": "reconciliation of the V2 regular-session denominator against V1's open1h_rvol",
    "OUT_OF_SAMPLE_STATE_VALUES": "raw state values of Logic-1/2 claims on rows outside their bound sample are descriptive only (status NOT_ELIGIBLE)",
}
STRATA_V2 = dict(year="calendar year", price_bucket="V1", dv_bucket="V1", gap_bucket="V1 (Logic 4: the D+1 anchor row's gap bucket, reported explicitly)",
                 mkt_regime="V1 MKT_OPEN1H terciles", base_state="BASE / BREAKOUT / OTHER (new, descriptive)")
THREE_RESOLUTION_RULE = ("15m = micro timing / where participation begins; 30m = whether participation survives a half-hour interval; "
                         "60m = whether a broader opening/closing regime exists. Resolution layers describe temporal structure; the study "
                         "must NOT choose a 'winning timeframe' and no conclusion may say '30m is best' because of outcome rank.")


# ── identity and bindings ──────────────────────────────────────────────────────────────────
def identity(cell: dict, samples: dict, default_as_of: str, default_entry: str) -> str:
    """Claim identity = feature/state + bin + scientific family + sample eligibility authority + causal
    as-of + entry rule. V1 cells carry family/feature/bin; their sample / as-of / entry are the V1
    registry-level bindings (samples[family], the registry entry rule)."""
    fam = cell["family"]
    return json.dumps(dict(kind=cell.get("kind"), family=fam, sample=cell.get("sample", samples.get(fam)),
                           feature=cell["feature"], bin=cell["bin"],
                           as_of=cell.get("as_of", default_as_of), entry=cell.get("entry", default_entry)), sort_keys=True)


def v1_sealed() -> tuple[dict, dict]:
    seal_p = os.path.join(FAMILY_DIR, "SEAL.json")
    if hashlib.sha256(open(seal_p, "rb").read()).hexdigest() != V1_SEAL_SHA256:
        raise HardStop("V1 SEAL.json bytes changed — V1 must remain immutable provenance")
    seal = json.load(open(seal_p))
    reg_p = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.json")
    if hashlib.sha256(open(reg_p, "rb").read()).hexdigest()[:16] != seal["registry_sha256_16"]:
        raise HardStop("V1 SEARCH_REGISTRY_V1.json digest != SEAL.json")
    return seal, json.load(open(reg_p))


def parent_families(v1_reg: dict, parent: str) -> str:
    """The EXACT family string of the V1 parent feature, read from the sealed V1 registry (e.g. 'AB')."""
    feat = v1_reg["features"].get(parent)
    if not feat:
        raise HardStop(f"V1 parent feature {parent} not in the sealed V1 registry")
    return feat[2]


def bindings(v1_reg: dict) -> list[dict]:
    """One binding per (claim, family). Logic 1/2 inherit their V1 parent's families; Logic 3/4 -> C."""
    out = []
    for c in CLAIMS:
        if c["parent"]:
            fams = parent_families(v1_reg, c["parent"])
            for fam in fams:
                out.append(dict(name=c["name"], family=fam, sample=v1_reg["samples"][fam], sample_sql=SAMPLE_SQL[fam],
                                sample_authority=f"V1 family {fam} (parent feature {c['parent']} registered on '{fams}')"))
        else:
            out.append(dict(name=c["name"], family="C", sample=FAMILY_C["sample"], sample_sql=SAMPLE_SQL["C"],
                            sample_authority="NEW family C (decline-context universe; DECLINE_CONTEXT inside the predicate)"))
    return out


def new_cells(v1_reg: dict) -> list[dict]:
    cells = []
    for b in bindings(v1_reg):
        c = CLAIM_BY_NAME[b["name"]]
        cells.append(dict(kind="state", family=b["family"], feature=c["name"], column=c["column"], bin="TRUE",
                          sample=b["sample"], sample_sql=b["sample_sql"], sample_authority=b["sample_authority"],
                          as_of=c["feature_available"], entry=c["entry"], parent=c["parent"],
                          status_column=status_column(c["name"], b["family"])))
    return cells


def status_column(name: str, family: str) -> str:
    return f"st_{CLAIM_COLUMNS[name]}_{family}"


def enumerate_v2(v1_reg: dict) -> dict:
    """Exact enumeration: V1 cells byte-identical + new cells whose full identity is not already present."""
    v1_cells = v1_reg["cells"]
    if len(v1_cells) != v1_reg["k"]["total"]:
        raise HardStop("V1 registry cells != k")
    samples, as_of, entry = v1_reg["samples"], v1_reg["entry"], v1_reg["entry"]
    v1_ids = {identity(c, samples, as_of, entry) for c in v1_cells}
    added, dedup = [], []
    for c in new_cells(v1_reg):
        if identity(c, samples, as_of, entry) in v1_ids:
            dedup.append(dict(claim=c["feature"], family=c["family"], reason="full identity already present in V1"))
        else:
            added.append(c)
    cells = list(v1_cells) + added
    ids = [identity(c, samples, as_of, entry) for c in cells]
    if len(set(ids)) != len(ids):
        raise HardStop("duplicate claim identities in the enumeration")
    k = len(cells)
    return dict(v1_k=len(v1_cells), states=len(CLAIMS), added=len(added), deduplicated=dedup, k=k,
                hard_stop_for_review=(k != EXPECTED_K), expected_k=EXPECTED_K, cells=cells,
                bindings=[dict(claim=c["feature"], family=c["family"], sample_authority=c["sample_authority"], as_of=c["as_of"], entry=c["entry"]) for c in added],
                v1_cells_sha256_16=hashlib.sha256(json.dumps(v1_cells, sort_keys=True).encode()).hexdigest()[:16],
                cells_sha256_16=hashlib.sha256(json.dumps(cells, sort_keys=True).encode()).hexdigest()[:16],
                bindings_sha256_16=hashlib.sha256(json.dumps(added, sort_keys=True).encode()).hexdigest()[:16])


def assert_registry_covers(builder_claims, cells) -> None:
    """Fixture 23: a confirmatory claim the builder emits MUST be a registered cell (k updated)."""
    reg = {(c["feature"], c["bin"]) for c in cells}
    missing = [n for n in builder_claims if (n, "TRUE") not in reg]
    if missing:
        raise HardStop(f"confirmatory claim(s) not in the registry / k not updated: {missing}")


def assert_not_promotable(name: str) -> None:
    """Fixture 28: a DESCRIPTIVE_ONLY state can never enter promotion / rescue / veto logic."""
    if name in DESCRIPTIVE_ONLY or name not in CLAIM_COLUMNS:
        raise HardStop(f"{name} is DESCRIPTIVE_ONLY (or unregistered) and cannot enter promotion logic")


def main():
    seal, v1 = v1_sealed()
    enum = enumerate_v2(v1)
    reg = dict(
        registry_id="OPENING_VOLUME_DYNAMICS_SEARCH_REGISTRY_V2_1",
        status="DRAFT_PRE_SEAL_HARD_STOP_REVIEW" if enum["hard_stop_for_review"] else "DRAFT_PRE_SEAL",
        amendment="OPENING_VOLUME_DYNAMICS_PRE_OUTCOME_RESEAL_V2_1",
        provenance=PROVENANCE_V2_1, change_classes=CHANGE_CLASSES_V2_1, wording=WORDING_V2_1, user_decision=USER_DECISION,
        parent_v2=dict(seal_sha256=V2_SEAL_SHA256), parent_v1=dict(seal_sha256=V1_SEAL_SHA256, sealed_at=seal["sealed_at"],
                                                                     registry_sha256_16=seal["registry_sha256_16"], k_total=seal["k_total"],
                                                                     cells_sha256_16=enum["v1_cells_sha256_16"]),
        spec=["FEATURE_SPEC_V2.draft.md", "FEATURE_SPEC_V2_1.amendment.md"], generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        claim_identity_rule="feature/state + threshold/category + scientific family + sample eligibility authority + causal as-of + entry rule; "
                            "same feature on ALL sessions != same feature on the BASE/pre-breakout sample",
        inherited_from_v1_unchanged=["samples A/B", "entry (mask on D -> D+1 open)", "exit (book default; horizon sweep descriptive)",
                                     "primary_statistic", "same_day_control", "multiplicity OPTION_1_GLOBAL", "breakout_predicate",
                                     "strata", "features (16)", "interactions (6)", "observation_window_authority",
                                     "report_only_diagnostics (POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5)", "limitations"],
        v1_registry=v1, family_C=FAMILY_C, sample_sql=SAMPLE_SQL, new_claims=CLAIMS, sample_bindings=enum["bindings"],
        entry_offset_from_D=ENTRY_OFFSET, no_new_interactions=True, descriptive_only=DESCRIPTIVE_ONLY, strata=STRATA_V2,
        three_resolution_rule=THREE_RESOLUTION_RULE,
        do_not_tighten="CLOSE60/OPEN60 >= 1.0 stays as sealed although broad (median ~2.07 in the X-only census); stricter thresholds "
                       "after outcome exposure are NEW post-exposure hypotheses unless registered before exposure",
        enumeration=dict(v1_k=enum["v1_k"], states=enum["states"], cells_added=enum["added"], deduplicated=enum["deduplicated"], k=enum["k"],
                         expected_k=EXPECTED_K, hard_stop_for_review=enum["hard_stop_for_review"],
                         rule="k derived by exact identity enumeration; byte-identical claims are not duplicated; no survivor-based shrinking; "
                              "no threshold/band additions after outcome exposure; k is never forced"),
        k=dict(v1=enum["v1_k"], single=v1["k"]["single"], interaction=v1["k"]["interaction"], state=enum["added"], total=enum["k"]),
        cells=enum["cells"])
    body = json.dumps(reg, sort_keys=True, default=str).encode()
    reg["registry_sha256_16"] = hashlib.sha256(body).hexdigest()[:16]
    with open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V2_1.draft.json"), "w") as f:
        json.dump(reg, f, indent=1, default=str)
    by_fam = {}
    for c in enum["cells"]:
        by_fam[c["family"]] = by_fam.get(c["family"], 0) + 1
    mult = dict(multiplicity_id="OPENING_VOLUME_DYNAMICS_MULTIPLICITY_UNIVERSE_V2_1", status=reg["status"],
                registry_sha256_16=reg["registry_sha256_16"], k_total=enum["k"], k_v1=enum["v1_k"], k_added=enum["added"],
                k_by_family=by_fam, parent_v2_seal_sha256=V2_SEAL_SHA256, parent_v1_seal_sha256=V1_SEAL_SHA256,
                correction="DSR bound to the GLOBAL k_total (V1 OPTION_1_GLOBAL retained); strata and DESCRIPTIVE_ONLY quantities are NOT claims",
                rule="k never shrinks; survivors counted against the full k; anything not enumerated here is POST_EXPOSURE_HYPOTHESIS")
    with open(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V2_1.draft.json"), "w") as f:
        json.dump(mult, f, indent=1)
    print(json.dumps(dict(k=reg["k"], by_family=by_fam, bindings=enum["bindings"], deduplicated=enum["deduplicated"],
                          hard_stop_for_review=enum["hard_stop_for_review"], registry_sha=reg["registry_sha256_16"],
                          cells=enum["cells_sha256_16"], bindings_sha=enum["bindings_sha256_16"]), indent=1))


if __name__ == "__main__":
    main()
