"""T5_FORWARD_VALIDATION_V1 — freeze the prospective test. No forward outcome exists yet.

Everything before this file is HISTORICAL evidence, however carefully it was gated. This is
the first test whose outcome has not been observed by anyone at the moment of freezing.

THE HONEST DESCRIPTION OF WHAT THIS IS

    The forward families were selected FROM historical survivors, but their membership and
    decision rules were frozen before any post-2026-08-20 outcome was observed.

That is not the same as an untouched hypothesis, and the artifact says so rather than calling
this a clean prospective study.

THE STOPPING RULE IS AN INFORMATION HORIZON, NOT A DATE

Locking on a calendar date lets market activity decide the sample size. The lock is the first
20,000 forward T5 episodes whose MFE_10D is fully observable. A quiet quarter then costs
calendar time, not power, and nobody gets to look at 12,000 episodes and decide that is
enough.

SUPPORT ELIGIBILITY IS DECLARED NOW, AT THE HISTORICAL FLOORS

No new threshold is invented for the forward window: 300 treated and 300 control in
maturity-complete forward episodes with overlap blocks, exactly as historically. A claim below
that is INSUFFICIENT_FORWARD_SUPPORT and leaves the family. Support is a property of X, so
that exclusion never consults an outcome — but k_eff shrinks, so k_eff is reported and the
family max-Z is taken over the eligible members only.

ACCRUAL IS BLIND

X may be watched: episode counts, maturity, coverage, missingness, per-claim occurrence,
data-quality failures. Y may not: no MFE, no Z, no theta, no ret, no survivor status, until
the lock condition is met.

THREE STATUSES, BECAUSE "VALIDATED" HIDES THE DISTINCTION THAT MATTERS

    FORWARD_STATISTICAL_PASS        survives its prospective family max-Z control
    FORWARD_DIRECTIONAL_CONSISTENCY theta_raw > 0, descriptive only
    TRADEABILITY                    NOT TESTED BY THIS PROTOCOL

The historical work already showed an MFE association whose median translation into a 10-day
terminal return was about 20%. Forward replication of the association still says nothing about
a profitable exit policy.

THE CROSS-RESOLUTION POCKET IS NOT IN THE PRIMARY GATE

VOL_W <-> VA-> is a post-exposure historical observation. Folding it in as a hand-picked
refinement of the 171-medoid family would be exactly the adaptation this protocol exists to
prevent. It may be registered separately as a SECONDARY_FORWARD_HYPOTHESIS with its own
claim, joint-event definition, outcome and statistic.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import pandas as pd                                                    # noqa: E402
import t5_artifact as ART, t5_dna as D                                 # noqa: E402

OUT   = "T5_FORWARD_VALIDATION_V1.json"
FAM   = os.path.join(D.ROOT, "data", "t5_forward_families.parquet")
CUTOFF = "2026-08-20"
LOCK_N = 20000


def main():
    ART.smoke_test(verbose=False)
    COMP = json.load(open("T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1.json"))
    SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
    ORD15 = json.load(open("T5_15M_CLAIM_ORDER_V2.json"))
    V2 = json.load(open("SEQUENCE_INFERENCE_V2.json"))

    # ── the two frozen families ────────────────────────────────────────────
    rows = []
    for r in COMP["per_cluster"]:
        rows.append(dict(family="1H", cluster=int(r["cluster_1h"]),
                         claim_id=r["representative"],
                         historical_support=int(r["n_1h_in_U"])))
    REP = pd.read_parquet(os.path.join(D.ROOT, "data",
                                       "t5_15m_cluster_representatives.parquet"))
    for r in REP.itertuples():
        rows.append(dict(family="15M", cluster=int(r.cluster), claim_id=r.claim_id,
                         historical_support=int(r.support)))
    F = pd.DataFrame(rows)
    F.to_parquet(FAM, index=False)
    n1h = int((F.family == "1H").sum()); n15 = int((F.family == "15M").sum())
    fam_digest = hashlib.sha256("|".join(F.family + ":" + F.claim_id).encode()).hexdigest()[:16]

    body = dict(
        spec_id="T5_FORWARD_VALIDATION_V1",
        status="FROZEN — NO POST-CUTOFF OUTCOME EXISTS AT THE TIME OF THIS FREEZE",

        what_this_is="The forward families were selected FROM historical survivors, but their "
                     "membership and decision rules were frozen before any post-2026-08-20 "
                     "outcome was observed. This is NOT an untouched hypothesis and must not "
                     "be described as one.",
        what_came_before="every prior artifact in this chain is HISTORICAL evidence, however "
                         "carefully gated",

        discovery_cutoff=dict(
            last_eligible_historical_signal_session=CUTOFF,
            rationale="the historical database reached 2026-08-20 on the update that "
                      "completed before this freeze"),

        forward_eligibility=dict(
            signal_session=f"> {CUTOFF}",
            no_backfill="an episode whose signal session is on or before the cutoff can never "
                        "enter, regardless of when it is processed",
            pit_semantics="same point-in-time availability and entry semantics as the "
                          "historical work: NEXT_SESSION_OPEN_V1",
            outcome="MFE_10D, identical definition",
            maturity="10 observable sessions after entry; an episode is counted only once its "
                     "MFE_10D is fully observable"),

        families=dict(
            H1=dict(k=n1h, members=[r["claim_id"] for r in rows if r["family"] == "1H"],
                    source="the 4 frozen 1H overlap-cluster medoids"),
            M15=dict(k=n15, source="the 171 frozen 15m survivor-cluster medoids",
                     members_artifact=os.path.basename(FAM)),
            separate="1H and 15m are SEPARATE forward families. Neither is an independent "
                     "confirmation of the other — they read the same episode universe and the "
                     "same outcome.",
            digest=fam_digest),

        statistic=dict(
            primary="blockwise bounded Mann-Whitney / AUC Z_rank, unchanged",
            direction="one-sided positive",
            multiplicity_1h=f"search-wide max Z_rank over the {n1h} fixed 1H claims",
            multiplicity_15m=f"search-wide max Z_rank over the {n15} fixed 15m claims",
            raw_theta="magnitude only; a large theta cannot rescue a Z failure",
            denominator="exact combinatorial null variance with the rank-sum tie correction, "
                        "rebuilt on the forward multiset",
            blocks="T5 date x pre-window liquidity LOW/HIGH x pre-window volatility LOW/HIGH, "
                   "anchored at the T5-2 close, cut cross-sectionally WITHIN the forward "
                   "window",
            inherits=dict(inference_spec=V2["spec_id"],
                          inference_digest=ART.file_digest("SEQUENCE_INFERENCE_V2.json"),
                          estimand_seal=SEAL["seal_digest"])),

        stopping_rule=dict(
            lock=f"the first {LOCK_N:,} mature forward T5 episodes",
            mature="MFE_10D fully observable",
            why_not_a_date="a calendar lock lets market activity decide the sample size; an "
                           "episode-count lock does not",
            no_interim_test="the prospective tests are run ONCE, after the lock condition is "
                            "met. No weekly peeking, no early stop, no extension."),

        support_eligibility=dict(
            floors=dict(treated=300, control=300),
            same_as_historical=True,
            scope="counted on maturity-complete forward episodes inside overlap blocks",
            below_floor="INSUFFICIENT_FORWARD_SUPPORT — the claim leaves its family",
            is_x_only="support never consults an outcome, so this exclusion is not an "
                      "outcome-dependent selection",
            k_eff="the family max-Z is taken over the ELIGIBLE members only, and k_eff is "
                  "reported alongside the original k",
            declared_before_any_forward_outcome=True),

        blind_accrual=dict(
            visible=["n new T5 episodes", "n mature", "coverage", "missingness",
                     "per-claim occurrence counts", "data-quality failures"],
            forbidden=["MFE", "Z_rank", "theta", "ret_10d", "survivor status",
                       "any ranking"],
            why="so that nobody looks at 12,000 episodes and decides that is enough"),

        outcome_statuses=dict(
            FORWARD_STATISTICAL_PASS="the claim survives its prospective family max-Z control",
            FORWARD_DIRECTIONAL_CONSISTENCY="theta_raw > 0; DESCRIPTIVE ONLY",
            TRADEABILITY="NOT TESTED BY THIS PROTOCOL",
            why_three="the historical work found an MFE association whose median translation "
                      "into a 10-day terminal return was about 20%. Forward replication of "
                      "the association still says nothing about a profitable exit policy."),

        cross_resolution_excluded=dict(
            observation="VOL_W <-> the 15m VA-> family, p90 o/e 21.7 and 71% containment",
            status="POST-EXPOSURE HISTORICAL OBSERVATION",
            not_in_primary_gate=True,
            why="hand-picking it as a refinement of the 171-medoid family is exactly the "
                "adaptation this protocol exists to prevent",
            if_wanted="register it separately as SECONDARY_FORWARD_HYPOTHESIS with its own "
                      "claim, joint-event definition, outcome and statistic, before any "
                      "forward outcome is seen"),

        external_validation=dict(
            not_this="recomputing over the same 2021-2026 history from a different file is "
                     "NOT external validation",
            requires="independence in at least one substantive dimension declared in advance",
            examples=["a security universe absent from discovery",
                      "a different market or geography",
                      "an independent raw-data vendor with disjoint forward dates",
                      "another genuinely untouched population"],
            relation_to_this_spec="forward validation is cleaner than a mislabelled 'external' "
                                  "dataset that is really the same market and period"),

        y_status="NO POST-CUTOFF OUTCOME OBSERVED",
        next_step="build the BLIND forward accrual ledger; expose nothing until the lock")

    dig = ART.seal(body, OUT, required=("spec_id", "discovery_cutoff", "families",
                                        "stopping_rule"))
    print(f"T5_FORWARD_VALIDATION_V1  {dig}  FROZEN")
    print(f"  discovery cutoff   {CUTOFF}")
    print(f"  families           1H k={n1h} · 15m k={n15} · digest {fam_digest}")
    print(f"  lock               first {LOCK_N:,} mature forward T5 episodes")
    print(f"  support floors     300 treated / 300 control, same as historical")
    print(f"  statuses           STATISTICAL_PASS · DIRECTIONAL_CONSISTENCY · "
          f"TRADEABILITY NOT TESTED")
    print(f"  cross-resolution   excluded from the primary gate")
    print(f"\n  WROTE {OUT}\n        {FAM}")


if __name__ == "__main__":
    main()
