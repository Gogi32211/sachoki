"""T5_FORWARD_MEDOID_IDENTITY_ONTOLOGY_AMENDMENT_V1 — the PASS stands; the wording does not.

T5_FORWARD_MEDOID_IDENTITY_V1 checked the nine memberships held in
T5_1H_MEMBERSHIP_CACHE_V1 and described them as "the frozen forward rule memberships".
That conflates two different objects:

    T5_1H_MEMBERSHIP_CACHE_V1        the frozen 1H RESEARCH state — nine historical
                                     survivor memberships
    T5_FORWARD_VALIDATION_V1.H1      the forward inferential family — k = 4, the four 1H
                                     overlap-cluster medoids

Nine identity checks is a SUPERSET proof of the four the forward family actually needs, so
the result is stronger, not weaker. But the forward family's multiplicity is 4 and must
never read as 9 in any downstream account.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys, time                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "T5_FORWARD_MEDOID_IDENTITY_ONTOLOGY_AMENDMENT_V1.json"


def main():
    ident = json.load(open("T5_FORWARD_MEDOID_IDENTITY_V1.json"))
    fwd = json.load(open("T5_FORWARD_VALIDATION_V1.json"))
    h1 = fwd["families"]["H1"]
    checked = {k.split("|", 1)[1]: v for k, v in
               ident["forward_memberships"]["per_claim"].items()}
    covered = {m: (m in checked) for m in h1["members"]}
    ok = all(covered.values()) and h1["k"] == 4 and len(checked) == 9

    body = dict(
        spec_id="T5_FORWARD_MEDOID_IDENTITY_ONTOLOGY_AMENDMENT_V1", status="AMENDMENT",
        amends="T5_FORWARD_MEDOID_IDENTITY_V1",
        amends_digest=ART.file_digest("T5_FORWARD_MEDOID_IDENTITY_V1.json"),
        result="PASS — the amended artifact's verdict is unchanged",
        correction=dict(
            was="the nine checked memberships were described as 'the frozen forward rule "
                "memberships', which reads as if the forward family had nine members",
            is_="the nine are the frozen 1H RESEARCH survivor memberships held in "
                 "T5_1H_MEMBERSHIP_CACHE_V1; the forward inferential family registered in "
                 "T5_FORWARD_VALIDATION_V1 is H1 with k = 4, the four 1H overlap-cluster "
                 "medoids",
            correct_statement="All nine sealed historical 1H survivor memberships were "
                              "identity-checked; therefore, in particular, all four 1H "
                              "medoids registered in T5_FORWARD_VALIDATION_V1 are "
                              "unchanged.",
            why_it_matters="forward multiplicity is 4; no downstream account may inherit a "
                           "forward family of 9 from a superset identity proof"),
        forward_family=dict(spec="T5_FORWARD_VALIDATION_V1.families.H1",
                            k=h1["k"], members=h1["members"], source=h1["source"]),
        coverage={m: dict(checked=bool(covered[m]),
                          n=checked.get(m, {}).get("n"),
                          membership_hash=checked.get(m, {}).get("membership_hash"))
                  for m in h1["members"]},
        superset=dict(n_checked=len(checked), n_required=h1["k"],
                      reading="9/9 identity is strictly stronger than the 4/4 the forward "
                              "family needs"),
        unchanged=dict(verdict="PASS", session_axis="the SESSION-SEMANTICS reason for the "
                                                    "T5 forward hold is lifted",
                       still_held="SOURCE DATA VERSION — ONE_HOUR_DATA_VERSION_DIVERGENCE_V1",
                       code_environment="prior status unchanged"),
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "correction", "forward_family",
                                      "coverage"), supersede=os.path.exists(OUT))
    for m, c in body["coverage"].items():
        print(f"  {'PASS' if c['checked'] else 'FAIL'}  {m:<34} n={c['n']} · "
              f"{c['membership_hash']}")
    print(f"\n  forward family k={h1['k']} · memberships identity-checked "
          f"{len(checked)} (superset)")
    print(f"T5_FORWARD_MEDOID_IDENTITY_ONTOLOGY_AMENDMENT_V1 · {d} · "
          f"{'PASS' if ok else 'REVIEW'}")


if __name__ == "__main__":
    main()
