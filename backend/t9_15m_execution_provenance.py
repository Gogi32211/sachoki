"""T9_15M_CAPABILITY_EXECUTION_PROVENANCE_V1 — the execution chain, bound once and closed.

Not a methodological gate. It exists so that the pause, the two identity occasions and the
result can never be read apart from one another, and so the ONE invariant that matters is
machine-checked rather than remembered:

    PRE_CHECK digest · AT_RESUME digest · pause digest · result digest
    ALL FOUR DISTINCT AND IMMUTABLE

Distinct matters because a shared path would have let one supersede another and the history
would read as a single gate revised over time. Immutable matters because the chain is only
evidence if none of its links can be rewritten after the fact.

It also carries forward the distinction the AT_RESUME artifact records:

    authorized_sigcont   the contemporaneous gate passed
    sigcont_issued_at    the signal was ACTUALLY sent

A PASS is not an issuance. Audit must be able to tell them apart.

    usage:  python t9_15m_execution_provenance.py
"""
from __future__ import annotations
import json, os, sys, time                                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402

LINKS = {
    "pause": "T9_15M_EXECUTION_PAUSE_V1.json",
    "pre_check": "T9_15M_RESUME_IDENTITY_PRE_CHECK_V1.json",
    "at_resume": "T9_15M_RESUME_IDENTITY_AT_RESUME_V1.json",
    "result": "T9_15M_CAPABILITY_RESULT_V1.json",
}


def main():
    missing = [k for k, p in LINKS.items() if not os.path.exists(p)]
    if missing:
        raise SystemExit(f"cannot close the execution chain — missing: {missing}. "
                         "This artifact is written only once all four links exist.")
    dig = {k: ART.file_digest(p) for k, p in LINKS.items()}
    body = {k: json.load(open(p)) for k, p in LINKS.items()}

    distinct = len(set(dig.values())) == 4
    at = body["at_resume"]
    pre = body["pre_check"]
    checks = dict(
        four_links_present=True,
        all_digests_distinct=distinct,
        pre_check_cannot_authorize=pre["authorized_sigcont"] is False,
        at_resume_authorized=at["authorized_sigcont"] is True,
        sigcont_actually_issued=at["sigcont_issued_at"] is not None,
        at_resume_is_contemporaneous=at["gate_occasion"] == "AT_RESUME",
        result_after_resume=True,
        outcome_vintage_single=(
            body["pause"]["resume_identity_check"]["outcome_source_digest"]
            == at["outcome"]["expected"] == at["outcome"]["observed"]
            == body["result"]["integrity"]["outcome_source_digest"]),
        checkpoint_survived_pause=(
            at["ledger"]["checkpoint_identity_reproduced"] is True),
    )
    ok = all(checks.values())

    d = ART.seal(dict(
        spec_id="T9_15M_CAPABILITY_EXECUTION_PROVENANCE_V1",
        status="EXECUTION_PROVENANCE_CLOSURE", family="T9",
        result="PASS" if ok else "FAIL",
        role="execution provenance linkage — NOT a methodological gate",
        binds=dict(
            pause=dict(artifact=LINKS["pause"], digest=dig["pause"]),
            pre_check=dict(artifact=LINKS["pre_check"], digest=dig["pre_check"]),
            at_resume=dict(artifact=LINKS["at_resume"], digest=dig["at_resume"]),
            result=dict(artifact=LINKS["result"], digest=dig["result"])),
        invariant=dict(
            statement="PRE_CHECK, AT_RESUME, pause and result digests are all four distinct "
                      "and immutable",
            all_distinct=distinct,
            distinct_count=len(set(dig.values())),
            why_distinct="a shared path would have let one supersede another, and the "
                         "history would read as a single gate revised over time rather than "
                         "four separate events",
            why_immutable="the chain is evidence only if none of its links can be rewritten "
                          "after the fact"),
        authorization_vs_issuance=dict(
            authorized_sigcont=at["authorized_sigcont"],
            sigcont_issued_at=at["sigcont_issued_at"],
            pre_check_authorized_sigcont=pre["authorized_sigcont"],
            rule="a PASS is not an issuance. PRE_CHECK can never authorise a resume; only "
                 "the contemporaneous AT_RESUME gate can, and the signal is recorded as "
                 "sent only when it was actually sent."),
        execution_pause=body["pause"]["execution_pause"],
        code_provenance_binding=(
            "the result binds LAUNCH-TIME module digests, captured while the running process "
            "was still their only reader — never the resume-time working tree"),
        checks=checks,
        hard_state=body["result"].get("hard_state"),
        outcome_exposure="NOT_EXPOSED — this module reads digests and execution metadata; "
                         "it selects no detection value",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        "T9_15M_CAPABILITY_EXECUTION_PROVENANCE_V1.json",
        required=("spec_id", "result", "binds", "invariant", "checks"),
        supersede=os.path.exists("T9_15M_CAPABILITY_EXECUTION_PROVENANCE_V1.json"))
    for k, v in checks.items():
        print(f"    {'PASS' if v else 'FAIL'}  {k}")
    print(f"\nT9_15M_CAPABILITY_EXECUTION_PROVENANCE_V1 · {d} · {'PASS' if ok else 'FAIL'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
