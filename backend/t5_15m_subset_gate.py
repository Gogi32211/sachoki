"""SCOPE_SUBSET_EQUIVALENCE — does TWO_BAR mode differ from FULL only by family inclusion?

A scope switch must be a MASK, not a second implementation. If dropping the 3-bar families
changes anything about a surviving 2-bar claim, the switch carries hidden semantics and no
scope revision may be frozen on top of it.

THE PROPERTY

    Removing 3-bar families may remove claims, but may not alter any surviving 2-bar claim
    before final cross-family equivalence resolution.

Two comparisons, because the last clause is not decoration:

    A  pre-final-dedup 2-bar membership classes — MUST be bit-identical.
       representative name, position family, length, every support statistic, the estimand
       signature and the estimand_qualified verdict.

    B  final representative identity — may legitimately differ ONLY where a 2-bar claim was
       exact-equivalent to a 3-bar claim in the FULL universe and the winner was chosen
       across families.

WHAT THIS IMPLEMENTATION PREDICTS FOR B

The sweep runs PAIRS before TRIPLES, and both dedup dictionaries keep the FIRST name seen for
a hash. So a 2-bar claim always wins any cross-length tie, and B should also come out
identical. n_names is the one column expected to differ: in FULL a 2-bar representative can
absorb 3-bar duplicates, which is exactly the cross-family alias count and is reported rather
than treated as a violation.

That prediction is stated here BEFORE the comparison runs, so a surprise cannot be explained
away afterwards.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_dna as D                                                     # noqa: E402

FULL_CL = os.path.join(D.ROOT, "data", "t5_15m_claims.parquet")
TWO_CL = os.path.join(D.ROOT, "data", "t5_15m_claims_2bar.parquet")
OUT = os.path.join(HERE, "T5_15M_SCOPE_SUBSET_EQUIVALENCE.json")

KEY = "representative"
IDENTITY = ["position_family", "length", "treated_all_pop", "tickers", "dates",
            "max_date_share", "treated_all", "treated_overlap", "control_overlap",
            "overlap_retention", "n_overlap_blocks", "estimand_membership_hash",
            "estimand_qualified", "block_constant_treated_share"]
EXPECTED_DIFF = ["n_names"]        # cross-family alias absorption, declared in advance


def main():
    F = pd.read_parquet(FULL_CL)
    T = pd.read_parquet(TWO_CL)
    F2 = F[F.length == 2].sort_values(KEY).reset_index(drop=True)
    T2 = T.sort_values(KEY).reset_index(drop=True)
    print("SCOPE_SUBSET_EQUIVALENCE · FULL 2-bar subset vs TWO_BAR mode")
    print(f"  FULL rows {len(F):,} · FULL 2-bar {len(F2):,} · TWO_BAR {len(T2):,}")

    same_set = set(F2[KEY]) == set(T2[KEY])
    only_full = sorted(set(F2[KEY]) - set(T2[KEY]))[:5]
    only_two = sorted(set(T2[KEY]) - set(F2[KEY]))[:5]
    print(f"  {'✓' if same_set else '✗'} A.1 identical claim sets"
          + ("" if same_set else f"  only-FULL {only_full} · only-TWO_BAR {only_two}"))

    diffs, a_ok = {}, same_set
    if same_set:
        for c in IDENTITY:
            n = int((F2[c].values != T2[c].values).sum())
            if n:
                diffs[c] = n
        a_ok = not diffs
        print(f"  {'✓' if a_ok else '✗'} A.2 every support/estimand column identical"
              + ("" if a_ok else f"  differing: {diffs}"))
        hF = hashlib.sha256(pd.util.hash_pandas_object(
            F2[[KEY] + IDENTITY], index=False).values.tobytes()).hexdigest()[:16]
        hT = hashlib.sha256(pd.util.hash_pandas_object(
            T2[[KEY] + IDENTITY], index=False).values.tobytes()).hexdigest()[:16]
        print(f"    identity hash  FULL {hF} · TWO_BAR {hT}")
    else:
        hF = hT = None

    b_ok = False
    cross = 0
    if same_set and "is_final_representative" in F2 and "is_final_representative" in T2:
        nb = int((F2.is_final_representative.values
                  != T2.is_final_representative.values).sum())
        b_ok = nb == 0
        print(f"  {'✓' if b_ok else '✗'} B final representative identity — {nb} differing"
              + ("" if b_ok else "  (permitted ONLY for cross-family alias classes; "
                                 "inspect before accepting)"))
        cross = int((F2.n_names.values != T2.n_names.values).sum())
        print(f"    cross-family alias absorption (n_names differs): {cross:,} claims "
              f"— expected, not a violation")

    ok = bool(same_set and a_ok and b_ok)
    json.dump(dict(spec_id="T5_15M_SCOPE_SUBSET_EQUIVALENCE",
                   property="removing 3-bar families may remove claims, but may not alter any "
                            "surviving 2-bar claim before final cross-family equivalence "
                            "resolution",
                   full_rows=int(len(F)), full_two_bar_rows=int(len(F2)),
                   two_bar_rows=int(len(T2)),
                   A_identical_claim_sets=bool(same_set),
                   A_identical_columns=bool(a_ok), A_differing_columns=diffs,
                   A_identity_hash=dict(full=hF, two_bar=hT),
                   B_final_representative_identical=bool(b_ok),
                   cross_family_alias_absorption_n_names_differs=cross,
                   expected_difference=EXPECTED_DIFF,
                   prediction_stated_before_run="PAIRS are swept before TRIPLES and both dedup "
                                                "dictionaries keep the first name per hash, so "
                                                "a 2-bar claim wins every cross-length tie and "
                                                "B was predicted identical",
                   passed=ok), open(OUT, "w"), indent=2)
    print(f"\n  {'PASS' if ok else 'FAIL'} · WROTE {OUT}")
    if not ok:
        raise SystemExit("scope mode carries hidden semantics — DO NOT freeze the revision")


if __name__ == "__main__":
    main()
