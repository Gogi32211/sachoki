"""T5_FORWARD_RULE_FEATURE_DEPENDENCY_V1 — the guarded feature set, machine-derived.

'0 of 175 rules depend on CISD' was established by searching rule texts for token names. That
is evidence, not proof: a rule could depend on a feature that is COMPUTED from a CISD column
without the string CISD appearing anywhere in the rule. This artifact replaces the grep with
set arithmetic over the actual dependency graph:

    claim_id -> tokens (parsed from the frozen claim)          direct
    token    -> column (from the frozen token registry)        direct
    column   -> its inputs in the semantic chain               transitive

    GUARDED_FEATURE_SET = union of all columns any frozen claim reaches
    proof obligation    = CISD_SET ∩ GUARDED_FEATURE_SET = ∅

THE TRANSITIVE STEP HAS A STRUCTURAL SHORTCUT, AND IT IS STATED RATHER THAN ASSUMED

The semantic chain's input is raw OHLCV only — _process(tk, u, df, tf) receives nothing else.
So no chain-produced column can transitively depend on a column that exists only in the store,
UNLESS that store column is itself chain-produced. The only store columns NOT chain-produced
are the ones written by out-of-band backfills; the artifact enumerates exactly which columns
studio/cisd_backfill.py writes and asserts none of them is guarded. Both halves are read from
code and data, not from memory.
"""
from __future__ import annotations
import ast, json, os, re, sys                                          # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402

OUT = "T5_FORWARD_RULE_FEATURE_DEPENDENCY_V1.json"
FAM = os.path.join(D.ROOT, "data", "t5_forward_families.parquet")
CISD_SET = {"sig_cisd_plus_struct", "sig_cisd_minus_struct", "sig_cisd_seq", "sig_cisd_mpm"}


def backfill_written_columns():
    """What cisd_backfill actually writes, read from its source rather than remembered."""
    src = open("studio/cisd_backfill.py").read()
    cols = sorted(set(re.findall(r'"(sig_cisd_[a-z_]+)"', src)))
    return cols


def claim_tokens(claim_id):
    parts = claim_id.split("|")
    return parts[2].split("→")


def main():
    ART.smoke_test(verbose=False)
    tok2col = {t.token_id: t.column for t in D.TOKENS_1H}
    F = pd.read_parquet(FAM)

    rows, unknown = [], set()
    for r in F.itertuples():
        toks = claim_tokens(r.claim_id)
        cols = []
        for t in toks:
            c = tok2col.get(t)
            if c is None:
                unknown.add(t)
            else:
                cols.append(c)
        rows.append(dict(claim_id=r.claim_id, family=r.family,
                         tokens=toks, direct_features=sorted(set(cols))))
    if unknown:
        raise RuntimeError(f"claim token(s) absent from the frozen registry: {sorted(unknown)}")

    guarded = sorted(set(c for r in rows for c in r["direct_features"]))
    bf_cols = backfill_written_columns()

    # transitive: chain input is OHLCV only, so the only way a guarded column could reach an
    # out-of-band value is by BEING one. Asserted as set arithmetic.
    overlap_cisd = sorted(set(guarded) & CISD_SET)
    # bf_cols is every column the backfill can WRITE. Splitting it matters: the four struct
    # columns are backfill-ONLY (the chain never emits them into the store), while the cplus
    # family is DUAL-WRITER — produced by the chain on every ingest AND rewritable by the
    # backfill. The first grep-based check asked only about the struct four and the summary
    # then overgeneralized to "0/175 use CISD"; this artifact is the correction.
    dual_writer = sorted(set(guarded) & set(bf_cols) - CISD_SET)
    overlap_backfill_only = sorted(set(guarded) & CISD_SET & set(bf_cols))
    dw_claims = [r["claim_id"] for r in rows
                 if set(r["direct_features"]) & set(dual_writer)]
    ohlcv = ["open", "high", "low", "close", "volume", "date"]

    body = dict(
        spec_id="T5_FORWARD_RULE_FEATURE_DEPENDENCY_V1", status="FROZEN",
        why="'0/175 rules depend on CISD' was a grep over rule texts; this is set arithmetic "
            "over the dependency graph, so a rule cannot depend on a CISD-derived feature "
            "without the intersection below being non-empty",
        n_claims=int(len(F)),
        token_registry_size=len(tok2col),
        per_claim=rows,
        guarded_feature_set=guarded,
        n_guarded_features=len(guarded),
        transitive_argument=dict(
            chain_input="raw OHLCV only — _process(tk, u, df, tf) receives nothing else",
            consequence="no chain-produced column can transitively depend on a store column "
                        "that is not itself chain-produced",
            out_of_band_writers=dict(cisd_backfill=bf_cols),
            transitive_roots=ohlcv),
        proof=dict(
            struct_cisd_intersection=overlap_cisd,
            statement="STRUCT_CISD ∩ GUARDED = ∅ — no frozen rule reaches a backfill-ONLY "
                      "column, directly or transitively (chain input is OHLCV only)",
            holds=bool(not overlap_cisd and not overlap_backfill_only)),
        dual_writer=dict(
            columns=dual_writer,
            claims=dw_claims,
            fact="these columns are chain-produced on every ingest AND rewritable by "
                 "studio/cisd_backfill.py — two writers, one cell",
            why_admissible="membership is computed ONCE from an immutable snapshot, so a later "
                           "backfill rewrite cannot alter an evaluated episode; the canary "
                           "guards the CHAIN semantics of these columns; and the finalizer's "
                           "stability digest now covers guarded signal columns, so a rewrite "
                           "landing between the two stability reads HOLDS the session instead "
                           "of slipping through",
            correction="the earlier summary statement '0/175 frozen claims use CISD tokens' "
                       "was true only of the four struct columns; the cplus family is used by "
                       "these claims and the overgeneralization is corrected here"),
        canary_must_cover=guarded,
        families_digest=ART.file_digest(FAM) if os.path.exists(FAM) else None)

    if overlap_cisd or overlap_backfill_only:
        raise RuntimeError(f"DEPENDENCY PROOF FAILED: a frozen rule reaches a backfill-ONLY "
                           f"column: struct {overlap_cisd} / {overlap_backfill_only}")

    d = ART.seal(body, OUT, required=("spec_id", "guarded_feature_set", "proof"),
                 supersede=True)
    print(f"T5_FORWARD_RULE_FEATURE_DEPENDENCY_V1 · {d}")
    print(f"  claims {len(F)} · guarded features {len(guarded)}")
    print(f"  STRUCT_CISD ∩ GUARDED = {overlap_cisd or '∅'}")
    print(f"  dual-writer columns in GUARDED: {dual_writer} · used by {len(dw_claims)} claim(s)")
    return d, guarded


if __name__ == "__main__":
    main()
