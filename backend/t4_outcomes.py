"""T4 outcome sidecar — computed, sealed, NOT exposed.

Mirror of the T5 sidecar (same economic question, T9-specific identity). The builder is
REUSED from t5_outcomes with documented overrides, not copied: a second implementation of
entry/maturity/path-status semantics is exactly how two families drift apart.

What this run prints is availability ONLY — path_status counts. No MFE, MAE or return value
appears in any output of this phase.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_outcomes as O5                                               # noqa: E402
import t4_dna as D9                                                    # noqa: E402

OUT_OC = os.path.join(D9.ROOT, "data", "t4_outcomes.parquet")
SPEC = os.path.join(HERE, "T4_OUTCOME_SPEC_V1.json")


def main():
    t0 = time.time()
    E = pd.read_parquet(D9.OUT_EP, columns=["episode_id", "ticker", "t4_date", "t4_close"])
    E = E.rename(columns={"t4_date": "t5_date", "t4_close": "t5_close"})  # builder vocabulary

    # documented override: same builder, T4 output path. Restored immediately after.
    prev = O5.OUT_OC
    O5.OUT_OC = OUT_OC
    try:
        O = O5.build_outcomes(E, verbose=False)
    finally:
        O5.OUT_OC = prev

    # T4 identity: rename the vocabulary columns and stamp the T4 definition hash
    O = pd.read_parquet(OUT_OC)
    O = O.rename(columns={"t5_date": "t4_date", "t5_close": "t4_close"})
    O["price_source_hash"] = D9.T4_DEF_HASH
    O.to_parquet(OUT_OC, index=False, compression="zstd")

    avail = {f"{h}d": O[f"path_status_{h}d"].value_counts().to_dict() for h in O5.HORIZONS}
    spec = dict(
        spec_id="T4_OUTCOME_SPEC_V1", family_id="T4_MICROSTRUCTURE_DNA_V1",
        signal="canonical T4 daily close", decision="after T4 session close",
        entry_semantics=O5.ENTRY_SEMANTICS,
        primary="MFE_10D percent", secondary_registered="MFE_ATR_10D",
        descriptive_only=["MAE_10D percent", "terminal R_10D percent"],
        not_promotion_paths=["MAE_10D", "R_10D"],
        exit_policy="NONE",
        formulas=dict(MFE="max(High_j/EntryOpen - 1)", MAE="min(Low_j/EntryOpen - 1)",
                      R="Close_h/EntryOpen - 1"),
        t4_definition_hash=D9.T4_DEF_HASH,
        builder="t5_outcomes.build_outcomes — REUSED byte-identical, output path overridden; "
                "a second implementation of entry/maturity semantics is how families drift",
        availability=avail,
        exposure="NOT EXPOSED — this phase prints availability counts only; no outcome VALUE "
                 "appears in any log or artifact of the X phase")
    spec["spec_digest"] = hashlib.sha256(
        json.dumps(spec, sort_keys=True, default=str).encode()).hexdigest()[:16]
    json.dump(spec, open(SPEC, "w"), indent=2, default=str)
    print(f"T4 outcome sidecar sealed · {spec['spec_digest']} · rows {len(O):,} · "
          f"{time.time()-t0:.0f}s")
    for h in (10,):
        print(f"  path_status_{h}d: " + " · ".join(f"{k}={v:,}" for k, v in
                                                   sorted(avail[f'{h}d'].items())))
    print("  NO OUTCOME VALUE PRINTED", flush=True)


if __name__ == "__main__":
    main()
