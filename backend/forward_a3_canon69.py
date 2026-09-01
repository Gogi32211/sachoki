"""A3 — canonical 69-X consolidation.

CONSOLIDATION, NOT RECOMPUTATION. Nothing here computes a feature. Rows are read from the
four qualified A2 family outputs, mapped onto the frozen canonical schema by the sealed
manifest, and written. Values, availability labels, categories and dtypes pass through
untouched.

The availability granularity DIFFERS BY SOURCE and that difference is information, not
noise: pd emits a coarse INITIALIZATION_SENSITIVE and must NEVER have it decomposed into
HEAD / POST_GAP, because the pd producer never made that distinction and reconstructing it
would fabricate provenance.

Import-side-effect free.
"""
from __future__ import annotations
import os, json, hashlib                                               # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
RUNTIME = os.path.join(HERE, "massive_new_family_runtime")
MANIFEST_V2 = os.path.join(RUNTIME, "CANONICAL_69_MANIFEST_V2.json")

# The sealed manifest digests were taken over sort_keys JSON, NOT over the raw file bytes
# the way t5_artifact.file_digest does it. Checking the manifest with file_digest reports a
# false mismatch; this is the convention the seals actually used.
MANIFEST_V2_SEALED_DIGEST = "58f21313250836bf"
MANIFEST_V1_SEALED_DIGEST = "a44fd5cda3f22ef9"
A2_STACK_DIGEST = "904873185d6b5346"
CANON69_AUTHORITY = "MASSIVE_NEW_FAMILY_CANONICAL_69_X_SURFACE_V1"
CANON69_AUTHORITY_DIGEST = "e8016cb7c39a969d"

CANON_SCHEMA = [("k", "VARCHAR"), ("bar_start", "BIGINT"), ("session_date", "VARCHAR"),
                ("feature", "VARCHAR"), ("bval", "TINYINT"), ("dval", "DOUBLE"),
                ("sval", "VARCHAR"), ("availability", "VARCHAR")]
N_FEATURES = 69
SOURCE_TABLES = {"d6", "pd", "c4", "p39"}


class A3Hold(Exception):
    """Fail-closed. Never downgraded."""


def manifest_digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()[:16]


def load_manifest(path=MANIFEST_V2, expect=MANIFEST_V2_SEALED_DIGEST):
    if not os.path.exists(path):
        raise A3Hold(f"GUARD manifest_missing: {path}")
    m = json.load(open(path))
    d = manifest_digest(m)
    if expect is not None and d != expect:
        raise A3Hold(f"GUARD manifest_digest: {d} != sealed {expect}")
    if len(m) != N_FEATURES:
        raise A3Hold(f"GUARD feature_count: {len(m)} != {N_FEATURES}")
    seen = set()
    for name, e in m.items():
        if e.get("feature") != name:
            raise A3Hold(f"GUARD feature_identity: key {name} != feature {e.get('feature')}")
        if name in seen:
            raise A3Hold(f"GUARD duplicate_feature: {name}")
        seen.add(name)
        if e.get("source_table") not in SOURCE_TABLES:
            raise A3Hold(f"GUARD source_family: {name} -> {e.get('source_table')}")
        for f in ("source_slot", "canonical_slot", "value_type", "source_artifact_digest",
                  "source_physical_db_digest"):
            if not e.get(f):
                raise A3Hold(f"GUARD manifest_field: {name} missing {f}")
        if e["canonical_slot"] not in ("bval", "dval", "sval"):
            raise A3Hold(f"GUARD canonical_slot: {name} -> {e['canonical_slot']}")
    return m


# per-family: how that family's A2 output columns map onto the canonical slots.
# pd is the only family whose value column is not already named for its canonical slot.
FAMILY_SLOTS = {
    "pd":  {"value": "bval"},
    "c4":  {"bval": "bval", "sval": "sval"},
    "p39": {"bval": "bval", "sval": "sval"},
    "d6":  {"bval": "bval", "dval": "dval", "sval": "sval"},
}


def consolidate_partition(frames, manifest, key):
    """frames: {source_table: DataFrame of that family's A2 output for ONE security}.
    Returns the canonical long-form frame. No value is computed, cast or normalised."""
    import numpy as np, pandas as pd
    out = []
    for name, e in manifest.items():
        st = e["source_table"]
        if st not in frames:
            raise A3Hold(f"GUARD source_absent: feature {name} needs family {st}")
        src = frames[st]
        sub = src[src.col == name]
        if not len(sub):
            raise A3Hold(f"GUARD feature_rows_absent: {name} has no rows in family {st}")
        slot_map = FAMILY_SLOTS[st]
        ssl = e["source_slot"]
        if ssl not in slot_map:
            raise A3Hold(f"GUARD source_slot: {name} slot {ssl!r} not produced by family {st}")
        if slot_map[ssl] != e["canonical_slot"]:
            raise A3Hold(f"GUARD slot_mapping: {name} {ssl} -> {slot_map[ssl]} "
                         f"but the manifest says {e['canonical_slot']}")
        n = len(sub)
        d = {"k": key, "bar_start": sub.bar_start.values,
             "session_date": sub.session_date.values, "feature": name,
             "bval": pd.array([None] * n, dtype="Int8"),
             "dval": pd.array([None] * n, dtype="Float64"),
             "sval": np.full(n, None, object),
             "availability": sub.availability.values}
        d[e["canonical_slot"]] = sub[ssl].values
        out.append(pd.DataFrame(d))
    r = pd.concat(out, ignore_index=True)
    dup = r.duplicated(subset=["bar_start", "feature"]).sum()
    if dup:
        raise A3Hold(f"GUARD canonical_key_duplicate: {dup} duplicate (bar_start, feature)")
    return r


# The ADMISSIBLE label set is the SOURCE-FAMILY CONTRACT, read from each frozen producer.
# A census of the historical CANON69 realisation is a CONFORMANCE ORACLE, never an
# authority: it can only tell us which admissible labels HAPPENED to occur. pd is the
# live example — its producer defines UNAVAILABLE_DEPENDENCY, and the sealed
# PD_12_PRODUCTION artifact records it as observed ZERO times, because an EMA is
# UNAVAILABLE exactly when its bar is contaminated so UNAVAILABLE_CURRENT always fires
# first under the precedence order. Treating the CANON69 census as the authority would
# make a legitimate future UNAVAILABLE_DEPENDENCY a violation.
CONTRACT_VOCABULARY = {
    "pd":  {"AVAILABLE_VALID", "INITIALIZATION_SENSITIVE", "UNAVAILABLE_DEPENDENCY",
            "UNAVAILABLE_CURRENT"},
    "c4":  {"AVAILABLE_VALID", "INITIALIZATION_SENSITIVE_HEAD",
            "INITIALIZATION_SENSITIVE_POST_GAP", "UNAVAILABLE_CURRENT"},
    "p39": {"AVAILABLE_VALID", "INITIALIZATION_SENSITIVE_HEAD",
            "INITIALIZATION_SENSITIVE_POST_GAP", "UNAVAILABLE_CURRENT"},
    "d6":  {"AVAILABLE_VALID", "INITIALIZATION_SENSITIVE_HEAD",
            "INITIALIZATION_SENSITIVE_POST_GAP", "UNAVAILABLE_CURRENT",
            "UNAVAILABLE_REQUIRED_HISTORY"},
}
PD_FORBIDDEN_DECOMPOSITION = {"INITIALIZATION_SENSITIVE_HEAD",
                              "INITIALIZATION_SENSITIVE_POST_GAP"}


def check_availability_vocabulary(observed_by_family, historical_by_family=None):
    """Two SEPARATE questions, deliberately not conflated.

    ADMISSIBILITY (authority = the frozen source-family contract):
        every observed label must lie inside that family's contract vocabulary.
    REALISATION CONFORMANCE (oracle = the historical CANON69 census, optional):
        the historical rebuild must reproduce exactly the labels history realised.
        This is a regression check on a REBUILD OF HISTORY; it must never be applied to
        forward data, where a contract-admissible but historically-unseen label is legal.
    """
    out = {}
    for fam, obs in observed_by_family.items():
        contract = CONTRACT_VOCABULARY.get(fam)
        if contract is None:
            raise A3Hold(f"GUARD availability_family_unknown: {fam}")
        extra = set(obs) - contract
        if extra:
            raise A3Hold(f"GUARD availability_not_admissible: family {fam} emitted "
                         f"{sorted(extra)}, outside its frozen contract {sorted(contract)}")
        if fam == "pd" and (set(obs) & PD_FORBIDDEN_DECOMPOSITION):
            raise A3Hold("GUARD pd_initialization_decomposed: pd's coarse "
                         "INITIALIZATION_SENSITIVE must never be split into HEAD/POST_GAP")
        out[fam] = dict(observed=sorted(obs), contract=sorted(contract),
                        admissible_but_unrealised=sorted(contract - set(obs)))
        if historical_by_family is not None:
            hist = set(historical_by_family.get(fam, []))
            if set(obs) != hist:
                raise A3Hold(f"GUARD historical_realisation: family {fam} rebuilt "
                             f"{sorted(obs)} != history {sorted(hist)}")
            out[fam]["matches_historical_realisation"] = True
    return out


def self_digest():
    return hashlib.sha256(open(os.path.abspath(__file__), "rb").read()).hexdigest()[:16]


if __name__ == "__main__":
    m = load_manifest()
    print(f"manifest V2 OK · {len(m)} features · digest {MANIFEST_V2_SEALED_DIGEST}")
    print(f"module digest {self_digest()}")
