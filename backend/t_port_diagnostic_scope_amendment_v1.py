"""MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1 — 1D admitted as evidence only.

Amendment 2 froze the port at 15m ONLY and said in as many words that the presence of other
datasets is not hypothesis registration. Reading the legacy store at 1D therefore needs its own
frozen permission rather than a quiet exception inside a harness, which is what this is.

THE REASON 1D IS UNAVOIDABLE HERE IS STRUCTURAL, NOT CONVENIENT. The bars table declares
`date DATE NOT NULL` with PRIMARY KEY (ticker, date, universe) and carries no timeframe,
interval or resolution column anywhere in its DDL; the importer writes none either. So
bars.t_sig cannot represent anything but a daily observation. There is no legacy 15m
materialization to compare against — not one that is hard to reach, one that does not exist.

WHICH MAKES THE HONEST SHAPE OF THE EVIDENCE ASYMMETRIC, AND IT MUST STAY THAT WAY. A 1D-vs-1D
comparison is genuinely clean: same timeframe, same semantics, only the feed differs. What it
cannot do is license a sentence about 15m. Whatever divergence 1D shows, the 15m port's feed
divergence remains NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA, because there is no matched 15m
legacy support to estimate it from. Carrying the 1D number across would be imputation dressed
as measurement.

SO LAYER 2 IS RENAMED. "Historical materialization conformance" invites exactly the misreading
this amendment exists to prevent — that something about 15m materialization was established. It
is LEGACY_1D_MATERIALIZER_STORAGE_CONFORMANCE: whether the canonical engine, fed the legacy
STORED 1D OHLC, reproduces the 1D observation the producer materialized at its own runtime.
Useful for provenance. Silent about 15m.

NOTHING HERE ENLARGES THE RESEARCH FAMILY. 1D T does not become a hypothesis, does not enter the
search space or any multiplicity family, does not reach the production feature registry, and may
not condition a single 15m parameter choice.
"""
from __future__ import annotations
import json, os, re, sys, time                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

AM2, AM2_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json", "bd9292853ee10f2c"
PROV, PROV_D = "MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json", "a8e1f3725ddfe7a1"
SMOKE, SMOKE_D = "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json", "6b811cae60e260e5"
DDL = "studio/db.py"
IMP = "studio/importer.py"


def main():
    for f, want in ((AM2, AM2_D), (PROV, PROV_D), (SMOKE, SMOKE_D)):
        got = ART.file_digest(f)
        if got != want:
            print(f"HOLD — digest mismatch {f}: {got}"); return 1

    # ---- prove the 1d-only claim from the schema, do not assert it ----------
    ddl = open(DDL).read()
    m = re.search(r"CREATE TABLE(?: IF NOT EXISTS)? bars\s*\((.*?)\n\s*\)", ddl, re.S)
    if not m:
        print("HOLD — could not locate the bars DDL"); return 1
    body = m.group(1)
    tf_cols = re.findall(r"^\s*(tf|interval|timeframe|freq|resolution|period)\b", body,
                         re.M | re.I)
    has_date = bool(re.search(r"^\s*date\s+DATE\s+NOT NULL", body, re.M | re.I))
    pk = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", body)
    imp_tf = re.findall(r"\b(interval|timeframe)\b", open(IMP).read())
    if tf_cols or not has_date or not pk:
        print("HOLD — the 1d-only premise did not verify:", tf_cols, has_date, bool(pk))
        return 1

    p = dict(
        amendment_id="MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1",
        status="MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1_FROZEN",
        task_class="SCOPE_AMENDMENT_ONLY",
        purpose="admit the existing 1D historical evidence for provenance and source "
                "diagnostics WITHOUT enlarging the research family",
        amends=dict(artifact="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2", digest=AM2_D,
                    disposition="FROZEN — not edited"),
        binds=dict(provenance=PROV_D, smoke=SMOKE_D),

        research_port_timeframe=dict(value="15m", only=True, status="UNCHANGED",
                                     source="Amendment 2 input_timeframe"),
        diagnostic_only_timeframe=dict(
            value="1D",
            classification="DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION",
            allowed_uses=[
                "canonical producer vs historical bars.t_sig",
                "legacy 1D OHLC vs Massive 1D OHLC",
                "price-basis / precision diagnostics",
                "source-input sensitivity diagnostics"],
            forbidden_consequences=[
                "1D T becomes a registered hypothesis",
                "1D T enters the search space",
                "1D T enters a multiplicity family",
                "1D T enters the production feature registry",
                "a 1D result conditions any 15m parameter choice",
                "1D divergence is imputed to 15m"]),

        why_1d_is_structurally_unavoidable=dict(
            evidence=dict(
                bars_ddl_timeframe_columns=tf_cols or [],
                bars_has_date_not_timestamp=has_date,
                bars_primary_key=pk.group(1).strip(),
                importer_writes_interval=bool(imp_tf)),
            conclusion="bars.t_sig can only ever be a daily observation; there is no legacy "
                       "15m materialization to compare against",
            not_a_convenience="the 1D admission follows from the schema, not from preference"),

        layer_structure=dict(
            LAYER_1=dict(name="same-input semantic correctness",
                         comparison="canonical producer vs port vs independent verifier",
                         requirement="EXACT", status_in_smoke="ACHIEVED (0/0/0)"),
            LAYER_2=dict(name="LEGACY_1D_MATERIALIZER_STORAGE_CONFORMANCE",
                         renamed_from="HISTORICAL MATERIALIZATION CONFORMANCE",
                         rename_reason="the old name invites the reading that something "
                                       "about 15m materialization was established",
                         question="does the canonical engine, fed the legacy STORED 1D OHLC, "
                                  "reproduce the 1D observation the producer materialized at "
                                  "its own runtime?",
                         classification="DIAGNOSTIC_ONLY",
                         explicitly_does_not="establish historical 15m materialization "
                                             "equivalence",
                         tuning_forbidden=True),
            LAYER_3A=dict(name="1D cross-source diagnostic",
                          comparison="canonical producer on legacy 1D vs canonical producer "
                                     "on Massive 1D",
                          classification="DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION",
                          includes="full matched legacy_OHLC == round(Massive_OHLC, 2) on "
                                   "each of open/high/low/close over the common support",
                          resolves_into=["precision/storage difference",
                                         "genuine source OHLC difference",
                                         "unresolved exception"]),
            LAYER_3B=dict(name="Massive 15m port capability",
                          content="evaluability / availability / state counts ONLY",
                          forbidden="any legacy feed-divergence claim",
                          legacy_15m_counterpart="DOES_NOT_EXIST")),

        fifteen_minute_feed_divergence=dict(
            status="NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA",
            reason="no matched legacy 15m support exists",
            explicitly_not="a value carried across from the 1D diagnostic",
            must_be_recorded_by="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1 as a stated "
                                "limitation"),

        what_the_15m_port_still_has=[
            "the canonical semantics are known exactly",
            "the caller context is known exactly",
            "an independent implementation agrees EXACTLY at Layer 1",
            "actual evaluability will be measured over the full 476 cohort",
            "contaminated or missing prior bar yields UNAVAILABLE, never FALSE",
            "state prevalence and availability stand as X-side capability only"],

        hash_discipline=dict(
            port_implementation_hash="5ab5390171e0d5d2",
            harness_hash="5a183315f801b568",
            producer_t_slice="2901107d9baa6320",
            rule="a fixture correction must not perturb the port hash, and a port change must "
                 "not hide behind a fixture edit",
            rerun_rule="on access restore the FULL fixture suite reruns under the final "
                       "harness hash; earlier results are supporting history only and no "
                       "mixed-harness packet may be sealed"),

        n16=dict(status="BLOCKED",
                 forbidden="inferring PASS from the documented layout",
                 required_on_restore=["supplemental namespace injection is rejected or "
                                      "cannot influence T",
                                      "supplemental rows consumed = 0",
                                      "new-vintage rows consumed = 0",
                                      "excluded securities consumed = 0"]),

        execution_plan=[
            "freeze this diagnostic-scope amendment",
            "restore volume access",
            "read actual schemas — no schema invention",
            "freeze the final harness hash if code changed",
            "rerun the full fixture suite",
            "rerun Layer 1 — EXACT required",
            "15m full-cohort census",
            "LEGACY_1D_MATERIALIZER_STORAGE_CONFORMANCE",
            "1D cross-source / price-basis diagnostic",
            "N16 real store-binding fixture",
            "seal PASS or HOLD"],

        research_family_enlarged=False,
        gates=dict(t_port_smoke="HOLD", t_port_acceptability="HOLD", t_production="HOLD",
                   z="HOLD", y="HOLD"),
        y_exposed=0, t_production_writes=0,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json",
                 required=("amendment_id", "status", "research_port_timeframe",
                           "diagnostic_only_timeframe", "why_1d_is_structurally_unavoidable",
                           "layer_structure", "fifteen_minute_feed_divergence"),
                 supersede=os.path.exists(
                     "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json"))
    print(f"MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1 · {d} · {p['status']}")
    print(f"  research    15m ONLY — UNCHANGED")
    print(f"  diagnostic  1D — DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION")
    print(f"  proof       bars: date DATE NOT NULL · PK ({pk.group(1).strip()}) · "
          f"timeframe columns {tf_cols or 'NONE'}")
    print(f"  L2 renamed  LEGACY_1D_MATERIALIZER_STORAGE_CONFORMANCE")
    print(f"  15m feed    NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA")
    print(f"  family      enlarged = False")
    print(f"  gates       smoke HOLD · acceptability HOLD · production HOLD · Z/Y HOLD")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
