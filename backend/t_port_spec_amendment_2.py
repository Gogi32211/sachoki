"""MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2 — rebind the port to the proven producer.

The base spec (545d293b4f1e4653) names Pine as the semantic parent. That was correct under the
provenance then available and is wrong under the provenance now proven, so this amendment moves
the authority — and only the authority. Not one formula is chosen or altered here on the basis
of what it produces.

    CANONICAL_LEGACY_T_MATERIALIZER = signal_engine.py :: compute_signals(df)

WHAT IS DERIVED FROM SOURCE RATHER THAN RESTATED. You asked that the priority chain, the state
mapping and the lookback be bound from the producer rather than carried over from Pine or from
memory, so this script extracts all three by parsing signal_engine.py and refuses to seal if the
extraction disagrees with what it expected to find. The same applies to the vintage proof: the
T-semantic diff is COMPUTED between the export-vintage blob and the file smoke will run, not
asserted from a commit message.

THE T-ISOLATION ARGUMENT, STATED SO IT CAN BE ATTACKED. The one post-export commit changed cZ1,
a bearish condition. The claim that this cannot touch T rests on three lines of the producer and
nothing else: sig_id = where(bc>0, BC_MAP[bc], ZC_MAP[zc]); zc is zeroed wherever bc>0; and the
exporter takes the cell only when the label starts with "T". So a bar whose bc>0 yields a
T-label determined entirely by the cT* block, and a bar whose bc==0 can only ever move between Z
labels, which the T column discards. Z conditions are therefore outside the T semantic slice by
construction, not by inspection. The slice digest is computed over that block alone, and the Z
region is excluded deliberately and on the record.

WHAT I AM NOT CLAIMING. Not "same materialized population" — the input feed and its precision
may differ, and that is a measurement for the conformance run, not an assumption here. And the
export-vintage blob is the last commit before 2026-05-29; any uncommitted working-tree state at
export time is unverifiable from this repository, which is recorded as a stated limitation
rather than quietly treated as absent.

1e-10 IS A NUMERICAL GUARD, NOT A TICK SIZE. It is a divide-by-zero floor on the previous bar's
body inside the producer. It is not syminfo.mintick, not an exchange MPV, not a fallback and not
inferred market metadata. UNAVAILABLE_MINTICK does not exist in this port.

NO RESEARCH OUTPUT. NO T PRODUCTION. NO Z. NO Y.
"""
from __future__ import annotations
import hashlib, json, os, re, subprocess, sys, time                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

BASE, BASE_D = "MASSIVE_T_FEATURE_PORT_SPEC_V1.json", "545d293b4f1e4653"
PROV, PROV_D = "MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json", "a8e1f3725ddfe7a1"
PRIOR_D = "1e465831f96bc855"
T4, T4_D = "T4_ENGULF_CONFIG_V1.json", "713f09144a245408"
MQ, MQ_D = "MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1.json", "0cad620d6a5fb7bc"
AM1_D = "de60bdc75fc61e3c"
ENG = "signal_engine.py"
EXPORT_DATE = "2026-05-29"

EXPECTED_CHAIN = ["T4", "T6", "T1G", "T2G", "T1", "T2", "T9", "T10", "T3", "T11", "T5", "T12"]


def t_slice(src: str):
    """The T-semantic slice: constants, the shared bar algebra, the cT* block, bc priority.

    The bearish condition region is excluded BY CONSTRUCTION — see the module docstring's
    isolation argument. Returns None if any marker is missing, which is a HOLD, not a guess.
    """
    def between(a, b):
        i, j = src.find(a), src.find(b)
        return src[i:j] if (i >= 0 and j > i) else None
    parts = {
        "constants": between("NONE = 0", "_ZC_TO_SID"),
        "bar_algebra_and_cT": between("def compute_signals(", "# ── Bearish patterns"),
        "bc_priority": between("# ── bc priority code", "# ── zc priority code"),
        "sid_and_name": between("sid = np.where(", "is_bear  ="),
    }
    if any(v is None for v in parts.values()):
        return None, parts
    blob = "\n".join(parts[k] for k in sorted(parts))
    return hashlib.sha256(blob.encode()).hexdigest()[:16], parts


def parse_priority(src: str):
    blk = src[src.find("bc_arr = np.zeros"):src.find("# ── zc priority code")]
    pairs = re.findall(r"\((\d+),\s*c(T\d+G?)\)", blk)
    return [n for _, n in sorted(pairs, key=lambda p: int(p[0]))]


def parse_sig_names(src: str):
    blk = src[src.find("SIG_NAMES"):src.find("BULLISH_SIGS")]
    return {int(k): v for k, v in re.findall(r"(\d+):\s*\"([A-Z0-9]+)\"", blk)}


def parse_bc_to_sid(src: str):
    m = re.search(r"_BC_TO_SID\s*=\s*\{(.*?)\}", src, re.S)
    return {int(k): int(v) for k, v in re.findall(r"(\d+):\s*(\d+)", m.group(1))} if m else {}


def parse_lookback(src: str):
    blk = src[src.find("def compute_signals("):src.find("# ── Bearish patterns")]
    shifts = sorted({int(n) for n in re.findall(r"\.shift\((\d+)\)", blk)})
    rolling = re.findall(r"\.(rolling|ewm|expanding)\(", blk)
    return shifts, rolling


def git(*a):
    return subprocess.run(["git", *a], cwd="..", capture_output=True, text=True,
                          timeout=120).stdout


def main():
    for f, want in ((BASE, BASE_D), (PROV, PROV_D), (T4, T4_D), (MQ, MQ_D)):
        got = ART.file_digest(f)
        if got != want:
            print(f"HOLD — digest mismatch {f}: {got} != {want}"); return 1

    cur = open(ENG).read()

    # ---- export-vintage blob, found rather than hardcoded ----------------
    log = git("log", "--format=%H %ad", "--date=short", f"--until={EXPORT_DATE}",
              "--", f"backend/{ENG}").strip().splitlines()
    if not log:
        print("HOLD — no commit found for the producer before the export date"); return 1
    vint_sha, vint_date = log[0].split()
    vint = git("show", f"{vint_sha}:backend/{ENG}")
    if not vint.strip():
        print("HOLD — could not read the export-vintage blob"); return 1

    # ---- T-semantic diff, COMPUTED ---------------------------------------
    cur_slice, cur_parts = t_slice(cur)
    vint_slice, _ = t_slice(vint)
    if cur_slice is None or vint_slice is None:
        print("HOLD — T-slice markers not found; refusing to guess the boundary"); return 1
    if cur_slice != vint_slice:
        print(f"HOLD — T semantic diff != 0 ({vint_slice} vs {cur_slice}); "
              f"smoke may not run against a changed T"); return 1

    # ---- bind from source -------------------------------------------------
    chain = parse_priority(cur)
    if chain != EXPECTED_CHAIN:
        print(f"HOLD — priority chain parsed from source is {chain}"); return 1
    names = parse_sig_names(cur)
    bc_sid = parse_bc_to_sid(cur)
    shifts, rolling = parse_lookback(cur)
    if rolling:
        print(f"HOLD — undeclared window function in the T path: {rolling}"); return 1
    if shifts != [1]:
        print(f"HOLD — T path uses shifts {shifts}; lookback must be declared"); return 1
    bc_to_label = {bc: names[sid] for bc, sid in sorted(bc_sid.items())}
    if [bc_to_label[i] for i in range(1, 13)] != EXPECTED_CHAIN:
        print("HOLD — bc->label mapping disagrees with the parsed priority chain"); return 1

    after = git("log", "--format=%h %ad %s", "--date=short", f"--since={EXPORT_DATE}",
                "--", f"backend/{ENG}").strip().splitlines()
    t4 = json.load(open(T4))

    p = dict(
        amendment_id="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2",
        status="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2_FROZEN",
        task_class="AUTHORITY_REBINDING_ONLY",
        purpose="rebind the Massive T port from the assumed Pine semantic authority to the "
                "proven canonical legacy materializer",
        amends=dict(artifact="MASSIVE_T_FEATURE_PORT_SPEC_V1", digest=BASE_D,
                    disposition="FROZEN — not edited"),
        authorities=dict(
            base_port_spec=dict(artifact="MASSIVE_T_FEATURE_PORT_SPEC_V1", digest=BASE_D),
            materializer_provenance=dict(
                artifact="MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1", digest=PROV_D,
                status="RESOLVED"),
            historical_unresolved=dict(
                artifact="MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1", digest=PRIOR_D,
                status="IMMUTABLE but SUPERSEDED_AUTHORITY_WISE")),

        canonical_producer=dict(
            frozen_as="CANONICAL_LEGACY_T_MATERIALIZER",
            implementation="signal_engine.py :: compute_signals(df)",
            proven_call_path=["compute_signals(df)", "main.py /api/bar-signals",
                              "SuperchartPanel.jsx", "exportCsv()", "*_signals_5y.csv",
                              "studio/importer.py", "bars.t_sig"],
            authority_established_by="the call path",
            explicitly_not_by="agreement percentage"),

        semantic_target=dict(
            replaces_claim="SAME RECOVERED PINE SEMANTIC PROGRAM",
            allowed_claim="SAME CANONICAL LEGACY T MATERIALIZER SEMANTICS, EXECUTED ON "
                          "MASSIVE 15m SOURCE DATA",
            forbidden_claim="same materialized population",
            why_forbidden="the input feed and its precision may differ"),

        pine_status=dict(
            artifact="260523_TZ_F_WLNBB_CMB_pattern.pine", role="REFERENCE_ONLY",
            usable_for="historical / design comparison",
            not_the="canonical materialization authority",
            conflict_rule="any disagreement between Pine and signal_engine.py resolves in "
                          "favour of the proven canonical producer for this port"),
        signal_logic_status=dict(
            path="analyzers/tz_wlnbb/signal_logic.py", role="NON_PRODUCER_REFERENCE",
            rule="not usable as the Massive T semantic authority unless a separately "
                 "registered study explicitly targets it"),

        frozen_producer_config=dict(
            use_wick=False, min_body_ratio=1.0, doji_thresh=0.05,
            doji_thresh_participates_in_isDoji=False,
            isDoji="(c == o)",
            prevBodySafe="max(prevBody, 1e-10)",
            one_e_minus_10=dict(
                classification="CANONICAL_NUMERICAL_GUARD",
                is_not=["exchange tick size", "syminfo.mintick", "fallback",
                        "inferred market metadata"],
                role="divide-by-zero floor on the previous bar's body"),
            tradingview_mintick_lookup_required=False,
            unavailable_mintick_state_exists=False),

        runtime_config=dict(
            status="RESOLVED",
            evidence="main.py:4060 invokes compute_signals(df) with no overrides",
            consequence="producer defaults ARE the materialization configuration",
            retires_classification="LEGACY_RUNTIME_CONFIGURATION_UNKNOWN",
            retired_scope="the canonical T materializer only"),

        export_vintage=dict(
            export_date=EXPORT_DATE,
            authority_commit=vint_sha, authority_commit_date=vint_date,
            authority_source_digest=vint_slice,
            current_file_slice_digest=cur_slice,
            t_semantic_diff=0,
            proof="the T-semantic slice — constants, bar algebra, cT* block, bc priority, "
                  "sid/name mapping — is byte-identical between the export-vintage blob and "
                  "the file smoke will run",
            slice_excludes="the bearish condition region",
            t_isolation_argument=[
                "sig_id = where(bc>0, BC_MAP[bc], ZC_MAP[zc])",
                "zc is zeroed wherever bc>0",
                "the exporter keeps the cell only when the label starts with 'T'",
                "therefore a Z-condition change can only move a bar between Z labels, "
                "which the T column discards"],
            commits_after_export=after,
            limitation="the vintage blob is the last COMMIT before the export date; "
                       "uncommitted working-tree state at export time is unverifiable from "
                       "this repository"),

        priority_chain=dict(
            frozen=chain, bound_from="signal_engine.py bc priority block, parsed",
            explicitly_not_from="Pine ordering or memory"),
        state_id_mapping=dict(
            sig_names={str(k): v for k, v in sorted(names.items())},
            bc_code_to_label={str(k): v for k, v in sorted(bc_to_label.items())},
            bc_to_sig_id={str(k): v for k, v in sorted(bc_sid.items())},
            bound_from="signal_engine.py SIG_NAMES and _BC_TO_SID, parsed",
            rule="state identity is the code mapping, never the label text"),

        lookback=dict(
            required_prior_bars=max(shifts), shifts_found=shifts,
            window_functions=[], derived_from="signal_engine.py T path, parsed",
            explicitly_not_from="Pine",
            rule="no hidden lookback may be discovered during production; a differing value "
                 "at smoke time is a HOLD"),

        z_dependency=dict(
            exists=True,
            expression="p1Bear = (c.shift(1) < o.shift(1)) | isDoji.shift(1)",
            classification="T_INTERNAL_SUPPORTING_PREDICATE",
            precise_form="RAW prior-bar doji equality",
            explicitly_not="the resolved Z7 signal (cZ7c = isDoji & ~anyB & ~anyZ)",
            why_it_matters="importing resolved Z7 instead of raw prior-bar doji would change "
                           "T on any bar where a Z or T also fired on the prior bar",
            present_in_canonical_path=True,
            research_facing_Z="HOLD"),

        t4_config=dict(
            artifact="T4_ENGULF_CONFIG_V1", digest=T4_D,
            status="REINSTATED_AS_VALID_SUPPORTING_AUTHORITY",
            citation_amendment_required=False,
            previously_suspected_defect="REVIEW ERROR, not an artifact defect",
            original_not_edited=True,
            checked=dict(use_wick=t4["use_wick"], min_body_ratio=t4["min_body_ratio"])),

        mintick_qualification=dict(
            artifact="MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1", digest=MQ_D,
            reinterpreted_as="supporting evidence for the producer's 1e-10 numerical guard",
            do_not_describe_1e_10_as="syminfo.mintick",
            tick_size_problem="CLOSED_AS_NOT_APPLICABLE",
            amendment_1=dict(digest=AM1_D, original="HOLD", now="RESOLVED")),

        timestamp_alignment=dict(rule="legacy.date == Massive.bar_start", tz="UTC",
                                 equality="EXACT", tolerance=None,
                                 nearest_timestamp_allowed=False),
        price_basis=dict(
            status="INDICATIVE_COMPATIBILITY_PENDING_FULL_CONFORMANCE",
            indication="legacy OHLC == Massive-equivalent OHLC rounded to 2 decimals",
            upgrade_requires="the full matched conformance run",
            upgraded_here=False),
        input_timeframe=dict(frozen="15m", only=True,
                             forbidden=["1m", "5m", "1H", "1D"],
                             note="the existence of those datasets is not hypothesis "
                                  "registration"),

        conformance_targets=dict(
            A="canonical producer recomputed on the exact same input bars and config",
            B="historical materialized bars.t_sig",
            C="canonical producer executed on Massive 15m inputs",
            primary_semantic_test="producer implementation vs independent reimplementation "
                                  "on IDENTICAL INPUT — must be EXACT",
            secondary="historical materialized t_sig is a provenance/conformance "
                      "observation, not the executable semantic authority",
            forbidden="optimising C to maximise agreement with B"),
        same_input_invariant=dict(
            statement="identical input bars + identical producer config + identical "
                      "canonical implementation semantics => identical T output",
            role="PRIMARY_SMOKE_INVARIANT",
            exactness="EXACT — 99.x% is not acceptable for same-input semantic verification"),
        feed_precision_divergence=dict(
            classification="X_SOURCE_INPUT_DIVERGENCE",
            not_automatically="an implementation defect",
            naming_rule="may not be called 'rounding' unless demonstrated from observed X "
                        "differences"),

        required_negative_fixtures=[
            "Pine implementation cannot silently replace signal_engine authority",
            "signal_logic.py cannot silently replace signal_engine authority",
            "min_body_ratio mutation rejected",
            "use_wick mutation rejected",
            "1e-10 mutation rejected",
            "threshold-doji cannot replace exact equality",
            "priority permutation rejected",
            "wrong SIG_NAMES mapping rejected",
            "non-15m timeframe rejected",
            "contaminated required Massive bar => UNAVAILABLE",
            "missing required prior history => UNAVAILABLE",
            "T_INTERNAL_SUPPORTING_PREDICATE cannot become research-facing Z",
            "materialized t_sig agreement cannot be used to fit parameters",
            "Y access rejected"],

        y_firewall=dict(y_exposed=0, forbidden=["future returns", "theta", "enrichment",
                                                "P&L", "outcome-conditioned conformance"]),
        gates=dict(t_port_smoke="UNBLOCKED_BY_THIS_AMENDMENT", t_production="HOLD",
                   z="HOLD", y="HOLD"),
        research_output=None, t_production_writes=0, smoke_started=False,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json",
                 required=("amendment_id", "status", "canonical_producer", "semantic_target",
                           "frozen_producer_config", "export_vintage", "priority_chain",
                           "state_id_mapping", "lookback", "z_dependency",
                           "conformance_targets", "same_input_invariant"),
                 supersede=os.path.exists(
                     "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2.json"))
    print(f"MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_2 · {d} · {p['status']}")
    print(f"  PRIMARY     signal_engine.py :: compute_signals   (Pine -> REFERENCE_ONLY)")
    print(f"  vintage     {vint_sha[:8]} ({vint_date}) · T-slice {vint_slice} == {cur_slice}"
          f" · T semantic diff = 0")
    print(f"  after       {len(after)} commit(s) since export — excluded region only")
    print(f"  chain       {' > '.join(chain)}   [parsed from source]")
    print(f"  mapping     bc 1..12 -> {[bc_to_label[i] for i in range(1,13)]}")
    print(f"  lookback    {max(shifts)} prior bar(s); window functions: none")
    print(f"  z-dep       raw isDoji.shift(1) = T_INTERNAL_SUPPORTING_PREDICATE "
          f"(NOT resolved Z7)")
    print(f"  1e-10       CANONICAL_NUMERICAL_GUARD · mintick lookup NOT required")
    print(f"  T4          reinstated · no citation amendment · original untouched")
    print(f"  invariant   same input + same config + same semantics => EXACT equality")
    print(f"  gates       smoke UNBLOCKED · production HOLD · Z HOLD · Y HOLD")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
