"""Operational activation of the frozen forward chain. No statistic is computed here.

The design freeze is finished; this is the gate every forward run passes through first.

FAIL CLOSED, NOT FAIL LOUD

startup_audit() verifies the eight artifact digests and the evaluator hash and RAISES. It does
not warn: an unattended nightly job that warns has failed silently with extra steps. If a digest
moved, the run does not proceed on the assumption that the change was probably fine.

THE BLIND AUDIT IS THE ONLY THING A FORWARD RUN PRINTS

X only — session state, timing, coverage, data quality. No MFE, no return, no Z, no rank. The
accrual process cannot print what it is not allowed to hold.

THE START MARKER IS A ONE-WAY BOUNDARY

Once the first forward occurrence row exists, every later methodology change is a POST-START
amendment and must not be confusable with the pre-data changes made before it. The marker
records what the world looked like at that instant — first episode, first signal session, the
code commit, the artifact chain digest — and is written exactly once.
"""
from __future__ import annotations
import hashlib, json, os, subprocess, sys                              # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D, t5_forward_eval as EV          # noqa: E402

CHAIN = {
    "T5_FORWARD_VALIDATION_V1":                 "c39c651868c727f4",
    "T5_FORWARD_ACCRUAL_LEDGER_V1":             "9a61d6365ba612b7",
    "T5_FORWARD_ACCRUAL_LEDGER_V1_AMENDMENT_1": "92f3a4ecc1d4ce2e",
    "T5_FORWARD_MEMBERSHIP_REPLAY_V1":          "24bc2e67ce06d27f",
    "T5_FORWARD_MEMBERSHIP_EVALUATOR_V1":       "9bf68a61627e34c8",
    "T5_FORWARD_OCCURRENCE_SCHEMA_V1":          "6a79d581962594e2",
    "T5_FORWARD_SESSION_LENGTH_V1":             "b873b7f153a1e124",
    "T5_FORWARD_LOCK_MANIFEST_V1":              "9ee0afc66385b3de",
}
EVALUATOR_HASH = "fbb79d0a2bad3854"
MARKER = "T5_FORWARD_ACCRUAL_STARTED.json"
DATA = os.path.join(D.ROOT, "data")
OCC = os.path.join(DATA, "t5_forward_occurrence.parquet")


class ChainDrift(RuntimeError):
    pass


def chain_digest():
    """One identity for the whole frozen chain, so a marker can name it in a single field."""
    return hashlib.sha256("|".join(f"{k}:{v}" for k, v in sorted(CHAIN.items()))
                          .encode()).hexdigest()[:16]


def code_commit():
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                              cwd=HERE, timeout=10).stdout.strip() or "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def tree_clean():
    try:
        out = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"],
                             capture_output=True, text=True, cwd=HERE, timeout=20).stdout
        return not out.strip(), [l for l in out.splitlines()[:5]]
    except Exception as e:
        return False, [f"git status failed: {e}"]


def runtime_pin():
    """A branch name is not a scientific identity — `main` will move next week. The runtime is a
    dedicated detached worktree carrying T5_RUNTIME_PIN with the commit it was created at, and
    identity is that SHA.

    Absence of the pin file is NOT a failure of the audit: reading the audit from a development
    checkout is legitimate. It IS a failure of accrual — assert_runtime_pinned() below is what
    the ingest calls, and it refuses anywhere but the pinned runtime. The threat here is no
    longer statistical leakage, it is deployment drift."""
    f = os.path.join(os.path.dirname(HERE), "T5_RUNTIME_PIN")
    if not os.path.exists(f):
        return dict(pinned=False, reason="no T5_RUNTIME_PIN — this is a development checkout",
                    head=code_commit())
    want = open(f).read().strip()
    head = code_commit()
    clean, dirt = tree_clean()
    return dict(pinned=(head == want and clean), pin=want, head=head,
                head_matches=(head == want), tree_clean=clean, dirty_sample=dirt)


def assert_runtime_pinned():
    """Called by the ingest, not by the audit. No warn-and-continue."""
    p = runtime_pin()
    if not p["pinned"]:
        raise ChainDrift(
            f"refusing to accrue outside the pinned runtime: {p}. Forward accrual runs only "
            f"from the dedicated worktree at its pinned commit with a clean tree, so that a "
            f"branch switch can never silently run the frozen protocol on different code.")
    return p


def startup_audit(verbose=True):
    bad = {}
    for name, want in sorted(CHAIN.items()):
        f = name + ".json"
        got = ART.file_digest(f) if os.path.exists(f) else "MISSING"
        if got != want:
            bad[name] = (want, got)
        if verbose:
            print(f"  {'OK ' if got == want else 'DRIFT'} {got}  {name}")
    ev = EV.evaluator_hash()
    if ev != EVALUATOR_HASH:
        bad["evaluator"] = (EVALUATOR_HASH, ev)
    if verbose:
        print(f"  {'OK ' if ev == EVALUATOR_HASH else 'DRIFT'} {ev}  evaluator source")
        print(f"       chain digest {chain_digest()}")
    if bad:
        raise ChainDrift(
            f"frozen chain drift, refusing to run: {bad}. Either an artifact was regenerated or "
            f"the code moved. Neither is something a forward ingest may decide to tolerate.")
    return chain_digest()


def blind_audit(row):
    """The only thing a forward run prints. X and operations, never an outcome."""
    fields = ["session_date", "source_final_ts", "session_length_commit_ts",
              "decision_deadline_ts", "commit_latency", "causal_headroom",
              "n_tickers", "modal_share", "session_length_status",
              "n_new_t5", "n_new_mature", "dq_codes"]
    forbidden = {"mfe", "ret", "return", "z", "z_rank", "theta", "rank", "win_rate", "pnl"}
    leak = sorted(k for k in row if k.lower() in forbidden)
    if leak:
        raise RuntimeError(f"blind audit was handed outcome field(s): {leak}")
    print("  ── forward blind audit ──────────────────────────────────────")
    for f in fields:
        print(f"     {f:<26} {row.get(f, '—')}")
    return {f: row.get(f) for f in fields}


def mark_accrual_started():
    """Written ONCE, when the first forward occurrence row exists. The boundary between
    pre-data design and post-start amendment."""
    if os.path.exists(MARKER):
        print(f"  marker already exists · {ART.file_digest(MARKER)} (written once, never again)")
        return ART.file_digest(MARKER)
    if not os.path.exists(OCC):
        raise RuntimeError("no occurrence table yet — nothing has started")
    O = pd.read_parquet(OCC)
    if not len(O):
        raise RuntimeError("occurrence table is empty — accrual has not started, so the marker "
                           "would be false")
    first = O.sort_values(["episode_id", "claim_id"]).iloc[0]
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "t5_date"])
    sig = E.set_index("episode_id").t5_date.get(first.episode_id, "UNKNOWN")
    dig = ART.seal(dict(
        spec_id="T5_FORWARD_ACCRUAL_STARTED", status="FROZEN",
        first_episode_id=str(first.episode_id),
        first_signal_session=str(sig),
        first_claim_id=str(first.claim_id),
        occurrence_rows_at_marking=int(len(O)),
        code_commit=code_commit(),
        artifact_chain_digest=chain_digest(),
        artifact_chain=dict(CHAIN),
        evaluator_hash=EVALUATOR_HASH,
        meaning="every methodology change after this instant is a POST-START amendment and "
                "must not be confusable with the pre-data changes made before it",
        written_once=True),
        MARKER, required=("spec_id", "first_episode_id", "artifact_chain_digest"))
    print(f"  FORWARD_ACCRUAL_STARTED · {dig}")
    return dig


def main():
    print("forward chain startup audit")
    startup_audit()
    print("\n  PASS — chain intact")
    E = pd.read_parquet(D.OUT_EP, columns=["t5_date"])
    cutoff = json.load(open("T5_FORWARD_VALIDATION_V1.json"))[
        "discovery_cutoff"]["last_eligible_historical_signal_session"]
    n = int((E.t5_date > cutoff).sum())
    T = pd.read_parquet(os.path.join(DATA, "t5_forward_session_length.parquet"))
    print(f"  post-cutoff episodes in source   {n}")
    print(f"  session lengths committed        {len(T)} (latest {T.session_date.max()})")
    print(f"  occurrence rows                  "
          f"{len(pd.read_parquet(OCC)) if os.path.exists(OCC) else 'table absent'}")
    print(f"  accrual started marker           "
          f"{'present' if os.path.exists(MARKER) else 'not yet'}")


if __name__ == "__main__":
    main()


def activation_manifest(activation_timestamp, out="T5_FORWARD_ACTIVATION_MANIFEST_V1.json"):
    """WHICH COMPLETE CONFIGURATION ENTERED FORWARD MODE.

    The base chain digest deliberately does NOT cover everything: it was computed over the
    eight scientific artifacts, before T5_FORWARD_COVERAGE_GAP_V1 existed. Rather than re-hash
    or re-seal the base chain — which would disturb artifacts nothing should disturb — the
    configuration is named one level up. Base identity stays fixed; activation identity is new.

    activation_timestamp is passed in rather than read from the clock, so the manifest is a
    function of its inputs and can be re-derived.
    """
    import pandas as _pd
    base = startup_audit(verbose=False)
    pin = runtime_pin()
    occ_rows = int(len(_pd.read_parquet(OCC))) if os.path.exists(OCC) else 0
    body = dict(
        spec_id="T5_FORWARD_ACTIVATION_MANIFEST_V1", status="FROZEN",
        question_it_answers="which complete configuration entered forward mode",
        base_chain_digest=base,
        base_chain=dict(CHAIN),
        base_chain_scope="the eight scientific artifacts ONLY. It was computed before the "
                         "coverage-gap note existed and is left unchanged on purpose: base "
                         "identity is fixed, activation identity is what this file adds.",
        coverage_gap_digest=ART.file_digest("T5_FORWARD_COVERAGE_GAP_V1.json"),
        ledger_amendment_digest=ART.file_digest(
            "T5_FORWARD_ACCRUAL_LEDGER_V1_AMENDMENT_1.json"),
        session_length_digest=ART.file_digest("T5_FORWARD_SESSION_LENGTH_V1.json"),
        evaluator_hash=EVALUATOR_HASH,
        runtime_commit=pin.get("pin") or pin.get("head"),
        runtime_pinned=bool(pin["pinned"]),
        runtime_identity="the commit SHA, never a branch name — `main` moves, a SHA does not",
        activation_timestamp=activation_timestamp,
        forward_episode_count=occ_rows,
        superseded=dict(
            digest="021c53cd32236cd3",
            status="SUPERSEDED_BUT_BYTES_NOT_PRESERVED",
            reason="the legacy seal() overwrote a fixed path; the predecessor's bytes are "
                   "unrecoverable and are NOT reconstructed, because rebuilding an artifact "
                   "after the fact is fabrication rather than preservation",
            successor="b873b7f153a1e124",
            fixed_by="seal() now refuses to overwrite differing content unless supersede=True, "
                     "which archives the predecessor first"),
        floors_naming=dict(
            call_it="prospective conservative eligibility restriction",
            not_="historical equivalence",
            why="the frozen historical table applied no floor at all; the forward rule does, "
                "so forward is strictly narrower and must not be described as reproducing "
                "historical semantics"),
        coverage=dict(
            nominal_discovery_cutoff="2026-08-20",
            effective_t5_microstructure_evidence_end="2026-08-17",
            pre_forward_excluded_gap="2026-08-18 through 2026-08-20",
            estimated_episodes=448,
            neither="not used in discovery, not admitted to forward accrual",
            denominator_note="448 is 2.24% OF THE 20,000 LOCK TARGET. It is not '2.24% of "
                             "historical data lost' — those are different denominators and "
                             "must not be conflated.",
            say_this_not_that="report 'nominal cutoff 2026-08-20, effective T5 evidence "
                              "through 2026-08-17' rather than 'historical data through "
                              "2026-08-20', which is true of the 1D bar store and no longer "
                              "true of T5"),
        fail_closed=["HEAD mismatch", "dirty tree", "artifact digest mismatch",
                     "unknown session", "session length not committed"],
        no_warn_and_continue=True)
    d = ART.seal(body, out, required=("spec_id", "base_chain_digest", "runtime_commit",
                                      "activation_timestamp"))
    return d, body
