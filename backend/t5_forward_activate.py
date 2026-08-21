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
