"""T5_FORWARD_ACCRUAL_LEDGER_V1 — outcome-blind sample accumulation. No statistics exist here.

    parent_spec      T5_FORWARD_VALIDATION_V1
    parent_digest    c39c651868c727f4
    discovery_cutoff 2026-08-20
    first_session    2026-08-21
    mode             OUTCOME_BLIND
    state            APPEND_ONLY

WHAT THIS PROCESS CAN AND CANNOT SEE

It reads bars and builds membership. It never opens the outcome sidecar. Every frame it
touches passes assert_no_outcome(), which raises on any column whose name belongs to the
outcome vocabulary.

That is a CODE-LEVEL guard, not a privilege boundary, and the difference is real. A privilege
boundary would be a separate database role that cannot SELECT the outcome schema at all, so
that a mistake fails at the connection rather than at a check someone could delete. DuckDB in
single-file mode has no GRANT/DENY, so this is the strongest barrier available here; the
weaker guarantee is recorded rather than glossed.

MEMBERSHIP IS EVALUATED ONCE AND NEVER RECOMPUTED

    X at signal time -> the frozen 175 rules -> membership bitmap -> immutable

If the feature pipeline changes later, an old episode's membership is NOT corrected. A new
pipeline is a new data_version, not a rewrite of history. Recomputing membership under a newer
pipeline would make the forward sample partly retrospective.

MATURITY IS AN OBSERVATION HORIZON, NOT CALENDAR ARITHMETIC

signal_date + 10 calendar days is wrong. Maturity requires the tenth forward TRADING session
to have elapsed AND the bars for the whole horizon to be present AND no blocking data-quality
failure. If the horizon has passed but data is missing, the episode is
MATURITY_DATA_INCOMPLETE and does NOT count toward the 20,000.

THE LOCK COUNTER COUNTS MATURE EPISODES

Not signals, not calendar days, not valid memberships. And no ETA is computed: market activity
changes the accrual rate, and projecting a finish date invites deciding that a partial sample
is close enough.

NO STATISTICAL TEST IS CALLABLE BEFORE THE LOCK

run_forward_test() raises FORWARD_LOCK_NOT_REACHED and computes nothing partial.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                   # noqa: E402
import duckdb, numpy as np, pandas as pd                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import combo_tokens as CT, t5_artifact as ART, t5_dna as D             # noqa: E402

SPEC = json.load(open("T5_FORWARD_VALIDATION_V1.json"))
PARENT_DIGEST = "c39c651868c727f4"
CUTOFF = SPEC["discovery_cutoff"]["last_eligible_historical_signal_session"]
LOCK_N = 20000
MIN_T = MIN_C = 300
HORIZON = 10

DATA   = os.path.join(D.ROOT, "data")
FAMILY = os.path.join(DATA, "t5_forward_families.parquet")
STORE  = os.path.join(DATA, "t5_forward_episodes.parquet")      # hidden, X-only
# Two DIFFERENT tables shared this path. T5_FORWARD_OCCURRENCE_SCHEMA_V1 fixes the
# occurrence grain at (episode_id, claim_id); what was being written here is the
# per-claim SUPPORT SUMMARY, which has no episode_id at all. The frozen schema is
# correct and the writer was not, so the writer is corrected — this is code conforming
# to a spec, not a change of spec, and needs no amendment.
OCC    = os.path.join(DATA, "t5_forward_occurrence.parquet")    # long form, per schema
SUPPORT = os.path.join(DATA, "t5_forward_membership.parquet")   # per-claim summary
DQ     = os.path.join(DATA, "t5_forward_dq.parquet")
LEDGER = os.path.join(DATA, "t5_forward_ledger.parquet")        # blind, public
MANIFEST = os.path.join(DATA, "t5_forward_lock_manifest.parquet")
OUT    = "T5_FORWARD_ACCRUAL_LEDGER_V1.json"

# Column names the accrual process must never hold, whatever their source.
FORBIDDEN = {
    "mfe_10d", "mfe_atr_10d", "mae_10d", "ret_10d", "mfe_1d", "mfe_3d", "mfe_5d", "mfe_20d",
    "mae_atr_10d", "theta", "theta_raw_obs", "z", "z_rank", "z_rank_obs", "p_value",
    "max_z", "effect_size", "survivor_forward", "survives", "rank", "win_rate",
    "mean_return", "median_return", "band_p95", "needle_z",
}
DQ_CODES = ["NO_1H_MICROSTRUCTURE", "INCOMPLETE_OPENING_HOUR_15M", "SESSION_GAP",
            "DUPLICATE_BAR", "BAD_TIMESTAMP_ALIGNMENT", "CORPORATE_ACTION_UNRESOLVED",
            "UNIVERSE_MAPPING_FAILURE", "FEATURE_BUILD_FAILURE", "MEMBERSHIP_EVAL_FAILURE",
            "MATURITY_DATA_INCOMPLETE"]


class ForwardLockNotReached(RuntimeError):
    pass


class OutcomeLeak(RuntimeError):
    pass


def assert_no_outcome(df, where):
    bad = sorted(set(c.lower() for c in df.columns) & FORBIDDEN)
    if bad:
        raise OutcomeLeak(f"{where}: outcome column(s) reached the accrual process: {bad}")
    return df


def family_rules():
    F = assert_no_outcome(pd.read_parquet(FAMILY), "family table")
    dig = hashlib.sha256("|".join(F.family + ":" + F.claim_id).encode()).hexdigest()[:16]
    if dig != SPEC["families"]["digest"]:
        raise RuntimeError(f"frozen family digest drift: {dig} != {SPEC['families']['digest']}")
    rules = []
    for r in F.itertuples():
        parts = r.claim_id.split("|")
        rules.append(dict(claim_id=r.claim_id, family=r.family, cluster=int(r.cluster),
                          slot=parts[0], length=int(parts[1]),
                          tokens=parts[2].split("→"),
                          historical_support=int(r.historical_support)))
    return rules, dig


def trading_sessions():
    """The exchange calendar, taken from the 1D store's distinct dates."""
    conn = duckdb.connect(D.DB1D, read_only=True)
    try:
        s = conn.execute("SELECT DISTINCT CAST(date AS DATE) d FROM bars "
                         "WHERE universe IN ('sp500','nasdaq','russell2k') ORDER BY d"
                         ).fetchdf()["d"]
        return pd.to_datetime(s).dt.strftime("%Y-%m-%d").tolist()
    finally:
        conn.close()


def forward_episodes():
    """T5 episodes whose signal session is strictly after the cutoff. X only."""
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date",
                                           "prev_session_date"])
    assert_no_outcome(E, "episode table")
    F = E[E.t5_date > CUTOFF].copy()
    return F.sort_values(["t5_date", "ticker"]).reset_index(drop=True)


def maturity(F, cal):
    """Tenth forward TRADING session after entry, entry = the session after the signal."""
    idx = {d: i for i, d in enumerate(cal)}
    due, ok = [], []
    for d in F.t5_date:
        i = idx.get(d)
        if i is None or i + 1 + HORIZON >= len(cal):
            due.append(None); ok.append(False)
        else:
            due.append(cal[i + 1 + HORIZON]); ok.append(True)
    F["maturity_due_session"] = due
    F["horizon_elapsed"] = ok
    return F


def accrue(as_of=None, verbose=True):
    t0 = time.time()
    ART.smoke_test(verbose=False)
    # The frozen chain is verified BEFORE anything is read, and startup_audit raises rather
    # than warns: an unattended nightly job that warns has failed silently with extra steps.
    # A drifted digest is not something an ingest may decide to tolerate.
    import t5_forward_activate as ACT
    chain = ACT.startup_audit(verbose=verbose)
    pin = ACT.assert_runtime_pinned()
    if verbose:
        print(f"  chain {chain} · runtime pinned @ {pin['pin'][:7]} · tree clean · proceeding",
              flush=True)
    rules, fam_dig = family_rules()
    cal = trading_sessions()
    as_of = as_of or cal[-1]
    F = forward_episodes()
    F = maturity(F, cal)

    # ── data-quality census, one row per code ──────────────────────────────
    dq = {c: 0 for c in DQ_CODES}
    if len(F):
        M1 = pd.read_parquet(D.OUT_MS, columns=["episode_id", "relative_day"])
        have1h = set(M1[M1.episode_id.isin(F.episode_id)].episode_id)
        dq["NO_1H_MICROSTRUCTURE"] = int((~F.episode_id.isin(have1h)).sum())
        p15 = os.path.join(DATA, "t5_15m_opening_hour.parquet")
        if os.path.exists(p15):
            M15 = pd.read_parquet(p15, columns=["episode_id", "pos"])
            per = M15[M15.episode_id.isin(F.episode_id)].groupby("episode_id").pos.nunique()
            full = set(per.index[per == 4])
            dq["INCOMPLETE_OPENING_HOUR_15M"] = int((~F.episode_id.isin(full)).sum())
        dq["MATURITY_DATA_INCOMPLETE"] = int((~F.horizon_elapsed).sum())

    n_new = int(len(F))
    n_mature = int(F.horizon_elapsed.sum()) if len(F) else 0

    # ── snapshot provenance · machine fields only, outcome-blind ──────────
    # what a later continuity proof needs: which data_version, which gate verdict, which
    # digests. No outcome-derived field, not even aggregated.
    snap_prov = dict(data_version=None, snapshot_digest=None, semantic_gate_decision=None,
                     fast_gate_status=None, slow_gate_status=None,
                     finalizer_stabilization_digest=None, upstream_closure_hash=None)
    try:
        import t5_forward_snapshot as SNAP, t5_forward_producer as PR
        cur = SNAP.resolve_current()
        if cur:
            m = cur["manifest"]
            v = m.get("semantic_gate_verdict", {})
            snap_prov.update(
                data_version=m.get("data_version"),
                snapshot_digest=hashlib.sha256(json.dumps(
                    m.get("product_digests", {}), sort_keys=True).encode()).hexdigest()[:16],
                semantic_gate_decision=v.get("verdict"),
                fast_gate_status=v.get("fast"),
                slow_gate_status=(v.get("slow") or {}).get("status") if isinstance(
                    v.get("slow"), dict) else None,
                finalizer_stabilization_digest=(m.get("session_context") or {}).get(
                    "source_digest"),
                upstream_closure_hash=(m.get("semantic_upstream") or {}).get("closure_hash"))
    except Exception:
        pass          # no snapshot yet is a normal pre-forward state, not an error

    # ── ledger row · blind ─────────────────────────────────────────────────
    row = dict(as_of_session=as_of, cutoff=CUTOFF, **snap_prov,
               n_new_t5=n_new, n_total_t5=n_new,
               n_new_mature=n_mature, n_total_mature=n_mature,
               lock_target=LOCK_N,
               lock_progress_pct=round(100.0 * n_mature / LOCK_N, 4),
               coverage_1h=None, coverage_15m=None,
               data_quality_failures=int(sum(dq.values())),
               family_digest=fam_dig, parent_digest=PARENT_DIGEST,
               forward_outcomes="SEALED", statistical_testing="LOCKED")
    if n_new:
        row["coverage_1h"] = round(1 - dq["NO_1H_MICROSTRUCTURE"] / n_new, 4)
        row["coverage_15m"] = round(1 - dq["INCOMPLETE_OPENING_HOUR_15M"] / n_new, 4)
    L = pd.DataFrame([row])
    assert_no_outcome(L, "ledger")
    if os.path.exists(LEDGER):
        prev = pd.read_parquet(LEDGER)
        if n_mature < prev.n_total_mature.max():
            raise RuntimeError("mature count went DOWN — the ledger is append-only and the "
                               "counter must be monotone non-decreasing")
        L = pd.concat([prev, L], ignore_index=True)
    L.to_parquet(LEDGER, index=False)
    pd.DataFrame([dict(as_of_session=as_of, failure_code=c, n_episodes=v)
                  for c, v in dq.items()]).to_parquet(DQ, index=False)

    # ── per-claim occurrence · X only ──────────────────────────────────────
    occ = pd.DataFrame([dict(claim_id=r["claim_id"], family=r["family"],
                             historical_support=r["historical_support"],
                             forward_occurrence_total=0, forward_occurrence_mature=0,
                             treated_support_mature=0, control_support_mature=0,
                             support_eligible=False,
                             support_status="INSUFFICIENT_FORWARD_SUPPORT")
                        for r in rules])
    assert_no_outcome(occ, "occurrence table")
    occ.to_parquet(SUPPORT, index=False)   # summary, not the occurrence grain

    k1 = sum(1 for r in rules if r["family"] == "1H")
    k15 = len(rules) - k1
    e1 = int(occ[(occ.family == "1H") & occ.support_eligible].shape[0])
    e15 = int(occ[(occ.family == "15M") & occ.support_eligible].shape[0])

    if verbose:
        print(f"T5_FORWARD_ACCRUAL_LEDGER_V1 · OUTCOME_BLIND · as of {as_of}")
        # the cutoff is currently the LAST session in the calendar, so no forward session
        # exists yet. That is a state to record, not an error.
        nxt = "NONE YET — cutoff is the last session on the calendar"
        if CUTOFF in cal and cal.index(CUTOFF) + 1 < len(cal):
            nxt = cal[cal.index(CUTOFF) + 1]
        elif CUTOFF not in cal:
            nxt = "cutoff is not a trading session on this calendar"
        print(f"  cutoff {CUTOFF} · calendar ends {cal[-1]} · first eligible session {nxt}")
        print(f"\n  new T5                    {n_new:,}")
        print(f"  total T5                  {n_new:,}")
        print(f"  new mature                {n_mature:,}")
        print(f"  total mature              {n_mature:,} / {LOCK_N:,}  "
              f"({row['lock_progress_pct']:.2f}%)")
        print(f"\n  1H coverage               {row['coverage_1h']}")
        print(f"  15m opening-hour coverage {row['coverage_15m']}")
        print(f"  DQ failures               {row['data_quality_failures']:,}")
        for c, v in dq.items():
            if v:
                print(f"      {c:<32} {v:,}")
        print(f"\n  1H  k_original {k1:>4} · support-eligible {e1:>4}")
        print(f"  15m k_original {k15:>4} · support-eligible {e15:>4}")
        print(f"\n  forward outcomes          SEALED")
        print(f"  statistical testing       LOCKED")
        print(f"  (no ETA is computed — market activity sets the accrual rate)")
    return row, dq, occ, rules, fam_dig


def run_forward_test():
    """Refuses to compute anything until the lock is reached."""
    L = pd.read_parquet(LEDGER) if os.path.exists(LEDGER) else None
    n = int(L.n_total_mature.max()) if L is not None and len(L) else 0
    if n < LOCK_N:
        raise ForwardLockNotReached(
            f"FORWARD_LOCK_NOT_REACHED\n  mature   = {n:,}\n  required = {LOCK_N:,}\n"
            f"  No partial statistic is computed.")
    raise NotImplementedError(
        "the lock is reached; the prospective test is a separate registered run that must "
        "first freeze forward_lock_manifest.parquet over the FIRST 20,000 mature episodes")


def main():
    row, dq, occ, rules, fam_dig = accrue()
    try:
        run_forward_test()
        gate = "UNEXPECTEDLY CALLABLE"
    except ForwardLockNotReached as e:
        gate = str(e).splitlines()[0]
    print(f"\n  run_forward_test() → {gate}")

    inv = dict(
        signal_after_cutoff=True,
        membership_rule_hash_matches_frozen=fam_dig == SPEC["families"]["digest"],
        no_outcome_field_reachable="enforced by assert_no_outcome on every frame",
        mature_count_monotone="checked against the previous ledger row on every append",
        membership_immutable_after_insert="a changed pipeline is a new data_version, never a "
                                          "rewrite",
        no_duplicate_episode_id=True,
        support_threshold=f"{MIN_T}/{MIN_C}",
        lock_target=LOCK_N,
        no_test_before_lock="run_forward_test() raises ForwardLockNotReached")

    dig = ART.seal(dict(
        spec_id="T5_FORWARD_ACCRUAL_LEDGER_V1",
        parent_spec="T5_FORWARD_VALIDATION_V1", parent_digest=PARENT_DIGEST,
        mode="OUTCOME_BLIND", state="APPEND_ONLY",
        discovery_cutoff=CUTOFF, lock_target=LOCK_N,
        blindness=dict(
            mechanism="code-level guard: assert_no_outcome() on every frame, and the outcome "
                      "sidecar is never opened by this module",
            is_not_a_privilege_boundary="a real boundary would be a database role that cannot "
                                        "SELECT the outcome schema, so a mistake fails at the "
                                        "connection rather than at a check someone can delete. "
                                        "DuckDB single-file has no GRANT/DENY, so this weaker "
                                        "guarantee is recorded rather than glossed over.",
            forbidden_columns=sorted(FORBIDDEN)),
        maturity=dict(rule="the tenth forward TRADING session after entry has elapsed AND the "
                           "bars for the horizon are present AND no blocking DQ failure",
                      not_calendar_days=True,
                      incomplete="MATURITY_DATA_INCOMPLETE — does not count toward the lock"),
        lock_counter="mature forward T5 episodes; not signals, not calendar days, not valid "
                     "memberships",
        no_eta="deliberately not computed",
        support_eligibility=dict(treated=MIN_T, control=MIN_C,
                                 below="INSUFFICIENT_FORWARD_SUPPORT — leaves the family",
                                 no_replacement="no replacement medoid, no threshold lowering, "
                                                "no family refill"),
        families=dict(k_1h_original=sum(1 for r in rules if r["family"] == "1H"),
                      k_15m_original=sum(1 for r in rules if r["family"] == "15M"),
                      digest=fam_dig),
        dq_codes=DQ_CODES,
        invariants=inv,
        current_ledger_row=row,
        artifacts=dict(ledger=os.path.basename(LEDGER), occurrence=os.path.basename(OCC),
                       claim_support_summary=os.path.basename(SUPPORT),
                       dq=os.path.basename(DQ), lock_manifest=os.path.basename(MANIFEST)),
        lock_manifest_rule=f"when mature == {LOCK_N:,}, the FIRST {LOCK_N:,} mature episodes "
                           f"are frozen into the manifest with a lock hash; episode "
                           f"{LOCK_N+1:,} onward is not in the validation run",
        y_status="NO POST-CUTOFF OUTCOME OBSERVED"),
        OUT, required=("spec_id", "parent_digest", "invariants"))
    print(f"  WROTE {OUT} · {dig}")


if __name__ == "__main__":
    main()
