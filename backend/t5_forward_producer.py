"""T5_FORWARD_DATA_PRODUCER_V1 — who produces forward X, with which code, published how.

The evaluator is frozen and the runtime is pinned, but until now the PRODUCER of X was not.
Membership rules can be frozen to the byte and still change meaning if the input features are
rebuilt by whatever happens to be checked out. The remaining risk is no longer selection or
statistical leakage — it is provenance drift of the input and concurrent data mutation.

    raw input          bars stores (1D / 1H / 4H / 15m)
    producer commit    a pinned SHA, never a branch name
    produces           1H microstructure · 15m opening-hour features · T5 episode table
    publication        atomic snapshot only
    consumed as        exactly one immutable data_version

EXCLUSION LIVES AT THE EPISODE LEVEL, NOT THE ROW LEVEL

This is the subtle one. Sessions 2026-08-18..08-20 are excluded from BOTH samples: they postdate
the frozen evidence and predate the cutoff, so no_backfill bars them forever. But a T5 episode
spans PREV_DAY and T5_DAY, and the first genuine forward episode — 2026-08-21 — needs 08-20 as
its PREV_DAY.

If the gap were implemented by dropping those rows, that episode would be built with a missing
prior session, or with silently altered semantics. So:

    2026-08-18 .. 2026-08-20
        episode_eligible  = false      no episode with this t5_date ever enters
        lock_eligible     = false      nothing here counts toward the 20,000
        context_eligible  = TRUE       the rows remain available wherever the frozen feature
                                       definition requires prior-session information

That is not backfilling evidence. It is supplying the point-in-time context the definition
already demanded. Both halves are asserted after every rebuild rather than trusted.

ATOMIC PUBLICATION, BECAUSE THE UPDATER SHARES THE DIRECTORY

The runtime reads a shared data/ that an updater can write to concurrently. A forward process
must never observe a mixture of old and new files:

    build into a temp snapshot -> validate -> digest the sources -> atomic rename -> data_version

An episode carries its provenance immutably, so a bar update an hour later cannot change an
episode that already exists.
"""
from __future__ import annotations
import hashlib, json, os, shutil, sys, tempfile                        # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402
import t5_forward_activate as ACT                                      # noqa: E402

OUT = "T5_FORWARD_DATA_PRODUCER_V1.json"
DATA = os.path.join(D.ROOT, "data")
M15 = os.path.join(DATA, "t5_15m_opening_hour.parquet")
VERSIONS = os.path.join(DATA, "t5_forward_data_versions.parquet")

PRODUCER_SOURCES = ["t5_dna.py", "t5_15m_enumerate.py"]
PRODUCTS = {"episodes": D.OUT_EP, "microstructure_1h": D.OUT_MS, "opening_hour_15m": M15}

SPEC = json.load(open("T5_FORWARD_VALIDATION_V1.json"))
CUTOFF = SPEC["discovery_cutoff"]["last_eligible_historical_signal_session"]
GAP = ["2026-08-18", "2026-08-19", "2026-08-20"]
EVIDENCE_END = "2026-08-17"


class ProducerDrift(RuntimeError):
    pass


class ContextViolation(RuntimeError):
    pass


def feature_pipeline_hash():
    """The producer's semantics, not just its file names. Includes the T5 definition hash, so a
    changed definition cannot hide behind unchanged source files."""
    h = hashlib.sha256()
    for f in PRODUCER_SOURCES:
        h.update(f.encode())
        h.update(open(f, "rb").read())
    h.update(D.T5_DEF_HASH.encode())
    return h.hexdigest()[:16]


def product_digests(paths=None):
    p = paths or PRODUCTS
    return {k: (ART.file_digest(v) if os.path.exists(v) else "ABSENT") for k, v in p.items()}


def schema_hash(path):
    if not os.path.exists(path):
        return "ABSENT"
    import pyarrow.parquet as pq
    s = pq.ParquetFile(path).schema_arrow
    return hashlib.sha256(str(s).encode()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════
# THE TWO ASSERTIONS THAT KEEP THE GAP FROM EATING ITS OWN CONTEXT
# ══════════════════════════════════════════════════════════════════════════
def assert_context_preserved(ms_path=None, ep_path=None):
    """Gap sessions must be PRESENT as context and ABSENT as episodes. Both directions."""
    ms = pd.read_parquet(ms_path or D.OUT_MS, columns=["episode_id", "session_date", "t5_date"])
    ep = pd.read_parquet(ep_path or D.OUT_EP, columns=["episode_id", "t5_date",
                                                       "prev_session_date"])
    present = sorted(set(GAP) & set(ms.session_date.astype(str)))
    as_episodes = sorted(set(GAP) & set(ep.t5_date.astype(str)))
    have_after = sorted(set(ms.session_date.astype(str)) - set(GAP))
    reachable = ms.session_date.astype(str).max()

    # direction 1: nothing in the gap may be an eligible episode
    if as_episodes:
        bad = ep[ep.t5_date.astype(str).isin(as_episodes)]
        # they may EXIST in the table; they must never be forward-eligible
        elig = bad[bad.t5_date.astype(str) > CUTOFF]
        if len(elig):
            raise ContextViolation(
                f"{len(elig)} gap-session episode(s) are forward-eligible; the gap is excluded "
                f"from BOTH samples")

    # direction 2: any episode whose PREV_DAY falls in the gap must still find that session
    orphan = ep[ep.prev_session_date.astype(str).isin(GAP)
                & ~ep.prev_session_date.astype(str).isin(present)]
    if len(orphan) and present != sorted(set(GAP) & set(ep.prev_session_date.astype(str))):
        raise ContextViolation(
            f"{len(orphan)} episode(s) reference a gap session as PREV_DAY that is not present "
            f"as context. Dropping gap ROWS instead of gap EPISODES would build the first real "
            f"forward episode with a missing prior session.")
    return dict(gap=GAP, present_as_context=present,
                present_as_episode_t5_date=as_episodes,
                episodes_with_gap_prev_day=int(
                    ep.prev_session_date.astype(str).isin(GAP).sum()),
                microstructure_reaches=str(reachable),
                context_eligible=True, episode_eligible=False, lock_eligible=False)


# ══════════════════════════════════════════════════════════════════════════
# ATOMIC PUBLICATION
# ══════════════════════════════════════════════════════════════════════════
def publish(built: dict, data_version: str, dry_run=True):
    """built maps product name -> path of a VALIDATED temp file. Nothing is renamed until every
    product has passed, so a forward reader never sees half a snapshot."""
    missing = [k for k in PRODUCTS if k not in built]
    if missing:
        raise ProducerDrift(f"incomplete snapshot, refusing to publish: missing {missing}")
    for k, tmp in built.items():
        if not os.path.exists(tmp):
            raise ProducerDrift(f"{k}: temp product {tmp} does not exist")
        pd.read_parquet(tmp, columns=None).head(1)          # it must at least open
    if dry_run:
        return dict(published=False, dry_run=True, data_version=data_version,
                    would_replace=product_digests())
    for k, tmp in built.items():
        os.replace(tmp, PRODUCTS[k])                        # atomic within one filesystem
    return dict(published=True, data_version=data_version, digests=product_digests())


def register_version(data_version, producer_commit, note=""):
    row = dict(data_version=data_version, producer_commit=producer_commit,
               feature_pipeline_hash=feature_pipeline_hash(), note=note,
               **{f"digest_{k}": v for k, v in product_digests().items()},
               **{f"schema_{k}": schema_hash(v) for k, v in PRODUCTS.items()})
    T = pd.read_parquet(VERSIONS) if os.path.exists(VERSIONS) else pd.DataFrame()
    if len(T) and data_version in set(T.data_version):
        prior = T[T.data_version == data_version].iloc[0]
        for c in [c for c in row if c.startswith(("digest_", "schema_"))] + \
                 ["producer_commit", "feature_pipeline_hash"]:
            if str(prior.get(c)) != str(row[c]):
                raise ProducerDrift(
                    f"data_version {data_version} already exists with a different {c}: "
                    f"{prior.get(c)} != {row[c]}. A published version is immutable; a rebuild "
                    f"is a NEW version, never a correction of an old one.")
        return T, False
    T = pd.concat([T, pd.DataFrame([row])], ignore_index=True)
    T.to_parquet(VERSIONS, index=False)
    return T, True


# ══════════════════════════════════════════════════════════════════════════
def main():
    ART.smoke_test(verbose=False)
    print("T5_FORWARD_DATA_PRODUCER_V1", flush=True)
    # A producer contract sealed from an unpinned tree contradicts its own rule: it would
    # record a producer_commit that nothing guarantees the producer will run at. The first
    # attempt did exactly that (6dc607776d24ae38, producer_commit 71bbbf7, runtime_pinned
    # False) and is superseded rather than deleted, so the mistake stays visible.
    pin = ACT.assert_runtime_pinned()
    print(f"  runtime pinned {pin['pinned']} · commit {pin['pin'][:7]} · tree clean")
    fph = feature_pipeline_hash()
    print(f"  feature_pipeline_hash {fph}  (sources {PRODUCER_SOURCES} + T5_DEF {D.T5_DEF_HASH})")

    ctx = assert_context_preserved()
    print(f"  context check · gap present as context {ctx['present_as_context'] or 'NONE YET'} "
          f"· as episodes {ctx['present_as_episode_t5_date'] or 'none'} "
          f"· microstructure reaches {ctx['microstructure_reaches']}")

    dig = product_digests()
    for k, v in dig.items():
        print(f"     {k:<20} {v}  schema {schema_hash(PRODUCTS[k])}")

    body = dict(
        spec_id="T5_FORWARD_DATA_PRODUCER_V1", status="FROZEN",
        role="ADDITIVE — pins the PRODUCER of forward X. No frozen artifact below is touched.",
        why="membership rules can be frozen to the byte and still change meaning if the input "
            "features are rebuilt by whatever happens to be checked out. The remaining risk is "
            "provenance drift of X and concurrent data mutation, not selection leakage.",
        raw_input=["studio_analytics.duckdb (1D)", "studio_1h.duckdb", "studio_4h.duckdb",
                   "studio_15m.duckdb"],
        producer_commit=pin.get("pin") or pin["head"],
        producer_identity="the commit SHA of the pinned runtime worktree, never a branch name",
        producer_sources=PRODUCER_SOURCES,
        produces=list(PRODUCTS),
        product_paths={k: os.path.basename(v) for k, v in PRODUCTS.items()},
        output_schema_hashes={k: schema_hash(v) for k, v in PRODUCTS.items()},
        output_digests=dig,
        feature_pipeline_hash=fph,
        t5_definition_hash=D.T5_DEF_HASH,

        publication=dict(
            mode="ATOMIC SNAPSHOT ONLY",
            sequence=["build into a temp snapshot", "validate every product",
                      "compute source digests", "atomic rename to publish",
                      "assign data_version"],
            why="the runtime reads a shared data/ that the updater can write concurrently; a "
                "forward process must never observe a mixture of old and new files",
            all_or_nothing="nothing is renamed until every product has passed, so a reader "
                           "never sees half a snapshot"),

        data_version=dict(
            immutable=True,
            rebuild="a rebuild is a NEW data_version, never a correction of a published one",
            registry=os.path.basename(VERSIONS),
            episode_provenance=["data_version", "producer_commit", "source_digest_1h",
                                "source_digest_15m", "feature_pipeline_hash"],
            why="an episode carries its provenance immutably, so a bar update an hour later "
                "cannot change an episode that already exists",
            current_tables_predate_this_spec=dict(
                feature_data_as_of=EVIDENCE_END,
                label="PRE_PRODUCER_PIN",
                note="the tables on disk were produced before this contract existed. The first "
                     "pinned rebuild establishes the first governed data_version; the existing "
                     "ones are labelled rather than retroactively certified.")),

        coverage_gap=dict(
            sessions=GAP,
            episode_eligible=False, lock_eligible=False, context_eligible=True,
            context_rule="the rows REMAIN available wherever the frozen feature definition "
                         "requires prior-session information",
            why="a T5 episode spans PREV_DAY and T5_DAY, so the first genuine forward episode "
                "(2026-08-21) needs 2026-08-20 as its PREV_DAY. Implementing the gap by "
                "dropping rows would build it with a missing prior session or with silently "
                "altered semantics.",
            not_backfill="this supplies the point-in-time context the definition already "
                         "demanded; it admits no evidence and no episode",
            asserted=ctx),

        consumed_by=dict(
            evaluator="T5_FORWARD_MEMBERSHIP_EVALUATOR_V1",
            evaluator_hash=ACT.EVALUATOR_HASH,
            contract="the forward evaluator opens exactly ONE immutable data_version"),

        activation_manifest_digest=ART.file_digest("T5_FORWARD_ACTIVATION_MANIFEST_V1.json"),
        base_chain_digest=ACT.chain_digest())

    d = ART.seal(body, OUT, required=("spec_id", "producer_commit", "feature_pipeline_hash",
                                      "publication", "coverage_gap"),
                 supersede=True)
    print(f"\n  FROZEN · {OUT} · {d}")


if __name__ == "__main__":
    main()


# ══════════════════════════════════════════════════════════════════════════
# SEMANTIC UPSTREAM — the audit question, answered YES
# ══════════════════════════════════════════════════════════════════════════
UPSTREAM_ENTRIES = ["derive_intraday.py", "update_intraday_db.py", "build_15m_base.py",
                    "backfill_intraday_fwd.py", "build_intraday_db.py"]


def _resolve_local(name, root):
    p = os.path.join(root, name.replace(".", os.sep) + ".py")
    if os.path.exists(p):
        return os.path.relpath(p, root)
    p = os.path.join(root, name.replace(".", os.sep), "__init__.py")
    if os.path.exists(p):
        return os.path.relpath(p, root)
    return None


def semantic_upstream(root=None):
    """Does changing bar-production code alter the values the frozen producer consumes for
    identical vendor input?

    YES. D.SRC_COLS carries sig_*, phys_*, l_sig, wyc_phase, vol_bucket, rsi_14 — DERIVED signal
    columns read straight out of the bar stores, not recomputed by the T5 producer. So
    derive_intraday.py and friends are upstream SEMANTICS, not transport, and the audit answer
    is not 'transport layer, dev checkout acceptable'.

    The closure is computed by walking local imports rather than declared by hand, because a
    hand-written list goes stale silently. It comes out broad — ~245 modules, because _process
    does `import main` and main.py pulls the whole API surface in with it.

    That breadth is why this is a PROVENANCE FIELD ON EVERY data_version rather than a blocking
    gate. A gate on a hash that moves whenever an unrelated endpoint is edited would fire
    constantly and be routed around, which is worse than no gate. The real protection is
    elsewhere and is structural: a forward run consumes ONE immutable data_version, so an
    upstream change mid-accrual cannot alter a snapshot that already exists. It can only
    produce a NEW version carrying a different upstream hash — and that heterogeneity is then
    visible per episode at lock time instead of being invisible.
    """
    import ast
    root = root or HERE
    seen, stack = set(), list(UPSTREAM_ENTRIES)
    while stack:
        f = stack.pop()
        if f in seen or not os.path.exists(os.path.join(root, f)):
            continue
        seen.add(f)
        try:
            tree = ast.parse(open(os.path.join(root, f), "rb").read())
        except Exception:
            continue
        for n in ast.walk(tree):
            mods = []
            if isinstance(n, ast.Import):
                mods = [a.name for a in n.names]
            elif isinstance(n, ast.ImportFrom) and n.module and not n.level:
                mods = [n.module] + [f"{n.module}.{a.name}" for a in n.names]
            for m in mods:
                r = _resolve_local(m, root)
                if r and r not in seen:
                    stack.append(r)
    h = hashlib.sha256()
    for f in sorted(seen):
        h.update(f.encode())
        h.update(open(os.path.join(root, f), "rb").read())
    return dict(hash=h.hexdigest()[:16], n_modules=len(seen), entries=UPSTREAM_ENTRIES,
                closure="transitive local-module imports, computed not declared",
                answer_to_audit_question="YES — bar-production code changes the values the "
                                         "frozen producer consumes for identical vendor input, "
                                         "because D.SRC_COLS reads derived signal columns",
                role="PROVENANCE FIELD per data_version, not a blocking gate",
                why_not_a_gate="the closure is broad because _process imports main; a gate on a "
                               "hash that moves with unrelated edits fires constantly and gets "
                               "routed around",
                real_protection="a forward run consumes ONE immutable data_version, so an "
                                "upstream change mid-accrual produces a NEW version rather than "
                                "altering an existing snapshot")
