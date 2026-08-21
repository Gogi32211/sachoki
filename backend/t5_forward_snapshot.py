"""Snapshot-level atomic publication. Three atomic renames are not an atomic snapshot.

THE WINDOW THAT FILE-LEVEL RENAMES LEAVE OPEN

Publishing three products by three independent renames leaves a real interval in which a
consumer can observe

    episodes            version B
    microstructure_1h   version B
    opening_hour_15m    version A

Each individual rename is atomic; the SET is not. So the unit of publication is a DIRECTORY,
renamed once:

    .tmp/v_000002/          write all three -> validate all three -> digest -> manifest
                            -> fsync -> rename the DIRECTORY -> update CURRENT

A consumer never opens "the latest parquet files". It resolves a data_version once and reads
only inside that directory for the whole run. If v_000003 is published meanwhile, the episodes
already being processed are untouched by construction rather than by timing luck.

CURRENT IS A POINTER, AND POINTING IS THE LAST STEP

The directory exists and is complete before anything points at it, so a reader that resolves
CURRENT can never land inside a half-built version.
"""
from __future__ import annotations
import hashlib, json, os, shutil, sys, tempfile                        # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402
import t5_forward_activate as ACT, t5_forward_producer as PR           # noqa: E402

VERSIONS = os.path.join(D.ROOT, "data", "t5_versions")
CURRENT = os.path.join(VERSIONS, "CURRENT")
TMP = os.path.join(D.ROOT, "data", ".t5_versions_tmp")
PRODUCTS = ["episodes.parquet", "microstructure_1h.parquet", "opening_hour_15m.parquet"]


class SnapshotError(RuntimeError):
    pass


def next_version():
    os.makedirs(VERSIONS, exist_ok=True)
    have = [d for d in os.listdir(VERSIONS) if d.startswith("v_")]
    n = max([int(d[2:]) for d in have], default=0) + 1
    return f"v_{n:06d}"


def resolve_current():
    """What a consumer reads. One resolution per run, then never re-resolved."""
    if not os.path.exists(CURRENT):
        return None
    v = open(CURRENT).read().strip()
    d = os.path.join(VERSIONS, v)
    if not os.path.isdir(d):
        raise SnapshotError(f"CURRENT points at {v} which does not exist")
    return dict(data_version=v, dir=d,
                manifest=json.load(open(os.path.join(d, "manifest.json"))),
                paths={p.split(".")[0]: os.path.join(d, p) for p in PRODUCTS})


def build_and_publish(sources: dict, session_context: dict, dry_run=True):
    """sources maps product filename -> an existing file to place in the snapshot.

    Nothing is visible until every product is written, validated and digested, the manifest is
    written, everything is fsynced, and the DIRECTORY is renamed as one operation."""
    pin = ACT.assert_runtime_pinned()
    missing = [p for p in PRODUCTS if p not in sources]
    if missing:
        raise SnapshotError(f"incomplete snapshot, refusing to publish: missing {missing}")

    v = next_version()
    os.makedirs(TMP, exist_ok=True)
    stage = os.path.join(TMP, v)
    if os.path.exists(stage):
        shutil.rmtree(stage)
    os.makedirs(stage)

    digests, rows = {}, {}
    for p in PRODUCTS:
        dst = os.path.join(stage, p)
        shutil.copy2(sources[p], dst)
        df = pd.read_parquet(dst)                      # validation: it must open and be non-empty
        if not len(df):
            raise SnapshotError(f"{p} is empty; refusing to publish a snapshot with no rows")
        rows[p] = int(len(df))
        digests[p] = ART.file_digest(dst)

    manifest = dict(
        data_version=v,
        producer_commit=pin["pin"],
        feature_pipeline_hash=PR.feature_pipeline_hash(),
        semantic_upstream=PR.semantic_upstream(),
        t5_definition_hash=D.T5_DEF_HASH,
        product_digests=digests, product_rows=rows,
        session_context=session_context,
        atomicity="the DIRECTORY is renamed once; three file-level renames would leave a window "
                  "in which a consumer sees a mixture of versions",
        immutable="a published version is never modified; a rebuild is a NEW version",
        consumed_as="a consumer resolves this data_version once and reads only inside this "
                    "directory for the whole run")
    ART.seal(manifest, os.path.join(stage, "manifest.json"),
             required=("data_version", "producer_commit", "product_digests"))

    fd = os.open(stage, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

    if dry_run:
        shutil.rmtree(stage)
        return dict(published=False, dry_run=True, data_version=v, digests=digests, rows=rows)

    final = os.path.join(VERSIONS, v)
    os.rename(stage, final)                            # ONE atomic operation for the whole set
    tmp_ptr = CURRENT + ".tmp"
    with open(tmp_ptr, "w") as f:
        f.write(v)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_ptr, CURRENT)                       # pointing is the LAST step
    return dict(published=True, data_version=v, dir=final, digests=digests, rows=rows)


def main():
    print("T5 forward snapshot store")
    cur = resolve_current()
    print(f"  versions dir   {VERSIONS}")
    print(f"  CURRENT        {cur['data_version'] if cur else 'none published yet'}")
    print(f"  next version   {next_version()}")
    u = PR.semantic_upstream()
    print(f"  upstream hash  {u['hash']} · {u['n_modules']} modules · {u['role']}")


if __name__ == "__main__":
    main()
