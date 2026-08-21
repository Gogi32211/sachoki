"""Artifact writer with a contract, because a correct result that never reaches disk is not
evidence.

A 55-minute sweep passed all five of its data gates and then died in the five lines that
serialize the answer. Every gate in that run asked a question about the DATA; none asked
whether the artifact could be produced. Those are different validations and both are needed:

    research validation          is the measurement right?
    artifact-production validation   can the measurement be written, re-read, and trusted?

THE CONTRACT

    build_payload(...)      pure; no I/O, no globals, no side effects
    validate(payload)       schema + type + finiteness, raises with the offending path
    seal(payload, path)     dumps(allow_nan=False) -> tmp -> fsync -> rename -> READ BACK
                            -> validate again -> return digest

NaN AND Inf ARE REFUSED, NOT COERCED

json.dumps writes bare NaN/Infinity by default, which is invalid JSON that many parsers accept
silently and others reject days later. allow_nan=False turns that into an error here, where
the run can still be fixed.

`default=str` IS NOT USED

It makes every serialization succeed, including the ones that should fail: a numpy array
becomes its repr, an unexpected object becomes a string that looks like data. The converter
below handles numpy scalars explicitly and raises on anything it does not recognise.

THE SMOKE TEST RUNS ON A FIXTURE, BEFORE THE LONG RUN

The production payload must not be the first place the serializer actually executes. The
fixture below carries the shapes that break serializers in practice — numpy integer and float
scalars, numpy bool, nested dicts, tuple keys' absence, empty containers — and it goes through
the SAME build/validate/seal functions.
"""
from __future__ import annotations
import hashlib, json, math, os, shutil, tempfile                      # noqa: E402
import numpy as np                                                    # noqa: E402


class ArtifactError(RuntimeError):
    pass


def _conv(o):
    """Explicit conversion. Anything not named here is an error, not a str()."""
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        f = float(o)
        if not math.isfinite(f):
            raise ArtifactError(f"non-finite numpy float in payload: {o!r}")
        return f
    if isinstance(o, np.bool_):
        return bool(o)
    if isinstance(o, np.ndarray):
        raise ArtifactError("ndarray in payload — write arrays to parquet, not JSON")
    raise ArtifactError(f"unserializable object of type {type(o).__name__}: {o!r}")


def validate(payload, required=(), _path="$"):
    """Type and finiteness check with the offending path named, plus required top-level keys."""
    if _path == "$":
        if not isinstance(payload, dict):
            raise ArtifactError("payload must be a dict")
        miss = [k for k in required if k not in payload]
        if miss:
            raise ArtifactError(f"payload missing required keys: {miss}")
    if isinstance(payload, dict):
        for k, v in payload.items():
            if not isinstance(k, str):
                raise ArtifactError(f"{_path}: non-string key {k!r}")
            validate(v, (), f"{_path}.{k}")
    elif isinstance(payload, (list, tuple)):
        for i, v in enumerate(payload):
            validate(v, (), f"{_path}[{i}]")
    elif isinstance(payload, float):
        if not math.isfinite(payload):
            raise ArtifactError(f"{_path}: non-finite float {payload!r}")
    elif isinstance(payload, (np.integer, np.floating, np.bool_)):
        _conv(payload)
    elif payload is None or isinstance(payload, (str, int, bool)):
        pass
    else:
        raise ArtifactError(f"{_path}: unsupported type {type(payload).__name__}")
    return True


def seal(payload, path, required=(), supersede=False):
    """Validate, write atomically, read back, validate again. Returns the content digest.

    A SEAL THAT SILENTLY OVERWRITES IS NOT A SEAL

    seal() writes to a fixed path, so re-sealing an artifact used to destroy its predecessor's
    bytes: only the successor's own record of the old digest survived. That happened once, to
    T5_FORWARD_SESSION_LENGTH_V1 (021c53cd32236cd3), and the predecessor is unrecoverable — it
    is NOT reconstructed here, because rebuilding an artifact after the fact is fabrication
    rather than preservation.

    So overwriting with different content now REFUSES unless supersede=True, and supersede
    copies the existing file to <name>.superseded.<digest>.json before replacing it. Identical
    content is a no-op re-run and always allowed.
    """
    validate(payload, required)
    text = json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False, default=_conv)
    d = os.path.dirname(os.path.abspath(path)) or "."

    if os.path.exists(path):
        old = hashlib.sha256(open(path, "rb").read()).hexdigest()[:16]
        new = hashlib.sha256(text.encode()).hexdigest()[:16]
        if old != new:
            if not supersede:
                raise ArtifactError(
                    f"refusing to overwrite {os.path.basename(path)} ({old}) with different "
                    f"content ({new}). A frozen artifact is replaced deliberately or not at "
                    f"all: pass supersede=True, which preserves the current file as "
                    f"{os.path.basename(path)}.superseded.{old}.json first.")
            keep = f"{path}.superseded.{old}.json"
            if not os.path.exists(keep):
                shutil.copy2(path, keep)
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        with open(tmp) as f:
            back = json.load(f)
        validate(back, required)
        if json.dumps(back, sort_keys=True, allow_nan=False) != \
           json.dumps(json.loads(text), sort_keys=True, allow_nan=False):
            raise ArtifactError("round-trip mismatch: what was written is not what was read")
        os.replace(tmp, path)
        tmp = None
    finally:
        if tmp and os.path.exists(tmp):
            os.unlink(tmp)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def file_digest(path):
    """The digest seal() returned for this artifact, recovered from the file itself.

    seal() hashes the exact text it writes and RETURNS the digest; it does not store it inside
    the payload, because a field holding its own content hash cannot be consistent. So the
    canonical identity of an artifact is the hash of its bytes, and every gate that checks
    "is this the spec I froze" must read it this way rather than looking for a spec_digest key
    that was never written.
    """
    return hashlib.sha256(open(path, "rb").read()).hexdigest()[:16]


def smoke_test(verbose=True):
    """The shapes that break serializers, through the same functions the long run uses."""
    fx = dict(
        spec_id="FIXTURE", n=np.int64(1032323), share=np.float64(0.9701),
        flag=np.bool_(True), nested=dict(a=[np.int32(1), np.int32(2)], b={"c": 1.5}),
        empty_list=[], empty_dict={}, none_value=None, unicode="M1→M2→M3 · ✓")
    tmp = os.path.join(tempfile.gettempdir(), "t5_artifact_fixture.json")
    dig = seal(fx, tmp, required=("spec_id", "n"))
    back = json.load(open(tmp))
    os.unlink(tmp)
    assert back["n"] == 1032323 and isinstance(back["n"], int)
    assert back["flag"] is True and back["unicode"].startswith("M1")

    checks = []
    for name, bad in (("NaN", dict(x=float("nan"))),
                      ("Inf", dict(x=float("inf"))),
                      ("numpy NaN", dict(x=np.float64("nan"))),
                      ("ndarray", dict(x=np.arange(3))),
                      ("non-string key", {1: "a"}),
                      ("unknown object", dict(x=object()))):
        try:
            seal(bad, os.path.join(tempfile.gettempdir(), "t5_bad.json"))
            checks.append((name, False))
        except (ArtifactError, TypeError, ValueError):
            checks.append((name, True))
    missing_ok = False
    try:
        seal(dict(a=1), os.path.join(tempfile.gettempdir(), "t5_bad.json"),
             required=("spec_id",))
    except ArtifactError:
        missing_ok = True
    if verbose:
        print(f"  fixture sealed · digest {dig}")
        for n, ok in checks:
            print(f"    {'✓' if ok else '✗'} refuses {n}")
        print(f"    {'✓' if missing_ok else '✗'} refuses missing required key")
    if not all(ok for _, ok in checks) or not missing_ok:
        raise ArtifactError("artifact writer smoke test FAILED")
    return True


if __name__ == "__main__":
    print("ARTIFACT WRITER SMOKE TEST")
    smoke_test()
    print("  PASS")
