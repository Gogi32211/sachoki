"""RE-EXECUTION of the four ▽△ families on the corrected anatomy history. k does NOT increase.

WHY. anatomy_build.py's `key` compared each prior low against its own row's window low — a moving
threshold — where main.py::api_day1h compares all 25 against ONE threshold, the window low at bar i.
It agreed with the chart on 54.7 % of days, bidirectionally. Over the 3,177,075 overlapping sessions
the correction moved:

    key / loc / s   43.938 %       at_floor  0.556 %       v  0.387 %       rs  0.000 %

so ANATOMY_LADDER_V1 (all three ladders read the score) and TURN_V1's TURN_QUALITY rung were VOID,
and the four verdict-based contrasts were unverified rather than wrong. Found by
backend/tests/test_anatomy_parity.py, which now guards the transcription.

WHAT THIS IS NOT. Not a new question, not a re-cut, not a second look. Each family's registry — the
ladders, the contrasts, the expected directions, the rung edges, the seeds, the shuffle counts, the
stop rule — is the SAME OBJECT: this driver imports the V1 module and rebinds only FAMILY and
FAMILY_DIR, so every declaration is executed from the identical source file. That is asserted, not
asserted-by-comment: before running anything, the module's sha256 is compared against the digest V1
recorded in its own SEAL.json at seal time. If the registry source moved even one byte since V1 was
sealed, this is re-specification rather than re-execution and the driver refuses to start.

The only inputs that differ are data/anatomy_signals.parquet and, downstream of it, X.parquet. Each
new SEAL.json records the new anatomy_build digest, so the difference is visible in the artifact.

RUN
  backend/.venv/bin/python backend/anatomy_rerun_keyfix.py            # all four
  backend/.venv/bin/python backend/anatomy_rerun_keyfix.py RS_TURN    # one
"""
from __future__ import annotations
import hashlib
import importlib
import json
import os
import sys
import time
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SUFFIX = "_R2_KEYFIX"
MASSIVE = "/Users/sachoki/MASSIVE_DATA"

# (short name, module, V1 family name, the SEAL.json code key that digests that module)
FAMILIES = [
    ("ANATOMY_LADDER", "anatomy_ladder_family", "ANATOMY_LADDER_V1", "anatomy_ladder_family"),
    ("TURN",           "turn_family",           "TURN_V1",           "turn_family"),
    ("ADJACENT_TURN",  "adjacent_turn_family",  "ADJACENT_TURN_V1",  "adjacent_turn_family"),
    ("RS_TURN",        "rs_turn_family",        "RS_TURN_V1",        "rs_turn_family"),
]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _assert_same_registry(mod, v1_family, code_key):
    """The registry must be the SAME SOURCE V1 sealed. Otherwise this is not a re-execution."""
    sp = os.path.join(MASSIVE, v1_family, "SEAL.json")
    if not os.path.exists(sp):
        raise SystemExit(f"{v1_family}: no SEAL.json — nothing to re-execute")
    v1 = json.load(open(sp))
    was = (v1.get("code") or {}).get(code_key)
    now = _dig(mod.__file__)
    if was is None:
        raise SystemExit(f"{v1_family}: SEAL.json has no code['{code_key}'] to compare against")
    if was != now:
        raise SystemExit(
            f"{v1_family}: registry source CHANGED since V1 was sealed ({was} -> {now}). "
            f"That would be re-specification, not re-execution. Refusing.")
    return v1


def run_one(short, modname, v1_family, code_key, log=print):
    mod = importlib.import_module(modname)
    v1 = _assert_same_registry(mod, v1_family, code_key)
    fam = v1_family.replace("_V1", "") + SUFFIX
    fdir = os.path.join(MASSIVE, fam)
    log(f"\n{'='*78}\n{short}: re-executing {v1_family}'s registry as {fam}")
    log(f"  registry source {modname}.py {_dig(mod.__file__)} == V1 seal  ✓")
    log(f"  V1 sealed {v1.get('sealed_at')} · k {v1.get('k')} · "
        f"anatomy_build then {(v1.get('code') or {}).get('anatomy_build')} "
        f"now {_dig(os.path.join(HERE, 'anatomy_build.py'))}")

    mod.FAMILY, mod.FAMILY_DIR = fam, fdir
    os.makedirs(fdir, exist_ok=True)
    json.dump(dict(
        reexecution_of=v1_family, reason="anatomy_build.py `key` used a moving threshold; the 0-8 "
        "score disagreed with main.py::api_day1h on 43.9% of sessions and the verdict on 0.387%",
        registry_source=modname + ".py", registry_sha256_16=_dig(mod.__file__),
        registry_identical_to_v1=True, k_unchanged=v1.get("k"),
        v1_sealed_at=v1.get("sealed_at"),
        anatomy_build_v1=(v1.get("code") or {}).get("anatomy_build"),
        anatomy_build_now=_dig(os.path.join(HERE, "anatomy_build.py")),
        started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        open(os.path.join(fdir, "REEXECUTION.json"), "w"), indent=1)

    t0 = time.time()
    mod.build(log=log)
    log(f"  built ({time.time()-t0:.0f}s)")
    s = mod.seal()
    log(f"  sealed {s.get('registry_sha256_16')} · k {s.get('k')} ({time.time()-t0:.0f}s)")
    out = mod.outcome(log=log)
    log(f"  outcome done ({time.time()-t0:.0f}s)")
    return out


if __name__ == "__main__":
    want = {a.upper() for a in sys.argv[1:]}
    done = {}
    for short, modname, v1, key in FAMILIES:
        if want and short not in want:
            continue
        try:
            done[short] = run_one(short, modname, v1, key)
        except Exception as e:
            print(f"\n!! {short} FAILED: {type(e).__name__}: {e}")
            traceback.print_exc()
            done[short] = {"error": f"{type(e).__name__}: {e}"}
    print(f"\n{'='*78}\nRE-EXECUTION SUMMARY")
    print(json.dumps(done, indent=1, default=str)[:6000])
