"""The PT family registry — one place that knows what each family's frozen structures ARE.

Semantics are NOT redefined here. They are lifted verbatim from the PT5 implementation:

    BASE    every canonical episode of the family
    1H      at least one frozen 1H overlap-cluster MEDOID matches
    15M     at least one frozen 15m cluster representative matches
    STRONG  both 1H and 15M
    class ladder is exclusive: STRONG > 15M > 1H > BASE

Only the MEDOIDS enter, never every historical survivor: within-cluster aliases must not
make a preview "stronger". T5 has 9 sealed 1H survivors and 4 medoids; T9 6 and 4; T3 8
and 7; T1 9 and 9 — T1's overlap graph produced nine singleton components, so there all
nine survivors ARE medoids.

Availability is three-valued. A family without a 15m phase reports 15M and STRONG as
UNAVAILABLE, never as FALSE. "We have not looked" and "we looked and found nothing" are
different facts and the UI must be able to tell them apart.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, os, sys                                                 # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)

AVAILABLE_TRUE = "AVAILABLE_TRUE"
AVAILABLE_FALSE = "AVAILABLE_FALSE"
UNAVAILABLE = "UNAVAILABLE"

FAMILIES = ("T5", "T9", "T3", "T1")


def medoids(fam: str) -> dict:
    """The frozen overlap-cluster medoids, read from that family's sealed characterization.

    T5 predates the shared characterization artifact, so its four medoids are read from the
    PT5 implementation that already governs them.
    """
    if fam == "T5":
        import pt5_build as PT5
        return {f"H1_{chr(65+i)}": claim
                for i, claim in enumerate(PT5.H1_FAMILIES.values())}
    art = f"{fam}_SURVIVOR_CHARACTERIZATION_V1.json"
    d = json.load(open(art))
    out = {}
    for r in d["medoid_characterization"]:
        out[f"j{r['j']}"] = r["medoid"]
    return out


def sealed_supports(fam: str) -> dict:
    if fam == "T5":
        import pt5_build as PT5
        return dict(PT5.H1_SEALED_COUNTS)
    d = json.load(open(f"{fam}_SURVIVOR_CHARACTERIZATION_V1.json"))
    return {r["medoid"]: int(r["support"]) for r in d["medoid_characterization"]}


def phases(fam: str) -> dict:
    """Which PT layers this family HAS. Missing is UNAVAILABLE, never FALSE."""
    has_15m = os.path.exists(f"{fam}_15M_CLUSTER_REPRESENTATIVES_V1.json") or fam == "T5"
    return dict(BASE=True, H1=True, M15=has_15m, STRONG=has_15m)


def dna(fam: str):
    import importlib
    return importlib.import_module(f"{fam.lower()}_dna")


def date_col(fam: str) -> str:
    return f"{fam.lower()}_date"


def definition_hash(fam: str) -> str:
    return getattr(dna(fam), f"{fam}_DEF_HASH")


def summary():
    out = {}
    for f in FAMILIES:
        m = medoids(f)
        out[f] = dict(medoids=len(m), claims=list(m.values()), phases=phases(f))
    return out


if __name__ == "__main__":
    for f, d in summary().items():
        av = "".join("Y" if d["phases"][k] else "-" for k in ("BASE", "H1", "M15", "STRONG"))
        print(f"{f}: medoids {d['medoids']} · phases BASE/1H/15M/STRONG = {av}")
        for c in d["claims"]:
            print(f"    {c}")
