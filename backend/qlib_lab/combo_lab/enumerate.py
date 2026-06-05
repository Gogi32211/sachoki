"""
enumerate.py — generate the candidate combo set from validated predicates.

Predicates are reused verbatim from ai_journal.bootstrap.PREDICATES — they are
the same atoms whose forward edges we already validated. We enumerate:
  - all singles                       (≈35)
  - all pairs (AND only, unordered)   (≈595)
  - all triples (AND only, unordered) (≈6500)
Filtering pairs/triples that are tautological (e.g. fly_abcd AND fly_cd are
mutually exclusive in source — we don't enumerate those).
"""
from __future__ import annotations
from itertools import combinations
import hashlib

# Reuse the curated predicate atoms.
from ai_journal.bootstrap import PREDICATES as _ATOM_LIST

# Predicate-name -> SQL
ATOMS: dict[str, str] = {name: sql for name, _cat, sql in _ATOM_LIST}

# Mutually-exclusive groups — never combined in an AND.
_EXCLUSIVE_GROUPS = [
    {"fly_abcd", "fly_cd"},
    {"vol_20x", "vol_10x", "vol_5x"},
    {"ult_ge2", "ult_ge1"},
    {"preup_p3", "preup_p2", "preup_p89", "preup_p50"},
    {"phase_D", "phase_C"},
    {"abs_and_bc", "abs", "bc"},          # already an aggregation hierarchy
    {"v3_ge40", "v3_ge30"},
]


def _is_redundant(names: tuple[str, ...]) -> bool:
    s = set(names)
    for g in _EXCLUSIVE_GROUPS:
        if len(s & g) >= 2:
            return True
    return False


def combo_id(names: tuple[str, ...]) -> str:
    key = "&".join(sorted(names))
    return hashlib.md5(key.encode()).hexdigest()[:12]


def enumerate_combos(sizes=(1, 2, 3)) -> list[dict]:
    """Returns [{combo_id, predicates(sorted tuple), size, sql}]."""
    names = list(ATOMS.keys())
    out = []
    for k in sizes:
        for combo in combinations(names, k):
            if _is_redundant(combo):
                continue
            sql = " AND ".join(f"({ATOMS[n]})" for n in combo)
            out.append({
                "combo_id": combo_id(combo),
                "predicates": tuple(sorted(combo)),
                "size": k,
                "sql": sql,
            })
    return out


if __name__ == "__main__":
    cs = enumerate_combos()
    by_size = {}
    for c in cs:
        by_size[c["size"]] = by_size.get(c["size"], 0) + 1
    print("enumerated combos:", by_size, "total:", len(cs))
    print("\nsample triples:")
    for c in [x for x in cs if x["size"] == 3][:5]:
        print(" ", c["predicates"])
