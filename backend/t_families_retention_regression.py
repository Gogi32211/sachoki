"""Retention-leak regression — closes one failure class for good, for all four new families.

The class: an inherited constant from another family's REGISTERED gate silently entering this
family's eligibility path. It happened once (MIN_OVERLAP_RETENTION=0.80 from T5 sat in the
k_final path of four estimands) and was identified during pre-first-use review, before any
estimand execution.

Acceptance criterion, verbatim from the review:

    changing retention from 0.01 -> 0.99 must NOT change
        eligible claim set · k_final · membership classes · claim order
    it MAY change only reported diagnostic fields / summaries

Three layers of proof:

    SIGNATURE   support_eligible(t_ov, c_ov) — retention cannot re-enter without changing the
                argument list, which this test asserts
    DYNAMIC     sweep MIN_OVERLAP_RETENTION over {0.01, 0.5, 0.80, 0.99} and evaluate the
                eligibility function over a grid — outputs must be bit-identical
    SEMANTIC    a sweep over the four families' source files for equivalents of the gate
                ('ret >=', 'retention >=', '>= 0.8', MIN_OVERLAP_RETENTION outside its
                definition/reporting) — none may sit in an eligibility path

And one CONTRAST assertion: T5's estimand must STILL contain its registered gate untouched —
removing it there would be the symmetric mistake.
"""
from __future__ import annotations
import ast, inspect, itertools, os, re, sys                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)

FAMS = ("t9", "t3", "t1", "t4")


def check_signature(mod):
    sig = inspect.signature(mod.support_eligible)
    assert list(sig.parameters) == ["t_ov", "c_ov"], \
        f"{mod.__name__}.support_eligible signature changed: {list(sig.parameters)} — a " \
        f"retention term may have re-entered eligibility"


def check_dynamic(mod):
    grid = list(itertools.product((0, 100, 299, 300, 301, 5000), repeat=2))
    baseline = None
    for r in (0.01, 0.5, 0.80, 0.99):
        mod.MIN_OVERLAP_RETENTION = r          # even if someone re-binds it, it must be inert
        got = tuple(bool(mod.support_eligible(t, c)) for t, c in grid)
        if baseline is None:
            baseline = got
        assert got == baseline, f"{mod.__name__}: eligibility moved with retention={r}"
    exp = tuple((t >= mod.MIN_EP and c >= mod.MIN_EP) for t, c in grid)
    assert baseline == exp, f"{mod.__name__}: eligibility != registered floors"


def check_semantic(tag):
    """No retention-like comparison in an eligibility path of this family's sources."""
    pats = [r"ret\s*>=", r"retention\s*>=", r">=\s*0\.8\b", r">=\s*MIN_OVERLAP_RETENTION"]
    hits = []
    for suffix in ("_sequence_estimand.py", "_sequence_grammar.py", "_outcomes.py", "_dna.py"):
        f = tag + suffix
        if not os.path.exists(f):
            continue
        for i, line in enumerate(open(f), 1):
            code = line.split("#")[0]              # comments may DESCRIBE the removed gate
            if '"' in code and ("registered" in line or "T5" in line):
                continue                            # artifact strings naming the T5 contrast
            for p in pats:
                if re.search(p, code):
                    hits.append(f"{f}:{i}: {line.strip()[:80]}")
    assert not hits, "retention-like comparison found in an eligibility path:\n  " + \
        "\n  ".join(hits)


def check_t5_contrast():
    src = open("t5_sequence_estimand.py").read()
    assert "MIN_OVERLAP_RETENTION" in src and re.search(r"ret\s*>=\s*MIN_OVERLAP_RETENTION",
                                                        src), \
        "T5's REGISTERED retention gate is missing — it must stay exactly as its own freeze " \
        "declared it"


def main():
    import importlib
    for tag in FAMS:
        mod = importlib.import_module(f"{tag}_sequence_estimand")
        check_signature(mod)
        check_dynamic(mod)
        check_semantic(tag)
        print(f"  {tag}: signature ✓ · retention-sweep inert ✓ · semantic sweep clean ✓")
    check_t5_contrast()
    print("  t5: registered gate PRESENT and untouched ✓")
    print("RETENTION REGRESSION: PASS")


if __name__ == "__main__":
    main()
