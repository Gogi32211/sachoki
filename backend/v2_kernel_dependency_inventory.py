"""Step 3A: what the functions to be extracted actually read, before anything is moved.

The risk in this extraction was never the formula. It is the closure. `boot()` and `verdict()`
live inside `v2_sealed_run.main()`, inside two nested loops, and they read whatever happens to
be in scope — support objects, dates, policy constants, RNG coordinates, loop variables. Moving
them out means naming every one of those, and the failure mode is silent: a dependency that gets
supplied from a slightly different place still runs, still looks right, and computes something
else.

So the inventory is taken by PARSING the current source, not by reading it. A free variable is a
name a function uses and does not bind — parameters, local assignments, comprehension targets
and builtins removed. Whatever is left is what the extraction must pass in explicitly.

WHAT THIS IS LOOKING FOR, specifically. Any capability-experiment coordinate that leaks into a
function the historical engine will also call. The sealed run knows things a historical run
cannot: which cell had a needle planted, how large δ was, which synthetic world and replicate is
being processed. A function that reads those is not a generic kernel, however much it looks like
one, and the criterion is the user's:

    if a parameter exists only because the sealed experiment knows the planted δ,
    it does not belong to the historical engine core.

Nothing here is moved, changed or improved. This file only looks.
"""
from __future__ import annotations

import ast
import builtins
import hashlib
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
INVENTORY = os.path.join(HERE, "V2_KERNEL_DEPENDENCIES.json")

# The functions the extraction will move, and where they live today.
TARGETS = [("v2_sealed_run.py", "main", "boot"),
           ("v2_sealed_run.py", "main", "verdict"),
           ("v2_kernel.py", None, "Frozen"),
           ("v2_kernel.py", None, "verdict"),
           ("v2_kernel.py", None, "support_hash")]

# Coordinates that exist only because a needle was planted. A kernel that reads one of these is
# a capability harness wearing a kernel's name.
CAPABILITY_COORDINATES = {"delta", "planted", "truth", "world", "rep", "seeds", "injected",
                          "DELTA_GRID", "M_PER_CLASS", "inject"}

_BUILTINS = set(dir(builtins))


class _Scope(ast.NodeVisitor):
    """Bound names inside one function: parameters, assignments, comprehension targets."""

    def __init__(self):
        self.bound: set = set()

    def visit_Name(self, node):
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.bound.add(node.id)
        self.generic_visit(node)

    def visit_arg(self, node):
        self.bound.add(node.arg)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        self.bound.add(node.name)
        self.generic_visit(node)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node):
        self.bound.add(node.name)
        self.generic_visit(node)


def _free_names(fn: ast.AST) -> list:
    scope = _Scope()
    for a in getattr(fn, "args", ast.arguments(args=[])).args if hasattr(fn, "args") else []:
        scope.bound.add(a.arg)
    scope.visit(fn)
    used = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)}
    attr_roots = set()
    for n in ast.walk(fn):
        if isinstance(n, ast.Attribute):
            root = n
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name):
                attr_roots.add(root.id)
    return sorted((used | attr_roots) - scope.bound - _BUILTINS)


def _find(tree: ast.AST, outer: str | None, name: str):
    """The target function, optionally nested inside `outer`."""
    if outer is None:
        for n in ast.walk(tree):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) \
                    and n.name == name:
                return n
        return None
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == outer:
            for m in ast.walk(n):
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name == name:
                    return m
    return None


def build() -> dict:
    out = {}
    for filename, outer, name in TARGETS:
        path = os.path.join(HERE, filename)
        with open(path) as f:
            tree = ast.parse(f.read(), filename=path)
        fn = _find(tree, outer, name)
        if fn is None:
            out[name] = {"error": f"{name} not found in {filename}"}
            continue
        free = _free_names(fn)
        leaks = sorted(set(free) & CAPABILITY_COORDINATES)
        out[name] = {
            "file": filename,
            "nested_in": outer,
            "params": [a.arg for a in fn.args.args] if hasattr(fn, "args") else [],
            "free_names": free,
            "capability_coordinates_read": leaks,
            "extractable_verbatim": not leaks,
        }
    payload = {"inventory_version": "v2_kernel_dependencies_v1", "targets": out}
    payload["inventory_hash"] = hashlib.sha256(
        json.dumps(out, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
    return payload


if __name__ == "__main__":
    fresh = build()
    print("=" * 100, flush=True)
    print("  V2 KERNEL DEPENDENCY INVENTORY — taken before anything moves", flush=True)
    print("=" * 100, flush=True)
    for name, d in fresh["targets"].items():
        if "error" in d:
            print(f"  {name}: {d['error']}", flush=True)
            continue
        where = f"{d['file']}::{d['nested_in']}::{name}" if d["nested_in"] else \
            f"{d['file']}::{name}"
        print(f"\n  {where}", flush=True)
        print(f"    params        {d['params']}", flush=True)
        print(f"    free names    {d['free_names']}", flush=True)
        if d["capability_coordinates_read"]:
            print(f"    LEAKS         {d['capability_coordinates_read']}", flush=True)
            print(f"    verdict       NOT extractable verbatim — these coordinates exist only "
                  f"because a needle was planted", flush=True)
        else:
            print(f"    LEAKS         none", flush=True)
            print(f"    verdict       extractable verbatim", flush=True)

    if os.path.exists(INVENTORY) and "--force" not in sys.argv:
        with open(INVENTORY) as f:
            frozen = json.load(f)
        same = frozen["inventory_hash"] == fresh["inventory_hash"]
        print(f"\n  frozen {frozen['inventory_hash']} · recomputed {fresh['inventory_hash']} · "
              f"{'IDENTICAL' if same else 'CHANGED'}", flush=True)
        sys.exit(0 if same else 1)
    with open(INVENTORY, "w") as f:
        json.dump(fresh, f, indent=1, sort_keys=True)
    print(f"\n  inventory hash {fresh['inventory_hash']} · written to "
          f"{os.path.basename(INVENTORY)}", flush=True)
