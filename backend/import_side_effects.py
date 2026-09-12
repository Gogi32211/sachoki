"""Static sweep for MUTATING side effects that happen merely by importing a module.

WHY THIS EXISTS. paths.py used to call os.makedirs(DATA_DIR, exist_ok=True) at module
level. Importing a path-constants module therefore created a directory — and once
DATA_DIR became a symlink to an external volume, that one line was capable of building a
phantom copy of the canonical tree on the internal disk. The mount guard cannot defend
against this, because the damage is done before any writer runs.

The defect was found by a runtime fixture. This module exists so it is also caught
STATICALLY, and so an equivalent line cannot reappear in a later refactor without a test
going red.

WHAT COUNTS AS IMPORT TIME. Any call reachable at module level: bare statements, and also
bodies of module-level `if`/`try`/`for`/`with`, which execute on import just the same. A
call inside a `def` or `class` body does not, and is not reported — that is a writer doing
its job, which is exactly where mutation belongs.

Grep cannot make that distinction (it cannot tell `os.makedirs(...)` at column 0 from the
same text inside a function), so this walks the AST instead.
"""
from __future__ import annotations

import ast
import os
import sys

# calls that change the filesystem or open a database
MUTATORS = {
    "os.makedirs", "os.mkdir", "os.rename", "os.replace", "os.remove", "os.unlink",
    "os.rmdir", "os.symlink", "os.link", "os.truncate", "os.chmod", "os.chown",
    "shutil.copy", "shutil.copy2", "shutil.copytree", "shutil.move", "shutil.rmtree",
    "duckdb.connect", "sqlite3.connect", "pathlib.Path.mkdir", "Path.mkdir",
    "Path.touch", "Path.write_text", "Path.write_bytes", "Path.unlink",
    "to_parquet", "to_csv", "NamedTemporaryFile", "mkstemp", "mkdtemp",
}
WRITE_MODES = {"w", "a", "x", "w+", "a+", "x+", "wb", "ab", "xb", "wb+", "ab+", "r+"}


def _name(node: ast.AST) -> str:
    """Dotted name of a call target, e.g. 'os.makedirs' or 'Path.mkdir'."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return ""


def _is_write_open(call: ast.Call) -> bool:
    """open(..., 'w') and friends — a file creation dressed as a read."""
    if _name(call.func) not in ("open", "io.open", "codecs.open"):
        return False
    mode = None
    if len(call.args) > 1 and isinstance(call.args[1], ast.Constant):
        mode = call.args[1].value
    for kw in call.keywords:
        if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
            mode = kw.value.value
    return isinstance(mode, str) and mode in WRITE_MODES


def scan_source(src: str, path: str) -> list[dict]:
    """Every mutating call reachable at import time in one module."""
    try:
        tree = ast.parse(src, filename=path)
    except SyntaxError as e:
        return [dict(path=path, line=getattr(e, "lineno", 0), call="<syntax error>",
                     detail=str(e))]

    found: list[dict] = []

    def walk_module_level(body):
        """Descend only through nodes that RUN on import; never into def/class."""
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            for sub in ast.walk(node):
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    continue
                if isinstance(sub, ast.Call):
                    nm = _name(sub.func)
                    short = nm.rsplit(".", 1)[-1]
                    if nm in MUTATORS or short in MUTATORS:
                        found.append(dict(path=path, line=sub.lineno, call=nm or short,
                                          detail="mutating call at import time"))
                    elif _is_write_open(sub):
                        found.append(dict(path=path, line=sub.lineno, call="open(mode=w/a/x)",
                                          detail="file created at import time"))

    walk_module_level(tree.body)
    return found


def scan_files(paths) -> list[dict]:
    out = []
    for p in paths:
        try:
            out.extend(scan_source(open(p, encoding="utf-8", errors="replace").read(), p))
        except OSError as e:
            out.append(dict(path=p, line=0, call="<unreadable>", detail=str(e)))
    return out


def bootstrap_chain(root: str) -> list[str]:
    """paths.py plus the path/bootstrap modules the app imports to reach it."""
    names = ["studio/paths.py", "studio/db.py", "studio/__init__.py",
             "studio/mount_guard.py", "config.py", "settings.py"]
    return [os.path.join(root, n) for n in names if os.path.isfile(os.path.join(root, n))]


def main():
    root = os.path.dirname(os.path.abspath(__file__))
    targets = sys.argv[1:] or bootstrap_chain(root)
    hits = scan_files(targets)
    print(f"  scanned {len(targets)} module(s) in the path/bootstrap chain")
    for t in targets:
        print(f"    {os.path.relpath(t, root)}")
    if hits:
        print(f"\n  IMPORT-TIME SIDE EFFECTS: {len(hits)}")
        for h in hits:
            print(f"    {os.path.relpath(h['path'], root)}:{h['line']}  "
                  f"{h['call']}  — {h['detail']}")
    else:
        print("\n  no import-time mkdir / touch / db-open / file creation found")
    print(f"\n  IMPORT_SIDE_EFFECT {'OPEN' if hits else 'CLOSED'}")
    return 1 if hits else 0


if __name__ == "__main__":
    raise SystemExit(main())
