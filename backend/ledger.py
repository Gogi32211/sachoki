"""B1 — the research ledger: an append-only record of every trial ever run.

WHY THIS EXISTS

`n_trials` has always been declared by hand inside each study — 8, 16, 24. That number feeds
DSR and PBO, so it decides how hard a result is deflated. But the honest count is not "how
many cells did THIS script score", it is "how many times has this question been asked, ever".
Across ~50 studies that is in the hundreds. Deflating against 12 when the true figure is 300
is not a correction, it is a formality.

THE TRAP THIS AVOIDS — and it is the reason the ledger is not just a counter

Our 119 registry entries are NOT 119 independent trials. They are ~35 families with gate
variants hanging off them:

    Washout · Washout🏆RS · Washout🧊CONSO · Washout💥vol · Washout🌀SC · Washout🔎iv

That is one edge and six records. Counting six independent trials over-deflates; counting one
under-deflates. So every entry carries a `family` and a `parent`, derived mechanically from
the naming convention rather than labelled by hand, and deflation can then be run per family
instead of over a flat list.

Three more fields exist because of specific incidents:
  data_snapshot   META rows were deleted from five databases mid-session. Studies either side
                  of that are not comparable and must not share a trial pool.
  rerun_of        a technical re-run must not inflate the multiple-testing penalty.
  code_version    a study on a changed engine is a different study.

Storage is JSONL: append-only by construction, survives partial writes, and stays diffable.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import asdict, dataclass, field

HERE = os.path.dirname(os.path.abspath(__file__))
LEDGER = os.path.join(HERE, "research_ledger.jsonl")

# ── gate markers, stripped to find the parent edge ───────────────────────────
# Order matters: longer/compound markers first so "🧊CONSO" is not left as "CONSO".
_GATES = [
    "🏆RS💎", "🏆RS", "🧱OB", "🧊CONSO", "❄️CONSO", "🔵dwell", "🕐DR", "🕐24-26", "🎋TLS",
    "🌀SC", "🔑KEY", "🔑", "🏛️BOS", "🕯️mid", "🕯️", "💥vol", "🔎iv", "🔇QUIET", "💎89+",
    "💎$21-89", "💎", "🥇", "·LEAD-in-LAG", "LEAD-in-LAG", "·L34pre", "·15mZ", "·CONF",
    "·EVR", "🟢L46🔵dw", "🟢L46", "🟢🔎iv", "🟡watch", "🟡", "-DiT", "-E",
]
_TRAIL = re.compile(r"[\s·\-–—]+$")


def family_of(name: str) -> str:
    """Strip gate markers to get the parent edge. Mechanical, not hand-labelled —
    hand-labelling 119 entries is exactly the step that never gets redone when the
    registry grows."""
    s = str(name)
    changed = True
    while changed:
        changed = False
        for g in _GATES:
            if s.endswith(g) and len(s) > len(g):
                s = s[: -len(g)]
                changed = True
        s2 = _TRAIL.sub("", s)
        if s2 != s:
            s, changed = s2, True
    return s or str(name)


def spec_hash(payload) -> str:
    """Stable id for what was actually tested, so a re-run is recognisable as one."""
    blob = json.dumps(payload, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _git_rev() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=HERE,
                              capture_output=True, text=True, timeout=5).stdout.strip() or "?"
    except Exception:
        return "?"


def data_snapshot(as_of: str = "", extra: str = "") -> str:
    """Identifies the data a result was produced on. Two studies with different snapshots
    are not comparable and must not pool their trial counts."""
    return spec_hash({"as_of": str(as_of), "extra": str(extra)})


@dataclass
class Trial:
    ts: str
    question: str
    family: str
    parent: str
    spec: str
    n_cells: int
    verdict: str = ""
    est: float | None = None
    n_obs: int | None = None
    n_eff: int | None = None
    sharpe: float | None = None
    dsr: float | None = None
    universe: str = ""
    params: dict = field(default_factory=dict)
    code_version: str = ""
    data_snapshot: str = ""
    rerun_of: str = ""
    script: str = ""


def log_trial(question: str, *, family: str = "", n_cells: int = 1, verdict: str = "",
              est: float | None = None, n_obs: int | None = None,
              n_eff: int | None = None, sharpe: float | None = None,
              dsr: float | None = None, universe: str = "", params: dict | None = None,
              as_of: str = "", rerun_of: str = "", script: str = "") -> Trial:
    """Append one trial. Never edits or deletes — that is what append-only means."""
    fam = family or question
    t = Trial(ts=time.strftime("%Y-%m-%dT%H:%M:%S"), question=question,
              family=fam, parent=family_of(fam),
              spec=spec_hash({"q": question, "f": fam, "p": params or {}, "u": universe}),
              n_cells=int(n_cells), verdict=verdict, est=est, n_obs=n_obs, n_eff=n_eff,
              sharpe=sharpe, dsr=dsr, universe=universe, params=params or {},
              code_version=_git_rev(), data_snapshot=data_snapshot(as_of),
              rerun_of=rerun_of, script=script or os.path.basename(
                  getattr(__import__("__main__"), "__file__", "") or "interactive"))
    with open(LEDGER, "a") as f:
        f.write(json.dumps(asdict(t), ensure_ascii=False) + "\n")
    return t


def read_ledger() -> list[dict]:
    if not os.path.exists(LEDGER):
        return []
    out = []
    with open(LEDGER) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out


def trial_count(parent: str = "", snapshot: str = "", exclude_reruns: bool = True) -> int:
    """How many trials has this question really had?

    Counts CELLS, not rows: a study that scored 12 variants spent 12 trials. Re-runs are
    excluded by default — a technical repeat is not new search."""
    n = 0
    for r in read_ledger():
        if exclude_reruns and r.get("rerun_of"):
            continue
        if parent and r.get("parent") != parent:
            continue
        if snapshot and r.get("data_snapshot") != snapshot:
            continue
        n += int(r.get("n_cells", 1))
    return n


def summary() -> dict:
    rows = read_ledger()
    if not rows:
        return {"trials": 0, "cells": 0, "families": 0}
    fams: dict[str, int] = {}
    for r in rows:
        fams[r.get("parent", "?")] = fams.get(r.get("parent", "?"), 0) + int(r.get("n_cells", 1))
    return {"trials": len(rows), "cells": sum(int(r.get("n_cells", 1)) for r in rows),
            "families": len(fams),
            "top": sorted(fams.items(), key=lambda kv: -kv[1])[:15],
            "snapshots": len({r.get("data_snapshot") for r in rows}),
            "verdicts": {v: sum(1 for r in rows if r.get("verdict") == v)
                         for v in {r.get("verdict", "") for r in rows} if v}}
