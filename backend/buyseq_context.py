"""
buyseq_context.py — live lookup for the BUY-signal × preceding-sequence conditioner
(2026-07-20 redesign; extended same day with 5 independent descriptor LAYERS).

Layers (each its own vocabulary, competing by |lift|):
  c  = coarse T/Z          f  = fine T/Z+L
  sx = close_suffix        bw = bar_body_wick     gr = bar_gap_range
  q5 = bar_line5           vb = vol_bucket
Rule keys in buyseq_context.json are layer-namespaced ("vb|N N B") because layers
share token alphabets (gap "N N" ≠ volume "N N").

Chip semantics: ⤴ booster / ⤵ suppressor context; the sequence ENDS ON the annotated bar
(completed pattern; the forecast covers the NEXT 20 bars). Table cells are era-consistent (TRAIN & TEST same side of
the signal's baseline), |lift|>=5pp, min-n gated.
"""
from __future__ import annotations
import json
import os

LAYER_NAMES = {"c": "TZ", "f": "TZ/L", "sx": "suffix", "bw": "body/wick",
               "gr": "gap/range", "q5": "line5", "vb": "volume"}

_TBL: list = [None]


def _tbl() -> dict:
    if _TBL[0] is None:
        try:
            with open(os.path.join(os.path.dirname(__file__), "buyseq_context.json")) as f:
                _TBL[0] = json.load(f).get("signals", {})
        except Exception:
            _TBL[0] = {}
    return _TBL[0]


def make_tokens(z: str, t: str, l: str) -> tuple[str, str]:
    """(coarse, fine) T/Z token for one bar — mirrors the analysis script."""
    c = z if z else ("-" + t if t else "-")
    return c, c + (l or "")


def lookup(layers: dict, signals: list) -> dict | None:
    """layers: {"c": [...], "f": [...], "sx": [...], "bw": [...], "gr": [...],
    "q5": [...], "vb": [...]} — each a list of that layer's tokens for the last bars INCLUDING
    the annotated one, oldest→newest (>=2, ideally 4 entries).
    signals: active keys among rev/brk/h4/h1/lh/ea/ep/fly/flyf/score/turn/anyb.
    Returns the strongest |lift| match or None."""
    t = _tbl()
    if not t or not signals:
        return None
    pref = {}
    for lay, toks in layers.items():
        toks = [x if x else "-" for x in toks]
        for d in (2, 3, 4):
            if len(toks) >= d:
                pref[f"{lay}|{' '.join(toks[-d:])}"] = lay
    best = None
    for sig in signals:
        st = t.get(sig)
        if not st:
            continue
        rules = st["rules"]
        for key, lay in pref.items():
            r = rules.get(key)
            if r and (best is None or abs(r["lift"]) > abs(best["lift"])):
                best = {**r, "sig": sig, "seq": key.split("|", 1)[1],
                        "layer": LAYER_NAMES.get(lay, lay), "base_up": st["base_up"]}
    return best


def lookup_ensemble(layers: dict, signals: list) -> dict | None:
    """ENSEMBLE read (2026-07-20c, user request): instead of one winner, ONE verdict
    PER LAYER (best |lift| across depths & active signals), then a consensus:
    n_up vs n_dn + |lift|-weighted average up%. Uses the WHOLE table's information
    without joint-vocabulary explosion. Returns None unless >=2 layers voted —
    a single verdict is already the SEQ-row winner chip and adds nothing."""
    t = _tbl()
    if not t or not signals:
        return None
    verdicts = []
    for lay, toks in layers.items():
        toks = [x if x else "-" for x in toks]
        best = None
        for d in (2, 3, 4):
            if len(toks) < d:
                continue
            key = f"{lay}|{' '.join(toks[-d:])}"
            for sig in signals:
                st = t.get(sig)
                r = st["rules"].get(key) if st else None
                if r and (best is None or abs(r["lift"]) > abs(best["lift"])):
                    best = {**r, "sig": sig, "layer": LAYER_NAMES.get(lay, lay),
                            "seq": key.split("|", 1)[1]}
        if best:
            verdicts.append(best)
    if len(verdicts) < 2:
        return None
    n_up = sum(1 for v in verdicts if v["dir"] == "up")
    n_dn = len(verdicts) - n_up
    n_tail = sum(1 for v in verdicts if v.get("kind") == "tail")
    w = sum(abs(v["lift"]) for v in verdicts) or 1.0
    up_avg = sum(v["up"] * abs(v["lift"]) for v in verdicts) / w
    return {"n_up": n_up, "n_dn": n_dn, "n_tail": n_tail,
            "dir": "up" if up_avg >= 50 else "down", "up_avg": round(up_avg, 1),
            "detail": [{"layer": v["layer"], "dir": v["dir"], "kind": v.get("kind"),
                        "up": v["up"], "seq": v["seq"], "sig": v["sig"]} for v in verdicts]}
