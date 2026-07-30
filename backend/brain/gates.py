"""brain/gates.py — Layer 4. Turns the registry's gates and nulls into EXECUTABLE checks.

spine.decide() promised a `disqualifiers` stage in its own docstring and never had one: it
matched a fired edge, ranked by median, sized it and bought. So 84 of the brain's 128
findings — every null, every gate, every law — were inert, and they happen to be the ones
that measure LARGEST:

    Dec-Mar             kills ALL 14 setups            a good edge is +2..+5
    sub-200 bear-rally  −1 .. −3.3 on every setup
    no volume EVENT     −4 .. −8 on ALL 29 TZ/L codes
    NOT-CONSO           −3.67 / win 43.6
    H > 0.65            −2.37
    🏆RS absent         the difference between a 2022 that works and one that does not

The brain's own law says it plainly: "knowing when NOT to trade is worth more than another
signal." This module is that law made executable.

DESIGN RULES
  1. A gate whose inputs are MISSING abstains — it never silently passes. `applicable` is
     False and the reason is logged. Fail-loud on ignorance, fail-open on absence of data.
  2. A gate returns a size MULTIPLIER, or vetoes. Nothing here invents new signals; every
     number is the one measured in the finding it comes from.
  3. Multipliers compose multiplicatively and are floored, so a stack of mild negatives
     cannot silently zero a position — that is the veto's job, and a veto must be explicit.
  4. Each check names the registry finding it enforces, so a NO is traceable to evidence.
"""
from __future__ import annotations
from typing import Any, Callable, Optional

# A stack of soft suppressors must not multiply into nothing; below this we prefer to say NO
# out loud rather than buy a token position.
MIN_MULT = 0.25
# Suppressors cut hard, boosters lift modestly. Three co-firing boosters would otherwise
# compound to ×1.46 — a 46% larger position for "nothing is wrong", which is not what the
# evidence supports: the gates were measured as protection against bad states, not as a
# licence to press good ones.
MAX_MULT = 1.20


class Gate:
    """One executable check. `need` lists the state keys it cannot run without."""

    def __init__(self, fid: str, title: str, need: tuple[str, ...],
                 fn: Callable[[dict], tuple[Optional[bool], float, str]]):
        self.fid, self.title, self.need, self.fn = fid, title, need, fn

    def run(self, st: dict) -> dict:
        missing = [k for k in self.need if st.get(k) is None]
        if missing:
            return {"id": self.fid, "title": self.title, "applicable": False,
                    "veto": False, "mult": 1.0,
                    "note": f"abstained — no {', '.join(missing)}"}
        veto, mult, note = self.fn(st)
        return {"id": self.fid, "title": self.title, "applicable": True,
                "veto": bool(veto), "mult": float(mult), "note": note}


# ── the checks ─────────────────────────────────────────────────────────────────────────
# Sizes are deliberately conservative: a measured −3pp on a setup whose edge is +2pp is a
# veto; a measured −1pp is a haircut. Where a gate is a documented WORST-YEAR rescuer its
# absence is a haircut rather than a veto, because the edge still has positive expectancy
# without it — it is the tail that degrades.

def _season(st):
    m = int(st["month"])
    if m in (12, 1, 2, 3):
        return True, 0.0, ("Dec-Mar: every one of the 14 setups is suppressed in this window, "
                           "in bull years too (only Z11 and GEM1 tolerate it)")
    return False, 1.0, "month outside the Dec-Mar suppression window"


def _sub200(st):
    if st["close"] < st["e200"] and st["e9"] > st["e20"] > st["e50"]:
        return True, 0.0, ("bear-market rally: close<EMA200 with e9>e20>e50 costs every setup "
                           "−1 to −3.3, era-independent")
    return False, 1.0, "not a sub-200 bear rally"


def _vol_event(st):
    if st["no_vol_event"]:
        return True, 0.0, ("no intraday volume EVENT: the day's biggest 15m bar never reached "
                           "2.5× the session average — every one of the 29 TZ/L codes loses "
                           "4-8 points on such a bar")
    return False, 1.0, "session had a real intraday volume event"


def _vol_extreme(st):
    if st["vol_extreme"]:
        return True, 0.0, "vol-extreme / bias-dn suppressor present — kills ALL setups"
    return False, 1.0, "no vol-extreme suppressor"


# ── UNCALIBRATED states (2026-07-30) ───────────────────────────────────────────────────
# These three carry a MEASURED sign but no measured SIZE. "NOT-CONSO is −3.67" is a fact;
# "therefore trade 55% of the position" is a number I invented from the median, and so were
# ×1.15 for a rough path and ×0.6 for broken RS. Sizing on invented numbers is exactly the
# mistake this project has now caught twice in inherited Pine weights (CCI0R, BB↑ +15) — the
# only difference would be that the invented number is mine.
# So they REPORT and do not resize (mult 1.0) until the walk-forward replay measures the
# counterfactual: what actually happened to the trades each state would have shrunk.
def _conso(st):
    if not st["conso"]:
        return False, 1.0, ("⚠ expansion state (NOT-CONSO): measured −3.67 / win 43.6 vs +0.03 "
                            "in compression — REPORTED ONLY, size effect not yet calibrated")
    return False, 1.0, "compression state"


def _hurst(st):
    h = float(st["hurst"])
    # The ONLY defensible veto in this gate: pf 0.90 (below 1), 3/6 positive years, win 44.8,
    # and it replicates on a 40-bar window (−2.33, pf 0.91). Rare — 0.23% of bars — so it
    # costs almost nothing to enforce. Everything below it is reported, not acted on.
    if h > 0.65:
        return True, 0.0, (f"H={h:.2f}: smooth persistent path — −2.37 / win 44.8 / pf 0.90, "
                           f"3/6 years, replicates on a 40-bar window")
    if h > 0.55:
        return False, 1.0, f"⚠ H={h:.2f}: smooth (hurts momentum setups) — REPORTED ONLY"
    if h < 0.45:
        return False, 1.0, f"H={h:.2f}: rough path (helped 9/10 setups) — REPORTED ONLY"
    return False, 1.0, f"H={h:.2f}: near random walk"


def _rs(st):
    if not st["rs_intact"]:
        return False, 1.0, ("⚠ RS broken vs sector — the universal worst-year rescuer is absent "
                            "(structural knife, not quality dip). REPORTED ONLY, size not calibrated")
    return False, 1.0, "RS intact — quality dip"


def _mtf(st):
    if not st["mtf_echo"]:
        return True, 0.0, ("no 4H/1H/15m echo: a 1D-only signal is negative in 0 of 6 years — "
                           "the daily alone is the picture the crowd already has")
    return False, 1.1, "confirmed on a lower timeframe"


GATES: list[Gate] = [
    Gate("gate_season_decmar", "🗓️ Dec-Mar season suppressor", ("month",), _season),
    Gate("gate_sub200", "⛔ Sub-200 bear-rally", ("close", "e200", "e9", "e20", "e50"), _sub200),
    Gate("gate_no_vol_event", "⛔ No intraday volume event", ("no_vol_event",), _vol_event),
    Gate("gate_vol_adjacency", "⛔ Vol-extreme / bias-dn veto", ("vol_extreme",), _vol_extreme),
    Gate("gate_mtf", "🕐 MTF confirmation", ("mtf_echo",), _mtf),
    Gate("gate_hurst_rough", "🌀 Path roughness (Hurst)", ("hurst",), _hurst),
    Gate("gate_conso_compression", "❄️ Compression state", ("conso",), _conso),
    Gate("gate_rs", "🏆 RS integrity", ("rs_intact",), _rs),
]

# Setups whose own definition REQUIRES range expansion — the compression gate measured
# NEGATIVE on exactly these three, so it must not be applied to them.
_CONSO_EXEMPT = {"engulfabs", "engulf_absorb_rev", "l43triple", "g3abs"}
# Wyckoff Spring is the one documented setup that prefers a SMOOTH path (+0.11→+1.58 with
# H>0.55) and is HURT by roughness (−0.57): a shakeout inside a controlled range.
_HURST_EXEMPT = {"spring", "wyckoff_spring"}


def evaluate(state: dict, edge_id: str = "") -> dict:
    """Run every applicable gate. Returns {veto, reason, mult, checks[], applied, abstained}."""
    checks, mult = [], 1.0
    for g in GATES:
        if g.fid == "gate_conso_compression" and edge_id in _CONSO_EXEMPT:
            checks.append({"id": g.fid, "title": g.title, "applicable": False, "veto": False,
                           "mult": 1.0, "note": f"exempt — {edge_id} requires range expansion"})
            continue
        if g.fid == "gate_hurst_rough" and edge_id in _HURST_EXEMPT:
            checks.append({"id": g.fid, "title": g.title, "applicable": False, "veto": False,
                           "mult": 1.0, "note": f"exempt — {edge_id} prefers a smooth path"})
            continue
        r = g.run(state)
        checks.append(r)
        if r["veto"]:
            return {"veto": True, "reason": f"{r['title']} — {r['note']}", "mult": 0.0,
                    "checks": checks,
                    "applied": sum(1 for c in checks if c["applicable"]),
                    "abstained": sum(1 for c in checks if not c["applicable"])}
        mult *= r["mult"]

    applied = sum(1 for c in checks if c["applicable"])
    abstained = sum(1 for c in checks if not c["applicable"])
    if mult < MIN_MULT:
        stack = "; ".join(f"{c['title']} ×{c['mult']:.2f}" for c in checks
                          if c["applicable"] and c["mult"] < 1)
        return {"veto": True, "reason": f"suppressor stack ×{mult:.2f} below {MIN_MULT} — {stack}",
                "mult": mult, "checks": checks, "applied": applied, "abstained": abstained}
    return {"veto": False, "reason": "", "mult": round(min(mult, MAX_MULT), 3), "checks": checks,
            "applied": applied, "abstained": abstained}


def summary(res: dict) -> str:
    """One log line for the spine."""
    parts = [f"{c['title'].split()[0]}{'⚠' if '⚠' in c['note'] else ('✓' if c['mult'] >= 1 else '↓')}"
             for c in res["checks"] if c["applicable"]]
    return (f"gates: ×{res['mult']} ({res['applied']} applied, {res['abstained']} abstained)"
            + (" · " + " ".join(parts) if parts else ""))
