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


def _vspike(st):
    # 💥 15m volume concentration (project_volume_magnitude, vspike_all_signals.py): the
    # session's biggest 15m bar ÷ session average. ≥4× improves the median of ALL 29 TZ/L
    # codes (Δ +0.28..+0.76, zero exceptions); its absence weakens every one. The severe
    # tail (<2.5×, ~3% of days) is already a hard veto in gate_no_vol_event — this gate
    # covers the 2.5-4× MIDDLE band, whose per-edge size effect is NOT calibrated, so it
    # REPORTS and does not resize (the gates' own no-invented-numbers rule).
    if st["iv_vspike"]:
        return False, 1.0, ("≥4× 15m volume event — the precondition the whole TZ/L system "
                            "needs (universal, 29/29 codes)")
    return False, 1.0, ("⚠ no ≥4× 15m event (middle band 2.5-4×): every code measured weaker "
                        "without it — REPORTED ONLY, size effect not yet calibrated")


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


def _t1nb(st):
    # 🕯️ T1+NB indecision bar (2026-08-03 suffix study): a no-effort, both-wick T1 close
    # measured −1.24pp vs other T1 bars, sign 6/6 YEARS (n=2,323) — gapless indecision means
    # nothing happened; the drift continues. REPORT-ONLY: the delta was measured on raw T1
    # bars, not on edge fires — the transfer is informational until measured on fires.
    # (Mirror: the SAME suffix on a gap bar, T1G+NB, is a POSITIVE — absorbed-and-held gap.)
    if st["t1_nb"]:
        return False, 1.0, ("⚠ today's bar is T1+NB (no-effort indecision): −1.24pp vs other "
                            "T1, 6/6 years — REPORTED ONLY")
    return False, 1.0, "no T1+NB indecision state"


def _earnings(st):
    # 📅 post-report window (2026-08-03 EDGAR study, pre-registered criteria PASSED):
    # an edge fire ≤5 days AFTER a report event underperforms its complement by −1.17pp with
    # the sign holding 6/6 YEARS (n=5,731) — the post-report information vacuum starves
    # mean-reversion setups. Absolute median is still positive in 2023-26 (+0.7..+5.2), so
    # this REPORTS rather than vetoes; the ⚠ reaches the Opus decider. The PRE side of the
    # same study is NOT acted on: the cadence predictor's median error is 33 days — noise.
    d = int(st["days_since_report"])
    if d <= 5:
        return False, 1.0, (f"⚠ {d}d after a SEC report event: post-report fires −1.17pp vs "
                            f"complement, 6/6 years — REPORTED ONLY (absolute median still "
                            f"positive in bull years)")
    return False, 1.0, f"{d}d since last report event — outside the post-report window"


def _gex(st):
    # 💠 GEX / options context (2026-08-03): net dealer gamma + VRP from the nightly forward
    # log (project_gex_options). REPORT-ONLY BY CONSTRUCTION: the log started 2026-07-22 —
    # days of history, no validation possible yet. The pre-registered study (negative-gamma
    # entry Δ≥1.0pp · high-VRP forward drag) runs when the log reaches ~4-6 months. The ⚠
    # notes flow into the Opus decider's context, which may weigh but not resize them.
    net = float(st["gex_net"])
    vrp = st.get("gex_vrp")
    vtxt = f", VRP {vrp:+.1f}" if isinstance(vrp, (int, float)) else ""
    if net < 0:
        return False, 1.0, (f"⚠ NEGATIVE net GEX ({net:,.0f}{vtxt}): dealers amplify moves "
                            f"(accelerant zone) — FORWARD-ONLY log, not calibrated, no resize")
    return False, 1.0, f"positive net GEX ({net:,.0f}{vtxt}): dealers dampen moves"


def _h1dr(st):
    # 🕐 1H dual-reclaim (project_oscillator_divergence_reclaim): the 2nd UNIVERSAL booster —
    # improved 52 of 63 setups by +1.34pp median, period-matched. BOOST-only by design:
    # it fires ~0.42×/ticker-year, so its ABSENCE is the norm and must not be a haircut.
    # ×1.1 is deliberately conservative (+1.34pp on a +2pp edge would justify far more) —
    # boosters lift modestly, suppressors cut hard; MAX_MULT caps the stack anyway.
    if st["h1_dr"]:
        return False, 1.1, "1H dual-reclaim printed on D or D-1 (+1.34pp booster, 52/63 setups)"
    return False, 1.0, "no 1H dual-reclaim (normal — not a suppressor)"


def _sector_lag(st):
    # 🥇 LEAD-in-LAG (2026-08-06, user's own hypothesis, validated per-edge): rs_intact says
    # the STOCK is strong vs its sector; this adds that the SECTOR is weak vs SPY (20d
    # relative < −1%). A leader inside a laggard group. Four-quadrant gradient on the pooled
    # reversal family (n=217k): strong×lagging +3.61 6/6yr worst +1.29 · strong×leading +2.38
    # · weak×lagging +1.60 4/6yr · weak×leading +0.91 4/6yr worst −4.08 — monotone, so it is
    # a 2-D effect and not one lucky cell. Per-edge: median lift 7/7 edges, SR lift 7/7, and
    # DSR crosses 0.000 → 0.88-0.95 on G3 / G3-Abs / L43 (the three built as own variants).
    # ×1.15 — larger than the 1H-DR booster because the lift is +2.0pp median, not +1.34pp,
    # and it carries a worst-year improvement too. MAX_MULT still caps the stack.
    # ⚠ EXEMPT: it DEGRADES D+L1 (3/5 years, worst −3.88) — see _LEAD_EXEMPT.
    if st["lead_in_lag"]:
        return False, 1.15, ("🥇 leader-in-laggard: stock strong vs its sector AND the sector "
                             "lagging SPY (+2.0pp median, 7/7 edges, DSR 0.00→0.9 on G3/G3A/L43)")
    return False, 1.0, "not a leader-in-laggard configuration (normal — not a suppressor)"


def _macro_vix(st):
    # 🌡️ macro VIX-up (2026-08-06): VIXY 5d change > +3% and NOT a vspike day — the 210
    # sessions (68% of rising-VIX days) that the existing 15m vspike gate cannot see.
    # REPORT-ONLY BY MEASUREMENT: it does NOT move Sharpe (DSR lift 0/7 edges), so it must
    # never resize. What it DOES do is convert 5/6 → 6/6 positive years with a POSITIVE
    # worst year on QZC (−0.09→+0.59), G3 (−0.45→+1.57), WSH (−2.79→+0.89), G3-Abs
    # (−0.92→+1.77) — a stabiliser, not an amplifier. ⚠ it HURTS L43 (worst +2.27→−5.71).
    # Rationale: reversal setups need something to revert FROM; rising fear supplies it.
    if st["macro_vix_up"]:
        return False, 1.0, ("✔ VIX rising (5d >+3%) outside a panic spike: reversal edges run "
                            "5/6→6/6 positive years here — REPORTED ONLY, no resize")
    return False, 1.0, "VIX not rising outside a spike"


def _adx_trend(st):
    # 📐 ADX TREND-UP suppressor (2026-08-07, from the user's Pine port; the script's own
    # hypothesis was the opposite and is REFUTED). ADX>=25 with DI+>DI− is the WORST regime
    # for this book — for BOTH families, because our whole book buys ABSORBED WEAKNESS, not
    # strength (what we call "momentum" — G3 gap-reclaim, Atomic weak-close gap-up, L43 — is
    # reversion-flavoured too). Measured on the ATR exit:
    #   reversal family base +1.87 → TREND-UP −0.83 (2/6yr, n=703 thin)
    #   momentum family base +1.86 → TREND-UP −0.03 (3/6yr, worst −5.81, n=10,242)
    #   per-edge on TREND-UP: QZC −3.25 · D+L1 −2.08 · ATM −1.38
    # REPORT-ONLY: every DSR in the study was 0.000, so this must never resize. It is
    # context for the Opus decider — "the fire is in the one regime our book dislikes".
    # NOT a duplicate of gate_hurst_rough: agreement only 63.5%, corr(adx,hurst) +0.20.
    if st["adx_trend_up"]:
        return False, 1.0, ("⚠ ADX TREND-UP (ADX≥25, DI+>DI−): the book's WEAKEST regime — "
                            "reversal −2.7pp / momentum −1.9pp vs their own base — REPORTED ONLY")
    return False, 1.0, "not in an ADX strong-uptrend regime"


GATES: list[Gate] = [
    Gate("gate_season_decmar", "🗓️ Dec-Mar season suppressor", ("month",), _season),
    Gate("gate_sub200", "⛔ Sub-200 bear-rally", ("close", "e200", "e9", "e20", "e50"), _sub200),
    Gate("gate_no_vol_event", "⛔ No intraday volume event", ("no_vol_event",), _vol_event),
    Gate("gate_vspike", "💥 15m volume concentration", ("iv_vspike",), _vspike),
    Gate("gate_vol_adjacency", "⛔ Vol-extreme / bias-dn veto", ("vol_extreme",), _vol_extreme),
    Gate("gate_mtf", "🕐 MTF confirmation", ("mtf_echo",), _mtf),
    Gate("gate_h1dr", "🕐 1H dual-reclaim booster", ("h1_dr",), _h1dr),
    Gate("gate_gex", "💠 GEX context", ("gex_net",), _gex),
    Gate("gate_earnings", "📅 Post-report window", ("days_since_report",), _earnings),
    Gate("gate_t1nb", "🕯️ T1+NB indecision", ("t1_nb",), _t1nb),
    Gate("gate_hurst_rough", "🌀 Path roughness (Hurst)", ("hurst",), _hurst),
    Gate("gate_conso_compression", "❄️ Compression state", ("conso",), _conso),
    Gate("gate_rs", "🏆 RS integrity", ("rs_intact",), _rs),
    Gate("gate_sector_lag", "🥇 Leader-in-laggard", ("lead_in_lag",), _sector_lag),
    Gate("gate_macro_vix", "🌡️ Macro VIX-up", ("macro_vix_up",), _macro_vix),
    Gate("gate_adx_trend", "📐 ADX trend-up regime", ("adx_trend_up",), _adx_trend),
]

# Setups whose own definition REQUIRES range expansion — the compression gate measured
# NEGATIVE on exactly these three, so it must not be applied to them.
_CONSO_EXEMPT = {"engulfabs", "engulf_absorb_rev", "l43triple", "g3abs"}
# Wyckoff Spring is the one documented setup that prefers a SMOOTH path (+0.11→+1.58 with
# H>0.55) and is HURT by roughness (−0.57): a shakeout inside a controlled range.
_HURST_EXEMPT = {"spring", "wyckoff_spring"}
# 🥇 the leader-in-laggard boost measured NEGATIVE on D+L1 (3/5 years, worst −3.88)
# and the 🌡️ VIX-up context measured negative on L43 (worst +2.27 → −5.71).
_LEAD_EXEMPT = {"dl1", "d_l1_reversal"}
_VIXUP_EXEMPT = {"l43triple", "l43triple_quiet"}


def evaluate(state: dict, edge_id: str = "") -> dict:
    """Run every applicable gate. Returns {veto, reason, mult, checks[], applied, abstained}."""
    checks, mult = [], 1.0
    for g in GATES:
        if g.fid == "gate_sector_lag" and edge_id in _LEAD_EXEMPT:
            checks.append({"id": g.fid, "title": g.title, "applicable": False, "veto": False,
                           "mult": 1.0, "note": f"exempt — measured negative on {edge_id}"})
            continue
        if g.fid == "gate_macro_vix" and edge_id in _VIXUP_EXEMPT:
            checks.append({"id": g.fid, "title": g.title, "applicable": False, "veto": False,
                           "mult": 1.0, "note": f"exempt — measured negative on {edge_id}"})
            continue
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
