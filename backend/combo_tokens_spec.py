"""M1 — the canonical token registry. What a Combo Miner is allowed to say.

A combination is a sentence, and this file is its dictionary. Everything downstream —
grammar, degeneracy checks, null routing, the count of structurally valid candidates —
reads its rules from here rather than restating them, because a grammar restated in
three places is a grammar that disagrees with itself on the fourth change.

WHY A REGISTRY AND NOT A LIST OF COLUMN NAMES

Six properties have to travel with a token, and only the first is a column:

    column + predicate   where it lives and what makes it true
    family               which dimension it belongs to
    null_requirement     WHICH NULL CAN BREAK ITS ASSOCIATION — see below
    exclusion_group      when two tokens cannot both be true, PROVABLY
    availability         the token is a property of the SIGNAL bar, known at its close
    source               `bars` or the opportunity table — they are different objects

N0's finding is the reason `null_requirement` is a field rather than a global setting.
ComboLab's band permutes outcomes within (date × family); a predictor CONSTANT WITHIN A
TRADING DATE puts its whole block in one arm, so no outcome ever crosses and the band has
no null to build. Measured, that is FWER 0.685 against a nominal 0.05, against 0.065 for
the opportunity-level claims. So the null mechanism is a property of the CLAIM, and a
combination inherits the union of its tokens':

    all OPPORTUNITY_LEVEL              → OPPORTUNITY_LEVEL   G1 null applies
    all DAY_LEVEL                      → DAY_LEVEL           deferred, no calibrated null
    any mix                            → MIXED_LEVEL         deferred, and NOT "both nulls"

MIXED_LEVEL is not a third null, it is the absence of one. Two separately calibrated 5%
bands do not compose into 5% over their union, so a generator may EMIT a mixed candidate
and an adjudicator may not pretend to calibrate it. That is the whole reason the field
exists here rather than being inferred later: the miner's most attractive-looking
candidates — setup + physics + market regime — are exactly the ones whose null is hardest,
and without this they would arrive wearing the same verdict as the easy ones.

DECLARED VERSUS MEASURED, and the line is drawn deliberately

    exclusion_group    DECLARED only where it is a theorem: two `eq` tokens on one
                       column cannot both hold, because a column has one value.
    everything else    MEASURED. `bar_body_wick` starts with X or with M and never both,
                       and `XF` implies `X` — those look equally obvious and are facts
                       about an encoding I did not write. They are found by the
                       implication and co-occurrence tests in combo_universe, where a
                       wrong guess shows up as a number instead of silently narrowing
                       the search space.

The same applies to `null_requirement`: what is written below is a PRIOR. The audit
measures within-date constancy for every token and reports any disagreement, because a
token I assumed varies within a day and does not would quietly re-create N0.

NO OUTCOME APPEARS ANYWHERE IN THIS FILE OR ITS CONSUMERS AT M0–M2.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

SPEC_VERSION = "combo_tokens_v1"

# ── null requirements ────────────────────────────────────────────────────────
OPPORTUNITY_LEVEL = "OPPORTUNITY_LEVEL"
DAY_LEVEL = "DAY_LEVEL"
MIXED_LEVEL = "MIXED_LEVEL"

NULL_STATUS = {
    OPPORTUNITY_LEVEL: dict(automatic_search="ENABLED", null="within-stratum outcome "
                            "permutation", calibrated_fwer=0.065),
    DAY_LEVEL: dict(automatic_search="DEFERRED", null="temporal-preserving predictor "
                    "permutation — NOT BUILT", calibrated_fwer=None),
    MIXED_LEVEL: dict(automatic_search="DEFERRED_UNTIL_NULL_DEFINED",
                      null="none exists; the union of two nulls is not a null",
                      calibrated_fwer=None),
}


def combine_null(reqs) -> str:
    """A combination's null requirement is the union of its tokens' — and a mix is not both."""
    s = set(reqs)
    if s == {OPPORTUNITY_LEVEL}:
        return OPPORTUNITY_LEVEL
    if s == {DAY_LEVEL}:
        return DAY_LEVEL
    return MIXED_LEVEL


# ── predicate kinds ──────────────────────────────────────────────────────────
# Deliberately few, and each is a total function of one column. A token whose truth needs
# two columns is a combination, and combinations are the miner's job, not the dictionary's.
KINDS = {
    "eq":    "column == value",
    "sw":    "column startswith value",
    "ct":    "column contains value (literal, not regex)",
    "ne":    "column is non-empty / non-zero",
    "flag":  "numeric column != 0",
    "ldig":  "l_sig matches ^L[1-6]+$ and contains the digit",
    "band":  "lo <= column < hi   (declared round cuts, never fitted)",
}


@dataclass(frozen=True)
class Token:
    token_id: str
    family: str
    source: str              # "bars" | "opportunities"
    column: str
    kind: str
    value: object = None
    null_requirement: str = OPPORTUNITY_LEVEL
    exclusion_group: str = ""   # non-empty only where exclusivity is a theorem
    note: str = ""

    def __post_init__(self):
        if self.kind not in KINDS:
            raise ValueError(f"{self.token_id}: unknown kind {self.kind!r}")
        if self.null_requirement not in (OPPORTUNITY_LEVEL, DAY_LEVEL):
            raise ValueError(f"{self.token_id}: a TOKEN is never MIXED — that is a "
                             f"property of a combination")
        # exclusivity is claimed only where it follows from a column having one value
        if self.exclusion_group and self.kind not in ("eq", "band"):
            raise ValueError(f"{self.token_id}: exclusion_group declared on kind "
                             f"{self.kind!r}, which does not make it a theorem. Let "
                             f"combo_universe MEASURE it.")


FAMILIES = {
    "PATTERN_T":     "TZ_WLNBB bullish priority codes",
    "PATTERN_Z":     "TZ_WLNBB bearish priority codes",
    "VOLUME_L":      "L-code — the VSA volume-line ladder (l_sig)",
    "VOLUME_BUCKET": "volume bucket vs its own history",
    "SUFFIX_NE":     "line-2 new/equal opening",
    "SUFFIX_WICK":   "line-2 wick side",
    "SUFFIX_PEN":    "line-2 penetration of the prior bar",
    "SUFFIX_CLOSE":  "line-2 close position ladder",
    "BODYWICK":      "line-3 body/wick geometry",
    "GAPRANGE":      "line-4 gap and range vs ATR",
    "LINE5":         "line-5 VIX-Fix / PSAR / RSI2",
    "PHYS_IMPACT":   "⚛ R — impact",
    "PHYS_REGIME":   "⚛ trend regime",
    "PHYS_ENERGY":   "⚛ E — stored energy",
    "PHYS_STRETCH":  "⚛ K — stretch",
    "PHYS_LANDSCAPE": "⚛ C — landscape",
    "PHYS_DISORDER": "⚛ H — disorder",
    "PHYS_MOMENTUM": "⚛ M — momentum",
    "PHYS_COHERENCE": "⚛ S — coherence",
    "PHYS_AD":       "⚛ absorbed-demand grade",
    "PHYS_GAP":      "⚛ true gap class",
    "PHYS_WYC":      "⚛ Wyckoff EVENT (not wyc_phase — they differ exactly on events)",
    "WYC_PHASE":     "legacy Wyckoff macro phase",
    "CISD":          "⟂ change in state of delivery",
    "RSI_BAND":      "RSI-14 decile-free bands, round cuts",
    "FLOW":          "volume-event flags",
    "STRUCTURE":     "breakout / engulf / L-cluster flags",
    "GOG":           "GOG G-series",
    "STATE_FLAG":    "screener state toggles",
    "PD_LADDER":     "P / D EMA-cross ladder",
    "AD_FLAG":       "absorbed-demand freshness",
    "CONTEXT_GATE":  "frame-computed gates (RS, ADX, hurst, conso, intraday echo)",
    "MACRO":         "market-wide state — constant within a trading date",
}

# ── the vocabulary ───────────────────────────────────────────────────────────
# It is deliberately the SCREENER's vocabulary. A miner searching tokens the user cannot
# select in the UI would produce results that cannot be looked at on a chart, and the first
# question about any survivor is "show me one".
_T = []


def _add(*tokens):
    _T.extend(tokens)


# T / Z priority codes. sig_t7 and sig_t8 exist in `bars` and are NOT exposed by the
# screener, so they are not searchable here. Recorded in EXCLUDED_FROM_SEARCH rather than
# omitted silently — an absent token and an excluded one look identical in a count.
_add(*[Token(f"T{n}", "PATTERN_T", "bars", f"sig_t{n}", "flag")
       for n in (1, 2, 3, 4, 5, 6, 9, 10, 11, 12)],
     Token("T1G", "PATTERN_T", "bars", "sig_t1g", "flag"),
     Token("T2G", "PATTERN_T", "bars", "sig_t2g", "flag"),
     Token("ANY_T", "PATTERN_T", "bars", "sig_t", "flag"),
     Token("TZ_FLIP", "PATTERN_T", "bars", "sig_tz_flip", "flag"))

_add(*[Token(f"Z{n}", "PATTERN_Z", "bars", f"sig_z{n}", "flag")
       for n in (1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12)],
     Token("Z1G", "PATTERN_Z", "bars", "sig_z1g", "flag"),
     Token("Z2G", "PATTERN_Z", "bars", "sig_z2g", "flag"),
     Token("ANY_Z", "PATTERN_Z", "bars", "sig_z", "flag"))

EXCLUDED_FROM_SEARCH = {
    "sig_t7": "materialized in bars, not exposed by the screener",
    "sig_t8": "materialized in bars, not exposed by the screener",
    "sig_z8": "materialized in bars, not exposed by the screener",
}

# L-code. Single digits are PRESENCE inside a multi-digit code (L4 is true inside L46);
# L34 and L46 are the exact codes. So L4 ⊇ L46 by construction — an implication the
# dependency map must find, and a check on whether it can.
_add(*[Token(f"L{d}", "VOLUME_L", "bars", "l_sig", "ldig", str(d)) for d in range(1, 7)],
     Token("L34x", "VOLUME_L", "bars", "l_sig", "eq", "L34", exclusion_group="l_sig"),
     Token("L46x", "VOLUME_L", "bars", "l_sig", "eq", "L46", exclusion_group="l_sig"),
     Token("ANY_L", "VOLUME_L", "bars", "l_sig", "ldig", ""))

_add(*[Token(f"VOL_{v}", "VOLUME_BUCKET", "bars", "vol_bucket", "eq", v,
             exclusion_group="vol_bucket") for v in ("VB", "B", "N", "L", "W")])

_add(Token("NE_N", "SUFFIX_NE", "bars", "ne_suffix", "sw", "N"),
     Token("NE_E", "SUFFIX_NE", "bars", "ne_suffix", "sw", "E"))
_add(*[Token(f"WK_{v}", "SUFFIX_WICK", "bars", "wick_suffix", "ct", v)
       for v in ("U", "D", "B")])
_add(*[Token(f"PEN_{v}", "SUFFIX_PEN", "bars", "full_suffix", "ct", v)
       for v in ("P", "R", "H")])
# The close ladder is `close_suffix`, NOT a letter inside `full_suffix`. Measured:
# full_suffix takes values ED/EU/ND/NU/NUR/NDP/NH/EDP/EB/… — it carries NE + wick +
# penetration and never an A/O/I, so a `contains` test on it is true on zero rows out of
# 8.7M. close_suffix is exactly one of O/A/I, which also makes these `eq` and mutually
# exclusive by theorem rather than by measurement.
#
# ⚠ The screener's three chips read the same wrong column:
#     UltraScanPanel.jsx  _cl_a/_cl_o/_cl_i → (r.tz_wlnbb_full_suffix||'').includes('A')
# In the LIVE path that alias may carry a longer string than the DB column of the same
# name, so this is stated as what was checked: against `bars.full_suffix` the test cannot
# fire. Not changed here — the UI is not this module's to touch.
_add(*[Token(f"CL_{v}", "SUFFIX_CLOSE", "bars", "close_suffix", "eq", v,
             exclusion_group="close_suffix") for v in ("A", "O", "I")])

_add(Token("BW_X", "BODYWICK", "bars", "bar_body_wick", "sw", "X"),
     Token("BW_M", "BODYWICK", "bars", "bar_body_wick", "sw", "M"),
     Token("BW_S", "BODYWICK", "bars", "bar_body_wick", "sw", "S"),
     Token("BW_J", "BODYWICK", "bars", "bar_body_wick", "ct", "J"),
     Token("BW_TB", "BODYWICK", "bars", "bar_body_wick", "ct", "TB"),
     Token("BW_BB", "BODYWICK", "bars", "bar_body_wick", "ct", "BB"),
     Token("BW_F", "BODYWICK", "bars", "bar_body_wick", "ct", "F"),
     Token("BW_XF", "BODYWICK", "bars", "bar_body_wick", "eq", "XF",
           exclusion_group="bar_body_wick"),
     Token("BW_MF", "BODYWICK", "bars", "bar_body_wick", "eq", "MF",
           exclusion_group="bar_body_wick"))

_add(*[Token(f"GR_{v}", "GAPRANGE", "bars", "bar_gap_range", "ct", v)
       for v in ("G1", "G2", "G3", "V", "C")],
     Token("GR_N", "GAPRANGE", "bars", "bar_gap_range", "eq", "N",
           exclusion_group="bar_gap_range"))

_add(*[Token(f"L5_{v}", "LINE5", "bars", "bar_line5", "ct", v)
       for v in ("VX", "VR", "PB", "PS", "R2H", "R2L", "R2X", "R2D")],
     Token("L5_ANY", "LINE5", "bars", "bar_line5", "ne"))

# ⚛ physics. Every one of these is an `eq` on a single categorical field, so the whole
# "forbid RA+RN, K2U+K2D, M2U+M0" section of the design is a theorem here rather than a
# blacklist somebody has to maintain.
_add(*[Token(v, "PHYS_IMPACT", "bars", "phys_r", "eq", v, exclusion_group="phys_r")
       for v in ("RA", "RN", "RF")])
_add(*[Token(f"REG_{v}", "PHYS_REGIME", "bars", "phys_regime", "eq", v,
             exclusion_group="phys_regime") for v in ("U", "D")])
_add(Token("E2", "PHYS_ENERGY", "bars", "phys_e", "sw", "E2"),
     Token("ESTAR", "PHYS_ENERGY", "bars", "phys_e", "ct", "★"))
_add(Token("K2", "PHYS_STRETCH", "bars", "phys_k", "sw", "K2"),
     Token("K1", "PHYS_STRETCH", "bars", "phys_k", "sw", "K1"),
     Token("K0", "PHYS_STRETCH", "bars", "phys_k", "eq", "K0", exclusion_group="phys_k"))
_add(*[Token(v, "PHYS_LANDSCAPE", "bars", "phys_c", "eq", v, exclusion_group="phys_c")
       for v in ("C0", "C1", "C2")])
_add(*[Token(v, "PHYS_DISORDER", "bars", "phys_h", "eq", v, exclusion_group="phys_h")
       for v in ("H0", "H2")])
_add(Token("M2", "PHYS_MOMENTUM", "bars", "phys_m", "eq", "M2", exclusion_group="phys_m"))
_add(*[Token(v, "PHYS_COHERENCE", "bars", "phys_s", "eq", v, exclusion_group="phys_s")
       for v in ("S3U", "S3D")])
_add(Token("AD_ANY", "PHYS_AD", "bars", "phys_ad", "ne"),
     Token("AD_1", "PHYS_AD", "bars", "phys_ad", "eq", "★", exclusion_group="phys_ad"),
     Token("AD_2", "PHYS_AD", "bars", "phys_ad", "eq", "★★", exclusion_group="phys_ad"),
     Token("AD_A", "PHYS_AD", "bars", "phys_ad", "eq", "★A", exclusion_group="phys_ad"))
_add(Token("gG3", "PHYS_GAP", "bars", "phys_gap_true", "eq", "G3",
           exclusion_group="phys_gap_true"))
_add(*[Token(f"WYC_{v}", "PHYS_WYC", "bars", "phys_wyc", "eq", v,
             exclusion_group="phys_wyc")
       for v in ("SPRING", "SPRING★", "UTAD", "SOS★", "ACC-TR", "DIST-TR",
                 "MARKUP", "MKDN")])
# Values read from the column, not assumed: ACC_TR / DIST_TR carry the underscore, and
# the phase column also emits four event-like values of its own. It reports UTAD on 4,470
# bars where phys_wyc reports it on 8,833 — the two columns agree on the macro phase and
# disagree exactly on events, so both are searchable and they are NOT aliases.
_add(*[Token(f"PH_{v}", "WYC_PHASE", "bars", "wyc_phase", "eq", v,
             exclusion_group="wyc_phase")
       for v in ("ACC_TR", "MARKUP", "DIST_TR", "MKDN", "NEUTRAL", "SOS", "UTAD",
                 "SPRING")])

_add(Token("CISD_PS", "CISD", "bars", "sig_cisd_plus_struct", "flag"),
     Token("CISD_MS", "CISD", "bars", "sig_cisd_minus_struct", "flag"),
     Token("CISD_SEQ", "CISD", "bars", "sig_cisd_seq", "flag"),
     Token("CISD_MPM", "CISD", "bars", "sig_cisd_mpm", "flag"),
     Token("CISD_CP", "CISD", "bars", "sig_cisd_cplus", "flag"),
     Token("CISD_CPM", "CISD", "bars", "sig_cisd_cplus_minus", "flag"),
     Token("CISD_CMM", "CISD", "bars", "sig_cisd_cplus_mm", "flag"))

# RSI bands. Round cuts, declared here and never moved. A band is exclusive by
# construction, which is why exclusion_group is permitted on `band`.
RSI_CUTS = ((0, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 101))
_add(*[Token(f"RSI_{lo}_{hi}", "RSI_BAND", "bars", "rsi_14", "band", (lo, hi),
             exclusion_group="rsi_14") for lo, hi in RSI_CUTS])

_add(Token("VBO_UP", "FLOW", "bars", "vbo_up", "flag"),
     Token("VOL5X", "FLOW", "bars", "sig_vol_5x", "flag"),
     Token("VOL10X", "FLOW", "bars", "sig_vol_10x", "flag"),
     Token("VOL20X", "FLOW", "bars", "sig_vol_20x", "flag"))
_add(*[Token(v.upper(), "STRUCTURE", "bars", v, "flag")
       for v in ("be_up", "bo_up", "bx_up", "l34", "l43", "l22")])
_add(*[Token(f"G{n}", "GOG", "bars", f"sig_g{n}", "flag") for n in (1, 2, 4, 6, 11)])
_add(*[Token(v.upper(), "STATE_FLAG", "bars", f"sig_{v}", "flag")
       for v in ("buy", "3g", "conso", "svs", "va")])
_add(*[Token(f"P{v}", "PD_LADDER", "bars", f"sig_p{v}", "flag")
       for v in (2, 3, 50, 55, 66, 89)],
     Token("ANY_P", "PD_LADDER", "bars", "sig_any_p", "flag"),
     *[Token(f"D{v}", "PD_LADDER", "bars", f"sig_d{v}", "flag")
       for v in (2, 3, 50, 55, 66, 89)],
     Token("ANY_D", "PD_LADDER", "bars", "sig_any_d", "flag"))
_add(Token("AD_FRESH", "AD_FLAG", "bars", "ad_fresh", "flag"),
     Token("AD_CLUSTER", "AD_FLAG", "bars", "ad_cluster", "flag"))

# Frame-computed context. These live in the opportunity table, NOT in `bars` — asking
# DuckDB for them returns nothing, which is exactly the silent failure M0 exists to stop.
_add(Token("RS_INTACT", "CONTEXT_GATE", "opportunities", "sig_rs_intact", "flag",
           note="🏆RS — the universal worst-year rescuer"),
     Token("LEAD_IN_LAG", "CONTEXT_GATE", "opportunities", "sig_lead_in_lag", "flag"),
     # ❄️CONSO is deliberately NOT re-declared here. It reaches the opportunity table as
     # `sig_conso` and `bars` as `sig_conso` — one column, read twice. Registering both
     # would create a guaranteed exact-duplicate pair: two names, one membership vector,
     # two top-K slots, and a tie the ranking has to break on sort order. The registry
     # refuses it at construction instead of letting combo_universe discover it later.
     Token("H1_DR", "CONTEXT_GATE", "opportunities", "sig_h1_dr", "flag"),
     Token("H1_QUIET", "CONTEXT_GATE", "opportunities", "sig_h1_quiet", "flag"),
     Token("M15_ZDOM", "CONTEXT_GATE", "opportunities", "sig_m15_zdom", "flag"),
     Token("IV_VSPIKE", "CONTEXT_GATE", "opportunities", "sig_iv_vspike", "flag"))

# The only DAY_LEVEL token in the vocabulary, and it is here to be ROUTED, not searched.
# Every combination containing it becomes MIXED_LEVEL and is deferred — which is the point:
# without it in the registry the routing rule would never be exercised by real data.
_add(Token("MACRO_VIX_UP", "MACRO", "opportunities", "sig_macro_vix_up", "flag",
           null_requirement=DAY_LEVEL,
           note="constant within a trading date — N0's FWER 0.685 family"))

REGISTRY: tuple = tuple(_T)
BY_ID = {t.token_id: t for t in REGISTRY}
if len(BY_ID) != len(REGISTRY):
    seen, dup = set(), []
    for t in REGISTRY:
        (dup.append(t.token_id) if t.token_id in seen else seen.add(t.token_id))
    raise ValueError(f"duplicate token_id: {sorted(set(dup))}")


class SilentMissingTokenError(RuntimeError):
    """A declared token has no materialized column.

    The pattern this exists to end is one line long and appears twice in this repo:

        cols = [c for c in SIG_COLS if c in g]          build_opportunities.py:70
        flags = [c for c in _SEQ3_FLAGS if c in _avail] studio/ultra_db_scan.py:681

    Both silently shrink the vocabulary. For a screener that is a missing chip; for a
    search manifest it is a k that does not match the space that was actually searched,
    and nothing in the output looks wrong.
    """


# ── grammar ──────────────────────────────────────────────────────────────────
MAX_CONTEXTUAL_DEPTH = 2      # Setup + A + B. The setup is the stratum, not a token.
BASE_SETUP_REQUIRED = True    # θ_c answers "what does the context ADD to a fired setup".
REQUIRE_DISTINCT_FAMILY = True

GRAMMAR_NOTE = """A candidate is BaseSetup + up to MAX_CONTEXTUAL_DEPTH context tokens.

base_setup = NONE is refused. The estimand is stratified within setup family; with no
stratum there is no incremental question, only a marginal one, and v1's null B measured
what that is worth: P(any promotion | Cell→Y destroyed) = 0.525. Finding NEW setups is a
different instrument, and calling this one by that name is how the 0.525 gets forgotten.

Two tokens from ONE family are refused. Not for redundancy — for interpretability: RA and
RF are two answers to one question, and their conjunction is either empty or a narrower
version of one of them. Cross-family is where an interaction can exist at all.
"""


def spec() -> dict:
    return {
        "spec_version": SPEC_VERSION,
        "n_tokens": len(REGISTRY),
        "families": {f: sum(1 for t in REGISTRY if t.family == f) for f in FAMILIES},
        "kinds": KINDS,
        "null_status": NULL_STATUS,
        "max_contextual_depth": MAX_CONTEXTUAL_DEPTH,
        "base_setup_required": BASE_SETUP_REQUIRED,
        "require_distinct_family": REQUIRE_DISTINCT_FAMILY,
        "excluded_from_search": EXCLUDED_FROM_SEARCH,
        "rsi_cuts": [list(c) for c in RSI_CUTS],
        "tokens": [dict(token_id=t.token_id, family=t.family, source=t.source,
                        column=t.column, kind=t.kind, value=t.value,
                        null_requirement=t.null_requirement,
                        exclusion_group=t.exclusion_group) for t in REGISTRY],
    }


def digest() -> str:
    return hashlib.sha256(json.dumps(spec(), sort_keys=True,
                                     default=str).encode()).hexdigest()[:16]


if __name__ == "__main__":
    s = spec()
    print(f"COMBO TOKEN REGISTRY  {SPEC_VERSION}  digest {digest()}")
    print(f"  {s['n_tokens']} tokens · {len([f for f, n in s['families'].items() if n])} "
          f"families")
    print(f"  grammar: BaseSetup + ≤{MAX_CONTEXTUAL_DEPTH} tokens, distinct families, "
          f"base setup REQUIRED")
    src = {}
    for t in REGISTRY:
        src[t.source] = src.get(t.source, 0) + 1
    print(f"  sources: " + " · ".join(f"{k}={v}" for k, v in sorted(src.items())))
    nl = {}
    for t in REGISTRY:
        nl[t.null_requirement] = nl.get(t.null_requirement, 0) + 1
    print(f"  null:    " + " · ".join(f"{k}={v}" for k, v in sorted(nl.items())))
    print()
    for f, n in s["families"].items():
        if n:
            ids = [t.token_id for t in REGISTRY if t.family == f]
            print(f"  {f:<16} {n:>3}  {' '.join(ids[:12])}"
                  + (" …" if len(ids) > 12 else ""))
