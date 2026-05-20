"""Final GO/WATCH_HIGH/WATCH/REJECT/SHORT_WATCH normalizer for TZ+WLNBB scanner.

Hard gate hierarchy:

  1. Hard reject roles                   → REJECT
  2. Short roles                         → SHORT_WATCH
  3. NO_EDGE / liquidity skip            → REJECT
  4. GO-eligible roles                   → GO / WATCH_HIGH / WATCH

GO requires (all):
  - volume bucket B or VB
  - ABR category B or B+ AND abr_gate_pass=True
  - matched_status STRONG or GOOD              (AVERAGE → WATCH_HIGH cap)
  - statistical_status_composite STRONG/GOOD/AVERAGE (not WEAK/REJECT)
  - sample_confidence ≥ DIRECTIONAL_ONLY
  - no conflict flag
  - no composite/seq4 REJECT in any lookup
  - no blacklist_rule_matched of any kind
  - no static-fallback REJECT composite match
  - full_suffix not weak (currently: EUR)

WATCH_HIGH requires (all):
  - everything required for GO except one of:
    * matched_status AVERAGE (with composite stat GOOD/STRONG)
    * (no other allowed soft caps — blacklist and weak-suffix block here)

WATCH = anything that fails the GO/WATCH_HIGH paths.

Adds diagnostic columns without modifying role, score, or raw T/Z logic.
"""
from __future__ import annotations
import csv
import os
from typing import Optional

from .stat_engine import (
    compute_stat_status, compute_sample_confidence, _safe_float, _safe_int,
)


def _safe_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return False


# ── Role classification ──────────────────────────────────────────────────────
_GO_ELIGIBLE_ROLES = frozenset({
    "BULL_A", "PULLBACK_GO", "PULLBACK_READY_A",
})
_WATCH_ELIGIBLE_ROLES = frozenset({
    "BULL_B", "BULL_CONTINUATION_A", "BULL_CONTINUATION_B",
    "PULLBACK_READY_B", "PULLBACK_CONFIRMING",
    "BULL_WATCH", "MIXED_WATCH", "EXTENDED_WATCH",
    "PULLBACK_WATCH", "DEEP_PULLBACK_WATCH",
})
_REJECT_ROLES = frozenset({"REJECT", "REJECT_LONG"})
_SHORT_ROLES  = frozenset({"SHORT_WATCH", "SHORT_GO"})

# ── Hard-coded gate constants ─────────────────────────────────────────────────
_GO_VOL_BUCKETS    = frozenset({"B", "VB"})
_ABR_GO_CATEGORIES = frozenset({"B", "B+"})
_ABR_REJECT_CATS   = frozenset({"R"})
_GO_MATCHED_STATUSES = frozenset({"STRONG", "GOOD"})

# Suffixes that are statistically weaker.
# Per latest rule: weak suffix is a HARD block (WATCH max) unless composite_seq4
# is STRONG.
_WEAK_SUFFIXES = frozenset({"EUR"})

# Static fallback REJECT-composite blacklist (29 composites with n≥50 and status=REJECT
# from the May 2026 SP500/1d statistical research pass). Used when no
# composite_blacklist.csv is loaded at runtime — keeps known-bad patterns
# blocked even if the operator forgets to regenerate the whitelist files.
_STATIC_REJECT_COMPOSITES = frozenset({
    "Z3L25NDPI",   "Z2L25NDPO",   "Z1L5EBO",     "Z1GL25NDO",   "Z1L25NRO",
    "T10L25NDPI",  "T1L34NHA",    "T3L34NHI",    "Z2GL25NURO",  "T10L46NDI",
    "Z4L46NHO",    "Z11L34NUI",   "Z2GL46NRO",   "T2GL34NBA",   "T1GL12NHA",
    "Z10L34NBI",   "Z1GL25NHO",   "Z5L12NDPA",   "Z5L12EUR",    "Z11L34NDPI",
    "T1GL12NPA",   "T2GL34NDPA",  "Z5L34NDPA",   "T5L25NBO",    "Z11L12NHA",
    "Z1GL46NBO",   "Z2GL46NHO",   "Z2GL25NBO",   "Z11L34NDPA",
})

# ── whitelist / blacklist CSV lookups (lazy-loaded) ───────────────────────────
_COMP_SEQ4_STATUS: dict[tuple[str, str], str] | None = None
_COMP_STATUS:      dict[str, str]                   | None = None
_SEQ4_STATUS:      dict[str, str]                   | None = None
_LOOKUP_DIRS = (".", "/tmp/whitelists", "/tmp")


def _try_load_pair_csv(path: str, default: str, key_a: str, key_b: str
                       ) -> dict[tuple[str, str], str]:
    out: dict[tuple[str, str], str] = {}
    if not os.path.isfile(path):
        return out
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                a = (row.get(key_a) or "").strip()
                b = (row.get(key_b) or "").strip()
                status = (row.get("status") or default).strip().upper()
                if a and b:
                    out[(a, b)] = status
    except Exception:
        pass
    return out


def _try_load_single_csv(path: str, default: str, key: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not os.path.isfile(path):
        return out
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                k = (row.get(key) or "").strip()
                status = (row.get("status") or default).strip().upper()
                if k:
                    out[k] = status
    except Exception:
        pass
    return out


def _ensure_comp_seq4_lookup() -> dict[tuple[str, str], str]:
    global _COMP_SEQ4_STATUS
    if _COMP_SEQ4_STATUS is not None:
        return _COMP_SEQ4_STATUS
    merged: dict[tuple[str, str], str] = {}
    for d in _LOOKUP_DIRS:
        merged.update(_try_load_pair_csv(
            os.path.join(d, "composite_seq4_blacklist.csv"), "REJECT", "composite", "seq4"))
        merged.update(_try_load_pair_csv(
            os.path.join(d, "composite_seq4_whitelist.csv"), "GOOD", "composite", "seq4"))
        merged.update(_try_load_pair_csv(
            os.path.join(d, "composite_seq4_stats.csv"), "UNKNOWN", "composite", "seq4"))
    _COMP_SEQ4_STATUS = merged
    return merged


def _ensure_comp_lookup() -> dict[str, str]:
    global _COMP_STATUS
    if _COMP_STATUS is not None:
        return _COMP_STATUS
    merged: dict[str, str] = {}
    for d in _LOOKUP_DIRS:
        merged.update(_try_load_single_csv(
            os.path.join(d, "composite_blacklist.csv"), "REJECT", "composite"))
        merged.update(_try_load_single_csv(
            os.path.join(d, "composite_whitelist.csv"), "GOOD", "composite"))
    _COMP_STATUS = merged
    return merged


def _ensure_seq4_lookup() -> dict[str, str]:
    global _SEQ4_STATUS
    if _SEQ4_STATUS is not None:
        return _SEQ4_STATUS
    merged: dict[str, str] = {}
    for d in _LOOKUP_DIRS:
        merged.update(_try_load_single_csv(
            os.path.join(d, "seq4_blacklist.csv"), "REJECT", "seq4"))
        merged.update(_try_load_single_csv(
            os.path.join(d, "seq4_whitelist.csv"), "GOOD", "seq4"))
    _SEQ4_STATUS = merged
    return merged


def reload_lookups() -> dict[str, int]:
    """Clear caches and reload. Returns counts per lookup."""
    global _COMP_SEQ4_STATUS, _COMP_STATUS, _SEQ4_STATUS
    _COMP_SEQ4_STATUS = None
    _COMP_STATUS      = None
    _SEQ4_STATUS      = None
    return {
        "composite_seq4": len(_ensure_comp_seq4_lookup()),
        "composite":      len(_ensure_comp_lookup()),
        "seq4":           len(_ensure_seq4_lookup()),
        "static_reject":  len(_STATIC_REJECT_COMPOSITES),
    }


def reload_comp_seq4_lookup() -> int:
    """Back-compat alias."""
    return reload_lookups()["composite_seq4"]


def _comp_seq4_status(composite: str, seq4: str) -> str:
    if not composite or not seq4:
        return "UNKNOWN"
    return _ensure_comp_seq4_lookup().get((composite, seq4), "UNKNOWN")


def _comp_status(composite: str) -> str:
    return _ensure_comp_lookup().get(composite, "UNKNOWN") if composite else "UNKNOWN"


def _seq4_status(seq4: str) -> str:
    return _ensure_seq4_lookup().get(seq4, "UNKNOWN") if seq4 else "UNKNOWN"


def normalize_final_action(clf: dict) -> dict:
    """Apply hard gates and add diagnostic fields to a classifier result dict."""
    role        = clf.get("role", "NO_EDGE")
    score_orig  = int(clf.get("score") or 0)
    vol_bkt     = (clf.get("vol_bucket") or "").strip()
    abr_cat     = (clf.get("abr_category") or "UNKNOWN").strip()
    abr_gate    = _safe_bool(clf.get("abr_gate_pass", False))
    abr_cfl     = (clf.get("abr_conflict_flag") or "").strip()
    orig_cfl    = _safe_bool(clf.get("conflict_flag", False))
    action_ovr  = (clf.get("action") or "").strip()
    reject_flgs = clf.get("reject_flags") or []
    good_flgs   = clf.get("good_flags")   or []
    matched_status = (clf.get("matched_status") or "").strip().upper()
    full_suffix    = (clf.get("full_suffix") or "").strip().upper()
    composite_pat  = (clf.get("composite_pattern") or "").strip()
    seq4_pat       = (clf.get("seq4") or "").strip()

    # ── Lookups: classify each (REJECT, WEAK, GOOD, STRONG, LOW_SAMPLE, UNKNOWN)
    comp_lookup_status     = _comp_status(composite_pat)
    seq4_lookup_status     = _seq4_status(seq4_pat)
    comp_seq4_status_value = _comp_seq4_status(composite_pat, seq4_pat)

    # Static fallback: hard REJECT regardless of CSV-loaded state
    static_reject_hit = composite_pat in _STATIC_REJECT_COMPOSITES

    comp_reject_hard  = comp_lookup_status == "REJECT" or static_reject_hit
    comp_weak_soft    = comp_lookup_status == "WEAK"
    seq4_reject_hard  = seq4_lookup_status == "REJECT"
    seq4_weak_soft    = seq4_lookup_status == "WEAK"
    comp_seq4_reject  = comp_seq4_status_value == "REJECT"
    comp_seq4_weak    = comp_seq4_status_value == "WEAK"
    comp_seq4_strong  = comp_seq4_status_value == "STRONG"

    legacy_whitelist = any(
        f.startswith("COMP:") or f.startswith("SEQ4:") for f in good_flgs
    )
    legacy_blacklist = any(
        f.startswith("COMP:") or f.startswith("SEQ4:") for f in reject_flgs
    )
    whitelist_matched = (
        legacy_whitelist
        or comp_lookup_status in ("STRONG", "GOOD")
        or seq4_lookup_status in ("STRONG", "GOOD")
    )
    # Any blacklist hit (REJECT or WEAK or legacy or static) — used to deny WATCH_HIGH
    blacklist_matched = (
        legacy_blacklist
        or comp_reject_hard or seq4_reject_hard
        or comp_weak_soft   or seq4_weak_soft
        or comp_seq4_reject or comp_seq4_weak
    )
    # STRONG whitelist exception: lets a weak suffix still reach WATCH_HIGH
    strong_whitelist_match = (
        comp_lookup_status == "STRONG"
        or seq4_lookup_status == "STRONG"
        or comp_seq4_strong
    )

    # Stat status of the matched matrix rule (per-row stats)
    m_n    = _safe_int(clf.get("matched_n"))
    m_med  = _safe_float(clf.get("matched_med10d_pct"))
    m_fail = _safe_float(clf.get("matched_fail10d_pct"))
    stat_status_comp = compute_stat_status(m_n or 0, m_med, m_fail)
    sample_conf      = compute_sample_confidence(m_n or 0)

    abr_n    = _safe_int(clf.get("abr_n"))
    abr_med  = _safe_float(clf.get("abr_med10d_pct"))
    abr_fail = _safe_float(clf.get("abr_fail10d_pct"))
    stat_status_seq4 = compute_stat_status(abr_n or 0, abr_med, abr_fail)

    abr_conflict_bool   = bool(abr_cfl and abr_cfl not in ("", "NONE", "NO_CONFLICT"))
    conflict_flag_final = orig_cfl or abr_conflict_bool

    volume_gate = ("PASS" if vol_bkt in _GO_VOL_BUCKETS
                   else "WARN" if vol_bkt == "N"
                   else "FAIL")
    abr_gate_ok = (abr_cat in _ABR_GO_CATEGORIES and abr_gate)
    abr_gate_status = ("PASS" if abr_gate_ok
                       else "WARN" if abr_cat == "UNKNOWN" or not abr_gate
                       else "FAIL")

    weak_suffix_flag = full_suffix in _WEAK_SUFFIXES

    # ── Diagnostic status_used columns ────────────────────────────────────────
    if static_reject_hit:
        composite_lookup_status_used = "REJECT:STATIC_FALLBACK"
    else:
        composite_lookup_status_used = comp_lookup_status or "UNKNOWN"
    seq4_lookup_status_used = seq4_lookup_status or "UNKNOWN"
    if weak_suffix_flag:
        suffix_lookup_status_used = f"WEAK_SUFFIX:{full_suffix}"
    else:
        suffix_lookup_status_used = f"SUFFIX:{full_suffix or 'none'}"
    volume_lookup_status_used = f"{volume_gate}:{vol_bkt or 'missing'}"

    downgrade_reasons: list[str] = []
    positive_reasons:  list[str] = []
    final_action: str = "WATCH"

    # ── Hard reject roles ─────────────────────────────────────────────────────
    if role in _REJECT_ROLES or action_ovr in ("IGNORE", "DO_NOT_BUY"):
        final_action = "REJECT"
        if blacklist_matched:
            downgrade_reasons.append("BLACKLIST_MATCH")

    # ── Short roles ───────────────────────────────────────────────────────────
    elif role in _SHORT_ROLES:
        final_action = "SHORT_WATCH"
        if abr_conflict_bool:
            downgrade_reasons.append(f"ABR_CONFLICT:{abr_cfl}")
        if seq4_reject_hard or seq4_weak_soft:
            downgrade_reasons.append(f"BLACKLIST_SEQ4:{seq4_lookup_status}")

    # ── No edge / liquidity skip ──────────────────────────────────────────────
    elif role == "NO_EDGE" or action_ovr in ("NO_ACTION", "LOW_LIQUIDITY_SKIP"):
        final_action = "REJECT"

    # ── GO-eligible roles: hard gates → GO / WATCH_HIGH / WATCH ───────────────
    elif role in _GO_ELIGIBLE_ROLES:

        # ── Hard blocks (any of these forces WATCH, never WATCH_HIGH) ────────
        # Only REJECT-level blacklist and structural failures hard-block here.
        # WEAK-level blacklist becomes a soft cap below (WATCH_HIGH eligible).
        hard_block: list[str] = []
        if volume_gate != "PASS":
            hard_block.append(f"VOL_GATE:vol={vol_bkt or 'missing'}")
        if not abr_gate_ok:
            hard_block.append(f"ABR_GATE:{abr_cat}:gate_pass={abr_gate}")
        if abr_cat in _ABR_REJECT_CATS:
            hard_block.append("ABR_CATEGORY:R")
        if conflict_flag_final:
            hard_block.append("CONFLICT_FLAG")
        if stat_status_comp in ("REJECT", "WEAK"):
            hard_block.append(f"STAT_COMP:{stat_status_comp}")
        if sample_conf == "LOW_SAMPLE":
            hard_block.append("LOW_SAMPLE")
        if comp_seq4_reject:
            hard_block.append("COMP_SEQ4:REJECT")
        # REJECT-level blacklist hits → hard block
        if static_reject_hit:
            hard_block.append(f"STATIC_REJECT_COMPOSITE:{composite_pat}")
        if comp_lookup_status == "REJECT":
            hard_block.append("BLACKLIST_COMPOSITE:REJECT")
        if seq4_lookup_status == "REJECT":
            hard_block.append("BLACKLIST_SEQ4:REJECT")
        if legacy_blacklist:
            hard_block.append("BLACKLIST_LEGACY")
        # Weak suffix (EUR) hard-blocks UNLESS STRONG whitelist override
        if weak_suffix_flag and not strong_whitelist_match:
            hard_block.append(f"WEAK_SUFFIX:{full_suffix}")

        if hard_block:
            final_action = "WATCH"
            downgrade_reasons.extend(hard_block)
        else:
            # All hard gates pass. Apply soft caps (any → WATCH_HIGH max).
            soft_caps: list[str] = []

            # Primary cap: matched_status=AVERAGE → WATCH_HIGH (strict GO needs
            # matched_status in GOOD/STRONG)
            if matched_status in _GO_MATCHED_STATUSES:
                positive_reasons.append(f"MATCHED_STATUS:{matched_status}")
            elif matched_status == "AVERAGE":
                soft_caps.append("MATCHED_STATUS_AVERAGE_CAP")
            elif matched_status:
                soft_caps.append(f"MATCHED_STATUS:{matched_status}")

            # WEAK-level blacklist hits → soft caps (WATCH_HIGH eligible).
            # These mean "below-average pattern" not "actively bad". They
            # demote a would-be GO to WATCH_HIGH but don't kill the
            # candidacy entirely.
            if comp_lookup_status == "WEAK":
                soft_caps.append("BLACKLIST_COMPOSITE:WEAK")
            if seq4_lookup_status == "WEAK":
                soft_caps.append("BLACKLIST_SEQ4:WEAK")
            if comp_seq4_weak:
                soft_caps.append("COMP_SEQ4:WEAK")

            positive_reasons.append(f"VOL_OK:{vol_bkt}")
            positive_reasons.append(f"ABR_OK:{abr_cat}")
            if stat_status_comp in ("GOOD", "STRONG"):
                positive_reasons.append(f"STAT_OK:{stat_status_comp}")
            if comp_seq4_strong or comp_seq4_status_value == "GOOD":
                positive_reasons.append(f"COMP_SEQ4:{comp_seq4_status_value}")

            if not soft_caps:
                final_action = "GO"
            else:
                avg_cap = "MATCHED_STATUS_AVERAGE_CAP" in soft_caps
                avg_cap_ok = (not avg_cap) or stat_status_comp in ("GOOD", "STRONG")
                if avg_cap_ok:
                    final_action = "WATCH_HIGH"
                else:
                    final_action = "WATCH"
                downgrade_reasons.extend(soft_caps)

    # ── WATCH-eligible roles ──────────────────────────────────────────────────
    elif role in _WATCH_ELIGIBLE_ROLES:
        final_action = "WATCH"
        if abr_cat in _ABR_REJECT_CATS:
            downgrade_reasons.append("ABR_CATEGORY:R→bearish_context")
        if volume_gate == "FAIL":
            downgrade_reasons.append(f"VOL_GATE:vol={vol_bkt or 'missing'}")
        if comp_reject_hard or comp_weak_soft:
            downgrade_reasons.append(f"BLACKLIST_COMPOSITE:{comp_lookup_status or 'STATIC_REJECT'}")
        if seq4_reject_hard or seq4_weak_soft:
            downgrade_reasons.append(f"BLACKLIST_SEQ4:{seq4_lookup_status}")

    # ── Fallback ──────────────────────────────────────────────────────────────
    else:
        final_action = "WATCH"
        downgrade_reasons.append(f"UNCLASSIFIED_ROLE:{role}")

    # ── Pine 260520 line-3 / line-4 modifier heuristics ───────────────────────
    # Conservative ±1 tier max. Replace with data-driven thresholds once
    # replay_tz_wlnbb_body_wick_perf.csv / gap_range_perf.csv accumulate data.
    bar_bw = str(clf.get("bar_body_wick") or "")
    bar_gr = str(clf.get("bar_gap_range") or "")
    modifier_flags: list[str] = []

    # Body/Wick modifiers
    if bar_bw.startswith("X") and final_action == "WATCH":
        final_action = "WATCH_HIGH"
        modifier_flags.append("BODY_EXPAND")
    if "J" in bar_bw and final_action == "GO":
        final_action = "WATCH_HIGH"
        modifier_flags.append("DOJI_BODY")
        downgrade_reasons.append("DOJI_BODY")
    if bar_bw.endswith("TB") and final_action == "GO":
        final_action = "WATCH_HIGH"
        modifier_flags.append("HEAVY_UPPER_WICK")
        downgrade_reasons.append("HEAVY_UPPER_WICK")

    # Gap/Range modifiers
    if "V" in bar_gr and final_action == "WATCH":
        final_action = "WATCH_HIGH"
        modifier_flags.append("RANGE_EXPAND")
    if bar_gr.startswith("G3"):
        modifier_flags.append("LARGE_GAP")  # flag only, no tier change
    if bar_gr == "C" and final_action == "GO":
        final_action = "WATCH_HIGH"
        modifier_flags.append("RANGE_CONTRACT")
        downgrade_reasons.append("RANGE_CONTRACT")

    # ── Quality + score ───────────────────────────────────────────────────────
    if final_action == "GO":
        final_quality = "HIGH"
    elif final_action == "WATCH_HIGH":
        final_quality = "MEDIUM_HIGH"
    elif final_action == "WATCH":
        final_quality = "MEDIUM"
    elif final_action == "SHORT_WATCH":
        final_quality = "SHORT"
    else:
        final_quality = "LOW"

    score_after = score_orig
    if final_action == "WATCH_HIGH" and score_orig >= 85:
        score_after = 84
    elif final_action == "WATCH" and score_orig >= 75:
        score_after = 74
    elif final_action == "REJECT":
        score_after = 0

    # ── final_reason: GATES_PASS reserved ONLY for true pass rows ─────────────
    # A "true pass" means: volume_gate=PASS AND abr_gate=PASS AND no downgrade
    # reasons accumulated. Anything else must reflect why the row was capped
    # or downgraded.
    gates_actually_passed = (volume_gate == "PASS") and (abr_gate_status == "PASS")

    if final_action == "GO":
        # GO requires gates to have passed by construction; positive_reasons
        # should always be populated. Fall back to GATES_PASS only if both
        # gates passed and nothing was added (defensive).
        if positive_reasons:
            final_reason = "GO:" + "|".join(positive_reasons)
        elif gates_actually_passed and not downgrade_reasons:
            final_reason = "GO:GATES_PASS"
        else:
            final_reason = "GO:" + " | ".join(downgrade_reasons or ["UNKNOWN"])
    elif final_action == "WATCH_HIGH":
        # Reserve WATCH_HIGH:GATES_PASS strictly for rows where gates truly
        # passed and the only reason this is WATCH_HIGH is a soft cap that
        # was already recorded in downgrade_reasons (or modifier).
        if downgrade_reasons or not gates_actually_passed:
            # Always include the downgrade context — never write a bare
            # WATCH_HIGH:GATES_PASS when gates failed or any downgrade exists.
            parts = list(downgrade_reasons)
            if not gates_actually_passed:
                if volume_gate != "PASS":
                    parts.append(f"VOL_GATE:{volume_gate}")
                if abr_gate_status != "PASS":
                    parts.append(f"ABR_GATE:{abr_gate_status}")
            final_reason = "WATCH_HIGH:" + " | ".join(parts) if parts else "WATCH_HIGH:DOWNGRADED"
        elif positive_reasons:
            final_reason = "WATCH_HIGH:" + "|".join(positive_reasons)
        else:
            final_reason = "WATCH_HIGH:GATES_PASS"
    elif downgrade_reasons:
        final_reason = " | ".join(downgrade_reasons)
    elif gates_actually_passed:
        final_reason = "GATES_PASS"
    else:
        # Catch-all: gates failed but no downgrade_reasons recorded — surface that.
        parts = []
        if volume_gate != "PASS":
            parts.append(f"VOL_GATE:{volume_gate}")
        if abr_gate_status != "PASS":
            parts.append(f"ABR_GATE:{abr_gate_status}")
        final_reason = " | ".join(parts) if parts else "UNKNOWN"

    # ── Hard guardrail: make the WATCH_HIGH:GATES_PASS bug impossible to ship ─
    # If gates didn't actually pass, final_reason must NOT be a bare "GATES_PASS"
    # variant. Repair it in-place rather than raising, so a single misclassified
    # row never silently leaks the pre-fix string into production CSVs.
    if (volume_gate != "PASS" or abr_gate_status != "PASS"):
        if final_reason in ("GATES_PASS", "WATCH_HIGH:GATES_PASS", "GO:GATES_PASS"):
            _repair_parts = list(downgrade_reasons)
            if volume_gate != "PASS":
                _repair_parts.append(f"VOL_GATE:{volume_gate}")
            if abr_gate_status != "PASS":
                _repair_parts.append(f"ABR_GATE:{abr_gate_status}")
            _prefix = (
                "GO:" if final_action == "GO"
                else "WATCH_HIGH:" if final_action == "WATCH_HIGH"
                else ""
            )
            final_reason = _prefix + (" | ".join(_repair_parts) if _repair_parts
                                       else "GATES_FAILED")

    return {
        **clf,
        "final_action":                      final_action,
        "final_quality":                     final_quality,
        "final_reason":                      final_reason,
        "downgrade_reason":                  " | ".join(downgrade_reasons),
        "volume_gate_status":                volume_gate,
        "abr_gate_status":                   abr_gate_status,
        "weak_suffix_flag":                  weak_suffix_flag,
        "statistical_status_signal":         "UNKNOWN",
        "statistical_status_composite":      stat_status_comp,
        "statistical_status_seq4":           stat_status_seq4,
        "statistical_status_composite_seq4": comp_seq4_status_value,
        "sample_confidence":                 sample_conf,
        "whitelist_rule_matched":            whitelist_matched,
        "blacklist_rule_matched":            blacklist_matched,
        "reject_rule_matched":               bool(reject_flgs),
        "conflict_flag_original":            orig_cfl,
        "abr_conflict_flag_bool":            abr_conflict_bool,
        "conflict_flag_final":               conflict_flag_final,
        "score_before_normalization":        score_orig,
        "score_after_normalization":         score_after,
        # ── New diagnostic *_lookup_status_used columns ──────────────────────
        "composite_lookup_status_used":      composite_lookup_status_used,
        "seq4_lookup_status_used":           seq4_lookup_status_used,
        "suffix_lookup_status_used":         suffix_lookup_status_used,
        "volume_lookup_status_used":         volume_lookup_status_used,
        "static_reject_match":               static_reject_hit,
        # Pine 260520 line-3 / line-4 modifier diagnostics
        "bar_body_wick":                     bar_bw,
        "bar_gap_range":                     bar_gr,
        "bar_modifier_flags":                "|".join(modifier_flags),
    }
