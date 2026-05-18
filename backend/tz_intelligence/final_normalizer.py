"""Final GO/WATCH_HIGH/WATCH/REJECT/SHORT_WATCH normalizer for TZ+WLNBB scanner.

Sits on top of classify_tz_event() output and applies a hierarchy of hard gates:

  1. Hard reject roles                   → REJECT
  2. Short roles                         → SHORT_WATCH
  3. NO_EDGE / liquidity skip            → REJECT
  4. GO-eligible roles                   → GO / WATCH_HIGH / WATCH

GO requires (all):
  - volume bucket B or VB
  - ABR category B or B+ AND abr_gate_pass=True
  - matched_status STRONG or GOOD              (AVERAGE → WATCH_HIGH cap)
  - statistical_status_composite STRONG/GOOD/AVERAGE (no WEAK/REJECT)
  - sample_confidence ≥ DIRECTIONAL_ONLY
  - no conflict flag
  - no composite/seq4 blacklist match
  - composite_seq4 not REJECT/WEAK (LOW_SAMPLE allowed — too sparse to gate)
  - full_suffix not in _WEAK_SUFFIXES (EUR cap → WATCH_HIGH max)

WATCH_HIGH = "would-be-GO except for one soft factor":
  - matched_status AVERAGE but composite GOOD/STRONG + all other gates pass
  - OR EUR suffix with otherwise clean GO conditions

WATCH = anything that fails harder gates (volume/ABR/blacklist/conflict).

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
    """Coerce string, int, or bool to Python bool."""
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "yes")
    return False


# Roles eligible for GO (require all hard gates)
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

# Gate constants
_GO_VOL_BUCKETS    = frozenset({"B", "VB"})
_ABR_GO_CATEGORIES = frozenset({"B", "B+"})
_ABR_REJECT_CATS   = frozenset({"R"})

# Suffixes that are statistically weaker. Capped at WATCH_HIGH max; only
# allowed to GO if composite_seq4 is STRONG with sufficient sample.
_WEAK_SUFFIXES = frozenset({"EUR"})

# Matrix matched_status values eligible for GO
_GO_MATCHED_STATUSES = frozenset({"STRONG", "GOOD"})

# ── whitelist / blacklist lookups (lazy-loaded) ───────────────────────────────
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
    }


def reload_comp_seq4_lookup() -> int:
    """Back-compat alias used by main.py."""
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

    # ── Lookup-driven blacklist/whitelist matches ─────────────────────────────
    # REJECT = hard block, WEAK = soft cap (WATCH_HIGH eligible)
    comp_lookup_status     = _comp_status(composite_pat)
    seq4_lookup_status     = _seq4_status(seq4_pat)
    comp_seq4_status_value = _comp_seq4_status(composite_pat, seq4_pat)

    comp_reject_hard  = comp_lookup_status == "REJECT"
    comp_weak_soft    = comp_lookup_status == "WEAK"
    seq4_reject_hard  = seq4_lookup_status == "REJECT"
    seq4_weak_soft    = seq4_lookup_status == "WEAK"
    comp_seq4_reject  = comp_seq4_status_value == "REJECT"
    comp_seq4_weak    = comp_seq4_status_value == "WEAK"

    # Legacy good_flags / reject_flags from classifier (in-row whitelist hints)
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
    blacklist_matched = (
        legacy_blacklist or comp_reject_hard or seq4_reject_hard
        or comp_weak_soft or seq4_weak_soft
    )

    # ── Statistical quality ───────────────────────────────────────────────────
    m_n    = _safe_int(clf.get("matched_n"))
    m_med  = _safe_float(clf.get("matched_med10d_pct"))
    m_fail = _safe_float(clf.get("matched_fail10d_pct"))
    stat_status_comp = compute_stat_status(m_n or 0, m_med, m_fail)
    sample_conf      = compute_sample_confidence(m_n or 0)

    abr_n    = _safe_int(clf.get("abr_n"))
    abr_med  = _safe_float(clf.get("abr_med10d_pct"))
    abr_fail = _safe_float(clf.get("abr_fail10d_pct"))
    stat_status_seq4 = compute_stat_status(abr_n or 0, abr_med, abr_fail)

    # ── Conflict / gate evaluation ────────────────────────────────────────────
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

        # Hard blocks: anything that fails these falls all the way to WATCH
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
        # composite_seq4 REJECT hard-blocks (LOW_SAMPLE/WEAK soft-cap below)
        if comp_seq4_reject:
            hard_block.append("COMP_SEQ4:REJECT")
        # Hard composite/seq4 REJECT blacklist always blocks
        if comp_reject_hard:
            hard_block.append("BLACKLIST_COMPOSITE:REJECT")
        if seq4_reject_hard:
            hard_block.append("BLACKLIST_SEQ4:REJECT")

        if hard_block:
            final_action = "WATCH"
            downgrade_reasons.extend(hard_block)
        else:
            # All hard gates pass. Distinguish GO vs WATCH_HIGH on soft factors.
            soft_caps: list[str] = []

            # matched_status: STRONG/GOOD → GO; AVERAGE → WATCH_HIGH; else WATCH
            if matched_status in _GO_MATCHED_STATUSES:
                positive_reasons.append(f"MATCHED_STATUS:{matched_status}")
            elif matched_status == "AVERAGE":
                soft_caps.append("MATCHED_STATUS_AVERAGE_CAP")
            elif matched_status:
                # WEAK / REJECT / LOW_SAMPLE / unknown matrix status
                soft_caps.append(f"MATCHED_STATUS:{matched_status}")

            # EUR (weak suffix) cap unless comp_seq4 is STRONG
            if weak_suffix_flag and comp_seq4_status_value != "STRONG":
                soft_caps.append(f"WEAK_SUFFIX:{full_suffix}")

            # WEAK seq4/composite blacklist → soft cap (WATCH_HIGH eligible)
            if comp_weak_soft:
                soft_caps.append("BLACKLIST_COMPOSITE:WEAK")
            if seq4_weak_soft:
                soft_caps.append("BLACKLIST_SEQ4:WEAK")
            # WEAK composite_seq4 → soft cap
            if comp_seq4_weak:
                soft_caps.append("COMP_SEQ4:WEAK")

            # Build positive context for final_reason
            positive_reasons.append(f"VOL_OK:{vol_bkt}")
            positive_reasons.append(f"ABR_OK:{abr_cat}")
            if stat_status_comp in ("GOOD", "STRONG"):
                positive_reasons.append(f"STAT_OK:{stat_status_comp}")
            if comp_seq4_status_value in ("STRONG", "GOOD"):
                positive_reasons.append(f"COMP_SEQ4:{comp_seq4_status_value}")

            if not soft_caps:
                final_action = "GO"
            else:
                # Any soft cap → at best WATCH_HIGH. Two conditions:
                # - matched_status AVERAGE cap is acceptable only if the
                #   per-row composite stat is GOOD/STRONG (otherwise the row
                #   is too weak even for WATCH_HIGH and falls to WATCH).
                # - All other soft caps individually are WATCH_HIGH-eligible.
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
            downgrade_reasons.append(f"BLACKLIST_COMPOSITE:{comp_lookup_status}")
        if seq4_reject_hard or seq4_weak_soft:
            downgrade_reasons.append(f"BLACKLIST_SEQ4:{seq4_lookup_status}")

    # ── Fallback ──────────────────────────────────────────────────────────────
    else:
        final_action = "WATCH"
        downgrade_reasons.append(f"UNCLASSIFIED_ROLE:{role}")

    # ── Quality + score after normalization ───────────────────────────────────
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

    # Build human-readable final_reason
    if final_action == "GO":
        final_reason = "GO:" + "|".join(positive_reasons) if positive_reasons else "GO:GATES_PASS"
    elif final_action == "WATCH_HIGH":
        pos = "|".join(positive_reasons) if positive_reasons else "GATES_PASS"
        final_reason = f"WATCH_HIGH:{pos}"
    elif downgrade_reasons:
        final_reason = " | ".join(downgrade_reasons)
    else:
        final_reason = "GATES_PASS"

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
    }
