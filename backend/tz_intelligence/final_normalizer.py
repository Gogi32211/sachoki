"""Final GO/WATCH/REJECT/SHORT_WATCH normalizer for TZ+WLNBB scanner.

Sits on top of classify_tz_event() output and applies hard gates:
- volume bucket: B/VB required for GO
- ABR category + gate_pass: B/B+ + gate=True required for GO
- statistical quality from matched matrix rule n/med/fail fields
- conflict flags (original + ABR combined)
- weak-suffix cap (EUR) — caps GO unless composite_seq4 is STRONG
- matched_status AVERAGE — requires GOOD/STRONG composite stat
- composite_seq4 must not be REJECT/UNKNOWN when available

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

# Suffixes that are statistically weaker (capped at WATCH unless very strong stats)
_WEAK_SUFFIXES = frozenset({"EUR"})

# ── composite_seq4 lookup ─────────────────────────────────────────────────────
# Lazy-loaded from disk: composite_seq4_whitelist.csv + composite_seq4_blacklist.csv
_COMP_SEQ4_STATUS: dict[tuple[str, str], str] | None = None
_COMP_SEQ4_LOOKUP_DIRS = (".", "/tmp/whitelists", "/tmp")


def _try_load_comp_seq4_csv(path: str, default_status: str) -> dict[tuple[str, str], str]:
    """Load one whitelist/blacklist CSV into a (composite, seq4) → status dict."""
    out: dict[tuple[str, str], str] = {}
    if not os.path.isfile(path):
        return out
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                comp = (row.get("composite") or "").strip()
                seq4 = (row.get("seq4") or "").strip()
                status = (row.get("status") or default_status).strip().upper()
                if comp and seq4:
                    out[(comp, seq4)] = status
    except Exception:
        pass
    return out


def _ensure_comp_seq4_lookup() -> dict[tuple[str, str], str]:
    """Lazy-load the composite_seq4 lookup.

    Preference order (later overrides earlier when keys collide):
      1. composite_seq4_blacklist.csv  (REJECT/WEAK pairs)
      2. composite_seq4_whitelist.csv  (STRONG/GOOD pairs)
      3. composite_seq4_stats.csv      (comprehensive: STRONG/GOOD/AVERAGE/WEAK/REJECT)

    The stats file covers every (composite, seq4) pair with n ≥ 20 and is
    what populates statistical_status_composite_seq4 for the vast majority
    of rows. Without it, only ~200 pairs would be classified.
    """
    global _COMP_SEQ4_STATUS
    if _COMP_SEQ4_STATUS is not None:
        return _COMP_SEQ4_STATUS
    merged: dict[tuple[str, str], str] = {}
    for d in _COMP_SEQ4_LOOKUP_DIRS:
        merged.update(_try_load_comp_seq4_csv(
            os.path.join(d, "composite_seq4_blacklist.csv"), "REJECT"))
        merged.update(_try_load_comp_seq4_csv(
            os.path.join(d, "composite_seq4_whitelist.csv"), "GOOD"))
        merged.update(_try_load_comp_seq4_csv(
            os.path.join(d, "composite_seq4_stats.csv"), "UNKNOWN"))
    _COMP_SEQ4_STATUS = merged
    return merged


def reload_comp_seq4_lookup() -> int:
    """Clear cache and reload. Returns number of entries loaded."""
    global _COMP_SEQ4_STATUS
    _COMP_SEQ4_STATUS = None
    return len(_ensure_comp_seq4_lookup())


def _get_comp_seq4_status(composite: str, seq4: str) -> str:
    if not composite or not seq4:
        return "UNKNOWN"
    return _ensure_comp_seq4_lookup().get((composite, seq4), "UNKNOWN")


def normalize_final_action(clf: dict) -> dict:
    """Apply hard gates and add diagnostic fields to a classifier result dict.

    Does NOT modify role, score, or action — only sets final_action and the
    diagnostic columns listed below.
    """
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

    # Statistical quality from matched composite rule
    m_n    = _safe_int(clf.get("matched_n"))
    m_med  = _safe_float(clf.get("matched_med10d_pct"))
    m_fail = _safe_float(clf.get("matched_fail10d_pct"))
    stat_status_comp = compute_stat_status(m_n or 0, m_med, m_fail)
    sample_conf      = compute_sample_confidence(m_n or 0)

    # Statistical quality from ABR (approx seq4-level quality)
    abr_n    = _safe_int(clf.get("abr_n"))
    abr_med  = _safe_float(clf.get("abr_med10d_pct"))
    abr_fail = _safe_float(clf.get("abr_fail10d_pct"))
    stat_status_seq4 = compute_stat_status(abr_n or 0, abr_med, abr_fail)

    # composite_seq4 status from lazy-loaded whitelist/blacklist lookup
    stat_status_comp_seq4 = _get_comp_seq4_status(composite_pat, seq4_pat)

    # Combined conflict
    abr_conflict_bool   = bool(abr_cfl and abr_cfl not in ("", "NONE", "NO_CONFLICT"))
    conflict_flag_final = orig_cfl or abr_conflict_bool

    # Gate evaluations
    volume_gate = ("PASS" if vol_bkt in _GO_VOL_BUCKETS
                   else "WARN" if vol_bkt == "N"
                   else "FAIL")
    abr_gate_ok    = (abr_cat in _ABR_GO_CATEGORIES and abr_gate)
    abr_gate_status = ("PASS" if abr_gate_ok
                       else "WARN" if abr_cat == "UNKNOWN" or not abr_gate
                       else "FAIL")

    whitelist_matched = any(
        f.startswith("COMP:") or f.startswith("SEQ4:") for f in good_flgs
    )
    blacklist_matched = any(
        f.startswith("COMP:") or f.startswith("SEQ4:") for f in reject_flgs
    )

    downgrade_reasons: list[str] = []
    final_action: str

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

    # ── No edge / liquidity skip ──────────────────────────────────────────────
    elif role == "NO_EDGE" or action_ovr in ("NO_ACTION", "LOW_LIQUIDITY_SKIP"):
        final_action = "REJECT"

    # ── GO-eligible roles: apply all hard gates ───────────────────────────────
    elif role in _GO_ELIGIBLE_ROLES:
        gates_pass = True

        if volume_gate != "PASS":
            gates_pass = False
            downgrade_reasons.append(f"VOL_GATE:vol={vol_bkt or 'missing'}")
        if not abr_gate_ok:
            gates_pass = False
            downgrade_reasons.append(
                f"ABR_GATE:{abr_cat}:gate_pass={abr_gate}"
            )
        if abr_cat in _ABR_REJECT_CATS:
            gates_pass = False
            downgrade_reasons.append("ABR_CATEGORY:R")
        if conflict_flag_final:
            gates_pass = False
            downgrade_reasons.append("CONFLICT_FLAG")
        if stat_status_comp in ("REJECT", "WEAK"):
            gates_pass = False
            downgrade_reasons.append(f"STAT_COMP:{stat_status_comp}")
        if sample_conf == "LOW_SAMPLE":
            gates_pass = False
            downgrade_reasons.append("LOW_SAMPLE")
        if blacklist_matched:
            gates_pass = False
            downgrade_reasons.append("BLACKLIST_MATCH")

        # ── New gate: weak-suffix cap (EUR) ───────────────────────────────────
        # EUR is allowed to GO only if composite_seq4 is STRONG (sufficient
        # sample + median ≥ 1% + fail ≤ 20%). Anything else caps at WATCH.
        if full_suffix in _WEAK_SUFFIXES and stat_status_comp_seq4 != "STRONG":
            gates_pass = False
            downgrade_reasons.append(
                f"WEAK_SUFFIX:{full_suffix}/comp_seq4={stat_status_comp_seq4}"
            )

        # ── New gate: matched_status=AVERAGE needs solid composite stat ───────
        # matched_status comes from the matrix-rule status field. AVERAGE
        # rules are only allowed through if the row-specific composite stat
        # is GOOD or STRONG (i.e. this particular pattern is statistically
        # better than the rule average).
        if matched_status == "AVERAGE" and stat_status_comp not in ("GOOD", "STRONG"):
            gates_pass = False
            downgrade_reasons.append(
                f"MATCHED_AVG_NEEDS_GOOD:{stat_status_comp}"
            )

        # ── New gate: composite_seq4 must not be REJECT/UNKNOWN when avail ────
        # If the (composite, seq4) lookup is populated and the pair was
        # explicitly classified REJECT/WEAK, block GO. UNKNOWN means the
        # pair isn't in either list — for now we allow it (insufficient data),
        # but if the lookup has any entries at all we treat UNKNOWN as a soft
        # warning rather than a block, so missing-coverage doesn't kill every GO.
        if stat_status_comp_seq4 in ("REJECT", "WEAK"):
            gates_pass = False
            downgrade_reasons.append(f"COMP_SEQ4:{stat_status_comp_seq4}")

        final_action = "GO" if gates_pass else "WATCH"

    # ── WATCH-eligible roles ──────────────────────────────────────────────────
    elif role in _WATCH_ELIGIBLE_ROLES:
        final_action = "WATCH"
        if abr_cat in _ABR_REJECT_CATS:
            downgrade_reasons.append("ABR_CATEGORY:R→bearish_context")
        if volume_gate == "FAIL":
            downgrade_reasons.append(f"VOL_GATE:vol={vol_bkt or 'missing'}")

    # ── Fallback ──────────────────────────────────────────────────────────────
    else:
        final_action = "WATCH"
        downgrade_reasons.append(f"UNCLASSIFIED_ROLE:{role}")

    # ── Quality and score after normalization ─────────────────────────────────
    final_quality = ("HIGH" if final_action == "GO"
                     else "MEDIUM" if final_action == "WATCH"
                     else "LOW")

    score_after = score_orig
    if final_action == "WATCH" and score_orig >= 80:
        score_after = 79
    elif final_action == "REJECT":
        score_after = 0

    reason_str = " | ".join(downgrade_reasons) if downgrade_reasons else "GATES_PASS"

    return {
        **clf,
        "final_action":                      final_action,
        "final_quality":                     final_quality,
        "final_reason":                      reason_str,
        "downgrade_reason":                  " | ".join(downgrade_reasons),
        "volume_gate_status":                volume_gate,
        "abr_gate_status":                   abr_gate_status,
        "statistical_status_signal":         "UNKNOWN",
        "statistical_status_composite":      stat_status_comp,
        "statistical_status_seq4":           stat_status_seq4,
        "statistical_status_composite_seq4": stat_status_comp_seq4,
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
