"""brain/miner.py — the SELF-DISCOVERY loop. The brain automatically searches for new signal
COMBINATIONS (a base edge ∧ a conditioner state), path-sims each honestly, and only promotes the
ones that survive a strict out-of-sample gate — then writes them into its own detectors so the
spine starts firing them. This is 'analyse the signals, find which combos actually work, update the
brain' done WITHOUT the multiple-testing trap that manufactures fake edges.

The gate (a combo must clear ALL of these to be promoted):
  • n ≥ MIN_N real trades
  • lift: beats its OWN base edge's median by ≥ MIN_LIFT (the conditioner must ADD value)
  • walk-forward: mean return > 0 in BOTH the train era (≤2023) AND the verify era (≥2024)
  • worst calendar year ≥ WORST_TOL (survives the bad year)
  • positive-year fraction ≥ POS_FRAC
  • DSR ≥ DSR_MIN, deflated against the WHOLE family of combos tried (honest about the search)
  • family PBO ≤ PBO_MAX — if the whole search looks overfit, promote NOTHING this run

Survivors → brain/mined_combos.json (edge_replay builds their masks) + registry (tier=watch, so the
spine sizes them at half until real trades prove them) + a lesson in the learning log. Everything is
isolated to brain/ files. Dry-run by default; apply=True commits.
"""
from __future__ import annotations
import json
import os
from datetime import date as _date

from . import registry

_DIR = os.path.dirname(os.path.abspath(__file__))
_MINED = os.path.join(_DIR, "mined_combos.json")

# gate thresholds
MIN_N = 40
MIN_LIFT = 0.4          # combo median must beat base median by ≥0.4pp
WORST_TOL = -0.5        # worst year tolerance (pp)
POS_FRAC = 0.6          # ≥60% of years positive
DSR_MIN = 0.5
PBO_MAX = 0.5
TRAIN_MAX_YEAR = 2023   # ≤2023 = train era, ≥2024 = verify era

# conditioner state columns on the edge_replay frame (must be boolean) + display suffix.
# Curated to the most MOTIVATED states (each is a validated concept from the memory), not every
# boolean — fewer, better hypotheses = less multiple-testing + a tractable weekly search.
CONDITIONERS = [
    ("rs_intact", "🏆RS"), ("ob_retest", "🧱OB"), ("key_level", "🔑KEY"),
    ("bos_up", "🏛️BOS"), ("dip_in_trend", "📉DIP"), ("spring", "SPRING"),
    ("wtevr", "EVR"), ("w2sc", "SC"), ("freshlow15", "FL15"), ("conf_anyfam", "CONF"),
    ("nonvb", "nonVB"), ("m15_zdom", "15mZ"),
]

# (base_col, cond_col) pairs already shipped as hand-built variants — skip to avoid duplicates
KNOWN_PAIRS = {
    ("E_qzcapit", "rs_intact"), ("E_g3abs", "rs_intact"), ("E_confluence", "rs_intact"),
    ("E_rtb_base", "rs_intact"), ("E_z11t11", "rs_intact"), ("E_washout", "rs_intact"),
    ("E_dl1", "rs_intact"), ("E_spring", "rs_intact"), ("E_engulfabs", "rs_intact"),
    ("E_l43triple", "rs_intact"),
    ("E_qzcapit", "ob_retest"), ("E_confluence", "ob_retest"), ("E_dl1", "ob_retest"),
    ("E_rtb_base", "ob_retest"), ("E_g3g3rl", "ob_retest"),
    ("E_spring", "key_level"), ("E_qzcapit", "key_level"), ("E_dl1", "key_level"),
    ("E_qzcapit", "tls_bar"), ("E_g3abs", "tls_bar"), ("E_qzcapit", "bos_up"),
}


def _base_edges() -> list:
    """Registry base signal-edges that map to a real edge_replay mask (the search's LHS).
    EXCLUDES already-mined combos (id 'M_*') — otherwise the miner would treat its own outputs as
    bases and stack a 2nd conditioner (M_M_*), exploding combinatorially onto ever-smaller samples
    (overfitting). Depth is capped at 1: base ∧ one conditioner."""
    import edge_replay as er
    cols = {c for _, c in er.SETUPS}
    return [(e["id"], e["col"]) for e in registry.live_edges()
            if e.get("action") == "signal" and e.get("col") in cols
            and not e["id"].startswith("M_")]


def _walk(per_year: dict) -> tuple:
    """(train_mean, verify_mean) across the ≤2023 / ≥2024 era split of per-year returns."""
    tr = [v for y, v in per_year.items() if y.isdigit() and int(y) <= TRAIN_MAX_YEAR]
    ve = [v for y, v in per_year.items() if y.isdigit() and int(y) > TRAIN_MAX_YEAR]
    tm = sum(tr) / len(tr) if tr else None
    vm = sum(ve) / len(ve) if ve else None
    return tm, vm


def _monthly(tr) -> dict:
    """{ 'YYYY-MM': mean_ret } for one candidate's trade frame — for the family PBO matrix."""
    if tr is None or len(tr) == 0:
        return {}
    m = tr.copy()
    m["ym"] = m["date_in"].astype(str).str[:7]
    return m.groupby("ym")["ret"].mean().to_dict()


def mine(apply: bool = False, months: int = 64, dv_floor: float = 3_000_000,
         date: str | None = None, sample: int = None) -> dict:
    """Run the discovery search. Returns a summary; apply=True promotes survivors.
    sample: cap the number of (base,conditioner) candidates — for a fast smoke test only."""
    import numpy as np
    import edge_replay as er
    import overfit_stats as ov
    if date is None:
        date = _date.today().isoformat()

    grp, as_of = er._frame(int(months), float(dv_floor))
    bases = _base_edges()
    TMP = "__MINE_TMP__"

    def _sim(build_mask):
        """build_mask(g)->bool Series per group; returns (_stats row, per-trade df)."""
        for g in grp.values():
            try:
                g[TMP] = build_mask(g)
            except Exception:
                pass
        tr = er._pathsim(grp, TMP, "trail", 0.10, 0.25, 0.25, 60)
        for g in grp.values():
            if TMP in g.columns:
                del g[TMP]
        if tr is None or len(tr) == 0:
            return {"n": 0, "median": 0, "per_year": {}, "total_years": 0}, tr
        return er._stats("x", tr), tr

    pairs = [(bid, bcol, cond, suff) for bid, bcol in bases for cond, suff in CONDITIONERS
             if (bcol, cond) not in KNOWN_PAIRS]
    if sample:
        pairs = pairs[:int(sample)]

    # base medians (denominator for lift) — only for bases that appear in the search
    base_med, base_cols = {}, {b: c for b, c, _, _ in pairs}
    for bid, bcol in base_cols.items():
        st, _ = _sim(lambda g, c=bcol: g[c].fillna(False).astype(bool) if c in g else False)
        base_med[bid] = st.get("median", 0)

    # every candidate combo
    cands = []
    for bid, bcol, cond, suff in pairs:
        st, tr = _sim(lambda g, b=bcol, c=cond: (
            (g[b].fillna(False).astype(bool) & g[c].fillna(False).astype(bool))
            if (b in g and c in g) else False))
        if st.get("n", 0) < MIN_N:
            continue
        rets = tr["ret"].to_numpy(float)
        sr = ov.sharpe(rets)
        cands.append({"base": bid, "base_col": bcol, "cond": cond, "suffix": suff,
                      "id": f"M_{bid}_{cond}", "display": f"{bid}·{suff}",
                      "stats": st, "rets": rets, "sr": sr, "monthly": _monthly(tr)})

    if not cands:
        return {"applied": apply, "summary": "no candidates cleared min-n", "tested": 0, "promoted": []}

    # family-aware DSR: deflate each candidate against the Sharpes of the WHOLE search
    trial_srs = [c["sr"] for c in cands]
    n_trials = len(cands)
    for c in cands:
        c["dsr"] = ov.dsr(c["rets"], trial_srs, n_trials=n_trials)["dsr"]

    # family PBO across all candidates (months × candidates matrix of monthly means)
    months_all = sorted({m for c in cands for m in c["monthly"]})
    pbo = None
    if len(months_all) >= 8 and len(cands) >= 2:
        M = np.array([[c["monthly"].get(m, 0.0) for c in cands] for m in months_all], float)
        pbo = ov.pbo_cscv(M, S=8).get("pbo")

    # gate
    survivors = []
    for c in cands:
        st = c["stats"]
        med = st.get("median", 0)
        lift = med - base_med.get(c["base"], 0)
        tm, vm = _walk(st.get("per_year", {}))
        pos = (st.get("pos_years", 0) / st["total_years"]) if st.get("total_years") else 0
        ok = (st.get("n", 0) >= MIN_N and lift >= MIN_LIFT
              and tm is not None and vm is not None and tm > 0 and vm > 0
              and (st.get("worst_year") is not None and st["worst_year"] >= WORST_TOL)
              and pos >= POS_FRAC and c["dsr"] >= DSR_MIN)
        if ok:
            survivors.append({**c, "median": round(med, 2), "lift": round(lift, 2),
                              "train_mean": round(tm, 2), "verify_mean": round(vm, 2),
                              "worst_year": st.get("worst_year"), "pos": f"{st.get('pos_years')}/{st['total_years']}"})

    family_overfit = pbo is not None and pbo > PBO_MAX
    promoted = []
    if apply and survivors and not family_overfit:
        existing = json.load(open(_MINED)) if os.path.exists(_MINED) else []
        by_id = {m["id"]: m for m in existing}
        for s in survivors:
            rec = {"id": s["id"], "display": s["display"], "base_col": s["base_col"],
                   "cond_col": s["cond"], "base": s["base"], "promoted": date,
                   "stats": {"median": s["median"], "lift": s["lift"], "win": s["stats"].get("win"),
                             "pf": s["stats"].get("pf"), "n": s["stats"].get("n"),
                             "pos_years": s["pos"], "worst_year": s["worst_year"],
                             "train_mean": s["train_mean"], "verify_mean": s["verify_mean"],
                             "dsr": s["dsr"]}}
            by_id[s["id"]] = rec
            registry.record(s["id"], "edge", f"Mined: {s['base']} · {s['suffix']}",
                            definition=f"{s['base']} ∧ {s['cond']} — auto-discovered combo (lift {s['lift']:+.2f}pp, "
                                       f"DSR {s['dsr']}, train {s['train_mean']:+.2f}/verify {s['verify_mean']:+.2f})",
                            layer=3, direction="long", action="signal", tier="watch", status="live",
                            col=s["id"], stats=rec["stats"], source="brain/miner.py", date=date)
            promoted.append(s["id"])
        with open(_MINED, "w") as f:
            json.dump(list(by_id.values()), f, indent=2, ensure_ascii=False)
            f.write("\n")
        try:
            from .learn import _log_append
            _log_append({"date": date, "kind": "combo_mined",
                         "observation": f"searched {n_trials} combos, {len(survivors)} passed OOS gate "
                                        f"(family PBO {pbo})",
                         "action": f"promoted {len(promoted)}: {', '.join(promoted)}"})
        except Exception:
            pass

    summary = (f"family PBO {pbo} > {PBO_MAX} — search looks overfit, promoted nothing"
               if family_overfit else
               f"{len(survivors)} combos passed of {n_trials} searched"
               + (f", promoted {len(promoted)}" if apply else " (dry-run)"))
    return {"applied": apply, "summary": summary, "tested": n_trials, "family_pbo": pbo,
            "n_survivors": len(survivors), "promoted": promoted,
            "survivors": [{k: s[k] for k in ("id", "display", "median", "lift", "train_mean",
                                             "verify_mean", "worst_year", "pos", "dsr")}
                          for s in survivors]}


if __name__ == "__main__":
    print(json.dumps(mine(apply=False), indent=2, ensure_ascii=False))
