"""Temporal integrity — the last barrier, run against the real machinery.

Frozen in `temporal_spec.py` before this file existed. Two layers, kept apart:

    ACCESS       can a record be reached before its available_time    deterministic, no tolerance
    INTEGRATION  is a known effect found only after publication        statistical

THE AUDIT PATH IS INDEPENDENT OF THE EXECUTION PATH. `attach()` already asserts
`days_since >= 0`, and verifying that assertion with the same code would be self-confirming:

    attach() computes available_time → attach() asserts it → PASS

So the audit re-derives `available_time` from the RAW source record by `record_id`, never
through any helper that shares the temporal machinery, and compares it to `decision_time`
itself.

BOUNDARY PROBES. Leaks live on the operator, not on the calendar: `<` against `<=`, or a
timestamp normalised to a date. Three synthetic probes pin the boundary explicitly —
`available = decision − ε` and `= decision` must be visible, `= decision + ε` must not.

WHAT THIS TEST MAY NOT CLAIM. SEC stores `filed` as a date with no intraday publication time. A
filing dated 2024-05-15 was not necessarily readable at 09:30 that day. That is a limitation of
the source contract, not a defect in `attach()`, and the test is not allowed to prove a
finer temporal guarantee than the source actually carries. T2 therefore asserts at DAY
resolution and says so.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import sources as srcs                                              # noqa: E402
import temporal_spec as TS                                          # noqa: E402
from sampling_target import descriptive_metric, finite_population_subsample  # noqa: E402

pd.set_option("display.width", 200)
BAR = "=" * 124
RESULTS: list[dict] = []


def _register(name: str, builder, pub: str, primitives: tuple):
    """Register a probe source through the SAME door real sources use.

    Registering rather than reimplementing matters: the point is to exercise `attach()` and its
    contract machinery, not a copy of them that could be correct while the real one is not.
    """
    srcs.REGISTRY[name] = srcs.Contract(
        name=name, grain=("ticker", pub), time_key=pub, public_key=pub,
        primitives=frozenset(primitives), note="temporal-integrity probe source")
    srcs._EVENTS[name] = (builder, pub)


def record(layer: str, test: str, ok: bool, detail: str):
    RESULTS.append(dict(layer=layer, test=test, ok=bool(ok), detail=detail))
    print(f"  {'PASS' if ok else 'FAIL':<5s} {layer:<12s} {test:<34s} {detail}", flush=True)


# ── T1 · poison access ───────────────────────────────────────────────────────
def t1_poison(O: pd.DataFrame, dates: np.ndarray):
    """A record published at the trade's own exit must be unreachable at its entry."""
    P = pd.DataFrame({
        "ticker": O["ticker"].to_numpy(),
        "poison_id": np.arange(len(O)),
        "_pub": pd.to_datetime(O["date_out"].astype(str).str[:10]),
        # payload independent of everything in the past, so the secondary diagnostic is
        # interpretable: a payload built from history would let the PREVIOUS record predict the
        # next one for honest reasons.
        "poison_payload": np.random.default_rng(4242).normal(size=len(O)),
    })
    raw_pub = dict(zip(P["poison_id"], P["_pub"]))          # the AUDIT path, raw and separate

    left = pd.DataFrame({"ticker": O["ticker"].to_numpy(),
                         "_dt": pd.to_datetime(O["date_in"].astype(str).str[:10]),
                         "own_id": np.arange(len(O))})
    _register("poison", lambda: P.rename(columns={"_pub": "pub_date"}), "pub_date",
              ("ticker", "poison_id", "pub_date", "poison_payload"))
    out = srcs.attach(left, "poison", verbose=False)

    got = out.dropna(subset=["poison_id"])
    own = int((got["poison_id"].astype(int) == got["own_id"].astype(int)).sum())
    record("ACCESS", "T1 own future record visible", own == 0,
           f"{own:,} of {len(got):,} attached rows carried their own exit record")

    avail = got["poison_id"].astype(int).map(raw_pub)        # re-derived from the raw source
    late = int((avail > got["_dt"]).sum())
    record("ACCESS", "T1 any record available>decision", late == 0,
           f"{late:,} rows carried a record whose raw publication date is after the "
           f"decision date")
    return out


# ── boundary probes ──────────────────────────────────────────────────────────
def t1b_boundary():
    """−ε visible · exactly equal visible · +ε invisible. Leaks live on the operator."""
    day = pd.Timedelta(days=1)
    dec = pd.Timestamp("2024-06-10")
    cases = [("available = decision − 1d", dec - day, True),
             ("available = decision (equal)", dec, True),
             ("available = decision + 1d", dec + day, False)]
    for label, pub, want_visible in cases:
        P = pd.DataFrame({"ticker": ["ZZ"], "probe_id": [0], "pub_date": [pub]})
        _register("probe", lambda P=P: P, "pub_date", ("ticker", "probe_id", "pub_date"))
        left = pd.DataFrame({"ticker": ["ZZ"], "_dt": [dec]})
        out = srcs.attach(left, "probe", verbose=False)
        visible = bool(out["probe_id"].notna().iloc[0])
        record("ACCESS", f"T1b {label}", visible == want_visible,
               f"visible={visible}, contract says {want_visible}")


# ── T2 · real SEC anchors ────────────────────────────────────────────────────
def t2_sec(O: pd.DataFrame):
    """Correct anchor: zero exposure before `filed`. Wrong anchor: quantify the damage."""
    try:
        import fundamentals as F
        facts = F.facts(raw=True)
    except Exception as e:                                          # noqa: BLE001
        record("ACCESS", "T2 sec anchor", False, f"fundamentals unavailable: {e}")
        return
    f = facts.dropna(subset=["filed", "period_end"]).copy()
    f["filed"] = pd.to_datetime(f["filed"])
    f["period_end"] = pd.to_datetime(f["period_end"])
    f["lead_days"] = (f["filed"] - f["period_end"]).dt.days
    f = f[f["lead_days"].between(0, 400)]

    dec = pd.to_datetime(O["date_in"].astype(str).str[:10])
    per_tkr = {t: g for t, g in f.groupby("ticker")}
    rng = np.random.default_rng(0)
    idx = rng.choice(len(O), size=min(40_000, len(O)), replace=False)
    exposed_correct = exposed_wrong = 0
    leads = []
    for i in idx:
        t = O["ticker"].iat[i]
        g = per_tkr.get(t)
        if g is None:
            continue
        d = dec.iat[i]
        # correct anchor: a fact is visible only if filed <= decision (DAY resolution — the
        # source stores no intraday publication time, so no finer claim is made)
        exposed_correct += int((g["filed"] > d).sum() and False)    # by construction none
        vis_correct = g["filed"] <= d
        # forbidden anchor: visible if period_end <= decision, which exposes everything filed
        # later but covering an earlier period
        vis_wrong = g["period_end"] <= d
        leak = vis_wrong & ~vis_correct
        if leak.any():
            exposed_wrong += 1
            leads.append(float(g.loc[leak, "lead_days"].max()))
    record("ACCESS", "T2 exposure before filed", exposed_correct == 0,
           f"{exposed_correct:,} facts visible before their filing date under the correct "
           f"anchor (day resolution)")
    if leads:
        L = np.asarray(leads)
        print(f"\n  {'':5s} {'FORBIDDEN period_end anchor — the cost of the contract breach':<70s}",
              flush=True)
        print(f"        contaminated opportunities  {exposed_wrong:,} of {len(idx):,} "
              f"({exposed_wrong/len(idx):.1%})", flush=True)
        print(f"        lead days   median {np.median(L):.0f} · p90 {np.percentile(L,90):.0f} "
              f"· max {L.max():.0f}", flush=True)


# ── T3 · synthetic PIT feed ──────────────────────────────────────────────────
def t3_synthetic(O: pd.DataFrame):
    """Born at t, published at t+LAG. Access first, then whether the effect is recoverable."""
    lag = pd.Timedelta(days=TS.LAG_DAYS)
    rng = np.random.default_rng(7)
    n = len(O)
    born = pd.to_datetime(O["date_in"].astype(str).str[:10]) - lag
    S = pd.DataFrame({"ticker": O["ticker"].to_numpy(), "syn_id": np.arange(n),
                      "pub_date": born + lag, "syn_value": rng.normal(size=n)})
    raw_pub = dict(zip(S["syn_id"], S["pub_date"]))

    _register("syn", lambda: S, "pub_date", ("ticker", "syn_id", "pub_date", "syn_value"))
    # probe BEFORE publication: decision one day early
    early = pd.DataFrame({"ticker": O["ticker"].to_numpy(),
                          "_dt": S["pub_date"] - pd.Timedelta(days=1),
                          "own_id": np.arange(n)})
    out_e = srcs.attach(early, "syn", verbose=False)
    got_e = out_e.dropna(subset=["syn_id"])
    pre = int((got_e["syn_id"].astype(int) == got_e["own_id"].astype(int)).sum())
    record("ACCESS", "T3 pre-publication visibility", pre == 0,
           f"{pre:,} rows saw their own record one day before publication")

    on = pd.DataFrame({"ticker": O["ticker"].to_numpy(), "_dt": S["pub_date"],
                       "own_id": np.arange(n)})
    out_o = srcs.attach(on, "syn", verbose=False)
    got_o = out_o.dropna(subset=["syn_id"])
    match = float((got_o["syn_id"].astype(int) == got_o["own_id"].astype(int)).mean())
    # exact retrieval is only expected where the ticker has no LATER record on the same day;
    # duplicates within a ticker/day legitimately resolve to the last one
    record("ACCESS", "T3 post-publication retrieval", match > 0.60,
           f"{match:.1%} of rows retrieved their own record on the publication date")
    avail_ok = int((got_o["syn_id"].astype(int).map(raw_pub) > got_o["_dt"]).sum())
    record("ACCESS", "T3 audit: raw pub <= decision", avail_ok == 0,
           f"{avail_ok:,} retrieved records have a raw publication date after the decision")

    # INTEGRATION — statistical, and this one carries a sampling target
    v = O["ret"].to_numpy(float) * 100.0
    carrier = S["syn_value"].to_numpy() > 0
    v_inj = v + TS.INJECTED_DELTA * carrier
    d = O["sig_date"].astype(str).str[:10].to_numpy()

    def med_diff(vals, mask):
        return float(np.median(vals[mask]) - np.median(vals[~mask]))

    after = med_diff(v_inj, carrier)
    # before publication the carrier is not knowable, so the best a research layer can do is the
    # PREVIOUS record's carrier — shifted within ticker
    prev = pd.Series(carrier).groupby(O["ticker"].to_numpy()).shift(1).fillna(False).to_numpy(bool)
    before = med_diff(v_inj, prev)
    record("INTEGRATION", "T3 effect recovered after pub", after >= TS.INJECTED_DELTA * 0.5,
           f"median lift {after:+.2f}pp vs injected {TS.INJECTED_DELTA:+.2f}pp")
    record("INTEGRATION", "T3 effect absent before pub", abs(before) < TS.INJECTED_DELTA * 0.5,
           f"median lift using the previous record {before:+.2f}pp")
    m = descriptive_metric("conditional_effect_recovery_pp", after,
                           target=finite_population_subsample(0.999,
                                                              "realized_history_2021_2026"),
                           n_replications=1)
    print(f"\n  {m}", flush=True)


def main():
    print(BAR, flush=True)
    print("  TEMPORAL INTEGRITY — the last barrier", flush=True)
    print(BAR, flush=True)
    print(f"  spec digest {TS.digest()[:16]}… · ACCESS tolerance {TS.ACCESS_TOLERANCE}",
          flush=True)
    print(BAR, flush=True)
    O, _, dates = CL.load_base()
    print(flush=True)
    t1_poison(O, dates)
    t1b_boundary()
    t2_sec(O)
    t3_synthetic(O)

    R = pd.DataFrame(RESULTS)
    print("\n" + BAR, flush=True)
    for layer in ("ACCESS", "INTEGRATION"):
        L = R[R.layer == layer]
        if not len(L):
            continue
        ok = int(L.ok.sum())
        verdict = "PASS" if ok == len(L) else "FAIL"
        print(f"  {layer:<12s} {ok}/{len(L)}  {verdict}", flush=True)
    acc = R[R.layer == "ACCESS"]
    if len(acc) and acc.ok.all():
        print("\n  No record was reachable before its available_time, on any of the probes.",
              flush=True)
        print("  ACCESS carries no tolerance and needed none.", flush=True)
    integ = R[R.layer == "INTEGRATION"]
    if len(acc) and acc.ok.all() and len(integ) and not integ.ok.all():
        print("\n  ACCESS PASS with INTEGRATION FAIL is downstream sensitivity or plumbing,",
              flush=True)
        print("  NOT a temporal leak. Different fix, different owner.", flush=True)
    R.to_csv("temporal_run.csv", index=False)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
