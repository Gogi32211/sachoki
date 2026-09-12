"""RESEARCH AUDIT of every edge chip the app displays (edge_replay.DISPLAY_SETUPS).

Every shipped edge was validated with one instrument: maxh=60 + ATR trail, judged on trade
counts. On 2026-09-02/03 that instrument was shown to (a) close 94-97% of trades on the
timer, so it cannot tell an event at the bar from slow drift, and (b) overstate evidence
when signals cluster on market-wide days (a 99.5% "confirmation" became 22% when counted by
DAYS). This audit re-reads every displayed setup through both corrections. It changes no
setup, no chip, no file in the book -- it only measures.

Uses the book's own frame (`_frame`, same universe/masks the chips fire from) and the
unmodified exit engine (`_pathsim`); only `maxh` varies, which is a parameter.

PRE-REGISTERED CLASSIFICATION (fixed before any number was produced):

  lpb(H)  = (setup trade-median - control trade-median) / H        "lift per bar"
  day-level at each H: one observation per ENTRY DAY;
      edge_d = setup day-median - control day-median (same day)
      day_med = median over days of edge_d ; day_win = % days with edge_d > 0

  EVENT   day_med(5) > 0  AND day_win(5) > 50  AND  lpb(5) >= 2 x lpb(60)
          (front-loaded: the edge lives at the bar)
  DRIFT   day_med(60) > 0 AND NOT front-loaded
          (real state, accrues with holding time; not entry timing)
  NULL    day_med(5) <= 0 AND day_med(60) <= 0
  MIXED   anything else
  THIN    flag when entry-days at H=60 < 80 (the L3 floor, now counted in DAYS)

The number the user currently SEES for each chip is the H=60 trade-level median / win /
pos_years. It is printed next to the day-level H=60 number; the gap is the audit finding.

═══ RE-RUN 2026-09-10 — THE 2026-09-03 RESULT USED A DEFECTIVE CONTROL ═══════════════════
The control below was `np.where(close >= 21)[0][::40]` — phase 0 of every ticker's own
eligible sequence. Tickers share entry sessions, so the grids ALIGNED and control trades
bunched onto few days: median 14/day, only 31 % of sessions reaching the 20 a day-median
needs, and the rest taking a "benchmark" off one or two trades. Because this audit's whole
verdict is DAY-level, the defect lands directly on the classification. Measured over the
122 registered setups it flattered day_med by a mean 1.06 pp, flipped 11 signs and moved 13
setups across the day-win 50 line (project-control-impact-audit), so the EVENT/DRIFT/NULL
labels this script produced — and the 🟡watch demotions taken from them — are not safe.

Same repair as `edge_replay` (CONTROL_V2_DEPHASED): identical rule, identical 1-in-40 RATE,
identical estimand; only the per-ticker grid OFFSET changes, plus the MIN_CTRL guard that
never existed. The previous artifact is ARCHIVED, not overwritten, so the two can be diffed.

This run also settles a second question. The five other studies still on phase-0
(t9_l34_mine, tsig_l34_probe, mtf_t_signal_l23, z2g_reversal_mine, z2g_t1g_1d_study) are
all TRADE-level: they pool the control median over the whole window instead of per day. A
different 1-in-40 phase is then just a different sample of the same population, which should
move a ~50k-trade median by noise, not bias. `ctrl_phase_check` measures that difference
directly at every horizon rather than assuming it.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
OUT = "/Users/sachoki/MASSIVE_DATA/APP_AUDIT"
MONTHS, DV = 72, 3_000_000
HOR = [3, 5, 10, 20, 60]
MIN_CTRL = 20            # a session needs this many control trades before its median counts


def day_table(tr, min_n: int = 0):
    """Per-day median. `min_n` drops sessions too thin to be a benchmark — the control is
    passed min_n=MIN_CTRL, setups are not (a setup's own day is the observation)."""
    import numpy as np, pandas as pd
    if tr is None or not len(tr):
        return pd.Series(dtype=float)
    d = pd.to_datetime(tr.date_in).dt.date
    g = (tr.ret * 100).groupby(d)
    return g.median().where(g.size() >= min_n).dropna() if min_n else g.median()


def main():
    import numpy as np, pandas as pd
    import edge_replay as ER
    from edge_replay import _frame, _pathsim, _stats, DISPLAY_SETUPS
    os.makedirs(OUT, exist_ok=True)
    p = os.path.join(OUT, "research_audit.json")
    if os.path.exists(p):
        # ARCHIVE the phase-0 result rather than delete it — the diff IS the finding.
        arch = os.path.join(OUT, "research_audit_PHASE0_2026-09-03.json")
        if not os.path.exists(arch):
            os.rename(p, arch)
            print(f"archived previous (phase-0) audit -> {arch}", flush=True)
        else:
            os.remove(p)
    t0 = time.time()
    grp, as_of = _frame(MONTHS, float(DV))
    print(f"frame: {len(grp):,} tickers · as_of {as_of} ({time.time()-t0:.0f}s)", flush=True)

    # Control column: every 40th bar >= $21 within each ticker, at a DE-PHASED per-ticker
    # offset (ER._ctrl_phase == control_keys.phase; sha256, never the salted python hash()).
    # AUDIT_CTRL_A keeps the old phase-0 grid for one comparison only — see ctrl_phase_check.
    for t, g in grp.items():
        e = np.where(g.close.to_numpy() >= 21)[0]
        a = np.zeros(len(g), dtype=bool)
        b = np.zeros(len(g), dtype=bool)
        if len(e):
            a[e[::40]] = True
            b[e[ER._ctrl_phase(t) % len(e)::40]] = True
        g["AUDIT_CTRL_A"] = a
        g["AUDIT_CTRL"] = b
    ctrl_tr, ctrl_day, ctrl_med, ctrl_cov, phase_check = {}, {}, {}, {}, {}
    for H in HOR:
        tr = _pathsim(grp, "AUDIT_CTRL", "trail", .10, .25, .25, H, atr_k=12.0)
        ctrl_tr[H] = tr
        ctrl_day[H] = day_table(tr, MIN_CTRL)
        ctrl_med[H] = float(np.median(tr.ret) * 100)
        n = (tr.ret).groupby(pd.to_datetime(tr.date_in).dt.date).size()
        ctrl_cov[H] = dict(trades=int(len(tr)), sessions=int(len(n)),
                           median_per_day=float(n.median()),
                           share_ge_min=round(float((n >= MIN_CTRL).mean() * 100), 1),
                           days_kept=int(len(ctrl_day[H])))
        # does the phase change the TRADE-level control median? (the other five studies)
        ta = _pathsim(grp, "AUDIT_CTRL_A", "trail", .10, .25, .25, H, atr_k=12.0)
        na = (ta.ret).groupby(pd.to_datetime(ta.date_in).dt.date).size()
        phase_check[H] = dict(
            trade_med_phase0=round(float(np.median(ta.ret) * 100), 4),
            trade_med_dephased=round(ctrl_med[H], 4),
            delta=round(float(np.median(ta.ret) * 100) - ctrl_med[H], 4),
            n_phase0=int(len(ta)), n_dephased=int(len(tr)),
            day_median_per_day_phase0=float(na.median()),
            day_share_ge_min_phase0=round(float((na >= MIN_CTRL).mean() * 100), 1))
        c = ctrl_cov[H]
        print(f"  control H={H:>2}: {c['trades']:>6,} trades · median {c['median_per_day']:>4.0f}/day "
              f"· >= {MIN_CTRL} on {c['share_ge_min']:>5.1f}% · days kept {c['days_kept']:>4}/{c['sessions']}"
              f"  |  trade-med phase0 {phase_check[H]['trade_med_phase0']:+.3f} vs dephased "
              f"{phase_check[H]['trade_med_dephased']:+.3f} (Δ {phase_check[H]['delta']:+.3f})", flush=True)
    print(f"control built ({time.time()-t0:.0f}s)", flush=True)

    cols = set(next(iter(grp.values())).columns)
    rows = []
    for label, col in DISPLAY_SETUPS:
        if col not in cols:
            rows.append(dict(chip=label, col=col, skipped="not a frame column")); continue
        rec = dict(chip=label, col=col, H={})
        for H in HOR:
            tr = _pathsim(grp, col, "trail", .10, .25, .25, H, atr_k=12.0)
            s = _stats(label, tr)
            if not s["n"]:
                rec["H"][H] = dict(n=0); continue
            sd = day_table(tr)
            j = pd.concat([sd.rename("s"), ctrl_day[H].rename("c")], axis=1).dropna()
            e = (j.s - j.c).to_numpy()
            pos = e[e > 0].sum()
            top2 = np.sort(e)[-2:].sum() if len(e) >= 2 else (e.sum() if len(e) else 0)
            rec["H"][H] = dict(
                n=int(s["n"]), trade_med=s["median"], trade_win=s["win"], pf=s["pf"],
                pos_years=s["pos_years"], total_years=s["total_years"],
                worst_year=s["worst_year"], med_mae=s["med_mae"], avg_hold=s["avg_hold"],
                lpb=(s["median"] - ctrl_med[H]) / H,
                n_days=int(len(e)),
                day_med=float(np.median(e)) if len(e) else None,
                day_win=float((e > 0).mean() * 100) if len(e) else None,
                top2_share=float(top2 / pos * 100) if pos > 0 else None)
        h5, h60 = rec["H"].get(5, {}), rec["H"].get(60, {})
        cls = "n/a"
        if h5.get("n") and h60.get("n") and h5.get("day_med") is not None:
            front = h5["lpb"] >= 2 * h60["lpb"] if h60["lpb"] > 0 else h5["lpb"] > 0
            if h5["day_med"] > 0 and h5["day_win"] > 50 and front:
                cls = "EVENT"
            elif h5["day_med"] <= 0 and h60["day_med"] <= 0:
                cls = "NULL"
            elif h60["day_med"] > 0 and not front:
                cls = "DRIFT"
            else:
                cls = "MIXED"
            if h60["n_days"] < 80:
                cls += " ·THIN"
        rec["class"] = cls
        rows.append(rec)
        if h60.get("n"):
            print(f"  {label:18s} {cls:12s} H60 trade {h60['trade_med']:+6.2f}/{h60['trade_win']:4.1f}% "
                  f"{h60['pos_years']}/{h60['total_years']}  |  day {h60['day_med']:+6.2f}/"
                  f"{h60['day_win']:4.1f}% n_days={h60['n_days']:>4}  |  lpb5 {h5['lpb']:+.3f} "
                  f"lpb60 {h60['lpb']:+.3f}  ({time.time()-t0:.0f}s)", flush=True)
        else:
            print(f"  {label:18s} n=0", flush=True)

    json.dump(dict(as_of=as_of, months=MONTHS, horizons=HOR, ctrl_trade_med=ctrl_med,
                   control=dict(version="CONTROL_V2_DEPHASED", stride=40, px_min=21,
                                offset="sha256(ticker)[:8] % 40", min_ctrl=MIN_CTRL,
                                coverage=ctrl_cov, phase_check=phase_check,
                                supersedes="research_audit_PHASE0_2026-09-03.json"),
                   rows=rows, run_id=time.strftime("AUDIT_%Y%m%dT%H%M%SZ", time.gmtime())),
              open(p, "w"), indent=1, default=str)
    print(f"\nwritten -> {p} ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
