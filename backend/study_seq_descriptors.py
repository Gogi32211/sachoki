"""Full-descriptor slice of the 5 sequence edges (user-approved plan, 2026-08-04).

The sequences were mined on t/z codes ALONE — violating the standing rule that a TZ
sequence must never be judged by t/z alone. This slices every fire by the two ignored
layers: the entry bar's L-line, the entry bar's suffix, and whether L34/L46 appears in
the 3-bar prefix.

METHOD: one path-sim per edge (exact, gap-realistic), then the trades table is joined
back to the SIGNAL bar's descriptors via date_in (entry = signal+1) and sliced in pandas.
No re-simulation per cell → the cells are exactly the parent's trades, partitioned.

REPORTED BOTH WAYS: boosters AND suppressors. Thin cells are NAMED, never hidden.
Gates: L1 years>=4/6 + worst>=-2 -> L2 parent+1pp -> L3 n>=50 + DSR over the trial family.
"""
import numpy as np
import pandas as pd
import edge_replay as er

grp, as_of = er._frame(60, 3_000_000)
print(f"frame {len(grp)} tickers, as_of {as_of}", flush=True)

EDGES = [("👑 CROWN", "E_z1gcrown"), ("🧺 SEQ-20", "E_seq20"),
         ("🧲 Z9-HL", "E_z9hl"), ("🌉 Z1G→T4", "E_z1gt4"),
         ("🌉v2", "E_z1gt36"), ("🥪 SAND", "E_t2gsand_rs"), ("🪨 T1G-NB", "E_t1gnb_rs")]

# ── descriptor table keyed by (ticker, NEXT bar's date) so it joins onto date_in ──
desc = []
for tk, g in grp.items():
    l = g["l"].fillna("")
    d = pd.DataFrame({
        "ticker": tk,
        # the entry bar this signal produces; normalised to YYYY-MM-DD so it joins the
        # trades table (whose date_in comes from a raw object array, not datetime64)
        "date_in": pd.to_datetime(g["date"]).shift(-1).astype(str).str[:10],
        "entry_l": l,
        "entry_sfx": g["fsfx"].fillna(""),
        "pre_l34": (l.shift(1).eq("L34") | l.shift(2).eq("L34") | l.shift(3).eq("L34")),
        "pre_l46": (l.shift(1).eq("L46") | l.shift(2).eq("L46") | l.shift(3).eq("L46")),
        "red": (g["close"] < g["open"]),
    })
    desc.append(d)
DESC = pd.concat(desc, ignore_index=True)
DESC = DESC[DESC["date_in"].notna() & (DESC["date_in"] != "NaT")]
print(f"descriptor rows {len(DESC):,}", flush=True)


def stats(tr):
    if len(tr) == 0:
        return None
    yr = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    return {"n": len(tr), "med": tr["ret"].median() * 100, "win": w.mean() * 100,
            "pf": (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
            "yr": {int(k): round(v, 1) for k, v in yr.items()},
            "pos": int((yr > 0).sum()), "ny": len(yr), "worst": float(yr.min())}


def line(label, s, base=None, flag=""):
    if s is None or s["n"] == 0:
        print(f"    {label:26s}   n=0", flush=True); return
    ys = "".join(f"{s['yr'].get(y, float('nan')):>6.1f}" for y in range(2021, 2027))
    d = f"{s['med']-base['med']:>+7.2f}" if base else "       "
    thin = " ⚠thin" if s["n"] < 50 else ""
    print(f"    {label:26s} {s['n']:>5d} {s['med']:>+7.2f} {s['win']:>5.1f} {s['pf']:>5.2f}"
          f" {ys}  {s['pos']}/{s['ny']} {s['worst']:>+6.1f} {d}{thin}{flag}", flush=True)


HDR = (f"    {'cell':26s} {'n':>5s} {'med':>7s} {'win':>5s} {'pf':>5s} "
       f"{'2021':>6s}{'2022':>6s}{'2023':>6s}{'2024':>6s}{'2025':>6s}{'2026':>6s}"
       f"  {'pos':>4s} {'worst':>6s} {'Δbase':>7s}")

ALL = []          # pooled family trades
CELLS = 0         # honest trial count for DSR

for name, col in EDGES:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
    if len(tr) == 0:
        print(f"\n##### {name} — no trades", flush=True); continue
    tr["date_in"] = pd.to_datetime(tr["date_in"]).astype(str).str[:10]
    tr = tr.merge(DESC, on=["ticker", "date_in"], how="left")
    if tr["entry_l"].isna().mean() > 0.05:       # join must not silently drop fires
        raise SystemExit(f"{col}: {tr['entry_l'].isna().mean():.1%} of trades failed to join")
    tr["entry_l"] = tr["entry_l"].fillna("")
    tr["entry_sfx"] = tr["entry_sfx"].fillna("")
    if col.startswith("E_z1g") or col in ("E_seq20", "E_z9hl"):
        ALL.append(tr.assign(_edge=name))
    base = stats(tr)
    print(f"\n##### {name}  ({col})  — base n={base['n']} med{base['med']:+.2f} "
          f"{base['pos']}/{base['ny']}yr worst{base['worst']:+.1f}\n" + HDR, flush=True)

    print("  ── entry-bar L-line ──", flush=True)
    for v, sub in sorted(tr.groupby("entry_l"), key=lambda x: -len(x[1])):
        if len(sub) < 15:
            continue
        CELLS += 1
        line(v or "(none)", stats(sub), base)
    print("  ── entry-bar suffix ──", flush=True)
    for v, sub in sorted(tr.groupby("entry_sfx"), key=lambda x: -len(x[1])):
        if len(sub) < 15:
            continue
        CELLS += 1
        line(v or "(none)", stats(sub), base)
    print("  ── L34/L46 in the 3-bar prefix ──", flush=True)
    for key, lab in [("pre_l34", "L34 in prefix"), ("pre_l46", "L46 in prefix")]:
        for val in (True, False):
            sub = tr[tr[key] == val]
            if len(sub) < 15:
                continue
            CELLS += 1
            line(f"{lab} = {val}", stats(sub), base)

# ── pooled family (the only slice with real sample size) ─────────────────────
pool = pd.concat(ALL, ignore_index=True)
pbase = stats(pool)
print(f"\n\n##### POOLED 5-seq family — base n={pbase['n']} med{pbase['med']:+.2f} "
      f"{pbase['pos']}/{pbase['ny']}yr worst{pbase['worst']:+.1f}\n" + HDR, flush=True)
print("  ── entry-bar L-line ──", flush=True)
for v, sub in sorted(pool.groupby("entry_l"), key=lambda x: -len(x[1])):
    if len(sub) < 25:
        continue
    CELLS += 1
    line(v or "(none)", stats(sub), pbase)
print("  ── entry-bar suffix ──", flush=True)
for v, sub in sorted(pool.groupby("entry_sfx"), key=lambda x: -len(x[1])):
    if len(sub) < 25:
        continue
    CELLS += 1
    line(v or "(none)", stats(sub), pbase)
print("  ── prefix L-content + entry colour ──", flush=True)
for key, lab in [("pre_l34", "L34 in prefix"), ("pre_l46", "L46 in prefix"), ("red", "red entry bar")]:
    for val in (True, False):
        sub = pool[pool[key] == val]
        if len(sub) < 25:
            continue
        CELLS += 1
        line(f"{lab} = {val}", stats(sub), pbase)

print(f"\nhonest trial count (cells tested): {CELLS}", flush=True)
print("DONE", flush=True)
