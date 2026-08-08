"""L3 verification of the one descriptor cell that passed L1+L2:
🪨 T1G-NB + L34 anywhere in the 3-bar prefix.

Checks: price buckets, DSR over the honest 112-cell trial family, overlap with the
existing L34-continuity edges (they share the L34 idea — must be shown disjoint or
the "new" edge is just a relabel), and the complement control.
"""
import numpy as np
import pandas as pd
import edge_replay as er
import overfit_stats as ofs

grp, as_of = er._frame(60, 3_000_000)
print(f"frame {len(grp)} as_of {as_of}\n", flush=True)

REL = ["E_l34cont", "E_l34cont_rs", "E_t1gnb_rs"]     # the family it must be disjoint from

for tk, g in grp.items():
    l = g["l"].fillna("")
    pre34 = (l.shift(1).eq("L34") | l.shift(2).eq("L34") | l.shift(3).eq("L34"))
    g["S_new"] = g["E_t1gnb_rs"] & pre34
    g["S_cmp"] = g["E_t1gnb_rs"] & ~pre34


def stats(tr):
    yr = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    return dict(n=len(tr), med=tr["ret"].median() * 100, win=w.mean() * 100,
                pf=(tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
                yr={int(k): round(v, 1) for k, v in yr.items()},
                pos=int((yr > 0).sum()), ny=len(yr), worst=float(yr.min()),
                sr=ofs.sharpe(tr["ret"].to_numpy()))


def show(lab, tr):
    if len(tr) == 0:
        print(f"  {lab:30s} n=0"); return None
    s = stats(tr)
    ys = "".join(f"{s['yr'].get(y, float('nan')):>7.1f}" for y in range(2021, 2027))
    print(f"  {lab:30s} {s['n']:>5d} {s['med']:>+7.2f} {s['win']:>5.1f} {s['pf']:>6.2f}"
          f" {ys}  {s['pos']}/{s['ny']} {s['worst']:>+6.1f}", flush=True)
    return s


HDR = (f"  {'cell':30s} {'n':>5s} {'med':>7s} {'win':>5s} {'pf':>6s} "
       f"{'2021':>7s}{'2022':>7s}{'2023':>7s}{'2024':>7s}{'2025':>7s}{'2026':>7s}  pos  worst")

print("===== 1. the cell vs its complement =====\n" + HDR, flush=True)
tr_new = er._pathsim(grp, "S_new", "trail", 0.10, 0.25, 0.25, 60)
tr_cmp = er._pathsim(grp, "S_cmp", "trail", 0.10, 0.25, 0.25, 60)
s_new = show("T1G-NB + L34 in prefix", tr_new)
s_cmp = show("T1G-NB, NO L34 in prefix", tr_cmp)
print(f"    Δ = {s_new['med'] - s_cmp['med']:+.2f}\n", flush=True)

print("===== 2. price buckets =====\n" + HDR, flush=True)
for lo, hi in [(21, 40), (40, 89), (21, 89)]:
    for tk, g in grp.items():
        g["_B"] = g["S_new"] & g["close"].between(lo, hi)
    show(f"${lo}-{hi}", er._pathsim(grp, "_B", "trail", 0.10, 0.25, 0.25, 60))

print("\n===== 3. overlap with the L34-continuity family =====", flush=True)
tot = 0
inter = {c: 0 for c in REL}
for g in grp.values():
    m = g["S_new"]
    tot += int(m.sum())
    for c in REL:
        if c in g:
            inter[c] += int((m & g[c].fillna(False)).sum())
for c in REL:
    print(f"  vs {c:16s} {100.0*inter[c]/tot:6.1f}%  of the new cell's fires", flush=True)

print("\n===== 4. DSR over the honest trial family (112 descriptor cells) =====", flush=True)
# trial SRs: the parent's own descriptor cells are the family that was searched
srs = []
for tk, g in grp.items():
    pass
# rebuild the 112-cell SR distribution cheaply from the parent trades + descriptors
desc = []
for tk, g in grp.items():
    l = g["l"].fillna("")
    desc.append(pd.DataFrame({
        "ticker": tk,
        "date_in": pd.to_datetime(g["date"]).shift(-1).astype(str).str[:10],
        "entry_l": l, "entry_sfx": g["fsfx"].fillna(""),
        "pre_l34": (l.shift(1).eq("L34") | l.shift(2).eq("L34") | l.shift(3).eq("L34")),
        "pre_l46": (l.shift(1).eq("L46") | l.shift(2).eq("L46") | l.shift(3).eq("L46")),
    }))
DESC = pd.concat(desc, ignore_index=True)
DESC = DESC[DESC["date_in"].notna() & (DESC["date_in"] != "NaT")]

for col in ["E_z1gcrown", "E_seq20", "E_z9hl", "E_z1gt4", "E_z1gt36",
            "E_t2gsand_rs", "E_t1gnb_rs"]:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
    if len(tr) == 0:
        continue
    tr["date_in"] = pd.to_datetime(tr["date_in"]).astype(str).str[:10]
    tr = tr.merge(DESC, on=["ticker", "date_in"], how="left")
    for key in ["entry_l", "entry_sfx"]:
        for _, sub in tr.groupby(tr[key].fillna("")):
            if len(sub) >= 15:
                srs.append(ofs.sharpe(sub["ret"].to_numpy()))
    for key in ["pre_l34", "pre_l46"]:
        for val in (True, False):
            sub = tr[tr[key] == val]
            if len(sub) >= 15:
                srs.append(ofs.sharpe(sub["ret"].to_numpy()))

d = ofs.dsr(tr_new["ret"].to_numpy(), srs)
print(f"  trials in family: {len(srs)}", flush=True)
print(f"  cell SR {d.get('sr'):.4f}   sr* {d.get('sr_star'):.4f}   DSR {d.get('dsr'):.3f}", flush=True)
print("\nDONE", flush=True)
