"""
gen_buyseq_context.py — build buyseq_context.json: the BUY-signal × preceding-sequence
conditioner table (2026-07-20 redesign). Serves the live ⤴/⤵ context chips.

Source: the fires parquet produced by analysis/260720_buyseq_conditioner.py
(pass its path as argv[1]; rerun that script after major DB re-enrichment).
Kept cells: n>=80 (coarse) / >=50 (fine), TRAIN>=20 & TEST>=20, era-CONSISTENT
(TRAIN and TEST up% on the SAME side of the signal's baseline), |lift| >= 5pp.
"""
import json, sys
import pandas as pd

SRC = sys.argv[1] if len(sys.argv) > 1 else "buyseq_fires.parquet"
OUT = "buyseq_context.json"
SIGS = ["rev", "brk", "h4", "h1", "lh", "ea", "ep", "fly", "flyf", "score", "turn", "anyb"]

# ── ERA MODE (2026-07-20f, user decision): serve the RECENT REGIME only ──────────
# The breakout-continuation grammar flipped sign around 2024 (2021-23 negative,
# 2024-26 positive — verified on the RGTI cells); the user chose to base the ctx
# layer on 2024-2026. Robustness gate becomes PER-YEAR consistency inside the era:
# every year with n>=15 must sit on the SAME side of the signal's baseline, and at
# least 2 of the 3 years must have n>=15. Set ERA_YEARS=None for the old full-era
# TRAIN/TEST mode.
ERA_YEARS = ("2024", "2025", "2026")

R = pd.read_parquet(SRC)
if ERA_YEARS:
    R = R[R.yr.isin(ERA_YEARS)]
out = {}
for col in SIGS:
    S = R[R[col]]
    if len(S) < 200:
        continue
    u0 = 100 * (S.r20 > 0).mean()
    m0 = 100 * S.r20.mean()
    rules = {}
    # anyb pools millions of bars — stricter n so cell count doesn't explode multi-testing
    _mul = 2.5 if col == "anyb" else 1
    if ERA_YEARS:
        _mul *= 0.5   # ~half the data in the 3-year era
    _cols = [("c", 80), ("f", 50), ("sx", 80), ("bw", 80), ("gr", 80), ("q5", 80), ("vb", 80)]
    for _lay, _base_n in _cols:
      for _d in (2, 3, 4):
        pc, minn = f"{_lay}{_d}", int(_base_n * _mul)
        for seq, sg in S.groupby(pc):
            if len(sg) < minn or "nan" in seq:
                continue
            u = 100 * (sg.r20 > 0).mean()
            _lift_up = u > u0
            _ok_yrs = 0; _bad = False
            for _y, _g in sg.groupby("yr"):
                if len(_g) < 15:
                    continue
                _uy = 100 * (_g.r20 > 0).mean()
                if (_uy > u0) == _lift_up:
                    _ok_yrs += 1
                else:
                    _bad = True
            if _bad or _ok_yrs < 2:
                continue
            lift = u - u0
            if abs(lift) < 5:
                continue
            if set(seq.split()) == {"-"}:
                continue
            _mean = 100 * sg.r20.mean()
            _dir = "up" if lift > 0 else "down"
            # 🎲 TAIL context (2026-07-20d, RGTI lesson): up% is BELOW baseline but the
            # MEAN is at/above it — rare continuation, fat when it comes (parabola fuel).
            # Distinguish from true chop-death ⤵ so the chip doesn't read as "run away".
            _kind = "tail" if (_dir == "down" and _mean >= max(m0, 1.0)) else _dir
            rules[f"{_lay}|{seq}"] = {"up": round(u, 1), "med": round(100 * sg.r20.median(), 2),
                          "mean": round(_mean, 2), "n": int(len(sg)), "lift": round(lift, 1),
                          "dir": _dir, "kind": _kind}
    out[col] = {"base_up": round(u0, 1), "base_mean": round(m0, 2), "n": int(len(S)), "rules": rules}
    _nt = sum(1 for r in rules.values() if r.get("kind") == "tail")
    print(f"   tail cells: {_nt}")
    print(f"{col}: base {u0:.1f}% · kept rules {len(rules)}")
json.dump({"as_of": "2026-07-20", "spec": "prefix ends 1 bar BEFORE the signal bar; "
           "coarse=T/Z, fine=T/Z+L; fwd-20 up%", "signals": out}, open(OUT, "w"))
print("→", OUT)
