"""Z11-T11 × engulf — does combining the top setup with the engulf-reversal help?
(A) Engulf-Abs whose ABSORBED edge is Z11-T11 (Z11-T11 in the swept 2 bars).
(B) Z11-T11 whose OWN bar range-engulfs the prior 2 bars (+ edge-in-2 / qual variants).
Plus a few other top setups × engulf for context. path-sim trail25/60, 62mo. READ-ONLY."""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
from edge_replay import _pathsim, _stats

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]


def _grp(df, m):
    d = df.copy(); d["_m"] = m.values if hasattr(m, "values") else m
    return {tk: g.reset_index(drop=True) for tk, g in d.groupby("ticker", sort=False)}


def _line(lbl, m, df):
    s = _stats("x", _pathsim(_grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:30s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr)/len(tr) if tr else float("nan"); te = sum(te)/len(te) if te else float("nan")
    return (f"  {lbl:30s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = ER._pull(62, 3_000_000)
    df = ER._prep(df)
    g = df.groupby("ticker", sort=False)
    h2 = g["high"].transform(lambda s: s.shift(1).rolling(2).max())
    l2 = g["low"].transform(lambda s: s.shift(1).rolling(2).min())
    df["engR2"] = (df["high"] >= h2) & (df["low"] <= l2)
    df["qual"] = (df["close"] >= 21) & (df["rsi_14"] < 45)

    def in2(col):
        c = df[col].astype(float).groupby(df["ticker"])
        return (c.shift(1).fillna(0) + c.shift(2).fillna(0)) > 0

    print(f"as_of {as_of} · trail25/60\n")
    print("── (A) Engulf-Abs whose ABSORBED edge = Z11-T11 ──")
    anybt = df["t"].str.match(r"^T\d").fillna(False) & df["clean"]
    print(_line("engulf-abs (any edge)", df["E_engulfabs"], df))
    print(_line("engulf2·qual · absorbs Z11-T11", anybt & df["engR2"] & df["qual"] & in2("E_z11t11"), df))

    print("\n── (B) Z11-T11 whose OWN bar engulfs 2 bars ──")
    z = df["E_z11t11"]
    print(_line("Z11-T11 base", z, df))
    print(_line("Z11-T11 & engulf-2", z & df["engR2"], df))
    print(_line("Z11-T11 & engulf-2 & qual", z & df["engR2"] & df["qual"], df))
    print(_line("Z11-T11 & engulf-2 & edge-in-2", z & df["engR2"] & in2("E_washout"), df))

    print("\n── context: other top setups whose OWN bar engulfs 2 ──")
    for name, col in [("L43-TRIPLE", "E_l43triple"), ("Atomic-R", "E_atomicR"), ("G3", "E_g3")]:
        print(_line(f"{name} & engulf-2", df[col] & df["engR2"], df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
