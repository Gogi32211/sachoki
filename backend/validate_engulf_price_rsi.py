"""
validate_engulf_price_rsi.py — layer the fib PRICE-ZONE law + RSI onto the engulf-reversal
edge (ANY bull-T, all universes, engulf-3 range). Which price zone × RSI band sharpens it?
Reports mean/med/win/PF + per-year, TRAIN(2021-23) vs TEST(2024-26) + 2022. READ-ONLY.
"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
import validate_t6_engulf as V
from edge_replay import _pathsim, _stats

V.SIG = "ANY"
KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
ZONES = [(5, 8, "$5-8 casino"), (8, 21, "$8-21 dead"), (21, 89, "$21-89 quality"),
         (89, 377, "$89-377"), (377, 1e9, ">$377 mega")]
RSIB = [(0, 30, "RSI<30"), (30, 40, "RSI30-40"), (40, 50, "RSI40-50"),
        (50, 60, "RSI50-60"), (60, 200, "RSI60+")]


def _line(lbl, m, df):
    s = _stats("x", _pathsim(V._grp(df, m), "_m", **KW))
    if not s or s.get("n", 0) == 0:
        return f"  {lbl:18s} n=0"
    py = s["per_year"]
    tr = [py[y] for y in TR if y in py]; te = [py[y] for y in TE if y in py]
    tr = sum(tr) / len(tr) if tr else float("nan"); te = sum(te) / len(te) if te else float("nan")
    return (f"  {lbl:18s} n={s['n']:>5} mean{s['mean']:+5.2f} med{s['median']:+5.2f} win{s['win']:4.1f} "
            f"pf{str(s['pf']):>4} | TR{tr:+5.2f} TE{te:+5.2f} '22{py.get('2022',float('nan')):+5.2f}")


def run():
    print("pulling…", flush=True)
    df, as_of = V._pull(V.MONTHS, V.DVF)
    df = V._prep(df)
    eng = df["t6"] & df["clean"] & df["engR3"]          # ANY bull-T, engulf-3 range, all univ
    print(f"as_of {as_of} · engulf-3 base n={int(eng.sum())} · trail25/60\n")

    print("── by PRICE ZONE ──")
    print(_line("ALL zones", eng, df))
    for lo, hi, lbl in ZONES:
        print(_line(lbl, eng & df["close"].between(lo, hi), df))

    print("\n── by RSI band ──")
    for lo, hi, lbl in RSIB:
        print(_line(lbl, eng & df["rsi_14"].between(lo, hi), df))

    print("\n── PRICE $21-89 × RSI (the quality zone, refined) ──")
    q = eng & df["close"].between(21, 89)
    print(_line("$21-89 all", q, df))
    for lo, hi, lbl in RSIB:
        print(_line("$21-89 " + lbl, q & df["rsi_14"].between(lo, hi), df))

    print("\n── candidate final cells ──")
    print(_line("$21-89 RSI<45", eng & df["close"].between(21, 89) & (df["rsi_14"] < 45), df))
    print(_line("$21-89 RSI<40", eng & df["close"].between(21, 89) & (df["rsi_14"] < 40), df))
    print(_line("≥$21 RSI<45", eng & (df["close"] >= 21) & (df["rsi_14"] < 45), df))
    print(f"\nas_of {as_of}")


if __name__ == "__main__":
    run()
