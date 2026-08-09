"""User's sequence: (T10|T11|Z11) → Z2G → T1G, all inside a 14-bar window. (2026-08-09)

Order matters, gaps are allowed: other bars may sit between the three elements, they just all
have to land inside the same window. Entry is the T1G bar (the sequence completes there, so
nothing looks ahead).

CONTROLS ARE THE POINT. Six legacy toolbar sequences were deleted on 2026-08-04 because they
were never tested against their own parts — two of their gates turned out to run BACKWARDS.
So every stage is measured alone and in pairs:
    T1G alone · Z2G→T1G · head→T1G (no Z2G) · the full chain
If the full chain does not beat its own pieces, the extra conditions are decoration.

The head is also SPLIT (T10 / T11 / Z11 separately). Pooling three tokens because they feel
similar is exactly how a dead component hides inside a live one — T10/T11 are bullish inside/
lower-open bars, Z11 is a bearish bar, and there is no reason to assume they behave alike.

WINDOW PLATEAU: 10 / 14 / 20 bars. If only 14 works, that is a fitted number, not a rule.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

N_TRIALS = 16
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp)} tickers", flush=True)

HEADS = {"all": ("T10", "T11", "Z11"), "T10": ("T10",), "T11": ("T11",), "Z11": ("Z11",)}


def token_arr(g):
    """one token per bar: the priority engine fills either t or z, never both."""
    t = g["t"].astype(str).to_numpy()
    z = g["z"].astype(str).to_numpy()
    out = np.where((t != "") & (t != "nan") & (t != "None"), t, z)
    return np.where((out == "nan") | (out == "None"), "", out)


def build(win, heads, need_mid=True, need_head=True):
    """mask on the T1G bar when the ordered chain completes inside `win` bars."""
    total = 0
    for tk, g in grp.items():
        tok = token_arr(g)
        n = len(tok)
        m = np.zeros(n, bool)
        is_t1g = tok == "T1G"
        is_mid = tok == "Z2G"
        is_head = np.isin(tok, heads)
        for i in np.where(is_t1g)[0]:
            lo = max(0, i - (win - 1))
            if not need_mid and not need_head:
                m[i] = True; continue
            if not need_mid:                       # head → T1G, no Z2G required
                m[i] = is_head[lo:i].any(); continue
            js = np.where(is_mid[lo:i])[0] + lo    # Z2G bars inside the window, before i
            if len(js) == 0:
                continue
            if not need_head:
                m[i] = True; continue
            for j in js:                           # need a head strictly BEFORE that Z2G
                if is_head[lo:j].any():
                    m[i] = True; break
        g["_S"] = m
        total += int(m.sum())
    return total


fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"board family: {len(fam)} setups\n", flush=True)

HDR = (f"  {'variant':44s} {'n':>6s} {'med':>7s} {'win':>5s} {'pf':>5s} "
       f"{'21':>6s}{'22':>6s}{'23':>6s}{'24':>6s}{'25':>6s}{'26':>6s} {'yrs':>4s} "
       f"{'worst':>7s} {'DSR':>6s}")


def score(label, extra=None):
    if extra is not None:
        for tk, g in grp.items():
            g["_S2"] = g["_S"] & extra(g)
        col = "_S2"
    else:
        col = "_S"
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 60:
        print(f"  {label:44s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
    ys = "".join(f"{ym.get(str(y), float('nan')):>6.1f}" for y in range(2021, 2027))
    med = tr["ret"].median() * 100
    print(f"  {label:44s} {len(tr):>6d} {med:>+7.2f} {w.mean()*100:>5.1f} {pf:>5.2f} {ys} "
          f"{int((ym>0).sum()):>2d}/{len(ym)} {ym.min():>+7.2f} {d['dsr']:>6.3f}", flush=True)
    return dict(med=med, worst=ym.min(), yrs=int((ym > 0).sum()), n=len(tr), dsr=d["dsr"])


# ── 0. baseline + the plain T1G population ────────────────────────────────────
print("===== 0. what we are trying to beat =====", flush=True)
print(HDR, flush=True)
for tk, g in grp.items():
    g["_S"] = np.arange(len(g)) % 10 == 0
score("BASELINE (10th bar)")
for tk, g in grp.items():
    g["_S"] = token_arr(g) == "T1G"
n_t1g = sum(int(g["_S"].sum()) for g in grp.values())
score(f"T1G alone ({n_t1g:,} bars)")

# ── 1. THE CHAIN, and every piece of it ───────────────────────────────────────
print("\n===== 1. the chain vs its own parts (window 14) =====", flush=True)
print(HDR, flush=True)
n = build(14, HEADS["all"], need_mid=True, need_head=False)
score(f"Z2G → T1G          ({n:,} fires)")
n = build(14, HEADS["all"], need_mid=False, need_head=True)
score(f"head → T1G, no Z2G ({n:,} fires)")
n = build(14, HEADS["all"], need_mid=True, need_head=True)
score(f"★ FULL: head → Z2G → T1G ({n:,} fires)")

# ── 2. split the head — do the three tokens behave alike? ─────────────────────
print("\n===== 2. the head split (T10 / T11 / Z11 are NOT the same bar) =====", flush=True)
print(HDR, flush=True)
for h in ("T10", "T11", "Z11"):
    n = build(14, HEADS[h], True, True)
    score(f"  head = {h}  ({n:,} fires)")

# ── 3. window plateau ─────────────────────────────────────────────────────────
print("\n===== 3. window plateau — is 14 special, or arbitrary? =====", flush=True)
print(HDR, flush=True)
for w in (8, 10, 14, 20, 30):
    n = build(w, HEADS["all"], True, True)
    score(f"  window {w:>2d} bars ({n:,} fires)")

# ── 4. the gates the book already trusts ──────────────────────────────────────
print("\n===== 4. the full chain + our validated gates =====", flush=True)
print(HDR, flush=True)
build(14, HEADS["all"], True, True)
score("  + 🏆RS", lambda g: g["rs_intact"])
score("  + RSI<45", lambda g: g["rsi_14"] < 45)
score("  + 🏆RS + RSI<45", lambda g: g["rs_intact"] & (g["rsi_14"] < 45))
score("  + ❄️CONSO", lambda g: g["conso"])
print("\n  price buckets:", flush=True)
for lo, hi in [(5, 21), (21, 89), (89, 377)]:
    score(f"  ${lo}-{hi}", lambda g, a=lo, b=hi: g["close"].between(a, b))

# ── 5. overlap ────────────────────────────────────────────────────────────────
print("\n===== 5. overlap with the board =====", flush=True)
build(14, HEADS["all"], True, True)
tot = sum(int(g["_S"].sum()) for g in grp.values())
hits = []
for name, ecol in er.SETUPS:
    inter = sum(int((g["_S"] & g[ecol].fillna(False)).sum()) for g in grp.values() if ecol in g)
    if inter and tot:
        hits.append((100.0 * inter / tot, name))
hits.sort(reverse=True)
print(f"  fires {tot:,} · " + (" · ".join(f"{nm} {p:.0f}%" for p, nm in hits[:8]) or "no overlap"),
      flush=True)

print("\nDONE", flush=True)
