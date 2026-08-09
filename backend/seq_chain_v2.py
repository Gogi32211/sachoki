"""The (T10|T11|Z11) → Z2G → T1G chain, re-run through the guard framework — old vs new.

The OLD study (seq_t10_z2g_t1g.py, 2026-08-09 morning) ran each mask as its OWN path-sim,
compared cells against the global 10th-bar baseline, printed point medians with no
intervals, and the decisive control ("the gates without the chain") was only added after
the user asked for it. Verdict then: NULL — the chain added +0.55pp over RS+RSI<45 alone,
below the +1pp bar.

This re-run asks the same question through research_kit, where the guards are mechanical:

  · ONE simulation per universe — baseline and every cell are the same trades, so the
    5-bar spacing interacts identically with every mask (in the old run each mask got its
    own spacing, so the n's were not strictly comparable)
  · day-clustered intervals and n_eff on every line
  · controls GENERATED from the components — which adds a question the old study never
    asked: the auto-control's FULL combination is the UNORDERED "all three in the window",
    so (ordered chain) − (unordered AND) isolates what the ORDERING itself is worth
  · the gated question gets its own MATCHED universe (RS & RSI<45), not a sampled proxy
  · plateau over the window with the peak/neighbour ratio printed
  · verdicts auto-logged to the research ledger

Old numbers are embedded at the end for the side-by-side.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from research_kit import EdgeStudy          # noqa: E402

WINDOWS = (8, 10, 14, 20, 30)
HEADS = ("T10", "T11", "Z11")


def token_arr(g) -> np.ndarray:
    t = g["t"].astype(str).to_numpy()
    z = g["z"].astype(str).to_numpy()
    out = np.where((t != "") & (t != "nan") & (t != "None"), t, z)
    return np.where((out == "nan") | (out == "None"), "", out)


def chain_cols(g) -> dict:
    """Per-bar flags, all computed on PRIOR bars only (nothing looks ahead)."""
    tok = token_arr(g)
    n = len(tok)
    is_t1g = tok == "T1G"
    is_mid = tok == "Z2G"
    is_head = np.isin(tok, HEADS)
    cols: dict[str, np.ndarray] = {"t1g": is_t1g}
    for w in WINDOWS:
        ordered = np.zeros(n, bool)
        had_mid = np.zeros(n, bool)
        had_head = np.zeros(n, bool)
        for i in np.where(is_t1g)[0]:
            lo = max(0, i - (w - 1))
            hm = is_mid[lo:i].any()
            had_mid[i], had_head[i] = hm, is_head[lo:i].any()
            if hm:
                for j in np.where(is_mid[lo:i])[0] + lo:
                    if is_head[lo:j].any():
                        ordered[i] = True
                        break
        cols[f"ch{w}"] = ordered
        if w == 14:
            cols["mid14"], cols["head14"] = had_mid, had_head
    for name, hset in (("h_t10", ("T10",)), ("h_t11", ("T11",)), ("h_z11", ("Z11",))):
        ish = np.isin(tok, hset)
        flag = np.zeros(n, bool)
        for i in np.where(is_t1g)[0]:
            lo = max(0, i - 13)
            for j in np.where(is_mid[lo:i])[0] + lo:
                if ish[lo:j].any():
                    flag[i] = True
                    break
        cols[name] = flag
    return cols


EXTRA = ("t1g", "ch8", "ch10", "ch14", "ch20", "ch30", "mid14", "head14",
         "h_t10", "h_t11", "h_z11")


def B(t, c):
    return t[f"sig_{c}"].fillna(False).astype(bool)


# ══ STUDY 1 · the raw chain against the whole population ══════════════════════
# n_trials: registered 8 on the first run and the guard refused at cell 10 — the seven
# auto-controls alone had eaten the budget. Re-registered at the TRUE count (2 cells +
# 7 controls + 3 head splits + 5 plateau points + 4 buckets + 4 concentration ≈ 25, held
# at 32), which is the honest move even though a larger k weakens every DSR downstream.
st = EdgeStudy("chain (T10|T11|Z11)→Z2G→T1G — does it beat its own parts?",
               n_trials=32, extra_sig_cols=EXTRA)
print("computing chain columns (prior-bars only)...", flush=True)
for tk, g in st.grp.items():
    for k, v in chain_cols(g).items():
        g[k] = v
print("done\n", flush=True)

st.universe("ყველა ბარი (მთელი პოპულაცია)", lambda g: pd.Series(True, index=g.index))
st.describe("rsi_14")

st.cell("T1G token", lambda t: B(t, "t1g"))
chain_cell = st.cell("★ ORDERED chain(14)", lambda t: B(t, "ch14"))

# the auto-control's FULL combination is the UNORDERED AND — so ordered − full
# isolates the value of the ORDERING itself, a question the old study never asked
st.controls({
    "T1G ending": lambda t: B(t, "t1g"),
    "Z2G in window": lambda t: B(t, "mid14"),
    "head in window": lambda t: B(t, "head14"),
})

print("\n  head split (the three tokens are NOT the same bar):", flush=True)
st.cell("  head = T10", lambda t: B(t, "h_t10"))
st.cell("  head = T11", lambda t: B(t, "h_t11"))
st.cell("  head = Z11", lambda t: B(t, "h_z11"))

st.plateau("window (bars)", lambda t, w: B(t, f"ch{w}"), list(WINDOWS))
st.buckets("ORDERED chain(14)", lambda t: B(t, "ch14"))
st.concentration("ORDERED chain(14)", lambda t: B(t, "ch14"))
st.verdict(chain_cell, mask_fn=lambda t: B(t, "ch14"), family="seq_chain_t10z2g_t1g")

# ══ STUDY 2 · the decisive question, on its own MATCHED universe ══════════════
st2 = EdgeStudy("does the chain add anything over 🏆RS & RSI<45 alone?",
                n_trials=8, extra_sig_cols=EXTRA)     # frame + columns cached in-process
st2.universe("🏆RS-intact & RSI<45",
             lambda g: g["rs_intact"].fillna(False).astype(bool) & (g["rsi_14"] < 45))
st2.cell("T1G token", lambda t: B(t, "t1g"))
g14 = st2.cell("★ ORDERED chain(14)", lambda t: B(t, "ch14"))
st2.cell("ORDERED chain(10)", lambda t: B(t, "ch10"))
st2.verdict(g14, mask_fn=lambda t: B(t, "ch14"), family="seq_chain_t10z2g_t1g")

# ══ the side-by-side ══════════════════════════════════════════════════════════
OLD = """
────────────────────────────────────────────────────────────────────────────────────────
ძველი ანალიზი (seq_t10_z2g_t1g.py) — ცალ-ცალკე სიმულაციები, ინტერვალების გარეშე:
  BASELINE (ყოველი მე-10 ბარი)   n=289,648   med +0.09        4/6yr  worst −3.70
  T1G მარტო                      n= 95,516   med +0.37
  Z2G→T1G                        n= 73,230   med +0.53
  head→T1G (Z2G-ის გარეშე)       n= 43,775   med +0.52
  ★ სრული ჯაჭვი(14)              n= 22,179   med +0.76        4/6yr  worst −2.06
  ფანჯრები: 8:+0.95 10:+0.95 14:+0.76 20:+0.74 30:+0.69   (მონოტონური კლება)
  თავები:   T10 +0.71 · T11 +1.41 · Z11 +0.83
გეიტებით (🏆RS & RSI<45) — კონტროლი პოსტ-ფაქტუმ დაემატა:
  ნებისმიერი ბარი + გეიტები      n= 14,280   med +3.39        5/5yr  worst +1.51
  T1G + გეიტები                  n=  2,536   med +3.61        5/5yr  worst +2.54
  ★ ჯაჭვი(14) + გეიტები          n=    840   med +3.94        5/5yr  worst +1.22  DSR 0.046
ვერდიქტი: NULL — ჯაჭვი გეიტებს +0.55pp-ს ამატებს (ზღვარი +1pp), n ×17-ჯერ იჭრება
────────────────────────────────────────────────────────────────────────────────────────
"""
print(OLD, flush=True)
print("DONE", flush=True)
