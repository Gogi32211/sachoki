"""Validation of NakedStudy: re-derive today's chain result down a completely separate path.

The hand-rolled run (seq_chain_naked.py) went through edge_replay's frame — its filtered
universe, its renamed columns, its ticker set. This one loads bars straight from DuckDB,
reads the raw t_sig / z_sig tokens, and never imports edge_replay at all. If the two agree,
the conclusion is a property of the market rather than of one code path; if they disagree,
one of the two paths has a bug worth finding.

The chain is the user's spec, unchanged: (T10|T11|Z11) → Z2G → T1G, all inside one 14-bar
window, strict order, gaps allowed, entry on the bar after the T1G that completes it.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from naked_study import NakedStudy

W = 14
HEADS = ("T10", "T11", "Z11")


def tokens(s_t, s_z) -> np.ndarray:
    t = pd.Series(s_t).fillna("").astype(str).to_numpy()
    z = pd.Series(s_z).fillna("").astype(str).to_numpy()
    out = np.where((t != "") & (t != "nan"), t, z)
    return np.where(out == "nan", "", out)


def chain_flags(tok: np.ndarray):
    """Tightest valid (head, Z2G) pair before each T1G. Prior bars only."""
    n = len(tok)
    is_t1g, is_mid, is_head = tok == "T1G", tok == "Z2G", np.isin(tok, HEADS)
    ch = np.zeros(n, bool)
    hd = np.full(n, "", object)
    g1 = np.zeros(n, np.int16)
    g2 = np.zeros(n, np.int16)
    for i in np.where(is_t1g)[0]:
        lo = max(0, i - (W - 1))
        for j in (np.where(is_mid[lo:i])[0] + lo)[::-1]:
            hs = np.where(is_head[lo:j])[0] + lo
            if len(hs):
                k = hs[-1]
                ch[i], hd[i], g1[i], g2[i] = True, tok[k], j - k, i - j
                break
    return ch, hd, g1, g2


st = NakedStudy("chain (T10|T11|Z11)→Z2G→T1G — measured with the book unplugged",
                n_trials=32, columns=("t_sig", "z_sig"))   # 2 signals + 27 cells + slack

df = st.df
print("\n  building chain flags from raw t_sig/z_sig ...", flush=True)
ch = np.zeros(len(df), bool)
hd = np.full(len(df), "", object)
g1 = np.zeros(len(df), np.int16)
g2 = np.zeros(len(df), np.int16)
start = 0
tk = df["ticker"].to_numpy()
tok_all = tokens(df["t_sig"], df["z_sig"])
bounds = np.r_[0, np.where(tk[1:] != tk[:-1])[0] + 1, len(df)]
for a, b in zip(bounds[:-1], bounds[1:]):
    c_, h_, x_, y_ = chain_flags(tok_all[a:b])
    ch[a:b], hd[a:b], g1[a:b], g2[a:b] = c_, h_, x_, y_
df["hd"], df["g1"], df["g2"] = hd, g1, g2
print(f"  {ch.sum():,} chain completions on {df.ticker[ch].nunique():,} tickers", flush=True)

st.population()
res = st.signal("chain(14)", ch)
st.signal("  head = T11 only", ch & (df.hd == "T11"))


def band(v):
    return np.where(v <= 2, "≤2", np.where(v <= 5, "3-5", "≥6"))


df["b1"], df["b2"] = band(df.g1.to_numpy()), band(df.g2.to_numpy())
cells = {f"{h} g1{a} g2{b}": ch & (df.hd == h) & (df.b1 == a) & (df.b2 == b)
         for h in HEADS for a in ("≤2", "3-5", "≥6") for b in ("≤2", "3-5", "≥6")}
st.enumerate_cells("27 CELLS (head × g1 × g2)", cells, N=20)

st.verdict(res, "chain(14)", N=20, family="seq_chain_t10z2g_t1g")
print("\nDONE", flush=True)
