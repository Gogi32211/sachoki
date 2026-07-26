# TZ + WLNBB + WICK — 5-Year Research (full replication of v260506)

Source: studio_analytics.duckdb · 2021-05-26 — 2026-06-09 · 8,073,913 analyzable bars · SP500 + NASDAQ + Russell2k.
Replicates EVERY section of the old per-signal reports (A Executive+Status, C baseline+MFE/MAE/Reward-Risk,
D composites+Status, E top+reject sequences, F prev1, G L+volume, H suffix+A/I/O, I MA50-reclaim+price-bucket)
over the FULL 5-year history, adding per-year regime stability + A/I/O subdivision.

Definitions: med/avg at 1/3/5/10/20d (%). win%=P(fwd10>0). big_win=P(fwd10>=10%). fail=P(fwd10<=-5%).
RR=avg(MFE)/|avg(MAE)|. composite=signal+Line5+suffix. sequence=[bar-3|bar-2|bar-1]→signal.
Status: GOOD(med>=0.7 & fail<=20) / AVERAGE / REJECT(med<=-0.1 or fail>=28).
Regime: STABLE(+ in >=60% years) / 2025-ARTIFACT(+ in <=40%) / MIXED.
