"""CROSS_STAR_CONCURRENCE_V1 — do the two star families add anything TOGETHER?

PRE-OUTCOME FEATURE AUDIT ONLY. No outcome is opened here and none may be until this section is
complete and internally consistent (user's stop rule).

THE TWO FAMILIES, traced to their producers rather than to their display glyphs:

  TOP STAR — backend/lbal_build.py -> data/lbal_signals.parquet
      states(udn, udn_c, colour), lbal_build.py:157-164, on 15m L-code balance vs the daily candle
        ★   star      = (udn U ∧ candle RED) ∨ (udn D ∧ candle GREEN)      divergence
        ★★  conflict  = (udn D ∧ udn_c U) ∨ (udn U ∧ udn_c D)             conflict
        ★★★ half_up   = (udn U ∧ udn_c N) ∨ (udn N ∧ udn_c U)             half-up
      The UNSUFFIXED columns are PRIMARY (MODE "V" = volume-confirmed bars, the user's TradingView
      setting); `*_all` is the alternate mode kept for a toggle. udn IS NULL  <=>  unlabelled
      session: the producer states outright that "no data" is not "double neutral".

  BOTTOM STAR — backend/studio/bar_physics.py -> bars.phys_ad
      bar_physics.py:293, ONE mutually-exclusive categorical, not three flags:
        np.select([cluster, ad_fresh & absorbed, ad_fresh], ["★★", "★A", "★"], default="")
        ★   AD-FRESH            Z1G/Z2G within 12 bars -> T4/T6/T2G/T2, position < 0.50
        ★★  AD-CLUSTER          2+ AD-FRESH inside an 8-bar window
        ★A  AD-FRESH + absorption at the exhaustion bar (r_at_a > r_med * R_ABS_MULT)
      bar_physics declares NO LOOKAHEAD: every window backward-looking, every groupby per ticker.

  NOTE the same concept exists TWICE in `bars`: `phys_ad` is bar_physics' Python port, while
  `ad_fresh` / `ad_cluster` are the Pine originals imported by studio/importer.py:129. They are
  independent computations of one rule, so this audit measures their agreement rather than assuming
  it.

PRIMARY (frozen before any outcome): same_day_cross vs matched TOP-only and BOTTOM-only.
±1 / ±2 day windows and the ordering diagnostics are SECONDARY, diagnostic only — declared here so
the window cannot be chosen after seeing results.
"""
from __future__ import annotations

import os
import duckdb
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
LBAL = os.path.join(ROOT, "data", "lbal_signals.parquet")
OUT = os.path.join(ROOT, "data", "cross_star_features.parquet")

L = lambda c="─", n=86: print(c * n)


def build(con) -> pd.DataFrame:
    """Feature-only ticker-day table. UNKNOWN stays UNKNOWN — coverage is a tri-state, never False."""
    con.execute(f"""
      CREATE OR REPLACE TEMP VIEW top AS
      SELECT ticker, CAST(date AS DATE) AS d,
             (udn IS NOT NULL)                       AS top_covered,
             COALESCE(star, FALSE)                   AS top_star_1,
             COALESCE(conflict, FALSE)               AS top_star_2,
             COALESCE(half_up, FALSE)                AS top_star_3,
             nv_lab
      FROM read_parquet('{LBAL}')""")
    con.execute("""
      CREATE OR REPLACE TEMP VIEW bot AS
      SELECT ticker, CAST(date AS DATE) AS d, universe, close, volume,
             (phys_ad IS NOT NULL)                   AS bot_covered,
             phys_ad,
             COALESCE(ad_fresh,0)   <> 0             AS pine_fresh,
             COALESCE(ad_cluster,0) <> 0             AS pine_cluster
      FROM bars WHERE universe <> 'index'
      QUALIFY row_number() OVER (PARTITION BY ticker, CAST(date AS DATE) ORDER BY universe) = 1""")
    return con.execute("""
      SELECT b.ticker, b.d AS date, b.universe, b.close, b.volume, b.close*b.volume AS dollar_vol,
             COALESCE(t.top_covered, FALSE) AS top_covered,
             b.bot_covered,
             COALESCE(t.top_star_1,FALSE) AS top_star_1,
             COALESCE(t.top_star_2,FALSE) AS top_star_2,
             COALESCE(t.top_star_3,FALSE) AS top_star_3,
             b.phys_ad = '★'  AS bottom_ad_fresh,
             b.phys_ad = '★★' AS bottom_ad_cluster,
             b.phys_ad = '★A' AS bottom_ad_absorption,
             b.pine_fresh, b.pine_cluster
      FROM bot b LEFT JOIN top t ON t.ticker = b.ticker AND t.d = b.d
      ORDER BY b.ticker, b.d""").fetchdf()


def main():
    con = duckdb.connect(DB, read_only=True)
    con.execute("pragma threads=8")
    X = build(con)
    X["top_star_any"] = X[["top_star_1", "top_star_2", "top_star_3"]].any(axis=1)
    X["bottom_star_any"] = X[["bottom_ad_fresh", "bottom_ad_cluster", "bottom_ad_absorption"]].any(axis=1)
    # eligibility: a comparison needing BOTH systems may only use rows where BOTH are covered
    X["both_covered"] = X["top_covered"] & X["bot_covered"]
    X["same_day_cross"] = X["both_covered"] & X["top_star_any"] & X["bottom_star_any"]

    L("═"); print("§1 · PROVENANCE"); L("═")
    print("""  TOP STAR     lbal_build.py:157-164 states(udn, udn_c, colour) -> data/lbal_signals.parquet
               ★ star · ★★ conflict · ★★★ half_up   (UNSUFFIXED = PRIMARY, MODE "V")
               covered  <=>  udn IS NOT NULL (nv_lab > 0). SAFE / DESCRIPTIVE — the module states
               "Nothing here is an edge, a score input or a filter with claimed lift."
  BOTTOM STAR  bar_physics.py:293 -> bars.phys_ad, ONE mutually-exclusive categorical
               ★ AD-FRESH · ★★ AD-CLUSTER · ★A AD-FRESH+absorption · '' none
               covered  <=>  phys_ad IS NOT NULL. SAFE — "NO LOOKAHEAD ANYWHERE" (bar_physics.py:24)
               Pine originals bars.ad_fresh / ad_cluster imported separately (importer.py:129)""")

    L("═"); print("§2 · COVERAGE — UNKNOWN is not False"); L("═")
    n = len(X)
    print(f"  ticker-days in `bars` (non-index, deduped)   {n:,}   "
          f"{X.date.min()} .. {X.date.max()}   {X.ticker.nunique():,} tickers")
    for lab, m in (("TOP covered", X.top_covered), ("BOTTOM covered", X.bot_covered),
                   ("BOTH covered  <- eligible", X.both_covered)):
        print(f"  {lab:28s} {m.sum():>10,}  ({100*m.mean():5.2f} %)   "
              f"median $vol {X.loc[m,'dollar_vol'].median():>14,.0f}")
    for lab, m in (("TOP only", X.top_covered & ~X.bot_covered),
                   ("BOTTOM only", X.bot_covered & ~X.top_covered),
                   ("NEITHER", ~X.top_covered & ~X.bot_covered)):
        print(f"  {lab:28s} {m.sum():>10,}  ({100*m.mean():5.2f} %)   "
              f"median $vol {X.loc[m,'dollar_vol'].median() if m.any() else float('nan'):>14,.0f}")

    L("═"); print("§3 · PORT PARITY — phys_ad vs the Pine originals (on rows where both exist)"); L("═")
    m = X.bot_covered & X.pine_fresh.notna()
    p_any = X.loc[m, "bottom_star_any"]; pine_any = X.loc[m, "pine_fresh"] | X.loc[m, "pine_cluster"]
    both = (p_any & pine_any).sum(); po = (p_any & ~pine_any).sum(); pn = (~p_any & pine_any).sum()
    print(f"  rows compared {int(m.sum()):,}")
    print(f"  agree (both fire)      {both:>9,}")
    print(f"  phys_ad only           {po:>9,}")
    print(f"  Pine only              {pn:>9,}")
    print(f"  disagreement           {po+pn:>9,}  ({100*(po+pn)/m.sum():.3f} % of compared rows)")

    L("═"); print("§4 · PREVALENCE on the ELIGIBLE universe (both covered)"); L("═")
    E = X[X.both_covered]
    print(f"  eligible ticker-days {len(E):,} · {E.ticker.nunique():,} tickers · {E.date.nunique():,} sessions")
    for lab, m in (("TOP any", E.top_star_any), ("  ★   divergence", E.top_star_1),
                   ("  ★★  conflict", E.top_star_2), ("  ★★★ half-up", E.top_star_3),
                   ("BOTTOM any", E.bottom_star_any), ("  ★   AD-FRESH", E.bottom_ad_fresh),
                   ("  ★★  AD-CLUSTER", E.bottom_ad_cluster), ("  ★A  +absorption", E.bottom_ad_absorption),
                   ("SAME-DAY CROSS", E.same_day_cross),
                   ("TOP only", E.top_star_any & ~E.bottom_star_any),
                   ("BOTTOM only", E.bottom_star_any & ~E.top_star_any),
                   ("neither", ~E.top_star_any & ~E.bottom_star_any)):
        print(f"  {lab:22s} {m.sum():>9,}  ({100*m.mean():6.3f} %)")

    L("═"); print("§5 · REDUNDANCY — are the two families the same thing?"); L("═")
    a, b = E.top_star_any.to_numpy(), E.bottom_star_any.to_numpy()
    pa, pb, pab = a.mean(), b.mean(), (a & b).mean()
    import numpy as np
    phi_den = np.sqrt(pa*(1-pa)*pb*(1-pb))
    print(f"  P(TOP)                     {pa:.4f}")
    print(f"  P(BOTTOM)                  {pb:.4f}")
    print(f"  P(TOP ∧ BOTTOM) observed   {pab:.5f}")
    print(f"  expected if independent    {pa*pb:.5f}")
    print(f"  LIFT over independence     {pab/(pa*pb):.3f}")
    print(f"  P(BOTTOM | TOP)            {pab/pa:.4f}   vs base {pb:.4f}")
    print(f"  P(TOP | BOTTOM)            {pab/pb:.4f}   vs base {pa:.4f}")
    print(f"  phi (binary correlation)   {(pab - pa*pb)/phi_den if phi_den else float('nan'):+.4f}")
    print(f"  Jaccard                    {pab/((a|b).mean()):.4f}")

    X.to_parquet(OUT, index=False)
    print(f"\nfeature table -> {OUT}  ({len(X):,} rows)")
    con.close()
    part2(X)


def part2(X):
    """Windows, concentration, stratification, data quality. Still NO outcome access."""
    import numpy as np
    E = X[X.both_covered].copy().sort_values(["ticker", "date"]).reset_index(drop=True)
    
    # ── temporal windows: SECONDARY diagnostics only, declared before any outcome ────────────
    sess = pd.Index(sorted(E.date.unique()))
    idx = pd.Series(range(len(sess)), index=sess)
    E["si"] = E.date.map(idx)                                   # session index, so ±1 means ±1 TRADING day
    g = E.groupby("ticker", sort=False)
    def shifted(col, k):
        """col at session-index si+k for the same ticker, aligned on the session grid (gaps stay gaps)."""
        src = E.loc[E[col], ["ticker", "si"]].copy(); src["si"] += k; src["hit"] = True
        return E.merge(src.drop_duplicates(), on=["ticker", "si"], how="left")["hit"].fillna(False).to_numpy()
    
    top_m1, top_p1 = shifted("top_star_any", 1), shifted("top_star_any", -1)
    bot_m1, bot_p1 = shifted("bottom_star_any", 1), shifted("bottom_star_any", -1)
    top_m2, top_p2 = shifted("top_star_any", 2), shifted("top_star_any", -2)
    bot_m2, bot_p2 = shifted("bottom_star_any", 2), shifted("bottom_star_any", -2)
    T, B = E.top_star_any.to_numpy(), E.bottom_star_any.to_numpy()
    E["same_day"] = T & B
    E["near_pm1"] = (T & (B | bot_m1 | bot_p1)) | (B & (top_m1 | top_p1))
    E["near_pm2"] = E.near_pm1 | (T & (bot_m2 | bot_p2)) | (B & (top_m2 | top_p2))
    E["top_before_bottom_1d"] = B & top_m1 & ~T          # TOP yesterday, BOTTOM today, not same-day
    E["bottom_before_top_1d"] = T & bot_m1 & ~B
    
    L("═"); print("§6 · TEMPORAL WINDOWS — same-day is PRIMARY; ±1/±2 and ordering are DIAGNOSTIC ONLY"); L("═")
    for lab, m in (("same_day_cross  <- PRIMARY", E.same_day), ("near_cross_pm1", E.near_pm1),
                   ("near_cross_pm2", E.near_pm2), ("TOP->BOTTOM within 1d", E.top_before_bottom_1d),
                   ("BOTTOM->TOP within 1d", E.bottom_before_top_1d)):
        print(f"  {lab:28s} {m.sum():>9,}  ({100*m.mean():6.3f} %)")
    print("\n  nesting (each window must contain the previous):")
    print(f"    same_day ⊆ pm1   {bool((E.same_day & ~E.near_pm1).sum() == 0)}"
          f"    ·  pm1 ⊆ pm2   {bool((E.near_pm1 & ~E.near_pm2).sum() == 0)}")
    print(f"    pm1 adds {int((E.near_pm1 & ~E.same_day).sum()):,} rows over same-day; "
          f"pm2 adds a further {int((E.near_pm2 & ~E.near_pm1).sum()):,}")
    
    L("═"); print("§7 · CONCENTRATION of the same-day cross"); L("═")
    C = E[E.same_day]
    vc, vd = C.ticker.value_counts(), C.date.value_counts()
    print(f"  {len(C):,} rows · {C.ticker.nunique():,} tickers · {C.date.nunique():,} sessions")
    print(f"  top ticker {vc.index[0]} {vc.iloc[0]:,} ({100*vc.iloc[0]/len(C):.2f} %) · "
          f"top-10 {100*vc.head(10).sum()/len(C):.1f} % · top-50 {100*vc.head(50).sum()/len(C):.1f} %")
    print(f"  top day    {vd.index[0]} {vd.iloc[0]:,} ({100*vd.iloc[0]/len(C):.2f} %) · "
          f"top-10 days {100*vd.head(10).sum()/len(C):.1f} %")
    print(f"  per-session share of cross: median {100*(C.groupby('date').size()/E.groupby('date').size()).median():.2f} %")
    
    L("═"); print("§8 · LIQUIDITY / PRICE / YEAR — does the cross live somewhere specific?"); L("═")
    E["yr"] = pd.to_datetime(E.date).dt.year
    q = E.dollar_vol.quantile([1/3, 2/3]).to_numpy()
    E["liq"] = np.where(E.dollar_vol <= q[0], "low", np.where(E.dollar_vol <= q[1], "mid", "high"))
    E["pxb"] = pd.cut(E.close, [-1, 2, 8, 21, 89, 377, 1e9], labels=["<$2","$2-8","$8-21","$21-89","$89-377",">=$377"])
    for key in ("liq", "pxb", "yr"):
        r = E.groupby(key, observed=True).agg(n=("same_day","size"), top=("top_star_any","mean"),
                                              bot=("bottom_star_any","mean"), cross=("same_day","mean"))
        r["lift"] = r["cross"] / (r["top"] * r["bot"])
        print(f"\n  by {key}:")
        print("    " + r.assign(**{c: (100*r[c]).round(2) for c in ("top","bot","cross")})
                .rename(columns={"top":"TOP%","bot":"BOT%","cross":"CROSS%"})
                .round({"lift":3}).to_string().replace("\n", "\n    "))
    
    L("═"); print("§9 · OVERLAP BY STRENGTH — is the cross driven by one grade?"); L("═")
    print(f"  {'TOP grade':16s} {'n':>9s} {'P(BOTTOM|TOP)':>14s} {'lift':>7s}")
    pb = E.bottom_star_any.mean()
    for lab, m in (("★   divergence", E.top_star_1), ("★★  conflict", E.top_star_2), ("★★★ half-up", E.top_star_3)):
        p = E.loc[m, "bottom_star_any"].mean()
        print(f"  {lab:16s} {m.sum():>9,} {p:>14.4f} {p/pb:>7.3f}")
    print(f"\n  {'BOTTOM grade':16s} {'n':>9s} {'P(TOP|BOTTOM)':>14s} {'lift':>7s}")
    pt = E.top_star_any.mean()
    for lab, m in (("★   AD-FRESH", E.bottom_ad_fresh), ("★★  AD-CLUSTER", E.bottom_ad_cluster),
                   ("★A  +absorption", E.bottom_ad_absorption)):
        p = E.loc[m, "top_star_any"].mean()
        print(f"  {lab:16s} {m.sum():>9,} {p:>14.4f} {p/pt:>7.3f}")
    
    L("═"); print("§10 · DATA-QUALITY"); L("═")
    dup = X.duplicated(["ticker","date"]).sum()
    print(f"  duplicate ticker-days           {dup:,}   {'OK' if dup==0 else '⛔'}")
    cov = E.groupby("yr").size() / X.assign(yr=pd.to_datetime(X.date).dt.year).groupby("yr").size()
    print(f"  eligible share by year          " + " · ".join(f"{y}:{100*v:.0f}%" for y, v in cov.items()))
    print(f"  eligible span                   {E.date.min()} .. {E.date.max()}")
    mc = E[E.same_day].close.lt(8).mean()
    print(f"  cross rows under $8             {100*mc:.1f} %   (eligible universe overall {100*E.close.lt(8).mean():.1f} %)")



if __name__ == "__main__":
    main()
