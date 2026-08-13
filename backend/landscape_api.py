"""🗺 Landscape on the chart — the volume-at-price mode, and where the thin air is.

WHAT THIS DRAWS AND WHAT IT DOES NOT CLAIM. The landscape survived three screens as a
DESCRIPTION: stage 2.5 showed the nine lines carry almost none of it (position R² 0.086 against
the whole alphabet), so where price sits in its own volume profile is genuinely not on the chart
yet. It did NOT survive as a signal — FREE FLIGHT was measured twice, on the wrong cell and then
the right one, and died at the declared gate both times with Δ −0.004 against its contrast.

Both facts ship together. The overlay carries its own measured result in the badge and the
tooltip, because a marker on a chart with no verdict beside it gets read as a setup, and this one
has a verdict: no standalone edge.

WHY IT IS STILL WORTH DRAWING. Two reasons, and the second is the real one.

  A volume profile is ordinary chart information a trader reads directly, and the mode is a more
  defensible equilibrium than an EMA — it is where trade actually happened rather than a
  smoothing constant.

  And "no standalone edge" is not "no value". This book is full of things worthless alone and
  useful as conditioners: the MTF-EMA stack is weak by itself and redeemed as a booster, 🏆RS
  rescues worst years, the ⛔ suppressors only ever work as vetoes. Seeing the landscape beside
  the existing edge markers is how one would notice whether a void changes what an edge does —
  which is the measurement that comes next and which the standalone test could not have found.

COMPUTED PER TICKER, ON THE FLY. One name is ~1300 bars and under a second; the materialised
parquet is for bulk studies. Fresh computation also means the chart cannot silently disagree
with a stale file.
"""
from __future__ import annotations

import os

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))

# the measured result, carried with every response so the overlay cannot be read as a setup
VERDICT = {
    "standalone_edge": "NONE",
    "measured": ("FREE FLIGHT v1 and v2, mining window 2021-05-27 → 2025-08-12: void above vs "
                 "the same position without it, Δ −0.004 median r10 against a declared +1.0"),
    "family_vs_market": ("both arms +0.12 median r10 against +0.34 for all bars — the whole "
                         "above-the-mode family underperforms"),
    "what_survived": ("the landscape as a DESCRIPTION: the nine lines explain 0.086 of position "
                      "out of sample, so this is not already on the chart"),
    "open_question": ("whether a void CONDITIONS an existing edge rather than predicting on its "
                      "own — not yet measured"),
}


def marks(ticker: str, universe: str | None = None, limit: int = 400) -> dict:
    import market_physics as MP                                       # noqa: PLC0415
    import market_physics_token as T                                  # noqa: PLC0415
    import sources as srcs                                            # noqa: PLC0415

    tk = ticker.upper().strip()
    for uni in ([universe] if universe else ["sp500", "nasdaq", "russell2k"]):
        df = srcs.bars("1d", universe=uni, verbose=False)
        g = df[df["ticker"] == tk]
        if len(g) >= MP.MIN_HISTORY:
            break
    else:
        return {"ticker": tk, "marks": [], "mode_line": [], "verdict": VERDICT,
                "error": f"{tk}: fewer than {MP.MIN_HISTORY} bars in any universe"}

    phys = MP.compute(g.copy(), verbose=False)
    phys["land_token"] = T.tokenise(phys)
    phys = phys.dropna(subset=["land_token"]).tail(limit)

    out = []
    for r in phys.itertuples():
        tokn = r.land_token
        out.append({
            "date": str(r.date)[:10], "token": tokn,
            "position": tokn[0], "void": tokn[1], "density": tokn[2],
            "dist_mode_atr": round(float(r.land_dist_mode_atr), 2),
            "barrier_up": round(float(r.land_barrier_up), 2),
            "barrier_dn": round(float(r.land_barrier_dn), 2),
            # the FREE FLIGHT cell: far above the mode with thin air overhead
            "free_flight": bool(tokn[0] in ("F", "A") and tokn[1] == "a"),
        })
    mode = [{"date": str(r.date)[:10], "price": round(float(r.land_mode_price), 4)}
            for r in phys.itertuples() if np.isfinite(r.land_mode_price)]
    counts = phys["land_token"].value_counts()
    return {"ticker": tk, "universe": uni, "bars": len(out), "marks": out,
            "mode_line": mode, "verdict": VERDICT,
            "token_version": T.TOKEN_VERSION,
            "alphabet": {"position": T.POSITION, "void": T.VOID, "density": T.DENSITY},
            "distribution": {k: int(v) for k, v in counts.head(10).items()}}


def build_router():
    from fastapi import APIRouter, HTTPException                      # noqa: PLC0415

    router = APIRouter(prefix="/api/studio/landscape", tags=["studio-landscape"])

    @router.get("/{ticker}")
    def _marks(ticker: str, universe: str = "", limit: int = 400):
        try:
            return marks(ticker, universe or None, limit)
        except Exception as e:                                        # noqa: BLE001
            raise HTTPException(500, detail=str(e))

    return router
