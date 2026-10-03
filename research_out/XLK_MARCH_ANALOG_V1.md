# XLK_MARCH_ANALOG_V1 — what drove the XLK turn of 2026-03-26 → 04-14, and who looks like that now

**Date** 2026-10-01 · user request. DESCRIPTIVE case study plus a state-similarity screen; no outcome claims.
**Set.** XLK proxy = S&P 500 ∩ "Technology" (`data/sector_map.json`, SIC-based, so approximate GICS): 113 names.

## 1. The process (XLK ETF)
| dates | price | what happened |
|---|---|---|
| Feb → 03-25 | $135-142 | range |
| **03-26 → 03-30** | low **$126.68** | breakdown below the range: Z2G, Z2G, Z2 |
| **03-31** | — | T1, reclaim |
| 04-01 → 04-10 | back inside the range | T2G, T6, T2G, T6, Z5, T9, T2G |
| **04-13 → 04-14** | above $143 | **breakout**: T6, T2G |
| May | $175 | T2G run |

A classic false breakdown (Wyckoff spring), then a breakout.

**Signals ENRICHED in the 113 names by phase** (vs the same names' Jan → mid-March base)
- **BREAKDOWN 03-26…03-30:** BD▼ 12 % (×3.7), EB↓ (×3.2), D50 / D2 (×3.0), **4BF↓ 48 % (×2.7)**, RF·D 37 % (×2.7), **🌀 shakeout / spring 34 % (×2.6)**, BX↓ / BO↓ / BE↓, ⚛E2 52 %.
  - T/Z mix: Z2G 23 %, Z1G 10 %, T3 9 %, T9 8 %. RSI median 40. **61 % death-cross (MKDN).**
- **TURN 03-31…04-02:** Q∧R (×4.0), SBC (×3.7), P2 / ANY P (×2.9-3.2), **ZRT Zone-Retest 29 % (×2.6)**, WRC, ★, **🔻💪 17 % (×2.4)**, BE↑, VUP.
  - T/Z mix: **T2G 23 %**, T1 8 %, T4 8 %. Still 62 % death-cross.
- **RECLAIM 04-06…04-10:** gG3 (×3.2), [E] (×3.0), ▼rel, RUP, R2H 41 %, ▲+2, 4BF 31 %, CCI0R.
- **BREAKOUT 04-13…04-17:** RH (×5.1), CD (×4.9), **K2U (extended above EMA20, ×4.1)**, B/S↑, [E], RUP, R2H 53 %, **RSI ≥ 70 15 % (×2.8)**, 4BF 42 %, 🔺 continuation 36 %.
  - T/Z mix: T2G 20 %, T1 10 %, T1G 8 %. RSI median 56.

**Reading.** The sequence was **capitulation → absorption (🌀, Q∧R, ZRT, 🔻💪) → reclaim (gG3, R2H) → expansion (K2U, RSI ≥ 70, 🔺)**. These are co-occurrences, not causes. The turn was **market-wide**:

| | XLK dd60 | SPY dd60 | QQQ dd60 | IWM dd60 | SMH dd60 |
|---|---|---|---|---|---|
| 2026-03-30 | −14.9 % | −9.4 % | −12.3 % | −11.8 % | −15.3 % |

Earlier research on the same signals says a T/Z sequence alone does not predict.

## 2. Late-March profile and today's analogues
**Profile** (median of the 113 names on 03-30 / 03-31):
- 20-bar return −6.1 %;
- 60-bar drawdown −22.8 %, position 0.20 in the 60-bar range;
- −3.2 % vs EMA20, −6.9 % vs EMA50, −11.4 % vs EMA200;
- RSI 42, ATR 4.7 %, 5/20 volume ratio 0.93, death-cross;
- 28 % had just made a spring (undercut and reclaim of the prior 20-bar low).

**Distance** = standardized (IQR-scaled) distance over 12 price-state features, best day in 09-20…09-30.
**Calibration:** the 113 names' own distance to the profile had median 0.83 and 75th pct 1.20.

**⚠️ The state is common now.** 1,623 of 2,656 eligible stocks are within 0.83.
- **Breadth now ≈ breadth on 03-30:**

| | below EMA50 | below EMA200 | RSI < 45 | dd60 ≤ −20 % |
|---|---|---|---|---|
| 2026-09-30 | 75 % | 53 % | 67 % | 35 % |
| 2026-03-30 | 75 % | 51 % | 64 % | 46 % |

- **BUT the indices are at highs now:**

| | XLK dd60 | SPY | QQQ | SMH | IWM |
|---|---|---|---|---|---|
| 2026-09-30 | −0.7 % | −2.1 % | −1.1 % | −1.5 % | −8.9 % |

  This is a **narrow market**: mega-caps hold the indices while most stocks correct.
- **The XLK set itself is NOT in the March state now:** median dd60 −13.6 %, RSI 51, 47 % below EMA50 (vs 94 % on 03-30).

**Top analogues** (distance 0.23-0.35; full ranked list in `XLK_MARCH_ANALOG_V1_list.csv`)
- FND, SAIA, BDC, SGRY, SNDX, SOLS, OFRM, RYZ, SGI, FLOC, HNRG, LPX, DRVN, RYAN, IMTX;
- **IBM**, PAHC, IRTC, LOAR, ALGN, ARLO, **OTEX**, PBH, INTR, **RDDT**, ACM, MTH, GXO, CHDN, ACI (⚛ SPRING★), AFRM, CIGI, AROC, CSL, AMTM, STNE, NU, EYE, **BL**, TAC.
- **Sectors:** mostly Industrials, Materials and Healthcare. Only 4 tech names (IBM, OTEX, RDDT, BL).
- **With a spring flag now** (undercut and reclaim of the 20-bar low): FND, SAIA, SGRY, SNDX, RYZ, SGI, RYAN, IRTC, LOAR, ARLO, CHDN, ACI, CIGI, STNE, NU, BL.

## Caveats
1. Similar state ≠ similar outcome. 0 of 65 path / sequence contrasts showed a forward edge, and the March turn was driven by a market-wide washout that is absent at the index level today.
2. The XLK proxy is SIC-based, not the official holdings list.

Artifacts (scratchpad `v4hist/`): `xlk1.py`, `xlk2.py`, `xlk3.py` and their logs.

---
## 3. "Growth has started" profile (user: the state at end of March / start of April, when the rise began)
**Profile** = median of the 113 XLK-set names on 04-01, 04-02, 04-06, 04-07 (after the turn, before the 04-13 breakout):
- **bounce:** +7 % off the 10-bar low, the low was 5 bars ago, 5-bar return +2.5 %, 10-bar return −0.9 %;
- **depth:** 60-bar drawdown still −18.7 % (position 0.30);
- **EMAs:** at EMA20 (−0.4 %), −4.1 % vs EMA50, −9.4 % vs EMA200;
- **momentum:** RSI 47 and rising (+6.4 in 5 bars), ATR 4.5 %, volume ratio 0.89;
- **flags:** 60 % had a spring (undercut and reclaim of the prior 20-bar low) in the last 10 bars, 81 % had a T1 / T2G / T1G / T4 / T6 in the last 3 bars, 36 % golden cross.

**Screen** on 2026-09-24…09-30 (best day per stock), 16 features, IQR-scaled.
- **Calibration:** XLK members' own distance had median 0.82 and 25th pct 0.59.
- **Coverage:** 362 stocks now sit inside the 25th-pct distance.

**Top 40** (distance 0.24-0.41)
- IRTC, CSL, SHEN, TLN, SITE, CRAI, RVLV, PBH, PAHC, RIVN, GVA, WFRD, SNDX, ECG, CRMD, TRI, LPX, CARR, ESI, RHLD;
- H, TNC, PTON, CSW, CRI, DFTX, LOAR, INGM, SUPN, WWD, RRX, TTWO, CMC, RYZ, TREX, ESE, AS, CGNX, VCEL, OMCL.
- **All 40 have spring10 = 1.** Mostly Industrials, Healthcare and Materials.
- **Already golden cross** (closer to the "quality" cell of GX_RS_V1): RIVN, WFRD, ECG, ESI, H, CSW, INGM, WWD, CMC, ESE, CGNX, VCEL.

**XLK-set members closest now:** TTWO 0.39, Q 0.42, AVGO 0.43, TOST 0.46, VRT 0.46, ADBE 0.47, PTC 0.50, HUBB 0.50, NXPI 0.54, XYZ 0.55, MCHP 0.56, DUOL 0.58.

Full list: `XLK_MARCH_ANALOG_V1_growthstart_list.csv`. The same caveats apply: similar state ≠ similar outcome, and the March move was index-wide while today's indices sit near their highs.
