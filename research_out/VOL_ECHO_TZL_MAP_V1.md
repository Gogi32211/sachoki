# VOL_ECHO_TZL_MAP_V1 — which TZ / L bars the 260925_VOL_ECHO signals land on

**Date** 2026-09-25 · **descriptive only** (no forward returns, no k, no verdict).

## Method
- `260925_VOL_ECHO` was ported exactly to Python at its default parameters (spike 1.5, echo 1.5, ±40 % volume, 50 % range overlap, quiet ≤ 0.8, release 3 bars, breakout 120 bars).
- It ran on the daily store, deduplicated on (ticker, date): 5,459,804 bars across 5,761 tickers (all universes), 2021-05 → 2026-09. S&P 500 alone (792,391 bars) was run as a robustness check.
- **Port check against TradingView (RGTI, Oct 2024):** Q on 10-15, VE on 10-16/17/18/21/22, ▲ on 10-16. This matches the user's chart.
- **Lift** = the code's share among signal bars ÷ the share expected from the same colour mix. BO▲ is always green, so its lift is measured against green bars and T-codes are not credited for being T.

## Composition (all universes; S&P 500 agrees in every row)

| signal | % of all bars | dominant TZ (lift) | dominant L (lift) |
|---|---|---|---|
| SPIKE (every spike) | 22.8 % | T2G / Z2G ≈ 12 % each (×1.15) | L46 35 % (×1.5) · L3 25 % (×1.7) · L34 13 % (×1.3) |
| SPK (echoed origin) | 9.3 % | same as SPIKE | same as SPIKE |
| VE (echo) | 13.5 % | same as SPIKE | L46 36 % · L3 25 % · L34 14 % |
| Q (quiet in zone) | 7.1 % | inside bars T9 (×1.32) / Z9 (×1.40), doji Z7 | L12 36 % (×1.5) · L25 28 % (×1.8) |
| Release ▲ | 4.0 % | T2G 24 % (×1.14) · T1G (×1.27) | **L3 50 % (×1.9)** · L34 22 % (×1.4) |
| Release ▼ | 3.9 % | Z2G 24 % (×1.12) · Z1G (×1.22) | **L46 69 % (×1.7)** |
| BO▲ | 2.8 % | **T2G 45 % (×2.1)** · T1G 13 % (×1.9) | **L12 53 %** (×1.3) · L3 33 % |
| BD▼ | 3.1 % | **Z2G 44 % (×2.1)** · Z1G 11 % (×1.9) | L46 40 % · **L5 36 % (×1.9)** |
| BOV▲ | 1.7 % | T2G 46 % (×2.2) · T1G (×1.9) | **L3 57 % (×2.1)** |
| BDV▼ | 1.8 % | Z2G 43 % (×2.1) · Z1G (×1.9) | **L46 69 % (×1.7)** |

## Reading
1. **The echo is not rare at defaults.** VE fires on 13.5 % of all bars (9.7 % in the S&P) and SPIKE on 22.8 %. At these settings the echo is a context state, not an event. Tightening the similarity (±25 %) or the spike (≥ 2.0) would make it rarer. That is a display choice, not changed here.
2. **VE's TZ/L profile is identical to SPIKE's.** The "same volume, same zone" condition does not select a different kind of bar. In TZL terms an echo is simply a high-volume bar: L46 / L3 / L34 over-represented, L12 / L25 / L5 under-represented.
3. **Much of the table is mechanical.**
   - Open and close outside the zone forces a gap-style T2G/T1G or Z2G/Z1G.
   - A volume filter forces the volume-defined L-lines: quiet gives L12/L25, above-average gives L3/L46.
4. **The one split the TZL language adds concerns BO▲, and it is not mechanical.**
   - **53 % of BO▲ bars are L12**, a breakout on *declining* volume (L1 no supply + L2).
   - **33 % are L3**, a breakout on rising volume (demand). BOV▲ isolates the L3 kind (57 %).
   - BD▼ splits the same way: **L5** is a breakdown on declining volume (no demand); BDV▼ is L46 (supply).
   - Whether the L12 and L3 kinds of breakout behave differently afterwards is an **open question**. It needs its own pre-registered plan and has not been measured.

Artifacts: session scratchpad `vecho/port.py`, `comp.py`, `flags_all.parquet`, `flags_sp500.parquet`, `comp_all.log`, `comp_sp500.log`. No UI, score, or memory change.
