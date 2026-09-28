# VOL_ECHO_TRADE_V1 — trading ▲ Release / BO▲ / BOV▲ with SL −5 % / TP +15 %

**Date** 2026-09-25 · plan approved by the user ("ki") · frozen to `trade_plan.txt` before any outcome · k = 9.
**Verdict: NULL — stopped at the deciding test.** No entry, in any context, beats the same exit on an ordinary bar by +0.5 pp.

## Setup
- **Entries.** ▲ Release, BO▲ and BOV▲ from the exact Pine port at its defaults.
- **Trade.** Entry at open[D+1]. SL −5 %, TP +15 % full exit, gap-realistic fills, time exit at 60 bars, slip 0.15 %, 5-bar cooldown (book `_pathsim`, fixed mode).
- **Contexts.** Raw · close > EMA200 · RS (close/SPY above its EMA200). This gives k = 9.
- **Universe.** Close ≥ $1 and dollar volume ≥ $2M on the signal bar.
- **Control.** The same exit on `control_keys`-phase sampled bars (stride 40) of the same universe. Δ per entry day = signal mean − control mean; days need ≥ 20 control trades. Day-clustered bootstrap.
- **Windows.** MINE 2021-23 (VERIFY not opened).
- **Warm-up caveat.** EMA200 and the RS ratio need 200 bars. Both SPY and the store start mid-2021, so the EMA200 and RS cells cover **2022-2023 only**.

## Result — MINE 2021-2023

| cell | trades | days | mean trade | win % | TP hit | SL hit | Δ vs control (pp) |
|---|---|---|---|---|---|---|---|
| ▲ Rel · raw | 33,348 | 623 | −0.77 % | 25 | 20 % | 74 % | +0.05 [−0.12, +0.21] |
| ▲ Rel · EMA200 | 11,792 | 453 | −0.91 % | 25 | 19 % | 74 % | −0.13 [−0.39, +0.13] |
| ▲ Rel · RS | 10,756 | 412 | −0.56 % | 26 | 21 % | 73 % | −0.05 [−0.32, +0.25] |
| BO▲ · raw | 42,740 | 625 | −0.79 % | 25 | 20 % | 74 % | −0.04 [−0.22, +0.14] |
| BO▲ · EMA200 | 16,748 | 452 | −0.81 % | 26 | 19 % | 73 % | **−0.34** [−0.64, −0.08] |
| BO▲ · RS | 15,171 | 411 | −0.51 % | 27 | 21 % | 72 % | −0.21 [−0.52, +0.06] |
| BOV▲ · raw | 29,866 | 627 | −0.79 % | 25 | 20 % | 74 % | −0.05 [−0.25, +0.15] |
| BOV▲ · EMA200 | 12,780 | 452 | −0.71 % | 26 | 19 % | 73 % | −0.12 [−0.40, +0.15] |
| BOV▲ · RS | 11,412 | 412 | −0.42 % | 27 | 21 % | 72 % | −0.08 [−0.37, +0.18] |

The control (ordinary bars, same exit) averages **−0.37 %** per trade over 84,573 trades.

## Reading
1. **The entries add nothing over a random bar.** Every Δ is within ±0.35 pp. The only CI excluding zero (BO▲ in an uptrend) is **negative**: breakouts above the zone in names already above EMA200 do slightly *worse* than random entries.
2. **The exit loses on its own, before any signal is involved.** With SL 5 % / TP 15 % the stop is hit about 3 times in 4 (72-74 %) and the target about 1 in 5. Break-even for a 1 : 3 bracket is about 26 % wins after slippage. Signals and control both sit at 25-27 %, so both lose (−0.4 to −0.9 % per trade).
   - For these names a 5 % stop is inside one day's normal range, which matches the book's ATR×12 exit law: a fixed stop tighter than the name's volatility bleeds on every setup.
   - This is **descriptive**; the exit was fixed by the user and was not tuned here.
3. **Context did not rescue the entries.** EMA200 and RS both moved the mean trade only slightly and pushed Δ vs control slightly negative. They cover 2022-23 only (warm-up), so 2022 weighs heavily.

## What this closes, and what it does not
- **Closed:** ▲ Release / BO▲ / BOV▲ as entries under a fixed 5/15 bracket, raw or with EMA200/RS context.
- **Not tested:** the same entries with a volatility-scaled exit (the book's ATR×12 trailing). That would be a new family and need its own plan. The point in reading 2 suggests the bracket, not only the entry, was working against the trade.

Artifacts: session scratchpad `vecho/trade_plan.txt`, `trade_v1.py`, `trade_v1.log`, `trade_v1_result.json`. No UI, score, or memory change.
