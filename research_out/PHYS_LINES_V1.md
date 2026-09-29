# PHYS_LINES_V1 — per-line sequences of the ⚛ physics row

**Date** 2026-09-28 · user: "analyse every line of this row and find good sequences" · plan frozen to `physlines_plan.txt` before any outcome.
**Lines.** rg (RA/RN/RF·U/D), m, e (incl. ★), k, c, h, s, gp (gG1-3), ad (★/★★/★A), w (Wyckoff⚛).
**Sequences.** 1-3 bars within one line.
**Outcome.** Per-bar book trade (ATR×12 trail), taken as the excess over the day's eligible bars, day-clustered.
**Search.** 1,119 sequences on MINE.

**Verdict:**
- **UP side: 5/20 pass.** All five are gap-line (gp) sequences.
- **DOWN side: 10/10 pass,** but after audit only **⚛ Wyckoff `ACC-TR`** is a real state. The "·" (no token) cells are a young-ticker artefact.

## UP side (buy) — passing sequences (VERIFY same-day Δ, pp)

| line | sequence | MINE | VERIFY [CI] · years | last token alone (VERIFY) |
|---|---|---|---|---|
| gp | **gG2 → gG2 → gG1** | +2.79 | **+3.15 [+1.16, +5.33]** · +2.3 / +4.5 / +2.5 | +0.23 |
| gp | **gG2 → · → gG2** | +1.72 | **+1.95 [+0.71, +3.07]** · +1.5 / +3.1 / +0.8 | +0.80 |
| gp | **gG1 → gG1 → gG2** | +3.41 | **+1.79 [+0.21, +3.40]** · +0.6 / +3.5 / +0.9 | +0.80 |
| gp | **gG2 → gG1** | +1.86 | **+1.10 [+0.38, +1.87]** · −0.3 / +2.7 / +0.7 | +0.23 |
| gp | gG2 (single) | +1.14 | +0.80 [+0.45, +1.17] · +0.1 / +1.8 / +0.3 | — |

- **Failed:** 15 others from the gp, rg, s and e lines, e.g. RN·U→RA·D→RN·D, E2→E2→E1★, S2D→S2U→S2D.
- **No m, k, c, h or ad sequence reached the MINE filter.**

**Reading.** The ⚛ *true-gap* line (gG1 small, gG2 medium) carries the buy information. **Repeated or returning gaps** over 2-3 bars add a lot over a single gap:
- gG2→gG2→gG1 is +3.15 against +0.23 for gG1 alone.
- gG2→·→gG2 is +1.95 against +0.80.

This fits the book's G3 / gap-chain family (G3², G3→G3→RL), which also pays on repeated gaps. Much of the VERIFY strength sits in 2025, and 2024 is weak for some of them, so treat these as candidates for a forward read.

## DOWN side (veto) — audited

| state | MINE | VERIFY | audit |
|---|---|---|---|
| **⚛w ACC-TR** (1, 2 or 3 bars) | −10.6…−10.8 | **−15.1…−16.2** [CI far below 0], every year | **Real, but mislabelled.** In `studio/bar_physics.py`, ACC-TR = **EMA50 < EMA200 AND ATR compressed** (a downtrend squeeze), fully causal. Its trades have median −18.9 %, and 28 % lose ≥ 40 %. The name "accumulation" is misleading: it is the sub-200 downtrend state the book already treats as a suppressor. |
| "·" (no c / rg / e token) | −9.2…−9.9 | −7.9…−9.4 | **Artefact.** These are young tickers (median 33 bars of history) before physics warms up; recent listings underperform. Not a signal. |

**Action item (not done).** `ultra_score.py` gives `ACC_TR_CONTEXT` +4 points (REGIME:ACC_TR). That is from a different wyc_phase field, but by name it would reward the state this study finds strongly negative. It is worth checking before trusting that +4. This needs the user's OK; nothing was changed.

Artifacts (session scratchpad `v4hist/`): `physlines_plan.txt`, `physlines.py`, `physlines.log`. No UI, score or memory change.
