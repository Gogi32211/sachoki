# SBUX_SEQ_V1: which TZL sequences preceded SBUX rises (2026-10-04)

## Set-up

- **Plan:** approved ("ki"). Script `backend/research_sbux_seq.py`, raw output `SBUX_SEQ_V1.json`, which holds all 47 episodes and their sequences.
- **Data:** 1D. Bars −3..−1 carry the T/Z state; bar 0 carries T/Z + L.
- **Rise:** +10 % (high) within 20 bars before a −7 % stop.
  - Entry at next open.
  - Open gap checked first, then stop before target.
- **Base rates:** SBUX 16.6 % (1,323 resolved bars); liquid universe 27.9 %.

## 1. Episodes

- There are 47 non-overlapping rise starts in 2021-06 … 2026-06.
- **All 47 have a different 4-bar sequence.** No exact sequence repeats before SBUX rises.

## 2. Sequences inside SBUX

| | count |
|---|---|
| sequences seen ≥ 3 times (k) | 59 |
| Bonferroni α | 0.00085 |
| best p | 0.016 (`Z4 > T4 > Z9·L25`, 3 of 4) |

**Nothing is significant.** The three best candidates (n = 3-4 on SBUX) were also checked across the universe, against the same day × ATR%-bin:

| sequence | SBUX | universe 2021-23 | universe 2024-26 | universe all |
|---|---|---|---|---|
| `Z4 > T4 > Z9·L25` | 3/4 | +1.3 [−4.9, +7.5] | −4.2 [−9.4, +1.1] | −1.8 |
| `T3 > T2G > Z5·L12` | 2/3 | −3.5 | +0.5 | −1.1 |
| `Z2G > Z2G > Z2·L46` | 2/3 | +0.9 | −1.2 | −0.3 |

None carries over. They are SBUX's own history, mostly coincidences on 3-4 bars.

## 3. A pattern the eye sees, and why it is an artefact

- **What the eye sees:** 41 of 47 episode starts (87 %) sit on a **Z bar**, mostly L46 / L5 / L25. It looks like "SBUX rises start after a red, volume-down-close bar".
- **What the forward test says:** the hit rate after a Z bar is **16.8 %** and after a T bar **16.4 %**, while Z bars make up 50.7 % of all SBUX bars. There is no difference.
- **Why:** "the start of a rise" is by definition the bar closest to the low, and a bar near a low is almost always red. This is the same leak as `feedback-turnzone-label-leaks-recent-low`: labelling where a move began selects the recent low, so the outcome is already inside the label.

## Conclusion

SBUX has no TZL sequence that precedes its rises more often than chance. The visible "Z before the rise" is a property of how a move's start is defined, not a signal.

- **Current SBUX (2026-10-02):** `T2 > Z4 > T9 > Z3·L46`. This matches none of the candidates (and none of them works anyway).
