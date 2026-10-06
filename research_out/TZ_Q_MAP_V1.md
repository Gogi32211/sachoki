# TZ_Q_MAP_V1: how much Q overlaps T/Z (descriptive, no outcome, no k), 2026-10-03

- **Sample:** 1D, liquid stocks (close ≥ $5, 20-day dollar volume ≥ $5M), 2,820,413 bars.
- **Q:** computed with the same SQL as the 🔷 Q tab.
- **Coverage:** every bar carries exactly one T or Z (no-signal bars 0.0 %, bars with both 0).

## Information overlap

| quantity | bits |
|---|---|
| H(Q) | 3.75 |
| H(T/Z) | 4.30 |
| H(Q \| T/Z) | **0.29** |
| H(T/Z \| Q) | 0.84 |
| mutual information | 3.46 |

- The mutual information is 92 % of H(Q).
- Q is almost a coarsening of T/Z: once you know the T/Z state, you know the Q code.
- T/Z is the finer alphabet:
  - Q1G = T2G or T1G
  - Q2G = T2 or T1
  - Q7R = Z2 or Z1
  - Q8R = Z2G or Z1G
  - Q8G = T5 or T12

## Where Q splits a T/Z state (its only new information)

The split is the body centre moving up or down relative to the previous bar.

| T/Z | Q split | meaning |
|---|---|---|
| T4 / T6 | Q3G 72-73 % · Q6G 27-28 % | engulf, centre ↑ vs ↓ |
| Z4 / Z6 | Q6R 70-72 % · Q3R 28-30 % | engulf, centre ↓ vs ↑ |
| T9 | Q5G 72 % · Q4G 28 % | inside, centre ↓ vs ↑ |
| T10 | Q4G 75 % · Q5G 25 % | inside, centre ↑ vs ↓ |
| Z9 | Q4R 73 % · Q5R 27 % | inside, centre ↑ vs ↓ |
| Z10 | Q5R 75 % · Q4R 25 % | inside, centre ↓ vs ↑ |
| Z11 | Q1R 59 % · Q2R 41 % | fully above vs overlap ↑ |
| Z5 | Q1R 96 % · Q2R 4 % | (almost one-to-one) |
| T5 | Q8G 95 % · Q7G 4 % | (almost one-to-one) |
| Z7 | Q1/Q8/Q5/Q4 with no colour | doji bars |

All other states map 100 % to one Q: T1, T1G, T2, T2G, T3, T11, T12, Z1, Z1G, Z2, Z2G, Z3, Z12.

## Implications

1. A Q-only sequence is a coarsened T/Z sequence. Q3_SEQ_V1 being NULL is therefore expected: the path into a T/Z bar never changes its return (0/63).
2. The Q tab's own value lies in the other lines (the V token, TRUE-gap, R·C·H, Wyckoff) and in the centre split above. The Q letter itself mostly re-encodes T/Z.
3. The only outcome question Q can add is the centre split inside 9 states (T4, T6, T9, T10, Z4, Z6, Z9, Z10, Z11). That is a small, pre-specifiable test with k = 9.
