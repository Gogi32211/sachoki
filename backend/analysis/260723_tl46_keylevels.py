"""T*L46 → future key-level test (2026-07-23, user hypothesis: bull-T bars carrying an
L46 crowd volume-line print a PRICE LEVEL that later acts as support/resistance).

Measured, not asserted: for each T*L46 bar take its level (close / high / low), walk
FORWARD, find genuine TESTS (price revisits the level intrabar after being away ≥1%),
and classify each test as HELD (level rejected price → acted as S/R) or BROKE (price
passed through). Compare the HOLD-RATE + touch-frequency + bounce-size to a matched
RANDOM control (same tickers, same count, random bars' close). If T*L46 levels hold
meaningfully more than random, the hypothesis stands. dv>=3M, non-index, $5+, 5yr.
"""
import duckdb
import numpy as np
import pandas as pd
from collections import defaultdict

TOL = 0.004        # a "touch" = price within ±0.4% of the level (intrabar low..high straddle)
AWAY = 0.012       # must have been ≥1.2% away before a test counts (a real revisit)
REACT = 3          # bars after the touch to judge hold vs break
LOOKFWD = 120      # forward window to hunt tests

c = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb', read_only=True)
df = c.execute("""
    WITH deduped AS (
        SELECT * FROM bars WHERE close>=5 AND universe<>'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker,date ORDER BY
            CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 4 END)=1
    )
    SELECT ticker, date, open, high, low, close, volume,
           coalesce(t_sig,'') tt, coalesce(l_sig,'') ll
    FROM deduped ORDER BY ticker, date
""").fetchdf()
c.close()
print(f"frame {len(df):,}", flush=True)

o=df.open.to_numpy(float); h=df.high.to_numpy(float); lo=df.low.to_numpy(float); cl=df.close.to_numpy(float)
vol=df.volume.to_numpy(float); tt=df.tt.to_numpy(); ll=df.ll.to_numpy(); tk=df.ticker.to_numpy(); n=len(df)
is_T = np.char.startswith(tt.astype(str), 'T')
idx=defaultdict(list)
for i,t in enumerate(tk): idx[t].append(i)
last={t:ii[-1] for t,ii in idx.items()}

def test_level(anchor_i, level):
    """Walk forward from anchor; return (n_tests, n_held) for `level`."""
    tl = last[tk[anchor_i]]
    end = min(anchor_i + LOOKFWD, tl)
    tests = held = 0
    j = anchor_i + 1
    while j <= end - REACT:
        # a test: this bar straddles the level, and the PRIOR bar was ≥AWAY from it
        straddle = lo[j] <= level*(1+TOL) and h[j] >= level*(1-TOL)
        away = abs(cl[j-1]-level)/level >= AWAY
        if straddle and away:
            tests += 1
            approached_above = cl[j-1] > level          # coming down onto the level = support test
            after = cl[j+REACT]
            if approached_above:
                if after > level: held += 1              # bounced back up = support held
            else:                                        # approached from below = resistance test
                if after < level: held += 1              # rejected back down = resistance held
            j += REACT + 1                               # skip past this test cluster
        else:
            j += 1
    return tests, held

# ── T*L46 anchors (bull-T + L46), plus red/green split ────────────────────────
anchors = [i for i in range(n) if is_T[i] and ll[i]=='L46' and cl[i]*vol[i]>=3e6]
print(f"T*L46 anchors: {len(anchors)}", flush=True)

def run(anchor_list, level_fn, label):
    tot_tests = tot_held = with_test = 0
    per = []
    for i in anchor_list:
        L = level_fn(i)
        if L is None or L<=0: continue
        te, he = test_level(i, L)
        tot_tests += te; tot_held += he
        if te>0: with_test += 1
        per.append((te, he))
    hold = 100*tot_held/tot_tests if tot_tests else 0
    freq = 100*with_test/len(anchor_list) if anchor_list else 0
    avg_touch = tot_tests/max(1,with_test)
    print(f"  {label:26} n={len(anchor_list):5} · tested={freq:4.1f}% · avg touches={avg_touch:.2f} · HOLD-rate={hold:4.1f}% ({tot_held}/{tot_tests})")
    return hold, tot_tests

print("\n══ LEVEL = bar CLOSE ══")
run(anchors, lambda i: cl[i], "T*L46 close")
# random control: same count of random bars, same dv gate
rng=np.random.default_rng(23)
pool=[i for i in range(n) if cl[i]*vol[i]>=3e6 and i<last[tk[i]]-REACT]
rand=list(rng.choice(pool, min(len(anchors),len(pool)), replace=False))
run(rand, lambda i: cl[i], "RANDOM close (control)")

print("\n══ LEVEL = bar HIGH ══")
run(anchors, lambda i: h[i], "T*L46 high")
run(rand,    lambda i: h[i], "RANDOM high (control)")

print("\n══ LEVEL = bar LOW ══")
run(anchors, lambda i: lo[i], "T*L46 low")
run(rand,    lambda i: lo[i], "RANDOM low (control)")

print("\n══ red vs green T*L46 (close level) ══")
red=[i for i in anchors if cl[i]<o[i]]; grn=[i for i in anchors if cl[i]>=o[i]]
run(red, lambda i: cl[i], "T*L46 RED close")
run(grn, lambda i: cl[i], "T*L46 GREEN close")

print("\n══ by T-code (close level, n>=200) ══")
bycode=defaultdict(list)
for i in anchors: bycode[tt[i]].append(i)
for code in sorted(bycode, key=lambda k:-len(bycode[k])):
    if len(bycode[code])>=200:
        run(bycode[code], lambda i: cl[i], f"{code}·L46 close")
