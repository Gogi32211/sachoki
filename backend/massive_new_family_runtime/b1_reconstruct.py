"""B1 EMA34/89 producer RECONSTRUCTION from the frozen semantic contract.

Contract source: MASSIVE_COARSE_EMA_34_89_PRODUCTION_V1 · 77e7113b174129ac
  periods            [34, 89]
  input              massive_coarse_v1/15m   (here: stageA_input, its exact projection)
  usable_input       coverage_state == COMPLETE
  recursion          alpha = 2/(p+1); seed = close at segment start;
                     equivalent to ewm(span=p, adjust=False) restarted per segment
  contaminated_gap   resets state
  validity           age < p  -> INITIALIZATION_SENSITIVE
                     age >= p -> VALID
                     contaminated -> UNAVAILABLE

Nothing here is invented. Every rule is quoted from the artifact.
"""
import os, sys, json, time, warnings; warnings.filterwarnings('ignore')
import duckdb, numpy as np, pandas as pd

STAGEA = '/Users/sachoki/MASSIVE_DATA/historical/stageA_input.duckdb'
REF = '/Users/sachoki/MASSIVE_DATA/historical/ema3489.duckdb'
PERIODS = (34, 89)
S = ('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/'
     '4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad')


def build_one(close, ok):
    """Returns value, state, age arrays for each period, per the frozen contract."""
    n = len(close)
    seg = np.cumsum(~ok)
    out = {}
    for p in PERIODS:
        a = 2.0 / (p + 1.0)
        val = np.full(n, np.nan)
        age = np.zeros(n, dtype=np.int64)
        prev = np.nan
        cnt = 0
        cur_seg = None
        for i in range(n):
            if not ok[i]:
                prev = np.nan; cnt = 0; cur_seg = None
                continue
            if cur_seg != seg[i]:
                cur_seg = seg[i]
                prev = close[i]            # seed = close at segment start
                cnt = 1
            else:
                prev = a * close[i] + (1.0 - a) * prev
                cnt += 1
            val[i] = prev; age[i] = cnt
        st = np.where(~ok, 'UNAVAILABLE',
                      np.where(age >= p, 'VALID', 'INITIALIZATION_SENSITIVE'))
        out[p] = (val, st, age)
    return out


if __name__ == '__main__':
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute(f"attach '{STAGEA}' as sa (read_only)")
    c.execute(f"attach '{REF}' as rf (read_only)")
    keys = [r[0] for r in c.execute("select distinct k from sa.bars order by k").fetchall()]
    if limit:
        keys = keys[:limit]
    print(f'securities: {len(keys)}', flush=True)
    t0 = time.time()
    tot = mis_v = mis_s = mis_a = 0
    worst = []
    for i, k in enumerate(keys, 1):
        d = c.execute("select bar_start, coverage_state, close from sa.bars "
                      "where k=? order by bar_start", [k]).df()
        ok = (d.coverage_state == 'COMPLETE').to_numpy()
        res = build_one(d.close.to_numpy(), ok)
        ref = c.execute("select bar_start, ema_period, ema_value, ema_validity_state, "
                        "ema_age_valid_bars from rf.ema where k=? order by ema_period, bar_start",
                        [k]).df()
        for p in PERIODS:
            r = ref[ref.ema_period == p]
            assert len(r) == len(d), (k, p, len(r), len(d))
            assert (r.bar_start.to_numpy() == d.bar_start.to_numpy()).all(), (k, p)
            val, st, age = res[p]
            rv = r.ema_value.to_numpy(); rs = r.ema_validity_state.to_numpy()
            ra = r.ema_age_valid_bars.to_numpy()
            bv = ~((np.isnan(val) & pd.isna(rv)) | (val == rv))
            bs = st != rs
            ba = age != ra
            tot += len(d); mis_v += int(bv.sum()); mis_s += int(bs.sum()); mis_a += int(ba.sum())
            if bv.any() and len(worst) < 3:
                j = int(np.flatnonzero(bv)[0])
                worst.append(dict(k=k[:12], p=p, idx=j, mine=float(val[j]),
                                  ref=None if pd.isna(rv[j]) else float(rv[j])))
        if i % 50 == 0:
            print(f'  {i}/{len(keys)}  rows {tot:,}  mism v={mis_v} s={mis_s} a={mis_a}  '
                  f'{time.time()-t0:.0f}s', flush=True)
    res = dict(securities=len(keys), rows_compared=tot, ema_value_mismatches=mis_v,
               validity_state_mismatches=mis_s, age_valid_bars_mismatches=mis_a,
               elapsed_s=round(time.time() - t0), worst=worst,
               VERDICT='EXACT' if (mis_v == mis_s == mis_a == 0) else 'MISMATCH')
    print(json.dumps(res, indent=1), flush=True)
    json.dump(res, open(S + '/_b1_reconstruct.json', 'w'), indent=1)
