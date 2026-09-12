"""A1b FULL historical regression — no sampling.
persisted/production 1m base -> A1b wrapper -> 15m stage-A projection
vs sealed stageA_input, two-way EXCEPT ALL over all 15,445,454 rows.
"""
import os, sys, json, time, warnings; warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/sachoki/Desktop/sachoki-desktop/backend')
os.chdir('/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb
from concurrent.futures import ProcessPoolExecutor

S = '/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
OUT = S + '/a1b_out'
os.makedirs(OUT, exist_ok=True)
STAGEA = '/Users/sachoki/MASSIVE_DATA/historical/stageA_input.duckdb'


def one(day):
    import warnings; warnings.filterwarnings('ignore')
    import forward_stagea_a1b as W
    import coarse_production_v1 as P
    global _CAL, _COH
    try:
        _CAL
    except NameError:
        _CAL = W.calendar(); _COH = W.load_cohort()
    try:
        ms, b = P.load(day)
        out, info = W.build_session(day, ms, b, _COH, _CAL)
        if out is None:
            return day, 0, info['rejected_rows'], None
        out.to_parquet(f'{OUT}/{day}.parquet', index=False, compression='zstd')
        return day, len(out), info['rejected_rows'], None
    except Exception as e:
        return day, -1, -1, f'{type(e).__name__}: {e}'


if __name__ == '__main__':
    c = duckdb.connect(); c.execute(f"attach '{STAGEA}' as sa (read_only)")
    days = [r[0] for r in c.execute("select distinct session_date from sa.bars order by 1").fetchall()]
    c.close()
    print(f'sessions to rebuild: {len(days)}', flush=True)
    t0 = time.time(); rows = 0; rej = 0; errs = []
    with ProcessPoolExecutor(max_workers=8) as ex:
        for i, (day, n, r, e) in enumerate(ex.map(one, days), 1):
            if e:
                errs.append((day, e))
            else:
                rows += n; rej += r
            if i % 100 == 0:
                print(f'  {i}/{len(days)}  rows {rows:,}  {time.time()-t0:.0f}s', flush=True)
    print(f'rebuild done: {rows:,} rows, {len(errs)} errors, rejected_rows={rej}, {time.time()-t0:.0f}s', flush=True)
    for d, e in errs[:10]:
        print('  ERR', d, e, flush=True)

    c = duckdb.connect(); c.execute("pragma threads=8"); c.execute("set memory_limit='8GB'")
    c.execute(f"attach '{STAGEA}' as sa (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    c.execute("create view R as select k,session_date,bar_start,interval_index,coverage_state,"
              "open,high,low,close,volume from sa.bars")
    nO = c.execute("select count(*) from O").fetchone()[0]
    nR = c.execute("select count(*) from R").fetchone()[0]
    d1 = c.execute("select count(*) from (select * from O except all select * from R)").fetchone()[0]
    d2 = c.execute("select count(*) from (select * from R except all select * from O)").fetchone()[0]
    kO = c.execute("select count(distinct k) from O").fetchone()[0]
    dO = c.execute("select count(distinct session_date) from O").fetchone()[0]
    res = dict(rebuilt_rows=nO, sealed_rows=nR, O_minus_R=d1, R_minus_O=d2,
               keys=kO, dates=dO, errors=len(errs), rejected_rows=rej,
               elapsed_s=round(time.time() - t0),
               VERDICT='EXACT' if (nO == nR and d1 == 0 and d2 == 0) else 'MISMATCH')
    print(json.dumps(res, indent=1), flush=True)
    json.dump(res, open(S + '/_a1b_regression.json', 'w'))
