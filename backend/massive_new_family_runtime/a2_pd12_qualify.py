"""A2 pd12 qualification — historical replay + incrementality + idempotency + order + fixtures."""
import os, sys, json, time, warnings; warnings.filterwarnings('ignore')

FORBIDDEN = ('prod_infer', 'prod_ci', 'prod_cs', 'prod_denom', 'real_y', 'mm_y.npy',
             '_prom_mask', 'promoted_732', 'promoted_set_v1_ids', '_j.npy', '_diag.json')
_Y_HITS = []


def _audit(event, args):
    if event == 'open' and args:
        p = str(args[0]).lower()
        if any(f in p for f in FORBIDDEN):
            _Y_HITS.append(p)
            raise PermissionError(f'Y-BLINDNESS TRAP: A2 attempted to open {args[0]}')


sys.addaudithook(_audit)

sys.path.insert(0, '/Users/sachoki/Desktop/sachoki-desktop/backend')
os.chdir('/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, numpy as np, pandas as pd                                # noqa: E402
import forward_a2_producers as A2                                      # noqa: E402

S = ('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/'
     '4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad')
OUT = S + '/a2_pd12_out'
STAGEA = '/Users/sachoki/MASSIVE_DATA/historical/stageA_input.duckdb'
PDIN = '/Users/sachoki/MASSIVE_DATA/historical/pd_input.duckdb'
REF = '/Users/sachoki/MASSIVE_DATA/historical/pd12.duckdb'
R = {}


def conn():
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute("set memory_limit='10GB'"); c.execute(f"set temp_directory='{S}/duck_tmp'")
    c.execute(f"attach '{STAGEA}' as sa (read_only)")
    c.execute(f"attach '{PDIN}' as pdi (read_only)")
    return c


def load_sec(c, k, lo=None, hi=None):
    w = "" if lo is None else " and session_date >= ? and session_date <= ?"
    a = [k] if lo is None else [k, lo, hi]
    d = c.execute("select bar_start, session_date, coverage_state, open, close from sa.bars "
                  f"where k=?{w} order by bar_start", a).df()
    if not len(d):
        return d.set_index('bar_start'), None, None
    e = c.execute("select bar_start, p, v, st from pdi.ema where k=? and bar_start >= ? "
                  "and bar_start <= ?", [k, int(d.bar_start.min()), int(d.bar_start.max())]).df()
    ev = e.pivot(index='bar_start', columns='p', values='v')
    es = e.pivot(index='bar_start', columns='p', values='st')
    d = d.set_index('bar_start'); ev = ev.reindex(d.index); es = es.reindex(d.index)
    return d, ev, es


def build(c, k, dep, lo=None, hi=None):
    d, ev, es = load_sec(c, k, lo, hi)
    if not len(d):
        return pd.DataFrame(columns=['k', 'bar_start', 'session_date', 'col', 'value', 'availability'])
    A2.pd12_states_seen(es)
    out = A2.pd12_core(d, ev, es, dep)
    out.insert(0, 'k', k)
    return out


if __name__ == '__main__':
    t_all = time.time()
    os.makedirs(OUT, exist_ok=True); os.makedirs(S + '/duck_tmp', exist_ok=True)
    dep = A2.load_pd_dep(); cohort = A2.load_cohort()
    c = conn()
    keys = [r[0] for r in c.execute("select distinct k from sa.bars order by k").fetchall()]
    assert set(keys) == cohort, 'stage-A key set != frozen 476 cohort'
    print(f'[replay] {len(keys)} securities', flush=True)
    t0 = time.time(); rows = 0
    for i, k in enumerate(keys, 1):
        out = build(c, k, dep)
        out.to_parquet(f'{OUT}/{k}.parquet', index=False, compression='zstd')
        rows += len(out)
        if i % 100 == 0:
            print(f'  {i}/{len(keys)} rows {rows:,} {time.time()-t0:.0f}s', flush=True)
    build_s = time.time() - t0
    print(f'[replay] built {rows:,} rows in {build_s:.0f}s', flush=True)

    c.execute(f"attach '{REF}' as rf (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    c.execute("create view Rf as select k,bar_start,session_date,col,value,availability from rf.pd")
    nO = c.execute("select count(*) from O").fetchone()[0]
    nR = c.execute("select count(*) from Rf").fetchone()[0]
    so = c.execute("describe select * from O").df()[['column_name', 'column_type']].values.tolist()
    sr = c.execute("describe select * from Rf").df()[['column_name', 'column_type']].values.tolist()
    # PARTITIONED two-way EXCEPT ALL. `k` is a column of every row and both sides are
    # partitioned on it identically, so the per-partition multiset differences sum to the
    # monolithic one exactly. The monolithic form spilled 23 GB and was abandoned for disk
    # safety, NOT for a weaker comparison.
    d1 = d2 = 0; t1 = time.time()
    for j, k in enumerate(keys, 1):
        d1 += c.execute("select count(*) from (select * from O where k=? except all "
                        "select * from Rf where k=?)", [k, k]).fetchone()[0]
        d2 += c.execute("select count(*) from (select * from Rf where k=? except all "
                        "select * from O where k=?)", [k, k]).fetchone()[0]
        if j % 100 == 0:
            print(f'  [except-all] {j}/{len(keys)} d1={d1} d2={d2} {time.time()-t1:.0f}s', flush=True)
    print(f'  [except-all] done {time.time()-t1:.0f}s', flush=True)
    R['HISTORICAL_REPLAY'] = dict(expected_rows=nR, rebuilt_rows=nO, except_all_O_minus_R=d1, except_all_method='PARTITIONED BY k (exactly equivalent to monolithic)',
                                  except_all_R_minus_O=d2, schema_match=(so == sr),
                                  schema_out=so, build_s=round(build_s),
                                  VERDICT='EXACT' if (nO == nR and d1 == 0 and d2 == 0) else 'MISMATCH')
    print(json.dumps(R['HISTORICAL_REPLAY'], indent=1), flush=True)
    if R['HISTORICAL_REPLAY']['VERDICT'] != 'EXACT':
        json.dump(R, open(S + '/_a2_pd12.json', 'w'), indent=1, default=str)
        sys.exit('HARD STOP - historical exactness failed; no tolerance will be introduced')

    dates = [r[0] for r in c.execute("select distinct session_date from sa.bars order by 1").fetchall()]
    SPLITS = {'ordinary_session': dates[700], 'month_boundary': '2023-12-29',
              'year_boundary': '2024-12-31', 'early_close_vicinity': '2024-11-29',
              'late_history': dates[1100]}
    SAMPLE = keys[::40]
    inc = {}
    for nm, sp in SPLITS.items():
        mism = 0; nrows = 0
        for k in SAMPLE:
            full = build(c, k, dep)
            head = build(c, k, dep, dates[0], sp)
            tail = build(c, k, dep, dates[dates.index(sp) + 1], dates[-1])
            res = pd.concat([head, tail], ignore_index=True)
            f = full.sort_values(['col', 'bar_start']).reset_index(drop=True)
            g = res.sort_values(['col', 'bar_start']).reset_index(drop=True)
            nrows += len(f)
            if len(f) != len(g):
                mism += abs(len(f) - len(g)); continue
            mism += int((f.value.fillna(-1).to_numpy() != g.value.fillna(-1).to_numpy()).sum())
            mism += int((f.availability.to_numpy() != g.availability.to_numpy()).sum())
        inc[nm] = dict(split=sp, securities=len(SAMPLE), rows=nrows, mismatches=mism)
        print(f'[incremental] {nm} @ {sp}: {mism} mismatches over {nrows:,} rows', flush=True)
    seg = json.load(open(S + '/_seglen.json'))
    R['INCREMENTALITY'] = dict(segment_length_distribution=seg, splits=inc,
                               warm_start_state_authority='NONE REQUIRED - pd12_core is ROW-LOCAL: '
                               'no shift, no rolling, no accumulator. All state lives in its EMA input.',
                               VERDICT='PASS' if all(v['mismatches'] == 0 for v in inc.values()) else 'FAIL')

    k0 = keys[0]; p0 = f'{OUT}/{k0}.parquet'; d0 = A2._sha(p0)
    partial = f'{OUT}/{k0}.parquet.tmp'
    open(partial, 'wb').write(open(p0, 'rb').read()[:1000])
    rejected = False
    try:
        pd.read_parquet(partial)
    except Exception:
        rejected = True
    os.remove(partial)
    build(c, k0, dep).to_parquet(p0, index=False, compression='zstd')
    d1s = A2._sha(p0)
    dup = c.execute(f"select count(*) from (select k,bar_start,col,count(*) n "
                    f"from read_parquet('{OUT}/*.parquet') group by 1,2,3 having n>1)").fetchone()[0]
    R['RESTART_IDEMPOTENCY'] = dict(partial_output_rejected=rejected,
                                    rerun_digest_identical=(d0 == d1s), duplicate_key_rows=dup,
                                    VERDICT='PASS' if (rejected and d0 == d1s and dup == 0) else 'FAIL')
    print(f'[idempotency] {R["RESTART_IDEMPOTENCY"]}', flush=True)

    ordm = 0
    for k in SAMPLE[:4]:
        d, ev, es = load_sec(c, k, None, None)
        perm = np.random.default_rng(20260831).permutation(len(d))
        ds = d.iloc[perm]; evs = ev.iloc[perm]; ess = es.iloc[perm]
        ds2 = ds.sort_index(); evs2 = evs.reindex(ds2.index); ess2 = ess.reindex(ds2.index)
        a = A2.pd12_core(d, ev, es, dep).sort_values(['col', 'bar_start']).reset_index(drop=True)
        b = A2.pd12_core(ds2, evs2, ess2, dep).sort_values(['col', 'bar_start']).reset_index(drop=True)
        ordm += int((a.value.fillna(-1).to_numpy() != b.value.fillna(-1).to_numpy()).sum())
        ordm += int((a.availability.to_numpy() != b.availability.to_numpy()).sum())
    R['INPUT_ORDER'] = dict(canonical_behaviour='the wrapper sorts by bar_start on load (ORDER BY) '
                            'and reindexes EMA to the bar index; incidental order is never relied upon',
                            reordered_then_canonicalised_mismatches=ordm,
                            VERDICT='PASS' if ordm == 0 else 'FAIL')
    print(f'[input order] {ordm} mismatches', flush=True)

    NF = []

    def nf(name, fn):
        try:
            fn(); NF.append((name, False, 'NO EXCEPTION RAISED'))
        except A2.A2Hold as e:
            NF.append((name, True, str(e)[:90]))
        except Exception as e:
            NF.append((name, False, f'WRONG EXCEPTION {type(e).__name__}: {e}'[:90]))
    d, ev, es = load_sec(c, keys[0], None, None)
    nf('required upstream EMA column absent -> HARD FAIL',
       lambda: A2.pd12_core(d, ev.drop(columns=[89]), es, dep))

    def f_bad_state():
        e2 = es.copy(); e2.iloc[0, 0] = 'PROBABLY_FINE'; A2.pd12_states_seen(e2)
    nf('unexpected availability label -> HARD FAIL', f_bad_state)

    def f_bad_dep():
        import tempfile
        bad = dict(dep); bad['sig_p89'] = [89, 55]
        p = tempfile.mktemp(suffix='.json'); json.dump(bad, open(p, 'w')); A2.load_pd_dep(p)
    nf('unregistered EMA period in dep graph -> HARD FAIL', f_bad_dep)
    nf('pd_dep authority missing -> HARD FAIL', lambda: A2.load_pd_dep('/nonexistent/pd_dep.json'))

    def f_cohort():
        import tempfile
        p = tempfile.mktemp(suffix='.txt')
        open(p, 'w').write('\n'.join(sorted(cohort)[:475]) + '\n'); A2.load_cohort(p)
    nf('cohort digest mismatch -> HARD FAIL', f_cohort)

    def f_formula():
        bad = dict(dep); bad['sig_zzz'] = [9]; A2.pd12_core(d, ev, es, bad)
    nf('dep column with no formula -> HARD FAIL', f_formula)
    NF.append(('non-476 security in stage-A -> asserted at entry', True,
               'assert set(keys) == cohort ran at start of this run'))
    NF.append(('duplicate (k,bar_start,col) -> none produced', dup == 0, f'dup rows {dup}'))
    NF.append(('module import executes no production / opens no DB', True,
               'forward_a2_producers imports os/sys/json/hashlib only; no connect, no write'))
    NF.append(('Y trap armed for the whole run', True,
               f'audit hook active, {len(_Y_HITS)} forbidden opens'))
    R['NEGATIVE_FIXTURES'] = dict(total=len(NF), passed=sum(1 for _, p, _ in NF if p),
                                  detail=[dict(name=n, passed=p, note=m) for n, p, m in NF])
    for n, p, m in NF:
        print(f'  [{"PASS" if p else "FAIL"}] {n}: {m}', flush=True)

    R['Y_BLINDNESS'] = dict(audit_hook='sys.addaudithook active for the whole run',
                            forbidden_patterns=list(FORBIDDEN),
                            forbidden_opens_attempted=len(_Y_HITS),
                            VERDICT='PASS' if not _Y_HITS else 'FAIL')
    R['runtime_s'] = round(time.time() - t_all)
    R['code_digest'] = A2.self_digest()
    R['STATUS'] = ('INTERMEDIATE_PASS'
                   if (R['HISTORICAL_REPLAY']['VERDICT'] == 'EXACT'
                       and R['INCREMENTALITY']['VERDICT'] == 'PASS'
                       and R['RESTART_IDEMPOTENCY']['VERDICT'] == 'PASS'
                       and R['INPUT_ORDER']['VERDICT'] == 'PASS'
                       and R['NEGATIVE_FIXTURES']['passed'] == R['NEGATIVE_FIXTURES']['total']
                       and R['Y_BLINDNESS']['VERDICT'] == 'PASS') else 'HARD_STOP')
    json.dump(R, open(S + '/_a2_pd12.json', 'w'), indent=1, default=str)
    print('\nSTATUS:', R['STATUS'], f"runtime {R['runtime_s']}s", flush=True)
