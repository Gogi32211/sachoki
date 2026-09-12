"""A2 STAGE_D_R2 qualification. Central gate: sig_buy COOLDOWN continuity across resume."""
import os, sys, json, time, warnings; warnings.filterwarnings('ignore')
FORBIDDEN = ('prod_infer', 'prod_ci', 'prod_cs', 'prod_denom', 'real_y', 'mm_y.npy',
             '_prom_mask', 'promoted_732', 'promoted_set_v1_ids', '_j.npy', '_diag.json')
_Y = []


def _audit(e, a):
    if e == 'open' and a:
        p = str(a[0]).lower()
        if any(f in p for f in FORBIDDEN):
            _Y.append(p); raise PermissionError('Y-BLINDNESS TRAP')


sys.addaudithook(_audit)
sys.path.insert(0, '/Users/sachoki/Desktop/sachoki-desktop/backend')
os.chdir('/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, numpy as np, pandas as pd                                # noqa: E402
import forward_a2_producers as A2                                      # noqa: E402

S = ('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/'
     '4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad')
OUT = S + '/a2_d6_out'
STAGEA = '/Users/sachoki/MASSIVE_DATA/historical/stageA_input.duckdb'
PDIN = '/Users/sachoki/MASSIVE_DATA/historical/pd_input.duckdb'
REF = '/Users/sachoki/MASSIVE_DATA/historical/STAGE_D_R2.duckdb'
RUN = 'A2_D6_HISTORICAL_REPLAY'; FAM = 'STAGE_D_R2'
R = {}
C = ['bar_start', 'session_date', 'coverage_state', 'open', 'high', 'low', 'close', 'volume']


def conn():
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute("set memory_limit='8GB'"); c.execute(f"set temp_directory='{S}/duck_tmp'")
    c.execute(f"attach '{STAGEA}' as sa (read_only)")
    c.execute(f"attach '{PDIN}' as pdi (read_only)")
    return c


def load(c, k):
    d = c.execute(f"select {','.join(C)} from sa.bars where k=? order by bar_start", [k]).df()
    e = c.execute("select bar_start,p,st from pdi.ema where k=? and p in (9,20,50)", [k]).df()
    es = e.pivot(index='bar_start', columns='p', values='st').reindex(d.bar_start.values)
    return d, es.reset_index(drop=True)


def full_run(d, es):
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    return A2.staged6_core(d, es, A2.stagec4_head_mask(ok))[0]


def resume_run(d, es, split_i):
    """Window begins at the open segment start; the COOLDOWN STATE SET and the previous
    bar's EMA validity are carried. Nothing is inferred from elapsed bars."""
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    seg, _ = A2.stagec4_segments(ok)
    head = A2.stagec4_head_mask(ok); hi = np.flatnonzero(head)
    start = split_i + 1
    if start >= len(d):
        return d.iloc[0:0], None
    w0 = int(np.flatnonzero(seg == seg[start])[0])
    if w0 == 0:
        cdS = None; ep0 = None
    else:
        _, st = A2.staged6_core(d.iloc[:w0].reset_index(drop=True), es.iloc[:w0], head[:w0])
        cdS = set(st['cd_S'])
        ep0 = {p: bool(es[p].to_numpy()[w0 - 1] == 'VALID') for p in (9, 20, 50)}
    head_closed = bool(len(hi)) and hi[-1] < w0
    ishw = np.zeros(len(d) - w0, bool) if head_closed else head[w0:]
    out, _ = A2.staged6_core(d.iloc[w0:].reset_index(drop=True), es.iloc[w0:].reset_index(drop=True),
                            ishw, cdS, w0, ep0)
    keep = set(d.bar_start.values[start:])
    return out[out.bar_start.isin(keep)].reset_index(drop=True), dict(
        window_start=w0, cooldown_state_carried=None if cdS is None else sorted(cdS),
        eprev_carried=ep0, head_closed=head_closed)


def cmp_frames(a, b):
    a = a.sort_values(['col', 'bar_start']).reset_index(drop=True)
    b = b.sort_values(['col', 'bar_start']).reset_index(drop=True)
    if len(a) != len(b):
        return abs(len(a) - len(b)), len(a)
    m = int((a.bval.fillna(-1).to_numpy() != b.bval.fillna(-1).to_numpy()).sum())
    m += int((a.dval.fillna(-9e9).to_numpy() != b.dval.fillna(-9e9).to_numpy()).sum())
    m += int((a.sval.fillna('~').to_numpy() != b.sval.fillna('~').to_numpy()).sum())
    m += int((a.availability.to_numpy() != b.availability.to_numpy()).sum())
    return m, len(a)


if __name__ == '__main__':
    t_all = time.time()
    os.makedirs(OUT, exist_ok=True); os.makedirs(S + '/duck_tmp', exist_ok=True)
    cohort = A2.load_cohort(); c = conn()
    keys = [r[0] for r in c.execute("select distinct k from sa.bars order by k").fetchall()]
    assert set(keys) == cohort
    print('[replay]', flush=True)
    t0 = time.time(); rows = 0
    for i, k in enumerate(keys, 1):
        d, es = load(c, k)
        out = full_run(d, es); out.insert(0, 'k', k)
        A2.write_partition(OUT, k, out, RUN, FAM, len(out))
        rows += len(out)
        if i % 50 == 0:
            print(f'  {i}/{len(keys)} rows {rows:,} {time.time()-t0:.0f}s', flush=True)
    build_s = time.time() - t0
    c.execute(f"attach '{REF}' as rf (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    c.execute("create view Rf as select k,bar_start,session_date,col,bval,dval,sval,availability from rf.d6")
    nO = c.execute("select count(*) from O").fetchone()[0]
    nR = c.execute("select count(*) from Rf").fetchone()[0]
    so = c.execute("describe select * from O").df()[['column_name', 'column_type']].values.tolist()
    sr = c.execute("describe select * from Rf").df()[['column_name', 'column_type']].values.tolist()
    d1 = d2 = 0
    for j, k in enumerate(keys, 1):
        d1 += c.execute("select count(*) from (select * from O where k=? except all select * from Rf where k=?)", [k, k]).fetchone()[0]
        d2 += c.execute("select count(*) from (select * from Rf where k=? except all select * from O where k=?)", [k, k]).fetchone()[0]
        if j % 150 == 0:
            print(f'  [except-all] {j}/{len(keys)} d1={d1} d2={d2}', flush=True)
    R['HISTORICAL_REPLAY'] = dict(expected_rows=nR, rebuilt_rows=nO, except_all_O_minus_R=d1,
                                  except_all_R_minus_O=d2, method='PARTITIONED BY k',
                                  schema_match=(so == sr), schema_out=so, build_s=round(build_s),
                                  VERDICT='EXACT' if (nO == nR and d1 == 0 and d2 == 0) else 'MISMATCH')
    print(json.dumps({a: b for a, b in R['HISTORICAL_REPLAY'].items() if a != 'schema_out'}, indent=1), flush=True)
    if R['HISTORICAL_REPLAY']['VERDICT'] != 'EXACT':
        json.dump(R, open(S + '/_a2_d6.json', 'w'), indent=1, default=str)
        sys.exit('HARD STOP - STAGE_D_R2 historical exactness failed')

    # ══ COOLDOWN CONTINUITY ════════════════════════════════════════════════
    SAMPLE = keys[::20]
    res = {'immediately_before_sig_buy_fire': [], 'immediately_after_fire': [],
           'inside_cooldown': [], 'exactly_at_cooldown_release': [],
           'across_contaminated_bars': [], 'inside_long_clean_run': []}
    carried_states = []
    for k in SAMPLE:
        d, es = load(c, k)
        ok = (d.coverage_state == 'COMPLETE').to_numpy()
        if ok.sum() < 120:
            continue
        f = full_run(d, es)
        sb = f[f.col == 'sig_buy'].sort_values('bar_start').reset_index(drop=True)
        fires = np.flatnonzero((sb.availability == 'AVAILABLE_VALID').to_numpy()
                               & (sb.bval.fillna(0).to_numpy() == 1))
        seg, age = A2.stagec4_segments(ok)
        pts = {}
        if len(fires):
            fi = int(fires[len(fires) // 2])
            pts['immediately_before_sig_buy_fire'] = fi - 1
            pts['immediately_after_fire'] = fi
            if fi + 3 < len(d):
                pts['inside_cooldown'] = fi + 3
            if fi + A2.D6_N_CD < len(d):
                pts['exactly_at_cooldown_release'] = fi + A2.D6_N_CD
        cont = np.flatnonzero(~ok)
        if len(cont):
            pts['across_contaminated_bars'] = int(cont[len(cont) // 2])
        runs = np.flatnonzero(seg == seg[-1])
        if len(runs) >= 5:
            pts['inside_long_clean_run'] = int(runs[len(runs) // 2])
        for nm, si in pts.items():
            if si is None or si < 1 or si >= len(d) - 1:
                continue
            r, meta = resume_run(d, es, si)
            exp = f[f.bar_start.isin(set(d.bar_start.values[si + 1:]))]
            m, tot = cmp_frames(exp, r)
            res[nm].append(dict(split=si, rows=tot, mismatches=m))
            if meta and meta['cooldown_state_carried'] is not None:
                carried_states.append(tuple(meta['cooldown_state_carried']))
    R['COOLDOWN_CONTINUITY'] = {nm: dict(cases=len(v), rows=sum(x['rows'] for x in v),
                                         mismatches=sum(x['mismatches'] for x in v))
                                for nm, v in res.items()}
    tot_m = sum(v['mismatches'] for v in R['COOLDOWN_CONTINUITY'].values())
    R['COOLDOWN_CONTINUITY']['TOTAL_MISMATCHES'] = tot_m
    R['COOLDOWN_CONTINUITY']['VERDICT'] = 'PASS' if tot_m == 0 else 'FAIL'
    from collections import Counter
    R['CARRIED_STATE_SHAPES'] = {str(list(s)): n for s, n in Counter(carried_states).most_common(10)}
    print(json.dumps(R['COOLDOWN_CONTINUITY'], indent=1), flush=True)
    print('carried cooldown state sets observed:', R['CARRIED_STATE_SHAPES'], flush=True)
    R['runtime_s'] = round(time.time() - t_all); R['y_forbidden_opens'] = len(_Y)
    json.dump(R, open(S + '/_a2_d6.json', 'w'), indent=1, default=str)
    print(f"\ndone {R['runtime_s']}s", flush=True)
