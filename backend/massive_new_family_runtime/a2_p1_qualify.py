"""A2 PHASE1_R1 qualification. Central gate: two WHOLE-SERIES reachable-state passes."""
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
OUT = S + '/a2_p1_out'
STAGEA = '/Users/sachoki/MASSIVE_DATA/historical/stageA_input.duckdb'
REF = '/Users/sachoki/MASSIVE_DATA/historical/PHASE1_R1.duckdb'
RUN = 'A2_P1_HISTORICAL_REPLAY'; FAM = 'PHASE1_R1'
R = {}
C = ['bar_start', 'session_date', 'coverage_state', 'open', 'high', 'low', 'close', 'volume']


def conn():
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute("set memory_limit='8GB'"); c.execute(f"set temp_directory='{S}/duck_tmp'")
    c.execute(f"attach '{STAGEA}' as sa (read_only)")
    return c


def bars(c, k):
    return c.execute(f"select {','.join(C)} from sa.bars where k=? order by bar_start", [k]).df()


def full_run(d):
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    return A2.phase1_core(d, A2.stagec4_head_mask(ok))[0]


def resume_run(d, split_i):
    """Window starts at the segment containing (split+1-LOOKBACK); the two reachable-state
    sets are carried to that point, exactly as a prior run would have checkpointed them."""
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    seg, _ = A2.stagec4_segments(ok)
    head = A2.stagec4_head_mask(ok)
    hi = np.flatnonzero(head)
    start = split_i + 1
    if start >= len(d):
        return d.iloc[0:0], None
    back = max(0, start - A2.P1_LOOKBACK)
    s = seg[back]
    w0 = int(np.flatnonzero(seg == s)[0])
    # checkpoint: run the head portion [0, w0) exactly as the prior run did
    if w0 == 0:
        tzS = vboS = vboM = None; pok = pcl = None
    else:
        _, st = A2.phase1_core(d.iloc[:w0].reset_index(drop=True), head[:w0], None, None, 0)
        tzS, vboS, vboM = set(st['tz_S']), set(st['vbo_S']), st['vbo_mat']
        pok = bool(ok[w0 - 1]); pcl = float(d.close.to_numpy()[w0 - 1])
    head_closed = bool(len(hi)) and hi[-1] < w0
    ishw = np.zeros(len(d) - w0, bool) if head_closed else head[w0:]
    out, _st = A2.phase1_core(d.iloc[w0:].reset_index(drop=True), ishw, tzS, vboS, w0,
                              vboM, pok, pcl)
    keep = set(d.bar_start.values[start:])
    return out[out.bar_start.isin(keep)].reset_index(drop=True), dict(
        window_start=w0, lookback_bars=start - w0, head_closed=head_closed,
        tz_state_carried=None if tzS is None else sorted(tzS),
        vbo_state_carried=None if vboS is None else len(vboS),
        vbo_materialised=None if vboM is None else len(vboM))


def cmp_frames(a, b):
    a = a.sort_values(['col', 'bar_start']).reset_index(drop=True)
    b = b.sort_values(['col', 'bar_start']).reset_index(drop=True)
    if len(a) != len(b):
        return abs(len(a) - len(b)), len(a)
    m = int((a.bval.fillna(-1).to_numpy() != b.bval.fillna(-1).to_numpy()).sum())
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
        d = bars(c, k)
        out = full_run(d); out.insert(0, 'k', k)
        A2.write_partition(OUT, k, out, RUN, FAM, len(out))
        rows += len(out)
        if i % 25 == 0:
            print(f'  {i}/{len(keys)} rows {rows:,} {time.time()-t0:.0f}s', flush=True)
    build_s = time.time() - t0
    c.execute(f"attach '{REF}' as rf (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    c.execute("create view Rf as select k,bar_start,session_date,col,bval,sval,availability from rf.p39")
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
        json.dump(R, open(S + '/_a2_p1.json', 'w'), indent=1, default=str)
        sys.exit('HARD STOP - PHASE1_R1 historical exactness failed')

    # ── stateful splits, with sig_tz_flip predecessor cases singled out ────
    SAMPLE = keys[::30]
    res = {'inside_head_run': [], 'before_first_contamination': [],
           'after_first_contamination': [], 'inside_later_segment': [], 'mid_open_segment': [],
           'tz_flip_predecessor_across_split': []}
    tzmm = 0; tz_cases = 0; tz_false_leak = 0
    for k in SAMPLE:
        d = bars(c, k)
        ok = (d.coverage_state == 'COMPLETE').to_numpy()
        if ok.sum() < 60:
            continue
        seg, age = A2.stagec4_segments(ok)
        head = A2.stagec4_head_mask(ok); hi = np.flatnonzero(head)
        cont = np.flatnonzero(~ok); cont = cont[cont > (hi[-1] if len(hi) else 0)]
        f = full_run(d)
        pts = {'inside_head_run': int(hi[len(hi) // 2]) if len(hi) >= 3 else None,
               'before_first_contamination': int(hi[-1]) if len(hi) and len(cont) else None,
               'after_first_contamination': int(cont[0]) if len(cont) else None}
        later = np.flatnonzero(ok & (np.arange(len(d)) > (cont[0] if len(cont) else 0)))
        pts['inside_later_segment'] = int(later[len(later) // 2]) if len(later) >= 2 else None
        openi = np.flatnonzero(seg == seg[-1])
        pts['mid_open_segment'] = int(openi[len(openi) // 2]) if len(openi) >= 3 else None
        # a split whose NEXT row has a genuinely available predecessor
        ff = f[f.col == 'sig_tz_flip']
        avail = ff.availability.to_numpy()
        cand = [i for i in range(60, len(d) - 2)
                if ok[i] and ok[i + 1] and avail[i] == 'AVAILABLE_VALID'
                and avail[i + 1] == 'AVAILABLE_VALID']
        pts['tz_flip_predecessor_across_split'] = cand[len(cand) // 2] if cand else None
        for nm, si in pts.items():
            if si is None or si >= len(d) - 1:
                continue
            r, meta = resume_run(d, si)
            exp = f[f.bar_start.isin(set(d.bar_start.values[si + 1:]))]
            m, tot = cmp_frames(exp, r)
            res[nm].append(dict(split=si, rows=tot, mismatches=m,
                                lookback=meta['lookback_bars'] if meta else None))
            if nm == 'tz_flip_predecessor_across_split':
                tz_cases += 1
                rr = r[r.col == 'sig_tz_flip']; ee = exp[exp.col == 'sig_tz_flip']
                rr = rr.sort_values('bar_start').reset_index(drop=True)
                ee = ee.sort_values('bar_start').reset_index(drop=True)
                tzmm += int((rr.bval.fillna(-1).to_numpy() != ee.bval.fillna(-1).to_numpy()).sum())
                tzmm += int((rr.availability.to_numpy() != ee.availability.to_numpy()).sum())
                # a resumed row must never be silently FALSE where the full run said INIT
                leak = ((ee.availability != 'AVAILABLE_VALID') & (rr.availability == 'AVAILABLE_VALID')
                        & (rr.bval.fillna(-1) == 0))
                tz_false_leak += int(leak.sum())
    R['STATEFUL_REPLAY'] = {nm: dict(cases=len(v), rows=sum(x['rows'] for x in v),
                                     mismatches=sum(x['mismatches'] for x in v))
                            for nm, v in res.items()}
    tot_m = sum(v['mismatches'] for v in R['STATEFUL_REPLAY'].values())
    R['STATEFUL_REPLAY']['TOTAL_MISMATCHES'] = tot_m
    R['STATEFUL_REPLAY']['VERDICT'] = 'PASS' if tot_m == 0 else 'FAIL'
    R['SIG_TZ_FLIP'] = dict(cases=tz_cases, mismatches=tzmm,
                            silent_false_where_full_run_said_unavailable=tz_false_leak,
                            VERDICT='PASS' if (tzmm == 0 and tz_false_leak == 0) else 'FAIL')
    print(json.dumps(R['STATEFUL_REPLAY'], indent=1), flush=True)
    print(json.dumps(R['SIG_TZ_FLIP'], indent=1), flush=True)
    R['runtime_s'] = round(time.time() - t_all); R['y_forbidden_opens'] = len(_Y)
    json.dump(R, open(S + '/_a2_p1.json', 'w'), indent=1, default=str)
    print(f"\ndone {R['runtime_s']}s", flush=True)
