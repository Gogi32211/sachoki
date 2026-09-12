"""A2 stageC4 qualification. Central gate: SEGMENT + HEAD-RUN equivalence under resume."""
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
OUT = S + '/a2_c4_out'
STAGEA = '/Users/sachoki/MASSIVE_DATA/historical/stageA_input.duckdb'
REF = '/Users/sachoki/MASSIVE_DATA/historical/stageC4.duckdb'
RUN = 'A2_C4_HISTORICAL_REPLAY'
FAM = 'stageC4'
R = {}
COLS = ['bar_start', 'session_date', 'coverage_state', 'open', 'high', 'low', 'close', 'volume']


def conn():
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute("set memory_limit='8GB'"); c.execute(f"set temp_directory='{S}/duck_tmp'")
    c.execute(f"attach '{STAGEA}' as sa (read_only)")
    return c


def bars(c, k):
    return c.execute(f"select {','.join(COLS)} from sa.bars where k=? order by bar_start",
                     [k]).df()


def full_run(d):
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    return A2.stagec4_core(d, A2.stagec4_head_mask(ok))


def resume_run(d, split_i):
    """Emit rows for positions > split_i, exactly as a forward append would.

    The window must begin at the START OF THE OPEN SEGMENT — compute_wlnbb uses
    rolling(20) and shift, so a truncated segment does not reproduce its own values.
    is_head is carried, never re-inferred: if the head run already ENDED at or before the
    split, the resumed window has no head bars at all."""
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    seg, _ = A2.stagec4_segments(ok)
    head_full = A2.stagec4_head_mask(ok)
    # checkpoint carried from the head portion — frozen X state only
    head_idx = np.flatnonzero(head_full)
    head_closed = bool(len(head_idx)) and head_idx[-1] <= split_i
    head_seg = seg[head_idx[0]] if len(head_idx) else -1
    # open segment at the resume point
    start = split_i + 1
    if start >= len(d):
        return d.iloc[0:0], None
    s = seg[start]
    seg_start = int(np.flatnonzero(seg == s)[0])
    win = d.iloc[seg_start:].reset_index(drop=True)
    okw = (win.coverage_state == 'COMPLETE').to_numpy()
    segw, _ = A2.stagec4_segments(okw)
    if head_closed:
        ish = np.zeros(len(win), bool)
    else:
        ish = okw & (segw == segw[np.argmax(okw)]) if okw.any() else np.zeros(len(win), bool)
        if s != head_seg:
            ish = np.zeros(len(win), bool)
    out = A2.stagec4_core(win, ish)
    keep = set(d.bar_start.values[start:])
    return out[out.bar_start.isin(keep)].reset_index(drop=True), dict(
        head_closed=head_closed, seg_start_offset=int(start - seg_start))


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
    # ── segment / head census ──────────────────────────────────────────────
    segstats = dict(total_segments=0, head_len=[], open_len=[])
    print('[census]', flush=True)
    for k in keys:
        ok = (bars(c, k).coverage_state == 'COMPLETE').to_numpy()
        seg, _ = A2.stagec4_segments(ok)
        if not ok.any():
            continue
        u, cnt = np.unique(seg[ok], return_counts=True)
        segstats['total_segments'] += len(u)
        segstats['head_len'].append(int(cnt[0]))
        segstats['open_len'].append(int(cnt[-1]) if seg[-1] == u[-1] else 0)
    H = np.array(segstats['head_len']); O = np.array(segstats['open_len'])
    R['SEGMENTS'] = dict(total_segments=segstats['total_segments'],
                         head_run_len={f'p{q}': float(np.percentile(H, q)) for q in (0, 25, 50, 75, 90, 100)},
                         head_run_mean=float(H.mean()),
                         head_runs_ge_2_bars=int((H >= 2).sum()), securities=len(H),
                         open_segment_len={f'p{q}': float(np.percentile(O[O > 0], q)) for q in (0, 50, 90, 100)},
                         securities_with_open_segment=int((O > 0).sum()))
    print(json.dumps(R['SEGMENTS'], indent=1), flush=True)

    # ── historical replay ──────────────────────────────────────────────────
    print('[replay]', flush=True)
    t0 = time.time(); rows = 0
    for i, k in enumerate(keys, 1):
        d = bars(c, k)
        out = full_run(d); out.insert(0, 'k', k)
        A2.write_partition(OUT, k, out, RUN, FAM, len(out))
        rows += len(out)
        if i % 50 == 0:
            print(f'  {i}/{len(keys)} rows {rows:,} {time.time()-t0:.0f}s', flush=True)
    build_s = time.time() - t0
    c.execute(f"attach '{REF}' as rf (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    c.execute("create view Rf as select k,bar_start,session_date,col,bval,sval,availability from rf.c4")
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
    print(json.dumps({k2: v for k2, v in R['HISTORICAL_REPLAY'].items() if k2 != 'schema_out'}, indent=1), flush=True)
    if R['HISTORICAL_REPLAY']['VERDICT'] != 'EXACT':
        json.dump(R, open(S + '/_a2_c4.json', 'w'), indent=1, default=str)
        sys.exit('HARD STOP - stageC4 historical exactness failed')

    # ── STATEFUL REPLAY: splits at the three required places ───────────────
    SAMPLE = [k for k in keys[::20]]
    sp_res = {'inside_head_run': [], 'immediately_before_first_contamination': [],
              'immediately_after_first_contamination': [], 'inside_a_later_segment': [],
              'mid_open_segment': []}
    falsehead = 0
    for k in SAMPLE:
        d = bars(c, k)
        ok = (d.coverage_state == 'COMPLETE').to_numpy()
        if not ok.any():
            continue
        seg, _ = A2.stagec4_segments(ok)
        head = A2.stagec4_head_mask(ok)
        hi = np.flatnonzero(head)
        if len(hi) < 2:
            continue
        cont = np.flatnonzero(~ok)
        cont = cont[cont > hi[-1]]
        pts = {}
        pts['inside_head_run'] = int(hi[len(hi) // 2]) if len(hi) >= 3 else None
        pts['immediately_before_first_contamination'] = int(hi[-1]) if len(cont) else None
        pts['immediately_after_first_contamination'] = int(cont[0]) if len(cont) else None
        later = np.flatnonzero(ok & (np.arange(len(d)) > (cont[0] if len(cont) else 0)))
        pts['inside_a_later_segment'] = int(later[len(later) // 2]) if len(later) >= 2 else None
        openi = np.flatnonzero(seg == seg[-1])
        pts['mid_open_segment'] = int(openi[len(openi) // 2]) if len(openi) >= 3 else None
        f = full_run(d)
        for nm, si in pts.items():
            if si is None or si >= len(d) - 1:
                continue
            r, meta = resume_run(d, si)
            exp = f[f.bar_start.isin(set(d.bar_start.values[si + 1:]))]
            m, tot = cmp_frames(exp, r)
            sp_res[nm].append(dict(security=k[:10], split_pos=si, rows=tot, mismatches=m,
                                   head_closed=meta['head_closed'] if meta else None,
                                   replay_from_seg_start_extra_bars=meta['seg_start_offset'] if meta else None))
            if meta and meta['head_closed']:
                rr = r[r.col.str.startswith('sig_cisd_')]
                falsehead += int((rr.availability == 'AVAILABLE_VALID').sum())
    R['STATEFUL_REPLAY'] = {nm: dict(cases=len(v), total_rows=sum(x['rows'] for x in v),
                                     mismatches=sum(x['mismatches'] for x in v))
                            for nm, v in sp_res.items()}
    R['HEAD_RUN_AUTHORITY'] = dict(
        later_segment_false_head_valid_cisd_rows=falsehead,
        rule='is_head is SUPPLIED by the orchestrator from a carried checkpoint, never '
             're-inferred inside a window; a resumed window whose head run already closed '
             'gets an all-False mask',
        VERDICT='PASS' if falsehead == 0 else 'FAIL')
    tot_m = sum(v['mismatches'] for v in R['STATEFUL_REPLAY'].values())
    R['STATEFUL_REPLAY']['TOTAL_MISMATCHES'] = tot_m
    R['STATEFUL_REPLAY']['VERDICT'] = 'PASS' if tot_m == 0 else 'FAIL'
    print(json.dumps(R['STATEFUL_REPLAY'], indent=1), flush=True)
    print(json.dumps(R['HEAD_RUN_AUTHORITY'], indent=1), flush=True)
    json.dump(R, open(S + '/_a2_c4.json', 'w'), indent=1, default=str)
    R['runtime_s'] = round(time.time() - t_all)
    R['y_forbidden_opens'] = len(_Y)
    json.dump(R, open(S + '/_a2_c4.json', 'w'), indent=1, default=str)
    print(f"\nphase1 done, runtime {R['runtime_s']}s", flush=True)
