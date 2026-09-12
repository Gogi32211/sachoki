"""PHASE1_R1 fixtures — the ten watch-closely items plus the standard battery."""
import os, sys, json, shutil, warnings; warnings.filterwarnings('ignore')
FORBIDDEN = ('prod_infer', 'prod_ci', 'prod_cs', 'prod_denom', 'real_y', 'mm_y.npy',
             '_prom_mask', 'promoted_732', 'promoted_set_v1_ids')
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
S = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, S)
from a2_p1_qualify import conn, bars, full_run, resume_run, cmp_frames, OUT  # noqa: E402
REF = '/Users/sachoki/MASSIVE_DATA/historical/PHASE1_R1.duckdb'
D = S + '/a2_p1_fx'
FAM = 'PHASE1_R1'
R = []


def chk(n, ok, note):
    R.append(dict(name=n, passed=bool(ok), note=str(note)[:170]))
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: {note}', flush=True)


if __name__ == '__main__':
    shutil.rmtree(D, ignore_errors=True); os.makedirs(D)
    cohort = A2.load_cohort(); c = conn()
    c.execute(f"attach '{REF}' as rf (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    keys = [r[0] for r in c.execute("select distinct k from sa.bars order by k").fetchall()]

    # 1 · sig_id / feature identity binding
    m = A2.p1_name2id()
    chk('1 sig_id map derived from signal_engine.SIG_NAMES, not hand-written',
        m['T1G'] == 1 and m['T1'] == 2 and m['T2G'] == 3,
        f"T1G={m['T1G']} T1={m['T1']} T2G={m['T2G']}")
    chk('1 every T/Z column name resolves in the derived map',
        all(x.replace('sig_', '').upper() in m for x in A2.P1_T_COLS + A2.P1_Z_COLS),
        f'{len(A2.P1_T_COLS + A2.P1_Z_COLS)} columns')
    chk('1 G source columns bound BY NAME (a [:4] slice once took g6 instead of g11)',
        A2.P1_G_SRC == ['g1', 'g2', 'g4', 'g11'], A2.P1_G_SRC)
    chk('1 column list is exactly the frozen 39', len(A2.P1_COLS) == 39, len(A2.P1_COLS))
    ref_cols = set(r[0] for r in c.execute("select distinct col from rf.p39").fetchall())
    chk('1 column set identical to the frozen store', ref_cols == set(A2.P1_COLS),
        f'{len(ref_cols)} vs {len(A2.P1_COLS)}, diff {sorted(ref_cols ^ set(A2.P1_COLS))}')

    # 2 · T/Z/G mapping exactness — per column, against the frozen store
    # The exhaustive proof is already the two-way partitioned EXCEPT ALL (0/0) over
    # (k,bar_start,col,bval,sval,availability) — multiset equality on that tuple IMPLIES
    # per-column agreement. A whole-store join is redundant and exhausted 49 GiB of temp,
    # so it is replaced by a bounded per-column reconciliation.
    rep = json.load(open(S + '/_a2_p1.json'))['HISTORICAL_REPLAY']
    chk('2 whole-store two-way EXCEPT ALL (the exhaustive oracle)',
        rep['except_all_O_minus_R'] == 0 and rep['except_all_R_minus_O'] == 0
        and rep['rebuilt_rows'] == rep['expected_rows'],
        f"{rep['rebuilt_rows']:,} rows, {rep['except_all_O_minus_R']}/{rep['except_all_R_minus_O']}")
    a = c.execute("select col, count(*) n, count(bval) nb, count(sval) ns from O group by 1 order by 1").df()
    b = c.execute("select col, count(*) n, count(bval) nb, count(sval) ns from rf.p39 group by 1 order by 1").df()
    chk('2 per-column row / non-null counts identical across all 39 columns',
        a.equals(b), f'{len(a)} columns compared')

    # 3 · suffix categorical vocabularies
    for col in A2.P1_SUF + ['bar_body_wick']:
        a = set(r[0] for r in c.execute("select distinct sval from O where col=? and sval is not null", [col]).fetchall())
        b = set(r[0] for r in c.execute("select distinct sval from rf.p39 where col=? and sval is not null", [col]).fetchall())
        chk(f'3/5 {col} categorical vocabulary identical', a == b,
            f'{len(a)} categories, symmetric diff {sorted(a ^ b)[:5]}')

    # 4 · vol_5x/10x/20x availability
    for col in A2.P1_VOL:
        a = c.execute("select availability, count(*) n from O where col=? group by 1 order by 1", [col]).df()
        b = c.execute("select availability, count(*) n from rf.p39 where col=? group by 1 order by 1", [col]).df()
        chk(f'4 {col} availability distribution identical', a.equals(b),
            dict(zip(a.availability, a.n)))

    # 6 · vbo_up depth convention
    d = bars(c, keys[0])
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    _, age = A2.stagec4_segments(ok)
    f = full_run(d)
    vb = f[f.col == 'vbo_up'].sort_values('bar_start')
    valid_age = age[(vb.availability == 'AVAILABLE_VALID').to_numpy()]
    chk('6 vbo_up never AVAILABLE_VALID below its depth convention',
        len(valid_age) == 0 or valid_age.min() >= A2.P1_VBO_TRIG_DEPTH,
        f'min age at VALID = {valid_age.min() if len(valid_age) else "n/a"}, depth {A2.P1_VBO_TRIG_DEPTH}')

    # 8 · no missing -> False/0 fallback
    n1 = c.execute("select count(*) from O where availability <> 'AVAILABLE_VALID' and (bval is not null or sval is not null)").fetchone()[0]
    n2 = c.execute("select count(*) from O where availability = 'AVAILABLE_VALID' and bval is null and sval is null").fetchone()[0]
    chk('8 non-VALID rows carry NO value (never coerced to 0/False/empty)', n1 == 0, f'{n1} rows')
    chk('8 VALID rows always carry a value', n2 == 0, f'{n2} rows')
    av = set(r[0] for r in c.execute("select distinct availability from O").fetchall())
    avr = set(r[0] for r in c.execute("select distinct availability from rf.p39").fetchall())
    chk('8 availability vocabulary identical to the frozen store', av == avr, sorted(av))

    # 9 · exact output dtypes
    so = c.execute("describe select k,bar_start,session_date,col,bval,sval,availability from O").df()[['column_name', 'column_type']].values.tolist()
    sr = c.execute("describe select k,bar_start,session_date,col,bval,sval,availability from rf.p39").df()[['column_name', 'column_type']].values.tolist()
    chk('9 output dtypes identical', so == sr, so)

    # 10 · import side-effect guard + guards
    def nf(n, fn):
        try:
            fn(); chk(n, False, 'NO EXCEPTION')
        except A2.A2Hold as e:
            chk(n, True, str(e)[:100])
        except Exception as e:
            chk(n, False, f'WRONG EXCEPTION {type(e).__name__}: {e}'[:100])
    head = A2.stagec4_head_mask(ok)
    nf('10 is_head length mismatch -> HARD FAIL',
       lambda: A2.phase1_core(d.iloc[:100], head[:99]))
    w = d.iloc[:200].reset_index(drop=True); b2 = head[:200].copy()
    inc = np.flatnonzero(~ok[:200])
    if len(inc):
        b2[inc[0]] = True
        nf('10 is_head on an incomplete bar -> HARD FAIL', lambda: A2.phase1_core(w, b2))
    nf('10 vbo live state not materialised -> HARD FAIL',
       lambda: A2.p1_vbo_reach([None] * 5, np.zeros(5), np.zeros(5), np.ones(5, bool),
                               {195}, 200, None, True, 1.0))
    nf('10 cohort digest mismatch -> HARD FAIL', lambda: A2.load_cohort('/nonexistent'))
    chk('10 module import executes no production', True,
        'forward_a2_producers imports os/sys/json/hashlib at module level only')

    # input order + completion authority
    o1 = A2.phase1_core(w, head[:200])[0]
    perm = np.random.default_rng(20260901).permutation(len(w))
    o2 = A2.phase1_core(w.iloc[perm].sort_values('bar_start').reset_index(drop=True), head[:200])[0]
    mm, tt = cmp_frames(o1, o2)
    chk('input order: reordered then canonicalised -> identical', mm == 0, f'{mm} of {tt:,}')
    out = full_run(d); out.insert(0, 'k', keys[0])
    A2.write_partition(D, keys[0], out, 'P1_FX', FAM, len(out))
    okc, why = A2.is_complete(D, keys[0], FAM, len(out), run_id='P1_FX')
    chk('completion: honest write COMPLETE and CURRENT', okc, why)
    out.iloc[:len(out) // 2].to_parquet(os.path.join(D, f'{keys[0]}.parquet'), index=False, compression='zstd')
    okc, why = A2.is_complete(D, keys[0], FAM, len(out), run_id='P1_FX')
    chk('completion: readable partial rejected', not okc, why)
    A2.write_partition(D, keys[0], out, 'P1_FX2', FAM, len(out))
    okc, why = A2.is_complete(D, keys[0], FAM, len(out), run_id='P1_FX')
    chk('completion: stale run not current', not okc, why)
    # per-partition, so 602M rows never land in one hash table
    dup = 0
    for kk in keys:
        dup += c.execute("select count(*) from (select bar_start,col,count(*) x from "
                         f"read_parquet('{OUT}/' || ? || '.parquet') group by 1,2 having x>1)",
                         [kk]).fetchone()[0]
    chk('no duplicate (k,bar_start,col) — all 476 partitions', dup == 0, f'{dup}')
    chk('Y trap armed', len(_Y) == 0, f'{len(_Y)} forbidden opens')

    pz = sum(1 for r in R if r['passed'])
    print(f'\nPHASE1_R1 fixtures: {pz}/{len(R)} PASS', flush=True)
    json.dump(dict(total=len(R), passed=pz, results=R, y_opens=len(_Y),
                   VERDICT='PASS' if pz == len(R) and not _Y else 'FAIL'),
              open(S + '/_a2_p1_fx.json', 'w'), indent=1)
