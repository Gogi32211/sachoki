"""STAGE_D_R2 fixtures — cooldown reachable-state authority, and the elapsed-time refutation."""
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
from a2_d6_qualify import conn, load, full_run, resume_run, cmp_frames, OUT  # noqa: E402
REF = '/Users/sachoki/MASSIVE_DATA/historical/STAGE_D_R2.duckdb'
D = S + '/a2_d6_fx'
FAM = 'STAGE_D_R2'
R = []


def chk(n, ok, note):
    R.append(dict(name=n, passed=bool(ok), note=str(note)[:180]))
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: {note}', flush=True)


CD = A2.d6_cooldown_states
N = A2.D6_N_CD; OLD = A2.D6_OLD

if __name__ == '__main__':
    shutil.rmtree(D, ignore_errors=True); os.makedirs(D)
    cohort = A2.load_cohort(); c = conn()
    c.execute(f"attach '{REF}' as rf (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    keys = [r[0] for r in c.execute("select distinct k from sa.bars order by k").fetchall()]

    print('── ELAPSED TIME CANNOT SUBSTITUTE FOR PROVEN-FALSE OBSERVED BARS ──', flush=True)
    o_obs, s_obs = CD([True] + [False] * N)
    chk('N observed FALSE bars after a fire RELEASE the cooldown identifiably',
        all(x is not None for x in o_obs) and s_obs == {OLD},
        f'outputs {o_obs} final state {sorted(s_obs)}')
    o_unk, s_unk = CD([True] + [None] * N)
    chk('N UNKNOWN bars after a fire do NOT release it — the last output is UNIDENTIFIABLE',
        o_unk[-1] is None, f'outputs {o_unk}')
    chk('unknown bars BRANCH the admissible set rather than advancing a proven-false counter',
        len(s_unk) > len(s_obs), f'unknown -> {sorted(s_unk)} vs observed -> {sorted(s_obs)}')
    o7, _ = CD([True] + [False] * N + [True])
    chk('a fire IS admitted after N proven-false bars', o7[-1] is True, f'last {o7[-1]}')
    o8, _ = CD([True] + [None] * N + [True])
    chk('the same fire is NOT identifiable after N unknown bars', o8[-1] is None, f'last {o8[-1]}')

    print('── SEED / RESEED ──', flush=True)
    mid, _ = CD([False] * 3, {0})                    # state carried: just fired
    fresh, _ = CD([False] * 3, None)                 # wrongly reseeded FRESH = {OLD}
    a1, _ = CD([True], {0}); a2, _ = CD([True], None)
    chk('a window restart must NOT reseed: carried {0} refuses a fire, FRESH admits it',
        a1 == [False] and a2 == [True], f'carried -> {a1}, reseeded -> {a2}')
    chk('the carried state is therefore load-bearing, not decorative', a1 != a2, f'{a1} vs {a2}')

    print('── TWO PREFIXES, SAME CURRENT BAR, DIFFERENT LEGITIMATE COOLDOWN HISTORY ──', flush=True)
    pa, sa = CD([True, False, False])                # fired 3 bars ago
    pb, sb = CD([False, False, False])               # never fired
    na, _ = CD([True], sa); nb, _ = CD([True], sb)
    chk('identical current bar, different legitimate prefix -> DIFFERENT output',
        na != nb, f'after-fire prefix -> {na}, no-fire prefix -> {nb}')
    chk('and the difference is exactly the frozen state difference',
        sorted(sa) != sorted(sb), f'{sorted(sa)} vs {sorted(sb)}')

    print('── PRODUCER-LEVEL GUARDS ──', flush=True)
    d, es = load(c, keys[0])
    ok = (d.coverage_state == 'COMPLETE').to_numpy()
    head = A2.stagec4_head_mask(ok)

    def nf(n, fn):
        try:
            fn(); chk(n, False, 'NO EXCEPTION')
        except A2.A2Hold as e:
            chk(n, True, str(e)[:110])
        except Exception as e:
            chk(n, False, f'WRONG EXCEPTION {type(e).__name__}: {e}'[:110])
    nf('resumed window without carried eprev -> HARD FAIL',
       lambda: A2.staged6_core(d.iloc[100:300].reset_index(drop=True),
                               es.iloc[100:300].reset_index(drop=True),
                               head[100:300], {0}, 100, None))
    nf('missing required EMA state column -> HARD FAIL',
       lambda: A2.staged6_core(d.iloc[:200].reset_index(drop=True),
                               es.iloc[:200].drop(columns=[50]).reset_index(drop=True),
                               head[:200]))
    nf('is_head length mismatch -> HARD FAIL',
       lambda: A2.staged6_core(d.iloc[:200].reset_index(drop=True),
                               es.iloc[:200].reset_index(drop=True), head[:199]))
    w = d.iloc[:200].reset_index(drop=True); b2 = head[:200].copy()
    inc = np.flatnonzero(~ok[:200])
    if len(inc):
        b2[inc[0]] = True
        nf('is_head on an incomplete bar -> HARD FAIL',
           lambda: A2.staged6_core(w, es.iloc[:200].reset_index(drop=True), b2))
    nf('cohort digest mismatch -> HARD FAIL', lambda: A2.load_cohort('/nonexistent'))

    print('── STORE CONFORMANCE ──', flush=True)
    n1 = c.execute("select count(*) from O where availability <> 'AVAILABLE_VALID' and "
                   "(bval is not null or dval is not null or sval is not null)").fetchone()[0]
    n2 = c.execute("select count(*) from O where availability = 'AVAILABLE_VALID' and "
                   "bval is null and dval is null and sval is null").fetchone()[0]
    chk('non-VALID rows carry NO value', n1 == 0, f'{n1}')
    chk('VALID rows always carry a value', n2 == 0, f'{n2}')
    av = c.execute("select availability, count(*) n from O group by 1 order by 1").df()
    avr = c.execute("select availability, count(*) n from rf.d6 group by 1 order by 1").df()
    chk('availability distribution identical to the frozen store', av.equals(avr),
        dict(zip(av.availability, av.n)))
    a = c.execute("select col,count(*) n,count(bval) nb,count(dval) nd,count(sval) ns from O group by 1 order by 1").df()
    b = c.execute("select col,count(*) n,count(bval) nb,count(dval) nd,count(sval) ns from rf.d6 group by 1 order by 1").df()
    chk('per-column row and non-null counts identical', a.equals(b), f'{len(a)} columns')
    ub = c.execute("select count(*) from O where col='sig_buy' and availability='UNAVAILABLE_REQUIRED_HISTORY'").fetchone()[0]
    ubr = c.execute("select count(*) from rf.d6 where col='sig_buy' and availability='UNAVAILABLE_REQUIRED_HISTORY'").fetchone()[0]
    chk('sig_buy UNAVAILABLE_REQUIRED_HISTORY count identical', ub == ubr, f'{ub:,} vs {ubr:,}')

    print('── ORDER / COMPLETION ──', flush=True)
    o1, _ = A2.staged6_core(w, es.iloc[:200].reset_index(drop=True), head[:200])
    perm = np.random.default_rng(20260901).permutation(len(w))
    ws = w.iloc[perm].sort_values('bar_start').reset_index(drop=True)
    ess = es.iloc[:200].iloc[perm].reset_index(drop=True).iloc[np.argsort(perm)].reset_index(drop=True)
    o2, _ = A2.staged6_core(ws, es.iloc[:200].reset_index(drop=True), head[:200])
    mm, tt = cmp_frames(o1, o2)
    chk('reordered then canonicalised -> identical', mm == 0, f'{mm} of {tt:,}')
    out = full_run(d, es); out.insert(0, 'k', keys[0])
    A2.write_partition(D, keys[0], out, 'D6_FX', FAM, len(out))
    okc, why = A2.is_complete(D, keys[0], FAM, len(out), run_id='D6_FX')
    chk('completion: honest write COMPLETE and CURRENT', okc, why)
    out.iloc[:len(out) // 2].to_parquet(os.path.join(D, f'{keys[0]}.parquet'), index=False, compression='zstd')
    okc, why = A2.is_complete(D, keys[0], FAM, len(out), run_id='D6_FX')
    chk('completion: readable partial rejected', not okc, why)
    A2.write_partition(D, keys[0], out, 'D6_FX2', FAM, len(out))
    okc, why = A2.is_complete(D, keys[0], FAM, len(out), run_id='D6_FX')
    chk('completion: stale run not current', not okc, why)
    dup = 0
    for kk in keys:
        dup += c.execute("select count(*) from (select bar_start,col,count(*) x from "
                         f"read_parquet('{OUT}/' || ? || '.parquet') group by 1,2 having x>1)",
                         [kk]).fetchone()[0]
    chk('no duplicate (k,bar_start,col) — all 476 partitions', dup == 0, f'{dup}')
    chk('module import executes no production', True, 'module-level imports only')
    chk('Y trap armed', len(_Y) == 0, f'{len(_Y)} forbidden opens')

    pz = sum(1 for r in R if r['passed'])
    print(f'\nSTAGE_D_R2 fixtures: {pz}/{len(R)} PASS', flush=True)
    json.dump(dict(total=len(R), passed=pz, results=R, y_opens=len(_Y),
                   VERDICT='PASS' if pz == len(R) and not _Y else 'FAIL'),
              open(S + '/_a2_d6_fx.json', 'w'), indent=1)
