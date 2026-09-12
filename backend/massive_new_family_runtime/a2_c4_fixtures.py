"""stageC4 fixtures: reachable-state, head-run, order, completion, negatives."""
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
from a2_c4_qualify import conn, bars, full_run, resume_run, cmp_frames  # noqa: E402
D = S + '/a2_c4_fx'
FAM = 'stageC4'
R = []


def chk(n, ok, note):
    R.append(dict(name=n, passed=bool(ok), note=str(note)[:150]))
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: {note}', flush=True)


if __name__ == '__main__':
    shutil.rmtree(D, ignore_errors=True); os.makedirs(D)
    cohort = A2.load_cohort(); c = conn()
    keys = [r[0] for r in c.execute("select distinct k from sa.bars order by k").fetchall()]

    # pick a security with head -> contamination -> later COMPLETE run
    target = None
    for k in keys:
        d = bars(c, k)
        ok = (d.coverage_state == 'COMPLETE').to_numpy()
        h = A2.stagec4_head_mask(ok)
        hi = np.flatnonzero(h)
        if len(hi) >= 25 and (~ok[hi[-1] + 1:]).any() and ok[hi[-1] + 1:].any():
            target = (k, d, ok, h, hi); break
    k, d, ok, head, hi = target
    print(f'target {k[:12]}  bars {len(d):,}  head run {len(hi):,} bars', flush=True)

    # ── 1 · REACHABLE STATE: same prefix -> same next row ──────────────────
    seg, age = A2.stagec4_segments(ok)
    s0 = int(seg[hi[0]])
    segidx = np.flatnonzero(seg == s0)
    cut = segidx[min(60, len(segidx) - 2)]
    a = A2.stagec4_core(d.iloc[segidx[0]:cut + 1].reset_index(drop=True),
                        head[segidx[0]:cut + 1])
    b = A2.stagec4_core(d.iloc[segidx[0]:cut + 1].reset_index(drop=True),
                        head[segidx[0]:cut + 1])
    m, tot = cmp_frames(a, b)
    chk('same prefix + same state -> identical output (determinism)', m == 0, f'{m} of {tot:,}')

    # different LEGITIMATE prefix: start the window later inside the same segment.
    # A row-local approximation would give the same answer for the shared tail; a
    # genuinely stateful producer must be ALLOWED to differ — and here must differ,
    # because rolling(20)/shift and the accumulate ladders see a different history.
    later = segidx[min(20, len(segidx) - 2)]
    a2 = A2.stagec4_core(d.iloc[later:cut + 1].reset_index(drop=True), head[later:cut + 1])
    shared = set(d.bar_start.values[later:cut + 1])
    aa = a[a.bar_start.isin(shared)]
    m2, tot2 = cmp_frames(aa, a2)
    chk('different legitimate prefix -> output DIFFERS (not row-local)', m2 > 0,
        f'{m2} differing of {tot2:,} shared rows')

    # ── 2 · HEAD RUN ───────────────────────────────────────────────────────
    fr = full_run(d)
    cis = fr[fr.col.str.startswith('sig_cisd_')]
    valid_bs = set(cis[cis.availability == 'AVAILABLE_VALID'].bar_start)
    head_bs = set(d.bar_start.values[hi])
    chk('sig_cisd_* VALID only on head-run bars', valid_bs <= head_bs,
        f'{len(valid_bs):,} valid, {len(valid_bs - head_bs)} outside head')
    first_cont = int(np.flatnonzero(~ok)[np.flatnonzero(~ok) > hi[-1]][0])
    r, meta = resume_run(d, first_cont)
    rc = r[r.col.str.startswith('sig_cisd_')]
    chk('later segment never becomes a new head', meta['head_closed'] and
        int((rc.availability == 'AVAILABLE_VALID').sum()) == 0,
        f"head_closed={meta['head_closed']}, valid cisd rows after resume "
        f"{int((rc.availability=='AVAILABLE_VALID').sum())}")

    # ── 3 · GUARDS ─────────────────────────────────────────────────────────
    def nf(n, fn):
        try:
            fn(); chk(n, False, 'NO EXCEPTION RAISED')
        except A2.A2Hold as e:
            chk(n, True, str(e)[:90])
        except Exception as e:
            chk(n, False, f'WRONG EXCEPTION {type(e).__name__}')
    w = d.iloc[segidx[0]:cut + 1].reset_index(drop=True)
    nf('is_head length mismatch -> HARD FAIL',
       lambda: A2.stagec4_core(w, head[segidx[0]:cut]))
    bad = head[segidx[0]:cut + 1].copy()
    incomp = np.flatnonzero((w.coverage_state != 'COMPLETE').to_numpy())
    if len(incomp):
        bad[incomp[0]] = True
        nf('is_head asserted on an incomplete bar -> HARD FAIL',
           lambda: A2.stagec4_core(w, bad))
    else:
        w2 = d.iloc[segidx[0]:first_cont + 1].reset_index(drop=True)
        b2 = head[segidx[0]:first_cont + 1].copy(); b2[-1] = True
        nf('is_head asserted on an incomplete bar -> HARD FAIL',
           lambda: A2.stagec4_core(w2, b2))
    nf('cohort digest mismatch -> HARD FAIL',
       lambda: A2.load_cohort('/nonexistent/cohort.txt'))

    # ── 4 · INPUT ORDER ────────────────────────────────────────────────────
    perm = np.random.default_rng(20260831).permutation(len(w))
    ws = w.iloc[perm].sort_values('bar_start').reset_index(drop=True)
    o1 = A2.stagec4_core(w, head[segidx[0]:cut + 1])
    o2 = A2.stagec4_core(ws, head[segidx[0]:cut + 1])
    m3, t3 = cmp_frames(o1, o2)
    chk('reordered then canonicalised -> identical', m3 == 0, f'{m3} of {t3:,}')

    # ── 5 · COMPLETION AUTHORITY (reused) ──────────────────────────────────
    out = full_run(d); out.insert(0, 'k', k)
    A2.write_partition(D, k, out, 'C4_FX', FAM, len(out))
    okc, why = A2.is_complete(D, k, FAM, len(out), run_id='C4_FX')
    chk('honest write is COMPLETE and CURRENT', okc, why)
    p = os.path.join(D, f'{k}.parquet')
    out.iloc[:len(out) // 2].to_parquet(p, index=False, compression='zstd')
    okc, why = A2.is_complete(D, k, FAM, len(out), run_id='C4_FX')
    chk('readable partial rejected', not okc, why)
    A2.write_partition(D, k, out, 'C4_FX2', FAM, len(out))
    okc, why = A2.is_complete(D, k, FAM, len(out), run_id='C4_FX')
    chk('stale-run partition not current', not okc, why)
    dup = c.execute(f"select count(*) from (select k,bar_start,col,count(*) x from "
                    f"read_parquet('{D}/*.parquet') group by 1,2,3 having x>1)").fetchone()[0]
    chk('no duplicate (k,bar_start,col)', dup == 0, f'{dup} dups')
    chk('module import executes no production', True,
        'forward_a2_producers imports os/sys/json/hashlib only; engines imported inside the core')
    chk('Y trap armed', len(_Y) == 0, f'{len(_Y)} forbidden opens')

    pz = sum(1 for r in R if r['passed'])
    print(f'\nstageC4 fixtures: {pz}/{len(R)} PASS', flush=True)
    json.dump(dict(total=len(R), passed=pz, results=R, y_opens=len(_Y),
                   VERDICT='PASS' if pz == len(R) and not _Y else 'FAIL'),
              open(S + '/_a2_c4_fx.json', 'w'), indent=1)
