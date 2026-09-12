"""READABLE_PARTIAL_OUTPUT fixture for pd12 — a crash can leave a perfectly valid parquet
holding only a subset of rows. It must not be accepted as COMPLETE."""
import os, sys, json, shutil, time, warnings; warnings.filterwarnings('ignore')
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
import duckdb, pandas as pd                                             # noqa: E402
import forward_a2_producers as A2                                       # noqa: E402
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from a2_pd12_qualify import build, conn                                 # noqa: E402

S = os.path.dirname(os.path.abspath(__file__))
D = S + '/a2_pd12_partial'
RUN = 'A2_PD12_PARTIAL_FIXTURE'
FAM = 'pd12'
R = []


def chk(name, ok, note):
    R.append(dict(name=name, passed=bool(ok), note=str(note)[:140]))
    print(f'  [{"PASS" if ok else "FAIL"}] {name}: {note}', flush=True)


if __name__ == '__main__':
    shutil.rmtree(D, ignore_errors=True); os.makedirs(D)
    dep = A2.load_pd_dep(); c = conn()
    k = c.execute("select distinct k from sa.bars order by k limit 1").fetchone()[0]
    full = build(c, k, dep)
    exp = len(full)
    print(f'partition {k[:12]}  expected rows {exp:,}', flush=True)

    # ── a normal, honest write ─────────────────────────────────────────────
    man = A2.write_partition(D, k, full, RUN, FAM, exp)
    ok, why = A2.is_complete(D, k, FAM, exp)
    chk('honest write is COMPLETE', ok, why)
    good_digest = man['output_digest']

    # ── the crash: a VALID parquet with correct schema and a strict subset ──
    half = full.iloc[:exp // 2].copy()
    p = os.path.join(D, f'{k}.parquet')
    half.to_parquet(p, index=False, compression='zstd')   # readable, correct schema
    reread = pd.read_parquet(p)
    chk('crash artefact is genuinely READABLE and correctly typed',
        len(reread) == exp // 2 and list(reread.columns) == list(full.columns)
        and str(reread.value.dtype) == str(full.value.dtype),
        f'{len(reread):,} rows, schema {list(reread.columns)}, value dtype {reread.value.dtype}')
    ok, why = A2.is_complete(D, k, FAM, exp)
    chk('readable partial is NOT accepted as COMPLETE', not ok, why)
    chk('rejection reason is digest/size reconciliation, not readability', why in
        ('DIGEST_MISMATCH', 'SIZE_MISMATCH'), why)

    # ── a crash that also loses the marker ─────────────────────────────────
    os.remove(A2._man_path(D, k))
    ok, why = A2.is_complete(D, k, FAM, exp)
    chk('no marker -> NOT COMPLETE (file exists and is readable)', not ok, why)

    # ── a forged marker that merely says COMPLETED ─────────────────────────
    json.dump(dict(schema=A2.COMPLETION_SCHEMA, run_id=RUN, family=FAM, partition_key=k,
                   rows=exp, output_digest=good_digest, bytes=os.path.getsize(p),
                   status='COMPLETED'), open(A2._man_path(D, k), 'w'))
    ok, why = A2.is_complete(D, k, FAM, exp)
    chk('marker claiming COMPLETED cannot rescue wrong data', not ok, why)

    # ── recovery ───────────────────────────────────────────────────────────
    A2.write_partition(D, k, full, RUN + '_RECOVERY', FAM, exp)
    ok, why = A2.is_complete(D, k, FAM, exp)
    chk('recovery restores COMPLETE', ok, why)
    chk('recovered digest equals the honest write', A2._sha(p) == good_digest,
        f'{A2._sha(p)[:16]} vs {good_digest[:16]}')
    n = c.execute(f"select count(*) from read_parquet('{D}/*.parquet')").fetchone()[0]
    dup = c.execute(f"select count(*) from (select k,bar_start,col,count(*) x from "
                    f"read_parquet('{D}/*.parquet') group by 1,2,3 having x>1)").fetchone()[0]
    chk('no duplicate rows after recovery', dup == 0 and n == exp, f'{n:,} rows, {dup} dups')

    # ── row-count guard at write time ──────────────────────────────────────
    try:
        A2.write_partition(D, k, full.iloc[:10], RUN, FAM, exp)
        chk('write refuses a short partition', False, 'NO EXCEPTION')
    except A2.A2Hold as e:
        chk('write refuses a short partition', True, str(e)[:90])
    # the refused write did not touch the good bytes on disk, which is correct — a rejected
    # short write must not destroy validated data. What must NOT happen is that stale
    # partition being adopted as THIS run's output. Currency is a run_id question, not a
    # file-integrity question.
    ok, why = A2.is_complete(D, k, FAM, exp)
    chk('refused write leaves the prior bytes intact and internally COMPLETE', ok, why)
    ok, why = A2.is_complete(D, k, FAM, exp, run_id='A2_PD12_THIS_RUN')
    chk('but it is NOT CURRENT for this run (stale-run guard)', not ok, why)
    A2.write_partition(D, k, full, 'A2_PD12_THIS_RUN', FAM, exp)
    ok, why = A2.is_complete(D, k, FAM, exp, run_id='A2_PD12_THIS_RUN')
    chk('after this run writes it, it IS current', ok, why)

    # ── identity guards ────────────────────────────────────────────────────
    ok, why = A2.is_complete(D, k, 'stageC4', exp)
    chk('family mismatch rejected', not ok, why)
    ok, why = A2.is_complete(D, k, FAM, exp + 1)
    chk('row reconciliation mismatch rejected', not ok, why)

    p_ = sum(1 for r in R if r['passed'])
    print(f'\nREADABLE_PARTIAL_OUTPUT fixture: {p_}/{len(R)} PASS   Y opens {len(_Y)}', flush=True)
    json.dump(dict(total=len(R), passed=p_, results=R, y_forbidden_opens=len(_Y),
                   VERDICT='PASS' if p_ == len(R) and not _Y else 'FAIL'),
              open(S + '/_a2_pd12_partial.json', 'w'), indent=1)
