"""A3 qualification: canonical 69-X consolidation.
TWO independent exactness proofs — vs sealed CANON69, and vs the CURRENT A2 sources."""
import os, sys, json, time, warnings; warnings.filterwarnings('ignore')
FORBIDDEN = ('prod_infer', 'prod_ci', 'prod_cs', 'prod_denom', 'real_y', 'mm_y.npy',
             '_prom_mask', 'promoted_732', 'promoted_set_v1_ids', '_j.npy', '_diag.json',
             'theta', 'p_adj')
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
import forward_a3_canon69 as A3                                        # noqa: E402

S = ('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/'
     '4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad')
SRC = {'pd': S + '/a2_pd12_out', 'c4': S + '/a2_c4_out',
       'p39': S + '/a2_p1_out', 'd6': S + '/a2_d6_out'}
OUT = S + '/a3_canon69_out'          # isolated DEV/regression namespace — never the forward root
FROZEN = '/Users/sachoki/MASSIVE_DATA/historical/CANON69.duckdb'
RUN = 'A3_HISTORICAL_REGRESSION'
FAM = 'CANON69'
R = {}


def conn():
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute("set memory_limit='8GB'"); c.execute(f"set temp_directory='{S}/duck_tmp'")
    c.execute(f"attach '{FROZEN}' as fz (read_only)")
    return c


if __name__ == '__main__':
    t_all = time.time()
    os.makedirs(OUT, exist_ok=True); os.makedirs(S + '/duck_tmp', exist_ok=True)
    man = A3.load_manifest()
    cohort = sorted(A2.load_cohort())
    c = conn()

    # ── input binding: the sources must be the CURRENT A2 outputs, not stale files ──
    src_state = {}
    for fam, d in SRC.items():
        n = len(os.listdir(d))
        mans = [f for f in os.listdir(d) if f.endswith('.manifest.json')]
        src_state[fam] = dict(dir=d, files=n, completion_markers=len(mans))
        if len(mans) == 0 and fam != 'pd':
            raise SystemExit(f'HARD STOP: family {fam} has no A2 completion markers')
    R['SOURCE_BINDING'] = dict(a2_stack_digest=A3.A2_STACK_DIGEST, sources=src_state,
                               manifest_v2_digest=A3.MANIFEST_V2_SEALED_DIGEST,
                               cohort_digest=A2.COHORT_DIGEST)
    print(json.dumps(R['SOURCE_BINDING'], indent=1), flush=True)

    # ── frozen availability vocabulary, per source family ──
    froz_vocab = {}
    for fam in ('pd', 'c4', 'p39', 'd6'):
        feats = [f for f, e in man.items() if e['source_table'] == fam]
        q = ','.join(f"'{f}'" for f in feats)
        froz_vocab[fam] = sorted(r[0] for r in c.execute(
            f"select distinct availability from fz.x69 where feature in ({q})").fetchall())
    R['FROZEN_AVAILABILITY_VOCABULARY'] = froz_vocab
    print(json.dumps(froz_vocab, indent=1), flush=True)

    # ── build ──
    print('[consolidate]', flush=True)
    t0 = time.time(); rows = 0
    for i, k in enumerate(cohort, 1):
        frames = {}
        for fam, d in SRC.items():
            p = os.path.join(d, f'{k}.parquet')
            if not os.path.exists(p):
                raise SystemExit(f'HARD STOP: missing A2 partition {fam}/{k}')
            frames[fam] = pd.read_parquet(p)
        r = A3.consolidate_partition(frames, man, k)
        A2.write_partition(OUT, k, r, RUN, FAM, len(r))
        rows += len(r)
        if i % 50 == 0:
            print(f'  {i}/{len(cohort)} rows {rows:,} {time.time()-t0:.0f}s', flush=True)
    build_s = time.time() - t0
    print(f'[consolidate] {rows:,} rows in {build_s:.0f}s', flush=True)

    c.execute(f"create view O as select k,bar_start,session_date,feature,bval,dval,sval,availability from read_parquet('{OUT}/*.parquet')")
    c.execute("create view F as select k,bar_start,session_date,feature,bval,dval,sval,availability from fz.x69")
    nO = c.execute("select count(*) from O").fetchone()[0]
    nF = c.execute("select count(*) from F").fetchone()[0]
    so = c.execute("describe select * from O").df()[['column_name', 'column_type']].values.tolist()
    sf = c.execute("describe select * from F").df()[['column_name', 'column_type']].values.tolist()
    feats_o = set(r[0] for r in c.execute("select distinct feature from O").fetchall())
    feats_f = set(r[0] for r in c.execute("select distinct feature from F").fetchall())
    keys_o = c.execute("select count(distinct k) from O").fetchone()[0]
    dates_o = c.execute("select min(session_date), max(session_date), count(distinct session_date) from O").fetchall()[0]
    dates_f = c.execute("select min(session_date), max(session_date), count(distinct session_date) from F").fetchall()[0]

    # ── PROOF 1 · vs the SEALED historical CANON69 ──
    print('[proof 1 · vs sealed CANON69]', flush=True)
    d1 = d2 = 0
    for j, k in enumerate(cohort, 1):
        d1 += c.execute("select count(*) from (select * from O where k=? except all select * from F where k=?)", [k, k]).fetchone()[0]
        d2 += c.execute("select count(*) from (select * from F where k=? except all select * from O where k=?)", [k, k]).fetchone()[0]
        if j % 100 == 0:
            print(f'  {j}/{len(cohort)} d1={d1} d2={d2}', flush=True)
    R['PROOF_1_VS_SEALED_CANON69'] = dict(
        expected_rows=nF, rebuilt_rows=nO, except_all_new_minus_frozen=d1,
        except_all_frozen_minus_new=d2, method='PARTITIONED BY k (exhaustive disjoint decomposition)',
        feature_count_new=len(feats_o), feature_count_frozen=len(feats_f),
        feature_set_identical=(feats_o == feats_f), security_keys=keys_o,
        session_range_new=list(dates_o), session_range_frozen=list(dates_f),
        schema_match=(so == sf), schema=so, build_s=round(build_s),
        VERDICT='EXACT' if (nO == nF and d1 == 0 and d2 == 0 and so == sf
                            and feats_o == feats_f and list(dates_o) == list(dates_f)) else 'MISMATCH')
    print(json.dumps({a: b for a, b in R['PROOF_1_VS_SEALED_CANON69'].items() if a != 'schema'}, indent=1), flush=True)
    if R['PROOF_1_VS_SEALED_CANON69']['VERDICT'] != 'EXACT':
        json.dump(R, open(S + '/_a3.json', 'w'), indent=1, default=str); sys.exit('HARD STOP - proof 1 failed')

    # ── PROOF 2 · vs the CURRENT qualified A2 sources, per feature ──
    print('[proof 2 · vs current A2 sources]', flush=True)
    for fam, d in SRC.items():
        c.execute(f"create view S_{fam} as select * from read_parquet('{d}/*.parquet')")
    per_feature = {}
    bad_feats = []
    for name, e in man.items():
        fam = e['source_table']; ssl = e['source_slot']; csl = e['canonical_slot']
        vsel = ', '.join(f"{ssl} as v" if ssl else 'NULL as v' for _ in [0])
        nsrc = c.execute(f"select count(*) from S_{fam} where col=?", [name]).fetchone()[0]
        ncan = c.execute("select count(*) from O where feature=?", [name]).fetchone()[0]
        mm = c.execute(
            f"select count(*) from (select k,bar_start,{ssl} as v,availability from S_{fam} where col=? "
            f"except all select k,bar_start,{csl} as v,availability from O where feature=?)",
            [name, name]).fetchone()[0]
        mm2 = c.execute(
            f"select count(*) from (select k,bar_start,{csl} as v,availability from O where feature=? "
            f"except all select k,bar_start,{ssl} as v,availability from S_{fam} where col=?)",
            [name, name]).fetchone()[0]
        per_feature[name] = dict(family=fam, source_rows=nsrc, canonical_rows=ncan,
                                 src_minus_canon=mm, canon_minus_src=mm2)
        if not (nsrc == ncan and mm == 0 and mm2 == 0):
            bad_feats.append(name)
    R['PROOF_2_VS_CURRENT_A2_SOURCES'] = dict(
        features=len(per_feature), exact=len(per_feature) - len(bad_feats),
        failing=bad_feats, per_feature=per_feature,
        VERDICT='EXACT' if not bad_feats else 'MISMATCH')
    print(f"  {len(per_feature)-len(bad_feats)}/{len(per_feature)} features exact; failing {bad_feats}", flush=True)

    # ── availability vocabulary conformance, per family ──
    obs = {}
    for fam in ('pd', 'c4', 'p39', 'd6'):
        feats = [f for f, e in man.items() if e['source_table'] == fam]
        q = ','.join(f"'{f}'" for f in feats)
        obs[fam] = sorted(r[0] for r in c.execute(
            f"select distinct availability from O where feature in ({q})").fetchall())
    try:
        A3.check_availability_vocabulary(obs, froz_vocab)
        vocab_ok = True; vocab_note = 'identical per family'
    except A3.A3Hold as ex:
        vocab_ok = False; vocab_note = str(ex)
    pd_init = c.execute("select count(*) from O where feature in "
                        "(select feature from O) and availability like 'INITIALIZATION_SENSITIVE_%'"
                        ).fetchone()[0]
    pd_feats = [f for f, e in man.items() if e['source_table'] == 'pd']
    q = ','.join(f"'{f}'" for f in pd_feats)
    pd_decomposed = c.execute(f"select count(*) from O where feature in ({q}) and "
                              "availability in ('INITIALIZATION_SENSITIVE_HEAD','INITIALIZATION_SENSITIVE_POST_GAP')").fetchone()[0]
    R['AVAILABILITY_CONFORMANCE'] = dict(observed=obs, frozen=froz_vocab, identical=vocab_ok,
                                         note=vocab_note,
                                         pd_INITIALIZATION_SENSITIVE_decomposed_rows=pd_decomposed,
                                         VERDICT='PASS' if (vocab_ok and pd_decomposed == 0) else 'FAIL')
    print(json.dumps({a: b for a, b in R['AVAILABILITY_CONFORMANCE'].items() if a not in ('observed', 'frozen')}, indent=1), flush=True)
    R['runtime_s'] = round(time.time() - t_all); R['y_forbidden_opens'] = len(_Y)
    R['module_digest'] = A3.self_digest()
    json.dump(R, open(S + '/_a3.json', 'w'), indent=1, default=str)
    print(f"\ndone {R['runtime_s']}s", flush=True)
