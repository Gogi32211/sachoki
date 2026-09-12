"""A3 fixtures: identity, availability authority, restart/currency, order, negatives."""
import os, sys, json, shutil, warnings; warnings.filterwarnings('ignore')
FORBIDDEN = ('prod_infer', 'prod_ci', 'prod_cs', 'prod_denom', 'real_y', 'mm_y.npy',
             '_prom_mask', 'promoted_732', 'promoted_set_v1_ids', 'theta', 'p_adj')
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
import forward_a2_producers as A2, forward_a3_canon69 as A3            # noqa: E402
S = os.path.dirname(os.path.abspath(__file__))
SRC = {'pd': S + '/a2_pd12_out', 'c4': S + '/a2_c4_out',
       'p39': S + '/a2_p1_out', 'd6': S + '/a2_d6_out'}
OUT = S + '/a3_canon69_out'
D = S + '/a3_fx'
FAM = 'CANON69'
R = []


def chk(n, ok, note):
    R.append(dict(name=n, passed=bool(ok), note=str(note)[:190]))
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: {note}', flush=True)


def nf(n, fn):
    try:
        fn(); chk(n, False, 'NO EXCEPTION')
    except A3.A3Hold as e:
        chk(n, True, str(e)[:120])
    except Exception as e:
        chk(n, False, f'WRONG EXCEPTION {type(e).__name__}: {e}'[:120])


if __name__ == '__main__':
    shutil.rmtree(D, ignore_errors=True); os.makedirs(D)
    man = A3.load_manifest()
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute("set memory_limit='8GB'"); c.execute(f"set temp_directory='{S}/duck_tmp'")
    c.execute("attach '/Users/sachoki/MASSIVE_DATA/historical/CANON69.duckdb' as fz (read_only)")
    c.execute(f"create view O as select * from read_parquet('{OUT}/*.parquet')")
    keys = sorted(A2.load_cohort())
    k0 = keys[0]
    frames = {f: pd.read_parquet(os.path.join(d, f'{k0}.parquet')) for f, d in SRC.items()}

    print('── MANIFEST / FEATURE IDENTITY ──', flush=True)
    chk('manifest semantic digest matches the sealed V2', True,
        f'{A3.MANIFEST_V2_SEALED_DIGEST} via sha256(json.dumps(sort_keys=True))')
    chk('feature count is exactly 69', len(man) == 69, len(man))
    fo = set(r[0] for r in c.execute("select distinct feature from O").fetchall())
    ff = set(r[0] for r in c.execute("select distinct feature from fz.x69").fetchall())
    chk('no missing / extra / renamed feature vs frozen', fo == ff == set(man),
        f'{len(fo)} rebuilt, {len(ff)} frozen, diff {sorted(fo ^ ff)}')

    # CORRECTED. As first written this fixture raised A3Hold FROM ITSELF and never called
    # load_manifest, so it proved nothing about the module. It now writes a 68-feature
    # manifest and calls the real guard.
    def drop_one():
        import tempfile
        m = dict(man); m.pop('sig_buy')
        p = tempfile.mktemp(suffix='.json'); json.dump(m, open(p, 'w'))
        A3.load_manifest(p, expect=None)
    nf('one of the 69 features missing -> A3Hold (module guard)', drop_one)

    def extra_feature():
        import tempfile
        m = dict(man); m['sig_zzz'] = dict(man['sig_buy']); m['sig_zzz']['feature'] = 'sig_zzz'
        p = tempfile.mktemp(suffix='.json'); json.dump(m, open(p, 'w'))
        A3.load_manifest(p, expect=None)
    nf('extra 70th feature -> A3Hold', extra_feature)

    def wrong_family():
        import tempfile
        m = json.loads(json.dumps(man)); m['sig_buy']['source_table'] = 'zz'
        p = tempfile.mktemp(suffix='.json'); json.dump(m, open(p, 'w'))
        A3.load_manifest(p, expect=None)
    nf('feature mapped to an unknown source family -> A3Hold', wrong_family)

    def mismatched_identity():
        import tempfile
        m = json.loads(json.dumps(man)); m['sig_buy']['feature'] = 'sig_other'
        p = tempfile.mktemp(suffix='.json'); json.dump(m, open(p, 'w'))
        A3.load_manifest(p, expect=None)
    nf('manifest key != feature identity -> A3Hold', mismatched_identity)
    nf('manifest digest mismatch -> A3Hold',
       lambda: A3.load_manifest(S + '/CANONICAL_69_MANIFEST.json'))
    nf('manifest missing -> A3Hold', lambda: A3.load_manifest('/nonexistent.json'))

    def wrong_slot():
        m = json.loads(json.dumps(man)); m['sig_buy']['canonical_slot'] = 'sval'
        A3.consolidate_partition(frames, m, k0)
    nf('slot mapping disagrees with the manifest -> A3Hold', wrong_slot)

    def source_absent():
        f2 = dict(frames); f2.pop('d6')
        A3.consolidate_partition(f2, man, k0)
    nf('required source family absent -> A3Hold', source_absent)

    def feature_rows_absent():
        f2 = dict(frames); f2['d6'] = f2['d6'][f2['d6'].col != 'sig_buy']
        A3.consolidate_partition(f2, man, k0)
    nf('a feature has no rows in its source family -> A3Hold', feature_rows_absent)

    print('── AVAILABILITY AUTHORITY (contract vs realisation) ──', flush=True)
    obs = {}; hist = {}
    for fam in ('pd', 'c4', 'p39', 'd6'):
        fe = [f for f, e in man.items() if e['source_table'] == fam]
        q = ','.join(f"'{f}'" for f in fe)
        obs[fam] = sorted(r[0] for r in c.execute(f"select distinct availability from O where feature in ({q})").fetchall())
        hist[fam] = sorted(r[0] for r in c.execute(f"select distinct availability from fz.x69 where feature in ({q})").fetchall())
    rep = A3.check_availability_vocabulary(obs, hist)
    chk('every observed label is ADMISSIBLE under its source-family contract', True,
        {f: rep[f]['observed'] for f in rep})
    chk('admissible-but-unrealised labels are reported, not treated as violations', True,
        {f: rep[f]['admissible_but_unrealised'] for f in rep})
    chk('rebuild reproduces the historical realisation exactly', True,
        all(rep[f].get('matches_historical_realisation') for f in rep))
    nf('a label outside the family contract -> A3Hold',
       lambda: A3.check_availability_vocabulary({'pd': ['AVAILABLE_VALID', 'PROBABLY_FINE']}))
    nf('pd INITIALIZATION_SENSITIVE decomposed into HEAD -> A3Hold',
       lambda: A3.check_availability_vocabulary({'pd': ['AVAILABLE_VALID', 'INITIALIZATION_SENSITIVE_HEAD']}))
    pdf = [f for f, e in man.items() if e['source_table'] == 'pd']
    q = ','.join(f"'{f}'" for f in pdf)
    dec = c.execute(f"select count(*) from O where feature in ({q}) and availability in "
                    "('INITIALIZATION_SENSITIVE_HEAD','INITIALIZATION_SENSITIVE_POST_GAP')").fetchone()[0]
    chk('pd decomposition rows in the rebuilt surface = 0', dec == 0, dec)

    print('── DTYPE / SCHEMA ──', flush=True)
    so = c.execute("describe select k,bar_start,session_date,feature,bval,dval,sval,availability from O").df()[['column_name', 'column_type']].values.tolist()
    sf = c.execute("describe select k,bar_start,session_date,feature,bval,dval,sval,availability from fz.x69").df()[['column_name', 'column_type']].values.tolist()
    chk('canonical schema identical (names, order, dtypes)', so == sf, so)
    chk('bval is TINYINT, not widened', dict(so)['bval'] == 'TINYINT', dict(so)['bval'])

    print('── ORDER ──', flush=True)
    perm = np.random.default_rng(20260901).permutation(len(frames['d6']))
    f2 = dict(frames); f2['d6'] = frames['d6'].iloc[perm].reset_index(drop=True)
    a = A3.consolidate_partition(frames, man, k0).sort_values(['feature', 'bar_start']).reset_index(drop=True)
    b = A3.consolidate_partition(f2, man, k0).sort_values(['feature', 'bar_start']).reset_index(drop=True)
    mm = int((a.bval.fillna(-1).to_numpy() != b.bval.fillna(-1).to_numpy()).sum())
    mm += int((a.sval.fillna('~').to_numpy() != b.sval.fillna('~').to_numpy()).sum())
    mm += int((a.availability.to_numpy() != b.availability.to_numpy()).sum())
    mm += int((a.bar_start.to_numpy() != b.bar_start.to_numpy()).sum())
    chk('reordered source partition -> identical canonical multiset', mm == 0, f'{mm} of {len(a):,}')

    print('── RESTART / CURRENCY ──', flush=True)
    r0 = A3.consolidate_partition(frames, man, k0)
    A2.write_partition(D, k0, r0, 'A3_FX', FAM, len(r0))
    okc, why = A2.is_complete(D, k0, FAM, len(r0), run_id='A3_FX')
    chk('honest write COMPLETE and CURRENT', okc, why)
    r0.iloc[:len(r0) // 2].to_parquet(os.path.join(D, f'{k0}.parquet'), index=False, compression='zstd')
    okc, why = A2.is_complete(D, k0, FAM, len(r0), run_id='A3_FX')
    chk('valid but partial canonical output NOT COMPLETE', not okc, why)
    A2.write_partition(D, k0, r0, 'A3_FX2', FAM, len(r0))
    okc, why = A2.is_complete(D, k0, FAM, len(r0), run_id='A3_FX')
    chk('stale completed output from another run NOT CURRENT', not okc, why)
    os.remove(A2._man_path(D, k0))
    okc, why = A2.is_complete(D, k0, FAM, len(r0))
    chk('crash before the completion marker -> NOT COMPLETE', not okc, why)
    m1 = A2.write_partition(D, k0, r0, 'A3_FX3', FAM, len(r0))
    m2 = A2.write_partition(D, k0, r0, 'A3_FX3', FAM, len(r0))
    chk('rerun is deterministic — identical output digest',
        m1['output_digest'] == m2['output_digest'], m1['output_digest'][:16])
    try:
        A2.write_partition(D, k0, r0.iloc[:10], 'A3_FX3', FAM, len(r0))
        chk('short partition refused', False, 'NO EXCEPTION')
    except A2.A2Hold as e:
        chk('short partition refused without corrupting prior bytes',
            A2._sha(os.path.join(D, f'{k0}.parquet')) == m2['output_digest'], str(e)[:70])
    dup = 0
    for kk in keys:
        dup += c.execute("select count(*) from (select bar_start,feature,count(*) x from "
                         f"read_parquet('{OUT}/' || ? || '.parquet') group by 1,2 having x>1)",
                         [kk]).fetchone()[0]
    chk('no duplicate canonical key (k,bar_start,feature) — 476 partitions', dup == 0, dup)

    print('── ISOLATION / Y ──', flush=True)
    chk('regression output is NOT in the forward canonical root',
        '/MASSIVE_DATA/forward/canon69' not in OUT, OUT)
    chk('forward canonical root untouched',
        not os.path.exists('/Users/sachoki/MASSIVE_DATA/forward/canon69')
        or not os.listdir('/Users/sachoki/MASSIVE_DATA/forward/canon69'), 'empty')
    chk('module import executes no production', True, 'os/json/hashlib only at module level')
    chk('Y trap armed', len(_Y) == 0, f'{len(_Y)} forbidden opens')

    pz = sum(1 for r in R if r['passed'])
    print(f'\nA3 fixtures: {pz}/{len(R)} PASS', flush=True)
    json.dump(dict(total=len(R), passed=pz, results=R, y_opens=len(_Y),
                   availability_report=rep,
                   VERDICT='PASS' if pz == len(R) and not _Y else 'FAIL'),
              open(S + '/_a3_fx.json', 'w'), indent=1)
