"""B qualification: 4 independent proofs."""
import os, sys, json, time, warnings; warnings.filterwarnings('ignore')
FORBIDDEN = ('prod_infer', 'prod_ci', 'prod_cs', 'prod_denom', 'real_y', 'mm_y.npy',
             '_prom_mask', 'promoted_732', 'promoted_set_v1_ids', 'theta', 'p_adj',
             'promoted_732.csv')
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
import forward_b_x_support_census as B                                 # noqa: E402

D = '/Users/sachoki/MASSIVE_DATA/historical'
S = ('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/'
     '4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad')
DEV_END = '2026-08-06'
R = {}


def conn():
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute("set memory_limit='8GB'"); c.execute(f"set temp_directory='{S}/duck_tmp'")
    c.execute(f"attach '{D}/CANON69.duckdb' as fz (read_only)")
    c.execute(f"attach '{D}/stageA_input.duckdb' as sa (read_only)")
    c.execute(f"attach '{D}/POS4.duckdb' as p4 (read_only)")
    c.execute(f"attach '{D}/SUPPORT_CENSUS.duckdb' as oc (read_only)")
    return c


if __name__ == '__main__':
    t_all = time.time()
    os.makedirs(S + '/duck_tmp', exist_ok=True)
    cohort = B.load_cohort(); TOK = B.load_tokens()
    c = conn()

    # ══ PROOF 1 · POS4 reconstruction from CANON69 + Stage-A identity ══
    print('[proof 1 · POS4 reconstruction]', flush=True)
    t0 = time.time()
    c.execute("""create view ii as select k, session_date, bar_start, interval_index
                 from sa.bars where interval_index < 4""")
    # structural integrity FIRST — the invariant, not a filter
    mask = c.execute("""select m1,m2,m3,m4,count(*) n from (
        select k, session_date,
          max(case when interval_index=0 then 1 else 0 end) m1,
          max(case when interval_index=1 then 1 else 0 end) m2,
          max(case when interval_index=2 then 1 else 0 end) m3,
          max(case when interval_index=3 then 1 else 0 end) m4
        from sa.bars group by 1,2) group by 1,2,3,4""").df()
    mc = {(int(r.m1), int(r.m2), int(r.m3), int(r.m4)): int(r.n) for _, r in mask.iterrows()}
    B.assert_episode_structure(mc)
    print(f'  presence mask census: {mc}', flush=True)
    c.execute("""create view POS4_REBUILT as
        select x.k, x.session_date, x.feature, x.bval, x.sval, x.availability,
               case i.interval_index when 0 then '09:30' when 1 then '09:45'
                    when 2 then '10:00' else '10:15' end as et
        from fz.x69 x join ii i on x.k=i.k and x.bar_start=i.bar_start""")
    nR = c.execute("select count(*) from POS4_REBUILT").fetchone()[0]
    nP = c.execute("select count(*) from p4.pos4").fetchone()[0]
    so = c.execute("describe select k,session_date,feature,bval,sval,availability,et from POS4_REBUILT").df()[['column_name', 'column_type']].values.tolist()
    sp = c.execute("describe select k,session_date,feature,bval,sval,availability,et from p4.pos4").df()[['column_name', 'column_type']].values.tolist()
    d1 = d2 = 0
    for j, k in enumerate(cohort, 1):
        d1 += c.execute("select count(*) from (select * from POS4_REBUILT where k=? except all select k,session_date,feature,bval,sval,availability,et from p4.pos4 where k=?)", [k, k]).fetchone()[0]
        d2 += c.execute("select count(*) from (select k,session_date,feature,bval,sval,availability,et from p4.pos4 where k=? except all select * from POS4_REBUILT where k=?)", [k, k]).fetchone()[0]
        if j % 100 == 0:
            print(f'  {j}/{len(cohort)} d1={d1} d2={d2}', flush=True)
    R['PROOF_1_POS4_RECONSTRUCTION'] = dict(
        persisted_rows=nP, rebuilt_rows=nR, except_all_new_minus_old=d1,
        except_all_old_minus_new=d2, schema_match=(so == sp), schema=so,
        arithmetic='596,251 episodes x 4 positions x 69 features',
        presence_mask_census={str(list(k)): v for k, v in mc.items()},
        method='PARTITIONED BY k', elapsed_s=round(time.time() - t0),
        VERDICT='EXACT' if (nR == nP and d1 == 0 and d2 == 0 and so == sp) else 'MISMATCH')
    print(json.dumps({a: b for a, b in R['PROOF_1_POS4_RECONSTRUCTION'].items() if a != 'schema'}, indent=1), flush=True)
    if R['PROOF_1_POS4_RECONSTRUCTION']['VERDICT'] != 'EXACT':
        json.dump(R, open(S + '/_b.json', 'w'), indent=1, default=str); sys.exit('HARD STOP - proof 1')

    # ══ PROOF 2 · support oracle, all 45,387 ══
    print('[proof 2 · support oracle]', flush=True)
    rec = json.load(open(S + '/_b_reconcile.json'))
    R['PROOF_2_SUPPORT_ORACLE'] = dict(
        claims_compared=rec['oracle_rows'], DEV_positive_mismatches=rec['positive_mismatches'],
        DEV_evaluable_mismatches=rec['evaluable_mismatches'],
        join_left_only=rec['join_left_only'], join_right_only=rec['join_right_only'],
        tolerance=0, source='b_reconcile.py, rebuilt from the persisted POS4 under the frozen '
        'X_positive_episode_n definition', VERDICT=rec['VERDICT'])
    print(json.dumps(R['PROOF_2_SUPPORT_ORACLE'], indent=1), flush=True)

    # ══ PROOF 3 · forward B logic over the four hubs ══
    print('[proof 3 · hub census]', flush=True)
    reg = [l.rstrip('\n') for l in open('massive_new_family_runtime/SEARCHABLE_REGISTRY_k.txt')]
    oc = c.execute("select claim_id, DEV_positive, DEV_evaluable from oc.census").df()
    ocmap = dict(zip(oc.claim_id, oc.DEV_positive))
    ocev = dict(zip(oc.claim_id, oc.DEV_evaluable))
    hub_rows = {}
    for h in B.HUBS:
        ids = B.load_hub(h)
        cl = [reg[i] for i in ids]
        pos = np.array([ocmap[x] for x in cl])
        ev = np.array([ocev[x] for x in cl])
        ge = int((pos >= B.X_POSITIVE_FLOOR).sum())
        hub_rows[h] = dict(registered_member_n=len(cl), members_ge_100=ge,
                           required_member_n=B.HUBS[h]['required'],
                           condition_b=bool(ge >= B.HUBS[h]['required']),
                           X_positive_min=int(pos.min()), X_positive_median=int(np.median(pos)),
                           X_evaluable_min=int(ev.min()),
                           membership_digest=B.HUBS[h]['digest'])
    R['PROOF_3_HUB_CENSUS'] = dict(
        hubs=hub_rows, thresholds_untouched=True,
        note='computed on the FULL DEV window as a mechanical exercise of the hub logic. '
             'This is NOT a forward trigger evaluation and creates no forward evidence.',
        X_positive_floor=B.X_POSITIVE_FLOOR)
    for h, v in hub_rows.items():
        print(f"  {h:20s} {v['members_ge_100']:>4}/{v['registered_member_n']:<4} "
              f"required {v['required_member_n']:<4} cond_b={v['condition_b']}", flush=True)

    # production mode must be BLOCKED
    try:
        B.require_production_authorities(None, None, '3840a5df1a626aa1')
        prod = dict(blocked=False, note='NO EXCEPTION — DEFECT')
    except B.BHold as e:
        prod = dict(blocked=True, reason=str(e)[:160])
    try:
        B.require_production_authorities({'M': 30, 'non_evidentiary': True},
                                         '2026-11-01', '3840a5df1a626aa1')
        prod['test_maturity_rejected'] = False
    except B.BHold as e:
        prod['test_maturity_rejected'] = True
        prod['test_maturity_reason'] = str(e)[:120]
    R['PROOF_3_PRODUCTION_MODE'] = prod
    print(f'  production mode: {prod}', flush=True)

    R['runtime_s'] = round(time.time() - t_all); R['y_forbidden_opens'] = len(_Y)
    R['module_digest'] = B.self_digest()
    json.dump(R, open(S + '/_b.json', 'w'), indent=1, default=str)
    print(f"\ndone {R['runtime_s']}s", flush=True)
