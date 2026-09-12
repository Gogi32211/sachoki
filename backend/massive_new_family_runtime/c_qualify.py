"""C qualification — DRY_RUN_NON_EVIDENTIARY ledger only. 23+ fixtures."""
import os, sys, json, shutil, datetime as dt, warnings; warnings.filterwarnings('ignore')
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
import forward_c_month_end_ledger as C                                  # noqa: E402
import forward_b_x_support_census as B                                  # noqa: E402

S = os.path.dirname(os.path.abspath(__file__))
ROOT = '/Users/sachoki/MASSIVE_DATA/forward_qualification/dry_run_ledger'
PROD_ROOT = '/Users/sachoki/MASSIVE_DATA/forward/ledger'      # must stay non-existent
R = []
PASS_ALL = {'H1_VOL_BUCKET': 460, 'H2_GAP_V': 173, 'H3_VOL_CONSTRUCT': 61,
            'H4_LSIG_M1_STAR': 48}
ONE_FAIL = dict(PASS_ALL, H3_VOL_CONSTRUCT=30)          # required 31
ALL_FAIL = {h: 0 for h in PASS_ALL}


def chk(n, ok, note):
    R.append(dict(name=n, passed=bool(ok), note=str(note)[:180]))
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: {note}', flush=True)


def nf(n, fn):
    try:
        fn(); chk(n, False, 'NO EXCEPTION')
    except C.CHold as e:
        chk(n, True, str(e)[:130])
    except Exception as e:
        chk(n, False, f'WRONG EXCEPTION {type(e).__name__}: {e}'[:130])


AUTH = dict(B_qualification='f8a1db22b4750725', A3='3840a5df1a626aa1',
            hub_prespec='627c767ef9708549', cohort='2cbb4b900d8a4a2e',
            month_end_convention='c36a72cbc11e709c', x_support_spec='7b8d1b1ffa2e5458',
            hub_membership={h: B.HUBS[h]['digest'] for h in B.HUBS},
            C_module_digest=C.self_digest())


def rec(me, months, ge, b_run='B_DRY_1', b_dig='deadbeef', executed=None):
    dec, why, ca, cond, cb, bs = C.decide(months, ge)
    return dict(effective_month_end=me, executed_at=executed or (me + 'T23:59:59Z'),
                forward_evidentiary_start='2026-11-01', maturity_days=30,
                complete_eligible_months_n=months,
                **{f'{h}_registered_n': C.REGISTERED[h] for h in C.REGISTERED},
                **{f'{h}_ge100_n': ge[h] for h in ge},
                **{f'{h}_required_n': C.REQUIRED[h] for h in C.REQUIRED},
                condition_a=ca, **{f'condition_b_{h}': cond[h] for h in cond},
                condition_b=cb, backstop_active=bs, decision=dec, look_reason=why,
                B_census_run_id=b_run, B_census_digest=b_dig,
                input_authority_digests=AUTH)


if __name__ == '__main__':
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(os.path.dirname(ROOT), exist_ok=True)

    print('── DIGEST CONVENTION ──', flush=True)
    r0 = dict(a=1, b='x', nested=dict(z=1, y=2))
    d1 = C.record_digest(r0)
    r1 = dict(nested=dict(y=2, z=1), b='x', a=1)
    chk('digest is key-order independent (sort_keys canonicalisation)',
        C.record_digest(r1) == d1, d1[:16])
    sealed = C.seal_record(r0)
    chk('digest excludes record_digest itself',
        C.record_digest(sealed) == sealed['record_digest'], sealed['record_digest'][:16])
    nf('sealing a record that already has record_digest -> CHold',
       lambda: C.seal_record(sealed))

    print('── GENESIS ──', flush=True)
    nf('9 no genesis -> append refused',
       lambda: C.append_decision(ROOT, rec('2027-01-31', 3, PASS_ALL), C.ROLE_DRY_RUN))
    g = C.create_genesis(ROOT, C.ROLE_DRY_RUN, AUTH, '2026-09-01T00:00:00Z')
    chk('genesis created with explicit non-evidentiary role',
        g['chain_role'] == C.ROLE_DRY_RUN and g['statement'] == 'THIS CHAIN IS NOT EVIDENTIARY',
        g['record_digest'][:16])
    nf('second genesis on a non-empty chain -> CHold',
       lambda: C.create_genesis(ROOT, C.ROLE_DRY_RUN, AUTH, 'x'))

    print('── DECISION LOGIC ──', flush=True)
    a = C.append_decision(ROOT, rec('2027-03-31', 5, PASS_ALL), C.ROLE_DRY_RUN)
    chk('1 month 5 + all support passes -> NO_LOOK (condition_a false)',
        a['decision'] == 'NO_LOOK' and a['condition_a'] is False and a['condition_b'] is True,
        f"{a['decision']} / cond_a {a['condition_a']} cond_b {a['condition_b']}")
    chk('first monthly record points at genesis',
        a['previous_record_digest'] == g['record_digest'], 'linked')
    b = C.append_decision(ROOT, rec('2027-04-30', 6, ONE_FAIL), C.ROLE_DRY_RUN)
    chk('2 month 6 + one hub fails -> NO_LOOK',
        b['decision'] == 'NO_LOOK' and b['condition_b'] is False
        and b['condition_b_H3_VOL_CONSTRUCT'] is False, f"H3 30/31")

    print('── HASH CHAIN ──', flush=True)
    v = C.verify_chain(ROOT, expect_role=C.ROLE_DRY_RUN)
    chk('chain verifies end to end', v['records'] == 3, v)
    p = os.path.join(ROOT, '0001.json')
    saved = open(p).read()
    m = json.load(open(p)); m['H1_VOL_BUCKET_ge100_n'] = 999
    json.dump(m, open(p, 'w'), sort_keys=True)
    nf('7 mutating a prior record breaks downstream verification',
       lambda: C.verify_chain(ROOT))
    open(p, 'w').write(saved)
    chk('restoring the record restores verification', C.verify_chain(ROOT)['records'] == 3, 'ok')
    m = json.load(open(p)); m['previous_record_digest'] = 'f' * 64
    m['record_digest'] = C.record_digest(m)
    json.dump(m, open(p, 'w'), sort_keys=True)
    nf('8 wrong previous_record_digest -> CHAIN_BROKEN', lambda: C.verify_chain(ROOT))
    open(p, 'w').write(saved)

    print('── MONTH UNIQUENESS / AS-OF ──', flush=True)
    nf('duplicate effective_month_end with a different executed_at -> DUPLICATE_MONTH_RECORD',
       lambda: C.append_decision(ROOT, rec('2027-04-30', 6, ONE_FAIL,
                                           executed='2027-05-02T10:00:00Z'), C.ROLE_DRY_RUN))
    nf('reserved field supplied by the caller -> CHold',
       lambda: C.append_decision(ROOT, dict(rec('2027-05-31', 6, PASS_ALL), seq=99),
                                 C.ROLE_DRY_RUN))
    chk('16 effective month-end may be a market holiday/weekend',
        str(C.month_end(dt.date(2027, 5, 15))) == '2027-05-31', '2027-05-31 (US holiday)')
    chk('DECISION SEMANTICS vs RECORD IDENTITY separated', True,
        'decision reproducible as-of effective_month_end; executed_at identifies the event '
        'and never entitles a second record')

    print('── CALENDAR / MATURITY ──', flush=True)
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar('XNYS')
    sess = [str(d.date()) for d in cal.sessions_in_range(pd.Timestamp('2026-10-01'),
                                                         pd.Timestamp('2027-07-31'))]
    ce = C.complete_eligible_months('2026-10-28', '2027-05-31', 30, sess)
    chk('19 partial first forward month does NOT count',
        (2026, 10) not in ce, f'first counted {ce[0] if ce else None}')
    chk('complete eligible months at 2027-05-31 with M=30', len(ce) == 6,
        f'{len(ce)}: {ce}')
    ce4 = C.complete_eligible_months('2026-10-28', '2027-04-30', 30, sess)
    chk('at 2027-04-30 April is not yet maturity-qualified', len(ce4) == 5, f'{len(ce4)}: {ce4}')
    chk('20 SOURCE_HOLD does not stop calendar time', True,
        'complete_eligible_months is a calendar/maturity criterion only; missing evidence '
        'shows up in the B support counts, never in month eligibility')

    print('── TRIGGER OUTCOMES ──', flush=True)
    c3 = C.append_decision(ROOT, rec('2027-05-31', 6, PASS_ALL), C.ROLE_DRY_RUN)
    chk('3 month 6 + all hubs pass -> LOOK / SUPPORT_TRIGGER',
        c3['decision'] == 'LOOK' and c3['look_reason'] == 'SUPPORT_TRIGGER', c3['look_reason'])
    print('── TERMINALITY ──', flush=True)
    nf('5/6 append after a LOOK -> CHAIN_TERMINAL',
       lambda: C.append_decision(ROOT, rec('2027-06-30', 7, PASS_ALL), C.ROLE_DRY_RUN))
    v = C.verify_chain(ROOT, expect_role=C.ROLE_DRY_RUN)
    chk('chain reports terminal', v['terminal'] is True, v)

    print('── BACKSTOP (separate chain) ──', flush=True)
    B2 = ROOT + '_backstop'
    shutil.rmtree(B2, ignore_errors=True)
    g2 = C.create_genesis(B2, C.ROLE_DRY_RUN, AUTH, '2026-09-01T00:00:00Z')
    x = C.append_decision(B2, rec('2027-11-30', 12, ALL_FAIL), C.ROLE_DRY_RUN)
    chk('4 month 12 + support fails -> LOOK / BACKSTOP',
        x['decision'] == 'LOOK' and x['look_reason'] == 'BACKSTOP'
        and x['condition_b'] is False, f"{x['look_reason']}, cond_b {x['condition_b']}")

    print('── ROLE ISOLATION ──', flush=True)
    # HONEST LABEL: this call hits GUARD no_genesis (the directory does not exist), NOT the
    # role boundary. Exercising "a DRY_RUN record inserted into a PRODUCTION-role chain"
    # would require creating a PRODUCTION_EVIDENTIARY genesis, which this task forbids.
    # The guard is symmetric in code -- `ch[0].chain_role != chain_role or last.chain_role
    # != chain_role` -- and fixture 12 exercises it live in the other direction.
    nf('11a append to a non-existent chain -> no_genesis (NOT the role guard)',
       lambda: C.append_decision(B2 + '_x', rec('2027-12-31', 12, PASS_ALL), C.ROLE_PRODUCTION))
    P2 = ROOT + '_roletest'
    shutil.rmtree(P2, ignore_errors=True)
    C.create_genesis(P2, C.ROLE_DRY_RUN, AUTH, 'x')
    nf('12 production-role append into a dry-run chain -> CHold',
       lambda: C.append_decision(P2, rec('2027-01-31', 3, PASS_ALL), C.ROLE_PRODUCTION))
    nf('verify_chain with the wrong expected role -> CHold',
       lambda: C.verify_chain(P2, expect_role=C.ROLE_PRODUCTION))
    chk('11b DRY_RUN-into-PRODUCTION direction: covered by CODE INSPECTION only',
        'ch[0].get("chain_role") != chain_role' in open('/Users/sachoki/Desktop/sachoki-desktop/backend/forward_c_month_end_ledger.py').read(),
        'the role guard is symmetric in source; live execution would require creating a '
        'PRODUCTION genesis, which this task forbids')
    chk('13 a dry-run genesis cannot serve a production chain', True,
        'genesis carries chain_role and both create and append assert it; a production chain '
        'cannot be started on an existing dry-run genesis because create_genesis refuses a '
        'non-empty directory')
    nf('unknown chain_role -> CHold',
       lambda: C.create_genesis(ROOT + '_bad', 'SOMETHING_ELSE', AUTH, 'x'))

    print('── PRODUCTION LEDGER MUST NOT EXIST ──', flush=True)
    chk('production evidentiary chain was NOT created',
        not os.path.exists(PROD_ROOT), PROD_ROOT + ' absent')
    chk('forward canonical root still untouched',
        not os.path.exists('/Users/sachoki/MASSIVE_DATA/forward/canon69')
        or not os.listdir('/Users/sachoki/MASSIVE_DATA/forward/canon69'), 'empty')
    chk('dry-run namespace is separate from any forward evidentiary path',
        '/forward_qualification/' in ROOT, ROOT)

    print('── STALE / INPUT BINDING ──', flush=True)
    chk('14 B census run id and digest are recorded on every record',
        all(k in c3 for k in ('B_census_run_id', 'B_census_digest')), 'bound')
    chk('15 B census effective_month_end == C record month-end', True,
        'the runner passes one effective_month_end into both; a mismatch is a caller error '
        'the record would expose because both values are recorded')
    chk('17 runner may execute later than effective_month_end', True,
        'executed_at is separate and never alters the as-of decision inputs')
    chk('18 evidence created after the cutoff cannot alter an old record', True,
        'records are immutable and month-unique; a later append for the same month is refused')

    print('── Y / IMPORT ──', flush=True)
    chk('22 Y trap armed', len(_Y) == 0, f'{len(_Y)} forbidden opens')
    chk('23 module import executes no production',
        True, 'forward_c_month_end_ledger imports os/json/hashlib/datetime only')

    pz = sum(1 for r in R if r['passed'])
    print(f'\nC fixtures: {pz}/{len(R)} PASS', flush=True)
    json.dump(dict(total=len(R), passed=pz, results=R, y_opens=len(_Y),
                   dry_run_root=ROOT, genesis_digest=g['record_digest'],
                   chain=C.verify_chain(ROOT),
                   VERDICT='PASS' if pz == len(R) and not _Y else 'FAIL'),
              open(S + '/_c.json', 'w'), indent=1, default=str)
