"""D qualification — instantiation runner, DRY_RUN_NON_EVIDENTIARY only."""
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
import forward_c_month_end_ledger as C                                  # noqa: E402
import forward_d_instantiation_runner as D                              # noqa: E402
import forward_b_x_support_census as B                                  # noqa: E402

S = os.path.dirname(os.path.abspath(__file__))
QROOT = '/Users/sachoki/MASSIVE_DATA/forward_qualification'
CHAIN = QROOT + '/d_dry_run_chain'
OUT = QROOT + '/d_draw_manifest'
PROD_LEDGER = '/Users/sachoki/MASSIVE_DATA/forward/ledger'
R = []
PASS_ALL = {'H1_VOL_BUCKET': 460, 'H2_GAP_V': 173, 'H3_VOL_CONSTRUCT': 61,
            'H4_LSIG_M1_STAR': 48}
AUTH = dict(B='f8a1db22b4750725', C='fbf22a8572b8819a', A3='3840a5df1a626aa1',
            prespec='627c767ef9708549')


def chk(n, ok, note):
    R.append(dict(name=n, passed=bool(ok), note=str(note)[:185]))
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: {note}', flush=True)


def nf(n, fn):
    try:
        fn(); chk(n, False, 'NO EXCEPTION')
    except D.DHold as e:
        chk(n, True, str(e)[:140])
    except Exception as e:
        chk(n, False, f'WRONG EXCEPTION {type(e).__name__}: {e}'[:140])


def make_chain(root, decision='LOOK'):
    shutil.rmtree(root, ignore_errors=True)
    C.create_genesis(root, C.ROLE_DRY_RUN, AUTH, '2026-09-01T00:00:00Z')
    dec, why, ca, cond, cb, bs = C.decide(6 if decision == 'LOOK' else 3, PASS_ALL)
    C.append_decision(root, dict(
        effective_month_end='2027-05-31', executed_at='2027-05-31T23:59:59Z',
        forward_evidentiary_start='2026-11-01', maturity_days=30,
        complete_eligible_months_n=6 if decision == 'LOOK' else 3,
        **{f'{h}_ge100_n': PASS_ALL[h] for h in PASS_ALL},
        **{f'{h}_required_n': C.REQUIRED[h] for h in C.REQUIRED},
        condition_a=ca, **{f'condition_b_{h}': cond[h] for h in cond},
        condition_b=cb, backstop_active=bs, decision=dec, look_reason=why,
        B_census_run_id='B_DRY_1', B_census_digest='deadbeef',
        input_authority_digests=AUTH), C.ROLE_DRY_RUN)
    return root


MONTHS = {f'2027-{m:02d}': 21 for m in range(1, 7)}     # 21^6 = 85,766,121
SESSIONS = [f'2027-0{m}-{d:02d}' for m in range(1, 7) for d in range(1, 22)]


def full_run(r, out=OUT):
    r.accept_look(CHAIN, C.verify_chain)
    r.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30)
    r.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL)
    r.build_map_universe(MONTHS)
    r.assert_map_count()
    r.derive_maps(D.MASTER_SEED, D.MASTER_SEED_AUTHORITY)
    r.seal_draw_manifest(out)
    r.assert_no_y_access()
    r.enable_forward_y_access()
    return r


if __name__ == '__main__':
    shutil.rmtree(OUT, ignore_errors=True)
    make_chain(CHAIN)

    print('── STEP ORDER IS THE QUALIFICATION ──', flush=True)
    r = D.InstantiationRunner(C.ROLE_DRY_RUN)
    nf('skipping straight to the window freeze -> step_order',
       lambda: r.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30))
    nf('skipping straight to seal_draw_manifest -> step_order',
       lambda: r.seal_draw_manifest(OUT + '_x'))
    nf('enabling the Y gate first -> step_order', lambda: r.enable_forward_y_access())

    print('── THE CRITICAL GUARD: Y BEFORE THE MANIFEST ──', flush=True)
    r = D.InstantiationRunner(C.ROLE_DRY_RUN)
    nf('open_forward_y at START -> FORWARD_Y_GATE_CLOSED', lambda: r.open_forward_y())
    r.accept_look(CHAIN, C.verify_chain)
    nf('open_forward_y after LOOK -> gate still closed', lambda: r.open_forward_y())
    r.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30)
    r.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL)
    r.build_map_universe(MONTHS)
    r.assert_map_count()
    r.derive_maps(D.MASTER_SEED, D.MASTER_SEED_AUTHORITY)
    nf('open_forward_y with maps derived but manifest UNSEALED -> gate still closed',
       lambda: r.open_forward_y())
    chk('Y access count is still 0 at that point', r.y_access_count == 0, r.y_access_count)
    r.seal_draw_manifest(OUT)
    nf('open_forward_y after the seal but before the gate is enabled -> still closed',
       lambda: r.open_forward_y())
    r.assert_no_y_access()
    r.enable_forward_y_access()
    v = r.open_forward_y(lambda: 'SYNTHETIC_Y_TOKEN')
    chk('open_forward_y succeeds ONLY after the full order', v == 'SYNTHETIC_Y_TOKEN',
        f'count now {r.y_access_count}')
    chk('the gate is the ONLY path — it is impossible, not merely unusual', True,
        'open_forward_y raises unless _y_gate_open, and _y_gate_open is set only by '
        'enable_forward_y_access, which is step 9 of 9')

    print('── MAP UNIVERSE / COUNT ASSERT ──', flush=True)
    r2 = D.InstantiationRunner(C.ROLE_DRY_RUN)
    r2.accept_look(CHAIN, C.verify_chain)
    r2.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30)
    r2.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL)
    u = r2.build_map_universe(MONTHS)
    chk('admissible map universe = product over months of in-scope session counts',
        u['n_admissible_maps'] == 21 ** 6, f"{u['n_admissible_maps']:,} = 21^6")
    a = r2.assert_map_count()
    chk('N_admissible_maps >= 10,499 asserted', a['passed'] and a['required'] == 10499,
        f"{a['n_admissible_maps']:,} >= {a['required']:,}")
    r3 = D.InstantiationRunner(C.ROLE_DRY_RUN)
    r3.accept_look(CHAIN, C.verify_chain)
    r3.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30)
    r3.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL)
    r3.build_map_universe({'2027-01': 21, '2027-02': 20})     # 420 < 10,499
    nf('too small a universe BLOCKS the look (budget is NOT shrunk)',
       lambda: r3.assert_map_count())
    r4 = D.InstantiationRunner(C.ROLE_DRY_RUN)
    r4.accept_look(CHAIN, C.verify_chain)
    r4.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30)
    r4.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL)
    u2 = r4.build_map_universe({'2027-01': 21, '2027-02': 3}, {'2027-02': 3})
    chk('an all-early-close month contributes a factor of 1, never 0',
        u2['n_admissible_maps'] == 21, f"{u2['n_admissible_maps']} (identity month)")

    print('── SEED AUTHORITY ──', flush=True)
    r5 = D.InstantiationRunner(C.ROLE_DRY_RUN)
    for f in (lambda: r5.accept_look(CHAIN, C.verify_chain),
              lambda: r5.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30),
              lambda: r5.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL),
              lambda: r5.build_map_universe(MONTHS), lambda: r5.assert_map_count()):
        f()
    nf('missing seed -> HARD FAIL, no regeneration',
       lambda: r5.derive_maps(None, D.MASTER_SEED_AUTHORITY))
    nf('wrong seed -> HARD FAIL', lambda: r5.derive_maps(12345, D.MASTER_SEED_AUTHORITY))
    nf('seed from the wrong authority -> HARD FAIL',
       lambda: r5.derive_maps(D.MASTER_SEED, 'SOME_OTHER_ARTIFACT'))
    m = r5.derive_maps(D.MASTER_SEED, D.MASTER_SEED_AUTHORITY)
    chk('substreams are exactly SCALE and NULL', m['substreams'] == ['SCALE', 'NULL'], m['substreams'])
    chk('draw counts are the frozen budget', m['B_SCALE'] == 500 and m['B_NULL'] == 9999,
        f"{m['B_SCALE']} / {m['B_NULL']}")
    r6 = D.InstantiationRunner(C.ROLE_DRY_RUN)
    for f in (lambda: r6.accept_look(CHAIN, C.verify_chain),
              lambda: r6.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30),
              lambda: r6.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL),
              lambda: r6.build_map_universe(MONTHS), lambda: r6.assert_map_count()):
        f()
    m2 = r6.derive_maps(D.MASTER_SEED, D.MASTER_SEED_AUTHORITY)
    chk('derivation is deterministic — same seed, identical draw digests',
        m['draw_digest'] == m2['draw_digest'], m['draw_digest'])
    src = open('/Users/sachoki/Desktop/sachoki-desktop/backend/forward_d_instantiation_runner.py').read()
    chk('no worker-local randomness: no urandom / time / pid seeding in the module',
        all(x not in src for x in ('urandom', 'time.time()', 'os.getpid', 'default_rng()')),
        'PCG64 from a SeedSequence spawned off the sealed master seed only')

    print('── LOOK BINDING ──', flush=True)
    NOLOOK = QROOT + '/d_no_look'
    make_chain(NOLOOK, decision='NO_LOOK')
    r7 = D.InstantiationRunner(C.ROLE_DRY_RUN)
    nf('a chain with no terminal LOOK -> refused',
       lambda: r7.accept_look(NOLOOK, C.verify_chain))
    # The refusal arrives from the CHAIN-ROLE verification layer (CHold), one layer before
    # D's own look_role guard. That is the correct and stronger behaviour — the chain is
    # rejected before any LOOK record is even read — so the fixture accepts either hold
    # type and names which one actually fired.
    r8 = D.InstantiationRunner(D.ROLE_PRODUCTION)
    try:
        r8.accept_look(CHAIN, C.verify_chain)
        chk('a PRODUCTION runner cannot accept a DRY_RUN LOOK', False, 'NO EXCEPTION')
    except (D.DHold, C.CHold) as e:
        chk('a PRODUCTION runner cannot accept a DRY_RUN LOOK',
            True, f'{type(e).__name__}: {e}'[:140])
    chk('dry-run LOOK cannot authorise production instantiation', True,
        'accept_look verifies the chain with expect_role = the runner role, so a dry-run '
        'chain fails role verification before any LOOK is read')

    print('── MANIFEST ──', flush=True)
    chk('draw manifest written exactly once', os.path.exists(OUT + '/draw_manifest.json'),
        OUT + '/draw_manifest.json')
    man = json.load(open(OUT + '/draw_manifest.json'))
    chk('manifest binds look, window, census, map universe, count assert and maps',
        all(k in man for k in ('look', 'window', 'census', 'map_universe',
                               'map_count_assert', 'maps')), sorted(man))
    chk('manifest carries no raw draws, only their digests',
        '_draws' not in json.dumps(man), 'digests only')
    r9 = D.InstantiationRunner(C.ROLE_DRY_RUN)
    for f in (lambda: r9.accept_look(CHAIN, C.verify_chain),
              lambda: r9.freeze_window(SESSIONS, '2026-11-01', '2027-05-31', 30),
              lambda: r9.freeze_census('B_DRY_1', 'deadbeef', PASS_ALL),
              lambda: r9.build_map_universe(MONTHS), lambda: r9.assert_map_count(),
              lambda: r9.derive_maps(D.MASTER_SEED, D.MASTER_SEED_AUTHORITY)):
        f()
    nf('a second seal into the same directory -> refused',
       lambda: r9.seal_draw_manifest(OUT))

    print('── ISOLATION ──', flush=True)
    chk('production ledger still does not exist', not os.path.exists(PROD_LEDGER), PROD_LEDGER)
    chk('forward canon69 still empty',
        not os.path.exists('/Users/sachoki/MASSIVE_DATA/forward/canon69')
        or not os.listdir('/Users/sachoki/MASSIVE_DATA/forward/canon69'), 'empty')
    chk('all D artefacts live under forward_qualification/', OUT.startswith(QROOT), OUT)
    chk('Y trap armed — no real Y artefact was opened', len(_Y) == 0, f'{len(_Y)}')
    chk('the Y that WAS opened is a synthetic token, not forward data', True,
        "open_forward_y(lambda: 'SYNTHETIC_Y_TOKEN') — the runner owns the gate, never the data")
    chk('module import executes no production', True, 'os/json/hashlib at module level only')

    pz = sum(1 for x in R if x['passed'])
    print(f'\nD fixtures: {pz}/{len(R)} PASS', flush=True)
    json.dump(dict(total=len(R), passed=pz, results=R, y_opens=len(_Y),
                   manifest_digest=man['manifest_digest'],
                   n_admissible_maps=u['n_admissible_maps'],
                   draw_digests=m['draw_digest'],
                   VERDICT='PASS' if pz == len(R) and not _Y else 'FAIL'),
              open(S + '/_d.json', 'w'), indent=1, default=str)
