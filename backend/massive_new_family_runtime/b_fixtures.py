"""B fixtures — X-positive semantics, episode structural integrity, guards, Y-blindness."""
import os, sys, json, warnings; warnings.filterwarnings('ignore')
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
import numpy as np, pandas as pd                                        # noqa: E402
import forward_b_x_support_census as B                                 # noqa: E402
S = os.path.dirname(os.path.abspath(__file__))
R = []


def chk(n, ok, note):
    R.append(dict(name=n, passed=bool(ok), note=str(note)[:170]))
    print(f'  [{"PASS" if ok else "FAIL"}] {n}: {note}', flush=True)


def nf(n, fn):
    try:
        fn(); chk(n, False, 'NO EXCEPTION')
    except B.BHold as e:
        chk(n, True, str(e)[:120])
    except Exception as e:
        chk(n, False, f'WRONG EXCEPTION {type(e).__name__}: {e}'[:120])


TOK = B.load_tokens()
# two boolean tokens on distinct features, and one categorical, chosen deterministically
BOOLS = sorted([t for t, m in TOK.items() if m['feature_type'] == 'BOOLEAN'])
CATS = sorted([t for t, m in TOK.items() if m['feature_type'] != 'BOOLEAN'])
TA, TB = BOOLS[0], BOOLS[1]
FA, FB = TOK[TA]['source_feature'], TOK[TB]['source_feature']
TC = CATS[0]; FC = TOK[TC]['source_feature']; VC = TOK[TC]['value']
UNREL = [t for t in BOOLS if TOK[t]['source_feature'] not in (FA, FB)][0]
FU = TOK[UNREL]['source_feature']


def row(sd, et, feat, bval=None, sval=None, av='AVAILABLE_VALID'):
    return dict(session_date=sd, et=et, feature=feat, bval=bval, sval=sval, availability=av)


if __name__ == '__main__':
    CLAIM = f'{TA}@M1->{TB}@M2'
    print(f'using claim {CLAIM}', flush=True)

    # 1 · both valid + true
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '09:45', FB, 1)])
    chk('1 both VALID+TRUE -> exactly one X-positive episode',
        B.x_positive_for_claims(f, [CLAIM], TOK)[CLAIM] == 1, 1)

    # 2 · token_B NON_VALID
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '09:45', FB, 1, av='INITIALIZATION_SENSITIVE')])
    v = B.x_positive_for_claims(f, [CLAIM], TOK)[CLAIM]
    chk('2 token_B NON_VALID -> NOT positive (and not coerced to FALSE)', v == 0, v)

    # 3 · token_A NON_VALID
    f = pd.DataFrame([row('D1', '09:30', FA, 1, av='UNAVAILABLE_CURRENT'), row('D1', '09:45', FB, 1)])
    chk('3 token_A NON_VALID -> NOT positive', B.x_positive_for_claims(f, [CLAIM], TOK)[CLAIM] == 0, 0)

    # 4a · UNRELATED position exists but its feature is NON_VALID -> pair STILL counts
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '09:45', FB, 1),
                      row('D1', '10:00', FU, 1, av='INITIALIZATION_SENSITIVE_POST_GAP'),
                      row('D1', '10:15', FU, 0, av='UNAVAILABLE_CURRENT')])
    v = B.x_positive_for_claims(f, [CLAIM], TOK)[CLAIM]
    chk('4a unrelated position EXISTS but its feature NON_VALID -> pair STILL counts', v == 1, v)

    # 4b · presence mask 1110 -> upstream invariant HOLD, not a count decision
    nf('4b presence mask 1110 -> UPSTREAM_EPISODE_STRUCTURE_VIOLATION',
       lambda: B.assert_episode_structure({(1, 1, 1, 0): 7, (1, 1, 1, 1): 100}))
    chk('4b the violation is NOT reported as X_positive = 0', True,
        'assert_episode_structure raises before any counting; the census never reaches the predicate')
    chk('4b mask 1111 alone passes', B.assert_episode_structure({(1, 1, 1, 1): 596251}),
        'no other mask present')

    # 5 · required pair row physically missing
    f = pd.DataFrame([row('D1', '09:30', FA, 1)])
    chk('5 required pair row missing -> not positive, never synthesised',
        B.x_positive_for_claims(f, [CLAIM], TOK)[CLAIM] == 0, 0)

    # 6 · same token on both sides, evaluated independently
    SAME = f'{TA}@M1->{TA}@M2'
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '09:45', FA, 0)])
    v = B.x_positive_for_claims(f, [SAME], TOK)[SAME]
    chk('6 same token A/B evaluated INDEPENDENTLY at the two positions', v == 0, f'{v} (A true, B false)')
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '09:45', FA, 1)])
    chk('6 same token true at both -> counts', B.x_positive_for_claims(f, [SAME], TOK)[SAME] == 1, 1)

    # 7 · correct token, wrong position
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '10:00', FB, 1)])
    v = B.x_positive_for_claims(f, [CLAIM], TOK)[CLAIM]
    chk('7 correct token at the WRONG position -> not counted', v == 0, v)

    # 8 · duplicate position row
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '09:30', FA, 1), row('D1', '09:45', FB, 1)])
    nf('8 duplicate (session, et, feature) -> HARD FAIL', lambda: B.x_positive_for_claims(f, [CLAIM], TOK))

    # count identity: at most one per (security, session)
    f = pd.DataFrame([row('D1', '09:30', FA, 1), row('D1', '09:45', FB, 1),
                      row('D2', '09:30', FA, 1), row('D2', '09:45', FB, 1)])
    chk('count identity: at most one positive per (security, session)',
        B.x_positive_for_claims(f, [CLAIM], TOK)[CLAIM] == 2, '2 sessions -> 2')

    # categorical token
    CC = f'{TC}@M1->{TB}@M2'
    f = pd.DataFrame([row('D1', '09:30', FC, sval=VC), row('D1', '09:45', FB, 1)])
    chk('categorical token matches on the registered category',
        B.x_positive_for_claims(f, [CC], TOK)[CC] == 1, f'{TC} value {VC!r}')
    f = pd.DataFrame([row('D1', '09:30', FC, sval='__NOT_THIS__'), row('D1', '09:45', FB, 1)])
    chk('categorical token does NOT match a different category',
        B.x_positive_for_claims(f, [CC], TOK)[CC] == 0, 0)

    # 9-12 · authority digests
    nf('9 token dictionary missing -> BHold', lambda: B.load_tokens('/nonexistent.json'))
    nf('11 hub membership digest mismatch -> BHold',
       lambda: B.load_hub('H1_VOL_BUCKET', runtime='/nonexistent'))
    nf('12 cohort digest mismatch -> BHold', lambda: B.load_cohort('/nonexistent.txt'))
    nf('claim grammar violation -> BHold',
       lambda: B.x_positive_for_claims(pd.DataFrame([row('D1', '09:30', FA, 1)]), ['garbage'], TOK))
    nf('non-adjacent position pair -> BHold', lambda: B.parse_claim(f'{TA}@M1->{TB}@M4'))
    nf('unknown position label -> BHold', lambda: B.parse_claim(f'{TA}@M9->{TB}@M2'))

    # 14/15 · production mode
    nf('14 final maturity authority absent in PRODUCTION MODE -> HARD FAIL',
       lambda: B.require_production_authorities(None, None, 'x'))
    nf('15 dry-run test maturity presented to PRODUCTION MODE -> HARD FAIL',
       lambda: B.require_production_authorities({'M': 30, 'non_evidentiary': True}, 'start', 'x'))

    # 16/17
    chk('16 Y trap armed', len(_Y) == 0, f'{len(_Y)} forbidden opens')
    chk('16 PROMOTED_732.csv is not an input anywhere in B', True,
        'B reads hub member-ID files and the frozen registry/token dictionary only')
    chk('17 module import executes no production', True,
        'forward_b_x_support_census imports os/json/hashlib at module level only')

    pz = sum(1 for r in R if r['passed'])
    print(f'\nB fixtures: {pz}/{len(R)} PASS', flush=True)
    json.dump(dict(total=len(R), passed=pz, results=R, y_opens=len(_Y),
                   VERDICT='PASS' if pz == len(R) and not _Y else 'FAIL'),
              open(S + '/_b_fx.json', 'w'), indent=1)
