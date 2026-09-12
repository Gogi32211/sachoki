"""X-support universe reconciliation.

COUNT_A = pair-local episode universe   (only the two registered positions must exist)
COUNT_B = four-position episode universe (all M1..M4 must exist, then pair-local tokens)

Both are computed from the PERSISTED stores and compared against the sealed pre-Y oracle.
Y-blind: the audit hook below raises on any outcome artifact.
"""
import os, sys, json, time, warnings; warnings.filterwarnings('ignore')
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

D = '/Users/sachoki/MASSIVE_DATA/historical'
RT = '/Users/sachoki/Desktop/sachoki-desktop/backend/massive_new_family_runtime'
S = os.path.dirname(os.path.abspath(__file__))
DEV_END = '2026-08-06'

TOK = json.load(open(RT + '/TOKEN_DICTIONARY_123.json'))
MAN = json.load(open(RT + '/CANONICAL_69_MANIFEST_V2.json'))
tok_ids = sorted(TOK); TIDX = {t: i for i, t in enumerate(tok_ids)}
feats = sorted({TOK[t]['source_feature'] for t in tok_ids} | set(MAN))
FIDX = {f: i for i, f in enumerate(feats)}
NF, NT = len(feats), len(tok_ids)
assert NT == 123, NT
BOOL_T, CAT_T = {}, {}
for t in tok_ids:
    m = TOK[t]
    if m['feature_type'] == 'BOOLEAN':
        BOOL_T[m['source_feature']] = TIDX[t]
    else:
        CAT_T[(m['source_feature'], m['value'])] = TIDX[t]
ET = ['09:30', '09:45', '10:00', '10:15']; EIDX = {e: i for i, e in enumerate(ET)}
PP = [(0, 1), (1, 2), (2, 3)]; PPN = ['M1->M2', 'M2->M3', 'M3->M4']

if __name__ == '__main__':
    con = duckdb.connect(D + '/POS4.duckdb', read_only=True); con.execute("pragma threads=8")
    keys = [r[0] for r in con.execute("select distinct k from pos4 order by k").fetchall()]
    NS = len(keys)
    EV = np.zeros((3, NF, NF), np.int64)
    PO = np.zeros((3, NT, NT), np.int64)
    t0 = time.time()
    for si, k in enumerate(keys):
        d = con.execute("""select session_date, et, feature, bval, sval, availability
                           from pos4 where k=? order by session_date""", [k]).df()
        if not len(d):
            continue
        sess = sorted(d.session_date.unique()); SI = {s: i for i, s in enumerate(sess)}
        n = len(sess)
        dev = np.array([s <= DEV_END for s in sess])
        si_arr = d.session_date.map(SI).to_numpy()
        ei = d.et.map(EIDX).to_numpy(); fi = d.feature.map(FIDX).to_numpy()
        valid = (d.availability.to_numpy() == 'AVAILABLE_VALID')
        V = np.zeros((4, NF, n), np.float32)
        V[ei[valid], fi[valid], si_arr[valid]] = 1.0
        Tm = np.zeros((4, NT, n), np.float32)
        bv = d.bval.to_numpy(); sv = d.sval.to_numpy(); ft = d.feature.to_numpy()
        for j in np.nonzero(valid)[0]:
            f = ft[j]
            ti = BOOL_T.get(f)
            if ti is not None:
                if bv[j] == 1:
                    Tm[ei[j], ti, si_arr[j]] = 1.0
            else:
                ti = CAT_T.get((f, sv[j] if sv[j] is not None else ''))
                if ti is not None:
                    Tm[ei[j], ti, si_arr[j]] = 1.0
        if dev.any():
            for p, (L, R) in enumerate(PP):
                EV[p] += (V[L][:, dev] @ V[R][:, dev].T).astype(np.int64)
                PO[p] += (Tm[L][:, dev] @ Tm[R][:, dev].T).astype(np.int64)
        if (si + 1) % 100 == 0:
            print(f'  {si+1}/{NS} {time.time()-t0:.0f}s', flush=True)
    print(f'census rebuilt in {time.time()-t0:.0f}s', flush=True)

    SRC = np.array([FIDX[TOK[t]['source_feature']] for t in tok_ids])
    rows = []
    for p, pn in enumerate(PPN):
        L, Rg = pn.split('->')
        for ai, a in enumerate(tok_ids):
            for bi, b in enumerate(tok_ids):
                rows.append(dict(claim_id=f'{a}@{L}->{b}@{Rg}',
                                 DEV_evaluable_rebuilt=int(EV[p][SRC[ai], SRC[bi]]),
                                 DEV_positive_rebuilt=int(PO[p][ai, bi])))
    R = pd.DataFrame(rows)
    o = duckdb.connect(D + '/SUPPORT_CENSUS.duckdb', read_only=True)
    ref = o.execute("select claim_id, DEV_evaluable, DEV_positive from census").df()
    m = ref.merge(R, on='claim_id', how='outer', indicator=True)
    res = dict(
        rebuilt_rows=len(R), oracle_rows=len(ref),
        join_left_only=int((m._merge == 'left_only').sum()),
        join_right_only=int((m._merge == 'right_only').sum()),
        positive_mismatches=int((m.DEV_positive != m.DEV_positive_rebuilt).sum()),
        evaluable_mismatches=int((m.DEV_evaluable != m.DEV_evaluable_rebuilt).sum()),
        elapsed_s=round(time.time() - t0), y_forbidden_opens=len(_Y))
    res['VERDICT'] = ('EXACT' if (res['positive_mismatches'] == 0 and res['evaluable_mismatches'] == 0
                                  and res['join_left_only'] == 0 and res['join_right_only'] == 0)
                      else 'MISMATCH')
    bad = m[(m.DEV_positive != m.DEV_positive_rebuilt) | (m.DEV_evaluable != m.DEV_evaluable_rebuilt)]
    res['sample_mismatches'] = bad.head(5).to_dict('records')
    print(json.dumps(res, indent=1, default=str), flush=True)
    json.dump(res, open(S + '/_b_reconcile.json', 'w'), indent=1, default=str)
