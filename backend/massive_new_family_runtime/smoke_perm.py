"""Permutation machinery per PERMUTATION_MAP_AUTHORITY_V1 ccdaa6a6f08e8588.
PRIMITIVE = whole session-date payload block.
ONE pi per session_length_class per draw, shared across ALL securities,
ALL THREE position pairs and ALL 20,751 claims."""
import hashlib, numpy as np
MASTER_SEED=20260829
def substream(name, idx=0):
    ss=np.random.SeedSequence(MASTER_SEED)
    names=['SCALE','NULL','CI','SMOKE']
    child=ss.spawn(len(names))[names.index(name)]
    return np.random.Generator(np.random.PCG64(child.spawn(idx+1)[idx]))

def date_map(date_class, rng, mode='CANONICAL', n_sec=None, n_pos=3):
    """Returns pi: (n_date,) int32 for CANONICAL. Mutation modes return richer shapes
    so the guards can detect them."""
    nd=len(date_class); pi=np.arange(nd,dtype=np.int32)
    for c in (0,1):
        idx=np.flatnonzero(date_class==c)
        pi[idx]=idx[rng.permutation(len(idx))]
    if mode=='CANONICAL': return pi
    if mode=='N4_CLASS_SWAP':                       # FULL <-> EARLY mixing
        allidx=np.arange(nd); pi2=allidx[rng.permutation(nd)]; return pi2.astype(np.int32)
    if mode=='N2_PER_SECURITY':                     # a different pi per security
        return np.stack([date_map(date_class,rng) for _ in range(n_sec)])
    if mode=='N3_PER_POSITION':                     # a different pi per position pair
        return np.stack([date_map(date_class,rng) for _ in range(n_pos)])
    raise ValueError(mode)

# ---------------- GUARDS ----------------
def guard_shape(pi, n_date):
    """One shared map => shape must be exactly (n_date,)."""
    if pi.ndim!=1: return False,'FAIL_SHARED_MAP / axis-specific map detected (ndim=%d)'%pi.ndim
    if pi.shape[0]!=n_date: return False,'FAIL_SHARED_MAP / wrong length'
    return True,'ok'
def guard_permutation(pi,n_date):
    return (np.array_equal(np.sort(pi),np.arange(n_date)), 'FAIL_NOT_A_PERMUTATION')
def guard_class_stratum(pi,date_class):
    ok=bool((date_class[pi]==date_class).all())
    return ok,'FAIL_SESSION_LENGTH_STRATUM'
def guard_cross_security_sync(pi,n_date):
    """The same pi must apply to every security: a (n_sec,n_date) map is a violation."""
    if pi.ndim>1: return False,'FAIL_CROSS_SECURITY_DATE_SYNC'
    return True,'ok'
def guard_cross_position_sync(pi_by_pos):
    """All three position pairs must receive the SAME pi."""
    a=pi_by_pos[0]
    ok=all(np.array_equal(a,p) for p in pi_by_pos)
    return ok,'FAIL_CROSS_POSITION_DATE_SYNC'
def guard_payload_atomicity(moved_value, moved_avail):
    return (moved_value==moved_avail),'FAIL_PAYLOAD_ATOMICITY'
def guard_active_window(applied):
    return bool(applied),'FAIL_ACTIVE_WINDOW'
def guard_fixed_max_set(T_by_draw):
    """Every draw's max must be taken over the SAME index set."""
    sets=[frozenset(np.flatnonzero(np.isfinite(t)).tolist()) for t in T_by_draw]
    return (len(set(sets))==1),'FAIL_FIXED_MAX_SET'

# ---------------- synthetic Y ----------------
def _u(tag,*keys):
    h=hashlib.sha256(('|'.join([tag]+[str(k) for k in keys])).encode()).digest()
    return int.from_bytes(h[:8],'big')/2**64
def synth_Y(sec_of,date_of,date_class,n_pos=3):
    n=len(sec_of); Y=np.zeros((n_pos,n),np.float64); A=np.zeros((n_pos,n),bool)
    ud={d:_u('DATE',d) for d in np.unique(date_of)}
    us={s:_u('SEC',s) for s in np.unique(sec_of)}
    up=[_u('POS',p) for p in range(n_pos)]
    for p in range(n_pos):
        for i in range(n):
            s=sec_of[i]; d=date_of[i]
            Y[p,i]=(0.50*ud[d] + 0.20*us[s] + 0.10*up[p] + 0.20*_u('CELL',s,d,p)
                    + 0.10*(1 if date_class[d]==1 else 0))
            A[p,i]= _u('MISS',s,d,p) >= 0.01
    return Y,A


# ================= TIME-LOCAL SYNCHRONIZED CIRCULAR MAP =================
# PERMUTATION_MAP_AUTHORITY_V1_AMENDMENT_1
# FULL_REGULAR : within each calendar year-month, ONE uniform cyclic offset per draw,
#                shared across all securities, all 3 position pairs, all 20,751 claims.
# EARLY_CLOSE  : identity — pi(d) = d. Not randomised in V1.
def date_map_timelocal(date_class, month_id, rng, mode='CANONICAL'):
    nd = len(date_class); pi = np.arange(nd, dtype=np.int32)
    for m in np.unique(month_id):
        idx = np.flatnonzero((month_id == m) & (date_class == 0))   # FULL only
        k = len(idx)
        if k == 0: continue
        if mode == 'N13_NONCYCLIC':
            pi[idx] = idx[rng.permutation(k)]                        # arbitrary, not cyclic
        else:
            off = int(rng.integers(k))
            pi[idx] = idx[(np.arange(k) + off) % k]
    if mode == 'N14_EARLY_RANDOMISED':
        e = np.flatnonzero(date_class == 1)
        if len(e) > 1: pi[e] = e[rng.permutation(len(e))]
    if mode == 'N12_GLOBAL':
        f = np.flatnonzero(date_class == 0); pi[f] = f[rng.permutation(len(f))]
    return pi

def guard_time_locality(pi, month_id, date_class):
    f = np.flatnonzero(date_class == 0)
    return bool((month_id[pi[f]] == month_id[f]).all()), 'FAIL_TIME_LOCALITY'

def guard_cyclic_structure(pi, month_id, date_class):
    """Within a month the map must be a CYCLIC SHIFT: the offset must be constant."""
    for m in np.unique(month_id):
        idx = np.flatnonzero((month_id == m) & (date_class == 0)); k = len(idx)
        if k < 2: continue
        pos = {d: i for i, d in enumerate(idx)}
        offs = {(pos[pi[d]] - i) % k for i, d in enumerate(idx)}
        if len(offs) != 1: return False, 'FAIL_CYCLIC_STRUCTURE'
    return True, 'ok'

def guard_early_close_identity(pi, date_class):
    e = np.flatnonzero(date_class == 1)
    return bool((pi[e] == e).all()), 'FAIL_EARLY_CLOSE_IDENTITY'


# ============ AMENDMENT_3: INFERENTIAL DATE SCOPE ============
# a11752cb3f51262d — restrict to the frozen inferential scope FIRST, then partition.
def date_map_scoped(date_class, month_id, in_scope, rng, mode='CANONICAL'):
    """in_scope: boolean mask of dates inside the frozen inferential scope (DEV).
    Order: 1 restrict -> 2 session_length_class -> 3 calendar year-month -> 4 cyclic shift."""
    nd=len(date_class); pi=np.arange(nd,dtype=np.int32)
    if mode=='OLD_UNSCOPED':
        for m in np.unique(month_id):
            idx=np.flatnonzero((month_id==m)&(date_class==0))     # NO scope restriction (the defect)
            k=len(idx)
            if k: off=int(rng.integers(k)); pi[idx]=idx[(np.arange(k)+off)%k]
        return pi
    for m in np.unique(month_id):
        idx=np.flatnonzero((month_id==m)&(date_class==0)&in_scope)   # scope FIRST
        k=len(idx)
        if k: off=int(rng.integers(k)); pi[idx]=idx[(np.arange(k)+off)%k]
    # EARLY_CLOSE stays identity; out-of-scope dates stay identity and never participate
    return pi

def guard_inferential_scope(pi, in_scope):
    """No in-scope date may draw from out-of-scope, and vice versa."""
    src=pi[np.flatnonzero(in_scope)]
    d2h=int((~in_scope[src]).sum())
    h=np.flatnonzero(~in_scope)
    h2d=int((in_scope[pi[h]]).sum()) if h.size else 0
    return (d2h==0 and h2d==0), f'FAIL_INFERENTIAL_SCOPE_GUARD d2h={d2h} h2d={h2d}'


def guard_cyclic_structure_scoped(pi, month_id, date_class, in_scope):
    """AMENDMENT_3: the cyclic universe is (month AND FULL_REGULAR AND in_scope).
    The parent guard tested (month AND FULL_REGULAR) and is superseded once the map
    is scope-restricted, because out-of-scope dates are held at identity by design."""
    for m in np.unique(month_id):
        idx=np.flatnonzero((month_id==m)&(date_class==0)&in_scope); k=len(idx)
        if k<2: continue
        pos={d:i for i,d in enumerate(idx)}
        offs={(pos[pi[d]]-i)%k for i,d in enumerate(idx)}
        if len(offs)!=1: return False,'FAIL_CYCLIC_STRUCTURE_SCOPED'
    return True,'ok'
