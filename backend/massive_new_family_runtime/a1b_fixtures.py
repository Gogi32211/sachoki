"""A1b boundary fixtures — 9 required, each with OUTPUT-STATE assertions.

Every fixture mutates a REAL session's 1m input and asserts what the FROZEN producer
does with it. Nothing here reimplements producer semantics; the assertions are on the
producer's output or on its refusal to produce.
"""
import os, sys, json, warnings; warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/sachoki/Desktop/sachoki-desktop/backend')
os.chdir('/Users/sachoki/Desktop/sachoki-desktop/backend')
import forward_stagea_a1b as W
import coarse_production_v1 as P
from coarse_aggregator_v1 import AggHold
import pandas as pd, numpy as np

CAL = W.calendar(); COH = W.load_cohort()
NORMAL = '2023-06-15'      # 390-minute regular session
EARLY = '2024-11-29'       # 210-minute early close (day after Thanksgiving)
R = []


def chk(fid, name, cond, detail):
    R.append(dict(fixture=fid, name=name, PASS=bool(cond), detail=detail))
    print(f"  [{'PASS' if cond else 'FAIL'}] {fid} {name}: {detail}", flush=True)
    return bool(cond)


def base(day):
    ms, b = P.load(day)
    ms = ms[ms.security_key_v1.isin(COH)].reset_index(drop=True)
    b = b[b.security_key_v1.isin(COH)].reset_index(drop=True)
    return ms, b


def run(day, ms, b):
    return W.build_session(day, ms, b, COH, CAL)


def one_key(ms):
    """a security with a fully COMPLETE first interval, so mutations are unambiguous"""
    for k, g in ms.groupby('security_key_v1'):
        if (g.observation_state == 'OBSERVED').all():
            return k
    raise SystemExit('no fully-observed security in the fixture session')


print('=== FIXTURE 1 · normal full session ===')
ms, b = base(NORMAL); out, _ = run(NORMAL, ms, b)
o_open, o_close, n_min = W.CoarseAggregatorV1(CAL, COH).session_bounds(NORMAL)
chk(1, 'session length', n_min == 390, f'{n_min} minutes')
chk(1, 'interval count', sorted(out.interval_index.unique()) == list(range(26)),
    f'{out.interval_index.nunique()} intervals, 0..{out.interval_index.max()}')
exp = {i: o_open + i * 15 * 60000 for i in range(26)}
got = out.groupby('interval_index').bar_start.agg(['min', 'max'])
chk(1, 'bar_start exact', all(got.loc[i, 'min'] == got.loc[i, 'max'] == exp[i] for i in range(26)),
    'bar_start == session_open + i*15min for every i')
chk(1, 'COMPLETE present', (out.coverage_state == 'COMPLETE').sum() > 0,
    f"COMPLETE {int((out.coverage_state=='COMPLETE').sum()):,} of {len(out):,}")

print('=== FIXTURE 2 · early close ===')
ms2, b2 = base(EARLY); out2, _ = run(EARLY, ms2, b2)
eo, ec, en = W.CoarseAggregatorV1(CAL, COH).session_bounds(EARLY)
chk(2, 'session length', en == 210, f'{en} minutes')
chk(2, 'own boundary respected', sorted(out2.interval_index.unique()) == list(range(14)),
    f'{out2.interval_index.nunique()} intervals, max {out2.interval_index.max()}')
chk(2, 'no synthetic post-close interval', out2.bar_start.max() + 15 * 60000 <= ec,
    f'max bar_start + 15m = {out2.bar_start.max()+900000} <= close {ec}')

print('=== FIXTURE 3 · missing INTERIOR 1m bar ===')
K = one_key(ms)
m3 = ms.copy(); sel = m3[m3.security_key_v1 == K].sort_values('minute_ts')
tgt = sel.iloc[100]                                   # interval 6, interior
m3.loc[tgt.name, 'observation_state'] = 'UNOBSERVED'
b3 = b[~((b.security_key_v1 == K) & (b.minute_ts == tgt.minute_ts))]
out3, _ = run(NORMAL, m3, b3)
row = out3[(out3.k == K) & (out3.interval_index == 100 // 15)]
chk(3, 'contaminated not COMPLETE', row.coverage_state.iloc[0] == 'UNOBSERVED_CONTAMINATED',
    f'interval {100//15} -> {row.coverage_state.iloc[0]}')
chk(3, 'contaminated carries no OHLCV', row[['open', 'high', 'low', 'close', 'volume']].isna().all().all(),
    'OHLCV all NULL')
chk(3, 'only that interval changed',
    (out3[out3.k == K].coverage_state.values != out[out.k == K].coverage_state.values).sum() == 1,
    'exactly 1 interval flipped')

print('=== FIXTURE 4 · missing OPENING minute ===')
m4 = ms.copy(); sel = m4[m4.security_key_v1 == K].sort_values('minute_ts')
first = sel.iloc[0]; m4.loc[first.name, 'observation_state'] = 'UNOBSERVED'
b4 = b[~((b.security_key_v1 == K) & (b.minute_ts == first.minute_ts))]
out4, _ = run(NORMAL, m4, b4)
r4 = out4[out4.k == K]
chk(4, 'interval 0 contaminated', r4[r4.interval_index == 0].coverage_state.iloc[0] == 'UNOBSERVED_CONTAMINATED',
    r4[r4.interval_index == 0].coverage_state.iloc[0])
chk(4, 'interval_index NOT shifted', sorted(r4.interval_index.unique()) == list(range(26)),
    f'still 0..{r4.interval_index.max()}')
chk(4, 'bar_start of interval 0 unchanged', r4[r4.interval_index == 0].bar_start.iloc[0] == o_open,
    'bar_start == session open')

print('=== FIXTURE 5 · missing FINAL minute ===')
m5 = ms.copy(); sel = m5[m5.security_key_v1 == K].sort_values('minute_ts')
last = sel.iloc[-1]; m5.loc[last.name, 'observation_state'] = 'UNOBSERVED'
b5 = b[~((b.security_key_v1 == K) & (b.minute_ts == last.minute_ts))]
out5, _ = run(NORMAL, m5, b5)
r5 = out5[out5.k == K]
chk(5, 'final interval contaminated', r5[r5.interval_index == 25].coverage_state.iloc[0] == 'UNOBSERVED_CONTAMINATED',
    r5[r5.interval_index == 25].coverage_state.iloc[0])
chk(5, 'no shortened path', sorted(r5.interval_index.unique()) == list(range(26)),
    f'{r5.interval_index.nunique()} intervals retained')

print('=== FIXTURE 6 · partial interval -> EXACT frozen coverage_state ===')
states = {}
for kdrop in [0, 1, 2, 7, 14, 15]:
    m6 = ms.copy(); sel = m6[m6.security_key_v1 == K].sort_values('minute_ts')
    idx = sel.iloc[30:30 + kdrop].index
    m6.loc[idx, 'observation_state'] = 'UNOBSERVED'
    b6 = b[~((b.security_key_v1 == K) & (b.minute_ts.isin(sel.iloc[30:30 + kdrop].minute_ts)))]
    o6, _ = run(NORMAL, m6, b6)
    rr = o6[(o6.k == K) & (o6.interval_index == 2)]
    states[kdrop] = rr.coverage_state.iloc[0]
chk(6, '0 unobserved -> COMPLETE', states[0] == 'COMPLETE', states[0])
chk(6, 'every k>=1 -> UNOBSERVED_CONTAMINATED',
    all(v == 'UNOBSERVED_CONTAMINATED' for k_, v in states.items() if k_ >= 1),
    json.dumps({str(k_): v for k_, v in states.items()}))
chk(6, 'NO partial-specific label exists', set(states.values()) <= {'COMPLETE', 'UNOBSERVED_CONTAMINATED'},
    f'states observed: {sorted(set(states.values()))}')
chk(6, 'is_stub impossible at 15m (390/15=26, 210/15=14)', 390 % 15 == 0 and 210 % 15 == 0,
    'both registered XNYS session lengths divide exactly')

print('=== FIXTURE 7 · ticker rename / same security_key ===')
ms_cols = list(P.load(NORMAL)[0].columns)
chk(7, 'no ticker column in minute_states', 'ticker' not in [c.lower() for c in ms_cols],
    f'columns: {ms_cols}')
chk(7, 'no ticker column in stage-A output', 'ticker' not in [c.lower() for c in out.columns],
    f'columns: {list(out.columns)}')
chk(7, 'admission keys on security_key_v1 only', K in COH and (out.k == K).any(),
    'the cohort member is admitted by key; no ticker participates in equality')

print('=== FIXTURE 8 · STRUCTURAL_NO_TRADE = FAIL-CLOSED GUARD ===')
m8 = ms.copy(); m8.loc[m8.index[:5], 'observation_state'] = 'STRUCTURAL_NO_TRADE'
raised = None
try:
    run(NORMAL, m8, b)
except AggHold as e:
    raised = str(e)
chk(8, 'POSITIVE: producer REFUSES, emits nothing', raised is not None and 'STRUCTURAL_NO_TRADE' in raised,
    (raised or 'NO EXCEPTION RAISED')[:110])
raised_neg = None
try:
    run(NORMAL, ms, b)
except AggHold as e:
    raised_neg = str(e)
chk(8, 'NEGATIVE: OBSERVED/UNOBSERVED only does NOT fire the guard', raised_neg is None,
    raised_neg or 'no exception')

print('=== FIXTURE 9 · same ticker / different or unresolved identity ===')
FAKE = 'f' * 64
m9 = ms.copy()
add = m9[m9.security_key_v1 == K].copy(); add['security_key_v1'] = FAKE
m9 = pd.concat([m9, add], ignore_index=True)
b9 = pd.concat([b, b[b.security_key_v1 == K].assign(security_key_v1=FAKE)], ignore_index=True)
out9, info9 = run(NORMAL, m9, b9)
chk(9, 'non-member REJECTED by the allowlist', FAKE in info9['rejected_keys'],
    f"rejected {len(info9['rejected_keys'])} key(s), {info9['rejected_rows']} rows")
chk(9, 'non-member absent from output', not (out9.k == FAKE).any(), 'not present')
chk(9, 'members unaffected', len(out9) == len(out), f'{len(out9):,} == {len(out):,}')
chk(9, 'not mapped onto any member', out9[out9.k == K].equals(out[out.k == K].reset_index(drop=True)
                                                              .set_index(out9[out9.k == K].index)),
    'the member rows are identical to the unpolluted run')

n = len(R); p = sum(1 for r in R if r['PASS'])
print(f'\nFIXTURES: {p}/{n} PASS')
json.dump(dict(total=n, passed=p, results=R,
               VERDICT='PASS' if p == n else 'FAIL'),
          open('/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/'
               '4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/_a1b_fixtures.json', 'w'), indent=1)
