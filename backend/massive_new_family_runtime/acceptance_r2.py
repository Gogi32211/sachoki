import sys, json, warnings; warnings.filterwarnings('ignore')
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend'); sys.path.insert(0,S)
import duckdb, numpy as np, pandas as pd, itertools
from sig_buy_raw import cooldown_states, cooldown_known
from indicators import apply_cooldown
R={}
# --- IDENTITY BINDING: refuse to score a DB that is not the one the gate certified ---
import os, hashlib
FC=S+'/FINAL_COMPLETE.json'
if not os.path.exists(FC):
    json.dump({'ACCEPTANCE':'NOT_RUN','reason':'FINAL_COMPLETE.json absent — completion gate did not certify the run'},
              open(S+'/ACCEPT_R2_REFUSED.json','w'),indent=1)
    print('REFUSED: no completion sentinel'); sys.exit(1)
_fc=json.load(open(FC))
def _sha(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        while True:
            b=f.read(1<<22)
            if not b: break
            h.update(b)
    return h.hexdigest()[:16]
_dbp=_fc['identity']['output_db']['path']; _now=_sha(_dbp)
if _now!=_fc['identity']['output_db']['sha16']:
    json.dump({'ACCEPTANCE':'NOT_RUN','reason':'output DB changed after certification',
               'certified':_fc['identity']['output_db']['sha16'],'observed':_now},
              open(S+'/ACCEPT_R2_REFUSED.json','w'),indent=1)
    print('REFUSED: output DB identity mismatch'); sys.exit(1)
R['IDENTITY_BINDING']={'sentinel':'FINAL_COMPLETE.json','output_db_sha16':_now,
    'input_surface':_fc['identity']['input_surface'],'code_hash':_fc['identity']['code_hash'],
    'securities':_fc['identity']['securities'],'verdict':'BOUND'}
c=duckdb.connect(_dbp,read_only=True)
c.execute("attach '"+S+"/stageA_input.duckdb' as A (read_only)")
VCOL={'rsi_14':'dval','bar_gap_range':'sval','sig_3g':'bval','sig_buy':'bval','sig_svs':'bval','sig_va':'bval'}
base=c.execute("select count(*) from A.bars").fetchone()[0]
tot=c.execute("select count(*) from d6").fetchone()[0]
R['ROWS']={'base':base,'expected':6*base,'actual':tot,'verdict':'PASS' if tot==6*base else 'FAIL'}
per=c.execute("select col,count(*) n from d6 group by col order by col").fetchall()
R['ROWS']['per_col']={k:v for k,v in per}
R['ROWS']['per_col_uniform']='PASS' if all(v==base for _,v in per) else 'FAIL'
g={}
for col,vc in VCOL.items():
    a=c.execute(f"select count(*) from d6 where col='{col}' and availability='AVAILABLE_VALID' and {vc} is null").fetchone()[0]
    b=c.execute(f"select count(*) from d6 where col='{col}' and availability<>'AVAILABLE_VALID' and {vc} is not null").fetchone()[0]
    others=[x for x in ('bval','dval','sval') if x!=vc]
    o=c.execute(f"select count(*) from d6 where col='{col}' and ({others[0]} is not null or {others[1]} is not null)").fetchone()[0]
    g[col]={'valid_and_null':a,'nonvalid_and_notnull':b,'wrong_slot_used':o}
R['GENERIC_NULL_INVARIANTS']=g
R['GENERIC_NULL_INVARIANTS']['verdict']='PASS' if all(v['valid_and_null']==0 and v['nonvalid_and_notnull']==0 and v['wrong_slot_used']==0 for v in g.values()) else 'FAIL'
d=c.execute("select count(*) from (select k,bar_start,col from d6 group by 1,2,3 having count(*)>1)").fetchone()[0]
R['DUPLICATES']={'n':d,'verdict':'PASS' if d==0 else 'FAIL'}
c.execute("""create or replace temp view ages as
  select k,bar_start,row_number() over (partition by k,grp order by bar_start) age, cs from (
    select k,bar_start,coverage_state cs,
      sum(case when coverage_state='COMPLETE' then 0 else 1 end) over (partition by k order by bar_start) grp
    from A.bars) where cs='COMPLETE'""")
T={}
for col,mn in (('sig_3g',51),('rsi_14',14),('bar_gap_range',14),('sig_svs',21),('sig_va',21),('sig_buy',50)):
    n=c.execute(f"""select count(*) from d6 x join ages a using(k,bar_start)
                    where x.col='{col}' and x.availability='AVAILABLE_VALID' and a.age<{mn}""").fetchone()[0]
    m=c.execute(f"""select count(*) from d6 x left join ages a using(k,bar_start)
                    where x.col='{col}' and x.availability='AVAILABLE_VALID' and a.age is null""").fetchone()[0]
    T[col]={'min_age':mn,'valid_below_min_age':n,'valid_on_incomplete_bar':m}
R['TEMPORAL']=T
R['TEMPORAL']['verdict']='PASS' if all(v['valid_below_min_age']==0 and v['valid_on_incomplete_bar']==0 for v in T.values()) else 'FAIL'
sb={}
bad=0
for L in range(1,14):
    for bits in itertools.product([False,True],repeat=L):
        if list(apply_cooldown(pd.Series(list(bits)),6).to_numpy())!=cooldown_known(list(bits)): bad+=1
sb['canonical_reduction']={'exhaustive_L_le_13':16382,'mismatches':bad,'verdict':'PASS' if bad==0 else 'FAIL'}
fx=json.load(open(S+'/FIXTURE_cooldown_composition.json'))
dec={'T':True,'F':False,'?':None}
fail=0
for f in fx['fixtures']:
    raw=[x=='T' for x in f['raw']]; obs=[x=='O' for x in f['obs']]
    got=cooldown_states([raw[i] if obs[i] else None for i in range(len(raw))])
    exp=[dec[x] for x in f['correct']]
    if got!=exp: fail+=1
    if got==[dec[x] for x in f['defective']] and f['correct']!=f['defective']: fail+=1
sb['frozen_regression_400']={'n':len(fx['fixtures']),'failures':fail,'verdict':'PASS' if fail==0 else 'FAIL'}
o=lambda s: cooldown_states(s)[-1]
ug={'6_observed_FALSE_recovery':o([True]+[False]*5+[True]) is True,
    '5_observed_FALSE_still_cooling':o([True]+[False]*4+[True]) is False,
    '6_UNKNOWN_stays_unknown':o([True]+[None]*6+[True]) is None,
    'unknown_within_n_minus_1_is_identifiable':o([True]+[None]*5+[True]) is True}
sb['unknown_gap_propagation']={**{k:bool(v) for k,v in ug.items()},'verdict':'PASS' if all(ug.values()) else 'FAIL'}
R['SIG_BUY']=sb
R['SIG_BUY']['verdict']='PASS' if all(sb[x]['verdict']=='PASS' for x in sb) else 'FAIL'
AV=c.execute("select col,availability,count(*) n from d6 group by 1,2 order by 1,2").fetchall()
R['AVAILABILITY_CENSUS']={}
for col,av,n in AV: R['AVAILABILITY_CENSUS'].setdefault(col,{})[av]=n
FR=c.execute("select col,sum(case when bval=1 then 1 else 0 end) f from d6 where availability='AVAILABLE_VALID' and bval is not null group by 1").fetchall()
R['FIRES_VALID']={k:v for k,v in FR}
keys=['ROWS','GENERIC_NULL_INVARIANTS','DUPLICATES','TEMPORAL','SIG_BUY']
R['SIG_BUY_AGE_NOTE']='age>=50 is NECESSARY not SUFFICIENT: cooldown ambiguity may leave INIT beyond age 50. Acceptance tests only (VALID and age<50)=0, never (age>=50 => VALID).'
R['OVERALL']='PASS' if all((R[k].get('verdict') if isinstance(R[k],dict) else None)=='PASS' for k in keys) and R['ROWS']['verdict']=='PASS' else 'REVIEW'
json.dump(R,open(S+'/ACCEPT_R2.json','w'),indent=1,default=str)
print(json.dumps(R,indent=1,default=str)[:4000])
