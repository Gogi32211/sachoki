import sys, json, hashlib, os, warnings; warnings.filterwarnings('ignore')
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb
R={}
FC=S+'/PHASE1_FINAL_COMPLETE.json'
if not os.path.exists(FC):
    print('REFUSED: no completion sentinel'); sys.exit(1)
fc=json.load(open(FC))
def sha(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        while True:
            b=f.read(1<<22)
            if not b: break
            h.update(b)
    return h.hexdigest()[:16]
DB=fc['identity']['output_db']['path']; now=sha(DB)
if now!=fc['identity']['output_db']['sha16']:
    json.dump({'ACCEPTANCE':'NOT_RUN','reason':'output DB changed after certification'},
              open(S+'/ACCEPT_PHASE1_REFUSED.json','w'),indent=1)
    print('REFUSED: digest mismatch'); sys.exit(1)
R['IDENTITY_BINDING']={'sentinel':'PHASE1_FINAL_COMPLETE.json','sha16':now,'exit_code':fc['identity']['exit_code'],
                       'securities':fc['identity']['securities'],'verdict':'BOUND'}
c=duckdb.connect(DB,read_only=True)
c.execute("attach '"+S+"/stageA_input.duckdb' as A (read_only)")
c.execute("pragma threads=8")
T_COLS=[f'sig_t{i}' for i in [1,2,3,4,5,6,9,10,11,12]]+['sig_t1g','sig_t2g']
Z_COLS=[f'sig_z{i}' for i in [1,2,3,4,5,6,7,9,10,11,12]]+['sig_z1g','sig_z2g']
G=['sig_g1','sig_g2','sig_g4','sig_g11']; SUF=['ne_suffix','wick_suffix','close_suffix','full_suffix']
VOL=['sig_vol_5x','sig_vol_10x','sig_vol_20x']
STR=SUF+['bar_body_wick']; BOOL=T_COLS+Z_COLS+G+VOL+['vbo_up','sig_tz_flip']
ALL=BOOL+STR
base=c.execute("select count(*) from A.bars").fetchone()[0]
tot=c.execute("select count(*) from p39").fetchone()[0]
R['ROWS']={'base':base,'expected':39*base,'actual':tot,'columns':len(ALL),
           'verdict':'PASS' if tot==39*base and len(ALL)==39 else 'FAIL'}
g={}
for col in ALL:
    vc='sval' if col in STR else 'bval'
    other='bval' if col in STR else 'sval'
    a=c.execute(f"select count(*) from p39 where col='{col}' and availability='AVAILABLE_VALID' and {vc} is null").fetchone()[0]
    b=c.execute(f"select count(*) from p39 where col='{col}' and availability<>'AVAILABLE_VALID' and {vc} is not null").fetchone()[0]
    o=c.execute(f"select count(*) from p39 where col='{col}' and {other} is not null").fetchone()[0]
    g[col]={'valid_and_null':a,'nonvalid_and_notnull':b,'wrong_slot':o}
R['GENERIC_NULL_INVARIANTS']={'per_col':g,'verdict':'PASS' if all(v['valid_and_null']==0 and v['nonvalid_and_notnull']==0 and v['wrong_slot']==0 for v in g.values()) else 'FAIL'}
d=c.execute("select count(*) from (select k,bar_start,col from p39 group by 1,2,3 having count(*)>1)").fetchone()[0]
R['DUPLICATES']={'n':d,'verdict':'PASS' if d==0 else 'FAIL'}
c.execute("""create or replace temp view ages as
 select k,bar_start,row_number() over (partition by k,grp order by bar_start) age from (
  select k,bar_start,coverage_state cs,
   sum(case when coverage_state='COMPLETE' then 0 else 1 end) over (partition by k order by bar_start) grp
  from A.bars) where cs='COMPLETE'""")
TEMP={}
for col in ALL:
    mn = 22 if col=='vbo_up' else (None if col=='sig_tz_flip' else 2)
    nc=c.execute(f"""select count(*) from p39 x left join ages a using(k,bar_start)
        where x.col='{col}' and x.availability='AVAILABLE_VALID' and a.age is null""").fetchone()[0]
    e={'valid_on_non_COMPLETE_bar':nc}
    if mn is not None:
        b=c.execute(f"""select count(*) from p39 x join ages a using(k,bar_start)
            where x.col='{col}' and x.availability='AVAILABLE_VALID' and a.age<{mn}""").fetchone()[0]
        e['min_age']=mn; e['valid_below_min_age']=b
    else:
        e['min_age']='NOT_APPLICABLE (FINITE_PERSISTENT_STATE)'
    TEMP[col]=e
R['TEMPORAL']={'per_col':TEMP,'verdict':'PASS' if all(v['valid_on_non_COMPLETE_bar']==0 and v.get('valid_below_min_age',0)==0 for v in TEMP.values()) else 'FAIL'}
q=','.join(f"'{x}'" for x in T_COLS+Z_COLS)
ex=c.execute(f"""select max(s), sum(case when s>1 then 1 else 0 end) from (
   select k,bar_start,sum(coalesce(bval,0)) s from p39 where col in ({q}) group by 1,2)""").fetchone()
R['TZ_ONE_HOT_EXCLUSIVITY']={'max_simultaneous':ex[0],'violations':ex[1],'verdict':'PASS' if ex[1]==0 else 'FAIL'}
# suffix wrap-defect enforcement: segment head must NEVER be AVAILABLE_VALID
hq=','.join(f"'{x}'" for x in SUF)
hv=c.execute(f"""select count(*) from p39 x join ages a using(k,bar_start)
   where x.col in ({hq}) and a.age=1 and x.availability='AVAILABLE_VALID'""").fetchone()[0]
R['SUFFIX_WRAP_GUARD']={'segment_head_VALID_rows':hv,'verdict':'PASS' if hv==0 else 'FAIL',
   'rule':'np.roll wraps at index 0; segment head must be NON-VALID'}
AV=c.execute("select col,availability,count(*) n from p39 group by 1,2").fetchall()
R['AVAILABILITY_CENSUS']={}
for col,av,n in AV: R['AVAILABILITY_CENSUS'].setdefault(col,{})[av]=n
FR=c.execute("select col,sum(case when bval=1 then 1 else 0 end) f from p39 where availability='AVAILABLE_VALID' and bval is not null group by 1").fetchall()
R['FIRES_VALID']={k:v for k,v in FR}
NE=c.execute("select col,sum(case when sval is not null and sval<>'' then 1 else 0 end) f from p39 where availability='AVAILABLE_VALID' group by 1").fetchall()
R['NONEMPTY_STRING']={k:v for k,v in NE if k in STR}
keys=['ROWS','GENERIC_NULL_INVARIANTS','DUPLICATES','TEMPORAL','TZ_ONE_HOT_EXCLUSIVITY','SUFFIX_WRAP_GUARD']
R['OVERALL']='PASS' if all(R[k]['verdict']=='PASS' for k in keys) else 'REVIEW'
json.dump(R,open(S+'/ACCEPT_PHASE1.json','w'),indent=1,default=str)
for k in keys: print(f"{k:28s} {R[k]['verdict']}")
print(f"{'OVERALL':28s} {R['OVERALL']}")
