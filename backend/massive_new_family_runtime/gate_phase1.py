"""Completion gate for PHASE1_R1. FIRST run under the frozen launch rule:
exitcode == 0 is now a REQUIRED criterion, not a substitute."""
import sys, os, json, time, hashlib, glob
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb
PID=int(sys.argv[1]); RUN='PHASE1_R1'; DB=f'{S}/{RUN}.duckdb'; EXPECT=602372706
def alive(p):
    try: os.kill(p,0); return True
    except OSError: return False
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}",flush=True)
def sha(p):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        while True:
            b=f.read(1<<22)
            if not b: break
            h.update(b)
    return h.hexdigest()[:16]
while alive(PID):
    ck=json.load(open(f'{S}/{RUN}.json')) if os.path.exists(f'{S}/{RUN}.json') else {}
    if ck.get('FINISHED'): log("state=WAITING_FOR_PROCESS_EXIT (FINISHED present, PID alive)")
    time.sleep(30)
log("producer PID gone")
for _ in range(60):
    tr=glob.glob(DB+'.wal')+glob.glob(DB+'.tmp*')
    if not tr: log("no transient artifacts"); break
    log(f"waiting on {[os.path.basename(t) for t in tr]}"); time.sleep(5)
stable=False
for _ in range(12):
    a=(sha(DB),os.path.getsize(DB)); time.sleep(5); b=(sha(DB),os.path.getsize(DB))
    if a==b: stable=True; log(f"stable sha16={a[0]} bytes={a[1]:,}"); break
DBSHA,DBB=sha(DB),os.path.getsize(DB)
crit={}; lg=open(f'{S}/{RUN}.log').read()
ck=json.load(open(f'{S}/{RUN}.json')) if os.path.exists(f'{S}/{RUN}.json') else {}
T=ck.get('T',{})
ec=None
if os.path.exists(f'{S}/{RUN}.exitcode'):
    try: ec=int(open(f'{S}/{RUN}.exitcode').read().strip())
    except: ec=None
crit['exit_code_zero']=(ec==0)          # REQUIRED, not substituted
crit['output_stable']=stable
crit['checkpoint_FINISHED']=bool(ck.get('FINISHED'))
crit['completed_securities_476']=T.get('done')==476
crit['producer_errors_zero']=T.get('err')==0
crit['terminal_DONE_line']=any(l.startswith('DONE rows') for l in lg.splitlines())
crit['no_traceback_in_log']='Traceback' not in lg
rows=ncol=None
try:
    c=duckdb.connect(DB,read_only=True)
    rows=c.execute("select count(*) from p39").fetchone()[0]
    ncol=c.execute("select count(distinct col) from p39").fetchone()[0]
    uni=c.execute("select count(distinct n) from (select col,count(*) n from p39 group by 1)").fetchone()[0]
    c.close()
    crit['db_opens_clean']=True; crit['rows_exact']= (rows==EXPECT)
    crit['columns_39']=(ncol==39); crit['per_column_uniform']=(uni==1)
except Exception as ex:
    crit['db_opens_clean']=False; crit['db_error']=str(ex)[:200]
    crit['rows_exact']=crit['columns_39']=crit['per_column_uniform']=False
ok=all(v for v in crit.values() if isinstance(v,bool))
ident=dict(run_id=RUN,exit_code=ec,output_db={'path':DB,'sha16':DBSHA,'bytes':DBB},
  rows=rows,expected_rows=EXPECT,columns=ncol,securities=f"{T.get('done')} / 476",
  elapsed_s=ck.get('el'),finished_ts=time.strftime('%Y-%m-%dT%H:%M:%S%z'))
name='PHASE1_FINAL_COMPLETE.json' if ok else 'PHASE1_RUN_INCOMPLETE.json'
body=dict(status='COMPLETE',criteria=crit,identity=ident) if ok else \
     dict(RUN_STATUS='INCOMPLETE_RUN',ACCEPTANCE='NOT_RUN',
          failed_criteria=[k for k,v in crit.items() if v is False],criteria=crit,identity=ident)
tmp=f'{S}/{name}.tmp'
with open(tmp,'w') as f: json.dump(body,f,indent=1); f.flush(); os.fsync(f.fileno())
os.replace(tmp,f'{S}/{name}')
log(f"{'FINAL_COMPLETE' if ok else 'INCOMPLETE_RUN'} written")
