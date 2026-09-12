"""Completion gate for STAGE_D_R2.

ORDER IS LOAD-BEARING:
  1. WAIT for producer PID to disappear.  PID disappearance is NOT an exit-code
     substitute; it only proves the producer is no longer writing output.
     Successful termination is proven by the terminal invariants below.
     While the PID lives, state = WAITING_FOR_PROCESS_EXIT even if DONE/FINISHED
     are already present -- that is NOT an INCOMPLETE_RUN.
  2. WAIT for transient WAL/temp artifacts to settle.
  3. STABLE-OUTPUT guard: digest+size read twice, must be identical.
  4. Only then evaluate terminal invariants and certify.
"""
import sys, os, json, time, hashlib, subprocess, glob
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb
PID=int(sys.argv[1]); RUN='STAGE_D_R2'; DB=f'{S}/{RUN}.duckdb'
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

# ---- 1. PID liveness -------------------------------------------------------
while alive(PID):
    ck=json.load(open(f'{S}/{RUN}.json')) if os.path.exists(f'{S}/{RUN}.json') else {}
    if ck.get('FINISHED'): log("state=WAITING_FOR_PROCESS_EXIT (FINISHED sentinel present, PID still alive)")
    time.sleep(30)
log("producer PID gone")

# ---- 2. WAL / temp settle --------------------------------------------------
for _ in range(60):
    trans=glob.glob(DB+'.wal')+glob.glob(DB+'.tmp*')+glob.glob(S+f'/{RUN}.duckdb.tmp*')
    if not trans: log("no transient WAL/temp artifacts"); break
    log(f"waiting on transient artifacts: {[os.path.basename(t) for t in trans]}")
    time.sleep(5)
else:
    log("WARNING: transient artifacts persisted past 300s")

# ---- 3. stable-output guard ------------------------------------------------
stable=False
for attempt in range(12):
    a=(sha(DB),os.path.getsize(DB)); time.sleep(5); b=(sha(DB),os.path.getsize(DB))
    if a==b: stable=True; log(f"output stable: sha16={a[0]} bytes={a[1]:,}"); break
    log(f"output still changing (attempt {attempt+1})")
DBSHA,DBBYTES=(sha(DB),os.path.getsize(DB))

# ---- 4. terminal invariants ------------------------------------------------
crit={}; logtxt=open(f'{S}/{RUN}.log').read()
ck=json.load(open(f'{S}/{RUN}.json')) if os.path.exists(f'{S}/{RUN}.json') else {}
T=ck.get('T',{})
crit['output_stable']=stable
crit['checkpoint_FINISHED']=bool(ck.get('FINISHED'))
crit['completed_securities_476']=T.get('done')==476
crit['producer_errors_zero']=T.get('err')==0
crit['terminal_DONE_line']=any(l.startswith('DONE rows') for l in logtxt.splitlines())
crit['no_traceback_in_log']='Traceback' not in logtxt
baserows=outrows=nsec=basesec=None
try:
    c=duckdb.connect(DB,read_only=True)
    c.execute("attach '"+S+"/stageA_input.duckdb' as A (read_only)")
    baserows=c.execute("select count(*) from A.bars").fetchone()[0]
    outrows=c.execute("select count(*) from d6").fetchone()[0]
    nsec=c.execute("select count(distinct k) from d6").fetchone()[0]
    basesec=c.execute("select count(distinct k) from A.bars").fetchone()[0]
    c.close(); crit['db_opens_clean']=True
    crit['output_rows_equal_6x_base']=(outrows==6*baserows)
except Exception as ex:
    crit['db_opens_clean']=False; crit['db_error']=str(ex)[:200]; crit['output_rows_equal_6x_base']=False
ok=all(v for v in crit.values() if isinstance(v,bool))
ident=dict(run_id=RUN,
  code_hash={n:sha(p) for n,p in [('stageD_r2.py',f'{S}/stageD_r2.py'),('sig_buy_raw.py',f'{S}/sig_buy_raw.py'),
     ('combo_engine.py','/Users/sachoki/Desktop/sachoki-desktop/backend/combo_engine.py'),
     ('indicators.py','/Users/sachoki/Desktop/sachoki-desktop/backend/indicators.py'),
     ('enricher.py','/Users/sachoki/Desktop/sachoki-desktop/backend/studio/enricher.py')]},
  input_surface={'stageA_input.duckdb':sha(f'{S}/stageA_input.duckdb'),'pd_input.duckdb':sha(f'{S}/pd_input.duckdb')},
  output_db={'path':DB,'sha16':DBSHA,'bytes':DBBYTES},
  securities=f"{T.get('done')} / 476", securities_in_output=nsec, securities_in_base=basesec,
  base_rows=baserows, output_rows=outrows, finished_ts=time.strftime('%Y-%m-%dT%H:%M:%S%z'),
  producer_elapsed_s=ck.get('el'),
  exit_code_captured=False,
  exit_code_substitute="terminal DONE line + FINISHED sentinel + 476/476 + row reconciliation; PID disappearance proves only that writing stopped")
if ok:
    t=f'{S}/FINAL_COMPLETE.tmp'
    with open(t,'w') as f: json.dump(dict(status='COMPLETE',criteria=crit,identity=ident),f,indent=1); f.flush(); os.fsync(f.fileno())
    os.replace(t,f'{S}/FINAL_COMPLETE.json'); log("FINAL_COMPLETE -> acceptance")
    subprocess.run(['/Users/sachoki/Desktop/sachoki-desktop/backend/.venv/bin/python','-W','ignore',f'{S}/acceptance_r2.py'],
                   stdout=open(f'{S}/ACCEPT_R2.log','w'),stderr=subprocess.STDOUT)
else:
    t=f'{S}/RUN_INCOMPLETE.tmp'
    with open(t,'w') as f: json.dump(dict(RUN_STATUS='INCOMPLETE_RUN',ACCEPTANCE='NOT_RUN',
        failed_criteria=[k for k,v in crit.items() if v is False],criteria=crit,identity=ident),f,indent=1); f.flush(); os.fsync(f.fileno())
    os.replace(t,f'{S}/RUN_INCOMPLETE.json'); log("INCOMPLETE_RUN -> acceptance NOT_RUN")
