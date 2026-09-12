"""CANONICAL 69-COLUMN X SURFACE — CONSOLIDATION, NOT RECOMPUTATION.
Values/states are COPIED from the four accepted durable stores. Nothing is recomputed.
Availability codes are preserved VERBATIM per source (pd's coarse INITIALIZATION_SENSITIVE
is NOT normalised into HEAD/POST_GAP, and the others are not coarsened to match it)."""
import os, json, time, sys
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
import duckdb
RUN='CANON69'; OUT=f'{S}/{RUN}.duckdb'; J=f'{S}/{RUN}.json'
def atomic(o):
    t=J+'.tmp'
    with open(t,'w') as f: json.dump(o,f); f.flush(); os.fsync(f.fileno())
    os.replace(t,J)
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}",flush=True)
if os.path.exists(OUT): os.remove(OUT)
man=json.load(open(S+'/CANONICAL_69_MANIFEST.json'))
c=duckdb.connect(OUT); c.execute("pragma threads=8")
c.execute("""create table x69(k VARCHAR, bar_start BIGINT, session_date VARCHAR, feature VARCHAR,
              bval TINYINT, dval DOUBLE, sval VARCHAR, availability VARCHAR)""")
for tag,db in [('d6','STAGE_D_R2.duckdb'),('pd','pd12.duckdb'),('c4','stageC4.duckdb'),('p39','PHASE1_R1.duckdb')]:
    c.execute(f"attach '{S}/{db}' as {tag}_src (read_only)")
t0=time.time(); T={'run':RUN,'sources_done':0,'rows':0}
PLAN=[('d6','d6_src.d6','bval','dval','sval'),
      ('pd','pd_src.pd','value','NULL','NULL'),
      ('c4','c4_src.c4','bval','NULL','sval'),
      ('p39','p39_src.p39','bval','NULL','sval')]
for tag,tbl,bv,dv,sv in PLAN:
    feats=[f for f,v in man.items() if v['source_table']==tag]
    q=','.join(f"'{f}'" for f in feats)
    log(f"copying {tag}: {len(feats)} features from {tbl}")
    c.execute(f"""insert into x69 select k,bar_start,session_date,col,
                    cast({bv} as TINYINT), cast({dv} as DOUBLE), cast({sv} as VARCHAR), availability
                  from {tbl} where col in ({q})""")
    n=c.execute("select count(*) from x69").fetchone()[0]
    T['sources_done']+=1; T['rows']=n
    log(f"  {tag} done — cumulative rows {n:,}  ({time.time()-t0:.0f}s)")
    atomic(dict(T=T,el=round(time.time()-t0),FINISHED=False))
nf=c.execute("select count(distinct feature) from x69").fetchone()[0]
nr=c.execute("select count(*) from x69").fetchone()[0]
log(f"features={nf} rows={nr:,}")
atomic(dict(T=T,features=nf,rows=nr,el=round(time.time()-t0),FINISHED=True))
c.close()
print(f"DONE rows {nr} features {nf}")
