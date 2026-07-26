"""
REVERSAL SEQUENCE ANALYSIS — 4-BAR SUFFIX + LINE5 CHAINS
bar0 = signal (current), bar-1/-2/-3 = prior bars (no lookahead).
Metrics mirror the composite report (pivot 3-3): N, HH%, HL%, f5med, f5avg, win5.
HH% = hh/(hh+hl). Per direction key: HIGH HL% = bullish base, HIGH HH% = topping.
Min N 200. Universe: SP500 + NASDAQ + Russell2K (all).
"""
import duckdb, datetime as dt
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/Users/sachoki/Desktop/sachoki-desktop/REVERSAL_SEQ_4BAR_260528.txt"
MINN=200
con=duckdb.connect(); con.execute(f"ATTACH '{DB}' AS s (READ_ONLY)")

print("building 4-bar lag base...")
con.execute("""
CREATE TABLE b AS
SELECT
  COALESCE(NULLIF(composite_full_suffix,''),'_') AS s0,
  COALESCE(NULLIF(LAG(composite_full_suffix,1) OVER w,''),'_') AS s1,
  COALESCE(NULLIF(LAG(composite_full_suffix,2) OVER w,''),'_') AS s2,
  COALESCE(NULLIF(LAG(composite_full_suffix,3) OVER w,''),'_') AS s3,
  COALESCE(NULLIF(bar_line5,''),'_') AS l0,
  COALESCE(NULLIF(LAG(bar_line5,1) OVER w,''),'_') AS l1,
  COALESCE(NULLIF(LAG(bar_line5,2) OVER w,''),'_') AS l2,
  COALESCE(NULLIF(LAG(bar_line5,3) OVER w,''),'_') AS l3,
  CAST(next_pivot_is_hh_3 AS INT) AS hh,
  CAST(next_pivot_is_hl_3 AS INT) AS hl,
  fwd_5d,
  ROW_NUMBER() OVER w AS rn
FROM s.bars
WINDOW w AS (PARTITION BY ticker ORDER BY date)
""")
# require at least 4 bars of history (rn>=4) so lags are real, not series-start padding
N=con.execute("SELECT COUNT(*) FROM b WHERE rn>=4").fetchone()[0]
print("usable rows (rn>=4):",N)

AGG="""
  COUNT(*) AS n, SUM(hh) AS hh, SUM(hl) AS hl,
  ROUND(MEDIAN(CASE WHEN isfinite(fwd_5d) THEN fwd_5d END),3) AS f5med,
  ROUND(AVG(CASE WHEN isfinite(fwd_5d) AND ABS(fwd_5d)<=100 THEN fwd_5d END),3) AS f5avg,
  ROUND(100.0*AVG(CASE WHEN isfinite(fwd_5d) AND fwd_5d>0 THEN 1.0 WHEN isfinite(fwd_5d) THEN 0.0 END),1) AS win5
"""
def hhp(r):
    d=(r['hh'] or 0)+(r['hl'] or 0); return round((r['hh'] or 0)/d*100,1) if d else 0.0
def hlp(r):
    d=(r['hh'] or 0)+(r['hl'] or 0); return round((r['hl'] or 0)/d*100,1) if d else 0.0

CW=[34,8,7,7,8,8,7]
HD=["seq (b-3 | b-2 | b-1 | b0)","N","HH%","HL%","f5med","f5avg","win5"]
def header():
    h="".join(x.ljust(CW[0]) if i==0 else x.rjust(CW[i]) for i,x in enumerate(HD)); return h+"\n"+"-"*len(h)
def fmt(label,r):
    cells=[str(label)[:CW[0]].ljust(CW[0])]
    vals=[int(r['n']),hhp(r),hlp(r),r['f5med'],r['f5avg'],r['win5']]
    for i,v in enumerate(vals,1):
        cells.append(("-" if v is None else (f"{int(v):,}" if i==1 else f"{v:.2f}")).rjust(CW[i]))
    return "".join(cells)

R=[]
R.append("="*92)
R.append("REVERSAL SEQUENCE ANALYSIS — 4-BAR SUFFIX + LINE5 CHAINS")
R.append("="*92)
R.append(f"Generated: {dt.datetime.now():%Y-%m-%d %H:%M} | daily TF | pivot 3-3 | usable rows {N:,}")
R.append("bar0=signal(now); b-1/-2/-3 prior. HH%=hh/(hh+hl). Min N "+str(MINN)+". All 3 universes.")
R.append("DIRECTION KEY: HIGH HL% + positive f5med = bullish base ; HIGH HH% + negative f5med = topping.")
R.append("'_' = no suffix/line5 on that bar. Compare to single-bar baselines in COMPOSITE report.")

def part_a(target, sort_metric):
    # group by prior 3-bar suffix chain, fixed s0=target
    q=f"""SELECT s3,s2,s1, {AGG} FROM b
          WHERE rn>=4 AND s0='{target}'
          GROUP BY s3,s2,s1 HAVING COUNT(*)>={MINN}"""
    df=con.execute(q).fetchdf()
    if len(df)==0: return [f"  (no chains with N>={MINN})"]
    df["_k"]=df.apply(hhp if sort_metric=="HH" else hlp, axis=1)
    df=df.sort_values("_k",ascending=False).head(15)
    out=[header()]
    for _,r in df.iterrows():
        lab=f"{r['s3']} -> {r['s2']} -> {r['s1']} -> {target}"
        out.append(fmt(lab,r))
    return out

def named_seq(seq, kind="suffix"):
    # seq = (b-3,b-2,b-1,b0)
    if kind=="suffix":
        w=f"s3='{seq[0]}' AND s2='{seq[1]}' AND s1='{seq[2]}' AND s0='{seq[3]}'"
    else:
        w=f"l3='{seq[0]}' AND l2='{seq[1]}' AND l1='{seq[2]}' AND l0='{seq[3]}'"
    r=con.execute(f"SELECT {AGG} FROM b WHERE rn>=4 AND {w}").fetchdf().iloc[0]
    lab=" -> ".join(seq)
    return fmt(lab,r) if (r['n'] or 0)>0 else f"{lab[:CW[0]].ljust(CW[0])}  (0 rows)"

# ---------- PART A ----------
R.append("\n\n"+"="*92)
R.append("PART A — COMPOSITE SUFFIX 4-BAR CHAINS")
R.append("="*92)
for t in ["EBO","EDP","ED","NRO","NURO"]:
    R.append(f"\n[bar0={t} — BULLISH target, top 15 by HL%]")
    R+=part_a(t,"HL")
for t in ["EBA","EU","EUR","NUA","NBA"]:
    R.append(f"\n[bar0={t} — BEARISH target, top 15 by HH%]")
    R+=part_a(t,"HH")

R.append("\n\n[Named structural patterns — suffix]")
R.append(header())
R.append(named_seq(("EBO","EBO","EBO","EBO")))      # momentum confirm
R.append(named_seq(("EBA","EBA","EBA","EBA")))
R.append(named_seq(("EBA","EBO","EBA","EBO")))      # alternating absorption
R.append(named_seq(("EBO","EBA","EBO","EBA")))
R.append(named_seq(("EBA","EBA","EBO","EBO")))      # exhaustion into reversal
R.append(named_seq(("EU","EU","EBO","EBO")))
R.append(named_seq(("ED","ED","ED","EBO")))

# ---------- PART B ----------
R.append("\n\n"+"="*92)
R.append("PART B — LINE5 4-BAR CHAINS")
R.append("="*92)
def part_b(target, sort_metric):
    q=f"""SELECT l3,l2,l1, {AGG} FROM b
          WHERE rn>=4 AND l0='{target}'
          GROUP BY l3,l2,l1 HAVING COUNT(*)>={MINN}"""
    df=con.execute(q).fetchdf()
    if len(df)==0: return [f"  (no chains with N>={MINN})"]
    df["_k"]=df.apply(hhp if sort_metric=="HH" else hlp, axis=1)
    df=df.sort_values("_k",ascending=False).head(15)
    out=[header()]
    for _,r in df.iterrows():
        out.append(fmt(f"{r['l3']} -> {r['l2']} -> {r['l1']} -> {target}",r))
    return out
for t in ["VX-PS-R2L","VR-PS-R2L","PS-R2L"]:
    R.append(f"\n[bar0={t} — BULLISH target, top 15 by HL%]")
    R+=part_b(t,"HL")
for t in ["VX-PB-R2H","VR-PB-R2H","PB-R2H"]:
    R.append(f"\n[bar0={t} — BEARISH target, top 15 by HH%]")
    R+=part_b(t,"HH")

R.append("\n\n[Named structural patterns — line5]")
R.append(header())
R.append(named_seq(("PS-R2L","PS-R2L","VR-PS-R2L","VX-PS-R2L"),"line5"))   # deepening oversold
R.append(named_seq(("PB","PS","PS-R2L","VX-PS-R2L"),"line5"))              # fresh spike
R.append(named_seq(("VX-PS-R2L","PS","VX-PS-R2L","VX-PS-R2L"),"line5"))    # double VX cluster
R.append(named_seq(("PB-R2H","PB-R2H","VR-PB-R2H","VX-PB-R2H"),"line5"))   # escalating top
R.append(named_seq(("PB","PB-R2H","PB-R2H","VX-PB-R2H"),"line5"))

# ---------- PART C ----------
R.append("\n\n"+"="*92)
R.append("PART C — COMBINED: bar0 (suffix+line5) conditioned on bar-1 (suffix+line5)")
R.append("="*92)
def part_c(s0,l0,sort_metric):
    q=f"""SELECT s1,l1, {AGG} FROM b
          WHERE rn>=4 AND s0='{s0}' AND l0='{l0}'
          GROUP BY s1,l1 HAVING COUNT(*)>={MINN}"""
    df=con.execute(q).fetchdf()
    if len(df)==0: return [f"  (no pairs with N>={MINN})"]
    df["_k"]=df.apply(hhp if sort_metric=="HH" else hlp, axis=1)
    df=df.sort_values("_k",ascending=False).head(20)
    out=[header().replace("seq (b-3 | b-2 | b-1 | b0)","bar-1: suffix | line5        ")]
    for _,r in df.iterrows():
        out.append(fmt(f"{r['s1']} | {r['l1']}",r))
    return out
R.append(f"\n[bar0 = EBO + VX-PS-R2L (BULLISH), top 20 prior-bar pairs by HL%]")
R+=part_c("EBO","VX-PS-R2L","HL")
R.append(f"\n[bar0 = EBA + VX-PB-R2H (BEARISH), top 20 prior-bar pairs by HH%]")
R+=part_c("EBA","VX-PB-R2H","HH")

R.append("\n\n"+"="*92)
R.append("NOTES")
R.append("-"*92)
R.append("  - No lookahead: chain uses only confirmed past bars; outcome measured forward from bar0.")
R.append("  - '_' = that bar had no suffix/line5 token.")
R.append("  - HH%/HL% denom = resolved pivots (hh+hl). f5med robust; f5avg clipped |.|<=100%.")
R.append("  - Sequences with small N are noisy even at N>=200 — weigh by N and f5med sign agreement.")
R.append("="*92)

open(OUT,"w").write("\n".join(R))
import os; print("written",OUT,os.path.getsize(OUT),"bytes")
