"""15M_PARALLEL_QUALIFICATION_V1 — does prange help, and at how many threads?

ONE LEVER AT A TIME. Batching is not touched here. If threads and batching were switched on
together and the result got faster, neither the speedup nor any change in memory behaviour
could be attributed.

FRESH SUBPROCESS PER CONFIGURATION

ru_maxrss is a high-water mark that never falls, so measuring several configurations inside
one process gave the previous run a "transient 0.00 GB" for candidates that plainly allocate
hundreds of megabytes — the first candidate had already raised the watermark. Each thread
count therefore runs in its own process and reports its own peak.

THE TIMING UNIT IS RECORDED, NOT ASSUMED

    timing_unit = ONE_RANK_VECTOR x ALL_K_CLAIMS

The capability grid needs SIX rank vectors per permutation, because injecting delta on the raw
outcome changes the within-block ranks. So the workload is 60 x 999 x 6 x t1 only while the
engine evaluates one vector per invocation. Once an engine fuses several vectors into one
call, t6 must be MEASURED — multiplying t1 by six would double-count the very fusion that
made it faster.

8 THREADS IS NOT ASSUMED TO WIN. 167.5M scattered reads can saturate memory bandwidth well
before the core count runs out, and past that point more threads cost rather than pay.
"""
from __future__ import annotations
import os, sys

MODE = sys.argv[1] if len(sys.argv) > 1 else "orchestrate"
if MODE == "worker":
    os.environ["NUMBA_NUM_THREADS"] = sys.argv[2]
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import json, resource, subprocess, time                               # noqa: E402
import numpy as np                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)

BUNDLE = os.path.join(os.path.dirname(HERE), "data", "t5_15m_engine_state.npz")
OUT = os.path.join(HERE, "T5_15M_PARALLEL_QUALIFICATION_V1.json")
THREADS = [1, 2, 4, 6, 8]
N_WARM, N_BENCH, N_EQ = 3, 10, 5
ARRS = ("eidx", "seg_ptr", "seg_nc", "claim_seg_ptr", "N_T", "const", "sd",
        "blk_n", "blk_start")


def rss_peak():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def cpu_time():
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def mapping(blk_n, blk_start, nE, seed):
    g = np.random.default_rng(seed)
    r2 = np.empty(nE, np.int32)
    for b in range(len(blk_n)):
        n = int(blk_n[b]); s = int(blk_start[b])
        r2[s:s + n] = g.permutation(np.arange(1, n + 1, dtype=np.int32)) * 2
    return r2


# ── build ───────────────────────────────────────────────────────────────────
def build():
    import t5_15m_engine_qualify as EQ
    S = EQ.State()
    np.savez(BUNDLE, eidx=S.eidx, seg_ptr=S.seg_ptr, seg_nc=S.seg_nc,
             claim_seg_ptr=S.claim_seg_ptr, N_T=S.N_T, const=S.const, sd=S.sd,
             blk_n=S.blk_n, blk_start=S.blk_start, nE=np.array([S.nE]),
             k=np.array([S.k]), nnz=np.array([S.nnz]), nseg=np.array([S.nseg]))
    print(f"  state bundle written · {os.path.getsize(BUNDLE)/1e9:.2f} GB", flush=True)


# ── worker ──────────────────────────────────────────────────────────────────
def worker(nthreads):
    from numba import njit, prange, get_num_threads

    @njit(cache=True, fastmath=False, nogil=True)
    def ser(rank2, eidx, seg_ptr, seg_nc, csp, N_T, const, sd, R, Z):
        for c in range(csp.shape[0] - 1):
            acc = 0.0
            for s in range(csp[c], csp[c + 1]):
                tot = 0
                for i in range(seg_ptr[s], seg_ptr[s + 1]):
                    tot += rank2[eidx[i]]
                acc += (tot * 0.5) / seg_nc[s]
            r = acc / N_T[c] + const[c]
            R[c] = r; Z[c] = r / sd[c]

    @njit(cache=True, fastmath=False, nogil=True, parallel=True)
    def par(rank2, eidx, seg_ptr, seg_nc, csp, N_T, const, sd, R, Z):
        for c in prange(csp.shape[0] - 1):
            acc = 0.0
            for s in range(csp[c], csp[c + 1]):
                tot = 0
                for i in range(seg_ptr[s], seg_ptr[s + 1]):
                    tot += rank2[eidx[i]]
                acc += (tot * 0.5) / seg_nc[s]
            r = acc / N_T[c] + const[c]
            R[c] = r; Z[c] = r / sd[c]

    z = np.load(BUNDLE)
    A = {n: z[n] for n in ARRS}
    nE = int(z["nE"][0]); k = int(z["k"][0])
    nnz = int(z["nnz"][0]); nseg = int(z["nseg"][0])
    args = (A["eidx"], A["seg_ptr"], A["seg_nc"], A["claim_seg_ptr"],
            A["N_T"], A["const"], A["sd"])
    R1, Z1, R2, Z2 = (np.empty(k) for _ in range(4))
    fn = ser if nthreads == 1 else par

    maps = [mapping(A["blk_n"], A["blk_start"], nE, 900 + i)
            for i in range(N_WARM + max(N_BENCH, N_EQ))]
    ser(maps[0], *args, R1, Z1)                      # compile both, discard
    fn(maps[0], *args, R2, Z2)

    worst = 0.0
    for i in range(N_EQ):
        ser(maps[i], *args, R1, Z1)
        fn(maps[i], *args, R2, Z2)
        worst = max(worst, float(np.max(np.abs(R1 - R2))),
                    float(np.max(np.abs(Z1 - Z2))))

    for i in range(N_WARM):
        fn(maps[i], *args, R2, Z2)
    c0, w0, ts = cpu_time(), time.time(), []
    for i in range(N_WARM, N_WARM + N_BENCH):
        t = time.time()
        fn(maps[i], *args, R2, Z2)
        ts.append((time.time() - t) * 1000)
    wall = time.time() - w0; cpu = cpu_time() - c0
    ts = np.array(ts)
    print("RESULT " + json.dumps(dict(
        threads=nthreads, numba_threads=int(get_num_threads()),
        median_ms=round(float(np.median(ts)), 1),
        p95_ms=round(float(np.percentile(ts, 95)), 1),
        min_ms=round(float(ts.min()), 1), max_ms=round(float(ts.max()), 1),
        cpu_utilization=round(cpu / wall, 2),
        peak_rss_gb=round(rss_peak(), 2),
        equivalence_max_abs_diff=worst,
        equivalence_pass=bool(worst <= 1e-10),
        nnz_per_sec_M=round(nnz / (float(np.median(ts)) / 1000) / 1e6, 1),
        seg_per_sec_M=round(nseg / (float(np.median(ts)) / 1000) / 1e6, 1))), flush=True)


# ── orchestrate ─────────────────────────────────────────────────────────────
def orchestrate():
    import t5_artifact as ART
    ART.smoke_test(verbose=False)
    t0 = time.time()
    if not os.path.exists(BUNDLE):
        print("15M_PARALLEL_QUALIFICATION_V1 · building state bundle once", flush=True)
        build()
    print(f"15M_PARALLEL_QUALIFICATION_V1 · fresh subprocess per thread count · "
          f"lever: prange ONLY (no batching)", flush=True)
    py = os.path.join(HERE, ".venv", "bin", "python")
    rows = []
    for t in THREADS:
        p = subprocess.run([py, __file__, "worker", str(t)], capture_output=True, text=True)
        line = [l for l in p.stdout.splitlines() if l.startswith("RESULT ")]
        if not line:
            print(f"  threads {t}: FAILED\n{p.stdout[-500:]}\n{p.stderr[-800:]}", flush=True)
            continue
        r = json.loads(line[0][7:])
        rows.append(r)
        print(f"  threads {r['threads']} (numba reports {r['numba_threads']})  "
              f"median {r['median_ms']:>7.1f} ms · p95 {r['p95_ms']:>7.1f} ms · "
              f"CPU {r['cpu_utilization']:>4.2f}x · peak RSS {r['peak_rss_gb']:.2f} GB · "
              f"{'✓' if r['equivalence_pass'] else '✗'} eq {r['equivalence_max_abs_diff']:.1e}",
              flush=True)

    ok = [r for r in rows if r["equivalence_pass"]]
    if not ok:
        raise RuntimeError("no thread configuration passed equivalence")
    best = min(ok, key=lambda r: r["median_ms"])
    base = next((r for r in ok if r["threads"] == 1), None)
    speedup = round(base["median_ms"] / best["median_ms"], 2) if base else None
    t1 = best["median_ms"] / 1000
    payload = dict(
        spec_id="15M_PARALLEL_QUALIFICATION_V1", status="FIXTURE — NO Y ACCESS",
        lever="prange over claims ONLY; batching deliberately untested here",
        timing_unit="ONE_RANK_VECTOR x ALL_K_CLAIMS",
        claim_order_hash=json.load(open("T5_15M_CLAIM_ORDER_V2.json"))["claim_order_hash"],
        equivalence=dict(reference="serial fused kernel", rtol=0, atol=1e-10,
                         mappings=N_EQ, all_claims=True),
        results=rows, winner_threads=best["threads"],
        serial_median_ms=base["median_ms"] if base else None,
        best_median_ms=best["median_ms"], speedup_vs_serial=speedup,
        t1_seconds=round(t1, 4),
        projected_capability_hours_if_t6_equals_6xt1=round(60 * 999 * 6 * t1 / 3600, 2),
        projection_caveat="valid ONLY while the engine evaluates one rank vector per "
                          "invocation. After batching, t6 must be measured directly — "
                          "multiplying t1 by six would double-count the fusion.",
        next_step="BATCH QUALIFICATION on the winning thread count, then a DIRECT t6 "
                  "measurement through the final engine",
        y_status="NO Y ACCESS; all rank mappings synthetic and deterministic",
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, OUT, required=("spec_id", "results", "winner_threads"))
    print(f"\n  WINNER {best['threads']} threads · {best['median_ms']:.1f} ms"
          + (f" · {speedup}x vs serial" if speedup else ""))
    print(f"  if t6 = 6 x t1 → 60 x 999 x 6 x {t1:.4f}s = "
          f"{60*999*6*t1/3600:.1f} h   (to be replaced by a measured t6)")
    print(f"  WROTE {OUT} · digest {dig}")


if __name__ == "__main__":
    if MODE == "build":
        build()
    elif MODE == "worker":
        worker(int(sys.argv[2]))
    else:
        orchestrate()
