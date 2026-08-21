"""15M_BATCH_QUALIFICATION_V1 — does amortising the index gather over B rank vectors pay?

Threads fixed at the measured winner (8). One lever at a time: nothing about scheduling,
partitioning or claim ordering changes here.

THE LAYOUT IS THE WHOLE BET

    ranksT[nE, B]  C-contiguous

so the inner loop reads eidx[i] ONCE and then B contiguous values. The transposed layout
ranks[B, nE] would read eidx[i] once too but scatter the B loads across B different pages,
which is the arrangement that cannot help. If batching does nothing under this layout, it
does nothing.

B = 6 IS NOT A GRID POINT, IT IS THE WORKLOAD

The capability evaluates six delta-injected rank vectors per permutation, so B = 6 is the
actual operating point. 1, 2, 4 and 8 are there to show the shape of the curve around it.

THE WINNER IS NOT THE LOWEST ms/vector

A configuration can win on throughput and lose on the job that has to be done. B = 8
computes eight lanes; feeding it six vectors wastes two. What decides is measured t6 — the
cost of putting exactly six vectors through — not ms/vector scaled by six.

    B=8: 420 ms / 8 lanes  ->  52.5 ms/vector, but t6 is still 420 ms
    B=6: 340 ms / 6 lanes  ->  56.7 ms/vector, and t6 is 340 ms

so B=6 would win the capability even though B=8 wins the throughput column.

EQUIVALENCE HAS TWO PARTS

    1  batched[:, b] vs the plain prange kernel on that exact rank vector, all 31,583 claims
    2  one B=6 invocation vs six independent single-vector invocations

The second is the capability's real execution pattern, so it is checked as such rather than
inferred from the first. Exact zero is plausible if each (claim, lane) accumulates in the same
order, but it is NOT required — the registered gate is atol 1e-10.
"""
from __future__ import annotations
import os, sys

MODE = sys.argv[1] if len(sys.argv) > 1 else "orchestrate"
THREADS = 8
if MODE == "worker":
    os.environ["NUMBA_NUM_THREADS"] = str(THREADS)
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import json, math, resource, subprocess, time                         # noqa: E402
import numpy as np                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_15m_par_qualify as PQ                                       # noqa: E402
# PQ parses sys.argv at module level and sets NUMBA_NUM_THREADS from argv[2]. Here argv[2] is
# the BATCH SIZE, so importing PQ would silently pin threads to B — B=6 would run on 6 threads
# and the batch curve would be measured against a moving thread count. Re-assert it, and
# assert that nothing else can have moved it before numba is imported inside worker().
if MODE == "worker":
    os.environ["NUMBA_NUM_THREADS"] = str(THREADS)
    assert "numba" not in sys.modules, "numba imported before the thread count was fixed"

BUNDLE = PQ.BUNDLE
OUT = os.path.join(HERE, "T5_15M_BATCH_QUALIFICATION_V1.json")
BATCHES = [1, 2, 4, 6, 8]
N_WARM, N_BENCH, N_EQ = 3, 10, 5
N_DELTA = 6                       # the capability's delta grid size


def rss_peak():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9


def rss_now():
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1e9
    except Exception:
        return float("nan")


def worker(B):
    from numba import njit, prange, get_num_threads

    @njit(cache=True, fastmath=False, nogil=True, parallel=True)
    def single(rank2, eidx, seg_ptr, seg_nc, csp, N_T, const, sd, R, Z):
        for c in prange(csp.shape[0] - 1):
            acc = 0.0
            for s in range(csp[c], csp[c + 1]):
                tot = 0
                for i in range(seg_ptr[s], seg_ptr[s + 1]):
                    tot += rank2[eidx[i]]
                acc += (tot * 0.5) / seg_nc[s]
            r = acc / N_T[c] + const[c]
            R[c] = r; Z[c] = r / sd[c]

    @njit(cache=True, fastmath=False, nogil=True, parallel=True)
    def batch(ranksT, eidx, seg_ptr, seg_nc, csp, N_T, const, sd, R, Z):
        nb = ranksT.shape[1]
        for c in prange(csp.shape[0] - 1):
            acc = np.zeros(nb)
            tot = np.zeros(nb)
            for s in range(csp[c], csp[c + 1]):
                for b in range(nb):
                    tot[b] = 0.0
                for i in range(seg_ptr[s], seg_ptr[s + 1]):
                    e = eidx[i]
                    for b in range(nb):
                        tot[b] += ranksT[e, b]
                inv = 0.5 / seg_nc[s]
                for b in range(nb):
                    acc[b] += tot[b] * inv
            nt = N_T[c]; cn = const[c]; sc = sd[c]
            for b in range(nb):
                r = acc[b] / nt + cn
                R[c, b] = r; Z[c, b] = r / sc

    z = np.load(BUNDLE)
    A = {n: z[n] for n in PQ.ARRS}
    nE = int(z["nE"][0]); k = int(z["k"][0])
    args = (A["eidx"], A["seg_ptr"], A["seg_nc"], A["claim_seg_ptr"],
            A["N_T"], A["const"], A["sd"])
    vecs = [PQ.mapping(A["blk_n"], A["blk_start"], nE, 900 + i)
            for i in range(N_WARM + N_BENCH + N_EQ + N_DELTA)]

    def pack(vs):
        M = np.empty((nE, len(vs)), np.int32)
        for i, v in enumerate(vs):
            M[:, i] = v
        return M

    Rb = np.empty((k, B)); Zb = np.empty((k, B))
    R1 = np.empty(k); Z1 = np.empty(k)
    single(vecs[0], *args, R1, Z1)                       # compile, discard
    batch(pack(vecs[:B]), *args, Rb, Zb)

    # ── equivalence 1 · every lane vs the plain single-vector kernel ────────
    w1 = 0.0
    for m in range(N_EQ):
        vs = vecs[m:m + B]
        batch(pack(vs), *args, Rb, Zb)
        for b in range(B):
            single(vs[b], *args, R1, Z1)
            w1 = max(w1, float(np.max(np.abs(Rb[:, b] - R1))),
                     float(np.max(np.abs(Zb[:, b] - Z1))))

    # ── equivalence 2 · the capability's real pattern: 6 vectors ───────────
    six = vecs[-N_DELTA:]
    R6 = np.empty((k, N_DELTA)); Z6 = np.empty((k, N_DELTA))
    batch(pack(six), *args, R6, Z6)
    w2 = 0.0
    for b in range(N_DELTA):
        single(six[b], *args, R1, Z1)
        w2 = max(w2, float(np.max(np.abs(R6[:, b] - R1))),
                 float(np.max(np.abs(Z6[:, b] - Z1))))

    # ── benchmark ──────────────────────────────────────────────────────────
    packs = [pack(vecs[i:i + B]) for i in range(N_WARM + N_BENCH)]
    for i in range(N_WARM):
        batch(packs[i], *args, Rb, Zb)
    steady = rss_now()
    ts = []
    for i in range(N_WARM, N_WARM + N_BENCH):
        t = time.time()
        batch(packs[i], *args, Rb, Zb)
        ts.append((time.time() - t) * 1000)
    ts = np.array(ts)
    med = float(np.median(ts))

    # ── measured t6 · exactly six vectors through THIS configuration ───────
    n_inv = math.ceil(N_DELTA / B)
    sixpacks = [pack((six * n_inv)[i * B:(i + 1) * B]) for i in range(n_inv)]
    for _ in range(2):
        for p in sixpacks:
            batch(p, *args, Rb, Zb)
    t6s = []
    for _ in range(N_BENCH):
        t = time.time()
        for p in sixpacks:
            batch(p, *args, Rb, Zb)
        t6s.append((time.time() - t) * 1000)
    t6 = float(np.median(np.array(t6s)))

    print("RESULT " + json.dumps(dict(
        B=B, numba_threads=int(get_num_threads()),
        ms_per_batch=round(med, 1), ms_per_vector=round(med / B, 1),
        p95_ms_per_batch=round(float(np.percentile(ts, 95)), 1),
        invocations_for_six=n_inv, lanes_computed_for_six=n_inv * B,
        wasted_lanes=n_inv * B - N_DELTA,
        t6_ms=round(t6, 1),
        steady_rss_gb=round(steady, 2), peak_rss_gb=round(rss_peak(), 2),
        eq_lane_vs_single=w1, eq_six_vs_six_singles=w2,
        eq_pass=bool(w1 <= 1e-10 and w2 <= 1e-10))), flush=True)


def orchestrate():
    import t5_artifact as ART
    ART.smoke_test(verbose=False)
    t0 = time.time()
    if not os.path.exists(BUNDLE):
        raise RuntimeError("state bundle missing — run the parallel qualification first")
    PAR = json.load(open("T5_15M_PARALLEL_QUALIFICATION_V1.json"))
    print(f"15M_BATCH_QUALIFICATION_V1 · threads fixed at {THREADS} "
          f"(measured winner, {PAR['best_median_ms']} ms) · fresh subprocess per B",
          flush=True)
    py = os.path.join(HERE, ".venv", "bin", "python")
    rows = []
    for B in BATCHES:
        p = subprocess.run([py, __file__, "worker", str(B)], capture_output=True, text=True)
        line = [l for l in p.stdout.splitlines() if l.startswith("RESULT ")]
        if not line:
            print(f"  B={B}: FAILED\n{p.stdout[-400:]}\n{p.stderr[-900:]}", flush=True)
            continue
        r = json.loads(line[0][7:]); rows.append(r)
        print(f"  B={r['B']}  batch {r['ms_per_batch']:>7.1f} ms · vector "
              f"{r['ms_per_vector']:>6.1f} ms · t6 {r['t6_ms']:>7.1f} ms "
              f"({r['invocations_for_six']} inv, {r['wasted_lanes']} wasted lane"
              f"{'s' if r['wasted_lanes'] != 1 else ''}) · RSS steady "
              f"{r['steady_rss_gb']:.2f} / peak {r['peak_rss_gb']:.2f} GB · "
              f"{'✓' if r['eq_pass'] else '✗'} eq {max(r['eq_lane_vs_single'], r['eq_six_vs_six_singles']):.1e}",
              flush=True)

    ok = [r for r in rows if r["eq_pass"]]
    if not ok:
        raise RuntimeError("no batch configuration passed equivalence")
    thr_win = min(ok, key=lambda r: r["ms_per_vector"])
    cap_win = min(ok, key=lambda r: r["t6_ms"])
    t6 = cap_win["t6_ms"] / 1000
    hours = 60 * 999 * t6 / 3600
    base_t6 = next((r["t6_ms"] for r in ok if r["B"] == 1), None)

    payload = dict(
        spec_id="15M_BATCH_QUALIFICATION_V1", status="FIXTURE — NO Y ACCESS",
        lever="batching ONLY; threads fixed at the measured winner",
        threads=THREADS, timing_unit="ms per invocation of B rank vectors x ALL_K_CLAIMS",
        layout="ranksT[nE, B] C-contiguous — eidx read once, B contiguous values",
        claim_order_hash=PAR["claim_order_hash"],
        equivalence=dict(rtol=0, atol=1e-10,
                         part1="each lane vs the plain single-vector prange kernel, all k",
                         part2="one B=6 invocation vs six independent single invocations"),
        results=rows,
        throughput_winner_B=thr_win["B"], throughput_ms_per_vector=thr_win["ms_per_vector"],
        capability_winner_B=cap_win["B"], measured_t6_ms=cap_win["t6_ms"],
        t6_speedup_vs_B1=round(base_t6 / cap_win["t6_ms"], 2) if base_t6 else None,
        winner_rule="capability winner minimises MEASURED t6, not ms/vector — a batch wider "
                    "than the delta grid computes lanes that are thrown away",
        capability_inner_workload_hours=round(hours, 2),
        workload_formula="60 worlds-needles x 999 perms x measured t6; no x6 factor, the "
                         "six deltas are inside t6",
        excludes="per-(needle,delta,world) rank rebuilds, outer-world setup and checkpoint I/O",
        not_attempted="claim-order / work-balanced partitioning. The 4-thread efficiency dip "
                      "(73% vs 99% at 2) looks like static-partition imbalance over claims "
                      "whose nnz spans 300..83,571, but changing scheduling would be a new "
                      "engine variant needing its own equivalence and benchmark.",
        y_status="NO Y ACCESS; all rank mappings synthetic and deterministic",
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, OUT, required=("spec_id", "results", "measured_t6_ms"))
    print(f"\n  throughput winner  B={thr_win['B']} · {thr_win['ms_per_vector']} ms/vector")
    print(f"  CAPABILITY winner  B={cap_win['B']} · measured t6 = {cap_win['t6_ms']} ms"
          + (f" · {round(base_t6/cap_win['t6_ms'],2)}x vs B=1" if base_t6 else ""))
    print(f"  inner workload  60 x 999 x {t6:.4f}s = {hours:.1f} h   (measured, no x6)")
    print(f"  WROTE {OUT} · digest {dig}")


if __name__ == "__main__":
    if MODE == "worker":
        worker(int(sys.argv[2]))
    else:
        orchestrate()
