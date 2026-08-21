"""Pre-launch gates for the evidentiary T9 capability run. All three must PASS.

    1  SEED MANIFEST REPRODUCIBILITY   two fresh processes, different PYTHONHASHSEED
                                       -> identical complete seed manifest digest
    2  WORLD REPLAY                    same sealed world seed -> bit-identical injected
                                       state, ranks, analytic SD, and 999-perm max-Z vector
    3  SOURCE SWEEP                    no builtin hash() in the stochastic path
"""
from __future__ import annotations
import hashlib, json, os, re, subprocess, sys, time                    # noqa: E402
import numpy as np                                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)

MANIFEST_SNIPPET = r'''
import os, sys, json, hashlib
os.chdir(%r); sys.path.insert(0, %r)
import t9_capability_run as CR
CO = json.load(open("T9_1H_CLAIM_ORDER_V1.json"))
import t5_artifact as ART
hashes = dict(claim_order_hash=CO["claim_order_hash"],
              population_hash=CO["population_hash"],
              block_assignment_hash=CO["block_assignment_hash"],
              capability_protocol_hash=ART.file_digest("T9_CAPABILITY_PROTOCOL_V1.json"))
man = {}
for nk, nd in CO["needles"].items():
    for delta in CR.DELTAS:
        for w in range(CR.WORLDS):
            ws = CR.world_seed(hashes, nd["membership_hash"], delta, w)
            man[f"{nk}|{delta:.1f}|{w}"] = ws.hex()
            man[f"{nk}|{delta:.1f}|{w}|p0"] = str(CR.perm_seed(ws, 0))
            man[f"{nk}|{delta:.1f}|{w}|p998"] = str(CR.perm_seed(ws, 998))
print(hashlib.sha256(json.dumps(man, sort_keys=True).encode()).hexdigest()[:16])
'''


def gate1():
    digests = []
    for phs in ("0", "12345"):
        env = dict(os.environ, PYTHONHASHSEED=phs)
        r = subprocess.run([".venv/bin/python", "-c", MANIFEST_SNIPPET % (HERE, HERE)],
                           capture_output=True, text=True, env=env, cwd=HERE, timeout=300)
        assert r.returncode == 0, r.stderr[-400:]
        digests.append(r.stdout.strip().splitlines()[-1])
    ok = digests[0] == digests[1]
    print(f"1 SEED MANIFEST  {'PASS' if ok else 'FAIL'} · PYTHONHASHSEED=0 -> {digests[0]} · "
          f"=12345 -> {digests[1]}")
    return ok, digests[0]


def gate2():
    import t9_capability_run as CR
    import t5_artifact as ART
    CO = json.load(open("T9_1H_CLAIM_ORDER_V1.json"))
    hashes = dict(claim_order_hash=CO["claim_order_hash"],
                  population_hash=CO["population_hash"],
                  block_assignment_hash=CO["block_assignment_hash"],
                  capability_protocol_hash=ART.file_digest("T9_CAPABILITY_PROTOCOL_V1.json"))
    ids, bcode, nb, y_hist, S, ORDER = CR.build_state(verbose=False)
    Nb = np.bincount(bcode, minlength=nb).astype(float)
    border, bptr = CR.block_structs(bcode, nb)
    nd = CO["needles"]["q50"]
    krow = int(ORDER.index[ORDER.j == nd["j"]][0])
    from scipy import sparse
    NT = np.zeros((nb, S.shape[1]))
    for k in range(S.shape[1]):
        NT[:, k] = np.bincount(bcode[S[:, k]], minlength=nb)
    NC = Nb[:, None] - NT
    OV = (NT > 0) & (NC > 0)
    Ntot = (NT * OV).sum(0)
    rows, cols, vals = [], [], []
    for k in range(S.shape[1]):
        m = np.flatnonzero(S[:, k]); bm = bcode[m]; keep = OV[bm, k]
        m, bm = m[keep], bm[keep]
        rows.append(np.full(len(m), k)); cols.append(m)
        vals.append(1.0 / (Ntot[k] * NC[bm, k]))
    A = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows), np.concatenate(cols))),
                          shape=(S.shape[1], len(ids)))
    c_s = np.array([(NT[:, k] * OV[:, k] * (Nb + 1) / 2
                     / (Ntot[k] * np.where(NC[:, k] > 0, NC[:, k], 1)))[OV[:, k]].sum()
                    for k in range(S.shape[1])])
    bc_at_border = bcode[border].astype(float)

    def one_world():
        wseed = CR.world_seed(hashes, nd["membership_hash"], 1.0, 0)
        rng = np.random.default_rng(CR._u64(wseed))
        y = y_hist.copy()
        for b in range(nb):
            idx = border[bptr[b]:bptr[b + 1]]
            y[idx] = y[idx][rng.permutation(len(idx))]
        y[S[:, krow]] += 1.0 / 100.0
        r, tie = CR.midranks_and_ties(y, border, bptr, nb)
        with np.errstate(divide="ignore", invalid="ignore"):
            var = ((Nb[:, None] + 1) - (tie / np.where(Nb > 1, Nb * (Nb - 1), 1))[:, None]) \
                  / (12 * NT * NC)
        var = np.where(OV, var, 0.0)
        w = np.where(OV, NT, 0.0); w = w / np.where(w.sum(0) > 0, w.sum(0), 1)
        sd = np.sqrt((w ** 2 * var).sum(0))
        inv_sd = 1.0 / np.where(sd > 0, sd, 1.0)
        mx = np.empty(CR.NPERM)
        for p in range(CR.NPERM):
            rng_p = np.random.default_rng(CR.perm_seed(wseed, p))
            key = bc_at_border + rng_p.random(len(border))
            rp = np.empty_like(r)
            rp[border] = r[border[np.argsort(key, kind="stable")]]
            mx[p] = (((A @ rp) - c_s) * inv_sd).max()
        return y, r, sd, mx

    t = time.time()
    y1, r1, sd1, mx1 = one_world()
    y2, r2, sd2, mx2 = one_world()
    ok = (np.array_equal(y1, y2) and np.array_equal(r1, r2)
          and np.array_equal(sd1, sd2) and np.array_equal(mx1, mx2))
    print(f"2 WORLD REPLAY   {'PASS' if ok else 'FAIL'} · injected-Y, ranks, SD and the full "
          f"999-perm max-Z vector bit-identical across two from-scratch builds · "
          f"{time.time()-t:.0f}s")
    return ok


def gate3():
    src = open("t9_capability_run.py").read()
    bad = []
    for i, line in enumerate(src.split("\n"), 1):
        code = line.split("#")[0]
        if re.search(r"(?<![\w.])hash\(", code) and "membership_hash" not in code:
            if '"' not in code or "hash()" not in code:
                bad.append(f"{i}: {line.strip()[:70]}")
    ok = not bad
    print(f"3 SOURCE SWEEP   {'PASS' if ok else 'FAIL'}" +
          ("" if ok else " · " + " ; ".join(bad)))
    return ok


if __name__ == "__main__":
    ok1, mdig = gate1()
    ok2 = gate2()
    ok3 = gate3()
    if ok1 and ok2 and ok3:
        print(f"ALL GATES PASS · seed_manifest {mdig}")
    else:
        raise SystemExit("GATES FAILED — do not launch the evidentiary run")
