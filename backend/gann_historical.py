"""GANN historical exposure — observed phi=0, Gate A, Gate B. ONE SHOT.

    python gann_historical.py --seal-spec
    python gann_historical.py --run

THE SPEC IS SEALED IN A SEPARATE INVOCATION and the run refuses to start without it. This
is the last point at which the promotion rule can be frozen without already knowing the
number it judges.

THIS IS A ONE-SHOT EXPOSURE. After it, seeing a result and then adjusting a tolerance, a
block definition or a horizon and re-running does not produce a corrected version of this
test — it produces a new search branch that cannot inherit this frozen inference.

    2  OBSERVED   the frozen phi = 0 statistic for ASC / DESC / CONFLUENCE, on the real
                  direction-neutral pair. No nullization.
    3  GATE A     the PRIMARY inferential null. The (LONG, SHORT) pair is permuted JOINTLY
                  within date x pre-liquidity x pre-volatility blocks, 999 times; each
                  permutation yields maxZ = max(Z_ASC, Z_DESC, Z_CONF); promotion is
                  Z_obs > p95(maxZ), STRICTLY. k_actual = 3.
    4  THETA      SKIPPED BY DESIGN. NO FROZEN ESTIMAND EXISTS — see GANN_THETA_STATUS_V1.
    5  GATE B     the REGISTERED SHIFTED-LATTICE FAMILY-WISE PLACEBO REFERENCE. 999 worlds,
                  one phi per ticker per world, shared by both families and every date.
                  maxZ_shift[w] = max over the three claims. NOT an exact null. NOT a second
                  p-value.

Gate A answers "is there a directional excursion association at all?". Gate B answers "is
any such association specific to the anchored integer phase?". A failure of B does not
retract A, and B alone establishes nothing.
"""
from __future__ import annotations
import os, sys                                                         # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
    os.environ.setdefault(_v, "1")
import hashlib, json, time                                             # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
import t5_artifact as ART                                              # noqa: E402
import gann_capability as CAP                                          # noqa: E402
import gann_fast as FA                                                 # noqa: E402
import gann_grid as G, gann_phase as PH                                # noqa: E402

SPEC = "GANN_HISTORICAL_SPEC_V1.json"
OUT = "GANN_HISTORICAL_EXPOSED_V1.json"
STATE = os.path.join(os.path.dirname(HERE), "data", "gann_estimand_state.parquet")
OCS = os.path.join(os.path.dirname(HERE), "data", "gann_outcomes.parquet")
CLAIMS = list(CAP.CLAIMS)
N_PERM = 999
N_PHASE = 999
PERM_NS = "GANN_HISTORICAL_PERM_V1"
PHASE_NS = "GANN_HISTORICAL_PHASE_V1"


def seal_spec():
    root = CAP.rng_root()
    d = ART.seal(dict(
        spec_id="GANN_HISTORICAL_SPEC_V1", status="FROZEN_BEFORE_COMPUTE", family="GANN",
        governing=dict(
            x_freeze=ART.file_digest("GANN_X_FREEZE_V1.json"),
            estimand=ART.file_digest("GANN_ESTIMAND_V1.json"),
            estimand_state=ART.file_digest("GANN_ESTIMAND_STATE_V1.json"),
            outcome_spec=ART.file_digest("GANN_OUTCOME_SPEC_V1.json"),
            outcome_values=ART.file_digest("GANN_OUTCOME_VALUES_V1.json"),
            inference_amendment=ART.file_digest("GANN_INFERENCE_AMENDMENT_V1.json"),
            phase_null=ART.file_digest("GANN_PHASE_NULL_V1.json"),
            capability_result=ART.file_digest("GANN_CAPABILITY_RESULT_V1.json"),
            prey_closure=ART.file_digest("GANN_CAPABILITY_PREY_CLOSURE_V1.json"),
            rank_input_contract=ART.file_digest("GANN_RANK_INPUT_CONTRACT_V1.json"),
            theta_status=ART.file_digest("GANN_THETA_STATUS_V1.json"),
            rank_code=ART.file_digest("gann_rank.py"),
            fast_twin=ART.file_digest("gann_fast.py")),
        k_actual=3, claims=[CAP.CLAIMS[c] for c in CLAIMS], phi=0,
        observed="the frozen phi = 0 statistic on the real direction-neutral pair; LONG rows "
                 "read MFE_LONG_10D and SHORT rows MFE_SHORT_10D, by the frozen X direction",
        gate_A=dict(
            name="PRIMARY INFERENTIAL NULL — paired outcome permutation",
            unit="the (LONG, SHORT) pair, permuted JOINTLY — one row, never two halves",
            blocks=["decision_date", "pre_liquidity LOW/HIGH", "pre_volatility LOW/HIGH"],
            direction_not_a_block="direction is derived from X, so it is not a permutation "
                                  "dimension; it stays a block of the comparison",
            n_perm=N_PERM,
            statistic="maxZ_perm = max(Z_ASC, Z_DESC, Z_CONF) per permutation",
            promotion="Z_obs(claim) > p95(maxZ_perm), STRICTLY greater — family-wise"),
        gate_B=dict(
            name="REGISTERED SHIFTED-LATTICE FAMILY-WISE PLACEBO REFERENCE",
            worlds=N_PHASE,
            mechanism="one phi per ticker per world, shared by both families and every date",
            statistic="maxZ_shift[w] = max(Z_ASC_shift, Z_DESC_shift, Z_CONF_shift)",
            threshold="Z_phi0(claim) > p95(maxZ_shift), strictly",
            forbidden_wording=["exact phase-exchangeability p-value", "randomization p-value",
                               "a second p-value", "probability the Gann construction is "
                               "false"],
            required_wording="registered shifted-lattice placebo reference",
            not_a_retraction="a gate-B failure does not retract gate A; gate B alone "
                             "establishes nothing"),
        theta=dict(status="SKIPPED BY DESIGN",
                   reason="NO FROZEN ESTIMAND EXISTS — OFFICIALLY UNDEFINED — INTENTIONALLY "
                          "NOT COMPUTED",
                   governed_by=ART.file_digest("GANN_THETA_STATUS_V1.json")),
        rng=dict(root=root.hex()[:16], gate_A_namespace=PERM_NS,
                 gate_B_namespace=PHASE_NS,
                 rule="derived from the sealed chain — never chosen"),
        one_shot=dict(
            statement="this exposure is ONE SHOT",
            forbidden="seeing a result and then adjusting a tolerance, a block definition, a "
                      "horizon or a claim and re-running",
            why="such a re-run is not a corrected version of this test; it is a new search "
                "branch, and it cannot inherit this frozen inference"),
        frozen_and_may_not_change=["X", "the outcome definition", "blocks", "k = 3",
                                   "permutation logic", "the promotion rule"],
        y_status="NO OUTCOME STATISTIC COMPUTED — this artifact freezes the rule",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        SPEC, required=("spec_id", "governing", "gate_A", "gate_B", "theta"),
        supersede=os.path.exists(SPEC))
    print(f"GANN_HISTORICAL_SPEC_V1 · {d} · FROZEN_BEFORE_COMPUTE")


def _blocks(date_code, dir_arr, liq_code, vol_code, n_liq=2):
    """date | direction | liq | vol as an integer code — no string factorize per world."""
    dmap = {1: 0, -1: 1, 0: 2, 9: 3}
    di = np.select([dir_arr == 1, dir_arr == -1, dir_arr == 0], [0, 1, 2], default=3)
    return ((date_code * 4 + di) * 2 + liq_code) * 2 + vol_code


def run():
    t0 = time.time()
    if not os.path.exists(SPEC):
        raise SystemExit("seal the spec first")
    S = json.load(open(SPEC))
    assert S["status"] == "FROZEN_BEFORE_COMPUTE"
    root = CAP.rng_root()
    assert root.hex()[:16] == S["rng"]["root"]

    St = pd.read_parquet(STATE)
    O = pd.read_parquet(OCS, columns=["ticker", "date", "MFE_LONG_10D", "MFE_SHORT_10D"])
    St = St.merge(O, on=["ticker", "date"], how="left")
    long_y = St.MFE_LONG_10D.to_numpy(float)
    short_y = St.MFE_SHORT_10D.to_numpy(float)
    FA.assert_finite(long_y, short_y)          # GANN_RANK_INPUT_CONTRACT_V1
    n = len(St)
    nullblock, _ = pd.factorize(St.date + "|" + St.liq_half + "|" + St.vol_half)
    nullblock = nullblock.astype(np.int32)
    NP = FA.nullize_prepare(nullblock, int(nullblock.max()) + 1)
    date_code, _ = pd.factorize(St.date)
    liq_code = (St.liq_half == "HIGH").to_numpy().astype(np.int64)
    vol_code = (St.vol_half == "HIGH").to_numpy().astype(np.int64)

    P0, dir0 = {}, {}
    for c in CLAIMS:
        d = St[f"{c}_dir"].to_numpy()
        treated = St[f"{c}_treated"].to_numpy() & np.isin(d, [1, -1])
        blk, _ = pd.factorize(St[f"{c}_block"])
        P0[c] = FA.prepare(treated, blk.astype(np.int32), int(blk.max()) + 1)
        dir0[c] = d
    print(f"  frame {n:,} rows · nullization blocks {NP['n_nullblock']:,}", flush=True)

    # ── 2 · OBSERVED phi = 0 ────────────────────────────────────────────────
    z_obs = {}
    for c in CLAIMS:
        y = CAP.select_component(long_y, short_y, dir0[c])
        z_obs[c] = float(FA.z_prepared(y, P0[c])[0])
    print(f"  OBSERVED phi=0 · " + " · ".join(f"{c} {z_obs[c]:+.4f}" for c in CLAIMS),
          flush=True)

    # ── 3 · GATE A ──────────────────────────────────────────────────────────
    mxA = np.empty(N_PERM)
    for p in range(N_PERM):
        seed = CAP._seed(PERM_NS, root, p)
        pl, ps, _ = FA.nullize_prepared(long_y, short_y, NP, seed)
        mxA[p] = max(FA.z_prepared(CAP.select_component(pl, ps, dir0[c]), P0[c])[0]
                     for c in CLAIMS)
        if (p + 1) % 200 == 0:
            print(f"    gate A perm {p+1}/{N_PERM} · {(time.time()-t0)/60:.1f} min",
                  flush=True)
    p95A = float(np.percentile(mxA, 95))
    gateA = {c: bool(z_obs[c] > p95A) for c in CLAIMS}
    print(f"  GATE A p95 {p95A:.4f} · promotions {gateA}", flush=True)

    # ── 5 · GATE B ──────────────────────────────────────────────────────────
    Gr = pd.read_parquet(G.OUT_GRID)
    st = PH.prepare(Gr)
    key = pd.DataFrame(dict(ticker=np.asarray(st["tickers"])[st["tick_code"]],
                            date=pd.Series(st["date"]).astype(str).str[:10].to_numpy()))
    key["gi"] = np.arange(len(key))
    j = key.merge(St[["ticker", "date"]].assign(si=np.arange(n)), on=["ticker", "date"],
                  how="inner")
    gi, si = j.gi.to_numpy(), j.si.to_numpy()
    dcode_g, liq_g, vol_g = date_code[si], liq_code[si], vol_code[si]
    yL, yS = long_y[si], short_y[si]
    n_tick = len(st["tickers"])
    mxB = np.empty(N_PHASE)
    for w in range(N_PHASE):
        rng = np.random.default_rng(CAP._seed(PHASE_NS, root, w))
        ws = PH.world_state(st, rng.random(n_tick))
        zs = []
        for c in CLAIMS:
            dw = ws[c]["direction"][gi]
            tw = ws[c]["onset"][gi] & np.isin(dw, [1, -1])
            bw = _blocks(dcode_g, dw, liq_g, vol_g)
            bw = pd.factorize(bw)[0].astype(np.int32)
            zs.append(FA.z_prepared(CAP.select_component(yL, yS, dw),
                                    FA.prepare(tw, bw, int(bw.max()) + 1))[0])
        mxB[w] = np.nanmax(zs)
        if (w + 1) % 100 == 0:
            print(f"    gate B world {w+1}/{N_PHASE} · {(time.time()-t0)/60:.1f} min",
                  flush=True)
    p95B = float(np.percentile(mxB, 95))
    gateB = {c: bool(z_obs[c] > p95B) for c in CLAIMS}
    print(f"  GATE B p95 {p95B:.4f} · {gateB}", flush=True)

    verdict = {}
    for c in CLAIMS:
        verdict[CAP.CLAIMS[c]] = (
            "A PASS, B PASS — association exists AND is concentrated at the anchored "
            "integer lattice relative to same-geometry phase shifts" if gateA[c] and gateB[c]
            else "A PASS, B FAIL — a price-reaction association exists, but it is NOT "
                 "specific to the Gann integer phase" if gateA[c]
            else "A FAIL — no registered directional association is established; gate B is "
                 "not interpreted")

    d = ART.seal(dict(
        spec_id="GANN_HISTORICAL_EXPOSED_V1", status="HISTORICAL_EXPOSED", family="GANN",
        spec=ART.file_digest(SPEC),
        capability_result=ART.file_digest("GANN_CAPABILITY_RESULT_V1.json"),
        theta_status=ART.file_digest("GANN_THETA_STATUS_V1.json"),
        k_actual=3, phi=0, n_perm=N_PERM, n_phase_worlds=N_PHASE,
        rng_root=root.hex()[:16],
        common_frame=dict(rows=n, nullization_blocks=int(NP["n_nullblock"]),
                          per_claim={c: dict(treated=int(P0[c]["nt"].sum()),
                                             eligible_blocks=int(P0[c]["eligible"].sum()))
                                     for c in CLAIMS}),
        observed={CAP.CLAIMS[c]: round(z_obs[c], 4) for c in CLAIMS},
        gate_A=dict(name="PRIMARY INFERENTIAL NULL — paired outcome permutation",
                    p95=round(p95A, 4), median=round(float(np.median(mxA)), 4),
                    min=round(float(mxA.min()), 4), max=round(float(mxA.max()), 4),
                    promotion="Z_obs > p95, STRICTLY",
                    result={CAP.CLAIMS[c]: ("PASS" if gateA[c] else "FAIL") for c in CLAIMS},
                    null_digest=hashlib.sha256(mxA.tobytes()).hexdigest()[:16]),
        gate_B=dict(name="REGISTERED SHIFTED-LATTICE FAMILY-WISE PLACEBO REFERENCE",
                    p95=round(p95B, 4), median=round(float(np.median(mxB)), 4),
                    min=round(float(mxB.min()), 4), max=round(float(mxB.max()), 4),
                    result={CAP.CLAIMS[c]: ("PASS" if gateB[c] else "FAIL") for c in CLAIMS},
                    wording="registered shifted-lattice placebo reference — NOT an exact "
                            "null, NOT a second p-value",
                    null_digest=hashlib.sha256(mxB.tobytes()).hexdigest()[:16]),
        theta="NO FROZEN ESTIMAND EXISTS — OFFICIALLY UNDEFINED — INTENTIONALLY NOT COMPUTED",
        verdict=verdict,
        one_shot="this exposure is not repeatable under the frozen inference; any change of "
                 "tolerance, block definition, horizon or claim after seeing it is a new "
                 "search branch",
        exposure_timestamp=time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        runtime_min=round((time.time() - t0) / 60, 1)),
        OUT, required=("spec_id", "observed", "gate_A", "gate_B", "theta", "verdict"),
        supersede=os.path.exists(OUT))
    print(f"\nGANN_HISTORICAL_EXPOSED_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    if "--seal-spec" in sys.argv:
        seal_spec()
    elif "--run" in sys.argv:
        run()
    else:
        raise SystemExit("use --seal-spec first, then --run")
