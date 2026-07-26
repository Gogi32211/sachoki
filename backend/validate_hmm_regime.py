"""
validate_hmm_regime.py — HMM market-regime gate for the Edge setups.

Honesty rules (per our validation discipline):
  · HMM is FIT on TRAIN only (2021-07 .. 2023-12-31).
  · States are decoded CAUSALLY with the forward filter P(state_t | obs_1..t) —
    NO Viterbi / NO smoothed posteriors (both peek at the future).
  · Trades are tagged by the state known at ENTRY date.

Daily market features (whole-universe, from studio_analytics):
  mkt_ret    — equal-weight mean daily return
  breadth20  — share of tickers closing above their 20d average close
  disp       — cross-sectional std of daily returns

Test battery:
  1. per-state market stats (sanity: states must separate).
  2. per-setup per-state trade stats — headline: Atomic-R risk-OFF-only claim.
  3. gated vs ungated portfolio (equal-slot daily curve) for the interesting setups.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb
from hmmlearn.hmm import GaussianHMM

from studio.paths import ANALYTICS_DB
import edge_replay as ER
from edge_tearsheet import daily_returns

TRAIN_END = "2023-12-31"
N_STATES = 3
SEED = 7


def market_features() -> pd.DataFrame:
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = con.execute("""
            WITH u AS (
              SELECT ticker, date, close,
                     row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
              FROM bars),
            b AS (
              SELECT ticker, date, close,
                     close / lag(close) OVER (PARTITION BY ticker ORDER BY date) - 1 AS ret,
                     avg(close) OVER (PARTITION BY ticker ORDER BY date
                                      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
              FROM u WHERE rn = 1)
            SELECT CAST(date AS VARCHAR)[:10] AS day,
                   avg(ret)                        AS mkt_ret,
                   avg(CASE WHEN close > ma20 THEN 1.0 ELSE 0.0 END) AS breadth20,
                   stddev_samp(ret)                AS disp,
                   count(*)                        AS n
            FROM b WHERE ret IS NOT NULL AND abs(ret) < 1.0
            GROUP BY 1 HAVING count(*) > 500 ORDER BY 1
        """).fetchdf()
    finally:
        con.close()
    return df


def forward_filter(model: GaussianHMM, X: np.ndarray) -> np.ndarray:
    """Causal filtered state probabilities P(state_t | obs_1..t). No lookahead."""
    from scipy.stats import multivariate_normal
    n, K = len(X), model.n_components
    logB = np.column_stack([
        multivariate_normal.logpdf(X, mean=model.means_[k], cov=model.covars_[k],
                                   allow_singular=True)
        for k in range(K)])
    A = model.transmat_
    probs = np.zeros((n, K))
    alpha = np.log(model.startprob_ + 1e-300) + logB[0]
    probs[0] = np.exp(alpha - alpha.max()); probs[0] /= probs[0].sum()
    for t in range(1, n):
        # alpha_t(k) = logB_t(k) + logsumexp_j(alpha_{t-1}(j) + log A[j,k])
        m = alpha.max()
        alpha = logB[t] + np.log(np.exp(alpha - m) @ A + 1e-300) + m
        p = np.exp(alpha - alpha.max()); probs[t] = p / p.sum()
    return probs


def main():
    feats = market_features()
    cols = ["mkt_ret", "breadth20", "disp"]
    X = feats[cols].to_numpy(float)
    # z-score with TRAIN moments only (no test leakage into scaling either)
    tr_mask = (feats["day"] <= TRAIN_END).to_numpy()
    mu, sd = X[tr_mask].mean(0), X[tr_mask].std(0)
    Xz = (X - mu) / sd

    hmm = GaussianHMM(n_components=N_STATES, covariance_type="full",
                      n_iter=300, random_state=SEED)
    hmm.fit(Xz[tr_mask])
    probs = forward_filter(hmm, Xz)          # causal, full series
    state = probs.argmax(1)
    feats["state"] = state

    # label states by their TRAIN market character
    lab = {}
    trf = feats[tr_mask]
    prof = trf.groupby("state")[cols].mean()
    riskoff = prof["disp"].idxmax()          # highest dispersion = panic
    riskon = prof["mkt_ret"].idxmax() if prof["mkt_ret"].idxmax() != riskoff else \
             prof.drop(index=riskoff)["mkt_ret"].idxmax()
    for s in range(N_STATES):
        lab[s] = "RISK_OFF" if s == riskoff else ("RISK_ON" if s == riskon else "NEUTRAL")
    feats["regime"] = feats["state"].map(lab)

    print("=== state profile (full period, causal decode) ===")
    g = feats.groupby("regime").agg(days=("day", "size"), mkt=("mkt_ret", "mean"),
                                    br=("breadth20", "mean"), disp=("disp", "mean"))
    print((g.assign(mkt=lambda d: (d["mkt"] * 100).round(3))).to_string())
    # persistence
    sw = (feats["state"].diff() != 0).sum()
    print(f"state switches: {sw} over {len(feats)} days (avg run {len(feats)/max(sw,1):.0f}d)")

    day2reg = dict(zip(feats["day"], feats["regime"]))

    # ── per-setup per-regime trade stats ──────────────────────────────────────
    grp, as_of = ER._frame(62, 3_000_000)
    FOCUS = ["Atomic-R", "T1-CapBounce", "Z11-T11", "L43-TRIPLE", "Parabola", "P55",
             "Washout", "G3-gap"]
    print(f"\n=== per-regime trade stats (entry-date regime, causal) · as_of {as_of} ===")
    print(f"  {'setup':14s} {'regime':9s} {'n':>6s} {'mean%':>7s} {'med%':>7s} {'win%':>5s}")
    gated_curves = {}
    for name in FOCUS:
        col = dict(ER.SETUPS)[name]
        tr = ER._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
        if not len(tr):
            continue
        tr["regime"] = tr["date_in"].str[:10].map(day2reg)
        for reg_, sub in tr.groupby("regime"):
            r = sub["ret"] * 100
            print(f"  {name:14s} {reg_:9s} {len(sub):6d} {r.mean():7.2f} {r.median():7.2f} "
                  f"{(r > 0).mean()*100:5.0f}")
        gated_curves[name] = tr

    # ── gated vs ungated portfolio (Atomic-R headline + tail setups) ──────────
    print("\n=== gated portfolio effect (equal-slot daily curve) ===")
    print(f"  {'setup':14s} {'variant':22s} {'n':>6s} {'cum%':>8s} {'sharpe':>7s} {'maxDD%':>7s}")
    def curve_stats(tr):
        if len(tr) < 30:
            return None
        rets, K = daily_returns(tr)
        eq = (1 + rets).cumprod()
        dd = (eq / eq.cummax() - 1).min() * 100
        sh = rets.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0
        return len(tr), (eq.iloc[-1] - 1) * 100, sh, dd
    for name, gates in [("Atomic-R", ["RISK_OFF"]), ("Parabola", ["RISK_ON"]),
                        ("P55", ["RISK_ON"]), ("Washout", ["RISK_OFF"]),
                        ("T1-CapBounce", ["RISK_OFF", "NEUTRAL"])]:
        tr = gated_curves.get(name)
        if tr is None:
            continue
        for label_, sub in [("ungated", tr), ("gated→" + "+".join(gates),
                                              tr[tr["regime"].isin(gates)])]:
            st = curve_stats(sub)
            if st:
                print(f"  {name:14s} {label_:22s} {st[0]:6d} {st[1]:8.1f} {st[2]:7.2f} {st[3]:7.1f}")

    # per-year split of the headline (Atomic-R gated) for era-independence
    tr = gated_curves["Atomic-R"]; sub = tr[tr["regime"] == "RISK_OFF"]
    print("\nAtomic-R RISK_OFF per-year mean%: ",
          {y: round(v * 100, 2) for y, v in sub.groupby("yr")["ret"].mean().items()})


if __name__ == "__main__":
    main()
