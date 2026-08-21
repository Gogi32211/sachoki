"""SEQUENCE_GRAMMAR_V1 — the frozen search space, enumerated X-only.

No outcome is joined. Frequency is reported because frequency is a property of X.

WHY STRICT ADJACENCY AND NOTHING ELSE

Allowing `A -> B` alongside `within 2`, `within 3`, `within 4` turns one economic idea into
four selectable claims, and the Combo Miner run measured what breadth costs: the band moved
4.67x while the search tax stayed identical. So V1 buys one syntax. WITHIN-k is a later
grammar with its own version, not a variant hidden inside this one.

ELEMENT SEMANTICS: ATOMIC TOKEN PRESENCE, NOT BAR STATE

    S(A->B)(e) = 1 if there exists t with A on bar t and B on bar t+1

The bar may carry anything else. The full `{token_set} -> {token_set}` form is almost a
fingerprint: support collapses, and one irrelevant token flipping creates an entirely new
sequence. It stays descriptive and is not searchable in V1.

THE EPISODE IS THE UNIT

A sequence occurring three times inside one episode is one episode, not three. Occurrence
counts are kept as descriptive fields and never enter membership.

THREE FAMILIES THAT MAY NOT MIX

    T5_INTRADAY    adjacent bars inside the T5 session
    PREV_INTRADAY  adjacent bars inside the previous session
    CROSS_DAY      the seam only: PREV_LAST -> T5_FIRST, and the two 3-bar forms around it

A sequence never crosses the session boundary by accident. `PREV_H3 -> PREV_H4 -> T5_H1` is
not a thing: adjacency means adjacent observed bars inside the declared window.

SESSION VALIDITY IS THE CALENDAR, NOT `bars_in_session == 7`

An early close is a complete session with fewer bars, and discarding it would delete real
market days. The exchange's session length for a date is taken as the modal bar count across
all tickers that traded it; a ticker's session is complete when it matches that. A ticker with
2 bars on a day the market ran 7 is incomplete — a data gap, not a short session.

SUPPORT FLOORS ARE STRICTER THAN THE COMBO MINER'S, DELIBERATELY

    treated >= 300, control >= 300      (ComboLab used 100)
    tickers >= 50 both arms
    dates   >= 50 both arms
    max single-date share <= 0.10       (ComboLab used 0.20)

The observable population here is ~123k episodes, so low floors are not needed; and the
sequence universe can grow far faster than a cell manifest. A claim standing on n=103 across
27 dates, judged later on a heavy-tailed MFE, is exactly what these numbers refuse in advance.
"""
from __future__ import annotations

import hashlib
import itertools
import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_tokens_spec as TS                                       # noqa: E402
import t5_dna as D                                                   # noqa: E402

SPEC = os.path.join(D.HERE, "T5_SEQUENCE_GRAMMAR_V1.json")
OUT = os.path.join(D.HERE, "T5_SEQUENCE_UNIVERSE_V1.json")

MIN_EP, MIN_TK, MIN_DT, MAX_DATE_SHARE = 300, 50, 50, 0.10
LENGTHS = (2, 3)
FAMILIES = ("T5_INTRADAY", "PREV_INTRADAY", "CROSS_DAY")

# Umbrella tokens are unions of members already in the registry: any sequence they enter is
# implied by its members, so they would occupy claim slots without adding a question.
UMBRELLA = {"ANY_T", "ANY_Z", "ANY_L", "L5_ANY", "AD_ANY", "ANY_P", "ANY_D"}


def searchable_tokens(M: pd.DataFrame) -> pd.DataFrame:
    """SEQUENCE_TOKEN_REGISTRY_V1 — every registry token with a searchable verdict + reason."""
    sets = M.token_set.str.split()
    flat = pd.Series([t for row in sets for t in row])
    cnt = flat.value_counts()
    n = len(M)
    rows = []
    for t in TS.REGISTRY:
        c = int(cnt.get(t.token_id, 0))
        share = c / n
        if t.source != "bars":
            ok, why = False, "frame-computed; no 1H counterpart"
        elif t.token_id in UMBRELLA:
            ok, why = False, "umbrella union of registry members; its sequences are implied"
        elif c == 0:
            ok, why = False, "never true on 1H bars in this population"
        elif share > 0.99:
            ok, why = False, f"true on {share:.1%} of bars — not a distinguishing element"
        else:
            ok, why = True, ""
        rows.append(dict(token=t.token_id, family=t.family, source=t.source,
                         bar_count=c, bar_share=round(share, 6),
                         searchable_in_sequence_v1=ok, reason=why))
    return pd.DataFrame(rows)


def build():
    t0 = time.time()
    M = pd.read_parquet(D.OUT_MS, columns=[
        "episode_id", "ticker", "t5_date", "relative_day", "session_date",
        "session_position", "bars_in_session", "token_set"])
    print(f"  microstructure {len(M):,} bars · {M.episode_id.nunique():,} episodes",
          flush=True)

    # session validity: the exchange's length for a date is its modal bar count
    modal = (M.groupby(["session_date", "ticker"])["bars_in_session"].first()
             .groupby("session_date").agg(lambda s: s.mode().iloc[0]))
    M["session_expected"] = M["session_date"].map(modal)
    M["session_complete"] = M["bars_in_session"] == M["session_expected"]
    inc = int((~M.session_complete).sum())
    print(f"  session validity: expected-length from the calendar · incomplete bars "
          f"{inc:,} ({inc/len(M):.2%})", flush=True)
    M = M[M.session_complete]

    REG = searchable_tokens(M)
    toks = REG.loc[REG.searchable_in_sequence_v1, "token"].tolist()
    tix = {t: i for i, t in enumerate(toks)}
    print(f"  searchable tokens {len(toks)} of {len(TS.REGISTRY)} "
          f"(excluded {len(TS.REGISTRY)-len(toks)})", flush=True)

    # bar x token boolean matrix
    B = np.zeros((len(M), len(toks)), dtype=bool)
    for r, s in enumerate(M.token_set.to_numpy()):
        for t in s.split():
            j = tix.get(t)
            if j is not None:
                B[r, j] = True

    M = M.reset_index(drop=True)
    ep_codes, ep_uniq = pd.factorize(M.episode_id)
    # Support is an EPISODE property, so its lookups are indexed by episode — not by bar.
    # The first version tested date concentration with np.isin(ep_codes, e), which selects
    # BARS: with ~14 bars per episode the numerator was inflated ~14x against an episode
    # denominator, and the 10% date-share floor rejected almost everything. One sequence
    # survived out of 16,546 candidates, which is what made the bug visible at all.
    _first = M.drop_duplicates("episode_id").set_index("episode_id")
    ep_ticker = pd.factorize(_first.loc[ep_uniq, "ticker"])[0]
    ep_date = pd.factorize(_first.loc[ep_uniq, "t5_date"])[0]
    order = np.lexsort((M.session_position.to_numpy(),
                        (M.relative_day == "T5_DAY").to_numpy(), ep_codes))
    Ms = M.iloc[order].reset_index(drop=True)
    Bs = B[order]
    ep = ep_codes[order]
    rd = (Ms.relative_day == "T5_DAY").to_numpy()
    pos = Ms.session_position.to_numpy()

    def adjacencies(family, span):
        """Index pairs/triples of consecutive bars inside the declared window."""
        n = len(Ms)
        idx = np.arange(n - span + 1)
        ok = np.ones(len(idx), bool)
        for j in range(span):
            ok &= ep[idx + j] == ep[idx]
        if family == "T5_INTRADAY":
            for j in range(span):
                ok &= rd[idx + j]
            for j in range(span - 1):
                ok &= pos[idx + j + 1] == pos[idx + j] + 1
        elif family == "PREV_INTRADAY":
            for j in range(span):
                ok &= ~rd[idx + j]
            for j in range(span - 1):
                ok &= pos[idx + j + 1] == pos[idx + j] + 1
        else:  # CROSS_DAY — must contain exactly one PREV->T5 transition at the seam
            trans = np.zeros(len(idx), int)
            for j in range(span - 1):
                step = (~rd[idx + j]) & rd[idx + j + 1]
                same = rd[idx + j] == rd[idx + j + 1]
                trans += step
                ok &= step | (same & (pos[idx + j + 1] == pos[idx + j] + 1))
            ok &= trans == 1
        return idx[ok]

    universe = {}
    freq = {}
    CLAIMS = []
    for fam in FAMILIES:
        for L in LENGTHS:
            a = adjacencies(fam, L)
            if len(a) == 0:
                universe[f"{fam}|{L}"] = dict(adjacencies=0, syntactic=0, support_ok=0,
                                              distinct=0, aliases=0)
                continue
            cols = [Bs[a + j] for j in range(L)]
            # stage 1 — adjacency-instance counts bound the episode counts from above
            if L == 2:
                inst = (cols[0].astype(np.float32).T @ cols[1].astype(np.float32))
                cand = [(i, j) for i, j in zip(*np.nonzero(inst >= MIN_EP))]
            else:
                prev = universe.get(f"{fam}|2", {}).get("survivors", [])
                cand = []
                for (i, j) in prev:                       # apriori: extend survivors only
                    m2 = cols[0][:, i] & cols[1][:, j]
                    if m2.sum() < MIN_EP:
                        continue
                    hit = cols[2][m2].sum(0)
                    cand += [(i, j, int(k)) for k in np.nonzero(hit >= MIN_EP)[0]]
            # stage 2 — exact episode-level membership and the support floors
            surv, sig, sup, stats = [], {}, [], []
            for c in cand:
                m = cols[0][:, c[0]]
                for j in range(1, L):
                    m &= cols[j][:, c[j]]
                e = np.unique(ep[a[m]])
                if len(e) < MIN_EP:
                    continue
                ntk = len(np.unique(ep_ticker[e]))
                if ntk < MIN_TK:
                    continue
                ud, dc = np.unique(ep_date[e], return_counts=True)
                share = dc.max() / len(e)
                if len(ud) < MIN_DT or share > MAX_DATE_SHARE:
                    continue
                surv.append(c)
                sup.append(len(e))
                name = "→".join(toks[x] for x in c)
                mh = hashlib.sha256(e.tobytes()).hexdigest()[:16]
                sig.setdefault(e.tobytes(), []).append(name)
                stats.append(dict(family=fam, length=L, canonical_sequence=name,
                                  token_1=toks[c[0]], token_2=toks[c[1]],
                                  token_3=toks[c[2]] if L > 2 else None,
                                  membership_hash=mh, episode_n=int(len(e)),
                                  ticker_n=int(ntk), date_n=int(len(ud)),
                                  max_date_share=round(float(share), 6),
                                  token_idx=json.dumps(list(map(int, c)))))
            CLAIMS.extend(stats)
            aliases = sum(len(v) - 1 for v in sig.values() if len(v) > 1)
            universe[f"{fam}|{L}"] = dict(
                adjacencies=int(len(a)), syntactic=len(toks) ** L,
                stage1_candidates=len(cand), support_ok=len(surv),
                distinct=len(sig), aliases=aliases, survivors=surv,
                support_quantiles={q: int(np.quantile(sup, v)) for q, v in
                                   (("q10", .1), ("q25", .25), ("median", .5),
                                    ("q75", .75), ("q90", .9))} if sup else {})
            top = sorted(zip(sup, ["→".join(toks[x] for x in c) for c in surv]),
                         reverse=True)[:12]
            freq[f"{fam}|{L}"] = [dict(sequence=s, episodes=int(n)) for n, s in top]
            print(f"  {fam:<14} {L}-bar · adjacencies {len(a):>9,} · "
                  f"stage1 {len(cand):>7,} · support-ok {len(surv):>6,} · "
                  f"distinct {len(sig):>6,} · aliases {aliases}", flush=True)

    # single-token frequency, X-only and therefore reportable
    single = REG[REG.searchable_in_sequence_v1].nlargest(15, "bar_count")[
        ["token", "family", "bar_count", "bar_share"]].to_dict("records")

    spec = dict(
        spec_id="SEQUENCE_GRAMMAR_V1",
        x_freeze_digest=json.load(open(os.path.join(D.HERE, "T5_DNA_X_V1.json")))["freeze_digest"],
        adjacency="STRICT_ADJACENT — no WITHIN-k in V1",
        lengths=list(LENGTHS), four_bar="DEFERRED_TO_V2",
        element_semantics="atomic canonical token presence on a bar; the bar may carry others",
        bar_state_sequences="DESCRIPTIVE_ONLY — not searchable claims in V1",
        occurrence_unit="episode-level binary membership; counts are descriptive",
        families=list(FAMILIES),
        session_validity="complete per exchange calendar (modal bar count for the date), "
                         "NOT bars_in_session == 7 — early closes are valid",
        support=dict(min_episodes_treated=MIN_EP, min_episodes_control=MIN_EP,
                     min_unique_tickers=MIN_TK, min_unique_dates=MIN_DT,
                     max_single_date_share=MAX_DATE_SHARE),
        primary_future_target="MFE_10D percent",
        secondary_future_target="MFE_ATR_10D",
        other_horizons="1D/3D/5D/20D descriptive secondary, NOT equal discovery targets",
        estimand="deferred to SEQUENCE_ESTIMAND_V1",
        outcome_exposure="NOT_EXPOSED",
        excluded_tokens=REG[~REG.searchable_in_sequence_v1][
            ["token", "family", "reason"]].to_dict("records"),
        searchable_token_count=len(toks))
    spec["spec_digest"] = hashlib.sha256(
        json.dumps(spec, sort_keys=True, default=str).encode()).hexdigest()[:16]
    with open(SPEC, "w") as f:
        json.dump(spec, f, indent=2, default=str)

    # The survivor index lists are the only irreplaceable output of an 854-second run, and
    # the first version stripped them before writing — the counts survived and the claims
    # themselves did not. Persisted separately so the estimand step never re-enumerates.
    C = pd.DataFrame(CLAIMS)
    C["equivalence_class_id"] = (C["family"] + "|" + C["length"].astype(str) + "|"
                                 + C.groupby(["family", "length", "membership_hash"],
                                             sort=False).ngroup().astype(str))
    C["claim_id"] = [hashlib.sha256(f"{r.family}|{r.length}|{r.canonical_sequence}"
                                    .encode()).hexdigest()[:16] for r in C.itertuples()]
    C["syntactic_aliases"] = C.groupby("equivalence_class_id")["canonical_sequence"] \
                              .transform("size") - 1
    C["grammar_digest"] = spec["spec_digest"]
    C["x_freeze_digest"] = spec["x_freeze_digest"]
    C.to_parquet(os.path.join(D.ROOT, "data", "t5_sequence_survivors_v1.parquet"),
                 index=False)

    # REPRODUCIBILITY GATE. A frozen grammar that does not rebuild its own universe is not
    # frozen, and the estimand step must not be reached on a different 863.
    tot_d = sum(v["distinct"] for v in universe.values())
    tot_a = sum(v["aliases"] for v in universe.values())
    exp = dict(distinct=863, aliases=30, digest="ccdeb2b7a03e81d0")
    got = dict(distinct=tot_d, aliases=tot_a, digest=spec["spec_digest"])
    if got != exp:
        raise RuntimeError(f"GRAMMAR NOT REPRODUCIBLE — expected {exp}, got {got}. "
                           f"Do not proceed to the estimand: the frozen universe and the "
                           f"rebuilt one are different objects.")
    print(f"  REPRODUCIBILITY GATE PASS · distinct {tot_d} · aliases {tot_a} · "
          f"digest {spec['spec_digest']}", flush=True)
    pd.DataFrame(dict(token=toks)).to_parquet(
        os.path.join(D.ROOT, "data", "t5_sequence_tokens_v1.parquet"), index=False)
    rep = {k: {kk: vv for kk, vv in v.items() if kk != "survivors"}
           for k, v in universe.items()}
    with open(OUT, "w") as f:
        json.dump(dict(spec_digest=spec["spec_digest"], universe=rep,
                       top_sequences=freq, top_single_tokens=single,
                       seconds=round(time.time() - t0, 1)), f, indent=2, default=str)
    print(f"\n  SEQUENCE_GRAMMAR_V1 {spec['spec_digest']} · "
          f"{time.time()-t0:.0f}s · NO OUTCOME JOINED", flush=True)
    return universe, freq, single, REG


if __name__ == "__main__":
    build()
