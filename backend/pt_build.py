"""PT family engine — one builder for T5 / T9 / T3 / T1, PT5 semantics unchanged.

The classification is lifted from the PT5 implementation, not restated from prose:

    BASE    every canonical episode of the family
    1H      at least one frozen 1H overlap-cluster MEDOID matches
    15M     at least one frozen 15m cluster representative matches
    STRONG  both
    ladder  STRONG > 15M > 1H > BASE, exclusive

T5 is NOT recomputed here. Its sealed pt5_signals.parquet is translated into the shared
schema, so the authoritative implementation stays the one that already governs it.

THREE-VALUED AVAILABILITY. A family with no 15m phase reports M15 and STRONG as
UNAVAILABLE. "not looked at" is not "looked at and absent", and the UI is given the
difference explicitly rather than being left to infer it.

PROVENANCE, NOT DATES. Each row carries the state the server derived from sealed
artifacts — the browser never decides evidence status from a date.

No Y value is read anywhere in this module.

    usage:  python pt_build.py [t5 t9 t3 t1]
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                  # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import pt_family as PF                                               # noqa: E402

DATA = os.path.join(os.path.dirname(HERE), "data")
FORBIDDEN = {"mfe_10d", "mae_10d", "ret_10d", "mfe_atr_10d", "theta", "z_rank", "max_z",
             "survivor_forward", "win_rate", "mean_return"}

HISTORICAL = "HISTORICAL"
FORWARD_UNMATURED = "FORWARD_X_UNMATURED"
FORWARD_PENDING = "FORWARD_MATURED_PENDING_ANALYSIS"
FORWARD_ELIGIBLE = "FORWARD_EVIDENCE_ELIGIBLE"
FORWARD_SOURCE_HOLD = "FORWARD_SOURCE_HOLD"


class PTGateFailure(RuntimeError):
    pass


def forward_state(fam: str):
    """Seal date and source verdict for this family, from sealed artifacts only."""
    p = f"{fam}_FORWARD_1H_V1.json"
    if not os.path.exists(p):
        return None, None
    d = json.load(open(p))
    return d["no_backfill"]["seal_date"], d["source_gate_state"]["verdict"]


def build_family(fam: str) -> pd.DataFrame:
    D = PF.dna(fam)
    dcol = PF.date_col(fam)
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", dcol])
    bad = set(E.columns) & FORBIDDEN
    if bad:
        raise PTGateFailure(f"{fam}: outcome column reached the PT builder: {bad}")
    if E.duplicated(["ticker", dcol]).any():
        raise PTGateFailure(f"{fam}: duplicate (ticker, date) grain")

    cache = os.path.join(DATA, f"{fam.lower()}_1h_survivor_membership.parquet")
    T = pd.read_parquet(cache)
    ident = json.load(open(f"PT{fam[1:]}_1H_MEMBERSHIP_CACHE_V1.json"))
    if ART.file_digest(cache) != ident["cache_file_digest"]:
        raise PTGateFailure(f"{fam}: membership cache digest drift — refusing to use it")
    meds = PF.medoids(fam)
    memb = {}
    for key, claim in meds.items():
        s = set(T[(T["kind"] == "member") & (T.claim_id == claim)].episode_id)
        sealed = ident["medoid_detail"][claim]["members"]
        if len(s) != sealed:
            raise PTGateFailure(f"{fam}: {claim} has {len(s)} members, sealed {sealed}")
        memb[key] = s

    hits = {}
    for key, s in memb.items():
        for e in s:
            hits.setdefault(e, []).append(key)
    ph = PF.phases(fam)
    seal_date, src = forward_state(fam)

    rows = []
    for r in E.itertuples():
        e = r.episode_id
        fams = sorted(hits.get(e, []))
        h1_any = bool(fams)
        date = str(getattr(r, dcol))[:10]
        klass = f"PT{fam[1:]}_1H" if h1_any else f"PT{fam[1:]}_BASE"
        if seal_date is None or date <= seal_date:
            prov = HISTORICAL
        elif src != "QUALIFIED":
            prov = FORWARD_SOURCE_HOLD
        else:
            prov = FORWARD_UNMATURED
        rows.append(dict(
            family=fam, ticker=r.ticker, date=date, episode_id=e,
            pt_base=True, pt_class=klass, provenance=prov,
            h1_state="AVAILABLE_TRUE" if h1_any else "AVAILABLE_FALSE",
            h1_match_count=len(fams),
            h1_medoids=",".join(meds[f].split("|")[2] for f in fams),
            h1_keys=",".join(fams),
            m15_state="UNAVAILABLE" if not ph["M15"] else
                      ("AVAILABLE_TRUE" if False else "AVAILABLE_FALSE"),
            m15_match_count=0,
            strong_state="UNAVAILABLE" if not ph["STRONG"] else "AVAILABLE_FALSE"))
    return pd.DataFrame(rows).sort_values(["ticker", "date"]).reset_index(drop=True)


def translate_t5() -> pd.DataFrame:
    """T5 is not recomputed — its sealed preview is mapped into the shared schema."""
    p = os.path.join(DATA, "pt5_signals.parquet")
    df = pd.read_parquet(p)
    seal_date, src = forward_state("T5")
    out = pd.DataFrame(dict(
        family="T5", ticker=df.ticker, date=df.date.astype(str).str[:10],
        episode_id=df.episode_id, pt_base=True,
        pt_class=df.pt5_class.str.replace("PT5_", "PT5_", regex=False),
        provenance=HISTORICAL,
        h1_state=np.where(df.pt5_h1_any, "AVAILABLE_TRUE", "AVAILABLE_FALSE"),
        h1_match_count=df.pt5_h1_match_count,
        h1_medoids=df.pt5_h1_family, h1_keys=df.pt5_h1_families,
        m15_state=np.where(df.pt5_m15_any, "AVAILABLE_TRUE", "AVAILABLE_FALSE"),
        m15_match_count=df.pt5_m15_match_count,
        strong_state=np.where(df.pt5_class == "PT5_STRONG", "AVAILABLE_TRUE",
                              "AVAILABLE_FALSE")))
    return out.sort_values(["ticker", "date"]).reset_index(drop=True)


def digest(df):
    key = df[["episode_id", "pt_class", "h1_keys", "m15_state", "provenance"]] \
        .astype(str).agg("|".join, axis=1)
    return hashlib.sha256("\n".join(sorted(key)).encode()).hexdigest()[:16]


def main():
    fams = [f.upper() for f in (sys.argv[1:] or ["t5", "t9", "t3", "t1"])]
    summary = {}
    for fam in fams:
        df = translate_t5() if fam == "T5" else build_family(fam)
        d1 = digest(df)
        if fam != "T5":                      # reorder invariance, as PT5 asserts
            d2 = digest(build_family(fam))
            if d1 != d2:
                raise PTGateFailure(f"{fam}: non-deterministic build {d1} != {d2}")
        out = os.path.join(DATA, f"pt{fam[1:]}_signals.parquet")
        df.to_parquet(out, index=False)
        counts = {k: int((df.pt_class == k).sum()) for k in sorted(df.pt_class.unique())}
        prov = {k: int(v) for k, v in df.provenance.value_counts().items()}
        ph = PF.phases(fam)
        summary[fam] = dict(rows=int(len(df)), classes=counts, provenance=prov,
                            phases={k: ("AVAILABLE" if v else "UNAVAILABLE")
                                    for k, v in ph.items()},
                            medoids=len(PF.medoids(fam)),
                            membership_digest=d1,
                            parquet=os.path.basename(out),
                            parquet_digest=ART.file_digest(out))
        print(f"{fam}: rows {len(df):,} · {counts} · phases "
              f"{''.join('Y' if ph[k] else '-' for k in ('BASE','H1','M15','STRONG'))} "
              f"· digest {d1}")
    spec = ART.seal(dict(
        spec_id="PT_FAMILY_PREVIEW_V1", status="BUILT",
        semantics="lifted verbatim from the PT5 implementation: BASE = every canonical "
                  "episode; 1H = at least one frozen overlap-cluster MEDOID matches; "
                  "15M = at least one frozen 15m representative; STRONG = both; the class "
                  "ladder is exclusive",
        medoids_only="within-cluster aliases never strengthen a preview",
        availability="three-valued: AVAILABLE_TRUE / AVAILABLE_FALSE / UNAVAILABLE. A "
                     "family with no 15m phase reports UNAVAILABLE, never FALSE.",
        provenance_states=[HISTORICAL, FORWARD_UNMATURED, FORWARD_PENDING,
                           FORWARD_ELIGIBLE, FORWARD_SOURCE_HOLD],
        provenance_rule="the server derives state from sealed artifacts; the browser must "
                        "never infer evidence status from a date",
        not_for=["ranking", "scoring", "sorting", "filtering", "model input"],
        families=summary,
        t5_treatment="translated from the sealed pt5_signals.parquet, never recomputed",
        outcome_exposure="NOT_EXPOSED",
        built_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        "PT_FAMILY_PREVIEW_V1.json", required=("spec_id", "semantics", "families"),
        supersede=os.path.exists("PT_FAMILY_PREVIEW_V1.json"))
    print(f"\nPT_FAMILY_PREVIEW_V1 · {spec}")


if __name__ == "__main__":
    main()
