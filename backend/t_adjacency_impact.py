"""ADJACENCY_IMPACT_REPORT_V1 — X and membership level only, for T5/T9/T3/T1.

Applies BOTH registered corrections and measures what moves:

    1  SESSION VALIDITY   the registered rule is the modal bar count across ALL TICKERS
                          that traded the date (built once from the 1H store), not the
                          family-local modal the implementation used
    2  ADJACENCY          adjacency over observed bars inside a session that is complete
                          under (1); acceptance invariant D verifies that no admitted
                          window spans a missing scheduled slot

Then, per family: grammar names/classes/aliases, class digest, per-claim support and
membership sets (Jaccard old vs corrected), support-threshold crossings, merges/splits,
membership cells added/removed, and — where anything moved — the corrected k.

NO outcome, Z, theta or survivor is computed anywhere.

    usage:  python t_adjacency_impact.py t5 t9 t3 t1
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                        # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402

OUT = "ADJACENCY_IMPACT_REPORT_V1.json"
CAL_PARQ = "/Users/sachoki/Desktop/sachoki-desktop/data/session_calendar_1h.parquet"
PART = "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/" \
       "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/impact_%s.json"


def calendar():
    C = pd.read_parquet(CAL_PARQ)
    return dict(zip(C.session_date, C.calendar_len.astype(int))), ART.file_digest(CAL_PARQ)


def family(fam, cal):
    D = importlib.import_module(f"{fam}_dna")
    G = importlib.import_module(f"{fam}_sequence_grammar")
    dcol = f"{fam}_date"

    # ── 1 · session validity, old rule vs registered rule ────────────────
    M = pd.read_parquet(D.OUT_MS, columns=["episode_id", "ticker", dcol, "relative_day",
                                           "session_date", "session_position",
                                           "bars_in_session", "ts_et", "token_set"])
    per = (M.groupby(["session_date", "ticker", "episode_id", "relative_day"])
             .bars_in_session.first().reset_index())
    fam_modal = (per.groupby(["session_date", "ticker"]).bars_in_session.first()
                    .groupby("session_date").agg(lambda s: s.mode().iloc[0]))
    per["fam_modal"] = per.session_date.map(fam_modal)
    per["cal_len"] = per.session_date.map(cal)
    dates = per[["session_date", "fam_modal", "cal_len"]].drop_duplicates("session_date")
    old_valid = per.bars_in_session == per.fam_modal
    new_valid = per.bars_in_session == per.cal_len
    flipped = per[old_valid ^ new_valid]
    validity = dict(
        dates_examined=int(len(dates)),
        dates_where_family_modal_differs_from_calendar=int((dates.fam_modal
                                                            != dates.cal_len).sum()),
        differing_dates=dates.loc[dates.fam_modal != dates.cal_len,
                                  "session_date"].tolist()[:20],
        sessions=int(len(per)),
        old_valid=int(old_valid.sum()), corrected_valid=int(new_valid.sum()),
        old_valid_to_corrected_invalid=int((old_valid & ~new_valid).sum()),
        old_invalid_to_corrected_valid=int((~old_valid & new_valid).sum()),
        episodes_affected=int(flipped.episode_id.nunique()),
        affected_examples=flipped.head(5)[["session_date", "ticker", "episode_id",
                                           "bars_in_session", "fam_modal",
                                           "cal_len"]].to_dict("records"))

    # ── 2 · corrected input frame: complete under the REGISTERED rule ────
    keep = set(zip(flipped.episode_id, flipped.relative_day))  # noqa: F841 (documented)
    ok = per[new_valid][["episode_id", "relative_day"]]
    Mc = M.merge(ok, on=["episode_id", "relative_day"], how="inner")
    old_ok = per[old_valid][["episode_id", "relative_day"]]
    Mo = M.merge(old_ok, on=["episode_id", "relative_day"], how="inner")
    same_rows = (len(Mo) == len(Mc)
                 and hashlib.sha256(pd.util.hash_pandas_object(
                     Mo.sort_values(["episode_id", "relative_day", "session_position"])
                       .reset_index(drop=True), index=False).values.tobytes()).hexdigest()
                 == hashlib.sha256(pd.util.hash_pandas_object(
                     Mc.sort_values(["episode_id", "relative_day", "session_position"])
                       .reset_index(drop=True), index=False).values.tobytes()).hexdigest())

    # invariant D on the corrected frame
    F = Mc.sort_values(["episode_id", "relative_day", "session_position"],
                       kind="stable").reset_index(drop=True)
    t = pd.to_datetime(F.ts_et)
    slot = np.floor((t.dt.hour * 60 + t.dt.minute - 570) / 60).astype(int).to_numpy()
    same = ((F.episode_id.values[1:] == F.episode_id.values[:-1])
            & (F.relative_day.values[1:] == F.relative_day.values[:-1]))
    posadj = F.session_position.values[1:] == F.session_position.values[:-1] + 1
    pair = same & posadj
    invD = int((pair & ((slot[1:] - slot[:-1]) > 1)).sum())

    # ── 3 · enumeration: sealed vs corrected ─────────────────────────────
    if not (hasattr(G, "SURV") and hasattr(G, "enumerate_grammar")):
        # T5's grammar is a monolithic build() with no reusable entry point. Its identity
        # rests on the INPUT: if the frame the sealed run consumed is bit-identical to the
        # corrected frame, the sealed output is the corrected output.
        return dict(family=fam.upper(), session_validity=validity,
                    corrected_input=dict(rows_old=int(len(Mo)), rows_corrected=int(len(Mc)),
                                         input_frames_bit_identical=bool(same_rows),
                                         invariant_D_admitted_gap_spanning_windows=invD),
                    grammar=dict(note="no reusable enumerate_grammar entry point in this "
                                      "module; identity established at the input frame"),
                    branch=("A — evidence inputs identical" if (same_rows and invD == 0)
                            else "B — evidence inputs moved"))
    C_old = pd.read_parquet(G.SURV)
    d_old = G.class_digest(C_old)
    if same_rows:
        # the corrected frame is bit-identical to what the sealed run consumed, so the
        # enumeration cannot differ; proved by input identity, then confirmed by digest
        u_new, C_new, _, occ_new = G.enumerate_grammar(Mc.drop(columns=["ts_et"]),
                                                       verbose=False, collect_occ=True)
    else:
        u_new, C_new, _, occ_new = G.enumerate_grammar(Mc.drop(columns=["ts_et"]),
                                                       verbose=False, collect_occ=True)
    d_new = G.class_digest(C_new)
    key = lambda C: C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence
    K_old, K_new = set(key(C_old)), set(key(C_new))
    sup_old = C_old.set_index(key(C_old)).episode_n
    sup_new = C_new.set_index(key(C_new)).episode_n
    mh_old = C_old.set_index(key(C_old)).membership_hash
    mh_new = C_new.set_index(key(C_new)).membership_hash
    common = sorted(K_old & K_new)
    hash_same = [c for c in common if mh_old[c] == mh_new[c]]
    hash_diff = [c for c in common if mh_old[c] != mh_new[c]]

    jac = {}
    cells_added = cells_removed = 0
    if hash_diff:
        _, _, _, occ_old = G.enumerate_grammar(Mo.drop(columns=["ts_et"]),
                                               verbose=False, collect_occ=True)
        so = occ_old.groupby("claim").episode_id.apply(set)
        sn = occ_new.groupby("claim").episode_id.apply(set)
        vals = []
        for c in hash_diff:
            a, b = so.get(c, set()), sn.get(c, set())
            vals.append(len(a & b) / len(a | b) if (a | b) else 1.0)
            cells_removed += len(a - b); cells_added += len(b - a)
        v = np.array(vals)
        jac = dict(n=len(v), min=round(float(v.min()), 4),
                   q10=round(float(np.quantile(v, .1)), 4),
                   median=round(float(np.median(v)), 4),
                   q90=round(float(np.quantile(v, .9)), 4),
                   max=round(float(v.max()), 4))

    delta = (sup_new[common] - sup_old[common])
    body = dict(
        family=fam.upper(),
        session_validity=validity,
        corrected_input=dict(rows_old=int(len(Mo)), rows_corrected=int(len(Mc)),
                             input_frames_bit_identical=bool(same_rows),
                             invariant_D_admitted_gap_spanning_windows=invD),
        grammar=dict(
            names_old=int(len(C_old)), names_corrected=int(len(C_new)),
            classes_old=int(C_old.equivalence_class_id.nunique()),
            classes_corrected=int(C_new.equivalence_class_id.nunique()),
            aliases_old=int(len(C_old) - C_old.equivalence_class_id.nunique()),
            aliases_corrected=int(len(C_new) - C_new.equivalence_class_id.nunique()),
            class_digest_old=d_old, class_digest_corrected=d_new,
            digest_identical=bool(d_old == d_new),
            names_only_old=sorted(K_old - K_new)[:10],
            n_names_only_old=len(K_old - K_new),
            names_only_corrected=sorted(K_new - K_old)[:10],
            n_names_only_corrected=len(K_new - K_old),
            common=len(common),
            membership_unchanged=len(hash_same),
            membership_changed=len(hash_diff),
            support_changed=int((delta != 0).sum()),
            support_delta_min=int(delta.min()) if len(delta) else 0,
            support_delta_max=int(delta.max()) if len(delta) else 0,
            membership_cells_added=cells_added,
            membership_cells_removed=cells_removed,
            jaccard_on_changed_claims=jac),
        branch=("A — evidence inputs identical" if (d_old == d_new and not hash_diff
                                                    and not (K_old ^ K_new))
                else "B — evidence inputs moved"))
    json.dump(body, open(PART % fam, "w"), indent=1)
    return body


def main():
    fams = sys.argv[1:] or ["t5", "t9", "t3", "t1"]
    cal, cal_digest = calendar()
    t0 = time.time()
    out = {}
    for f in fams:
        print(f"── {f.upper()} ─────────────────────────", flush=True)
        b = family(f, cal)
        out[f.upper()] = b
        v, g = b["session_validity"], b["grammar"]
        if "names_old" not in g:
            print(f"  validity: differing dates "
                  f"{v['dates_where_family_modal_differs_from_calendar']} · input identical:"
                  f" {b['corrected_input']['input_frames_bit_identical']} · invariant D "
                  f"{b['corrected_input']['invariant_D_admitted_gap_spanning_windows']} · "
                  f"{b['branch']}", flush=True)
            continue
        print(f"  validity: differing dates {v['dates_where_family_modal_differs_from_calendar']}"
              f" · flips {v['old_valid_to_corrected_invalid']}/"
              f"{v['old_invalid_to_corrected_valid']} · episodes {v['episodes_affected']}",
              flush=True)
        print(f"  input identical: {b['corrected_input']['input_frames_bit_identical']} · "
              f"invariant D: {b['corrected_input']['invariant_D_admitted_gap_spanning_windows']}",
              flush=True)
        print(f"  grammar: names {g['names_old']}->{g['names_corrected']} · classes "
              f"{g['classes_old']}->{g['classes_corrected']} · digest {g['digest_identical']}"
              f" · membership changed {g['membership_changed']} · {b['branch']}", flush=True)

    if len(fams) == 4:
        d = ART.seal(dict(spec_id="ADJACENCY_IMPACT_REPORT_V1", status="X_ONLY",
                          remediation_spec=ART.file_digest("ADJACENCY_REMEDIATION_V1.json"),
                          session_validity_amendment=ART.file_digest(
                              "SESSION_VALIDITY_REMEDIATION_AMENDMENT_V1.json"),
                          calendar_source="session_calendar_1h.parquet · " + cal_digest,
                          calendar_rule="modal bar count across ALL TICKERS that traded the "
                                        "date, from the 1H store — the registered rule",
                          families=out,
                          outcome_exposure="NOT_EXPOSED — no outcome, Z, theta or survivor "
                                           "computed anywhere in this report",
                          runtime_min=round((time.time() - t0) / 60, 1)),
                     OUT, required=("spec_id", "families"),
                     supersede=os.path.exists(OUT))
        print(f"\nADJACENCY_IMPACT_REPORT_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
