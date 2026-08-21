"""Finalize one forward session. Runs after the settle deadline; no-ops if the session is not ready.

TWO IDENTICAL LOCAL DIGESTS DO NOT PROVE THE VENDOR IS FINAL

The chronology makes this concrete:

    08-21 close        ~00:00 Tbilisi
    nightly ingest      03:00
    settle deadline     06:00

If nothing re-reads the source between 03:00 and 06:00, then "the local store is unchanged at
03:00 and at 06:00" is close to a tautology — it proves the local store was not written, not
that the vendor issued no correction. The stability check must therefore straddle a FRESH READ:

    close -> ingest -> digest_A -> wait to last_bar_ts + SETTLE -> REFRESH SOURCE -> digest_B

    digest_A == digest_B  ->  SOURCE_FINAL
    digest_A != digest_B  ->  the new snapshot BECOMES the reference and the stability window
                              restarts from it. No membership evaluation until it settles.

The existing 03:00 job cannot satisfy the proxy alone: it runs three hours before the deadline.
That is why finality is a separate step rather than a side effect of the nightly.

NOTHING HERE DECIDES ANYTHING

Every branch is a frozen rule. A session is either accepted or HELD with a named DQ code; no
human chooses whether a given 2026-08-21 episode counts.
"""
from __future__ import annotations
import json, os, subprocess, sys                                       # noqa: E402
import pandas as pd                                                    # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402
import t5_forward_activate as ACT, t5_forward_producer as PR           # noqa: E402
import t5_forward_session_length as SL, t5_forward_snapshot as SNAP    # noqa: E402

SETTLE_HOURS = 6
STATE = os.path.join(D.ROOT, "data", "t5_forward_session_state.parquet")


class NotReady(RuntimeError):
    pass


def source_snapshot(session_date):
    """Read the session's bars from the store RIGHT NOW and digest them. Called twice, with a
    genuine refresh in between — that is the whole point."""
    # session_position / bars_in_session are NOT columns — the producer derives them with a
    # window over (ticker, session_date). This mirrors that derivation exactly rather than
    # inventing a second one, because a finalizer that computes session length differently from
    # the producer is worse than one that does not compute it at all.
    import duckdb
    c = duckdb.connect(D.DB1H, read_only=True)
    try:
        df = c.execute(
            """
            SELECT ticker, date, open, high, low, close, volume,
                   row_number() OVER (PARTITION BY ticker, CAST(date AS DATE)
                                      ORDER BY date)               session_position,
                   count(*)     OVER (PARTITION BY ticker, CAST(date AS DATE))
                                                                   bars_in_session
            FROM bars WHERE CAST(date AS DATE) = ? ORDER BY ticker, date
            """, [session_date]).fetchdf()
    finally:
        c.close()
    if not len(df):
        return None, 0, None
    import hashlib
    dig = hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()[:16]
    return dig, int(len(df)), str(df.date.max())


def refresh_source(session_date, verbose=True):
    """The FRESH READ the proxy depends on. Without it the second digest is a re-read of an
    untouched file and proves nothing about the vendor."""
    if verbose:
        print(f"  refreshing source for {session_date} …", flush=True)
    r = subprocess.run([".venv/bin/python", "update_intraday_db.py", "--tf", "1h",
                        "--workers", "8"], cwd=HERE, capture_output=True, text=True,
                       timeout=3600)
    return dict(returncode=r.returncode, tail=r.stdout[-400:] if r.stdout else r.stderr[-400:])


def read_state():
    return pd.read_parquet(STATE) if os.path.exists(STATE) else pd.DataFrame()


def write_state(row):
    T = read_state()
    T = T[T.session_date != row["session_date"]] if len(T) else T
    T = pd.concat([T, pd.DataFrame([row])], ignore_index=True)
    T.to_parquet(STATE, index=False)
    return T


def finalize(session_date, now_ts, do_refresh=True, dry_run=True, verbose=True):
    """1 settle · 2 refresh · 3 completeness · 4 stability · 5 commit · 6 publish · 7 accrue."""
    ACT.startup_audit(verbose=False)
    pin = ACT.assert_runtime_pinned()
    out = dict(session_date=session_date, now=str(now_ts), producer_commit=pin["pin"])

    # ── 1 settle deadline ─────────────────────────────────────────────────
    dig_a, n_a, last_bar = source_snapshot(session_date)
    if dig_a is None:
        out.update(status="HELD", dq="SESSION_NOT_INGESTED")
        return write_state(out) and out
    deadline = pd.Timestamp(last_bar) + pd.Timedelta(hours=SETTLE_HOURS)
    out.update(last_bar_ts=str(last_bar), settle_deadline=str(deadline),
               digest_a=dig_a, rows_a=n_a)
    if pd.Timestamp(now_ts) < deadline:
        out.update(status="HELD", dq="SETTLE_WINDOW_OPEN",
                   note=f"settle ends {deadline}; no-op, not a failure")
        if verbose:
            print(f"  {session_date} · settle window open until {deadline} · no-op")
        write_state(out)
        return out

    # ── 2 fresh source refresh ────────────────────────────────────────────
    out["refresh"] = refresh_source(session_date, verbose) if do_refresh else \
        dict(returncode=None, tail="SKIPPED — digest_b would be a re-read, not a refresh")
    if not do_refresh:
        out.update(status="HELD", dq="NO_FRESH_SOURCE_READ",
                   why="two digests without a refresh in between prove the local store was not "
                       "written, not that the vendor issued no correction")
        write_state(out)
        return out

    # ── 3 completeness + 4 digest stability ───────────────────────────────
    dig_b, n_b, last_b = source_snapshot(session_date)
    out.update(digest_b=dig_b, rows_b=n_b, last_bar_after_refresh=str(last_b))
    if dig_a != dig_b:
        out.update(status="HELD", dq="SOURCE_STILL_MOVING",
                   why="the refresh changed the session; the NEW snapshot becomes the reference "
                       "and the stability window restarts from it. No membership evaluation "
                       "until it settles.",
                   stability_window_restarts_from=dig_b)
        if verbose:
            print(f"  {session_date} · source moved {dig_a} -> {dig_b} · HELD, window restarts")
        write_state(out)
        return out
    out["source_final"] = True

    # ── 5 session length commit ───────────────────────────────────────────
    import duckdb
    c = duckdb.connect(D.DB1H, read_only=True)
    try:
        rows = c.execute(
            "SELECT ticker, count(*) bars_in_session FROM bars "
            "WHERE CAST(date AS DATE) = ? GROUP BY ticker", [session_date]).fetchdf()
    finally:
        c.close()
    min_t, min_s = SL.floors()
    L, meta = SL.establish(rows, min_t, min_s)
    out.update(session_length=L, **{f"sl_{k}": v for k, v in meta.items()})
    if L is None:
        out.update(status="HELD", dq="SESSION_LENGTH_NOT_ESTABLISHED")
        if verbose:
            print(f"  {session_date} · {meta} · HELD")
        write_state(out)
        return out
    if not dry_run:
        SL.commit(session_date, L, dict(n_tickers=meta["n_tickers"],
                                        modal_share=meta["modal_share"],
                                        source_digest=dig_b, data_version="FORWARD",
                                        origin="FORWARD"))
    out["session_length_committed"] = not dry_run

    # ── 6 publish · 7 accrue are deliberately NOT auto-run here ───────────
    out.update(status="READY_FOR_SNAPSHOT" if not dry_run else "READY_DRY_RUN",
               next_steps=["pinned producer rebuild", "snapshot publication (directory rename)",
                           "startup audit", "frozen evaluator", "occurrence append",
                           "FORWARD_ACCRUAL_STARTED once"])
    if verbose:
        print(f"  {session_date} · SOURCE_FINAL · length {L} "
              f"(n_tickers {meta['n_tickers']}, modal_share {meta['modal_share']:.3f}) "
              f"· {out['status']}")
    write_state(out)
    return out


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("session_date")
    ap.add_argument("--now", default=None)
    ap.add_argument("--no-refresh", action="store_true")
    ap.add_argument("--commit", action="store_true", help="not a dry run")
    a = ap.parse_args()
    now = a.now or str(pd.Timestamp.now())
    r = finalize(a.session_date, now, do_refresh=not a.no_refresh, dry_run=not a.commit)
    print(json.dumps({k: v for k, v in r.items() if k != "refresh"}, indent=2, default=str))


if __name__ == "__main__":
    main()
