"""Canonical 1m derived BASE layer builder — five separate datasets, no synthetic bars.

The central architectural choice is that an absent expected minute never becomes a row that
looks like a market bar. Expected coverage lives in `minute_states`; actual market data
lives in `observed_regular_bars`. Nothing joins them into a single table where a missing
minute wears OHLCV columns full of nulls or zeros, because that shape is what invites a
downstream `fillna(0)` two months later.

    security_sessions           one row per in-scope (security, session)
    minute_states               expected regular-minute lattice + observation state
    observed_regular_bars       ONLY emitted vendor bars on expected regular slots
    session_close_boundary_bars the optional close-stamped bar, kept apart
    extended_hours_bars         everything outside the RTH lattice

STRUCTURAL_NO_TRADE IS NEVER EMITTED. The semantics amendment defines the state but records
its classification capability as UNAVAILABLE, because request-level completeness (proven)
is not minute-emission completeness (not proven). So absent expected minutes are UNOBSERVED
with reason SOURCE_COMPLETENESS_UNPROVEN, and emitting a single STRUCTURAL_NO_TRADE row
fails the build rather than quietly improving the coverage statistics.

IDENTITY IS security_key_v1 AND NOTHING ELSE. Ticker is metadata. `identity_basis` travels
with every row because 474 keys rest on a share-class identifier and 29 rest on a CIK being
a singleton in this universe — different evidentiary strength wearing the same shape.

STAGING, VALIDATION, THEN ATOMIC FINALISATION. A partition becomes canonical only after its
own checks pass. An interrupted run leaves a staging directory that can never be mistaken
for finalized output.

    usage:  python derived_base_builder.py smoke        scratch-only, non-canonical
            python derived_base_builder.py build [N]    production (requires spec seal)
"""
from __future__ import annotations
import gzip, hashlib, json, os, shutil, sys, time                        # noqa: E402
from datetime import datetime, timezone, timedelta                       # noqa: E402
from zoneinfo import ZoneInfo                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                # noqa: E402

ET = ZoneInfo("America/New_York")
VOLUME = "/Volumes/QUANT_RESEARCH"
RAW = f"{VOLUME}/source_data/massive/canonical_1m"
OUT = f"{VOLUME}/derived/massive_1m_base_v1"
SCRATCH = f"{VOLUME}/scratch/temp/massive_1m_base_smoke"
LINEAGE = "MASSIVE_TICKER_LINEAGE_V6.json"
IDENTITY = "MASSIVE_SECURITY_IDENTITY_KEY_AMENDMENT_V1.json"
SEMANTICS = "MASSIVE_1M_DERIVED_SEMANTICS_AMENDMENT_V1.json"
BOUNDARY = "MASSIVE_SOURCE_TICKER_BOUNDARY_AMENDMENT_V1.json"
SPEC = "MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V1.json"

DATASETS = ["security_sessions", "minute_states", "observed_regular_bars",
            "session_close_boundary_bars", "extended_hours_bars"]
BUILDER_VERSION = "derived_base_builder/1"


def builder_code_hash() -> str:
    return hashlib.sha256(open(__file__, "rb").read()).hexdigest()


class BuildHold(RuntimeError):
    pass


# ── frozen inputs ──────────────────────────────────────────────────────────────
def load_contracts():
    lin = json.load(open(LINEAGE))
    ident = json.load(open(IDENTITY))
    sem = json.load(open(SEMANTICS))
    bnd = json.load(open(BOUNDARY))
    for name, art, want in (("lineage", lin, "FROZEN"),
                            ("identity", ident, "FROZEN"),
                            ("semantics", sem, "DERIVED_SEMANTICS_FROZEN"),
                            ("boundary", bnd, "FROZEN")):
        st = art.get("status")
        if st != want:
            raise BuildHold(f"{name} artifact status is {st!r}, expected {want!r}")
    keys = {r["frozen_current_ticker"]: r for r in ident["mapping"]}
    secs = {s["current_ticker"]: s for s in lin["securities"]}
    boundary_cases = {(c["security"], c["xnys_session"]) for c in bnd["cases"]}
    return lin, ident, sem, bnd, keys, secs, boundary_cases


def lattice(session):
    import exchange_calendars as xc
    import pandas as pd
    cal = xc.get_calendar("XNYS")
    ts = pd.Timestamp(session)
    if not cal.is_session(ts):
        return [], None, None
    o = cal.session_open(ts).tz_convert(ET)
    c = cal.session_close(ts).tz_convert(ET)
    slots, t = [], o
    while t < c:
        slots.append(int(t.timestamp() * 1000))
        t += timedelta(minutes=1)
    return slots, o, c


def et_hhmm(ms):
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).astimezone(
        ET).strftime("%H:%M")


def interval_for(sec, day):
    for iv in sec.get("regular_way_intervals") or []:
        if (iv.get("effective_from") or "0000") <= day <= (iv.get("effective_to")
                                                           or "9999"):
            return iv
    return None


# ── one session ────────────────────────────────────────────────────────────────
def build_session(day, ctx):
    lin, ident, sem, bnd, keys, secs, boundary_cases = ctx
    man_p = os.path.join(RAW, "_manifest", f"{day}.json")
    if not os.path.exists(man_p):
        raise BuildHold(f"missing raw manifest for {day}")
    man = json.load(open(man_p))
    payload_p = os.path.join(RAW, man["payload"])
    raw_bytes = open(payload_p, "rb").read()
    if hashlib.sha256(raw_bytes).hexdigest() != man["sha256"]:
        raise BuildHold(f"raw payload digest mismatch for {day}")
    blob = json.loads(gzip.decompress(raw_bytes))

    slots, s_open, s_close = lattice(day)
    if not slots:
        raise BuildHold(f"{day} is not an XNYS session")
    slotset = set(slots)
    close_ms = int(s_close.timestamp() * 1000)

    sessions, minutes, observed, boundary_rows, extended = [], [], [], [], []
    unclassified, dup_keys = [], []

    for ticker, key in keys.items():
        sec = secs[ticker]
        iv = interval_for(sec, day)
        raw_state = (man.get("states") or {}).get(ticker)
        entry = (blob.get("securities") or {}).get(ticker) or {}
        rows = entry.get("results") or []
        src_ticker = entry.get("massive_ticker")

        if iv is None:
            elig = ("WHEN_ISSUED_EXCLUDED"
                    if any(i["lineage_state"] == "WHEN_ISSUED"
                           for i in (sec.get("intervals") or [])
                           if (i.get("effective_from") or "0000") <= day
                           <= (i.get("effective_to") or "9999"))
                    else "NOT_YET_REGULAR_WAY")
        else:
            elig = "REGULAR_WAY_EXPECTED"

        access = "VALID"
        if (ticker, day) in boundary_cases:
            access = "INVALID_OR_CONFLICTED"

        base = dict(security_key_v1=key["security_key_v1"],
                    identity_basis=key["identity_basis"],
                    cik_normalized=key["cik_normalized"],
                    share_class_figi_normalized=key["share_class_figi_normalized"],
                    frozen_current_ticker=ticker, session_date=day)

        sessions.append(dict(**base, eligibility_state=elig,
                             source_ticker_access_state=access,
                             source_ticker_used_in_original_archive=src_ticker,
                             raw_ingest_pair_state=raw_state,
                             expected_regular_minute_count=(len(slots)
                                                            if elig ==
                                                            "REGULAR_WAY_EXPECTED" else 0),
                             xnys_open_utc=int(s_open.timestamp() * 1000),
                             xnys_close_utc=close_ms,
                             raw_payload_sha256=man["sha256"],
                             raw_manifest=os.path.basename(man_p)))

        seen = {}
        for b in rows:
            t = b.get("t")
            if t in slotset:
                scope = "REGULAR_SESSION_MINUTE"
            elif t == close_ms:
                scope = "SESSION_CLOSE_BOUNDARY_BAR"
            else:
                scope = "EXTENDED_HOURS_BAR"
            rec = dict(**base, bar_start_ms_utc=t, bar_scope=scope,
                       open=b.get("o"), high=b.get("h"), low=b.get("l"),
                       close=b.get("c"), volume=b.get("v"),
                       source_ticker=src_ticker, raw_payload_sha256=man["sha256"])
            if scope == "REGULAR_SESSION_MINUTE":
                if elig != "REGULAR_WAY_EXPECTED":
                    unclassified.append(dict(ticker=ticker, t=t,
                                             reason="regular slot but not REGULAR_WAY"))
                    continue
                if t in seen:
                    dup_keys.append(dict(ticker=ticker, t=t))
                    continue
                seen[t] = True
                observed.append(rec)
            elif scope == "SESSION_CLOSE_BOUNDARY_BAR":
                boundary_rows.append(rec)
            else:
                extended.append(rec)

        if elig == "REGULAR_WAY_EXPECTED":
            for t in slots:
                if t in seen:
                    obs, reason = "OBSERVED", None
                    if access != "VALID":
                        obs, reason = "UNOBSERVED", "SOURCE_REFERENCE_CONFLICT"
                elif access != "VALID":
                    obs, reason = "UNOBSERVED", "SOURCE_REFERENCE_CONFLICT"
                else:
                    obs, reason = "UNOBSERVED", "SOURCE_COMPLETENESS_UNPROVEN"
                minutes.append(dict(**base, bar_start_ms_utc=t,
                                    eligibility_state=elig, observation_state=obs,
                                    unobserved_reason=reason,
                                    source_ticker_access_state=access,
                                    observed_bar_present=t in seen))

    if unclassified:
        raise BuildHold(f"{day}: unclassified source rows {unclassified[:3]}")
    if dup_keys:
        raise BuildHold(f"{day}: duplicate regular source keys {dup_keys[:3]}")
    if any(m["observation_state"] == "STRUCTURAL_NO_TRADE" for m in minutes):
        raise BuildHold(f"{day}: STRUCTURAL_NO_TRADE emitted — capability is UNAVAILABLE")
    bc = {}
    for r in boundary_rows:
        bc[r["security_key_v1"]] = bc.get(r["security_key_v1"], 0) + 1
    if any(v > 1 for v in bc.values()):
        raise BuildHold(f"{day}: more than one close-boundary bar for a security")

    return dict(security_sessions=sessions, minute_states=minutes,
                observed_regular_bars=observed,
                session_close_boundary_bars=boundary_rows,
                extended_hours_bars=extended), man


def write_partition(day, tables, man, root, spec_digest):
    import pandas as pd
    stage = os.path.join(root, "_staging", day)
    final = os.path.join(root, day[:4], day)
    if os.path.exists(stage):
        shutil.rmtree(stage)
    os.makedirs(stage, exist_ok=True)
    digests, counts = {}, {}
    for name in DATASETS:
        df = pd.DataFrame(tables[name])
        p = os.path.join(stage, f"{name}.parquet")
        df.to_parquet(p, index=False, compression="zstd")
        digests[name] = hashlib.sha256(open(p, "rb").read()).hexdigest()
        counts[name] = len(df)
    pman = dict(session=day, finalized=True, counts=counts, digests=digests,
                input_raw_payload_sha256=man["sha256"],
                builder_version=BUILDER_VERSION, builder_code_hash=builder_code_hash(),
                spec_digest=spec_digest,
                written_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    with open(os.path.join(stage, "_partition_manifest.json"), "w") as f:
        json.dump(pman, f, separators=(",", ":"))
    os.makedirs(os.path.dirname(final), exist_ok=True)
    if os.path.exists(final):
        shutil.rmtree(final)
    os.rename(stage, final)                 # atomic finalisation
    return pman


def partition_ok(day, root, spec_digest):
    final = os.path.join(root, day[:4], day)
    mp = os.path.join(final, "_partition_manifest.json")
    if not os.path.exists(mp):
        return False
    m = json.load(open(mp))
    if not m.get("finalized") or m.get("spec_digest") != spec_digest:
        return False
    if m.get("builder_code_hash") != builder_code_hash():
        return False
    for name, dg in (m.get("digests") or {}).items():
        p = os.path.join(final, f"{name}.parquet")
        if not os.path.exists(p) or hashlib.sha256(open(p, "rb").read()).hexdigest() != dg:
            return False
    return True


def run(mode, sessions, root, spec_digest):
    from studio.mount_guard import require_external_volume
    require_external_volume(purpose=f"derived base {mode}")
    ctx = load_contracts()
    os.makedirs(root, exist_ok=True)
    tot = {k: 0 for k in DATASETS}
    obs_states = {}
    t0 = time.time()
    for i, day in enumerate(sessions):
        if mode == "build" and partition_ok(day, root, spec_digest):
            continue
        tables, man = build_session(day, ctx)
        write_partition(day, tables, man, root, spec_digest)
        for k in DATASETS:
            tot[k] += len(tables[k])
        for m in tables["minute_states"]:
            obs_states[m["observation_state"]] = obs_states.get(
                m["observation_state"], 0) + 1
        if i % 25 == 0:
            print(f"    {i}/{len(sessions)} {day}  {time.time()-t0:.0f}s", flush=True)
    return tot, obs_states, round(time.time() - t0, 1)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "smoke"
    spec_digest = ART.file_digest(SPEC) if os.path.exists(SPEC) else "UNSEALED"
    if mode == "build" and spec_digest == "UNSEALED":
        raise SystemExit("production build requires a sealed builder spec")
    if mode == "smoke":
        days = json.load(open(SPEC))["smoke_plan"]["sessions"] \
            if os.path.exists(SPEC) else []
        root = SCRATCH
    else:
        import exchange_calendars as xc
        import pandas as pd
        cal = xc.get_calendar("XNYS")
        days = [str(d.date()) for d in cal.sessions_in_range(
            pd.Timestamp("2021-08-25"), pd.Timestamp("2026-08-24"))]
        if len(sys.argv) > 2:
            days = days[:int(sys.argv[2])]
        root = OUT
    tot, states, el = run(mode, days, root, spec_digest)
    print(f"  {mode}: {len(days)} sessions · {el}s")
    for k in DATASETS:
        print(f"    {k:28s} {tot[k]:>12,}")
    print(f"    observation states: {states}")


if __name__ == "__main__":
    main()
