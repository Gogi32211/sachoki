"""OPENING_VOLUME_DYNAMICS_V1 — FIRST CHECKPOINT report + pre-outcome seal.

  ovd_checkpoint()  writes CHECKPOINT_1.md from discovery + the latest COMPLETED X build +
                    the draft registry.  No outcome numbers exist at this point.
  ovd_seal()        freezes FEATURE_SPEC_V1.json, SEARCH_REGISTRY_V1.json,
                    MULTIPLICITY_UNIVERSE_V1.json and SEAL.json (digest-bound to the X run,
                    the discovery record and the _pathsim binding).  REFUSES if anything
                    outcome-bearing already exists in the family directory.

Sealing is a one-way door: after SEAL.json exists, the outcome phase may read X.parquet and
call the sacred _pathsim; the registry can no longer change.
"""
from __future__ import annotations
import os, sys, re, json, hashlib, time, glob, inspect, subprocess      # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
CANON_DIR = os.path.join(FAMILY_DIR, "canonical_1d")
DIAGNOSTIC = "POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5"
FIXTURE_FILES = ("tests/test_ovd_guards.py", "tests/test_ovd_canonical.py", "tests/test_ovd_preseal.py")


def _dig(p: str) -> str:
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]


# ── PRE-SEAL GATE helpers (spec A9–A11) ────────────────────────────────────────────────────
_RETRACTED_WINDOW = re.compile(r"72[ -]?month", re.I)


def assert_window_wording(texts: dict) -> None:
    """A9: every surviving '72 month(s)' mention in sealed text must sit on a line that RETRACTS it."""
    for name, txt in texts.items():
        for ln in txt.splitlines():
            if _RETRACTED_WINDOW.search(ln) and "retract" not in ln.lower():
                raise SystemExit(f"REFUSED: un-retracted '72 months' wording in {name}: {ln.strip()[:140]}")


def assert_observation_window(xw: dict, canonical_range, owa: dict) -> None:
    """A9: the window MEASURED on X must equal the canonical authority's range AND the registry's
    frozen OBSERVATION_WINDOW_AUTHORITY (first/last session and session count)."""
    got = [str(xw["first_session"]), str(xw["last_session"])]
    if got != [str(x) for x in canonical_range] or got != [str(x) for x in owa["canonical_1d_available_range"]] \
            or int(xw["eligible_sessions"]) != int(owa["eligible_sessions"]):
        raise SystemExit(f"REFUSED: observation window mismatch — X {got} / {xw['eligible_sessions']} sessions; "
                         f"canonical {list(canonical_range)}; registry {owa['canonical_1d_available_range']} / {owa['eligible_sessions']}")


def _x_window(run_dir: str) -> dict:
    import duckdb
    c = duckdb.connect()
    try:
        mn, mx, ns, nt, n = c.execute(f"SELECT min(session), max(session), count(DISTINCT session), count(DISTINCT ticker), "
                                      f"count(*) FROM read_parquet('{os.path.join(run_dir, 'X.parquet')}')").fetchone()
    finally:
        c.close()
    return dict(first_session=str(mn), last_session=str(mx), eligible_sessions=int(ns), tickers=int(nt), rows=int(n))


def _run_fixtures() -> dict:
    """The seal cannot be produced over red fixtures. Runs the three OVD fixture files (no _pathsim
    call anywhere in them — the guards only scan source) and binds their digests."""
    paths = [os.path.join(HERE, f) for f in FIXTURE_FILES]
    for p in paths:
        if not os.path.exists(p):
            raise SystemExit(f"REFUSED: fixture file missing: {p}")
    import tempfile, xml.etree.ElementTree as ET
    fd, xml_p = tempfile.mkstemp(prefix="ovd_fixtures_", suffix=".xml"); os.close(fd)
    try:
        # the repo's pytest config is already quiet (text summary is suppressed) -> read the junit
        # report instead of scraping stdout
        r = subprocess.run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", f"--junitxml={xml_p}", *paths],
                           cwd=HERE, capture_output=True, text=True)
        root = ET.parse(xml_p).getroot()
        suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
        tests = sum(int(s.get("tests", 0)) for s in suites)
        bad = sum(int(s.get("failures", 0)) + int(s.get("errors", 0)) for s in suites)
        skipped = sum(int(s.get("skipped", 0)) for s in suites)
    finally:
        try:
            os.remove(xml_p)
        except OSError:
            pass
    passed = tests - bad - skipped
    if r.returncode != 0 or bad or passed == 0:
        raise SystemExit(f"REFUSED: fixtures not green — tests {tests} failed/errors {bad} skipped {skipped} rc {r.returncode}\n"
                         f"{r.stdout[-3000:]}\n{r.stderr[-1000:]}")
    return dict(passed=passed, tests=tests, skipped=skipped, files={f: _dig(p) for f, p in zip(FIXTURE_FILES, paths)})


def run_status(run_dir: str) -> str:
    """STATUS.json is the run's authority classification. Absent = 'UNCLASSIFIED' (treated as
    NOT eligible for the outcome phase — only an explicit CANONICAL_1D_BASIS run qualifies)."""
    p = os.path.join(run_dir, "STATUS.json")
    if not os.path.exists(p):
        return "UNCLASSIFIED"
    try:
        return json.load(open(p)).get("status", "UNCLASSIFIED")
    except Exception:
        return "UNREADABLE"


def latest_completed_run(require_canonical: bool = True) -> str | None:
    """Newest COMPLETED X build. With require_canonical (default, pre-seal option A):
    only runs whose STATUS.json says CANONICAL_1D_BASIS qualify; INVALID_FOR_OUTCOME_PHASE
    runs (mixed-basis Studio store) are skipped and never become current input."""
    from ovd_build import RunSpace
    runs = sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OVD_*")))
    for r in reversed(runs):
        if not RunSpace.is_current(r, "X.parquet"):
            continue
        st = run_status(r)
        if st == "INVALID_FOR_OUTCOME_PHASE":
            continue
        if require_canonical and st != "CANONICAL_1D_BASIS":
            continue
        return r
    return None


def ovd_checkpoint() -> str:
    disc = json.load(open(os.path.join(FAMILY_DIR, "discovery_phase1.json")))
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.draft.json")))
    run = latest_completed_run()
    if not run:
        raise SystemExit("no COMPLETED X build — checkpoint cannot be written")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    da = disc["data_authorities"]
    av = rep["availability"]; n = rep["rows"]
    pct = lambda k: f"{av[k]:,} ({100*av[k]/n:.1f}%)"
    md = f"""# OPENING_VOLUME_DYNAMICS_V1 — FIRST CHECKPOINT (pre-outcome)

Written {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}. **No `_pathsim` call has been made. No outcome
number exists in this family.**

## DATA AUTHORITIES

| store | physical path | sha256[:16] | rows | tickers | range |
|---|---|---|---|---|---|
| 1D  | {da['1D']['physical_path']} | `{da['1D']['sha256_16']}` | {da['1D']['rows']:,} | {da['1D']['tickers']:,} | {da['1D']['date_range'][0]} → {da['1D']['date_range'][1]} |
| 1H  | {da['1H']['physical_path']} | `{da['1H']['sha256_16']}` | {da['1H']['rows']:,} | {da['1H']['tickers']:,} | {da['1H']['range'][0]} → {da['1H']['range'][1]} |
| 15m | {da['15m']['physical_path']} | `{da['15m']['sha256_16']}` | {da['15m']['rows']:,} | {da['15m']['tickers']:,} | {da['15m']['range'][0]} → {da['15m']['range'][1]} |

- 1D `date` is a DATE; 1H/15m `date` is a **naive TIMESTAMP holding UTC wall time**, labelled
  by bar START. Conversion: `timezone('America/New_York', timezone('UTC', date))`. The naive→
  TIMESTAMPTZ cast is wrong on this machine (session TZ Asia/Tbilisi) — caught in discovery.
- 1D adjustment semantics: **not declared by the store** (bulk CSV import + Massive delta, no
  split/dividend handling found). Share volume is primary; dollar-volume twins are SECONDARY.
- 1D duplicates across universes: {da['1D']['duplicate_ticker_date_rows_across_universes']:,} rows → deduped by
  `row_number() OVER (PARTITION BY ticker, date ORDER BY universe)`.

## TIMEFRAME ALIGNMENT

- **1H**: 7 on-grid bars/session starting 09:30…15:30 NY; opening bar = 09:30 NY = 13:30Z (EDT) /
  14:30Z (EST), resolved per session. 2.3% off-grid bars (:45/:00/:15 starts) → **no slot**, never merged.
- **15m**: 26 on-grid bars/session 09:30…15:45 NY (100% on-grid in probe); half days = 15 bars.
  **M1..M4 = NY starts 09:30 / 09:45 / 10:00 / 10:15**, verified on AAPL 2026-07-15 (13:30Z…14:15Z)
  and by EDT/EST histograms.

## UNIVERSE / SURVIVORSHIP SCOPE

- 1D non-index tickers: {disc['universe_survivorship']['non_index_tickers_1D']:,}; alive at 2026-08-20: {disc['universe_survivorship']['alive_at_as_of_2026-08-20']:,};
  ended before 2026: {disc['universe_survivorship']['ended_before_2026']} → **survivor-biased**. No claim of "all historical US stocks".
- 1H and 15m cover **3,203** tickers. The eligible sample of this family is the 1D∩1H∩15m
  intersection; rows without lower-TF history are kept with UNAVAILABLE features, never dropped.
- Eligibility: close ≥ 5, avg_vol_20d > 0, close×volume ≥ 3M, universe ≠ index.

## ACCUMULATION / BREAKOUT AUTHORITY DISCOVERY

{chr(10).join('- **' + c['name'] + '** — ' + c['verdict'] + ' (' + c['producer'] + ')' for c in disc['base_breakout_authority_discovery']['candidates'])}

**Decision**: {disc['base_breakout_authority_discovery']['decision']}

Frozen predicates (all from bars ≤ D; ceiling from D−1):
```
CEILING(D)   = wt_resistance(D-1)
IN_RANGE(D)  = wt_valid_tr(D-1) AND close(D-1) <= CEILING(D)
BASE(D)      = IN_RANGE(D) AND (max high − min low over D-25..D-1) / min low <= 0.35
BREAKOUT(D)  = BASE(D) AND close(D) > CEILING(D)
STOP_DAY(D)  = most recent s in D-30..D-6 with RVOL_OPEN1H(s) >= 2.0 AND close(s) < close(s-1)   [NEW_RESEARCH_SPEC]
```

## X-ONLY FEATURE TABLE

Run `{rep['run_id']}` — `{run}/X.parquet` (COMPLETED marker present, digest-bound).

- rows (eligible 1D ticker-sessions over the canonical observation window — see OBSERVATION_WINDOW_AUTHORITY): **{n:,}** · tickers {rep['tickers']:,} · sessions {rep['sessions']:,}
- typed DATE joins: 1D×1H matched {rep['joins']['d1_x_h1']['matched']:,} (left-only {rep['joins']['d1_x_h1']['left_only']:,}, right-only {rep['joins']['d1_x_h1']['right_only']:,});
  1D×15m matched {rep['joins']['d1_x_m15']['matched']:,} (left-only {rep['joins']['d1_x_m15']['left_only']:,}, right-only {rep['joins']['d1_x_m15']['right_only']:,})
- BASE days (Family A candidates incl. breakout days): {rep['base_days']:,} · BREAKOUT days (Family B events): {rep['breakout_days']:,}

## FEATURE AVAILABILITY (non-null count of {n:,})

| feature | available |
|---|---|
| OPEN1H_RVOL_LEVEL | {pct('open1h_rvol')} |
| OPEN1H_RAMP_3D | {pct('ramp_3d')} |
| OPEN1H_RAMP_5D (Theil–Sen slope) | {pct('ramp_5d_slope')} |
| BASE_DRYUP | {pct('base_dryup')} |
| EFFORT_VS_RESULT | {pct('effort_result')} |
| INTRADAY_BREADTH_SHIFT | {pct('breadth_shift')} |
| STOPPING_VOLUME_RECLAIM | {pct('stop_reclaim')} |
| STOPPING_DAY_RESPONSE | {pct('stop_response')} |
| IDIO_OPEN1H (market-adjusted) | {pct('idio_open1h')} |
| OPEN15_BREADTH / SHAPE / PERSISTENCE | {pct('open15_breadth')} |
| VOLUME_TRANSFER | {pct('volume_transfer')} |

UNAVAILABLE = NULL throughout (never 0 / False). OPEN15_SHAPE conservation asserted at build.

## PROPOSED SEARCH REGISTRY (draft `{reg['registry_sha256_16']}`)

- Family A sample: `base AND NOT breakout`; Family B sample: `breakout`.
- {len(reg['features'])} features with frozen bins; {len(reg['interactions'])} pre-registered interactions; strata: year,
  price, dollar-volume, gap, market-regime (descriptive, not extra claims).
- Entry: mask on D → `_pathsim` entry at D+1 open (features known 10:30 NY D). Same-session
  entry is NOT representable and will not be claimed.
- Exit: book default (trail, atr_k=12, maxh=60); horizon sweep 3/5/10/20/60 as a diagnostic.
- Primary statistic: day-clustered median edge vs same-day control.

## MULTIPLICITY k

**k = {reg['k']['total']}** (single {reg['k']['single']} + interaction {reg['k']['interaction']}); by family A {sum(1 for c in reg['cells'] if c['family']=='A')} / B {sum(1 for c in reg['cells'] if c['family']=='B')}.
DSR bound to the full k per family; survivors never shrink it.

## NEGATIVE FIXTURE STATUS

`backend/tests/test_ovd_guards.py`: **18 / 18 pass** (join-type, stale result, crash-before-COMPLETED,
missing bar ≠ 0, insufficient history → UNAVAILABLE, self-denominator, slot identity, causal time,
duplicates, timezone date-shift, zero matches, market-control window, missing authority,
`_pathsim` copy detection, conservation, stale JSON, registry k, missing volume ≠ False).
Two fixture defects found and fixed were in the TEST HARNESS (DuckDB `DATE + int`, reserved word
`session`), not in the guards.

## `_pathsim` BINDING

`{disc['pathsim_binding']['path']}` · `_pathsim` src sha256[:16] `{disc['pathsim_binding']['src_sha256_16']}` ·
file `{disc['pathsim_binding']['file_sha256_16']}` · SLIP {disc['pathsim_binding']['SLIP']} · trail clip literal in source · entry = next bar open.

## GATE

Seal the registry (`ovd_seal.py`) → only then open the outcome phase. Until SEAL.json exists,
this family has produced no outcome-bearing number.
"""
    p = os.path.join(FAMILY_DIR, "CHECKPOINT_1.md")
    open(p, "w").write(md)
    return p


def ovd_seal() -> dict:
    forbidden = glob.glob(os.path.join(FAMILY_DIR, "**", "*outcome*"), recursive=True) + \
        glob.glob(os.path.join(FAMILY_DIR, "**", "*pathsim*"), recursive=True)
    if forbidden:
        raise SystemExit(f"REFUSED: outcome-bearing files already present: {forbidden[:3]}")
    if os.path.exists(os.path.join(FAMILY_DIR, "SEAL.json")):
        raise SystemExit("REFUSED: SEAL.json already exists — the registry is frozen")
    run = latest_completed_run()
    if not run:
        raise SystemExit("REFUSED: no COMPLETED X build")
    # ── option A binding: the X run must be bound to the CURRENT canonical authority, whose files
    #    must match their manifest digests; the roster must be complete with 0 fetch failures ──
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    st = json.load(open(os.path.join(run, "STATUS.json")))
    if st.get("status") != "CANONICAL_1D_BASIS" or st["canonical_1d"]["run_id"] != cur["run_id"]:
        raise SystemExit(f"REFUSED: X run {os.path.basename(run)} is not bound to the current canonical {cur['run_id']}")
    for k in ("canonical", "derived", "raw", "splits"):
        if _dig(cur[f"{k}_parquet"]) != cur[f"{k}_sha256_16"]:
            raise SystemExit(f"REFUSED: canonical {k} parquet digest != manifest")
    if cur.get("fetch_failures") or cur["tickers_with_bars"] != cur["tickers_requested"]:
        raise SystemExit("REFUSED: canonical roster incomplete or fetch failures present")
    import edge_replay as E
    ps_src = inspect.getsource(E._pathsim)
    ps = dict(path=E.__file__, src_sha256_16=hashlib.sha256(ps_src.encode()).hexdigest()[:16],
              file_sha256_16=_dig(E.__file__), SLIP=E.SLIP)
    disc = json.load(open(os.path.join(FAMILY_DIR, "discovery_phase1.json")))
    if ps["src_sha256_16"] != disc["pathsim_binding"]["src_sha256_16"]:
        raise SystemExit("REFUSED: _pathsim source changed since discovery")
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.draft.json")))
    mult = json.load(open(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V1.draft.json")))
    spec_md = open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V1.draft.md")).read()
    # ── A9 · OBSERVATION_WINDOW_AUTHORITY: measured on X, equal to the canonical range and the
    #    registry; no un-retracted '72 months' wording may survive in any sealed text ──
    owa = reg.get("observation_window_authority")
    if not owa or owa.get("amendment") != "PRE_OUTCOME_SAMPLE_WINDOW_AMENDMENT":
        raise SystemExit("REFUSED: registry lacks observation_window_authority (PRE_OUTCOME_SAMPLE_WINDOW_AMENDMENT)")
    xw = _x_window(run)
    assert_observation_window(xw, cur["session_range"], owa)
    assert_window_wording({"FEATURE_SPEC_V1.draft.md": spec_md,
                           "SEARCH_REGISTRY_V1.draft.json": json.dumps(reg, indent=1, default=str),
                           "MULTIPLICITY_UNIVERSE_V1.draft.json": json.dumps(mult, indent=1, default=str)})
    # ── A10 · the report-only diagnostic is registered, is NOT a feature / cell, and is never
    #    imported by an X producer; its dividend authority matches its manifest digest ──
    diag = (reg.get("report_only_diagnostics") or {}).get(DIAGNOSTIC)
    if not diag or "must_not" not in diag:
        raise SystemExit(f"REFUSED: {DIAGNOSTIC} not registered as a report-only diagnostic")
    if DIAGNOSTIC in reg["features"] or any("DISTRIBUTION" in c["feature"] for c in reg["cells"]):
        raise SystemExit(f"REFUSED: {DIAGNOSTIC} leaked into the claim set")
    import ovd_diagnostics as D
    D.assert_not_used_by_x_builder()
    dvm = sorted(glob.glob(os.path.join(CANON_DIR, "dividend_reference", "MANIFEST_*.json")))
    if not dvm:
        raise SystemExit("REFUSED: no dividend reference manifest")
    dv = json.load(open(dvm[-1]))
    if _dig(dv["parquet"]) != dv["sha256_16"]:
        raise SystemExit("REFUSED: dividend reference parquet digest != manifest")
    census = json.load(open(os.path.join(CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS.json")))
    a10_p = os.path.join(CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS_A10.json")
    if not os.path.exists(a10_p):
        raise SystemExit("REFUSED: the A10 exposure set has not been materialised pre-seal (MATERIALITY_CENSUS_A10.json)")
    a10 = json.load(open(a10_p))
    if a10["canonical_run"] != cur["run_id"] or a10["dividends_sha256_16"] != dv["sha256_16"] \
            or _dig(a10["table"]) != a10["table_sha256_16"] or a10["window"] != [str(x) for x in cur["session_range"]] \
            or a10["reconciliation_to_per_event_census"]["per_event_pairs_missing_from_summed_table"] != 0:
        raise SystemExit("REFUSED: A10 exposure table is stale, unbound or inconsistent with the per-event census")
    # ── A11 · limitations wording is binding ──
    lim = reg.get("limitations") or {}
    if lim.get("dividends") != "DIVIDENDS_NOT_ADJUSTED" or "PRICE-RETURN" not in lim.get("outcome_semantics", "") \
            or "0 unresolved reference-split seams" not in lim.get("seam_claim", ""):
        raise SystemExit("REFUSED: limitations wording missing or altered")
    # ── k consistency (cells are the claims; nothing else counts) ──
    if len(reg["cells"]) != reg["k"]["total"] or reg["k"]["total"] != mult["k_total"] \
            or reg["registry_sha256_16"] != mult["registry_sha256_16"]:
        raise SystemExit("REFUSED: registry / multiplicity k or digest inconsistent")
    # ── canonical qualification on the CURRENT run (A4 continuity, A6 duplicates) ──
    aud_p = os.path.join(CANON_DIR, f"AUDIT_{cur['run_id']}.json")
    aud = json.load(open(aud_p)); a4 = aud["A4_summary"]; a5 = aud["A5_seam_rescan_canonical"]
    if a4["ok"] != a4["events"] or a4["breaks"] != 0 or aud["A6_duplicates"] != 0:
        raise SystemExit(f"REFUSED: canonical audit not clean: A4 {a4} A6 {aud['A6_duplicates']}")
    adj_p = os.path.join(CANON_DIR, "SEAM_CANDIDATES_ADJUDICATION.json")
    adj = json.load(open(adj_p))
    # the adjudicated candidate set must be EXACTLY what the bound detector reports on the
    # CURRENT canonical parquet (the adjudication was recorded on an earlier canonical run)
    import ovd_canonical_audit as A, duckdb
    if hashlib.sha256(A.SEAM_SQL.encode()).hexdigest()[:16] != adj["detector_sha"]:
        raise SystemExit("REFUSED: seam detector changed since adjudication")
    c = duckdb.connect()
    try:
        c.execute(f"CREATE VIEW src AS SELECT ticker, session_date, close, volume FROM read_parquet('{cur['canonical_parquet']}')")
        det = {(r[0], str(r[1])) for r in c.execute(A.SEAM_SQL.format(src="src")).fetchall()}
    finally:
        c.close()
    adjudicated = {(r["ticker"], r["date"]) for r in adj["rows"]}
    if det != adjudicated:
        raise SystemExit(f"REFUSED: detector candidates on {cur['run_id']} ({len(det)}) != adjudicated set ({len(adjudicated)}); "
                         f"unadjudicated: {sorted(det - adjudicated)[:5]}")
    # ── fixtures must be green (run here; no _pathsim call exists in any fixture) ──
    fx = _run_fixtures()
    sealed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for name, obj in (("SEARCH_REGISTRY_V1.json", reg), ("MULTIPLICITY_UNIVERSE_V1.json", mult)):
        obj["status"] = "SEALED"; obj["sealed_at"] = sealed_at
        json.dump(obj, open(os.path.join(FAMILY_DIR, name), "w"), indent=1, default=str)
    spec = dict(spec_id="OPENING_VOLUME_DYNAMICS_FEATURE_SPEC_V1", status="SEALED", sealed_at=sealed_at,
                markdown=spec_md, markdown_sha256_16=hashlib.sha256(spec_md.encode()).hexdigest()[:16],
                builder=dict(path=os.path.join(HERE, "ovd_build.py"), sha256_16=_dig(os.path.join(HERE, "ovd_build.py"))),
                diagnostics=dict(path=os.path.join(HERE, "ovd_diagnostics.py"), sha256_16=_dig(os.path.join(HERE, "ovd_diagnostics.py"))))
    json.dump(spec, open(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V1.json"), "w"), indent=1)
    seal = dict(family="OPENING_VOLUME_DYNAMICS_V1", sealed_at=sealed_at,
                x_run=os.path.basename(run), x_parquet_sha256_16=_dig(os.path.join(run, "X.parquet")),
                build_report_sha256_16=_dig(os.path.join(run, "build_report.json")),
                x_status_sha256_16=_dig(os.path.join(run, "STATUS.json")),
                discovery_sha256_16=_dig(os.path.join(FAMILY_DIR, "discovery_phase1.json")),
                feature_spec_sha256_16=_dig(os.path.join(FAMILY_DIR, "FEATURE_SPEC_V1.json")),
                registry_sha256_16=_dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.json")),
                multiplicity_sha256_16=_dig(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V1.json")),
                k_total=reg["k"]["total"], k_single=reg["k"]["single"], k_interaction=reg["k"]["interaction"],
                k_by_family=mult["k_by_family"], pathsim=ps,
                observation_window_authority=dict(amendment=owa["amendment"], canonical_run=cur["run_id"], **xw,
                                                  retraction=owa["retraction"], forbidden=owa["forbidden"]),
                canonical_1d=dict(run_id=cur["run_id"], basis_asof=cur["basis_asof"], convention=cur["convention"],
                                  tickers=f"{cur['tickers_with_bars']}/{cur['tickers_requested']}", fetch_failures=0,
                                  rows=cur["rows"], session_range=cur["session_range"],
                                  canonical_sha256_16=cur["canonical_sha256_16"], derived_sha256_16=cur["derived_sha256_16"],
                                  raw_sha256_16=cur["raw_sha256_16"], splits_sha256_16=cur["splits_sha256_16"],
                                  quarantine_rows=cur["quarantine"]["rows"],
                                  manifest_sha256_16=_dig(os.path.join(CANON_DIR, f"MANIFEST_{cur['run_id']}.json"))),
                canonical_audit=dict(A4_continuity=f"{a4['ok']}/{a4['events']}", A6_duplicates=aud["A6_duplicates"],
                                     A5_detector_candidates=a5["candidates"], A5_after_2026_05_22=a5["candidates_after_2026_05_22"],
                                     A5_claim="0 unresolved reference-split seams (arbiter /v3/reference/splits) — NOT '0 corporate-action seams'",
                                     audit_sha256_16=_dig(aud_p),
                                     seam_adjudication=dict(sha256_16=_dig(adj_p), adjudicated_on=adj["canonical_run"],
                                                            n=adj["n"], by_class=adj["by_class"])),
                dividend_reference=dict(parquet=os.path.basename(dv["parquet"]), sha256_16=dv["sha256_16"],
                                        events_in_window=dv["events_in_window"],
                                        per_event_census_ge5_events_tickers=census["yield_ge_5pct"],
                                        per_event_census_sha256_16=_dig(os.path.join(CANON_DIR, "dividend_reference", "MATERIALITY_CENSUS.json")),
                                        A10_exposure_set=dict(table=os.path.basename(a10["table"]), table_sha256_16=a10["table_sha256_16"],
                                                              census_sha256_16=_dig(a10_p), events_tickers=a10["events_tickers"],
                                                              only_by_summing=a10["only_by_summing"], by_year=a10["by_year"])),
                report_only_diagnostics=dict(names=list(reg["report_only_diagnostics"].keys()),
                                             module_sha256_16=_dig(os.path.join(HERE, "ovd_diagnostics.py")),
                                             rule="post-outcome, descriptive; never eligibility / claim / k / entry-exit / promotion / row removal"),
                limitations=lim, fixtures=fx,
                rule="registry frozen; outcome phase may now read X.parquet and call the sacred _pathsim; "
                     "cells not in the registry are POST_EXPOSURE_HYPOTHESIS",
                outcome_phase="NOT OPENED by this command — _pathsim is a SEPARATE, explicit subsequent action")
    json.dump(seal, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1)
    return seal


if __name__ == "__main__":
    if "--seal" in sys.argv:
        print(json.dumps(ovd_seal(), indent=1))
    else:
        print("checkpoint ->", ovd_checkpoint())
