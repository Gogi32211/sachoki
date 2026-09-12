"""Derived base builder V2 — implementation of MASSIVE_1M_DERIVED_BASE_BUILDER_SPEC_V2.

Every guard here is fail-closed and raises BuildHold rather than degrading. The two that
matter most are the ones the spec was written around:

  * a missing expected minute becomes UNOBSERVED / SOURCE_COMPLETENESS_UNPROVEN. It never
    becomes a zero-volume bar, a carried-forward candle, or STRUCTURAL_NO_TRADE. The
    capability to say "nothing traded here" does not exist, and the builder is not permitted
    to invent it.

  * the session close boundary comes from the calendar, never from the clock. On an early
    close the boundary is 13:00 and a 16:00 bar is extended hours. Hardcoding 16:00 passes an
    ordinary session and silently corrupts the early ones.

minute_states carries NO price or volume columns at all. That is structural rather than
conventional: a dataset with nowhere to put a fabricated bar cannot accidentally contain one,
and validate() rejects any row that grew such a field anyway.

Source resolution is confined to the canonical archive root by path check, so a supplemental
or diagnostic payload cannot be read even if a caller asks for it by path.
"""
from __future__ import annotations
import gzip, hashlib, json, os                                          # noqa: E402

CANON_ROOT = "/Volumes/QUANT_RESEARCH/source_data/massive/canonical_1m"
OHLCV_KEYS = {"o", "h", "l", "c", "v", "vw", "n", "open", "high", "low", "close",
              "volume", "vwap", "price"}


class BuildHold(Exception):
    """Fail-closed. Never caught internally, never downgraded to a warning."""


class DerivedBaseBuilderV2:
    def __init__(self, cohort_keys, key_of, lineage, spec_digest, elig_digest,
                 cohort_digest, calendar):
        self.cohort_keys = set(cohort_keys)
        self.key_of = dict(key_of)
        self.basis = {}
        self.lin = lineage
        self.spec_digest = spec_digest
        self.elig_digest = elig_digest
        self.cohort_digest = cohort_digest
        self.cal = calendar
        self.stats = dict(structural_no_trade_emitted=0, synthetic_rows=0,
                          supplemental_rows_consumed=0,
                          supplemental_payloads_consumed=0)

    # ---------------------------------------------------------------- source
    def _payload_path(self, day, source_root=None):
        root = source_root or CANON_ROOT
        real = os.path.realpath(root)
        if real != os.path.realpath(CANON_ROOT):
            raise BuildHold(
                f"GUARD source_path: refusing a source outside the canonical archive "
                f"({real}). Supplemental and diagnostic payloads are not admissible.")
        p = os.path.join(root, day[:4], day[5:7], f"{day}.json.gz")
        if not os.path.exists(p):
            raise BuildHold(f"GUARD source_missing: no canonical payload for {day}")
        return p

    def load(self, day, source_root=None):
        p = self._payload_path(day, source_root)
        with gzip.open(p, "rb") as f:
            raw = f.read()
        return json.loads(raw), hashlib.sha256(raw).hexdigest()

    # ------------------------------------------------------------ lattice
    def lattice(self, day):
        import pandas as pd
        try:
            row = self.cal.schedule.loc[pd.Timestamp(day)]
        except KeyError:
            raise BuildHold(f"GUARD calendar: {day} is not an XNYS session")
        o, c = row["open"].tz_convert("UTC"), row["close"].tz_convert("UTC")
        mins = pd.date_range(o, c, freq="1min", inclusive="left")
        exp = len(mins)
        if exp not in (390, 210):
            raise BuildHold(
                f"GUARD calendar_lattice: {day} yields {exp} expected regular minutes; "
                f"only 390 (normal) and 210 (early close) are contracted")
        return [int(t.value // 10 ** 6) for t in mins], int(c.value // 10 ** 6), exp

    # ------------------------------------------------------------ eligibility
    def eligibility(self, tk, day):
        s = self.lin.get(tk)
        if s is None:
            raise BuildHold(f"GUARD lineage_missing: no V6 lineage for {tk}")
        for iv in s.get("when_issued_intervals") or []:
            if iv.get("effective_from", "") <= day <= iv.get("effective_to", ""):
                return "WHEN_ISSUED_EXCLUDED"
        for iv in s["regular_way_intervals"]:
            if iv["effective_from"] <= day <= iv["effective_to"]:
                return "REGULAR_WAY_EXPECTED"
        first = min(i["effective_from"] for i in s["regular_way_intervals"])
        if day < first:
            return "NOT_YET_REGULAR_WAY"
        raise BuildHold(f"GUARD eligibility_unclassified: {tk} on {day}")

    def ticker_at_time(self, tk, day):
        for iv in self.lin[tk]["intervals"]:
            if iv["effective_from"] <= day <= iv["effective_to"]:
                return iv["massive_ticker_at_time"], iv.get("lineage_state")
        return None, None

    # ------------------------------------------------------------ build
    def build_session(self, day, tickers=None, source_root=None, payload=None):
        d, payload_sha = (payload if payload else self.load(day, source_root))
        if payload:
            payload_sha = "INJECTED_FIXTURE"
        mins, close_ms, expected = self.lattice(day)
        mins_set = set(mins)
        out = dict(security_sessions=[], minute_states=[], observed_regular_bars=[],
                   session_close_boundary_bars=[], extended_hours_bars=[])
        secs = d.get("securities") or {}
        names = sorted(tickers if tickers is not None else secs)

        for tk in names:
            key = self.key_of.get(tk)
            if key is None:
                raise BuildHold(f"GUARD identity_missing: no security_key_v1 for {tk}")
            if key not in self.cohort_keys:
                raise BuildHold(
                    f"GUARD cohort_admission: {tk} is not in the frozen V1 cohort "
                    f"(476 securities); excluded securities may not enter production")
            elig = self.eligibility(tk, day)
            tat, lstate = self.ticker_at_time(tk, day)
            rows = (secs.get(tk) or {}).get("results") or []

            if elig != "REGULAR_WAY_EXPECTED":
                if rows:
                    raise BuildHold(
                        f"GUARD ineligible_bars: {tk} on {day} is {elig} but the payload "
                        f"carries {len(rows)} bars; a non-regular-way line may not enter "
                        f"regular-way coverage")
                out["security_sessions"].append(dict(
                    security_key_v1=key, session_date=day, ticker_at_time=tat,
                    eligibility_state=elig, expected_regular_minutes=0,
                    observed_regular_minutes=0, unobserved_regular_minutes=0,
                    close_boundary_present=False, extended_hours_bar_count=0))
                continue

            obs, boundary, ext, seen = {}, [], [], set()
            # AMENDMENT_1: extended-hours rows get their own uniqueness contract. The key is
            # three-part on purpose — one research security can have two legitimate
            # simultaneous trading lines (GEHC/SOLV/VLTO each traded a WI line and a
            # regular-way line on the same session), so a (key, timestamp) key would fail
            # closed on correct data.
            ext_seen = set()
            for b in rows:
                t = int(b["t"])
                if t in mins_set:
                    if t in seen:
                        raise BuildHold(
                            f"GUARD duplicate_source_key: {tk} {day} has two bars at "
                            f"timestamp {t}")
                    seen.add(t)
                    obs[t] = b
                elif t == close_ms:
                    boundary.append(b)
                elif t < mins[0] or t > close_ms:
                    ek = (key, tat, t)
                    if ek in ext_seen:
                        raise BuildHold(
                            f"GUARD duplicate_extended_source_key: {tk} {day} has two "
                            f"extended-hours bars for source line {tat} at timestamp {t}. "
                            f"No deduplication is permitted — not keep-first, keep-last, "
                            f"aggregate or average.")
                    ext_seen.add(ek)
                    ext.append(b)
                else:
                    raise BuildHold(
                        f"GUARD unclassified_row: {tk} {day} bar at {t} matches no "
                        f"regular minute, close boundary or extended-hours window")
            if len(boundary) > 1:
                raise BuildHold(
                    f"GUARD close_boundary_duplicate: {tk} {day} has {len(boundary)} "
                    f"close-boundary bars; at most one is contracted")

            for t in mins:
                b = obs.get(t)
                st = dict(security_key_v1=key, session_date=day, minute_ts=t,
                          eligibility_state=elig,
                          observation_state="OBSERVED" if b else "UNOBSERVED",
                          source_ticker_access_state="VALID",
                          provenance_payload_sha256=payload_sha)
                if not b:
                    st["unobserved_reason"] = "SOURCE_COMPLETENESS_UNPROVEN"
                out["minute_states"].append(st)
                if b:
                    out["observed_regular_bars"].append(dict(
                        security_key_v1=key, session_date=day, minute_ts=t,
                        ticker_at_time=tat, o=b["o"], h=b["h"], l=b["l"], c=b["c"],
                        v=b["v"], n=b.get("n"),
                        provenance_payload_sha256=payload_sha))
            for b in boundary:
                out["session_close_boundary_bars"].append(dict(
                    security_key_v1=key, session_date=day, minute_ts=close_ms,
                    classification="SESSION_CLOSE_BOUNDARY_BAR",
                    counted_as_regular_minute=False,
                    asserted_auction_bar=False,
                    o=b["o"], h=b["h"], l=b["l"], c=b["c"], v=b["v"]))
            for b in ext:
                out["extended_hours_bars"].append(dict(
                    security_key_v1=key, session_date=day, minute_ts=int(b["t"]),
                    classification="EXTENDED_HOURS_BAR",
                    o=b["o"], h=b["h"], l=b["l"], c=b["c"], v=b["v"]))
            out["security_sessions"].append(dict(
                security_key_v1=key, session_date=day, ticker_at_time=tat,
                eligibility_state=elig, expected_regular_minutes=expected,
                observed_regular_minutes=len(obs),
                unobserved_regular_minutes=expected - len(obs),
                close_boundary_present=bool(boundary),
                extended_hours_bar_count=len(ext)))
        return out

    # ------------------------------------------------------------ validate
    def validate(self, out):
        """Always run. Structural guards that do not depend on how rows were produced."""
        seen = set()
        for r in out["observed_regular_bars"]:
            k = (r["security_key_v1"], r["minute_ts"])
            if k in seen:
                raise BuildHold(f"GUARD duplicate_output_key: {k} emitted twice")
            seen.add(k)
        cb = {}
        for r in out["session_close_boundary_bars"]:
            k = (r["security_key_v1"], r["session_date"])
            cb[k] = cb.get(k, 0) + 1
            if cb[k] > 1:
                raise BuildHold(f"GUARD close_boundary_duplicate: {k} has {cb[k]}")
        for r in out["minute_states"]:
            bad = OHLCV_KEYS & set(r)
            if bad:
                self.stats["synthetic_rows"] += 1
                raise BuildHold(
                    f"GUARD synthetic_ohlcv: minute_states carries market data {sorted(bad)}; "
                    f"zero-fill, carry-forward and synthetic candles are forbidden")
            if r["observation_state"] == "STRUCTURAL_NO_TRADE":
                self.stats["structural_no_trade_emitted"] += 1
                raise BuildHold(
                    "GUARD structural_no_trade: the classification capability is "
                    "UNAVAILABLE; a missing minute may not be called a no-trade minute")
            if r["observation_state"] not in ("OBSERVED", "UNOBSERVED"):
                raise BuildHold(f"GUARD observation_state: {r['observation_state']}")
            if (r["observation_state"] == "UNOBSERVED"
                    and r.get("unobserved_reason") != "SOURCE_COMPLETENESS_UNPROVEN"):
                raise BuildHold("GUARD unobserved_reason: missing or wrong reason")
        for r in out["observed_regular_bars"]:
            if "vwap" in r or "derived_vwap" in r:
                raise BuildHold("GUARD vwap: the VWAP feature is DISABLED")
        return True
