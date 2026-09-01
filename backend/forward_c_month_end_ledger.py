"""C — append-only month-end NO_LOOK / LOOK decision ledger.

Consumes a CURRENT B census. Never recomputes X. Never opens Y.

DIGEST CONVENTION — LEDGER_RECORD_DIGEST_V1
    digest_payload = canonical JSON of the complete record EXCLUDING record_digest
                     (UTF-8, sort_keys=True, separators=(',',':'), no pretty-print dependence)
    record_digest  = SHA-256(digest_payload)
A record can never contain its own hash, so the hash is taken over the record MINUS that
field. The same convention applies to the genesis record.

DECISION SEMANTICS vs RECORD IDENTITY
    DECISION SEMANTICS is reproducible from frozen inputs as-of effective_month_end.
    RECORD IDENTITY additionally includes executed_at, so it identifies THIS ledger event.
    Re-running the runner may recompute the same decision, but it may NOT append a second
    record for an effective_month_end that already has one.

Import-side-effect free.
"""
from __future__ import annotations
import os, json, hashlib, datetime as _dt                              # noqa: E402

SCHEMA_VERSION = "C_LEDGER_V1"
DIGEST_CONVENTION = "LEDGER_RECORD_DIGEST_V1"
ROLE_DRY_RUN = "DRY_RUN_NON_EVIDENTIARY"
ROLE_PRODUCTION = "PRODUCTION_EVIDENTIARY"
CONDITION_A_MONTHS = 6
BACKSTOP_MONTHS = 12
REQUIRED = {"H1_VOL_BUCKET": 230, "H2_GAP_V": 87, "H3_VOL_CONSTRUCT": 31,
            "H4_LSIG_M1_STAR": 24}
REGISTERED = {"H1_VOL_BUCKET": 460, "H2_GAP_V": 173, "H3_VOL_CONSTRUCT": 61,
              "H4_LSIG_M1_STAR": 48}


class CHold(Exception):
    """Fail-closed. Never downgraded."""


# ── digest convention ───────────────────────────────────────────────────────
def canon_payload(rec):
    r = {k: v for k, v in rec.items() if k != "record_digest"}
    return json.dumps(r, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def record_digest(rec):
    return hashlib.sha256(canon_payload(rec)).hexdigest()


def seal_record(rec):
    if "record_digest" in rec:
        raise CHold("GUARD digest_self_reference: record already carries record_digest")
    rec = dict(rec)
    rec["record_digest"] = record_digest(rec)
    return rec


# ── calendar / maturity ─────────────────────────────────────────────────────
def month_end(d):
    """Calendar month-end (America/New_York calendar date), holiday or not."""
    y, m = d.year, d.month
    nxt = _dt.date(y + (m == 12), 1 if m == 12 else m + 1, 1)
    return nxt - _dt.timedelta(days=1)


def complete_eligible_months(evidentiary_start, effective_month_end, maturity_days,
                             sessions):
    """A calendar month counts iff (1) it lies WHOLLY after the evidentiary start, and
    (2) by effective_month_end its final scheduled session has reached maturity.

    A SOURCE_HOLD does not stop calendar time — it only removes evidence from the B
    support counts. This is a calendar/maturity criterion, never a data-completeness one.
    """
    if isinstance(evidentiary_start, str):
        evidentiary_start = _dt.date.fromisoformat(evidentiary_start)
    if isinstance(effective_month_end, str):
        effective_month_end = _dt.date.fromisoformat(effective_month_end)
    months = []
    seen = set()
    for s in sessions:
        s = _dt.date.fromisoformat(s) if isinstance(s, str) else s
        key = (s.year, s.month)
        if key in seen:
            continue
        seen.add(key)
        first = _dt.date(s.year, s.month, 1)
        if first <= evidentiary_start:          # not WHOLLY after
            continue
        last_session = max(x for x in
                           (_dt.date.fromisoformat(y) if isinstance(y, str) else y
                            for y in sessions)
                           if (x.year, x.month) == key)
        if last_session + _dt.timedelta(days=maturity_days) <= effective_month_end:
            months.append(key)
    return sorted(months)


# ── decision ────────────────────────────────────────────────────────────────
def decide(complete_months_n, hub_ge100):
    missing = [h for h in REQUIRED if h not in hub_ge100]
    if missing:
        raise CHold(f"GUARD hub_inputs_missing: {missing}")
    cond = {h: bool(hub_ge100[h] >= REQUIRED[h]) for h in REQUIRED}
    condition_a = bool(complete_months_n >= CONDITION_A_MONTHS)
    condition_b = all(cond.values())
    backstop = bool(complete_months_n >= BACKSTOP_MONTHS)
    if backstop:
        return "LOOK", "BACKSTOP", condition_a, cond, condition_b, backstop
    if condition_a and condition_b:
        return "LOOK", "SUPPORT_TRIGGER", condition_a, cond, condition_b, backstop
    return "NO_LOOK", "NONE", condition_a, cond, condition_b, backstop


# ── chain storage ───────────────────────────────────────────────────────────
def _files(chain_dir):
    if not os.path.isdir(chain_dir):
        return []
    return sorted(f for f in os.listdir(chain_dir) if f.endswith(".json"))


def load_chain(chain_dir):
    return [json.load(open(os.path.join(chain_dir, f))) for f in _files(chain_dir)]


def create_genesis(chain_dir, chain_role, authorities, created_at):
    if chain_role not in (ROLE_DRY_RUN, ROLE_PRODUCTION):
        raise CHold(f"GUARD chain_role: {chain_role!r}")
    if _files(chain_dir):
        raise CHold("GUARD genesis_exists: the chain is not empty")
    os.makedirs(chain_dir, exist_ok=True)
    g = dict(schema_version=SCHEMA_VERSION, digest_convention=DIGEST_CONVENTION,
             record_type="GENESIS", seq=0, chain_role=chain_role,
             purpose="ENGINEERING_QUALIFICATION_ONLY" if chain_role == ROLE_DRY_RUN
             else "FORWARD_EVIDENTIARY",
             statement=("THIS CHAIN IS NOT EVIDENTIARY" if chain_role == ROLE_DRY_RUN
                        else "THIS CHAIN IS EVIDENTIARY"),
             authorities=authorities, created_at=created_at)
    g = seal_record(g)
    with open(os.path.join(chain_dir, "0000.json"), "w") as f:
        json.dump(g, f, sort_keys=True)
    return g


def append_decision(chain_dir, rec, chain_role):
    """rec must NOT carry seq, previous_record_digest or record_digest — C assigns them."""
    ch = load_chain(chain_dir)
    if not ch:
        raise CHold("GUARD no_genesis: a chain must begin with an immutable GENESIS record")
    if ch[0].get("record_type") != "GENESIS":
        raise CHold("GUARD no_genesis: first record is not GENESIS")
    verify_chain(chain_dir)
    last = ch[-1]
    if last.get("chain_role") != chain_role or ch[0].get("chain_role") != chain_role:
        raise CHold(f"GUARD chain_role_boundary: appending {chain_role} to a "
                    f"{ch[0].get('chain_role')} chain")
    for r in ch[1:]:
        if r.get("effective_month_end") == rec.get("effective_month_end"):
            raise CHold(f"DUPLICATE_MONTH_RECORD: {rec.get('effective_month_end')} already "
                        f"has a record (seq {r.get('seq')}); a different executed_at does "
                        f"not entitle a second record")
        if r.get("decision") == "LOOK":
            raise CHold(f"CHAIN_TERMINAL: seq {r.get('seq')} recorded LOOK; nothing may be "
                        f"appended under this registration")
    for k in ("seq", "previous_record_digest", "record_digest"):
        if k in rec:
            raise CHold(f"GUARD reserved_field: {k} is assigned by the ledger")
    out = dict(rec)
    out["schema_version"] = SCHEMA_VERSION
    out["digest_convention"] = DIGEST_CONVENTION
    out["record_type"] = "MONTH_END_DECISION"
    out["chain_role"] = chain_role
    out["seq"] = len(ch)
    out["previous_record_digest"] = last["record_digest"]
    out = seal_record(out)
    with open(os.path.join(chain_dir, f"{out['seq']:04d}.json"), "w") as f:
        json.dump(out, f, sort_keys=True)
    return out


def verify_chain(chain_dir, expect_role=None):
    ch = load_chain(chain_dir)
    if not ch:
        raise CHold("GUARD empty_chain")
    for i, r in enumerate(ch):
        if r.get("seq") != i:
            raise CHold(f"GUARD seq: record {i} declares seq {r.get('seq')}")
        d = record_digest(r)
        if d != r.get("record_digest"):
            raise CHold(f"CHAIN_BROKEN: seq {i} digest {d[:16]} != recorded "
                        f"{str(r.get('record_digest'))[:16]} — the record was mutated")
        if expect_role and r.get("chain_role") != expect_role:
            raise CHold(f"GUARD chain_role: seq {i} is {r.get('chain_role')}, expected {expect_role}")
        if i == 0:
            if r.get("record_type") != "GENESIS":
                raise CHold("GUARD no_genesis")
            if r.get("previous_record_digest") is not None:
                raise CHold("GUARD genesis_has_previous")
        else:
            if r.get("previous_record_digest") != ch[i - 1]["record_digest"]:
                raise CHold(f"CHAIN_BROKEN: seq {i} previous_record_digest does not match "
                            f"seq {i-1}")
    return dict(records=len(ch), genesis_digest=ch[0]["record_digest"],
                head_digest=ch[-1]["record_digest"],
                terminal=any(r.get("decision") == "LOOK" for r in ch[1:]))


def self_digest():
    h = hashlib.sha256()
    with open(os.path.abspath(__file__), "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()[:16]


if __name__ == "__main__":
    print(f"schema {SCHEMA_VERSION} · digest convention {DIGEST_CONVENTION}")
    print(f"required {REQUIRED}")
    print(f"module digest {self_digest()}")
