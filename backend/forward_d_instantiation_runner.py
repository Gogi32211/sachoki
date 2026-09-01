"""D — the instantiation runner: the mechanical path from a terminal LOOK to the first
forward-Y access.

The ORDER is the qualification. Every step is gated on the previous one having completed,
and the Y gate cannot be opened before the draw manifest is sealed. It is not enough that
this happens to run in the right order — opening Y earlier must be IMPOSSIBLE, not merely
unusual.

    LOOK record (terminal, verified chain)
      -> freeze the exact forward window
      -> freeze the X-support census
      -> build the admissible permutation-map universe
      -> assert N_admissible_maps >= 10,499
      -> derive SCALE / NULL maps from the SEALED master seed
      -> seal the draw manifest
      -> assert FORWARD_Y_ACCESS_COUNT == 0
      -> ONLY THEN enable the first forward-Y access

Import-side-effect free. Opens no Y itself; it only owns the gate.
"""
from __future__ import annotations
import os, json, hashlib                                               # noqa: E402

B_SCALE = 500
B_NULL = 9999
MIN_ADMISSIBLE_MAPS = B_SCALE + B_NULL          # 10,499
MASTER_SEED_AUTHORITY = "MASSIVE_NEW_FAMILY_FORWARD_HUB_PRESPEC_V1"
MASTER_SEED_DIGEST = "627c767ef9708549"
MASTER_SEED = 9968714824903163673
SUBSTREAMS = ("SCALE", "NULL")
ROLE_DRY_RUN = "DRY_RUN_NON_EVIDENTIARY"
ROLE_PRODUCTION = "PRODUCTION_EVIDENTIARY"

STEPS = ["LOOK_ACCEPTED", "WINDOW_FROZEN", "CENSUS_FROZEN", "MAP_UNIVERSE_BUILT",
         "MAP_COUNT_ASSERTED", "MAPS_DERIVED", "DRAW_MANIFEST_SEALED",
         "Y_ACCESS_COUNT_ASSERTED", "Y_ACCESS_ENABLED"]


class DHold(Exception):
    """Fail-closed. Never downgraded."""


class InstantiationRunner:
    """A one-way state machine. Steps may only advance, never skip, never repeat."""

    def __init__(self, chain_role, seed_authority_path=None):
        if chain_role not in (ROLE_DRY_RUN, ROLE_PRODUCTION):
            raise DHold(f"GUARD chain_role: {chain_role!r}")
        self.chain_role = chain_role
        self.done = []
        self.state = {}
        self._y_gate_open = False
        self._y_access_count = 0
        self.seed_authority_path = seed_authority_path

    # ── ordering ────────────────────────────────────────────────────────────
    def _check(self, step):
        """Validate ORDER ONLY. A step is committed after its own validation passes, so a
        REFUSED step never advances the machine — a rejected derive_maps must leave the
        runner exactly where it was, not one step further along."""
        if STEPS.index(step) != len(self.done):
            expected = STEPS[len(self.done)] if len(self.done) < len(STEPS) else "<complete>"
            raise DHold(f"GUARD step_order: {step} attempted at position {len(self.done)}; "
                        f"the next admissible step is {expected}")

    def _commit(self, step):
        self.done.append(step)

    # ── 1 · LOOK ────────────────────────────────────────────────────────────
    def accept_look(self, chain_dir, verify_chain):
        self._check("LOOK_ACCEPTED")
        info = verify_chain(chain_dir, expect_role=self.chain_role)
        if not info.get("terminal"):
            raise DHold("GUARD no_look: the chain has no terminal LOOK record")
        recs = [json.load(open(os.path.join(chain_dir, f)))
                for f in sorted(os.listdir(chain_dir)) if f.endswith(".json")]
        looks = [r for r in recs if r.get("decision") == "LOOK"]
        if len(looks) != 1:
            raise DHold(f"GUARD look_count: {len(looks)} LOOK records; exactly one is permitted")
        look = looks[0]
        if look.get("chain_role") != self.chain_role:
            raise DHold(f"GUARD look_role: the LOOK record is {look.get('chain_role')}, "
                        f"this runner is {self.chain_role}")
        if self.chain_role == ROLE_PRODUCTION and look.get("chain_role") == ROLE_DRY_RUN:
            raise DHold("GUARD dry_run_look_cannot_authorise_production")
        self.state["look"] = dict(effective_month_end=look["effective_month_end"],
                                  look_reason=look["look_reason"],
                                  record_digest=look["record_digest"],
                                  genesis_digest=info["genesis_digest"])
        self._commit("LOOK_ACCEPTED")
        return self.state["look"]

    # ── 2 · window ──────────────────────────────────────────────────────────
    def freeze_window(self, sessions, evidentiary_start, effective_month_end, maturity_days):
        self._check("WINDOW_FROZEN")
        adm = [s for s in sessions if s > evidentiary_start]
        if not adm:
            raise DHold("GUARD empty_window")
        self.state["window"] = dict(sessions=sorted(adm), n_sessions=len(adm),
                                    first=min(adm), last=max(adm),
                                    evidentiary_start=evidentiary_start,
                                    effective_month_end=effective_month_end,
                                    maturity_days=maturity_days,
                                    digest=hashlib.sha256(
                                        json.dumps(sorted(adm), separators=(",", ":")
                                                   ).encode()).hexdigest()[:16])
        self._commit("WINDOW_FROZEN")
        return self.state["window"]

    # ── 3 · census ──────────────────────────────────────────────────────────
    def freeze_census(self, census_run_id, census_digest, hub_counts):
        self._check("CENSUS_FROZEN")
        if not census_run_id or not census_digest:
            raise DHold("GUARD census_binding: run_id and digest are both required")
        self.state["census"] = dict(run_id=census_run_id, digest=census_digest,
                                    hub_counts=dict(hub_counts))
        self._commit("CENSUS_FROZEN")
        return self.state["census"]

    # ── 4 · map universe ────────────────────────────────────────────────────
    def build_map_universe(self, month_session_counts, early_close_sessions=()):
        """Time-local circular shift within (scope INTERSECT year-month INTERSECT session
        class). EARLY_CLOSE sessions take the identity map, so they contribute a factor of
        one, never zero — a month of only early closes must not silently annihilate the
        product."""
        self._check("MAP_UNIVERSE_BUILT")
        n = 1
        per_month = {}
        for m, cnt in sorted(month_session_counts.items()):
            full = cnt - early_close_sessions.get(m, 0) if isinstance(early_close_sessions, dict) else cnt
            if full < 0:
                raise DHold(f"GUARD month_counts: {m} has more early closes than sessions")
            factor = full if full > 0 else 1
            per_month[m] = dict(sessions=cnt, full_regular=full, factor=factor)
            n *= factor
        self.state["map_universe"] = dict(per_month=per_month, n_admissible_maps=n)
        self._commit("MAP_UNIVERSE_BUILT")
        return self.state["map_universe"]

    # ── 5 · the count assert ────────────────────────────────────────────────
    def assert_map_count(self):
        self._check("MAP_COUNT_ASSERTED")
        n = self.state["map_universe"]["n_admissible_maps"]
        if n < MIN_ADMISSIBLE_MAPS:
            raise DHold(f"GUARD map_universe_too_small: {n:,} < {MIN_ADMISSIBLE_MAPS:,}. "
                        f"This BLOCKS the look. The budget is NOT shrunk to fit.")
        self.state["map_count_assert"] = dict(n_admissible_maps=n,
                                              required=MIN_ADMISSIBLE_MAPS, passed=True)
        self._commit("MAP_COUNT_ASSERTED")
        return self.state["map_count_assert"]

    # ── 6 · maps from the sealed seed ───────────────────────────────────────
    def derive_maps(self, seed=None, seed_source=None):
        self._check("MAPS_DERIVED")
        if seed is None:
            raise DHold("GUARD seed_absent: the master seed must be READ from the sealed "
                        "authority; it is never regenerated, and there is no fallback")
        if seed != MASTER_SEED:
            raise DHold(f"GUARD seed_mismatch: {seed} != the sealed master seed")
        if seed_source != MASTER_SEED_AUTHORITY:
            raise DHold(f"GUARD seed_source: {seed_source!r} is not {MASTER_SEED_AUTHORITY}")
        import numpy as np
        ss = np.random.SeedSequence(seed)
        children = dict(zip(SUBSTREAMS, ss.spawn(len(SUBSTREAMS))))
        draws = {}
        for name, n in (("SCALE", B_SCALE), ("NULL", B_NULL)):
            g = np.random.Generator(np.random.PCG64(children[name]))
            draws[name] = [int(x) for x in g.integers(0, 2 ** 62, size=n)]
        self.state["maps"] = dict(
            substreams=list(SUBSTREAMS), B_SCALE=B_SCALE, B_NULL=B_NULL,
            seed_authority=MASTER_SEED_AUTHORITY, seed_authority_digest=MASTER_SEED_DIGEST,
            draw_digest={k: hashlib.sha256(json.dumps(v, separators=(",", ":")).encode()
                                           ).hexdigest()[:16] for k, v in draws.items()},
            _draws=draws)
        self._commit("MAPS_DERIVED")
        return {k: v for k, v in self.state["maps"].items() if k != "_draws"}

    # ── 7 · draw manifest ───────────────────────────────────────────────────
    def seal_draw_manifest(self, out_dir):
        self._check("DRAW_MANIFEST_SEALED")
        man = dict(schema="D_DRAW_MANIFEST_V1", chain_role=self.chain_role,
                   look=self.state["look"], window=self.state["window"],
                   census=self.state["census"],
                   map_universe={k: v for k, v in self.state["map_universe"].items()},
                   map_count_assert=self.state["map_count_assert"],
                   maps={k: v for k, v in self.state["maps"].items() if k != "_draws"})
        payload = json.dumps(man, sort_keys=True, separators=(",", ":")).encode()
        man["manifest_digest"] = hashlib.sha256(payload).hexdigest()
        os.makedirs(out_dir, exist_ok=True)
        p = os.path.join(out_dir, "draw_manifest.json")
        if os.path.exists(p):
            raise DHold("GUARD manifest_exists: a draw manifest is written exactly once")
        with open(p, "w") as f:
            json.dump(man, f, sort_keys=True)
        self.state["draw_manifest"] = dict(path=p, digest=man["manifest_digest"])
        self._commit("DRAW_MANIFEST_SEALED")
        return self.state["draw_manifest"]

    # ── 8 · the Y-access assert ─────────────────────────────────────────────
    def assert_no_y_access(self):
        self._check("Y_ACCESS_COUNT_ASSERTED")
        if self._y_access_count != 0:
            raise DHold(f"GUARD forward_y_access: FORWARD_Y_ACCESS_COUNT is "
                        f"{self._y_access_count}, must be 0")
        self.state["y_access_assert"] = dict(FORWARD_Y_ACCESS_COUNT=0)
        self._commit("Y_ACCESS_COUNT_ASSERTED")
        return self.state["y_access_assert"]

    # ── 9 · the gate ────────────────────────────────────────────────────────
    def enable_forward_y_access(self):
        self._check("Y_ACCESS_ENABLED")
        if "draw_manifest" not in self.state:
            raise DHold("GUARD gate_order: the draw manifest must be sealed first")
        self._y_gate_open = True
        self._commit("Y_ACCESS_ENABLED")
        return True

    def open_forward_y(self, reader=None):
        """The ONLY path to forward Y. Refuses while the gate is shut, which is the whole
        point: opening Y early must be impossible, not merely unusual."""
        if not self._y_gate_open:
            raise DHold(f"FORWARD_Y_GATE_CLOSED: attempted at step "
                        f"{len(self.done)}/{len(STEPS)} "
                        f"({self.done[-1] if self.done else 'START'}); the gate opens only "
                        f"after DRAW_MANIFEST_SEALED and the zero-access assert")
        self._y_access_count += 1
        return reader() if reader else None

    @property
    def y_access_count(self):
        return self._y_access_count

    def status(self):
        return dict(chain_role=self.chain_role, steps_done=list(self.done),
                    next_step=STEPS[len(self.done)] if len(self.done) < len(STEPS) else None,
                    y_gate_open=self._y_gate_open,
                    FORWARD_Y_ACCESS_COUNT=self._y_access_count)


def self_digest():
    h = hashlib.sha256()
    with open(os.path.abspath(__file__), "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()[:16]


if __name__ == "__main__":
    print(f"steps: {STEPS}")
    print(f"B_SCALE {B_SCALE} · B_NULL {B_NULL} · min admissible maps {MIN_ADMISSIBLE_MAPS:,}")
    print(f"module digest {self_digest()}")
