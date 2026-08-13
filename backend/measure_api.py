"""The measurement console's API: expression in, matched-control measurement out, k charged.

    POST /api/studio/measure            {expr, universe} → effect vs matched control + k
    GET  /api/studio/measure/columns    the palette the UI builds its hints from

The statistics live in `measure_worker` (a separate process — NakedStudy's clean room refuses to
construct where edge_replay is loaded, and the backend loads it). This module owns the two
things that must NOT restart with the worker: the durable k ledger, and the decision about what
counts as one claim.

WHAT IS ONE CLAIM. The canonical expression (whitespace and formatting normalised by the AST) +
universe + timeframe. The horizon set is NOT a per-claim knob — all four preregistered horizons
come back together, so "try 5d, then try 10d" is not two claims, it is one claim read fully. A
selectable horizon would be the cheapest multiplier of k on the screen, which is exactly why it
is not offered.

CHARGED ON DELIVERY. The claim is recorded when a result is returned, not when the button is
pressed: a request refused by the validator or failed in the worker exposed nothing. This is the
same definition of exposure the whole system runs on — delivery, not attempt.
"""
from __future__ import annotations

import json
import os
import select
import subprocess
import sys
import threading
import time
from typing import Optional

from pydantic import BaseModel

import measure_expression as MX
from search_counter import SearchCounter

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.join(HERE, "measure_worker.py")

FIRST_CALL_TIMEOUT = 300.0          # cold worker loads ~774k bars and runs the baseline
WARM_TIMEOUT = 120.0

COUNTER = SearchCounter(
    "measurement_search.jsonl", "measurement-console",
    note=("k counts DIFFERENT measurement claims (canonical expression + universe). All four "
          "horizons return together, so trying another horizon is not another claim — a "
          "selectable horizon would be the cheapest multiplier of k on the screen."))


class MeasureRequest(BaseModel):
    """Module level — PEP 563 + a locally-scoped pydantic model degrades the body to a query
    parameter. Third near-miss with this exact shape in one project; see studio_describe."""
    expr: str
    universe: str = "sp500"


class _Worker:
    """One subprocess, restarted on death, spoken to under a lock.

    The lock serialises measurements. That is a feature at this scale, not a bottleneck: two
    concurrent bootstrap runs on one study object would race its RNG and its control cache.
    """

    def __init__(self):
        self.p: subprocess.Popen | None = None
        self.lock = threading.Lock()
        self.warmed_universe: str | None = None
        self._n = 0

    def _alive(self) -> bool:
        return self.p is not None and self.p.poll() is None

    def _spawn(self) -> None:
        self.p = subprocess.Popen(
            [sys.executable, WORKER], cwd=HERE,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None,  # stderr → backend log
            text=True, bufsize=1)
        self.warmed_universe = None

    def _read_line(self, deadline: float) -> str:
        fd = self.p.stdout.fileno()
        buf = b""
        while time.time() < deadline:
            ready, _, _ = select.select([fd], [], [], min(1.0, deadline - time.time()))
            if not ready:
                if not self._alive():
                    raise RuntimeError("the measurement worker died mid-request; its traceback "
                                       "is in the backend log")
                continue
            chunk = os.read(fd, 65536)
            if not chunk:
                raise RuntimeError("the measurement worker closed its pipe; see the backend log")
            buf += chunk
            if b"\n" in buf:
                line, _, _rest = buf.partition(b"\n")
                return line.decode()
        self.p.kill()
        self.p = None
        raise RuntimeError("the measurement timed out and the worker was restarted. A cold "
                           "start loads the whole study first — try once more.")

    def call(self, payload: dict) -> dict:
        with self.lock:
            if not self._alive():
                self._spawn()
            cold = self.warmed_universe != payload.get("universe")
            self._n += 1
            payload["id"] = str(self._n)
            self.p.stdin.write(json.dumps(payload) + "\n")
            self.p.stdin.flush()
            line = self._read_line(time.time()
                                   + (FIRST_CALL_TIMEOUT if cold else WARM_TIMEOUT))
            resp = json.loads(line)
            if "error" not in resp:
                self.warmed_universe = payload.get("universe")
            return resp


_WORKER = _Worker()


# ── palette ──────────────────────────────────────────────────────────────────
EXAMPLES = [
    "t_sig == 'T1' and rsi_14 < 35",
    "l_sig == 'L34' and vol_sig in ('B', 'VB')",
    "t_sig.startswith('T') and full_suffix.shift(1) == 'ED'",
    "swing_type_3 == 'HL' and swing_type_3.shift(1) == 'LL'",
    "close > open.shift(1) and (high - low) > 2 * atr_14",
    "g_sig.startswith('G3') and rsi_14 < 45 and close >= 21 and close <= 89",
]


def palette() -> dict:
    import studio_describe as SD                                      # noqa: PLC0415
    cols = SD.columns()["columns"]
    numeric = [{"column": c, "label": c, "kind": "numeric"}
               for c in sorted(MX.NUMERIC_PRIMITIVES)]
    tokens = [{"column": c["column"], "label": c["label"], "kind": "token"}
              for c in cols if c["column"] not in MX.NUMERIC_PRIMITIVES]
    return {"columns": tokens + numeric, "examples": EXAMPLES,
            "methods": sorted(MX.ALLOWED_METHODS), "max_shift": MX.MAX_SHIFT,
            "language": ("boolean expression · == != < <= > >= in · and/or/not · "
                         ".shift(k) = same ticker, k bars back, calendar-gap safe · "
                         ".startswith/.endswith/.contains/.isin"),
            "excluded": ("our scores, tiers and forward labels are not inputs — the refusal "
                         "quotes the contract's reason. Price bands go IN the expression: "
                         "close >= 21 and close <= 89")}


def build_router():
    from fastapi import APIRouter, HTTPException                      # noqa: PLC0415

    router = APIRouter(prefix="/api/studio/measure", tags=["studio-measure"])

    @router.get("/columns")
    def _columns():
        return palette()

    @router.get("/accounting")
    def _accounting():
        return COUNTER.accounting()

    @router.post("")
    def _measure(req: MeasureRequest):
        # fast refusal in-process; nothing was exposed, nothing is charged
        try:
            info = MX.validate(req.expr)
        except MX.ExpressionError as e:
            raise HTTPException(400, detail=str(e))
        if req.universe not in ("sp500", "nasdaq", "russell2k"):
            raise HTTPException(400, detail=f"unknown universe {req.universe!r}")

        try:
            resp = _WORKER.call({"cmd": "measure", "expr": req.expr,
                                 "universe": req.universe})
        except Exception as e:                                        # noqa: BLE001
            raise HTTPException(503, detail=str(e))
        if "error" in resp:
            raise HTTPException(400, detail=resp["error"])

        # charged on delivery — the numbers are now available, so the claim is exposed
        resp["search_accounting"] = COUNTER.safe_record(
            {"expr": info["canonical"], "universe": req.universe, "tf": "1d",
             "horizons": [5, 10, 20, 60]},
            extra={"n_matched": resp.get("n_matched")})
        return resp

    return router
