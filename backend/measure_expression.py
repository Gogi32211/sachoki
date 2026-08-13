"""The measurement console's input language: one boolean expression over bar primitives.

    t_sig == 'T1' and rsi_14 < 35 and close > open.shift(1)
    l_sig == 'L34' and full_suffix.shift(1) == 'ED' and vol_sig in ('B', 'VB')
    swing_type_3 == 'HL' and swing_type_3.shift(1) == 'LL'

NEVER `eval` ON USER TEXT. The expression is parsed with `ast`, every node is checked against a
whitelist, and evaluation is a walk over the validated tree that produces pandas operations. A
name that is not an allowed column is refused before anything runs, and the refusal carries the
CONTRACT's reason when there is one — `ultra_score_v3` is rejected with the sentence from
`BAR_FORBIDDEN`, not with "invalid input".

WHAT `.shift()` MEANS, AND THE TRAP IN IT. `close.shift(1)` is "the previous bar of the SAME
ticker" — a global shift would read ticker A's last bar as ticker B's first. And "previous ROW"
is still not "previous day": a price floor deletes bars from the middle of a history, so the
previous row can be months away (the dup-row incident's cousin — see
feedback-data-contract-first). Every row whose shift window crosses a calendar gap is therefore
EXCLUDED from the mask, using the `prev_ok` adjacency flag that `sources.bars()` attaches, and
the count of rows dropped for this reason is reported rather than absorbed.

THE `on` COLUMN. `matched()` once compared a gap-selected cell against a control matched on
price and liquidity alone; the control sat at a shallower gap and the framework printed SIGNAL
on an effect that was zero once gap depth was equalised. So this module reports which CONTINUOUS
column the expression selected on, and the caller passes it to the matcher. With several, the
most-used one is chosen and the choice is stated in the result.
"""
from __future__ import annotations

import ast
import difflib

import numpy as np
import pandas as pd

from sources import BAR_FORBIDDEN, BAR_PRIMITIVES

# columns an expression may name: the shared allowlist plus the raw bar itself
OHLCV = ("open", "high", "low", "close", "volume")
NUMERIC_PRIMITIVES = frozenset({"rsi_14", "cci_20", "atr_14", "avg_vol_20d", "change_pct",
                                *OHLCV})
ALLOWED_NAMES = frozenset(BAR_PRIMITIVES) | frozenset(OHLCV)

ALLOWED_METHODS = {"shift", "startswith", "endswith", "contains", "isin"}
MAX_SHIFT = 10

_CMP = {ast.Eq: "==", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">=",
        ast.In: "in", ast.NotIn: "not in"}
_BIN = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/"}


class ExpressionError(ValueError):
    """The expression cannot be validated. The message is the whole answer."""


def _refuse_name(name: str) -> None:
    if name in BAR_FORBIDDEN:
        raise ExpressionError(
            f"{name!r} cannot be an input here: {BAR_FORBIDDEN[name]}")
    close = difflib.get_close_matches(name, sorted(ALLOWED_NAMES), n=3, cutoff=0.5)
    hint = f" Did you mean: {', '.join(close)}?" if close else ""
    raise ExpressionError(
        f"{name!r} is not an allowed column. Inputs are the bar primitives and OHLCV — the same "
        f"allowlist as sources.bars().{hint}")


# ── validation ──────────────────────────────────────────────────────────────
def validate(expr: str) -> dict:
    """Parse and check every node. Returns what the caller needs to know about the claim."""
    expr = (expr or "").strip()
    if not expr:
        raise ExpressionError("the expression is empty")
    if len(expr) > 2000:
        raise ExpressionError("the expression is longer than 2000 characters")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise ExpressionError(f"not a valid expression: {e.msg} (offset {e.offset})")

    columns: set = set()
    numeric_compared: list = []
    max_shift = 0

    def walk(node, depth=0):
        nonlocal max_shift
        if depth > 40:
            raise ExpressionError("the expression nests deeper than 40 levels")
        if isinstance(node, ast.Expression):
            return walk(node.body, depth + 1)
        if isinstance(node, ast.BoolOp) and isinstance(node.op, (ast.And, ast.Or)):
            for v in node.values:
                walk(v, depth + 1)
            return
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.Not, ast.USub)):
            return walk(node.operand, depth + 1)
        if isinstance(node, ast.Compare):
            for op in node.ops:
                if type(op) not in _CMP:
                    raise ExpressionError(f"comparison {type(op).__name__} is not supported")
            walk(node.left, depth + 1)
            for c in node.comparators:
                walk(c, depth + 1)
            # remember which numeric columns the claim SELECTS on — the matcher needs them.
            # Walk WHOLE sides: `(high - low) > 2 * atr_14` selects on range and ATR even
            # though neither is the top node of its side.
            for side in (node.left, *node.comparators):
                for sub in ast.walk(side):
                    if isinstance(sub, ast.Name) and sub.id in NUMERIC_PRIMITIVES:
                        numeric_compared.append(sub.id)
            return
        if isinstance(node, ast.BinOp):
            if type(node.op) not in _BIN:
                raise ExpressionError(f"operator {type(node.op).__name__} is not supported")
            walk(node.left, depth + 1)
            walk(node.right, depth + 1)
            return
        if isinstance(node, ast.Call):
            f = node.func
            if not isinstance(f, ast.Attribute) or f.attr not in ALLOWED_METHODS:
                raise ExpressionError(
                    "only these methods are supported: "
                    + ", ".join(sorted(ALLOWED_METHODS))
                    + " — e.g. close.shift(1), t_sig.startswith('T')")
            if node.keywords:
                raise ExpressionError(f".{f.attr}() takes no keyword arguments here")
            if f.attr == "shift":
                if (len(node.args) != 1 or not isinstance(node.args[0], ast.Constant)
                        or not isinstance(node.args[0].value, int)
                        or not (1 <= node.args[0].value <= MAX_SHIFT)):
                    raise ExpressionError(f".shift(k) needs a literal 1 ≤ k ≤ {MAX_SHIFT}")
                max_shift = max(max_shift, node.args[0].value)
            elif f.attr == "isin":
                if (len(node.args) != 1
                        or not isinstance(node.args[0], (ast.Tuple, ast.List))
                        or not all(isinstance(x, ast.Constant) for x in node.args[0].elts)):
                    raise ExpressionError(".isin() takes a literal tuple, e.g. .isin(('T1','T2'))")
            else:
                if (len(node.args) != 1 or not isinstance(node.args[0], ast.Constant)
                        or not isinstance(node.args[0].value, str)):
                    raise ExpressionError(f".{f.attr}() takes one literal string")
            return walk(f.value, depth + 1)
        if isinstance(node, ast.Name):
            if node.id not in ALLOWED_NAMES:
                _refuse_name(node.id)
            columns.add(node.id)
            return
        if isinstance(node, ast.Constant):
            if not isinstance(node.value, (str, int, float, bool)):
                raise ExpressionError(f"literal {node.value!r} is not supported")
            return
        if isinstance(node, (ast.Tuple, ast.List)):
            for e in node.elts:
                walk(e, depth + 1)
            return
        raise ExpressionError(f"{type(node).__name__} is not part of this language")

    walk(tree)
    if not columns:
        raise ExpressionError("the expression names no column, so it selects nothing")

    on = None
    if numeric_compared:
        # most-used first, alphabetical to break ties — deterministic, and reported
        on = sorted(numeric_compared, key=lambda c: (-numeric_compared.count(c), c))[0]

    return {"tree": tree, "canonical": ast.unparse(tree),
            "columns": sorted(columns), "max_shift": max_shift, "on_column": on,
            "token_columns": sorted(columns - NUMERIC_PRIMITIVES)}


# ── evaluation ──────────────────────────────────────────────────────────────
def evaluate(df: pd.DataFrame, expr: str) -> tuple:
    """(mask, info). The mask is adjacency-safe: rows whose shift window crosses a calendar
    gap are excluded, and the exclusion is counted in info rather than silent."""
    info = validate(expr)
    for col in info["columns"]:
        if col not in df.columns:
            raise ExpressionError(f"{col!r} is allowed but not loaded in this frame")

    g = df.groupby("ticker", sort=False)
    shift_cache: dict = {}

    def shifted(col: str, k: int) -> pd.Series:
        key = (col, k)
        if key not in shift_cache:
            shift_cache[key] = g[col].shift(k)
        return shift_cache[key]

    def ev(node):
        if isinstance(node, ast.Expression):
            return ev(node.body)
        if isinstance(node, ast.BoolOp):
            parts = [_as_bool(ev(v)) for v in node.values]
            out = parts[0]
            for p in parts[1:]:
                out = (out & p) if isinstance(node.op, ast.And) else (out | p)
            return out
        if isinstance(node, ast.UnaryOp):
            v = ev(node.operand)
            return ~_as_bool(v) if isinstance(node.op, ast.Not) else -v
        if isinstance(node, ast.Compare):
            left = ev(node.left)
            out = None
            for op, comp in zip(node.ops, node.comparators):
                right = ev(comp)
                if isinstance(op, (ast.In, ast.NotIn)):
                    r = pd.Series(left).isin(list(right))
                    if isinstance(op, ast.NotIn):
                        r = ~r
                else:
                    r = {"==": left == right, "!=": left != right, "<": left < right,
                         "<=": left <= right, ">": left > right, ">=": left >= right,
                         }[_CMP[type(op)]]
                out = r if out is None else (_as_bool(out) & _as_bool(r))
                left = right
            return out
        if isinstance(node, ast.BinOp):
            l, r = ev(node.left), ev(node.right)
            return {"+": l + r, "-": l - r, "*": l * r, "/": l / r}[_BIN[type(node.op)]]
        if isinstance(node, ast.Call):
            attr = node.func.attr
            if attr == "shift":
                base = node.func.value
                if not isinstance(base, ast.Name):
                    # shift of a shifted/derived thing — evaluate then shift per ticker
                    v = ev(base)
                    return pd.Series(v, index=df.index).groupby(df["ticker"]).shift(
                        node.args[0].value)
                return shifted(base.id, node.args[0].value)
            base_v = pd.Series(ev(node.func.value)).astype(str)
            arg = node.args[0]
            if attr == "isin":
                return base_v.isin([str(x.value) for x in arg.elts])
            return {"startswith": base_v.str.startswith(arg.value),
                    "endswith": base_v.str.endswith(arg.value),
                    "contains": base_v.str.contains(arg.value, regex=False)}[attr]
        if isinstance(node, ast.Name):
            return df[node.id]
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, (ast.Tuple, ast.List)):
            return [e.value for e in node.elts]
        raise ExpressionError(f"unreachable node {type(node).__name__}")

    def _as_bool(v) -> pd.Series:
        return pd.Series(v, index=df.index).fillna(False).astype(bool)

    mask = _as_bool(ev(info["tree"]))
    n_raw = int(mask.sum())

    dropped = 0
    if info["max_shift"] and "prev_ok" in df.columns:
        adj = pd.Series(True, index=df.index)
        prev = df["prev_ok"].fillna(False).astype(bool)
        for j in range(info["max_shift"]):
            adj &= g["prev_ok"].shift(j).fillna(False).astype(bool) if j else prev
        before = int(mask.sum())
        mask &= adj
        dropped = before - int(mask.sum())

    info.pop("tree")
    info.update({"n_matched_raw": n_raw, "n_dropped_nonadjacent": int(dropped),
                 "n_matched": int(mask.sum())})
    return mask, info
