# qlib_lab — lean factor research for the QLIB tab

Trains ML models on the **precomputed point-in-time signal columns** in the
Studio DuckDB (`bars`) and measures whether they actually predict the next-bar
forward return — **IC / Rank IC / ICIR + feature importance**, with a strict
time-ordered out-of-sample split.

The **default engine** is `DuckDB → pandas → LightGBM` — no Microsoft qlib
dependency, lean, fast. The model layer (`models.py`) is **pluggable**:

- `lightgbm` (default, always available) — native LightGBM.
- `qlib-lgbm` — the **real Microsoft qlib `LGBModel`**, fed our DuckDB-derived
  data through a qlib `DatasetH` (StaticDataLoader → DataHandlerLP). No dump_bin
  / `.bin` step. It produces effectively the same IC/Rank IC as the native path
  (same gradient booster underneath) but routes through qlib's machinery. It's
  only offered if `pyqlib` is importable; otherwise the picker shows it greyed
  out (`available_models()` probes for it). `qlib.init` runs once with
  `MLFLOW_ALLOW_FILE_STORE=true` so qlib's recorder doesn't trip newer mlflow.

`pyqlib` drags in ~100 packages (mlflow/jupyter/gym/cvxpy) and prefers numpy 2.x,
so it stays **out of `requirements.txt`** — install it locally only if you want
the `qlib-lgbm` backend. The lean path keeps the prod deploy small.

### Label horizon

The label can look forward 1 / 5 / 10 / 20 bars (UI selector). For any horizon:

```
label[T] = close[T+1+H] / close[T+1] - 1     # H = horizon; enter next bar, hold H bars
```

Entry is always the *next* bar's close (never the signal bar's own close), so
every horizon is point-in-time safe.

## The one rule: `fwd_*` is never a feature

Any column matching `fwd_`, `mfe_`, `mae_`, `hit_`, `drop_`, `fwd_swing_` is an
**outcome label** computed with future data. It is excluded from the feature
picker (`columns.py: FORBIDDEN_RE`) and rejected if it ever reaches the API
(`validate_features`). Features are *only* the signal/score columns you select.

The label the model learns is built from **price only**, one bar *after* the
signal:

```
label[T] = close[T+2] / close[T+1] - 1      # qlib: Ref($close,-2)/Ref($close,-1)-1
```

You see the signal at the close of bar `T`; you can only act on the next bar, so
you enter at `T+1`'s close and hold one bar to `T+2`. Features use only data up
to `T`; the label uses only future prices → no look-ahead, and no unfillable
same-bar entry.

## Pipeline

| File | Role |
|------|------|
| `columns.py` | Lists selectable feature columns, grouped by family; excludes forbidden outcome columns. |
| `data.py`    | Reads `bars` **read-only**, builds the price label, drops short tickers / null features, encodes string signals as category codes. Caches the matrix to `~/.sachoki/qlib_lab/`. |
| `models.py`  | Pluggable model registry (`LightGBMModel` default; `qlib-lgbm` stub). |
| `metrics.py` | Cross-sectional IC / Rank IC / ICIR (+ per-day series, quantile spread). |
| `train.py`   | Time-ordered train/valid/test split → fit → evaluate on the test set. |
| `jobs.py`    | In-memory background-job registry (build/train) for status polling. |
| `api.py`     | FastAPI router under `/api/qlib`. |

## API

```
GET  /api/qlib/columns?universe=sp500
GET  /api/qlib/models
POST /api/qlib/build   {universe, features[], date_from, date_to, min_bars}  -> {job_id}
POST /api/qlib/train   {universe, features[], model, splits, ...}            -> {job_id}
GET  /api/qlib/job/{job_id}
```

Build/train run as background tasks; poll `/job/{id}` until `status == "done"`.

## Requirements

```
pip install lightgbm scikit-learn duckdb
# macOS (Apple Silicon) LightGBM needs the OpenMP runtime:
brew install libomp
```

`scipy` (Rank IC) and `pandas`/`numpy` are already backend dependencies.
`pyqlib` is **optional** and only needed if/when the `qlib-lgbm` backend is wired up.

## Reading the numbers

- **IC** = mean daily cross-sectional Pearson corr(prediction, realised return).
- **Rank IC** = same with Spearman (rank) — robust to outliers.
- **ICIR** = mean(daily IC) / std(daily IC) — consistency / t-stat-like.

Rough reads: `IC > 0.02` with `ICIR > 0.3` and a positive top-minus-bottom
quantile spread = real cross-sectional edge. **IC ≈ 0 or negative is a valid,
honest result** (the selected signals don't predict next-bar returns), not a bug.

> **Price-feed caveat:** the DuckDB feed differs slightly from TradingView, so
> treat absolute returns as indicative. IC / Rank IC measure *ranking* skill and
> are the trustworthy outputs.
