"""
qlib_lab/models.py — pluggable model registry for the QLIB tab.

Every model implements a tiny interface so the API/UI never has to care which
engine is underneath:

    model.fit(X_train, y_train, X_valid, y_valid, categorical) -> None
    model.predict(X)            -> np.ndarray
    model.feature_importance()  -> dict[str, float]   (normalised, sums to ~1)

DEFAULT = "lightgbm" (native LightGBM, no Microsoft qlib needed).

To add the real qlib backend later, register a class here whose .fit/.predict
wrap qlib's LGBModel + DatasetH — nothing else in the pipeline changes. A stub
("qlib-lgbm") is registered below and raises a clear NotImplementedError until
that work is done, so the model dropdown can already advertise it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


class LightGBMModel:
    """Native LightGBM regressor. Fast, clean gain-based feature importance,
    handles NaN and categorical (integer-coded) features natively."""

    name = "lightgbm"
    label = "LightGBM (native)"

    def __init__(self, params: dict | None = None, num_boost_round: int = 300):
        self.params = {
            "objective": "regression",
            "metric": "l2",
            "num_leaves": 31,
            "learning_rate": 0.03,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "min_data_in_leaf": 200,
            "verbose": -1,
            "num_threads": 0,
        }
        if params:
            self.params.update(params)
        self.num_boost_round = num_boost_round
        self._booster = None
        self._features: list[str] = []

    def fit(self, train_df, valid_df, features, categorical=None):
        import lightgbm as lgb

        self._features = list(features)
        categorical = categorical or "auto"
        dtrain = lgb.Dataset(train_df[self._features], label=train_df["label"],
                             categorical_feature=categorical)
        valid_sets, valid_names, callbacks = [dtrain], ["train"], []
        if valid_df is not None and len(valid_df) > 0:
            dvalid = lgb.Dataset(valid_df[self._features], label=valid_df["label"],
                                 reference=dtrain, categorical_feature=categorical)
            valid_sets.append(dvalid)
            valid_names.append("valid")
            callbacks.append(lgb.early_stopping(50, verbose=False))
        callbacks.append(lgb.log_evaluation(0))
        self._booster = lgb.train(
            self.params, dtrain,
            num_boost_round=self.num_boost_round,
            valid_sets=valid_sets, valid_names=valid_names,
            callbacks=callbacks,
        )
        return self

    def predict(self, X) -> np.ndarray:
        return self._booster.predict(X[self._features])

    def feature_importance(self) -> dict:
        gain = self._booster.feature_importance(importance_type="gain")
        total = float(gain.sum()) or 1.0
        imp = {f: float(g) / total for f, g in zip(self._features, gain)}
        return dict(sorted(imp.items(), key=lambda kv: kv[1], reverse=True))

    @property
    def best_iteration(self):
        return getattr(self._booster, "best_iteration", None)


_QLIB_INITED = False


def _ensure_qlib():
    """Initialise qlib once per process. No data provider is needed — we feed a
    static in-memory panel via StaticDataLoader, so there's no dump_bin / .bin
    step. The mlflow file-store opt-out keeps qlib's recorder from raising on
    newer mlflow versions."""
    global _QLIB_INITED
    import os
    os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")
    if not _QLIB_INITED:
        import qlib
        qlib.init(region="us", logging_level="ERROR")
        _QLIB_INITED = True


class QlibLGBModel:
    """The real Microsoft qlib `LGBModel`, fed our DuckDB-derived data through a
    qlib `DatasetH` (StaticDataLoader → DataHandlerLP). Same metrics as the
    native path, but routed through qlib's model + dataset machinery — so this is
    the genuine qlib backend, not a re-implementation. No dump_bin / .bin needed."""

    name = "qlib-lgbm"
    label = "Microsoft qlib · LGBModel"

    def __init__(self, params: dict | None = None, num_boost_round: int = 300):
        self.kw = {
            "num_leaves": 31, "learning_rate": 0.03,
            "colsample_bytree": 0.8, "subsample": 0.8,
            "num_boost_round": num_boost_round, "early_stopping_rounds": 50,
        }
        if params:
            self.kw.update(params)
        self._model = None
        self._features: list[str] = []

    @staticmethod
    def _panel(df, features):
        """Build a qlib panel: MultiIndex rows (datetime, instrument), MultiIndex
        columns ('feature', f) / ('label', 'LABEL0')."""
        d = df.copy()
        d["datetime"] = pd.to_datetime(d["date"])
        d["instrument"] = d["ticker"].astype(str)
        d = d.set_index(["datetime", "instrument"]).sort_index()
        feat = d[features].astype("float64")
        lab = d[["label"]].astype("float64").rename(columns={"label": "LABEL0"})
        return pd.concat({"feature": feat, "label": lab}, axis=1)

    def fit(self, train_df, valid_df, features, categorical=None):
        _ensure_qlib()
        from qlib.data.dataset import DatasetH
        from qlib.data.dataset.handler import DataHandlerLP
        from qlib.data.dataset.loader import StaticDataLoader
        from qlib.contrib.model.gbdt import LGBModel

        if valid_df is None or len(valid_df) == 0:
            raise ValueError(
                "qlib LGBModel needs a non-empty validation split for early "
                "stopping — widen the 'valid' date range or use the native model."
            )
        self._features = list(features)
        panel = self._panel(pd.concat([train_df, valid_df]), self._features)
        handler = DataHandlerLP(data_loader=StaticDataLoader(config=panel),
                                process_type=DataHandlerLP.PTYPE_I)
        seg = {
            "train": (str(train_df["date"].min().date()), str(train_df["date"].max().date())),
            "valid": (str(valid_df["date"].min().date()), str(valid_df["date"].max().date())),
        }
        ds = DatasetH(handler=handler, segments=seg)
        self._model = LGBModel(**self.kw)
        self._model.fit(ds)
        return self

    def predict(self, df) -> np.ndarray:
        from qlib.data.dataset import DatasetH
        from qlib.data.dataset.handler import DataHandlerLP
        from qlib.data.dataset.loader import StaticDataLoader

        panel = self._panel(df, self._features)
        handler = DataHandlerLP(data_loader=StaticDataLoader(config=panel),
                                process_type=DataHandlerLP.PTYPE_I)
        seg = {"test": (str(df["date"].min().date()), str(df["date"].max().date()))}
        ds = DatasetH(handler=handler, segments=seg)
        pred = self._model.predict(ds, segment="test")  # Series idx (datetime, instrument)
        # realign to df row order so it lines up with the caller's labels/dates
        key = pd.MultiIndex.from_arrays(
            [pd.to_datetime(df["date"]), df["ticker"].astype(str)],
            names=["datetime", "instrument"],
        )
        return pd.Series(pred).reindex(key).to_numpy()

    def feature_importance(self) -> dict:
        gain = self._model.model.feature_importance(importance_type="gain")
        total = float(gain.sum()) or 1.0
        imp = {f: float(g) / total for f, g in zip(self._features, gain)}
        return dict(sorted(imp.items(), key=lambda kv: kv[1], reverse=True))

    @property
    def best_iteration(self):
        m = getattr(self._model, "model", None)
        return getattr(m, "best_iteration", None) if m else None


_REGISTRY = {
    LightGBMModel.name: LightGBMModel,
    QlibLGBModel.name: QlibLGBModel,
}


def _qlib_installed() -> bool:
    import importlib.util
    return importlib.util.find_spec("qlib") is not None


def available_models() -> list[dict]:
    qlib_ok = _qlib_installed()
    return [
        {"name": LightGBMModel.name, "label": LightGBMModel.label, "ready": True},
        {"name": QlibLGBModel.name,
         "label": QlibLGBModel.label + ("" if qlib_ok else " — pip install pyqlib"),
         "ready": qlib_ok},
    ]


def make_model(name: str, **kwargs):
    if name not in _REGISTRY:
        raise ValueError(f"unknown model {name!r}; available: {list(_REGISTRY)}")
    return _REGISTRY[name](**kwargs)
