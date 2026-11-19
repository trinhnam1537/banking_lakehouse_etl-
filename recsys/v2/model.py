"""Mô hình champion của hệ gợi ý v2: LightGBM multi-task dạng "dài" + hiệu chỉnh xác suất isotonic theo sản phẩm.

Vì sao multi-task (docs/RECSYS-BENCH.md mục 5-6): tốt nhất trên test MBD (MAP@3 0,697, CI 95% dương so với LightGBM gốc),
train nhanh nhất, và một mô hình cho MỘT thang điểm chung giữa các sản phẩm, đúng thứ mà xếp hạng sản phẩm cho từng khách
cần. Sản phẩm hiếm (sp2 MBD, thẻ vàng Berka) mượn được quy luật của sản phẩm khác.

Dạng dài: mỗi dòng (khách, tháng) nhân K lần, thêm cột `product` (phân loại) và `own_*` = lịch sử của CHÍNH sản phẩm đó.
Chỉ dòng (khách, sản phẩm) ĐỦ ĐIỀU KIỆN mới vào huấn luyện (dòng đã giữ luôn nhãn 0, học vô nghĩa).

Hiệu chỉnh hai bước:
  1. Bù lấy mẫu âm (giữ tỉ lệ r mẫu âm => odds bị nhân 1/r): odds_goc = odds * r.
  2. Isotonic theo từng sản phẩm, học trên VALID (đơn điệu, không giả định dạng sigmoid). Sau bước này `p_cal` đọc được
     như xác suất ("khả năng mua 3,2%"), đo bằng Brier / ECE ở evaluate.py. Xếp hạng giữa sản phẩm dùng p_cal.

Giải thích: SHAP của cây (LightGBM pred_contrib) trên dòng dài của đúng sản phẩm được gợi ý.
"""

from __future__ import annotations

import json
import pathlib
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import lightgbm as lgb  # noqa: I001
import numpy as np
import pandas as pd

from .data import Dataset

NEG_RATE = 0.25
SEED = 7
PARAMS = {"objective": "binary", "learning_rate": 0.05, "num_leaves": 63, "min_data_in_leaf": 200,
          "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 1.0,
          "metric": "average_precision", "verbosity": -1, "seed": SEED, "num_threads": 8}


def _X(d: pd.DataFrame, cols: List[str]) -> np.ndarray:
    return np.ascontiguousarray(d[cols].to_numpy(dtype=np.float32, na_value=np.nan))


def to_long(ds: Dataset, d: pd.DataFrame, only_eligible: bool = True) -> pd.DataFrame:
    parts = []
    for j, c in enumerate(ds.domain.codes):
        x = d[ds.features].copy()
        x["product"] = j
        for name in ds.own_names:
            col = ds.own[c].get(name)
            x[name] = d[col].to_numpy() if col else np.nan
        x["y"] = d[ds.domain.label_prefix + c].fillna(0).astype(int).to_numpy()
        x["elig"] = d[f"elig_{c}"].astype(bool).to_numpy()
        x["_row"] = np.arange(len(d))
        parts.append(x[x["elig"]] if only_eligible else x)
    return pd.concat(parts, ignore_index=True)


class Isotonic:
    """Isotonic regression lưu được dạng JSON (ngưỡng x, y) -> nội suy tuyến tính khi dự đoán."""

    def __init__(self, x: Optional[List[float]] = None, y: Optional[List[float]] = None):
        self.x, self.y = x or [0.0, 1.0], y or [0.0, 1.0]

    def fit(self, p: np.ndarray, y: np.ndarray) -> "Isotonic":
        from sklearn.isotonic import IsotonicRegression

        ir = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip").fit(p, y)
        self.x, self.y = [float(v) for v in ir.X_thresholds_], [float(v) for v in ir.y_thresholds_]
        return self

    def __call__(self, p: np.ndarray) -> np.ndarray:
        return np.interp(p, self.x, self.y)


@dataclass
class Ranker:
    domain: str
    codes: List[str]
    features: List[str]
    own_names: List[str]
    categorical: List[str]
    booster: Optional[lgb.Booster] = None
    calibrators: Dict[str, Isotonic] = field(default_factory=dict)
    meta: Dict = field(default_factory=dict)

    @property
    def fcols(self) -> List[str]:
        return self.features + self.own_names + ["product"]

    # ------------------------------------------------------------------ huấn luyện
    @classmethod
    def fit(cls, ds: Dataset, log=print, params: Optional[Dict] = None) -> "Ranker":
        t0 = time.time()
        m = cls(ds.domain.name, ds.domain.codes, ds.features, ds.own_names, ds.categorical)
        tr, va = ds.split("train"), ds.split("valid")
        Ltr, Lva = to_long(ds, tr), to_long(ds, va)
        keep = (Ltr["y"] > 0).to_numpy() | (np.random.default_rng(SEED).random(len(Ltr)) < NEG_RATE)
        cat = [m.fcols.index(c) for c in ds.categorical + ["product"]]
        dtr = lgb.Dataset(_X(Ltr, m.fcols)[keep], Ltr["y"].to_numpy(float)[keep], feature_name=m.fcols,
                          categorical_feature=cat, free_raw_data=False).construct()
        dva = lgb.Dataset(_X(Lva, m.fcols), Lva["y"].to_numpy(float), feature_name=m.fcols, categorical_feature=cat,
                          reference=dtr, free_raw_data=False).construct()
        m.booster = lgb.train({**PARAMS, **(params or {})}, dtr, 3000, valid_sets=[dva],
                              callbacks=[lgb.early_stopping(100, verbose=False)])
        log(f"  {ds.domain.name}: multitask best_iter={m.booster.best_iteration} "
            f"valid_ap={m.booster.best_score['valid_0']['average_precision']:.4f}")
        # hiệu chỉnh isotonic theo sản phẩm trên VALID (chỉ dòng đủ điều kiện)
        pv = m._raw(Lva)
        for j, c in enumerate(m.codes):
            sel = (Lva["product"] == j).to_numpy()
            m.calibrators[c] = Isotonic().fit(pv[sel], Lva["y"].to_numpy(float)[sel])
        m.meta = {"model": "lgb_multitask", "domain": ds.domain.name, "best_iter": m.booster.best_iteration,
                  "valid_ap": float(m.booster.best_score["valid_0"]["average_precision"]), "neg_rate": NEG_RATE,
                  "params": {**PARAMS, **(params or {})}, "n_train_rows": int(keep.sum()), "n_valid_rows": len(Lva),
                  "train_months": [str(tr["month"].min()), str(tr["month"].max())],
                  "valid_months": [str(va["month"].min()), str(va["month"].max())],
                  "trained_at": time.strftime("%Y-%m-%dT%H:%M:%S"), "train_seconds": round(time.time() - t0, 1)}
        return m

    # ------------------------------------------------------------------ dự đoán
    def _raw(self, L: pd.DataFrame) -> np.ndarray:
        p = np.clip(self.booster.predict(_X(L, self.fcols), num_iteration=self.booster.best_iteration), 1e-12, 1 - 1e-12)
        o = p / (1 - p) * self.meta.get("neg_rate", NEG_RATE)
        return o / (1 + o)

    def predict(self, ds: Dataset, d: pd.DataFrame, calibrated: bool = True) -> np.ndarray:
        """Ma trận (n, K): xác suất mua; sản phẩm không đủ điều kiện = 0."""
        L = to_long(ds, d)
        raw = self._raw(L)
        P = np.zeros((len(d), len(self.codes)))
        for j, c in enumerate(self.codes):
            sel = (L["product"] == j).to_numpy()
            v = raw[sel]
            P[L["_row"].to_numpy()[sel], j] = self.calibrators[c](v) if calibrated else v
        return P

    def contributions(self, ds: Dataset, d: pd.DataFrame, product_idx: np.ndarray, top: int = 3) -> List[List[tuple]]:
        """SHAP: với mỗi dòng, `top` đặc trưng đẩy điểm của sản phẩm product_idx[i] lên nhiều nhất [(tên, đóng góp)]."""
        L = to_long(ds, d, only_eligible=False)
        n = len(d)
        rows = L.iloc[product_idx * n + np.arange(n)]     # to_long(only_eligible=False) xếp theo sản phẩm rồi dòng
        C = self.booster.predict(_X(rows, self.fcols), num_iteration=self.booster.best_iteration, pred_contrib=True)[:, :-1]
        out = []
        for i in range(n):
            idx = np.argsort(-C[i])[:top]
            out.append([(self.fcols[k], float(C[i, k])) for k in idx if C[i, k] > 0])
        return out

    # ------------------------------------------------------------------ lưu / nạp (registry)
    def save(self, root: pathlib.Path, version: str, metrics: Optional[Dict] = None) -> pathlib.Path:
        out = root / self.domain / version
        out.mkdir(parents=True, exist_ok=True)
        self.booster.save_model(str(out / "model.lgb.txt"), num_iteration=self.booster.best_iteration)
        card = {**self.meta, "version": version, "codes": self.codes, "features": self.features,
                "own_names": self.own_names, "categorical": self.categorical,
                "calibrators": {c: {"x": k.x, "y": k.y} for c, k in self.calibrators.items()}, "metrics": metrics or {}}
        (out / "model_card.json").write_text(json.dumps(card, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
        (root / self.domain / "CHAMPION").write_text(version, encoding="utf-8")
        return out

    @classmethod
    def load(cls, root: pathlib.Path, domain: str, version: Optional[str] = None) -> "Ranker":
        version = version or (root / domain / "CHAMPION").read_text(encoding="utf-8").strip()
        d = root / domain / version
        card = json.loads((d / "model_card.json").read_text(encoding="utf-8"))
        m = cls(domain, card["codes"], card["features"], card["own_names"], card["categorical"],
                booster=lgb.Booster(model_file=str(d / "model.lgb.txt")),
                calibrators={c: Isotonic(v["x"], v["y"]) for c, v in card["calibrators"].items()}, meta=card)
        m.booster.best_iteration = 0       # file đã cắt đúng best_iteration khi lưu
        return m
