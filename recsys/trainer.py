"""Huấn luyện + đánh giá chung cho mọi bộ dữ liệu "wide": mỗi dòng (khách, tháng), một cột nhãn mỗi sản phẩm.

Mô hình: một LightGBM nhị phân cho mỗi sản phẩm (propensity), dừng sớm trên tập valid theo PR-AUC (nhãn rất hiếm,
ROC-AUC dễ đẹp giả). Điểm các sản phẩm được so trực tiếp để xếp hạng cho từng khách: mỗi mô hình ước xác suất
P(mua p | khách, tháng) nên cùng thang đo, không cần hiệu chỉnh thêm khi chỉ cần thứ tự.

Đối chứng (baseline), bắt buộc phải thắng mới có giá trị:
  popularity   tỉ lệ mua của sản phẩm trên tập train (mọi khách cùng một thứ tự)
  repeat       luật hành vi: mua tháng trước > số lần đã mua > độ phổ biến (tận dụng tính lặp lại rất mạnh)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import numpy as np

from . import metrics as M


@dataclass
class Spec:
    name: str
    products: List[str]                  # ten san pham (hien thi)
    label_cols: List[str]                # cot nhan theo cung thu tu
    feature_cols: List[str]
    split_col: str = "split"
    categorical: List[str] = field(default_factory=list)
    eligible_cols: Optional[List[str]] = None   # cot bool: san pham con duoc goi y (chua giu); None = tat ca
    repeat_score: Optional[Callable] = None     # f(df) -> ma tran diem (n, n_sp) cho baseline repeat


def _lgb_params(seed: int) -> Dict:
    return {"objective": "binary", "learning_rate": 0.05, "num_leaves": 31, "min_data_in_leaf": 200,
            "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 1.0,
            "metric": "average_precision", "verbosity": -1, "seed": seed}


def fit(spec: Spec, df, seed: int = 7, max_rounds: int = 2000, neg_rate: float = 1.0) -> Dict:
    """neg_rate < 1: lay mau am tren TRAIN (giu moi duong). Cung ti le cho moi san pham nen odds bi nhan cung mot hang
    so -> thu tu xep hang trong mot san pham va giua cac san pham khong doi; valid/test giu nguyen."""
    import lightgbm as lgb

    tr, va, te = (df[df[spec.split_col] == s] for s in ("train", "valid", "test"))
    # Cot phan loai phai duoc ma hoa so truoc (spec.categorical = ten cot da la ma so nguyen).
    # ascontiguousarray: to_numpy co the tra mang thu tu Fortran (tuy cach pandas xep khoi cot); LightGBM 4.7 tren
    # Windows doc mang khong C-contiguous thi crash "access violation" o set_label (do 11-18).
    X = lambda d: np.ascontiguousarray(d[spec.feature_cols].to_numpy(dtype=np.float32, na_value=np.nan))  # noqa: E731
    Yv = lambda d, c: d[c].to_numpy(dtype=np.float64)  # noqa: E731
    cat_idx = [spec.feature_cols.index(c) for c in spec.categorical]
    models, imp = {}, {}
    S_te = np.zeros((len(te), len(spec.products)))
    Xtr, Xva, Xte = X(tr), X(va), X(te)
    for j, (p, y) in enumerate(zip(spec.products, spec.label_cols)):
        ytr, yva = Yv(tr, y), Yv(va, y)
        keep = (ytr > 0) | (np.random.default_rng(seed + j).random(len(ytr)) < neg_rate)
        vkeep = np.ones(len(yva), bool)
        if spec.eligible_cols:   # chi hoc tren dong san pham CON duoc goi y: dong da giu luon nhan 0, hoc vo nghia
            keep &= tr[spec.eligible_cols[j]].to_numpy(bool)
            vkeep = va[spec.eligible_cols[j]].to_numpy(bool)
        dtr = lgb.Dataset(Xtr[keep], ytr[keep], feature_name=list(spec.feature_cols), categorical_feature=cat_idx or "auto",
                          free_raw_data=False).construct()
        dva = lgb.Dataset(Xva[vkeep], yva[vkeep], reference=dtr, free_raw_data=False).construct()
        m = lgb.train(_lgb_params(seed), dtr, max_rounds, valid_sets=[dva],
                      callbacks=[lgb.early_stopping(100, verbose=False)])
        models[p] = m
        S_te[:, j] = m.predict(Xte, num_iteration=m.best_iteration)
        g = m.feature_importance("gain")
        imp[p] = dict(sorted(zip(spec.feature_cols, (g / max(g.sum(), 1e-9)).round(4)), key=lambda kv: -kv[1])[:10])
        print(f"  {spec.name}/{p}: best_iter={m.best_iteration} valid_ap={m.best_score['valid_0']['average_precision']:.4f}")

    Y_te = te[spec.label_cols].astype(int).to_numpy()
    elig = te[spec.eligible_cols].astype(bool).to_numpy() if spec.eligible_cols else None
    pop = tr[spec.label_cols].astype(float).mean().to_numpy()
    scores = {"popularity": np.tile(pop, (len(te), 1)), "lightgbm": S_te}
    if spec.repeat_score is not None:
        scores["repeat"] = spec.repeat_score(te) + pop * 1e-3   # pha hoa bang do pho bien
    res = {"dataset": spec.name, "n_train": len(tr), "n_valid": len(va), "n_test": len(te),
           "per_product": [], "ranking": [], "importance": imp}
    for mname, S in scores.items():
        for j, p in enumerate(spec.products):
            mask = elig[:, j] if elig is not None else np.ones(len(te), bool)
            res["per_product"].append({"model": mname, "product": p,
                                       **M.per_product(Y_te[mask, j], S[mask, j])})
        res["ranking"].append({"model": mname, **M.ranking(Y_te, S, elig, k=min(3, len(spec.products)))})
    res["models"] = models
    return res


def report(res: Dict) -> str:
    rk = res["ranking"]
    kcol = next(k for k in rk[0] if k.startswith("map@"))
    pp = [r for r in res["per_product"] if r["model"] != "popularity"]
    out = [f"### {res['dataset']}", "",
           f"Train {res['n_train']:,} / valid {res['n_valid']:,} / test {res['n_test']:,} dong.", "",
           "**Xep hang san pham cho tung khach** (dong test co >= 1 san pham duoc mua):", "",
           M.table(rk, ["model", "rows", "hit@1", "mrr", kcol]), "",
           "**Nham khach theo tung san pham** (tap test):", "",
           M.table(pp, ["model", "product", "pos", "rate", "roc_auc", "pr_auc", "lift@1%", "recall@5%"]), "",
           "**Dac trung quan trong nhat (ty le gain, LightGBM)**:", ""]
    for p, d in res["importance"].items():
        out.append(f"- {p}: " + ", ".join(f"{k} {v:.2f}" for k, v in list(d.items())[:6]))
    return "\n".join(out) + "\n"
