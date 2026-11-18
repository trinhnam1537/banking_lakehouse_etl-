"""Chỉ số đánh giá gợi ý sản phẩm: theo SẢN PHẨM (nhắm khách cho một chiến dịch) và theo KHÁCH (xếp hạng sản phẩm).

Hai góc nhìn vì hai cách dùng của ngân hàng:
  - Chiến dịch một sản phẩm: chọn top k% khách có điểm cao nhất -> ROC-AUC, PR-AUC, lift@k%, recall@k%.
  - Gợi ý cho từng khách (RM, app): xếp hạng các sản phẩm khách CHƯA mua -> Hit@1, MRR, MAP@K, chỉ tính trên
    các (khách, tháng) có ít nhất một sản phẩm dương (giống MAP@7 của Santander Product Recommendation).
"""

from __future__ import annotations

from typing import Dict, List

import numpy as np


def per_product(y: np.ndarray, s: np.ndarray, ks=(0.01, 0.05)) -> Dict[str, float]:
    from sklearn.metrics import average_precision_score, roc_auc_score

    out = {"n": int(len(y)), "pos": int(y.sum()), "rate": float(y.mean())}
    if 0 < y.sum() < len(y):
        out["roc_auc"] = float(roc_auc_score(y, s))
        out["pr_auc"] = float(average_precision_score(y, s))
    order = np.argsort(-s, kind="stable")
    for k in ks:
        top = order[: max(1, int(round(k * len(y))))]
        hit = y[top].sum()
        out[f"lift@{k:.0%}"] = float(hit / len(top) / y.mean()) if y.mean() > 0 else float("nan")
        out[f"recall@{k:.0%}"] = float(hit / y.sum()) if y.sum() > 0 else float("nan")
    return out


def ranking(Y: np.ndarray, S: np.ndarray, eligible: np.ndarray | None = None, k: int = 3) -> Dict[str, float]:
    """Y, S: (n_dong, n_san_pham). eligible: True = san pham duoc phep goi y (vd chua giu). Chi tinh dong co >= 1 duong."""
    S = S.astype(float).copy()
    if eligible is not None:
        S[~eligible] = -np.inf
        Y = Y * eligible
    rows = Y.sum(1) > 0
    Y, S = Y[rows], S[rows]
    if len(Y) == 0:
        return {"rows": 0}
    order = np.argsort(-S, axis=1, kind="stable")
    rel = np.take_along_axis(Y, order, axis=1)
    hit1 = rel[:, 0].mean()
    first = np.argmax(rel > 0, axis=1)
    mrr = (1.0 / (first + 1)).mean()
    kk = min(k, Y.shape[1])
    prec_at = np.cumsum(rel[:, :kk], axis=1) / np.arange(1, kk + 1)
    ap = (prec_at * rel[:, :kk]).sum(1) / np.minimum(Y.sum(1), kk)
    return {"rows": int(len(Y)), "hit@1": float(hit1), "mrr": float(mrr), f"map@{kk}": float(ap.mean())}


def table(rows: List[Dict], cols: List[str]) -> str:
    def f(v):
        if isinstance(v, float):
            return f"{v:.4f}" if abs(v) < 10 else f"{v:,.1f}"
        return str(v)
    return "\n".join(["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
                     + ["| " + " | ".join(f(r.get(c, "")) for c in cols) + " |" for r in rows]) + "\n"
