"""Phục vụ gợi ý (batch): chấm điểm tháng báo cáo MỚI NHẤT bằng mô hình đã lưu, trả top-k sản phẩm cho từng khách kèm
LÝ DO (3 đặc trưng đóng góp nhiều nhất, SHAP của LightGBM: pred_contrib).

    python -m recsys.recommend --k 2                 # MBD: ghi recsys/reports/mbd_topk_<thang>.parquet
    from recsys.recommend import recommend_mbd       # dung trong portal / agent: recommend_mbd([client_id, ...], k=2)

Mô hình lấy từ recsys/reports/mbd_<sp>.lgb.txt (sinh bởi `python -m recsys.mbd`). Điểm là xác suất ĐÃ BỊ LỆCH do lấy mẫu
âm 25% khi huấn luyện (odds x4): dùng để XẾP HẠNG, không đọc như xác suất tuyệt đối. Cột `p_calibrated` hiệu chỉnh lại
odds theo tỉ lệ lấy mẫu.
"""

from __future__ import annotations

import argparse
import pathlib
from typing import Iterable, List, Optional

import numpy as np

from . import mbd
from .data import athena_df

REPORTS = pathlib.Path(__file__).resolve().parent / "reports"
NEG_RATE = 0.25
REASON_VI = {
    "lag1": "đã mua tháng trước", "cnt": "số lần đã mua", "since": "số tháng từ lần mua cuối",
    "trx_m": "số giao dịch tháng này", "trx_3m": "số giao dịch 3 tháng", "trx_m1": "số giao dịch tháng trước",
    "trx_share_m": "tỉ trọng giao dịch tháng này", "trx_per_month": "giao dịch trung bình/tháng", "dtd": "khoảng ngày giao dịch",
    "any_cnt": "tổng số lần mua mọi sản phẩm", "midx": "thâm niên quan sát (tháng)", "moy": "tháng trong năm",
    "tc": "tổng giao dịch luỹ kế", "lag2": "đã mua 2 tháng trước", "lag3": "đã mua 3 tháng trước",
}


def _reason(name: str) -> str:
    key = name.split("_", 1)[1] if name[:2] in ("t1", "t2", "t3", "t4") and name[2] == "_" else name
    pre = f"[{name[:2].replace('t', 'sp')}] " if key != name else ""
    return pre + REASON_VI.get(key, name)


def load_models():
    import lightgbm as lgb

    return {p: lgb.Booster(model_file=str(REPORTS / f"mbd_{p}.lgb.txt")) for p in mbd.PRODUCTS}


def score(df, models, k: int = 2):
    import pandas as pd

    X = np.ascontiguousarray(df[mbd.FEATURES].to_numpy(dtype=np.float32, na_value=np.nan))
    P = np.column_stack([models[p].predict(X) for p in mbd.PRODUCTS])
    contrib = {p: models[p].predict(X, pred_contrib=True)[:, :-1] for p in mbd.PRODUCTS}
    order = np.argsort(-P, axis=1)[:, :k]
    rows = []
    for i in range(len(df)):
        for rank, j in enumerate(order[i], 1):
            p = mbd.PRODUCTS[j]
            top = np.argsort(-contrib[p][i])[:3]
            odds = P[i, j] / max(1 - P[i, j], 1e-9) * NEG_RATE
            rows.append({"client_id": df.client_id.iat[i], "report_month": df.report_month.iat[i], "rank": rank,
                         "product": p, "score": float(P[i, j]), "p_calibrated": float(odds / (1 + odds)),
                         "reasons": "; ".join(_reason(mbd.FEATURES[t]) for t in top if contrib[p][i, t] > 0)})
    return pd.DataFrame(rows)


def latest_features():
    df = athena_df(mbd.FEATURE_SQL)
    return df[df.report_month == df.report_month.max()].reset_index(drop=True)


def recommend_mbd(client_ids: Optional[Iterable] = None, k: int = 2):
    df = latest_features()
    if client_ids is not None:
        df = df[df.client_id.isin(list(client_ids))].reset_index(drop=True)
    return score(df, load_models(), k)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Goi y top-k cho thang bao cao moi nhat (MBD)")
    ap.add_argument("--k", type=int, default=2)
    a = ap.parse_args(argv)
    out = recommend_mbd(k=a.k)
    m = out.report_month.iat[0]
    path = REPORTS / f"mbd_top{a.k}_{m}.parquet"
    out.to_parquet(path, index=False)
    print(f"{out.client_id.nunique():,} khach, {len(out):,} goi y -> {path}")
    print(out.sort_values("p_calibrated", ascending=False).head(8).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
