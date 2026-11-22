"""Đặc trưng v2 cho MBD-mini: tính bằng pandas TRÊN bảng đặc trưng gốc của recsys.mbd (không thêm truy vấn Athena).

Quy tắc chống rò rỉ (giống recsys.mbd): dòng (khách, m) chỉ được dùng
  - hoạt động giao dịch tới hết tháng m (trans_count luỹ kế, diff_trans_date của chính dòng m),
  - nhãn của các dòng TRƯỚC m (t{k} của dòng m-1 là việc mua trong tháng m, đã biết vào ngày báo cáo m).
Nhãn t{k} của chính dòng m (mua trong tháng m+1) KHÔNG bao giờ vào đặc trưng. Mọi phép "shift" dưới đây đều
theo khách và theo thứ tự report_month; rolling chỉ nhìn về quá khứ.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from recsys import mbd

K = range(1, 5)


def add_v2(df: pd.DataFrame) -> pd.DataFrame:
    d = df.sort_values(["client_id", "report_month"]).reset_index(drop=True).copy()
    g = d.groupby("client_id", sort=False)

    # --- hoạt động giao dịch: thêm độ trễ, trung bình/độ lệch trượt, xu hướng, chuỗi tháng không giao dịch
    for l in (2, 3):
        d[f"trx_m{l}"] = g["trx_m"].shift(l)
    roll = lambda s, w, f: s.rolling(w, min_periods=1).agg(f)  # noqa: E731  (cửa sổ gồm tháng m và trước đó)
    d["trx_mean3"] = g["trx_m"].transform(lambda s: roll(s, 3, "mean"))
    d["trx_std3"] = g["trx_m"].transform(lambda s: roll(s, 3, "std"))
    d["trx_mean6"] = g["trx_m"].transform(lambda s: roll(s, 6, "mean"))
    d["trx_max6"] = g["trx_m"].transform(lambda s: roll(s, 6, "max"))
    prev3 = g["trx_m"].transform(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
    d["trx_trend"] = d["trx_m"] - prev3                      # tháng này so với TB 3 tháng trước
    d["trx_ratio_mean"] = d["trx_m"] / (d["trx_mean6"] + 1.0)
    zero = (d["trx_m"] <= 0).astype(int)
    # độ dài chuỗi tháng liên tiếp không có giao dịch, kết thúc ở m
    d["zero_streak"] = zero.groupby((zero != zero.shift()).cumsum()).cumsum() * zero
    d.loc[g.cumcount() == 0, "zero_streak"] = zero[g.cumcount() == 0]
    d["dtd_delta"] = d["dtd"] - g["dtd"].shift(1)
    d["log_tc"] = np.log1p(d["tc"].clip(lower=0))

    # --- lịch sử mua (chỉ từ nhãn các dòng trước m: các cột lag/cnt/since của mbd đã đúng mốc)
    for k in K:
        d[f"t{k}_rate"] = d[f"t{k}_cnt"] / d["midx"].where(d["midx"] > 0)
        d[f"t{k}_ever"] = (d[f"t{k}_cnt"] > 0).astype(float)
        d[f"t{k}_recent3"] = d[[f"t{k}_lag1", f"t{k}_lag2", f"t{k}_lag3"]].fillna(0).sum(1)
    d["any_lag1"] = d[[f"t{k}_lag1" for k in K]].fillna(0).max(1)
    d["any_recent3"] = d[[f"t{k}_recent3" for k in K]].sum(1)
    d["any_since"] = d[[f"t{k}_since" for k in K]].min(1)
    d["n_products_ever"] = d[[f"t{k}_ever" for k in K]].sum(1)
    d["any_rate"] = d["any_cnt"] / d["midx"].where(d["midx"] > 0)
    return d


V2_EXTRA = (["trx_m2", "trx_m3", "trx_mean3", "trx_std3", "trx_mean6", "trx_max6", "trx_trend", "trx_ratio_mean",
             "zero_streak", "dtd_delta", "log_tc", "any_lag1", "any_recent3", "any_since", "n_products_ever", "any_rate"]
            + [f"t{k}_{s}" for k in K for s in ("rate", "ever", "recent3")])
FEATURES_V2 = list(mbd.FEATURES) + V2_EXTRA
LABELS = [f"t{k}" for k in K]


def check_no_leak(d: pd.DataFrame) -> None:
    """Kiểm tra tự động: không đặc trưng nào tương quan gần tuyệt đối với nhãn của chính dòng (dấu hiệu rò rỉ),
    và lag1 của dòng m đúng bằng nhãn của dòng m-1 cùng khách."""
    s = d.sort_values(["client_id", "report_month"])
    for k in K:
        prev = s.groupby("client_id")[f"t{k}"].shift(1)
        both = prev.notna() & s[f"t{k}_lag1"].notna()
        assert (prev[both] == s.loc[both, f"t{k}_lag1"]).all(), f"t{k}_lag1 lech mot dong"
        for c in FEATURES_V2:
            x = s[c].astype(float)
            ok = x.notna()
            if x[ok].std() > 0:
                r = np.corrcoef(x[ok], s.loc[ok, f"t{k}"].astype(float))[0, 1]
                assert abs(r) < 0.5, f"{c} tuong quan {r:.2f} voi nhan t{k}: nghi ro ri"
