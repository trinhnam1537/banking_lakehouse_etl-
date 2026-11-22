"""Nạp dữ liệu cho hệ gợi ý v2: cùng một hợp đồng cho mọi miền.

Mỗi miền trả về `Dataset`: DataFrame một dòng (khách, tháng) gồm
  - cột nhãn  `domain.labels`      (mua sản phẩm trong cửa sổ sau tháng chụp; đã chống rò rỉ ở SQL/transform)
  - cột đủ điều kiện `elig_<code>` (True = sản phẩm còn được gợi ý: khách chưa giữ)
  - cột đặc trưng `features`, cột `split` (train/valid/test theo THỜI GIAN), cột tháng `month`
  - `own`: với mỗi sản phẩm, tên đặc trưng lịch sử của CHÍNH sản phẩm đó (cho mô hình multi-task dạng dài)
  - `segments`: cột phân khúc để đánh giá công bằng / ổn định

Dữ liệu đọc qua Athena DƯỚI PERSONA (recsys.data.athena_df, có cache theo băm câu SQL), không đọc thẳng S3.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import numpy as np
import pandas as pd

from .config import BERKA, DOMAINS, MBD, Domain

OWN_SUFFIX = ("lag1", "lag2", "lag3", "cnt", "since", "rate", "ever", "recent3")


@dataclass
class Dataset:
    domain: Domain
    df: pd.DataFrame
    features: List[str]
    own: Dict[str, Dict[str, str]]                 # code -> {own_<s>: cột}
    categorical: List[str] = field(default_factory=list)
    segments: Dict[str, str] = field(default_factory=dict)    # tên phân khúc -> cột
    id_col: str = "client_id"
    month_col: str = "month"

    @property
    def own_names(self) -> List[str]:
        return sorted({k for m in self.own.values() for k in m})

    def split(self, name: str) -> pd.DataFrame:
        return self.df[self.df["split"] == name]


def load_mbd(use_cache: bool = True) -> Dataset:
    from recsys import mbd
    from recsys.bench.features import FEATURES_V2, add_v2
    from recsys.data import athena_df

    df = add_v2(athena_df(mbd.FEATURE_SQL, use_cache=use_cache))
    df["month"] = df["report_month"]
    for c in MBD.codes:
        df[f"elig_{c}"] = True                      # MBD: sản phẩm mua lặp được, luôn đủ điều kiện
    own = {c: {f"own_{s}": f"t{c}_{s}" for s in OWN_SUFFIX} for c in MBD.codes}
    df["seg_activity"] = np.where(df["trx_m"] > 0, "co giao dich thang nay", "khong giao dich")
    df["seg_history"] = np.where(df["any_cnt"] > 0, "da tung mua", "chua tung mua")
    df["seg_tenure"] = pd.cut(df["midx"], [-1, 2, 5, 8, 99], labels=["0-2 thang", "3-5", "6-8", "9+"]).astype(str)
    return Dataset(MBD, df, list(FEATURES_V2), own,
                   segments={"hoat dong": "seg_activity", "lich su mua": "seg_history", "tham nien": "seg_tenure"})


def load_berka(use_cache: bool = True) -> Dataset:
    from recsys import berka
    from recsys.data import athena_df

    df = athena_df(berka.SQL, use_cache=use_cache)
    df["region_code"] = df["region"].astype("category").cat.codes
    for p in berka.PRODUCTS:
        df[f"elig_{p}"] = df[f"elig_{p}"].astype(bool)
    keep = BERKA.codes
    df = df[df[[f"elig_{c}" for c in keep]].any(axis=1)].reset_index(drop=True)
    df["seg_tenure"] = pd.cut(df["tenure_months"], [0, 6, 12, 24, 999],
                              labels=["<=6 thang", "7-12", "13-24", ">24"]).astype(str)
    df["seg_gender"] = np.where(df["is_male"] == 1, "nam", "nu")
    df["seg_region"] = df["region"].astype(str)
    df["seg_balance"] = pd.qcut(df["avg_balance_3m"].rank(method="first"), 3, labels=["thap", "vua", "cao"]).astype(str)
    return Dataset(BERKA, df, list(berka.FEATURES), {c: {} for c in keep}, categorical=["region_code"],
                   segments={"tham nien": "seg_tenure", "gioi tinh": "seg_gender", "vung": "seg_region",
                             "so du": "seg_balance"})


def load(domain: str, use_cache: bool = True) -> Dataset:
    if domain not in DOMAINS:
        raise ValueError(f"mien {domain!r} khong co; chon {sorted(DOMAINS)}")
    return {"mbd": load_mbd, "berka": load_berka}[domain](use_cache)


def matrices(ds: Dataset, d: pd.DataFrame):
    """(Y, E): nhãn và đủ điều kiện dạng ma trận (n, K) theo thứ tự domain.codes."""
    Y = d[ds.domain.labels].fillna(0).astype(int).to_numpy()
    E = d[[f"elig_{c}" for c in ds.domain.codes]].astype(bool).to_numpy()
    return Y * E, E
