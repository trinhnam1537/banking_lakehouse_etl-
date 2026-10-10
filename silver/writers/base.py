"""Giao diện chung của writer.

Mọi writer tuân hai luật:

  1. IDEMPOTENT: chạy lại cùng một đầu vào (cùng manifest / cùng ngày) cho ra
     cùng trạng thái bảng. SFN retry, SQS giao lại, người bấm chạy lại — cả
     ba đều xảy ra thật, và không cái nào được phép nhân đôi dữ liệu.
  2. Trả số liệu (inserted/updated/deleted/...) để ghi vào job_log — "chạy
     xong" mà không biết đã đổi bao nhiêu dòng thì không điều tra được gì.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict

from ..config import SilverConfig


class Writer(ABC):
    def __init__(self, spark, cfg: SilverConfig, source_label: str):
        self.spark = spark
        self.cfg = cfg
        self.table = cfg.target.fqn
        self.source_label = source_label

    @abstractmethod
    def write(self, df) -> Dict[str, int]:
        """Ghi `df` (đã có cột kỹ thuật, đã khử trùng lặp) vào bảng đích."""

    # ---------- tien ich ----------

    def sql(self, text: str):
        return self.spark.sql(text)

    def _lit(self, s: str) -> str:
        return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _counts_by_status(self, df) -> Dict[str, int]:
        rows = df.groupBy("ds_record_status").count().collect()
        m = {r["ds_record_status"]: r["count"] for r in rows}
        return {"inserted": m.get("I", 0), "updated": m.get("U", 0),
                "deleted": m.get("D", 0), "unchanged": m.get("N", 0)}
