"""Giao diện chung của mọi reader.

Toàn bộ khác biệt giữa REST API, Postgres, Kafka và file S3 được gói sau đúng
một phương thức: `read()`. Runner không biết nó đang đọc gì — nhờ vậy thêm một
loại nguồn mới chỉ là thêm một lớp con, không đụng tới luồng chính.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

from ..config import CdcMode, SourceConfig
from ..state import Checkpoint


@dataclass
class Batch:
    """Một lô bản ghi kèm mốc mới.

    `watermark` là mốc SAU KHI đã đọc hết lô này. Runner chỉ ghi nó xuống
    DynamoDB sau khi lô đã nằm an toàn trên S3 — thứ tự đó là điều kiện để
    job có thể chạy lại sau khi chết mà không mất dữ liệu.

    `partition` cho phép một job có nhiều mốc độc lập (mỗi partition Kafka một
    mốc). Nguồn API và JDBC luôn dùng "default".
    """

    records: List[Dict[str, Any]]
    watermark: Optional[str] = None
    partition: str = "default"
    meta: Dict[str, Any] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.records)


def _as_utc(value: str) -> Optional[dt.datetime]:
    """Chuỗi ISO 8601 -> datetime UTC; không múi giờ thì coi là UTC. Lỗi -> None."""
    try:
        ts = dt.datetime.fromisoformat(value.replace("Z", "+00:00").replace(" ", "T", 1))
    except ValueError:
        return None
    return ts.replace(tzinfo=dt.timezone.utc) if ts.tzinfo is None else ts


class Reader(ABC):
    """Lớp cha của mọi reader.

    Hợp đồng:
      - CDC1: dùng `checkpoint` để chỉ lấy phần mới. Trả `watermark` trong
              mỗi Batch. Nếu không có gì mới, không yield lô nào.
      - CDC5: **bỏ qua** `checkpoint` hoàn toàn, kéo trọn bộ. `watermark`
              để None. Runner sẽ ghi đè partition raw thay vì append.
    """

    def __init__(self, cfg: SourceConfig):
        self.cfg = cfg
        self.is_full_snapshot = cfg.cdc.mode is CdcMode.CDC5

    @abstractmethod
    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        """Sinh ra từng lô bản ghi.

        Dùng generator chứ không trả về list: nguồn có thể lớn hơn bộ nhớ, và
        runner cần ghi từng lô xuống S3 ngay khi đọc xong thay vì gom hết rồi
        mới ghi.
        """
        raise NotImplementedError

    def close(self) -> None:
        """Đóng kết nối. Mặc định không làm gì."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    # ---------- tiện ích dùng chung cho lớp con ----------

    def effective_since(
        self, checkpoint: Optional[Checkpoint], partition: str = "default"
    ) -> Optional[str]:
        """Mốc thực sự dùng để lọc, đã trừ lookback.

        `lookback_seconds` cho phép kéo chồng lấn một khoảng để bắt bản ghi
        đến muộn. Cái giá là bản ghi trùng — tầng bronze phải khử trùng lặp
        theo khoá nghiệp vụ. Đánh đổi này nên nêu rõ trong luận văn: chồng lấn
        đổi lấy khử trùng lặp, so với không chồng lấn mà chấp nhận sót.
        """
        if self.is_full_snapshot:
            return None

        raw = None
        if checkpoint is not None:
            raw = checkpoint.get(partition)
        if raw is None:
            raw = self.cfg.cdc.initial_watermark
        if raw is None:
            return None

        lb = self.cfg.cdc.lookback_seconds
        if lb <= 0 or self.cfg.cdc.watermark_type != "timestamp":
            return raw

        base = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if base.tzinfo is None:
            base = base.replace(tzinfo=dt.timezone.utc)
        return (base - dt.timedelta(seconds=lb)).isoformat(timespec="seconds")

    def max_watermark(self, current: Optional[str], value: Any) -> Optional[str]:
        """Mốc lớn hơn trong hai giá trị, trả về dạng chuỗi.

        So sánh theo `watermark_type`, không so chuỗi trần: với mốc `int`,
        chuỗi "9" > "10" nên mốc sẽ **đi lùi** và job kéo lại dữ liệu cũ mãi
        mà không báo lỗi gì. Timestamp ISO cùng định dạng thì so chuỗi đúng.
        """
        if value is None:
            return current
        sv = value.isoformat() if hasattr(value, "isoformat") else str(value)
        if current is None:
            return sv
        if self.cfg.cdc.watermark_type == "int":
            return sv if int(sv) > int(current) else current
        if self.cfg.cdc.watermark_type == "timestamp":
            # So theo THỜI ĐIỂM, không so chuỗi: driver trả datetime không múi giờ
            # ("2026-09-02T03:00:00") còn mốc khởi tạo có "+00:00"; so chuỗi giữa
            # hai định dạng, hoặc giữa hai múi giờ, cho kết quả sai.
            a, b = _as_utc(sv), _as_utc(current)
            if a is not None and b is not None:
                return sv if a > b else current
        return sv if sv > current else current

    def stamp(self, rec: Dict[str, Any]) -> Dict[str, Any]:
        """Gắn metadata truy vết vào từng bản ghi.

        Tầng raw giữ nguyên hình dạng dữ liệu nguồn, nhưng bốn trường `_src_*`
        này là ngoại lệ có chủ ý: không có chúng thì khi bronze phát hiện bản
        ghi lạ, không cách nào lần ngược về job và lần chạy đã sinh ra nó.
        """
        rec["_src_job"] = self.cfg.name
        rec["_src_type"] = self.cfg.source_type.value
        rec["_src_cdc"] = self.cfg.cdc.mode.value
        rec["_src_ingest_ts"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        return rec
