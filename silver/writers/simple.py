"""Simple writer — không lịch sử phiên bản, ba chế độ.

  append               thêm dòng. Idempotent theo ds_source: trước khi thêm,
                       xoá dòng của CHÍNH manifest này (nếu lần trước đã ghi dở
                       hay SFN retry). Hợp cho bảng sự kiện: lệnh khớp, tin tức.
  overwrite_partition  ghi đè đúng các partition có trong lô (dynamic
                       overwrite của Iceberg). Hợp cho snapshot theo ngày khi
                       không cần trạng thái I/U/D — ticker 24h, frames SEC.
  overwrite_table      thay cả bảng. Hợp cho bảng tham chiếu nhỏ: mã quốc gia,
                       loại hình pháp lý. Lô RỖNG bị từ chối — một API trả
                       mảng rỗng không được phép xoá sạch bảng tham chiếu.
"""

from __future__ import annotations

from typing import Dict

from ..config import SimpleMode
from .base import Writer


class SimpleWriter(Writer):
    def write(self, df) -> Dict[str, int]:
        from pyspark.sql import functions as F

        mode = self.cfg.writer.mode
        n = df.count()

        if mode is SimpleMode.APPEND:
            # DELETE truoc roi APPEND: hai commit Iceberg. Chet giua hai buoc thi
            # lan chay lai xoa (khong co gi) roi ghi lai — van dung.
            self.sql(f"DELETE FROM {self.table} WHERE ds_source = {self._lit(self.source_label)}")
            if n:
                df.writeTo(self.table).append()
            return {"inserted": n}

        if n == 0:
            if mode is SimpleMode.OVERWRITE_TABLE:
                raise RuntimeError(f"[{self.cfg.name}] tu choi overwrite_table voi lo RONG "
                                   f"(se xoa sach bang {self.table})")
            return {"inserted": 0}

        if mode is SimpleMode.OVERWRITE_PARTITION:
            df.writeTo(self.table).overwritePartitions()
        else:
            df.writeTo(self.table).overwrite(F.lit(True))
        return {"inserted": n}
