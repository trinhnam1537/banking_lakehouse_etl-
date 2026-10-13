"""Reader cho file do bên ngoài đẩy vào S3.

Đây là nhánh batch: ai đó (đối tác, file EOD, tải tay) đặt file vào một prefix,
job này quét và nạp vào raw.

Mốc CDC1 ở đây là **khoá S3 của file cuối cùng đã xử lý**, không phải thời
gian. Lý do: `LastModified` có thể trùng nhau ở mức giây khi upload hàng loạt,
còn khoá thì duy nhất và `list_objects_v2` trả về theo thứ tự từ điển — nên
"đã xử lý tới khoá X" là một mốc chặt chẽ và tiếp tục được.

Đánh đổi: file có khoá đứng trước X mà upload muộn sẽ bị bỏ sót. Với bố cục
prefix theo ngày (`.../dt=2026-09-26/...`) thì điều đó hầu như không xảy ra;
nếu nguồn không bảo đảm được, dùng luồng Lambda theo sự kiện S3 thay vì quét.

File được đọc THEO LUỒNG (`fileparse.iter_records`) và chia lô `batch_size`:
không file nào phải nằm trọn trong RAM. Chỉ lô CUỐI của mỗi file mang mốc, nên
checkpoint không bao giờ ghi nhận một file mới đọc được một nửa.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional

import boto3

from ..config import SourceConfig
from ..state import Checkpoint
from .base import Batch, Reader
from .fileparse import is_gzip, iter_records


class S3FileReader(Reader):
    def __init__(self, cfg: SourceConfig, s3_client=None):
        super().__init__(cfg)
        if cfg.s3file is None:
            raise ValueError(f"[{cfg.name}] thieu khoi cau hinh 's3file'")
        self.s = cfg.s3file
        self._s3 = s3_client or boto3.client("s3")

    def _list_new(self, after_key: Optional[str]) -> List[str]:
        keys: List[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        kwargs: Dict[str, Any] = {"Bucket": self.s.bucket, "Prefix": self.s.prefix}
        if after_key:
            # StartAfter để S3 bỏ qua phía server, không phải tải về rồi lọc.
            kwargs["StartAfter"] = after_key

        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                name = k.rsplit("/", 1)[-1]
                if k.endswith("/") or obj["Size"] == 0 or name.startswith(("_", ".")):
                    # thu muc, file rong, file danh dau (_SUCCESS, .keep)
                    continue
                keys.append(k)
                if len(keys) >= self.s.max_files_per_run:
                    return keys
        return keys

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        after = None if self.is_full_snapshot else self.effective_since(checkpoint)
        keys = self._list_new(after)
        if not keys:
            return

        for key in keys:
            body = self._s3.get_object(Bucket=self.s.bucket, Key=key)["Body"]
            buf: List[Dict[str, Any]] = []
            total = 0
            try:
                for row_no, rec in iter_records(
                    body, self.s.file_format, name=key,
                    # compression: none van giai nen file duoi .gz (hanh vi cu)
                    gzip_on=is_gzip(key, "gzip" if self.s.compression == "gzip" else "auto"),
                    encoding=self.s.encoding, csv_delimiter=self.s.csv_delimiter,
                    repeated_header=self.s.csv_repeated_header,
                ):
                    rec["_src_file"] = key
                    rec["_src_row"] = row_no
                    buf.append(self.stamp(rec))
                    if len(buf) >= self.s.batch_size:
                        total += len(buf)
                        # Lo giua file: KHONG mang moc
                        yield Batch(records=buf, watermark=None, meta={"file": key})
                        buf = []
            finally:
                close = getattr(body, "close", None)
                if close:
                    close()
            total += len(buf)
            # Lo cuoi cua file mang moc = khoa file (ke ca khi lo rong: file chi co
            # header van phai duoc danh dau la da xu ly, neu khong se doc lai mai).
            yield Batch(records=buf, watermark=key, meta={"file": key, "records": total})

        if len(keys) >= self.s.max_files_per_run:
            print(f"[{self.cfg.name}] Da xu ly {self.s.max_files_per_run} file (tran mot lan chay). "
                  f"Con file chua doc — lan chay sau se tiep tuc tu checkpoint.")
