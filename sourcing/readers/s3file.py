"""Reader cho file do bên ngoài đẩy vào S3.

Đây là nhánh batch: ai đó (script, đối tác, tải tay) đặt file vào một prefix,
job này quét và nạp vào raw.

Mốc CDC1 ở đây là **khoá S3 của file cuối cùng đã xử lý**, không phải thời
gian. Lý do: `LastModified` có thể trùng nhau ở mức giây khi upload hàng loạt,
còn khoá thì duy nhất và `list_objects_v2` trả về theo thứ tự từ điển — nên
"đã xử lý tới khoá X" là một mốc chặt chẽ và tiếp tục được.

Đánh đổi: file có khoá đứng trước X mà upload muộn sẽ bị bỏ sót. Với bố cục
prefix theo ngày (`.../dt=2026-09-26/...`) thì điều đó hầu như không xảy ra;
nếu nguồn không bảo đảm được, dùng luồng Lambda theo sự kiện S3 thay vì quét.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
from typing import Any, Dict, Iterator, List, Optional

import boto3

from ..config import SourceConfig
from ..state import Checkpoint
from .base import Batch, Reader

MAX_FILES_PER_RUN = 500


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
                if k.endswith("/") or obj["Size"] == 0:
                    continue  # bo qua "thu muc" va file rong
                keys.append(k)
                if len(keys) >= MAX_FILES_PER_RUN:
                    return keys
        return keys

    def _parse(self, key: str, body: bytes) -> List[Dict[str, Any]]:
        if self.s.compression == "gzip" or key.endswith(".gz"):
            body = gzip.decompress(body)

        fmt = self.s.file_format
        if fmt == "json":
            doc = json.loads(body.decode("utf-8"))
            return doc if isinstance(doc, list) else [doc]
        if fmt == "ndjson":
            return [json.loads(line) for line in body.decode("utf-8").splitlines() if line.strip()]
        if fmt == "csv":
            text = io.StringIO(body.decode("utf-8-sig"))  # -sig: bo BOM neu co
            return list(csv.DictReader(text, delimiter=self.s.csv_delimiter))
        raise ValueError(f"[{self.cfg.name}] file_format khong ho tro: {fmt}")

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        after = None if self.is_full_snapshot else self.effective_since(checkpoint)
        keys = self._list_new(after)
        if not keys:
            return

        for key in keys:
            body = self._s3.get_object(Bucket=self.s.bucket, Key=key)["Body"].read()
            records = self._parse(key, body)
            for r in records:
                r["_src_file"] = key
            # Mốc chỉ tiến sau khi cả file đã ghi xong: file nửa vời không
            # bao giờ được đánh dấu là đã xử lý.
            yield Batch(
                records=[self.stamp(r) for r in records],
                watermark=key,
                meta={"file": key, "records": len(records)},
            )

        if len(keys) >= MAX_FILES_PER_RUN:
            print(f"[{self.cfg.name}] Da xu ly {MAX_FILES_PER_RUN} file (tran mot lan chay). "
                  f"Con file chua doc — lan chay sau se tiep tuc tu checkpoint.")
