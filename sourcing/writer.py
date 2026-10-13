"""Ghi tầng raw xuống S3.

Định dạng: NDJSON nén gzip. Ba lý do:

  1. Không cần pyarrow — Glue Python Shell chạy được ngay, không phải khai
     thêm thư viện.
  2. Tầng raw **không nên áp schema**. Nguồn thêm một trường thì file vẫn
     ghi được; nếu ép Parquet ngay ở đây, job sẽ chết vì lệch schema và bạn
     mất luôn dữ liệu của lần chạy đó.
  3. `bronze_ingest.py` đã đọc JSON từ landing — không phải sửa gì phía sau.

Ép kiểu là việc của tầng bronze, nơi có Spark và có bảng Iceberg để nhận.
"""

from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import boto3

from .config import SourceConfig


@dataclass
class WriteResult:
    files: int = 0
    records: int = 0
    bytes_out: int = 0
    keys: Optional[List[str]] = None

    def __post_init__(self):
        if self.keys is None:
            self.keys = []


class RawWriter:
    """Ghi lô bản ghi thành file NDJSON gzip, CUỘN file theo ngưỡng.

    Bản ghi được nén ngay khi vào (gzip theo luồng), nên bộ đệm trong RAM là
    dung lượng ĐÃ NÉN của một file, không phải list dict. File được cắt khi chạm
    `max_records_per_file` hoặc `max_file_mb`, hoặc khi runner gọi `flush()`.

    Vì sao gom qua nhiều lô: reader trả lô nhỏ (JDBC 5.000 dòng, Kafka 10.000
    theo partition). Một lô một file thì bảng 1 triệu dòng thành 200 file nhỏ —
    bài toán "small files" làm Spark/Athena chậm ở mọi tầng sau. Kafka Connect
    S3 sink và Firehose giải đúng bài này bằng ngưỡng flush.size / buffer size.

    Hợp đồng với runner (giữ bất biến "ghi trước, tiến mốc sau"): bản ghi còn
    trong bộ đệm CHƯA nằm trên S3, nên runner phải `flush()` trước mỗi lần tiến
    checkpoint.
    """

    def __init__(self, cfg: SourceConfig, bucket: str, run_id: str, s3_client=None):
        self.cfg = cfg
        self.bucket = bucket
        self.run_id = run_id
        self._s3 = s3_client or boto3.client("s3")
        self._partition = dt.datetime.now(dt.timezone.utc).strftime(cfg.target.partition_format)
        self._seq = 0
        self._max_bytes = cfg.target.max_file_mb * 1024 * 1024
        self._buf: Optional[io.BytesIO] = None
        self._gz: Optional[gzip.GzipFile] = None
        self._buf_records = 0
        self.result = WriteResult()

    @property
    def prefix(self) -> str:
        return f"{self.cfg.raw_path}/{self._partition}"

    @property
    def pending_records(self) -> int:
        return self._buf_records

    # ---------- ghi ----------

    def write(self, records: List[Dict[str, Any]]) -> None:
        for rec in records:
            if self._gz is None:
                self._buf = io.BytesIO()
                # mtime=0: cung mot noi dung cho ra cung mot byte, khong phu thuoc
                # thoi diem chay. Tien khi so sanh hai lan chay co giong nhau khong.
                self._gz = gzip.GzipFile(fileobj=self._buf, mode="wb", mtime=0)
            self._gz.write(json.dumps(rec, ensure_ascii=False, default=str).encode("utf-8"))
            self._gz.write(b"\n")
            self._buf_records += 1
            if (self._buf_records >= self.cfg.target.max_records_per_file
                    or self._buf.tell() >= self._max_bytes):
                self.flush()

    def flush(self) -> None:
        """Đẩy bộ đệm hiện tại thành một file trên S3. Không có gì thì thôi."""
        if self._gz is None or self._buf_records == 0:
            return
        self._gz.close()
        payload = self._buf.getvalue()
        records = self._buf_records
        self._gz, self._buf, self._buf_records = None, None, 0

        self._seq += 1
        # run_id trong ten file: hai lan chay song song khong ghi de nhau,
        # va nhin ten file la biet no den tu lan chay nao.
        key = (f"{self.prefix}/part-{self.run_id}-{self._seq:05d}-"
               f"{uuid.uuid4().hex[:8]}.ndjson.gz")

        self._s3.put_object(
            Bucket=self.bucket, Key=key, Body=payload,
            ContentType="application/x-ndjson", ContentEncoding="gzip",
            Metadata={
                "job": self.cfg.name,
                "run-id": self.run_id,
                "cdc-mode": self.cfg.cdc.mode.value,
                "records": str(records),
            },
        )

        self.result.files += 1
        self.result.records += records
        self.result.bytes_out += len(payload)
        self.result.keys.append(key)

    # ---------- CDC5: dọn ảnh chụp cũ SAU khi ảnh chụp mới hoàn chỉnh ----------

    def cleanup_superseded(self) -> int:
        """Xoá file của các lần chạy trước trong partition HÔM NAY.

        Gọi SAU khi manifest của lần chạy này đã ghi (write-then-swap). Cách cũ
        xoá partition TRƯỚC lô đầu tiên: job chết giữa chừng thì hôm đó không còn
        ảnh chụp nào, và silver đang đọc manifest cũ gặp file đã bị xoá. Giờ ảnh
        chụp cũ chỉ biến mất khi ảnh chụp mới đã đủ — job chết thì cái cũ còn
        nguyên, file dở dang của lần chết được lần thành công sau dọn.

        Chỉ đụng partition hôm nay: snapshot ngày trước là lịch sử hợp lệ, và
        lifecycle của bucket tự dọn sau 30 ngày. Vẫn idempotent trong ngày.
        """
        keep = set(self.result.keys)
        paginator = self._s3.get_paginator("list_objects_v2")
        to_delete: List[Dict[str, str]] = []
        deleted = 0
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix + "/"):
            for obj in page.get("Contents", []):
                if obj["Key"] in keep:
                    continue
                to_delete.append({"Key": obj["Key"]})
                if len(to_delete) == 1000:  # gioi han cua delete_objects
                    self._s3.delete_objects(Bucket=self.bucket, Delete={"Objects": to_delete})
                    deleted += len(to_delete)
                    to_delete = []
        if to_delete:
            self._s3.delete_objects(Bucket=self.bucket, Delete={"Objects": to_delete})
            deleted += len(to_delete)
        return deleted

    # ---------- manifest: tín hiệu "lần chạy này đã ghi xong" ----------

    MANIFEST_ROOT = "_manifests"

    @property
    def manifest_key(self) -> str:
        # Tach khoi thu muc du lieu co chu dich:
        #   - S3 event loc theo prefix `landing/_manifests/` -> MOT event moi
        #     lan chay, thay vi mot event moi file (FDIC: 28 file = 28 event).
        #   - `_clear_partition_once` cua CDC5 khong xoa nham manifest.
        #   - Spark doc ca partition du lieu khong vuong file .json la.
        root = self.cfg.target.prefix.rstrip("/")
        return (f"{root}/{self.MANIFEST_ROOT}/{self.cfg.dataset}/"
                f"{self._partition}/{self.run_id}.json")

    def write_manifest(self, status: str, extra: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Ghi SAU CÙNG, khi mọi file dữ liệu đã nằm trên S3.

        Silver chỉ nghe file này, nên nó vừa là trigger vừa là bằng chứng
        đủ dữ liệu: danh sách file đọc ở đây, không phải liệt kê thư mục
        (liệt kê có thể lẫn file của lần chạy khác cùng ngày).
        """
        if not self.result.keys:
            return None  # NO_DATA: khong co gi cho silver xu ly
        body = {
            "job": self.cfg.name,
            "run_id": self.run_id,
            "dataset": self.cfg.dataset,
            "partition": self._partition,
            "cdc_mode": self.cfg.cdc.mode.value,
            "status": status,
            "bucket": self.bucket,
            "files": self.result.keys,
            "records": self.result.records,
            "bytes": self.result.bytes_out,
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            **(extra or {}),
        }
        self._s3.put_object(
            Bucket=self.bucket, Key=self.manifest_key,
            Body=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        return self.manifest_key

    def write_batches(self, batches: Iterable) -> WriteResult:
        for b in batches:
            self.write(b.records)
        self.flush()
        return self.result
