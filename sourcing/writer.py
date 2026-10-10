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

from .config import CdcMode, SourceConfig


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
    def __init__(self, cfg: SourceConfig, bucket: str, run_id: str, s3_client=None):
        self.cfg = cfg
        self.bucket = bucket
        self.run_id = run_id
        self._s3 = s3_client or boto3.client("s3")
        self._partition = dt.datetime.now(dt.timezone.utc).strftime(cfg.target.partition_format)
        self._seq = 0
        self._cleared = False
        self.result = WriteResult()

    @property
    def prefix(self) -> str:
        return f"{self.cfg.raw_path}/{self._partition}"

    # ---------- CDC5: dọn partition trước khi ghi ----------

    def _clear_partition_once(self) -> None:
        """Xoá dữ liệu cũ trong partition hôm nay, chỉ một lần mỗi lần chạy.

        Vì sao chỉ xoá partition HÔM NAY chứ không xoá cả dataset: snapshot
        của những ngày trước là lịch sử hợp lệ, và lifecycle của bucket đã
        tự dọn sau 30 ngày. Xoá cả dataset rồi job chết giữa chừng là mất
        sạch dữ liệu mà không có gì thay thế.

        Cách này cũng làm job **idempotent trong ngày**: chạy lại lần hai
        cho ra đúng một bộ dữ liệu, không nhân đôi.
        """
        if self._cleared:
            return
        self._cleared = True

        paginator = self._s3.get_paginator("list_objects_v2")
        to_delete: List[Dict[str, str]] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix + "/"):
            for obj in page.get("Contents", []):
                to_delete.append({"Key": obj["Key"]})
                if len(to_delete) == 1000:  # gioi han cua delete_objects
                    self._s3.delete_objects(Bucket=self.bucket,
                                            Delete={"Objects": to_delete})
                    to_delete = []
        if to_delete:
            self._s3.delete_objects(Bucket=self.bucket, Delete={"Objects": to_delete})

    # ---------- ghi ----------

    def write(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        if self.cfg.cdc.mode is CdcMode.CDC5:
            self._clear_partition_once()

        chunk = self.cfg.target.max_records_per_file
        for i in range(0, len(records), chunk):
            self._write_one(records[i:i + chunk])

    def _write_one(self, records: List[Dict[str, Any]]) -> None:
        buf = io.BytesIO()
        # mtime=0: cung mot noi dung cho ra cung mot byte, khong phu thuoc
        # thoi diem chay. Tien khi so sanh hai lan chay co giong nhau khong.
        with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
            for rec in records:
                gz.write(json.dumps(rec, ensure_ascii=False, default=str).encode("utf-8"))
                gz.write(b"\n")
        payload = buf.getvalue()

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
                "records": str(len(records)),
            },
        )

        self.result.files += 1
        self.result.records += len(records)
        self.result.bytes_out += len(payload)
        self.result.keys.append(key)

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
        return self.result
