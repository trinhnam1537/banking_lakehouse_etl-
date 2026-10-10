"""Một lần chạy raw -> silver cho một entry.

    RUN# RUNNING
      -> giải đầu vào: manifest (danh sách file chính xác) | partition (liệt kê)
      -> extract -> transform (+quality) -> ensure_table -> writer
    RUN# SUCCESS | NO_DATA | FAILED   (+ TRIG# cùng trạng thái)

Đọc đầu vào từ MANIFEST chứ không liệt kê thư mục: cùng ngày có thể có nhiều
lần chạy sourcing (chạy lại, CDC1 nhiều lượt); liệt kê sẽ trộn file của lần
khác vào, còn manifest nói đúng lần chạy nào sinh file nào.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import boto3

from . import extract, transform
from .config import ReadMode, Shape, SilverConfig
from .ddl import ensure_table
from .state import SilverLogStore
from .writers import build_writer


@dataclass
class Inputs:
    files: List[str]
    label: str                        # ds_source
    shape: Shape
    records_path: Optional[str]
    ingest_date: Optional[str] = None


@dataclass
class SilverResult:
    job_name: str
    run_id: str
    status: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.status in ("SUCCESS", "NO_DATA")


_RAW_MODE_TO_SHAPE = {"response": Shape.API_RESPONSE, "records": Shape.RECORDS,
                      "kafka_message": Shape.KAFKA_MESSAGE}


class SilverRunner:
    def __init__(self, spark, cfg: SilverConfig, bucket: str, joblog_table: Optional[str],
                 run_id: Optional[str] = None, manifest: Optional[str] = None,
                 data_date: Optional[str] = None, ddl_text: Optional[str] = None,
                 rebuild_from: Optional[str] = None, s3_client=None):
        self.spark = spark
        self.cfg = cfg
        self.bucket = bucket
        self.manifest = manifest
        self.data_date = dt.date.fromisoformat(data_date) if data_date else None
        self.rebuild_from = dt.date.fromisoformat(rebuild_from) if rebuild_from else None
        self.ddl_text = ddl_text
        self._s3 = s3_client
        # joblog_table=None: chay thu tren may / test, khong ghi DynamoDB
        self.log = SilverLogStore(joblog_table) if joblog_table else None
        self.run_id = run_id or SilverLogStore.new_run_id()

    # ---------- dau vao ----------

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.client("s3")
        return self._s3

    def _uri(self, key: str) -> str:
        return key if "://" in key else f"s3://{self.bucket}/{key}"

    def resolve_inputs(self) -> Inputs:
        src = self.cfg.source
        if self.manifest and src.read is ReadMode.MANIFEST:
            m = self._read_manifest(self.manifest)
            if m.get("dataset") != src.dataset:
                raise ValueError(f"[{self.cfg.name}] manifest thuoc dataset '{m.get('dataset')}', "
                                 f"entry doc '{src.dataset}'")
            shape = src.shape or _RAW_MODE_TO_SHAPE.get(m.get("raw_mode", "records"), Shape.RECORDS)
            return Inputs(files=[self._uri(k) for k in m.get("files", [])], label=self.manifest,
                          shape=shape, records_path=src.records_path,
                          ingest_date=m.get("partition", "").replace("ingest_date=", "") or None)

        # partition: moi file .ndjson.gz cua mot ngay — backfill / chay tay
        day = (self.data_date or dt.date.today()).isoformat()
        prefix = f"{src.prefix}/{src.dataset}/ingest_date={day}/"
        files = self._list(prefix)
        return Inputs(files=[self._uri(k) for k in files], label=prefix,
                      shape=src.shape or self._shape_of_day(day), records_path=src.records_path,
                      ingest_date=day)

    def _shape_of_day(self, day: str) -> Shape:
        """Không có manifest trong tay (backfill) -> mượn raw_mode của một
        manifest cùng ngày. Đoán mặc định `records` sẽ bóc sai nguồn API."""
        src = self.cfg.source
        mprefix = f"{src.prefix}/_manifests/{src.dataset}/ingest_date={day}/"
        for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.bucket, Prefix=mprefix, PaginationConfig={"MaxItems": 1}):
            for o in page.get("Contents", []):
                m = self._read_manifest(o["Key"])
                return _RAW_MODE_TO_SHAPE.get(m.get("raw_mode", "records"), Shape.RECORDS)
        return Shape.RECORDS

    def _read_manifest(self, manifest: str) -> Dict[str, Any]:
        local = manifest[len("file://"):] if manifest.startswith("file://") else manifest
        if os.path.isfile(local):   # chay tren may / test
            with open(local, encoding="utf-8") as f:
                return json.load(f)
        key = manifest.split(f"s3://{self.bucket}/", 1)[-1]
        body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        return json.loads(body)

    def _list(self, prefix: str) -> List[str]:
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".ndjson.gz")]
        return out

    # ---------- chay ----------

    def run(self) -> SilverResult:
        cfg = self.cfg
        t0 = time.monotonic()
        run_key = None
        if self.log:
            run_key = self.log.start_run(cfg.name, self.run_id, {
                "layer": "silver", "writer": cfg.writer.type.value,
                "target": cfg.target.fqn, "source_dataset": cfg.source.dataset,
                "manifest": self.manifest or "", "data_date": str(self.data_date or ""),
            })
        print(f"[{cfg.name}] run_id={self.run_id} writer={cfg.writer.type.value} -> {cfg.target.fqn}")

        try:
            inp = self.resolve_inputs()
            metrics: Dict[str, Any] = {"files": len(inp.files), "source": inp.label,
                                       "target": cfg.target.fqn}
            if not inp.files:
                return self._finish(run_key, "NO_DATA", metrics, t0)

            raw = extract.extract(self.spark, inp.files, cfg, inp.shape, inp.records_path)
            df, rep = transform.build(self.spark, raw, cfg, self.run_id, inp.label,
                                      self.data_date or (dt.date.fromisoformat(inp.ingest_date)
                                                         if inp.ingest_date else None))
            metrics.update(rep.as_dict())
            for w in rep.warnings:
                print(f"[{cfg.name}] CANH BAO: {w}")
            if rep.rows_out == 0:
                return self._finish(run_key, "NO_DATA", metrics, t0)

            metrics["ddl"] = ensure_table(self.spark, cfg, self.ddl_text)
            writer = build_writer(self.spark, cfg, inp.label)
            if cfg.writer.type.value == "scd2":
                metrics.update(writer.write(df, rebuild_from=self.rebuild_from))
            else:
                metrics.update(writer.write(df))
            return self._finish(run_key, "SUCCESS", metrics, t0)

        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-2000:]}"
            self._finish(run_key, "FAILED", {}, t0, err)
            raise

    def _finish(self, run_key, status: str, metrics: Dict[str, Any], t0: float,
                error: str = "") -> SilverResult:
        metrics["seconds"] = round(time.monotonic() - t0, 1)
        if self.log and run_key:
            self.log.finish_silver_run(self.cfg.name, run_key, status, metrics, error)
            if self.manifest:
                self.log.mark_trigger(self.cfg.name, self.manifest, status, self.run_id)
        print(f"[{self.cfg.name}] {status} {json.dumps(metrics, default=str)[:600]}")
        return SilverResult(self.cfg.name, self.run_id, status, metrics, error)
