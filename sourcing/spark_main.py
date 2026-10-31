"""Điểm vào Glue Spark của sourcing (profile `src-spark`) — nguồn > ~1 triệu dòng.

Cùng hợp đồng với `sourcing.main` (Python Shell), chỉ khác cách đọc/ghi:

    Python Shell                          Spark (file này)
    reader.read() sinh từng lô             MỘT DataFrame, đọc SONG SONG
    RawWriter gom lô -> ndjson.gz          df.write.json(gzip) — mỗi partition một file
    mốc = bản ghi cuối của mỗi lô          mốc = MAX(cột mốc) của cả DataFrame

Giữ nguyên những thứ phía sau phụ thuộc vào: RUN#/CKPT# trong job_log, layout
`landing/<dataset>/ingest_date=.../`, cột `_src_*`, manifest `landing/_manifests/...`
(silver chain đọc manifest), CDC5 write-then-swap.

JDBC song song (`jdbc.parallel`):
    spark.read.jdbc(dbtable="(<query đã thay :watermark>) q",
                    partitionColumn=column, lowerBound, upperBound, numPartitions)
Spark chia [lower, upper] thành num_partitions khoảng đều nhau, mỗi khoảng một kết nối
`... WHERE column >= a AND column < b`. Bound bỏ trống -> MIN/MAX tính trên đúng câu
query (đã lọc mốc), nên lần chạy CDC1 chỉ chia đúng phần dữ liệu mới.

Ngữ nghĩa lỗi giống runner: dữ liệu lên S3 -> tiến mốc -> manifest. Spark không có
ảnh chụp PARTIAL: job chết thì lần sau đọc lại từ mốc cũ (at-least-once như cũ).

Không dùng UDF Python: executor không cần code ETL (không phải truyền --extra-py-files).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
import traceback
from typing import Any, Dict, List, Optional, Tuple

import boto3

from .config import CdcMode, SourceConfig, SourceType, decode_snapshot, from_dict, load_config
from .main import _load_from_dynamo, _promote_to_env, _resolve
from .readers.base import Reader
from .sqltemplate import render_literal
from .state import Checkpoint, JobLogStore, RunStats
from .writer import RawWriter

JDBC_URL = {
    "mysql": ("jdbc:mysql://{host}:{port}/{dbname}?zeroDateTimeBehavior=CONVERT_TO_NULL"
              "&useCursorFetch=true&connectTimeout=15000", "com.mysql.cj.jdbc.Driver", 3306),
    "postgresql": ("jdbc:postgresql://{host}:{port}/{dbname}?sslmode={sslmode}"
                   "&ApplicationName=fingraph-src-spark", "org.postgresql.Driver", 5432),
}


class _Since(Reader):
    """Mượn `Reader.effective_since` (checkpoint, lookback, initial_watermark) — không đọc gì."""

    def read(self, checkpoint):  # pragma: no cover - khong dung
        raise NotImplementedError


class SparkSourceRunner:
    def __init__(self, cfg: SourceConfig, spark, raw_bucket: str, joblog_table: str,
                 run_id: Optional[str] = None, dry_run: bool = False, s3_client=None, secrets_client=None):
        self.cfg, self.spark, self.raw_bucket, self.dry_run = cfg, spark, raw_bucket, dry_run
        self.log = JobLogStore(joblog_table)
        self.run_id = run_id or JobLogStore.new_run_id()
        self._s3 = s3_client or boto3.client("s3")
        self._secrets = secrets_client

    # ---------- đọc ----------

    def _jdbc_options(self) -> Tuple[Dict[str, str], str]:
        j = self.cfg.jdbc
        sm = self._secrets or boto3.client("secretsmanager")
        s = json.loads(sm.get_secret_value(SecretId=j.secret_name)["SecretString"])
        tmpl, driver, port = JDBC_URL[j.driver]
        url = tmpl.format(host=s["host"], port=int(s.get("port", port)), dbname=s["dbname"],
                          sslmode=s.get("sslmode", "prefer"))
        opts = {"url": url, "driver": driver, "user": s["username"], "password": s["password"],
                "fetchsize": str(j.fetch_size)}
        if j.init_sql:
            opts["sessionInitStatement"] = "; ".join(j.init_sql)
        return opts, url

    def read_jdbc(self, checkpoint: Checkpoint):
        j = self.cfg.jdbc
        values: Dict[str, Any] = dict(j.params)
        values["run_ts"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        if self.cfg.cdc.mode is CdcMode.CDC1:
            values["watermark"] = _Since(self.cfg).effective_since(checkpoint)
        sql = render_literal(j.query.strip().rstrip(";"), values)
        opts, url = self._jdbc_options()
        print(f"[{self.cfg.name}] JDBC {url.split('?')[0]} — query:\n{sql}")

        reader = self.spark.read.format("jdbc").options(**opts)
        par = j.parallel or {}
        if not par:
            return reader.option("dbtable", f"({sql}) q").load(), None

        col = par["column"]
        n = int(par.get("num_partitions", 8))
        lo, hi = par.get("lower_bound"), par.get("upper_bound")
        if lo is None:
            row = (self.spark.read.format("jdbc").options(**opts)
                   .option("dbtable", f"(SELECT MIN({col}) AS lo, MAX({col}) AS hi FROM ({sql}) b) mm")
                   .load().collect()[0])
            lo, hi = row["lo"], row["hi"]
            if lo is None:
                print(f"[{self.cfg.name}] query khong tra dong nao -> NO_DATA")
                return None, None
        print(f"[{self.cfg.name}] doc song song theo {col} [{lo}, {hi}] x {n} partition")
        df = (reader.option("dbtable", f"({sql}) q")
              .option("partitionColumn", col).option("lowerBound", str(lo))
              .option("upperBound", str(hi)).option("numPartitions", str(n)).load())
        return df, (str(lo), str(hi), n)

    def read_s3file(self):
        from pyspark.sql import functions as F

        s = self.cfg.s3file
        path = f"s3://{s.bucket}/{s.prefix}"
        if s.file_format == "csv":
            df = (self.spark.read.option("header", "true").option("sep", s.csv_delimiter)
                  .option("encoding", s.encoding.replace("-sig", "")).option("mode", "PERMISSIVE")
                  # Tang raw khong ep kieu: moi cot la chuoi, silver tu cast
                  .option("inferSchema", "false").csv(path))
        elif s.file_format in ("ndjson", "json"):
            df = self.spark.read.option("multiLine", s.file_format == "json").json(path)
        else:
            raise ValueError(f"[{self.cfg.name}] spark s3file chua ho tro {s.file_format}")
        # _src_file + _src_row giong reader Python: khoa tu nhien de silver khu trung
        return (df.withColumn("_src_file", F.input_file_name())
                  .withColumn("_src_row", F.monotonically_increasing_id())), None

    # ---------- ghi ----------

    def _to_raw(self, df):
        """Cột `_src_*` + kiểu JSON giống writer Python (json.dumps(default=str)):
        số nguyên/thực giữ là số; decimal, ngày, giờ thành chuỗi."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import DateType, DecimalType, TimestampType

        cols = []
        for f in df.schema.fields:
            c = F.col(f"`{f.name}`")
            if isinstance(f.dataType, (DecimalType, DateType, TimestampType)):
                c = c.cast("string")
            cols.append(c.alias(f.name))
        now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        return df.select(*cols,
                         F.lit(self.cfg.name).alias("_src_job"),
                         F.lit(self.cfg.source_type.value).alias("_src_type"),
                         F.lit(self.cfg.cdc.mode.value).alias("_src_cdc"),
                         F.lit(now).alias("_src_ingest_ts"))

    def _list_parts(self, prefix: str) -> Tuple[List[str], int]:
        keys, size = [], 0
        for page in self._s3.get_paginator("list_objects_v2").paginate(Bucket=self.raw_bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                name = o["Key"].rsplit("/", 1)[-1]
                if name.startswith("part-") and o["Size"] > 0:
                    keys.append(o["Key"])
                    size += o["Size"]
        return sorted(keys), size

    # ---------- luồng chính ----------

    def run(self) -> str:
        from pyspark import StorageLevel
        from pyspark.sql import functions as F

        cfg = self.cfg
        stats = RunStats()
        t0 = time.monotonic()
        run_key = self.log.start_run(cfg.name, self.run_id, {
            "source_type": cfg.source_type.value, "cdc_mode": cfg.cdc.mode.value,
            "dataset": cfg.dataset, "engine": "spark", "profile": cfg.runtime.profile,
            "parallel": json.dumps(cfg.jdbc.parallel) if cfg.jdbc else "",
        })
        writer = RawWriter(cfg, self.raw_bucket, self.run_id, s3_client=self._s3)
        try:
            checkpoint = Checkpoint(job_name=cfg.name)
            if cfg.cdc.mode is CdcMode.CDC1:
                checkpoint = self.log.read_checkpoint(cfg.name)
                print(f"[{cfg.name}] moc truoc: {checkpoint.watermarks or '(chua co)'}")

            if cfg.source_type is SourceType.JDBC:
                df, bounds = self.read_jdbc(checkpoint)
            else:
                df, bounds = self.read_s3file()

            new_wm = None
            mkey = None
            if df is not None:
                df = df.persist(StorageLevel.MEMORY_AND_DISK)
                stats.records_read = df.count()
                stats.batches = df.rdd.getNumPartitions()
                print(f"[{cfg.name}] doc {stats.records_read} dong / {stats.batches} partition "
                      f"trong {time.monotonic() - t0:.1f}s")
                if stats.records_read and cfg.cdc.mode is CdcMode.CDC1:
                    wcol = cfg.cdc.watermark_column
                    new_wm = df.agg(F.max(F.col(f"`{wcol}`"))).collect()[0][0]
                    new_wm = new_wm.isoformat() if hasattr(new_wm, "isoformat") else str(new_wm)

            if stats.records_read and not self.dry_run:
                out_prefix = f"{writer.prefix}/spark-{self.run_id}"
                self.spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                # ignoreNullFields=false: giu khoa null nhu json.dumps cua writer Python
                (self._to_raw(df).write.mode("append").option("compression", "gzip")
                 .option("ignoreNullFields", "false")
                 .option("maxRecordsPerFile", cfg.target.max_records_per_file)
                 .json(f"s3://{self.raw_bucket}/{out_prefix}"))
                keys, size = self._list_parts(out_prefix + "/")
                writer.result.keys, writer.result.files = keys, len(keys)
                writer.result.records, writer.result.bytes_out = stats.records_read, size
                stats.records_written, stats.files_written, stats.bytes_written = stats.records_read, len(keys), size

            status = "SUCCESS" if stats.records_read else "NO_DATA"
            if new_wm is not None and not self.dry_run:
                self.log.advance_checkpoint(cfg.name, self.run_id, {"default": new_wm}, "SUCCESS")

            if not self.dry_run:
                extra = {"raw_mode": "records", "etl_release": os.environ.get("ETL_RELEASE", ""),
                         "engine": "spark"}
                if df is not None:
                    extra["columns"] = [c for c in df.columns if not c.startswith("_src_")]
                if bounds:
                    extra["parallel"] = {"lower": bounds[0], "upper": bounds[1], "partitions": bounds[2]}
                mkey = writer.write_manifest(status, extra)
                if mkey:
                    print(f"[{cfg.name}] manifest s3://{self.raw_bucket}/{mkey}")
                    if cfg.cdc.mode is CdcMode.CDC5:
                        n_old = writer.cleanup_superseded()
                        if n_old:
                            print(f"[{cfg.name}] CDC5: xoa {n_old} file cua lan chay truoc trong ngay")
            try:
                self.log.record_orch_result(cfg.name, self.run_id, "DRY_RUN" if self.dry_run else status, mkey)
            except Exception as e:  # noqa: BLE001
                print(f"[{cfg.name}] CANH BAO: khong ghi duoc ORCH#{self.run_id}: {e}")

            final = self.log.read_checkpoint(cfg.name).watermarks
            self.log.finish_run(cfg.name, run_key, status, stats, watermarks=final)
            print(f"[{cfg.name}] {status} doc={stats.records_read} ghi={stats.records_written} "
                  f"file={stats.files_written} {time.monotonic() - t0:.1f}s")
            if df is not None:
                df.unpersist()
            return status
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-2000:]}"
            partial = self.log.read_checkpoint(cfg.name).watermarks
            self.log.finish_run(cfg.name, run_key, "FAILED", stats, watermarks=partial, error=err)
            print(f"[{cfg.name}] FAILED sau {time.monotonic() - t0:.1f}s: {e}")
            raise


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Sourcing tren Glue Spark (profile src-spark)")
    p.add_argument("--config")
    p.add_argument("--JOB_NAME", dest="job_name")
    p.add_argument("--JOB_CONFIG_TABLE", dest="config_table")
    p.add_argument("--JOBLOG_TABLE", dest="joblog_table")
    p.add_argument("--RAW_BUCKET", dest="raw_bucket")
    p.add_argument("--RUN_ID", dest="run_id")
    p.add_argument("--CONFIG_SNAPSHOT", dest="config_snapshot")
    p.add_argument("--DRY_RUN", dest="dry_run_flag", default="false")
    args, unknown = p.parse_known_args(argv)
    _promote_to_env(unknown)
    if args.raw_bucket:
        os.environ.setdefault("RAW_BUCKET", args.raw_bucket)

    if args.config:
        cfg = load_config(args.config)
    elif args.config_snapshot:
        cfg = from_dict(decode_snapshot(args.config_snapshot), default_name=args.job_name or "")
    elif args.config_table and args.job_name:
        cfg = _load_from_dynamo(args.config_table, args.job_name)
    else:
        raise SystemExit("Phai truyen --config, hoac --CONFIG_SNAPSHOT, hoac --JOB_CONFIG_TABLE + --JOB_NAME")
    if not cfg.enabled:
        print(f"[{cfg.name}] enabled=false -> bo qua.")
        return 0

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(f"src-{cfg.name}").getOrCreate()
    status = SparkSourceRunner(
        cfg, spark,
        raw_bucket=_resolve("RAW_BUCKET", args.raw_bucket, "raw_bucket"),
        joblog_table=_resolve("JOBLOG_TABLE", args.joblog_table, "joblog_table"),
        run_id=args.run_id, dry_run=str(args.dry_run_flag).lower() == "true",
    ).run()
    return 0 if status in ("SUCCESS", "NO_DATA") else 1


if __name__ == "__main__":
    sys.exit(main())
