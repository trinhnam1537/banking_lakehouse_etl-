"""Điểm vào Glue Spark của sourcing (profile `src-spark`) — nguồn > ~1 triệu dòng.

Cùng hợp đồng với `sourcing.main` (Python Shell), chỉ khác cách đọc/ghi:

    Python Shell                          Spark (file này)
    reader.read() sinh từng lô             MỘT DataFrame, đọc SONG SONG
    RawWriter gom lô -> ndjson.gz          df.write.json(gzip), số file theo số dòng
    mốc = bản ghi cuối của mỗi lô          jdbc: MAX(cột mốc) · s3file: khoá file cuối

Giữ nguyên những thứ phía sau phụ thuộc vào: RUN#/CKPT# trong job_log, layout
`landing/<dataset>/ingest_date=.../`, cột `_src_*`, manifest `landing/_manifests/...`
(silver chain đọc manifest), CDC5 write-then-swap.

JDBC song song (`jdbc.parallel`, xem `plan_partitions`):
    column      spark.read.jdbc(dbtable="(<query đã thay :watermark>) q",
                                partitionColumn, lowerBound, upperBound, numPartitions)
                Spark chia [lower, upper] thành N khoảng đều nhau, mỗi khoảng một kết nối
                `... WHERE column >= a AND column < b`. Bound bỏ trống -> MIN/MAX (và COUNT
                nếu khai rows_per_partition) tính trên đúng câu query (đã lọc mốc), nên lần
                chạy CDC1 chỉ chia đúng phần dữ liệu mới.
    predicates  mỗi điều kiện WHERE một partition: cột chuỗi, ngày phân bố lệch.

s3file (`cdc1` theo khoá S3 hoặc `cdc5` cả prefix): Spark chia file KHÔNG nén theo
`spark.max_partition_mb`; file .gz không chia được nên một file = một task -> nguồn
lớn nên chia sẵn thành nhiều file (simulators.aml.replay history: 1 triệu dòng/file).

Ngữ nghĩa lỗi giống runner: dữ liệu lên S3 -> tiến mốc -> manifest. Spark không có
ảnh chụp PARTIAL: job chết thì lần sau đọc lại từ mốc cũ (at-least-once như cũ).

Không dùng UDF Python: executor không cần code ETL (không phải truyền --extra-py-files).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
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

    def _spark_conf(self) -> None:
        sp = self.cfg.spark
        conf = {"spark.sql.adaptive.enabled": "true",
                "spark.sql.files.maxPartitionBytes": str(sp.max_partition_mb * 1024 * 1024),
                **sp.conf}
        for k, v in conf.items():
            self.spark.conf.set(k, str(v))

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
            return reader.option("dbtable", f"({sql}) q").load(), {}

        if par.get("predicates"):
            preds = list(par["predicates"])
            print(f"[{self.cfg.name}] doc song song theo {len(preds)} predicate")
            props = {k: v for k, v in opts.items() if k != "url"}
            df = self.spark.read.jdbc(url=opts["url"], table=f"({sql}) q", predicates=preds,
                                      properties=props)
            return df, {"parallel": {"predicates": len(preds), "partitions": len(preds)}}

        col = par["column"]
        lo, hi, count = par.get("lower_bound"), par.get("upper_bound"), None
        if lo is None:
            want_count = "rows_per_partition" in par
            cnt_sql = ", COUNT(*) AS n" if want_count else ""
            row = (self.spark.read.format("jdbc").options(**opts)
                   .option("dbtable", f"(SELECT MIN({col}) AS lo, MAX({col}) AS hi{cnt_sql} "
                                      f"FROM ({sql}) b) mm")
                   .load().collect()[0])
            lo, hi = row["lo"], row["hi"]
            count = int(row["n"]) if want_count else None
            if lo is None:
                print(f"[{self.cfg.name}] query khong tra dong nao -> NO_DATA")
                return None, {}
        n = plan_partitions(lo, hi, count, par)
        print(f"[{self.cfg.name}] doc song song theo {col} [{lo}, {hi}] x {n} partition"
              + (f" ({count} dong)" if count is not None else ""))
        df = (reader.option("dbtable", f"({sql}) q")
              .option("partitionColumn", col).option("lowerBound", str(lo))
              .option("upperBound", str(hi)).option("numPartitions", str(n)).load())
        return df, {"parallel": {"lower": str(lo), "upper": str(hi), "partitions": n}}

    def _uri(self, key: str) -> str:
        """Đường dẫn Spark của một khoá trong bucket nguồn (test đè để đọc file local)."""
        return f"s3://{self.cfg.s3file.bucket}/{key}"

    def read_s3file(self, checkpoint: Optional[Checkpoint] = None):
        from pyspark.sql import functions as F

        from .readers.s3file import S3FileReader

        s = self.cfg.s3file
        info: Dict[str, Any] = {}
        if self.cfg.cdc.mode is CdcMode.CDC1:
            # Cùng cách liệt kê với reader Python (StartAfter = mốc, bỏ file đánh dấu,
            # trần max_files_per_run) -> hai engine đổi qua lại vẫn nối tiếp mốc.
            lister = S3FileReader(self.cfg, s3_client=self._s3)
            keys = lister._list_new(lister.effective_since(checkpoint))
            if not keys:
                print(f"[{self.cfg.name}] khong co file moi sau moc -> NO_DATA")
                return None, info
            paths = [self._uri(k) for k in keys]
            info.update(watermark=keys[-1], files_read=len(keys), first_key=keys[0], last_key=keys[-1])
            print(f"[{self.cfg.name}] {len(keys)} file moi: {keys[0]} .. {keys[-1]}")
        else:
            paths = [self._uri(s.prefix)]

        if s.file_format == "csv":
            df = (self.spark.read.option("header", "true").option("sep", s.csv_delimiter)
                  .option("encoding", s.encoding.replace("-sig", "")).option("mode", "PERMISSIVE")
                  # Tang raw khong ep kieu: moi cot la chuoi, silver tu cast
                  .option("inferSchema", "false").csv(paths))
            if s.column_names:
                if len(s.column_names) != len(df.columns):
                    raise ValueError(f"[{self.cfg.name}] s3file.column_names co {len(s.column_names)} ten, "
                                     f"file co {len(df.columns)} cot: {df.columns}")
                df = df.toDF(*s.column_names)
        elif s.file_format in ("ndjson", "json"):
            df = self.spark.read.option("multiLine", s.file_format == "json").json(paths)
        elif s.file_format == "parquet":
            df = self.spark.read.parquet(*paths)
        else:
            raise ValueError(f"[{self.cfg.name}] spark s3file chua ho tro {s.file_format}")
        # _src_file = KHOA S3 (khong kem s3://bucket/) giong reader Python; cung _src_row
        # la khoa tu nhien de silver khu trung. _src_row tren Spark la id duy nhat trong
        # lan chay, KHONG phai so dong trong file.
        return (df.withColumn("_src_file", F.regexp_replace(F.input_file_name(), r"^s3a?://[^/]+/", ""))
                  .withColumn("_src_row", F.monotonically_increasing_id())), info

    def _shape(self, df):
        """Lấy mẫu theo khoá + chọn cột, TRƯỚC khi persist: phần bỏ đi không bao giờ
        được cache hay ghi xuống S3."""
        sp = self.cfg.spark
        if sp.sample:
            df = df.where(sample_condition(sp.sample["keys"], sp.sample["keep_pct"]))
            print(f"[{self.cfg.name}] lay mau {sp.sample['keep_pct']}% theo {sp.sample['keys']}")
        if sp.select:
            df = df.select(*[f"`{c}`" for c in sp.select],
                           *[f"`{c}`" for c in df.columns if c.startswith("_src_")])
        return df

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

            self._spark_conf()
            if cfg.source_type is SourceType.JDBC:
                df, info = self.read_jdbc(checkpoint)
            else:
                df, info = self.read_s3file(checkpoint)

            # s3file cdc1: mốc = khoá file cuối, có cả khi mọi file chỉ có header
            new_wm = info.get("watermark")
            mkey = None
            if df is not None:
                df = self._shape(df).persist(StorageLevel.MEMORY_AND_DISK)
                stats.records_read = df.count()
                stats.batches = df.rdd.getNumPartitions()
                print(f"[{cfg.name}] doc {stats.records_read} dong / {stats.batches} partition "
                      f"trong {time.monotonic() - t0:.1f}s")
                if stats.records_read and cfg.cdc.mode is CdcMode.CDC1 and new_wm is None:
                    wcol = cfg.cdc.watermark_column
                    new_wm = df.agg(F.max(F.col(f"`{wcol}`"))).collect()[0][0]
                    new_wm = new_wm.isoformat() if hasattr(new_wm, "isoformat") else str(new_wm)

            if stats.records_read and not self.dry_run:
                out_prefix = f"{writer.prefix}/spark-{self.run_id}"
                self.spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                # ignoreNullFields=false: giu khoa null nhu json.dumps cua writer Python
                n_out = output_files(stats.records_read, cfg.target.max_records_per_file,
                                     cfg.spark.output_files)
                out = self._to_raw(df)
                # df đã persist: coalesce gộp partition ĐÃ CACHE, không đọc lại nguồn và
                # không shuffle. Cần NHIỀU file hơn số partition thì mới repartition.
                out = out.coalesce(n_out) if n_out <= stats.batches else out.repartition(n_out)
                print(f"[{cfg.name}] ghi {n_out} file ({stats.batches} partition doc)")
                (out.write.mode("append").option("compression", "gzip")
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
                extra.update({k: v for k, v in info.items() if k != "watermark"})
                if cfg.spark.sample:
                    # raw là TẬP CON: silver/gold phải biết để không diễn giải sai tỷ lệ
                    extra["sample"] = cfg.spark.sample
                cap = cfg.runtime.glue_capacity()
                if cap:
                    extra["capacity"] = {"worker_type": cap[0], "workers": cap[1]}
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


def plan_partitions(lo: Any, hi: Any, count: Optional[int], par: Dict[str, Any]) -> int:
    """Số partition JDBC theo cột.

    num_partitions khai cứng thắng. rows_per_partition cần COUNT: ceil(count / rpp),
    chặn trên bởi max_partitions (số kết nối DB nguồn chịu được). Khoảng số nguyên
    hẹp hơn N thì không chia nhỏ hơn 1 giá trị/partition (partition rỗng vẫn tốn một
    kết nối + một truy vấn).
    """
    if "num_partitions" in par:
        n = int(par["num_partitions"])
    elif "rows_per_partition" in par and count is not None:
        n = min(math.ceil(count / int(par["rows_per_partition"])), int(par.get("max_partitions", 16)))
    else:
        n = 8
    if isinstance(lo, int) and isinstance(hi, int) and not isinstance(lo, bool):
        n = min(n, hi - lo + 1)
    return max(1, n)


def sample_condition(keys: List[str], keep_pct: float):
    """Giữ dòng khi MỌI khoá rơi vào keep_pct% đầu của pmod(xxhash64(khoá), 10000).

    Ép chuỗi trước khi băm: cùng tài khoản "8000EBD30" ở bảng giao dịch (chuỗi) và
    bảng khác (có thể là số) cho cùng quyết định. Tất định: chạy lại ra đúng tập cũ.
    """
    from functools import reduce

    from pyspark.sql import functions as F

    cut = int(round(keep_pct * 100))
    conds = [F.pmod(F.xxhash64(F.col(f"`{k}`").cast("string")), F.lit(10000)) < cut for k in keys]
    return reduce(lambda a, b: a & b, conds)


def output_files(records: int, max_records_per_file: int, configured: int = 0) -> int:
    """Số file raw: khai cứng, hoặc đủ để mỗi file <= max_records_per_file dòng."""
    if configured:
        return configured
    return max(1, math.ceil(records / max(1, max_records_per_file)))


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
