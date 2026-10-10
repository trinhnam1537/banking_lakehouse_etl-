"""Điểm vào Glue job silver và chạy tay.

Chế độ (--MODE):
  run        (mặc định) raw -> silver cho một entry
  ddl        chỉ tạo / tiến hoá bảng của một entry (không đọc dữ liệu)
  apply_ddl  chạy MỌI file .sql dưới --DDL_PREFIX — tạo bảng trên Glue Catalog
             từ thư mục silver_etl/ddl/ (Terraform upload lên S3)

Cấu hình:
  --JOB_CONFIG_TABLE + --JOB_NAME   từ DynamoDB (Glue)
  --config <file.yml>               từ file (chạy tay)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Optional

from .config import SilverConfig, from_dict, load_config
from .ddl import ensure_table, split_statements


def spark_session():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    # BAT BUOC: Spark mac dinh theo mui gio may. Glue la UTC, may dev la UTC+7
    # -> cung du lieu ra ds_partition_date khac nhau. Ghim UTC o mot cho.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def _load_from_dynamo(table: str, job_name: str) -> SilverConfig:
    from .state import JobConfigStore

    item = JobConfigStore(table).get(job_name)
    if item is None:
        raise SystemExit(f"Khong thay entry silver '{job_name}' trong {table}")
    raw = dict(item.get("config") or {})
    raw.setdefault("name", job_name)
    raw["enabled"] = item.get("enabled", True)
    return from_dict(raw, default_name=job_name)


def _read_text(uri: str) -> Optional[str]:
    """Đọc file DDL: đường dẫn máy hoặc s3://. Không có thì None."""
    if uri.startswith("s3://"):
        import boto3
        from botocore.exceptions import ClientError

        bucket, key = uri[5:].split("/", 1)
        try:
            return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
    return open(uri, encoding="utf-8").read() if os.path.isfile(uri) else None


def _ddl_for(cfg: SilverConfig, ddl_prefix: Optional[str]) -> Optional[str]:
    if not cfg.target.ddl:
        return None
    root = ddl_prefix or "silver_etl/ddl"
    text = _read_text(f"{root.rstrip('/')}/{cfg.target.ddl}")
    if text is None:
        raise SystemExit(f"[{cfg.name}] target.ddl '{cfg.target.ddl}' khong thay duoi {root}")
    return text


def apply_ddl_dir(spark, ddl_prefix: str) -> int:
    """Chạy mọi .sql dưới prefix, theo thứ tự tên. Trả về số câu lệnh."""
    files = []
    if ddl_prefix.startswith("s3://"):
        import boto3

        bucket, prefix = ddl_prefix[5:].split("/", 1)
        for page in boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            files += [f"s3://{bucket}/{o['Key']}" for o in page.get("Contents", []) if o["Key"].endswith(".sql")]
    else:
        for root, _, names in os.walk(ddl_prefix):
            files += [os.path.join(root, n) for n in names if n.endswith(".sql")]

    n = 0
    for f in sorted(files):
        for stmt in split_statements(_read_text(f) or ""):
            if stmt.upper().startswith("CREATE TABLE"):
                # Dam bao database ton tai (glue_catalog.db.table -> glue_catalog.db)
                fqn = stmt.split("(")[0].split()[-1]
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {fqn.rsplit('.', 1)[0]}")
            spark.sql(stmt)
            n += 1
        print(f"DDL ok: {f}")
    return n


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="raw -> silver")
    p.add_argument("--config")
    p.add_argument("--JOB_NAME", dest="job_name")
    p.add_argument("--JOB_CONFIG_TABLE", dest="config_table")
    p.add_argument("--JOBLOG_TABLE", dest="joblog_table")
    p.add_argument("--RAW_BUCKET", dest="bucket")
    p.add_argument("--MANIFEST", dest="manifest")
    p.add_argument("--DATA_DATE", dest="data_date")
    p.add_argument("--REBUILD_FROM", dest="rebuild_from")
    p.add_argument("--RUN_ID", dest="run_id")
    p.add_argument("--DDL_PREFIX", dest="ddl_prefix")
    p.add_argument("--MODE", dest="mode", default="run", choices=["run", "ddl", "apply_ddl"])
    args, _ = p.parse_known_args(argv)

    # SFN truyen chuoi rong cho tham so khong dung ("--MANIFEST ''") -> coi nhu khong co
    for k in ("manifest", "data_date", "rebuild_from", "run_id"):
        if getattr(args, k) in ("", "-", "none", "None"):
            setattr(args, k, None)

    spark = spark_session()

    if args.mode == "apply_ddl":
        n = apply_ddl_dir(spark, args.ddl_prefix or "silver_etl/ddl")
        print(f"apply_ddl: {n} cau lenh")
        return 0

    if args.config:
        cfg = load_config(args.config)
    elif args.config_table and args.job_name:
        cfg = _load_from_dynamo(args.config_table, args.job_name)
    else:
        raise SystemExit("Can --config, hoac --JOB_CONFIG_TABLE va --JOB_NAME")

    if not cfg.enabled:
        print(f"[{cfg.name}] enabled=false -> bo qua")
        return 0

    ddl_text = _ddl_for(cfg, args.ddl_prefix)
    if args.mode == "ddl":
        print(json.dumps(ensure_table(spark, cfg, ddl_text)))
        return 0

    from .runner import SilverRunner

    res = SilverRunner(spark, cfg, bucket=args.bucket or os.environ.get("RAW_BUCKET", ""),
                       joblog_table=args.joblog_table, run_id=args.run_id,
                       manifest=args.manifest, data_date=args.data_date,
                       ddl_text=ddl_text, rebuild_from=args.rebuild_from).run()
    return 0 if res.ok else 1


if __name__ == "__main__":
    sys.exit(main())
