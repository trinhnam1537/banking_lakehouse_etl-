"""Chạy THÔNG LUỒNG nguồn -> landing -> silver trên máy, đúng phép nối của orchestrator.

    python -m local.run_flow corebank                    # mọi entry corebank_*: sourcing rồi silver
    python -m local.run_flow corebank_trans              # một entry
    python -m local.run_flow corebank --silver-only      # bỏ sourcing, xử lý manifest mới nhất hôm nay
    python -m local.run_flow corebank --sql "SELECT count(*) FROM glue_catalog.silver_corebank.trans"

Trên AWS:  Glue sourcing ghi manifest -> SFN bắn "Job Run Succeeded" -> Lambda trigger tìm
           entry silver có source.dataset = dataset của manifest -> Glue slv-spark.
Ở đây:     SourceRunner (moto S3) -> cùng phép tìm entry silver -> SilverRunner (Spark local).

Khác Glue ở ba chỗ, đều là hạ tầng chứ không phải logic:
  * file raw và manifest được TẢI từ moto về data/local_lake/ (Spark local không đọc s3://
    nếu không có hadoop-aws), manifest viết lại thành đường file:///
  * catalog Iceberg kiểu hadoop trên đĩa (data/local_warehouse/), vẫn đặt tên glue_catalog
    -> SQL của writer y hệt
  * cần Java 17/21 + pyspark 3.5 (+ winutils trên Windows); lần đầu tải iceberg-spark-runtime

    $env:JAVA_HOME = "C:\\Program Files\\Java\\jdk-21"
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import List, Optional

from . import env  # noqa: F401

import boto3
import yaml

from deploy.build import REPO

LAKE = REPO / "data" / "local_lake"
WAREHOUSE = REPO / "data" / "local_warehouse"
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"


def spark_session():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    WAREHOUSE.mkdir(parents=True, exist_ok=True)
    s = (SparkSession.builder.master("local[2]").appName("fingraph-local-silver")
         .config("spark.jars.packages", ICEBERG_PKG)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.glue_catalog.type", "hadoop")
         .config("spark.sql.catalog.glue_catalog.warehouse", "file:///" + WAREHOUSE.as_posix().lstrip("/"))
         .config("spark.sql.session.timeZone", "UTC")   # giong silver.main.spark_session
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.memory", "2g")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    return s


def source_entries(selector: str) -> List[str]:
    root = REPO / "sourcing_etl" / "sources"
    names = sorted(p.stem for p in root.rglob("*.yml"))
    hit = [n for n in names if n == selector or n.startswith(selector + "_")]
    if not hit:
        raise SystemExit(f"Khong co entry sourcing nao khop '{selector}'")
    return hit


def silver_for(dataset: str):
    """Cùng phép nối với orchestrator.job_types: entry silver BẬT có source.dataset khớp."""
    from silver.config import load_all

    return [c for c in load_all(str(REPO / "silver_etl" / "entries")).values()
            if c.enabled and c.source.dataset == dataset]


def run_source(job: str):
    """Chạy sourcing bằng framework thật; trả (cfg, manifest_key | None)."""
    from sourcing.config import from_dict
    from sourcing.runner import SourceRunner

    path = next((REPO / "sourcing_etl" / "sources").rglob(f"{job}.yml"))
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    raw["enabled"] = True
    raw.setdefault("runtime", {})["profile"] = "src-shell"   # xem local/run_entry.py
    cfg = from_dict(raw)
    res = SourceRunner(cfg, os.environ["RAW_BUCKET"], os.environ["JOBLOG_TABLE"]).run()
    day = dt.datetime.now(dt.timezone.utc).strftime(cfg.target.partition_format)
    key = f"{cfg.target.prefix}/_manifests/{cfg.dataset}/{day}/{res.run_id}.json"
    if res.status != "SUCCESS":
        return cfg, None
    return cfg, key


def latest_manifest(dataset: str) -> Optional[str]:
    s3 = boto3.client("s3")
    day = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    resp = s3.list_objects_v2(Bucket=os.environ["RAW_BUCKET"],
                              Prefix=f"landing/_manifests/{dataset}/ingest_date={day}/")
    objs = sorted(resp.get("Contents", []), key=lambda o: o["LastModified"])
    return objs[-1]["Key"] if objs else None


def materialize(manifest_key: str) -> str:
    """Tải manifest + file raw từ moto về đĩa, viết lại manifest với đường file:///.

    Giữ nguyên cấu trúc `.../ingest_date=YYYY-MM-DD/...`: silver lấy ngày ingest từ ĐƯỜNG DẪN.
    """
    s3, bucket = boto3.client("s3"), os.environ["RAW_BUCKET"]
    m = json.loads(s3.get_object(Bucket=bucket, Key=manifest_key)["Body"].read())
    local_files = []
    for key in m["files"]:
        dst = LAKE / key
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            s3.download_file(bucket, key, str(dst))
        # KHONG dung as_uri(): no ma hoa '=' thanh %3D, Spark doc nguyen van
        local_files.append("file:///" + dst.as_posix().lstrip("/"))
    m["files"] = local_files
    out = LAKE / manifest_key
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(m), encoding="utf-8")
    return str(out)


def run_silver(spark, cfgs, manifest_local: str) -> List[dict]:
    from silver.main import _ddl_for
    from silver.runner import SilverRunner

    out = []
    for c in cfgs:
        try:
            res = SilverRunner(spark, c, bucket=os.environ["RAW_BUCKET"],
                               joblog_table=os.environ["SLV_JOBLOG_TABLE"], manifest=manifest_local,
                               ddl_text=_ddl_for(c, str(REPO / "silver_etl" / "ddl"))).run()
        except Exception as e:  # noqa: BLE001 — mot bang loi khong chan cac bang sau
            out.append({"silver": c.name, "status": "FAILED", "error": f"{type(e).__name__}: {e}"[:300],
                        "table": c.target.fqn})
            continue
        m = res.metrics
        out.append({"silver": c.name, "status": res.status, "rows_in": m.get("rows_in"),
                    "inserted": m.get("inserted"), "updated": m.get("updated"),
                    "unchanged": m.get("unchanged"), "rolled_back": m.get("rolled_back"),
                    "table": c.target.fqn})
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("selector", help="ten entry hoac tien to nguon (vd corebank)")
    ap.add_argument("--silver-only", action="store_true")
    ap.add_argument("--sql", action="append", default=[], help="truy van in ra sau khi chay")
    args = ap.parse_args(argv)

    spark = None
    summary = []
    for job in source_entries(args.selector):
        if args.silver_only:
            from sourcing.config import load_config
            cfg = load_config(str(next((REPO / "sourcing_etl" / "sources").rglob(f"{job}.yml"))))
            mkey = latest_manifest(cfg.dataset)
        else:
            cfg, mkey = run_source(job)
        targets = silver_for(cfg.dataset)
        row = {"source": job, "dataset": cfg.dataset, "manifest": mkey, "silver": []}
        if mkey and targets:
            spark = spark or spark_session()
            row["silver"] = run_silver(spark, targets, materialize(mkey))
        summary.append(row)

    print("\n===== TOM TAT =====")
    for r in summary:
        note = "khong co manifest (NO_DATA)" if not r["manifest"] else \
               ("khong co entry silver doc dataset nay" if not r["silver"] else "")
        print(f"{r['source']:26s} -> {r['dataset']:24s} {note}")
        for s in r["silver"]:
            if s["status"] == "FAILED":
                print(f"    silver {s['silver']:24s} FAILED   {s['error']}")
                continue
            print(f"    silver {s['silver']:24s} {s['status']:8s} in={s['rows_in']} I={s['inserted']} "
                  f"U={s['updated']} N={s['unchanged']} rollback={s['rolled_back']}")
    for q in args.sql:
        spark = spark or spark_session()
        print(f"\n> {q}")
        spark.sql(q).show(50, truncate=False)
    return 0 if all(s["status"] in ("SUCCESS", "NO_DATA") for r in summary for s in r["silver"]) else 1


if __name__ == "__main__":
    sys.exit(main())
