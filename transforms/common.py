"""Tiện ích dùng chung cho job gold/pre-train: đọc silver, ghi Iceberg."""

from __future__ import annotations

from typing import Iterable, Optional

CATALOG = "glue_catalog"  # khop spark.sql.catalog.glue_catalog trong --conf cua Glue job


def table(spark, db: str, name: str):
    return spark.table(f"{CATALOG}.{db}.{name}")


def current(df, key: str):
    """Bản ghi hiện hành của bảng silver: bỏ dòng xoá (ds_record_status = 'D'), và nếu
    một khoá xuất hiện nhiều lần (writer append, nguồn bị kéo chồng) thì lấy bản mới nhất
    theo ds_etl_timestamp. Bảng không có cột ds_* thì trả nguyên."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    cols = set(df.columns)
    if "ds_record_status" in cols:
        df = df.where(F.coalesce(F.col("ds_record_status"), F.lit("I")) != "D")
    if "ds_etl_timestamp" in cols:
        w = Window.partitionBy(key).orderBy(F.col("ds_etl_timestamp").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return df.drop(*[c for c in df.columns if c.startswith("ds_")])


def write_table(df, db: str, name: str, partition_by: Optional[Iterable[str]] = None,
                comment: str = "") -> int:
    """Ghi đè TOÀN BỘ bảng Iceberg (createOrReplace): tầng gold/pre-train tính lại từ
    silver mỗi lần, idempotent — chạy lại bao nhiêu lần cũng ra cùng kết quả, và Iceberg
    giữ snapshot cũ để time travel / so sánh hai phiên bản bộ dữ liệu."""
    from pyspark.sql import functions as F

    full = f"{CATALOG}.{db}.{name}"
    w = (df.writeTo(full).using("iceberg")
         .tableProperty("format-version", "2")
         .tableProperty("write.parquet.compression-codec", "zstd"))
    if comment:
        w = w.tableProperty("comment", comment)
    if partition_by:
        w = w.partitionedBy(*[F.col(c) for c in partition_by])
    w.createOrReplace()
    n = df.sparkSession.table(full).count()
    print(f"  ghi {full}: {n} dong")
    return n
