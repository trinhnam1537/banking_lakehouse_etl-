"""
Phương án A1: gộp file do Kafka Connect S3 Sink ghi ra thành bảng Iceberg bronze.

Chạy 5 phút một lần qua EventBridge Scheduler -> Step Functions -> Glue job.
Độ trễ đầu-cuối 2-7 phút, chi phí gần như bằng không khi không chạy.

Terraform quản lý file này: sửa xong `terraform apply` là job chạy bản mới.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ["JOB_NAME", "LAKEHOUSE_BUCKET"])
BUCKET = args["LAKEHOUSE_BUCKET"]

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

TRADE_SCHEMA = T.StructType([
    T.StructField("venue", T.StringType()),
    T.StructField("symbol", T.StringType()),
    T.StructField("trade_id", T.LongType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("qty", T.DoubleType()),
    T.StructField("exchange_ts_ms", T.LongType()),
])

TABLE = "glue_catalog.track_a_bronze.trades_raw"

# Job bookmark của Glue nhớ file nào đã đọc, nên đây luôn là dữ liệu mới.
# Nếu cần nạp lại từ đầu: chạy với --job-bookmark-option job-bookmark-reset.
raw = (
    glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [f"s3://{BUCKET}/landing/kafka/trades/"],
            "recurse": True,
            "groupFiles": "inPartition",  # gộp file nhỏ ngay lúc đọc
            "groupSize": "134217728",
        },
        format="json",
        transformation_ctx="trades_landing",  # bookmark bám vào tên này, đừng đổi
    )
    .toDF()
)

if raw.rdd.isEmpty():
    # Không có file mới — thoát sạch thay vì ghi một snapshot Iceberg rỗng.
    # Snapshot rỗng vẫn tính vào lịch sử bảng và làm nhiễu số liệu độ tươi ở mục 2.6.
    job.commit()
    sys.exit(0)

trades = (
    raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
    .select("t.*")
    .withColumn("exchange_ts", (F.col("exchange_ts_ms") / 1000).cast("timestamp"))
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("payload", F.col("value").cast("string"))
    .drop("exchange_ts_ms")
    # Connect có thể ghi trùng khi retry -> khử trùng lặp theo khoá nghiệp vụ
    .dropDuplicates(["venue", "trade_id"])
)

# MERGE INTO thay vì append: idempotent, chạy lại cùng lô không sinh bản ghi thừa.
trades.createOrReplaceTempView("incoming")
spark.sql(f"""
    MERGE INTO {TABLE} AS t
    USING incoming AS s
      ON t.venue = s.venue AND t.trade_id = s.trade_id
    WHEN NOT MATCHED THEN INSERT *
""")

job.commit()
