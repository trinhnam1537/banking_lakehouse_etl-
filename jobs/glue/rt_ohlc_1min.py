"""
Phương án A2: Glue Streaming đọc thẳng Kafka, ghi nến 1 phút vào Iceberg gold.

Độ trễ 1-2 phút, nhưng tốn ~20 USD/NGÀY vì Glue Streaming tính tối thiểu
2 DPU chạy liên tục. Chỉ bật trong tuần benchmark (tuần 14), chạy xong tắt ngay.

Bật bằng:  terraform apply -var="enable_streaming=true" -var="enable_nat_gateway=true"
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "KAFKA_BOOTSTRAP", "CHECKPOINT_PATH"]
)

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

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", args["KAFKA_BOOTSTRAP"])
    .option("subscribe", "fingraph.trades")
    .option("startingOffsets", "latest")
    # Trần số bản ghi mỗi micro-batch. Thiếu tham số này, lần chạy đầu sau
    # một quãng dừng sẽ nuốt toàn bộ 72 giờ log cùng lúc và làm chết executor.
    .option("maxOffsetsPerTrigger", 300000)
    .load()
)

trades = (
    raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
    .select("t.*")
    .withColumn("exchange_ts", (F.col("exchange_ts_ms") / 1000).cast("timestamp"))
    # Watermark 5 phút: bản tin đến muộn hơn thế bị bỏ. Đây là đánh đổi
    # giữa độ chính xác và dung lượng state — nên nêu rõ trong luận văn.
    .withWatermark("exchange_ts", "5 minutes")
    .dropDuplicates(["venue", "trade_id"])
)

bars = trades.groupBy(
    F.window("exchange_ts", "1 minute"), "venue", "symbol"
).agg(
    F.first("price").alias("open_px"),
    F.max("price").alias("high_px"),
    F.min("price").alias("low_px"),
    F.last("price").alias("close_px"),
    F.sum("qty").alias("volume"),
    F.count("*").alias("trade_count"),
)

out = bars.select(
    F.col("window.start").alias("bar_ts"),
    "venue",
    "symbol",
    "open_px",
    "high_px",
    "low_px",
    "close_px",
    "volume",
    "trade_count",
)

query = (
    out.writeStream.format("iceberg")
    .outputMode("append")
    # Checkpoint là thứ giữ offset Kafka. Xoá nó = đọc lại từ đầu.
    # Đổi schema bảng mà không xoá checkpoint = job chết khi khởi động.
    .option("checkpointLocation", args["CHECKPOINT_PATH"])
    .option("fanout-enabled", "true")
    .trigger(processingTime="60 seconds")
    .toTable("glue_catalog.track_a_gold.rt_ohlc_1min")
)

query.awaitTermination()
