"""
Tính bao đóng bắc cầu quan hệ sở hữu (mục 4 tài liệu thiết kế).

Luồng A không dùng được WITH RECURSIVE của Athena một cách tin cậy, nên bảng
onto._closure KHÔNG phải là tối ưu hoá tuỳ chọn — nó là thành phần bắt buộc.
Job này chạy hằng đêm, API chỉ đọc bảng đã tính sẵn.

Ba chi tiết đáng viết vào luận văn nằm ở vòng lặp bên dưới: left_anti chống
chu trình, localCheckpoint cắt lineage, isEmpty dừng sớm.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "MAX_DEPTH"])
MAX_DEPTH = int(args["MAX_DEPTH"])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

edges = (
    spark.table("glue_catalog.track_a_onto.triple")
    .filter(F.col("predicate_iri") == "fin:isDirectlyOwnedBy")
    .filter(
        F.current_date().between(
            F.col("valid_from"),
            F.coalesce(F.col("valid_to"), F.lit("9999-12-31").cast("date")),
        )
    )
    .select("subject_uid", "object_uid", "weight")
    .cache()
)

frontier = edges.select(
    F.col("subject_uid").alias("start_uid"),
    F.col("object_uid").alias("node_uid"),
    F.lit(1).alias("depth"),
    F.col("weight").alias("eff_pct"),
)
closure = frontier

for _ in range(MAX_DEPTH):
    nxt = (
        frontier.alias("f")
        .join(edges.alias("e"), F.col("f.node_uid") == F.col("e.subject_uid"))
        .select(
            F.col("f.start_uid"),
            F.col("e.object_uid").alias("node_uid"),
            (F.col("f.depth") + 1).alias("depth"),
            (F.col("f.eff_pct") * F.col("e.weight")).alias("eff_pct"),
        )
        # Chống chu trình: bỏ cặp (start, node) đã thấy ở vòng trước.
        # Thiếu dòng này, một vòng sở hữu chéo làm job chạy đủ MAX_DEPTH vòng
        # và sinh ra dữ liệu rác nhân lên theo cấp số nhân.
        .join(
            closure.select("start_uid", "node_uid"),
            ["start_uid", "node_uid"],
            "left_anti",
        )
        # Cắt lineage. Thiếu dòng này, kế hoạch thực thi của Spark phình theo
        # cấp số nhân và job chết ở vòng thứ sáu hoặc bảy — lỗi trông như
        # tràn bộ nhớ driver chứ không chỉ ra nguyên nhân thật.
        .localCheckpoint(eager=True)
    )

    # Dừng sớm khi đồ thị nông hơn MAX_DEPTH
    if nxt.isEmpty():
        break

    closure = closure.unionByName(nxt)
    frontier = nxt

(
    closure.withColumn("as_of_date", F.current_date())
    .withColumnRenamed("start_uid", "descendant_uid")
    .withColumnRenamed("node_uid", "ancestor_uid")
    .withColumnRenamed("eff_pct", "effective_weight")
    .write.format("iceberg")
    .mode("overwrite")
    .saveAsTable("glue_catalog.track_a_onto._closure")
)

job.commit()
