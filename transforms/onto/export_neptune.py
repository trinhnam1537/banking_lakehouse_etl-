"""onto.export_neptune — track_a_onto -> CSV bulk load openCypher của Neptune (docs/NEPTUNE.md mục 4–5).

    aws glue start-job-run --job-name fingraph-dev-onto-spark --arguments '{"--TRANSFORM":"onto.export_neptune",
        "--EXPORT_PATH":"s3://<bucket>/_ops/neptune", "--AS_OF":"1997-01-01"}'

Neptune là BẢN CHIẾU dựng lại được từ Iceberg: đỉnh ~id = uid, cạnh ~id = triple_id, nên trích dẫn
của agent trên Neptune tra ngược được về track_a_onto và dòng silver gốc.

Quy tắc chiếu:
  - Nhãn đỉnh = lớp + also_types + MỌI lớp tổ tiên (bảng subclass): MATCH (p:CreditProduct) trả cả
    GoldCard mà Neptune không cần suy luận OWL.
  - Cạnh literal (occupation, holdingStatus, amount) -> thuộc tính của đỉnh chủ thể (giá trị mới nhất).
  - KHÔNG xuất natural_key: Neptune chỉ phân quyền mức cụm, không che cột được như Lake Formation.
  - --AS_OF (tuỳ chọn): chỉ cạnh/đỉnh có valid_from < AS_OF — bản point-in-time cho Neptune Analytics
    tính đặc trưng model mà không rò tương lai (quy tắc O9).
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..common import CATALOG

DB = "track_a_onto"


def _local(col):
    from pyspark.sql import functions as F

    return F.regexp_replace(col, "^[a-z]+:", "")


def frames(entity, triple, subclass, as_of: Optional[str] = None):
    """(vertices, edges) đúng tên cột của bulk loader openCypher."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if as_of:
        cut = F.lit(as_of).cast("date")
        entity = entity.where(F.col("valid_from").isNull() | (F.col("valid_from") < cut))
        triple = triple.where(F.col("valid_from").isNull() | (F.col("valid_from") < cut))

    # Nhãn: lớp chính + also_types, mở rộng theo bao đóng subClassOf
    types = entity.select("uid", F.explode(F.array_union(F.array("class_iri"), F.col("also_types"))).alias("t"))
    labels = (types.join(subclass, types.t == subclass.class_iri, "left")
              .select("uid", F.coalesce("ancestor_iri", "t").alias("a"))
              .groupBy("uid").agg(F.array_join(F.array_sort(F.collect_set(_local(F.col("a")))), ";").alias(":LABEL")))

    # Thuộc tính tĩnh: cột cố định theo tập khoá attributes thực có (bulk loader cần header cố định)
    keys = sorted(r[0] for r in entity.select(F.explode(F.map_keys("attributes"))).distinct().collect())
    # Cạnh literal -> thuộc tính đỉnh, lấy giá trị có valid_from mới nhất
    lit = triple.where(F.col("object_uid").isNull() & F.col("object_literal").isNotNull())
    w = Window.partitionBy("subject_uid", "predicate_iri").orderBy(F.col("valid_from").desc_nulls_last(),
                                                                    F.col("triple_id"))
    lit = (lit.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
           .select("subject_uid", _local(F.col("predicate_iri")).alias("p"), "object_literal"))
    lit_props = sorted(r[0] for r in lit.select("p").distinct().collect())
    lit_wide = (lit.groupBy("subject_uid").pivot("p", lit_props).agg(F.first("object_literal"))
                if lit_props else None)

    v = entity.join(labels, "uid", "left")
    cols = [F.col("uid").alias(":ID"), F.col(":LABEL"), F.col("label").alias("label:String"),
            F.col("source_system").alias("source_system:String"),
            F.col("valid_from").cast("string").alias("valid_from:Date"),
            *[F.col("attributes")[k].alias(f"{k}:String") for k in keys if k not in lit_props]]
    if lit_wide is not None:
        v = v.join(lit_wide, v.uid == lit_wide.subject_uid, "left")
        cols += [F.col(p).alias(f"{p}:String") for p in lit_props]
    vertices = v.select(*cols)

    edges = (triple.where(F.col("object_uid").isNotNull())
             .select(F.col("triple_id").alias(":ID"), F.col("subject_uid").alias(":START_ID"),
                     F.col("object_uid").alias(":END_ID"), _local(F.col("predicate_iri")).alias(":TYPE"),
                     F.col("triple_id").alias("triple_id:String"),
                     F.col("valid_from").cast("string").alias("valid_from:Date"),
                     F.col("valid_to").cast("string").alias("valid_to:Date"),
                     F.col("weight").alias("weight:Double"), F.col("confidence").alias("confidence:Double"),
                     F.col("is_inferred").alias("is_inferred:Bool"),
                     F.col("source_dataset").alias("source_dataset:String")))
    return vertices, edges


def run(spark, args: Dict[str, str]) -> Dict[str, Any]:
    db = args.get("ONTO_DB", DB)
    base = args["EXPORT_PATH"].rstrip("/")
    as_of = args.get("AS_OF")
    ent, tri, sub = (spark.table(f"{CATALOG}.{db}.{t}") for t in ("entity", "triple", "subclass"))
    run_id = tri.select("run_id").first()[0]
    vertices, edges = frames(ent, tri, sub.select("class_iri", "ancestor_iri"), as_of)
    out = f"{base}/{run_id}" + (f"/as_of={as_of}" if as_of else "")
    opts = {"header": "true", "escape": '"', "quoteAll": "false", "emptyValue": ""}
    # Bulk loader nap MOI file duoi prefix: khong de Spark ghi file rong _SUCCESS
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    vertices.write.mode("overwrite").options(**opts).csv(f"{out}/vertices")
    edges.write.mode("overwrite").options(**opts).csv(f"{out}/edges")
    n_v, n_e = spark.read.option("header", "true").csv(f"{out}/vertices").count(), \
        spark.read.option("header", "true").csv(f"{out}/edges").count()
    print(f"  xuat {out}: {n_v} dinh, {n_e} canh (graph run_id {run_id})")
    return {"path": out, "graph_run_id": run_id, "vertices": n_v, "edges": n_e,
            "loader_hint": {"source": out + "/", "format": "opencypher", "parallelism": "HIGH"}}
