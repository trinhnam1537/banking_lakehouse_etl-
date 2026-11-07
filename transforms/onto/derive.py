"""onto.derive — suy diễn tính sẵn trên đồ thị (bước O9, docs/ONTOLOGY-BUILD.md; ONTOLOGY.md mục 6).

    aws glue start-job-run --job-name fingraph-dev-onto-spark --arguments '{"--TRANSFORM":"onto.derive"}'
    # mốc cắt train riêng một nguồn: "--CUTOFF_BERKA":"1997-07-01"

Vì sao tính sẵn: Athena không đệ quy tin cậy, LLM suy luận bắc cầu sai, model cần đặc trưng dạng bảng.
Mọi suy diễn thành bảng Iceberg; tool agent và pre-train chỉ đọc.

  closure             bao đóng bắc cầu partOf* (quận -> vùng -> nước), độ sâu tới MAX_DEPTH.
                      Cùng hàm dùng cho isDirectlyOwnedBy* khi OpenSanctions lên đồ thị.
  product_transition  P(mua B trong H tháng | đã có A) và lift, theo lớp sản phẩm của taxonomy
                      (dùng chung cho mọi nguồn có trục thời gian).

CHỐNG RÒ RỈ (bắt buộc, O9): product_transition chỉ dùng sự kiện TRƯỚC mốc cắt train của từng nguồn.
Các nguồn nằm trên trục thời gian khác nhau (Berka 1993–1998, corebank dời +24 năm, MBD 2022), nên
mốc cắt tính RIÊNG mỗi nguồn: mặc định phân vị 80% của ngày mua (`--CUTOFF_<NGUON>` để ghi đè).
Thêm chống CENSORING: sản phẩm A chỉ được đếm nếu mua sớm ít nhất H tháng trước mốc cắt — nếu không,
khách chưa kịp "có cơ hội" mua B và xác suất bị kéo thấp.
Bank Marketing không có ngày (valid_from NULL) nên tự động không tham gia.
"""

from __future__ import annotations

from typing import Any, Dict, List

from ..common import CATALOG, write_table

DB = "track_a_onto"
HORIZONS = (3, 6, 12)
MAX_DEPTH = 6
TRANSITIVE = ("fin:partOf",)
CUTOFF_QUANTILE = 0.8
# Khach trung giua nguon (fin:sameAs, O8): chi giu ban cua nguon dung TRUOC trong danh sach. Khong bo
# thi lich su mua cua cung mot nguoi (Berka va corebank nap tu Berka) bi dem HAI lan: p khong doi nhung
# n gap doi -> do tin cay gia tao (gap tren dev 11-07: n = 7.234 thay vi ~3.600).
SOURCE_PRIORITY = ("corebank", "berka")


def closure(triple, predicates=TRANSITIVE, max_depth: int = MAX_DEPTH):
    """(start_uid, node_uid, predicate_iri, depth) với depth >= 1. Lặp join tới khi không thêm được
    cặp mới hoặc chạm MAX_DEPTH — chu trình không làm lặp vô hạn."""
    from pyspark.sql import functions as F

    edges = (triple.where(F.col("predicate_iri").isin(*predicates) & F.col("object_uid").isNotNull())
             .select(F.col("subject_uid").alias("s"), F.col("object_uid").alias("o"), "predicate_iri").distinct())
    edges = edges.localCheckpoint()
    reach = edges.select(F.col("s").alias("start_uid"), F.col("o").alias("node_uid"), "predicate_iri",
                         F.lit(1).alias("depth")).localCheckpoint()
    # frontier, reach, new phai la cac checkpoint RIENG: cung lineage thi join frontier->reach bi
    # "conflicting references in Join" (Spark tren Glue 5, gap 11-07; Spark local cho qua).
    frontier = reach.localCheckpoint()
    for d in range(2, max_depth + 1):
        step = (frontier.alias("f").join(edges.alias("e"), (F.col("f.node_uid") == F.col("e.s"))
                                         & (F.col("f.predicate_iri") == F.col("e.predicate_iri")))
                .select(F.col("f.start_uid"), F.col("e.o").alias("node_uid"), F.col("f.predicate_iri"),
                        F.lit(d).alias("depth"))
                .where(F.col("start_uid") != F.col("node_uid")))
        new = step.join(reach, ["start_uid", "node_uid", "predicate_iri"], "left_anti").dropDuplicates(
            ["start_uid", "node_uid", "predicate_iri"]).localCheckpoint()
        if new.isEmpty():
            break
        reach = reach.unionByName(new).localCheckpoint()
        frontier = new
    return reach


def acquisitions(entity, triple):
    """Lần ĐẦU mỗi khách có mỗi lớp sản phẩm: (customer_uid, source_system, product, acquired)."""
    from pyspark.sql import functions as F

    h = triple.where("predicate_iri = 'fin:hasHolding'").select(
        F.col("subject_uid").alias("customer_uid"), F.col("object_uid").alias("h"), F.col("valid_from").alias("t"))
    o = triple.where("predicate_iri = 'fin:ofProduct'").select(F.col("subject_uid").alias("h"),
                                                               F.col("object_uid").alias("p"))
    prod = entity.select(F.col("uid").alias("p"), F.col("class_iri").alias("product"))
    cust = entity.select(F.col("uid").alias("customer_uid"), "source_system")
    acq = (h.where(F.col("t").isNotNull()).join(o, "h").join(prod, "p").join(cust, "customer_uid")
           .groupBy("customer_uid", "source_system", "product").agg(F.min("t").alias("acquired")))
    return acq.join(duplicates(entity, triple), "customer_uid", "left_anti")


def duplicates(entity, triple):
    """uid khach se BO vi da co ban o nguon uu tien hon (cap fin:sameAs)."""
    from pyspark.sql import functions as F

    rank = F.create_map(*[x for i, s in enumerate(SOURCE_PRIORITY) for x in (F.lit(s), F.lit(i))])
    src = entity.select("uid", "source_system")
    pairs = (triple.where("predicate_iri = 'fin:sameAs'").select(F.col("subject_uid").alias("a"),
                                                               F.col("object_uid").alias("b"))
             .join(src.withColumnRenamed("uid", "a").withColumnRenamed("source_system", "sa"), "a")
             .join(src.withColumnRenamed("uid", "b").withColumnRenamed("source_system", "sb"), "b")
             .withColumn("ra", F.coalesce(rank[F.col("sa")], F.lit(99)))
             .withColumn("rb", F.coalesce(rank[F.col("sb")], F.lit(99))))
    # Bo ben co hang KEM hon (so lon hon); bang hang thi giu ca hai (khong doan)
    return (pairs.where("ra < rb").select(F.col("b").alias("customer_uid"))
            .union(pairs.where("rb < ra").select(F.col("a").alias("customer_uid"))).distinct())


def cutoffs(acq, overrides: Dict[str, str]) -> Dict[str, str]:
    from pyspark.sql import functions as F

    q = {r[0]: str(r[1]) for r in acq.groupBy("source_system").agg(
        F.expr(f"percentile_approx(acquired, {CUTOFF_QUANTILE})").alias("c")).collect()}
    q.update({k.lower(): v for k, v in overrides.items()})
    return q


def product_transition(acq, cut: Dict[str, str], horizons=HORIZONS):
    """(from_product, to_product, horizon_months, n_from, n_both, p, base_rate, lift, sources)."""
    from pyspark.sql import functions as F

    cut_col = F.create_map(*[x for k, v in cut.items() for x in (F.lit(k), F.lit(v).cast("date"))])
    train = acq.withColumn("cutoff", cut_col[F.col("source_system")]).where(F.col("acquired") < F.col("cutoff"))
    n_cust = train.select("customer_uid").distinct().count()
    out = None
    for hz in horizons:
        # A đủ sớm để còn quan sát trọn H tháng trước mốc cắt (chống censoring)
        a = (train.where(F.add_months("acquired", hz) <= F.col("cutoff"))
             .select("customer_uid", "source_system", F.col("product").alias("from_product"),
                     F.col("acquired").alias("ta"))).localCheckpoint()
        # a, b, base tu cung `train`: checkpoint rieng de join khong "conflicting references" (xem closure)
        b = train.select("customer_uid", F.col("product").alias("to_product"),
                         F.col("acquired").alias("tb")).localCheckpoint()
        both = (a.join(b, "customer_uid")
                .where((F.col("from_product") != F.col("to_product")) & (F.col("tb") > F.col("ta"))
                       & (F.col("tb") <= F.add_months("ta", hz))))
        n_from = a.groupBy("from_product").agg(F.countDistinct("customer_uid").alias("n_from"),
                                               F.array_sort(F.collect_set("source_system")).alias("sources"))
        n_both = both.groupBy("from_product", "to_product").agg(F.countDistinct("customer_uid").alias("n_both"))
        base = train.groupBy(F.col("product").alias("to_product")).agg(
            (F.countDistinct("customer_uid") / F.lit(max(n_cust, 1))).alias("base_rate")).localCheckpoint()
        df = (n_both.join(n_from, "from_product").join(base, "to_product")
              .withColumn("horizon_months", F.lit(hz))
              .withColumn("p", F.col("n_both") / F.col("n_from"))
              .withColumn("lift", F.col("p") / F.col("base_rate")))
        out = df if out is None else out.unionByName(df)
    return out.select("from_product", "to_product", "horizon_months", "n_from", "n_both", "p", "base_rate",
                      "lift", "sources")


def run(spark, args: Dict[str, str]) -> Dict[str, Any]:
    from pyspark.sql import functions as F

    db = args.get("ONTO_DB", DB)
    entity, triple = (spark.table(f"{CATALOG}.{db}.{t}") for t in ("entity", "triple"))
    run_id = triple.select("run_id").first()[0]
    stamp = [F.lit(run_id).alias("graph_run_id"), F.current_timestamp().alias("recorded_at")]

    clo = closure(triple)
    n_clo = write_table(clo.select("*", *stamp), db, "closure",
                        comment="O9: bao dong bac cau tinh san (partOf*), depth >= 1")

    acq = acquisitions(entity, triple).cache()
    cut = cutoffs(acq, {k[len("CUTOFF_"):]: v for k, v in args.items() if k.startswith("CUTOFF_")})
    print(f"  moc cat train theo nguon: {cut}")
    pt = product_transition(acq, cut)
    cut_json = ",".join(f"{k}={v}" for k, v in sorted(cut.items()))
    n_pt = write_table(pt.select("*", F.lit(cut_json).alias("train_cutoffs"), *stamp), db, "product_transition",
                       comment="O9: P(mua B trong H thang | co A) + lift, CHI khoang train (truoc moc cat tung nguon)")
    top = (pt.where("horizon_months = 6 AND n_from >= 30").orderBy(F.col("lift").desc()).limit(5).collect())
    for r in top:
        print(f"    {r.from_product} -> {r.to_product} (6 thang): p={r.p:.3f} lift={r.lift:.2f} n={r.n_from}")
    acq.unpersist()
    return {"graph_run_id": run_id, "closure": n_clo, "product_transition": n_pt, "cutoffs": cut}
