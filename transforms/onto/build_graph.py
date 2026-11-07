"""onto.build_graph — mapping + silver -> track_a_onto.entity, track_a_onto.triple (bước O7).

    aws glue start-job-run --job-name fingraph-dev-onto-spark \
        --arguments '{"--TRANSFORM":"onto.build_graph"}'
    # chỉ dựng một số nguồn:  "--MAPPINGS":"berka,mbd"

Luồng:
  1. Đọc TBox (tbox.json) + mapping (mappings/*.yml), KIỂM TĨNH (mapping.validate): lớp,
     quan hệ, domain/range. Sai thì dừng trước khi đọc dữ liệu.
  2. Mỗi bảng silver được tham chiếu -> view tạm chỉ giữ bản HIỆN HÀNH (ds_is_current, bỏ
     dòng xoá, khoá trùng lấy bản mới nhất theo ds_etl_timestamp).
  3. Sinh node (entities + node sản phẩm từ TBox + node ProductHolding) và cạnh.
  4. KIỂM BẤT BIẾN (docs/ONTOLOGY-BUILD.md O7). Vi phạm -> raise, KHÔNG ghi: bảng giữ phiên
     bản cũ, agent không bao giờ đọc một đồ thị hỏng.
  5. Ghi đè toàn bộ (createOrReplace): idempotent, snapshot Iceberg cũ còn để so phiên bản.

Mỗi lần dựng lại TOÀN BỘ nguồn được chọn. Khi chỉ dựng một số nguồn (--MAPPINGS), các
nguồn khác bị xoá khỏi bảng — dùng cho thử nghiệm; lịch chạy thật dựng tất cả.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List

from ..common import CATALOG, write_table
from . import ids
from . import mapping as M
from . import resolve as R
from . import tbox as T

DB = "track_a_onto"
OPEN_END = "9999-12-31"
ENTITY_COLS = ["uid", "class_iri", "also_types", "label", "source_system", "source_dataset",
               "natural_key", "attributes", "valid_from", "valid_to"]
TRIPLE_COLS = ["subject_uid", "predicate_iri", "object_uid", "weight", "valid_from", "valid_to",
               "triple_id", "object_literal", "object_datatype", "source_dataset", "source_key",
               "confidence", "is_inferred", "mapping_id"]


class InvariantError(RuntimeError):
    pass


# ---------------------------------------------------------------- nguồn

def current_view(df):
    """Bản hiện hành của một bảng silver bất kỳ (simple, scd2, scd4a)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    cols = set(df.columns)
    if "ds_is_current" in cols:
        df = df.where(F.col("ds_is_current"))
    if "ds_record_status" in cols:
        df = df.where(F.coalesce(F.col("ds_record_status"), F.lit("I")) != "D")
    if "ds_key" in cols and "ds_etl_timestamp" in cols:
        w = Window.partitionBy("ds_key").orderBy(F.col("ds_etl_timestamp").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return df.drop(*[c for c in df.columns if c.startswith("ds_")])


def view_name(table: str) -> str:
    return "_onto_" + table.replace(".", "__")


def register_sources(spark, tables) -> None:
    for t in sorted(tables):
        current_view(spark.table(f"{CATALOG}.{t}")).createOrReplaceTempView(view_name(t))


def source_df(spark, src: M.Source):
    return spark.sql(M.TABLE_REF.sub(lambda m: view_name(f"{m.group(1)}.{m.group(2)}"), src.sql))


# ---------------------------------------------------------------- node, cạnh

def _date(expr):
    """Biểu thức ngày; 9999-12-31 (mốc mở của SCD2) -> NULL = còn hiệu lực."""
    from pyspark.sql import functions as F

    if expr is None:
        return F.lit(None).cast("date")
    c = F.expr(expr).cast("date")
    return F.when(c >= F.lit(OPEN_END).cast("date"), F.lit(None).cast("date")).otherwise(c)


def _nkey(names, exprs):
    from pyspark.sql import functions as F

    return ids.natural_key_col(list(names), [F.expr(x) for x in exprs])


def _null_key(exprs):
    from pyspark.sql import functions as F

    return F.greatest(*[F.expr(x).isNull() for x in exprs]) if len(exprs) > 1 else F.expr(exprs[0]).isNull()


def _ref_uid(m: M.Mapping, ref: M.Ref, concepts=None):
    if ref.concept is not None:
        # notation -> IRI khái niệm -> uid node khái niệm. Không tra được thì trỏ vào IRI không tồn tại
        # để bất biến "cạnh mồ côi" chặn lại (NULL sẽ lọt vì cạnh literal có object_uid NULL).
        from pyspark.sql import functions as F

        lookup = F.create_map(*[x for k, v in (concepts or {}).items() for x in (F.lit(k), F.lit(v))])
        iri = F.coalesce(lookup[F.upper(F.trim(F.expr(ref.concept)))], F.lit("fin:UNRESOLVED_CONCEPT"))
        return _product_uid_col(iri)
    e = m.entities[ref.entity]
    return ids.uid_col(e.cls, m.source_system, _nkey(e.key.keys(), ref.key))


def entity_frame(spark, m: M.Mapping, e: M.Entity):
    from pyspark.sql import functions as F

    df = source_df(spark, e.source)
    attrs = (F.create_map(*[x for k, v in e.attributes.items() for x in (F.lit(k), F.expr(v).cast("string"))])
             if e.attributes else F.create_map().cast("map<string,string>"))
    nk = _nkey(e.key.keys(), list(e.key.values()))
    return df.select(
        ids.uid_col(e.cls, m.source_system, nk).alias("uid"),
        F.lit(e.cls).alias("class_iri"),
        F.array(*[F.lit(a) for a in e.also]).cast("array<string>").alias("also_types"),
        F.expr(e.label).cast("string").alias("label"),
        F.lit(m.source_system).alias("source_system"),
        F.lit(e.source.dataset).alias("source_dataset"),
        nk.alias("natural_key"),
        attrs.alias("attributes"),
        _date(e.valid_from).alias("valid_from"),
        _date(e.valid_to).alias("valid_to"),
        _null_key(list(e.key.values())).alias("_null_key"),
        F.lit(f"{m.source_system}.{e.id}").alias("_entity_id"),
    )


def product_entities(spark, tb: Dict[str, Any]):
    """Một node cho mỗi lớp sản phẩm của taxonomy: ofProduct trỏ vào đây, còn câu hỏi theo lớp
    cha (CQ2) join entity.class_iri với bảng subclass."""
    anc = M.ancestors(tb)
    rows = [(ids.product_uid(c["iri"]), c["iri"], c["label_vi"], f"iri={c['iri']}")
            for c in tb["classes"] if "fin:Product" in anc.get(c["iri"], set())]
    from pyspark.sql import functions as F

    return (spark.createDataFrame(rows, "uid string, class_iri string, label string, natural_key string")
            .select("uid", "class_iri", F.array().cast("array<string>").alias("also_types"), "label",
                    F.lit(ids.PRODUCT_SYSTEM).alias("source_system"), F.lit("ontology/fingraph.ttl").alias("source_dataset"),
                    "natural_key", F.create_map().cast("map<string,string>").alias("attributes"),
                    F.lit(None).cast("date").alias("valid_from"), F.lit(None).cast("date").alias("valid_to"),
                    F.lit(False).alias("_null_key"), F.lit("ontology.product").alias("_entity_id")))


def concept_entities(spark, tb: Dict[str, Any]):
    """Node cho khái niệm SKOS của TBox (8 kiểu hình rửa tiền): hasTypology trỏ vào đây."""
    from pyspark.sql import functions as F

    rows = [(ids.product_uid(c["iri"]), M.SCHEME_CLASS[c["scheme"]], c["pref_label"], f"iri={c['iri']}",
             c["notation"]) for c in tb["concepts"] if c["scheme"] in M.SCHEME_CLASS]
    return (spark.createDataFrame(rows, "uid string, class_iri string, label string, natural_key string, notation string")
            .select("uid", "class_iri", F.array().cast("array<string>").alias("also_types"), "label",
                    F.lit(ids.PRODUCT_SYSTEM).alias("source_system"), F.lit("ontology/fingraph.ttl").alias("source_dataset"),
                    "natural_key", F.create_map(F.lit("notation"), F.col("notation")).alias("attributes"),
                    F.lit(None).cast("date").alias("valid_from"), F.lit(None).cast("date").alias("valid_to"),
                    F.lit(False).alias("_null_key"), F.lit("ontology.concept").alias("_entity_id")))


def _triple(subject, predicate, obj_uid, literal, datatype, weight, vf, vt, dataset, skey, mapping_id):
    from pyspark.sql import functions as F

    return [subject.alias("subject_uid"), F.lit(predicate).alias("predicate_iri"),
            obj_uid.cast("string").alias("object_uid"), weight.cast("double").alias("weight"),
            vf.alias("valid_from"), vt.alias("valid_to"),
            literal.cast("string").alias("object_literal"), datatype.cast("string").alias("object_datatype"),
            F.lit(dataset).alias("source_dataset"), skey.cast("string").alias("source_key"),
            F.lit(1.0).alias("confidence"), F.lit(False).alias("is_inferred"), F.lit(mapping_id).alias("mapping_id")]


def edge_frame(spark, m: M.Mapping, e: M.Edge, concepts=None):
    from pyspark.sql import functions as F

    df = source_df(spark, e.source)
    subj = _ref_uid(m, e.subject)
    skey = F.expr(e.source_key) if e.source_key else _nkey(m.entities[e.subject.entity].key.keys(), e.subject.key)
    weight = F.expr(e.weight) if e.weight else F.lit(None)
    if e.obj is not None:
        cols = _triple(subj, e.predicate, _ref_uid(m, e.obj, concepts), F.lit(None), F.lit(None), weight,
                       _date(e.valid_from), _date(e.valid_to), e.source.dataset, skey, f"{m.source_system}.{e.id}")
    else:
        cols = _triple(subj, e.predicate, F.lit(None), F.expr(e.literal["expr"]), F.lit(e.literal["datatype"]),
                       weight, _date(e.valid_from), _date(e.valid_to), e.source.dataset, skey, f"{m.source_system}.{e.id}")
    out = df.select(*cols)
    return out.where(F.col("object_literal").isNotNull()) if e.obj is None else out


def holding_frames(spark, m: M.Mapping, h: M.Holding, notations: Dict[str, str], labels: Dict[str, str]):
    """Một dòng nguồn -> node fin:ProductHolding + hasHolding, ofProduct, [viaAccount],
    [holdingStatus], [amount]. Trả (entity_df, triple_df)."""
    from pyspark.sql import functions as F

    df = source_df(spark, h.source)
    if h.product is not None:
        df = df.withColumn("_product", F.lit(h.product))
    else:
        lookup = F.create_map(*[x for k, v in notations.items() if k.startswith(m.source_system + ":")
                                for x in (F.lit(k), F.lit(v))])
        df = df.withColumn("_product", lookup[F.concat(F.lit(m.source_system + ":"), F.expr(h.product_notation))])
    label_map = F.create_map(*[x for k, v in labels.items() for x in (F.lit(k), F.lit(v))])
    hk = F.concat(F.lit("product="), F.coalesce(F.col("_product"), F.lit("")), F.lit("|"),
                  _nkey(h.key.keys(), list(h.key.values())))
    df = (df.withColumn("_hk", hk)
            .withColumn("_huid", ids.uid_col("fin:ProductHolding", m.source_system, F.col("_hk")))
            .withColumn("_cuid", _ref_uid(m, h.customer))
            .withColumn("_vf", _date(h.valid_from)).withColumn("_vt", _date(h.valid_to)))
    if h.account is not None:
        df = df.withColumn("_auid", _ref_uid(m, h.account))
    attrs = dict(h.attributes)
    if h.status:
        attrs["status"] = h.status
    if h.amount:
        attrs["amount"] = h.amount
    df = df.withColumn("_skey", F.expr(h.source_key) if h.source_key else F.col("_hk"))
    ent = df.select(
        F.col("_huid").alias("uid"), F.lit("fin:ProductHolding").alias("class_iri"),
        F.array().cast("array<string>").alias("also_types"),
        F.coalesce(label_map[F.col("_product")], F.lit("?")).alias("label"),
        F.lit(m.source_system).alias("source_system"), F.lit(h.source.dataset).alias("source_dataset"),
        F.col("_hk").alias("natural_key"),
        (F.create_map(*[x for k, v in attrs.items() for x in (F.lit(k), F.expr(v).cast("string"))])
         if attrs else F.create_map().cast("map<string,string>")).alias("attributes"),
        F.col("_vf").alias("valid_from"), F.col("_vt").alias("valid_to"),
        (_null_key(list(h.key.values())) | F.col("_product").isNull()).alias("_null_key"),
        F.lit(f"{m.source_system}.{h.id}").alias("_entity_id"))
    ds, mid = h.source.dataset, f"{m.source_system}.{h.id}"
    vf, vt, sk = F.col("_vf"), F.col("_vt"), F.col("_skey")
    none = F.lit(None)
    parts = [
        df.select(*_triple(F.col("_cuid"), "fin:hasHolding", F.col("_huid"), none, none, none, vf, vt, ds, sk, mid)),
        df.select(*_triple(F.col("_huid"), "fin:ofProduct", _product_uid_col(F.col("_product")),
                           none, none, none, vf, vt, ds, sk, mid)),
    ]
    if h.account is not None:
        parts.append(df.select(*_triple(F.col("_huid"), "fin:viaAccount", F.col("_auid"), none, none, none,
                                        vf, vt, ds, sk, mid)))
    if h.status:
        parts.append(df.where(F.expr(h.status).isNotNull()).select(*_triple(
            F.col("_huid"), "fin:holdingStatus", none, F.expr(h.status), F.lit("xsd:string"), none, vf, vt, ds, sk, mid)))
    if h.amount:
        parts.append(df.where(F.expr(h.amount).isNotNull()).select(*_triple(
            F.col("_huid"), "fin:amount", none, F.expr(h.amount), F.lit("xsd:decimal"), none, vf, vt, ds, sk, mid)))
    tri = parts[0]
    for p in parts[1:]:
        tri = tri.unionByName(p)
    return ent, tri


def _product_uid_col(product_col):
    """uid của node sản phẩm, tính trong Spark — khớp ids.product_uid."""
    from pyspark.sql import functions as F

    return F.substring(F.sha2(F.concat(product_col, F.lit(f"|{ids.PRODUCT_SYSTEM}|iri="), product_col), 256),
                       1, ids.UID_LEN)


# ---------------------------------------------------------------- bất biến

def check_invariants(spark, entity, triple, expectations: List[Dict[str, Any]], tb: Dict[str, Any]) -> Dict[str, Any]:
    """Trả số liệu đối soát; vi phạm -> InvariantError (không ghi gì)."""
    from pyspark.sql import functions as F

    props = {p["iri"] for p in tb["properties"]}
    problems: List[str] = []

    n_null_key = entity.where("_null_key").count()
    if n_null_key:
        problems.append(f"{n_null_key} node co cot khoa NULL hoac ma san pham khong tra duoc skos:notation")
    dup_uid = entity.groupBy("uid").count().where("count > 1").count()
    if dup_uid:
        problems.append(f"{dup_uid} uid trung trong entity")
    dup_tid = triple.groupBy("triple_id").count().where("count > 1").count()
    if dup_tid:
        ex = triple.groupBy("mapping_id", "triple_id").count().where("count > 1").limit(3).collect()
        problems.append(f"{dup_tid} triple_id trung (vd {[(r[0], r[1]) for r in ex]})")
    uids = entity.select("uid")
    orphan_s = triple.join(uids, triple.subject_uid == uids.uid, "left_anti").count()
    orphan_o = (triple.where(F.col("object_uid").isNotNull())
                .join(uids, triple.object_uid == uids.uid, "left_anti").count())
    if orphan_s or orphan_o:
        ex = (triple.join(uids, triple.subject_uid == uids.uid, "left_anti")
              .unionByName(triple.where(F.col("object_uid").isNotNull())
                           .join(uids, triple.object_uid == uids.uid, "left_anti"))
              .groupBy("mapping_id").count().collect())
        sample = (triple.join(uids, triple.object_uid == uids.uid, "left_anti")
                  .where(F.col("object_uid").isNotNull()).select("mapping_id", "source_key", "object_uid").limit(3).collect())
        nodes = sorted((r[0], r[1]) for r in entity.groupBy("_entity_id").count().collect())
        problems.append(f"canh mo coi: {orphan_s} subject, {orphan_o} object khong co trong entity "
                        f"(theo mapping: {[(r[0], r[1]) for r in ex]}; vd {[tuple(r) for r in sample]}; "
                        f"so node theo muc: {nodes})")
    bad_pred = [r[0] for r in triple.select("predicate_iri").distinct().collect() if r[0] not in props]
    if bad_pred:
        problems.append(f"predicate khong co trong TBox: {bad_pred}")
    bad_time = triple.where("valid_to < valid_from").count() + entity.where("valid_to < valid_from").count()
    if bad_time:
        problems.append(f"{bad_time} dong co valid_to < valid_from")

    per_mapping = {r[0]: r[1] for r in triple.groupBy("mapping_id").agg(
        F.countDistinct("source_key").alias("n")).collect()}
    recon = []
    for x in expectations:
        expected = source_df(spark, M.Source(sql=x["sql"], tables=[])).first()[0]
        got = per_mapping.get(x["id"], 0)
        recon.append({"mapping_id": x["id"], "expected": expected, "got": got})
        if expected != got:
            problems.append(f"doi soat {x['id']}: nguon {expected} dong, do thi {got}")
    if problems:
        raise InvariantError("Do thi vi pham bat bien, KHONG ghi:\n  - " + "\n  - ".join(problems))
    return {"reconciliation": recon}


# ---------------------------------------------------------------- chạy

def build(spark, mappings: List[M.Mapping], tb: Dict[str, Any]):
    """Trả (entity_df, triple_df, expectations) — chưa kiểm, chưa ghi (test gọi thẳng)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    M.validate(mappings, tb)
    register_sources(spark, set().union(*[m.tables() for m in mappings]) if mappings else set())
    notations = M.resolve_notations(tb)
    labels = {c["iri"]: c["label_vi"] for c in tb["classes"]}

    ents = [product_entities(spark, tb), concept_entities(spark, tb)]
    concepts = {c["notation"].upper(): c["iri"] for c in tb["concepts"] if c["scheme"] in M.SCHEME_CLASS}
    tris = []
    expectations = []
    for m in mappings:
        for e in m.entities.values():
            ents.append(entity_frame(spark, m, e))
        for e in m.edges:
            tris.append(edge_frame(spark, m, e, concepts))
            if e.expect_rows:
                expectations.append({"id": f"{m.source_system}.{e.id}", "sql": e.expect_rows})
        for h in m.holdings:
            ent, tri = holding_frames(spark, m, h, notations, labels)
            ents.append(ent)
            tris.append(tri)
            if h.expect_rows:
                expectations.append({"id": f"{m.source_system}.{h.id}", "sql": h.expect_rows})

    entity = ents[0]
    for x in ents[1:]:
        entity = entity.unionByName(x)
    # Cùng node xuất hiện nhiều lần (vd tài khoản đối tác lặp trên nhiều giao dịch): giữ MỘT
    # dòng, valid_from sớm nhất. Thứ tự cố định để chạy lại ra cùng nhãn/thuộc tính.
    w = Window.partitionBy("uid").orderBy(F.col("valid_from").asc_nulls_last(), F.col("label"))
    entity = (entity.withColumn("_vf_min", F.min("valid_from").over(Window.partitionBy("uid")))
              .withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
              .withColumn("valid_from", F.col("_vf_min")).drop("_rn", "_vf_min"))

    triple = tris[0] if tris else None
    for x in tris[1:]:
        triple = triple.unionByName(x)
    # O8: cạnh fin:sameAs — chỉ cặp confidence >= AUTO_THRESHOLD (khoá chung, khớp mạnh); cặp yếu hơn
    # nằm ở hàng đợi duyệt, KHÔNG thành cạnh. Đi qua cùng bất biến O7.
    auto, _ = R.all_links(spark, entity)
    if auto is not None and triple is not None:
        triple = triple.unionByName(R.same_as_triples(auto))
    triple = triple.withColumn("triple_id", ids.triple_id_col(
        F.col("subject_uid"), F.col("predicate_iri"), F.coalesce(F.col("object_uid"), F.col("object_literal")),
        F.col("valid_from"), F.col("source_dataset")))
    # Chốt kết quả MỘT lần (cắt lineage): đồ thị là hợp của hàng chục nhánh SQL/join; không chốt thì
    # mỗi phép kiểm bất biến tính lại cả cây và kế hoạch truy vấn phình tới mức driver hết heap khi
    # in plan (gặp khi dựng berka + corebank cùng lúc). localCheckpoint giữ trên executor, đủ cho
    # một job chạy lại được từ đầu.
    return entity.localCheckpoint(eager=True), triple.localCheckpoint(eager=True), expectations


def run(spark, args: Dict[str, str]) -> Dict[str, Any]:
    from pyspark.sql import functions as F

    tb = T.load()
    db = args.get("ONTO_DB", DB)
    names = [x.strip() for x in args["MAPPINGS"].split(",")] if args.get("MAPPINGS") else None
    mappings = M.load_all(names)
    run_id = args.get("JOB_RUN_ID") or f"local-{uuid.uuid4().hex[:12]}"
    print(f"  mapping: {[m.name for m in mappings]}, TBox {tb['version']}, run_id {run_id}")

    entity, triple, expectations = build(spark, mappings, tb)
    report = check_invariants(spark, entity, triple, expectations, tb)

    stamp = [F.lit(tb["version"]).alias("ontology_version"), F.current_timestamp().alias("recorded_at"),
             F.lit(run_id).alias("run_id")]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{db}")
    n_ent = write_table(entity.select(*ENTITY_COLS, *stamp), db, "entity", partition_by=["class_iri"],
                        comment="ABox FinGraph: node (uid on dinh, lop, nhan khong PII, nguon)")
    n_tri = write_table(triple.select(*TRIPLE_COLS, *stamp), db, "triple",
                        partition_by=["predicate_iri", F.bucket(16, F.col("subject_uid"))],
                        comment="ABox FinGraph: canh (triple_id de trich dan, thoi gian nghiep vu, nguon)")
    auto, review = R.all_links(spark, entity)
    n_same = n_review = 0
    if auto is not None:
        n_same = write_table(auto.select(*R.SAME_AS_COLS, *stamp), db, "same_as",
                             comment="O8: cap thuc the trung (confidence >= 0.9), da thanh canh fin:sameAs")
        n_review = write_table(review.select(*R.SAME_AS_COLS, *stamp), db, "same_as_review",
                               comment="O8: ung vien [0.6, 0.9) cho nguoi duyet, KHONG thanh canh (evidence khong chua ten)")
        for r in review.groupBy("rule_id").count().collect():
            print(f"    cho duyet {r[0]}: {r[1]}")
    by_class = {r[0]: r[1] for r in entity.groupBy("class_iri").count().collect()}
    by_pred = {r[0]: r[1] for r in triple.groupBy("predicate_iri").count().collect()}
    for k in sorted(by_class):
        print(f"    node {k}: {by_class[k]}")
    for k in sorted(by_pred):
        print(f"    canh {k}: {by_pred[k]}")
    for r in report["reconciliation"]:
        print(f"    doi soat {r['mapping_id']}: nguon {r['expected']} = do thi {r['got']}")
    return {"ontology_version": tb["version"], "run_id": run_id, "entity": n_ent, "triple": n_tri, "same_as": n_same, "same_as_review": n_review,
            "by_class": by_class, "by_predicate": by_pred, **report}
