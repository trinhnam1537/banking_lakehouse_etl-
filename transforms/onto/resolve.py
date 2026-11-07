"""Hợp nhất thực thể (bước O8, docs/ONTOLOGY-BUILD.md; thiết kế ONTOLOGY.md mục 4.4).

KHÔNG gộp cứng hai node: gộp sai hai người khác nhau thì không gỡ được và sinh kết luận AML sai.
Mỗi cặp trùng = một cạnh fin:sameAs (đối xứng trong TBox, lưu MỘT chiều uid_a < uid_b) kèm
`confidence` và luật sinh ra nó; cùng dữ liệu ghi bảng track_a_onto.same_as để truy vấn / duyệt.

Luật (RULES) khai báo ở đây, mỗi luật PHẢI có lý do. Không có luật nào giữa hai "vũ trụ" không có
khoá chung (IBM AML, PaySim, Bank Marketing, MBD với corebank): ONTOLOGY-BUILD.md O0, O8.

| Luật                   | Cặp                          | Khớp                  | confidence |
|------------------------|------------------------------|-----------------------|------------|
| corebank_berka_client  | khách corebank ↔ khách Berka | cùng natural_key      | 1.0        |
| (lượt sau) sanctions   | khách ↔ OpenSanctions        | tên + ngày sinh + nước| 0.6–0.95, người duyệt khi < 0.9 |
"""

from __future__ import annotations

from typing import Any, Dict, List

RULES: List[Dict[str, Any]] = [
    {
        "id": "corebank_berka_client",
        "a": {"source_system": "corebank", "class_iri": "fin:Customer"},
        "b": {"source_system": "berka", "class_iri": "fin:Customer"},
        "on": "natural_key",          # 'client_id=N' ở cả hai nguồn
        "confidence": 1.0,
        "why": "core banking được nạp từ Berka (simulators/corebank/seed.py): cùng client_id gốc là cùng người",
    },
]

SAME_AS_COLS = ["uid_a", "uid_b", "confidence", "rule_id", "evidence"]


def links(entity, rules: List[Dict[str, Any]] = RULES):
    """DataFrame (uid_a, uid_b, confidence, rule_id, evidence), uid_a < uid_b, không trùng cặp."""
    from pyspark.sql import functions as F

    out = None
    for r in rules:
        sel = lambda side: entity.where(  # noqa: E731
            (F.col("source_system") == r[side]["source_system"]) & (F.col("class_iri") == r[side]["class_iri"]))
        a = sel("a").select(F.col("uid").alias("ua"), F.col(r["on"]).alias("k"))
        b = sel("b").select(F.col("uid").alias("ub"), F.col(r["on"]).alias("k"))
        df = (a.join(b, "k")
              .select(F.least("ua", "ub").alias("uid_a"), F.greatest("ua", "ub").alias("uid_b"),
                      F.lit(float(r["confidence"])).alias("confidence"), F.lit(r["id"]).alias("rule_id"),
                      F.concat(F.lit(f"{r['on']}="), F.col("k")).alias("evidence")))
        out = df if out is None else out.unionByName(df)
    if out is None:
        return None
    return out.dropDuplicates(["uid_a", "uid_b"])


def same_as_triples(same_as):
    """Cạnh fin:sameAs cho bảng triple: is_inferred = true (hệ thống suy ra, không lấy thẳng từ nguồn),
    confidence theo luật. Agent bắt buộc nói "khả năng" khi confidence < 1 (ONTOLOGY.md 4.4)."""
    from pyspark.sql import functions as F

    return same_as.select(
        F.col("uid_a").alias("subject_uid"), F.lit("fin:sameAs").alias("predicate_iri"),
        F.col("uid_b").alias("object_uid"), F.lit(None).cast("double").alias("weight"),
        F.lit(None).cast("date").alias("valid_from"), F.lit(None).cast("date").alias("valid_to"),
        F.lit(None).cast("string").alias("object_literal"), F.lit(None).cast("string").alias("object_datatype"),
        F.concat(F.lit("rule:"), F.col("rule_id")).alias("source_dataset"),
        F.col("evidence").alias("source_key"), F.col("confidence"), F.lit(True).alias("is_inferred"),
        F.concat(F.lit("same_as."), F.col("rule_id")).alias("mapping_id"))


# ---------------------------------------------------------------- khớp mờ khách <-> cấm vận (CQ8)

AUTO_THRESHOLD = 0.9      # >= : cạnh fin:sameAs; truy vấn mặc định đi qua
REVIEW_THRESHOLD = 0.6    # [0.6, 0.9): hàng đợi người duyệt (track_a_onto.same_as_review), KHÔNG thành cạnh
FUZZY_MIN_RATIO = 0.85


def _norm_udf():
    import re
    import unicodedata

    from pyspark.sql import functions as F

    def norm(s):
        if not s:
            return None
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
        toks = sorted(t for t in re.split(r"[^\w]+", s) if t)
        return " ".join(toks) or None

    return F.udf(norm, "string")


def sanctions_links(spark, entity, catalog: str = "glue_catalog"):
    """Ứng viên (khách corebank, người bị cấm vận) theo tên + năm sinh + nước.

    Đọc full_name (PII) của silver_corebank.client CHỈ trong job; đầu ra không chứa tên —
    evidence là cờ (name=exact|fuzzy:0.91; birth=full|year|no; country=yes|no).
    Trả None khi thiếu một trong hai nguồn trên đồ thị."""
    from pyspark.sql import functions as F

    from . import ids
    from .build_graph import current_view

    have = {r[0] for r in entity.select("source_system").distinct().collect()}
    if not {"corebank", "opensanctions"} <= have:
        return None
    try:
        cl = current_view(spark.table(f"{catalog}.silver_corebank.client"))
        tg = current_view(spark.table(f"{catalog}.silver_opensanctions.target"))
    except Exception as e:  # noqa: BLE001 — thiếu bảng: bỏ luật, không làm hỏng cả đồ thị
        print(f"  bo qua khop cam van: {e}")
        return None
    if "full_name" not in cl.columns:
        return None
    norm = _norm_udf()
    c = cl.select("client_id", F.col("birth_date").cast("string").alias("c_birth"),
                  norm("full_name").alias("n")).where("n IS NOT NULL")
    names = F.array_union(F.array("name"), F.coalesce("aliases", F.array().cast("array<string>")))
    t = (tg.where("entity_schema = 'Person'")
         .select("entity_id", F.col("birth_date").alias("t_birth"), "countries", F.explode(names).alias("nm"))
         .select("entity_id", "t_birth", "countries", norm("nm").alias("n")).where("n IS NOT NULL").distinct())
    # Chặn (blocking) theo MỌI từ >= 4 ký tự: hai tên được so khi có chung ít nhất một từ. Chặn theo một
    # từ duy nhất (vd từ dài nhất) bỏ sót đúng ca sai chính tả ở từ đó ('aleksandrovic' / 'aleksandrovich').
    toks = F.explode(F.array_distinct(F.expr("filter(split(n, ' '), x -> length(x) >= 4)")))
    c = c.withColumn("blk", toks).localCheckpoint()
    t = t.withColumn("blk", toks).localCheckpoint()
    pairs = (c.alias("c").join(t.alias("t"), "blk")
             .select("c.client_id", "c.c_birth", "t.entity_id", "t.t_birth", "t.countries",
                     (1 - F.levenshtein("c.n", "t.n") / F.greatest(F.length("c.n"), F.length("t.n"))).alias("ratio"))
             .where(F.col("ratio") >= FUZZY_MIN_RATIO)
             .groupBy("client_id", "c_birth", "entity_id", "t_birth", "countries").agg(F.max("ratio").alias("ratio")))
    exact = F.col("ratio") >= 0.9999
    name_score = F.when(exact, F.lit(0.80)).otherwise(0.60 + (F.col("ratio") - FUZZY_MIN_RATIO) / (1 - FUZZY_MIN_RATIO) * 0.15)
    births = F.split(F.coalesce("t_birth", F.lit("")), ";")
    full = F.array_contains(births, F.col("c_birth"))
    year = F.exists(births, lambda b: F.substring(b, 1, 4) == F.substring(F.col("c_birth"), 1, 4))
    birth_score = F.when(full, 0.15).when(year, 0.10).otherwise(0.0)
    cz = F.array_contains(F.coalesce("countries", F.array().cast("array<string>")), "cz")
    conf = F.least(F.lit(0.95), name_score + birth_score + F.when(cz, 0.05).otherwise(0.0))
    cust = ids.uid_col("fin:Customer", "corebank", F.concat(F.lit("client_id="), F.col("client_id").cast("string")))
    party = ids.uid_col("fin:Party", "opensanctions", F.concat(F.lit("entity_id="), F.col("entity_id")))
    ev = F.concat(F.lit("name="), F.when(exact, F.lit("exact")).otherwise(F.concat(F.lit("fuzzy:"),
                  F.format_number("ratio", 2))),
                  F.lit(";birth="), F.when(full, "full").when(year, "year").otherwise("no"),
                  F.lit(";country="), F.when(cz, "yes").otherwise("no"))
    out = (pairs.select(cust.alias("ua"), party.alias("ub"), conf.alias("confidence"), ev.alias("evidence"))
           .where(F.col("confidence") >= REVIEW_THRESHOLD))
    uids = entity.select("uid")
    out = (out.join(uids.withColumnRenamed("uid", "ua"), "ua", "left_semi")
              .join(uids.withColumnRenamed("uid", "ub"), "ub", "left_semi"))
    return out.select(F.least("ua", "ub").alias("uid_a"), F.greatest("ua", "ub").alias("uid_b"), "confidence",
                      F.lit("sanctions_name").alias("rule_id"), "evidence").dropDuplicates(["uid_a", "uid_b"])


def all_links(spark, entity):
    """(auto, review): auto >= AUTO_THRESHOLD thành cạnh; review vào hàng đợi người duyệt."""
    from pyspark.sql import functions as F

    parts = [x for x in (links(entity), sanctions_links(spark, entity)) if x is not None]
    if not parts:
        return None, None
    df = parts[0]
    for x in parts[1:]:
        df = df.unionByName(x)
    df = df.localCheckpoint()
    return df.where(F.col("confidence") >= AUTO_THRESHOLD), df.where(F.col("confidence") < AUTO_THRESHOLD)
