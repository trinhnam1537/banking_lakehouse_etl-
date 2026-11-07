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
