"""onto.load_schema — TBox (tbox.json) -> bảng track_a_onto (bước O3/O7, docs/ONTOLOGY-BUILD.md).

    aws glue start-job-run --job-name fingraph-dev-onto-spark --arguments '{"--TRANSFORM":"onto.load_schema"}'

Ba bảng để SQL (Athena), pre-train và tool agent join được với đồ thị:

    class     một dòng một lớp: nhãn vi/en, định nghĩa, cha trực tiếp, mã nguồn (skos:notation)
    property  một dòng một quan hệ: loại, domain, range, bắc cầu / đối xứng
    subclass  BAO ĐÓNG subClassOf* (lớp, tổ tiên, độ sâu) — tính sẵn vì Athena không đệ
              quy tin cậy: "khách giữ sản phẩm tín dụng" = JOIN subclass ON ancestor = fin:CreditProduct
    concept   khái niệm SKOS (8 kiểu hình rửa tiền)

Ghi đè toàn bộ (createOrReplace) như gold: idempotent, snapshot Iceberg cũ còn để so phiên bản TBox.
"""

from __future__ import annotations

from typing import Any, Dict

from ..common import write_table
from . import tbox as T

DB = "track_a_onto"


def frames(spark, tb: Dict[str, Any]):
    """DataFrame của 4 bảng — tách riêng để test không cần ghi."""
    v = tb["version"]
    cls = spark.createDataFrame(
        [(c["iri"], c["label_vi"], c["label_en"], c["definition"], c["parents"], c["notations"],
          c["source_datasets"], c["materialized_in"], c["competency_questions"], c["close_match"], v)
         for c in tb["classes"]],
        "iri string, label_vi string, label_en string, definition string, parents array<string>, "
        "notations array<string>, source_datasets array<string>, materialized_in array<string>, "
        "competency_questions array<string>, close_match array<string>, ontology_version string")
    prop = spark.createDataFrame(
        [(p["iri"], p["kind"], p["label_vi"], p["label_en"], p["definition"], p["domain"] or None,
          p["range"] or None, p["transitive"], p["symmetric"], p["competency_questions"], v)
         for p in tb["properties"]],
        "iri string, kind string, label_vi string, label_en string, definition string, domain string, "
        "range string, transitive boolean, symmetric boolean, competency_questions array<string>, "
        "ontology_version string")
    sub = spark.createDataFrame(
        [(r["class_iri"], r["ancestor_iri"], r["depth"], v) for r in T.subclass_closure(tb)],
        "class_iri string, ancestor_iri string, depth int, ontology_version string")
    concept = spark.createDataFrame(
        [(c["iri"], c["scheme"], c["notation"], c["pref_label"], v) for c in tb["concepts"]],
        "iri string, scheme string, notation string, pref_label string, ontology_version string")
    return {"class": cls, "property": prop, "subclass": sub, "concept": concept}


def run(spark, args: Dict[str, str]) -> Dict[str, Any]:
    tb = T.load()
    db = args.get("ONTO_DB", DB)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{db}")
    comments = {
        "class": "TBox FinGraph: lop (nhan vi/en, dinh nghia, ma nguon skos:notation)",
        "property": "TBox FinGraph: quan he (domain, range, bac cau, doi xung)",
        "subclass": "Bao dong subClassOf* tinh san (lop, to tien, do sau)",
        "concept": "Khai niem SKOS (kieu hinh rua tien)",
    }
    out = {name: write_table(df, db, name, comment=comments[name]) for name, df in frames(spark, tb).items()}
    return {"ontology_version": tb["version"], **out}
