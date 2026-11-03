"""onto.load_schema trên Spark + Iceberg local: TBox thành bảng track_a_onto, và bảng bao đóng
trả lời được câu hỏi theo lớp cha mà không cần đệ quy (CQ2, CQ15)."""

from __future__ import annotations

import pytest

from transforms.onto import load_schema
from transforms.onto import tbox as T

from .conftest import requires_spark

pytestmark = requires_spark


@pytest.fixture(autouse=True)
def _ctas_writer(monkeypatch):
    """Thay the bang nguyen khoi (writeTo().createOrReplace() / CTAS) goi Hadoop NativeIO ma
    winutils tren may Windows nay khong ho tro; Glue (Linux) khong gap. Test dung CREATE TABLE +
    INSERT OVERWRITE (cung bang Iceberg, cung ghi de toan bo du lieu); code production giu nguyen."""
    def ctas(df, db, name, partition_by=None, comment=""):
        view = f"_v_{name}"
        df.createOrReplaceTempView(view)
        full = f"glue_catalog.{db}.{name}"
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
        spark = df.sparkSession
        spark.sql(f"CREATE TABLE IF NOT EXISTS {full} ({cols}) USING iceberg")
        spark.sql(f"INSERT OVERWRITE {full} SELECT * FROM {view}")
        return spark.table(full).count()

    monkeypatch.setattr(load_schema, "write_table", ctas)


def test_nap_tbox_thanh_bang(spark):
    out = load_schema.run(spark, {})
    tb = T.load()
    assert out["class"] == len(tb["classes"]) and out["property"] == len(tb["properties"])
    assert out["concept"] == 8 and out["ontology_version"] == tb["version"]

    # Moi lop la to tien cua chinh no (do sau 0) -> JOIN theo lop cha gom ca chinh lop do
    self_rows = spark.sql("SELECT count(*) FROM glue_catalog.track_a_onto.subclass WHERE depth = 0").first()[0]
    assert self_rows == out["class"]

    # CQ2: "san pham tin dung" = moi lop co to tien fin:CreditProduct
    credit = {r[0] for r in spark.sql(
        "SELECT class_iri FROM glue_catalog.track_a_onto.subclass WHERE ancestor_iri = 'fin:CreditProduct'"
    ).collect()}
    assert {"fin:Loan", "fin:Mortgage", "fin:CreditCard", "fin:GoldCard", "fin:Guarantee"} <= credit
    assert "fin:TermDeposit" not in credit

    # CQ16: tu ma nguon Santander ra lop chung bang mot phep tra
    hit = spark.sql("SELECT iri, label_vi FROM glue_catalog.track_a_onto.class "
                    "WHERE array_contains(notations, 'santander:ind_tjcr_fin_ult1')").collect()
    assert [(r[0], r[1]) for r in hit] == [("fin:CreditCard", "Thẻ tín dụng")]

    # Quan he bac cau duoc danh dau (closure vi tri tinh o O9)
    trans = spark.sql("SELECT iri FROM glue_catalog.track_a_onto.property WHERE transitive").collect()
    assert [r[0] for r in trans] == ["fin:partOf"]


def test_chay_lai_cho_cung_ket_qua(spark):
    a = load_schema.run(spark, {})
    b = load_schema.run(spark, {})
    assert a == b
