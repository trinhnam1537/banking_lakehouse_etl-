"""onto.build_graph (O7) trên Spark + Iceberg local: silver mẫu nhỏ -> entity, triple.

Dữ liệu mẫu Berka đủ cho mọi loại mục: node, cạnh, cạnh gộp (transfersTo theo tháng), holding
n-ary có lớp cố định (Loan) và tra theo skos:notation (thẻ, k_symbol). Đáp án tính tay ở comment.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from transforms.onto import build_graph as B
from transforms.onto import ids
from transforms.onto import mapping as M
from transforms.onto import tbox as T

from .conftest import requires_spark

pytestmark = requires_spark
D = dt.date


def _table(spark, full, rows, schema):
    """CREATE + INSERT (không CTAS: Hadoop NativeIO trên Windows, xem test_load_schema)."""
    df = spark.createDataFrame(rows, schema)
    view = "_seed_" + full.replace(".", "_")
    df.createOrReplaceTempView(view)
    cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS glue_catalog.{full} ({cols}) USING iceberg")
    spark.sql(f"INSERT OVERWRITE glue_catalog.{full} SELECT * FROM {view}")


@pytest.fixture(scope="module")
def silver(spark):
    for db in ("silver_berka", "silver_mbd", "silver_bank_marketing"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue_catalog.{db}")
    _table(spark, "silver_berka.district", [
        (1, "Hl.m. Praha", "Prague", 1204953, 12541, Decimal("100.0"), Decimal("0.43")),
        (2, "Brno - mesto", "south Moravia", 387570, 9897, Decimal("100.0"), Decimal("1.96")),
    ], "district_id int, district_name string, region string, inhabitants int, avg_salary int, "
       "urban_ratio decimal(5,1), unemployment_96 decimal(5,2)")
    _table(spark, "silver_berka.client", [
        (1, "F", D(1970, 5, 1), 1), (2, "M", D(1965, 1, 1), 2), (3, "F", D(1990, 2, 2), 1),
    ], "client_id int, gender string, birth_date date, district_id int")
    _table(spark, "silver_berka.account", [
        (10, 1, D(1995, 1, 10), "MONTHLY"), (20, 2, D(1996, 3, 1), "WEEKLY"),
    ], "account_id int, district_id int, opened_date date, statement_frequency string")
    _table(spark, "silver_berka.disp", [
        (100, 1, 10, "OWNER", True), (101, 3, 10, "DISPONENT", False), (200, 2, 20, "OWNER", True),
    ], "disp_id int, client_id int, account_id int, disp_type string, is_owner boolean")
    _table(spark, "silver_berka.card", [
        (500, 100, "classic", D(1996, 5, 1)), (501, 101, "junior ", D(1997, 1, 1)),
    ], "card_id int, disp_id int, card_type string, issued_date date")
    _table(spark, "silver_berka.loan", [
        (900, 20, D(1996, 6, 1), Decimal("1000.00"), "RUNNING_OK", D(1997, 6, 1)),
    ], "loan_id int, account_id int, granted_date date, amount decimal(18,2), loan_status string, "
       "maturity_date date")
    _table(spark, "silver_berka.trans", [
        (1, 10, D(1995, 2, 3), "DEBIT", Decimal("100"), "POJISTNE", "AB", 555),
        (2, 10, D(1995, 2, 20), "DEBIT", Decimal("50"), "SIPO", "AB", 555),
        (3, 10, D(1995, 3, 1), "DEBIT", Decimal("100"), "POJISTNE", "AB", 555),
        (4, 20, D(1996, 7, 1), "CREDIT", Decimal("300"), None, "CD", 777),
        (5, 20, D(1996, 7, 15), "DEBIT", Decimal("20"), None, None, None),
    ], "trans_id bigint, account_id int, trans_date date, direction string, amount decimal(18,2), "
       "k_symbol string, counterparty_bank string, counterparty_account bigint")
    # MBD: khach a mua SP1 o thang 1 va 2 (mot holding, purchase_months=2), SP3 o thang 2; khach b khong mua
    _table(spark, "silver_mbd.target_month", [
        ("a" * 64, D(2022, 1, 31), True, False, False, False, 0),
        ("a" * 64, D(2022, 2, 28), True, False, True, False, 0),
        ("b" * 64, D(2022, 1, 31), False, False, False, False, 1),
    ], "client_id string, report_month date, target_1 boolean, target_2 boolean, target_3 boolean, "
       "target_4 boolean, fold int")
    bm = "source_file string, source_row bigint, age int, job string, marital string, education string, " \
         "credit_default string, subscribed boolean"
    _table(spark, "silver_bank_marketing.contact_full", [   # chi bank-full co balance_eur
        ("bank-full.csv", 1, 58, "management", "married", "tertiary", "no", True, Decimal("2143.00")),
        ("bank-full.csv", 2, 44, "unknown", "single", "secondary", "no", False, Decimal("29.00")),
    ], bm + ", balance_eur decimal(18,2)")
    _table(spark, "silver_bank_marketing.contact_additional", [
        ("bank-additional-full.csv", 1, 30, "technician", "single", "university.degree", "no", True),
    ], bm)
    return spark


@pytest.fixture(scope="module")
def graph(silver):
    tb = T.load()
    ms = M.load_all(["berka", "mbd", "bank_marketing"])
    entity, triple, exp = B.build(silver, ms, tb)
    report = B.check_invariants(silver, entity, triple, exp, tb)
    entity.createOrReplaceTempView("entity")
    triple.createOrReplaceTempView("triple")
    silver.createDataFrame(T.subclass_closure(tb)).createOrReplaceTempView("subclass")
    return silver, entity, triple, report


def _count(spark, sql):
    return spark.sql(sql).first()[0]


def test_berka_va_corebank_cung_luc_doi_soat_rieng_tung_nguon(silver):
    """Hai nguồn cùng id mục (holds_account...) nhưng số dòng khác nhau: corebank có thêm chủ tài khoản
    do simulator mở. Lỗi gặp trên dev (04-11): đối soát gộp theo id mục -> lệch 4.500 / 4.534."""
    spark = silver
    spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.silver_corebank")
    for t in ("district", "account", "disp", "card", "loan", "trans"):
        src = spark.table(f"glue_catalog.silver_berka.{t}")
        cols = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in src.schema.fields)
        spark.sql(f"CREATE TABLE IF NOT EXISTS glue_catalog.silver_corebank.{t} ({cols}) USING iceberg")
        spark.sql(f"INSERT OVERWRITE glue_catalog.silver_corebank.{t} SELECT * FROM glue_catalog.silver_berka.{t}")
    _table(spark, "silver_corebank.client", [
        (1, "F", D(1970, 5, 1), 1, "ACTIVE"), (2, "M", D(1965, 1, 1), 2, "ACTIVE"),
        (3, "F", D(1990, 2, 2), 1, "CLOSED"), (4, "M", D(1980, 3, 3), 2, "ACTIVE"),
    ], "client_id int, gender string, birth_date date, district_id int, status string")
    spark.sql("INSERT INTO glue_catalog.silver_corebank.account VALUES (30, 2, DATE'2023-01-05', 'MONTHLY')")
    spark.sql("INSERT INTO glue_catalog.silver_corebank.disp VALUES (300, 4, 30, 'OWNER', true)")
    tb = T.load()
    entity, triple, exp = B.build(spark, M.load_all(["berka", "corebank"]), tb)
    report = B.check_invariants(spark, entity, triple, exp, tb)
    recon = {r["mapping_id"]: (r["expected"], r["got"]) for r in report["reconciliation"]}
    assert recon["berka.holds_account"] == (2, 2) and recon["corebank.holds_account"] == (3, 3)
    # cung khach 1 o hai nguon = hai uid (same_as o buoc O8), khong gop
    assert entity.where("natural_key = 'client_id=1' AND class_iri = 'fin:Customer'").count() == 2


def test_so_node_va_canh_dung_dap_an(graph):
    spark, entity, triple, report = graph
    by_class = {r[0]: r[1] for r in spark.sql(
        "SELECT class_iri, count(*) FROM entity WHERE source_system <> 'ontology' GROUP BY 1").collect()}
    # Berka: 1 nuoc, 2 vung, 2 quan, 2 NH doi tac; KH 3 + MBD 2 + BM 3; TK 2 + 2 ngoai
    # holding: berka TK 2 + vay 1 + the 2 + POJISTNE/SIPO 2; MBD 2; BM 2
    assert by_class == {"fin:Country": 1, "fin:Region": 2, "fin:District": 2, "fin:Bank": 2,
                        "fin:Customer": 8, "fin:Account": 4, "fin:ProductHolding": 11}
    by_pred = {r[0]: r[1] for r in spark.sql("SELECT predicate_iri, count(*) FROM triple GROUP BY 1").collect()}
    assert by_pred["fin:holdsAccount"] == 2 and by_pred["fin:isAuthorizedOn"] == 1
    assert by_pred["fin:transfersTo"] == 3          # acc10->AB-555 thang 2 va 3, CD-777->acc20 thang 7
    assert by_pred["fin:hasHolding"] == by_pred["fin:ofProduct"] == 11
    assert by_pred["fin:viaAccount"] == 7           # chi holding Berka co tai khoan
    assert by_pred["fin:occupation"] == 2           # 'unknown' -> NULL -> khong co canh
    assert by_pred["fin:partOf"] == 4               # 2 quan -> vung, 2 vung -> nuoc
    assert all(r["expected"] == r["got"] for r in report["reconciliation"])
    assert {r["mapping_id"] for r in report["reconciliation"]} >= {"berka.holds_account", "berka.loan", "berka.card",
                                                                      "bank_marketing.term_deposit"}


def test_canh_gop_giu_tong_tien_va_khoang_thang(graph):
    spark = graph[0]
    rows = spark.sql("""SELECT t.valid_from, t.valid_to, t.weight FROM triple t
                        WHERE predicate_iri = 'fin:transfersTo' AND mapping_id = 'berka.transfers_out'
                        ORDER BY valid_from""").collect()
    assert [(r[0], r[1], r[2]) for r in rows] == [(D(1995, 2, 1), D(1995, 2, 28), 150.0),
                                                   (D(1995, 3, 1), D(1995, 3, 31), 100.0)]


def test_cq1_khach_giu_san_pham_tai_ngay(graph):
    """CQ1 chỉ bằng SQL trên entity + triple: khách 1 giữ gì tại một ngày?"""
    spark = graph[0]
    cust = ids.make_uid("fin:Customer", "berka", "client_id=1")
    q = f"""
      SELECT p.class_iri FROM triple h
      JOIN triple o ON o.subject_uid = h.object_uid AND o.predicate_iri = 'fin:ofProduct'
      JOIN entity p ON p.uid = o.object_uid
      WHERE h.subject_uid = '{cust}' AND h.predicate_iri = 'fin:hasHolding'
        AND h.valid_from <= DATE'{{d}}' AND (h.valid_to IS NULL OR h.valid_to >= DATE'{{d}}')"""
    at = lambda d: {r[0] for r in spark.sql(q.format(d=d)).collect()}  # noqa: E731
    assert at("1995-02-15") == {"fin:CurrentAccount", "fin:InsuranceProduct"}   # SIPO tu 20-02
    assert at("1995-02-25") == {"fin:CurrentAccount", "fin:InsuranceProduct", "fin:RecurringPayment"}
    assert at("1996-06-01") == {"fin:CurrentAccount", "fin:InsuranceProduct", "fin:RecurringPayment",
                                "fin:ClassicCard"}


def test_cq2_san_pham_tin_dung_qua_bao_dong_lop(graph):
    """CQ2: ai giữ SẢN PHẨM TÍN DỤNG — thẻ classic/junior và vay đều là lớp con fin:CreditProduct."""
    spark = graph[0]
    who = {r[0] for r in spark.sql("""
      SELECT DISTINCT c.label FROM triple h
      JOIN triple o ON o.subject_uid = h.object_uid AND o.predicate_iri = 'fin:ofProduct'
      JOIN entity p ON p.uid = o.object_uid
      JOIN subclass s ON s.class_iri = p.class_iri AND s.ancestor_iri = 'fin:CreditProduct'
      JOIN entity c ON c.uid = h.subject_uid
      WHERE h.predicate_iri = 'fin:hasHolding'""").collect()}
    assert who == {"Khách 1", "Khách 2", "Khách 3"}


def test_mbd_va_bank_marketing_dung_taxonomy_chung(graph):
    spark = graph[0]
    rows = spark.sql("""
      SELECT p.class_iri, h.valid_from, e.attributes['purchase_months'] FROM triple o
      JOIN entity p ON p.uid = o.object_uid
      JOIN entity e ON e.uid = o.subject_uid
      JOIN triple h ON h.object_uid = o.subject_uid AND h.predicate_iri = 'fin:hasHolding'
      WHERE o.predicate_iri = 'fin:ofProduct' AND e.source_system = 'mbd' ORDER BY 1""").collect()
    assert [(r[0], r[1], r[2]) for r in rows] == [("fin:AnonymousProduct_1", D(2022, 2, 1), "2"),
                                                   ("fin:AnonymousProduct_3", D(2022, 3, 1), "1")]
    # CQ5 / CQ12: tien gui co ky han cua Bank Marketing la fin:TermDeposit, dem theo nghe nghiep
    occ = {r[0]: r[1] for r in spark.sql("""
      SELECT j.object_literal, count(*) FROM triple h
      JOIN triple j ON j.subject_uid = h.subject_uid AND j.predicate_iri = 'fin:occupation'
      WHERE h.predicate_iri = 'fin:hasHolding' AND h.source_dataset LIKE 'silver_bank_marketing%'
      GROUP BY 1""").collect()}
    assert occ == {"management": 1, "technician": 1}


def test_nhan_khong_co_pii_va_uid_khop_ban_python(graph):
    spark, entity = graph[0], graph[1]
    labels = [r[0] for r in entity.where("class_iri = 'fin:Customer'").select("label").collect()]
    assert "Khách 1" in labels and not any("@" in x for x in labels)
    got = entity.where("natural_key = 'client_id=1' AND source_system = 'berka'").first()["uid"]
    assert got == ids.make_uid("fin:Customer", "berka", "client_id=1")
    gold = entity.where("class_iri = 'fin:GoldCard'").first()["uid"]
    assert gold == ids.product_uid("fin:GoldCard")


def test_dung_lai_cho_cung_uid_va_triple_id(graph, silver):
    _, entity, triple, _ = graph
    e2, t2, _ = B.build(silver, M.load_all(["berka", "mbd", "bank_marketing"]), T.load())
    assert entity.select("uid").exceptAll(e2.select("uid")).count() == 0
    assert triple.select("triple_id").exceptAll(t2.select("triple_id")).count() == 0


def test_canh_mo_coi_lam_job_that_bai(graph):
    from pyspark.sql import functions as F

    spark, entity, triple, _ = graph
    bad = triple.limit(1).withColumn("object_uid", F.lit("khong-ton-tai")).withColumn("triple_id", F.lit("x"))
    with pytest.raises(B.InvariantError, match="mo coi"):
        B.check_invariants(spark, entity, triple.unionByName(bad), [], T.load())


def test_doi_soat_lech_lam_job_that_bai(graph):
    spark, entity, triple, _ = graph
    exp = [{"id": "berka.loan", "sql": "SELECT count(*) + 1 FROM {silver_berka.loan}"}]
    with pytest.raises(B.InvariantError, match="doi soat berka.loan"):
        B.check_invariants(spark, entity, triple, exp, T.load())
