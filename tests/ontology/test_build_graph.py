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


@pytest.fixture(scope="module")
def corebank(silver):
    """silver_corebank = bản sao Berka + một khách (4) và tài khoản (30) do simulator mở thêm."""
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
    return spark


def test_berka_va_corebank_cung_luc_doi_soat_rieng_tung_nguon(corebank):
    """Hai nguồn cùng id mục (holds_account...) nhưng số dòng khác nhau: corebank có thêm chủ tài khoản
    do simulator mở. Lỗi gặp trên dev (04-11): đối soát gộp theo id mục -> lệch 4.500 / 4.534."""
    spark = corebank
    tb = T.load()
    entity, triple, exp = B.build(spark, M.load_all(["berka", "corebank"]), tb)
    report = B.check_invariants(spark, entity, triple, exp, tb)
    recon = {r["mapping_id"]: (r["expected"], r["got"]) for r in report["reconciliation"]}
    assert recon["berka.holds_account"] == (2, 2) and recon["corebank.holds_account"] == (3, 3)
    # cung khach 1 o hai nguon = hai uid, KHONG gop; noi bang canh fin:sameAs (O8)
    assert entity.where("natural_key = 'client_id=1' AND class_iri = 'fin:Customer'").count() == 2
    same = triple.where("predicate_iri = 'fin:sameAs'").collect()
    assert len(same) == 3                     # khach 1, 2, 3 co o ca hai nguon; khach 4 chi corebank
    assert all(r.confidence == 1.0 and r.is_inferred and r.subject_uid < r.object_uid for r in same)
    pair = {ids.make_uid("fin:Customer", "berka", "client_id=2"), ids.make_uid("fin:Customer", "corebank", "client_id=2")}
    assert pair in [{r.subject_uid, r.object_uid} for r in same]


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


def test_chieu_sang_neptune_nhan_theo_cay_lop_va_khong_lo_khoa_goc(graph):
    """docs/NEPTUNE.md mục 4: nhãn gồm lớp tổ tiên, literal thành thuộc tính, không xuất natural_key."""
    from transforms.onto import export_neptune as X

    spark, entity, triple, _ = graph
    sub = spark.table("subclass").select("class_iri", "ancestor_iri")
    v, e = X.frames(entity, triple, sub)
    assert v.count() == entity.count()
    assert e.count() == triple.where("object_uid IS NOT NULL").count()
    assert not [c for c in v.columns if "natural_key" in c]
    lab = {r[0]: r[1] for r in v.select(":ID", ":LABEL").collect()}
    assert lab[ids.product_uid("fin:ClassicCard")] == "ClassicCard;CreditCard;CreditProduct;Product"
    assert lab[ids.make_uid("fin:Customer", "berka", "client_id=1")] == "Customer;Party;Person"
    bm = ids.make_uid("fin:Customer", "bank_marketing", "file=bank-full.csv|row=1")
    assert v.where(f"`:ID` = '{bm}'").first()["occupation:String"] == "management"
    assert {r[0] for r in e.select(":TYPE").distinct().collect()} >= {"holdsAccount", "hasHolding", "transfersTo"}
    # Ban point-in-time: truoc 1996 chua co the (1996-05-01) va vay (1996-06-01)
    _, e95 = X.frames(entity, triple, sub, as_of="1996-01-01")
    assert e95.where("`valid_from:Date` >= '1996-01-01'").count() == 0
    assert e95.where("`:TYPE` = 'transfersTo'").count() == 2


def test_aml_vong_tien_kieu_hinh_va_chu_so_huu(silver):
    """O5 lượt 2 (IBM AML): vòng A->B->C->A thuộc một vụ CYCLE, tài khoản C chỉ có trong giao dịch,
    giao dịch tự chuyển bị loại, kiểu hình trỏ vào khái niệm SKOS fin:Cycle của TBox."""
    spark = silver
    spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.silver_aml")
    _table(spark, "silver_aml.account", [
        # File tai khoan KHONG co so 0 dau, file giao dich CO ('001') -> phai ve cung mot khoa
        ("1", "Bank 1", "A1", "E1", "Corporation #1", "Corporation"),
        ("1", "Bank 1", "A2", "E1", "Corporation #1", "Corporation"),   # E1 so huu 2 tai khoan
        ("2", "Bank 2", "B1", "E2", "Partnership #2", "Partnership"),
    ], "bank_id string, bank_name string, account_number string, entity_id string, entity_name string, "
       "entity_type string")
    t = "txn_id string, event_date date, from_bank string, from_account string, to_bank string, " \
        "to_account string, is_self_transfer boolean"
    _table(spark, "silver_aml.transfer", [
        ("t1", D(2023, 1, 2), "001", "A1", "002", "B1", False),
        ("t2", D(2023, 1, 3), "002", "B1", "003", "C1", False),   # C1 khong co trong bang account
        ("t3", D(2023, 1, 4), "003", "C1", "001", "A1", False),
        ("t4", D(2023, 1, 5), "001", "A1", "002", "B1", False),   # cung cap A1->B1 -> gop, weight 2
        ("t5", D(2023, 1, 5), "001", "A2", "001", "A2", True),    # tu chuyen -> loai
    ], t)
    _table(spark, "silver_aml.laundering_attempt", [
        ("HI-1", "CYCLE", "cycle 3", "t1", "001", "A1", "002", "B1"),
        ("HI-1", "CYCLE", "cycle 3", "t2", "002", "B1", "003", "C1"),
        ("HI-1", "CYCLE", "cycle 3", "t3", "003", "C1", "001", "A1"),
    ], "attempt_id string, typology string, typology_detail string, txn_id string, from_bank string, "
       "from_account string, to_bank string, to_account string")
    tb = T.load()
    entity, triple, exp = B.build(spark, M.load_all(["aml"]), tb)
    report = B.check_invariants(spark, entity, triple, exp, tb)
    assert all(r["expected"] == r["got"] for r in report["reconciliation"])
    acc = lambda b, a: ids.make_uid("fin:Account", "aml", f"bank_id={b}|account_number={a}")  # noqa: E731
    assert entity.where(f"uid = '{acc('3', 'C1')}'").count() == 1          # chi co trong giao dich
    # 3 tai khoan bang tham chieu + C1 = 4: KHONG nhan doi vi '001' / '1'
    assert entity.where("class_iri = 'fin:Account'").count() == 4
    assert entity.where("class_iri = 'fin:Bank'").count() == 3
    tr = {(r.subject_uid, r.object_uid): r.weight for r in triple.where("predicate_iri = 'fin:transfersTo'").collect()}
    assert tr == {(acc("1", "A1"), acc("2", "B1")): 2.0, (acc("2", "B1"), acc("3", "C1")): 1.0,
                  (acc("3", "C1"), acc("1", "A1")): 1.0}
    typ = triple.where("predicate_iri = 'fin:hasTypology'").first()
    assert typ.object_uid == ids.product_uid("fin:Cycle")
    assert entity.where(f"uid = '{typ.object_uid}'").first().class_iri == "fin:Typology"
    # CQ9: A1 tham gia vu HI-1; CQ10: chu so huu cua A1 (E1) co 2 tai khoan
    assert triple.where(f"predicate_iri = 'fin:involvedIn' AND subject_uid = '{acc('1', 'A1')}'").count() == 1
    owner = ids.make_uid("fin:Party", "aml", "entity_id=E1")
    assert triple.where(f"predicate_iri = 'fin:isHeldBy' AND object_uid = '{owner}'").count() == 2


def test_aml_kieu_hinh_khong_tra_duoc_lam_job_that_bai(silver):
    """Mã kiểu hình lạ (không có skos:notation) phải bị chặn, không được thành cạnh trỏ NULL."""
    spark = silver
    spark.sql("INSERT INTO glue_catalog.silver_aml.laundering_attempt VALUES "
              "('HI-2', 'SPIRAL', 'la', 't4', '001', 'A1', '002', 'B1')")
    tb = T.load()
    entity, triple, exp = B.build(spark, M.load_all(["aml"]), tb)
    with pytest.raises(B.InvariantError, match="mo coi"):
        B.check_invariants(spark, entity, triple, exp, tb)
    spark.sql("DELETE FROM glue_catalog.silver_aml.laundering_attempt WHERE attempt_id = 'HI-2'")


def test_aml_vung_dieu_tra_cho_neptune(silver):
    """--SCOPE aml_investigation: hạt giống = tài khoản trong vụ; +1 bước chuyển tiền; +cùng chủ sở hữu;
    cạnh AML giữ khi cả hai đầu trong vùng; nguồn khác không đổi."""
    from transforms.onto import export_neptune as X

    spark = silver
    spark.sql("INSERT INTO glue_catalog.silver_aml.transfer VALUES "
              "('t6', DATE'2023-01-06', '003', 'C1', '004', 'D1', false), "   # D1: doi tac truc tiep -> giu
              "('t7', DATE'2023-01-07', '004', 'D1', '009', 'E9', false)")    # E9: cach 2 buoc -> bo
    tb = T.load()
    entity, triple, _ = B.build(spark, M.load_all(["aml", "mbd"]), tb)
    ent, tri = X.aml_investigation(entity, triple)
    acc = lambda b, a: ids.make_uid("fin:Account", "aml", f"bank_id={b}|account_number={a}")  # noqa: E731
    kept = {r.uid for r in ent.where("class_iri = 'fin:Account'").collect()}
    assert {acc("1", "A1"), acc("2", "B1"), acc("3", "C1"), acc("4", "D1"), acc("1", "A2")} <= kept
    assert acc("9", "E9") not in kept
    pairs = {(r.subject_uid, r.object_uid) for r in tri.where("predicate_iri = 'fin:transfersTo'").collect()}
    assert (acc("3", "C1"), acc("4", "D1")) in pairs and (acc("4", "D1"), acc("9", "E9")) not in pairs
    # Khong canh mo coi trong ban cat; nguon khac (MBD) giu nguyen; kieu hinh van tro khai niem
    uids = {r.uid for r in ent.select("uid").collect()}
    assert all(r.subject_uid in uids and (r.object_uid is None or r.object_uid in uids) for r in tri.collect())
    assert ent.where("source_system = 'mbd'").count() == entity.where("source_system = 'mbd'").count()
    assert tri.where("predicate_iri = 'fin:hasTypology'").count() == 1
    spark.sql("DELETE FROM glue_catalog.silver_aml.transfer WHERE txn_id IN ('t6', 't7')")


def test_o9_closure_vi_tri(graph):
    from transforms.onto import derive as DV

    _, entity, triple, _ = graph
    clo = DV.closure(triple).collect()
    district = ids.make_uid("fin:District", "berka", "district_id=1")
    country = ids.make_uid("fin:Country", "berka", "code=CZ")
    assert (district, country, 2) in {(r.start_uid, r.node_uid, r.depth) for r in clo}
    assert max(r.depth for r in clo) == 2


def test_o9_product_transition_va_khong_ro_ri(graph, silver):
    """P(B | A, H) đúng đáp án tính tay; thêm sự kiện SAU mốc cắt không đổi bất kỳ giá trị nào (O9)."""
    import datetime as dt

    from transforms.onto import derive as DV

    spark, entity, triple, _ = graph
    acq = DV.acquisitions(entity, triple).cache()
    cut = {"berka": "1997-12-31", "mbd": "2022-12-31"}
    pt = {(r.from_product, r.to_product, r.horizon_months): (r.n_from, r.n_both, r.p)
          for r in DV.product_transition(acq, cut, horizons=(3,)).collect()}
    # Khach 1: TK thanh toan 1995-01-10 -> bao hiem 02-03, dinh ky 02-20 (trong 3 thang)
    # Khach 2: TK thanh toan 1996-03-01 -> vay 1996-06-01 (dung 3 thang) => 2 khach co CurrentAccount
    assert pt[("fin:CurrentAccount", "fin:InsuranceProduct", 3)] == (2, 1, 0.5)
    assert pt[("fin:CurrentAccount", "fin:Loan", 3)] == (2, 1, 0.5)
    assert ("fin:ClassicCard", "fin:Loan", 3) not in pt          # the sau vay: khong phai A -> B
    # base_rate chi tren nguon CO san pham: bao hiem chi o Berka (3 khach) -> 1/3, KHONG chia them khach MBD
    row = next(r for r in DV.product_transition(acq, cut, horizons=(3,)).collect()
               if (r.from_product, r.to_product) == ("fin:CurrentAccount", "fin:InsuranceProduct"))
    assert abs(row.base_rate - 1 / 3) < 1e-9 and abs(row.lift - 1.5) < 1e-9
    # Ro ri: them luot mua SAU moc cat -> bang khong doi
    extra = spark.createDataFrame(
        [(ids.make_uid("fin:Customer", "berka", "client_id=1"), "berka", "fin:Loan", dt.date(1998, 3, 1))],
        acq.schema)
    pt2 = {(r.from_product, r.to_product, r.horizon_months): (r.n_from, r.n_both, r.p)
           for r in DV.product_transition(acq.unionByName(extra), cut, horizons=(3,)).collect()}
    assert pt2 == pt


def test_o9_khach_trung_sameas_chi_dem_mot_lan(corebank):
    """Berka và corebank là cùng người (sameAs): product_transition chỉ đếm bản corebank (nguồn ưu tiên)."""
    from transforms.onto import derive as DV

    tb = T.load()
    entity, triple, _ = B.build(corebank, M.load_all(["berka", "corebank"]), tb)
    acq = DV.acquisitions(entity, triple)
    assert {r.source_system for r in acq.select("source_system").distinct().collect()} == {"corebank"}
    n_cust = acq.select("customer_uid").distinct().count()
    assert n_cust == entity.where("source_system = 'corebank' AND class_iri = 'fin:Customer'").join(
        acq.select(acq.customer_uid.alias("uid")).distinct(), "uid").count()


def test_opensanctions_so_huu_va_khop_ten_vao_hang_doi_duyet(corebank):
    """OpenSanctions lên đồ thị (Party + SanctionedEntity, isDirectlyOwnedBy) và khớp mờ khách <-> cấm vận:
    tên trùng (như corebank.seed --sanction-hits) phải vào HÀNG ĐỢI DUYỆT, không tự thành cạnh sameAs."""
    from transforms.onto import derive as DV
    from transforms.onto import resolve as R

    spark = corebank
    spark.sql("CREATE NAMESPACE IF NOT EXISTS glue_catalog.silver_opensanctions")
    if "full_name" not in spark.table("glue_catalog.silver_corebank.client").columns:
        spark.sql("ALTER TABLE glue_catalog.silver_corebank.client ADD COLUMN full_name string")
    for cid, nm in [(1, "Jana Nováková"), (2, "Ivan Aleksandrovich TYRYSHKIN"), (3, "Petr Svoboda"),
                    (4, "Tyryshkin Ivan Aleksandrovic")]:
        spark.sql(f"UPDATE glue_catalog.silver_corebank.client SET full_name = '{nm}' WHERE client_id = {cid}")
    _table(spark, "silver_opensanctions.target", [
        ("NK-1", "Person", "Ivan Aleksandrovich TYRYSHKIN", "1961-03-05", ["Иван Тырышкин"], ["ru"]),
        ("NK-2", "Company", "Sanctioned Holding LLC", None, [], ["ru"]),
    ], "entity_id string, entity_schema string, name string, birth_date string, aliases array<string>, "
       "countries array<string>")
    import datetime as dt
    ts = dt.datetime(2026, 10, 1)
    _table(spark, "silver_opensanctions.entity", [
        ("NK-1", "Person", "Ivan TYRYSHKIN", True, {"name": ["Ivan Aleksandrovich TYRYSHKIN"], "country": ["ru"]}, ts),
        ("NK-2", "Company", "Sanctioned Holding", True, {"name": ["Sanctioned Holding LLC"]}, ts),
        ("NK-3", "Company", "Shell s.r.o.", False, {"name": ["Shell s.r.o."]}, ts),
        ("OWN-1", "Ownership", "own", False, {"asset": ["NK-3"], "owner": ["NK-1"], "percentage": ["50%"]}, ts),
        ("OWN-2", "Ownership", "own", False, {"asset": ["VESSEL-9"], "owner": ["NK-1"]}, ts),  # tau: bo
    ], "entity_id string, entity_schema string, caption string, is_target boolean, "
       "properties map<string,array<string>>, first_seen timestamp")
    tb = T.load()
    entity, triple, exp = B.build(spark, M.load_all(["corebank", "opensanctions"]), tb)
    B.check_invariants(spark, entity, triple, exp, tb)
    nk1 = ids.make_uid("fin:Party", "opensanctions", "entity_id=NK-1")
    nk3 = ids.make_uid("fin:Party", "opensanctions", "entity_id=NK-3")
    assert set(entity.where(f"uid = '{nk1}'").first().also_types) == {"fin:SanctionedEntity", "fin:Person"}
    own = triple.where("predicate_iri = 'fin:isDirectlyOwnedBy'").collect()
    assert [(r.subject_uid, r.object_uid, r.weight) for r in own] == [(nk3, nk1, 0.5)]
    # Khop: KHONG tu thanh canh (ten trung nhung nam sinh / nuoc khong khop -> < 0.9)
    assert triple.where("predicate_iri = 'fin:sameAs'").count() == 0
    auto, review = R.all_links(spark, entity)
    rv = {r.uid_a if r.uid_b == nk1 else r.uid_b: (r.confidence, r.evidence) for r in review.collect()}
    c2 = ids.make_uid("fin:Customer", "corebank", "client_id=2")
    c4 = ids.make_uid("fin:Customer", "corebank", "client_id=4")
    assert rv[c2] == (0.8, "name=exact;birth=no;country=no")
    assert rv[c4][1].startswith("name=fuzzy:") and 0.6 <= rv[c4][0] < 0.8
    assert ids.make_uid("fin:Customer", "corebank", "client_id=1") not in rv
    assert not any("TYRYSHKIN" in e or "Ivan" in e for _, e in rv.values())   # evidence khong chua ten
    # O9: bao dong so huu
    clo = DV.closure(triple).where("predicate_iri = 'fin:isDirectlyOwnedBy'").collect()
    assert [(r.start_uid, r.node_uid, r.depth) for r in clo] == [(nk3, nk1, 1)]
