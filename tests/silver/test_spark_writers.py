"""Writer trên Spark + Iceberg thật: đi hết extract -> transform -> writer.

Mỗi test dựng raw đúng định dạng sourcing, chạy SilverRunner như Glue chạy,
rồi đọc bảng Iceberg để kiểm. Tên kịch bản mô tả đúng tình huống thật.
"""

from __future__ import annotations

import pytest

from silver.config import from_dict
from silver.runner import SilverRunner
from silver.transform import QualityError
from silver.writers.scd2 import OutOfOrderError

from .conftest import requires_spark

pytestmark = requires_spark


def _cfg(db, **over):
    base = {
        "name": "t_entity",
        "source": {"dataset": "t/entity", "records_path": "data"},
        "target": {"database": db, "table": "entity"},
        "columns": [
            {"name": "id", "nullable": False},
            {"name": "name"},
            {"name": "amount", "type": "decimal(18,2)"},
            {"name": "updated", "type": "timestamp"},
            {"name": "total", "from": "response", "path": "meta.total", "type": "int", "hash": False},
        ],
        "technical": {"keys": ["id"], "cdc_index": "updated"},
    }
    for k, v in over.items():
        base[k] = v if not isinstance(v, dict) else {**base.get(k, {}), **v}
    return from_dict(base)


def _resp(*rows):
    return {"meta": {"total": len(rows)},
            "data": [{"id": r[0], "name": r[1], "amount": r[2], "updated": r[3]} for r in rows]}


def _run(spark, cfg, manifest, **kw):
    return SilverRunner(spark, cfg, bucket="", joblog_table=None, manifest=manifest, **kw).run()


def _rows(spark, cfg, where="true", order="id, ds_valid_from"):
    return [r.asDict() for r in spark.sql(
        f"SELECT * FROM {cfg.target.fqn} WHERE {where} ORDER BY {order}").collect()]


# ======================= SCD2 =======================


def _scd2(db):
    return _cfg(db, writer={"type": "scd2", "snapshot": True})


def test_scd2_vong_doi_day_du(spark, lake, fresh_db):
    cfg = _scd2(fresh_db)
    d1 = lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1.5", "2026-09-01T00:00:00Z"),
                                                        ("B", "Binh", "2", "2026-09-01T00:00:00Z"))])
    r1 = _run(spark, cfg, d1)
    assert r1.metrics["inserted"] == 2

    # Ngay 2: A doi ten, B bien mat, C moi
    d2 = lake.api_run("t/entity", "2026-09-02", [_resp(("A", "An moi", "1.5", "2026-09-02T00:00:00Z"),
                                                        ("C", "Cuong", "3", "2026-09-02T00:00:00Z"))])
    r2 = _run(spark, cfg, d2)
    assert (r2.metrics["inserted"], r2.metrics["updated"], r2.metrics["deleted"]) == (1, 1, 1)

    cur = {r["id"]: r for r in _rows(spark, cfg, "ds_is_current")}
    assert cur["A"]["name"] == "An moi" and cur["A"]["ds_record_status"] == "U"
    assert cur["B"]["ds_record_status"] == "D" and cur["B"]["name"] == "Binh"   # bia mo mang gia tri cu
    assert cur["C"]["ds_record_status"] == "I"

    a = _rows(spark, cfg, "id = 'A'")
    assert [str(x["ds_valid_from"]) for x in a] == ["2026-09-01", "2026-09-02"]
    assert str(a[0]["ds_valid_to"]) == "2026-09-02" and a[0]["ds_is_current"] is False
    assert str(a[1]["ds_valid_to"]) == "9999-12-31"


def test_scd2_khong_doi_thi_khong_sinh_phien_ban(spark, lake, fresh_db):
    """Cot hash:false (meta.total) doi khong duoc tinh la thay doi."""
    cfg = _scd2(fresh_db)
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))]))
    same = _resp(("A", "An", "1", "2026-09-01T00:00:00Z"))
    same["meta"]["total"] = 999
    r = _run(spark, cfg, lake.api_run("t/entity", "2026-09-02", [same]))
    assert r.metrics["unchanged"] == 1 and r.metrics["updated"] == 0
    assert len(_rows(spark, cfg)) == 1


def test_scd2_chay_lai_cung_ngay_idempotent(spark, lake, fresh_db):
    cfg = _scd2(fresh_db)
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))]))
    m2 = lake.api_run("t/entity", "2026-09-02", [_resp(("A", "An 2", "1", "2026-09-02T00:00:00Z"))])
    _run(spark, cfg, m2)
    before = _rows(spark, cfg)
    r = _run(spark, cfg, m2)               # SFN retry / chay lai tay
    after = _rows(spark, cfg)
    assert r.metrics["rolled_back"] == 1
    strip = lambda rows: [{k: v for k, v in x.items() if not k.startswith("ds_etl")} for x in rows]  # noqa: E731
    assert strip(after) == strip(before)


def test_scd2_du_lieu_den_tre_bi_tu_choi(spark, lake, fresh_db):
    cfg = _scd2(fresh_db)
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-05", [_resp(("A", "An", "1", "2026-09-05T00:00:00Z"))]))
    late = lake.api_run("t/entity", "2026-09-03", [_resp(("A", "Cu", "1", "2026-09-03T00:00:00Z"))])
    with pytest.raises(OutOfOrderError):
        _run(spark, cfg, late)


def test_scd2_key_tai_xuat_hien_sau_khi_xoa(spark, lake, fresh_db):
    cfg = _scd2(fresh_db)
    row = ("A", "An", "1", "2026-09-01T00:00:00Z")
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(row, ("B", "Binh", "1", "2026-09-01T00:00:00Z"))]))
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-02", [_resp(row)]))                      # B xoa
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-03", [_resp(row, ("B", "Binh", "1", "2026-09-03T00:00:00Z"))]))
    b = _rows(spark, cfg, "id = 'B'")
    assert [x["ds_record_status"] for x in b] == ["I", "D", "I"]
    assert [x["ds_is_current"] for x in b] == [False, False, True]


def _delta(db):
    """Nguồn CDC1: mỗi manifest chỉ là PHẦN THAY ĐỔI, không phải ảnh chụp."""
    return _cfg(db, writer={"type": "scd2", "snapshot": False})


def test_scd2_delta_hai_lo_cung_ngay_khong_mat_thay_doi(spark, lake, fresh_db):
    """Lô 2 cùng ngày chỉ có A. Không được hoàn tác B mà lô 1 đã ghi."""
    cfg = _delta(fresh_db)
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"),
                                                                   ("B", "Binh", "1", "2026-09-01T00:00:00Z"))]))
    r = _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An 2", "1", "2026-09-01T05:00:00Z"))]))
    assert r.metrics["rolled_back"] == 0 and r.metrics["updated"] == 1
    cur = {x["id"]: x for x in _rows(spark, cfg, "ds_is_current")}
    assert cur["A"]["name"] == "An 2" and cur["B"]["name"] == "Binh"
    assert len(_rows(spark, cfg, "id = 'A'")) == 2


def test_scd2_delta_chay_lai_cung_manifest_idempotent(spark, lake, fresh_db):
    cfg = _delta(fresh_db)
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"),
                                                                   ("B", "Binh", "1", "2026-09-01T00:00:00Z"))]))
    m2 = lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An 2", "1", "2026-09-01T05:00:00Z"))])
    _run(spark, cfg, m2)
    before = _rows(spark, cfg)
    r = _run(spark, cfg, m2)                   # SFN retry: chi hoan tac DUNG manifest nay
    assert r.metrics["rolled_back"] == 1
    strip = lambda rows: [{k: v for k, v in x.items() if not k.startswith("ds_etl")} for x in rows]  # noqa: E731
    assert strip(_rows(spark, cfg)) == strip(before)


def test_scd2_delta_chay_lai_manifest_cu_sau_manifest_moi_bi_tu_choi(spark, lake, fresh_db):
    cfg = _delta(fresh_db)
    m1 = lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))])
    _run(spark, cfg, m1)
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An 2", "1", "2026-09-01T05:00:00Z"))]))
    with pytest.raises(OutOfOrderError, match="REBUILD_FROM"):
        _run(spark, cfg, m1)


def test_scd2_trung_key_trong_lo_giu_ban_moi_nhat(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "scd2"})
    m = lake.api_run("t/entity", "2026-09-01", [_resp(("A", "cu", "1", "2026-09-01T01:00:00Z"),
                                                       ("A", "moi", "1", "2026-09-01T05:00:00Z"))])
    r = _run(spark, cfg, m)
    assert r.metrics["duplicate_keys"] == 1
    (a,) = _rows(spark, cfg)
    assert a["name"] == "moi"


# ======================= SCD4a =======================


def test_scd4a_trang_thai_so_voi_anh_truoc(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "scd4a"})
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"),
                                                                    ("B", "Binh", "1", "2026-09-01T00:00:00Z"))]))
    r = _run(spark, cfg, lake.api_run("t/entity", "2026-09-02", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"),
                                                                        ("C", "Cuong", "1", "2026-09-02T00:00:00Z"))]))
    assert (r.metrics["unchanged"], r.metrics["inserted"], r.metrics["deleted"]) == (1, 1, 1)
    d2 = {x["id"]: x["ds_record_status"] for x in _rows(spark, cfg, "ds_partition_date = DATE'2026-09-02'", "id")}
    assert d2 == {"A": "N", "B": "D", "C": "I"}
    # "hist": anh ngay 1 van nguyen
    assert len(_rows(spark, cfg, "ds_partition_date = DATE'2026-09-01'", "id")) == 2


def test_scd4a_chay_lai_ghi_de_dung_ngay_do(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "scd4a"})
    m = lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))])
    _run(spark, cfg, m)
    _run(spark, cfg, m)
    assert len(_rows(spark, cfg, order="id")) == 1


# ======================= simple =======================


def test_append_chay_lai_cung_manifest_khong_nhan_doi(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "simple", "mode": "append"})
    m = lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))])
    _run(spark, cfg, m)
    _run(spark, cfg, m)
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("B", "Binh", "1", "2026-09-01T00:00:00Z"))]))
    assert sorted(x["id"] for x in _rows(spark, cfg, order="id")) == ["A", "B"]


def test_overwrite_partition_chi_de_ngay_do(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "simple", "mode": "overwrite_partition"},
               target={"partition_by": ["ds_partition_date"]})
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))]))
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-02", [_resp(("B", "Binh", "1", "2026-09-02T00:00:00Z"))]))
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-02", [_resp(("C", "Cuong", "1", "2026-09-02T00:00:00Z"))]))
    assert sorted(x["id"] for x in _rows(spark, cfg, order="id")) == ["A", "C"]


def test_overwrite_table_tu_choi_lo_rong(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "simple", "mode": "overwrite_table"})
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))]))
    # Response rong: runner dung o NO_DATA, khong goi writer -> bang con nguyen
    r = _run(spark, cfg, lake.api_run("t/entity", "2026-09-02", [{"meta": {"total": 0}, "data": []}]))
    assert r.status == "NO_DATA" and len(_rows(spark, cfg, order="id")) == 1


# ======================= ep kieu + quality + schema =======================


def test_loi_ep_kieu_vuot_nguong_thi_that_bai(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "simple"})
    m = lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "khong-phai-so", "2026-09-01T00:00:00Z"))])
    with pytest.raises(QualityError, match="amount"):
        _run(spark, cfg, m)


def test_kieu_va_cot_envelope_dung(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "simple"})
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "12.345", "2026-09-01T07:08:09Z"))]))
    (a,) = _rows(spark, cfg, order="id")
    assert str(a["amount"]) == "12.35" and a["total"] == 1                 # decimal lam tron, envelope
    # So sanh chuoi do SPARK dinh dang (session UTC): collect() ve Python doi
    # timestamp sang gio MAY, nen doc a["updated"] tren may UTC+7 se lech 7 tieng.
    ts = spark.sql(f"SELECT date_format(updated, 'yyyy-MM-dd HH:mm:ss') FROM {cfg.target.fqn}").first()[0]
    assert ts == "2026-09-01 07:08:09"
    assert str(a["ds_partition_date"]) == "2026-09-01" and len(a["ds_key"]) == 64


def test_them_cot_vao_entry_thi_bang_tu_them_cot(spark, lake, fresh_db):
    cfg = _cfg(fresh_db, writer={"type": "simple"})
    _run(spark, cfg, lake.api_run("t/entity", "2026-09-01", [_resp(("A", "An", "1", "2026-09-01T00:00:00Z"))]))
    cols = cfg.columns + [type(cfg.columns[0])(name="name_upper", expr="upper(name)")]
    cfg2 = _cfg(fresh_db, writer={"type": "simple"},
                columns=[c.__dict__ | {"from": c.from_.value} for c in cols])
    r = _run(spark, cfg2, lake.api_run("t/entity", "2026-09-02", [_resp(("B", "Binh", "1", "2026-09-02T00:00:00Z"))]))
    assert r.metrics["ddl"]["added"] == ["name_upper"]
    assert {x["id"]: x["name_upper"] for x in _rows(spark, cfg2, order="id")} == {"A": None, "B": "BINH"}
