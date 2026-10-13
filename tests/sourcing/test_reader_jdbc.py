"""Reader JDBC với câu SQL khai trong entry (sourcing/readers/jdbc.py, sqltemplate.py).

Driver (psycopg2, PyMySQL) do profile Glue cài; test thay kết nối và cursor bằng
bản giả tối thiểu, nên chạy được mà không cần database.
"""

from __future__ import annotations

import datetime as dt
import decimal
import sys
import types
import uuid

import pytest

from sourcing.config import ConfigError
from sourcing.readers.jdbc import JdbcReader, _jsonable, _mysql_ts
from sourcing.sqltemplate import check_select, referenced_params, to_pyformat
from sourcing.state import Checkpoint

from .conftest import make_cfg

Q_CDC1 = "SELECT * FROM core.trans WHERE updated_at > :watermark ORDER BY updated_at"
CDC1_TS = {"mode": "cdc1", "watermark_column": "updated_at",
           "initial_watermark": "1970-01-01T00:00:00+00:00"}


def _cfg(query=Q_CDC1, cdc=None, **jdbc):
    return make_cfg(
        source_type="jdbc",
        jdbc={"secret_name": "fingraph/src/s", "query": query, **jdbc},
        cdc=cdc or CDC1_TS,
    )


# ---------- sqltemplate ----------


def test_tham_so_bo_qua_chuoi_comment_va_ep_kieu_postgres():
    sql = """SELECT ':watermark' AS s, x::timestamp -- :run_ts
             /* :chunk_size */ FROM t WHERE id > :watermark AND k LIKE 'VN%' AND d < :run_ts"""
    assert referenced_params(sql) == {"watermark", "run_ts"}
    out = to_pyformat(sql, ["watermark", "run_ts", "chunk_size"])
    assert "x::timestamp" in out
    assert "id > %(watermark)s" in out and "d < %(run_ts)s" in out
    assert "':watermark'" in out            # trong chuoi: giu nguyen
    assert "LIKE 'VN%%'" in out             # % tu thoat cho driver
    assert "/* :chunk_size */" in out


def test_tham_so_la_giu_nguyen_de_db_bao_loi():
    assert to_pyformat("SELECT :la", ["watermark"]) == "SELECT :la"


@pytest.mark.parametrize("sql,ok", [
    ("SELECT 1", True),
    ("  -- ghi chu\n with x as (select 1) select * from x;", True),
    ("SELECT ';' AS s", True),
    ("UPDATE t SET a = 1", False),
    ("SELECT 1; DELETE FROM t", False),
    ("", False),
])
def test_check_select(sql, ok):
    assert check_select(sql)[0] is ok


# ---------- kiểm tra cấu hình ----------


@pytest.mark.parametrize("over,msg", [
    ({"query": "SELECT * FROM t"}, "khong loc theo :watermark"),
    ({"cdc": {"mode": "cdc1", "watermark_column": "id"}}, "can cdc.initial_watermark"),
    ({"cdc": {"mode": "cdc5"}}, "cdc5 khong co moc"),
    ({"query": "DELETE FROM t"}, "bat dau bang SELECT"),
    ({"query": Q_CDC1 + " AND k = :ma"}, "tham so chua khai"),
    ({"params": {"watermark": 1}}, "trung tham so co san"),
    ({"chunk_size": 100}, "watermark_type int"),
    ({"driver": "oracle"}, "khong ho tro"),
])
def test_cau_hinh_sai_bi_chan_luc_nap(over, msg):
    cdc = over.pop("cdc", None)
    query = over.pop("query", Q_CDC1)
    with pytest.raises(ConfigError, match=msg):
        _cfg(query=query, cdc=cdc, **over)


def test_chunk_thieu_limit_bi_chan():
    with pytest.raises(ConfigError, match="LIMIT :chunk_size"):
        _cfg(query="SELECT * FROM t WHERE id > :watermark ORDER BY id", chunk_size=10,
             cdc={"mode": "cdc1", "watermark_column": "id", "watermark_type": "int",
                  "initial_watermark": "0"})


def test_params_tham_chieu_duoc():
    cfg = _cfg(query=Q_CDC1 + " AND branch = :branch", params={"branch": "HN"})
    assert JdbcReader(cfg).build_params("2026-01-01T00:00:00+00:00")["branch"] == "HN"


# ---------- bind tham số ----------


def test_bind_int_la_so_that():
    cfg = _cfg(query="SELECT * FROM t WHERE id > :watermark",
               cdc={"mode": "cdc1", "watermark_column": "id", "watermark_type": "int",
                    "initial_watermark": "0"})
    assert JdbcReader(cfg).build_params("10")["watermark"] == 10


def test_bind_timestamp_mysql_bo_T_va_mui_gio():
    r = JdbcReader(_cfg(driver="mysql"))
    p = r.build_params("2026-09-01T07:00:00+07:00")
    assert p["watermark"] == "2026-09-01 00:00:00"
    assert " " in p["run_ts"] and "+" not in p["run_ts"]
    assert _mysql_ts("2026-09-01T00:00:00.250000Z") == "2026-09-01 00:00:00.250000"


def test_bind_timestamp_postgres_giu_iso():
    assert JdbcReader(_cfg()).build_params("2026-09-01T00:00:00+00:00")["watermark"] == \
        "2026-09-01T00:00:00+00:00"


def test_jsonable_ep_kieu_driver():
    u = uuid.UUID(int=1)
    out = _jsonable({
        "ts": dt.datetime(2026, 9, 1, 7, 30), "d": dt.date(2026, 9, 1),
        "n": decimal.Decimal("1.25"), "id": u, "raw": memoryview(b"\x01\xff"), "s": "x",
    })
    # Decimal -> chuoi: float lam sai so tien ngay o tang raw
    assert out == {"ts": "2026-09-01T07:30:00", "d": "2026-09-01", "n": "1.25",
                   "id": str(u), "raw": "01ff", "s": "x"}


# ---------- read() với kết nối giả ----------


class _Cursor:
    """Mỗi execute() lấy kết quả kế tiếp trong `results` (một list dòng)."""

    def __init__(self, results, log):
        self.results, self.log = results, log
        self.rows = []
        self.description = None

    def execute(self, sql, params):
        self.log.append((sql, dict(params)))
        self.rows = self.results.pop(0) if self.results else []
        self.description = [(k,) for k in (self.rows[0] if self.rows else {})]

    def __iter__(self):
        return iter(self.rows)

    def close(self):
        pass


class _Conn:
    def __init__(self, results):
        self.results = results
        self.executed = []
        self.opened = []
        self.rollbacks = 0

    def cursor(self, *a, **kw):
        self.opened.append((a, kw))
        return _Cursor(self.results, self.executed)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


def _reader(monkeypatch, cfg, *results):
    r = JdbcReader(cfg)
    conn = _Conn(list(results))
    monkeypatch.setattr(r, "_connect", lambda: conn)
    fake = types.ModuleType("psycopg2")
    fake.extras = types.SimpleNamespace(RealDictCursor=object)
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake.extras)
    return r, conn


def test_read_dung_nguyen_cau_sql_cua_entry_va_bind_moc(monkeypatch):
    cfg = _cfg(cdc={**CDC1_TS, "lookback_seconds": 600})
    rows = [{"id": 1, "updated_at": dt.datetime(2026, 9, 2, 1)},
            {"id": 2, "updated_at": dt.datetime(2026, 9, 2, 3)}]
    r, conn = _reader(monkeypatch, cfg, rows)

    (b,) = list(r.read(Checkpoint("t_job", {"default": "2026-09-01T00:10:00+00:00"})))
    sql, params = conn.executed[0]
    assert sql == "SELECT * FROM core.trans WHERE updated_at > %(watermark)s ORDER BY updated_at"
    assert params["watermark"] == "2026-09-01T00:00:00+00:00"  # da tru lookback
    assert b.watermark == "2026-09-02T03:00:00"
    assert b.records[0]["updated_at"] == "2026-09-02T01:00:00"
    assert r.observed_columns == ["id", "updated_at"]


def test_read_lan_dau_dung_initial_watermark(monkeypatch):
    r, conn = _reader(monkeypatch, _cfg(), [])
    assert list(r.read(None)) == []
    assert conn.executed[0][1]["watermark"] == "1970-01-01T00:00:00+00:00"


def test_moc_timestamp_so_theo_thoi_diem_khong_so_chuoi(monkeypatch):
    """Dòng thứ hai MUỘN hơn nhưng chuỗi '2026-09-02T09:00:00+07:00' < '...T03...' sai."""
    rows = [{"updated_at": "2026-09-02T04:00:00+00:00"},
            {"updated_at": "2026-09-02T12:00:00+07:00"},   # = 05:00 UTC
            {"updated_at": "2026-09-02T03:00:00"}]
    r, _ = _reader(monkeypatch, _cfg(), rows)
    assert list(r.read(None))[0].watermark == "2026-09-02T12:00:00+07:00"


def test_thieu_cot_moc_trong_ket_qua_bao_loi(monkeypatch):
    r, _ = _reader(monkeypatch, _cfg(), [{"id": 1}])
    with pytest.raises(RuntimeError, match="khong co cot moc 'updated_at'"):
        list(r.read(None))


def test_cdc5_khong_lay_moc(monkeypatch):
    cfg = _cfg(query="SELECT * FROM ref.branch", cdc={"mode": "cdc5"})
    r, conn = _reader(monkeypatch, cfg, [{"code": "HN"}])
    (b,) = list(r.read(None))
    assert b.watermark is None and conn.executed[0][1]["watermark"] is None


def test_chia_lo_theo_fetch_size(monkeypatch):
    rows = [{"id": i, "updated_at": f"2026-09-0{i + 1}"} for i in range(5)]
    r, _ = _reader(monkeypatch, _cfg(fetch_size=2), rows)
    assert [len(b) for b in r.read(None)] == [2, 2, 1]


CHUNK_CDC = {"mode": "cdc1", "watermark_column": "id", "watermark_type": "int",
             "initial_watermark": "0"}
Q_CHUNK = "SELECT * FROM t WHERE id > :watermark ORDER BY id LIMIT :chunk_size"


def test_chia_khuc_chay_lai_query_voi_moc_moi_toi_khi_khuc_thieu(monkeypatch):
    cfg = _cfg(query=Q_CHUNK, cdc=CHUNK_CDC, chunk_size=2)
    r, conn = _reader(monkeypatch, cfg,
                      [{"id": 1}, {"id": 2}], [{"id": 5}, {"id": 9}], [{"id": 12}])
    batches = list(r.read(None))
    assert [p["watermark"] for _, p in conn.executed] == [0, 2, 9]
    assert all(p["chunk_size"] == 2 for _, p in conn.executed)
    assert [b.watermark for b in batches] == ["2", "9", "12"]
    # Ket thuc transaction sau moi khuc: khong giu snapshot suot lan chay
    assert conn.rollbacks == 3


def test_chia_khuc_moc_khong_tien_thi_dung_thay_vi_lap_vo_han(monkeypatch):
    cfg = _cfg(query=Q_CHUNK, cdc=CHUNK_CDC, chunk_size=2)
    r, _ = _reader(monkeypatch, cfg, [{"id": 0}, {"id": 0}])
    with pytest.raises(RuntimeError, match="moc khong tien"):
        list(r.read(None))


def test_mysql_dung_cursor_doc_dan(monkeypatch):
    """Cursor mặc định của PyMySQL tải CẢ kết quả vào RAM (trans Berka 1,06 triệu dòng)."""
    cfg = _cfg(driver="mysql", query="SELECT * FROM t WHERE id > :watermark ORDER BY id",
               cdc=CHUNK_CDC)
    r, conn = _reader(monkeypatch, cfg, [{"id": 7, "amount": decimal.Decimal("1.5")}])
    sscursor = object()
    fake = types.ModuleType("pymysql")
    fake.cursors = types.SimpleNamespace(SSDictCursor=sscursor, DictCursor=object())
    monkeypatch.setitem(sys.modules, "pymysql", fake)
    monkeypatch.setitem(sys.modules, "pymysql.cursors", fake.cursors)

    (b,) = list(r.read(None))
    assert conn.opened == [((sscursor,), {})]
    assert b.watermark == "7" and b.records[0]["amount"] == "1.5"


def test_driver_khong_ho_tro_luc_ket_noi(aws):
    import json

    import boto3

    boto3.client("secretsmanager").create_secret(Name="fingraph/src/s", SecretString=json.dumps(
        {"host": "h", "username": "u", "password": "p", "dbname": "d"}))
    r = JdbcReader(_cfg())
    r.j.driver = "oracle"  # vuot qua kiem tra cau hinh de thu nhanh ket noi
    with pytest.raises(ValueError, match="driver khong ho tro: oracle"):
        r._connect()
