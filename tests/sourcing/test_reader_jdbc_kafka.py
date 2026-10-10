"""Reader JDBC và Kafka.

Hai reader này cần thư viện ngoài (psycopg2, confluent-kafka) mà Glue chỉ cài
khi entry khai `additional_python_modules`. Test không cài chúng: phần SQL và
ép kiểu là hàm thuần, còn driver/consumer được thay bằng bản giả tối thiểu
nạp qua `sys.modules`.
"""

from __future__ import annotations

import datetime as dt
import decimal
import sys
import types
import uuid

import pytest

from sourcing.readers.jdbc import JdbcReader, _jsonable
from sourcing.state import Checkpoint

from .conftest import make_cfg


def _jdbc_cfg(cdc=None, **jdbc):
    return make_cfg(
        source_type="jdbc",
        jdbc={"secret_name": "fingraph/src/s", "schema": "public", "table": "instruments", **jdbc},
        cdc=cdc or {"mode": "cdc1", "watermark_column": "updated_at"},
    )


# ---------- JDBC: SQL ----------


def test_sql_cdc1_tham_so_hoa_va_sap_xep():
    sql, params = JdbcReader(_jdbc_cfg())._build_sql("2026-09-01T00:00:00+00:00")
    assert sql == ('SELECT * FROM "public"."instruments" WHERE "updated_at" > %s '
                   'ORDER BY "updated_at" ASC')
    assert params == ["2026-09-01T00:00:00+00:00"]


def test_sql_lan_dau_khong_loc_moc():
    sql, params = JdbcReader(_jdbc_cfg())._build_sql(None)
    assert "WHERE" not in sql and params == []


def test_sql_extra_where_boc_ngoac():
    sql, _ = JdbcReader(_jdbc_cfg(extra_where="a = 1 OR b = 2"))._build_sql("x")
    assert 'WHERE "updated_at" > %s AND (a = 1 OR b = 2)' in sql


def test_sql_cdc5_khong_order_khong_loc():
    sql, params = JdbcReader(_jdbc_cfg(cdc={"mode": "cdc5"}))._build_sql(None)
    assert sql == 'SELECT * FROM "public"."instruments"' and params == []


def test_sql_mysql_boc_backtick_ke_ca_tu_khoa():
    """MariaDB coi "x" la CHUOI; bang `order` cua Berka la tu khoa SQL."""
    cfg = _jdbc_cfg(driver="mysql", schema="financial", table="order",
                    cdc={"mode": "cdc1", "watermark_column": "order_id", "watermark_type": "int"})
    sql, params = JdbcReader(cfg)._build_sql("10")
    assert sql == ("SELECT * FROM `financial`.`order` WHERE `order_id` > %s "
                   "ORDER BY `order_id` ASC")
    assert params == ["10"]


def test_sql_boc_dinh_danh_chua_ky_tu_boc():
    pg = JdbcReader(_jdbc_cfg(table='a"b'))
    my = JdbcReader(_jdbc_cfg(driver="mysql", table="a`b"))
    assert pg._quote('a"b') == '"a""b"' and my._quote("a`b") == "`a``b`"


def test_jsonable_ep_kieu_driver():
    u = uuid.UUID(int=1)
    out = _jsonable({
        "ts": dt.datetime(2026, 9, 1, 7, 30), "d": dt.date(2026, 9, 1),
        "n": decimal.Decimal("1.25"), "id": u, "raw": memoryview(b"\x01\xff"), "s": "x",
    })
    # Decimal -> chuoi: float lam sai so tien ngay o tang raw
    assert out == {"ts": "2026-09-01T07:30:00", "d": "2026-09-01", "n": "1.25",
                   "id": str(u), "raw": "01ff", "s": "x"}


# ---------- JDBC: read() với cursor giả ----------


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = None

    def execute(self, sql, params):
        self.executed = (sql, params)

    def __iter__(self):
        return iter(self.rows)

    def close(self):
        pass


def _jdbc_reader_with_rows(monkeypatch, rows, cfg):
    r = JdbcReader(cfg)
    cur = _Cursor(rows)
    conn = types.SimpleNamespace(cursor=lambda *a, **kw: cur.opened.append((a, kw)) or cur,
                                 close=lambda: None)
    cur.opened = []
    monkeypatch.setattr(r, "_connect", lambda: conn)
    # read() import psycopg2.extras cho driver postgresql
    fake = types.ModuleType("psycopg2")
    fake.extras = types.SimpleNamespace(RealDictCursor=object)
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake.extras)
    return r, cur


def test_jdbc_read_lookback_va_moc_theo_datetime(monkeypatch):
    cfg = _jdbc_cfg(cdc={"mode": "cdc1", "watermark_column": "updated_at",
                         "lookback_seconds": 600})
    rows = [{"id": 1, "updated_at": dt.datetime(2026, 9, 2, 1)},
            {"id": 2, "updated_at": dt.datetime(2026, 9, 2, 3)}]
    r, cur = _jdbc_reader_with_rows(monkeypatch, rows, cfg)

    (b,) = list(r.read(Checkpoint("t_job", {"default": "2026-09-01T00:10:00+00:00"})))
    assert cur.executed[1] == ["2026-09-01T00:00:00+00:00"]  # da tru lookback
    assert b.watermark == "2026-09-02T03:00:00"
    assert b.records[0]["updated_at"] == "2026-09-02T01:00:00"


def test_jdbc_moc_so_nguyen(monkeypatch):
    cfg = _jdbc_cfg(cdc={"mode": "cdc1", "watermark_column": "id", "watermark_type": "int"})
    r, _ = _jdbc_reader_with_rows(monkeypatch, [{"id": 9}, {"id": 10}, {"id": 2}], cfg)
    assert list(r.read(None))[0].watermark == "10"


def test_jdbc_chia_lo_theo_fetch_size(monkeypatch):
    import sourcing.readers.jdbc as jdbc_mod

    monkeypatch.setattr(jdbc_mod, "FETCH_SIZE", 2)
    r, _ = _jdbc_reader_with_rows(monkeypatch, [{"id": i, "updated_at": str(i)} for i in range(5)],
                                  _jdbc_cfg())
    assert [len(b) for b in r.read(None)] == [2, 2, 1]


def test_jdbc_mysql_dung_cursor_doc_dan(monkeypatch):
    """Cursor mac dinh cua PyMySQL tai CA bang vao RAM — trans Berka 1,06 trieu dong."""
    cfg = _jdbc_cfg(driver="mysql", cdc={"mode": "cdc1", "watermark_column": "trans_id",
                                         "watermark_type": "int"})
    r, cur = _jdbc_reader_with_rows(monkeypatch, [{"trans_id": 7, "amount": decimal.Decimal("1.5")}], cfg)
    sscursor = object()
    fake = types.ModuleType("pymysql")
    fake.cursors = types.SimpleNamespace(SSDictCursor=sscursor)
    monkeypatch.setitem(sys.modules, "pymysql", fake)
    monkeypatch.setitem(sys.modules, "pymysql.cursors", fake.cursors)

    (b,) = list(r.read(None))
    assert cur.opened == [((sscursor,), {})]
    assert b.watermark == "7" and b.records[0]["amount"] == "1.5"
    assert cur.executed[0].startswith("SELECT * FROM `public`.`instruments`")


def test_jdbc_driver_khong_ho_tro(aws):
    import json

    import boto3

    boto3.client("secretsmanager").create_secret(Name="fingraph/src/s", SecretString=json.dumps(
        {"host": "h", "username": "u", "password": "p", "dbname": "d"}))
    with pytest.raises(ValueError, match="driver khong ho tro: oracle"):
        JdbcReader(_jdbc_cfg(driver="oracle"))._connect()


# ---------- Kafka với consumer giả ----------


class _Msg:
    def __init__(self, partition, offset, value=b'{"p":1}', key=b"k"):
        self._p, self._o, self._v, self._k = partition, offset, value, key

    def error(self):
        return None

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def topic(self):
        return "trades"

    def timestamp(self):
        return (1, 1_700_000_000_000)

    def key(self):
        return self._k

    def value(self):
        return self._v


class _Consumer:
    def __init__(self, conf, partitions, rounds):
        self.conf = conf
        self.partitions = partitions
        self.rounds = list(rounds)
        self.assigned = None
        self.closed = False

    def list_topics(self, topic, timeout=None):
        t = types.SimpleNamespace(error=None, partitions={p: None for p in self.partitions})
        return types.SimpleNamespace(topics={"trades": t})

    def assign(self, tps):
        self.assigned = tps

    def consume(self, num_messages, timeout):
        return self.rounds.pop(0) if self.rounds else []

    def close(self):
        self.closed = True


@pytest.fixture
def fake_kafka(monkeypatch):
    mod = types.ModuleType("confluent_kafka")
    mod.OFFSET_BEGINNING, mod.OFFSET_END = -2, -1
    mod.TopicPartition = lambda topic, p, off: (topic, p, off)
    mod.KafkaError = types.SimpleNamespace(_PARTITION_EOF=-191)
    holder = {}

    def _consumer(conf):
        c = _Consumer(conf, holder["partitions"], holder["rounds"])
        holder["c"] = c
        return c

    mod.Consumer = _consumer
    monkeypatch.setitem(sys.modules, "confluent_kafka", mod)
    return holder


def _kafka_cfg(**kafka):
    return make_cfg(
        source_type="kafka",
        kafka={"bootstrap_servers": "b:9092", "topic": "trades", "group_id": "g", **kafka},
        cdc={"mode": "cdc1"},
    )


def test_kafka_tua_toi_offset_da_luu_cong_1(fake_kafka):
    from sourcing.readers.kafka import KafkaReader

    fake_kafka.update(partitions=[0, 1], rounds=[])
    r = KafkaReader(_kafka_cfg())
    list(r.read(Checkpoint("t_job", {"0": "41"})))
    c = fake_kafka["c"]
    assert c.assigned == [("trades", 0, 42), ("trades", 1, -2)]
    assert c.conf["enable.auto.commit"] is False


def test_kafka_latest_khi_chua_co_moc(fake_kafka):
    from sourcing.readers.kafka import KafkaReader

    fake_kafka.update(partitions=[0], rounds=[])
    list(KafkaReader(_kafka_cfg(auto_offset_reset="latest")).read(None))
    assert fake_kafka["c"].assigned == [("trades", 0, -1)]


def test_kafka_moi_partition_mot_lo_kem_offset_cuoi(fake_kafka):
    from sourcing.readers.kafka import KafkaReader

    fake_kafka.update(partitions=[0, 1], rounds=[
        [_Msg(0, 5), _Msg(1, 7), _Msg(0, 6)],
        [_Msg(1, 8, value=None, key=None)],
    ])
    r = KafkaReader(_kafka_cfg())
    batches = {b.partition: b for b in r.read(None)}
    assert batches["0"].watermark == "6" and len(batches["0"]) == 2
    assert batches["1"].watermark == "8"
    rec = batches["1"].records[-1]
    assert rec["value"] is None and rec["_kafka_key"] is None and rec["_kafka_offset"] == 8
    r.close()
    assert fake_kafka["c"].closed


def test_kafka_dung_o_max_records_per_run(fake_kafka):
    from sourcing.readers.kafka import KafkaReader

    fake_kafka.update(partitions=[0], rounds=[[_Msg(0, i) for i in range(3)]] * 5)
    batches = list(KafkaReader(_kafka_cfg(max_records_per_run=3)).read(None))
    assert sum(len(b) for b in batches) == 3


def test_kafka_topic_khong_ton_tai(fake_kafka):
    from sourcing.readers.kafka import KafkaReader

    fake_kafka.update(partitions=[0], rounds=[])
    with pytest.raises(RuntimeError, match="khong thay topic 'khac'"):
        list(KafkaReader(_kafka_cfg(topic="khac")).read(None))
