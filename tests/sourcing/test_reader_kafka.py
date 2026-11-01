"""Reader Kafka (JDBC: test_reader_jdbc.py).

confluent-kafka do profile Glue cài, test không cần: consumer được thay bằng
bản giả tối thiểu nạp qua `sys.modules`.
"""

from __future__ import annotations

import sys
import types

import pytest

from sourcing.state import Checkpoint

from .conftest import make_cfg


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
    def __init__(self, conf, partitions, rounds, high=None):
        self.conf = conf
        self.partitions = partitions
        self.rounds = list(rounds)
        # high-watermark tung partition; mac dinh rat lon = "chua toi cuoi"
        self.high = high or {}
        self.assigned = None
        self.closed = False

    def list_topics(self, topic, timeout=None):
        t = types.SimpleNamespace(error=None, partitions={p: None for p in self.partitions})
        return types.SimpleNamespace(topics={"trades": t})

    def get_watermark_offsets(self, tp, timeout=None):
        return 0, self.high.get(tp[1], 10**9)

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
        c = _Consumer(conf, holder["partitions"], holder["rounds"], holder.get("high"))
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


def test_kafka_dung_o_high_watermark_chup_luc_bat_dau(fake_kafka):
    """Producer chay lien tuc: consume khong bao gio rong. Lan chay phai dung o moc
    chup luc bat dau (su co 10-31: consumer chay mai, khong ghi gi xuong S3)."""
    from sourcing.readers.kafka import KafkaReader

    # high = 3 (offset 0..2 co san luc bat dau); producer day tiep offset 3, 4... vo han
    fake_kafka.update(partitions=[0], high={0: 3},
                      rounds=[[_Msg(0, 0), _Msg(0, 1)], [_Msg(0, 2), _Msg(0, 3)]] + [[_Msg(0, 9)]] * 50)
    batches = list(KafkaReader(_kafka_cfg()).read(None))
    assert [r["_kafka_offset"] for b in batches for r in b.records] == [0, 1, 2]
    assert batches[-1].watermark == "2"


def test_kafka_khong_co_tin_moi_thi_khong_consume(fake_kafka):
    from sourcing.readers.kafka import KafkaReader

    fake_kafka.update(partitions=[0, 1], high={0: 42, 1: 0}, rounds=[[_Msg(0, 99)]])
    batches = list(KafkaReader(_kafka_cfg()).read(Checkpoint("t_job", {"0": "41"})))
    assert batches == []
