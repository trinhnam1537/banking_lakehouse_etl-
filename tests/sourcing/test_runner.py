"""Luồng source → raw (sourcing/runner.py).

Reader thật được thay bằng `FakeReader` để điều khiển chính xác lô nào đến,
lô nào kèm mốc gì, và job chết ở đâu. S3 và DynamoDB là moto.

Trọng tâm là hai bất biến ghi ở đầu runner.py:
  1. ghi dữ liệu trước, tiến mốc sau
  2. mốc chỉ tiến với PARTIAL/SUCCESS — lần chạy FAILED không đẩy mốc
"""

from __future__ import annotations

import json

import boto3
import pytest

import sourcing.runner as runner_mod
from sourcing.readers.base import Batch
from sourcing.runner import SourceRunner
from sourcing.state import JobLogStore

from .conftest import make_cfg

CDC1 = {"mode": "cdc1", "watermark_column": "ts"}


class Boom(RuntimeError):
    pass


class FakeReader:
    def __init__(self, batches, fail_after=None):
        self.batches = batches
        self.fail_after = fail_after
        self.seen_checkpoint = "chua_goi"
        self.closed = False

    def read(self, checkpoint):
        self.seen_checkpoint = checkpoint
        for i, b in enumerate(self.batches):
            if self.fail_after is not None and i == self.fail_after:
                raise Boom("nguon chet giua chung")
            yield b

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True
        return False


@pytest.fixture
def use_reader(monkeypatch):
    holder = {}

    def _install(batches, fail_after=None):
        reader = FakeReader(batches, fail_after)
        holder["r"] = reader
        monkeypatch.setattr(runner_mod, "build_reader", lambda cfg, **kw: reader)
        return reader

    return _install


def _batch(n, wm=None, partition="default"):
    return Batch(records=[{"i": i} for i in range(n)], watermark=wm, partition=partition)


def _all_keys(bucket):
    return [o["Key"] for o in boto3.client("s3").list_objects_v2(Bucket=bucket).get("Contents", [])]


def _s3_keys(bucket):
    """Chỉ file dữ liệu — manifest có test riêng bên dưới."""
    return [k for k in _all_keys(bucket) if "/_manifests/" not in k]


def _manifests(bucket):
    s3 = boto3.client("s3")
    return [json.loads(s3.get_object(Bucket=bucket, Key=k)["Body"].read())
            for k in _all_keys(bucket) if "/_manifests/" in k]


def _run(cfg, raw_bucket, joblog_table, **kw):
    return SourceRunner(cfg, raw_bucket, joblog_table, run_id="run1", **kw).run()


# ---------- thành công ----------


def test_cdc1_thanh_cong_ghi_s3_va_tien_moc(use_reader, raw_bucket, joblog_table):
    use_reader([_batch(3, "t1"), _batch(2, "t2")])
    res = _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table)

    assert res.ok and res.status == "SUCCESS"
    assert (res.stats.records_read, res.stats.records_written, res.stats.batches) == (5, 5, 2)
    assert res.stats.files_written == 2 and len(_s3_keys(raw_bucket)) == 2
    assert res.watermarks == {"default": "t2"}

    # Manifest: trigger cua silver, liet ke DUNG cac file cua lan chay nay
    (m,) = _manifests(raw_bucket)
    assert m["run_id"] == "run1" and m["status"] == "SUCCESS" and m["records"] == 5
    assert sorted(m["files"]) == sorted(_s3_keys(raw_bucket))
    assert m["raw_mode"] == "records" and m["dataset"] == "t_job"

    log = JobLogStore(joblog_table)
    ck = log.read_checkpoint("t_job")
    assert ck.get() == "t2" and ck.source_status == "SUCCESS" and ck.run_id == "run1"
    (run,) = log.recent_runs("t_job")
    assert run["status"] == "SUCCESS" and run["records_written"] == 5
    assert run["config"]["cdc_mode"] == "cdc1" and run["config"]["watermark_column"] == "ts"


def test_cdc1_truyen_checkpoint_cu_cho_reader(use_reader, raw_bucket, joblog_table):
    JobLogStore(joblog_table).advance_checkpoint("t_job", "r0", {"default": "t0"}, "SUCCESS")
    reader = use_reader([])
    _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table)
    assert reader.seen_checkpoint.get() == "t0"


def test_cdc5_khong_doc_checkpoint_khong_tien_moc(use_reader, raw_bucket, joblog_table):
    JobLogStore(joblog_table).advance_checkpoint("t_job", "r0", {"default": "cu"}, "SUCCESS")
    reader = use_reader([_batch(2, wm="khong_duoc_dung")])
    res = _run(make_cfg(), raw_bucket, joblog_table)

    assert reader.seen_checkpoint.is_empty
    assert res.status == "SUCCESS"
    assert JobLogStore(joblog_table).read_checkpoint("t_job").get() == "cu"


def test_khong_co_du_lieu_la_no_data(use_reader, raw_bucket, joblog_table):
    use_reader([])
    res = _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table)
    assert res.status == "NO_DATA" and res.ok
    assert _s3_keys(raw_bucket) == []
    assert JobLogStore(joblog_table).recent_runs("t_job")[0]["status"] == "NO_DATA"


def test_reader_duoc_dong(use_reader, raw_bucket, joblog_table):
    reader = use_reader([_batch(1)])
    _run(make_cfg(), raw_bucket, joblog_table)
    assert reader.closed


def test_kafka_moc_rieng_tung_partition(use_reader, raw_bucket, joblog_table):
    use_reader([_batch(1, "10", "0"), _batch(1, "7", "1"), _batch(1, "12", "0")])
    res = _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table)
    assert res.watermarks == {"0": "12", "1": "7"}


# ---------- dry run ----------


def test_dry_run_khong_ghi_s3_khong_tien_moc(use_reader, raw_bucket, joblog_table):
    use_reader([_batch(4, "t1")])
    res = _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table, dry_run=True)
    assert res.stats.records_read == 4 and res.stats.records_written == 0
    assert _s3_keys(raw_bucket) == []
    assert JobLogStore(joblog_table).read_checkpoint("t_job").is_empty


# ---------- PARTIAL ----------


def test_partial_sau_moi_n_lo(use_reader, raw_bucket, joblog_table, monkeypatch):
    monkeypatch.setattr(runner_mod, "PARTIAL_EVERY_N_BATCHES", 2)
    use_reader([_batch(1, f"t{i}") for i in range(1, 6)])
    _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table)

    t = boto3.resource("dynamodb").Table(joblog_table)
    hist = [i for i in t.scan()["Items"]
            if i["run_key"].startswith("CKPT#") and i["run_key"] != "CKPT#LATEST"]
    statuses = sorted((h["watermarks"]["default"], h["source_status"]) for h in hist)
    # lo 2 va 4 -> PARTIAL, lo 5 (cuoi) -> SUCCESS
    assert statuses == [("t2", "PARTIAL"), ("t4", "PARTIAL"), ("t5", "SUCCESS")]


# ---------- thất bại ----------


def test_that_bai_khong_tien_moc_phan_doc_do(use_reader, raw_bucket, joblog_table):
    JobLogStore(joblog_table).advance_checkpoint("t_job", "r0", {"default": "t0"}, "SUCCESS")
    use_reader([_batch(2, "t1"), _batch(2, "t2")], fail_after=1)

    with pytest.raises(Boom):
        _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table)

    log = JobLogStore(joblog_table)
    # Lo t1 DA nam tren S3 nhung moc van o t0: at-least-once, lan sau doc lai.
    assert log.read_checkpoint("t_job").get() == "t0"
    assert len(_s3_keys(raw_bucket)) == 1
    (run,) = log.recent_runs("t_job")
    assert run["status"] == "FAILED"
    assert "Boom: nguon chet giua chung" in run["err"]
    assert run["records_written"] == 2
    assert run["watermarks"] == {"default": "t0"}


def test_that_bai_giu_moc_partial_da_ghi(use_reader, raw_bucket, joblog_table, monkeypatch):
    """Backlog lớn hơn timeout vẫn nhích lên qua từng lần chạy nhờ PARTIAL."""
    monkeypatch.setattr(runner_mod, "PARTIAL_EVERY_N_BATCHES", 2)
    use_reader([_batch(1, "t1"), _batch(1, "t2"), _batch(1, "t3")], fail_after=2)

    with pytest.raises(Boom):
        _run(make_cfg(cdc=CDC1), raw_bucket, joblog_table)

    ck = JobLogStore(joblog_table).read_checkpoint("t_job")
    assert ck.get() == "t2" and ck.source_status == "PARTIAL"


def test_loi_ghi_s3_khong_tien_moc(use_reader, joblog_table, aws):
    """Bucket không tồn tại: ghi hỏng thì mốc tuyệt đối không được tiến."""
    use_reader([_batch(1, "t1")])
    with pytest.raises(Exception):
        _run(make_cfg(cdc=CDC1), "bucket-khong-ton-tai", joblog_table)
    log = JobLogStore(joblog_table)
    assert log.read_checkpoint("t_job").is_empty
    assert log.recent_runs("t_job")[0]["status"] == "FAILED"


# ---------- manifest ----------


def test_no_data_khong_ghi_manifest(use_reader, raw_bucket, joblog_table):
    use_reader([])
    _run(make_cfg(), raw_bucket, joblog_table)
    assert _manifests(raw_bucket) == []


def test_dry_run_khong_ghi_manifest(use_reader, raw_bucket, joblog_table):
    use_reader([_batch(2)])
    _run(make_cfg(), raw_bucket, joblog_table, dry_run=True)
    assert _all_keys(raw_bucket) == []


def test_that_bai_khong_ghi_manifest(use_reader, raw_bucket, joblog_table):
    """Silver khong duoc thay mot lan chay do dang."""
    use_reader([_batch(2), _batch(2)], fail_after=1)
    with pytest.raises(Exception):
        _run(make_cfg(), raw_bucket, joblog_table)
    assert _manifests(raw_bucket) == [] and len(_s3_keys(raw_bucket)) == 1
