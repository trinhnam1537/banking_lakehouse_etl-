"""Chỗ framework sourcing bắt tay với orchestrator: override, snapshot, ORCH#."""

from __future__ import annotations

import boto3
import pytest

import sourcing.runner as runner_mod
from sourcing import main as main_mod
from sourcing.config import (
    ConfigError,
    apply_overrides,
    decode_snapshot,
    encode_snapshot,
)
from sourcing.readers.base import Batch
from sourcing.runner import SourceRunner
from sourcing.state import JobLogStore

from .conftest import entry, make_cfg


class _OneBatch:
    def read(self, checkpoint):
        yield Batch(records=[{"a": 1}])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


# ---------- apply_overrides ----------


def test_override_duong_dan_cham_va_khong_sua_ban_goc():
    raw = entry()
    out = apply_overrides(raw, {"api.max_pages": 1, "target.dataset": "x/y"})
    assert out["api"]["max_pages"] == 1 and out["target"]["dataset"] == "x/y"
    assert "max_pages" not in raw["api"] and "target" not in raw


@pytest.mark.parametrize("key", ["name", "source_type", "enabled", "runtime.timeout_minutes",
                                 "schedule.expression", ""])
def test_override_khoa_cam(key):
    with pytest.raises(ConfigError):
        apply_overrides(entry(), {key: 1})


def test_snapshot_khu_hoi_va_ngan_hon_json():
    raw = entry(api={"query": {"q": "Việt Nam \"có ngoặc\"\n"}})
    enc = encode_snapshot(raw)
    assert decode_snapshot(enc) == raw
    assert set(enc) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=")


# ---------- main: tham số Glue do trigger truyền ----------


def test_main_doc_snapshot_va_dry_run_glue(raw_bucket, joblog_table, monkeypatch):
    seen = {}
    monkeypatch.setattr(runner_mod, "build_reader",
                        lambda cfg, **kw: seen.setdefault("cfg", cfg) and _OneBatch())
    snap = encode_snapshot(entry(name="snap_job", api={"max_pages": 3}))
    rc = main_mod.main(["--JOB_NAME", "snap_job", "--CONFIG_SNAPSHOT", snap,
                        "--DRY_RUN", "true", "--RAW_BUCKET", raw_bucket,
                        "--JOBLOG_TABLE", joblog_table, "--RUN_ID", "r1"])
    assert rc == 0 and seen["cfg"].name == "snap_job" and seen["cfg"].api.max_pages == 3
    # DRY_RUN=true -> khong ghi gi xuong S3
    assert "Contents" not in boto3.client("s3").list_objects_v2(Bucket=raw_bucket)


def test_main_snapshot_uu_tien_hon_bang_config(raw_bucket, joblog_table, monkeypatch):
    """Có snapshot thì KHÔNG đọc DynamoDB — bảng không tồn tại cũng không sao."""
    monkeypatch.setattr(runner_mod, "build_reader", lambda cfg, **kw: _OneBatch())
    rc = main_mod.main(["--JOB_NAME", "t_job", "--JOB_CONFIG_TABLE", "khong-ton-tai",
                        "--CONFIG_SNAPSHOT", encode_snapshot(entry()),
                        "--RAW_BUCKET", raw_bucket, "--JOBLOG_TABLE", joblog_table])
    assert rc == 0


# ---------- ORCH# ----------


def test_record_orch_result_khong_tao_item_mo_coi(joblog_table):
    log = JobLogStore(joblog_table)
    assert log.record_orch_result("j", "r1", "SUCCESS", "m.json") is False
    item = boto3.resource("dynamodb").Table(joblog_table).get_item(
        Key={"job_name": "j", "run_key": "ORCH#r1"}).get("Item")
    assert item is None


def test_runner_bao_manifest_ve_orch(raw_bucket, joblog_table, monkeypatch):
    table = boto3.resource("dynamodb").Table(joblog_table)
    table.put_item(Item={"job_name": "t_job", "run_key": "ORCH#r7", "orch_status": "STARTED"})
    monkeypatch.setattr(runner_mod, "build_reader", lambda cfg, **kw: _OneBatch())

    res = SourceRunner(make_cfg(), raw_bucket, joblog_table, run_id="r7").run()

    orch = table.get_item(Key={"job_name": "t_job", "run_key": "ORCH#r7"})["Item"]
    assert res.status == "SUCCESS" and orch["run_status"] == "SUCCESS"
    assert orch["manifest"].endswith("/r7.json") and orch["orch_status"] == "STARTED"


def test_runner_no_data_khong_co_manifest(raw_bucket, joblog_table, monkeypatch):
    table = boto3.resource("dynamodb").Table(joblog_table)
    table.put_item(Item={"job_name": "t_job", "run_key": "ORCH#r8"})

    class _Empty(_OneBatch):
        def read(self, checkpoint):
            return iter(())

    monkeypatch.setattr(runner_mod, "build_reader", lambda cfg, **kw: _Empty())
    SourceRunner(make_cfg(), raw_bucket, joblog_table, run_id="r8").run()
    orch = table.get_item(Key={"job_name": "t_job", "run_key": "ORCH#r8"})["Item"]
    assert orch["run_status"] == "NO_DATA" and "manifest" not in orch


def test_loi_ghi_orch_khong_lam_hong_lan_chay(raw_bucket, joblog_table, monkeypatch):
    monkeypatch.setattr(runner_mod, "build_reader", lambda cfg, **kw: _OneBatch())

    def boom(*a, **kw):
        raise RuntimeError("mat mang")

    monkeypatch.setattr(JobLogStore, "record_orch_result", boom)
    res = SourceRunner(make_cfg(), raw_bucket, joblog_table, run_id="r9").run()
    assert res.status == "SUCCESS"
