"""Checkpoint và nhật ký chạy trên DynamoDB (sourcing/state.py)."""

from __future__ import annotations

import json
import time
from decimal import Decimal

import boto3
import pytest

from sourcing.state import (
    CKPT_LATEST_SK,
    JobConfigStore,
    JobLogStore,
    RunStats,
    _clean,
    _plain,
)


def _ticking_clock(monkeypatch):
    """_iso() tang 1 phut moi lan goi, de thu tu run_key xac dinh duoc."""
    import datetime as dt
    import itertools

    import sourcing.state as st

    base = dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)
    counter = itertools.count()
    monkeypatch.setattr(st, "_iso", lambda ts=None: (
        ts or base + dt.timedelta(minutes=next(counter))).isoformat(timespec="seconds"))
    monkeypatch.setattr(st, "_now", lambda: base + dt.timedelta(days=1))


def _items(table: str, job: str):
    t = boto3.resource("dynamodb").Table(table)
    return t.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("job_name").eq(job)
    )["Items"]


# ---------- chuẩn hoá kiểu ----------


def test_clean_float_thanh_decimal_va_bo_none():
    out = _clean({"a": 1.5, "b": None, "c": [2.25, {"d": None, "e": "x"}]})
    assert out == {"a": Decimal("1.5"), "c": [Decimal("2.25"), {"e": "x"}]}


def test_plain_decimal_ve_int_hoac_float():
    assert _plain({"a": Decimal("3"), "b": [Decimal("1.5")]}) == {"a": 3, "b": [1.5]}
    assert isinstance(_plain(Decimal("3")), int)


# ---------- checkpoint ----------


def test_checkpoint_rong_khi_chua_chay(joblog_table):
    ck = JobLogStore(joblog_table).read_checkpoint("job")
    assert ck.is_empty and ck.get() is None and ck.job_name == "job"


def test_advance_roi_doc_lai(joblog_table):
    log = JobLogStore(joblog_table)
    log.advance_checkpoint("job", "r1", {"default": "2026-09-01T00:00:00+00:00"}, "SUCCESS")
    ck = log.read_checkpoint("job")
    assert ck.get() == "2026-09-01T00:00:00+00:00"
    assert ck.run_id == "r1" and ck.source_status == "SUCCESS" and ck.updated_at


def test_advance_merge_khong_xoa_moc_partition_khac(joblog_table):
    """Kafka tiến từng partition — ghi đè cả map sẽ làm mất mốc partition khác."""
    log = JobLogStore(joblog_table)
    log.advance_checkpoint("k", "r1", {"0": "100", "1": "200"}, "PARTIAL")
    log.advance_checkpoint("k", "r2", {"1": "250"}, "SUCCESS")
    assert log.read_checkpoint("k").watermarks == {"0": "100", "1": "250"}


def test_advance_ep_gia_tri_thanh_chuoi(joblog_table):
    log = JobLogStore(joblog_table)
    log.advance_checkpoint("k", "r1", {"0": 42}, "SUCCESS")
    assert log.read_checkpoint("k").get("0") == "42"


@pytest.mark.parametrize("status", ["FAILED", "RUNNING", "NO_DATA", ""])
def test_advance_tu_choi_trang_thai_khac(joblog_table, status):
    with pytest.raises(ValueError, match="PARTIAL/SUCCESS"):
        JobLogStore(joblog_table).advance_checkpoint("j", "r", {"default": "1"}, status)


def test_ckpt_latest_khong_co_ttl_lich_su_thi_co(joblog_table):
    """TTL xoá CKPT#LATEST là job CDC1 kéo lại từ đầu — lỗi im lặng và đắt."""
    log = JobLogStore(joblog_table)
    log.advance_checkpoint("j", "r1", {"default": "1"}, "SUCCESS")
    items = {i["run_key"]: i for i in _items(joblog_table, "j")}

    assert "expires_at" not in items[CKPT_LATEST_SK]
    history = [v for k, v in items.items() if k.startswith("CKPT#") and k != CKPT_LATEST_SK]
    assert len(history) == 1
    assert history[0]["expires_at"] > time.time() + 80 * 86400


def test_reset_checkpoint(joblog_table):
    log = JobLogStore(joblog_table)
    log.advance_checkpoint("j", "r1", {"default": "1"}, "SUCCESS")
    log.reset_checkpoint("j")
    assert log.read_checkpoint("j").is_empty


# ---------- vòng đời lần chạy ----------


def test_start_roi_finish_run(joblog_table, monkeypatch):
    monkeypatch.setenv("GLUE_RUN_ID", "jr_abc")
    log = JobLogStore(joblog_table)
    run_key = log.start_run("j", "r1", {"cdc_mode": "cdc5", "ratio": 0.5})
    assert run_key.startswith("RUN#") and run_key.endswith("#r1")

    (running,) = log.recent_runs("j")
    assert running["status"] == "RUNNING"
    assert running["glue_job_run_id"] == "jr_abc"
    assert running["config"] == {"cdc_mode": "cdc5", "ratio": 0.5}
    assert running["expires_at"] > time.time()

    stats = RunStats(records_read=10, records_written=10, bytes_written=123, files_written=1, batches=2)
    log.finish_run("j", run_key, "SUCCESS", stats, watermarks={"default": "5"})

    (done,) = log.recent_runs("j")
    assert done["status"] == "SUCCESS"
    assert done["records_read"] == 10 and done["bytes_written"] == 123
    assert done["watermarks"] == {"default": "5"}
    assert done["duration_seconds"] >= 0
    assert done["err"] == ""


def test_finish_run_cat_loi_dai(joblog_table):
    log = JobLogStore(joblog_table)
    rk = log.start_run("j", "r1", {})
    log.finish_run("j", rk, "FAILED", RunStats(), error="x" * 10_000)
    assert len(log.recent_runs("j")[0]["err"]) == 4000


def test_recent_runs_moi_nhat_truoc_va_bo_ban_ghi_ckpt(joblog_table, monkeypatch):
    _ticking_clock(monkeypatch)
    log = JobLogStore(joblog_table)
    log.start_run("j", "a", {})
    log.start_run("j", "b", {})
    log.advance_checkpoint("j", "b", {"default": "1"}, "SUCCESS")

    runs = log.recent_runs("j")
    assert [r["run_id"] for r in runs] == ["b", "a"]


def test_last_success_bo_qua_lan_hong(joblog_table, monkeypatch):
    _ticking_clock(monkeypatch)
    log = JobLogStore(joblog_table)

    ok = log.start_run("j", "ok", {})
    log.finish_run("j", ok, "SUCCESS", RunStats())
    bad = log.start_run("j", "bad", {})
    log.finish_run("j", bad, "FAILED", RunStats())

    assert log.last_success("j")["run_id"] == "ok"
    assert JobLogStore(joblog_table).last_success("khong_co") is None


# ---------- bảng job_config ----------


def _seed_like_terraform(table: str, name: str, cfg: dict, **top):
    """Ghi item đúng hình dạng terraform/modules/sourcing/dynamodb.tf seed."""
    item = {
        "job_name": name,
        "enabled": True,
        "schedule_enabled": cfg.get("schedule", {}).get("enabled", False),
        "schedule_expression": cfg.get("schedule", {}).get("expression", ""),
        "config": json.dumps(cfg),
        **top,
    }
    boto3.resource("dynamodb").Table(table).put_item(Item=item)


def test_job_config_get_boc_chuoi_json(config_table):
    _seed_like_terraform(config_table, "j", {"source_type": "api", "n": 1.5})
    item = JobConfigStore(config_table).get("j")
    assert item["config"] == {"source_type": "api", "n": 1.5}
    assert JobConfigStore(config_table).get("khong_co") is None


def test_job_config_json_hong_thanh_dict_rong(config_table):
    boto3.resource("dynamodb").Table(config_table).put_item(
        Item={"job_name": "j", "enabled": True, "config": "{hong"}
    )
    assert JobConfigStore(config_table).get("j")["config"] == {}


def test_list_enabled_loc_job_tat(config_table):
    _seed_like_terraform(config_table, "bat", {})
    _seed_like_terraform(config_table, "tat", {}, enabled=False)
    names = [j["job_name"] for j in JobConfigStore(config_table).list_enabled()]
    assert names == ["bat"]


def test_mark_dispatched(config_table):
    _seed_like_terraform(config_table, "j", {})
    store = JobConfigStore(config_table)
    store.mark_dispatched("j", "run123")
    item = store.get("j")
    assert item["last_dispatch_run_id"] == "run123"
    assert item["last_dispatched_at"].startswith("20")
