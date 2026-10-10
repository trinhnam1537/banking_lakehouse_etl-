"""Lambda fan-out (sourcing/lambda_dispatcher.py)."""

from __future__ import annotations

import datetime as dt
import json

import boto3
import pytest

from sourcing import lambda_dispatcher as d

UTC = dt.timezone.utc


def at(y, m, day, h=0, mi=0):
    return dt.datetime(y, m, day, h, mi, tzinfo=UTC)


def job(expr, last=None, enabled=True):
    j = {"job_name": "j", "schedule_enabled": enabled, "schedule_expression": expr}
    if last is not None:
        j["last_dispatched_at"] = last.isoformat() if isinstance(last, dt.datetime) else last
    return j


# ---------- parse_interval_minutes ----------


@pytest.mark.parametrize("expr, minutes", [
    ("rate(5 minutes)", 5),
    ("rate(1 minute)", 1),
    ("rate(1 hour)", 60),
    ("rate(6 hours)", 360),
    ("rate(1 day)", 1440),
    ("cron(0 19 * * ? *)", 1440),
    ("cron(30 19 * * ? *)", 1440),
    ("cron(0 10 ? * MON-FRI *)", 1440),
    ("cron(0 19 ? * SUN *)", 10080),
    ("cron(0 23 1 * ? *)", 43200),
])
def test_parse_interval(expr, minutes):
    assert d.parse_interval_minutes(expr) == minutes


@pytest.mark.parametrize("expr", [
    "rate(5 seconds)", "", "cron(0/15 * * * ? *)", "cron(0 10 L * ? *)", "at(2026-01-01T00:00:00)",
])
def test_parse_interval_khong_ho_tro(expr):
    assert d.parse_interval_minutes(expr) is None


# ---------- is_due: rate ----------


def test_schedule_tat():
    assert d.is_due(job("rate(5 minutes)", enabled=False), at(2026, 9, 1)) == (False, "schedule.enabled=false")


def test_lich_khong_ho_tro_khong_chay():
    due, why = d.is_due(job("cron(0/15 * * * ? *)"), at(2026, 9, 1))
    assert not due and "khong ho tro" in why


def test_chua_tung_chay():
    assert d.is_due(job("rate(1 hour)"), at(2026, 9, 1))[0]


def test_rate_du_han_co_dung_sai_30_giay():
    now = at(2026, 9, 1, 12, 0)
    assert d.is_due(job("rate(5 minutes)", now - dt.timedelta(minutes=4, seconds=31)), now)[0]
    assert not d.is_due(job("rate(5 minutes)", now - dt.timedelta(minutes=4)), now)[0]


def test_last_dispatched_hong_thi_chay_lai():
    assert d.is_due(job("rate(1 hour)", "khong-phai-ngay"), at(2026, 9, 1)) == (
        True, "last_dispatched_at hong -> chay lai")


def test_last_dispatched_khong_timezone_coi_la_utc():
    assert not d.is_due(job("rate(1 hour)", "2026-09-01T11:30:00"), at(2026, 9, 1, 12))[0]


def test_doc_schedule_trong_config_khi_thieu_truong_cap_cao():
    j = {"job_name": "j", "config": {"schedule": {"enabled": True, "expression": "rate(5 minutes)"}}}
    assert d.is_due(j, at(2026, 9, 1))[0]


# ---------- is_due: cron hằng ngày ----------


DAILY_19 = "cron(0 19 * * ? *)"


def test_daily_chua_toi_gio():
    due, why = d.is_due(job(DAILY_19, at(2026, 9, 1, 19)), at(2026, 9, 2, 18, 55))
    assert not due and "chua den gio 19:00" in why


def test_daily_qua_gio_ngay_moi():
    assert d.is_due(job(DAILY_19, at(2026, 9, 1, 19)), at(2026, 9, 2, 19, 5))[0]


def test_daily_da_chay_hom_nay():
    assert not d.is_due(job(DAILY_19, at(2026, 9, 2, 19, 1)), at(2026, 9, 2, 22))[0]


def test_daily_vua_chay_duoi_mot_gio():
    due, why = d.is_due(job(DAILY_19, at(2026, 9, 1, 23, 50)), at(2026, 9, 2, 0, 10))
    assert not due and "moi chay" in why


# ---------- is_due: cron theo thứ / ngày trong tháng ----------
# 2026-09-06 la Chu nhat, 2026-09-07 la thu Hai.


def test_ngay_thuong_khong_chay_cuoi_tuan():
    expr = "cron(0 10 ? * MON-FRI *)"
    assert not d.is_due(job(expr, at(2026, 9, 4, 10)), at(2026, 9, 5, 11))[0]  # thu Bay
    assert not d.is_due(job(expr, at(2026, 9, 4, 10)), at(2026, 9, 6, 11))[0]  # Chu nhat
    assert d.is_due(job(expr, at(2026, 9, 4, 10)), at(2026, 9, 7, 10, 5))[0]   # thu Hai


def test_hang_tuan_chi_chay_chu_nhat():
    expr = "cron(0 19 ? * SUN *)"
    last = at(2026, 8, 30, 19)  # Chu nhat tuan truoc
    for day in range(1, 6):     # thu Ba .. thu Bay
        assert not d.is_due(job(expr, last), at(2026, 9, day, 20))[0], day
    assert d.is_due(job(expr, last), at(2026, 9, 6, 19, 5))[0]


def test_hang_thang_chi_chay_ngay_1():
    expr = "cron(0 23 1 * ? *)"
    last = at(2026, 9, 1, 23)
    assert not d.is_due(job(expr, last), at(2026, 9, 15, 23, 30))[0]
    assert not d.is_due(job(expr, last), at(2026, 9, 30, 23, 30))[0]
    assert d.is_due(job(expr, last), at(2026, 10, 1, 23, 5))[0]


def test_danh_sach_thu():
    expr = "cron(0 8 ? * MON,WED *)"
    last = at(2026, 9, 7, 8)  # thu Hai
    assert not d.is_due(job(expr, last), at(2026, 9, 8, 9))[0]  # thu Ba
    assert d.is_due(job(expr, last), at(2026, 9, 9, 9))[0]      # thu Tu


# ---------- handler ----------


@pytest.fixture
def run_queue(aws):
    sqs = boto3.client("sqs")
    url = sqs.create_queue(QueueName="test-src-run.fifo",
                           Attributes={"FifoQueue": "true"})["QueueUrl"]
    return url


@pytest.fixture
def env(config_table, run_queue, monkeypatch):
    monkeypatch.setenv("JOB_CONFIG_TABLE", config_table)
    monkeypatch.setenv("RUN_QUEUE_URL", run_queue)
    monkeypatch.delenv("FORCE_ALL", raising=False)
    return config_table, run_queue


def _seed(table, name, expr="rate(5 minutes)", enabled=True, **extra):
    boto3.resource("dynamodb").Table(table).put_item(Item={
        "job_name": name, "enabled": enabled, "schedule_enabled": True,
        "schedule_expression": expr, "config": json.dumps({"name": name}), **extra,
    })


def _messages(url):
    sqs = boto3.client("sqs")
    out = []
    while True:
        resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10)
        msgs = resp.get("Messages", [])
        if not msgs:
            return out
        for m in msgs:
            out.append(json.loads(m["Body"]))
            sqs.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"])


def test_handler_gui_job_den_han_va_danh_dau(env):
    table, url = env
    _seed(table, "den_han")
    _seed(table, "chua_den_han", last_dispatched_at=dt.datetime.now(UTC).isoformat())
    _seed(table, "tat", enabled=False)

    assert d.handler({}, None) == {"dispatched": 1, "skipped": 1}

    (msg,) = _messages(url)
    assert msg["job_name"] == "den_han" and len(msg["run_id"]) == 16
    from sourcing.state import JobConfigStore
    item = JobConfigStore(table).get("den_han")
    assert item["last_dispatch_run_id"] == msg["run_id"]


def test_handler_chay_tay_theo_danh_sach(env):
    table, url = env
    for n in ("a", "b", "c"):
        _seed(table, n, last_dispatched_at=dt.datetime.now(UTC).isoformat())
    event = {"Records": [{"body": json.dumps({"jobs": ["a", "c"]})}, {"body": "hong"}]}

    assert d.handler(event, None)["dispatched"] == 2
    assert sorted(m["job_name"] for m in _messages(url)) == ["a", "c"]


def test_handler_force_all(env, monkeypatch):
    table, url = env
    _seed(table, "a", last_dispatched_at=dt.datetime.now(UTC).isoformat())
    monkeypatch.setenv("FORCE_ALL", "true")
    assert d.handler(None, None)["dispatched"] == 1
    assert _messages(url)[0]["reason"] == "FORCE_ALL"
