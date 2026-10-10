"""Glue job dùng chung theo profile + Lambda schedule-sync."""

from __future__ import annotations

import json

import pytest
from botocore.exceptions import ClientError

from orchestrator import schedule_sync as S
from orchestrator.job_types import SilverType, SourcingType
from orchestrator.trigger import Trigger

from .conftest import RUNNER_ARN, Env

SHA = "0ea3cd86598539d3addd3754ce8c877d65b102fa"


class _FakeSsm:
    def __init__(self, value=SHA):
        self.value, self.calls = value, 0

    def get_parameter(self, Name):
        self.calls += 1
        if self.value is None:
            raise ClientError({"Error": {"Code": "ParameterNotFound", "Message": ""}}, "GetParameter")
        return {"Parameter": {"Value": self.value}}


def _profile_trigger(env: Env, ssm=None, now=1_000_000.0):
    jobs = {"src-shell": "fingraph-dev-src-shell"}
    return Trigger(
        {"src": SourcingType(Env.SRC_CFG, Env.SRC_LOG, "t-src-", dynamodb=env.ddb, profile_jobs=jobs),
         "slv": SilverType(Env.SLV_CFG, Env.SLV_LOG, "t-slv-", dynamodb=env.ddb,
                           profile_jobs={"slv-spark": "fingraph-dev-slv-spark"})},
        RUNNER_ARN, sfn=env.sfn, sleep=lambda s: None, release_param="/fingraph/dev/etl/release",
        artifact_bucket="lake", ssm=ssm or _FakeSsm(), clock=lambda: now)


# ---------- chọn Glue job ----------


def test_profile_co_trong_ha_tang_thi_dung_job_chung(env: Env):
    env.seed_src("a", runtime={"timeout_minutes": 20})
    res = _profile_trigger(env).handle({"job_id": "src.a"})
    inp = env.sfn.started[0]["input"]
    assert res["status"] == "STARTED" and res["profile"] == "src-shell" and res["etl_release"] == SHA
    assert inp["glue_job"] == "fingraph-dev-src-shell"
    assert inp["glue_args"]["--JOB_NAME"] == "a" and inp["glue_args"]["--ETL_RELEASE"] == SHA
    assert inp["timeout_minutes"] == 20
    # lease = now + timeout + 10 phut du, TTL them 1 ngay
    assert inp["lease_now"] == "1000000" and inp["lease_until"] == str(1_000_000 + 20 * 60 + 600)
    assert int(inp["lease_expires"]) == int(inp["lease_until"]) + 86400


def test_chua_co_profile_thi_roi_ve_job_rieng(env: Env):
    """Trong luc chuyen doi: ha tang chua khai profile_jobs -> van chay job theo entry."""
    env.seed_src("a")
    env.trigger.handle({"job_id": "src.a"})
    inp = env.sfn.started[0]["input"]
    assert inp["glue_job"] == "t-src-a" and "--ETL_RELEASE" not in inp["glue_args"]


def test_chua_deploy_release_van_chay_co_ghi_chu(env: Env):
    env.seed_src("a")
    res = _profile_trigger(env, ssm=_FakeSsm(value=None)).handle({"job_id": "src.a"})
    assert res["status"] == "STARTED" and res["etl_release"] is None
    assert any("ETL_RELEASE" in n for n in res["notes"])
    assert "--ETL_RELEASE" not in env.sfn.started[0]["input"]["glue_args"]


def test_release_cache_khong_doc_ssm_moi_lan(env: Env):
    env.seed_src("a")
    env.seed_src("b")
    ssm = _FakeSsm()
    t = _profile_trigger(env, ssm=ssm)
    t.handle({"job_ids": ["a", "b"]})
    assert ssm.calls == 1


def test_validate_hien_profile_va_release(env: Env):
    env.seed_src("a")
    res = _profile_trigger(env).handle({"job_id": "a", "action": "validate"})
    assert res["glue_job"] == "fingraph-dev-src-shell" and res["profile"] == "src-shell"
    assert res["glue_args"]["--ETL_RELEASE"] == SHA and env.sfn.started == []


def test_silver_spark_kem_extra_py_files(env: Env):
    name = env.seed_slv_from_repo()
    _profile_trigger(env).handle({"job_id": f"slv.{name}", "data_date": "2026-10-01"})
    inp = env.sfn.started[0]["input"]
    assert inp["glue_job"] == "fingraph-dev-slv-spark"
    assert inp["glue_args"]["--extra-py-files"] == f"s3://lake/_ops/etl/releases/{SHA}/etl.zip"
    assert inp["timeout_minutes"] == 60


def test_orch_ghi_release_va_profile(env: Env):
    env.seed_src("a")
    res = _profile_trigger(env).handle({"job_id": "a", "run_id": "r1"})
    orch = env.item(Env.SRC_LOG, "a", "ORCH#r1")
    assert orch["etl_release"] == SHA and orch["profile"] == "src-shell" and res["run_id"] == "r1"


# ---------- schedule-sync ----------


class _FakeScheduler:
    def __init__(self, existing=()):
        self.schedules = {n: {} for n in existing}
        self.calls = []

    def _nf(self, op):
        return ClientError({"Error": {"Code": "ResourceNotFoundException", "Message": ""}}, op)

    def update_schedule(self, **kw):
        self.calls.append(("update", kw["Name"]))
        if kw["Name"] not in self.schedules:
            raise self._nf("UpdateSchedule")
        self.schedules[kw["Name"]] = kw

    def create_schedule(self, **kw):
        self.calls.append(("create", kw["Name"]))
        self.schedules[kw["Name"]] = kw

    def delete_schedule(self, Name, GroupName):
        self.calls.append(("delete", Name))
        if Name not in self.schedules:
            raise self._nf("DeleteSchedule")
        del self.schedules[Name]

    def list_schedules(self, GroupName, NamePrefix, NextToken=None):
        return {"Schedules": [{"Name": n} for n in self.schedules if n.startswith(NamePrefix)]}


_REAL_SYNCER = S.ScheduleSyncer  # giu ban goc: test handler thay S.ScheduleSyncer


def _syncer(fake):
    return _REAL_SYNCER("fingraph-dev-jobs", "arn:trigger", "arn:role", scheduler=fake)


@pytest.mark.parametrize("item, expected", [
    ({"enabled": True, "schedule_enabled": True, "schedule_expression": "cron(0 19 * * ? *)"},
     "cron(0 19 * * ? *)"),
    ({"enabled": False, "schedule_enabled": True, "schedule_expression": "rate(1 day)"}, None),
    ({"enabled": True, "schedule_enabled": False, "schedule_expression": "rate(1 day)"}, None),
    ({"enabled": True, "schedule_enabled": True, "schedule_expression": " "}, None),
    (None, None),
])
def test_desired_expression(item, expected):
    assert S.desired_expression(item) == expected


def test_tao_roi_sua_roi_xoa():
    fake = _FakeScheduler()
    s = _syncer(fake)
    assert s.apply("gleif_lei", "cron(0 19 * * ? *)") == "created"
    params = fake.schedules["src-gleif_lei"]
    body = json.loads(params["Target"]["Input"])
    assert body == {"job_id": "src.gleif_lei",
                    "trigger": {"type": "schedule", "scheduled_time": "<aws.scheduler.scheduled-time>"}}
    assert params["ScheduleExpressionTimezone"] == "UTC" and params["Target"]["Arn"] == "arn:trigger"
    assert s.apply("gleif_lei", "rate(1 day)") == "updated"
    assert s.apply("gleif_lei", None) == "deleted"
    assert s.apply("gleif_lei", None) == "absent"


def _stream(job, new=None):
    rec = {"eventName": "MODIFY" if new else "REMOVE",
           "dynamodb": {"Keys": {"job_name": {"S": job}}}}
    if new:
        rec["dynamodb"]["NewImage"] = {k: ({"BOOL": v} if isinstance(v, bool) else {"S": v})
                                       for k, v in {"job_name": job, **new}.items()}
    return rec


def test_handler_doc_stream(monkeypatch):
    fake = _FakeScheduler(existing=["src-cu"])
    monkeypatch.setattr(S, "ScheduleSyncer", lambda *a, **kw: _syncer(fake))
    monkeypatch.setenv("SCHEDULES_ENABLED", "true")
    for k in ("SCHEDULE_GROUP", "TRIGGER_ARN", "SCHEDULER_ROLE_ARN"):
        monkeypatch.setenv(k, "x")
    out = S.handler({"Records": [
        _stream("moi", {"enabled": True, "schedule_enabled": True, "schedule_expression": "rate(1 day)"}),
        _stream("cu"),  # item bi xoa -> xoa lich
        _stream("tat", {"enabled": True, "schedule_enabled": False, "schedule_expression": "rate(1 day)"}),
    ]})
    assert out == {"moi": "created", "cu": "deleted", "tat": "absent"}


def test_handler_tat_cong_tac_khong_dong_vao_lich(monkeypatch):
    monkeypatch.setenv("SCHEDULES_ENABLED", "false")
    monkeypatch.setattr(S, "ScheduleSyncer", lambda *a, **kw: pytest.fail("khong duoc tao syncer"))
    assert S.handler({"Records": [_stream("a")]}) == {"skipped": True}


def test_resync_dat_lai_va_xoa_lich_mo_coi(env: Env):
    env.ddb.Table(Env.SRC_CFG).put_item(Item={"job_name": "a", "enabled": True,
                                              "schedule_enabled": True, "schedule_expression": "rate(1 day)"})
    env.ddb.Table(Env.SRC_CFG).put_item(Item={"job_name": "b", "enabled": True, "schedule_enabled": False})
    fake = _FakeScheduler(existing=["src-mo_coi", "src-b"])
    out = S.resync(_syncer(fake), Env.SRC_CFG, dynamodb=env.ddb)
    assert out == {"a": "created", "b": "deleted", "mo_coi": "deleted"}
    assert set(fake.schedules) == {"src-a"}
