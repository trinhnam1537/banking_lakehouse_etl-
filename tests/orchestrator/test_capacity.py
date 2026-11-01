"""Cỡ máy theo entry: Lambda gửi worker_type/number_of_workers cho Step Functions
(state RunGlueJobSized), entry không khai thì không gửi (cỡ mặc định của job)."""

from __future__ import annotations

from .conftest import Env

_SPARK = dict(
    source_type="s3file",
    s3file={"bucket": "b", "prefix": "landing/_inbox/x/", "file_format": "csv"},
    cdc={"mode": "cdc1", "watermark_column": "_s3_key", "watermark_type": "string"},
)


def test_entry_co_size_gui_co_may(env: Env):
    env.seed_src("big", runtime={"engine": "spark", "profile": "src-spark", "size": "M"}, **_SPARK)
    res = env.trigger.handle({"job_id": "src.big"})
    assert res["status"] == "STARTED" and "co may: 5 x G.1X" in res["notes"]
    inp = env.sfn.started[0]["input"]
    assert inp["worker_type"] == "G.1X" and inp["number_of_workers"] == 5


def test_entry_khong_size_khong_gui(env: Env):
    env.seed_src("small")
    env.trigger.handle({"job_id": "src.small"})
    inp = env.sfn.started[0]["input"]
    assert "worker_type" not in inp and "number_of_workers" not in inp


def test_override_size_khong_tinh_la_override_du_lieu(env: Env):
    # cdc1 + override du lieu thi bi chan (tien checkpoint that); override co may thi khong,
    # va van chain xuong silver vi du lieu khong doi.
    env.seed_src("big", runtime={"engine": "spark", "profile": "src-spark", "size": "S"}, **_SPARK)
    res = env.trigger.handle({"job_id": "src.big", "overrides": {"runtime.size": "L"}})
    assert res["status"] == "STARTED" and res["chain"] is True
    inp = env.sfn.started[0]["input"]
    assert inp["worker_type"] == "G.2X" and inp["number_of_workers"] == 10


def test_override_size_sai_bi_chan(env: Env):
    env.seed_src("big", runtime={"engine": "spark", "profile": "src-spark"}, **_SPARK)
    res = env.trigger.handle({"job_id": "src.big", "overrides": {"runtime.size": "HUGE"}})
    assert res["ok"] is False and "runtime.size" in res["error"]
