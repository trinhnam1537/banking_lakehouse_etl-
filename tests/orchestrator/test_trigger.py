"""Lambda trigger (orchestrator/trigger.py)."""

from __future__ import annotations

import json
import os

import pytest
import yaml

from orchestrator.trigger import EVENT_SOURCE, SUCCEEDED_DETAIL_TYPE, Trigger
from sourcing.config import decode_snapshot

from .conftest import ROOT, RUNNER_ARN, Env


def succeeded_event(job_id="src.binance_ticker_24hr", run_id="r1",
                    manifest="landing/_manifests/binance/ticker_24hr/ingest_date=2026-09-28/r1.json",
                    chain=True):
    return {"source": EVENT_SOURCE, "detail-type": SUCCEEDED_DETAIL_TYPE,
            "detail": {"job_id": job_id, "job_type": job_id.split(".")[0],
                       "job_name": job_id.split(".")[1], "run_id": run_id,
                       "manifest": manifest, "chain": chain}}


# ---------- chuẩn hoá event ----------


def test_thieu_job_id_bao_loi_kem_vi_du(env: Env):
    res = env.trigger.handle({})
    assert res["ok"] is False and "job_id" in res["error"]


def test_job_ids_nhieu_job_mot_lan(env: Env):
    env.seed_src("a")
    env.seed_src("b")
    res = env.trigger.handle({"job_ids": ["src.a", "b"]})
    assert res["ok"] and [r["status"] for r in res["results"]] == ["STARTED", "STARTED"]
    assert len(env.sfn.started) == 2


def test_run_id_chi_cho_mot_job(env: Env):
    res = env.trigger.handle({"job_ids": ["a", "b"], "run_id": "x"})
    assert res["ok"] is False and "run_id" in res["error"]


def test_records_sqs_duoc_boc(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"Records": [{"body": json.dumps({"job_id": "src.a"})}]})
    assert res["status"] == "STARTED"


def test_chuoi_true_false_hieu_dung(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "dry_run": "true", "chain": "false"})
    assert res["dry_run"] is True and res["chain"] is False


def test_loai_job_khong_co(env: Env):
    res = env.trigger.handle({"job_id": "gld.x"})
    assert res["ok"] is False and "src" in res["error"] and "slv" in res["error"]


def test_action_la(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "action": "xoa"})
    assert res["ok"] is False and "action" in res["error"]


# ---------- run: sourcing ----------


def test_run_src_ghi_orch_va_gui_dung_input(env: Env):
    cfg = env.seed_src("gleif_lei")
    res = env.trigger.handle({"job_id": "src.gleif_lei"})

    assert res["ok"] and res["status"] == "STARTED"
    [started] = env.sfn.started
    inp = started["input"]
    assert started["arn"] == RUNNER_ARN
    assert started["name"] == f"src-gleif_lei-{res['run_id']}" and len(started["name"]) <= 80
    assert inp["glue_job"] == "t-src-gleif_lei"
    assert inp["log_table"] == Env.SRC_LOG and inp["chain"] is True
    args = inp["glue_args"]
    assert args["--JOB_NAME"] == "gleif_lei" and args["--RUN_ID"] == res["run_id"]
    assert args["--DRY_RUN"] == "false"
    # Glue nhan dung cau hinh Lambda da doc, khong phai doc lai
    snap = decode_snapshot(args["--CONFIG_SNAPSHOT"])
    assert snap["api"] == cfg["api"] and snap["enabled"] is True
    # Moi gia tri tham so Glue phai khac rong
    assert all(isinstance(v, str) and v for v in args.values())

    orch = env.item(Env.SRC_LOG, "gleif_lei", f"ORCH#{res['run_id']}")
    assert orch["orch_status"] == "STARTED" and orch["execution_arn"] == res["execution_arn"]
    assert orch["trigger"] == {"type": "manual"} and int(orch["expires_at"]) > 0


def test_ten_execution_dai_van_toi_da_80(env: Env):
    name = "x" * 100
    out = Trigger.execution_name("src", name, "0123456789abcdef")
    assert len(out) <= 80 and out.endswith("-0123456789abcdef")


def test_khong_co_trong_bang(env: Env):
    res = env.trigger.handle({"job_id": "src.khong_co"})
    assert res["ok"] is False and "terraform apply" in res["error"]
    assert env.sfn.started == []


def test_job_tat_thi_bo_qua(env: Env):
    env.seed_src("tat", enabled=False)
    res = env.trigger.handle({"job_id": "src.tat"})
    assert res["status"] == "SKIPPED" and env.sfn.started == []


def test_override_ap_vao_snapshot_va_tat_chain(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "overrides": {"api.max_pages": 1,
                                                           "target.prefix": "landing_test"}})
    assert res["status"] == "STARTED" and res["chain"] is False and res["notes"]
    snap = decode_snapshot(env.sfn.started[0]["input"]["glue_args"]["--CONFIG_SNAPSHOT"])
    assert snap["api"]["max_pages"] == 1 and snap["target"]["prefix"] == "landing_test"
    orch = env.item(Env.SRC_LOG, "a", f"ORCH#{res['run_id']}")
    assert json.loads(orch["overrides"]) == {"api.max_pages": 1, "target.prefix": "landing_test"}


@pytest.mark.parametrize("key", ["runtime.timeout_minutes", "name", "schedule.enabled"])
def test_override_khoa_cam(env: Env, key):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "overrides": {key: 1}})
    assert res["ok"] is False and "khong override" in res["error"]
    assert env.sfn.started == []


def test_override_sai_bi_from_dict_chan(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "overrides": {"cdc.mode": "cdc9"}})
    assert res["ok"] is False and "cdc.mode" in res["error"]


def test_override_tren_cdc1_can_dry_run(env: Env):
    env.seed_src("inc", cdc={"mode": "cdc1", "watermark_column": "updated"})
    ev = {"job_id": "inc", "overrides": {"api.max_pages": 1}}
    assert "checkpoint" in env.trigger.handle(ev)["error"]
    assert env.trigger.handle({**ev, "dry_run": True})["status"] == "STARTED"
    assert env.trigger.handle({**ev, "allow_checkpoint_advance": True})["status"] == "STARTED"


def test_dry_run_tat_chain(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "dry_run": True})
    inp = env.sfn.started[0]["input"]
    assert inp["glue_args"]["--DRY_RUN"] == "true" and inp["chain"] is False


def test_manifest_khong_dung_cho_src(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "manifest": "x"})
    assert res["ok"] is False and "slv" in res["error"]


def test_lich_goi_lap_cung_tick_chi_chay_mot_lan(env: Env):
    env.seed_src("a")
    ev = {"job_id": "src.a", "trigger": {"type": "schedule",
                                         "scheduled_time": "2026-09-28T19:00:00Z"}}
    first = env.trigger.handle(ev)
    second = env.trigger.handle(ev)
    assert first["status"] == "STARTED" and second["status"] == "DUPLICATE"
    assert first["run_id"] == second["run_id"] and len(env.sfn.started) == 1
    # Tick khac -> run_id khac
    third = env.trigger.handle({**ev, "trigger": {"type": "schedule",
                                                  "scheduled_time": "2026-09-29T19:00:00Z"}})
    assert third["status"] == "STARTED" and third["run_id"] != first["run_id"]


def test_start_execution_loi_thi_danh_dau_va_raise(env: Env):
    env.seed_src("a")
    env.sfn.fail_start = "AccessDeniedException"
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError):
        env.trigger.handle({"job_id": "a", "run_id": "r9"})
    assert env.item(Env.SRC_LOG, "a", "ORCH#r9")["orch_status"] == "START_FAILED"


# ---------- đợi kết quả ----------


def test_wait_tra_tom_tat_khi_xong(env: Env):
    env.seed_src("a")
    env.sfn.statuses = ["RUNNING", "RUNNING", "SUCCEEDED"]
    # Glue job da ghi RUN# + bao ket qua ve ORCH# (mo phong)
    from sourcing.state import JobLogStore, RunStats
    log = JobLogStore(Env.SRC_LOG, client=env.ddb)

    orig = env.sfn.describe_execution

    def describe(executionArn):
        out = orig(executionArn)
        if out["status"] == "SUCCEEDED" and not log.recent_runs("a"):
            rk = log.start_run("a", "r1", {})
            log.finish_run("a", rk, "SUCCESS", RunStats(records_read=3, records_written=3))
            log.record_orch_result("a", "r1", "SUCCESS", "landing/_manifests/x/r1.json")
        return out

    env.sfn.describe_execution = describe
    res = env.trigger.handle({"job_id": "a", "run_id": "r1", "wait_seconds": 120})
    assert res["ok"] and res["status"] == "SUCCEEDED"
    assert res["run"]["status"] == "SUCCESS" and res["run"]["records_written"] == 3
    assert res["orch"]["manifest"] == "landing/_manifests/x/r1.json"
    assert env.slept == [10, 10]


def test_wait_that_bai_tra_ok_false(env: Env):
    env.seed_src("a")
    env.sfn.statuses = ["FAILED"]
    env.sfn.cause = "Glue job FAILED: KeyError 'data'"
    res = env.trigger.handle({"job_id": "a", "wait_seconds": 60})
    assert res["ok"] is False and "KeyError" in res["error"]


def test_wait_het_gio_bao_van_dang_chay(env: Env):
    env.seed_src("a")
    env.sfn.statuses = ["RUNNING"]

    class Ctx:
        def get_remaining_time_in_millis(self):
            return 30_000  # con 30s -> ngan sach doi = 0

    res = env.trigger.handle({"job_id": "a", "wait_seconds": 600}, Ctx())
    assert res["status"] == "RUNNING" and "status" in res["note"]


# ---------- validate / status ----------


def test_validate_khong_ghi_gi(env: Env):
    env.seed_src("a")
    res = env.trigger.handle({"job_id": "a", "action": "validate",
                              "overrides": {"api.max_pages": 2}})
    assert res["ok"] and res["config"]["api"]["max_pages"] == 2
    assert res["glue_args"]["--CONFIG_SNAPSHOT"].startswith("<")
    assert env.sfn.started == []
    assert env.ddb.Table(Env.SRC_LOG).scan()["Items"] == []


def test_status_co_checkpoint_va_lan_chay(env: Env):
    env.seed_src("a")
    from sourcing.state import JobLogStore, RunStats
    log = JobLogStore(Env.SRC_LOG, client=env.ddb)
    rk = log.start_run("a", "r1", {"x": 1})
    log.finish_run("a", rk, "SUCCESS", RunStats(records_read=1))
    log.advance_checkpoint("a", "r1", {"default": "5"}, "SUCCESS")

    res = env.trigger.handle({"job_id": "a", "action": "status"})
    assert res["checkpoint"]["watermarks"] == {"default": "5"}
    assert res["recent_runs"][0]["run_id"] == "r1" and "config" not in res["recent_runs"][0]

    one = env.trigger.handle({"job_id": "a", "action": "status", "run_id": "r1"})
    assert one["run"]["run_id"] == "r1" and one["orch"] is None


# ---------- silver: gọi tay và nối từ sourcing ----------


def test_slv_can_manifest_hoac_ngay(env: Env):
    name = env.seed_slv_from_repo()
    res = env.trigger.handle({"job_id": f"slv.{name}"})
    assert res["ok"] is False and "data_date" in res["error"]


def test_slv_goi_tay_theo_ngay(env: Env):
    name = env.seed_slv_from_repo()
    res = env.trigger.handle({"job_id": f"slv.{name}", "data_date": "2026-09-28"})
    args = env.sfn.started[0]["input"]["glue_args"]
    assert res["status"] == "STARTED"
    assert args == {"--JOB_NAME": name, "--RUN_ID": res["run_id"], "--MANIFEST": "-",
                    "--DATA_DATE": "2026-09-28", "--REBUILD_FROM": "-"}
    assert env.sfn.started[0]["input"]["log_table"] == Env.SLV_LOG


def test_slv_tu_choi_override_va_dry_run(env: Env):
    name = env.seed_slv_from_repo()
    base = {"job_id": f"slv.{name}", "data_date": "2026-09-28"}
    assert env.trigger.handle({**base, "overrides": {"a": 1}})["ok"] is False
    assert env.trigger.handle({**base, "dry_run": True})["ok"] is False


def test_src_xong_thi_kich_silver_doc_dataset_do(env: Env):
    name = env.seed_slv_from_repo()  # source.dataset = binance/ticker_24hr
    res = env.trigger.handle(succeeded_event())
    assert res["status"] == "STARTED" and res["job_id"] == f"slv.{name}"
    inp = env.sfn.started[0]["input"]
    assert inp["glue_args"]["--MANIFEST"].endswith("/r1.json")
    assert inp["glue_args"]["--DATA_DATE"] == "2026-09-28"
    orch = env.item(Env.SLV_LOG, name, f"ORCH#{res['run_id']}")
    assert orch["trigger"]["type"] == "upstream"
    assert orch["trigger"]["parent_job_id"] == "src.binance_ticker_24hr"


def test_cung_manifest_hai_lan_chi_chay_mot_lan(env: Env):
    """Event orchestrator va rule S3 cu cua silver cung dua mot manifest."""
    env.seed_slv_from_repo()
    assert env.trigger.handle(succeeded_event())["status"] == "STARTED"
    assert env.trigger.handle(succeeded_event(run_id="r1"))["status"] == "DUPLICATE"
    assert len(env.sfn.started) == 1


def test_force_chay_lai_manifest_da_xu_ly(env: Env):
    name = env.seed_slv_from_repo()
    m = "landing/_manifests/binance/ticker_24hr/ingest_date=2026-09-28/r1.json"
    env.trigger.handle({"job_id": f"slv.{name}", "manifest": m})
    res = env.trigger.handle({"job_id": f"slv.{name}", "manifest": m, "force": True})
    assert res["status"] == "STARTED" and len(env.sfn.started) == 2


def test_start_loi_thi_nha_trigger_cho_lan_sau(env: Env):
    name = env.seed_slv_from_repo()
    env.sfn.fail_start = "ThrottlingException"
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError):
        env.trigger.handle(succeeded_event())
    env.sfn.fail_start = ""
    assert env.trigger.handle(succeeded_event())["status"] == "STARTED"


def test_chain_false_khong_kich_gi(env: Env):
    env.seed_slv_from_repo()
    res = env.trigger.handle(succeeded_event(chain=False))
    assert res == {"ok": True, "results": []} and env.sfn.started == []


def test_dataset_khong_ai_doc(env: Env):
    env.seed_slv_from_repo()
    m = "landing/_manifests/khac/ds/ingest_date=2026-09-28/r1.json"
    assert env.trigger.handle(succeeded_event(manifest=m))["results"] == []


# ---------- from_env ----------


def test_from_env(env: Env, monkeypatch):
    monkeypatch.setenv("JOB_TYPES", json.dumps({
        "src": {"config_table": Env.SRC_CFG, "log_table": Env.SRC_LOG, "glue_prefix": "p-src-"},
        "slv": {"config_table": Env.SLV_CFG, "log_table": Env.SLV_LOG, "glue_prefix": "p-slv-"},
    }))
    monkeypatch.setenv("RUNNER_ARN", RUNNER_ARN)
    t = Trigger.from_env()
    assert set(t.types) == {"src", "slv"} and t.types["slv"].glue_prefix == "p-slv-"


def test_from_env_loai_la(env: Env, monkeypatch):
    monkeypatch.setenv("JOB_TYPES", json.dumps({"gld": {"config_table": "a", "log_table": "b",
                                                        "glue_prefix": "c"}}))
    monkeypatch.setenv("RUNNER_ARN", RUNNER_ARN)
    with pytest.raises(RuntimeError, match="gld"):
        Trigger.from_env()


# ---------- event mẫu trong orchestrator/events/ phải luôn dùng được ----------

EVENTS_DIR = os.path.join(ROOT, "orchestrator", "events")


def _enabled_entries(rel: str):
    out = {}
    for dirpath, _, files in os.walk(os.path.join(ROOT, rel)):
        for f in files:
            if f.endswith(".yml"):
                with open(os.path.join(dirpath, f), encoding="utf-8") as fh:
                    out[f[:-4]] = yaml.safe_load(fh).get("enabled", True)
    return out


@pytest.mark.parametrize("fname", sorted(os.listdir(EVENTS_DIR)))
def test_event_mau_hop_le(env: Env, fname):
    with open(os.path.join(EVENTS_DIR, fname), encoding="utf-8") as f:
        event = json.load(f)
    src = _enabled_entries("sourcing_etl/sources")
    slv = _enabled_entries("silver_etl/entries")
    env.seed_slv_from_repo()  # de event upstream co nguoi nhan

    reqs = env.trigger.requests_from(event)
    assert reqs, fname
    for r in reqs:
        code, _, name = r.job_id.partition(".")
        assert (src if code == "src" else slv).get(name) is True, f"{fname}: {r.job_id} khong bat"
