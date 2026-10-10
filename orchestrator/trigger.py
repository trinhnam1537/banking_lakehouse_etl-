"""Lambda trigger — MỘT cửa vào cho mọi luồng job.

    EventBridge Scheduler (mỗi job một lịch)   {"job_id": "src.x", "trigger": {...}}
    Gọi tay / Lambda console "Test"             {"job_id": "src.x", "wait_seconds": 300}
    EventBridge rule "Job Run Succeeded"        luồng trước chạy xong -> luồng sau
    SQS (nếu sau này cần đệm)                   {"Records": [{"body": "<một trong các dạng trên>"}]}
        │
        ▼
    Lambda NÀY
      · chuẩn hoá mọi dạng event -> RunRequest
      · đọc job_config MỘT lần, áp override, kiểm tra bằng đúng from_dict()
      · ghi ORCH#<run_id> có điều kiện (chống chạy trùng cùng một tick lịch)
      · StartExecution(job-runner, name=<type>-<job>-<run_id>)
      · (tuỳ chọn) đợi execution xong rồi trả tóm tắt — để test không cần script
        │
        ▼
    Step Functions job-runner (dùng chung mọi luồng)
      Glue startJobRun.sync -> RecordSuccess/RecordFailure -> (chain) PutEvents

job_id: `<type>.<ten>` — `src.gleif_lei`, `slv.gleif_legal_entity`. Không có
tiền tố thì hiểu là DEFAULT_JOB_TYPE (src), để `gleif_lei` vẫn chạy được.

Ba action:
    run       (mặc định) kích một lần chạy
    validate  chỉ đọc config + override + kiểm tra, trả tham số Glue sẽ truyền.
              Không ghi gì, không tốn Glue.
    status    checkpoint + vài lần chạy gần nhất (hoặc đúng một run_id)
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from sourcing.state import _iso, _plain

from .job_types import JOB_TYPE_CLASSES, JobType, RunRequest, TriggerError

EVENT_SOURCE = "fingraph.jobs"
SUCCEEDED_DETAIL_TYPE = "Job Run Succeeded"

TERMINAL = ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")
POLL_SECONDS = 10
MAX_WAIT_SECONDS = 840  # Lambda toi da 900s, chua 60s de tom tat ket qua

# Khoa trong event gan cho tung RunRequest. Khoa khac bi bo qua co chu y:
# EventBridge/Scheduler chen them truong cua rieng no.
_REQUEST_KEYS = ("action", "run_id", "overrides", "chain", "dry_run", "force",
                 "allow_checkpoint_advance", "wait_seconds", "manifest",
                 "data_date", "rebuild_from", "trigger")


def _as_bool(v: Any, default: bool) -> bool:
    if v is None:
        return default
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "y")
    return bool(v)


class Trigger:
    def __init__(self, job_types: Dict[str, JobType], runner_arn: str,
                 default_type: str = "src", sfn=None,
                 sleep: Callable[[float], None] = time.sleep):
        self.types = job_types
        self.runner_arn = runner_arn
        self.default_type = default_type
        self.sfn = sfn or boto3.client("stepfunctions")
        self.sleep = sleep

    @classmethod
    def from_env(cls) -> "Trigger":
        """JOB_TYPES = {"src": {"config_table", "log_table", "glue_prefix"}, "slv": {...}}."""
        spec = json.loads(os.environ["JOB_TYPES"])
        ddb = boto3.resource("dynamodb")
        types = {}
        for code, conf in spec.items():
            klass = JOB_TYPE_CLASSES.get(code)
            if klass is None:
                raise RuntimeError(f"JOB_TYPES co loai '{code}' ma code khong biet")
            types[code] = klass(conf["config_table"], conf["log_table"],
                                conf["glue_prefix"], dynamodb=ddb)
        return cls(types, os.environ["RUNNER_ARN"],
                   default_type=os.environ.get("DEFAULT_JOB_TYPE", "src"))

    # ---------- chuẩn hoá event ----------

    def requests_from(self, event: Any) -> List[RunRequest]:
        if isinstance(event, str):
            event = json.loads(event)
        if not isinstance(event, dict):
            raise TriggerError(f"event phai la object JSON, nhan {type(event).__name__}")

        if "Records" in event:  # SQS dat truoc Lambda
            out: List[RunRequest] = []
            for rec in event["Records"]:
                out.extend(self.requests_from(rec.get("body") or "{}"))
            return out

        if event.get("source") == EVENT_SOURCE and event.get("detail-type") == SUCCEEDED_DETAIL_TYPE:
            detail = event.get("detail") or {}
            if not _as_bool(detail.get("chain"), True):
                return []
            out = []
            for jt in self.types.values():
                out.extend(jt.consumers_of(detail))
            if not out:
                print(f"{detail.get('job_id')}: khong co job nao chay sau")
            return out

        ids = event.get("job_ids") or ([event["job_id"]] if event.get("job_id") else [])
        if not ids:
            raise TriggerError(
                'thieu "job_id". Vi du: {"job_id": "src.gleif_lei", "wait_seconds": 300}'
            )
        if len(ids) > 1 and event.get("run_id"):
            raise TriggerError('"run_id" chi dung khi chay MOT job')

        common = {k: event[k] for k in _REQUEST_KEYS if k in event}
        reqs = []
        for jid in ids:
            reqs.append(RunRequest(
                job_id=str(jid),
                action=str(common.get("action", "run")),
                run_id=common.get("run_id"),
                overrides=dict(common.get("overrides") or {}),
                chain=_as_bool(common.get("chain"), True),
                dry_run=_as_bool(common.get("dry_run"), False),
                force=_as_bool(common.get("force"), False),
                allow_checkpoint_advance=_as_bool(common.get("allow_checkpoint_advance"), False),
                wait_seconds=int(common.get("wait_seconds") or 0),
                manifest=common.get("manifest"),
                data_date=common.get("data_date"),
                rebuild_from=common.get("rebuild_from"),
                trigger=dict(common.get("trigger") or {"type": "manual"}),
            ))
        return reqs

    def resolve(self, job_id: str) -> Tuple[JobType, str]:
        code, sep, name = job_id.partition(".")
        if not sep:
            code, name = self.default_type, job_id
        jt = self.types.get(code)
        if jt is None:
            raise TriggerError(f"loai job '{code}' khong co. Co: {', '.join(sorted(self.types))}")
        if not name:
            raise TriggerError(f"job_id rong ten: {job_id!r}")
        return jt, name

    # ---------- run ----------

    @staticmethod
    def make_run_id(job_id: str, trigger: Dict[str, Any]) -> str:
        """Lịch: run_id suy ra từ (job, giờ theo lịch) -> Scheduler có gọi lặp
        lại cùng một tick thì ORCH# có điều kiện và tên execution đều chặn."""
        tick = trigger.get("scheduled_time")
        if tick:
            return hashlib.sha256(f"{job_id}|{tick}".encode("utf-8")).hexdigest()[:16]
        return uuid.uuid4().hex[:16]

    @staticmethod
    def execution_name(code: str, name: str, run_id: str) -> str:
        # Toi da 80 ky tu, chi [A-Za-z0-9-_]
        return f"{code}-{name}"[:62] + f"-{run_id}"

    def _load(self, jt: JobType, name: str) -> Dict[str, Any]:
        item = jt.get_item(name)
        if item is None:
            raise TriggerError(
                f"khong thay '{jt.code}.{name}' trong {jt.config_table}. "
                f"Entry YAML da duoc terraform apply chua?"
            )
        return item

    def validate(self, req: RunRequest) -> Dict[str, Any]:
        jt, name = self.resolve(req.job_id)
        item = self._load(jt, name)
        run_id = req.run_id or "validate"
        prep = jt.prepare(name, item, req, run_id)
        args = dict(prep.glue_args)
        if "--CONFIG_SNAPSHOT" in args:  # hien ban giai nen cho de doc
            args["--CONFIG_SNAPSHOT"] = f"<{len(args['--CONFIG_SNAPSHOT'])} ky tu>"
        return {"ok": True, "action": "validate", "job_id": f"{jt.code}.{name}",
                "enabled": bool(item.get("enabled", True)), "glue_job": prep.glue_job,
                "glue_args": args, "chain": prep.chain, "notes": prep.notes,
                "config": prep.config}

    def run(self, req: RunRequest, context=None) -> Dict[str, Any]:
        jt, name = self.resolve(req.job_id)
        job_id = f"{jt.code}.{name}"
        item = self._load(jt, name)
        if not item.get("enabled", True):
            return {"ok": True, "job_id": job_id, "status": "SKIPPED",
                    "reason": "enabled=false (khong co Glue job cho entry tat)"}

        run_id = req.run_id or self.make_run_id(job_id, req.trigger)
        prep = jt.prepare(name, item, req, run_id)
        exec_name = self.execution_name(jt.code, name, run_id)

        if not jt.claim(name, req, run_id):
            return {"ok": True, "job_id": job_id, "status": "DUPLICATE",
                    "reason": f"manifest da duoc xu ly: {req.manifest} (force=true de chay lai)"}

        created = jt.orch_create(name, run_id, {
            "job_id": job_id,
            "orch_status": "STARTED",
            "execution_name": exec_name,
            "glue_job": prep.glue_job,
            "trigger": req.trigger,
            # Chuoi JSON: override co the co float/None ma DynamoDB kho chiu
            "overrides": json.dumps(req.overrides, ensure_ascii=False),
            "dry_run": req.dry_run,
            "chain": prep.chain,
            "requested_at": _iso(),
        })
        if not created:
            return {"ok": True, "job_id": job_id, "run_id": run_id, "status": "DUPLICATE",
                    "reason": "run_id nay da duoc kich (lich goi lap cung mot tick?)"}

        sfn_input = {
            "job_id": job_id,
            "job_type": jt.code,
            "job_name": name,
            "run_id": run_id,
            "glue_job": prep.glue_job,
            "glue_args": prep.glue_args,
            "log_table": jt.log_table,
            "chain": prep.chain,
        }
        try:
            resp = self.sfn.start_execution(stateMachineArn=self.runner_arn, name=exec_name,
                                            input=json.dumps(sfn_input, ensure_ascii=False))
        except ClientError as e:
            code = e.response["Error"]["Code"]
            jt.orch_set(name, run_id, orch_status="START_FAILED", err=f"{code}: {e}"[:4000])
            jt.release_claim(name, req, run_id)
            if code == "ExecutionAlreadyExists":
                return {"ok": True, "job_id": job_id, "run_id": run_id, "status": "DUPLICATE",
                        "reason": f"execution {exec_name} da ton tai"}
            raise

        arn = resp["executionArn"]
        jt.orch_set(name, run_id, execution_arn=arn)

        out: Dict[str, Any] = {
            "ok": True, "job_id": job_id, "run_id": run_id, "status": "STARTED",
            "glue_job": prep.glue_job, "chain": prep.chain, "dry_run": req.dry_run,
            "execution_arn": arn, "console": self._console_url(arn),
        }
        if prep.notes:
            out["notes"] = prep.notes
        if req.wait_seconds > 0:
            out.update(self._wait(jt, name, run_id, arn, req.wait_seconds, context))
        return out

    # ---------- đợi + tóm tắt: để test một job chỉ bằng một lần invoke ----------

    @staticmethod
    def _console_url(arn: str) -> str:
        region = arn.split(":")[3] if arn.count(":") >= 3 else ""
        return (f"https://{region}.console.aws.amazon.com/states/home?region={region}"
                f"#/v2/executions/details/{arn}")

    def _wait(self, jt: JobType, name: str, run_id: str, arn: str,
              seconds: int, context) -> Dict[str, Any]:
        budget = min(seconds, MAX_WAIT_SECONDS)
        if context is not None and hasattr(context, "get_remaining_time_in_millis"):
            budget = min(budget, context.get_remaining_time_in_millis() / 1000 - 30)
        deadline = time.monotonic() + max(budget, 0)

        status = "RUNNING"
        desc: Dict[str, Any] = {}
        while True:
            desc = self.sfn.describe_execution(executionArn=arn)
            status = desc["status"]
            if status in TERMINAL or time.monotonic() >= deadline:
                break
            self.sleep(POLL_SECONDS)

        out: Dict[str, Any] = {"status": status}
        if status not in TERMINAL:
            out["note"] = "het thoi gian doi, job VAN DANG CHAY — goi lai voi action=status"
        out.update(self.summary(jt, name, run_id))
        if status in ("FAILED", "TIMED_OUT", "ABORTED"):
            out["ok"] = False
            out["error"] = desc.get("cause") or desc.get("error") or out.get("orch", {}).get("err")
        return out

    def summary(self, jt: JobType, name: str, run_id: str) -> Dict[str, Any]:
        orch = jt.orch_get(name, run_id)
        run = jt.find_run(name, run_id)
        out = {"orch": _plain(orch) if orch else None, "run": _plain(run) if run else None}
        if run and run.get("err"):
            out["run"]["err"] = str(run["err"])[-1500:]
        return out

    # ---------- status ----------

    def status(self, req: RunRequest) -> Dict[str, Any]:
        jt, name = self.resolve(req.job_id)
        job_id = f"{jt.code}.{name}"
        if req.run_id:
            return {"ok": True, "action": "status", "job_id": job_id, "run_id": req.run_id,
                    **self.summary(jt, name, req.run_id)}
        runs = [_plain(r) for r in jt.log.recent_runs(name, limit=5)]
        for r in runs:
            r.pop("config", None)
            if r.get("err"):
                r["err"] = str(r["err"])[-500:]
        return {"ok": True, "action": "status", "job_id": job_id,
                "recent_runs": runs, **jt.status(name)}

    # ---------- điểm vào ----------

    def handle(self, event: Any, context=None) -> Dict[str, Any]:
        try:
            reqs = self.requests_from(event)
        except TriggerError as e:
            return {"ok": False, "error": str(e)}

        results = []
        for req in reqs:
            try:
                if req.action == "validate":
                    res = self.validate(req)
                elif req.action == "status":
                    res = self.status(req)
                elif req.action == "run":
                    res = self.run(req, context)
                else:
                    raise TriggerError(f"action '{req.action}' khong co. Co: run, validate, status")
            except TriggerError as e:
                # Loi cua YEU CAU: tra lai, KHONG raise — raise thi EventBridge
                # retry mot yeu cau sai mai van sai.
                res = {"ok": False, "job_id": req.job_id, "error": str(e)}
            print(json.dumps({"request": {"job_id": req.job_id, "action": req.action,
                                          "trigger": req.trigger},
                              "result": {k: res.get(k) for k in ("ok", "status", "run_id", "error")}},
                             ensure_ascii=False, default=str))
            results.append(res)

        if len(results) == 1:
            return results[0]
        return {"ok": all(r.get("ok") for r in results), "results": results}


_TRIGGER: Optional[Trigger] = None


def handler(event, context=None):
    global _TRIGGER
    if _TRIGGER is None:  # tai dung client giua cac lan goi am
        _TRIGGER = Trigger.from_env()
    return _TRIGGER.handle(event, context)


__all__ = ["Trigger", "handler", "EVENT_SOURCE", "SUCCEEDED_DETAIL_TYPE"]
