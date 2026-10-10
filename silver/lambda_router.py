"""Lambda router: manifest mới trên S3 -> các job silver cần chạy.

    S3 (landing/_manifests/**.json, Object Created)
      -> EventBridge rule -> SQS silver-trigger (chuẩn, có DLQ)
        -> Lambda NÀY
             · tách dataset + ngày từ đường dẫn manifest
             · tìm entry silver ĐANG BẬT có source.dataset khớp (một dataset
               có thể nuôi nhiều bảng silver: GLEIF lei_records -> pháp nhân
               + quan hệ mẹ-con)
             · TRIG# có điều kiện trong job_log: manifest đến hai lần chỉ chạy một
             · gửi SQS silver-run.fifo, MessageGroupId = tên job
      -> EventBridge Pipes -> Step Functions -> Glue job của entry

Vì sao MessageGroupId = tên job: FIFO giao TUẦN TỰ trong một group. Hai
manifest của cùng bảng xếp hàng theo thứ tự đến; các bảng khác nhau chạy
song song. SCD2 cần đúng điều này.

Gọi tay (chạy lại, backfill) — invoke trực tiếp, không qua SQS:
    {"job_name": "gleif_legal_entity", "data_date": "2026-09-28"}
    {"job_name": "...", "manifest": "landing/_manifests/.../x.json"}
    {"job_name": "...", "data_date": "2026-09-20", "rebuild_from": "2026-09-20"}
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import boto3

from .state import JobConfigStore, SilverLogStore

MANIFEST_RE = re.compile(r"^(?P<prefix>.+?)/_manifests/(?P<dataset>.+)/ingest_date=(?P<date>\d{4}-\d{2}-\d{2})/(?P<run>[^/]+)\.json$")

# Glue tu choi tham so job co gia tri rong -> dung "-" lam "khong co";
# silver.main coi "-" la None.
NONE = "-"

_CACHE: Dict[str, Any] = {"at": 0.0, "jobs": []}
CACHE_SECONDS = 60


def parse_manifest_key(key: str) -> Optional[Tuple[str, str]]:
    """`landing/_manifests/gleif/lei_records/ingest_date=2026-09-28/abc.json`
    -> ("gleif/lei_records", "2026-09-28"). Không khớp -> None."""
    m = MANIFEST_RE.match(key)
    return (m["dataset"], m["date"]) if m else None


def _source_dataset(item: Dict[str, Any]) -> str:
    # Terraform seed san cot source_dataset; du phong doc tu config
    return item.get("source_dataset") or ((item.get("config") or {}).get("source") or {}).get("dataset", "")


def enabled_jobs(store: JobConfigStore) -> List[Dict[str, Any]]:
    """Cache ngắn trong container: một đợt 20 manifest không quét bảng 20 lần."""
    now = time.monotonic()
    if now - _CACHE["at"] > CACHE_SECONDS:
        _CACHE["jobs"] = store.list_enabled()
        _CACHE["at"] = now
    return _CACHE["jobs"]


def _dedup_id(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:64]


class Router:
    def __init__(self, config_table: str, joblog_table: str, run_queue_url: str,
                 sqs_client=None, config_store=None, log_store=None):
        self.configs = config_store or JobConfigStore(config_table)
        self.log = log_store or SilverLogStore(joblog_table)
        self.queue = run_queue_url
        self.sqs = sqs_client or boto3.client("sqs")

    def _send(self, job: str, body: Dict[str, Any], dedup: str) -> None:
        self.sqs.send_message(
            QueueUrl=self.queue,
            MessageBody=json.dumps(body),
            MessageGroupId=job,
            MessageDeduplicationId=dedup,
        )

    def route_manifest(self, bucket: str, key: str) -> List[str]:
        parsed = parse_manifest_key(key)
        if not parsed:
            print(f"bo qua (khong phai manifest): s3://{bucket}/{key}")
            return []
        dataset, day = parsed

        sent = []
        for job in enabled_jobs(self.configs):
            if _source_dataset(job) != dataset:
                continue
            name = job["job_name"]
            run_id = SilverLogStore.new_run_id()
            if not self.log.claim_trigger(name, key, run_id):
                print(f"[{name}] manifest da duoc nhan truoc do -> bo qua: {key}")
                continue
            self._send(name, {"job_name": name, "run_id": run_id, "manifest": key,
                              "data_date": day, "rebuild_from": NONE},
                       _dedup_id(name, key))
            sent.append(name)
        if not sent:
            print(f"khong co entry silver nao doc dataset '{dataset}'")
        return sent

    def route_manual(self, event: Dict[str, Any]) -> List[str]:
        name = event["job_name"]
        run_id = SilverLogStore.new_run_id()
        body = {"job_name": name, "run_id": run_id,
                "manifest": event.get("manifest") or NONE,
                "data_date": event.get("data_date") or NONE,
                "rebuild_from": event.get("rebuild_from") or NONE}
        self._send(name, body, _dedup_id(name, run_id))
        return [name]


def _s3_from_record(record: Dict[str, Any]) -> Tuple[str, str]:
    """Body SQS là event EventBridge của S3."""
    ev = json.loads(record["body"])
    d = ev.get("detail") or {}
    return d.get("bucket", {}).get("name", ""), d.get("object", {}).get("key", "")


def handler(event, context=None):
    router = Router(os.environ["JOB_CONFIG_TABLE"], os.environ["JOBLOG_TABLE"],
                    os.environ["RUN_QUEUE_URL"])

    if "job_name" in event:  # goi tay
        return {"sent": router.route_manual(event)}

    # SQS: bao loi TUNG message (ReportBatchItemFailures) — mot manifest hong
    # khong duoc lam ca lo 10 message bi giao lai.
    failures = []
    for rec in event.get("Records", []):
        try:
            bucket, key = _s3_from_record(rec)
            router.route_manifest(bucket, key)
        except Exception as e:  # noqa: BLE001
            print(f"LOI message {rec.get('messageId')}: {type(e).__name__}: {e}")
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
