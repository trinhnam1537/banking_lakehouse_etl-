"""job_config / job_log của silver — cùng thiết kế bảng với sourcing.

Tái dùng nguyên `sourcing.state` (khoá job_name + run_key, TTL 90 ngày, RUN#
ghi ngay lúc bắt đầu). Silver thêm hai thứ:

  finish_run  số liệu khác raw: rows_in/out, inserted/updated/deleted,
              cast_errors, bảng đích, manifest đã xử lý.
  TRIG#<manifest>  khoá chống xử lý trùng. S3 -> EventBridge -> SQS giao
              "ít nhất một lần": cùng một manifest có thể đến hai lần. Router
              ghi TRIG# có điều kiện; lần thứ hai thấy đã có thì bỏ qua.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from decimal import Decimal
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from sourcing.state import JobConfigStore, JobLogStore, _clean, _iso, _now, _ttl

__all__ = ["JobConfigStore", "SilverLogStore", "trigger_key"]


def trigger_key(manifest: str) -> str:
    # Khoa DynamoDB toi da 1 KB; duong dan manifest co the dai -> bam
    return "TRIG#" + hashlib.sha256(manifest.encode("utf-8")).hexdigest()[:32]


class SilverLogStore(JobLogStore):
    def finish_silver_run(self, job_name: str, run_key: str, status: str,
                          metrics: Dict[str, Any], error: str = "") -> None:
        started = dt.datetime.fromisoformat(run_key.split("#")[1])
        self._table.update_item(
            Key={"job_name": job_name, "run_key": run_key},
            UpdateExpression="SET #s = :s, ended_at = :e, duration_seconds = :d, "
                             "metrics = :m, err = :err",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=_clean({
                ":s": status,
                ":e": _iso(),
                ":d": Decimal(str(round((_now() - started).total_seconds(), 2))),
                ":m": metrics,
                ":err": error[:4000],
            }),
        )

    # ---------- chong xu ly trung ----------

    def claim_trigger(self, job_name: str, manifest: str, run_id: str) -> bool:
        """True nếu lần này giành được quyền xử lý manifest; False nếu đã có người giành."""
        try:
            self._table.put_item(
                Item=_clean({
                    "job_name": job_name,
                    "run_key": trigger_key(manifest),
                    "manifest": manifest,
                    "run_id": run_id,
                    "status": "QUEUED",
                    "claimed_at": _iso(),
                    "expires_at": _ttl(),
                }),
                # Cho phep gianh lai neu lan truoc FAILED — de chay lai sau khi sua loi
                ConditionExpression="attribute_not_exists(run_key) OR #s = :failed",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":failed": "FAILED"},
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def mark_trigger(self, job_name: str, manifest: str, status: str, run_id: str) -> None:
        self._table.update_item(
            Key={"job_name": job_name, "run_key": trigger_key(manifest)},
            UpdateExpression="SET #s = :s, run_id = :r, updated_at = :u",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": status, ":r": run_id, ":u": _iso()},
        )

    def get_trigger(self, job_name: str, manifest: str) -> Optional[Dict[str, Any]]:
        r = self._table.get_item(Key={"job_name": job_name, "run_key": trigger_key(manifest)})
        return r.get("Item")
