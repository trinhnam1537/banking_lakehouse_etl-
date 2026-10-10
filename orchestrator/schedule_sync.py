"""Lambda schedule-sync: bảng job_config -> EventBridge Schedule, mỗi entry một lịch.

    config-sync (CI repo ETL) ghi job_config
      └─▶ DynamoDB Streams (NEW_AND_OLD_IMAGES)
            └─▶ Lambda NÀY
                  · enabled && schedule_enabled && có expression
                        -> tạo/sửa  <group>/src-<job>  đích Lambda trigger
                  · ngược lại (tắt lịch, tắt entry, xoá item)
                        -> xoá lịch

Vì sao không để CI repo ETL gọi thẳng API Scheduler: CI sẽ cần quyền Scheduler
và iam:PassRole — đúng loại quyền không nên có ở repo chỉ chứa logic. Ở đây
quyền đó thuộc hàm do repo hạ tầng tạo, CI ETL chỉ cần ghi được bảng config.

Gọi tay {"action": "resync"}: quét cả bảng, đặt lại mọi lịch và xoá lịch mồ côi
(entry đã mất khỏi bảng). Dùng lần đầu bật, hoặc khi nghi lịch lệch bảng.

Lịch trong YAML tính theo giờ UTC (cron(0 19 ...) = 02:00 giờ VN).
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import ClientError

NAME_PREFIX = "src-"


def schedule_name(job_name: str) -> str:
    return (NAME_PREFIX + job_name)[:64]  # gioi han cua Scheduler


def desired_expression(item: Optional[Dict[str, Any]]) -> Optional[str]:
    """Biểu thức lịch nếu entry CẦN có lịch, None nếu không."""
    if not item:
        return None
    expr = (item.get("schedule_expression") or "").strip()
    if item.get("enabled", True) and item.get("schedule_enabled") and expr:
        return expr
    return None


class ScheduleSyncer:
    def __init__(self, group: str, target_arn: str, role_arn: str, scheduler=None):
        self.group = group
        self.target_arn = target_arn
        self.role_arn = role_arn
        self.scheduler = scheduler or boto3.client("scheduler")

    def _params(self, job_name: str, expression: str) -> Dict[str, Any]:
        return {
            "Name": schedule_name(job_name),
            "GroupName": self.group,
            "ScheduleExpression": expression,
            "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "State": "ENABLED",
            "Target": {
                "Arn": self.target_arn,
                "RoleArn": self.role_arn,
                # scheduled-time -> Lambda trigger suy ra run_id co dinh cho tick:
                # Scheduler goi lap cung tick van chi chay mot lan.
                "Input": json.dumps({
                    "job_id": f"src.{job_name}",
                    "trigger": {"type": "schedule",
                                "scheduled_time": "<aws.scheduler.scheduled-time>"},
                }),
                "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600},
            },
        }

    def apply(self, job_name: str, expression: Optional[str]) -> str:
        name = schedule_name(job_name)
        if expression is None:
            try:
                self.scheduler.delete_schedule(Name=name, GroupName=self.group)
                return "deleted"
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return "absent"
                raise
        params = self._params(job_name, expression)
        try:
            self.scheduler.update_schedule(**params)
            return "updated"
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        self.scheduler.create_schedule(**params)
        return "created"

    def existing_jobs(self) -> List[str]:
        out: List[str] = []
        kwargs: Dict[str, Any] = {"GroupName": self.group, "NamePrefix": NAME_PREFIX}
        while True:
            page = self.scheduler.list_schedules(**kwargs)
            out.extend(s["Name"][len(NAME_PREFIX):] for s in page.get("Schedules", []))
            if not page.get("NextToken"):
                return out
            kwargs["NextToken"] = page["NextToken"]


def _from_stream(records: Iterable[Dict[str, Any]]):
    """(job_name, item mới hoặc None) cho từng bản ghi stream, theo thứ tự."""
    from boto3.dynamodb.types import TypeDeserializer

    des = TypeDeserializer()
    for rec in records:
        ddb = rec.get("dynamodb") or {}
        keys = {k: des.deserialize(v) for k, v in (ddb.get("Keys") or {}).items()}
        new = ddb.get("NewImage")
        item = {k: des.deserialize(v) for k, v in new.items()} if new else None
        yield keys["job_name"], item


def resync(syncer: ScheduleSyncer, table_name: str, dynamodb=None) -> Dict[str, str]:
    table = (dynamodb or boto3.resource("dynamodb")).Table(table_name)
    items: Dict[str, Dict[str, Any]] = {}
    kwargs: Dict[str, Any] = {}
    while True:
        page = table.scan(**kwargs)
        for it in page.get("Items", []):
            items[it["job_name"]] = it
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    result = {name: syncer.apply(name, desired_expression(it)) for name, it in sorted(items.items())}
    for orphan in syncer.existing_jobs():
        if orphan not in items:
            result[orphan] = syncer.apply(orphan, None)
    return result


def handler(event, context=None):
    if os.environ.get("SCHEDULES_ENABLED", "false").lower() != "true":
        # Cong tac o repo ha tang (use_orchestrator_schedules). Tat thi dispatcher
        # cu dang lo lich — tao them lich o day se chay moi job HAI lan.
        print("SCHEDULES_ENABLED != true -> bo qua")
        return {"skipped": True}

    syncer = ScheduleSyncer(os.environ["SCHEDULE_GROUP"], os.environ["TRIGGER_ARN"],
                            os.environ["SCHEDULER_ROLE_ARN"])
    if (event or {}).get("action") == "resync":
        result = resync(syncer, os.environ["JOB_CONFIG_TABLE"])
    else:
        # Loi -> raise: event source mapping giao lai lo, khong mat thay doi nao
        result = {job: syncer.apply(job, desired_expression(item))
                  for job, item in _from_stream(event.get("Records", []))}
    print(json.dumps(result, ensure_ascii=False))
    return result
