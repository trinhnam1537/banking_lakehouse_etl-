"""Entry YAML -> bảng job_config. Thay `aws_dynamodb_table_item` của Terraform.

Item giữ ĐÚNG hình dạng Terraform từng seed (Lambda trigger, dispatcher, silver
router và Glue đều đọc nó), nên chuyển chủ sở hữu không phải sửa phía đọc.

Ba quy tắc:

  1. Kiểm tra TRƯỚC khi ghi bằng đúng from_dict() mà Glue/Lambda dùng. Một
     entry sai -> không ghi gì cả (CI đỏ), không có trạng thái nửa vời.
  2. UpdateItem chỉ các thuộc tính mình sở hữu. Thuộc tính khác trên item
     (last_dispatched_at của dispatcher cũ...) được giữ nguyên.
  3. Entry bị xoá khỏi repo -> enabled = false, KHÔNG xoá item: còn lịch sử,
     và Lambda trigger trả SKIPPED thay vì "không tìm thấy".

So sánh theo NGỮ NGHĨA (config giải mã ra dict), không theo chuỗi: Terraform
và Python tuần tự hoá JSON khác nhau, so chuỗi thì lần nào cũng thấy "đổi".
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

ConfigFields = Dict[str, Any]


@dataclass
class SyncReport:
    table: str
    created: List[str] = field(default_factory=list)
    updated: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    disabled: List[str] = field(default_factory=list)

    def summary(self) -> str:
        return (f"{self.table}: +{len(self.created)} tao, ~{len(self.updated)} sua, "
                f"={len(self.unchanged)} giu, -{len(self.disabled)} tat")


# ---------- đọc YAML ----------


def _read_yaml_tree(directory: str) -> Dict[str, Dict[str, Any]]:
    import yaml

    out: Dict[str, Dict[str, Any]] = {}
    for root, dirs, files in os.walk(directory):
        dirs.sort()
        for fname in sorted(files):
            if fname.endswith((".yml", ".yaml")):
                with open(os.path.join(root, fname), encoding="utf-8") as f:
                    out[os.path.splitext(fname)[0]] = yaml.safe_load(f) or {}
    return out


def _config_json(raw: Dict[str, Any]) -> str:
    # default=str: YAML tu doi `2026-01-01` thanh date
    return json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)


def _sha(raw: Dict[str, Any]) -> str:
    return hashlib.sha256(_config_json(raw).encode("utf-8")).hexdigest()


def load_sourcing(directory: str) -> Dict[str, ConfigFields]:
    """Item cho bảng src job_config. load_all() kiểm tra trùng tên, name = tên file."""
    from sourcing.config import load_all

    load_all(directory)  # nem ConfigError neu bat ky entry nao sai
    items: Dict[str, ConfigFields] = {}
    for name, raw in _read_yaml_tree(directory).items():
        sched = raw.get("schedule") or {}
        items[name] = {
            "enabled": bool(raw.get("enabled", True)),
            "source_type": raw.get("source_type", "unknown"),
            "cdc_mode": (raw.get("cdc") or {}).get("mode", "cdc1"),
            "schedule_enabled": bool(sched.get("enabled", False)),
            "schedule_expression": sched.get("expression", ""),
            "owner": raw.get("owner", ""),
            "description": raw.get("description", ""),
            "config": _config_json(raw),
            "config_sha": _sha(raw),
        }
    return items


def load_silver(directory: str) -> Dict[str, ConfigFields]:
    from silver.config import load_all

    load_all(directory)
    items: Dict[str, ConfigFields] = {}
    for name, raw in _read_yaml_tree(directory).items():
        target = raw.get("target") or {}
        items[name] = {
            "enabled": bool(raw.get("enabled", True)),
            "source_dataset": (raw.get("source") or {}).get("dataset", ""),
            "writer": (raw.get("writer") or {}).get("type", "simple"),
            "target_table": f"{target.get('database', 'track_a_silver')}.{target.get('table', '')}",
            "owner": raw.get("owner", ""),
            "description": raw.get("description", ""),
            "config": _config_json(raw),
            "config_sha": _sha(raw),
        }
    return items


# ---------- ghi ----------


def _same(current: Dict[str, Any], desired: ConfigFields) -> bool:
    for k, v in desired.items():
        if k == "config_sha":
            continue  # sha cua Terraform tinh khac -> khong dung de so
        if k == "config":
            try:
                if json.loads(current.get("config") or "{}") != json.loads(v):
                    return False
            except ValueError:
                return False
        elif current.get(k) != v:
            return False
    return True


def sync(table_name: str, desired: Dict[str, ConfigFields], dry_run: bool = False,
         dynamodb=None, log: Callable[[str], None] = print) -> SyncReport:
    import boto3

    table = (dynamodb or boto3.resource("dynamodb")).Table(table_name)
    existing: Dict[str, Dict[str, Any]] = {}
    kwargs: Dict[str, Any] = {}
    while True:
        page = table.scan(**kwargs)
        for it in page.get("Items", []):
            existing[it["job_name"]] = it
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    report = SyncReport(table_name)

    def update(name: str, fields: ConfigFields) -> None:
        if dry_run:
            return
        names = {f"#f{i}": k for i, k in enumerate(fields)}
        values = {f":v{i}": v for i, v in enumerate(fields.values())}
        table.update_item(
            Key={"job_name": name},
            UpdateExpression="SET " + ", ".join(f"#f{i} = :v{i}" for i in range(len(fields))),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    for name, fields in sorted(desired.items()):
        cur = existing.get(name)
        if cur is None:
            update(name, fields)
            report.created.append(name)
        elif _same(cur, fields):
            report.unchanged.append(name)
        else:
            update(name, fields)
            report.updated.append(name)

    for name, cur in sorted(existing.items()):
        if name not in desired and cur.get("enabled", True):
            update(name, {"enabled": False})
            report.disabled.append(name)

    for kind in ("created", "updated", "disabled"):
        names = getattr(report, kind)
        if names:
            log(f"  {kind}: {', '.join(names)}")
    log(("[dry-run] " if dry_run else "") + report.summary())
    return report

