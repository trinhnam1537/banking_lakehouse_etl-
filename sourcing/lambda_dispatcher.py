"""Lambda fan-out: một lịch duy nhất → N job.

Vị trí trong luồng:

    EventBridge Scheduler (1 lich, 5 phut/lan)
        └─> SQS dispatch-queue
              └─> Lambda NAY
                    · quet bang job_config lay job dang bat
                    · loc ra job den han (theo schedule + last_dispatched_at)
                    · ban N message vao SQS run-queue
                          └─> EventBridge Pipes
                                └─> Step Functions (moi job mot execution)

Vì sao có Lambda này thay vì mỗi job một lịch EventBridge: thêm một job chỉ
là thêm một dòng trong bảng config, **không cần tạo resource mới**. Và toàn
bộ quyết định "job nào chạy lúc nào" nằm ở một chỗ đọc được, thay vì rải ra
hàng chục lịch riêng lẻ.

Lambda này cố ý **không** khởi động Glue job. Nó chỉ quyết định *cái gì* cần
chạy; *chạy thế nào* là việc của Step Functions. Tách vậy để khi một job
hỏng, chỗ cần xem là execution của nó chứ không phải log của một Lambda chung.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

import boto3

_RATE = re.compile(r"^rate\((\d+)\s+(minute|minutes|hour|hours|day|days)\)$")
_CRON = re.compile(r"^cron\((.+)\)$")

_MINUTES = {"minute": 1, "minutes": 1, "hour": 60, "hours": 60, "day": 1440, "days": 1440}

# Thu trong tuan theo cu phap EventBridge: ten, hoac so 1-7 voi 1 = SUN.
# Doi ve weekday() cua Python: MON = 0 ... SUN = 6.
_DOW_NAMES = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


@dataclass(frozen=True)
class CronSpec:
    """Tập con cron EventBridge mà dispatcher hiểu: chạy một lần vào H:M UTC
    trong những ngày khớp `days_of_month` và `days_of_week` (None = mọi ngày).
    """

    minute: int
    hour: int
    days_of_month: Optional[FrozenSet[int]] = None
    days_of_week: Optional[FrozenSet[int]] = None

    def matches_day(self, day: dt.date) -> bool:
        if self.days_of_month is not None and day.day not in self.days_of_month:
            return False
        if self.days_of_week is not None and day.weekday() not in self.days_of_week:
            return False
        return True


def _dow_value(token: str) -> Optional[int]:
    if token in _DOW_NAMES:
        return _DOW_NAMES[token]
    if token.isdigit() and 1 <= int(token) <= 7:
        return (int(token) - 2) % 7  # 1=SUN -> 6, 2=MON -> 0
    return None


def _parse_dow(field: str) -> Optional[FrozenSet[int]]:
    out = set()
    for part in field.upper().split(","):
        lo, sep, hi = part.partition("-")
        a = _dow_value(lo)
        b = _dow_value(hi) if sep else a
        if a is None or b is None:
            return None
        # Cho phep vong qua cuoi tuan: FRI-MON
        out.update((a + i) % 7 for i in range((b - a) % 7 + 1))
    return frozenset(out)


def parse_cron(expression: str) -> Optional[CronSpec]:
    """Phân tích `cron(M H DOM MON DOW YEAR)` dạng một lần mỗi ngày được chọn.

    Hỗ trợ: phút và giờ là số; ngày-trong-tháng `*`/`?`/danh sách số; tháng và
    năm `*`; thứ `*`/`?`/tên/số/khoảng/danh sách (`MON-FRI`, `SUN`, `MON,WED`).
    Ngoài tập đó (bước nhảy `0/15`, `L`, `W`, `#`) trả None — thà báo "không hỗ
    trợ" còn hơn đoán sai và chạy job vào ngày không mong muốn.
    """
    m = _CRON.match(expression.strip())
    if not m:
        return None
    fields = m.group(1).split()
    if len(fields) != 6:
        return None
    minute, hour, dom, month, dow, year = fields
    if not (minute.isdigit() and hour.isdigit()) or month != "*" or year != "*":
        return None

    dom_set: Optional[FrozenSet[int]] = None
    if dom not in ("*", "?"):
        parts = dom.split(",")
        if not all(x.isdigit() and 1 <= int(x) <= 31 for x in parts):
            return None
        dom_set = frozenset(int(x) for x in parts)

    dow_set: Optional[FrozenSet[int]] = None
    if dow not in ("*", "?"):
        dow_set = _parse_dow(dow)
        if dow_set is None:
            return None

    return CronSpec(int(minute), int(hour), dom_set, dow_set)


def parse_interval_minutes(expression: str) -> Optional[int]:
    """Chu kỳ gần đúng của lịch, tính bằng phút. None = dispatcher không hỗ trợ.

    `rate(N unit)` là chính xác. Với cron chỉ là chu kỳ danh nghĩa (ngày /
    tuần / tháng) — quyết định chạy thật nằm ở `CronSpec.matches_day`.
    Lịch không hỗ trợ thì job đó phải giữ lịch EventBridge riêng.
    """
    m = _RATE.match(expression.strip())
    if m:
        return int(m.group(1)) * _MINUTES[m.group(2)]
    spec = parse_cron(expression)
    if spec is None:
        return None
    if spec.days_of_month is not None:
        return 43200  # ~30 ngay
    if spec.days_of_week is not None and len(spec.days_of_week) == 1:
        return 10080
    return 1440


def is_due(job: Dict[str, Any], now: Optional[dt.datetime] = None) -> Tuple[bool, str]:
    """Job này đã đến hạn chạy chưa? Trả kèm lý do để log đọc được."""
    now = now or _now()
    # Ưu tiên trường ở cấp cao nhất (Terraform seed riêng để lọc được bằng
    # DynamoDB), rồi mới rơi về khối schedule trong config.
    schedule = (job.get("config") or {}).get("schedule") or {}
    enabled = job.get("schedule_enabled", schedule.get("enabled", False))
    expression = job.get("schedule_expression", schedule.get("expression", ""))

    if not enabled:
        return False, "schedule.enabled=false"
    interval = parse_interval_minutes(expression)
    if interval is None:
        return False, f"lich '{expression}' khong ho tro fan-out (dung lich rieng)"

    last = job.get("last_dispatched_at")
    if not last:
        return True, "chua tung chay"

    try:
        last_dt = dt.datetime.fromisoformat(str(last))
    except ValueError:
        return True, "last_dispatched_at hong -> chay lai"
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=dt.timezone.utc)

    elapsed = (now - last_dt).total_seconds() / 60.0

    # Với cron, chỉ chạy vào ngày khớp lịch VÀ đã qua giờ mục tiêu — tránh
    # việc job "02:00 hằng ngày" chạy lúc 14:00 chỉ vì đã đủ 1440 phút, hay
    # job "chủ nhật hằng tuần" chạy mỗi ngày.
    #
    # Hệ quả có chủ ý: dispatcher ngừng suốt ngày khớp lịch (Lambda lỗi cả
    # ngày chủ nhật) thì job hằng tuần lỡ tuần đó, không chạy bù vào thứ hai.
    spec = parse_cron(expression)
    if spec is not None:
        hm = f"{spec.hour:02d}:{spec.minute:02d} UTC"
        if elapsed < 60:
            return False, f"moi chay {elapsed:.0f} phut truoc"
        if not spec.matches_day(now.date()):
            return False, f"hom nay khong nam trong lich '{expression}'"
        target_passed = (now.hour, now.minute) >= (spec.hour, spec.minute)
        same_day = last_dt.date() == now.date()
        if target_passed and not same_day:
            return True, f"den gio {hm}"
        return False, f"chua den gio {hm}"

    # Trừ 30 giây dung sai: lịch 5 phút hiếm khi khớp đúng từng giây, không
    # có dung sai thì cứ hai lần lại trượt một lần.
    if elapsed >= interval - 0.5:
        return True, f"da {elapsed:.1f} phut >= {interval} phut"
    return False, f"moi {elapsed:.1f}/{interval} phut"


def handler(event, context):  # noqa: ANN001, ARG001
    config_table = os.environ["JOB_CONFIG_TABLE"]
    run_queue_url = os.environ["RUN_QUEUE_URL"]
    force = str(os.environ.get("FORCE_ALL", "")).lower() == "true"

    from .state import JobConfigStore

    store = JobConfigStore(config_table)
    sqs = boto3.client("sqs")

    # Message của EventBridge có thể yêu cầu chạy riêng vài job — tiện khi
    # muốn chạy lại tay mà không đợi tới lịch.
    only: List[str] = []
    for record in (event or {}).get("Records", []):
        try:
            body = json.loads(record.get("body") or "{}")
            only.extend(body.get("jobs", []))
        except (ValueError, TypeError):
            continue

    jobs = store.list_enabled()
    dispatched, skipped = [], []

    for job in jobs:
        name = job["job_name"]
        if only and name not in only:
            continue

        if only or force:
            due, reason = True, "yeu cau chay tay" if only else "FORCE_ALL"
        else:
            due, reason = is_due(job)

        if not due:
            skipped.append({"job": name, "reason": reason})
            continue

        run_id = uuid.uuid4().hex[:16]
        sqs.send_message(
            QueueUrl=run_queue_url,
            MessageBody=json.dumps({
                "job_name": name,
                "run_id": run_id,
                "dispatched_at": _now().isoformat(timespec="seconds"),
                "reason": reason,
            }),
            # Khử trùng lặp trong 5 phút: nếu vì lý do nào đó Lambda được gọi
            # hai lần cho cùng một tick, job vẫn chỉ chạy một lần.
            MessageGroupId=name,
            MessageDeduplicationId=f"{name}-{_now().strftime('%Y%m%d%H%M')}",
        )
        # Đánh dấu NGAY sau khi gửi. Đánh dấu trước khi gửi thì message hỏng
        # sẽ làm job im lặng bỏ lỡ cả chu kỳ.
        store.mark_dispatched(name, run_id)
        dispatched.append({"job": name, "run_id": run_id, "reason": reason})

    print(json.dumps({
        "dispatched": dispatched,
        "skipped": skipped,
        "total_enabled": len(jobs),
    }, ensure_ascii=False))

    return {"dispatched": len(dispatched), "skipped": len(skipped)}
