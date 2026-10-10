"""Hai bảng DynamoDB: job config và job log.

    job_config   — cấu hình tĩnh của từng job. Terraform seed từ YAML trong
                   git mỗi lần apply. Lambda điều phối đọc bảng này để biết
                   job nào đang bật và đến hạn chạy.

    job_log      — mọi thứ động. Một bảng, ba loại bản ghi phân biệt bằng
                   tiền tố sort key:

                     RUN#<iso>#<run_id>    vòng đời một lần chạy
                     CKPT#<iso>#<run_id>   ảnh chụp tiến độ (lịch sử)
                     CKPT#LATEST           mốc hiện hành  <- checkpoint

Checkpoint lấy từ lần chạy thành công gần nhất, đúng như thiết kế đã chốt.
Điểm bổ sung quan trọng: job dài **ghi bản ghi PARTIAL sau mỗi N lô**, nên
tiến độ vẫn nhích lên kể cả khi cả lần chạy cuối cùng thất bại.

Vì sao cần điểm bổ sung đó: không có nó, một job gặp backlog lớn hơn timeout
sẽ đọc 80% rồi chết, lần sau bắt đầu lại từ đúng điểm cũ, lại chết — **lặp vô
hạn, không bao giờ tiến**, và raw phình lên vì mỗi lần đều ghi lại phần trùng.

TTL: chỉ bản ghi `RUN#` và `CKPT#<iso>` mang thuộc tính `expires_at`.
`CKPT#LATEST` **cố ý không có** — DynamoDB chỉ xoá item nào CÓ thuộc tính TTL
đã quá hạn, nên mốc hiện hành sống mãi trong khi nhật ký tự dọn sau 90 ngày.
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Key

LOG_TTL_DAYS = 90
CKPT_LATEST_SK = "CKPT#LATEST"

# Ghi ảnh chụp tiến độ sau mỗi ngần này lô. Nhỏ quá thì tốn write; lớn quá
# thì job chết mất nhiều tiến độ. 20 lô ~ vài trăm nghìn bản ghi là cân bằng.
PARTIAL_EVERY_N_BATCHES = 20


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _iso(ts: Optional[dt.datetime] = None) -> str:
    return (ts or _now()).isoformat(timespec="seconds")


def _ttl(days: int = LOG_TTL_DAYS) -> int:
    return int(time.time()) + days * 86400


def _clean(node: Any) -> Any:
    """Chuẩn hoá kiểu cho DynamoDB.

    DynamoDB không nhận float (mất chính xác) và không nhận chuỗi rỗng trong
    một số ngữ cảnh cũ. Chuyển float -> Decimal và bỏ giá trị None.
    """
    if isinstance(node, float):
        return Decimal(str(node))
    if isinstance(node, dict):
        return {k: _clean(v) for k, v in node.items() if v is not None}
    if isinstance(node, list):
        return [_clean(v) for v in node]
    return node


def _plain(node: Any) -> Any:
    """Ngược lại: Decimal -> int/float để code Python dùng bình thường."""
    if isinstance(node, Decimal):
        return int(node) if node % 1 == 0 else float(node)
    if isinstance(node, dict):
        return {k: _plain(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_plain(v) for v in node]
    return node


# ============================================================
# BẢNG 1 — JOB CONFIG
# ============================================================


class JobConfigStore:
    """Đọc cấu hình job lúc chạy.

    Git vẫn là nguồn chân lý: Terraform seed bảng này từ `sourcing_etl/
    sources/*.yml` mỗi lần apply. Bảng chỉ là **bản đọc** cho runtime.

    Đừng sửa item trực tiếp bằng tay — lần apply sau sẽ ghi đè, và thay đổi
    đó không có trong git nên không ai review được.
    """

    def __init__(self, table_name: str, client=None):
        self.table_name = table_name
        self._table = (client or boto3.resource("dynamodb")).Table(table_name)

    @staticmethod
    def _decode(item: Dict[str, Any]) -> Dict[str, Any]:
        """Bóc `config` từ chuỗi JSON thành dict.

        Cấu hình được lưu thành MỘT chuỗi JSON thay vì map lồng nhau của
        DynamoDB. Lý do thuần thực dụng: Terraform không có hàm chuyển dict
        sang định dạng attribute-value của DynamoDB, nên map lồng nhau phải
        viết tay từng kiểu (`{"S": ...}`, `{"N": ...}`, `{"M": {...}}`) —
        dài dòng và dễ sai.

        Những trường mà Lambda điều phối cần lọc (`enabled`,
        `schedule_expression`) vẫn nằm ở cấp cao nhất để truy vấn được.
        """
        out = _plain(dict(item))
        raw = out.get("config")
        if isinstance(raw, str):
            import json

            try:
                out["config"] = json.loads(raw)
            except ValueError:
                out["config"] = {}
        return out

    def get(self, job_name: str) -> Optional[Dict[str, Any]]:
        item = self._table.get_item(Key={"job_name": job_name}).get("Item")
        return self._decode(item) if item else None

    def list_enabled(self) -> List[Dict[str, Any]]:
        """Mọi job đang bật.

        Dùng Scan chứ không Query: bảng này có vài chục item, mỗi item vài KB.
        Một lần scan tốn chưa tới 1 RCU. Dựng GSI cho quy mô này là phức tạp
        hoá không cần thiết.
        """
        items: List[Dict[str, Any]] = []
        kwargs: Dict[str, Any] = {
            "FilterExpression": "enabled = :t",
            "ExpressionAttributeValues": {":t": True},
        }
        while True:
            resp = self._table.scan(**kwargs)
            items.extend(self._decode(i) for i in resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                return items
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    def mark_dispatched(self, job_name: str, run_id: str) -> None:
        """Ghi lại thời điểm điều phối gần nhất.

        Lambda fan-out dùng giá trị này để biết job đã đến hạn chưa. Ghi ở
        đây chứ không ở job_log vì nó là trạng thái điều phối, không phải
        kết quả chạy — job được điều phối nhưng Glue từ chối khởi động thì
        vẫn tính là đã điều phối.
        """
        self._table.update_item(
            Key={"job_name": job_name},
            UpdateExpression="SET last_dispatched_at = :t, last_dispatch_run_id = :r",
            ExpressionAttributeValues={":t": _iso(), ":r": run_id},
        )


# ============================================================
# BẢNG 2 — JOB LOG (kiêm luôn checkpoint)
# ============================================================


@dataclass
class Checkpoint:
    """Mốc hiện hành, đọc từ bản ghi `CKPT#LATEST`.

    `watermarks` là map partition -> giá trị. Nguồn API/JDBC/S3 chỉ có khoá
    "default"; Kafka có một khoá cho mỗi partition, nên một topic 12 partition
    vẫn gọn trong đúng một item.

    Gói cả map vào một bản ghi thay vì mỗi partition một dòng giúp việc đọc
    checkpoint luôn là một GetItem duy nhất, không phụ thuộc số partition.
    """

    job_name: str
    watermarks: Dict[str, str] = field(default_factory=dict)
    run_id: str = ""
    updated_at: str = ""
    source_status: str = ""  # SUCCESS hoac PARTIAL — lan ghi nao sinh ra moc nay

    def get(self, partition: str = "default") -> Optional[str]:
        return self.watermarks.get(partition)

    @property
    def is_empty(self) -> bool:
        return not self.watermarks


@dataclass
class RunStats:
    records_read: int = 0
    records_written: int = 0
    bytes_written: int = 0
    files_written: int = 0
    batches: int = 0
    pages_fetched: int = 0


class JobLogStore:
    def __init__(self, table_name: str, client=None):
        self.table_name = table_name
        self._table = (client or boto3.resource("dynamodb")).Table(table_name)

    @staticmethod
    def new_run_id() -> str:
        return uuid.uuid4().hex[:16]

    # ---------- checkpoint ----------

    def read_checkpoint(self, job_name: str) -> Checkpoint:
        """Một GetItem. Không quét, không lọc, không phụ thuộc số lần chạy hỏng.

        Đây là lý do dùng sort key cố định `CKPT#LATEST` thay vì truy vấn
        "run SUCCESS gần nhất": một job hỏng liên tục 500 lần thì truy vấn có
        lọc sẽ phải đọc qua 500 bản ghi FAILED mới tới bản ghi cần tìm.
        """
        item = self._table.get_item(
            Key={"job_name": job_name, "run_key": CKPT_LATEST_SK}
        ).get("Item")
        if not item:
            return Checkpoint(job_name=job_name)
        return Checkpoint(
            job_name=job_name,
            watermarks={k: str(v) for k, v in (item.get("watermarks") or {}).items()},
            run_id=item.get("run_id", ""),
            updated_at=item.get("updated_at", ""),
            source_status=item.get("source_status", ""),
        )

    def advance_checkpoint(
        self, job_name: str, run_id: str, watermarks: Dict[str, str], status: str
    ) -> None:
        """Tiến mốc. CHỈ gọi với status PARTIAL hoặc SUCCESS.

        Ghi hai bản ghi:
          - `CKPT#LATEST`  bị ghi đè, không TTL  -> mốc hiện hành
          - `CKPT#<iso>`   bản ghi mới, có TTL   -> lịch sử tiến độ

        Bản lịch sử cho phép trả lời "tối qua job tiến tới đâu" khi điều tra,
        mà không phải giữ mãi mọi ảnh chụp.

        Merge chứ không thay thế: job Kafka có thể chỉ tiến được vài partition
        trong một lô: ghi đè cả map sẽ **xoá mốc của những partition khác**.
        """
        if status not in ("PARTIAL", "SUCCESS"):
            raise ValueError(f"advance_checkpoint chi nhan PARTIAL/SUCCESS, nhan '{status}'")

        current = self.read_checkpoint(job_name)
        merged = {**current.watermarks, **{k: str(v) for k, v in watermarks.items()}}
        now = _iso()

        self._table.put_item(Item=_clean({
            "job_name": job_name,
            "run_key": CKPT_LATEST_SK,
            "watermarks": merged,
            "run_id": run_id,
            "updated_at": now,
            "source_status": status,
            # KHONG co expires_at -> TTL khong dong toi
        }))

        # Khoa lich su chinh xac toi micro giay: nhieu anh chup PARTIAL trong
        # cung mot giay cua cung lan chay se ghi de len nhau neu chi toi giay.
        history_ts = _now().isoformat(timespec="microseconds")
        self._table.put_item(Item=_clean({
            "job_name": job_name,
            "run_key": f"CKPT#{history_ts}#{run_id}",
            "watermarks": merged,
            "run_id": run_id,
            "updated_at": now,
            "source_status": status,
            "expires_at": _ttl(),
        }))

    def reset_checkpoint(self, job_name: str) -> None:
        """Xoá mốc để buộc job kéo lại từ đầu."""
        self._table.delete_item(Key={"job_name": job_name, "run_key": CKPT_LATEST_SK})

    # ---------- vòng đời một lần chạy ----------

    def start_run(self, job_name: str, run_id: str, config_summary: Dict[str, Any]) -> str:
        """Ghi bản ghi RUNNING ngay lúc bắt đầu.

        Ghi ngay chứ không đợi kết thúc: job bị Glue giết vì hết timeout vẫn
        để lại dấu vết — đó chính là những lần chạy cần điều tra nhất.
        """
        run_key = f"RUN#{_iso()}#{run_id}"
        self._table.put_item(Item=_clean({
            "job_name": job_name,
            "run_key": run_key,
            "run_id": run_id,
            "status": "RUNNING",
            "started_at": _iso(),
            "host": socket.gethostname(),
            "glue_job_run_id": os.environ.get("GLUE_RUN_ID", ""),
            "config": config_summary,
            "expires_at": _ttl(),
        }))
        return run_key

    def finish_run(
        self,
        job_name: str,
        run_key: str,
        status: str,
        stats: RunStats,
        watermarks: Optional[Dict[str, str]] = None,
        error: str = "",
    ) -> None:
        started = dt.datetime.fromisoformat(run_key.split("#")[1])
        duration = (_now() - started).total_seconds()

        self._table.update_item(
            Key={"job_name": job_name, "run_key": run_key},
            UpdateExpression=(
                "SET #s = :s, ended_at = :e, duration_seconds = :d, "
                "records_read = :rr, records_written = :rw, bytes_written = :bw, "
                "files_written = :fw, batches = :b, err = :err, watermarks = :wm"
            ),
            # "status" la tu khoa danh rieng cua DynamoDB -> phai dat bi danh
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=_clean({
                ":s": status,
                ":e": _iso(),
                ":d": Decimal(str(round(duration, 2))),
                ":rr": stats.records_read,
                ":rw": stats.records_written,
                ":bw": stats.bytes_written,
                ":fw": stats.files_written,
                ":b": stats.batches,
                ":err": error[:4000],  # DynamoDB gioi han 400KB moi item
                ":wm": watermarks or {},
            }),
        )

    def record_orch_result(
        self, job_name: str, run_id: str, status: str, manifest: Optional[str] = None
    ) -> bool:
        """Báo kết quả nghiệp vụ về bản ghi `ORCH#<run_id>` của orchestrator.

        Step Functions chỉ biết Glue SUCCEEDED hay không, không biết lần chạy
        có dữ liệu không và manifest nằm đâu — mà luồng kế tiếp (silver) cần
        đúng hai thứ đó. Job ghi vào đây, Step Functions đọc lại ở bước
        RecordSuccess rồi mới quyết định có bắn event cho luồng sau.

        Chỉ cập nhật khi bản ghi ĐÃ tồn tại (Lambda trigger tạo trước lúc
        khởi động). Chạy tay hay chạy qua dispatcher cũ thì không có ORCH#,
        trả False và không để lại item mồ côi.
        """
        from botocore.exceptions import ClientError

        expr = "SET run_status = :s, run_finished_at = :t"
        values: Dict[str, Any] = {":s": status, ":t": _iso()}
        if manifest:
            expr += ", manifest = :m"
            values[":m"] = manifest
        try:
            self._table.update_item(
                Key={"job_name": job_name, "run_key": f"ORCH#{run_id}"},
                UpdateExpression=expr,
                ConditionExpression="attribute_exists(run_key)",
                ExpressionAttributeValues=values,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def recent_runs(self, job_name: str, limit: int = 20) -> List[Dict[str, Any]]:
        resp = self._table.query(
            KeyConditionExpression=(
                Key("job_name").eq(job_name) & Key("run_key").begins_with("RUN#")
            ),
            ScanIndexForward=False,  # moi nhat truoc
            Limit=limit,
        )
        return [_plain(i) for i in resp.get("Items", [])]

    def last_success(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Lần chạy thành công gần nhất, dùng khi điều tra sự cố."""
        for item in self.recent_runs(job_name, limit=100):
            if item.get("status") == "SUCCESS":
                return item
        return None
