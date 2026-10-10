"""Điểm vào của Glue job và của chạy tay.

Ba nguồn cấu hình, theo thứ tự ưu tiên:

  --config <duong/dan.yml>          đọc từ file      (chay tay, kiem thu)
  --CONFIG_SNAPSHOT <b64>           ảnh chụp do Lambda trigger đã đọc, áp
                                    override và kiểm tra — lần chạy dùng
                                    ĐÚNG cấu hình đã được duyệt, kể cả khi
                                    bảng config đổi giữa chừng
  --JOB_CONFIG_TABLE + --JOB_NAME   đọc từ DynamoDB  (Glue job chạy thật)

Cả ba đi qua cùng một hàm `from_dict()` nên cùng một bộ kiểm tra.

Glue truyền tham số dạng `--KEY value` và tự chen thêm vài tham số của riêng
nó, nên dùng `parse_known_args`. Tham số VIẾT HOA không nhận ra được đẩy vào
biến môi trường — đó là cách entry YAML tham chiếu `${KAFKA_BOOTSTRAP}` được,
vì Glue không cho đặt biến môi trường tuỳ ý cho job.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

from .config import SourceConfig, decode_snapshot, from_dict, load_config
from .runner import SourceRunner


def _promote_to_env(unknown_args) -> None:
    """Biến tham số `--TEN_HOA value` thành biến môi trường `TEN_HOA`.

    Chỉ nhận tham số VIẾT HOA để không nuốt nhầm cờ nội bộ của Glue
    (`--job-bookmark-option`, `--enable-metrics`...).
    """
    i = 0
    promoted = []
    while i < len(unknown_args):
        token = unknown_args[i]
        if token.startswith("--") and token[2:].replace("_", "").isupper():
            key = token[2:]
            if i + 1 < len(unknown_args) and not unknown_args[i + 1].startswith("--"):
                os.environ[key] = unknown_args[i + 1]
                promoted.append(key)
                i += 2
                continue
        i += 1
    if promoted:
        print(f"Tham so -> bien moi truong: {', '.join(promoted)}")


def _load_from_dynamo(table: str, job_name: str) -> SourceConfig:
    from .state import JobConfigStore

    item = JobConfigStore(table).get(job_name)
    if item is None:
        raise SystemExit(
            f"Khong thay job '{job_name}' trong bang {table}. "
            f"Da `terraform apply` sau khi them entry YAML chua?"
        )
    # Cấu hình nằm ở khoá `config`; các khoá còn lại (enabled, last_dispatched_at)
    # là siêu dữ liệu điều phối, không thuộc lược đồ SourceConfig.
    raw = dict(item.get("config") or {})  # JobConfigStore da boc chuoi JSON
    raw.setdefault("name", job_name)
    raw["enabled"] = item.get("enabled", True)
    return from_dict(raw, default_name=job_name)


def _resolve(env_key: str, arg_val: Optional[str], what: str) -> str:
    val = arg_val or os.environ.get(env_key)
    if not val:
        raise SystemExit(f"Thieu {what}. Truyen --{env_key} hoac dat bien moi truong {env_key}.")
    return val


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Keo du lieu tu nguon xuong tang raw")
    p.add_argument("--config", help="Duong dan file YAML tren may (chay tay)")
    p.add_argument("--JOB_NAME", dest="job_name")
    p.add_argument("--JOB_CONFIG_TABLE", dest="config_table")
    p.add_argument("--JOBLOG_TABLE", dest="joblog_table")
    p.add_argument("--RAW_BUCKET", dest="raw_bucket")
    # Step Functions sinh run_id va truyen vao, de ban ghi ma SFN cap nhat
    # luc ket thuc trung dung ban ghi ma job da tao luc bat dau.
    p.add_argument("--RUN_ID", dest="run_id")
    p.add_argument("--CONFIG_SNAPSHOT", dest="config_snapshot",
                   help="Cau hinh da nen (sourcing.config.encode_snapshot), do Lambda trigger truyen")
    p.add_argument("--dry-run", action="store_true",
                   help="Doc nguon va dem ban ghi nhung KHONG ghi S3, KHONG tien moc")
    # Glue chi truyen cap `--KEY value`, khong co co khong gia tri nhu --dry-run
    p.add_argument("--DRY_RUN", dest="dry_run_flag", default="false")

    args, unknown = p.parse_known_args(argv)
    _promote_to_env(unknown)
    dry_run = args.dry_run or str(args.dry_run_flag).lower() == "true"

    if args.config:
        cfg = load_config(args.config)
    elif args.config_snapshot:
        cfg = from_dict(decode_snapshot(args.config_snapshot), default_name=args.job_name or "")
        print(f"[{cfg.name}] cau hinh tu snapshot cua Lambda trigger")
    elif args.config_table and args.job_name:
        cfg = _load_from_dynamo(args.config_table, args.job_name)
    else:
        raise SystemExit("Phai truyen --config, hoac ca --JOB_CONFIG_TABLE va --JOB_NAME")

    if not cfg.enabled:
        # Tắt bằng `enabled: false` thay vì xoá job: job vẫn tồn tại, vẫn có
        # lịch sử, chỉ là không làm gì. Bật lại là sửa một dòng.
        print(f"[{cfg.name}] enabled=false -> bo qua, khong lam gi.")
        return 0

    runner = SourceRunner(
        cfg=cfg,
        raw_bucket=_resolve("RAW_BUCKET", args.raw_bucket, "raw_bucket"),
        joblog_table=_resolve("JOBLOG_TABLE", args.joblog_table, "joblog_table"),
        run_id=args.run_id,
        dry_run=dry_run,
    )
    result = runner.run()
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
