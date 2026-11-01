"""Mỗi luồng là một JobType.

Lambda trigger không biết gì về sourcing hay silver. Nó chỉ hỏi JobType bốn
câu:

    get_item(name)                  job này có trong bảng config không
    prepare(name, item, req, run)   kiểm tra + dựng tên Glue job và tham số
    claim(name, req, run)           giành quyền chạy (chống trùng), nếu cần
    consumers_of(detail)            lần chạy vừa xong của luồng TRƯỚC thì
                                    luồng NÀY cần chạy những job nào

Thêm luồng gold sau này = thêm một lớp ở đây + một dòng trong biến môi
trường JOB_TYPES. Lambda trigger và Step Functions không phải sửa.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sourcing.state import JobConfigStore, JobLogStore

# Glue tu choi tham so job co gia tri rong -> "-" nghia la "khong co".
# silver.main cung quy uoc nay.
NONE = "-"

# Gia tri tham so Glue co gioi han do dai; snapshot vuot muc nay thi Glue tu
# doc config tu DynamoDB (chi duoc khi khong co override).
MAX_SNAPSHOT_CHARS = 4000


class TriggerError(ValueError):
    """Yêu cầu sai (job không có, override cấm, ...). Trả lại người gọi, KHÔNG retry."""


@dataclass
class RunRequest:
    """Một yêu cầu đã chuẩn hoá, bất kể đến từ lịch, gọi tay hay luồng trước."""

    job_id: str
    action: str = "run"  # run | validate | status
    run_id: Optional[str] = None
    overrides: Dict[str, Any] = field(default_factory=dict)
    chain: bool = True
    dry_run: bool = False
    force: bool = False
    allow_checkpoint_advance: bool = False
    wait_seconds: int = 0
    manifest: Optional[str] = None
    data_date: Optional[str] = None
    rebuild_from: Optional[str] = None
    trigger: Dict[str, Any] = field(default_factory=lambda: {"type": "manual"})


@dataclass
class Prepared:
    glue_job: str
    glue_args: Dict[str, str]
    config: Dict[str, Any]
    chain: bool
    notes: List[str] = field(default_factory=list)
    timeout_minutes: int = 60
    # Ten profile neu chay tren Glue job DUNG CHUNG; None = job rieng theo entry (cu)
    profile: Optional[str] = None


class JobType:
    code = ""
    default_profile = ""

    def __init__(self, config_table: str, log_table: str, glue_prefix: str,
                 dynamodb=None, profile_jobs: Optional[Dict[str, str]] = None):
        self.config_table = config_table
        self.log_table = log_table
        self.glue_prefix = glue_prefix
        # profile -> ten Glue job dung chung, vd {"src-shell": "fingraph-dev-src-shell"}.
        # Repo ha tang khai trong JOB_TYPES. Profile chua co o day thi roi ve job
        # rieng theo entry (glue_prefix + ten) — giu chay duoc trong luc chuyen doi.
        self.profile_jobs = dict(profile_jobs or {})
        self.configs = JobConfigStore(config_table, client=dynamodb)
        self.log = JobLogStore(log_table, client=dynamodb)

    def glue_job_for(self, name: str, profile: str):
        """(ten Glue job, profile neu dung job chung / None neu job rieng)."""
        if profile in self.profile_jobs:
            return self.profile_jobs[profile], profile
        return f"{self.glue_prefix}{name}", None

    def get_item(self, name: str) -> Optional[Dict[str, Any]]:
        return self.configs.get(name)

    def _raw_config(self, name: str, item: Dict[str, Any]) -> Dict[str, Any]:
        raw = copy.deepcopy(item.get("config") or {})
        raw.setdefault("name", name)
        # Co `enabled` cap cao nhat (Terraform seed) quyet dinh, nhu sourcing.main
        raw["enabled"] = item.get("enabled", True)
        return raw

    def prepare(self, name: str, item: Dict[str, Any], req: RunRequest,
                run_id: str) -> Prepared:
        raise NotImplementedError

    def claim(self, name: str, req: RunRequest, run_id: str) -> bool:
        return True

    def release_claim(self, name: str, req: RunRequest, run_id: str) -> None:
        """Gọi khi đã claim nhưng không khởi động được execution."""

    def consumers_of(self, detail: Dict[str, Any]) -> List[RunRequest]:
        return []

    def status(self, name: str) -> Dict[str, Any]:
        return {}

    # ---------- ORCH#<run_id>: góc nhìn điều phối của một lần chạy ----------
    # Lambda tạo lúc kích, Glue job ghi run_status + manifest, Step Functions
    # ghi kết quả cuối. Cùng một item, ba bên, khoá biết trước.

    def _orch_key(self, name: str, run_id: str) -> Dict[str, str]:
        return {"job_name": name, "run_key": f"ORCH#{run_id}"}

    def orch_create(self, name: str, run_id: str, fields: Dict[str, Any]) -> bool:
        """False nếu run_id này đã được kích trước đó."""
        from botocore.exceptions import ClientError

        from sourcing.state import _clean, _ttl

        try:
            self.log._table.put_item(
                Item=_clean({**self._orch_key(name, run_id), "run_id": run_id,
                             "expires_at": _ttl(), **fields}),
                ConditionExpression="attribute_not_exists(run_key)",
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def orch_set(self, name: str, run_id: str, **fields: Any) -> None:
        names = {f"#k{i}": k for i, k in enumerate(fields)}
        values = {f":v{i}": v for i, v in enumerate(fields.values())}
        self.log._table.update_item(
            Key=self._orch_key(name, run_id),
            UpdateExpression="SET " + ", ".join(f"#k{i} = :v{i}" for i in range(len(fields))),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    def orch_get(self, name: str, run_id: str) -> Optional[Dict[str, Any]]:
        return self.log._table.get_item(Key=self._orch_key(name, run_id)).get("Item")

    def find_run(self, name: str, run_id: str) -> Optional[Dict[str, Any]]:
        """Bản ghi RUN# mà Glue job tự ghi, tìm theo run_id."""
        for item in self.log.recent_runs(name, limit=25):
            if item.get("run_id") == run_id:
                return item
        return None


# ============================================================
# SOURCING — nguồn -> landing/
# ============================================================


class SourcingType(JobType):
    code = "src"

    def prepare(self, name, item, req, run_id):
        from sourcing.config import (
            CdcMode,
            ConfigError,
            apply_overrides,
            encode_snapshot,
            from_dict,
        )

        if req.manifest or req.data_date or req.rebuild_from:
            raise TriggerError("manifest/data_date/rebuild_from chi dung cho job slv.*")

        notes: List[str] = []
        try:
            raw = apply_overrides(self._raw_config(name, item), req.overrides)
            cfg = from_dict(copy.deepcopy(raw), default_name=name)
        except ConfigError as e:
            raise TriggerError(f"cau hinh khong hop le: {e}") from None

        if cfg.runtime.profile == "edge":
            raise TriggerError(
                f"{name}: profile edge chay tren may nguon (systemd fingraph-kafka-sink), khong qua "
                f"Glue. Chay tay: aws ssm start-session ... roi `sudo systemctl start fingraph-kafka-sink`."
            )

        # Test voi override tren job CDC1 ma khong dry-run se TIEN CHECKPOINT
        # THAT bang du lieu cua lan chay "khac thuong" do (vd max_pages=1) —
        # lan chay theo lich sau do bo sot phan con lai ma khong bao loi gi.
        if (req.overrides and cfg.cdc.mode is CdcMode.CDC1 and not req.dry_run
                and not req.allow_checkpoint_advance):
            raise TriggerError(
                "override tren job cdc1 se tien checkpoint THAT. Dung \"dry_run\": true, "
                "hoac \"allow_checkpoint_advance\": true neu that su muon."
            )

        chain = req.chain and not req.dry_run
        if req.overrides and chain:
            # Du lieu cua lan chay co override khong phai du lieu that — khong
            # duoc chay xuong silver.
            chain = False
            notes.append("co override -> tat chain, silver khong chay theo")

        args = {
            "--JOB_NAME": name,
            "--RUN_ID": run_id,
            "--DRY_RUN": "true" if req.dry_run else "false",
        }
        snapshot = encode_snapshot(raw)
        if len(snapshot) <= MAX_SNAPSHOT_CHARS:
            args["--CONFIG_SNAPSHOT"] = snapshot
        elif req.overrides:
            raise TriggerError(
                f"cau hinh sau override qua lon ({len(snapshot)} ky tu) de truyen cho Glue"
            )
        else:
            notes.append("snapshot qua lon -> Glue tu doc job_config")

        glue_job, profile = self.glue_job_for(name, cfg.runtime.profile)
        return Prepared(glue_job=glue_job, glue_args=args, config=raw, chain=chain,
                        notes=notes, timeout_minutes=cfg.runtime.timeout_minutes,
                        profile=profile)

    def status(self, name):
        ck = self.log.read_checkpoint(name)
        return {"checkpoint": {"watermarks": ck.watermarks, "run_id": ck.run_id,
                               "updated_at": ck.updated_at,
                               "source_status": ck.source_status} if not ck.is_empty else None}


# ============================================================
# SILVER — landing/ -> Iceberg
# ============================================================


class SilverType(JobType):
    code = "slv"

    default_profile = "slv-spark"

    def __init__(self, config_table: str, log_table: str, glue_prefix: str,
                 dynamodb=None, profile_jobs: Optional[Dict[str, str]] = None):
        super().__init__(config_table, log_table, glue_prefix, dynamodb=dynamodb,
                         profile_jobs=profile_jobs)
        from silver.state import SilverLogStore

        self.silver_log = SilverLogStore(log_table, client=dynamodb)

    def prepare(self, name, item, req, run_id):
        if req.overrides:
            raise TriggerError("job slv.* chua ho tro overrides")
        if req.dry_run:
            raise TriggerError("job slv.* chua ho tro dry_run")

        raw = self._raw_config(name, item)
        runtime = raw.get("runtime") or {}
        timeout = int(runtime.get("timeout_minutes", 60))
        try:
            from silver.config import from_dict
            timeout = from_dict(copy.deepcopy(raw), default_name=name).runtime.timeout_minutes
        except ImportError:
            pass  # goi Lambda thieu silver/ -> bo qua kiem tra, Glue se kiem
        except (ValueError, TypeError) as e:
            raise TriggerError(f"cau hinh silver khong hop le: {e}") from None

        data_date = req.data_date
        if req.manifest and not data_date:
            from silver.manifest import parse_manifest_key

            parsed = parse_manifest_key(req.manifest)
            if not parsed:
                raise TriggerError(f"manifest khong dung dinh dang: {req.manifest}")
            data_date = parsed[1]
        if not req.manifest and not data_date:
            raise TriggerError("job slv.* can \"manifest\" hoac \"data_date\"")

        glue_job, profile = self.glue_job_for(name, runtime.get("profile", self.default_profile))
        return Prepared(
            glue_job=glue_job,
            glue_args={
                "--JOB_NAME": name,
                "--RUN_ID": run_id,
                "--MANIFEST": req.manifest or NONE,
                "--DATA_DATE": data_date or NONE,
                "--REBUILD_FROM": req.rebuild_from or NONE,
            },
            config=raw,
            chain=req.chain,
            timeout_minutes=timeout,
            profile=profile,
        )

    def claim(self, name, req, run_id):
        # Cung mot manifest co the den tu hai duong (event cua orchestrator va
        # rule S3 cu cua module silver) -> TRIG# co dieu kien chan lan thu hai.
        # force=true: chay lai manifest da xu ly (test, sua loi).
        if not req.manifest or req.force:
            return True
        return self.silver_log.claim_trigger(name, req.manifest, run_id)

    def release_claim(self, name, req, run_id):
        if req.manifest and not req.force:
            self.silver_log.mark_trigger(name, req.manifest, "FAILED", run_id)

    def consumers_of(self, detail):
        if detail.get("job_type") != "src" or not detail.get("manifest"):
            return []
        from silver.manifest import parse_manifest_key

        parsed = parse_manifest_key(detail["manifest"])
        if not parsed:
            return []
        dataset, day = parsed
        out = []
        for item in self.configs.list_enabled():
            src_ds = item.get("source_dataset") or \
                ((item.get("config") or {}).get("source") or {}).get("dataset", "")
            if src_ds != dataset:
                continue
            out.append(RunRequest(
                job_id=f"{self.code}.{item['job_name']}",
                manifest=detail["manifest"],
                data_date=day,
                chain=bool(detail.get("chain", True)),
                trigger={"type": "upstream",
                         "parent_job_id": detail.get("job_id", ""),
                         "parent_run_id": detail.get("run_id", "")},
            ))
        return out


JOB_TYPE_CLASSES = {c.code: c for c in (SourcingType, SilverType)}
