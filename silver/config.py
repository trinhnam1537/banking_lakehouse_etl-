"""Cấu hình một entry silver — đọc từ YAML (chạy tay, CI) hoặc DynamoDB (Glue).

Một entry = một bảng silver. Nó trả lời năm câu hỏi:

  source      đọc raw nào, bóc bản ghi ở đâu trong dòng raw
  columns     cột nào, lấy từ đường nào, ép về kiểu gì
  technical   khoá nghiệp vụ, ngày dữ liệu, thứ tự CDC — sinh cột ds_*
  writer      ghi thế nào: scd2 | scd4a | simple(append/overwrite)
  quality     điều kiện để một lần chạy được phép ghi

Không có dòng Spark nào ở đây: file này phải nạp được trong CI, trong Lambda
router và trong `python -m silver.ddl` mà không cần pyspark.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ConfigError(ValueError):
    """Entry sai — dừng lúc nạp, không để job chạy rồi hỏng giữa chừng."""


class Shape(str, Enum):
    """Hình dạng một dòng raw — sourcing ghi vào manifest (`raw_mode`)."""

    API_RESPONSE = "api_response"   # {_request, _records_path, _response: <nguyen van>}
    RECORDS = "records"             # moi dong da la mot ban ghi (jdbc, s3file, api raw_mode=records)
    KAFKA_MESSAGE = "kafka_message"  # {_kafka_key, value: "<json string>", ...}


class ReadMode(str, Enum):
    MANIFEST = "manifest"    # dung danh sach file trong manifest (mac dinh, chinh xac)
    PARTITION = "partition"  # moi file cua mot ingest_date (backfill / chay tay)


class WriterType(str, Enum):
    SCD2 = "scd2"
    SCD4A = "scd4a"
    SIMPLE = "simple"


class SimpleMode(str, Enum):
    APPEND = "append"
    OVERWRITE_TABLE = "overwrite_table"
    OVERWRITE_PARTITION = "overwrite_partition"


class ColumnFrom(str, Enum):
    """Cột lấy từ đâu trong dòng raw.

    record    trong bản ghi đã bóc theo records_path (mặc định)
    response  gốc `_response` — envelope: SEC frames `tag`, GLEIF `meta...`
    request   `_request` — tham số đã gọi (mã chứng khoán, trang)
    line      gốc dòng raw — `_src_ingest_ts`, `_kafka_offset`...
    """

    RECORD = "record"
    RESPONSE = "response"
    REQUEST = "request"
    LINE = "line"


# Kieu duoc phep o cot `type`. Kiem bang regex de bat loi go sai ngay luc nap
# ("decmal(20,8)"), khong doi toi luc Spark bao loi mo ho giua job.
_SIMPLE_TYPES = {
    "string", "int", "integer", "bigint", "long", "smallint", "tinyint",
    "double", "float", "boolean", "date", "timestamp", "binary",
}
_TYPE_RE = re.compile(
    r"^(decimal\(\s*\d+\s*,\s*\d+\s*\)|array<.+>|map<.+>|struct<.+>)$", re.IGNORECASE
)

# Cot ky thuat — ten co dinh, moi bang silver deu co (tru cot rieng cua SCD2).
TECH_COLUMNS: List[tuple] = [
    ("ds_key", "string", "Khoa nghiep vu bam SHA-256 (technical.keys)"),
    ("ds_hash", "string", "Bam cac cot nghiep vu — phat hien thay doi"),
    ("ds_record_status", "string", "I=moi, U=doi, D=xoa, N=khong doi (scd4a)"),
    ("ds_partition_date", "date", "Ngay du lieu (technical.partition_date)"),
    ("ds_cdc_index", "bigint", "Thu tu CDC: micro-giay hoac so tang dan"),
    ("ds_etl_timestamp", "timestamp", "Thoi diem ghi"),
    ("ds_etl_job_id", "string", "run_id cua lan chay ghi dong nay"),
    ("ds_source", "string", "Manifest (hoac prefix) ma dong nay den tu"),
]
SCD2_COLUMNS: List[tuple] = [
    ("ds_valid_from", "date", "Phien ban co hieu luc tu ngay (gom)"),
    ("ds_valid_to", "date", "Het hieu luc tu ngay (khong gom); 9999-12-31 = hien hanh"),
    ("ds_is_current", "boolean", "Phien ban hien hanh"),
]


def valid_type(t: str) -> bool:
    t = t.strip().lower()
    return t in _SIMPLE_TYPES or bool(_TYPE_RE.match(t))


@dataclass
class ColumnSpec:
    name: str
    type: str = "string"
    path: Optional[str] = None       # duong cham trong nguon; mac dinh = name
    from_: ColumnFrom = ColumnFrom.RECORD
    # Bien doi CHUOI truoc khi ep kieu, bieu thuc Spark SQL voi {col}:
    #   "regexp_extract({col}, 'lei-records/(\\w+)', 1)"
    transform: Optional[str] = None
    # Cot TINH tu cac cot khac SAU khi ep kieu (khong doc tu nguon):
    #   expr: "price * qty"
    expr: Optional[str] = None
    # Dinh dang cho date/timestamp: mau Java ("dd/MM/yyyy"), "epoch_ms", "epoch_s"
    format: Optional[str] = None
    nullable: bool = True
    default: Optional[Any] = None
    comment: str = ""
    # false = cot trung gian: dung de tinh cot khac, KHONG ghi xuong bang
    keep: bool = True
    # false = khong tinh vao ds_hash (vd. ngay xuat ban golden copy doi moi ngay)
    hash: bool = True

    @property
    def source_path(self) -> str:
        return self.path or self.name


@dataclass
class SourceSpec:
    dataset: str                      # khop `dataset` trong manifest sourcing
    shape: Optional[Shape] = None     # None = lay tu manifest (raw_mode)
    records_path: Optional[str] = None  # None = lay tu cot `_records_path` cua dong raw
    filter: Optional[str] = None      # Spark SQL tren cot da boc, vd "type = 'lei-records'"
    read: ReadMode = ReadMode.MANIFEST
    prefix: str = "landing"


@dataclass
class TargetSpec:
    table: str
    database: str = "track_a_silver"
    catalog: str = "glue_catalog"
    # File DDL trong silver_etl/ddl/, tuong doi. Khong khai = sinh tu columns.
    ddl: Optional[str] = None
    partition_by: List[str] = field(default_factory=list)
    properties: Dict[str, str] = field(default_factory=dict)
    # add_columns: them cot moi vao bang da co (Iceberg cho phep, khong ghi lai du lieu)
    # strict: lech schema la loi
    schema_evolution: str = "add_columns"

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.database}.{self.table}"


@dataclass
class PartitionDateSpec:
    """ds_partition_date lấy từ đâu.

    ingest_date  partition raw `ingest_date=YYYY-MM-DD` (mặc định — ngày kéo)
    run_date     tham số --DATA_DATE của lần chạy (backfill)
    column       một cột nghiệp vụ đã ép kiểu (vd. `record_date` của Treasury)
    expr         biểu thức Spark SQL tuỳ ý
    """

    source: str = "ingest_date"
    column: Optional[str] = None
    expr: Optional[str] = None


@dataclass
class TechnicalSpec:
    keys: List[str] = field(default_factory=list)
    partition_date: PartitionDateSpec = field(default_factory=PartitionDateSpec)
    # Thu tu de khu trung lap trong lo va chan du lieu den muon (SCD2).
    # Mot cot timestamp/so, hoac bieu thuc. None = dung `_src_ingest_ts`.
    cdc_index: Optional[str] = None
    # Dong nao la lenh XOA tu nguon (CDC co cot op): "op = 'D'"
    delete_when: Optional[str] = None


@dataclass
class WriterSpec:
    type: WriterType = WriterType.SIMPLE
    mode: SimpleMode = SimpleMode.APPEND          # chi cho simple
    # Nguon la ANH CHUP day du (CDC5): key vang mat = da bi xoa.
    # Sai voi nguon delta (CDC1) — delta vang mat khong co nghia la xoa.
    snapshot: bool = False
    # scd4a: ghi them dong D cho key co o anh truoc ma anh nay khong con
    emit_deletes: bool = True


@dataclass
class QualitySpec:
    not_null: List[str] = field(default_factory=list)
    # Ty le toi da dong co gia tri nguon KHAC NULL ma ep kieu ra NULL
    max_cast_error_ratio: float = 0.0
    # Trung key TRONG LO (truoc khu trung lap) — canh bao hay loi
    duplicate_keys: str = "warn"      # warn | fail
    min_rows: int = 0
    on_fail: str = "fail"             # fail | warn


@dataclass
class RuntimeSpec:
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    timeout_minutes: int = 60
    # Co may dung chung bang SPARK_SIZES cua sourcing (S/M/L/XL). Khai thi de len
    # worker_type/number_of_workers; Lambda truyen cho RIENG lan chay (StartJobRun).
    size: Optional[str] = None

    def glue_capacity(self):
        from sourcing.config import SPARK_SIZES

        if self.size:
            return SPARK_SIZES[self.size]
        return self.worker_type, int(self.number_of_workers)

    def validate(self, name: str) -> None:
        from sourcing.config import MAX_SPARK_WORKERS, SPARK_SIZES, SPARK_WORKER_TYPES

        if self.size is not None and self.size not in SPARK_SIZES:
            raise ConfigError(f"[{name}] runtime.size = {self.size!r} khong co. Cho phep: {', '.join(SPARK_SIZES)}")
        wt, n = self.glue_capacity()
        if wt not in SPARK_WORKER_TYPES or not 2 <= n <= MAX_SPARK_WORKERS:
            raise ConfigError(f"[{name}] co may {n} x {wt} khong hop le "
                              f"({'/'.join(SPARK_WORKER_TYPES)}, 2..{MAX_SPARK_WORKERS} worker).")


@dataclass
class SilverConfig:
    name: str
    source: SourceSpec
    target: TargetSpec
    columns: List[ColumnSpec]
    technical: TechnicalSpec = field(default_factory=TechnicalSpec)
    writer: WriterSpec = field(default_factory=WriterSpec)
    quality: QualitySpec = field(default_factory=QualitySpec)
    runtime: RuntimeSpec = field(default_factory=RuntimeSpec)
    description: str = ""
    owner: str = ""
    enabled: bool = True

    @property
    def business_columns(self) -> List[ColumnSpec]:
        return [c for c in self.columns if c.keep]

    @property
    def hash_columns(self) -> List[str]:
        return [c.name for c in self.columns if c.keep and c.hash]

    @property
    def technical_columns(self) -> List[tuple]:
        extra = SCD2_COLUMNS if self.writer.type is WriterType.SCD2 else []
        return TECH_COLUMNS + extra


# ---------------- dung tu dict ----------------


def _enum(value, cls, what: str, name: str):
    if value is None or isinstance(value, cls):
        return value
    try:
        return cls(value)
    except ValueError:
        allowed = ", ".join(e.value for e in cls)
        raise ConfigError(f"[{name}] {what} '{value}' khong hop le. Chon: {allowed}") from None


def _column(raw: Dict[str, Any], name: str) -> ColumnSpec:
    if "name" not in raw:
        raise ConfigError(f"[{name}] mot cot thieu 'name': {raw}")
    raw = dict(raw)
    raw["from_"] = _enum(raw.pop("from", "record"), ColumnFrom, "columns.from", name)
    unknown = set(raw) - set(ColumnSpec.__dataclass_fields__)
    if unknown:
        raise ConfigError(f"[{name}] cot '{raw['name']}' co khoa la: {sorted(unknown)}")
    return ColumnSpec(**raw)


def from_dict(raw: Dict[str, Any], default_name: str = "") -> SilverConfig:
    name = raw.get("name") or default_name
    if not name:
        raise ConfigError("entry thieu 'name'")
    for block in ("source", "target", "columns"):
        if not raw.get(block):
            raise ConfigError(f"[{name}] thieu khoi bat buoc '{block}'")

    src = dict(raw["source"])
    src["shape"] = _enum(src.get("shape"), Shape, "source.shape", name)
    src["read"] = _enum(src.get("read", "manifest"), ReadMode, "source.read", name)

    tech = dict(raw.get("technical") or {})
    pd = tech.pop("partition_date", None)
    if isinstance(pd, str):          # viet tat: partition_date: run_date
        pd = {"source": pd}
    tech_spec = TechnicalSpec(partition_date=PartitionDateSpec(**(pd or {})), **tech)

    wr = dict(raw.get("writer") or {})
    wr["type"] = _enum(wr.get("type", "simple"), WriterType, "writer.type", name)
    wr["mode"] = _enum(wr.get("mode", "append"), SimpleMode, "writer.mode", name)

    cfg = SilverConfig(
        name=name,
        source=SourceSpec(**src),
        target=TargetSpec(**raw["target"]),
        columns=[_column(c, name) for c in raw["columns"]],
        technical=tech_spec,
        writer=WriterSpec(**wr),
        quality=QualitySpec(**(raw.get("quality") or {})),
        runtime=RuntimeSpec(**(raw.get("runtime") or {})),
        description=raw.get("description", ""),
        owner=raw.get("owner", ""),
        enabled=raw.get("enabled", True),
    )
    _validate(cfg)
    return cfg


def _validate(cfg: SilverConfig) -> None:
    n = cfg.name
    cfg.runtime.validate(n)
    names = [c.name for c in cfg.columns]
    dup = {x for x in names if names.count(x) > 1}
    if dup:
        raise ConfigError(f"[{n}] trung ten cot: {sorted(dup)}")

    tech_names = {t[0] for t in TECH_COLUMNS + SCD2_COLUMNS}
    clash = tech_names & set(names)
    if clash:
        raise ConfigError(f"[{n}] ten cot trung cot ky thuat: {sorted(clash)}")

    for c in cfg.columns:
        if not valid_type(c.type):
            raise ConfigError(f"[{n}] cot '{c.name}': kieu '{c.type}' khong hop le")
        if c.expr and (c.path or c.transform):
            raise ConfigError(f"[{n}] cot '{c.name}': 'expr' la cot tinh, khong di kem path/transform")
        if c.transform and "{col}" not in c.transform:
            raise ConfigError(f"[{n}] cot '{c.name}': transform phai chua {{col}}")

    kept = {c.name for c in cfg.columns if c.keep}
    for k in cfg.technical.keys:
        if k not in names:
            raise ConfigError(f"[{n}] technical.keys '{k}' khong co trong columns")

    w = cfg.writer
    if w.type in (WriterType.SCD2, WriterType.SCD4A) and not cfg.technical.keys:
        raise ConfigError(f"[{n}] writer {w.type.value} can technical.keys")
    if w.type is WriterType.SCD4A and "ds_partition_date" not in (cfg.target.partition_by or ["ds_partition_date"]):
        raise ConfigError(f"[{n}] scd4a phai partition theo ds_partition_date")
    if w.type is WriterType.SIMPLE and w.mode is SimpleMode.OVERWRITE_PARTITION \
            and not cfg.target.partition_by:
        raise ConfigError(f"[{n}] overwrite_partition can target.partition_by")

    pd = cfg.technical.partition_date
    if pd.source not in ("ingest_date", "run_date", "column", "expr"):
        raise ConfigError(f"[{n}] partition_date.source '{pd.source}' khong hop le")
    if pd.source == "column" and pd.column not in kept:
        raise ConfigError(f"[{n}] partition_date.column '{pd.column}' khong co trong columns")
    if pd.source == "expr" and not pd.expr:
        raise ConfigError(f"[{n}] partition_date.source=expr can 'expr'")

    for col in cfg.quality.not_null:
        if col not in kept:
            raise ConfigError(f"[{n}] quality.not_null '{col}' khong co trong columns")
    for col in cfg.target.partition_by:
        if col not in kept and col not in tech_names:
            raise ConfigError(f"[{n}] target.partition_by '{col}' khong phai cot cua bang")


# ---------------- nap file ----------------


def load_config(path: str) -> SilverConfig:
    import yaml  # tai cho: Glue doc tu DynamoDB, khong can YAML

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return from_dict(raw, default_name=os.path.splitext(os.path.basename(path))[0])


def load_all(directory: str) -> Dict[str, SilverConfig]:
    """Mọi entry trong cây thư mục. Tên file phải duy nhất và trùng khoá `name`
    — cùng quy ước với sourcing (Terraform khoá job theo tên file)."""
    out: Dict[str, SilverConfig] = {}
    for root, _, files in os.walk(directory):
        for fn in sorted(files):
            if not fn.endswith((".yml", ".yaml")):
                continue
            path = os.path.join(root, fn)
            stem = os.path.splitext(fn)[0]
            cfg = load_config(path)
            if cfg.name != stem:
                raise ConfigError(f"{path}: khoa 'name' ({cfg.name}) khong trung ten file ({stem})")
            if stem in out:
                raise ConfigError(f"Hai entry trung ten file: {stem}")
            out[stem] = cfg
    return out
