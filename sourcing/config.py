"""Đọc và kiểm tra entry cấu hình nguồn.

Một entry YAML mô tả trọn vẹn một job kéo dữ liệu. Thêm file YAML mới vào
`sourcing_etl/sources/` là có job Glue mới — không sửa dòng Python nào.

Cùng một file YAML được đọc bởi hai phía:

  - Terraform (`yamldecode`) đọc để sinh Glue job, lịch chạy, quyền IAM
  - Framework này đọc lúc runtime để biết phải kéo gì, ghi đâu

Hệ quả: schema ở đây và schema Terraform đọc **phải khớp nhau**. Đổi tên một
khoá thì phải đổi ở cả hai chỗ. Đó là cái giá của việc cấu hình một nơi,
dùng hai nơi — đổi lại không bao giờ lệch giữa định nghĩa job và code chạy job.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class CdcMode(str, Enum):
    """Hai chế độ kéo dữ liệu.

    CDC1 — incremental. Chỉ lấy bản ghi mới hơn checkpoint lần chạy trước.
           Rẻ, nhanh, nhưng phụ thuộc nguồn có cột mốc đáng tin (updated_at,
           id tăng dần, offset Kafka). Ghi **append** xuống raw.

    CDC5 — full snapshot. Kéo toàn bộ mỗi lần, **ghi đè** partition raw.
           Không dùng checkpoint. Dành cho bảng tham chiếu nhỏ mà nguồn
           không có cột mốc, hoặc bản ghi bị sửa hồi tố tuỳ ý
           (danh mục mã CK, bảng ánh xạ LEI).

    Chọn sai chế độ là lỗi im lặng nguy hiểm nhất của tầng sourcing: CDC1 trên
    nguồn bị sửa hồi tố sẽ **bỏ sót thay đổi mà không báo lỗi gì**.
    """

    CDC1 = "cdc1"
    CDC5 = "cdc5"


class SourceType(str, Enum):
    API = "api"
    JDBC = "jdbc"
    KAFKA = "kafka"
    S3FILE = "s3file"


class Engine(str, Enum):
    """Loại Glue job.

    PYTHON_SHELL — 0.0625 DPU, khoảng 0.03 USD/giờ. Đủ cho API và bảng nhỏ.
    SPARK        — tối thiểu 2 DPU. Chỉ dùng khi dữ liệu không vừa bộ nhớ
                   một tiến trình, hoặc cần đọc song song từ JDBC.

    Mặc định là PYTHON_SHELL. Dùng Spark cho một job kéo 500 bản ghi từ REST
    API là đốt tiền vô ích — đây là sai lầm phổ biến nhất khi mới dùng Glue.
    """

    PYTHON_SHELL = "python_shell"
    SPARK = "spark"


@dataclass
class ApiSource:
    base_url: str
    path: str = ""
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    query: Dict[str, Any] = field(default_factory=dict)
    # Đường tới mảng bản ghi trong JSON trả về, ngăn bằng dấu chấm.
    # "" nghĩa là toàn bộ response đã là một mảng.
    # Ở chế độ raw_mode=response, nó KHÔNG cắt dữ liệu ghi xuống raw — chỉ
    # dùng để biết trang rỗng (dừng phân trang) và lấy mốc CDC1.
    records_path: str = ""
    # Tầng raw ghi gì:
    #   response — mỗi HTTP response một dòng, NGUYÊN VĂN, kèm request đã gọi.
    #              Không mất envelope (SEC frames: tag/uom nằm ở đó; GLEIF:
    #              ngày golden copy; Treasury: từ điển cột). Schema do silver quyết.
    #   records  — bóc theo records_path, mỗi bản ghi một dòng (cách cũ).
    raw_mode: str = "response"
    # Phân trang. none | page | cursor | offset
    pagination: str = "none"
    page_param: str = "page"
    page_size_param: str = "limit"
    page_size: int = 500
    cursor_path: str = ""
    cursor_param: str = "cursor"
    max_pages: int = 200
    # Tên secret trong Secrets Manager; giá trị được nạp vào headers/query
    # theo secret_inject. KHÔNG bao giờ để khoá thật trong YAML.
    secret_name: Optional[str] = None
    secret_inject: Dict[str, str] = field(default_factory=dict)
    rate_limit_per_sec: float = 5.0
    timeout_seconds: int = 30


@dataclass
class JdbcSource:
    # Secrets Manager chứa host/port/user/password/dbname
    secret_name: str
    schema: str
    table: str
    driver: str = "postgresql"
    # Chỉ dùng cho SPARK: chia truy vấn thành N luồng theo cột số này
    partition_column: Optional[str] = None
    num_partitions: int = 1
    # Lọc thêm, dán vào sau WHERE. Để rỗng nếu không cần.
    extra_where: str = ""


@dataclass
class KafkaSource:
    bootstrap_servers: str
    topic: str
    group_id: str
    # earliest chỉ có tác dụng lần chạy đầu; sau đó checkpoint quyết định
    auto_offset_reset: str = "earliest"
    max_records_per_run: int = 500_000
    poll_timeout_seconds: float = 10.0


@dataclass
class S3FileSource:
    """Nguồn file do bên ngoài đẩy vào S3.

    Dùng cho luồng: ai đó upload file → S3 event → Lambda → đăng ký vào raw.
    Reader này quét theo prefix; Lambda xử lý theo từng file là đường khác,
    xem `sourcing/lambda_handler.py`.
    """

    bucket: str
    prefix: str
    file_format: str = "json"  # json | ndjson | csv
    csv_delimiter: str = ","
    compression: str = "none"  # none | gzip


@dataclass
class CdcConfig:
    mode: CdcMode = CdcMode.CDC1
    # CDC1: cột dùng làm mốc. Với Kafka thì bỏ trống (dùng offset).
    watermark_column: Optional[str] = None
    # Kiểu mốc, quyết định cách so sánh và cách tuần tự hoá checkpoint
    watermark_type: str = "timestamp"  # timestamp | int | string
    # Lùi lại một khoảng khi so sánh, để bắt bản ghi đến muộn.
    # 0 = so sánh đúng bằng checkpoint.
    lookback_seconds: int = 0
    # Mốc khởi tạo cho lần chạy đầu tiên khi chưa có checkpoint
    initial_watermark: Optional[str] = None


@dataclass
class TargetConfig:
    """Nơi ghi tầng raw.

    Lưu ý thuật ngữ: tài liệu thiết kế gọi tầng này là `landing/`, ở đây gọi
    là raw. Cùng một chỗ trên S3 — giữ nguyên prefix `landing/` để không lệch
    với bố cục đã khai trong module lakehouse.
    """

    prefix: str = "landing"
    dataset: str = ""  # mặc định lấy theo tên job
    # Partition theo ngày chạy. Với CDC5 thì partition này bị ghi đè mỗi lần.
    partition_format: str = "ingest_date=%Y-%m-%d"
    # NDJSON nén gzip: không cần pyarrow, giữ nguyên hình dạng dữ liệu nguồn,
    # và job bronze_ingest đã đọc JSON sẵn. Tầng raw không nên áp schema.
    file_format: str = "ndjson.gz"
    max_records_per_file: int = 50_000


@dataclass
class ScheduleConfig:
    enabled: bool = False
    # Cú pháp EventBridge: "rate(5 minutes)" hoặc "cron(0 2 * * ? *)"
    expression: str = "rate(1 hour)"


# Glue job DÙNG CHUNG ("profile") mà repo hạ tầng tạo sẵn. Một profile = một bộ
# thư viện + một kiểu mạng; mọi entry cùng profile chạy trên cùng một Glue job,
# tham số từng lần chạy (--JOB_NAME, --CONFIG_SNAPSHOT, --ETL_RELEASE) quyết định
# chạy entry nào bằng bản code nào.
#
#   src-shell  Python Shell, có internet: api, s3file, jdbc tới DB công khai.
#              Thư viện sẵn: PyMySQL.
#
# Thêm profile (vd src-vpc cho RDS private, src-kafka) = việc của repo hạ tầng
# TRƯỚC, rồi mới thêm tên vào đây. Tên lạ bị chặn lúc nạp entry thay vì lúc
# Lambda gọi một Glue job không tồn tại.
PROFILES = ("src-shell",)
DEFAULT_PROFILE = "src-shell"

# Glue sourcing chỉ được đọc secret dưới tiền tố này (IAM ở repo hạ tầng:
# secret:fingraph/src/*). Khai secret ngoài tiền tố thì job chết AccessDenied
# lúc chạy — chặn sớm ở đây cho lỗi hiện ra trong pytest.
SECRET_PREFIX = "fingraph/src/"


@dataclass
class RuntimeConfig:
    engine: Engine = Engine.PYTHON_SHELL
    timeout_minutes: int = 30
    max_retries: int = 0
    profile: str = DEFAULT_PROFILE
    # Chỉ có tác dụng với engine SPARK
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    # CŨ: thư viện theo entry, chỉ còn tác dụng với Glue job theo entry (đang
    # gỡ dần). Glue job profile cài thư viện theo profile, bỏ qua khoá này.
    additional_python_modules: List[str] = field(default_factory=list)


@dataclass
class SourceConfig:
    name: str
    source_type: SourceType
    description: str = ""
    enabled: bool = True
    owner: str = ""
    cdc: CdcConfig = field(default_factory=CdcConfig)
    target: TargetConfig = field(default_factory=TargetConfig)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    api: Optional[ApiSource] = None
    jdbc: Optional[JdbcSource] = None
    kafka: Optional[KafkaSource] = None
    s3file: Optional[S3FileSource] = None

    @property
    def dataset(self) -> str:
        return self.target.dataset or self.name

    @property
    def raw_path(self) -> str:
        """Tiền tố S3 của dataset, chưa gồm partition."""
        return f"{self.target.prefix.rstrip('/')}/{self.dataset}"


class ConfigError(ValueError):
    """Entry sai — dừng ngay lúc nạp, không để job chạy rồi hỏng giữa chừng."""


def _enum(cls, value, field_name: str, name: str):
    try:
        return cls(value)
    except ValueError:
        allowed = ", ".join(m.value for m in cls)
        raise ConfigError(
            f"[{name}] {field_name} = {value!r} khong hop le. Cho phep: {allowed}"
        ) from None


def _build_source_block(raw: Dict[str, Any], stype: SourceType, name: str): 
    """Dựng khối cấu hình riêng của từng loại nguồn.

    Bắt lỗi thiếu khoá bắt buộc ngay tại đây, kèm tên job — vì khi có vài chục
    entry, thông báo `KeyError: 'base_url'` trần trụi không nói được là file nào.
    """
    block = raw.get(stype.value) or {}
    klass = {
        SourceType.API: ApiSource,
        SourceType.JDBC: JdbcSource,
        SourceType.KAFKA: KafkaSource,
        SourceType.S3FILE: S3FileSource,
    }[stype]
    try:
        return klass(**block)
    except TypeError as e:
        raise ConfigError(f"[{name}] khoi '{stype.value}' sai: {e}") from None


def _expand_env(node: Any) -> Any:
    """Thay ${BIEN} bằng biến môi trường, đệ quy khắp cấu hình.

    Cần cho những giá trị đổi theo lần apply mà không nên ghi cứng vào repo:
    `bootstrap_servers: "${KAFKA_BOOTSTRAP}"` — IP riêng của EC2 đổi mỗi lần
    spot bị thu hồi và máy được dựng lại.

    Biến chưa đặt thì để nguyên chuỗi `${TEN}` chứ không thay bằng rỗng —
    một bootstrap servers rỗng sẽ gây lỗi kết nối khó hiểu, còn chuỗi
    `${KAFKA_BOOTSTRAP}` lọt vào thông báo lỗi thì nhìn là biết thiếu gì.
    """
    if isinstance(node, str):
        if "${" not in node:
            return node
        out = node
        for key, val in os.environ.items():
            out = out.replace("${" + key + "}", val)
        return out
    if isinstance(node, dict):
        return {k: _expand_env(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_expand_env(v) for v in node]
    return node


def load_config(path: str) -> SourceConfig:
    """Nạp entry từ file YAML — dùng khi chạy tay và trong CI."""
    # Import tai cho: Glue doc cau hinh tu DynamoDB, khong can YAML, va
    # runtime Python Shell khong bao dam co PyYAML.
    import yaml

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return from_dict(raw, default_name=os.path.splitext(os.path.basename(path))[0])


def from_dict(raw: Dict[str, Any], default_name: str = "") -> SourceConfig:
    """Dựng cấu hình từ một dict.

    Hai đường vào cùng dùng hàm này:
      - file YAML (chạy tay, CI kiểm cú pháp)
      - item DynamoDB (Glue job lúc chạy thật)

    Chung một bộ kiểm tra nên cấu hình sai bị chặn ở cả hai đường, và không
    có chuyện YAML hợp lệ mà item Dynamo lại hỏng.
    """
    raw = _expand_env(raw)
    name = raw.get("name") or default_name

    if "source_type" not in raw:
        raise ConfigError(f"[{name}] thieu 'source_type'")
    stype = _enum(SourceType, raw["source_type"], "source_type", name)

    cdc_raw = raw.get("cdc") or {}
    cdc = CdcConfig(
        mode=_enum(CdcMode, cdc_raw.get("mode", "cdc1"), "cdc.mode", name),
        watermark_column=cdc_raw.get("watermark_column"),
        watermark_type=cdc_raw.get("watermark_type", "timestamp"),
        lookback_seconds=int(cdc_raw.get("lookback_seconds", 0)),
        initial_watermark=cdc_raw.get("initial_watermark"),
    )

    runtime_raw = raw.get("runtime") or {}
    runtime = RuntimeConfig(
        engine=_enum(Engine, runtime_raw.get("engine", "python_shell"),
                     "runtime.engine", name),
        timeout_minutes=int(runtime_raw.get("timeout_minutes", 30)),
        max_retries=int(runtime_raw.get("max_retries", 0)),
        profile=str(runtime_raw.get("profile", DEFAULT_PROFILE)),
        worker_type=runtime_raw.get("worker_type", "G.1X"),
        number_of_workers=int(runtime_raw.get("number_of_workers", 2)),
        additional_python_modules=list(runtime_raw.get("additional_python_modules", [])),
    )

    cfg = SourceConfig(
        name=name,
        source_type=stype,
        description=raw.get("description", ""),
        enabled=bool(raw.get("enabled", True)),
        owner=raw.get("owner", ""),
        cdc=cdc,
        target=TargetConfig(**(raw.get("target") or {})),
        schedule=ScheduleConfig(**(raw.get("schedule") or {})),
        runtime=runtime,
    )
    setattr(cfg, stype.value, _build_source_block(raw, stype, name))

    _validate(cfg)
    return cfg


def _validate(cfg: SourceConfig) -> None:
    n = cfg.name

    if cfg.cdc.mode is CdcMode.CDC1:
        # Kafka dùng offset làm mốc nên không cần cột; các nguồn khác thì cần.
        if cfg.source_type is not SourceType.KAFKA and not cfg.cdc.watermark_column:
            raise ConfigError(
                f"[{n}] cdc.mode=cdc1 nhung thieu cdc.watermark_column. "
                f"Khong co cot moc thi khong biet lan truoc keo toi dau — "
                f"dung cdc5 (full snapshot) neu nguon that su khong co cot nao."
            )
    else:
        # CDC5 ghi đè toàn bộ; khai watermark_column ở đây là hiểu nhầm chế độ.
        if cfg.cdc.watermark_column:
            raise ConfigError(
                f"[{n}] cdc.mode=cdc5 (full snapshot) khong dung watermark_column. "
                f"Bo khoa nay, hoac doi sang cdc1."
            )

    if cfg.source_type is SourceType.JDBC and cfg.runtime.engine is Engine.PYTHON_SHELL:
        if cfg.jdbc and cfg.jdbc.num_partitions > 1:
            raise ConfigError(
                f"[{n}] jdbc.num_partitions > 1 chi co tac dung voi engine=spark."
            )

    if cfg.runtime.profile not in PROFILES:
        raise ConfigError(
            f"[{n}] runtime.profile = {cfg.runtime.profile!r} khong co. "
            f"Profile hien co: {', '.join(PROFILES)} (them profile o repo ha tang truoc)."
        )

    secret = (cfg.api and cfg.api.secret_name) or (cfg.jdbc and cfg.jdbc.secret_name)
    if secret and not secret.startswith(SECRET_PREFIX):
        raise ConfigError(
            f"[{n}] secret_name = {secret!r} nam ngoai '{SECRET_PREFIX}'. "
            f"Glue sourcing chi duoc doc secret duoi tien to nay."
        )

    if cfg.target.file_format not in ("ndjson.gz", "ndjson"):
        raise ConfigError(
            f"[{n}] target.file_format = {cfg.target.file_format!r} chua ho tro. "
            f"Tang raw chi ghi ndjson/ndjson.gz — khong ap schema o tang nay."
        )


# ---------- Override và snapshot: dùng khi orchestrator kích một lần chạy ----------

# Không cho override những khoá này: đổi chúng thì lần chạy không còn là
# "job này với tham số khác" nữa. `name`/`source_type` lệch với Glue job,
# còn `runtime`/`schedule`/`enabled` do Terraform và lịch quyết định, sửa ở
# đây không có tác dụng gì ngoài gây hiểu nhầm.
OVERRIDE_FORBIDDEN = ("name", "source_type", "enabled", "runtime", "schedule")


def apply_overrides(raw: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    """Ghi đè cấu hình theo đường dẫn chấm: {"api.max_pages": 1}.

    Trả bản sao, không sửa `raw`. Không kiểm tra kiểu ở đây — kết quả phải đi
    qua `from_dict()` như mọi cấu hình khác, nên override sai bị chặn đúng
    bằng bộ kiểm tra của YAML.
    """
    import copy

    out = copy.deepcopy(raw)
    for path, value in (overrides or {}).items():
        parts = [p for p in str(path).split(".") if p]
        if not parts:
            raise ConfigError(f"override rong: {path!r}")
        if parts[0] in OVERRIDE_FORBIDDEN:
            raise ConfigError(
                f"khong override duoc '{path}'. Cam: {', '.join(OVERRIDE_FORBIDDEN)}"
            )
        node = out
        for p in parts[:-1]:
            if not isinstance(node.get(p), dict):
                node[p] = {}
            node = node[p]
        node[parts[-1]] = value
    return out


def encode_snapshot(raw: Dict[str, Any]) -> str:
    """Nén cấu hình thành một tham số Glue: JSON → zlib → base64 url-safe.

    Nén vì giá trị tham số Glue có giới hạn độ dài, còn base64 thì để không
    ký tự nào (ngoặc kép, xuống dòng) bị hiểu sai trên đường đi.
    """
    import base64
    import json
    import zlib

    payload = json.dumps(raw, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return base64.urlsafe_b64encode(zlib.compress(payload, 9)).decode("ascii")


def decode_snapshot(value: str) -> Dict[str, Any]:
    import base64
    import json
    import zlib

    return json.loads(zlib.decompress(base64.urlsafe_b64decode(value.encode("ascii"))))


def load_all(directory: str) -> Dict[str, SourceConfig]:
    """Nạp mọi entry trong thư mục, KỂ CẢ thư mục con.

    Mỗi nguồn dữ liệu một thư mục, mỗi bảng của nguồn đó một file. Terraform
    quét cùng cây này bằng `fileset(dir, "**/*.yml")`.

    Hai bất biến được kiểm ở đây, vì cả hai đều hỏng *im lặng* ở phía Terraform:

      - **tên file phải duy nhất trên toàn cây.** Terraform khoá `local.entries`
        theo `basename(f)` — tên job Glue và khoá DynamoDB không chứa được dấu
        `/`. Hai file trùng tên ở hai thư mục sẽ đè lên nhau và một job biến mất.
      - **`name` phải trùng tên file.** Terraform lấy tên job từ tên file, còn
        framework lúc chạy lấy từ khoá `name`. Lệch nhau thì job Glue tên A đi
        tìm checkpoint của job tên B.
    """
    out: Dict[str, SourceConfig] = {}
    seen: Dict[str, str] = {}

    for root, dirs, files in os.walk(directory):
        dirs.sort()
        for fname in sorted(files):
            if not fname.endswith((".yml", ".yaml")):
                continue
            path = os.path.join(root, fname)
            stem = os.path.splitext(fname)[0]
            cfg = load_config(path)

            if cfg.name != stem:
                raise ConfigError(
                    f"[{cfg.name}] khoa 'name' khong trung ten file {fname!r}. "
                    f"Terraform lay ten job tu TEN FILE, nen hai thu nay lech "
                    f"nhau se tao job chay bang cau hinh cua job khac."
                )
            if cfg.name in out:
                raise ConfigError(
                    f"Trung ten job: {cfg.name} — {path} va {seen[cfg.name]}. "
                    f"Ten file phai duy nhat tren toan bo cay thu muc."
                )

            seen[cfg.name] = path
            out[cfg.name] = cfg

    return out
