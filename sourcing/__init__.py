"""Framework kéo dữ liệu từ nguồn xuống tầng raw.

Đây là phần CODE. Phần CẤU HÌNH nằm ở `sourcing_etl/sources/*.yml` —
thêm một file YAML ở đó là có một job Glue mới, không sửa gì ở đây.

    sourcing/          <- framework (thu muc nay)
    sourcing_etl/      <- entries sinh ra job

Import ở đây là **lazy**: chỉ `config` được nạp ngay, phần còn lại nạp khi
được dùng tới. Lý do: kiểm tra cú pháp entry YAML (trong CI, hoặc trên máy
bạn) chỉ cần PyYAML — không nên bắt cài cả boto3 chỉ để chạy `load_all()`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .config import (
    CdcMode,
    ConfigError,
    Engine,
    SourceConfig,
    SourceType,
    load_all,
    load_config,
)

if TYPE_CHECKING:  # chi de type checker thay, khong nap luc chay
    from .runner import RunResult, SourceRunner
    from .state import Checkpoint, JobConfigStore, JobLogStore, RunStats
    from .writer import RawWriter

__all__ = [
    "CdcMode", "ConfigError", "Engine", "SourceConfig", "SourceType",
    "load_config", "load_all",
    "SourceRunner", "RunResult",
    "Checkpoint", "JobConfigStore", "JobLogStore", "RunStats",
    "RawWriter",
]

_LAZY = {
    "SourceRunner": ".runner",
    "RunResult": ".runner",
    "Checkpoint": ".state",
    "JobConfigStore": ".state",
    "JobLogStore": ".state",
    "RunStats": ".state",
    "RawWriter": ".writer",
}


def __getattr__(name: str):
    """PEP 562: nạp module con khi có ai đó thực sự cần tới nó."""
    module_name = _LAZY.get(name)
    if module_name is None:
        raise AttributeError(f"module 'sourcing' khong co thuoc tinh '{name}'")
    import importlib

    module = importlib.import_module(module_name, __name__)
    return getattr(module, name)
