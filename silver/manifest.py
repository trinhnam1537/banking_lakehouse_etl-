"""Đường dẫn manifest mà sourcing ghi sau mỗi lần chạy (sourcing/writer.py).

    landing/_manifests/<dataset>/ingest_date=YYYY-MM-DD/<run_id>.json

Orchestrator dùng để biết manifest thuộc dataset nào (tìm entry silver đọc
dataset đó) và ngày dữ liệu. Trước nằm trong silver/lambda_router.py — Lambda
router và chuỗi S3 -> SQS -> Pipes đã gỡ, việc nối sourcing -> silver giờ do
event "Job Run Succeeded" + Lambda trigger lo.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

MANIFEST_RE = re.compile(
    r"^(?P<prefix>.+?)/_manifests/(?P<dataset>.+)/ingest_date=(?P<date>\d{4}-\d{2}-\d{2})/(?P<run>[^/]+)\.json$"
)


def parse_manifest_key(key: str) -> Optional[Tuple[str, str]]:
    """`landing/_manifests/berka/trans/ingest_date=2026-10-12/abc.json`
    -> ("berka/trans", "2026-10-12"). Không khớp -> None."""
    m = MANIFEST_RE.match(key)
    return (m["dataset"], m["date"]) if m else None
