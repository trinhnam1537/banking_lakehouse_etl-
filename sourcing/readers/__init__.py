"""Sổ đăng ký reader.

Thêm loại nguồn mới = thêm một lớp con `Reader` + một dòng ở đây. Runner và
phần cấu hình không phải đổi gì.

Import bên trong hàm (lazy) là có chủ ý: reader JDBC cần `psycopg2`, reader
Kafka cần `confluent-kafka`. Import ở đầu module thì một job kéo REST API
cũng sẽ chết vì thiếu thư viện nó không hề dùng.
"""

from __future__ import annotations

from ..config import SourceConfig, SourceType
from .base import Batch, Reader

__all__ = ["Batch", "Reader", "build_reader"]


def build_reader(cfg: SourceConfig, **kwargs) -> Reader:
    if cfg.source_type is SourceType.API:
        from .api import ApiReader

        return ApiReader(cfg, secrets_client=kwargs.get("secrets_client"))

    if cfg.source_type is SourceType.JDBC:
        from .jdbc import JdbcReader

        return JdbcReader(cfg, secrets_client=kwargs.get("secrets_client"))

    if cfg.source_type is SourceType.KAFKA:
        from .kafka import KafkaReader

        return KafkaReader(cfg, checkpoint=kwargs.get("checkpoint"))

    if cfg.source_type is SourceType.S3FILE:
        from .s3file import S3FileReader

        return S3FileReader(cfg, s3_client=kwargs.get("s3_client"))

    if cfg.source_type is SourceType.HTTPFILE:
        from .httpfile import HttpFileReader

        return HttpFileReader(cfg)

    if cfg.source_type is SourceType.ONEDRIVE:
        from .onedrive import OneDriveReader

        return OneDriveReader(cfg, secrets_client=kwargs.get("secrets_client"))

    raise ValueError(f"[{cfg.name}] chua co reader cho '{cfg.source_type}'")
