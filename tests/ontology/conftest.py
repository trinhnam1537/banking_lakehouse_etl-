"""Dùng lại phiên Spark + Iceberg local của bộ test silver (catalog glue_catalog kiểu hadoop)."""

from tests.silver.conftest import requires_spark, spark  # noqa: F401
