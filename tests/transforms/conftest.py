"""Dùng lại SparkSession + Iceberg (catalog `glue_catalog` trên đĩa) của test silver."""

from tests.silver.conftest import requires_spark, spark  # noqa: F401
