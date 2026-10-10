"""Fixture cho test silver.

Test Spark chạy Spark 3.5 + Iceberg 1.7 THẬT (cùng phiên bản Glue 5.0), catalog
Hadoop trên đĩa nhưng vẫn đặt tên `glue_catalog` — SQL của writer y hệt khi
chạy trên Glue. Bị bỏ qua nếu máy không có pyspark/Java.

Lần đầu tải iceberg-spark-runtime (~40 MB) từ Maven về ~/.ivy2.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import sys
from typing import Any, Dict, List

import pytest

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"


def _spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return shutil.which("java") is not None


requires_spark = pytest.mark.skipif(not _spark_available(), reason="can pyspark + Java")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    if not _spark_available():
        pytest.skip("can pyspark + Java")
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    wh = tmp_path_factory.mktemp("warehouse").as_posix()
    s = (SparkSession.builder.master("local[2]").appName("silver-tests")
         .config("spark.jars.packages", ICEBERG_PKG)
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.glue_catalog.type", "hadoop")
         .config("spark.sql.catalog.glue_catalog.warehouse", "file:///" + wh.lstrip("/"))
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


class RawLake:
    """Ghi file raw ĐÚNG định dạng sourcing ghi (NDJSON.gz + manifest)."""

    def __init__(self, root):
        self.root = root
        self.n = 0

    def api_run(self, dataset: str, day: str, responses: List[Any], records_path: str = "data",
                raw_mode: str = "response") -> str:
        """Một lần chạy sourcing: mỗi response một dòng. Trả về đường manifest."""
        self.n += 1
        run = f"run{self.n:03d}"
        d = self.root / "landing" / dataset / f"ingest_date={day}"
        d.mkdir(parents=True, exist_ok=True)
        f = d / f"part-{run}-00001.ndjson.gz"
        with gzip.open(f, "wt", encoding="utf-8") as g:
            for i, resp in enumerate(responses):
                line: Dict[str, Any]
                if raw_mode == "response":
                    line = {"_request": {"page": i + 1}, "_records_path": records_path,
                            "_response": resp}
                else:
                    line = dict(resp)
                line.update({"_src_job": dataset, "_src_type": "api", "_src_cdc": "cdc5",
                             "_src_ingest_ts": f"{day}T0{self.n % 10}:00:00+00:00"})
                g.write(json.dumps(line) + "\n")
        m = self.root / "landing" / "_manifests" / dataset / f"ingest_date={day}" / f"{run}.json"
        m.parent.mkdir(parents=True, exist_ok=True)
        m.write_text(json.dumps({
            "job": dataset, "run_id": run, "dataset": dataset,
            "partition": f"ingest_date={day}", "raw_mode": raw_mode, "status": "SUCCESS",
            # KHONG dung f.as_uri(): no ma hoa `=` thanh %3D, Spark doc nguyen van
            "files": ["file:///" + f.as_posix().lstrip("/")],
        }))
        return str(m)


@pytest.fixture
def lake(tmp_path):
    return RawLake(tmp_path)


@pytest.fixture
def fresh_db(spark, request):
    """Mỗi test một database riêng — không dính bảng của test khác."""
    import re
    return "t_" + re.sub(r"[^a-z0-9_]", "_", request.node.name.lower())[:50]
