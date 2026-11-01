"""Framework Spark của sourcing (11-2026): cỡ máy theo entry, khối `spark:`,
jdbc.parallel (rows_per_partition, predicates), s3file cdc1 trên Spark."""

from __future__ import annotations

import os
import shutil
import sys

import pytest

from sourcing.config import CAPACITY_OVERRIDES, ConfigError, apply_overrides
from sourcing.spark_main import output_files, plan_partitions

from .conftest import make_cfg

_JDBC = {
    "source_type": "jdbc",
    "jdbc": {"secret_name": "fingraph/src/x", "driver": "postgresql",
             "query": "SELECT id FROM t WHERE id > :watermark",
             "parallel": {"column": "id"}},
    "cdc": {"mode": "cdc1", "watermark_column": "id", "watermark_type": "int", "initial_watermark": "0"},
    "runtime": {"engine": "spark", "profile": "src-spark"},
}
_S3 = {
    "source_type": "s3file",
    "s3file": {"bucket": "b", "prefix": "landing/_inbox/x/", "file_format": "csv", "compression": "gzip"},
    "cdc": {"mode": "cdc1", "watermark_column": "_s3_key", "watermark_type": "string"},
    "runtime": {"engine": "spark", "profile": "src-spark"},
}


def _jdbc(parallel=None, **runtime):
    j = {**_JDBC["jdbc"], "parallel": parallel if parallel is not None else {"column": "id"}}
    return make_cfg(**{**_JDBC, "jdbc": j, "runtime": {**_JDBC["runtime"], **runtime}})


# ---------- cỡ máy ----------


@pytest.mark.parametrize("size,expected", [("S", ("G.1X", 2)), ("M", ("G.1X", 5)),
                                           ("L", ("G.2X", 10)), ("XL", ("G.2X", 20))])
def test_size_ra_dung_co_may(size, expected):
    assert _jdbc(size=size).runtime.glue_capacity() == expected


def test_khong_khai_co_thi_dung_mac_dinh_cua_job():
    assert _jdbc().runtime.glue_capacity() is None


def test_worker_khai_tuong_minh_de_len_size():
    rt = _jdbc(size="M", worker_type="G.2X").runtime
    assert rt.glue_capacity() == ("G.2X", 5)
    assert _jdbc(number_of_workers=7).runtime.glue_capacity() == ("G.1X", 7)


@pytest.mark.parametrize("runtime,match", [
    ({"size": "XXL"}, "runtime.size"),
    ({"worker_type": "Standard"}, "worker_type"),
    ({"number_of_workers": 50}, "ngoai"),
    ({"number_of_workers": 1}, "ngoai"),
])
def test_co_may_sai_bi_chan(runtime, match):
    with pytest.raises(ConfigError, match=match):
        _jdbc(**runtime)


def test_co_may_chi_cho_engine_spark():
    with pytest.raises(ConfigError, match="chi dung voi engine spark"):
        make_cfg(runtime={"size": "M"})


def test_khoi_spark_chi_cho_engine_spark():
    with pytest.raises(ConfigError, match="khoi 'spark'"):
        make_cfg(spark={"output_files": 4})


def test_override_co_may_duoc_phep_con_runtime_khac_bi_cam():
    raw = {"runtime": {"engine": "spark"}}
    out = apply_overrides(raw, {"runtime.size": "L"})
    assert out["runtime"] == {"engine": "spark", "size": "L"}
    assert "runtime.size" in CAPACITY_OVERRIDES
    with pytest.raises(ConfigError, match="khong override"):
        apply_overrides(raw, {"runtime.timeout_minutes": 5})


# ---------- khối spark ----------


def test_khoi_spark_hop_le():
    cfg = make_cfg(**_S3, spark={"sample": {"keys": ["a", "b"], "keep_pct": 25}, "output_files": 8,
                                 "max_partition_mb": 256, "select": ["a"],
                                 "conf": {"spark.sql.shuffle.partitions": "64"}})
    assert cfg.spark.sample["keep_pct"] == 25 and cfg.spark.output_files == 8


@pytest.mark.parametrize("spark,match", [
    ({"sample": {"keys": [], "keep_pct": 10}}, "sample.keys"),
    ({"sample": {"keys": ["a"], "keep_pct": 100}}, "keep_pct"),
    ({"sample": {"keys": ["a"], "keep_pct": 10, "seed": 1}}, "chi co"),
    ({"conf": {"spark.executor.memory": "8g"}}, "spark.sql"),
    ({"max_partition_mb": 1}, "max_partition_mb"),
    ({"output_files": -1}, "output_files"),
    ({"khong_co": 1}, "khoi 'spark' sai"),
])
def test_khoi_spark_sai_bi_chan(spark, match):
    with pytest.raises(ConfigError, match=match):
        make_cfg(**_S3, spark=spark)


# ---------- s3file trên spark ----------


def test_s3file_spark_cdc1_theo_khoa_s3():
    cfg = make_cfg(**_S3)
    assert cfg.cdc.watermark_column == "_s3_key"


def test_s3file_spark_cdc1_sai_cot_moc():
    with pytest.raises(ConfigError, match="_s3_key"):
        make_cfg(**{**_S3, "cdc": {"mode": "cdc1", "watermark_column": "event_ts"}})


def test_s3file_column_names_chi_cho_spark_csv():
    s3 = {**_S3["s3file"], "column_names": ["a", "b"]}
    assert make_cfg(**{**_S3, "s3file": s3}).s3file.column_names == ["a", "b"]
    with pytest.raises(ConfigError, match="chi dung voi csv"):
        make_cfg(**{**_S3, "s3file": {**s3, "file_format": "ndjson"}})
    with pytest.raises(ConfigError, match="chi dung voi engine spark"):
        make_cfg(source_type="s3file", s3file=s3, cdc=_S3["cdc"])


def test_s3file_spark_doc_duoc_parquet():
    assert make_cfg(**{**_S3, "s3file": {**_S3["s3file"], "file_format": "parquet"}}).s3file


# ---------- jdbc.parallel ----------


def test_parallel_predicates_hop_le():
    cfg = _jdbc({"predicates": ["m = 1", "m = 2"]})
    assert cfg.jdbc.parallel["predicates"] == ["m = 1", "m = 2"]


@pytest.mark.parametrize("parallel,match", [
    ({"column": "id", "predicates": ["a"]}, "MOT trong hai"),
    ({"num_partitions": 4}, "MOT trong hai"),
    ({"predicates": ["a", ""]}, "khac rong"),
    ({"predicates": ["a"], "num_partitions": 4}, "khong dung cung"),
    ({"column": "id", "num_partitions": 4, "rows_per_partition": 10_000}, "HOAC"),
    ({"column": "id", "rows_per_partition": 10}, "qua nho"),
    ({"column": "id", "rows_per_partition": 10_000, "lower_bound": 1, "upper_bound": 9}, "dem dong"),
])
def test_parallel_sai_bi_chan(parallel, match):
    with pytest.raises(ConfigError, match=match):
        _jdbc(parallel)


@pytest.mark.parametrize("lo,hi,count,par,expected", [
    (1, 1_000_000, None, {"num_partitions": 12}, 12),
    (1, 1_000_000, None, {}, 8),
    # CDC1 lần đầu nhiều dòng -> nhiều kết nối, chặn bởi max_partitions
    (1, 9_000_000, 9_000_000, {"rows_per_partition": 500_000}, 16),
    (1, 9_000_000, 9_000_000, {"rows_per_partition": 500_000, "max_partitions": 24}, 18),
    # lần sau ít dòng -> 1 kết nối
    (8_999_000, 9_000_000, 1_000, {"rows_per_partition": 500_000}, 1),
    # khoảng số nguyên hẹp hơn N
    (5, 7, None, {"num_partitions": 8}, 3),
    ("2023-01-01", "2023-12-31", None, {"num_partitions": 12}, 12),
])
def test_plan_partitions(lo, hi, count, par, expected):
    assert plan_partitions(lo, hi, count, par) == expected


@pytest.mark.parametrize("records,per_file,configured,expected", [
    (32_000_000, 500_000, 0, 64), (10, 500_000, 0, 1), (1_000_001, 500_000, 0, 3), (5, 10, 7, 7),
])
def test_output_files(records, per_file, configured, expected):
    assert output_files(records, per_file, configured) == expected


# ---------- entry IBM AML thật trong repo ----------


def test_entry_aml_transfers_history():
    from sourcing.config import load_config

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_config(os.path.join(root, "sourcing_etl/sources/ibm_aml/aml_transfers_history.yml"))
    assert cfg.runtime.glue_capacity() == ("G.1X", 5)
    assert cfg.target.dataset == "aml/transfers_history"


# ---------- lấy mẫu tất định (cần pyspark + Java) ----------


def _spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return shutil.which("java") is not None


@pytest.mark.skipif(not _spark_available(), reason="can pyspark + Java")
def test_runner_s3file_cdc1_doc_file_moi_dat_ten_cot_lay_mau(joblog_table, tmp_path):
    """SparkSourceRunner thật (dry run): liệt kê file sau mốc trên S3 (moto), Spark đọc
    bản local cùng khoá, đặt tên cột theo vị trí (header trùng tên kiểu IBM AML gốc),
    lấy mẫu theo khoá. Dry run không ghi S3 và không tiến mốc."""
    import gzip

    import boto3
    from pyspark.sql import SparkSession

    from sourcing.spark_main import SparkSourceRunner
    from sourcing.state import JobLogStore

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="src-bucket", CreateBucketConfiguration={"LocationConstraint": "ap-southeast-1"})
    keys = [f"landing/_inbox/x/dt=2023-01-0{d}/part-00000.csv.gz" for d in (1, 2, 3)]
    for d, k in enumerate(keys, start=1):
        body = "Account,Account,Amount\n" + "".join(f"A{d}{i},B{i},{i}\n" for i in range(500))
        data = gzip.compress(body.encode())
        s3.put_object(Bucket="src-bucket", Key=k, Body=data)
        (tmp_path / k).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / k).write_bytes(data)
    s3.put_object(Bucket="src-bucket", Key="landing/_inbox/x/_SUCCESS", Body=b"x")

    cfg = make_cfg(**{**_S3, "s3file": {**_S3["s3file"], "bucket": "src-bucket", "column_names": ["src", "dst", "amount"]},
                      "cdc": {**_S3["cdc"], "initial_watermark": keys[0]}},
                   spark={"sample": {"keys": ["src"], "keep_pct": 50}})

    class LocalRunner(SparkSourceRunner):
        def _uri(self, key):
            return "file:///" + (tmp_path / key).as_posix().lstrip("/")

    runner = LocalRunner(cfg, spark, raw_bucket="src-bucket", joblog_table=joblog_table, dry_run=True, s3_client=s3)
    assert runner.run() == "SUCCESS"

    df, info = runner.read_s3file(JobLogStore(joblog_table).read_checkpoint(cfg.name))
    # mốc ban đầu = file 1 -> chỉ đọc file 2, 3; bỏ _SUCCESS
    assert info["files_read"] == 2 and info["watermark"] == keys[2]
    assert df.columns[:3] == ["src", "dst", "amount"] and df.count() == 1000
    kept = runner._shape(df).count()
    assert 300 < kept < 700
    [run] = JobLogStore(joblog_table).recent_runs(cfg.name)
    assert int(run["records_read"]) == kept


@pytest.mark.skipif(not _spark_available(), reason="can pyspark + Java")
def test_sample_tat_dinh_va_khop_giua_cac_bang():
    from pyspark.sql import SparkSession

    from sourcing.spark_main import sample_condition

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    trans = spark.createDataFrame([(f"A{i % 400}", f"A{(i * 7) % 400}") for i in range(4000)],
                                  ["from_account", "to_account"])
    accounts = spark.createDataFrame([(f"A{i}",) for i in range(400)], ["from_account"])

    keep = sample_condition(["from_account", "to_account"], 50)
    t1 = sorted(tuple(r) for r in trans.where(keep).collect())
    t2 = sorted(tuple(r) for r in trans.where(keep).collect())
    assert t1 == t2 and 0 < len(t1) < 4000

    kept_acc = {r[0] for r in accounts.where(sample_condition(["from_account"], 50)).collect()}
    # Mọi giao dịch còn lại có CẢ HAI đầu nằm trong tập tài khoản giữ lại (cùng hàm băm)
    assert all(a in kept_acc and b in kept_acc for a, b in t1)
    # Hai khoá độc lập: giữ ~50% tài khoản -> ~25% giao dịch
    assert 0.15 < len(t1) / 4000 < 0.35
