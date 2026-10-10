"""Ghi tầng raw xuống S3 (sourcing/writer.py)."""

from __future__ import annotations

import datetime as dt
import gzip
import json

import boto3

from sourcing.writer import RawWriter

from .conftest import make_cfg


def _read_all(bucket: str, prefix: str):
    s3 = boto3.client("s3")
    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", []))
    out = {}
    for k in keys:
        body = gzip.decompress(s3.get_object(Bucket=bucket, Key=k)["Body"].read())
        out[k] = [json.loads(line) for line in body.decode("utf-8").splitlines()]
    return out


def test_ghi_ndjson_gzip_dung_prefix_va_metadata(raw_bucket):
    cfg = make_cfg(target={"dataset": "binance/depth"})
    w = RawWriter(cfg, raw_bucket, "run1")
    w.write([{"a": 1, "ten": "Việt"}, {"a": 2, "ts": dt.datetime(2026, 9, 1)}])

    today = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    assert w.prefix == f"landing/binance/depth/ingest_date={today}"

    files = _read_all(raw_bucket, w.prefix + "/")
    ((key, rows),) = files.items()
    assert key.startswith(f"{w.prefix}/part-run1-00001-") and key.endswith(".ndjson.gz")
    # ensure_ascii=False giu nguyen dau; default=str cho datetime
    assert rows == [{"a": 1, "ten": "Việt"}, {"a": 2, "ts": "2026-09-01 00:00:00"}]

    head = boto3.client("s3").head_object(Bucket=raw_bucket, Key=key)
    assert head["Metadata"] == {"job": "t_job", "run-id": "run1", "cdc-mode": "cdc5", "records": "2"}
    assert head["ContentEncoding"] == "gzip"

    assert (w.result.files, w.result.records) == (1, 2)
    assert w.result.bytes_out > 0 and w.result.keys == [key]


def test_chia_file_theo_max_records_per_file(raw_bucket):
    cfg = make_cfg(target={"max_records_per_file": 2})
    w = RawWriter(cfg, raw_bucket, "r")
    w.write([{"i": i} for i in range(5)])

    files = _read_all(raw_bucket, w.prefix + "/")
    assert [len(v) for v in files.values()] == [2, 2, 1]
    # part-<run_id>-<seq>-<uuid>.ndjson.gz
    assert [k.rsplit("/", 1)[1].split("-")[2] for k in files] == ["00001", "00002", "00003"]


def test_lo_rong_khong_ghi_gi(raw_bucket):
    w = RawWriter(make_cfg(), raw_bucket, "r")
    w.write([])
    assert w.result.files == 0
    assert _read_all(raw_bucket, "landing/") == {}


def test_noi_dung_giong_nhau_cho_byte_giong_nhau(raw_bucket):
    """mtime=0 trong gzip: hai lần chạy cùng dữ liệu so sánh được bằng byte."""
    cfg = make_cfg(cdc={"mode": "cdc1", "watermark_column": "ts"})  # cdc5 se xoa file w1
    w1 = RawWriter(cfg, raw_bucket, "r1")
    w2 = RawWriter(cfg, raw_bucket, "r2")
    w1.write([{"x": 1}])
    w2.write([{"x": 1}])
    assert w1.result.bytes_out == w2.result.bytes_out
    s3 = boto3.client("s3")
    b1 = s3.get_object(Bucket=raw_bucket, Key=w1.result.keys[0])["Body"].read()
    b2 = s3.get_object(Bucket=raw_bucket, Key=w2.result.keys[0])["Body"].read()
    assert b1 == b2


def test_cdc5_ghi_de_partition_hom_nay(raw_bucket):
    """Chạy lại CDC5 trong ngày không được nhân đôi dữ liệu."""
    cfg = make_cfg(cdc={"mode": "cdc5"})
    RawWriter(cfg, raw_bucket, "r1").write([{"v": "cu"}] * 3)

    w = RawWriter(cfg, raw_bucket, "r2")
    w.write([{"v": "moi"}])
    w.write([{"v": "moi2"}])  # lo thu hai KHONG duoc xoa lo thu nhat

    rows = [r for v in _read_all(raw_bucket, w.prefix + "/").values() for r in v]
    assert sorted(r["v"] for r in rows) == ["moi", "moi2"]


def test_cdc5_khong_dung_partition_ngay_khac(raw_bucket):
    cfg = make_cfg(cdc={"mode": "cdc5"})
    s3 = boto3.client("s3")
    s3.put_object(Bucket=raw_bucket, Key="landing/t_job/ingest_date=2020-01-01/old.ndjson.gz", Body=b"x")
    # Prefix "ingest_date=<hom nay>" khong duoc an sang dataset co ten bat dau giong
    s3.put_object(Bucket=raw_bucket, Key="landing/t_job_khac/x.ndjson.gz", Body=b"x")

    RawWriter(cfg, raw_bucket, "r").write([{"v": 1}])
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket=raw_bucket)["Contents"]}
    assert "landing/t_job/ingest_date=2020-01-01/old.ndjson.gz" in keys
    assert "landing/t_job_khac/x.ndjson.gz" in keys


def test_cdc5_xoa_duoc_hon_1000_file(raw_bucket):
    """delete_objects giới hạn 1000 khoá mỗi lần gọi."""
    cfg = make_cfg(cdc={"mode": "cdc5"})
    w = RawWriter(cfg, raw_bucket, "r")
    s3 = boto3.client("s3")
    for i in range(1005):
        s3.put_object(Bucket=raw_bucket, Key=f"{w.prefix}/old-{i:05d}", Body=b"")
    w.write([{"v": 1}])
    left = s3.list_objects_v2(Bucket=raw_bucket, Prefix=w.prefix + "/")["Contents"]
    assert [o["Key"] for o in left] == w.result.keys


def test_cdc1_append_khong_xoa(raw_bucket):
    cfg = make_cfg(cdc={"mode": "cdc1", "watermark_column": "ts"})
    RawWriter(cfg, raw_bucket, "r1").write([{"v": 1}])
    w = RawWriter(cfg, raw_bucket, "r2")
    w.write([{"v": 2}])
    assert len(_read_all(raw_bucket, w.prefix + "/")) == 2
