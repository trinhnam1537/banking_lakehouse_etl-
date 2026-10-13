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


def _w(cfg, bucket, run_id, records):
    w = RawWriter(cfg, bucket, run_id)
    w.write(records)
    w.flush()
    return w


def test_ghi_ndjson_gzip_dung_prefix_va_metadata(raw_bucket):
    cfg = make_cfg(target={"dataset": "binance/depth"})
    w = RawWriter(cfg, raw_bucket, "run1")
    w.write([{"a": 1, "ten": "Việt"}, {"a": 2, "ts": dt.datetime(2026, 9, 1)}])
    # Con trong bo dem -> CHUA nam tren S3 (runner phai flush truoc khi tien moc)
    assert w.result.files == 0 and w.pending_records == 2
    w.flush()

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


def test_gom_nhieu_lo_vao_mot_file(raw_bucket):
    """Lô nhỏ của reader (JDBC 5.000 dòng) không còn sinh file nhỏ."""
    w = RawWriter(make_cfg(), raw_bucket, "r")
    for i in range(10):
        w.write([{"lo": i, "j": j} for j in range(3)])
    w.flush()
    assert w.result.files == 1 and w.result.records == 30


def test_cat_file_theo_max_records_per_file(raw_bucket):
    cfg = make_cfg(target={"max_records_per_file": 2})
    w = _w(cfg, raw_bucket, "r", [{"i": i} for i in range(5)])

    files = _read_all(raw_bucket, w.prefix + "/")
    assert [len(v) for v in files.values()] == [2, 2, 1]
    # part-<run_id>-<seq>-<uuid>.ndjson.gz
    assert [k.rsplit("/", 1)[1].split("-")[2] for k in files] == ["00001", "00002", "00003"]


def test_cat_file_theo_dung_luong_nen(raw_bucket):
    w = RawWriter(make_cfg(), raw_bucket, "r")
    w._max_bytes = 1  # moi ban ghi vuot nguong ngay
    w.write([{"i": i} for i in range(3)])
    w.flush()
    assert w.result.files == 3


def test_lo_rong_va_flush_rong_khong_ghi_gi(raw_bucket):
    w = RawWriter(make_cfg(), raw_bucket, "r")
    w.write([])
    w.flush()
    assert w.result.files == 0
    assert _read_all(raw_bucket, "landing/") == {}


def test_noi_dung_giong_nhau_cho_byte_giong_nhau(raw_bucket):
    """mtime=0 trong gzip: hai lần chạy cùng dữ liệu so sánh được bằng byte."""
    cfg = make_cfg(cdc={"mode": "cdc1", "watermark_column": "ts"})
    w1 = _w(cfg, raw_bucket, "r1", [{"x": 1}])
    w2 = _w(cfg, raw_bucket, "r2", [{"x": 1}])
    assert w1.result.bytes_out == w2.result.bytes_out
    s3 = boto3.client("s3")
    b1 = s3.get_object(Bucket=raw_bucket, Key=w1.result.keys[0])["Body"].read()
    b2 = s3.get_object(Bucket=raw_bucket, Key=w2.result.keys[0])["Body"].read()
    assert b1 == b2


def test_cdc5_anh_chup_cu_chi_bi_xoa_sau_khi_anh_moi_du(raw_bucket):
    """Chạy lại CDC5 trong ngày không nhân đôi dữ liệu, và job chết giữa chừng
    không làm mất ảnh chụp cũ (write-then-swap)."""
    cfg = make_cfg(cdc={"mode": "cdc5"})
    _w(cfg, raw_bucket, "r1", [{"v": "cu"}] * 3)

    w = _w(cfg, raw_bucket, "r2", [{"v": "moi"}, {"v": "moi2"}])
    # Truoc cleanup: anh cu VAN CON (lan r2 chet o day thi hom nay van co du lieu)
    assert len(_read_all(raw_bucket, w.prefix + "/")) == 2
    assert w.cleanup_superseded() == 1

    rows = [r for v in _read_all(raw_bucket, w.prefix + "/").values() for r in v]
    assert sorted(r["v"] for r in rows) == ["moi", "moi2"]


def test_cdc5_khong_dung_partition_ngay_khac(raw_bucket):
    cfg = make_cfg(cdc={"mode": "cdc5"})
    s3 = boto3.client("s3")
    s3.put_object(Bucket=raw_bucket, Key="landing/t_job/ingest_date=2020-01-01/old.ndjson.gz", Body=b"x")
    # Prefix "ingest_date=<hom nay>" khong duoc an sang dataset co ten bat dau giong
    s3.put_object(Bucket=raw_bucket, Key="landing/t_job_khac/x.ndjson.gz", Body=b"x")

    _w(cfg, raw_bucket, "r", [{"v": 1}]).cleanup_superseded()
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
    w.flush()
    assert w.cleanup_superseded() == 1005
    left = s3.list_objects_v2(Bucket=raw_bucket, Prefix=w.prefix + "/")["Contents"]
    assert [o["Key"] for o in left] == w.result.keys


def test_cdc1_append_khong_xoa(raw_bucket):
    cfg = make_cfg(cdc={"mode": "cdc1", "watermark_column": "ts"})
    _w(cfg, raw_bucket, "r1", [{"v": 1}])
    w = _w(cfg, raw_bucket, "r2", [{"v": 2}])
    assert len(_read_all(raw_bucket, w.prefix + "/")) == 2
