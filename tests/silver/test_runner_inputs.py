"""Giải đầu vào của runner (manifest | partition) — S3 giả lập, không cần Spark."""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

from silver.config import Shape, from_dict
from silver.runner import SilverRunner

B = "lake"
DAY = "2026-09-28"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-southeast-1")
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(k, "test")
    with mock_aws():
        c = boto3.client("s3", region_name="ap-southeast-1")
        c.create_bucket(Bucket=B, CreateBucketConfiguration={"LocationConstraint": "ap-southeast-1"})
        base = f"landing/fdic/institutions/ingest_date={DAY}"
        for i in (1, 2):
            c.put_object(Bucket=B, Key=f"{base}/part-r1-0000{i}.ndjson.gz", Body=b"x")
        c.put_object(Bucket=B, Key=f"{base}/ghi-chu.txt", Body=b"x")        # khong phai du lieu
        c.put_object(Bucket=B, Key=f"landing/_manifests/fdic/institutions/ingest_date={DAY}/r1.json",
                     Body=json.dumps({"dataset": "fdic/institutions", "raw_mode": "response",
                                      "partition": f"ingest_date={DAY}",
                                      "files": [f"{base}/part-r1-00001.ndjson.gz"]}))
        yield c


def _cfg(**src):
    return from_dict({"name": "x", "source": {"dataset": "fdic/institutions", **src},
                      "target": {"table": "t"}, "columns": [{"name": "id"}]})


def _runner(cfg, **kw):
    return SilverRunner(spark=None, cfg=cfg, bucket=B, joblog_table=None, **kw)


def test_manifest_doc_dung_file_cua_lan_chay(s3):
    m = f"landing/_manifests/fdic/institutions/ingest_date={DAY}/r1.json"
    inp = _runner(_cfg(), manifest=m).resolve_inputs()
    assert inp.files == [f"s3://{B}/landing/fdic/institutions/ingest_date={DAY}/part-r1-00001.ndjson.gz"]
    assert inp.shape is Shape.API_RESPONSE and inp.label == m and inp.ingest_date == DAY


def test_manifest_sai_dataset_bi_tu_choi(s3):
    m = f"landing/_manifests/fdic/institutions/ingest_date={DAY}/r1.json"
    with pytest.raises(ValueError, match="dataset"):
        _runner(from_dict({"name": "x", "source": {"dataset": "gleif/lei_records"},
                           "target": {"table": "t"}, "columns": [{"name": "id"}]}),
                manifest=m).resolve_inputs()


def test_partition_lay_moi_file_du_lieu_va_muon_raw_mode_tu_manifest(s3):
    inp = _runner(_cfg(read="partition"), data_date=DAY).resolve_inputs()
    assert len(inp.files) == 2 and all(f.endswith(".ndjson.gz") for f in inp.files)
    assert inp.shape is Shape.API_RESPONSE      # khong doan 'records'


def test_backfill_khong_manifest_thi_doc_ca_ngay(s3):
    """Goi tay {job_name, data_date} -> router gui manifest '-' -> main doi thanh None."""
    inp = _runner(_cfg(), data_date=DAY).resolve_inputs()
    assert len(inp.files) == 2
