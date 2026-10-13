"""job_log của silver trên DynamoDB (moto) — bắt lỗi chỉ lộ ra khi ghi thật.

moto kiểm UpdateExpression như DynamoDB thật: dùng tên thuộc tính trùng từ khoá
dành riêng (status, metrics...) mà không đặt bí danh là ValidationException.
"""

from __future__ import annotations

import boto3
import pytest
from moto import mock_aws

from silver.state import SilverLogStore


@pytest.fixture
def store(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "t", "AWS_SECRET_ACCESS_KEY": "t",
                 "AWS_DEFAULT_REGION": "ap-southeast-1"}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        boto3.client("dynamodb").create_table(
            TableName="slv-log", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "job_name", "KeyType": "HASH"},
                       {"AttributeName": "run_key", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "job_name", "AttributeType": "S"},
                                  {"AttributeName": "run_key", "AttributeType": "S"}])
        yield SilverLogStore("slv-log")


def test_vong_doi_run_ghi_duoc_metrics(store):
    run_key = store.start_run("corebank_trans", "r1", {"layer": "silver"})
    store.finish_silver_run("corebank_trans", run_key, "SUCCESS",
                            {"rows_in": 10, "inserted": 10, "cast_errors": {}}, "")
    (run,) = store.recent_runs("corebank_trans")
    assert run["status"] == "SUCCESS" and run["metrics"]["inserted"] == 10


def test_trigger_gianh_quyen_mot_lan(store):
    m = "landing/_manifests/corebank/trans/ingest_date=2026-10-13/r1.json"
    assert store.claim_trigger("corebank_trans", m, "r1") is True
    assert store.claim_trigger("corebank_trans", m, "r2") is False
    store.mark_trigger("corebank_trans", m, "FAILED", "r1")
    assert store.claim_trigger("corebank_trans", m, "r3") is True   # FAILED thi gianh lai duoc
