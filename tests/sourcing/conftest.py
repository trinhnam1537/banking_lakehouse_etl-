"""Fixture dùng chung cho test framework sourcing.

Mọi thứ AWS (DynamoDB, S3, Secrets Manager, SQS) đều giả lập bằng `moto`
trong bộ nhớ — test không bao giờ chạm tài khoản thật, không tốn tiền, và
chạy được trong CI mà không cần credential.
"""

from __future__ import annotations

import copy
from typing import Any, Dict

import boto3
import pytest
from moto import mock_aws

from sourcing.config import from_dict

REGION = "ap-southeast-1"


@pytest.fixture(autouse=True)
def _fake_aws_env(monkeypatch):
    # Credential gia: lo co test nao quen mock_aws thi boto3 cung khong the
    # dung profile that tren may de goi len AWS.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


@pytest.fixture
def aws():
    with mock_aws():
        yield


@pytest.fixture
def joblog_table(aws) -> str:
    """Bảng job_log, cùng key schema với terraform/modules/sourcing/dynamodb.tf."""
    name = "test-src-job-log"
    boto3.client("dynamodb").create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[
            {"AttributeName": "job_name", "KeyType": "HASH"},
            {"AttributeName": "run_key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "job_name", "AttributeType": "S"},
            {"AttributeName": "run_key", "AttributeType": "S"},
        ],
    )
    return name


@pytest.fixture
def config_table(aws) -> str:
    name = "test-src-job-config"
    boto3.client("dynamodb").create_table(
        TableName=name,
        BillingMode="PAY_PER_REQUEST",
        KeySchema=[{"AttributeName": "job_name", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "job_name", "AttributeType": "S"}],
    )
    return name


@pytest.fixture
def raw_bucket(aws) -> str:
    name = "test-raw-bucket"
    boto3.client("s3").create_bucket(
        Bucket=name, CreateBucketConfiguration={"LocationConstraint": REGION}
    )
    return name


def _deep_merge(base: Dict[str, Any], over: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


BASE_ENTRY: Dict[str, Any] = {
    "name": "t_job",
    "source_type": "api",
    # records: phan lon test kiem logic boc/phan trang/moc theo tung ban ghi.
    # Che do response (mac dinh that) co test rieng trong test_reader_api.py.
    "api": {"base_url": "https://api.test", "rate_limit_per_sec": 0, "raw_mode": "records"},
    "cdc": {"mode": "cdc5"},
}


def entry(**over) -> Dict[str, Any]:
    """Entry hợp lệ tối thiểu, ghi đè từng phần: entry(cdc={"mode": "cdc1"})."""
    return _deep_merge(BASE_ENTRY, over)


def make_cfg(**over):
    return from_dict(entry(**over))
