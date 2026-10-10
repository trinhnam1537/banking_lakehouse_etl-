"""Fixture cho Lambda trigger.

DynamoDB giả lập bằng moto. Step Functions thì dùng một client GIẢ tự viết:
test cần kiểm tra đúng input gửi đi và điều khiển trạng thái execution trả
về (RUNNING rồi SUCCEEDED...), việc mà moto không cho chỉnh.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import boto3
import pytest
import yaml
from botocore.exceptions import ClientError
from moto import mock_aws

from orchestrator.job_types import SilverType, SourcingType
from orchestrator.trigger import Trigger

REGION = "ap-southeast-1"
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUNNER_ARN = f"arn:aws:states:{REGION}:111122223333:stateMachine:test-job-runner"


@pytest.fixture(autouse=True)
def _fake_aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_PROFILE", raising=False)


def _create(ddb, name: str, with_range: bool) -> None:
    keys = [{"AttributeName": "job_name", "KeyType": "HASH"}]
    attrs = [{"AttributeName": "job_name", "AttributeType": "S"}]
    if with_range:
        keys.append({"AttributeName": "run_key", "KeyType": "RANGE"})
        attrs.append({"AttributeName": "run_key", "AttributeType": "S"})
    ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                     KeySchema=keys, AttributeDefinitions=attrs)


class FakeSfn:
    """start_execution/describe_execution đủ dùng cho trigger."""

    def __init__(self):
        self.started: List[Dict[str, Any]] = []
        self.statuses: List[str] = ["SUCCEEDED"]
        self.fail_start: str = ""
        self.cause = ""

    def start_execution(self, stateMachineArn, name, input):  # noqa: A002
        if self.fail_start or any(s["name"] == name for s in self.started):
            code = self.fail_start or "ExecutionAlreadyExists"
            raise ClientError({"Error": {"Code": code, "Message": code}}, "StartExecution")
        self.started.append({"arn": stateMachineArn, "name": name, "input": json.loads(input)})
        return {"executionArn": f"{stateMachineArn.replace(':stateMachine:', ':execution:')}:{name}"}

    def describe_execution(self, executionArn):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        out = {"executionArn": executionArn, "status": status}
        if self.cause:
            out["cause"] = self.cause
        return out


class Env:
    """Hai bảng mỗi luồng, giống terraform/modules/{sourcing,silver}/dynamodb.tf."""

    SRC_CFG, SRC_LOG = "t-src-job-config", "t-src-job-log"
    SLV_CFG, SLV_LOG = "t-slv-job-config", "t-slv-job-log"

    def __init__(self):
        self.ddb = boto3.resource("dynamodb", region_name=REGION)
        client = boto3.client("dynamodb", region_name=REGION)
        for n, rng in ((self.SRC_CFG, False), (self.SRC_LOG, True),
                       (self.SLV_CFG, False), (self.SLV_LOG, True)):
            _create(client, n, rng)
        self.sfn = FakeSfn()
        self.slept: List[float] = []
        self.trigger = Trigger(
            {"src": SourcingType(self.SRC_CFG, self.SRC_LOG, "t-src-", dynamodb=self.ddb),
             "slv": SilverType(self.SLV_CFG, self.SLV_LOG, "t-slv-", dynamodb=self.ddb)},
            RUNNER_ARN, sfn=self.sfn, sleep=self.slept.append,
        )

    # ---------- seed đúng hình dạng item Terraform ghi ----------

    def seed_src(self, name: str, enabled: bool = True, **cfg_over) -> Dict[str, Any]:
        cfg = {"name": name, "source_type": "api",
               "api": {"base_url": "https://api.test", "path": "x", "max_pages": 50},
               "cdc": {"mode": "cdc5"},
               "target": {"dataset": f"demo/{name}"}}
        for k, v in cfg_over.items():
            cfg[k] = v
        self.ddb.Table(self.SRC_CFG).put_item(Item={
            "job_name": name, "enabled": enabled, "config": json.dumps(cfg)})
        return cfg

    def seed_slv_from_repo(self, rel: str = "tests/fixtures/silver/demo_trans.yml") -> str:
        with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        self.ddb.Table(self.SLV_CFG).put_item(Item={
            "job_name": cfg["name"], "enabled": True,
            "source_dataset": cfg["source"]["dataset"], "config": json.dumps(cfg)})
        return cfg["name"]

    def item(self, table: str, name: str, run_key: str):
        return self.ddb.Table(table).get_item(
            Key={"job_name": name, "run_key": run_key}).get("Item")


@pytest.fixture
def env():
    with mock_aws():
        yield Env()
