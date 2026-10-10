"""Lambda router: manifest -> job silver -> SQS FIFO. AWS giả lập bằng moto."""

from __future__ import annotations

import json

import boto3
import pytest
from moto import mock_aws

import silver.lambda_router as R

REGION = "ap-southeast-1"
MANIFEST = "landing/_manifests/gleif/lei_records/ingest_date=2026-09-28/abc123.json"


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(k, "test")
    R._CACHE.update(at=0.0, jobs=[])
    with mock_aws():
        ddb = boto3.resource("dynamodb", region_name=REGION)
        for name, keys in (("cfg", [("job_name", "HASH")]),
                           ("log", [("job_name", "HASH"), ("run_key", "RANGE")])):
            ddb.create_table(TableName=name, BillingMode="PAY_PER_REQUEST",
                             KeySchema=[{"AttributeName": a, "KeyType": t} for a, t in keys],
                             AttributeDefinitions=[{"AttributeName": a, "AttributeType": "S"} for a, _ in keys])
        cfg = ddb.Table("cfg")
        for job, ds, en in (("gleif_legal_entity", "gleif/lei_records", True),
                            ("gleif_parent_entity", "gleif/lei_records", True),
                            ("fdic_institution", "fdic/institutions", True),
                            ("gleif_tat", "gleif/lei_records", False)):
            cfg.put_item(Item={"job_name": job, "enabled": en, "source_dataset": ds, "config": "{}"})
        sqs = boto3.client("sqs", region_name=REGION)
        q = sqs.create_queue(QueueName="run.fifo", Attributes={"FifoQueue": "true"})["QueueUrl"]
        monkeypatch.setenv("JOB_CONFIG_TABLE", "cfg")
        monkeypatch.setenv("JOBLOG_TABLE", "log")
        monkeypatch.setenv("RUN_QUEUE_URL", q)
        yield sqs, q


def _sqs_event(*keys):
    return {"Records": [{"messageId": f"m{i}", "body": json.dumps(
        {"detail-type": "Object Created", "detail": {"bucket": {"name": "lake"}, "object": {"key": k}}})}
        for i, k in enumerate(keys)]}


def _drain(sqs, q):
    out = []
    while True:
        r = sqs.receive_message(QueueUrl=q, MaxNumberOfMessages=10, AttributeNames=["All"])
        msgs = r.get("Messages", [])
        if not msgs:
            return out
        for m in msgs:
            out.append((m["Attributes"]["MessageGroupId"], json.loads(m["Body"])))
            sqs.delete_message(QueueUrl=q, ReceiptHandle=m["ReceiptHandle"])


def test_parse_manifest_key():
    assert R.parse_manifest_key(MANIFEST) == ("gleif/lei_records", "2026-09-28")
    assert R.parse_manifest_key("landing/gleif/lei_records/ingest_date=2026-09-28/part-1.ndjson.gz") is None


def test_mot_dataset_nuoi_nhieu_bang_silver(env):
    sqs, q = env
    assert R.handler(_sqs_event(MANIFEST)) == {"batchItemFailures": []}
    msgs = _drain(sqs, q)
    assert sorted(g for g, _ in msgs) == ["gleif_legal_entity", "gleif_parent_entity"]  # khong co job tat
    body = msgs[0][1]
    assert body["manifest"] == MANIFEST and body["data_date"] == "2026-09-28" and body["run_id"]


def test_manifest_den_hai_lan_chi_chay_mot_lan(env):
    sqs, q = env
    R.handler(_sqs_event(MANIFEST))
    R.handler(_sqs_event(MANIFEST))          # S3/EventBridge giao lai
    assert len(_drain(sqs, q)) == 2          # van chi 2 (moi bang mot lan)


def test_trigger_failed_duoc_gianh_lai(env):
    sqs, q = env
    R.handler(_sqs_event(MANIFEST))
    _drain(sqs, q)
    from silver.state import SilverLogStore
    SilverLogStore("log").mark_trigger("gleif_legal_entity", MANIFEST, "FAILED", "r1")
    R.handler(_sqs_event(MANIFEST))
    assert [g for g, _ in _drain(sqs, q)] == ["gleif_legal_entity"]


def test_file_khong_phai_manifest_bi_bo_qua(env):
    sqs, q = env
    R.handler(_sqs_event("landing/_manifests/readme.txt"))
    assert _drain(sqs, q) == []


def test_mot_message_hong_khong_keo_ca_lo(env, monkeypatch):
    sqs, q = env
    ev = _sqs_event(MANIFEST)
    ev["Records"].append({"messageId": "hong", "body": "khong phai json"})
    assert R.handler(ev) == {"batchItemFailures": [{"itemIdentifier": "hong"}]}
    assert len(_drain(sqs, q)) == 2


def test_goi_tay_de_backfill(env):
    sqs, q = env
    R.handler({"job_name": "fdic_institution", "data_date": "2026-09-20"})
    ((group, body),) = _drain(sqs, q)
    assert group == "fdic_institution" and body["data_date"] == "2026-09-20" and body["manifest"] == "-"
