"""Điểm vào Glue job / chạy tay (sourcing/main.py) và package `sourcing`."""

from __future__ import annotations

import json
import os

import boto3
import pytest

import sourcing
import sourcing.runner as runner_mod
from sourcing import main as main_mod
from sourcing.readers.base import Batch

YAML = """\
name: tay
source_type: api
enabled: {enabled}
api:
  base_url: https://api.test
cdc:
  mode: cdc5
"""


@pytest.fixture
def yml(tmp_path):
    def _make(enabled=True):
        p = tmp_path / "tay.yml"
        p.write_text(YAML.format(enabled=str(enabled).lower()), encoding="utf-8")
        return str(p)
    return _make


class _OneBatch:
    def read(self, checkpoint):
        yield Batch(records=[{"a": 1}, {"a": 2}])

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_promote_to_env_chi_nhan_tham_so_viet_hoa(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP", raising=False)
    monkeypatch.delenv("job-bookmark-option", raising=False)
    main_mod._promote_to_env([
        "--KAFKA_BOOTSTRAP", "10.0.0.1:9092",
        "--job-bookmark-option", "job-bookmark-disable",
        "--FLAG_KHONG_GIA_TRI", "--enable-metrics",
    ])
    assert os.environ["KAFKA_BOOTSTRAP"] == "10.0.0.1:9092"
    assert "job-bookmark-option" not in os.environ
    assert "FLAG_KHONG_GIA_TRI" not in os.environ


def test_thieu_nguon_cau_hinh():
    with pytest.raises(SystemExit, match="--config"):
        main_mod.main([])


def test_job_tat_thi_thoat_0_khong_cham_aws(yml, monkeypatch):
    monkeypatch.setattr(main_mod, "SourceRunner", lambda **kw: pytest.fail("khong duoc chay"))
    assert main_mod.main(["--config", yml(enabled=False)]) == 0


def test_thieu_raw_bucket(yml, monkeypatch):
    monkeypatch.delenv("RAW_BUCKET", raising=False)
    with pytest.raises(SystemExit, match="RAW_BUCKET"):
        main_mod.main(["--config", yml(), "--JOBLOG_TABLE", "t"])


def test_chay_tu_file_yaml_end_to_end(yml, raw_bucket, joblog_table, monkeypatch):
    monkeypatch.setattr(runner_mod, "build_reader", lambda cfg, **kw: _OneBatch())
    rc = main_mod.main(["--config", yml(), "--RAW_BUCKET", raw_bucket,
                        "--JOBLOG_TABLE", joblog_table, "--RUN_ID", "sfn123",
                        "--extra-glue-flag", "x"])
    assert rc == 0
    keys = [o["Key"] for o in boto3.client("s3").list_objects_v2(Bucket=raw_bucket)["Contents"]]
    data = [k for k in keys if "/_manifests/" not in k]
    assert len(data) == 1 and "part-sfn123-" in data[0]
    assert [k for k in keys if "/_manifests/" in k][0].endswith("/sfn123.json")


def test_bien_moi_truong_thay_cho_tham_so(yml, raw_bucket, joblog_table, monkeypatch):
    monkeypatch.setattr(runner_mod, "build_reader", lambda cfg, **kw: _OneBatch())
    monkeypatch.setenv("RAW_BUCKET", raw_bucket)
    monkeypatch.setenv("JOBLOG_TABLE", joblog_table)
    assert main_mod.main(["--config", yml(), "--dry-run"]) == 0


def test_doc_cau_hinh_tu_dynamodb(config_table, raw_bucket, joblog_table, monkeypatch):
    """Đúng hình dạng item mà Terraform seed: config là MỘT chuỗi JSON."""
    boto3.resource("dynamodb").Table(config_table).put_item(Item={
        "job_name": "tu_dynamo", "enabled": True,
        "config": json.dumps({"name": "tu_dynamo", "source_type": "api",
                              "api": {"base_url": "https://api.test"},
                              "cdc": {"mode": "cdc5"}}),
    })
    seen = {}
    monkeypatch.setattr(runner_mod, "build_reader",
                        lambda cfg, **kw: seen.setdefault("cfg", cfg) and _OneBatch())
    rc = main_mod.main(["--JOB_CONFIG_TABLE", config_table, "--JOB_NAME", "tu_dynamo",
                        "--RAW_BUCKET", raw_bucket, "--JOBLOG_TABLE", joblog_table])
    assert rc == 0 and seen["cfg"].name == "tu_dynamo"


def test_job_khong_co_trong_dynamodb(config_table):
    with pytest.raises(SystemExit, match="terraform apply"):
        main_mod.main(["--JOB_CONFIG_TABLE", config_table, "--JOB_NAME", "khong_co"])


def test_dynamodb_enabled_false_thang_config(config_table, monkeypatch):
    """Cờ `enabled` cấp cao nhất (Terraform seed) quyết định, không phải trong config."""
    boto3.resource("dynamodb").Table(config_table).put_item(Item={
        "job_name": "j", "enabled": False,
        "config": json.dumps({"name": "j", "source_type": "api", "enabled": True,
                              "api": {"base_url": "https://x"}, "cdc": {"mode": "cdc5"}}),
    })
    monkeypatch.setattr(main_mod, "SourceRunner", lambda **kw: pytest.fail("khong duoc chay"))
    assert main_mod.main(["--JOB_CONFIG_TABLE", config_table, "--JOB_NAME", "j"]) == 0


# ---------- package ----------


@pytest.mark.parametrize("name", sourcing.__all__)
def test_moi_ten_trong_all_deu_import_duoc(name):
    assert getattr(sourcing, name) is not None


def test_ten_la_bao_attribute_error():
    with pytest.raises(AttributeError):
        sourcing.KhongTonTai  # noqa: B018
