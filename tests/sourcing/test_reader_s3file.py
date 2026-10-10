"""Reader file S3 (sourcing/readers/s3file.py)."""

from __future__ import annotations

import gzip
import json

import boto3
import pytest

import sourcing.readers.s3file as s3file_mod
from sourcing.readers.s3file import S3FileReader
from sourcing.state import Checkpoint

from .conftest import make_cfg

INBOX = "landing/_inbox/market/"


def _cfg(bucket, **s3file):
    return make_cfg(
        source_type="s3file",
        s3file={"bucket": bucket, "prefix": INBOX, **s3file},
        cdc={"mode": "cdc1", "watermark_column": "_s3_key", "watermark_type": "string"},
    )


def _put(bucket, name, body: bytes):
    boto3.client("s3").put_object(Bucket=bucket, Key=INBOX + name, Body=body)


def test_csv_co_bom_va_dau_phan_cach(raw_bucket):
    _put(raw_bucket, "a.csv", "﻿sym;price\nFPT;120\nVNM;70\n".encode("utf-8"))
    r = S3FileReader(_cfg(raw_bucket, file_format="csv", csv_delimiter=";"))
    (b,) = list(r.read(Checkpoint("t_job")))
    assert [(x["sym"], x["price"]) for x in b.records] == [("FPT", "120"), ("VNM", "70")]
    assert b.records[0]["_src_file"] == INBOX + "a.csv"
    assert b.watermark == INBOX + "a.csv"


def test_json_mang_va_object(raw_bucket):
    _put(raw_bucket, "1.json", json.dumps([{"a": 1}, {"a": 2}]).encode())
    _put(raw_bucket, "2.json", json.dumps({"a": 3}).encode())
    batches = list(S3FileReader(_cfg(raw_bucket, file_format="json")).read(None))
    assert [[x["a"] for x in b.records] for b in batches] == [[1, 2], [3]]


def test_ndjson_gzip_theo_duoi_file(raw_bucket):
    body = gzip.compress(b'{"a":1}\n\n{"a":2}\n')
    _put(raw_bucket, "x.ndjson.gz", body)
    (b,) = list(S3FileReader(_cfg(raw_bucket, file_format="ndjson")).read(None))
    assert [x["a"] for x in b.records] == [1, 2]


def test_tiep_tuc_sau_khoa_checkpoint(raw_bucket):
    for n in ("dt=2026-09-01/a.json", "dt=2026-09-02/b.json", "dt=2026-09-03/c.json"):
        _put(raw_bucket, n, b"[]")
    r = S3FileReader(_cfg(raw_bucket))
    ck = Checkpoint("t_job", {"default": INBOX + "dt=2026-09-01/a.json"})
    assert [b.watermark for b in r.read(ck)] == [
        INBOX + "dt=2026-09-02/b.json", INBOX + "dt=2026-09-03/c.json"]


def test_bo_qua_thu_muc_va_file_rong(raw_bucket):
    _put(raw_bucket, "sub/", b"")
    _put(raw_bucket, "rong.json", b"")
    _put(raw_bucket, "that.json", b'[{"a":1}]')
    assert [b.meta["file"] for b in S3FileReader(_cfg(raw_bucket)).read(None)] == [INBOX + "that.json"]


def test_khong_co_file_moi(raw_bucket):
    assert list(S3FileReader(_cfg(raw_bucket)).read(None)) == []


def test_tran_so_file_moi_lan_chay(raw_bucket, monkeypatch, capsys):
    monkeypatch.setattr(s3file_mod, "MAX_FILES_PER_RUN", 2)
    for i in range(3):
        _put(raw_bucket, f"{i}.json", b"[]")
    batches = list(S3FileReader(_cfg(raw_bucket)).read(None))
    assert len(batches) == 2
    assert "tran mot lan chay" in capsys.readouterr().out


def test_dinh_dang_khong_ho_tro(raw_bucket):
    _put(raw_bucket, "a.xml", b"<x/>")
    with pytest.raises(ValueError, match="file_format khong ho tro: xml"):
        list(S3FileReader(_cfg(raw_bucket, file_format="xml")).read(None))
