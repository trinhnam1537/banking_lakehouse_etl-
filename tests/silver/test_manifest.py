"""Đường dẫn manifest (silver/manifest.py)."""

from __future__ import annotations

import pytest

from silver.manifest import parse_manifest_key


def test_tach_dataset_va_ngay():
    key = "landing/_manifests/berka/trans/ingest_date=2026-10-12/abc123.json"
    assert parse_manifest_key(key) == ("berka/trans", "2026-10-12")


@pytest.mark.parametrize("key", [
    "landing/berka/trans/ingest_date=2026-10-12/part-1.ndjson.gz",   # file du lieu, khong phai manifest
    "landing/_manifests/berka/trans/abc.json",                       # thieu ingest_date
    "landing/_manifests/berka/trans/ingest_date=2026-10-12/x.txt",
])
def test_khong_phai_manifest(key):
    assert parse_manifest_key(key) is None
