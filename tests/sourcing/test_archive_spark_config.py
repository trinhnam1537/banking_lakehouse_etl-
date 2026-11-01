"""Archive (zip lồng zip, tar.gz + parquet) của httpfile, render SQL cho Spark JDBC,
và ràng buộc cấu hình engine spark / jdbc.parallel (10-25)."""

from __future__ import annotations

import io
import tarfile
import zipfile

import pytest

from sourcing.config import ConfigError
from sourcing.readers.fileparse import hive_parts, iter_archive, iter_records
from sourcing.readers.httpfile import HttpFileReader
from sourcing.sqltemplate import render_literal

from .conftest import make_cfg
from .test_reader_httpfile import _Resp

# ---------- archive ----------


def _zip(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in files.items():
            z.writestr(name, data)
    return buf.getvalue()


def test_zip_long_zip_kieu_uci_bank_marketing():
    inner = _zip({"bank-additional/full.csv": b'"a";"y"\n1;"no"\n2;"yes"\n',
                  "bank-additional/names.txt": b"x"})
    outer = _zip({"bank-additional.zip": inner, "bank.zip": _zip({"bank-full.csv": b"a\n9\n"}),
                  "__MACOSX/._rac": b"rac"})
    got = list(iter_archive(io.BytesIO(outer), "zip", ["bank-additional.zip!bank-additional/*.csv"]))
    assert [m for m, _ in got] == ["bank-additional.zip!bank-additional/full.csv"]


def test_tar_gz_doc_tuan_tu_va_cot_partition_hive():
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for fold, ids in ((0, ["a", "b"]), (1, ["c"])):
            pbuf = io.BytesIO()
            pq.write_table(pa.table({"client_id": ids, "target_1": [1] * len(ids)}), pbuf)
            data = pbuf.getvalue()
            info = tarfile.TarInfo(f"targets/fold={fold}/part-0.snappy.parquet")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        info = tarfile.TarInfo("targets/README")
        info.size = 1
        tf.addfile(info, io.BytesIO(b"x"))

    rows = []
    for member, stream in iter_archive(io.BytesIO(buf.getvalue()), "tar.gz", ["targets/*.parquet"]):
        for _, rec in iter_records(stream, "parquet", name=member):
            rows.append({**rec, **hive_parts(member)})
    assert rows == [{"client_id": "a", "target_1": 1, "fold": "0"},
                    {"client_id": "b", "target_1": 1, "fold": "0"},
                    {"client_id": "c", "target_1": 1, "fold": "1"}]


def test_httpfile_zip_gan_khoa_member_va_moc_cuoi(monkeypatch):
    url = "https://data.test/bank.zip"
    body = _zip({"bank.zip": _zip({"bank-full.csv": b'"age";"y"\n58;"no"\n44;"yes"\n'})})
    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=None: _Resp(body))
    cfg = make_cfg(source_type="httpfile", cdc={"mode": "cdc5"}, httpfile={
        "urls": {"uci": url}, "compression": "zip", "archive_members": ["bank.zip!bank-full.csv"],
        "file_format": "csv", "csv_delimiter": ";"})
    recs = [r for b in HttpFileReader(cfg).read(None) for r in b.records]
    assert [(r["age"], r["y"]) for r in recs] == [("58", "no"), ("44", "yes")]
    assert recs[0]["_src_file"] == "uci/bank.zip!bank-full.csv"


def test_httpfile_archive_khong_member_nao_khop_bao_loi(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=None: _Resp(_zip({"a.txt": b"x"})))
    cfg = make_cfg(source_type="httpfile", cdc={"mode": "cdc5"}, httpfile={
        "urls": {"k": "https://d.test/a.zip"}, "compression": "zip", "archive_members": ["*.csv"]})
    with pytest.raises(RuntimeError, match="khong co member nao khop"):
        list(HttpFileReader(cfg).read(None))


def test_config_archive_can_archive_members():
    with pytest.raises(ConfigError, match="archive_members"):
        make_cfg(source_type="httpfile", cdc={"mode": "cdc5"},
                 httpfile={"urls": {"k": "https://d.test/a.zip"}, "compression": "zip"})


# ---------- SQL cho Spark JDBC ----------


def test_render_literal_giu_chuoi_comment_va_ep_kieu():
    sql = "SELECT ':x', a::int FROM t -- :watermark\nWHERE id > :watermark AND s = :name"
    out = render_literal(sql, {"watermark": "1056320", "name": "O'Brien"})
    assert out == "SELECT ':x', a::int FROM t -- :watermark\nWHERE id > 1056320 AND s = 'O''Brien'"


# ---------- engine spark ----------

_SPARK_JDBC = {
    "source_type": "jdbc",
    "jdbc": {"secret_name": "fingraph/src/x", "driver": "mysql",
             "query": "SELECT id FROM t WHERE id > :watermark",
             "parallel": {"column": "id", "num_partitions": 8}},
    "cdc": {"mode": "cdc1", "watermark_column": "id", "watermark_type": "int", "initial_watermark": "0"},
    "runtime": {"engine": "spark", "profile": "src-spark"},
}


def test_spark_jdbc_parallel_hop_le():
    cfg = make_cfg(**_SPARK_JDBC)
    assert cfg.jdbc.parallel == {"column": "id", "num_partitions": 8}


def test_parallel_chi_cho_engine_spark():
    with pytest.raises(ConfigError, match="khong khop profile|chi dung voi"):
        make_cfg(**{**_SPARK_JDBC, "runtime": {"profile": "src-shell"}})


def test_profile_spark_bat_buoc_engine_spark():
    j = {k: v for k, v in _SPARK_JDBC["jdbc"].items() if k != "parallel"}
    with pytest.raises(ConfigError, match="khong khop profile"):
        make_cfg(**{**_SPARK_JDBC, "jdbc": j, "runtime": {"engine": "python_shell", "profile": "src-spark"}})


def test_parallel_khai_mot_bound_bi_chan():
    j = {**_SPARK_JDBC["jdbc"], "parallel": {"column": "id", "lower_bound": 1}}
    with pytest.raises(ConfigError, match="lower_bound va upper_bound"):
        make_cfg(**{**_SPARK_JDBC, "jdbc": j})


def test_spark_khong_ho_tro_httpfile():
    with pytest.raises(ConfigError, match="chua ho tro source_type"):
        make_cfg(source_type="httpfile", cdc={"mode": "cdc5"}, httpfile={"urls": {"k": "https://x"}},
                 runtime={"engine": "spark", "profile": "src-spark"})
