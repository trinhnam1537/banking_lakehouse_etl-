"""Reader file qua HTTP (httpfile) và cửa sổ ngày của reader API."""

from __future__ import annotations

import datetime as dt
import gzip
import io
import json

import pytest

from sourcing.readers.api import ApiReader
from sourcing.readers.httpfile import HttpFileReader
from sourcing.state import Checkpoint

from .conftest import make_cfg

INDEX = "https://data.test/datasets/latest/sanctions/index.json"
DELTA = "https://data.test/artifacts/sanctions/v3/delta.json"


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


@pytest.fixture
def web(monkeypatch):
    pages = {}
    calls = []

    def _open(req, timeout=None):
        url = req.full_url
        calls.append(url)
        body = pages[url]
        return _Resp(body if isinstance(body, bytes) else json.dumps(body).encode())

    monkeypatch.setattr("urllib.request.urlopen", _open)
    return pages, calls


def _ndjson(*recs):
    return "".join(json.dumps(r) + "\n" for r in recs).encode()


def _delta_cfg(**over):
    h = {"index_url": INDEX, "index_follow": ["delta_url"], "files_path": "versions",
         "file_format": "ndjson", "require_contiguous": True, **over}
    return make_cfg(source_type="httpfile", httpfile=h,
                    cdc={"mode": "cdc1", "watermark_column": "_file_key", "watermark_type": "string"})


def _seed_delta(pages):
    pages[INDEX] = {"version": "v3", "delta_url": DELTA}
    pages[DELTA] = {"versions": {f"v{i}": f"https://data.test/v{i}/entities.delta.json"
                                 for i in (1, 2, 3)}}
    for i in (1, 2, 3):
        pages[f"https://data.test/v{i}/entities.delta.json"] = _ndjson(
            {"op": "ADD", "entity": {"id": f"e{i}"}}, {"op": "DEL", "entity": {"id": f"x{i}"}})


def test_delta_chi_tai_phien_ban_moi_hon_moc_theo_thu_tu(web):
    pages, calls = web
    _seed_delta(pages)
    batches = list(HttpFileReader(_delta_cfg()).read(Checkpoint("t", {"default": "v1"})))
    assert [b.watermark for b in batches] == ["v2", "v3"]
    assert batches[0].records[0]["_src_file"] == "v2" and batches[0].records[0]["_src_row"] == 1
    assert not any("v1/entities" in c for c in calls)


def test_delta_mat_lien_tuc_bao_loi(web):
    pages, _ = web
    _seed_delta(pages)
    with pytest.raises(RuntimeError, match="MAT LIEN TUC"):
        list(HttpFileReader(_delta_cfg()).read(Checkpoint("t", {"default": "v0"})))


def test_delta_tran_so_file_moi_lan(web, capsys):
    pages, _ = web
    _seed_delta(pages)
    batches = list(HttpFileReader(_delta_cfg(max_files_per_run=1)).read(None))
    assert [b.watermark for b in batches] == ["v1"]
    assert "lan sau tiep tuc" in capsys.readouterr().out


def test_urls_tinh_cdc5_csv_gzip_chia_lo(web):
    pages, _ = web
    url = "https://data.test/targets.simple.csv.gz"
    rows = "id,name\n" + "".join(f"{i},Ten {i}\n" for i in range(5))
    pages[url] = gzip.compress(rows.encode())
    cfg = make_cfg(source_type="httpfile", cdc={"mode": "cdc5"},
                   httpfile={"urls": {"targets": url}, "batch_size": 2})
    batches = list(HttpFileReader(cfg).read(None))
    assert [len(b) for b in batches] == [2, 2, 1]
    assert all(b.watermark is None for b in batches)   # CDC5 khong co moc
    assert batches[-1].records[-1]["name"] == "Ten 4"


def test_csv_header_lap_lai_giua_file(web):
    """CNB đổi danh sách tiền tệ giữa năm bằng một dòng header mới."""
    pages, _ = web
    url = "https://cnb.test/year.txt?year=1995"
    pages[url] = b"Date|1 USD|1 DEM\n02.01.1995|27.9|18.0\nDate|1 USD|1 XEU\n03.07.1995|26.1|34.3\n"
    cfg = make_cfg(source_type="httpfile", cdc={"mode": "cdc5"},
                   httpfile={"urls": {"1995": url}, "csv_delimiter": "|",
                             "csv_repeated_header": True})
    (b,) = list(HttpFileReader(cfg).read(None))
    assert [{k: v for k, v in r.items() if not k.startswith("_")} for r in b.records] == [
        {"Date": "02.01.1995", "1 USD": "27.9", "1 DEM": "18.0"},
        {"Date": "03.07.1995", "1 USD": "26.1", "1 XEU": "34.3"},
    ]
    assert [r["_src_row"] for r in b.records] == [1, 2]


# ---------- API: cửa sổ ngày (tỷ giá VCB) ----------


def _vcb_cfg(**dw):
    return make_cfg(
        api={"base_url": "https://vcb.test/api/exchangerates", "records_path": "Data",
             "raw_mode": "response",
             "date_window": {"param": "date", "end_offset_days": 1, **dw}},
        cdc={"mode": "cdc1", "watermark_column": "_window", "watermark_type": "string",
             "initial_watermark": "2026-10-01"},
    )


def _freeze_today(monkeypatch, today):
    class _DT(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(today.year, today.month, today.day, 12, tzinfo=dt.timezone.utc)

    monkeypatch.setattr("sourcing.readers.api.dt.datetime", _DT)


def test_date_window_moi_ngay_mot_request_tu_sau_moc(monkeypatch):
    _freeze_today(monkeypatch, dt.date(2026, 10, 6))
    r = ApiReader(_vcb_cfg())
    asked = []
    monkeypatch.setattr(r, "_fetch", lambda q: asked.append(q["date"]) or {"Data": [{"currencyCode": "USD"}]})
    batches = list(r.read(Checkpoint("t", {"default": "2026-10-02"})))
    # Tu 03 toi 05 (dung truoc hom nay 1 ngay: ngay dang chay chua chot)
    assert asked == ["2026-10-03", "2026-10-04", "2026-10-05"]
    assert [b.watermark for b in batches] == asked
    assert batches[0].records[0]["_request"]["query"] == {"date": "2026-10-03"}


def test_date_window_lan_dau_tu_initial_va_tran_moi_lan(monkeypatch, capsys):
    _freeze_today(monkeypatch, dt.date(2026, 10, 30))
    r = ApiReader(_vcb_cfg(max_windows=3))
    monkeypatch.setattr(r, "_fetch", lambda q: {"Data": []})
    batches = list(r.read(None))
    assert [b.watermark for b in batches] == ["2026-10-01", "2026-10-02", "2026-10-03"]
    assert "max_windows" in capsys.readouterr().out
