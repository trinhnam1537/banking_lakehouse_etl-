"""Reader REST API (sourcing/readers/api.py).

Không gọi mạng thật: hoặc thay `_fetch` bằng hàm giả trả từng trang, hoặc thay
`urllib.request.urlopen` khi cần test chính phần HTTP (retry, mã lỗi).
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.parse

import boto3
import pytest

from sourcing.readers import build_reader
from sourcing.readers.api import DEFAULT_UA, ApiReader
from sourcing.state import Checkpoint

from .conftest import make_cfg


class FakeApi:
    """Thay `_fetch`: ghi lại query của từng lần gọi, trả trang theo thứ tự."""

    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def __call__(self, query):
        self.calls.append(dict(query))
        return self.pages.pop(0) if self.pages else []


def _reader(monkeypatch, pages, **over):
    r = ApiReader(make_cfg(**over))
    fake = FakeApi(pages)
    monkeypatch.setattr(r, "_fetch", fake)
    return r, fake


def _all(reader, checkpoint=None):
    return list(reader.read(checkpoint))


# ---------- bóc bản ghi ----------


def test_response_la_mang(monkeypatch):
    r, _ = _reader(monkeypatch, [[{"a": 1}, {"a": 2}, "khong_phai_dict"]])
    (batch,) = _all(r)
    assert [x["a"] for x in batch.records] == [1, 2]
    assert batch.watermark is None and batch.partition == "default"


def test_records_path_long_nhau(monkeypatch):
    r, _ = _reader(monkeypatch, [{"data": {"items": [{"a": 1}]}}],
                   api={"records_path": "data.items"})
    (batch,) = _all(r)
    assert batch.records[0]["a"] == 1


def test_response_la_object_don(monkeypatch):
    r, _ = _reader(monkeypatch, [{"total": 5}])
    (batch,) = _all(r)
    assert batch.records[0]["total"] == 5


def test_records_path_khong_ton_tai_tra_rong(monkeypatch):
    """API trả lỗi dạng {"error": ...} không được làm job chết vì KeyError."""
    r, _ = _reader(monkeypatch, [{"error": "rate limited"}], api={"records_path": "data.items"})
    assert _all(r) == []


def test_stamp_gan_metadata_truy_vet(monkeypatch):
    r, _ = _reader(monkeypatch, [[{"a": 1}]])
    rec = _all(r)[0].records[0]
    assert rec["_src_job"] == "t_job" and rec["_src_type"] == "api" and rec["_src_cdc"] == "cdc5"
    assert rec["_src_ingest_ts"].endswith("+00:00")


# ---------- phân trang ----------


def test_pagination_page_dung_khi_trang_chua_day(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"i": 1}, {"i": 2}], [{"i": 3}]],
                      api={"pagination": "page", "page_size": 2, "page_param": "p",
                           "page_size_param": "n"})
    batches = _all(r)
    assert [len(b) for b in batches] == [2, 1]
    assert fake.calls == [{"p": 1, "n": 2}, {"p": 2, "n": 2}]
    assert [b.meta["page"] for b in batches] == [1, 2]


def test_pagination_page_dung_khi_trang_rong(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"i": 1}], []], api={"pagination": "page", "page_size": 1})
    assert len(_all(r)) == 1
    assert len(fake.calls) == 2


def test_pagination_offset(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"i": 1}] * 3, [{"i": 2}] * 3, [{"i": 3}]],
                      api={"pagination": "offset", "page_size": 3, "page_param": "offset"})
    _all(r)
    assert [c["offset"] for c in fake.calls] == [0, 3, 6]


def test_pagination_cursor(monkeypatch):
    pages = [
        {"data": [{"i": 1}], "meta": {"next": "c2"}},
        {"data": [{"i": 2}], "meta": {"next": None}},
    ]
    r, fake = _reader(monkeypatch, pages, api={
        "pagination": "cursor", "records_path": "data", "cursor_path": "meta.next",
        "cursor_param": "after"})
    assert len(_all(r)) == 2
    assert fake.calls == [{}, {"after": "c2"}]


def test_pagination_none_chi_goi_mot_lan(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"i": 1}], [{"i": 2}]])
    _all(r)
    assert len(fake.calls) == 1


def test_cham_tran_max_pages_canh_bao(monkeypatch, capsys):
    r, fake = _reader(monkeypatch, [[{"i": 1}]] * 10,
                      api={"pagination": "page", "page_size": 1, "max_pages": 3})
    assert len(_all(r)) == 3
    assert len(fake.calls) == 3
    assert "cham tran max_pages=3" in capsys.readouterr().out


# ---------- CDC1: mốc ----------


CDC1 = {"mode": "cdc1", "watermark_column": "updated_at"}


def test_cdc1_watermark_la_max_cua_lo(monkeypatch):
    r, _ = _reader(monkeypatch, [[{"updated_at": "2026-09-02"}, {"updated_at": "2026-09-05"},
                                  {"updated_at": None}, {"updated_at": "2026-09-01"}]], cdc=CDC1)
    assert _all(r)[0].watermark == "2026-09-05"


def test_cdc1_watermark_tang_dan_qua_cac_trang(monkeypatch):
    r, _ = _reader(monkeypatch, [[{"updated_at": "2026-09-05"}], [{"updated_at": "2026-09-03"}]],
                   cdc=CDC1, api={"pagination": "page", "page_size": 1})
    assert [b.watermark for b in _all(r)] == ["2026-09-05", "2026-09-05"]


def test_cdc1_watermark_so_nguyen_so_sanh_theo_so(monkeypatch):
    """watermark_type=int: "10" phải lớn hơn "9". So chuỗi thì mốc lùi lại."""
    r, _ = _reader(monkeypatch, [[{"id": 9}, {"id": 10}]],
                   cdc={"mode": "cdc1", "watermark_column": "id", "watermark_type": "int"})
    assert _all(r)[0].watermark == "10"


def test_cdc1_chen_since_vao_placeholder(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"updated_at": "x"}]], cdc=CDC1,
                      api={"query": {"from": "{since}", "symbol": "BTC"}})
    _all(r, Checkpoint("t_job", {"default": "2026-09-01T00:00:00+00:00"}))
    # query tinh (symbol) do _fetch gop vao; read() chi truyen phan thay doi
    assert fake.calls[0] == {"from": "2026-09-01T00:00:00+00:00"}


def test_cdc1_since_ap_dung_lookback(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"updated_at": "x"}]],
                      cdc={**CDC1, "lookback_seconds": 600},
                      api={"query": {"from": "{since}"}})
    _all(r, Checkpoint("t_job", {"default": "2026-09-01T00:10:00Z"}))
    assert fake.calls[0]["from"] == "2026-09-01T00:00:00+00:00"


def test_cdc1_lan_dau_dung_initial_watermark(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"updated_at": "x"}]],
                      cdc={**CDC1, "initial_watermark": "2020-01-01T00:00:00+00:00"},
                      api={"query": {"from": "{since}"}})
    _all(r, Checkpoint("t_job"))
    assert fake.calls[0]["from"] == "2020-01-01T00:00:00+00:00"


def test_since_placeholder_khong_lot_ra_url(monkeypatch):
    """Chưa có mốc nào: KHÔNG được gửi chuỗi "{since}" nguyên văn lên API."""
    sent = []
    monkeypatch.setattr("urllib.request.urlopen", _fake_urlopen(sent, [[]]))
    r = ApiReader(make_cfg(cdc=CDC1, api={"query": {"from": "{since}", "symbol": "BTC"}}))
    _all(r, Checkpoint("t_job"))
    assert "since" not in urllib.parse.unquote(sent[0])
    assert "symbol=BTC" in sent[0]


def test_cdc5_bo_qua_checkpoint(monkeypatch):
    r, fake = _reader(monkeypatch, [[{"a": 1}]], api={"query": {"q": "x"}})
    batch = _all(r, Checkpoint("t_job", {"default": "2026"}))[0]
    assert batch.watermark is None
    assert fake.calls[0] == {}  # query tinh do _fetch tu gop, khong phai read()


# ---------- raw_mode=response: raw giu nguyen van response ----------


def _resp_reader(monkeypatch, pages, **api):
    return _reader(monkeypatch, pages, api={"raw_mode": "response", **api})


def test_response_giu_envelope_nguyen_van(monkeypatch):
    """SEC frames: tag/uom nam o envelope, bo di thi silver khong biet chi tieu nao."""
    payload = {"tag": "Revenues", "uom": "USD", "data": [{"cik": 1, "val": 5}, {"cik": 2, "val": 7}]}
    r, _ = _resp_reader(monkeypatch, [payload], records_path="data", query={"x": "1"})
    (batch,) = _all(r)
    assert len(batch.records) == 1                     # 1 response = 1 dong raw
    row = batch.records[0]
    assert row["_response"] == payload                 # khong mat gi
    assert row["_records_path"] == "data"
    assert row["_request"]["query"] == {"x": "1"} and row["_request"]["page"] == 1
    assert row["_src_job"] == "t_job"
    assert batch.meta["records"] == 2                  # van dem ban ghi nghiep vu


def test_response_moi_trang_mot_dong_va_van_dung_phan_trang(monkeypatch):
    r, fake = _resp_reader(monkeypatch, [{"data": [{"i": 1}, {"i": 2}]}, {"data": [{"i": 3}]}],
                           records_path="data", pagination="page", page_size=2,
                           page_param="p", page_size_param="n")
    batches = _all(r)
    assert [b.records[0]["_request"]["query"]["p"] for b in batches] == [1, 2]
    assert len(fake.calls) == 2                        # trang 2 chua day -> dung


def test_response_watermark_van_lay_tu_ban_ghi(monkeypatch):
    r, _ = _reader(monkeypatch, [{"data": [{"ts": "2026-01-02"}, {"ts": "2026-01-05"}]}],
                   api={"raw_mode": "response", "records_path": "data"},
                   cdc={"mode": "cdc1", "watermark_column": "ts",
                        "initial_watermark": "2026-01-01"})
    (batch,) = _all(r, Checkpoint("t_job", {}))
    assert batch.watermark == "2026-01-05"


def test_response_trang_dau_loi_van_ghi_bang_chung(monkeypatch):
    r, _ = _resp_reader(monkeypatch, [{"error": "Invalid API call"}], records_path="data")
    (batch,) = _all(r)
    assert batch.records[0]["_response"] == {"error": "Invalid API call"}
    assert batch.meta["records"] == 0


def test_response_trang_cuoi_rong_khong_ghi(monkeypatch):
    r, _ = _resp_reader(monkeypatch, [{"data": [{"i": 1}, {"i": 2}]}, {"data": []}],
                        records_path="data", pagination="page", page_size=2)
    assert len(_all(r)) == 1


def test_response_khong_ghi_secret_xuong_raw(secret, monkeypatch):
    cfg = make_cfg(api={"raw_mode": "response", "query": {"symbol": "IBM"},
                        "secret_name": "fingraph/test", "secret_inject": {"apikey": "query.apikey"}})
    r = ApiReader(cfg)
    assert r._query_extra == {"apikey": "K123"}        # khoa da nap that
    monkeypatch.setattr(r, "_fetch", FakeApi([[{"a": 1}]]))
    row = _all(r)[0].records[0]
    assert row["_request"]["query"] == {"symbol": "IBM"}
    assert "K123" not in json.dumps(row)


def test_raw_mode_sai_bi_chan():
    with pytest.raises(ValueError, match="raw_mode"):
        ApiReader(make_cfg(api={"raw_mode": "parquet"}))


# ---------- HTTP ----------


class _Resp:
    def __init__(self, payload):
        self._b = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_urlopen(sent, responses):
    """responses: payload (dict/list) hoặc exception, trả lần lượt."""
    responses = list(responses)

    def _open(req, timeout=None):
        sent.append(req.full_url)
        _open.headers = dict(req.header_items())
        nxt = responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return _Resp(nxt)

    return _open


def _http_error(code, body=b"loi"):
    return urllib.error.HTTPError("https://api.test", code, "x", {}, io.BytesIO(body))


@pytest.fixture
def no_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))
    return slept


def test_fetch_dung_url_query_va_header(monkeypatch, no_sleep):
    sent = []
    opener = _fake_urlopen(sent, [[{"a": 1}]])
    monkeypatch.setattr("urllib.request.urlopen", opener)
    r = ApiReader(make_cfg(api={"base_url": "https://api.test/v3/", "path": "/ticker",
                                "query": {"symbol": "BTC", "limit": 5},
                                "headers": {"X-Custom": "1"}}))
    assert r._fetch({"page": 2}) == [{"a": 1}]
    assert sent == ["https://api.test/v3/ticker?symbol=BTC&limit=5&page=2"]
    assert opener.headers["User-agent"] == DEFAULT_UA
    assert opener.headers["X-custom"] == "1"


def test_fetch_retry_429_va_5xx_roi_thanh_cong(monkeypatch, no_sleep):
    sent = []
    monkeypatch.setattr("urllib.request.urlopen",
                        _fake_urlopen(sent, [_http_error(429), _http_error(503), {"ok": 1}]))
    assert ApiReader(make_cfg())._fetch({}) == {"ok": 1}
    assert len(sent) == 3
    assert no_sleep == [1.0, 2.0]  # lui theo cap so nhan


def test_fetch_het_luot_retry_nem_loi(monkeypatch, no_sleep):
    monkeypatch.setattr("urllib.request.urlopen",
                        _fake_urlopen([], [_http_error(500)] * 5))
    with pytest.raises(urllib.error.HTTPError):
        ApiReader(make_cfg())._fetch({})
    assert len(no_sleep) == 4


def test_fetch_4xx_that_bai_ngay_kem_ten_job(monkeypatch, no_sleep):
    sent = []
    monkeypatch.setattr("urllib.request.urlopen",
                        _fake_urlopen(sent, [_http_error(403, b"Forbidden: UA")]))
    with pytest.raises(RuntimeError, match=r"\[t_job\] HTTP 403.*Forbidden: UA"):
        ApiReader(make_cfg())._fetch({})
    assert len(sent) == 1 and no_sleep == []


def test_fetch_loi_mang_duoc_retry(monkeypatch, no_sleep):
    monkeypatch.setattr("urllib.request.urlopen",
                        _fake_urlopen([], [urllib.error.URLError("dns"), [1]]))
    assert ApiReader(make_cfg())._fetch({}) == [1]


def test_throttle_gian_cach(monkeypatch, no_sleep):
    r = ApiReader(make_cfg(api={"rate_limit_per_sec": 2}))
    r._throttle()
    r._throttle()
    assert no_sleep and 0 < no_sleep[-1] <= 0.5


# ---------- bí mật ----------


@pytest.fixture
def secret(aws):
    sm = boto3.client("secretsmanager")
    sm.create_secret(Name="fingraph/test", SecretString=json.dumps({"apikey": "K123", "tok": "T9"}))
    return sm


def test_secret_chen_vao_query_va_header(secret, monkeypatch, no_sleep):
    sent = []
    opener = _fake_urlopen(sent, [[]])
    monkeypatch.setattr("urllib.request.urlopen", opener)
    r = ApiReader(make_cfg(api={"secret_name": "fingraph/test",
                                "secret_inject": {"apikey": "query.apikey",
                                                  "tok": "header.Authorization"}}))
    r._fetch({})
    assert "apikey=K123" in sent[0]
    assert opener.headers["Authorization"] == "T9"


def test_secret_thieu_khoa(secret):
    with pytest.raises(KeyError, match="khong co khoa 'nope'"):
        ApiReader(make_cfg(api={"secret_name": "fingraph/test",
                                "secret_inject": {"nope": "query.x"}}))


def test_secret_inject_sai_dinh_dang(secret):
    with pytest.raises(ValueError, match="header.<Ten>"):
        ApiReader(make_cfg(api={"secret_name": "fingraph/test",
                                "secret_inject": {"apikey": "body.x"}}))


def test_khong_khai_secret_thi_khong_goi_secrets_manager(monkeypatch):
    monkeypatch.setattr(boto3, "client", lambda *a, **k: pytest.fail("khong duoc goi AWS"))
    ApiReader(make_cfg())


def test_build_reader_chon_dung_lop():
    assert isinstance(build_reader(make_cfg()), ApiReader)
