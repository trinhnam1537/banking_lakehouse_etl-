"""Reader OneDrive cá nhân: đổi token + ghi lại secret, tìm thư mục tên tiếng Việt,
lọc file theo mẫu/độ "nguội", mốc CDC1, CSV và XLSX. Graph + login giả lập qua urlopen."""

from __future__ import annotations

import datetime as dt
import io
import json
import unicodedata
import urllib.error
import urllib.parse

import boto3
import pytest

from sourcing.config import ConfigError
from sourcing.readers.onedrive import GRAPH, OneDriveAuthError, OneDriveReader
from sourcing.state import Checkpoint

from .conftest import make_cfg

SECRET = "fingraph/src/oauth/onedrive"
TOKEN_URL = "https://login.microsoftonline.com/consumers/oauth2/v2.0/token"
OLD = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.123Z")
OLDER = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
HOT = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# OneDrive web luu ten o dang TO HOP (NFD) — dung y nhu link nguoi dung gui
TAI_LIEU_NFD = unicodedata.normalize("NFD", "Tài liệu")


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


@pytest.fixture
def graph(monkeypatch):
    """pages: url -> body. token_replies: list cac phan hoi lan luot cua endpoint token."""
    state = {"pages": {}, "token_replies": [], "calls": [], "auth": []}

    def _open(req, timeout=None):
        url = req.full_url
        state["calls"].append(url)
        if url == TOKEN_URL:
            form = urllib.parse.parse_qs(req.data.decode())
            state["auth"].append(form)
            reply = state["token_replies"].pop(0)
            if isinstance(reply, urllib.error.HTTPError):
                raise reply
            return _Resp(json.dumps(reply).encode())
        if url.startswith(GRAPH):
            assert req.get_header("Authorization", "").startswith("Bearer ")
        else:
            # downloadUrl da ky san: khong duoc gui kem token
            assert req.get_header("Authorization") is None
        body = state["pages"][url]
        return _Resp(body if isinstance(body, bytes) else json.dumps(body).encode())

    monkeypatch.setattr("urllib.request.urlopen", _open)
    return state


@pytest.fixture
def secret(aws):
    sm = boto3.client("secretsmanager")
    sm.create_secret(Name=SECRET, SecretString=json.dumps(
        {"client_id": "app-1", "refresh_token": "rt-1", "tenant": "consumers"}))
    return sm


def _cfg(mode="cdc1", **over):
    od = {"secret_name": SECRET, "folder_path": "Tài liệu/inbox", "name_pattern": "*.csv",
          "file_format": "csv", **over}
    cdc = ({"mode": "cdc1", "watermark_column": "_file_key", "watermark_type": "string"}
           if mode == "cdc1" else {"mode": "cdc5"})
    return make_cfg(source_type="onedrive", onedrive=od, cdc=cdc)


def _file(name, modified, url):
    return {"id": f"id-{name}", "name": name, "size": 10, "file": {},
            "lastModifiedDateTime": modified, "@microsoft.graph.downloadUrl": url}


def _seed(state, files, ok_tokens=1):
    p = state["pages"]
    p[f"{GRAPH}/me/drive/root?$select=id,name"] = {"id": "root", "name": "root"}
    p[f"{GRAPH}/me/drive/items/root/children?$top=200"] = {
        "value": [{"id": "f-docs", "name": TAI_LIEU_NFD, "folder": {}},
                  {"id": "f-other", "name": "Ảnh", "folder": {}}]}
    p[f"{GRAPH}/me/drive/items/f-docs/children?$top=200"] = {
        "value": [{"id": "f-inbox", "name": "Inbox", "folder": {}}]}
    p[f"{GRAPH}/me/drive/items/f-inbox/children?$top=200"] = {"value": files}
    state["token_replies"] += [{"access_token": f"at-{i}", "refresh_token": f"rt-{i + 2}"}
                               for i in range(ok_tokens)]


# ---------- config ----------

def test_config_chan_secret_ngoai_tien_to():
    with pytest.raises(ConfigError, match="nam ngoai"):
        _cfg(secret_name="khac/onedrive")


def test_config_chan_folder_rong():
    with pytest.raises(ConfigError, match="folder_path hoac folder_id"):
        _cfg(folder_path="/")


def test_config_chan_khai_ca_folder_path_va_folder_id():
    with pytest.raises(ConfigError, match="folder_path hoac folder_id"):
        _cfg(folder_id="ABC!s1")


def test_config_cdc1_can_cot_moc():
    with pytest.raises(ConfigError, match="watermark_column"):
        make_cfg(source_type="onedrive", cdc={"mode": "cdc1"},
                 onedrive={"secret_name": SECRET, "folder_path": "a"})


# ---------- xác thực ----------

def test_doi_token_va_ghi_lai_refresh_token_moi(graph, secret):
    _seed(graph, [_file("a.csv", OLD, "https://dl.test/a")])
    graph["pages"]["https://dl.test/a"] = b"x,y\n1,2\n"

    batches = list(OneDriveReader(_cfg(), secrets_client=secret).read(None))

    assert graph["auth"][0]["refresh_token"] == ["rt-1"]
    assert graph["auth"][0]["client_id"] == ["app-1"]
    assert "offline_access" in graph["auth"][0]["scope"][0]
    stored = json.loads(secret.get_secret_value(SecretId=SECRET)["SecretString"])
    assert stored["refresh_token"] == "rt-2" and stored["client_id"] == "app-1"
    assert [r["x"] for b in batches for r in b.records] == ["1"]


def test_refresh_token_het_han_bao_ro_cach_sua(graph, secret):
    err = urllib.error.HTTPError(TOKEN_URL, 400, "Bad Request", {},
                                 io.BytesIO(json.dumps({"error": "invalid_grant"}).encode()))
    graph["token_replies"].append(err)
    with pytest.raises(OneDriveAuthError, match="onedrive_login.py"):
        list(OneDriveReader(_cfg(), secrets_client=secret).read(None))


# ---------- liệt kê ----------

def test_tim_thu_muc_ten_to_hop_nfd_va_loc_file(graph, secret):
    _seed(graph, [
        _file("b.csv", OLD, "https://dl.test/b"),
        _file("A.CSV", OLDER, "https://dl.test/A"),     # mau khong phan biet hoa thuong
        _file("dang_sua.csv", HOT, "https://dl.test/h"),  # chua du nguoi
        _file("~$khoa.csv", OLD, "https://dl.test/k"),  # file khoa tam cua Office
        _file("ghi_chu.txt", OLD, "https://dl.test/t"),
        {"id": "sub", "name": "con", "folder": {}},     # recursive: false
    ])
    files = OneDriveReader(_cfg(), secrets_client=secret).list_files()

    assert [f["path"] for f in files] == [f"{TAI_LIEU_NFD}/Inbox/A.CSV", f"{TAI_LIEU_NFD}/Inbox/b.csv"]
    assert files[1]["key"].endswith("Z|" + files[1]["path"])
    assert ".123" not in files[1]["key"]  # bo phan le giay: khoa co dinh do dai


def test_thu_muc_khong_co_bao_loi_ro(graph, secret):
    _seed(graph, [])
    with pytest.raises(RuntimeError, match="khong co thu muc 'nope'"):
        OneDriveReader(_cfg(folder_path="Tài liệu/nope"), secrets_client=secret).list_files()


# ---------- đọc + mốc ----------

def test_cdc1_chi_nap_file_sau_moc_va_lo_cuoi_mang_moc(graph, secret):
    _seed(graph, [_file("a.csv", OLDER, "https://dl.test/a"), _file("b.csv", OLD, "https://dl.test/b")])
    graph["pages"]["https://dl.test/a"] = b"v\n1\n"
    graph["pages"]["https://dl.test/b"] = b"v\n2\n3\n"
    reader = OneDriveReader(_cfg(), secrets_client=secret)
    key_a = reader.list_files()[0]["key"]

    ck = Checkpoint(job_name="t_job", watermarks={"default": key_a})
    batches = list(reader.read(ck))

    assert [r["v"] for b in batches for r in b.records] == ["2", "3"]
    assert batches[-1].watermark.endswith("/Inbox/b.csv")
    rec = batches[-1].records[0]
    assert rec["_src_item_id"] == "id-b.csv" and rec["_src_row"] == 1
    assert rec["_src_type"] == "onedrive"
    assert "https://dl.test/a" not in graph["calls"]


def test_cdc5_nap_lai_tat_ca_khong_mang_moc(graph, secret):
    _seed(graph, [_file("a.csv", OLDER, "https://dl.test/a")])
    graph["pages"]["https://dl.test/a"] = b"v\n1\n"
    ck = Checkpoint(job_name="t_job", watermarks={"default": "9999"})
    batches = list(OneDriveReader(_cfg(mode="cdc5"), secrets_client=secret).read(ck))
    assert [r["v"] for b in batches for r in b.records] == ["1"]
    assert all(b.watermark is None for b in batches)


def test_xlsx_sheet_va_dong_header(graph, secret):
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    wb.active.title = "bia"
    ws = wb.create_sheet("data")
    ws.append(["Bao cao thang 10"])                # dong tieu de, khong phai header
    ws.append(["ma", "ngay", "so_tien", None])
    ws.append(["A1", dt.date(2026, 10, 1), 1500.5, None])
    ws.append([None, None, None, None])           # dong trong -> bo
    ws.append(["A2", dt.date(2026, 10, 2), 7, "thua"])
    buf = io.BytesIO()
    wb.save(buf)

    _seed(graph, [_file("bc.xlsx", OLD, "https://dl.test/x")])
    graph["pages"]["https://dl.test/x"] = buf.getvalue()
    cfg = _cfg(name_pattern="*.xlsx", file_format="xlsx", sheet="data", header_row=2)
    recs = [r for b in OneDriveReader(cfg, secrets_client=secret).read(None) for r in b.records]

    assert [(r["ma"], r["ngay"], r["so_tien"]) for r in recs] == [
        ("A1", "2026-10-01T00:00:00", 1500.5), ("A2", "2026-10-02T00:00:00", 7)]
    assert recs[1]["_col4"] == "thua" and recs[0]["_col4"] is None
    assert [r["_src_row"] for r in recs] == [1, 2]


def test_folder_id_hoi_thang_graph_khong_di_tu_goc(graph, secret):
    fid = "AFEECB8F63339F68!s891c"
    graph["pages"][f"{GRAPH}/me/drive/items/{fid}?$select=id,name,folder,parentReference"] = {
        "id": fid, "name": "inbox", "folder": {}, "parentReference": {"path": "/drive/root:/Tài liệu"}}
    graph["pages"][f"{GRAPH}/me/drive/items/{fid}/children?$top=200"] = {
        "value": [_file("a.csv", OLD, "https://dl.test/a")]}
    graph["token_replies"].append({"access_token": "at", "refresh_token": "rt-2"})
    files = OneDriveReader(_cfg(folder_path="", folder_id=fid), secrets_client=secret).list_files()
    assert [f["path"] for f in files] == ["Tài liệu/inbox/a.csv"]
    assert not any(u.endswith("/root?$select=id,name") for u in graph["calls"])
