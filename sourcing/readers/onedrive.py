"""Reader cho file người dùng upload lên MỘT thư mục OneDrive cá nhân.

Luồng nghiệp vụ: người dùng (BA, đối tác) kéo thả file CSV/Excel vào một thư mục
OneDrive; job chạy hằng ngày kéo file mới về raw. Không ai phải đụng tới S3.

    Secrets Manager ──refresh_token──> login.microsoftonline.com ──access_token──┐
         ▲                                     │ refresh_token MỚI                 │
         └──────────── ghi lại ────────────────┘                                   ▼
    graph.microsoft.com/v1.0/me/drive  ──liệt kê thư mục──> @microsoft.graph.downloadUrl ──> fileparse

Xác thực — vì sao là refresh token chứ không phải mật khẩu:
  Tài khoản Microsoft CÁ NHÂN chỉ cho đọc OneDrive bằng quyền ủy nhiệm (delegated)
  — phải có người đăng nhập. Đăng nhập bằng mật khẩu (ROPC) không được hỗ trợ cho
  tài khoản cá nhân, còn quyền ứng dụng (client credentials) chỉ có cho tenant
  tổ chức. Nên: đăng nhập MỘT lần bằng device code (`scripts/onedrive_login.py`),
  lưu refresh token; mỗi lần chạy đổi lấy access token (1 giờ) + refresh token mới
  và ghi đè vào secret. Refresh token tài khoản cá nhân sống ~90 ngày kể từ lần
  dùng cuối, nên job chạy hằng ngày thì không bao giờ phải đăng nhập lại.

Mốc CDC1 = `<lastModifiedDateTime>|<đường dẫn>` của file cuối đã nạp. Sắp xếp
được theo thứ tự từ điển (ISO UTC cố định độ dài), và file upload đè cùng tên có
thời điểm mới nên được nạp lại. Như `s3file`: chỉ lô cuối của mỗi file mang mốc.

Đánh đổi: hai file cùng giây sửa, file có đường dẫn đứng trước mốc mà tới muộn sẽ
bị bỏ sót — `min_age_seconds` (mặc định 5 phút) thu hẹp gần hết khả năng này,
vì job không nạp file còn "nóng".
"""

from __future__ import annotations

import datetime as dt
import fnmatch
import json
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3

from ..config import SourceConfig
from ..state import Checkpoint
from .api import DEFAULT_UA
from .base import Batch, Reader
from .fileparse import is_gzip, iter_records

GRAPH = "https://graph.microsoft.com/v1.0"
LOGIN = "https://login.microsoftonline.com"
# Files.Read: doc file cua chinh nguoi dang nhap. offline_access: duoc cap refresh token.
SCOPE = "Files.Read offline_access"


def _norm(name: str) -> str:
    """Tên so khớp: Unicode NFC + không phân biệt hoa thường.

    OneDrive web lưu "Tài liệu" ở dạng TỔ HỢP (NFD: a + dấu huyền rời), còn
    người gõ tay trong YAML ra dạng DỰNG SẴN (NFC). Hai chuỗi trông y hệt nhưng
    khác byte -> so trần thì "không tìm thấy thư mục".
    """
    return unicodedata.normalize("NFC", name).casefold()


def _utc_iso(value: str) -> str:
    """'2026-10-25T03:12:45.123Z' -> '2026-10-25T03:12:45Z' (cố định độ dài để so chuỗi)."""
    ts = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class OneDriveAuthError(RuntimeError):
    """Refresh token hết hạn / bị thu hồi — cần người đăng nhập lại."""


class OneDriveReader(Reader):
    def __init__(self, cfg: SourceConfig, secrets_client=None):
        super().__init__(cfg)
        if cfg.onedrive is None:
            raise ValueError(f"[{cfg.name}] thieu khoi cau hinh 'onedrive'")
        self.o = cfg.onedrive
        self._secrets = secrets_client
        self._token: Optional[str] = None

    # ---------- xác thực ----------

    def _secrets_client(self):
        if self._secrets is None:
            self._secrets = boto3.client("secretsmanager")
        return self._secrets

    def _refresh_access_token(self) -> str:
        sm = self._secrets_client()
        secret = json.loads(sm.get_secret_value(SecretId=self.o.secret_name)["SecretString"])
        for k in ("client_id", "refresh_token"):
            if not secret.get(k):
                raise OneDriveAuthError(
                    f"[{self.cfg.name}] secret '{self.o.secret_name}' thieu '{k}'. "
                    f"Chay: python scripts/onedrive_login.py --secret {self.o.secret_name}"
                )
        tenant = secret.get("tenant", "consumers")
        form = urllib.parse.urlencode({
            "client_id": secret["client_id"],
            "grant_type": "refresh_token",
            "refresh_token": secret["refresh_token"],
            "scope": SCOPE,
        }).encode()
        req = urllib.request.Request(f"{LOGIN}/{tenant}/oauth2/v2.0/token", data=form,
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=self.o.timeout_seconds) as r:
                tok = json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            try:
                err = json.loads(body)
            except ValueError:
                err = {"error": body[:200]}
            if err.get("error") in ("invalid_grant", "interaction_required"):
                raise OneDriveAuthError(
                    f"[{self.cfg.name}] refresh token het han hoac bi thu hoi "
                    f"({err.get('error')}): dang nhap lai bang "
                    f"`python scripts/onedrive_login.py --secret {self.o.secret_name}`"
                ) from None
            raise RuntimeError(f"[{self.cfg.name}] doi token loi HTTP {e.code}: "
                               f"{err.get('error')} {err.get('error_description', '')[:200]}") from None

        new_rt = tok.get("refresh_token")
        if new_rt and new_rt != secret["refresh_token"]:
            # Ghi token MOI ngay: token cu van song toi khi het han, nen ghi loi
            # chi canh bao, khong lam hong lan chay.
            secret["refresh_token"] = new_rt
            secret["rotated_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
            try:
                sm.put_secret_value(SecretId=self.o.secret_name, SecretString=json.dumps(secret))
            except Exception as e:  # noqa: BLE001
                print(f"[{self.cfg.name}] CANH BAO: khong ghi lai duoc refresh token moi ({e}). "
                      f"Token cu con dung toi khi het han; kiem tra quyen secretsmanager:PutSecretValue.")
        return tok["access_token"]

    # ---------- HTTP ----------

    def _open(self, url: str, auth: bool = True):
        """Mở URL Graph; thử lại 429/5xx theo Retry-After, 401 thì đổi token một lần."""
        refreshed = False
        delay = 2.0
        for attempt in range(6):
            headers = {"User-Agent": DEFAULT_UA}
            if auth:
                if self._token is None:
                    self._token = self._refresh_access_token()
                headers["Authorization"] = f"Bearer {self._token}"
            req = urllib.request.Request(url, headers=headers)
            try:
                return urllib.request.urlopen(req, timeout=self.o.timeout_seconds)
            except urllib.error.HTTPError as e:
                if e.code == 401 and auth and not refreshed:
                    self._token, refreshed = None, True
                    continue
                if (e.code == 429 or e.code >= 500) and attempt < 5:
                    wait = e.headers.get("Retry-After") if e.headers else None
                    time.sleep(float(wait) if wait and wait.isdigit() else delay)
                    delay *= 2
                    continue
                if e.code == 404:
                    raise RuntimeError(f"[{self.cfg.name}] khong tim thay tren OneDrive: "
                                       f"{urllib.parse.unquote(url)[:200]}") from None
                raise RuntimeError(f"[{self.cfg.name}] Graph HTTP {e.code} tu {url[:160]}") from None
            except urllib.error.URLError:
                if attempt == 5:
                    raise
                time.sleep(delay)
                delay *= 2
        raise RuntimeError(f"[{self.cfg.name}] het luot thu lai: {url[:160]}")

    def _get_json(self, url: str) -> Dict[str, Any]:
        with self._open(url) as r:
            return json.loads(r.read().decode("utf-8"))

    def _children(self, item_id: str) -> Iterator[Dict[str, Any]]:
        url: Optional[str] = f"{GRAPH}/me/drive/items/{item_id}/children?$top=200"
        while url:
            page = self._get_json(url)
            yield from page.get("value", [])
            url = page.get("@odata.nextLink")

    # ---------- liệt kê ----------

    def _resolve_folder(self) -> Tuple[str, str]:
        """-> (id, đường dẫn thật). folder_id: hỏi thẳng Graph; folder_path: đi từng đoạn
        từ gốc, so tên theo `_norm`."""
        if self.o.folder_id:
            fid = urllib.parse.quote(self.o.folder_id.strip(), safe="!")
            node = self._get_json(f"{GRAPH}/me/drive/items/{fid}?$select=id,name,folder,parentReference")
            if "folder" not in node:
                raise RuntimeError(f"[{self.cfg.name}] folder_id {self.o.folder_id!r} khong phai thu muc "
                                   f"(la file {node.get('name')!r}).")
            parent = (node.get("parentReference") or {}).get("path", "")
            # "/drive/root:/Tai lieu" -> "Tai lieu/<ten>"
            base = parent.split("root:", 1)[-1].strip("/") if "root:" in parent else ""
            return node["id"], "/".join(p for p in (base, node["name"]) if p)
        node = self._get_json(f"{GRAPH}/me/drive/root?$select=id,name")
        item_id: str = node["id"]
        real: List[str] = []
        for part in [p for p in self.o.folder_path.replace("\\", "/").split("/") if p]:
            match = next((c for c in self._children(item_id)
                          if "folder" in c and _norm(c["name"]) == _norm(part)), None)
            if match is None:
                raise RuntimeError(
                    f"[{self.cfg.name}] khong co thu muc '{part}' trong "
                    f"'/{'/'.join(real)}' tren OneDrive (folder_path = {self.o.folder_path!r})."
                )
            item_id = match["id"]
            real.append(match["name"])
        return item_id, "/".join(real)

    def list_files(self) -> List[Dict[str, Any]]:
        """File khớp mẫu, đã đủ "nguội", sắp theo khoá mốc."""
        root_id, root_path = self._resolve_folder()
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=self.o.min_age_seconds)
        pattern = self.o.name_pattern.casefold()
        out: List[Dict[str, Any]] = []
        stack = [(root_id, root_path)]
        while stack:
            fid, fpath = stack.pop()
            for c in self._children(fid):
                path = f"{fpath}/{c['name']}"
                if "folder" in c:
                    if self.o.recursive:
                        stack.append((c["id"], path))
                    continue
                if "file" not in c or c["name"].startswith(("~$", ".", "_")):
                    continue  # ~$: file khoa tam cua Office dang mo
                if not fnmatch.fnmatch(unicodedata.normalize("NFC", c["name"]).casefold(), pattern):
                    continue
                modified = _utc_iso(c["lastModifiedDateTime"])
                if dt.datetime.fromisoformat(modified.replace("Z", "+00:00")) > cutoff:
                    print(f"[{self.cfg.name}] bo qua {path}: vua sua luc {modified} "
                          f"(< {self.o.min_age_seconds}s), lan sau moi nap.")
                    continue
                out.append({
                    "key": f"{modified}|{path}",
                    "path": path,
                    "id": c["id"],
                    "modified": modified,
                    "size": c.get("size", 0),
                    "download_url": c.get("@microsoft.graph.downloadUrl"),
                })
        return sorted(out, key=lambda f: f["key"])

    # ---------- đọc ----------

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        since = None if self.is_full_snapshot else self.effective_since(checkpoint)
        files = [f for f in self.list_files() if since is None or f["key"] > since]
        if not files:
            return
        if len(files) > self.o.max_files_per_run:
            print(f"[{self.cfg.name}] {len(files)} file moi, chi nap {self.o.max_files_per_run} "
                  f"lan nay; lan sau tiep tuc tu checkpoint.")
            files = files[: self.o.max_files_per_run]

        for f in files:
            # downloadUrl da ky san (song ~1 gio), KHONG gui kem token
            if f["download_url"]:
                resp = self._open(f["download_url"], auth=False)
            else:
                resp = self._open(f"{GRAPH}/me/drive/items/{f['id']}/content")
            buf: List[Dict[str, Any]] = []
            total = 0
            with resp:
                for row_no, rec in iter_records(
                    resp, self.o.file_format, name=f["path"],
                    gzip_on=is_gzip(f["path"], self.o.compression),
                    encoding=self.o.encoding, csv_delimiter=self.o.csv_delimiter,
                    repeated_header=self.o.csv_repeated_header,
                    sheet=self.o.sheet, header_row=self.o.header_row,
                ):
                    rec["_src_file"] = f["path"]
                    rec["_src_row"] = row_no
                    rec["_src_item_id"] = f["id"]
                    rec["_src_modified"] = f["modified"]
                    buf.append(self.stamp(rec))
                    if len(buf) >= self.o.batch_size:
                        total += len(buf)
                        yield Batch(records=buf, watermark=None, meta={"file": f["path"]})
                        buf = []
            total += len(buf)
            yield Batch(records=buf, watermark=None if self.is_full_snapshot else f["key"],
                        meta={"file": f["path"], "records": total})
