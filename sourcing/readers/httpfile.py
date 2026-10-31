"""Reader cho file tải qua HTTP(S): bulk export, file công bố định kỳ.

Khác reader `api`: một "trang" ở đây là cả một FILE (CSV/NDJSON hàng trăm MB),
không phải một JSON response. File được tải và parse theo luồng
(`fileparse.iter_records`), không bao giờ nằm trọn trong RAM hay trên đĩa.

Hai cách lấy danh sách file (xem `HttpFileSource`):

  urls       {khoá: url} tĩnh.                         CDC5: tải lại tất cả mỗi lần.
  index_url  tài liệu JSON liệt kê file, có thể qua    CDC1: chỉ tải khoá > mốc,
             nhiều bước (index_follow).                 theo thứ tự từ điển.

Ví dụ OpenSanctions (delta mỗi ~6 giờ, giữ 100 phiên bản gần nhất):

    index.json --delta_url--> delta.json {"versions": {"2026...-euu": ".../entities.delta.json"}}

Mốc = khoá của file cuối đã nạp xong. Như `s3file`: chỉ lô cuối của mỗi file
mang mốc, file đọc dở không bao giờ được tính là đã xử lý.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ..config import SourceConfig
from ..state import Checkpoint
from .api import DEFAULT_UA
from .base import Batch, Reader
from .fileparse import ARCHIVE_KINDS, hive_parts, is_gzip, iter_archive, iter_records


class HttpFileReader(Reader):
    def __init__(self, cfg: SourceConfig):
        super().__init__(cfg)
        if cfg.httpfile is None:
            raise ValueError(f"[{cfg.name}] thieu khoi cau hinh 'httpfile'")
        self.h = cfg.httpfile
        self._headers = dict(self.h.headers)
        self._headers.setdefault("User-Agent", DEFAULT_UA)
        self.index_meta: Dict[str, Any] = {}

    # ---------- HTTP ----------

    def _open(self, url: str):
        """Mở kết nối, thử lại lỗi mạng/429/5xx. Chỉ thử lại lúc MỞ: đứt giữa
        lúc đang đọc thì cả lần chạy hỏng, mốc giữ ở file trước (at-least-once)."""
        req = urllib.request.Request(url, headers=self._headers)
        delay = 2.0
        for attempt in range(5):
            try:
                return urllib.request.urlopen(req, timeout=self.h.timeout_seconds)
            except urllib.error.HTTPError as e:
                if (e.code == 429 or e.code >= 500) and attempt < 4:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise RuntimeError(f"[{self.cfg.name}] HTTP {e.code} tu {url[:160]}") from None
            except urllib.error.URLError:
                if attempt == 4:
                    raise
                time.sleep(delay)
                delay *= 2
        raise RuntimeError(f"[{self.cfg.name}] het luot thu lai: {url[:160]}")

    def _get_json(self, url: str) -> Any:
        with self._open(url) as r:
            return json.loads(r.read().decode("utf-8"))

    # ---------- danh sách file ----------

    def list_files(self) -> List[Tuple[str, str]]:
        """[(khoá, url)] sắp theo khoá."""
        if self.h.urls:
            return sorted(self.h.urls.items())

        doc = self._get_json(self.h.index_url)
        for fld in self.h.index_follow:
            nxt = doc.get(fld) if isinstance(doc, dict) else None
            if not nxt:
                raise RuntimeError(f"[{self.cfg.name}] index khong co truong '{fld}'")
            # Giu lai phien ban/ngay cap nhat cua index cho manifest
            self.index_meta = {k: doc[k] for k in ("version", "updated_at", "last_change")
                               if isinstance(doc, dict) and k in doc}
            doc = self._get_json(nxt)

        node: Any = doc
        for part in filter(None, self.h.files_path.split(".")):
            node = node.get(part) if isinstance(node, dict) else None
        if isinstance(node, dict):
            items = [(str(k), str(v)) for k, v in node.items()]
        elif isinstance(node, list):
            # Danh sach URL tran: khoa = ten file
            items = [(str(u).rstrip("/").rsplit("/", 1)[-1], str(u)) for u in node]
        else:
            raise RuntimeError(f"[{self.cfg.name}] files_path '{self.h.files_path}' khong tro "
                               f"toi dict hoac list trong index")
        return sorted(items)

    # ---------- đọc ----------

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        files = self.list_files()
        since = None if self.is_full_snapshot else self.effective_since(checkpoint)

        if since is not None and files and self.h.require_contiguous:
            keys = [k for k, _ in files]
            if since < keys[0] and since not in keys:
                raise RuntimeError(
                    f"[{self.cfg.name}] MAT LIEN TUC: moc {since} cu hon phien ban cu nhat "
                    f"nguon con giu ({keys[0]}). Cac delta o giua da bi xoa. Nap lai ban day "
                    f"du (entry CDC5 tuong ung) roi reset_checkpoint('{self.cfg.name}')."
                )

        todo = [(k, u) for k, u in files if since is None or k > since]
        if len(todo) > self.h.max_files_per_run:
            print(f"[{self.cfg.name}] {len(todo)} file moi, chi nap {self.h.max_files_per_run} "
                  f"lan nay; lan sau tiep tuc tu checkpoint.")
            todo = todo[: self.h.max_files_per_run]

        for key, url in todo:
            buf: List[Dict[str, Any]] = []
            total = 0
            with self._open(url) as resp:
                for src_file, member, stream in self._members(key, url, resp):
                    if member:
                        gz = is_gzip(member, "auto")
                    else:
                        gz = is_gzip(url.split("?", 1)[0], self.h.compression)
                    parts = hive_parts(member) if member else {}
                    for row_no, rec in iter_records(
                        stream, self.h.file_format, name=src_file, gzip_on=gz,
                        encoding=self.h.encoding, csv_delimiter=self.h.csv_delimiter,
                        repeated_header=self.h.csv_repeated_header,
                    ):
                        for k, v in parts.items():
                            rec.setdefault(k, v)  # cot partition Hive (MBD: fold)
                        rec["_src_file"] = src_file
                        rec["_src_url"] = url
                        rec["_src_row"] = row_no
                        buf.append(self.stamp(rec))
                        if len(buf) >= self.h.batch_size:
                            total += len(buf)
                            yield Batch(records=buf, watermark=None, meta={"file": src_file})
                            buf = []
            total += len(buf)
            # Archive: moc chi tien khi doc HET moi member cua URL nay
            yield Batch(records=buf, watermark=None if self.is_full_snapshot else key,
                        meta={"file": key, "records": total})

    def _members(self, key: str, url: str, resp):
        """(khoá file, member, luồng). File thường: một phần tử, member = None."""
        if self.h.compression not in ARCHIVE_KINDS:
            yield key, None, resp
            return
        found = 0
        for member, stream in iter_archive(resp, self.h.compression, self.h.archive_members, name=url):
            found += 1
            yield f"{key}/{member}", member, stream
        if not found:
            raise RuntimeError(f"[{self.cfg.name}] archive {url[:120]} khong co member nao khop "
                               f"{self.h.archive_members}")
