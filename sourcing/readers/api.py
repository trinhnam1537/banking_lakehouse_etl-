"""Reader cho REST API.

Xử lý ba việc mà mọi API đều bắt bạn tự làm: phân trang, giới hạn tốc độ,
và lấy khoá bí mật ra khỏi file cấu hình.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterator, List, Optional

import boto3

from ..config import SourceConfig
from ..state import Checkpoint
from .base import Batch, Reader

# SEC EDGAR từ chối request không có User-Agent mô tả danh tính, và trả 403
# với thông báo không nói rõ lý do. Đặt mặc định để không ai vấp lại.
DEFAULT_UA = "HUST FinGraph Research (contact via repo)"

# Placeholder trong api.query, thay bang moc CDC1 luc chay
SINCE = "{since}"

RAW_MODES = ("response", "records")


class ApiReader(Reader):
    def __init__(self, cfg: SourceConfig, secrets_client=None):
        super().__init__(cfg)
        if cfg.api is None:
            raise ValueError(f"[{cfg.name}] thieu khoi cau hinh 'api'")
        self.api = cfg.api
        if self.api.raw_mode not in RAW_MODES:
            raise ValueError(
                f"[{cfg.name}] api.raw_mode '{self.api.raw_mode}' khong hop le. "
                f"Chon mot trong: {', '.join(RAW_MODES)}"
            )
        self._secrets = secrets_client
        self._last_call = 0.0
        self._headers = dict(self.api.headers)
        self._headers.setdefault("User-Agent", DEFAULT_UA)
        self._headers.setdefault("Accept", "application/json")
        self._query_extra: Dict[str, Any] = {}
        self._load_secret()

    # ---------- bí mật ----------

    def _load_secret(self) -> None:
        """Nạp khoá từ Secrets Manager vào header hoặc query.

        YAML chỉ khai *tên* secret và *chỗ* cần chèn:

            secret_name: fingraph/finnhub
            secret_inject:
              token: query.token          # secret["token"] -> ?token=...
              api_key: header.X-Api-Key   # secret["api_key"] -> header

        Khoá thật không bao giờ nằm trong repo, và đổi khoá không cần sửa YAML.
        """
        if not self.api.secret_name or not self.api.secret_inject:
            return

        client = self._secrets or boto3.client("secretsmanager")
        raw = client.get_secret_value(SecretId=self.api.secret_name)["SecretString"]
        secret = json.loads(raw)

        for secret_key, destination in self.api.secret_inject.items():
            if secret_key not in secret:
                raise KeyError(
                    f"[{self.cfg.name}] secret '{self.api.secret_name}' "
                    f"khong co khoa '{secret_key}'"
                )
            where, _, target = destination.partition(".")
            if where == "header":
                self._headers[target] = secret[secret_key]
            elif where == "query":
                self._query_extra[target] = secret[secret_key]
            else:
                raise ValueError(
                    f"[{self.cfg.name}] secret_inject '{destination}' sai dinh dang. "
                    f"Dung 'header.<Ten>' hoac 'query.<ten>'."
                )

    # ---------- gọi HTTP ----------

    def _throttle(self) -> None:
        """Giãn cách giữa hai lần gọi.

        Nhiều API công khai giới hạn theo giây và trả 429 khi vượt. Tự giới hạn
        ở phía mình rẻ hơn nhiều so với bị chặn IP rồi phải chờ gỡ.
        """
        if self.api.rate_limit_per_sec <= 0:
            return
        min_gap = 1.0 / self.api.rate_limit_per_sec
        elapsed = time.monotonic() - self._last_call
        if elapsed < min_gap:
            time.sleep(min_gap - elapsed)
        self._last_call = time.monotonic()

    def _fetch(self, query: Dict[str, Any]) -> Any:
        self._throttle()
        url = self.api.base_url.rstrip("/")
        if self.api.path:
            url = f"{url}/{self.api.path.lstrip('/')}"
        # Bo placeholder "{since}": read() tu chen gia tri that vao `query` khi
        # co moc. Chua co moc thi tham so do bi bo han, khong gui nguyen van.
        static = {k: v for k, v in self.api.query.items() if v != SINCE}
        merged = {**static, **self._query_extra, **query}
        if merged:
            url = f"{url}?{urllib.parse.urlencode(merged, doseq=True)}"

        req = urllib.request.Request(url, headers=self._headers, method=self.api.method)

        # Lui theo cap so nhan cho 429 va loi 5xx. Loi 4xx khac thi that bai
        # ngay — thu lai mot request sai dinh dang chi ton thoi gian.
        delay = 1.0
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=self.api.timeout_seconds) as r:
                    return json.loads(r.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                if e.code == 429 or e.code >= 500:
                    if attempt == 4:
                        raise
                    time.sleep(delay)
                    delay *= 2
                    continue
                body = e.read()[:300].decode("utf-8", "replace")
                raise RuntimeError(
                    f"[{self.cfg.name}] HTTP {e.code} tu {url[:160]}: {body}"
                ) from None
            except urllib.error.URLError:
                if attempt == 4:
                    raise
                time.sleep(delay)
                delay *= 2
        raise RuntimeError(f"[{self.cfg.name}] het luot thu lai")

    # ---------- bóc bản ghi ----------

    def _extract(self, payload: Any) -> List[Dict[str, Any]]:
        node = payload
        if self.api.records_path:
            for part in self.api.records_path.split("."):
                if node is None:
                    return []
                # .get: API tra loi dang {"error": ...} thi coi nhu khong co
                # ban ghi, khong de job chet vi KeyError.
                node = node.get(part) if isinstance(node, dict) else None
        if node is None:
            return []
        if isinstance(node, dict):
            return [node]
        return [r for r in node if isinstance(r, dict)]

    def _raw_response(self, payload: Any, query: Dict[str, Any], page: int) -> Dict[str, Any]:
        """Một dòng raw = một HTTP response nguyên văn.

        `_request` giúp silver biết response này trả lời câu hỏi nào (mã nào,
        trang nào) — cần khi cùng dataset gọi nhiều tham số. CHỈ gồm query
        khai trong YAML và tham số phân trang, KHÔNG gồm `_query_extra`: đó là
        chỗ chứa khoá bí mật từ Secrets Manager, không được rơi xuống S3.
        """
        static = {k: v for k, v in self.api.query.items() if v != SINCE}
        return self.stamp({
            "_request": {
                "method": self.api.method,
                "url": f"{self.api.base_url.rstrip('/')}/{self.api.path.lstrip('/')}".rstrip("/"),
                "query": {**static, **query},
                "page": page,
            },
            "_records_path": self.api.records_path,
            "_response": payload,
        })

    @staticmethod
    def _dig(payload: Any, path: str) -> Optional[Any]:
        node = payload
        for part in path.split("."):
            if not isinstance(node, dict):
                return None
            node = node.get(part)
        return node

    # ---------- vòng lặp chính ----------

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        since = self.effective_since(checkpoint)
        base_query: Dict[str, Any] = {}

        # CDC1: chèn mốc vào query. Tên tham số lọc khai trong api.query bằng
        # placeholder "{since}" — mỗi API đặt tên một kiểu (from, since,
        # updated_after...), nên để YAML quyết định thay vì đoán.
        if since is not None:
            base_query = {k: since for k, v in self.api.query.items() if v == SINCE}

        page = 1
        cursor: Optional[str] = None
        max_wm: Optional[str] = None
        pages = 0

        while pages < self.api.max_pages:
            q = dict(base_query)
            if self.api.pagination == "page":
                q[self.api.page_param] = page
                q[self.api.page_size_param] = self.api.page_size
            elif self.api.pagination == "offset":
                q[self.api.page_param] = (page - 1) * self.api.page_size
                q[self.api.page_size_param] = self.api.page_size
            elif self.api.pagination == "cursor" and cursor:
                q[self.api.cursor_param] = cursor

            payload = self._fetch(q)
            records = self._extract(payload)
            pages += 1

            if not records:
                # Trang dau rong van ghi o che do response: API tra 200 kem
                # {"error": ...} thi raw phai giu lai bang chung. Trang rong o
                # cuoi phan trang thi bo — chi la tin hieu "het du lieu".
                if self.api.raw_mode == "response" and pages == 1 and payload not in (None, [], {}):
                    yield Batch(records=[self._raw_response(payload, q, pages)],
                                watermark=max_wm, meta={"page": pages, "records": 0})
                break

            wm_col = self.cfg.cdc.watermark_column
            if wm_col and not self.is_full_snapshot:
                for rec in records:
                    max_wm = self.max_watermark(max_wm, rec.get(wm_col))

            if self.api.raw_mode == "response":
                out = [self._raw_response(payload, q, pages)]
            else:
                out = [self.stamp(r) for r in records]
            yield Batch(
                records=out,
                watermark=max_wm,
                meta={"page": pages, "records": len(records)},
            )

            if self.api.pagination == "none":
                break
            if self.api.pagination == "cursor":
                cursor = self._dig(payload, self.api.cursor_path) if self.api.cursor_path else None
                if not cursor:
                    break
            else:
                # Trang chưa đầy nghĩa là đã hết dữ liệu — dừng sớm, khỏi gọi
                # thêm một request chỉ để nhận mảng rỗng.
                if len(records) < self.api.page_size:
                    break
                page += 1

        if pages >= self.api.max_pages:
            # Không ném lỗi: dữ liệu đã kéo được vẫn hợp lệ và checkpoint vẫn
            # tiến. Nhưng phải để lại dấu vết, vì chạm trần thường có nghĩa là
            # max_pages đặt quá thấp so với khối lượng thật.
            print(f"[{self.cfg.name}] CANH BAO: cham tran max_pages="
                  f"{self.api.max_pages}. Co the con du lieu chua keo.")
