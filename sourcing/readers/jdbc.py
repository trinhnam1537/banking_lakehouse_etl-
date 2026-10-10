"""Reader cho cơ sở dữ liệu quan hệ (RDS PostgreSQL / MySQL).

Chưa có RDS trong hạ tầng hiện tại — reader này viết sẵn để khi dựng lên là
dùng được ngay, chỉ cần thêm một entry YAML.

Kết nối lấy từ Secrets Manager, không bao giờ từ YAML. Secret dạng chuẩn mà
RDS tự sinh khi bật "managed credentials":

    {"host": ..., "port": 5432, "username": ..., "password": ..., "dbname": ...}
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List, Optional

import boto3

from ..config import SourceConfig
from ..state import Checkpoint
from .base import Batch, Reader

FETCH_SIZE = 5_000


class JdbcReader(Reader):
    def __init__(self, cfg: SourceConfig, secrets_client=None):
        super().__init__(cfg)
        if cfg.jdbc is None:
            raise ValueError(f"[{cfg.name}] thieu khoi cau hinh 'jdbc'")
        self.j = cfg.jdbc
        self._secrets = secrets_client
        self._conn = None

    def _connect(self):
        if self._conn is not None:
            return self._conn

        client = self._secrets or boto3.client("secretsmanager")
        s = json.loads(client.get_secret_value(SecretId=self.j.secret_name)["SecretString"])

        if self.j.driver == "postgresql":
            import psycopg2  # noqa: PLC0415

            self._conn = psycopg2.connect(
                host=s["host"], port=int(s.get("port", 5432)),
                user=s["username"], password=s["password"], dbname=s["dbname"],
                connect_timeout=15,
            )
        elif self.j.driver == "mysql":
            import pymysql  # noqa: PLC0415

            self._conn = pymysql.connect(
                host=s["host"], port=int(s.get("port", 3306)),
                user=s["username"], password=s["password"], database=s["dbname"],
                connect_timeout=15, cursorclass=pymysql.cursors.DictCursor,
            )
        else:
            raise ValueError(f"[{self.cfg.name}] driver khong ho tro: {self.j.driver}")
        return self._conn

    def _build_sql(self, since: Optional[str]) -> tuple:
        """Dựng câu truy vấn kèm tham số.

        Luôn dùng tham số hoá (`%s`) cho giá trị mốc, không nội suy chuỗi.
        Mốc đến từ DynamoDB nên về lý thuyết là dữ liệu mình kiểm soát, nhưng
        thói quen nội suy chuỗi vào SQL là thứ không nên có ở bất kỳ đâu.

        Tên bảng và cột thì buộc phải nội suy (SQL không tham số hoá được
        định danh) — bù lại chúng đến từ file YAML trong repo, đã qua review,
        không phải từ dữ liệu người dùng.
        """
        q = self._quote
        table = f"{q(self.j.schema)}.{q(self.j.table)}"
        where: List[str] = []
        params: List[Any] = []

        if since is not None and self.cfg.cdc.watermark_column:
            where.append(f"{q(self.cfg.cdc.watermark_column)} > %s")
            params.append(since)
        if self.j.extra_where:
            where.append(f"({self.j.extra_where})")

        sql = f"SELECT * FROM {table}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        if self.cfg.cdc.watermark_column and not self.is_full_snapshot:
            # Sắp xếp theo mốc: nếu job chết giữa chừng, phần đã ghi luôn là
            # một tiền tố liên tục của dữ liệu, nên checkpoint vẫn nhất quán.
            sql += f" ORDER BY {q(self.cfg.cdc.watermark_column)} ASC"
        return sql, params

    def _quote(self, name: str) -> str:
        """Bọc định danh theo driver.

        Postgres dùng `"x"`; MySQL/MariaDB dùng backtick — với nó `"x"` là CHUỖI,
        nên `SELECT * FROM "financial"."order"` là lỗi cú pháp, và bảng `order`
        (từ khoá SQL) không bọc thì cũng lỗi. Ký tự bọc bên trong tên được nhân
        đôi theo đúng quy tắc của từng hệ.
        """
        if self.j.driver == "mysql":
            return "`" + name.replace("`", "``") + "`"
        return '"' + name.replace('"', '""') + '"'

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        since = self.effective_since(checkpoint)
        sql, params = self._build_sql(since)
        conn = self._connect()

        # Server-side cursor: không kéo cả bảng vào RAM. Với Python Shell chỉ
        # có 1 GB bộ nhớ thì đây là khác biệt giữa chạy được và chết.
        if self.j.driver == "postgresql":
            import psycopg2.extras  # noqa: PLC0415

            cur = conn.cursor(name=f"src_{self.cfg.name}",
                              cursor_factory=psycopg2.extras.RealDictCursor)
            cur.itersize = FETCH_SIZE
        else:
            # Cursor mặc định của PyMySQL tải TOÀN BỘ kết quả vào RAM trước
            # khi trả dòng đầu tiên — bảng trans của Berka (1,06 triệu dòng)
            # vượt 1 GB của Python Shell. SSDictCursor đọc dần từ socket.
            # Cái giá: không được dừng lâu giữa hai lần đọc (server cắt sau
            # net_write_timeout, mặc định 60 giây) — mỗi lô chỉ ghi một file
            # S3 nên còn xa giới hạn đó.
            import pymysql.cursors  # noqa: PLC0415

            cur = conn.cursor(pymysql.cursors.SSDictCursor)

        cur.execute(sql, params)
        wm_col = self.cfg.cdc.watermark_column
        max_wm: Optional[str] = None
        buf: List[Dict[str, Any]] = []

        for row in cur:
            rec = dict(row)
            if wm_col and not self.is_full_snapshot:
                max_wm = self.max_watermark(max_wm, rec.get(wm_col))
            buf.append(self.stamp(_jsonable(rec)))

            if len(buf) >= FETCH_SIZE:
                yield Batch(records=buf, watermark=max_wm)
                buf = []

        if buf:
            yield Batch(records=buf, watermark=max_wm)
        cur.close()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _jsonable(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Ép kiểu của driver về kiểu JSON ghi được.

    psycopg2 trả về datetime, Decimal, UUID, memoryview... `json.dumps` không
    nuốt được. Xử lý ở đây thay vì ở writer, vì đây là chỗ duy nhất biết dữ
    liệu đến từ driver nào.
    """
    import datetime as _dt
    import decimal
    import uuid as _uuid

    out: Dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
            out[k] = v.isoformat()
        elif isinstance(v, decimal.Decimal):
            # Chuỗi, KHÔNG float: float làm sai số tiền ngay ở tầng raw và
            # không lấy lại được. Silver ép về decimal(p,s) theo entry của nó.
            out[k] = str(v)
        elif isinstance(v, _uuid.UUID):
            out[k] = str(v)
        elif isinstance(v, (bytes, bytearray, memoryview)):
            out[k] = bytes(v).hex()
        else:
            out[k] = v
    return out
