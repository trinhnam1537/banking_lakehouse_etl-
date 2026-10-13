"""Reader cho cơ sở dữ liệu quan hệ (PostgreSQL / MySQL / MariaDB).

Entry khai NGUYÊN câu SQL (`jdbc.query`); reader không tự dựng câu nào. Việc của
reader chỉ còn:

  1. kết nối (thông tin từ Secrets Manager, không bao giờ từ YAML), mở phiên
     READ ONLY, chạy `init_sql`
  2. bind tham số: `:watermark` (mốc CDC1), `:chunk_size`, `:run_ts`, `jdbc.params`
  3. đọc kết quả theo luồng, chia lô, lấy mốc từ cột `cdc.watermark_column`

Vì sao để SQL trong entry thay vì sinh từ schema/table như trước: nguồn thật
hiếm khi là "SELECT * một bảng". Cần JOIN để lấy khoá nghiệp vụ, chọn cột để bỏ
PII, ép kiểu ngay ở nguồn, hay lọc mềm (`deleted_at IS NULL`). Cách làm này giống
`query` mode của Kafka Connect JDBC / `--query ... $CONDITIONS` của Sqoop, nhưng
tham số tường minh (`:watermark`) thay vì để framework tự chèn WHERE.

Hai chế độ đọc:

  một truy vấn (mặc định)   Server-side cursor, đọc dần, phù hợp bảng vừa.
  chia khúc (chunk_size>0)  Keyset pagination: chạy lại câu với `:watermark` mới
                            sau mỗi khúc. Mỗi khúc là một truy vấn ngắn: không giữ
                            transaction/snapshot hàng giờ, đứt mạng chỉ mất một
                            khúc. Đây là cách Debezium incremental snapshot và
                            Airbyte đọc bảng lớn.

Secret dạng chuẩn mà RDS sinh khi bật managed credentials:

    {"host": ..., "port": 5432, "username": ..., "password": ..., "dbname": ...}
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, Iterator, List, Optional

import boto3

from ..config import SourceConfig
from ..sqltemplate import BUILTIN_PARAMS, to_pyformat
from ..state import Checkpoint
from .base import Batch, Reader


class JdbcReader(Reader):
    def __init__(self, cfg: SourceConfig, secrets_client=None):
        super().__init__(cfg)
        if cfg.jdbc is None:
            raise ValueError(f"[{cfg.name}] thieu khoi cau hinh 'jdbc'")
        self.j = cfg.jdbc
        self._secrets = secrets_client
        self._conn = None
        self._run_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        # Tên cột thực nhận từ cursor.description -> manifest, để silver phát hiện
        # schema drift (nguồn thêm/bớt cột) mà không phải đọc file.
        self.observed_columns: List[str] = []
        self.sql = to_pyformat(self.j.query, list(BUILTIN_PARAMS) + list(self.j.params))

    # ---------- kết nối ----------

    def _connect(self):
        if self._conn is not None:
            return self._conn

        client = self._secrets or boto3.client("secretsmanager")
        s = json.loads(client.get_secret_value(SecretId=self.j.secret_name)["SecretString"])

        if self.j.driver == "postgresql":
            import psycopg2  # noqa: PLC0415

            # sslmode trong secret: Postgres nguon mo ra internet (core banking tren
            # may EC2 nguon) BAT BUOC "require". Mac dinh "prefer": giu hanh vi cu.
            self._conn = psycopg2.connect(
                host=s["host"], port=int(s.get("port", 5432)),
                user=s["username"], password=s["password"], dbname=s["dbname"],
                sslmode=s.get("sslmode", "prefer"),
                connect_timeout=15, application_name=f"fingraph-src-{self.cfg.name}"[:63],
            )
            if self.j.read_only:
                # Moi cau trong phien deu READ ONLY: UPDATE lot vao entry bi DB tu choi
                self._conn.set_session(readonly=True)
            init_cur = self._conn.cursor()
        elif self.j.driver == "mysql":
            import pymysql  # noqa: PLC0415

            self._conn = pymysql.connect(
                host=s["host"], port=int(s.get("port", 3306)),
                user=s["username"], password=s["password"], database=s["dbname"],
                connect_timeout=15, cursorclass=pymysql.cursors.DictCursor,
            )
            init_cur = self._conn.cursor()
            if self.j.read_only:
                init_cur.execute("SET SESSION TRANSACTION READ ONLY")
        else:
            raise ValueError(f"[{self.cfg.name}] driver khong ho tro: {self.j.driver}")

        for stmt in self.j.init_sql:
            init_cur.execute(stmt)
        init_cur.close()
        return self._conn

    # ---------- tham số ----------

    def _bind_value(self, value: Optional[str]) -> Any:
        """Ép mốc về kiểu mà driver so sánh đúng.

        - int: số thật, không phải chuỗi — so chuỗi '9' > '10'.
        - timestamp + MySQL: 'YYYY-MM-DD HH:MM:SS[.ffffff]' UTC, vì MySQL không
          hiểu 'T' và hậu tố '+00:00' của ISO 8601 (so sánh âm thầm sai).
        - timestamp + Postgres: giữ ISO 8601 (timestamptz đọc được).
        """
        if value is None:
            return None
        wtype = self.cfg.cdc.watermark_type
        if wtype == "int":
            return int(value)
        if wtype == "timestamp" and self.j.driver == "mysql":
            return _mysql_ts(value)
        return value

    def build_params(self, watermark: Optional[str]) -> Dict[str, Any]:
        run_ts = self._run_ts.isoformat()
        params: Dict[str, Any] = dict(self.j.params)
        params.update({
            "watermark": self._bind_value(watermark),
            "chunk_size": self.j.chunk_size,
            "run_ts": _mysql_ts(run_ts) if self.j.driver == "mysql" else run_ts,
        })
        return params

    # ---------- đọc ----------

    def _open_cursor(self, conn, streaming: bool):
        if self.j.driver == "postgresql":
            import psycopg2.extras  # noqa: PLC0415

            if streaming:
                # Named cursor = server-side: khong keo ca ket qua vao 1 GB RAM
                cur = conn.cursor(name=f"src_{self.cfg.name}"[:63],
                                  cursor_factory=psycopg2.extras.RealDictCursor)
                cur.itersize = self.j.fetch_size
                return cur
            return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # PyMySQL: cursor mac dinh tai TOAN BO ket qua truoc dong dau tien.
        # SSDictCursor doc dan tu socket; gia la khong duoc dung lau giua hai lan
        # doc (net_write_timeout) — moi lo chi la mot lan ghi bo dem, xa gioi han do.
        import pymysql.cursors  # noqa: PLC0415

        return conn.cursor(pymysql.cursors.SSDictCursor if streaming else pymysql.cursors.DictCursor)

    def _end_txn(self, conn) -> None:
        # Ket thuc transaction doc giua cac khuc: khong giu snapshot cu (Postgres
        # giu dead tuple, MySQL giu undo log) trong suot lan chay dai.
        rollback = getattr(conn, "rollback", None)
        if rollback:
            rollback()

    def _run_query(self, conn, watermark: Optional[str], streaming: bool) -> Iterator[Dict[str, Any]]:
        cur = self._open_cursor(conn, streaming)
        try:
            cur.execute(self.sql, self.build_params(watermark))
            if not self.observed_columns and getattr(cur, "description", None):
                self.observed_columns = [d[0] for d in cur.description]
            for row in cur:
                yield dict(row)
        finally:
            cur.close()

    def _watermark_of(self, rec: Dict[str, Any]) -> Any:
        col = self.cfg.cdc.watermark_column
        if col not in rec:
            # Khong co cot moc trong ket qua thi moc khong bao gio tien: lan nao
            # cung keo lai tu dau ma khong bao loi. Chan ngay o dong dau tien.
            raise RuntimeError(
                f"[{self.cfg.name}] ket qua query khong co cot moc '{col}'. "
                f"Cot hien co: {', '.join(rec)}. SELECT them cot nay (hoac dat alias)."
            )
        return rec[col]

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        conn = self._connect()
        cdc1 = not self.is_full_snapshot
        since = self.effective_since(checkpoint) if cdc1 else None
        size = self.j.fetch_size

        if self.j.chunk_size > 0:
            yield from self._read_chunks(conn, since, size)
            return

        max_wm: Optional[str] = None
        buf: List[Dict[str, Any]] = []
        for rec in self._run_query(conn, since, streaming=True):
            if cdc1:
                max_wm = self.max_watermark(max_wm, self._watermark_of(rec))
            buf.append(self.stamp(_jsonable(rec)))
            if len(buf) >= size:
                yield Batch(records=buf, watermark=max_wm)
                buf = []
        if buf:
            yield Batch(records=buf, watermark=max_wm)
        self._end_txn(conn)

    def _read_chunks(self, conn, since: Optional[str], size: int) -> Iterator[Batch]:
        cursor_wm = since
        chunk_no = 0
        while True:
            chunk_no += 1
            n = 0
            max_wm = cursor_wm
            buf: List[Dict[str, Any]] = []
            # Khuc co gioi han (LIMIT) nen cursor thuong la du, khong can server-side
            for rec in self._run_query(conn, cursor_wm, streaming=False):
                n += 1
                max_wm = self.max_watermark(max_wm, self._watermark_of(rec))
                buf.append(self.stamp(_jsonable(rec)))
                if len(buf) >= size:
                    yield Batch(records=buf, watermark=max_wm, meta={"chunk": chunk_no})
                    buf = []
            if buf:
                yield Batch(records=buf, watermark=max_wm, meta={"chunk": chunk_no})
            self._end_txn(conn)

            if n < self.j.chunk_size:
                return
            if max_wm == cursor_wm:
                # Ca khuc cung mot gia tri moc: query khong loc dung `> :watermark`
                # -> chay tiep la lap vo han tren cung mot khuc.
                raise RuntimeError(
                    f"[{self.cfg.name}] moc khong tien sau khuc {chunk_no} (van = {max_wm}). "
                    f"Query phai loc `{self.cfg.cdc.watermark_column} > :watermark` va "
                    f"ORDER BY cot do."
                )
            cursor_wm = max_wm

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _mysql_ts(value: str) -> str:
    """ISO 8601 (có thể kèm múi giờ) -> 'YYYY-MM-DD HH:MM:SS[.ffffff]' theo UTC."""
    ts = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is not None:
        ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return ts.isoformat(sep=" ")


def _jsonable(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Ép kiểu của driver về kiểu JSON ghi được.

    psycopg2 trả về datetime, Decimal, UUID, memoryview... `json.dumps` không
    nuốt được. Xử lý ở đây thay vì ở writer, vì đây là chỗ duy nhất biết dữ
    liệu đến từ driver nào.
    """
    import decimal
    import uuid as _uuid

    out: Dict[str, Any] = {}
    for k, v in rec.items():
        if isinstance(v, (dt.datetime, dt.date, dt.time)):
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
