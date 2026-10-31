"""Câu SQL khai trong entry: tham số có tên, kiểm tra tĩnh, dịch sang cú pháp driver.

Entry JDBC khai NGUYÊN câu truy vấn; framework chỉ làm hai việc là bind tham số
và đọc kết quả. Không ghép chuỗi, không tự sinh WHERE/ORDER BY:

    jdbc:
      query: |
        SELECT t.*, a.district_id
        FROM core.trans t JOIN core.account a USING (account_id)
        WHERE t.updated_at > :watermark
        ORDER BY t.updated_at

Tham số dùng cú pháp `:ten` (giống Oracle/SQLAlchemy/JDBC named parameter):

    :watermark   mốc CDC1 (checkpoint đã trừ lookback, lần đầu = cdc.initial_watermark)
    :chunk_size  số dòng mỗi lần truy vấn ở chế độ chia khúc (jdbc.chunk_size > 0)
    :run_ts      thời điểm bắt đầu lần chạy (UTC) — làm cận trên cho CDC1 theo thời gian
    :<khoa>      mọi khoá trong jdbc.params (override được khi test)

Vì sao không để người viết entry dùng thẳng `%s`/`%(x)s` của driver: dấu `%` trong
`LIKE 'VN%'` sẽ phải nhân đôi bằng tay, quên là lỗi khó hiểu lúc chạy. Ở đây `%`
được tự thoát, còn `::timestamp` (ép kiểu Postgres) và chuỗi/comment trong SQL
không bao giờ bị nhận nhầm là tham số.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Set, Tuple

BUILTIN_PARAMS = ("watermark", "chunk_size", "run_ts")

# Thứ tự alternation quan trọng: chuỗi và comment được "nuốt" trước, nên `:x`
# nằm trong '...' hay -- ... không bao giờ khớp nhánh tham số.
_TOKEN = re.compile(
    r"""
      (?P<sq>'(?:''|[^'])*')            # chuoi '...' ('' la dau nhay thoat)
    | (?P<dq>"(?:""|[^"])*")            # dinh danh "..."
    | (?P<bt>`(?:``|[^`])*`)            # dinh danh `...` (MySQL)
    | (?P<lc>--[^\n]*)                  # comment dong
    | (?P<bc>/\*.*?\*/)                 # comment khoi
    | (?P<cast>::)                      # ep kieu Postgres, khong phai tham so
    | :(?P<name>[A-Za-z_][A-Za-z0-9_]*)
    """,
    re.VERBOSE | re.DOTALL,
)


def referenced_params(sql: str) -> Set[str]:
    """Tên các tham số `:ten` xuất hiện trong câu (bỏ qua chuỗi, comment, `::`)."""
    return {m.group("name") for m in _TOKEN.finditer(sql) if m.group("name")}


def to_pyformat(sql: str, known: Iterable[str]) -> str:
    """`:ten` -> `%(ten)s` (paramstyle pyformat của psycopg2 và PyMySQL).

    Chỉ dịch tên nằm trong `known`; `:abc` lạ được giữ nguyên để lỗi hiện ra từ
    database thay vì bị nuốt im lặng. Mọi `%` có sẵn được nhân đôi — driver chạy
    toán tử `%` của Python trên CẢ câu, kể cả bên trong chuỗi.
    """
    names = set(known)
    out: List[str] = []
    pos = 0
    for m in _TOKEN.finditer(sql):
        out.append(sql[pos:m.start()].replace("%", "%%"))
        name = m.group("name")
        if name and name in names:
            out.append(f"%({name})s")
        else:
            out.append(m.group(0).replace("%", "%%"))
        pos = m.end()
    out.append(sql[pos:].replace("%", "%%"))
    return "".join(out)


def _strip_comments(sql: str) -> str:
    parts: List[str] = []
    pos = 0
    for m in _TOKEN.finditer(sql):
        parts.append(sql[pos:m.start()])
        if m.group("lc") or m.group("bc"):
            parts.append(" ")
        elif m.group("sq") or m.group("dq") or m.group("bt"):
            parts.append("''")  # noi dung chuoi khong tinh khi kiem tra ';'
        else:
            parts.append(m.group(0))
        pos = m.end()
    parts.append(sql[pos:])
    return "".join(parts)


def check_select(sql: str) -> Tuple[bool, str]:
    """Kiểm tra tĩnh: đúng MỘT câu, bắt đầu bằng SELECT hoặc WITH.

    Không thay được quyền chỉ đọc ở phía database (reader còn mở phiên READ ONLY),
    nhưng chặn sớm trong pytest những lỗi hiển nhiên: dán nhầm hai câu, hoặc một
    câu UPDATE lọt vào entry.
    """
    body = _strip_comments(sql).strip()
    if body.endswith(";"):
        body = body[:-1].rstrip()
    if not body:
        return False, "query rong"
    if ";" in body:
        return False, "query chua nhieu cau lenh (dau ';' o giua)"
    first = body.split(None, 1)[0].upper()
    if first not in ("SELECT", "WITH"):
        return False, f"query phai bat dau bang SELECT hoac WITH, dang la {first!r}"
    return True, ""


def sql_literal(value) -> str:
    """Giá trị Python -> literal SQL. Số giữ nguyên, còn lại thành chuỗi '...' ('' thoát)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    s = str(value)
    # Moc int luu dang chuoi trong checkpoint ("1056320"): giu la so de DB dung index
    if s.lstrip("-").isdigit():
        return s
    return "'" + s.replace("'", "''") + "'"


def render_literal(sql: str, values: dict) -> str:
    """`:ten` -> literal. Dành cho Spark JDBC: `dbtable=(<sql>) q` không bind được tham số.

    Giá trị đến từ checkpoint/params của entry (không phải người dùng cuối) và đi
    qua `sql_literal` (thoát dấu nháy). Chuỗi, comment, `::` được giữ nguyên như
    `to_pyformat`. Tên không có trong `values` -> giữ nguyên để DB báo lỗi.
    """
    out: List[str] = []
    pos = 0
    for m in _TOKEN.finditer(sql):
        out.append(sql[pos:m.start()])
        name = m.group("name")
        out.append(sql_literal(values[name]) if name and name in values else m.group(0))
        pos = m.end()
    out.append(sql[pos:])
    return "".join(out)
