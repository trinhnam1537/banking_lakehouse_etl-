"""Ép giá trị raw (luôn là CHUỖI) về kiểu trong entry.

Mọi hàm ở đây trả về **biểu thức Spark SQL dạng chuỗi**, không đụng pyspark:
test được không cần Spark, và in ra được để đọc khi điều tra một cột lạ.

Hai nguyên tắc:

  1. KHÔNG làm chết job vì một giá trị hỏng. Dùng `try_cast` / `try_to_timestamp`:
     giá trị không ép được thành NULL, và runner ĐẾM chúng (cast error) để
     quality quyết định có cho ghi hay không. Một bản ghi lỗi trong 27.834
     ngân hàng không được làm mất 27.833 bản ghi còn lại.
  2. Raw giữ nguyên văn nên giá trị lồng (object/mảng) đến đây dưới dạng chuỗi
     JSON. Kiểu phức (array/map/struct) ép bằng `from_json`, không phải cast.
"""

from __future__ import annotations

from typing import Dict, List

from .config import ColumnSpec

TRUE_WORDS = ("true", "t", "y", "yes", "1")
FALSE_WORDS = ("false", "f", "n", "no", "0")

_INT_ALIASES = {"int": "int", "integer": "int", "bigint": "bigint", "long": "bigint",
                "smallint": "smallint", "tinyint": "tinyint"}


def q(name: str) -> str:
    """Trích dẫn tên cột cho Spark SQL — tên nguồn hay có dấu `-`, `.`."""
    return "`" + name.replace("`", "``") + "`"


def _lit(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def normalize_type(t: str) -> str:
    t = t.strip().lower()
    return _INT_ALIASES.get(t, t)


def is_complex(t: str) -> bool:
    return normalize_type(t).startswith(("array<", "map<", "struct<"))


def cast_sql(expr: str, col: ColumnSpec) -> str:
    """Biểu thức ép `expr` (chuỗi) về kiểu của cột.

    >>> cast_sql("`x`", ColumnSpec(name="x", type="date", format="dd/MM/yyyy"))
    "to_date(try_to_timestamp(`x`, 'dd/MM/yyyy'))"
    """
    t = normalize_type(col.type)
    fmt = col.format

    if t == "string":
        return expr

    if t == "boolean":
        v = f"lower(trim({expr}))"
        trues = ", ".join(_lit(w) for w in TRUE_WORDS)
        falses = ", ".join(_lit(w) for w in FALSE_WORDS)
        return f"CASE WHEN {v} IN ({trues}) THEN true WHEN {v} IN ({falses}) THEN false END"

    if t in ("timestamp", "date"):
        if fmt == "epoch_ms":
            ts = f"timestamp_millis(try_cast({expr} AS bigint))"
        elif fmt == "epoch_s":
            ts = f"timestamp_seconds(try_cast({expr} AS bigint))"
        elif fmt:
            ts = f"try_to_timestamp({expr}, {_lit(fmt)})"
        else:
            # ISO 8601, co hoac khong co 'T'/'Z' — cast chuoi cua Spark hieu het.
            return f"try_cast({expr} AS {t})"
        return ts if t == "timestamp" else f"to_date({ts})"

    if is_complex(t):
        return f"from_json({expr}, {_lit(t)})"

    # so + decimal: bo khoang trang dau/cuoi ma nguon hay de lai
    return f"try_cast(trim({expr}) AS {t})"


def transform_sql(expr: str, col: ColumnSpec) -> str:
    """Chèn cột vào biểu thức `transform` của entry trước khi ép kiểu."""
    return col.transform.replace("{col}", expr) if col.transform else expr


def json_path(path: str, root: str = "$") -> str:
    """`a.b-c.d` -> `$['a']['b-c']['d']` cho get_json_object.

    Dạng ngoặc vuông vì tên trường thật có dấu `-` (GLEIF `direct-parent`)
    mà dạng chấm của get_json_object không nhận.
    """
    if not path:
        return root
    return root + "".join(f"[{_lit(p)}]" for p in path.split("."))


# ---------- schema cho from_json: moi truong la mot la chuoi ----------


def path_tree(paths: List[str]) -> Dict:
    """Gom các đường chấm thành cây; lá là None.

    Hai đường mà một cái là tiền tố của cái kia (`a.b` và `a.b.c`) không cùng
    nằm được trong một schema — `a.b` vừa là lá chuỗi vừa là struct. Hàm trả
    về cây của các đường KHÔNG xung đột; đường xung đột được bóc bằng
    get_json_object thay thế (xem extract.py).
    """
    tree: Dict = {}
    for p in sorted(set(paths), key=lambda s: (s.count("."), s)):
        node = tree
        parts = p.split(".")
        for i, part in enumerate(parts):
            last = i == len(parts) - 1
            if part in node and node[part] is None:
                raise ValueError(p)  # tien to da la la
            if last:
                if part in node:
                    raise ValueError(p)  # dai hon da la nhanh
                node[part] = None
            else:
                node = node.setdefault(part, {})
    return tree


def split_conflicts(paths: List[str]):
    """(đường vào được schema, đường phải bóc riêng)."""
    ok, conflict = [], []
    for p in sorted(set(paths), key=lambda s: (s.count("."), s)):
        try:
            path_tree(ok + [p])
            ok.append(p)
        except ValueError:
            conflict.append(p)
    return ok, conflict


def tree_to_ddl(tree: Dict) -> str:
    """Cây -> chuỗi schema DDL: `struct<a:string,b:struct<c:string>>`.

    Lá là STRING có chủ ý: Spark giữ nguyên văn JSON của object/mảng khi schema
    khai là chuỗi, và mọi việc ép kiểu dồn về cast_sql ở một chỗ.
    """
    parts = []
    for k, v in tree.items():
        inner = "string" if v is None else tree_to_ddl(v)
        parts.append(f"{q(k)}:{inner}")
    return f"struct<{','.join(parts)}>"
