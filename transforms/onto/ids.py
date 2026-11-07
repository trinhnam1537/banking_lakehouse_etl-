"""Định danh node và cạnh của đồ thị (bước O6, docs/ONTOLOGY-BUILD.md).

    uid       = sha256(class_iri | source_system | natural_key)[:32]
    triple_id = sha256(subject_uid | predicate_iri | object | valid_from | source_dataset)[:32]

Hai quy tắc này KHÔNG được đổi sau khi đã có người dùng: trích dẫn của agent, embedding
đã train, Recommendation đã ghi đều trỏ vào uid/triple_id. Bản Python (test, agent) và bản
cột Spark (job) phải cho cùng kết quả — test_ids kiểm điều đó.

natural_key có dạng "ten1=gia_tri1|ten2=gia_tri2" theo thứ tự khai báo trong mapping, ví dụ
"client_id=2". NULL viết là chuỗi rỗng để hai khoá (NULL, 'x') và ('', 'x') vẫn khác nhau
về tên cột nhưng không làm hỏng cả chuỗi như concat của SQL.
"""

from __future__ import annotations

import hashlib
from typing import Optional, Sequence

UID_LEN = 32
PRODUCT_SYSTEM = "ontology"  # node sản phẩm sinh từ TBox, không thuộc nguồn nào


def natural_key(names: Sequence[str], values: Sequence[object]) -> str:
    return "|".join(f"{n}={'' if v is None else v}" for n, v in zip(names, values))


def make_uid(class_iri: str, source_system: str, nkey: str) -> str:
    return hashlib.sha256(f"{class_iri}|{source_system}|{nkey}".encode("utf-8")).hexdigest()[:UID_LEN]


def make_triple_id(subject_uid: str, predicate_iri: str, obj: str, valid_from: Optional[str],
                   source_dataset: str) -> str:
    raw = f"{subject_uid}|{predicate_iri}|{obj}|{valid_from or ''}|{source_dataset}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:UID_LEN]


def product_uid(class_iri: str) -> str:
    """Node sản phẩm (fin:Loan...) — một node cho mỗi lớp sản phẩm của taxonomy."""
    return make_uid(class_iri, PRODUCT_SYSTEM, f"iri={class_iri}")


# ---------- Bản cột Spark (import pyspark tại chỗ: module dùng được khi không có Spark) ----------

def natural_key_col(names: Sequence[str], cols):
    from pyspark.sql import functions as F

    parts = []
    for n, c in zip(names, cols):
        parts.append(F.concat(F.lit(f"{n}="), F.coalesce(c.cast("string"), F.lit(""))))
    return F.concat_ws("|", *parts)


def uid_col(class_iri: str, source_system: str, nkey_col):
    from pyspark.sql import functions as F

    return F.substring(F.sha2(F.concat(F.lit(f"{class_iri}|{source_system}|"), nkey_col), 256), 1, UID_LEN)


def triple_id_col(subject_col, predicate_col, obj_col, valid_from_col, source_col):
    from pyspark.sql import functions as F

    s = lambda c: F.coalesce(c.cast("string"), F.lit(""))  # noqa: E731
    raw = F.concat_ws("|", s(subject_col), s(predicate_col), s(obj_col), s(valid_from_col), s(source_col))
    return F.substring(F.sha2(raw, 256), 1, UID_LEN)
