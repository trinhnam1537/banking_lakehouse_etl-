"""DDL bảng silver: sinh từ entry, tạo trên Glue Catalog, tiến hoá schema.

Hai cách có bảng, chọn theo entry:

  target.ddl khai   dùng file silver_etl/ddl/<...>.sql — khi cần kiểm soát
                    tay: partition transform (bucket, days), TBLPROPERTIES,
                    comment, sort order.
  không khai        sinh từ `columns` + cột kỹ thuật. Đủ cho đa số bảng.

Dù cách nào, schema của BẢNG THẬT phải chứa mọi cột entry sẽ ghi.
`ensure_table` kiểm điều đó mỗi lần chạy:
  - bảng chưa có      -> CREATE
  - thiếu cột          -> ALTER TABLE ADD COLUMNS (schema_evolution=add_columns)
  - cột lệch kiểu      -> lỗi. Iceberg chỉ cho nới kiểu an toàn (int->bigint);
                          đổi kiểu tuỳ tiện là việc có chủ đích, làm bằng tay.

CLI (không cần Spark):
    python -m silver.ddl generate                      # in DDL mọi entry
    python -m silver.ddl generate --out silver_etl/ddl # ghi file (không đè file có sẵn)
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Dict, List, Optional

from .config import SilverConfig, WriterType, load_all
from .types import normalize_type, q

DEFAULT_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    # Athena doc duoc MOR v2; ghi nho, doc gop delete file. Compaction dinh ky
    # (rewrite_data_files) de delete file khong tich tu.
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}


def expected_columns(cfg: SilverConfig) -> List[tuple]:
    """(tên, kiểu, comment) đúng thứ tự sẽ ghi."""
    cols = [(c.name, normalize_type(c.type), c.comment) for c in cfg.business_columns]
    return cols + [(n, t, cm) for n, t, cm in cfg.technical_columns]


def partition_by(cfg: SilverConfig) -> List[str]:
    if cfg.target.partition_by:
        return cfg.target.partition_by
    if cfg.writer.type is WriterType.SCD4A:
        return ["ds_partition_date"]
    return []


def _comment(s: str) -> str:
    return " COMMENT '" + s.replace("'", "\\'") + "'" if s else ""


def generate_ddl(cfg: SilverConfig) -> str:
    cols = ",\n".join(f"    {q(n)} {t}{_comment(cm)}" for n, t, cm in expected_columns(cfg))
    parts = partition_by(cfg)
    props = {**DEFAULT_PROPERTIES, **cfg.target.properties}
    lines = [
        f"-- Sinh tu entry `{cfg.name}` (writer: {cfg.writer.type.value}). Sua tay duoc;",
        "-- runner van tu them cot moi cua entry (ALTER TABLE ADD COLUMNS).",
        f"CREATE TABLE IF NOT EXISTS {cfg.target.fqn} (",
        cols,
        ")",
        "USING iceberg",
    ]
    if parts:
        lines.append(f"PARTITIONED BY ({', '.join(parts)})")
    if cfg.description:
        lines.append(_comment(cfg.description).strip())
    lines.append("TBLPROPERTIES (")
    lines.append(",\n".join(f"    '{k}' = '{v}'" for k, v in props.items()))
    lines.append(");")
    return "\n".join(lines) + "\n"


def split_statements(text: str) -> List[str]:
    """Tách file .sql thành câu lệnh; bỏ comment `--` và câu rỗng.

    Quét từng ký tự thay vì split(";"): `;` và `--` nằm TRONG chuỗi
    ('... (khong gom); 9999-12-31') hay tên trích dẫn không phải ranh giới.
    """
    out, buf = [], []
    quote = None           # "'" | '"' | "`" khi dang trong chuoi
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == "\\" and i + 1 < n:          # ky tu thoat trong chuoi
                buf.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j                  # bo den het dong
            continue
        elif ch == ";":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


# ---------------- can Spark ----------------


def _table_exists(spark, fqn: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {fqn}")
        return True
    except Exception as e:  # noqa: BLE001 — AnalysisException, khong import duoc khi chua co Spark
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "not found" in str(e).lower():
            return False
        raise


def _spark_type_str(dtype) -> str:
    return normalize_type(dtype.simpleString())


def ensure_table(spark, cfg: SilverConfig, ddl_text: Optional[str] = None) -> Dict[str, List[str]]:
    """Bảo đảm bảng tồn tại và đủ cột. Trả về {"created": [...], "added": [...]}."""
    t = cfg.target
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {t.catalog}.{t.database}")
    report = {"created": [], "added": []}

    if not _table_exists(spark, t.fqn):
        for stmt in split_statements(ddl_text or generate_ddl(cfg)):
            spark.sql(stmt)
        if not _table_exists(spark, t.fqn):
            raise RuntimeError(f"[{cfg.name}] chay DDL xong van khong thay {t.fqn} — "
                               f"file DDL tao nham ten bang?")
        report["created"].append(t.fqn)

    have = {f.name: _spark_type_str(f.dataType) for f in spark.table(t.fqn).schema.fields}
    missing, mismatch = [], []
    for name, typ, cm in expected_columns(cfg):
        if name not in have:
            missing.append((name, typ, cm))
        elif have[name].replace(" ", "") != typ.replace(" ", ""):
            mismatch.append(f"{name}: bang={have[name]} entry={typ}")

    if mismatch:
        raise RuntimeError(f"[{cfg.name}] lech kieu cot voi {t.fqn}: {mismatch}. "
                           f"Doi kieu la thay doi co chu dich — ALTER bang tay.")
    if missing:
        if t.schema_evolution != "add_columns":
            raise RuntimeError(f"[{cfg.name}] {t.fqn} thieu cot {[m[0] for m in missing]} "
                               f"(schema_evolution=strict)")
        cols = ", ".join(f"{q(n)} {ty}{_comment(cm)}" for n, ty, cm in missing)
        spark.sql(f"ALTER TABLE {t.fqn} ADD COLUMNS ({cols})")
        report["added"] = [m[0] for m in missing]
    return report


# ---------------- CLI ----------------


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Sinh DDL bang silver tu entry")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--entries", default="silver_etl/entries")
    g.add_argument("--out", help="Thu muc ghi file .sql; bo trong = in ra man hinh")
    g.add_argument("--only", help="Chi mot entry")
    g.add_argument("--force", action="store_true", help="Ghi de file da co")
    a = p.parse_args(argv)

    cfgs = load_all(a.entries)
    for name, cfg in sorted(cfgs.items()):
        if a.only and name != a.only:
            continue
        text = generate_ddl(cfg)
        if not a.out:
            print(text)
            continue
        rel = cfg.target.ddl or f"{cfg.target.database}/{cfg.target.table}.sql"
        path = os.path.join(a.out, rel)
        if os.path.exists(path) and not a.force:
            print(f"bo qua (da co, dung --force de ghi de): {path}")
            continue
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        print(f"da ghi {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
