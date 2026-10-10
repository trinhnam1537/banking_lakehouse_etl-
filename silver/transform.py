"""Cột chuỗi -> cột có kiểu + cột kỹ thuật ds_* + khử trùng lặp + quality.

Thứ tự trong `build()` là có chủ ý:

  transform -> cast -> default -> expr -> filter -> technical -> dedup -> quality

  - expr đứng SAU cast: `price * qty` phải nhân số, không nhân chuỗi.
  - filter đứng SAU cast: điều kiện viết trên kiểu thật (`d >= date'2024-01-01'`).
  - dedup đứng SAU technical: cần ds_key và ds_cdc_index để chọn bản mới nhất.
  - quality đứng CUỐI: đếm trên đúng thứ sắp ghi.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import extract as X
from .config import SilverConfig
from .types import cast_sql, q, transform_sql

SEP = "\u0001"          # phan cach khi bam — khong xuat hien trong du lieu thuc
NULL_MARK = "\u0000N"    # NULL khac chuoi rong khi bam
OPEN_END = "9999-12-31"  # ds_valid_to cua phien ban hien hanh (SCD2)


class QualityError(RuntimeError):
    pass


@dataclass
class TransformReport:
    rows_in: int = 0
    rows_out: int = 0
    duplicate_keys: int = 0
    cast_errors: Dict[str, int] = field(default_factory=dict)
    null_violations: Dict[str, int] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict:
        return {
            "rows_in": self.rows_in, "rows_out": self.rows_out,
            "duplicate_keys": self.duplicate_keys,
            "cast_errors": {k: v for k, v in self.cast_errors.items() if v},
            "null_violations": {k: v for k, v in self.null_violations.items() if v},
            "warnings": self.warnings,
        }


# ---------------- cot nghiep vu ----------------


def apply_types(df, cfg: SilverConfig):
    """transform -> cast -> default -> expr. Trả về (df, danh sách cột cờ lỗi cast)."""
    from pyspark.sql import functions as F

    flags = []
    for c in cfg.columns:
        if c.expr:
            continue
        raw = f"_x_raw_{c.name}"
        df = df.withColumnRenamed(c.name, raw)
        src = transform_sql(q(raw), c)
        df = df.withColumn(c.name, F.expr(cast_sql(src, c)))
        # Loi ep kieu = nguon CO gia tri ma ket qua NULL. Nguon NULL thi khong
        # tinh — thieu du lieu la viec cua quality.not_null, khong phai cast.
        flag = f"_x_casterr_{c.name}"
        df = df.withColumn(flag, F.col(raw).isNotNull() & (F.trim(F.col(raw)) != "")
                           & F.col(c.name).isNull())
        flags.append(flag)
        df = df.drop(raw)
        if c.default is not None:
            df = df.withColumn(c.name, F.coalesce(F.col(c.name), F.lit(c.default).cast(c.type)))

    for c in cfg.columns:
        if c.expr:
            df = df.withColumn(c.name, F.expr(c.expr).cast(c.type))
    return df, flags


# ---------------- cot ky thuat ----------------


def _hash_of(cols: List[str]):
    from pyspark.sql import functions as F

    parts = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_MARK)) for c in cols]
    return F.sha2(F.concat_ws(SEP, *parts), 256)


def _cdc_index_col(df, expr: Optional[str]):
    """ds_cdc_index là BIGINT so sánh được: timestamp -> micro-giây, số -> số."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    src = F.expr(expr) if expr else F.to_timestamp(F.col(X.INGEST_TS))
    probe = df.select(src.alias("_p")).schema["_p"].dataType
    if isinstance(probe, (T.TimestampType, T.TimestampNTZType)):
        return F.unix_micros(src)
    if isinstance(probe, T.DateType):
        return F.unix_micros(src.cast("timestamp"))
    if isinstance(probe, T.StringType):
        return F.unix_micros(F.expr(f"try_cast({expr or q(X.INGEST_TS)} AS timestamp)"))
    return src.cast("bigint")


def add_technical(df, cfg: SilverConfig, run_id: str, source_label: str,
                  data_date: Optional[dt.date]):
    from pyspark.sql import functions as F

    t = cfg.technical
    pd = t.partition_date
    if pd.source == "ingest_date":
        part = F.to_date(F.col(X.INGEST_DATE))
    elif pd.source == "run_date":
        if data_date is None:
            raise ValueError(f"[{cfg.name}] partition_date=run_date nhung khong co --DATA_DATE")
        part = F.lit(data_date.isoformat()).cast("date")
    elif pd.source == "column":
        part = F.col(pd.column).cast("date")
    else:
        part = F.expr(pd.expr).cast("date")

    keys = t.keys or cfg.hash_columns   # khong khai key: moi cot la khoa (bang su kien)
    status = F.lit("I")
    if t.delete_when:
        status = F.when(F.expr(t.delete_when), F.lit("D")).otherwise(F.lit("I"))

    return (df
            .withColumn("ds_key", _hash_of(keys))
            .withColumn("ds_hash", _hash_of(cfg.hash_columns))
            .withColumn("ds_record_status", status)
            .withColumn("ds_partition_date", part)
            .withColumn("ds_cdc_index", _cdc_index_col(df, t.cdc_index))
            .withColumn("ds_etl_timestamp", F.current_timestamp())
            .withColumn("ds_etl_job_id", F.lit(run_id))
            .withColumn("ds_source", F.lit(source_label)))


def dedup(df, cfg: SilverConfig):
    """Mỗi ds_key giữ bản ghi có ds_cdc_index lớn nhất.

    Hoà thì ưu tiên dòng raw đọc sau (`_x_ord`) — cùng mốc thì bản kéo sau
    là bản đúng hơn. Không có key thì không khử (bảng sự kiện).
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not cfg.technical.keys:
        return df
    w = Window.partitionBy("ds_key").orderBy(
        F.col("ds_cdc_index").desc_nulls_last(), F.col(X.INGEST_TS).desc_nulls_last(),
        F.col(X.ORD).desc())
    return df.withColumn("_x_rn", F.row_number().over(w)).where("_x_rn = 1").drop("_x_rn")


def output_columns(cfg: SilverConfig) -> List[str]:
    """Thứ tự cột cuối cùng: nghiệp vụ rồi kỹ thuật (trừ cột riêng SCD2 — writer tự thêm)."""
    from .config import TECH_COLUMNS

    return [c.name for c in cfg.business_columns] + [t[0] for t in TECH_COLUMNS]


# ---------------- ghep lai ----------------


def build(spark, df, cfg: SilverConfig, run_id: str, source_label: str,
          data_date: Optional[dt.date] = None):
    """DataFrame chuỗi (từ extract) -> DataFrame sẵn sàng cho writer + báo cáo."""
    from pyspark.sql import functions as F

    rep = TransformReport()
    df, flags = apply_types(df, cfg)
    if cfg.source.filter:
        df = df.where(F.expr(cfg.source.filter))
    df = add_technical(df, cfg, run_id, source_label, data_date)

    # Mot lan quet cho moi con so thong ke truoc dedup
    aggs = [F.count(F.lit(1)).alias("_n")]
    aggs += [F.sum(F.col(f).cast("int")).alias(f) for f in flags]
    aggs += [F.sum(F.col(c).isNull().cast("int")).alias(f"_nn_{c}") for c in cfg.quality.not_null]
    if cfg.technical.keys:
        aggs.append(F.countDistinct("ds_key").alias("_nk"))
    df = df.cache()
    stats = df.agg(*aggs).collect()[0].asDict()

    rep.rows_in = stats["_n"] or 0
    rep.cast_errors = {f.replace("_x_casterr_", ""): stats[f] or 0 for f in flags}
    rep.null_violations = {c: stats[f"_nn_{c}"] or 0 for c in cfg.quality.not_null}
    if cfg.technical.keys:
        rep.duplicate_keys = rep.rows_in - (stats["_nk"] or 0)

    out = dedup(df, cfg).select(*output_columns(cfg))
    rep.rows_out = rep.rows_in - rep.duplicate_keys if cfg.technical.keys else rep.rows_in
    check_quality(cfg, rep)
    return out, rep


def check_quality(cfg: SilverConfig, rep: TransformReport) -> None:
    qc = cfg.quality
    errors = []
    if rep.rows_out < qc.min_rows:
        errors.append(f"chi co {rep.rows_out} dong, can toi thieu {qc.min_rows}")
    for col, n in rep.null_violations.items():
        if n:
            errors.append(f"{n} dong NULL o cot not_null '{col}'")
    if rep.rows_in:
        for col, n in rep.cast_errors.items():
            ratio = n / rep.rows_in
            if n and ratio > qc.max_cast_error_ratio:
                errors.append(f"cot '{col}': {n} gia tri khong ep duoc kieu "
                              f"({ratio:.2%} > {qc.max_cast_error_ratio:.2%})")
    if rep.duplicate_keys:
        msg = f"{rep.duplicate_keys} dong trung ds_key trong lo (da giu ban moi nhat)"
        (errors if qc.duplicate_keys == "fail" else rep.warnings).append(msg)

    if errors:
        if qc.on_fail == "warn":
            rep.warnings.extend(errors)
        else:
            raise QualityError(f"[{cfg.name}] quality that bai: " + "; ".join(errors))
