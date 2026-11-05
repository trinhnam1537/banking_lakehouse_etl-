"""Raw NDJSON -> DataFrame mỗi dòng một bản ghi, mỗi cột entry một chuỗi.

Ba bước, cố ý tách để mỗi bước làm đúng một việc:

  1. read_lines   đọc file thành từng dòng TEXT — không suy schema. Raw nhiều
                  hình dạng; để Spark đoán schema trên 18 MB exchangeInfo vừa
                  chậm vừa sai khi hai dòng khác cấu trúc.
  2. explode      bóc mảng bản ghi theo records_path; lấy cột envelope
                  (from: response/request/line) TRƯỚC khi nổ mảng — nếu làm sau,
                  mỗi bản ghi phải parse lại cả dòng 18 MB.
  3. select       một lần from_json với schema sinh từ các path của entry.

Đầu ra: mọi cột là STRING. Ép kiểu là việc của transform.py.
"""

from __future__ import annotations

from typing import List, Optional

from .config import ColumnFrom, Shape, SilverConfig
from .types import json_path, path_tree, split_conflicts, split_path, tree_to_ddl

# Cot noi bo, tien to `_x_` de khong dung ten cot nghiep vu
LINE = "_x_line"
REC = "_x_rec"
FILE = "_x_file"
INGEST_DATE = "_x_ingest_date"
INGEST_TS = "_x_ingest_ts"
ORD = "_x_ord"

INTERNAL = [LINE, REC, FILE, INGEST_DATE, INGEST_TS, ORD]


def read_lines(spark, files: List[str]):
    """Mỗi dòng raw một hàng. `.gz` được Spark tự giải nén theo đuôi file."""
    from pyspark.sql import functions as F

    return (spark.read.text(files)
            .withColumnRenamed("value", LINE)
            .withColumn(FILE, F.input_file_name())
            # ingest_date lay tu DUONG DAN, khong tu noi dung: dung ca voi nguon
            # khong gan _src_ingest_ts (file ben ngoai day vao landing/).
            .withColumn(INGEST_DATE, F.regexp_extract(F.col(FILE), r"ingest_date=(\d{4}-\d{2}-\d{2})", 1))
            .withColumn(INGEST_TS, F.get_json_object(F.col(LINE), "$._src_ingest_ts")))


def _records_array(shape: Shape, records_path: Optional[str]):
    """Cột chuỗi JSON của mảng/đối tượng bản ghi trong một dòng; None = cả dòng."""
    from pyspark.sql import functions as F

    if shape is Shape.API_RESPONSE:
        return F.get_json_object(F.col(LINE), json_path(records_path or "", "$['_response']"))
    if shape is Shape.KAFKA_MESSAGE:
        # value la CHUOI JSON long trong dong -> get_json_object tra chuoi do
        return F.get_json_object(F.col(LINE), "$['value']")
    return None  # RECORDS: ca dong la ban ghi


def explode_records(lines, cfg: SilverConfig, shape: Shape, records_path: Optional[str]):
    """Một hàng mỗi bản ghi nghiệp vụ, cột REC là chuỗi JSON của bản ghi đó."""
    from pyspark.sql import functions as F

    # 1) cot envelope: tinh o muc DONG, truoc khi no mang
    roots = {ColumnFrom.RESPONSE: "$['_response']", ColumnFrom.REQUEST: "$['_request']",
             ColumnFrom.LINE: "$"}
    df = lines
    for c in cfg.columns:
        if c.expr or c.from_ is ColumnFrom.RECORD:
            continue
        df = df.withColumn(c.name, F.get_json_object(F.col(LINE), json_path(c.source_path, roots[c.from_])))

    # 2) no mang ban ghi
    arr = _records_array(shape, records_path)
    if arr is None:
        df = df.withColumn(REC, F.col(LINE))
    else:
        # Response co the la MANG ban ghi (FDIC data) hoac MOT object (SEC
        # submissions, AV overview). Object thi boc thanh mang mot phan tu.
        as_list = F.when(F.ltrim(arr).startswith("["), F.from_json(arr, "array<string>")) \
                   .otherwise(F.when(arr.isNotNull(), F.array(arr)))
        df = df.withColumn(REC, F.explode_outer(as_list))
        # explode_outer giu dong co response rong (API loi) de dem duoc,
        # roi bo o day — mot dong khong ban ghi khong phai du lieu.
        df = df.where(F.col(REC).isNotNull())

    # BO DONG RAW NGAY SAU KHI NO: moi ban ghi van mang ca dong goc (FDIC:
    # 3 MB/dong x 27.834 ban ghi = ~83 GB khi cache -> OutOfMemory, da gap
    # 09-28). Envelope va REC da boc xong, dong goc khong con can.
    return df.drop(LINE).withColumn(ORD, F.monotonically_increasing_id())


def select_record_columns(df, cfg: SilverConfig):
    """Bóc cột `from: record` bằng MỘT lần from_json, schema sinh từ path."""
    from pyspark.sql import functions as F

    rec_cols = [c for c in cfg.columns if not c.expr and c.from_ is ColumnFrom.RECORD]
    ok, conflict = split_conflicts([c.source_path for c in rec_cols])

    if ok:
        df = df.withColumn("_x_parsed", F.from_json(F.col(REC), tree_to_ddl(path_tree(ok))))
    for c in rec_cols:
        if c.source_path in ok:
            node = F.col("_x_parsed")
            for part in split_path(c.source_path):
                node = node.getField(part)
            df = df.withColumn(c.name, node)
        else:
            # Duong xung dot (vua la la vua la nhanh cua duong khac) -> boc rieng
            df = df.withColumn(c.name, F.get_json_object(F.col(REC), json_path(c.source_path)))
    # Chuoi ban ghi cung bo — cot da boc xong; giu lai chi ton bo nho khi cache
    return df.drop("_x_parsed", REC)


def extract(spark, files: List[str], cfg: SilverConfig, shape: Shape, records_path: Optional[str]):
    lines = read_lines(spark, files)
    df = explode_records(lines, cfg, shape, records_path)
    return select_record_columns(df, cfg)
