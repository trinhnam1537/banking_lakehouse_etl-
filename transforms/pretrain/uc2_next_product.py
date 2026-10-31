"""PRE-TRAIN — UC2 "sản phẩm tiếp theo": bộ dữ liệu huấn luyện từ gold Berka.

Đầu vào: track_a_gold.berka_client_month + berka_product_event (transforms.gold.berka_uc2).
Đầu ra (track_a_pretrain):

  uc2_next_product        mỗi dòng = (khách, tháng chụp m, sản phẩm p CHƯA giữ tại m)
      label = 1 nếu khách mua p trong H tháng SAU m (m+1 .. m+H), ngược lại 0
      đặc trưng = toàn bộ cột của client_month tại m (kể cả has_* của sản phẩm khác)
      split = train | valid | test theo THỜI GIAN của m
  uc2_next_product_stats  (split, product): số dòng, số dương, tỉ lệ dương — kiểm nhanh
                          trước khi huấn luyện (mất cân bằng, tập nào rỗng)

Dạng "dài" (một dòng một cặp khách-sản phẩm) thay vì 6 cột nhãn: mô hình xếp hạng
(LightGBM lambdarank, two-tower) và mô hình propensity theo từng sản phẩm đều đọc thẳng
được; cột `product` làm đặc trưng phân loại hoặc làm khoá lọc.

Ba điều kiện chống rò rỉ (leakage), là lý do tầng này tách khỏi gold:
  1. Đặc trưng ở m chỉ dùng dữ liệu <= cuối tháng m (bất biến của gold).
  2. Bỏ cặp (khách, p) mà khách ĐÃ giữ p tại m — không có gì để dự đoán.
  3. Chia tập theo thời gian, không ngẫu nhiên: train < VALID_START <= valid < TEST_START <= test.
     Bỏ tháng chụp có cửa sổ nhãn vượt quá dữ liệu (m + H > tháng cuối) — nhãn 0 ở đó là
     "chưa biết", không phải "không mua".
  Thêm: bỏ tháng có tenure < MIN_TENURE (chưa đủ lịch sử cho đặc trưng 3 tháng).

Tham số: GOLD_DB, PRETRAIN_DB, HORIZON_MONTHS (3), MIN_TENURE (3),
         VALID_START (1997-07-01), TEST_START (1998-01-01), FEATURE_VERSION (v1).
"""

from __future__ import annotations

from typing import Dict

from ..common import table, write_table
from ..gold.berka_uc2 import PRODUCTS

ID_COLS = ("client_id", "month", "month_end")


def run(spark, args: Dict[str, str]) -> Dict[str, int]:
    from pyspark.sql import functions as F

    gold = args.get("GOLD_DB", "track_a_gold")
    out_db = args.get("PRETRAIN_DB", "track_a_pretrain")
    horizon = int(args.get("HORIZON_MONTHS", "3"))
    min_tenure = int(args.get("MIN_TENURE", "3"))
    valid_start = args.get("VALID_START", "1997-07-01")
    test_start = args.get("TEST_START", "1998-01-01")
    version = args.get("FEATURE_VERSION", "v1")

    cm = table(spark, gold, "berka_client_month")
    ev = table(spark, gold, "berka_product_event")
    last_month = cm.agg(F.max("month")).first()[0]
    print(f"  thang cuoi {last_month}, horizon {horizon}, valid tu {valid_start}, test tu {test_start}")

    snap = cm.where((F.add_months("month", horizon) <= F.lit(last_month))
                    & (F.col("tenure_months") >= min_tenure))
    acq = ev.groupBy("client_id").pivot("product", list(PRODUCTS)).agg(F.min("acquired_month"))
    snap = snap.join(acq.select("client_id", *[F.col(p).alias(f"acq_{p}") for p in PRODUCTS]),
                     "client_id", "left")

    feature_cols = [c for c in cm.columns if c not in ID_COLS]
    parts = []
    for p in PRODUCTS:
        acq_p = F.col(f"acq_{p}")
        parts.append(
            snap.where(~F.col(f"has_{p}"))
            .select(*ID_COLS, F.lit(p).alias("product"),
                    (acq_p.isNotNull() & (acq_p > F.col("month"))
                     & (acq_p <= F.add_months("month", horizon))).cast("int").alias("label"),
                    *feature_cols))
    ds = parts[0]
    for d in parts[1:]:
        ds = ds.unionByName(d)

    ds = (ds.withColumn("split", F.when(F.col("month") < F.lit(valid_start).cast("date"), "train")
                        .when(F.col("month") < F.lit(test_start).cast("date"), "valid").otherwise("test"))
          .withColumn("horizon_months", F.lit(horizon))
          .withColumn("feature_version", F.lit(version))
          .withColumn("built_at", F.current_timestamp()))

    n = write_table(ds, out_db, "uc2_next_product", partition_by=["split"],
                    comment=f"UC2 next-product Berka: label = mua san pham trong {horizon} thang sau thang chup")
    stats = (spark.table(f"glue_catalog.{out_db}.uc2_next_product")
             .groupBy("split", "product")
             .agg(F.count("*").alias("rows"), F.sum("label").alias("positives"),
                  F.round(F.avg("label"), 5).alias("positive_rate"),
                  F.countDistinct("client_id").alias("clients"),
                  F.min("month").alias("from_month"), F.max("month").alias("to_month")))
    write_table(stats, out_db, "uc2_next_product_stats", comment="Phan bo nhan theo split x san pham")
    for r in stats.orderBy("split", "product").collect():
        print(f"  {r['split']:<5} {r['product']:<13} rows={r['rows']:>7} pos={r['positives']:>5} "
              f"rate={r['positive_rate']}")
    return {"uc2_next_product": n}
