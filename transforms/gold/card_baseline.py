"""GOLD — baseline chi tiêu từng thẻ, cho rule gian lận realtime (ClickHouse).

Hai bảng, tính lại toàn bộ từ silver_cards mỗi lần (createOrReplace):

  card_spend_baseline     (card_token) — thói quen của TỪNG thẻ trong BASELINE_DAYS ngày
      số giao dịch, trung bình/độ lệch, phân vị số tiền, tỷ lệ giao dịch đêm/online,
      khoảng cách nhà -> merchant.
  card_category_baseline  (category)   — phân vị số tiền theo nhóm merchant. Dùng khi
      thẻ mới, chưa đủ lịch sử để so với chính nó.

Rule "số tiền bất thường" ở ClickHouse so với p90 của thẻ, KHÔNG so với p99 hay
trung bình + k độ lệch: nhãn gian lận đến trễ 20 ngày nên baseline lẫn cả giao dịch
gian lận cũ. Một đợt 3-8 giao dịch gian lận trong ~200 giao dịch của thẻ đẩy p99 và
độ lệch chuẩn lên mức của chính gian lận, còn p90 chỉ lệch khi > 10% là gian lận.

Giờ "đêm" (0-4h) tính trên trans_ts, đúng như simulator sinh (giờ UTC không múi).

SERVING_EXPORT (vd s3://bucket/serving/realtime): ghi thêm bản Parquet một file của
hai bảng ra <export>/card_baseline/ và <export>/category_baseline/. ClickHouse trên
máy nguồn đọc bản này mỗi giờ (repo hạ tầng modules/kafka/realtime) — nó không đọc
được Iceberg qua Glue Catalog + Lake Formation, và không cần: chỉ cần ảnh chụp mới nhất.

Tham số (--KHOA): CARDS_DB (silver_cards), GOLD_DB (track_a_gold), BASELINE_DAYS (90),
SERVING_EXPORT (rỗng = không xuất).
"""

from __future__ import annotations

from typing import Dict

from ..common import CATALOG, current, table, write_table

NIGHT_HOURS = 5  # 0h-4h59


def _distance_km(F, lat1, lon1, lat2, lon2):
    """Haversine, km."""
    r = F.lit(6371.0)
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dp, dl = F.radians(lat2 - lat1), F.radians(lon2 - lon1)
    a = F.sin(dp / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dl / 2) ** 2
    return 2 * r * F.asin(F.sqrt(a))


def run(spark, args: Dict[str, str]) -> Dict[str, int]:
    from pyspark.sql import functions as F

    src = args.get("CARDS_DB", "silver_cards")
    gold = args.get("GOLD_DB", "track_a_gold")
    days = int(args.get("BASELINE_DAYS", "90"))
    export = (args.get("SERVING_EXPORT") or "").rstrip("/")

    tx = current(table(spark, src, "card_authorization"), "trans_num")
    as_of = tx.agg(F.max("txn_date")).first()[0]
    if as_of is None:
        print(f"  {src}.card_authorization rong -> bo qua")
        return {}
    print(f"  baseline {days} ngay toi {as_of}")

    amt = F.col("amount").cast("double")
    tx = (tx.where(F.col("txn_date") > F.date_sub(F.lit(as_of), days))
          .select("card_token", "category", "txn_date", amt.alias("amt"),
                  (F.hour("trans_ts") < NIGHT_HOURS).cast("int").alias("is_night"),
                  F.col("category").endswith("_net").cast("int").alias("is_online"),
                  _distance_km(F, F.col("lat"), F.col("lon"), F.col("merch_lat"), F.col("merch_lon"))
                  .alias("distance_km"))
          .cache())

    q = F.percentile_approx("amt", [0.5, 0.9, 0.95, 0.99], 1000)
    card = (tx.groupBy("card_token").agg(
                F.count("*").alias("n_txn"),
                F.countDistinct("txn_date").alias("n_days"),
                F.avg("amt").alias("avg_amt"),
                F.coalesce(F.stddev_samp("amt"), F.lit(0.0)).alias("std_amt"),
                q.alias("_q"),
                F.max("amt").alias("max_amt"),
                F.avg("is_night").alias("night_ratio"),
                F.avg("is_online").alias("online_ratio"),
                F.avg("distance_km").alias("avg_distance_km"),
                F.percentile_approx("distance_km", 0.95, 1000).alias("p95_distance_km"),
                F.min("txn_date").alias("first_seen"),
                F.max("txn_date").alias("last_seen"))
            .select("card_token", "n_txn", "n_days", "avg_amt", "std_amt",
                    *[F.col("_q")[i].alias(c) for i, c in enumerate(("p50_amt", "p90_amt", "p95_amt", "p99_amt"))],
                    "max_amt", "night_ratio", "online_ratio", "avg_distance_km", "p95_distance_km",
                    "first_seen", "last_seen",
                    F.lit(as_of).alias("as_of"), F.lit(days).alias("baseline_days")))

    category = (tx.groupBy("category").agg(
                    F.count("*").alias("n_txn"),
                    F.avg("amt").alias("avg_amt"),
                    F.coalesce(F.stddev_samp("amt"), F.lit(0.0)).alias("std_amt"),
                    q.alias("_q"))
                .select("category", "n_txn", "avg_amt", "std_amt",
                        *[F.col("_q")[i].alias(c) for i, c in enumerate(("p50_amt", "p90_amt", "p95_amt", "p99_amt"))],
                        F.lit(as_of).alias("as_of"), F.lit(days).alias("baseline_days")))

    n_card = write_table(card, gold, "card_spend_baseline",
                         comment=f"Baseline chi tieu tung the ({days} ngay) cho rule realtime ClickHouse")
    n_cat = write_table(category, gold, "card_category_baseline",
                        comment=f"Baseline so tien theo nhom merchant ({days} ngay), du phong cho the moi")
    tx.unpersist()

    if export:
        # Doc lai tu bang Iceberg vua ghi: ban xuat trung dung du lieu gold, khong tinh lai
        for name, sub in (("card_spend_baseline", "card_baseline"), ("card_category_baseline", "category_baseline")):
            path = f"{export}/{sub}/"
            spark.table(f"{CATALOG}.{gold}.{name}").coalesce(1).write.mode("overwrite").parquet(path)
            print(f"  xuat {name} -> {path}")
    return {"card_spend_baseline": n_card, "card_category_baseline": n_cat}
