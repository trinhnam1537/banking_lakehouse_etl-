"""GOLD — UC2 gợi ý sản phẩm ngân hàng, dữ liệu Berka (silver_berka).

Hai bảng, đều tính lại toàn bộ từ silver mỗi lần (createOrReplace):

  berka_product_event   (client_id, product, acquired_month, source)
      Tháng ĐẦU TIÊN khách sở hữu mỗi sản phẩm — "sự kiện mua" là nhãn của UC2.
  berka_client_month    (client_id, month) — ảnh chụp CUỐI THÁNG của mỗi khách
      hồ sơ + hành vi giao dịch (1 và 3 tháng gần nhất) + cờ đang giữ từng sản phẩm.

Sáu sản phẩm suy ra được từ Berka (1993-1998):

  loan           loan.granted_date                     tài khoản -> khách OWNER
  card_classic   card.issued_date, card_type=classic   thẻ gắn disp -> khách (OWNER/DISPONENT)
  card_junior    card_type=junior
  card_gold      card_type=gold
  insurance      giao dịch đầu tiên k_symbol=POJISTNE  (lệnh định kỳ trả phí bảo hiểm)
  household      giao dịch đầu tiên k_symbol=SIPO      (lệnh định kỳ thanh toán hộ gia đình)

Vì sao bảo hiểm/hộ gia đình lấy từ GIAO DỊCH chứ không từ standing_order: bảng lệnh
định kỳ của Berka là ảnh chụp cuối kỳ, KHÔNG có ngày tạo — không biết khách bắt đầu
khi nào. Khoản trừ POJISTNE/SIPO đầu tiên trên sổ giao dịch là mốc sớm nhất quan sát được.

Point-in-time: mọi đặc trưng của tháng m chỉ dùng giao dịch có trans_date <= cuối tháng m.
Tầng pre-train dựa vào bất biến này để không rò rỉ tương lai vào đặc trưng.

Tham số (--KHOA): SOURCE_DB (silver_berka), GOLD_DB (track_a_gold).
"""

from __future__ import annotations

from typing import Dict

from ..common import current, table, write_table

PRODUCTS = ("loan", "card_classic", "card_junior", "card_gold", "insurance", "household")


def run(spark, args: Dict[str, str]) -> Dict[str, int]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    src = args.get("SOURCE_DB", "silver_berka")
    gold = args.get("GOLD_DB", "track_a_gold")
    month = lambda c: F.trunc(F.col(c), "month")  # noqa: E731

    client = current(table(spark, src, "client"), "client_id")
    disp = current(table(spark, src, "disp"), "disp_id")
    account = current(table(spark, src, "account"), "account_id")
    district = current(table(spark, src, "district"), "district_id")
    loan = current(table(spark, src, "loan"), "loan_id")
    card = current(table(spark, src, "card"), "card_id")
    trans = current(table(spark, src, "trans"), "trans_id").cache()

    owner = disp.where(F.col("is_owner")).select("client_id", "account_id")
    last_month = trans.agg(F.max(month("trans_date"))).first()[0]
    print(f"  du lieu giao dich toi thang {last_month}")

    # ---------- 1. Sự kiện mua sản phẩm ----------
    ev_loan = (owner.join(loan, "account_id")
               .select("client_id", F.lit("loan").alias("product"),
                       month("granted_date").alias("acquired_month"), F.lit("loan").alias("source")))
    ev_card = (disp.join(card, "disp_id")
               .select("client_id", F.concat(F.lit("card_"), F.lower(F.trim("card_type"))).alias("product"),
                       month("issued_date").alias("acquired_month"), F.lit("card").alias("source")))
    ksym = F.upper(F.trim(F.col("k_symbol")))
    ev_so = (trans.where(ksym.isin("POJISTNE", "SIPO"))
             .join(owner, "account_id")
             .select("client_id",
                     F.when(ksym == "POJISTNE", "insurance").otherwise("household").alias("product"),
                     month("trans_date").alias("acquired_month"), F.lit("trans.k_symbol").alias("source")))
    events = (ev_loan.unionByName(ev_card).unionByName(ev_so)
              .where(F.col("product").isin(*PRODUCTS))
              .groupBy("client_id", "product")
              .agg(F.min("acquired_month").alias("acquired_month"), F.first("source").alias("source")))

    # ---------- 2. Hành vi theo tháng của mỗi tài khoản -> khách ----------
    amt = F.col("signed_amount").cast("double")
    acc_m = (trans.groupBy("account_id", month("trans_date").alias("month")).agg(
        F.count("*").alias("txn_cnt"),
        F.sum(F.when(amt > 0, amt).otherwise(0.0)).alias("inflow"),
        F.sum(F.when(amt < 0, -amt).otherwise(0.0)).alias("outflow"),
        F.avg(F.col("balance_after").cast("double")).alias("avg_balance"),
        F.min(F.col("balance_after").cast("double")).alias("min_balance"),
        # So du cuoi thang = so du sau giao dich CUOI CUNG trong thang
        F.max_by(F.col("balance_after").cast("double"), F.struct("trans_date", "trans_id")).alias("end_balance"),
        F.sum(F.when(F.upper(F.col("operation_code")) == "VYBER", 1).otherwise(0)).alias("cash_wd_cnt"),
        F.sum(F.when(F.upper(F.col("operation_code")) == "VYBER KARTOU", 1).otherwise(0)).alias("card_wd_cnt"),
        F.max(F.when(ksym == "DUCHOD", 1).otherwise(0)).alias("pension_flag"),
        F.max(F.when(F.col("balance_after") < 0, 1).otherwise(0)).alias("overdraft_flag"),
    ))
    cli_m = (owner.join(acc_m, "account_id").groupBy("client_id", "month").agg(
        *[F.sum(c).alias(c) for c in ("txn_cnt", "inflow", "outflow", "end_balance",
                                      "cash_wd_cnt", "card_wd_cnt")],
        F.sum("avg_balance").alias("avg_balance"), F.sum("min_balance").alias("min_balance"),
        F.max("pension_flag").alias("pension_flag"), F.max("overdraft_flag").alias("overdraft_flag"),
    ))

    # ---------- 3. Lưới khách x tháng: từ tháng mở tài khoản đầu tiên tới cuối dữ liệu ----------
    start = (owner.join(account, "account_id").groupBy("client_id").agg(
        F.min(month("opened_date")).alias("first_open_month"),
        F.countDistinct("account_id").alias("n_accounts")))
    grid = start.select("client_id", "first_open_month", "n_accounts",
                        F.explode(F.sequence("first_open_month", F.lit(last_month),
                                             F.expr("interval 1 month"))).alias("month"))

    w_hist = Window.partitionBy("client_id").orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
    w3 = Window.partitionBy("client_id").orderBy("month").rowsBetween(-2, 0)
    cm = grid.join(cli_m, ["client_id", "month"], "left")
    for c in ("txn_cnt", "inflow", "outflow", "cash_wd_cnt", "card_wd_cnt", "pension_flag", "overdraft_flag"):
        cm = cm.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
    cm = (cm
          # Thang khong co giao dich: so du giu nguyen thang truoc (forward fill)
          .withColumn("end_balance", F.last("end_balance", ignorenulls=True).over(w_hist))
          .withColumn("avg_balance", F.coalesce("avg_balance", "end_balance"))
          .withColumn("min_balance", F.coalesce("min_balance", "end_balance"))
          .withColumn("txn_cnt_3m", F.sum("txn_cnt").over(w3))
          .withColumn("inflow_3m", F.sum("inflow").over(w3))
          .withColumn("outflow_3m", F.sum("outflow").over(w3))
          .withColumn("cash_wd_cnt_3m", F.sum("cash_wd_cnt").over(w3))
          .withColumn("card_wd_cnt_3m", F.sum("card_wd_cnt").over(w3))
          .withColumn("min_balance_3m", F.min("min_balance").over(w3))
          .withColumn("avg_balance_3m", F.avg("avg_balance").over(w3))
          .withColumn("pension_3m", F.max("pension_flag").over(w3))
          .withColumn("overdraft_3m", F.max("overdraft_flag").over(w3))
          .withColumn("inflow_trend", F.col("inflow") / F.greatest(F.col("inflow_3m") / 3, F.lit(1.0)))
          .withColumn("net_flow_3m", F.col("inflow_3m") - F.col("outflow_3m"))
          .withColumn("tenure_months", F.months_between("month", "first_open_month").cast("int")))

    # ---------- 4. Hồ sơ khách + vùng ----------
    prof = (client.join(district.select(
                "district_id", "region", F.col("avg_salary").cast("double").alias("district_avg_salary"),
                F.col("unemployment_96").cast("double").alias("district_unemployment_96"),
                F.col("urban_ratio").cast("double").alias("district_urban_ratio"),
                F.col("inhabitants").alias("district_inhabitants"),
                F.col("entrepreneurs_per_1000").alias("district_entrepreneurs_per_1000")),
                "district_id", "left")
            .select("client_id", "gender", "birth_date", "district_id", "region", "district_avg_salary",
                    "district_unemployment_96", "district_urban_ratio", "district_inhabitants",
                    "district_entrepreneurs_per_1000"))
    cm = (cm.join(prof, "client_id", "left")
          .withColumn("age", F.floor(F.months_between("month", "birth_date") / 12).cast("int"))
          .drop("birth_date"))

    # ---------- 5. Cờ đang giữ từng sản phẩm tại cuối tháng ----------
    acq = events.groupBy("client_id").pivot("product", list(PRODUCTS)).agg(F.min("acquired_month"))
    cm = cm.join(acq.select("client_id", *[F.col(p).alias(f"acq_{p}") for p in PRODUCTS]), "client_id", "left")
    for p in PRODUCTS:
        cm = cm.withColumn(f"has_{p}", (F.col(f"acq_{p}").isNotNull() & (F.col(f"acq_{p}") <= F.col("month"))))
    cm = (cm.withColumn("n_products", sum(F.col(f"has_{p}").cast("int") for p in PRODUCTS))
          .drop(*[f"acq_{p}" for p in PRODUCTS], "pension_flag", "overdraft_flag")
          .withColumn("month_end", F.last_day("month")))

    n_ev = write_table(events, gold, "berka_product_event",
                       comment="UC2: thang dau tien khach so huu tung san pham (Berka)")
    n_cm = write_table(cm, gold, "berka_client_month", partition_by=None,
                       comment="UC2: anh chup cuoi thang khach x thang - ho so, hanh vi, san pham dang giu (Berka)")
    trans.unpersist()
    return {"berka_product_event": n_ev, "berka_client_month": n_cm}
